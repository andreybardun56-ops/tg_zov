# tg_zov/services/event_manager.py
import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from config import ADMIN_IDS
from services.accounts_manager import load_all_users
from services.event_checker import check_all_events
from services.gas_event import run_gas_event
from services.promo_code import run_promo_code, load_promo_history, save_promo_history
from services.puzzle2_bundle import run_puzzle2_all_sources
from services.flop_pair import run_flop_pair
from services.thanksgiving_event import run_thanksgiving_event
from services.castle_machine import run_castle_machine
from services.lucky_wheel_auto import run_lucky_wheel
from services.dragon_quest import run_dragon_quest
from services.browser_patches import (
    launch_masked_persistent_context,
    get_random_browser_profile,
    cookies_to_playwright,
    BROWSER_PATH,
)
from services.cookies_io import load_all_cookies, save_all_cookies
from playwright.async_api import async_playwright

logger = logging.getLogger("event_manager")

EVENT_HANDLERS = {
    "flop_pair": run_flop_pair,
    "thanksgiving_event": run_thanksgiving_event,
    "castle_machine": run_castle_machine,
    "lucky_wheel": run_lucky_wheel,
    "dragon_quest": run_dragon_quest,
    "gas": run_gas_event
}

PROMO_INBOX_TXT = Path("data/new_promo.txt")
PROMO_INBOX_JSON = Path("data/new_promo.json")
STATUS_FILE = Path("data/event_status.json")


# ────────────────────────────────────────────────
# 🔔 Проверка и активация новых промокодов
# ────────────────────────────────────────────────
async def check_and_apply_new_promo(bot=None) -> str | None:
    code = None
    if PROMO_INBOX_JSON.exists():
        try:
            with open(PROMO_INBOX_JSON, "r", encoding="utf-8") as f:
                obj = json.load(f)
            c = (obj or {}).get("code", "")
            if isinstance(c, str) and c.strip():
                code = c.strip().upper()
        except Exception as e:
            logger.warning(f"[PROMO] Ошибка чтения {PROMO_INBOX_JSON}: {e}")

    if not code and PROMO_INBOX_TXT.exists():
        try:
            c = PROMO_INBOX_TXT.read_text(encoding="utf-8").strip()
            if c:
                code = c.split()[0].strip().upper()
        except Exception as e:
            logger.warning(f"[PROMO] Ошибка чтения {PROMO_INBOX_TXT}: {e}")

    if not code:
        return None

    history = load_promo_history()
    if code in history:
        logger.info(f"[PROMO] Код {code} уже активировался ранее — пропуск.")
        for f in (PROMO_INBOX_JSON, PROMO_INBOX_TXT):
            f.unlink(missing_ok=True)
        return f"🎟️ Промокод {code} уже активирован ранее — пропуск."

    logger.info(f"[PROMO] 🚀 Новый промокод: {code}. Запуск активации…")
    results = await run_promo_code(code)
    history.append(code)
    save_promo_history(history)
    for f in (PROMO_INBOX_JSON, PROMO_INBOX_TXT):
        f.unlink(missing_ok=True)

    if bot:
        for user_id, msgs in results.items():
            if msgs:
                text = f"🎟️ Промокод <b>{code}</b>:\n\n" + "\n".join(msgs)
                await bot.send_message(user_id, text, parse_mode="HTML")

        applied_count = sum(len(v) for v in results.values())
        summary = (
            f"✅ Новый промокод активирован: <b>{code}</b>\n"
            f"👥 Пользователей: <b>{len(results)}</b>\n"
            f"📨 Сообщений: <b>{applied_count}</b>"
        )
        await bot.send_message(ADMIN_IDS[0], summary, parse_mode="HTML")

    return f"✅ Промокод {code} успешно обработан."


# ────────────────────────────────────────────────
# 🔄 Полный цикл: проверка акций → сбор активных
# ────────────────────────────────────────────────
async def run_full_event_cycle(bot=None, manual=False):
    logger.info("🚀 Запуск полного цикла проверки и сбора акций…")

    # 1️⃣ Проверяем свежесть event_status.json (не старше 10 минут)
    need_refresh = True
    if STATUS_FILE.exists():
        mtime = datetime.fromtimestamp(STATUS_FILE.stat().st_mtime)
        diff = datetime.now() - mtime
        if diff < timedelta(minutes=10):
            need_refresh = False
            logger.info(f"📄 Пропускаю повторную проверку — статус свежий ({diff.seconds // 60} мин назад)")

    # 2️⃣ Только если файл старый — обновляем статусы
    if need_refresh:
        logger.info("🔍 Проверяю и обновляю event_status.json через event_checker...")
        admin_id = ADMIN_IDS[0] if ADMIN_IDS else None
        await check_all_events(bot=bot, admin_id=admin_id)

    # 3️⃣ Загружаем актуальные данные
    event_status = {}
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                event_status = json.load(f)
            logger.info("📄 Загружен event_status.json")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка чтения {STATUS_FILE}: {e}")

    # 4️⃣ Определяем активные акции
    active_events = [name for name, active in event_status.items() if active]
    logger.info(f"✅ Активные акции: {', '.join(active_events) or 'нет'}")

    if not active_events:
        msg = "⏸ Нет активных акций — цикл завершён."
        logger.info(msg)
        if bot and ADMIN_IDS:
            await bot.send_message(ADMIN_IDS[0], msg, parse_mode="HTML")
        return {"success": False, "message": msg}

    # 5️⃣ Проверяем промокоды перед сбором
    try:
        promo_note = await check_and_apply_new_promo(bot)
        if promo_note:
            logger.info(f"[PROMO] {promo_note}")
    except Exception as e:
        logger.warning(f"[PROMO] Ошибка при проверке промокодов: {e}")

    # 6️⃣ Запускаем фарм активных акций
    all_users = load_all_users()
    total_success = total_errors = total_attempts_over = 0
    summary_lines = []
    # ❌ Пропускаем puzzle2, если она фактически не активна
    if "puzzle2" in active_events and not event_status.get("puzzle2", False):
        logger.info("⏸ Puzzle2 указана, но фактически не активна — пропускаем фарм.")
        active_events.remove("puzzle2")

    async def _send_result(event_key: str, user_id: str, uid: str, username: str, result: dict):
        nonlocal total_success, total_errors, total_attempts_over, summary_lines
        msg = result.get("message", "❓ Нет ответа")
        success = result.get("success", False)
        msg_text = result.get("message", "").lower()

        if "попытки" in msg_text and "закончились" in msg_text:
            prefix = "⚙️"
            total_attempts_over += 1
        elif success:
            prefix = "✅"
            total_success += 1
        else:
            prefix = "⚠️"
            total_errors += 1

        summary_lines.append(f"{prefix} <b>{username}</b> — {event_key}: {msg}")

        import re
        import html

        if bot:
            try:
                clean_msg = re.sub(r"<[^>]+>", "", str(msg))
                safe_msg = html.escape(clean_msg)

                if success:
                    await bot.send_message(
                        user_id,
                        f"✅ {event_key}: {safe_msg[:3800]}",
                        parse_mode="HTML"
                    )
                else:
                    await bot.send_message(
                        ADMIN_IDS[0],
                        f"❌ [{event_key}] {username} ({uid}): {safe_msg[:3800]}",
                        parse_mode="HTML"
                    )
            except Exception as e:
                logger.warning(f"[Telegram send] Ошибка отправки ({event_key}): {e}")
                try:
                    await bot.send_message(
                        user_id if success else ADMIN_IDS[0],
                        f"{'✅' if success else '❌'} [{event_key}] {msg[:3800]}",
                        parse_mode=None
                    )
                except Exception as inner:
                    logger.error(f"[Telegram send] Повторная ошибка ({event_key}): {inner}")

        await asyncio.sleep(1)

    async def run_with_single_session(event_keys: list[str]):
        nonlocal total_errors
        cookies_db = load_all_cookies()
        for user_id, accounts in all_users.items():
            for acc in accounts:
                uid = str(acc.get("uid"))
                username = acc.get("username", "Игрок")
                if not uid:
                    continue

                async with async_playwright() as p:
                    profile = get_random_browser_profile()
                    ctx = await launch_masked_persistent_context(
                        p,
                        user_data_dir=f"data/chrome_profiles/{uid}_events",
                        browser_path=BROWSER_PATH,
                        headless=True,
                        slow_mo=30,
                        profile=profile,
                    )
                    context = ctx["context"]
                    page = ctx["page"]

                    try:
                        acc_cookies = cookies_db.get(str(user_id), {}).get(str(uid), {})
                        if acc_cookies:
                            await context.add_cookies(cookies_to_playwright(acc_cookies))

                        for event_key in event_keys:
                            handler = EVENT_HANDLERS.get(event_key)
                            if not handler:
                                logger.warning(f"[{event_key}] ⚠️ Нет обработчика — пропуск.")
                                continue

                            logger.info(f"▶️ Запуск обработки события: {event_key}")
                            try:
                                result = await handler(user_id, uid, context=context)
                            except TypeError:
                                result = await handler(user_id, uid)

                            await _send_result(event_key, user_id, uid, username, result)

                    except Exception as e:
                        err = f"❌ [session] {username} ({uid}): {e}"
                        logger.exception(err)
                        total_errors += 1
                        summary_lines.append(err)
                    finally:
                        try:
                            if page:
                                await page.close()
                            if context:
                                fresh = await context.cookies()
                                fresh_map = {c["name"]: c["value"] for c in fresh if "name" in c}
                                if fresh_map:
                                    cookies_db.setdefault(str(user_id), {})[str(uid)] = fresh_map
                                    save_all_cookies(cookies_db)
                                await context.close()
                        except Exception:
                            pass

    if manual:
        await run_with_single_session(active_events)
    else:
        for event_key in active_events:
            handler = EVENT_HANDLERS.get(event_key)
            if not handler:
                logger.warning(f"[{event_key}] ⚠️ Нет обработчика — пропуск.")
                continue

            logger.info(f"▶️ Запуск обработки события: {event_key}")
            for user_id, accounts in all_users.items():
                for acc in accounts:
                    uid = str(acc.get("uid"))
                    username = acc.get("username", "Игрок")
                    try:
                        result = await handler(user_id, uid)
                        await _send_result(event_key, user_id, uid, username, result)

                    except Exception as e:
                        total_errors += 1
                        err = f"❌ [{event_key}] {username} ({uid}): {e}"
                        logger.exception(err)
                        summary_lines.append(err)

    # 7️⃣ Итог
    summary = (
        f"{'🔄 Ручной' if manual else '🕛 Ежедневный'} цикл завершён\n"
        f"Активные акции: {', '.join(active_events)}\n"
        f"✅ Успешно: {total_success}\n"
        f"⚙️ Попытки закончились: {total_attempts_over}\n"
        f"⚠️ Ошибок: {total_errors}\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}"
    )

    logger.info(summary)
    if bot and ADMIN_IDS:
        await bot.send_message(ADMIN_IDS[0], summary, parse_mode="HTML")

    return {"success": True, "message": summary}


# ────────────────────────────────────────────────
# ⏰ Планировщик: запуск каждый день в 10:02 (локальное) = 00:02 (UTC)
# ────────────────────────────────────────────────
async def schedule_daily_events(bot):
    LOCAL_TZ = timezone(timedelta(hours=10))
    SERVER_TZ = timezone.utc

    while True:
        now_local = datetime.now(LOCAL_TZ)
        next_run_local = now_local.replace(hour=10, minute=2, second=0, microsecond=0)
        if next_run_local <= now_local:
            next_run_local += timedelta(days=1)

        wait_seconds = (next_run_local - now_local).total_seconds()
        next_run_server = next_run_local.astimezone(SERVER_TZ)

        logger.info(
            f"[SCHEDULER] 🕛 Следующий запуск:\n"
            f"🌍 Серверное время (UTC): {next_run_server:%Y-%m-%d %H:%M:%S}\n"
            f"🕙 Локальное время (UTC+10): {next_run_local:%Y-%m-%d %H:%M:%S}"
        )

        await asyncio.sleep(wait_seconds)
        try:
            await run_full_event_cycle(bot)
        except Exception as e:
            logger.exception(f"[SCHEDULER] ❌ Ошибка цикла: {e}")