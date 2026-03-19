# tg_zov/handlers/callback.py
import asyncio
import time
import logging
import os
import json
from aiogram import Router, types, F
from aiogram.types import Message

from config import ADMIN_IDS
from services.flop_pair import run_flop_pair, find_flop_pairs
from services.castle_api import refresh_cookies_mvp
from services.castle_machine import run_castle_machine
from services.thanksgiving_event import run_thanksgiving_event
from services.promo_code import run_promo_code, load_promo_history, save_promo_history
from services.accounts_manager import get_active_account, load_all_users
from services.event_manager import run_full_event_cycle

router = Router()
logger = logging.getLogger("callback")

# 🔐 Глобальная блокировка, чтобы не запускать автосбор дважды
_RUN_LOCK = asyncio.Lock()
PROMO_HISTORY_FILE = "data/promo_history.json"


# ---------------------------------------
# 🔄 Обновить cookies (асинхронно, в фоне)
# ---------------------------------------
@router.message(F.text == "🔄 Обновить cookies")
async def handle_update_cookies(message: Message):
    """Асинхронное массовое обновление cookies в фоне (не блокирует бота)."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("🚫 Только для админов.")
        return

    await message.answer("♻️ Запускаю обновление cookies в фоне...")

    async def background_update():
        try:
            all_users = load_all_users()
            queue = []
            for user_id, accounts in all_users.items():
                if not isinstance(accounts, list):
                    continue
                for acc in accounts:
                    if not isinstance(acc, dict):
                        continue
                    queue.append((str(user_id), acc))

            summary = {
                "success": 0,
                "failed": 0,
                "skipped": [],
                "failures": [],
                "total": len(queue),
            }

            if not queue:
                await message.answer("⚠️ Нет аккаунтов для обновления cookies.")
                return

            for idx, (user_id, acc) in enumerate(queue, start=1):
                uid = str(acc.get("uid") or "").strip()
                username = acc.get("username") or "Игрок"
                prefix = f"🔁 <b>{idx}/{len(queue)}</b> — <b>{username}</b> (<code>{uid or '—'}</code>): "

                if not uid:
                    summary["skipped"].append(
                        {"user_id": user_id, "uid": None, "reason": "missing_uid"}
                    )
                    await message.answer(prefix + "⚠️ пропуск (нет UID)", parse_mode="HTML")
                    continue

                try:
                    result = await refresh_cookies_mvp(user_id, uid)
                except Exception as exc:
                    summary["failed"] += 1
                    summary["failures"].append(
                        {"user_id": user_id, "uid": uid, "error": str(exc)}
                    )
                    await message.answer(prefix + f"❌ ошибка: <i>{exc}</i>", parse_mode="HTML")
                    continue

                if result.get("success"):
                    summary["success"] += 1
                    await message.answer(prefix + "✅ cookies обновлены", parse_mode="HTML")
                else:
                    err = str(result.get("error", "unknown_error"))
                    summary["failed"] += 1
                    summary["failures"].append(
                        {"user_id": user_id, "uid": uid, "error": err}
                    )
                    await message.answer(prefix + f"❌ ошибка: <i>{err}</i>", parse_mode="HTML")

            summary_lines = [
                "📊 <b>Итоги обновления cookies:</b>",
                f"• ✅ Успешно: <b>{summary['success']}</b>",
                f"• ❌ Ошибки: <b>{summary['failed']}</b>",
                f"• ⚠️ Пропущено: <b>{len(summary['skipped'])}</b>",
                f"• Всего аккаунтов: <b>{summary['total']}</b>",
            ]

            if summary["failures"]:
                summary_lines.append("\n❌ <b>Ошибки:</b>")
                summary_lines.extend(
                    f" - {item['user_id']}:{item.get('uid', '—')} — {item['error']}"
                    for item in summary["failures"]
                )

            if summary["skipped"]:
                summary_lines.append("\n⚠️ <b>Пропущенные аккаунты:</b>")
                summary_lines.extend(
                    f" - {item['user_id']}:{item.get('uid', '—')} — {item['reason']}"
                    for item in summary["skipped"]
                )

            await message.answer("\n".join(summary_lines), parse_mode="HTML")
        except Exception as exc:
            logger.exception("Ошибка фонового обновления cookies: %s", exc)
            await message.answer(
                f"❌ Фоновое обновление cookies завершилось с ошибкой: <code>{exc}</code>",
                parse_mode="HTML",
            )

    # 🔥 Запуск в фоне — бот не блокируется
    asyncio.create_task(background_update())


# ---------------------------------------
# 🔍 Проверить пары (админская команда)
# ---------------------------------------
@router.message(F.text == "🔍 Проверить пары")
async def handle_find_pairs(message: Message):
    """Сканирует страницу акции 'Найди пару' и сохраняет найденные пары."""
    user_id = str(message.from_user.id)

    if int(user_id) not in ADMIN_IDS:
        await message.answer("🚫 Эта команда доступна только администраторам.")
        return

    await message.answer("🔍 Начинаю сканирование карт и поиск пар...")

    result = await find_flop_pairs(user_id)

    msg = result.get("message", "⚠️ Ошибка при поиске пар.")
    await message.answer(msg)


# ---------------------------------------
# 🃏 Найди пару
# ---------------------------------------
@router.message(F.text == "🃏 Найди пару")
async def handle_flop_pair(message: Message):
    """Запускает 'Найди пару' для всех аккаунтов пользователя."""
    user_id = str(message.from_user.id)
    from services.accounts_manager import get_all_accounts
    accounts = get_all_accounts(user_id)

    if not accounts:
        await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
        return

    await message.answer(f"🃏 Запускаю акцию 'Найди пару' для всех твоих аккаунтов ({len(accounts)} шт)...")

    total_success = 0
    total_fail = 0
    messages = []

    for acc in accounts:
        uid = acc.get("uid")
        username = acc.get("username", "Игрок")
        if not uid:
            continue

        await message.answer(f"🎯 Запуск для <b>{username}</b> (<code>{uid}</code>)...", parse_mode="HTML")

        try:
            result = await run_flop_pair(user_id, uid=uid)
            if result.get("success"):
                total_success += 1
            else:
                total_fail += 1
            messages.append(result.get("message", "⚠️ Неизвестный ответ"))
        except Exception as e:
            total_fail += 1
            messages.append(f"❌ Ошибка при обработке {uid}: {e}")
        await asyncio.sleep(2)

    summary = (
        f"✅ Завершено!\n"
        f"Всего аккаунтов: {len(accounts)}\n"
        f"Успешно: {total_success}\n"
        f"Ошибки: {total_fail}\n\n"
        "📜 Результаты:\n\n" + "\n\n".join(messages)
    )

    await message.answer(summary, parse_mode="HTML")


# ---------------------------------------
# 🧪 Тест
# ---------------------------------------
@router.message(F.text == "🧪 Тест")
async def handle_test(message: Message):
    """Проверка, что бот работает и активный аккаунт выбран."""
    user_id = str(message.from_user.id)
    active_acc = get_active_account(user_id)

    if active_acc:
        await message.answer(
            f"✅ Бот работает!\nАктивный аккаунт: <code>{active_acc['uid']}</code>",
            parse_mode="HTML"
        )
    else:
        await message.answer("✅ Бот работает! Но активный аккаунт не выбран.")


# ---------------------------------------
# 🔁 Автосбор наград
# ---------------------------------------
@router.message(F.text == "🔁 Автосбор наград")
async def handle_manual_autocollect(message: Message):
    """Ручной запуск автосбора для всех пользователей и аккаунтов (в фоне)."""
    user_id = message.from_user.id

    if user_id not in ADMIN_IDS:
        await message.answer("🚫 Эта команда доступна только администраторам.")
        return

    if _RUN_LOCK.locked():
        await message.answer("⏳ Автосбор уже выполняется. Дождитесь завершения текущего запуска.")
        return

    await message.answer(
        "🚀 Запускаю ручной автосбор по всем пользователям и аккаунтам…\n"
        "Я пришлю подробный отчёт по завершении."
    )

    async def _run():
        start_ts = time.perf_counter()
        async with _RUN_LOCK:
            try:
                result = await run_full_event_cycle(bot=message.bot, manual=True)
                took = time.perf_counter() - start_ts
                summary = (
                    f"✅ <b>Ручной автосбор завершён!</b>\n"
                    f"🕒 Время выполнения: <b>{took:.1f} сек</b>\n"
                    f"📄 Сообщение: {result.get('message', '—')}"
                )
                await message.answer(summary, parse_mode="HTML")
            except Exception as e:
                logger.exception(f"❌ Ошибка во время ручного автосбора: {e}")
                await message.answer(f"❌ Автосбор завершился с ошибкой: {e}", parse_mode="HTML")

    asyncio.create_task(_run())


# ---------------------------------------
# 🧩 Маленькая помощь (GAS) — асинхронно
# ---------------------------------------
from services.gas_event import run_gas_event

@router.message(F.text == "🧩 Маленькая помощь")
async def handle_gas_event(message: Message):
    """🧩 Асинхронный запуск события 'Маленькая помощь' для всех аккаунтов пользователя."""
    user_id = str(message.from_user.id)
    await message.answer("🧩 Запускаю событие <b>Маленькая помощь</b> в фоне... ⏳", parse_mode="HTML")

    async def background_gas():
        from services.accounts_manager import get_all_accounts
        accounts = get_all_accounts(user_id)
        if not accounts:
            await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
            return

        total = len(accounts)
        success_count = 0
        fail_count = 0

        # 🚀 Запускаем все аккаунты параллельно
        tasks = [run_gas_event(user_id, acc["uid"]) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for acc, res in zip(accounts, results):
            uid = acc.get("uid")
            username = acc.get("username", "Игрок")

            if isinstance(res, Exception):
                fail_count += 1
                await message.answer(f"❌ {username} ({uid}) — ошибка: {res}", parse_mode="HTML")
                continue

            msg = res.get("message", "⚠️ Неизвестный ответ")
            await message.answer(msg, parse_mode="HTML")

            if res.get("success"):
                success_count += 1
            else:
                fail_count += 1

        report = (
            f"🧩 <b>Маленькая помощь завершена!</b>\n\n"
            f"👥 Аккаунтов: <b>{total}</b>\n"
            f"✅ Успешно: <b>{success_count}</b>\n"
            f"⚠️ Ошибок: <b>{fail_count}</b>"
        )
        await message.answer(report, parse_mode="HTML")

    asyncio.create_task(background_gas())


# ---------------------------------------
# ⚙️ Создающая машина — асинхронно
# ---------------------------------------
@router.message(F.text == "⚙️ Создающая машина")
async def handle_castle_machine(message: types.Message):
    """⚙️ Асинхронный запуск акции 'Создающая машина'."""
    user_id = str(message.from_user.id)
    await message.answer("⚙️ Запускаю акцию <b>Создающая машина</b> в фоне... ⏳", parse_mode="HTML")

    async def background_castle_machine():
        from services.accounts_manager import get_all_accounts
        accounts = get_all_accounts(user_id)
        if not accounts:
            await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
            return

        total = len(accounts)
        success_count = 0
        fail_count = 0

        # 🚀 Параллельный запуск всех аккаунтов
        tasks = [run_castle_machine(user_id, acc["uid"]) for acc in accounts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for acc, res in zip(accounts, results):
            uid = acc.get("uid")
            username = acc.get("username", "Игрок")

            if isinstance(res, Exception):
                fail_count += 1
                await message.answer(f"❌ {username} ({uid}) — ошибка: {res}", parse_mode="HTML")
                continue

            msg = res.get("message", "❓ Неизвестный ответ")
            await message.answer(msg, parse_mode="HTML")

            if res.get("success"):
                success_count += 1
            else:
                fail_count += 1

        summary = (
            f"⚙️ <b>Создающая машина завершена!</b>\n\n"
            f"👥 Аккаунтов: <b>{total}</b>\n"
            f"✅ Успешно: <b>{success_count}</b>\n"
            f"⚠️ Ошибок: <b>{fail_count}</b>"
        )
        await message.answer(summary, parse_mode="HTML")

    asyncio.create_task(background_castle_machine())


# ---------------------------------------
# 🎁 10 дней призов (асинхронно, в фоне)
# ---------------------------------------
@router.message(F.text == "🎁 10 дней призов")
async def handle_thanksgiving_event(message: types.Message):
    """Ручной запуск акции '10 дней призов' (не блокирует бота)."""
    user_id = str(message.from_user.id)
    await message.answer("🎁 Запускаю акцию <b>10 дней призов</b> в фоне... ⏳", parse_mode="HTML")

    async def background_thanksgiving():
        try:
            from services.accounts_manager import get_all_accounts
            accounts = get_all_accounts(user_id)

            if not accounts:
                await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
                return

            total = len(accounts)
            success_count = 0
            fail_count = 0

            # Запускаем все аккаунты параллельно
            tasks = [run_thanksgiving_event(user_id, acc.get("uid")) for acc in accounts if acc.get("uid")]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Соответствие accounts/results по порядку uid
            seq_accounts = [acc for acc in accounts if acc.get("uid")]

            for acc, res in zip(seq_accounts, results):
                uid = acc.get("uid")
                username = acc.get("username", "Игрок")

                if isinstance(res, Exception):
                    fail_count += 1
                    await message.answer(f"❌ <b>{username}</b> ({uid}) — ошибка: {res}", parse_mode="HTML")
                else:
                    msg = res.get("message", "❓ Неизвестный ответ")
                    await message.answer(msg, parse_mode="HTML")
                    if res.get("success"):
                        success_count += 1
                    else:
                        fail_count += 1

            summary = (
                f"✅ <b>Проверка акции '10 дней призов' завершена!</b>\n\n"
                f"👥 Всего аккаунтов: <b>{total}</b>\n"
                f"✅ Успешно: <b>{success_count}</b>\n"
                f"⚠️ Ошибки: <b>{fail_count}</b>"
            )
            await message.answer(summary, parse_mode="HTML")

        except Exception as e:
            await message.answer(f"❌ Ошибка при запуске акции: <code>{e}</code>", parse_mode="HTML")

    # 🚀 Запуск в фоне — бот остаётся отзывчивым
    asyncio.create_task(background_thanksgiving())


# ------------------------------------------
# 🎟 Ввод промокода
# ------------------------------------------
@router.message(F.text == "🎁 Ввод промокода")
async def ask_promo_code(message: types.Message):
    await message.answer("🎁 Отправь промокод, я попробую активировать его на всех аккаунтах в базе.")


@router.message(F.text.regexp(r"^[A-Za-z0-9]+$"))
async def apply_promo_code(message: types.Message):
    code = message.text.strip().upper()
    history = load_promo_history()

    # Проверка на дубликаты
    if code in history:
        await message.answer(f"⚠️ Промокод <b>{code}</b> уже использовался ранее.", parse_mode="HTML")
        return

    await message.answer(f"🚀 Пробую активировать промокод <b>{code}</b> на всех аккаунтах...", parse_mode="HTML")

    results = await run_promo_code(code)

    # Сохраняем в историю
    history.append(code)
    save_promo_history(history)

    # Рассылаем пользователям отчёты
    for user_id, msgs in results.items():
        if not msgs:
            continue
        text = f"🎟 Результат промокода <b>{code}</b>:\n\n" + "\n".join(msgs)
        try:
            await message.bot.send_message(user_id, text, parse_mode="HTML")
        except Exception:
            pass

    await message.answer("✅ Промокод обработан по всем аккаунтам!", parse_mode="HTML")
