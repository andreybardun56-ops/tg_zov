# tg_zov/services/thanksgiving_event.py
import asyncio
import json
import logging
import aiohttp
from datetime import datetime

from services.browser_patches import run_event_with_browser
from services.accounts_manager import get_all_accounts
from services.castle_api import load_cookies_for_account

logger = logging.getLogger("thanksgiving_event")
from pathlib import Path

STATE_FILE = Path("data/thanksgiving_state.json")

def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

BASE_URL = "https://event-cc.igg.com/event/thanksgiving_time/"
API_URL = f"{BASE_URL}ajax.req.php?apid="

# 🎁 ID наград
ACHIEVE_IDS = ["achieve-6", "achieve-12", "achieve-18", "achieve-24"]
NORMAL_IDS = [f"normal-{i}" for i in range(1, 25)]


async def run_thanksgiving_event(user_id: str, uid: str = None, context=None) -> dict:
    """
    🎉 Акция "10 дней призов"
    Проверяет доступность события, даты и получает только актуальные награды.
    """
    accounts = get_all_accounts(user_id)
    if not accounts:
        return {"success": False, "message": "⚠️ У пользователя нет аккаунтов."}

    acc = next((a for a in accounts if a.get("uid") == uid), accounts[0])
    uid = acc.get("uid")
    username = acc.get("username", "Игрок")

    # 🕒 Проверка интервала между сборами
    state = load_state()
    uid_state = state.get(str(uid), {})
    last_claim = uid_state.get("last_claim")
    now = datetime.utcnow()

    if last_claim:
        try:
            dt = datetime.fromisoformat(last_claim)
            if (now - dt).total_seconds() < 12 * 3600:
                remaining = int(12 * 3600 - (now - dt).total_seconds()) // 60
                return {
                    "success": True,
                    "message": f"⏸ <b>{username}</b> ({uid}) — награды уже собирались недавно.\n"
                               f"Следующий сбор через {remaining} мин."
                }
        except Exception:
            pass

    cookies_dict = load_cookies_for_account(user_id, uid)
    if not cookies_dict:
        return {"success": False, "message": f"⚠️ Cookies не найдены ({username})."}

    async def handler(page):
        """Выполняется внутри запущенного браузера с маскировкой."""
        try:
            html = (await page.content()).lower()

            # 🕓 Проверка статуса события
            if any(x in html for x in ["событие еще не началось", "уже завершилось"]):
                return {
                    "success": True,
                    "message": f"⚠️ <b>{username}</b> ({uid}) — событие ещё не началось или уже завершилось."
                }

            # 📅 Извлекаем даты события
            event_period = "неизвестно"
            try:
                elem = await page.query_selector("div.chance span.event-time")
                if elem:
                    event_period = (await elem.inner_text()).strip()
            except Exception:
                pass

            # 🕒 Проверяем текущий день, чтобы не спамить наградами вне диапазона
            current_day = datetime.utcnow().day
            logger.info(f"[thanksgiving_event] {username} ({uid}) — проверка дня: {current_day}")

            # --- Подготавливаем сессию для API-запросов ---
            cookies = {name: value for name, value in cookies_dict.items()}
            headers = {
                "User-Agent": "Mozilla/5.0",
                "Referer": BASE_URL,
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
            }

            rewards_normal, rewards_achieve = [], []

            async with aiohttp.ClientSession(headers=headers, cookies=cookies) as session:
                async def claim(apid: str):
                    """Выполняет запрос получения награды."""
                    try:
                        async with session.get(f"{API_URL}{apid}", timeout=15) as resp:
                            text = await resp.text()
                            try:
                                data = json.loads(text)
                            except Exception:
                                data = None

                            if not data:
                                return False, f"⚠️ {apid}: неизвестный ответ"

                            status = str(data.get("status"))
                            msg = data.get("msg") or "Неизвестный ответ"

                            # 🎯 Состояния награды
                            if status == "1":
                                return True, f"✅ {apid}: {msg}"
                            elif status == "0":
                                if any(w in msg.lower() for w in ["через", "позже", "hours", "hour", "ещё недоступна"]):
                                    return None, f"⏸️ {apid}: {msg}"
                                elif any(w in msg.lower() for w in ["уже получена", "already claimed"]):
                                    return False, f"🔹 {apid}: уже получена"
                                return True, f"🟢 {apid}: {msg}"
                            return False, f"⚠️ {apid}: {text[:120]}"
                    except Exception as e:
                        return False, f"❌ {apid}: ошибка {e}"

                # --- Проверяем доступные обычные награды ---
                logger.info(f"[thanksgiving_event] {username} ({uid}) — начинаю сбор обычных наград")
                for apid in NORMAL_IDS:
                    result, msg = await claim(apid)
                    rewards_normal.append(msg)
                    if result is None:  # награда ещё недоступна
                        rewards_normal.append("⏸️ Следующая награда станет доступна позже (возможно через 12 часов).")
                        break
                    await asyncio.sleep(0.5)

                # --- Проверяем бонусные награды ---
                logger.info(f"[thanksgiving_event] {username} ({uid}) — начинаю сбор бонусных наград")
                for apid in ACHIEVE_IDS:
                    _, msg = await claim(apid)
                    rewards_achieve.append(msg)
                    await asyncio.sleep(0.5)

            # 🧾 Формируем итог
            summary = (
                f"📅 <b>Период:</b> {event_period}\n\n"
                f"📦 <b>Ежедневные награды:</b>\n" + "\n".join(rewards_normal) +
                "\n\n🎯 <b>Бонусные награды:</b>\n" + "\n".join(rewards_achieve)
            )

            msg = (
                f"🎉 <b>{username}</b> ({uid}) — акция <b>10 дней призов</b>\n\n"
                f"{summary}\n\n✅ Проверка завершена!"
            )

            # 🕒 сохраняем дату последнего успешного сбора
            state[str(uid)] = {
                "last_claim": datetime.utcnow().isoformat(),
                "collected": state.get(str(uid), {}).get("collected", 0) + 1
            }
            save_state(state)

            return {"success": True, "message": msg}

        except Exception as e:
            logger.exception(f"[thanksgiving_event] ❌ Ошибка в handler: {e}")
            return {"success": False, "message": f"❌ Ошибка выполнения: {e}"}

    # 🧠 Запускаем универсально через browser_patches
    return await run_event_with_browser(user_id, uid, BASE_URL, "10 дней призов", handler, context=context)