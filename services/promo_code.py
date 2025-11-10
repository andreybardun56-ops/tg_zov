# tg_zov/services/promo_code.py
import asyncio
import json
import os
import logging
from datetime import datetime

from services.browser_patches import run_event_with_browser
from services.accounts_manager import get_all_accounts, load_all_users
from services.castle_api import load_cookies_for_account

logger = logging.getLogger("promo_code")

PROMO_HISTORY_FILE = "data/promo_history.json"
CDKEY_URL = "https://event-cc.igg.com/event/cdkey/ajax.req.php?lang=de&iggid={uid}&cdkey={code}"


# ----------------------------- 💾 История -----------------------------
def load_promo_history() -> list:
    if not os.path.exists(PROMO_HISTORY_FILE):
        return []
    try:
        with open(PROMO_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_promo_history(history: list):
    os.makedirs(os.path.dirname(PROMO_HISTORY_FILE), exist_ok=True)
    with open(PROMO_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


# ----------------------------- 🧩 Активация одного промокода -----------------------------
async def activate_promo_for_account(page, uid: str, username: str, code: str) -> str:
    """
    Активирует промокод для одного аккаунта в рамках уже открытой сессии.
    Возвращает текст результата.
    """
    url = CDKEY_URL.format(uid=uid, code=code)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)

        text = await page.content()
        lower = text.lower()

        if any(w in lower for w in ["успешно", "success", "成功"]):
            return f"✅ <b>{username}</b> ({uid}): Успешно активирован!"
        elif any(w in lower for w in ["already", "已使用", "уже использ"]):
            return f"⚠️ <b>{username}</b> ({uid}): Код уже использован."
        elif any(w in lower for w in ["invalid", "無效", "неверный", "ошибка"]):
            return f"❌ <b>{username}</b> ({uid}): Неверный или недействительный код."
        else:
            snippet = text.strip().replace("\n", " ")[:120]
            return f"⚠️ <b>{username}</b> ({uid}): Неизвестный ответ — <code>{snippet}</code>"

    except Exception as e:
        return f"❌ <b>{username}</b> ({uid}): Ошибка {e}"


# ----------------------------- 🚀 Массовая активация -----------------------------
async def run_promo_code(code: str) -> dict:
    """
    🎁 Массовая активация промокода для всех пользователей.
    Возвращает словарь user_id -> [список сообщений].
    """
    logger.info(f"[PROMO] 🚀 Запуск массовой активации кода: {code}")
    all_users = load_all_users()
    results = {}
    history = load_promo_history()

    # Пропускаем уже активированные коды
    if any(entry.get("code") == code for entry in history):
        logger.warning(f"[PROMO] ⚠️ Код {code} уже есть в истории — повтор не выполняется.")
        return {"error": f"⚠️ Код {code} уже был активирован ранее."}

    # Внутренняя функция для одного пользователя
    async def handle_user(user_id: str, accounts: list):
        user_results = []
        for acc in accounts:
            uid = acc.get("uid")
            username = acc.get("username", "Игрок")
            if not uid:
                continue

            cookies_dict = load_cookies_for_account(user_id, uid)
            if not cookies_dict:
                user_results.append(f"⚠️ <b>{username}</b> ({uid}): Cookies не найдены.")
                continue

            async def handler(page):
                return await activate_promo_for_account(page, uid, username, code)

            result = await run_event_with_browser(user_id, uid, CDKEY_URL.format(uid=uid, code=code), f"Промокод {code}", handler)
            msg = result.get("message") if isinstance(result, dict) else str(result)
            user_results.append(msg)
            await asyncio.sleep(0.5)
        return user_results

    # 🚀 Перебираем всех пользователей
    for user_id, accounts in all_users.items():
        results[user_id] = await handle_user(user_id, accounts)
        await asyncio.sleep(1)

    # 💾 Сохраняем историю
    if any("Успешно" in " ".join(v) for v in results.values()):
        save_promo_history(history)

    history.append({
        "code": code,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "results": results
    })
    save_promo_history(history)

    logger.info(f"[PROMO] ✅ Код {code} активирован для {len(all_users)} пользователей.")
    return results
