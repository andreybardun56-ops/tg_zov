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

ERROR_MAP = {
    0: "Неизвестная ошибка.",
    1: "Код уже использован.",
    2: "Лимит активаций исчерпан.",
    3: "Недействительный или просроченный код.",
    4: "Код недоступен в вашем регионе.",
    5: "Ошибка авторизации. Требуется вход.",
    6: "Код предназначен для другой платформы."
}

# ----------------------------- 🧩 Активация одного промокода -----------------------------
async def activate_promo_for_account(page, uid: str, username: str, code: str) -> str:
    url = CDKEY_URL.format(uid=uid, code=code)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)

        raw = await page.content()
        lower = raw.lower()

        # --- Попытка извлечь JSON внутри страницы ---
        try:
            # extract {} JSON substring
            import re
            match = re.search(r"\{.*\}", raw, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
                err = int(data.get("error", -1))
                st = int(data.get("status", -1))

                if st == 1:
                    return f"✅ <b>{username}</b> ({uid}): Успешно активирован!"

                # ошибка
                message = ERROR_MAP.get(err, "Неизвестная ошибка.")

                return f"❌ <b>{username}</b> ({uid}): {message}"
        except Exception:
            pass

        # --- fallback, если JSON нет ---
        if "success" in lower or "успеш" in lower:
            return f"✅ <b>{username}</b> ({uid}): Успешно активирован!"
        if "already" in lower or "использ" in lower:
            return f"⚠️ <b>{username}</b> ({uid}): Код уже использован."
        if "invalid" in lower or "ошибка" in lower:
            return f"❌ <b>{username}</b> ({uid}): Неверный или недействительный код."

        snippet = raw.strip().replace("\n", " ")[:150]
        return f"⚠️ <b>{username}</b> ({uid}): Неизвестный ответ сервера — <code>{snippet}</code>"

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

    # Пропускаем уже активированные коды (поддержка старого формата)
    for entry in history:
        if isinstance(entry, dict):
            # новый формат
            if entry.get("code") == code:
                logger.warning(f"[PROMO] ⚠️ Код {code} уже есть в истории — повтор не выполняется.")
                return {"error": f"⚠️ Код {code} уже был активирован ранее."}
        else:
            # старый формат — просто строка
            if entry == code:
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
                text = await activate_promo_for_account(page, uid, username, code)
                # Превращаем в dict
                return {
                    "success": "Успешно" in text or "success" in text.lower(),
                    "message": text
                }

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
    # перед сохранением — конвертируем старые строки в словари
    normalized_history = []
    for entry in history:
        if isinstance(entry, dict):
            normalized_history.append(entry)
        else:
            normalized_history.append({
                "code": entry,
                "timestamp": "unknown",
                "results": {}
            })
    history = normalized_history

    logger.info(f"[PROMO] ✅ Код {code} активирован для {len(all_users)} пользователей.")
    return results
