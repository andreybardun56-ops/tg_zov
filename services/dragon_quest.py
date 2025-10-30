import json
import logging

from services.browser_patches import run_event_with_browser
from services.accounts_manager import get_all_accounts
from services.castle_api import load_cookies_for_account

BASE_URL = "https://event-eu-cc.igg.com/event/dragon_quest/"
ATTACK_URL = f"{BASE_URL}ajax.req.php?action=attack"

logger = logging.getLogger("dragon_quest")


def format_rewards(data: dict) -> str:
    """🎁 Показывает только поле `prizes`. Если пусто — пишет 'Награды нет'."""
    prizes = data.get("prizes")

    # Иногда prizes может быть вложено в data/prizes
    if not prizes and isinstance(data.get("data"), dict):
        prizes = data["data"].get("prizes")

    # 🟡 Если вообще нет поля или оно пустое
    if not prizes or prizes in ([], {}, "", None):
        return "\n🎁 Награды нет"

    lines = []
    if isinstance(prizes, list):
        for i, r in enumerate(prizes, 1):
            if isinstance(r, dict):
                name = (
                    r.get("name")
                    or r.get("item_name")
                    or r.get("title")
                    or r.get("desc")
                    or str(r)
                )
                cnt = r.get("count") or r.get("num") or 1
                lines.append(f"{i}. {name} ×{cnt}")
            else:
                s = str(r).strip()
                if s:
                    lines.append(f"{i}. {s}")

    elif isinstance(prizes, dict):
        name = (
            prizes.get("name")
            or prizes.get("item_name")
            or prizes.get("title")
            or prizes.get("desc")
            or str(prizes)
        )
        cnt = prizes.get("count") or prizes.get("num") or 1
        lines.append(f"1. {name} ×{cnt}")

    elif isinstance(prizes, str) and prizes.strip():
        lines.append(prizes.strip())

    # 🟢 Возвращаем текст
    if not lines:
        return "\n🎁 Награды нет"
    return "\n🎁 " + "\n🎁 ".join(lines)

async def run_dragon_quest(user_id: str, uid: str = None) -> dict:
    """
    ⚔️ Событие 'Рыцари Драконы'
    1️⃣ Проверяет активность
    2️⃣ Отправляет запрос attack
    3️⃣ Возвращает награду или причину неудачи
    """
    logger.info(f"[DRAGON_QUEST] ▶ Запуск для user_id={user_id}, uid={uid}")

    accounts = get_all_accounts(user_id)
    if not accounts:
        return {"success": False, "message": "⚠️ У пользователя нет аккаунтов."}

    acc = next((a for a in accounts if a.get("uid") == uid), accounts[0])
    uid = acc.get("uid")
    username = acc.get("username", "Игрок")

    cookies_dict = load_cookies_for_account(user_id, uid)
    if not cookies_dict:
        return {"success": False, "message": f"⚠️ Cookies не найдены ({username})."}
    logger.info(f"[DRAGON_QUEST] 🍪 Cookies загружены для {username} ({uid}) — {len(cookies_dict)} шт.")

    async def handler(page):
        # --- Проверка активности события ---
        html = (await page.content()).lower()
        if any(x in html for x in ["событие еще не началось", "уже завершилось"]):
            return {"success": True, "message": f"⚠️ {username} ({uid}) — событие ещё не началось или завершилось."}

        try:
            from services.event_checker import check_event_active
            active = await check_event_active("dragon_quest")
            if not active:
                logger.warning(f"[DRAGON_QUEST] ⚠️ Акция неактивна по данным event_checker.")
                return {"success": True, "message": f"⚠️ {username} ({uid}) — акция не активна."}
        except Exception as e:
            logger.warning(f"[DRAGON_QUEST] ⚠️ Ошибка при проверке активности: {e}")

        # --- Выполняем основной запрос ---
        logger.info(f"[DRAGON_QUEST] ⚔️ Отправляю запрос attack для {uid}")
        try:
            resp = await page.evaluate(
                f"""
                async () => {{
                    const res = await fetch("{ATTACK_URL}", {{
                        method: "GET",
                        credentials: "include",
                        headers: {{ "X-Requested-With": "XMLHttpRequest" }}
                    }});
                    return await res.text();
                }}
                """
            )
        except Exception as e:
            return {"success": False, "message": f"❌ Ошибка при запросе attack: {e}"}

        if not resp:
            return {"success": False, "message": f"⚠️ Пустой ответ attack ({username})."}

        # --- Разбор JSON-ответа ---
        try:
            data = json.loads(resp)
        except Exception:
            data = None

        # ✅ Успех
        if data and str(data.get("status")) == "1":
            msg = data.get("msg") or "Атака выполнена успешно!"
            rewards_text = format_rewards(data)
            logger.info(f"[DRAGON_QUEST] ✅ Успешная атака для {uid}")
            return {
                "success": True,
                "message": (
                    f"⚔️ <b>{username}</b> ({uid}) — акция <b>Рыцари Драконы</b>\n\n"
                    f"{msg}{rewards_text}\n\n✅ Проверка завершена!"
                )
            }

        # ⚠️ Закончились попытки
        if data == {"data": [], "error": 1, "status": 0}:
            logger.info(f"[DRAGON_QUEST] ⚔️ Для {uid} — попытки закончились.")
            return {
                "success": True,
                "message": f"⚔️ <b>{username}</b> ({uid}) — попытки в событии закончились."
            }

        # ❌ Неизвестный ответ
        snippet = str(resp).strip().replace("\n", " ")[:200]
        logger.warning(f"[DRAGON_QUEST] ⚠️ Неизвестный ответ от сервера: {snippet}")
        return {
            "success": False,
            "message": f"⚠️ <b>{username}</b> ({uid}) — неизвестный ответ:\n<code>{snippet}</code>"
        }

    return await run_event_with_browser(user_id, uid, BASE_URL, "Рыцари Драконы", handler)
