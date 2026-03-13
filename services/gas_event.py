# tg_zov/services/gas_event.py
import json
import logging
import html
from services.browser_patches import run_event_with_browser
from services.accounts_manager import get_all_accounts
from services.castle_api import load_cookies_for_account

logger = logging.getLogger("gas_event")

BASE_URL = "https://event-cc.igg.com/event/gas/"
API_URL = f"{BASE_URL}ajax.req.php?action=battlepower"


async def run_gas_event(user_id: str, uid: str = None, context=None) -> dict:
    """
    🧩 Акция 'Маленькая помощь (gas)'
    Проверяет наличие кнопки 'Получено' и, если доступно, получает награду.
    """

    accounts = get_all_accounts(user_id)
    if not accounts:
        return {"success": False, "message": "⚠️ У пользователя нет аккаунтов."}

    acc = next((a for a in accounts if a.get("uid") == uid), accounts[0])
    uid = acc.get("uid")
    username = acc.get("username", "Игрок")

    cookies_dict = load_cookies_for_account(user_id, uid)
    if not cookies_dict:
        return {"success": False, "message": f"⚠️ Cookies не найдены ({username})."}

    async def handler(page):
        # 🧠 Проверяем статус события
        html_text = (await page.content()).lower()
        if any(x in html_text for x in ["событие еще не началось", "уже завершилось"]):
            return {
                "success": True,
                "message": f"⚠️ {username} ({uid}) — событие ещё не началось или завершилось."
            }

        # 🟢 Проверяем кнопку "Получено"
        try:
            disable_btns = await page.locator(".gifts-get-btn.disable a").all_inner_texts()
            if any("Получено" in t for t in disable_btns):
                return {
                    "success": True,
                    "message": f"🟢 {username} ({uid}) — награда уже была получена сегодня ✅"
                }
        except Exception:
            pass

        # 📡 Делаем запрос на получение награды
        logger.info(f"[GAS] 🚀 Отправляем запрос на получение награды для {username} ({uid})")
        try:
            resp = await page.evaluate(
                f"""
                async () => {{
                    const res = await fetch("{API_URL}", {{
                        method: "GET",
                        credentials: "include",
                        headers: {{
                            "X-Requested-With": "XMLHttpRequest"
                        }}
                    }});
                    return await res.text();
                }}
                """
            )
        except Exception as e:
            return {"success": False, "message": f"❌ Ошибка при отправке запроса: {e}"}

        # 📦 Обрабатываем ответ
        text = str(resp)
        if not text:
            return {"success": False, "message": f"⚠️ Пустой ответ от сервера ({username})."}

        try:
            data = json.loads(text)
        except Exception:
            data = None

        if data:
            msg = (
                data.get("msg")
                or data.get("message")
                or (data.get("data", {}).get("msg") if isinstance(data.get("data"), dict) else None)
                or str(data)
            )
            msg = msg.strip()

            # ✅ успешное получение
            if str(data.get("status")) in ["1", "true"] or str(data.get("code")) == "0":
                reward_text = msg.replace("Поздравляем!", "🎁 Поздравляем!").strip()
                reward_text = html.escape(reward_text)
                return {
                    "success": True,
                    "message": f"🎉 <b>{username}</b> ({uid})\n🏆 Награда: {reward_text}"
                }

            # 🟢 уже получена
            if any(word in msg.lower() for word in ["уже получ", "повтор", "already", "получена"]):
                return {
                    "success": True,
                    "message": f"🟢 <b>{username}</b> ({uid}) — награда уже получена ✅"
                }

            # ⚠️ событие не активно
            if "не началось" in msg.lower() or "завершилось" in msg.lower():
                return {
                    "success": False,
                    "message": f"⚠️ Событие ещё не началось или завершилось ({username})."
                }

            return {"success": False, "message": f"⚠️ {username}: {html.escape(msg)}"}

        # Если не JSON — пробуем по тексту
        if any(x in text for x in ["Поздравляем", "Success", "成功", "вы выиграли"]):
            snippet = html.escape(text.strip().replace("\n", " ")[:150])
            return {
                "success": True,
                "message": f"🎉 <b>{username}</b> ({uid})\n🏆 Награда: {snippet}"
            }

        if "событие еще не началось" in text or "уже завершилось" in text:
            return {
                "success": False,
                "message": f"⚠️ Событие ещё не началось или завершилось ({username})."
            }

        return {"success": False, "message": f"⚠️ Неизвестный ответ от сервера ({username})."}

    return await run_event_with_browser(user_id, uid, BASE_URL, "Маленькая помощь", handler, context=context)