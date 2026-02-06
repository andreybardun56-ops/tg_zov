# tg_zov/services/castle_machine.py
import json
import logging

from services.browser_patches import run_event_with_browser
from services.accounts_manager import get_all_accounts
from services.castle_api import load_cookies_for_account

BASE_URL = "https://event-cc.igg.com/event/castle_machine/"
MAKE_URL = f"{BASE_URL}ajax.req.php?action=make&type=free"
LOTTERY_URL = f"{BASE_URL}ajax.req.php?action=lottery"

logger = logging.getLogger("castle_machine")

from datetime import datetime
from pathlib import Path

RESULTS_FILE = Path("data/event_logs/castle_machine_results.json")
RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)


def save_result_record(entry: dict):
    """Сохраняет результат выполнения в общий лог JSON."""
    try:
        if RESULTS_FILE.exists():
            with open(RESULTS_FILE, "r", encoding="utf-8") as f:
                all_data = json.load(f)
        else:
            all_data = []

        all_data.append(entry)
        # ограничим лог 500 последними записями
        all_data = all_data[-500:]

        with open(RESULTS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"[CASTLE_MACHINE] ⚠️ Не удалось записать результат в лог: {e}")

def format_rewards(data: dict) -> str:
    """🧩 Форматирует список наград из JSON-ответа"""
    rewards = []
    for key in ("reward", "gift", "data", "list", "items", "item"):
        value = data.get(key)
        if isinstance(value, list):
            for i, r in enumerate(value, 1):
                if isinstance(r, dict):
                    name = r.get("name") or r.get("item_name") or r.get("title") or str(r)
                    cnt = r.get("count") or r.get("num") or 1
                    rewards.append(f"{i}. {name} ×{cnt}")
                else:
                    rewards.append(f"{i}. {r}")
        elif isinstance(value, dict):
            for k, v in value.items():
                rewards.append(f"{k}: {v}")
        elif isinstance(value, str):
            rewards.append(value)

    if not rewards:
        return ""
    return "\n🎁 " + "\n🎁 ".join(rewards)


async def run_castle_machine(user_id: str, uid: str = None) -> dict:
    logger.info(f"[CASTLE_MACHINE] ▶ Запуск для user_id={user_id}, uid={uid}")

    """
    ⚙️ Событие 'Создающая машина'
    1️⃣ Первая фаза (make&type=free)
    2️⃣ Вторая фаза (lottery)
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
    logger.info(f"[CASTLE_MACHINE] 🍪 Cookies загружены для {username} ({uid}) — {len(cookies_dict)} шт.")

    async def handler(page):
        html = (await page.content()).lower()
        if any(x in html for x in ["событие еще не началось", "уже завершилось"]):
            return {"success": True, "message": f"⚠️ {username} ({uid}) — событие ещё не началось или завершилось."}

        # === Определяем фазу по датам ===
        try:
            from services.event_checker import check_event_active
            phase = await check_event_active("castle_machine")
            if phase == 1:
                logger.info(f"[CASTLE_MACHINE] 🏗 Текущая фаза: 1 (Создание)")
            elif phase == 2:
                logger.info(f"[CASTLE_MACHINE] 🎁 Текущая фаза: 2 (Розыгрыш)")
            else:
                logger.warning(f"[CASTLE_MACHINE] ⚠️ Фаза не определена или акция не активна.")
                return {"success": True, "message": f"⚠️ {username} ({uid}) — акция не активна или вне даты."}
        except Exception as e:
            logger.warning(f"[CASTLE_MACHINE] ⚠️ Ошибка при определении фазы: {e}")
            phase = None

        # --- Получаем таймер ---
        formatted_time = None
        try:
            timer_div = await page.query_selector("#count-down")
            if timer_div:
                timer_text = (await timer_div.inner_text() or "").strip()
                if timer_text and ":" in timer_text:
                    formatted_time = timer_text
                else:
                    left_attr = await timer_div.get_attribute("left_time")
                    if left_attr and left_attr.isdigit():
                        sec = int(left_attr)
                        h, m, s = sec // 3600, (sec % 3600) // 60, sec % 60
                        formatted_time = f"{h:02}:{m:02}:{s:02}"
            if formatted_time:
                logger.info(f"[CASTLE_MACHINE] Таймер найден: {formatted_time}")
            else:
                logger.info(f"[CASTLE_MACHINE] Таймер не найден или пуст.")
        except Exception:
            pass

        # --- Этапы ---
        stage_text = "неизвестно"
        try:
            times = await page.query_selector_all("div.event-time-group .event-time")
            if times:
                stage_info = [await t.inner_text() for t in times]
                stage_text = " / ".join([s.strip() for s in stage_info if s.strip()])
        except Exception:
            pass

        # === Выбираем URL в зависимости от фазы ===
        if phase == 1:
            action_url = MAKE_URL
            action_name = "make&type=free"
        elif phase == 2:
            action_url = LOTTERY_URL
            action_name = "lottery"
        else:
            return {"success": True, "message": f"⚠️ {username} ({uid}) — акция вне активных фаз."}

        logger.info(f"[CASTLE_MACHINE] ▶ Отправляю запрос {action_name} для {uid}")

        # === Лог сетевых запросов ===
        page.on("request", lambda req: logger.info(f"🌍 REQUEST → {req.method} {req.url}"))
        page.on("response", lambda res: logger.info(f"📩 RESPONSE ← {res.status} {res.url}"))

        logger.info(f"[CASTLE_MACHINE] ▶ Отправляю запрос {action_name} для {uid}")

        # === Лог сетевых запросов ===
        page.on("request", lambda req: logger.info(f"🌍 REQUEST → {req.method} {req.url}"))
        page.on("response", lambda res: logger.info(f"📩 RESPONSE ← {res.status} {res.url}"))

        # === Выполняем fetch с полными заголовками и fallback через XHR ===
        try:
            resp = await page.evaluate(f"""
                async () => {{
                    // Основной запрос через fetch
                    let res;
                    try {{
                        const r = await fetch("{action_url}", {{
                            method: "GET",
                            credentials: "include",
                            headers: {{
                                "X-Requested-With": "XMLHttpRequest",
                                "Referer": "{BASE_URL}",
                                "Accept": "application/json, text/javascript, */*; q=0.01",
                                "User-Agent": navigator.userAgent
                            }}
                        }});
                        res = await r.text();
                    }} catch (e) {{
                        res = "fetch_error:" + e;
                    }}

                    // Если fetch вернул пусто — fallback на XHR
                    if (!res) {{
                        const xhrResp = await new Promise((resolve) => {{
                            const xhr = new XMLHttpRequest();
                            xhr.open("GET", "{action_url}", true);
                            xhr.withCredentials = true;
                            xhr.setRequestHeader("X-Requested-With", "XMLHttpRequest");
                            xhr.setRequestHeader("Referer", "{BASE_URL}");
                            xhr.onload = () => resolve(xhr.responseText);
                            xhr.onerror = () => resolve("XHR error");
                            xhr.send();
                        }});
                        res = xhrResp;
                    }}
                    return res;
                }}
            """)
        except Exception as e:
            return {"success": False, "message": f"❌ Ошибка при запросе {action_name}: {e}"}

        # Логируем первые 500 символов ответа (для диагностики)
        logger.debug(f"[CASTLE_MACHINE] Raw response: {resp[:500]!r}")

        if not resp:
            return {"success": False, "message": f"⚠️ Пустой ответ {action_name} ({username})."}

        # --- Обработка ответа ---
        try:
            data = json.loads(resp)
        except Exception:
            data = None

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # ✅ Успешный результат
        if data and str(data.get("status")) == "1":
            msg = data.get("msg") or "Награда успешно получена!"
            rewards_text = format_rewards(data)
            logger.info(f"[CASTLE_MACHINE] ✅ Получена награда {action_name} для {uid}")

            # 💾 Сохраняем результат
            save_result_record({
                "timestamp": timestamp,
                "user_id": user_id,
                "uid": uid,
                "username": username,
                "phase": phase,
                "action": action_name,
                "success": True,
                "raw": data,
            })

            return {
                "success": True,
                "message": (
                    f"✅ <b>{username}</b> ({uid}) — акция <b>Создающая машина</b>\n\n"
                    f"{msg}{rewards_text}\n\n📅 Этапы: {stage_text}"
                )
            }

        # ⚠️ Ошибка: пропущен первый сегмент
        if data and data.get("error") == -3000 and data.get("status") == 0:
            logger.warning(f"[CASTLE_MACHINE] ⚠️ {username} ({uid}) — пропущен первый сегмент события!")

            save_result_record({
                "timestamp": timestamp,
                "user_id": user_id,
                "uid": uid,
                "username": username,
                "phase": phase,
                "action": action_name,
                "success": False,
                "raw": data,
            })

            return {
                "success": True,
                "message": (
                    f"⚠️ <b>{username}</b> ({uid}) — вы пропустили первый сегмент события 🕒\n\n"
                    f"Теперь можно участвовать только во второй фазе (розыгрыше призов 🎁).\n"
                    f"📅 Этапы: {stage_text}"
                )
            }

        # ❓ Неизвестный ответ
        snippet = str(resp).strip().replace("\n", " ")[:200]
        logger.warning(f"[CASTLE_MACHINE] ⚠️ Неизвестный ответ от сервера: {snippet}")

        save_result_record({
            "timestamp": timestamp,
            "user_id": user_id,
            "uid": uid,
            "username": username,
            "phase": phase,
            "action": action_name,
            "success": False,
            "raw": snippet,
        })

        return {
            "success": False,
            "message": f"⚠️ <b>{username}</b> ({uid}) — неизвестный ответ:\n<code>{snippet}</code>"
        }

    return await run_event_with_browser(user_id, uid, BASE_URL, "Создающая машина", handler)
