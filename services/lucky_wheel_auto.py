import asyncio
import json
import logging
import random
from pathlib import Path
from typing import Optional, Callable
from playwright.async_api import async_playwright
from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    launch_masked_persistent_context,
    cookies_to_playwright,
)
from services.cookies_io import load_all_cookies, save_all_cookies

# === Настройки ===
FAIL_DIR = Path("data/fails/lucky_wheel")
FAIL_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://event-eu-cc.igg.com/event/lucky_wheel/"
API = "https://event-eu-cc.igg.com/event/lucky_wheel/ajax.req.php?action=lottery&times=1"

CONCURRENT = 3
DELAY_BETWEEN_ACCOUNTS = 3
REQUEST_TIMEOUT = 35000

logger = logging.getLogger("lucky_wheel_auto")


# ───────────────────────── helpers ─────────────────────────
def pick_all_accounts_from_cookies():
    """Возвращает список (user_id, uid, cookies_dict) всех аккаунтов из cookies.json"""
    cookies_db = load_all_cookies()
    if not cookies_db:
        logger.warning("[lucky_wheel] ⚠️ cookies.json пустой")
        return []

    accounts = []
    for user_id, accs in cookies_db.items():
        if isinstance(accs, dict):
            for uid, cookies in accs.items():
                if isinstance(cookies, dict) and cookies:
                    accounts.append((str(user_id), str(uid), cookies))
    return accounts


async def save_response(uid: str, data: dict):
    """Сохраняет JSON-ответ для отладки"""
    file_path = FAIL_DIR / f"{uid}_response.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"[{uid}] 💾 Ответ сохранён в {file_path.name}")


# ───────────────────────── core ─────────────────────────
async def process_account(p, user_id: str, uid: str, cookies: dict, send_callback: Optional[Callable] = None):
    context = page = None
    try:
        logger.info(f"[{uid}] 🎡 Начинаю вращение колеса фортуны")

        ctx = await launch_masked_persistent_context(
            p,
            user_data_dir=f"data/chrome_profiles/{uid}_wheel",
            browser_path=BROWSER_PATH,
            headless=True,
            slow_mo=30,
            profile=get_random_browser_profile(),
        )
        context, page = ctx["context"], ctx["page"]

        await context.add_cookies(cookies_to_playwright(cookies))
        await page.goto(URL, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
        await asyncio.sleep(random.uniform(1.5, 3.0))

        # 🌀 Отправляем запрос lottery
        js = f"""
            async () => {{
                const res = await fetch("{API}", {{
                    method: "GET",
                    credentials: "include"
                }});
                const text = await res.text();
                try {{
                    return JSON.parse(text);
                }} catch {{
                    return {{ raw: text }};
                }}
            }}
        """
        response = await page.evaluate(js)
        await save_response(uid, response)

        # 🧾 Парсим результат
        reward_text = None
        if isinstance(response, dict):
            data = response.get("data", {})
            err = response.get("error")
            status = response.get("status")

            # 🎯 Нет попыток
            if err == 10 or (status == 0 and data == []):
                reward_text = "🚫 Попытки вращения закончились."
            # 🎁 Есть награда
            elif isinstance(data, dict) and "rewards" in data:
                rewards = data.get("rewards", [])
                if rewards and isinstance(rewards[0], dict):
                    reward = rewards[0].get("ap_name") or rewards[0].get("ap_desc") or "Неизвестная награда"
                    reward_text = f"🎁 Получено: {reward}"
                else:
                    reward_text = f"⚠️ rewards пустые или некорректные: {rewards}"
            # ❓ Другие случаи
            else:
                reward_text = f"⚠️ Неизвестный ответ: {response}"
        else:
            reward_text = f"⚠️ Ответ не является словарём: {response}"

        # 📩 Отправляем сообщение в Telegram (если задан callback)
        if send_callback and reward_text:
            await send_callback(uid, reward_text)

        # 💾 Обновляем cookies
        fresh = await context.cookies()
        fresh_map = {c["name"]: c["value"] for c in fresh if "name" in c and "value" in c}
        if fresh_map:
            cookies_db = load_all_cookies()
            cookies_db.setdefault(str(user_id), {}).setdefault(str(uid), {}).update(fresh_map)
            save_all_cookies(cookies_db)
            logger.info(f"[{uid}] 🔄 Cookies обновлены")

    except Exception as e:
        msg = f"[{uid}] ❌ Ошибка: {e}"
        logger.error(msg)
        if send_callback:
            await send_callback(uid, msg)

    finally:
        try:
            if page:
                await page.close()
            if context:
                await context.close()
        except Exception:
            pass
        await asyncio.sleep(DELAY_BETWEEN_ACCOUNTS)


# ───────────────────────── main ─────────────────────────
async def run_lucky_wheel(send_callback: Optional[Callable] = None):
    """
    Главная функция автосбора.
    send_callback(uid, text) — функция для отправки сообщений в Telegram.
    """
    accounts = pick_all_accounts_from_cookies()
    if not accounts:
        logger.warning("⚠️ Нет аккаунтов для обработки (cookies.json пустой)")
        if send_callback:
            await send_callback("system", "⚠️ Нет аккаунтов для обработки (cookies.json пустой)")
        return

    logger.info(f"🎡 Найдено аккаунтов: {len(accounts)}")

    sem = asyncio.Semaphore(CONCURRENT)
    async with async_playwright() as p:
        async def worker(user_id, uid, cookies):
            async with sem:
                await process_account(p, user_id, uid, cookies, send_callback)

        tasks = [asyncio.create_task(worker(*acc)) for acc in accounts]
        await asyncio.gather(*tasks)

    logger.info("✅ Колесо фортуны завершено для всех аккаунтов.")
    if send_callback:
        await send_callback("system", "✅ Колесо фортуны завершено для всех аккаунтов.")
