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

FAIL_DIR = Path("data/fails/magic_wheel")
FAIL_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://event-eu-cc.igg.com/event/double_turntable/"
API = "https://event-eu-cc.igg.com/event/double_turntable/ajax.req.php?action=lottery&times=1"

CONCURRENT = 3
DELAY_BETWEEN_ACCOUNTS = 3
REQUEST_TIMEOUT = 35000

logger = logging.getLogger("magic_wheel_auto")


def pick_accounts_from_cookies(user_id: Optional[str] = None):
    cookies_db = load_all_cookies()
    if not cookies_db:
        logger.warning("[magic_wheel] ⚠️ cookies.json пустой")
        return []

    accounts = []
    if user_id is not None:
        user_block = cookies_db.get(str(user_id), {})
        if not isinstance(user_block, dict):
            return []
        for uid, cookies in user_block.items():
            if isinstance(cookies, dict) and cookies:
                accounts.append((str(user_id), str(uid), cookies))
        return accounts

    for owner_id, accs in cookies_db.items():
        if isinstance(accs, dict):
            for uid, cookies in accs.items():
                if isinstance(cookies, dict) and cookies:
                    accounts.append((str(owner_id), str(uid), cookies))
    return accounts


async def save_response(uid: str, data: dict):
    file_path = FAIL_DIR / f"{uid}_response.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"[{uid}] 💾 Ответ сохранён в {file_path.name}")


def _parse_reward_text(response: dict) -> str:
    data = response.get("data", {})
    err = response.get("error")
    status = response.get("status")

    if err == 10 or (status == 0 and data == []):
        return "🚫 Попытки закончились."
    if isinstance(data, dict) and "rewards" in data:
        rewards = data.get("rewards", [])
        if rewards and isinstance(rewards[0], dict):
            reward = rewards[0].get("ap_name") or rewards[0].get("ap_desc") or "Неизвестная награда"
            return f"🎁 Получено: {reward}"
        return f"⚠️ rewards пустые или некорректные: {rewards}"
    return f"⚠️ Неизвестный ответ: {response}"


async def process_account(p, user_id: str, uid: str, cookies: dict, send_callback: Optional[Callable] = None):
    context = page = None
    try:
        logger.info(f"[{uid}] 🎡 Запуск 'Магического колеса'")
        ctx = await launch_masked_persistent_context(
            p,
            user_data_dir=f"data/chrome_profiles/{uid}_magic_wheel",
            browser_path=BROWSER_PATH,
            headless=True,
            slow_mo=30,
            profile=get_random_browser_profile(),
        )
        context, page = ctx["context"], ctx["page"]

        await context.add_cookies(cookies_to_playwright(cookies))
        await page.goto(URL, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT)
        await asyncio.sleep(random.uniform(1.5, 3.0))

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
        await save_response(uid, response if isinstance(response, dict) else {"raw": str(response)})

        if send_callback:
            if isinstance(response, dict):
                await send_callback(uid, _parse_reward_text(response))
            else:
                await send_callback(uid, f"⚠️ Ответ не является словарём: {response}")

        fresh = await context.cookies()
        fresh_map = {c["name"]: c["value"] for c in fresh if "name" in c and "value" in c}
        if fresh_map:
            cookies_db = load_all_cookies()
            cookies_db.setdefault(str(user_id), {}).setdefault(str(uid), {}).update(fresh_map)
            save_all_cookies(cookies_db)
            logger.info(f"[{uid}] 🔄 Cookies обновлены")

    except Exception as e:
        logger.exception(f"[{uid}] ❌ Ошибка в magic_wheel: {e}")
        if send_callback:
            await send_callback(uid, f"❌ Ошибка: {e}")
    finally:
        try:
            if page:
                await page.close()
            if context:
                await context.close()
        except Exception:
            pass
        await asyncio.sleep(DELAY_BETWEEN_ACCOUNTS)


async def run_magic_wheel(
    user_id: Optional[str] = None,
    send_callback: Optional[Callable] = None,
):
    """
    Запуск акции 'Магическое колесо':
    - если передан user_id -> запуск только по аккаунтам этого пользователя;
    - если user_id не передан -> запуск по всем аккаунтам (админский режим).
    """
    accounts = pick_accounts_from_cookies(user_id=user_id)
    if not accounts:
        msg = "⚠️ Нет аккаунтов с cookies для обработки."
        logger.warning(msg)
        if send_callback:
            await send_callback("system", msg)
        return {"success": False, "message": msg}

    sem = asyncio.Semaphore(CONCURRENT)
    async with async_playwright() as p:
        async def worker(owner_id: str, uid: str, cookies: dict):
            async with sem:
                await process_account(p, owner_id, uid, cookies, send_callback)

        tasks = [asyncio.create_task(worker(*acc)) for acc in accounts]
        await asyncio.gather(*tasks)

    done_msg = "✅ Магическое колесо завершено."
    if send_callback:
        await send_callback("system", done_msg)
    return {"success": True, "message": done_msg}
