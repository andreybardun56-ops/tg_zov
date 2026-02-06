# tg_zov/services/puzzle_exchange_auto.py
import os
import json
import asyncio
import random
from pathlib import Path
from typing import Dict, Any, Optional
from html import escape
from playwright.async_api import async_playwright, Playwright, BrowserContext, Page

from services.logger import logger
from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    cookies_to_playwright,
    launch_masked_persistent_context,
    humanize_pre_action,
)

LOG = logger
FAIL_DIR = Path("data/fails")
FAIL_DIR.mkdir(parents=True, exist_ok=True)

EVENT_PAGE = "https://event-eu-cc.igg.com/event/puzzle2/"
EVENT_API = f"{EVENT_PAGE}ajax.req.php"
PROFILE_DIR = Path("data/chrome_profiles")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- SESSION STORAGE ----------------
# user_id -> {"page": Page, "context": BrowserContext, "playwright": Playwright, "timer": Task}
active_sessions: Dict[str, Dict[str, Any]] = {}

# ---------------- HELPERS ----------------
def parse_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        return None

async def keep_browser_open(user_id: str, timeout: int = 60):
    await asyncio.sleep(timeout)
    session = active_sessions.get(user_id)
    if session:
        page = session.get("page")
        context = session.get("context")
        p: Playwright = session.get("playwright")
        if page:
            try: await page.close()
            except Exception as e: LOG.warning(f"[keep_browser_open] Ошибка при закрытии page: {e}")
        if context:
            try: await context.close()
            except Exception as e: LOG.warning(f"[keep_browser_open] Ошибка при закрытии context: {e}")
        if p:
            try: await p.stop()
            except Exception as e: LOG.warning(f"[keep_browser_open] Ошибка при остановке playwright: {e}")
        if session.get("timer"): session["timer"].cancel()
        del active_sessions[user_id]
        LOG.info(f"[keep_browser_open] Сессия для user {user_id} закрыта по таймауту")

# ---------------- HANDLERS ----------------
async def handle_get_fragment_count(page: Page):
    js = f"""
    async () => {{
        const res = await fetch("{EVENT_API}?action=get_resource", {{
            method: 'POST',
            credentials: 'include',
            headers: {{
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': '{EVENT_PAGE}',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
            }}
        }});
        return {{status: res.status, text: await res.text()}};
    }}
    """
    resp = await page.evaluate(js)
    text = resp.get("text", "")
    (FAIL_DIR / "get_resource_raw.txt").write_text(text, encoding="utf-8")
    data = parse_json(text)
    if not data:
        return {"success": False, "message": "Ответ не JSON"}
    puzzle_left = (
        data.get("puzzle_left")
        or data.get("data", {}).get("puzzle_left")
        or data.get("data", {}).get("user", {}).get("puzzle_left")
    )
    if puzzle_left is None:
        return {"success": False, "message": "Не найден puzzle_left"}
    return {"success": True, "puzzle_left": puzzle_left}

async def handle_exchange_item(page: Page, item_id: str):
    clean_id = str(item_id).split(":")[-1].strip()
    exchange_url = f"{EVENT_API}?action=exchange&id={clean_id}"
    await humanize_pre_action(page)
    await asyncio.sleep(random.uniform(0.5, 1.0))
    js = f"""
    async () => {{
        const res = await fetch("{exchange_url}", {{
            method: 'POST',
            credentials: 'include',
            headers: {{
                'X-Requested-With': 'XMLHttpRequest',
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Referer': '{EVENT_PAGE}',
                'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
            }}
        }});
        const txt = await res.text();
        return {{status: res.status, text: txt}};
    }}
    """
    resp = await page.evaluate(js)
    text = resp.get("text", "")
    (FAIL_DIR / f"exchange_{clean_id}_raw.txt").write_text(text, encoding="utf-8")
    parsed = parse_json(text)
    if not parsed: return {"success": False, "message": "Ответ не JSON"}
    if parsed.get("status") == 1:
        return {"success": True, "message": parsed.get("msg", "Обмен успешен")}
    return {"success": False, "message": parsed.get("msg", "Обмен не выполнен")}

# ---------------- PUBLIC API ----------------
async def start_session(user_id: str, iggid: str, cookies: list):
    LOG.info(f"[start_session] user_id={user_id}, iggid={iggid}")

    if user_id in active_sessions:
        LOG.info(f"[start_session] Используем существующую сессию для {user_id}")
        return active_sessions[user_id]

    # ---- Запуск Playwright без async with ----
    p: Playwright = await async_playwright().start()
    ctx_info = await launch_masked_persistent_context(
        p,
        user_data_dir=str(PROFILE_DIR / iggid),
        browser_path=BROWSER_PATH,
        headless=True,
        slow_mo=50,
        profile=get_random_browser_profile()
    )
    context: BrowserContext = ctx_info["context"]
    page: Page = ctx_info["page"]

    await context.add_cookies(cookies_to_playwright(cookies))
    await page.goto(EVENT_PAGE, wait_until="domcontentloaded", timeout=30000)
    await asyncio.sleep(1.5)
    await humanize_pre_action(page)

    timer_task = asyncio.create_task(keep_browser_open(user_id, 60))
    active_sessions[user_id] = {
        "page": page,
        "context": context,
        "playwright": p,  # сохраняем Playwright, чтобы не закрывался
        "timer": timer_task
    }

    LOG.info(f"[start_session] Сессия успешно создана для {user_id}")
    return active_sessions[user_id]

async def get_fragments(user_id: str):
    session = active_sessions.get(user_id)
    if not session: return {"success": False, "message": "Нет активной сессии"}
    return await handle_get_fragment_count(session["page"])

async def exchange(user_id: str, item_id: str, times: int):
    session = active_sessions.get(user_id)
    if not session: return {"success": False, "message": "Нет активной сессии"}
    page: Page = session["page"]
    results = []
    for _ in range(times):
        results.append(await handle_exchange_item(page, item_id))
        await asyncio.sleep(random.uniform(0.5, 1.0))
    return results

async def close_session(user_id: str):
    session = active_sessions.get(user_id)
    if not session: return
    page: Page = session.get("page")
    context: BrowserContext = session.get("context")
    p: Playwright = session.get("playwright")

    if page:
        try: await page.close()
        except Exception as e: LOG.warning(f"[close_session] Ошибка при закрытии page: {e}")
    if context:
        try: await context.close()
        except Exception as e: LOG.warning(f"[close_session] Ошибка при закрытии context: {e}")
    if p:
        try: await p.stop()
        except Exception as e: LOG.warning(f"[close_session] Ошибка при остановке playwright: {e}")
    if session.get("timer"): session["timer"].cancel()
    del active_sessions[user_id]
    LOG.info(f"[close_session] Сессия для user {user_id} закрыта вручную")
