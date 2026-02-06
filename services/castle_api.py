"""castleclash MVP через HTTP (aiohttp) — исправленная версия."""

import asyncio
import re
import json
import os
from typing import Any
from playwright.async_api import TimeoutError as PlaywrightTimeout
from services.logger import logger
from services.browser_patches import (
    launch_masked_persistent_context,
    get_random_browser_profile,
)
from services.cookies_io import load_all_cookies, save_all_cookies
from config import COOKIES_FILE

# ───────────────────────────────────────────────
# 🧱 Работа с cookies.json
# ───────────────────────────────────────────────

def load_cookies_for_account(user_id: str, uid: str) -> dict:
    """Возвращает cookies конкретного аккаунта из cookies.json"""
    if not os.path.exists(COOKIES_FILE):
        logger.warning("[COOKIES] ⚠️ Файл cookies.json не найден")
        return {}
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get(str(user_id), {}).get(str(uid), {})
    except Exception as e:
        logger.error(f"[COOKIES] ❌ Ошибка загрузки cookies: {e}")
        return {}

# ───────────────────────────────────────────────
# 🔄 Обновление cookies через MVP (через browser_patches)
# ───────────────────────────────────────────────

async def refresh_cookies_mvp(user_id: str, uid: str) -> dict[str, Any]:
    """
    🔄 Обновляет cookies через MVP-ссылку, используя browser_patches.
    """
    from .accounts_manager import get_all_accounts

    logger.info(f"[COOKIES] 🌐 Обновляю cookies для UID={uid} (user_id={user_id})")

    accounts = get_all_accounts(str(user_id))
    acc = next((a for a in accounts if a.get("uid") == uid), None)
    if not acc or not acc.get("mvp_url"):
        return {"success": False, "error": "MVP ссылка не найдена. Добавь аккаунт заново."}

    mvp_url = acc["mvp_url"]
    cookies_result: dict[str, str] = {}
    ctx = None

    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            profile = get_random_browser_profile()
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir=f"data/chrome_profiles/{uid}",
                headless=True,
                slow_mo=30,
                profile=profile,
            )

            context = ctx["context"]
            page = ctx["page"]

            await page.goto(mvp_url, wait_until="domcontentloaded", timeout=60000)
            logger.info("[COOKIES] 🌍 Открыта страница MVP")

            # ✅ Кнопка "Accept all"
            try:
                try:
                    await page.click('div.i-cookie__btn[data-value="all"]', timeout=8000)
                    logger.info("[COOKIES] ✅ Нажата 'Accept all' (div.i-cookie__btn)")
                except PlaywrightTimeout:
                    await page.click("text=Accept all", timeout=3000)
                    logger.info("[COOKIES] ✅ Нажата 'Accept all' (по тексту)")
                await asyncio.sleep(1.5)
            except Exception:
                logger.info("[COOKIES] ⚠️ Кнопка 'Accept all' не найдена — возможно, баннера нет")

            # 📦 Сохраняем cookies
            cookies_list = await context.cookies()
            cookies_result = {c["name"]: c["value"] for c in cookies_list}

            all_data = load_all_cookies()
            all_data.setdefault(str(user_id), {})[str(uid)] = cookies_result
            save_all_cookies(all_data)

            logger.info(f"[COOKIES] 💾 Cookies обновлены для UID={uid}")
            return {"success": True, "cookies": cookies_result}

    except Exception as e:
        logger.exception(f"[COOKIES] ❌ Ошибка при обновлении cookies: {e}")
        return {"success": False, "error": str(e)}

    finally:
        try:
            if ctx:
                if "page" in ctx:
                    await ctx["page"].close()
                if "context" in ctx:
                    await ctx["context"].close()
        except Exception:
            pass

    return {"success": False, "error": "Ошибка: неизвестный результат обновления cookies"}

# ───────────────────────────────────────────────
# 🎁 Извлечение награды из ответа
# ───────────────────────────────────────────────

def extract_reward_from_response(text: str) -> str:
    """Пытается извлечь описание награды из JSON или HTML."""
    try:
        data = json.loads(text)
        for key in ["reward", "reward_name", "item_name", "name", "desc", "title", "msg"]:
            if key in data and isinstance(data[key], str):
                return data[key]
        if "data" in data and isinstance(data["data"], dict):
            for key in ["reward", "reward_name", "item_name", "name", "msg"]:
                if key in data["data"]:
                    return str(data["data"][key])
    except Exception:
        pass

    match = re.search(r'奖励[:： ]*([^"<>{}\n\r]+)', text)
    if match:
        return match.group(1).strip()

    match2 = re.search(r'"reward"\s*:\s*"([^"]+)"', text)
    if match2:
        return match2.group(1).strip()

    return None

# ───────────────────────────────────────────────
# 🌐 Извлечение IGG ID и имени со страницы MVP (через browser_patches)
# ───────────────────────────────────────────────

async def extract_player_info_from_page(url: str) -> dict:
    """
    🌐 Открывает MVP ссылку и извлекает IGG ID + имя игрока (через browser_patches).
    """
    logger.info(f"[MVP] 🌐 Открываю страницу для получения данных: {url}")
    result = {"uid": None, "username": None}

    from playwright.async_api import async_playwright
    try:
        async with async_playwright() as p:
            profile = get_random_browser_profile()
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir="data/chrome_profiles/_extract_tmp",
                headless=True,
                slow_mo=30,
                profile=profile,
            )
            context = ctx["context"]
            page = ctx["page"]

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            logger.info("[MVP] ⏳ Ожидание загрузки страницы...")

            try:
                await page.click('div.i-cookie__btn[data-value="all"]', timeout=5000)
                logger.info("[MVP] ✅ Кнопка 'Accept All' нажата")
                await page.wait_for_timeout(1500)
            except Exception:
                logger.info("[MVP] ⚠️ Баннер cookies не найден — пропускаем")

            await page.wait_for_selector(".user__infos-item", timeout=45000)
            blocks = await page.query_selector_all(".user__infos-item")

            for b in blocks:
                text = (await b.inner_text()).strip()
                if "IGG ID" in text:
                    match = re.search(r"\b\d{6,12}\b", text)
                    if match:
                        result["uid"] = match.group(0)
                elif "Имя игрока" in text:
                    match = re.search(r"Имя игрока[:：]?\s*(.+)", text)
                    if match:
                        result["username"] = match.group(1).strip()

            if result["uid"] and result["username"]:
                logger.info(f"[MVP] ✅ Найден IGG ID={result['uid']}, username={result['username']}")
                return {"success": True, **result}

            return {"success": False, "error": "Не удалось извлечь IGG ID или имя"}

    except Exception as e:
        logger.error(f"[MVP] ❌ Ошибка при открытии страницы: {e}")
        return {"success": False, "error": str(e)}

    finally:
        try:
            if "page" in locals():
                await page.close()
            if "context" in locals():
                await context.close()
        except Exception:
            pass
