"""castleclash MVP через HTTP (aiohttp) — исправленная версия."""

import asyncio
import base64
import importlib
import importlib.util
import json
import os
import re
import time
from datetime import datetime
from typing import Any, TypedDict
from playwright.async_api import BrowserContext, Page, Playwright, TimeoutError as PlaywrightTimeout
from services.logger import logger
from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    humanize_pre_action,
    launch_masked_persistent_context,
)
from services.cookies_io import load_all_cookies, save_all_cookies
from config import COOKIES_FILE

# ───────────────────────────────────────────────
# 🧱 Работа с cookies.json
# ───────────────────────────────────────────────
SLOW_MO = 50


class ShopContext(TypedDict):
    context: BrowserContext
    page: Page


class PlayerInfoResult(TypedDict):
    success: bool
    error: str | None
    uid: str | None
    username: str | None


def _get_stealth_callable():
    spec = importlib.util.find_spec("playwright_stealth")
    if spec is None:
        return None
    module = importlib.import_module("playwright_stealth")
    return getattr(module, "stealth_async", None) or getattr(module, "stealth", None)

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


def load_first_account_cookies(exclude: set[str] | None = None) -> dict:
    if exclude is None:
        exclude = set()
    if not os.path.exists(COOKIES_FILE):
        logger.warning("[COOKIES] ⚠️ Файл cookies.json не найден")
        return {}
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict) or not data:
            return {}
        first_user = next(iter(data.values()))
        if not isinstance(first_user, dict) or not first_user:
            return {}
        first_uid = next(iter(first_user.values()))
        if not isinstance(first_uid, dict):
            return {}
        return {k: v for k, v in first_uid.items() if k not in exclude and v}
    except Exception as e:
        logger.error(f"[COOKIES] ❌ Ошибка загрузки cookies первого аккаунта: {e}")
        return {}


def jwt_get_uid(token: str) -> str | None:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        obj = json.loads(decoded.decode("utf-8"))
        for key in ("sub", "uid", "userId", "user_id", "id", "jti"):
            if key in obj and obj[key]:
                return str(obj[key])
    except Exception as exc:
        logger.debug("[SHOP] JWT decode failed: %s", exc)
        return None
    return None


async def _accept_cookies(page: Page) -> None:
    selectors = [
        "#onetrust-accept-btn-handler",
        "div.i-cookie__btn[data-value=\"all\"]",
        "text=Accept all",
        "text=Accept All",
        "text=Принять все",
    ]
    for selector in selectors:
        locator = page.locator(selector)
        try:
            if await locator.count() == 0:
                continue
            await locator.first.click(timeout=5000)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except PlaywrightTimeout:
                pass
            return
        except PlaywrightTimeout as exc:
            logger.debug("[SHOP] Cookies banner click timeout (%s): %s", selector, exc)
        except Exception as exc:
            logger.debug("[SHOP] Cookies banner click failed (%s): %s", selector, exc)


async def wait_shop_ready(page: Page, timeout: int = 60000) -> None:
    deadline = time.monotonic() + (timeout / 1000)
    def _remaining_ms() -> int:
        return int(max(0, (deadline - time.monotonic()) * 1000))

    try:
        remaining_ms = min(15000, _remaining_ms())
        if remaining_ms > 0:
            await page.wait_for_load_state("domcontentloaded", timeout=remaining_ms)
    except PlaywrightTimeout:
        pass
    except Exception as exc:
        logger.debug("[SHOP] domcontentloaded wait failed: %s", exc)

    try:
        remaining_ms = min(15000, _remaining_ms())
        if remaining_ms > 0:
            await page.wait_for_load_state("networkidle", timeout=remaining_ms)
    except PlaywrightTimeout:
        pass
    except Exception as exc:
        logger.debug("[SHOP] networkidle wait failed: %s", exc)

    url = (page.url or "").lower()
    selectors = (
        [
            ".user__infos-item",
            ".user__infos",
        ]
        if "mvp" in url
        else [
            "div.btn-login.login__btn.before-login:has-text('Авторизация')",
            "div.userbar .btn-login.login__btn.before-login",
            ".main .userbar .btn-login.login__btn.before-login",
            ".passport--modal",
            ".userbar",
        ]
    )

    for selector in selectors:
        remaining_ms = _remaining_ms()
        if remaining_ms <= 0:
            logger.warning("[SHOP] Page readiness timeout exceeded.")
            raise PlaywrightTimeout("Shop readiness timeout exceeded.")
        try:
            await page.wait_for_selector(selector, state="visible", timeout=remaining_ms)
            return
        except PlaywrightTimeout:
            continue
        except Exception as exc:
            logger.debug("[SHOP] Wait selector failed (%s): %s", selector, exc)

    logger.warning("[SHOP] No readiness selector appeared within timeout.")
    raise PlaywrightTimeout("Shop readiness selector not found within timeout.")


async def _open_login_modal(page: Page) -> bool:
    selectors = [
        "div.btn-login.login__btn.before-login:has-text('Авторизация')",
        "div.userbar .btn-login.login__btn.before-login",
        ".main .userbar .btn-login.login__btn.before-login",
        "text=Авторизация",
    ]
    for selector in selectors:
        try:
            await page.wait_for_selector(selector, state="visible", timeout=8000)
            btn = page.locator(selector).first
            await btn.scroll_into_view_if_needed()
            await btn.click(timeout=5000)
            return True
        except PlaywrightTimeout as exc:
            logger.debug("[SHOP] Login modal button timeout (%s): %s", selector, exc)
        except Exception as exc:
            logger.debug("[SHOP] Login modal button failed (%s): %s", selector, exc)

    return False


async def _select_login_tab(page: Page, mode: str) -> None:
    if mode == "email":
        selectors = [
            "a.email.passport--on:has-text('E-mail адрес')",
            "a.email:has-text('E-mail адрес')",
            "a:has-text('E-mail адрес')",
        ]
    else:
        selectors = [
            "a.email.passport--on:has-text('IGG ID')",
            "a.email:has-text('IGG ID')",
            "a:has-text('IGG ID')",
        ]
    for selector in selectors:
        try:
            el = page.locator(selector)
            if await el.count() == 0:
                continue
            await el.first.click(timeout=3000)
            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except PlaywrightTimeout:
                pass
            return
        except Exception as exc:
            logger.debug("[SHOP] Login tab switch failed (%s): %s", selector, exc)


async def _is_access_denied(page: Page) -> bool:
    try:
        if await page.locator("text=Access Denied").count() > 0:
            return True
        if await page.locator("text=You don't have permission to access").count() > 0:
            return True
    except Exception as exc:
        logger.debug("[SHOP] Access denied check failed: %s", exc)
        return False
    return False


async def _fill_first_input(page: Page, selectors: list[str], value: str) -> bool:
    for selector in selectors:
        try:
            el = page.locator(selector)
            if await el.count() > 0:
                await el.first.fill(value, timeout=4000)
                return True
        except Exception as exc:
            logger.debug("[SHOP] Failed to fill input (%s): %s", selector, exc)
    return False


async def _capture_login_error_screenshot(page: Page | None, tag: str) -> str | None:
    if not page:
        return None
    try:
        screenshots_dir = os.path.join("logs", "screenshots", f"{datetime.now():%Y-%m-%d}")
        os.makedirs(screenshots_dir, exist_ok=True)
        safe_tag = re.sub(r"[^a-zA-Z0-9_-]+", "_", tag).strip("_")[:40] or "error"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = os.path.join(
            screenshots_dir,
            f"passport_login_{safe_tag}_{ts}.png",
        )
        await page.screenshot(path=screenshot_path)
        logger.info(f"[SHOP] 📸 Скриншот ошибки: {screenshot_path}")
        return screenshot_path
    except Exception as se:
        logger.warning(f"[SHOP] ⚠️ Не удалось сделать скриншот ошибки: {se}")
        return None


async def login_shop_email(email: str, password: str) -> dict[str, Any]:
    """
    Авторизация на https://castleclash.igg.com/shop/ через email+пароль.
    Возвращает cookies и uid (если найден).
    """
    ctx: ShopContext | None = None
    page: Page | None = None
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            profile = get_random_browser_profile()
            stealth_callable = _get_stealth_callable()
            logger.info("[SHOP] ▶ Запуск браузера для входа по email")
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir="data/chrome_profiles/_shop_email",
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=SLOW_MO,
                profile=profile,
                stealth_callable=stealth_callable,
            )
            context = ctx["context"]
            page = ctx["page"]
            try:
                await context.clear_cookies()
            except Exception as exc:
                logger.debug("[SHOP] Failed to clear cookies: %s", exc)

            logger.info("[SHOP] 🌍 Открываем страницу магазина")
            await page.goto("https://castleclash.igg.com/shop/", timeout=60000)
            await _accept_cookies(page)
            await wait_shop_ready(page)
            if await _is_access_denied(page):
                await _capture_login_error_screenshot(page, "access_denied")
                return {
                    "success": False,
                    "error": "Access Denied при открытии страницы (возможна блокировка по IP).",
                    "uid": None,
                    "cookies": None,
                    "username": None,
                }
            await humanize_pre_action(page)

            if not await _open_login_modal(page):
                await _capture_login_error_screenshot(page, "open_login_modal")
                return {
                    "success": False,
                    "error": "Не удалось открыть окно авторизации.",
                    "uid": None,
                    "cookies": None,
                    "username": None,
                }

            await _accept_cookies(page)
            await _select_login_tab(page, "email")

            logger.info("[SHOP] ✉️ Вводим email")
            filled_email = await _fill_first_input(
                page,
                [
                    'input[type="email"]',
                    'input.passport--email-ipt',
                    '.passport--email-item input.passport--email-ipt',
                    '.passport--email-item input.passport--form-ipt',
                    'input[placeholder*="E-mail"]',
                    'input[placeholder*="Email"]',
                    'input[placeholder*="Почта"]',
                    'input[placeholder*="имя пользователя"]',
                    'input.passport--form-ipt',
                ],
                email,
            )
            if not filled_email:
                await _capture_login_error_screenshot(page, "email_not_found")
                return {
                    "success": False,
                    "error": "Не найдено поле для email.",
                    "uid": None,
                    "cookies": None,
                    "username": None,
                }

            logger.info("[SHOP] 🔒 Вводим пароль")
            filled_pass = await _fill_first_input(
                page,
                [
                    'input[type="password"]',
                    'input.passport--password-ipt',
                    '.passport--email-item input.passport--password-ipt',
                    '.passport--email-item input[type="password"]',
                    'input[placeholder*="текущий пароль"]',
                    'input[placeholder*="Пароль"]',
                    'input[placeholder*="Password"]',
                ],
                password,
            )
            if not filled_pass:
                await _capture_login_error_screenshot(page, "password_not_found")
                return {
                    "success": False,
                    "error": "Не найдено поле для пароля.",
                    "uid": None,
                    "cookies": None,
                    "username": None,
                }

            logger.info("[SHOP] ✅ Нажимаем кнопку входа")
            login_btn = page.locator(
                ".passport--form-ipt-btns a.passport--passport-common-btn.passport--yellow"
            )
            if await login_btn.count() == 0:
                login_btn = page.locator(
                    "a.passport--passport-common-btn.passport--yellow:has-text('Вход')"
                )
            if await login_btn.count() > 0:
                await login_btn.first.click(timeout=5000)
            else:
                await page.keyboard.press("Enter")

            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except PlaywrightTimeout:
                pass

            logger.info("[SHOP] 🔎 Проверяем cookies после входа")
            cookies_list = await context.cookies()
            cookies_result = {c["name"]: c["value"] for c in cookies_list}
            token = cookies_result.get("gpc_sso_token")
            uid = jwt_get_uid(token) if token else None
            if not uid:
                await _capture_login_error_screenshot(page, "uid_not_found")
                return {
                    "success": False,
                    "error": "Не удалось получить IGG ID после входа.",
                    "uid": None,
                    "cookies": cookies_result,
                    "username": None,
                }

            logger.info("[SHOP] ✅ Вход успешен, UID=%s", uid)
            return {
                "success": True,
                "error": None,
                "uid": uid,
                "cookies": cookies_result,
                "username": "Игрок",
            }
    except Exception as e:
        await _capture_login_error_screenshot(page, "exception")
        logger.exception(f"[SHOP] ❌ Ошибка при входе по email: {e}")
        return {
            "success": False,
            "error": str(e),
            "uid": None,
            "cookies": None,
            "username": None,
        }
    finally:
        try:
            if ctx:
                if "page" in ctx:
                    await ctx["page"].close()
                if "context" in ctx:
                    await ctx["context"].close()
        except Exception as exc:
            logger.debug("[SHOP] Cleanup failed after email login: %s", exc)


async def start_shop_login_igg(igg_id: str) -> dict[str, Any]:
    """
    Запускает авторизацию по IGG ID: открывает окно и нажимает «Получить код».
    Возвращает context/page для продолжения.
    """
    ctx: ShopContext | None = None
    playwright: Playwright | None = None
    async def _cleanup() -> None:
        try:
            if ctx:
                await ctx["page"].close()
                await ctx["context"].close()
            if playwright:
                await playwright.stop()
        except Exception as exc:
            logger.debug("[SHOP] Cleanup failed after IGG login: %s", exc)
    try:
        from playwright.async_api import async_playwright
        playwright = await async_playwright().start()
        profile = get_random_browser_profile()
        stealth_callable = _get_stealth_callable()
        logger.info("[SHOP] ▶ Запуск браузера для входа по IGG ID")
        ctx = await launch_masked_persistent_context(
            playwright,
            user_data_dir=f"data/chrome_profiles/_shop_igg_{igg_id}",
            browser_path=BROWSER_PATH,
            headless=True,
            slow_mo=SLOW_MO,
            profile=profile,
            stealth_callable=stealth_callable,
        )
        context = ctx["context"]
        page = ctx["page"]

        logger.info("[SHOP] 🌍 Открываем страницу магазина (IGG ID)")
        await page.goto("https://castleclash.igg.com/shop/", timeout=60000)
        await _accept_cookies(page)
        await wait_shop_ready(page)
        if await _is_access_denied(page):
            await _capture_login_error_screenshot(page, "access_denied")
            await _cleanup()
            return {
                "success": False,
                "error": "Access Denied при открытии страницы (возможна блокировка по IP).",
                "context": None,
                "page": None,
                "playwright": None,
                "igg_id": igg_id,
                "owns_playwright": False,
            }
        await humanize_pre_action(page)

        if not await _open_login_modal(page):
            await _cleanup()
            return {
                "success": False,
                "error": "Не удалось открыть окно авторизации.",
                "context": None,
                "page": None,
                "playwright": None,
                "igg_id": igg_id,
                "owns_playwright": False,
            }

        await _select_login_tab(page, "igg")

        logger.info("[SHOP] 🆔 Вводим IGG ID")
        filled = await _fill_first_input(
            page,
            [
                'input[placeholder*="IGG"]',
                'input.passport--form-ipt',
                'input[type="text"]',
            ],
            igg_id,
        )
        if not filled:
            await _cleanup()
            return {
                "success": False,
                "error": "Не найдено поле для IGG ID.",
                "context": None,
                "page": None,
                "playwright": None,
                "igg_id": igg_id,
                "owns_playwright": False,
            }

        logger.info("[SHOP] 📩 Нажимаем «Получить код»")
        code_btn = page.locator("button.passport--sub-btn:has-text('Получить код')")
        if await code_btn.count() > 0:
            await code_btn.first.click(timeout=5000)
        else:
            await _cleanup()
            return {
                "success": False,
                "error": "Не удалось нажать «Получить код».",
                "context": None,
                "page": None,
                "playwright": None,
                "igg_id": igg_id,
                "owns_playwright": False,
            }

        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except PlaywrightTimeout:
            pass

        await page.wait_for_selector(
            'input.passport--password-ipt, input[placeholder*="Код"]',
            timeout=15000,
        )

        return {
            "success": True,
            "error": None,
            "context": context,
            "page": page,
            "playwright": playwright,
            "igg_id": igg_id,
            "owns_playwright": True,
        }
    except Exception as e:
        logger.exception(f"[SHOP] ❌ Ошибка при входе по IGG ID: {e}")
        await _cleanup()
        return {
            "success": False,
            "error": str(e),
            "context": None,
            "page": None,
            "playwright": None,
            "igg_id": igg_id,
            "owns_playwright": False,
        }


async def complete_shop_login_igg(
    context: BrowserContext,
    page: Page,
    code: str,
    playwright: Playwright | None = None,
) -> dict[str, Any]:
    """
    Завершает авторизацию по IGG ID кодом.
    """
    try:
        filled = await _fill_first_input(
            page,
            [
                'input.passport--password-ipt',
                'input[placeholder*="Код"]',
                'input[type="text"]',
            ],
            code,
        )
        if not filled:
            return {
                "success": False,
                "error": "Не найдено поле для кода.",
                "uid": None,
                "cookies": None,
                "username": None,
            }

        login_btn = page.locator("a.passport--passport-common-btn.passport--yellow")
        if await login_btn.count() > 0:
            await login_btn.first.click(timeout=5000)
        else:
            await page.keyboard.press("Enter")

        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeout:
            pass

        cookies_list = await context.cookies()
        cookies_result = {c["name"]: c["value"] for c in cookies_list}
        token = cookies_result.get("gpc_sso_token")
        uid = jwt_get_uid(token) if token else None

        return {
            "success": bool(uid),
            "error": None if uid else "Не удалось получить IGG ID после входа.",
            "uid": uid,
            "cookies": cookies_result,
            "username": "Игрок",
        }
    except Exception as e:
        logger.exception(f"[SHOP] ❌ Ошибка при подтверждении кода: {e}")
        return {
            "success": False,
            "error": str(e),
            "uid": None,
            "cookies": None,
            "username": None,
        }
    finally:
        if playwright is None:
            try:
                await page.close()
                await context.close()
            except Exception as exc:
                logger.debug("[SHOP] Cleanup failed after IGG code: %s", exc)

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
        return {
            "success": False,
            "error": "MVP ссылка не найдена. Добавь аккаунт заново.",
            "cookies": None,
        }

    mvp_url = acc["mvp_url"]
    cookies_result: dict[str, str] = {}
    ctx: ShopContext | None = None
    cookies_saved = False

    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            profile = get_random_browser_profile()
            stealth_callable = _get_stealth_callable()
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir=f"data/chrome_profiles/{uid}",
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=SLOW_MO,
                profile=profile,
                stealth_callable=stealth_callable,
            )

            context = ctx["context"]
            page = ctx["page"]

            await page.goto(mvp_url, timeout=60000)
            await _accept_cookies(page)
            await wait_shop_ready(page)
            logger.info("[COOKIES] 🌍 Открыта страница MVP")
            await humanize_pre_action(page)

            # 📦 Сохраняем cookies
            cookies_list = await context.cookies()
            cookies_result = {c["name"]: c["value"] for c in cookies_list}

            all_data = load_all_cookies()
            all_data.setdefault(str(user_id), {})[str(uid)] = cookies_result
            save_all_cookies(all_data)
            cookies_saved = True

            logger.info(f"[COOKIES] 💾 Cookies обновлены для UID={uid}")
            return {
                "success": True,
                "error": None,
                "cookies": cookies_result,
            }

    except Exception as e:
        if cookies_saved:
            logger.warning("[COOKIES] ⚠️ Ошибка после сохранения cookies: %s", e)
            return {
                "success": True,
                "error": None,
                "cookies": cookies_result,
            }
        logger.exception(f"[COOKIES] ❌ Ошибка при обновлении cookies: {e}")
        return {
            "success": False,
            "error": str(e),
            "cookies": None,
        }

    finally:
        try:
            if ctx:
                if "page" in ctx:
                    await ctx["page"].close()
                if "context" in ctx:
                    await ctx["context"].close()
        except Exception as exc:
            logger.debug("[COOKIES] Cleanup failed: %s", exc)


# ───────────────────────────────────────────────
# 🎁 Извлечение награды из ответа
# ───────────────────────────────────────────────

def extract_reward_from_response(text: str) -> str | None:
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
    except Exception as exc:
        logger.debug("[MVP] Reward parse failed: %s", exc)

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

async def extract_player_info_from_page(url: str) -> PlayerInfoResult:
    """
    🌐 Открывает MVP ссылку и извлекает IGG ID + имя игрока (через browser_patches).
    """
    logger.info(f"[MVP] 🌐 Открываю страницу для получения данных: {url}")
    uid: str | None = None
    username: str | None = None
    ctx: ShopContext | None = None

    from playwright.async_api import async_playwright
    try:
        async with async_playwright() as p:
            profile = get_random_browser_profile()
            stealth_callable = _get_stealth_callable()
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir="data/chrome_profiles/_extract_tmp",
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=SLOW_MO,
                profile=profile,
                stealth_callable=stealth_callable,
            )
            context = ctx["context"]
            page = ctx["page"]

            await page.goto(url, timeout=60000)
            await _accept_cookies(page)
            await wait_shop_ready(page)
            logger.info("[MVP] ⏳ Ожидание загрузки страницы...")
            await humanize_pre_action(page)

            await page.wait_for_selector(".user__infos-item", timeout=45000)
            blocks = await page.query_selector_all(".user__infos-item")

            for b in blocks:
                text = (await b.inner_text()).strip()
                if "IGG ID" in text:
                    match = re.search(r"\b\d{6,12}\b", text)
                    if match:
                        uid = match.group(0)
                elif "Имя игрока" in text:
                    match = re.search(r"Имя игрока[:：]?\s*(.+)", text)
                    if match:
                        username = match.group(1).strip()

            if uid and username:
                logger.info(f"[MVP] ✅ Найден IGG ID={uid}, username={username}")
                return {
                    "success": True,
                    "error": None,
                    "uid": uid,
                    "username": username,
                }

            return {
                "success": False,
                "error": "Не удалось извлечь IGG ID или имя",
                "uid": uid,
                "username": username,
            }

    except Exception as e:
        logger.error(f"[MVP] ❌ Ошибка при открытии страницы: {e}")
        return {
            "success": False,
            "error": str(e),
            "uid": None,
            "username": None,
        }

    finally:
        try:
            if ctx:
                await ctx["page"].close()
                await ctx["context"].close()
        except Exception as exc:
            logger.debug("[MVP] Cleanup failed: %s", exc)
