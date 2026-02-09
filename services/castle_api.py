"""castleclash MVP через HTTP (aiohttp) — исправленная версия."""

import asyncio
import base64
import json
import os
import re
import time
from datetime import datetime
from typing import Any, TypedDict
from playwright.async_api import (
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeout,
)
from services.logger import logger
from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    humanize_pre_action,
    jitter,
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


SHOP_READY_SELECTORS = [
    "div.btn-login.login__btn.before-login:has-text('Авторизация')",
    "div.userbar .btn-login.login__btn.before-login",
    ".main .userbar .btn-login.login__btn.before-login",
    ".passport--modal",
    "#component_passport .passport--frame-close",
    ".passport--container-outer",
    ".passport--container",
    ".userbar",
    "#userBar",
]

MVP_READY_SELECTOR = ".user__infos-item"


async def wait_shop_ready(page: Page, timeout: int = 60000, attempts: int = 2) -> None:
    deadline = time.monotonic() + (timeout / 1000)
    def _remaining_ms() -> int:
        return int(max(0, (deadline - time.monotonic()) * 1000))

    async def _has_userbar() -> bool:
        try:
            return await page.locator("#userBar, .userbar").count() > 0
        except Exception as exc:
            logger.debug("[SHOP] Userbar presence check failed: %s", exc)
            return False

    for attempt in range(1, attempts + 1):
        try:
            remaining_ms = min(15000, _remaining_ms())
            if remaining_ms > 0:
                await page.wait_for_load_state("domcontentloaded", timeout=remaining_ms)
        except PlaywrightTimeout:
            pass
        except Exception as exc:
            logger.debug("[SHOP] domcontentloaded wait failed: %s", exc)

        try:
            remaining_ms = min(12000, _remaining_ms())
            if remaining_ms > 0:
                await page.wait_for_load_state("networkidle", timeout=remaining_ms)
        except PlaywrightTimeout:
            pass
        except Exception as exc:
            logger.debug("[SHOP] networkidle wait failed: %s", exc)

        for selector in SHOP_READY_SELECTORS:
            remaining_ms = _remaining_ms()
            if remaining_ms <= 0:
                if await _has_userbar():
                    logger.info("[SHOP] Userbar present despite readiness timeout.")
                    return
                logger.warning("[SHOP] Page readiness timeout exceeded.")
                raise PlaywrightTimeout("Shop readiness timeout exceeded.")
            try:
                await page.wait_for_selector(selector, state="visible", timeout=remaining_ms)
                return
            except PlaywrightTimeout:
                continue
            except Exception as exc:
                logger.debug("[SHOP] Wait selector failed (%s): %s", selector, exc)

        if attempt < attempts:
            if await _has_userbar():
                logger.info("[SHOP] Userbar present after readiness attempts.")
                return
            logger.warning("[SHOP] ⚠️ Готовность магазина не подтверждена, попытка %s/%s.", attempt, attempts)
            await asyncio.sleep(jitter(1.0, 0.5))

    logger.warning("[SHOP] No readiness selector appeared within timeout.")
    raise PlaywrightTimeout("Shop readiness selector not found within timeout.")


async def wait_mvp_ready(page: Page, timeout: int = 60000) -> None:
    try:
        await page.wait_for_selector(MVP_READY_SELECTOR, state="visible", timeout=timeout)
    except PlaywrightTimeout:
        logger.warning("[MVP] MVP readiness timeout exceeded.")
        raise
    except Exception as exc:
        logger.debug("[MVP] MVP readiness wait failed: %s", exc)
        raise


async def open_shop_page_with_retry(page: Page, url: str, attempts: int = 3) -> None:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            if attempt == 1:
                logger.info("[SHOP] 🌍 Открываем страницу магазина")
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            else:
                logger.warning("[SHOP] ⚠️ Повтор загрузки магазина (%s/%s).", attempt, attempts)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=60000)
                except Exception:
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await _accept_cookies(page)
            await wait_shop_ready(page)
            return
        except Exception as exc:
            last_error = exc
            await _capture_login_error_screenshot(page, f"shop_ready_retry_{attempt}")
            if attempt < attempts:
                await asyncio.sleep(jitter(1.5, 0.6))
            else:
                break
    if last_error:
        raise last_error


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


async def _dispatch_vue_input_events(page: Page, selectors: list[str]) -> bool:
    for selector in selectors:
        contexts = [page, *page.frames]
        for ctx in contexts:
            try:
                found = await ctx.evaluate(
                    """(sel) => {
                        const el = document.querySelector(sel);
                        if (!el) return false;
                        const events = ["input", "change"];
                        for (const type of events) {
                            const event = new Event(type, { bubbles: true });
                            el.dispatchEvent(event);
                        }
                        if (typeof el.blur === "function") {
                            el.blur();
                        }
                        return true;
                    }""",
                    selector,
                )
                if found:
                    return True
            except Exception as exc:
                logger.debug("[SHOP] Vue input event dispatch failed (%s): %s", selector, exc)
    return False


async def _wait_for_auth_cookie(
    page: Page,
    context: BrowserContext,
    timeout_ms: int = 20000,
) -> bool:
    cookie_names = {"gpc_sso_token", "ssoToken", "passport_token"}
    deadline = time.monotonic() + (timeout_ms / 1000)
    while time.monotonic() < deadline:
        try:
            cookies = await context.cookies()
        except Exception as exc:
            logger.debug("[SHOP] Cookie read failed while waiting auth cookie: %s", exc)
            cookies = []
        if any(cookie.get("name") in cookie_names for cookie in cookies):
            return True
        await page.wait_for_timeout(500)
    return False


async def _wait_for_login_response(page: Page, timeout_ms: int = 20000) -> bool:
    def _matches(response) -> bool:
        try:
            url = response.url.lower()
            return ("passport" in url or "/login" in url) and response.ok
        except Exception:
            return False

    try:
        response = await page.wait_for_response(_matches, timeout=timeout_ms)
        if response:
            logger.debug("[SHOP] Login response OK: %s", response.url)
            return True
    except PlaywrightTimeout:
        return False
    except Exception as exc:
        logger.debug("[SHOP] Login response wait failed: %s", exc)
    return False


async def _wait_for_login_success(page: Page, context: BrowserContext, timeout_ms: int = 20000) -> bool:
    response_task = asyncio.create_task(_wait_for_login_response(page, timeout_ms))
    cookie_task = asyncio.create_task(_wait_for_auth_cookie(page, context, timeout_ms))
    done, pending = await asyncio.wait(
        {response_task, cookie_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    if not done:
        return False
    return any(task.result() for task in done if not task.cancelled())


async def _try_vue_login(page: Page, selectors: list[str]) -> bool:
    for selector in selectors:
        contexts = [page, *page.frames]
        for ctx in contexts:
            try:
                invoked = await ctx.evaluate(
                    """(sel) => {
                        const btn = document.querySelector(sel);
                        if (!btn || !btn.__vueParentComponent) return false;
                        const comp = btn.__vueParentComponent;
                        const login = comp?.ctx?.login;
                        if (typeof login === "function") {
                            login.call(comp.ctx);
                            return true;
                        }
                        return false;
                    }""",
                    selector,
                )
                if invoked:
                    return True
            except Exception as exc:
                logger.debug("[SHOP] Vue login invoke failed (%s): %s", selector, exc)
    return False


async def _clear_page_storage(page: Page) -> None:
    try:
        await page.evaluate("() => { localStorage.clear(); sessionStorage.clear(); }")
    except Exception as exc:
        logger.debug("[SHOP] Storage clear failed: %s", exc)


async def _click_login_button(page: Page, selectors: list[str]) -> bool:
    try:
        await page.locator(".passport--container-outer").wait_for(state="hidden", timeout=3000)
    except PlaywrightTimeout:
        pass
    except Exception as exc:
        logger.debug("[SHOP] Login overlay wait failed: %s", exc)

    found_any = False
    for selector in selectors:
        contexts = [page, *page.frames]
        for ctx in contexts:
            try:
                try:
                    clicked = await ctx.evaluate(
                        """(sel) => {
                            const btn = document.querySelector(sel);
                            if (!btn) return false;
                            btn.scrollIntoView({ block: "center", inline: "center" });
                            const down = new PointerEvent("pointerdown", {
                                bubbles: true,
                                cancelable: true,
                                pointerType: "mouse",
                            });
                            const up = new PointerEvent("pointerup", {
                                bubbles: true,
                                cancelable: true,
                                pointerType: "mouse",
                            });
                            btn.dispatchEvent(down);
                            btn.dispatchEvent(up);
                            if (typeof btn.click === "function") {
                                btn.click();
                            }
                            return true;
                        }""",
                        selector,
                    )
                    if clicked:
                        return True
                except Exception as exc:
                    logger.warning("[SHOP] Dispatch login click failed (%s): %s", selector, exc)
            except Exception as exc:
                logger.debug("[SHOP] Login button lookup failed (%s): %s", selector, exc)
    if not found_any:
        logger.error("[SHOP] Не найдена кнопка входа для клика.")
    return False


async def _close_passport_frame(page: Page) -> None:
    selectors = [
        "#component_passport .passport--frame-close",
        ".passport--container-outer .passport--frame-close",
        "div.passport--frame-close",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            if await locator.count() > 0 and await locator.first.is_visible():
                await locator.first.click(timeout=3000)
                return
        except Exception as exc:
            logger.debug("[SHOP] Passport close failed (%s): %s", selector, exc)

    for frame in page.frames:
        for selector in selectors:
            try:
                locator = frame.locator(selector)
                if await locator.count() > 0 and await locator.first.is_visible():
                    await locator.first.click(timeout=3000)
                    return
            except Exception as exc:
                logger.debug("[SHOP] Passport frame close failed (%s): %s", selector, exc)


async def _extract_userbar_info(page: Page) -> tuple[str | None, str | None]:
    uid: str | None = None
    username: str | None = None
    selectors = [
        "#userBar .name",
        "#userBar .username",
        "#userBar .nickname",
        "#userBar .user-name",
        ".userbar .name",
        ".userbar .username",
        ".userbar .nickname",
        ".userbar__name",
        ".userbar .userbar__name",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector)
            if await locator.count() == 0:
                continue
            text = (await locator.first.inner_text()).strip()
            if text:
                username = text
                break
        except Exception as exc:
            logger.debug("[SHOP] Username lookup failed (%s): %s", selector, exc)

    try:
        userbar = page.locator("#userBar, .userbar").first
        if await userbar.count() > 0:
            text = (await userbar.inner_text()).strip()
            if text:
                match = re.search(r"\b\d{6,12}\b", text)
                if match:
                    uid = match.group(0)
                if not username:
                    lines = [line.strip() for line in text.splitlines() if line.strip()]
                    for line in lines:
                        if "IGG" in line or "ID" in line:
                            continue
                        if re.search(r"\b\d{6,12}\b", line):
                            continue
                        username = line
                        break
    except Exception as exc:
        logger.debug("[SHOP] Userbar parse failed: %s", exc)

    return uid, username


async def _capture_login_error_screenshot(page: Page | None, tag: str) -> str | None:
    if not page:
        return None
    try:
        if page.is_closed():
            logger.warning("[SHOP] ⚠️ Не удалось сделать скриншот ошибки: page already closed.")
            return None
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


async def _is_login_form_visible(page: Page, selectors: list[str]) -> bool:
    for frame in page.frames:
        for selector in selectors:
            try:
                locator = frame.locator(selector)
                if await locator.count() == 0:
                    continue
                if await locator.first.is_visible():
                    return True
            except Exception as exc:
                logger.debug("[SHOP] Login form visibility check failed (%s): %s", selector, exc)
    return False


async def _wait_for_login_form(page: Page, timeout: int = 15000) -> bool:
    selectors = [
        ".passport--modal",
        ".passport--form",
        ".passport--form-ipt",
        "input[type=\"email\"]",
        "input.passport--email-ipt",
        "input[type=\"password\"]",
        "input.passport--password-ipt",
    ]
    deadline = time.monotonic() + (timeout / 1000)
    while time.monotonic() < deadline:
        if await _is_login_form_visible(page, selectors):
            return True
        await asyncio.sleep(0.4)
    return False


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
            profile.update(
                {
                    "user_agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "locale": "ru-RU",
                    "accept_language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                }
            )
            logger.info("[SHOP] ▶ Запуск браузера для входа по email")
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir="data/chrome_profiles/_shop_email",
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=SLOW_MO,
                profile=profile,
                apply_patches=False,
                set_extra_headers=False,
            )
            context = ctx["context"]
            page = ctx["page"]
            try:
                await context.clear_cookies()
            except Exception as exc:
                logger.debug("[SHOP] Cookie clear failed before login: %s", exc)
            await open_shop_page_with_retry(page, "https://castleclash.igg.com/shop/")
            await _clear_page_storage(page)
            await _accept_cookies(page)
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

            login_modal_ready = False
            for attempt in range(1, 3):
                modal_opened = await _open_login_modal(page)
                if modal_opened:
                    if await _wait_for_login_form(page, timeout=15000):
                        login_modal_ready = True
                        break
                    logger.warning("[SHOP] ⚠️ Окно авторизации не появилось за 15 секунд.")
                else:
                    logger.warning("[SHOP] ⚠️ Не удалось нажать кнопку авторизации.")
                if attempt < 2:
                    await page.reload(wait_until="domcontentloaded", timeout=60000)
                    await _accept_cookies(page)

            if not login_modal_ready:
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
            await _dispatch_vue_input_events(
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
            )

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
            await _dispatch_vue_input_events(
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
            )

            logger.info("[SHOP] ✅ Нажимаем кнопку входа")
            await _accept_cookies(page)
            login_button_selectors = [
                "a.passport--passport-common-btn.passport--yellow",
            ]
            clicked = await _click_login_button(page, login_button_selectors)
            if not clicked:
                await _capture_login_error_screenshot(page, "login_button_not_found")
                return {
                    "success": False,
                    "error": "Не удалось найти кнопку входа.",
                    "uid": None,
                    "cookies": None,
                    "username": None,
                }
            await page.wait_for_timeout(7000)
            await _capture_login_error_screenshot(page, "after_login_click_delay")

            login_success = await _wait_for_login_success(page, context, timeout_ms=30000)
            if not login_success:
                logger.info("[SHOP] 🔁 Пробуем вызвать Vue login() напрямую.")
                invoked = await _try_vue_login(page, login_button_selectors)
                if invoked:
                    login_success = await _wait_for_login_success(page, context, timeout_ms=20000)

            if not login_success:
                await _capture_login_error_screenshot(page, "login_failed")
                logger.error("[SHOP] ❌ Не удалось дождаться подтверждения входа.")
                return {
                    "success": False,
                    "error": "Не удалось дождаться подтверждения входа.",
                    "uid": None,
                    "cookies": None,
                    "username": None,
                }

            # await _close_passport_frame(page)
            parsed_uid, parsed_username = await _extract_userbar_info(page)

            logger.info("[SHOP] 🔎 Проверяем cookies после входа")
            cookies_list = await context.cookies()
            cookies_result = {c["name"]: c["value"] for c in cookies_list}
            token = cookies_result.get("gpc_sso_token")
            uid = jwt_get_uid(token) if token else None
            if parsed_uid and not uid:
                uid = parsed_uid
            if not uid:
                await _capture_login_error_screenshot(page, "uid_not_found")
                return {
                    "success": False,
                    "error": "Не удалось получить IGG ID после входа.",
                    "uid": None,
                    "cookies": cookies_result,
                    "username": parsed_username,
                }

            logger.info("[SHOP] ✅ Вход успешен, UID=%s", uid)
            try:
                await wait_shop_ready(page)
            except PlaywrightTimeout as exc:
                logger.error("[SHOP] ❌ Таймаут полной загрузки магазина: %s", exc)
            except Exception as exc:
                logger.error("[SHOP] ❌ Ошибка ожидания полной готовности магазина: %s", exc)
            return {
                "success": True,
                "error": None,
                "uid": uid,
                "cookies": cookies_result,
                "username": parsed_username or "Игрок",
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
        logger.info("[SHOP] ▶ Запуск браузера для входа по IGG ID")
        ctx = await launch_masked_persistent_context(
            playwright,
            user_data_dir=f"data/chrome_profiles/_shop_igg_{igg_id}",
            browser_path=BROWSER_PATH,
            headless=True,
            slow_mo=SLOW_MO,
            profile=profile,
            apply_patches=False,
            set_extra_headers=False,
        )
        context = ctx["context"]
        page = ctx["page"]
        try:
            await context.clear_cookies()
        except Exception as exc:
            logger.debug("[SHOP] Cookie clear failed before IGG login: %s", exc)

        logger.info("[SHOP] 🌍 Открываем страницу магазина (IGG ID)")
        await open_shop_page_with_retry(page, "https://castleclash.igg.com/shop/")
        await _clear_page_storage(page)
        await _accept_cookies(page)
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

        await _accept_cookies(page)
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

        await _accept_cookies(page)

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
        await _accept_cookies(page)
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
        await _dispatch_vue_input_events(
            page,
            [
                'input.passport--password-ipt',
                'input[placeholder*="Код"]',
                'input[type="text"]',
            ],
        )

        login_button_selectors = [
            "a.passport--passport-common-btn.passport--yellow",
        ]
        clicked = await _click_login_button(page, login_button_selectors)
        if not clicked:
            return {
                "success": False,
                "error": "Не удалось найти кнопку входа.",
                "uid": None,
                "cookies": None,
                "username": None,
            }

        await _accept_cookies(page)

        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except PlaywrightTimeout:
            pass

        login_success = await _wait_for_login_success(page, context, timeout_ms=30000)
        if not login_success:
            logger.info("[SHOP] 🔁 Пробуем вызвать Vue login() напрямую.")
            invoked = await _try_vue_login(page, login_button_selectors)
            if invoked:
                login_success = await _wait_for_login_success(page, context, timeout_ms=20000)
        if not login_success:
            logger.debug("[SHOP] Login success markers not found after IGG login.")
            return {
                "success": False,
                "error": "Не удалось дождаться подтверждения входа.",
                "uid": None,
                "cookies": None,
                "username": None,
            }

        # await _close_passport_frame(page)
        parsed_uid, parsed_username = await _extract_userbar_info(page)

        cookies_list = await context.cookies()
        cookies_result = {c["name"]: c["value"] for c in cookies_list}
        token = cookies_result.get("gpc_sso_token")
        uid = jwt_get_uid(token) if token else None

        if parsed_uid and not uid:
            uid = parsed_uid

        return {
            "success": bool(uid),
            "error": None if uid else "Не удалось получить IGG ID после входа.",
            "uid": uid,
            "cookies": cookies_result,
            "username": parsed_username or "Игрок",
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
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir=f"data/chrome_profiles/{uid}",
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=SLOW_MO,
                profile=profile,
                apply_patches=False,
                set_extra_headers=False,
            )

            context = ctx["context"]
            page = ctx["page"]

            await page.goto(mvp_url, wait_until="domcontentloaded", timeout=60000)
            await _accept_cookies(page)
            await wait_mvp_ready(page)
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
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir="data/chrome_profiles/_extract_tmp",
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=SLOW_MO,
                profile=profile,
                apply_patches=False,
                set_extra_headers=False,
            )
            context = ctx["context"]
            page = ctx["page"]

            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await _accept_cookies(page)
            await wait_mvp_ready(page)
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
