# tg_zov/services/browser_patches.py
"""
Универсальные утилиты для запуска замаскированных Playwright-контекстов и
общие helper'ы.

Содержимое:
- silence_asyncio_exceptions(loop, context)     — приглушаем «шумные» asyncio-исключения
- get_random_browser_profile()                  — профиль браузера (UA, viewport, locale, tz, …)
- humanize_pre_action(page)                     — лёгкая «очеловеченная» активность
- jitter(base, variance)                        — джиттер для задержек
- cookies_to_playwright(cookies, domain)        — {name: value} -> cookies Playwright
- apply_headless_patches(context, ...)          — init-скрипты маскировки headless + stealth
- launch_masked_persistent_context(...)         — запуск persistent context с патчами
- update_new_data_files_with_cookies(...)       — обновление cookies в new_data*.json
- run_event_with_browser(...)                   — единый раннер события (подставляет куки, открывает URL, зовёт handler)
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import random
import re
import platform
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.logger import logger
from services.cookies_io import load_all_cookies, save_all_cookies
from playwright.async_api import Page, BrowserContext, async_playwright

logger = logging.getLogger("browser_patches")

# ───────────────────────────────────────────────
# 📁 Пути и глобальные параметры
# ───────────────────────────────────────────────


def detect_chromium_path() -> str | None:
    """
    🔍 Универсальный поиск пути к Chrome/Chromium
    Работает на Windows, Linux, macOS и Android/Termux.
    Возвращает None — если нужно, чтобы Playwright сам выбрал встроенный Chromium.
    """
    system = platform.system().lower()
    candidates = []

    # 🪟 Windows
    if "windows" in system:
        candidates += [
            r".venv/Chrome/Application/chrome.exe",
            r"C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            r"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
        ]

    # 🍎 macOS
    elif "darwin" in system:
        candidates += [
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/usr/local/bin/chromium",
        ]

    # 🐧 Linux / Ubuntu / Debian / Termux
    else:
        candidates += [
            shutil.which("google-chrome-stable"),
            shutil.which("google-chrome"),
            shutil.which("chromium-browser"),
            shutil.which("chromium"),
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium-browser",
            "/usr/bin/chromium",
            "/data/data/com.termux/files/usr/bin/chromium",  # Termux
        ]

    for path in candidates:
        if path and os.path.exists(path):
            return path

    print("⚠️ [detect_chromium_path] Chrome/Chromium не найден, Playwright сам выберет встроенный.")
    return None


# ✅ Глобальный путь (используй эту переменную везде)
BROWSER_PATH = detect_chromium_path()
print(f"[browser_patches] Используется браузер: {BROWSER_PATH}")


PROFILE_DIR = Path("data/chrome_profiles")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)


# ───────────────────────────────────────────────────────────────────────────────
# asyncio handler
# ───────────────────────────────────────────────────────────────────────────────

def silence_asyncio_exceptions(loop, context):
    """
    Игнорируем некоторые ожидаемые предупреждения/ошибки при массовом Playwright.
    """
    msg = context.get("message")
    exc = context.get("exception")

    if isinstance(exc, asyncio.CancelledError):
        return
    if exc and "Target page, context or browser has been closed" in str(exc):
        return
    if msg and "Future exception was never retrieved" in msg:
        return

    loop.default_exception_handler(context)


# ───────────────────────────────────────────────────────────────────────────────
# Профили / "очеловечивание" / джиттер
# ───────────────────────────────────────────────────────────────────────────────

def get_random_browser_profile() -> Dict[str, Any]:
    """Формирует профиль браузера (UA, viewport, locale, timezone, accept-language и пр.)."""
    UAS = [
        # desktop
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36) (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36) (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit(537.36) (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        # mobile
        "Mozilla/5.0 (iPhone; CPU iPhone OS 16_1_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Mobile/15E148 Safari/604.1",
        "Mozilla/5.0 (Linux; Android 10; SM-A205U) AppleWebKit(537.36) (KHTML, like Gecko) Chrome/89.0.4389.105 Mobile Safari/537.36",
        "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit(537.36) (KHTML, like Gecko) Chrome/120.0 Mobile Safari/537.36",
    ]
    viewports = [
        {"width": 360, "height": 640},
        {"width": 375, "height": 667},
        {"width": 390, "height": 844},
        {"width": 412, "height": 915},
        {"width": 768, "height": 1024},
        {"width": 800, "height": 600},
        {"width": 1024, "height": 768},
    ]
    locales = ["en-US", "en-GB", "ru-RU", "de-DE", "fr-FR"]
    timezones = [
        "Europe/Moscow", "Europe/Prague", "America/New_York",
        "Asia/Shanghai", "Asia/Tokyo", "Europe/London",
    ]
    accept_languages = [
        "en-US,en;q=0.9",
        "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "de-DE,de;q=0.9,en;q=0.8",
        "fr-FR,fr;q=0.9,en;q=0.8",
    ]

    ua = random.choice(UAS)
    vp = random.choice(viewports)
    locale = random.choice(locales)
    timezone = random.choice(timezones)
    accept_language = random.choice(accept_languages)

    is_mobile = ("Mobile" in ua) or ("iPhone" in ua) or ("Android" in ua)
    device_scale_factor = random.choice([1, 1, 1.5, 2])
    hardware_concurrency = random.choice([2, 4, 6, 8])

    chrome_match = re.search(r"Chrome/(\d+)", ua)
    chrome_version = chrome_match.group(1) if chrome_match else "120"

    if "Android" in ua:
        platform = "Android"
        sec_platform = '"Android"'
    elif "iPhone" in ua or "iPad" in ua:
        platform = "iOS"
        sec_platform = '"iOS"'
    elif "Macintosh" in ua:
        platform = "macOS"
        sec_platform = '"macOS"'
    else:
        platform = "Windows"
        sec_platform = '"Windows"'

    sec_ch_ua = (
        f'"Not/A)Brand";v="8", "Chromium";v="{chrome_version}", '
        f'"Google Chrome";v="{chrome_version}"'
    )

    return {
        "user_agent": ua,
        "viewport": vp,
        "locale": locale,
        "timezone": timezone,
        "accept_language": accept_language,
        "is_mobile": is_mobile,
        "device_scale_factor": device_scale_factor,
        "hardware_concurrency": hardware_concurrency,
        "platform": platform,
        "sec_ch_ua": sec_ch_ua,
        "sec_ch_ua_mobile": "?1" if is_mobile else "?0",
        "sec_ch_ua_platform": sec_platform,
    }


async def humanize_pre_action(page: Page):
    """Лёгкая имитация поведения пользователя перед действиями."""
    try:
        await page.mouse.move(100 + random.randint(-20, 20), 100 + random.randint(-15, 15), steps=random.randint(6, 12))
        await asyncio.sleep(0.15 + random.random() * 0.35)
        await page.mouse.move(200 + random.randint(-30, 30), 140 + random.randint(-20, 20), steps=random.randint(5, 9))
        await asyncio.sleep(0.05 + random.random() * 0.2)
        await page.mouse.wheel(0, random.randint(100, 300))
        await asyncio.sleep(0.1 + random.random() * 0.25)
    except Exception:
        pass


def jitter(base: float, variance: float = 0.5) -> float:
    """Случайная задержка: base ± (0..variance*base)."""
    delta = random.uniform(-variance * base, variance * base)
    return max(0.1, base + delta)


# ───────────────────────────────────────────────────────────────────────────────
# Cookies helpers
# ───────────────────────────────────────────────────────────────────────────────

def cookies_to_playwright(cookies: Dict[str, str], domain: str = ".event-eu-cc.igg.com") -> List[Dict[str, Any]]:
    """{name: value} -> список cookie-объектов Playwright."""
    return [{"name": str(k), "value": str(v), "domain": domain, "path": "/"} for k, v in cookies.items()]


# ───────────────────────────────────────────────────────────────────────────────
# Headless patches / stealth
# ───────────────────────────────────────────────────────────────────────────────

async def _maybe_call_stealth(stealth_callable, page: Page):
    """
    Корректный вызов stealth (может быть sync/async).
    """
    try:
        if inspect.iscoroutinefunction(stealth_callable):
            await stealth_callable(page)
        else:
            res = stealth_callable(page)
            if inspect.isawaitable(res):
                await res
    except Exception as e:
        logger.warning("stealth failed: %s", e)


async def apply_headless_patches(
    context: BrowserContext,
    page: Optional[Page] = None,
    profile: Optional[Dict[str, Any]] = None,
    stealth_callable=None,
):
    """
    Добавляет init-скрипты маскировки headless (webdriver, plugins, languages, WebGL, connection…)
    и при наличии — вызывает stealth.
    """
    try:
        patch_script = """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined, configurable: true });
            window.chrome = { runtime: {} };
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3], configurable: true });
            Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru', 'en-US'], configurable: true });
            Object.defineProperty(Notification, 'permission', { get: () => 'default' });
            Object.defineProperty(navigator, 'pdfViewerEnabled', { get: () => true });
            const _getParameter = WebGLRenderingContext.prototype.getParameter;
            WebGLRenderingContext.prototype.getParameter = function(param) {
                if (param === 37445) return 'NVIDIA GeForce RTX 3080';
                if (param === 37446) return 'Google Inc. (NVIDIA)';
                return _getParameter.call(this, param);
            };
        """
        connection_patch = """
            Object.defineProperty(navigator, 'connection', {
              value: { rtt: 50, downlink: 10, effectiveType: '4g', saveData: false },
              configurable: true
            });
            Object.defineProperty(navigator, 'deviceMemory', { value: 8 });
        """
        extra_mask_patch = """
            try {
              if (navigator.permissions && navigator.permissions.query) {
                const orig = navigator.permissions.query;
                navigator.permissions.query = (param) => (
                  param && param.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : orig(param)
                );
              }
              Object.defineProperty(navigator, 'hardwareConcurrency', { value: 8 });
              Object.defineProperty(navigator, 'maxTouchPoints', { value: 1 });
            } catch (e) {}
        """

        await context.add_init_script(connection_patch)
        await context.add_init_script(patch_script)
        await context.add_init_script(extra_mask_patch)

        if page is not None and stealth_callable is not None:
            await _maybe_call_stealth(stealth_callable, page)

    except Exception as e:
        logger.warning("apply_headless_patches error: %s", e)


# ───────────────────────────────────────────────────────────────────────────────
# Запуск persistent context с патчами
# ───────────────────────────────────────────────────────────────────────────────

async def launch_masked_persistent_context(
    p,
    user_data_dir: str,
    *,
    browser_path: Optional[str] = None,
    headless: bool = True,
    slow_mo: int = 0,
    profile: Optional[Dict[str, Any]] = None,
    stealth_callable=None,
    extra_args: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Удобный wrapper для запуска persistent context с маскировкой.
    Возвращает: {"context": BrowserContext, "page": Page, "profile": dict}
    """
    if profile is None:
        profile = get_random_browser_profile()

    window_args = [
        "--headless=new",
        "--disable-gpu",
        "--mute-audio",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-blink-features=AutomationControlled",
        "--disable-infobars",
        "--hide-scrollbars",
        "--window-size=1920,1080",
        "--blink-settings=imagesEnabled=true",
        "--disable-extensions",
        "--disable-translate",
        "--disable-background-timer-throttling",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
    ]
    if extra_args:
        window_args.extend(extra_args)

    launch_kwargs = dict(
        headless=headless,
        slow_mo=slow_mo,
        viewport=profile["viewport"],
        user_agent=profile["user_agent"],
        locale=profile["locale"],
        timezone_id=profile["timezone"],
        is_mobile=profile["is_mobile"],
        device_scale_factor=profile["device_scale_factor"],
        java_script_enabled=True,
        args=window_args,
    )
    if browser_path:
        launch_kwargs["executable_path"] = browser_path

    context = await p.chromium.launch_persistent_context(user_data_dir, **launch_kwargs)

    page = await context.new_page()
    try:
        await apply_headless_patches(context, page=page, profile=profile, stealth_callable=stealth_callable)
    except Exception:
        pass

    try:
        await context.set_extra_http_headers({"Accept-Language": profile.get("accept_language", "en-US,en")})
    except Exception:
        pass

    return {"context": context, "page": page, "profile": profile}


# ───────────────────────────────────────────────────────────────────────────────
# Обновление cookies в new_data*.json
# ───────────────────────────────────────────────────────────────────────────────

def atomic_write_json(path: Path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(str(tmp), str(path))


def update_new_data_files_with_cookies(data_dir: Path, uid: str, cookie_dict: Dict[str, str]) -> int:
    """
    Для каждого файла new_data*.json в data_dir, если находим запись с ключом == uid,
    подменяем значение на cookie_dict. Возвращает количество изменённых файлов.
    """
    changed_count = 0
    if not data_dir.exists():
        logger.warning("update_new_data_files_with_cookies: data_dir не найден: %s", data_dir)
        return 0

    for file_path in sorted(data_dir.glob("new_data*.json")):
        modified = False
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as e:
            logger.warning("Не удалось прочитать %s: %s", file_path, e)
            continue

        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and str(uid) in entry and isinstance(entry.get(str(uid)), dict):
                    entry[str(uid)] = cookie_dict
                    modified = True
                    break
        else:
            # Неожиданный формат — пропускаем
            pass

        if modified:
            try:
                atomic_write_json(file_path, data)
                logger.info("Cookies обновлены в %s для uid=%s", file_path.name, uid)
                changed_count += 1
            except Exception as e:
                logger.warning("Ошибка при записи %s: %s", file_path, e)

    return changed_count


# ───────────────────────────────────────────────
# 🧩 Универсальный запуск события
# ───────────────────────────────────────────────
async def run_event_with_browser(user_id: str, uid: str, event_url: str, event_name: str, handler_fn):
    """
    Унифицированный запуск браузера и передача страницы в обработчик.
    handler_fn(page) -> {"success": bool, "message": str}
    """
    user_id, uid = str(user_id), str(uid)
    cookies_db = load_all_cookies()
    user_cookies = cookies_db.get(user_id, {})
    acc_cookies = user_cookies.get(uid, {})

    async with async_playwright() as p:
        profile = get_random_browser_profile()
        ctx = await launch_masked_persistent_context(
            p,
            user_data_dir=str(PROFILE_DIR / uid),
            browser_path=BROWSER_PATH,
            headless=True,  # 👈 можно вынести в config
            slow_mo=30,
            profile=profile,
        )
        context, page = ctx["context"], ctx["page"]

        try:
            # 🍪 применяем cookies
            if acc_cookies:
                await context.add_cookies(cookies_to_playwright(acc_cookies))

            # 🌍 переходим на страницу акции
            await page.goto(event_url, wait_until="domcontentloaded", timeout=45_000)
            await asyncio.sleep(2)
            await humanize_pre_action(page)

            # ⚙️ выполняем обработчик события
            result = await handler_fn(page)

            # 🔄 сохраняем свежие cookies
            fresh = await context.cookies()
            fresh_map = {c["name"]: c["value"] for c in fresh if "name" in c}
            cookies_db.setdefault(user_id, {})[uid] = fresh_map
            save_all_cookies(cookies_db)
            logger.info(f"[{event_name}] 🔄 Cookies обновлены для {uid}")

            return {
                "success": bool(result.get("success")),
                "message": result.get("message", "❓ Нет сообщения"),
                "event": event_name,
            }

        except Exception as e:
            logger.exception(f"[{event_name}] ❌ Ошибка выполнения: {e}")
            return {"success": False, "message": f"❌ Ошибка при выполнении {event_name}: {e}", "event": event_name}

        finally:
            try:
                await page.close()
                await context.close()
            except Exception:
                pass