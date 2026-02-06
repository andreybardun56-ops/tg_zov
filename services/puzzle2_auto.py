# services/puzzle2_auto
import asyncio
import inspect
import json
import logging
import os
import random
import shutil
import tempfile
import time
import warnings

from services.browser_patches import BROWSER_PATH


# === Настройка тишины для asyncio и Playwright ===
def silence_asyncio_exceptions(loop, context):
    msg = context.get("message")
    exc = context.get("exception")
    if isinstance(exc, asyncio.CancelledError):
        return  # игнорируем
    if exc and "Target page, context or browser has been closed" in str(exc):
        return  # игнорируем ошибки закрытия браузера
    if msg and "Future exception was never retrieved" in msg:
        return  # игнорируем «Future not retrieved»
    loop.default_exception_handler(context)


try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
loop.set_exception_handler(silence_asyncio_exceptions)

# Глобальный флаг остановки для аккуратного завершения фарма
STOP_EVENT = asyncio.Event()
FARM_RUNNING = False
def is_farm_running() -> bool:
    return FARM_RUNNING
def request_stop() -> None:
    """Помечает, что нужно остановить текущую обработку аккаунтов."""
    STOP_EVENT.set()


def is_stop_requested() -> bool:
    """Возвращает True, если поступил сигнал на остановку."""
    return STOP_EVENT.is_set()


def clear_stop_request() -> None:
    """Сбрасывает флаг остановки (используется при новом запуске)."""
    STOP_EVENT.clear()

# 🔇 Подавляем лишние предупреждения из Playwright и asyncio
warnings.filterwarnings("ignore", category=RuntimeWarning)
logging.getLogger("asyncio").setLevel(logging.ERROR)
logging.getLogger("playwright").setLevel(logging.WARNING)
base = "https://event-eu-cc.igg.com/event/puzzle2/ajax.req.php"
# === Импорт stealth ===
try:
    from playwright_stealth import stealth_async
except Exception:
    try:
        from playwright_stealth import stealth

        stealth_async = stealth
    except Exception:
        stealth_async = None

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from tqdm.asyncio import tqdm_asyncio
from playwright.async_api import async_playwright

# === Папки и файлы ===
DATA_DIR = Path("data/data_akk")
LOG_DIR = Path("data/logs")
SCREEN_DIR = Path("data/screenshots")
DATA_FILE = Path("data/puzzle_data.jsonl")
FAIL_DIR = Path("data/failures")
FARM_STATE_FILE = Path("data/farm_state.json")
# === Настройки ===
CONCURRENT = 4  # количество аккаов
REQUEST_TIMEOUT = 30000  # время ожидания загрузки
COOKIE_CAPTURE_WAIT = 3  # Ждёт пока установятся куки
DELAY_BETWEEN_ACCOUNTS = 3  # Пауза (в секундах) между стартом обработки одного аккаунта и переходом к следующему.
DELAY_BETWEEN_LOTTERY = 1.5  # Промежуток между запросами lottery

# === Настройки батчей ===
BATCH_SIZE = 20  # после этого числа аккаунтов данные будут сохраняться
BATCH_RETRY_SIZE = 100  # батч для повторной обработки 403
# ---------------- глобальные переменные ----------------
puzzle_batch = []  # текущий батч для сохранения
processed_count = 0  # количество обработанных аккаунтов
puzzle_lock = asyncio.Lock()  # для безопасного доступа к батчу в async

accounts_with_attempts: List[Dict[str, Any]] = []
CHECK_EVERY = 100

HEADLESS = True
# Путь к реальному Chrome (если хочешь использовать настоящий Chrome).
# Если оставишь None — Playwright будет использовать свою сборку Chromium.
# пример Windows: r"C:\Program Files\Google\Chrome\Application\chrome.exe"
# Базовая папка для persistent профилей (user_data_dir)
PROFILE_BASE_DIR = Path("data/chrome_profiles")
PROFILE_BASE_DIR.mkdir(parents=True, exist_ok=True)
# 🛠 Попытка автоматически исправить права на папку (Windows)
try:
    import subprocess

    subprocess.run([
        "icacls", str(PROFILE_BASE_DIR),
        "/grant", f"{os.getlogin()}:(OI)(CI)F", "/T"
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
except Exception:
    pass
# Небольшая задержка между действиями (имитация человека)
SLOW_MO = 50  # мс
# === Логирование ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
FAIL_DIR.mkdir(parents=True, exist_ok=True)
# === Логирование с ротацией файлов ===
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

LOG_DIR = Path("data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "puzzle2_auto.log"

logger = logging.getLogger("puzzle2_auto")
logger.setLevel(logging.INFO)
logger.handlers.clear()  # убираем старые handlers

# RotatingFileHandler: макс. размер 2 МБ, хранить до 5 старых файлов
file_handler = RotatingFileHandler(
    LOG_FILE,
    mode='a',
    maxBytes=2 * 1024 * 1024,  # 2 МБ
    backupCount=5,
    encoding='utf-8',
)

formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# ---------------- helpers ----------------
def get_random_browser_profile():
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
        {"width": 360, "height": 640},  # смартфоны
        {"width": 375, "height": 667},
        {"width": 390, "height": 844},
        {"width": 412, "height": 915},
        {"width": 768, "height": 1024},  # планшеты
        {"width": 800, "height": 600},
        {"width": 1024, "height": 768},
    ]

    locales = ["en-US", "en-GB", "ru-RU", "de-DE", "fr-FR"]
    timezones = [
        "Europe/Moscow", "Europe/Prague", "America/New_York",
        "Asia/Shanghai", "Asia/Tokyo", "Europe/London"
    ]
    accept_languages = [
        "en-US,en;q=0.9",
        "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "de-DE,de;q=0.9,en;q=0.8",
        "fr-FR,fr;q=0.9,en;q=0.8"
    ]

    ua = random.choice(UAS)
    vp = random.choice(viewports)
    locale = random.choice(locales)
    timezone = random.choice(timezones)
    accept_language = random.choice(accept_languages)

    is_mobile = ("Mobile" in ua) or ("iPhone" in ua) or ("Android" in ua)

    device_scale_factor = random.choice([1, 1, 1.5, 2])  # чаще 1
    hardware_concurrency = random.choice([2, 4, 6, 8])

    return {
        "user_agent": ua,
        "viewport": vp,
        "locale": locale,
        "timezone": timezone,
        "accept_language": accept_language,
        "is_mobile": is_mobile,
        "device_scale_factor": device_scale_factor,
        "hardware_concurrency": hardware_concurrency,
    }


async def humanize_pre_action(page):
    """Небольшая имитация человека перед важными действиями."""
    try:
        await page.mouse.move(100 + random.randint(-20, 20), 100 + random.randint(-15, 15), steps=random.randint(6, 12))
        await asyncio.sleep(0.15 + random.random() * 0.35)
        await page.mouse.move(200 + random.randint(-30, 30), 140 + random.randint(-20, 20), steps=random.randint(5, 9))
        await asyncio.sleep(0.05 + random.random() * 0.2)
        # лёгкий скролл как будто проглядели страницу
        await page.mouse.wheel(0, random.randint(100, 300))  #
        await asyncio.sleep(0.1 + random.random() * 0.25)
    except Exception:
        pass


def load_accounts() -> List[Dict[str, Any]]:
    """Считать все new_data*.json и вернуть список аккаунтов с uid+cookies+mail"""
    out = []
    if not DATA_DIR.exists():
        logger.error("Папка не найдена: %s", DATA_DIR)
        return out
    for f in sorted(DATA_DIR.glob("new_data*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list):
                for entry in data:
                    mail = entry.get("mail")
                    for k, v in entry.items():
                        if k.isdigit() and isinstance(v, dict):
                            out.append({"file": f.name, "mail": mail, "uid": k, "cookies": v})
        except Exception as e:
            logger.warning("Не удалось прочитать %s: %s", f.name, e)
    return out
def save_farm_state(index: int):
    FARM_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FARM_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "current_index": index,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, f, ensure_ascii=False, indent=2)


def load_farm_state() -> int:
    if not FARM_STATE_FILE.exists():
        return 0
    try:
        with open(FARM_STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return int(data.get("current_index", 0))
    except Exception:
        return 0

def reset_farm_state() -> None:
    """Сбрасывает сохранённое состояние фарма."""
    try:
        if FARM_STATE_FILE.exists():
            FARM_STATE_FILE.unlink()
    except Exception:
        pass


def cookies_to_playwright(cookies: Dict[str, str], domain: str = ".event-eu-cc.igg.com") -> List[Dict[str, Any]]:
    """Преобразует {name: value} в формат Playwright cookie"""
    return [{"name": str(k), "value": str(v), "domain": domain, "path": "/"} for k, v in cookies.items()]


def save_puzzle_data(entry: dict, file_path: Path):
    """Сохраняет или обновляет данные аккаунта в многострочном JSON-формате"""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    entry["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    existing = []
    updated = False

    if file_path.exists():
        with open(file_path, "r", encoding="utf-8") as f:
            buffer = ""
            for line in f:
                if line.strip():
                    buffer += line
                else:
                    try:
                        data = json.loads(buffer)
                        if data.get("iggid") == entry.get("iggid"):
                            existing.append(entry)
                            updated = True
                        else:
                            existing.append(data)
                    except Exception:
                        pass
                    buffer = ""
            if buffer.strip():
                try:
                    data = json.loads(buffer)
                    if data.get("iggid") == entry.get("iggid"):
                        existing.append(entry)
                        updated = True
                    else:
                        existing.append(data)
                except Exception:
                    pass

    if not updated:
        existing.append(entry)

    temp_fd, temp_path = tempfile.mkstemp(dir=file_path.parent)
    with open(temp_fd, "w", encoding="utf-8") as tmp:
        for obj in existing:
            json.dump(obj, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n\n")
    shutil.move(temp_path, file_path)


def jitter(base: float, variance: float = 0.5):
    """
    Возвращает случайную задержку: base ± (0..variance*base)
    Пример: jitter(3, 0.5) -> число в ~[1.5, 4.5]
    """
    # можно использовать random.uniform для равномерного разброса
    delta = random.uniform(-variance * base, variance * base)
    return max(0.1, base + delta)


def calculate_puzzle_totals(file_path: Path, accounts_processed: int = None):
    """Считает общее количество каждого пазла (1–9) по всем аккаунтам (только дубликаты)."""
    totals = {str(i): 0 for i in range(1, 10)}
    count_accounts = 0

    if not file_path.exists():
        logger.warning("Файл %s не найден для подсчёта пазлов", file_path)
        return totals

    with open(file_path, "r", encoding="utf-8") as f:
        buffer = ""
        for line in f:
            if line.strip():
                buffer += line
            else:
                if buffer.strip():
                    try:
                        data = json.loads(buffer)
                        puzzle_data = data.get("puzzle", {})
                        for pid, count in puzzle_data.items():
                            if pid in totals:
                                totals[pid] += int(count)
                        count_accounts += 1
                    except Exception:
                        pass
                    buffer = ""
        if buffer.strip():
            try:
                data = json.loads(buffer)
                puzzle_data = data.get("puzzle", {})
                for pid, count in puzzle_data.items():
                    if pid in totals:
                        totals[pid] += int(count)
                count_accounts += 1
            except Exception:
                pass

    total_sum = sum(totals.values())
    # logger.info("=== 🧩 Итоги по пазлам (только дубликаты) ===")
    # for pid, cnt in totals.items():
    # logger.info(f"Пазл {pid}: {cnt} шт.")
    # logger.info("=========================")
    # logger.info(f"Всего дубликатов: {total_sum}")
    # if accounts_processed is not None:
    # logger.info(f"🔢 Аккаунтов обработано: {accounts_processed}")
    # else:
    # logger.info(f"🔢 Аккаунтов с дубликатами: {count_accounts}")

    summary_path = Path("data/puzzle_summary.json")
    with open(summary_path, "w", encoding="utf-8") as out:
        json.dump({
            "totals": totals,
            "accounts": count_accounts,
            "all_duplicates": total_sum,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }, out, ensure_ascii=False, indent=2)

    return totals


# ---------------- per-account workflow ----------------
def is_403_response(status: int, text: str) -> bool:
    if status == 403:
        return True
    if not text:
        return False
    return "403 FORBIDDEN" in text.upper()


async def process_account(account: Dict[str, Any], p) -> bool:
    uid = account.get("uid")
    mail = account.get("mail", "?")
    cookies = account.get("cookies", {})
    context = page = None
    start_time = time.perf_counter()

    try:
        # logger.info("[%s] → старт обработки (mail=%s)", uid, mail)

        # 1 / 3 / 4 — профиль + параметры при создании контекста
        profile = get_random_browser_profile()
        ua = profile["user_agent"]
        vp = profile["viewport"]
        locale = profile["locale"]

        PROFILE_BASE_DIR.mkdir(parents=True, exist_ok=True)
        user_data_dir = str(PROFILE_BASE_DIR / f"{uid}")

        # === скрытый фоновый режим (headless masked) ===
        window_args = [
            "--headless=new",  # фон без окна
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

        launch_kwargs = dict(
            headless=True,  # полностью фоновый режим
            slow_mo=SLOW_MO,
            viewport=vp,
            user_agent=ua,
            locale=locale,
            timezone_id=profile["timezone"],
            is_mobile=profile["is_mobile"],
            device_scale_factor=profile["device_scale_factor"],
            java_script_enabled=True,
            args=window_args,
        )

        if BROWSER_PATH:
            launch_kwargs["executable_path"] = BROWSER_PATH

        # logger.info("[%s] 🕶 Запуск браузера в фоновом режиме (headless masked)", uid)
        context = await p.chromium.launch_persistent_context(user_data_dir, **launch_kwargs)

        # === Маскировка headless через JS ===
        try:
            patch_script = """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru', 'en-US'] });
                Object.defineProperty(Notification, 'permission', { get: () => 'default' });
            """
            # Имитация navigator.connection и deviceMemory (Akamai check)
            connection_patch = """
            Object.defineProperty(navigator, 'connection', {
              value: { rtt: 50, downlink: 10, effectiveType: '4g', saveData: false },
              configurable: true
            });
            Object.defineProperty(navigator, 'deviceMemory', { value: 8 });
            """
            await context.add_init_script(connection_patch)

            await context.add_init_script(patch_script)

            # logger.info("[%s] 🧩 Headless patch активирован", uid)
        except Exception as e:
            logger.warning("[%s] ⚠️ Ошибка headless patch: %s", uid, e)

        # 4 — Accept-Language через заголовки
        try:
            await context.set_extra_http_headers({"Accept-Language": profile["accept_language"]})
        except Exception:
            pass

        page = await context.new_page()

        # === 🔥 Блокировка лишних ресурсов для ускорения ===
        async def block_resources(page):
            async def handler(route):
                if route.request.resource_type in ["image", "media", "font"]:
                    await route.abort()
                else:
                    await route.continue_()

            await page.route("**/*", handler)

        await block_resources(page)

        # Если есть cookies из файла аккаунтов — добавим (это НЕ пункт 10: мы не грузим внешние экспорты)
        try:
            if cookies:
                await context.add_cookies(cookies_to_playwright(cookies))
        except Exception as e:
            logger.warning("[%s] Не удалось добавить cookies: %s", uid, e)

        # 6 — Аккуратный init_script (webdriver/languages/plugins/hardwareConcurrency/permissions)
        try:
            lang_list = ["en-US", "en"]
            if locale.startswith("ru"):
                lang_list = ["ru-RU", "ru", "en-US", "en"]
            elif locale.startswith("de"):
                lang_list = ["de-DE", "de", "en-US", "en"]
            elif locale.startswith("fr"):
                lang_list = ["fr-FR", "fr", "en-US", "en"]

            init_script = f"""
            (() => {{
                try {{
                    Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined, configurable: true }});
                    Object.defineProperty(navigator, 'languages', {{ get: () => {json.dumps(lang_list)}, configurable: true }});
                    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {profile['hardware_concurrency']}, configurable: true }});
                    // простая имитация plugins/mimeTypes — без переусердствования
                    const pluginArray = [1,2,3];
                    Object.defineProperty(navigator, 'plugins', {{ get: () => pluginArray, configurable: true }});
                    Object.defineProperty(navigator, 'mimeTypes', {{ get: () => pluginArray, configurable: true }});
                    // permissions patch (частый чек)
                    if (navigator.permissions && navigator.permissions.query) {{
                        const orig = navigator.permissions.query.bind(navigator.permissions);
                        navigator.permissions.query = (params) => {{
                            if (params && params.name === 'notifications') {{
                                return Promise.resolve({{ state: Notification.permission }});
                            }}
                            return orig(params);
                        }};
                    }}
                }} catch (e) {{}}
            }})();
            """
            await context.add_init_script(init_script)
        except Exception as e:
            logger.warning("[%s] add_init_script error: %s", uid, e)

        # 7 — stealth: пробуем async и sync
        if stealth_async is not None:
            try:
                # если stealth_async — асинхронная функция (defined with `async def`)
                if inspect.iscoroutinefunction(stealth_async):
                    await stealth_async(page)
                else:
                    # вызывает функцию (sync) — возможно она вернёт awaitable (корутину)
                    result = stealth_async(page)
                    # если результат awaitable — await'им его
                    if inspect.isawaitable(result):
                        await result
            except Exception as e:
                logger.warning("[%s] stealth failed: %s", uid if 'uid' in locals() else "?", e)

        # 8 — лёгкая имитация человека
        await humanize_pre_action(page)

        # Переход на страницу и cookie banner
        await page.goto("https://event-eu-cc.igg.com/event/puzzle2/", wait_until="networkidle", timeout=REQUEST_TIMEOUT)

        await asyncio.sleep(COOKIE_CAPTURE_WAIT)

        try:
            btn = page.locator("#onetrust-accept-btn-handler")
            if await btn.count() > 0:
                await asyncio.sleep(0.2 + random.random() * 0.3)
                await btn.click(timeout=2000)
        except Exception:
            pass
        await humanize_pre_action(page)
        # 🧩 Первый запрос lottery — проверяем, есть ли шансы
        try:
            await asyncio.sleep(jitter(DELAY_BETWEEN_LOTTERY, variance=0.9))
            js = f"""
                async () => {{
                    const res = await fetch('{base}?action=lottery', {{
                        method: 'GET',
                        credentials: 'include',
                        headers: {{
                            'X-Requested-With': 'XMLHttpRequest',
                            'Referer': 'https://event-eu-cc.igg.com/event/puzzle2/'
                        }}
                    }});
                    const txt = await res.text();
                    return {{status: res.status, text: txt}};
                }}
            """
            resp = await page.evaluate(js)
            text = resp.get("text", "")
            status = resp.get("status", 0)
            logger.info(f"[{uid}] 🎯 Ответ lottery (1-й запрос): {status} | {text[:200]}")
            if is_403_response(status, text):
                logger.warning(f"[{uid}] 🚫 Получен 403 на lottery, добавляем в повтор.")
                return True

            # Разбираем JSON-ответ
            try:
                data = json.loads(text)
                err = data.get("error")
                st = data.get("status")

                try:
                    # Если сервер вернул ошибку 1 — шансы закончились
                    if (err == 1 or err == "1") and st == 0:
                        logger.info(f"[{uid}] 🚫 Шансы закончились — пропускаем дополнительные lottery.")
                    elif st == 1:
                        # выполняем ещё 2 запроса
                        for j in range(2):
                            await asyncio.sleep(jitter(DELAY_BETWEEN_LOTTERY, variance=0.9))
                            resp = await page.evaluate(f"""
                                async () => {{
                                    const res = await fetch('{base}?action=lottery', {{
                                        method: 'GET',
                                        credentials: 'include'
                                    }});
                                    const txt = await res.text();
                                    return {{status: res.status, text: txt}};
                                }}
                            """)
                            text = resp.get("text", "")
                            status = resp.get("status", 0)
                            logger.info(
                                f"[{uid}] 🎯 Ответ lottery ({j + 2}-й запрос): {status} | {text[:200]}")  # j=0 → 2-й, j=1 → 3-й
                            if is_403_response(status, text):
                                logger.warning(f"[{uid}] 🚫 Получен 403 на lottery, добавляем в повтор.")
                                return True

                except Exception as e:
                    logger.warning(f"[{uid}] ⚠️ Ошибка при разборе lottery, пропускаем: {e}")
            except Exception:
                pass
        except Exception as e:
            logger.warning(f"[{uid}] ⚠️ Ошибка lottery-запроса: {e}")

            # Имитируем активность пользователя перед следующими действиями
    #            await humanize_pre_action(page)
    #            await asyncio.sleep(1.5 + random.random() * 2.0)

    # Проверяем куки перед get_resource
    #
    #                cookies_before = await context.cookies("https://event-eu-cc.igg.com/")
    #                cookie_names = [c.get("name") for c in cookies_before] if cookies_before else []
    #            except Exception:
    #                pass

    #            # === get_resource ===
    #            await asyncio.sleep(jitter(1.5, variance=1.0))  # пауза 0.5–3 сек
    #            await humanize_pre_action(page)
    #            await asyncio.sleep(1.0 + random.random() * 2.0)

    #            # 🔄 Основной запрос через JS в контексте страницы
    #            js_code = f"""
    #                async () => {{
    #                    const res = await fetch('{base}?action=get_resource', {{
    #                        method: 'POST',
    #                       credentials: 'include',
    #                       headers: {{
    #                           'X-Requested-With': 'XMLHttpRequest',
    #                           'Accept': 'application/json, text/javascript, */*; q=0.01',
    #                           'Referer': 'https://event-eu-cc.igg.com/event/puzzle2/'
    #                        }}
    #                    }});
    #                    return await res.text();
    #                }}
    #            """
    #            async def fetch_get_resource():
    #                return await page.evaluate(js_code)

    #            text = await fetch_get_resource()

    # 💾 сохраняем ответ для отладки
    # raw_path = FAIL_DIR / f"{uid}_get_resource_raw.txt"
    # raw_path.write_text(text, encoding="utf-8")
    # logger.info("[%s] 💾 Ответ get_resource сохранён в %s", uid, raw_path)

    # Проверяем — JSON ли это
    #            if not text.strip().startswith("{"):
    # debug_path = FAIL_DIR / f"{uid}_get_resource_response.html"
    # debug_path.write_text(text, encoding="utf-8")
    #                logger.error("[%s] ⚠️ get_resource: сервер вернул HTML, сохранено в %s", uid,)
    #            else:
    #                # ✅ Парсим JSON
    #                data = json.loads(text)
    #                data_section = data.get("data", {})

    #                if isinstance(data_section, list) and data_section:
    #                    user = data_section[0].get("user", {})
    #                elif isinstance(data_section, dict):
    #                    user = data_section.get("user", {})
    #                else:
    #                    user = {}

    # ec_free
    #                ec_free = user.get("ec_free", "0")
    #                try:
    #                    ec_free_int = int(ec_free)
    #                except Exception:
    #                    ec_free_int = 0

    #                if ec_free_int > 0:
    #                    accounts_with_attempts.append(account)
    #
    #                # puzzle
    #                puzzle_data = {}
    #                extra_info = user.get("extra_info")
    #                if isinstance(extra_info, dict):
    #                    puzzle_data = extra_info.get("puzzle", {})
    #                else:
    #                    ec_extra = user.get("ec_extra_info", "{}")
    #                    try:
    #                        ec_extra_json = json.loads(ec_extra)
    #                        puzzle_data = ec_extra_json.get("puzzle", {})
    #                    except Exception:
    #                        puzzle_data = {}

    #               puzzle_data = {str(k): v for k, v in puzzle_data.items()}

    #                # ✅ ЭТОТ ЛОГ ТЕПЕРЬ БУДЕТ ПИСАТЬСЯ В ФАЙЛ
    #                logger.info("[%s] 🧩 Попытки: %s | Пазлы: %s", uid, ec_free, puzzle_data)

    # --- формируем entry для батча ---
    #               entry = None
    #                if user and puzzle_data:
    #                    entry = {
    #                        "iggid": user.get("iggid"),
    #                        "ec_param": user.get("ec_param"),
    #                        "puzzle": puzzle_data,
    #                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #                   }

    #                if entry:
    #                    global puzzle_batch, processed_count
    #                    async with puzzle_lock:
    #                        puzzle_batch.append(entry)
    #                        processed_count += 1
    #
    #                        # --- батчинг: каждые BATCH_SIZE аккаунтов ---
    #                        if processed_count % BATCH_SIZE == 0:
    #                            logger.info(f"💾 Пройдено {processed_count} аккаунтов — сохраняем batch")
    #                            for e in puzzle_batch:
    #                                duplicates = {}
    #                                for pid, count in e["puzzle"].items():
    #                                    try:
    #                                        if int(count) >= 2:
    #                                            duplicates[pid] = int(count) - 1
    #                                    except Exception:
    #                                        continue
    #                                if duplicates:
    #                                    e_to_save = e.copy()
    #                                    e_to_save["puzzle"] = duplicates
    #                                    save_puzzle_data(e_to_save, DATA_FILE)
    #                            puzzle_batch.clear()  # очищаем батч после сохранения

    #                    try:
    #                        calculate_puzzle_totals(DATA_FILE, accounts_processed=processed_count)
    #                    except Exception as e:
    #                        logger.warning(f"⚠️ Не удалось обновить puzzle_summary.json: {e}")

    #                    puzzle_batch.clear()

    #        except Exception as e:
    # 9 — сохраняем артефакты и при общей ошибке
    #            try:
    #                if page:
    #                    await page.screenshot(path=str(FAIL_DIR / f"{uid}_exception.png"))
    #                    html = await page.content()
    #                    (FAIL_DIR / f"{uid}_exception.html").write_text(html, encoding="utf-8")
    #            except Exception:
    #                pass
    #            logger.error("[%s] ❌ Общая ошибка: %s", uid, e)
    finally:
        # duration = round(time.perf_counter() - start_time, 2)
        # logger.info("[%s] ⏱ Завершено за %s сек.", uid, duration)

        # 🔒 Корректно закрываем page и context
        try:
            if page:
                await page.close()
            if context:
                await context.close()
        except Exception:
            pass

        # 🧹 Автоудаление профиля браузера после завершения аккаунта
        try:
            user_data_dir_path = PROFILE_BASE_DIR / f"{uid}"
            if user_data_dir_path.exists():
                shutil.rmtree(user_data_dir_path, ignore_errors=True)
                # logger.info("[%s] 🧹 Папка профиля удалена: %s", uid, user_data_dir_path)
        except Exception as e:
            logger.warning("[%s] ⚠️ Не удалось удалить профиль: %s", uid, e)

        await asyncio.sleep(jitter(DELAY_BETWEEN_ACCOUNTS, variance=0.6))

    return False


# ---------------- main ----------------
async def main():
    global FARM_RUNNING
    clear_stop_request()
    FARM_RUNNING = True
    try:
        accounts = load_accounts()
        start_index = load_farm_state()

        if start_index >= len(accounts):
            start_index = 0

        accounts = accounts[start_index:]

        if not accounts:
            logger.error("Аккаунты не найдены в %s", DATA_DIR)
            logger.info("▶️ Старт фарма с аккаунта #%d", start_index)

            return

        start_time = time.perf_counter()
        stats = {"total": len(accounts), "success": 0, "fail": 0}
        processed_total = 0
        logger.info("Всего аккаунтов: %d", len(accounts))
        sem = asyncio.Semaphore(CONCURRENT)

        async with async_playwright() as p:

            async def run_batch(batch_accounts, allow_retry: bool, count_for_state: bool):
                nonlocal processed_total
                retry_accounts = []

                async def worker(acc):
                    uid = acc.get("uid")
                    if STOP_EVENT.is_set():
                        logger.info("[%s] ⏹ Остановка. Сохраняем позицию %d", uid, start_index + processed_total)
                        save_farm_state(start_index + processed_total)
                        return
                    async with sem:
                        if STOP_EVENT.is_set():
                            logger.info("[%s] ⏹ Завершаем перед стартом", uid)
                            return

                        try:
                            needs_retry = await process_account(acc, p)
                            if needs_retry:
                                if allow_retry:
                                    retry_accounts.append(acc)
                                else:
                                    stats["fail"] += 1
                            else:
                                stats["success"] += 1
                        except Exception as e:
                            stats["fail"] += 1
                            logger.error(f"[{uid}] ❌ Ошибка: {e}")
                        finally:
                            if count_for_state:
                                processed_total += 1
                                save_farm_state(start_index + processed_total)

                tasks = [asyncio.create_task(worker(acc)) for acc in batch_accounts]

                try:
                    await tqdm_asyncio.gather(*tasks, desc="Обработка аккаунтов", total=len(tasks))
                except asyncio.CancelledError:
                    for t in tasks:
                        t.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    raise

                return retry_accounts

            for batch_start in range(0, len(accounts), BATCH_RETRY_SIZE):
                if STOP_EVENT.is_set():
                    break
                batch = accounts[batch_start:batch_start + BATCH_RETRY_SIZE]
                retry_accounts = await run_batch(batch, allow_retry=True, count_for_state=True)
                if retry_accounts and not STOP_EVENT.is_set():
                    await run_batch(retry_accounts, allow_retry=False, count_for_state=False)

        # сохраняем остатки батча
        async with puzzle_lock:
            if puzzle_batch:
                logger.info(f"💾 Сохраняем остаток данных: {len(puzzle_batch)} аккаунтов")
                for e in puzzle_batch:
                    save_puzzle_data(e, DATA_FILE)
                puzzle_batch.clear()

        # итоговый подсчёт
        try:
            calculate_puzzle_totals(DATA_FILE, accounts_processed=stats["success"])
            logger.info("🧮 Итоговый подсчёт пазлов выполнен успешно")
        except Exception as e:
            logger.warning("⚠️ Не удалось пересчитать итоговые пазлы: %s", e)

        total_time = round(time.perf_counter() - start_time, 2)
        logger.info("=== ✅ Итог ===")
        logger.info(f"Всего аккаунтов: {stats['total']}")
        logger.info(f"Успешно: {stats['success']}")
        logger.info(f"Ошибок: {stats['fail']}")
        logger.info(f"Время выполнения: {total_time} сек.")
        logger.info("Все аккаунты обработаны.")
    finally:
        FARM_RUNNING = False
if __name__ == "__main__":
    print("🚀 Запуск puzzle2_auto.py...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Работа прервана пользователем.")
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
