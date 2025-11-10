# services/puzzle2_auto.py
import os
import asyncio
import warnings
import logging
import json
import tempfile
import shutil
import time
import random
import inspect
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List
from tqdm.asyncio import tqdm_asyncio
from playwright.async_api import async_playwright

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

# === Настройки ===
CONCURRENT = 5  # количество аккаов
REQUEST_TIMEOUT = 40000  #время ожидания загрузки
COOKIE_CAPTURE_WAIT = 3     #Ждёт пока установятся куки
DELAY_BETWEEN_ACCOUNTS = 3   #Пауза (в секундах) между стартом обработки одного аккаунта и переходом к следующему.
DELAY_BETWEEN_LOTTERY = 1.5    #Промежуток между запросами lottery

HEADLESS = True
# Путь к реальному Chrome (если хочешь использовать настоящий Chrome).
# Если оставишь None — Playwright будет использовать свою сборку Chromium.
# Путь к реальному Chrome (Windows)
import platform

if platform.system() == "Windows":
    BROWSER_PATH = r".venv\Chrome\Application\chrome.exe"
else:
    BROWSER_PATH = "/usr/bin/google-chrome-stable"
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
logger = logging.getLogger("puzzle2_auto")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_DIR / "puzzle2_auto.log", encoding="utf-8", mode="w")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
logger.handlers.clear()
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
        await page.mouse.wheel(0, random.randint(100, 300))
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

def calculate_puzzle_totals(file_path: Path):
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
    logger.info("=== 🧩 Итоги по пазлам (только дубликаты) ===")
    for pid, cnt in totals.items():
        logger.info(f"Пазл {pid}: {cnt} шт.")
    logger.info("=========================")
    logger.info(f"Всего дубликатов: {total_sum}")
    logger.info(f"Аккаунтов обработано: {count_accounts}")

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
async def process_account(account: Dict[str, Any], p) -> None:
    uid = account.get("uid")
    mail = account.get("mail", "?")
    cookies = account.get("cookies", {})
    context = page = None
    start_time = time.perf_counter()

    try:
        logger.info("[%s] → старт обработки (mail=%s)", uid, mail)

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

        logger.info("[%s] 🕶 Запуск браузера в фоновом режиме (headless masked)", uid)
        context = await p.chromium.launch_persistent_context(user_data_dir, **launch_kwargs)

        # === Маскировка headless через JS ===
        try:
            patch_script = """
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                window.chrome = { runtime: {} };
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
                Object.defineProperty(navigator, 'languages', { get: () => ['ru-RU', 'ru', 'en-US'] });
                Object.defineProperty(Notification, 'permission', { get: () => 'default' });
                Object.defineProperty(navigator, 'pdfViewerEnabled', { get: () => true });
                const getParameter = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(param) {
                    if (param === 37445) return 'NVIDIA GeForce RTX 3080';
                    if (param === 37446) return 'Google Inc. (NVIDIA)';
                    return getParameter(param);
                };
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
            # Дополнительная маскировка под реальный браузер
            extra_mask_patch = """
            try {
              // маскируем доступ к permissions
              if (navigator.permissions && navigator.permissions.query) {
                const orig = navigator.permissions.query;
                navigator.permissions.query = (param) => (
                  param && param.name === 'notifications'
                    ? Promise.resolve({ state: Notification.permission })
                    : orig(param)
                );
              }
              // маскируем аппаратную информацию
              Object.defineProperty(navigator, 'hardwareConcurrency', { value: 8 });
              Object.defineProperty(navigator, 'maxTouchPoints', { value: 1 });
            } catch (e) {}
            """
            await context.add_init_script(extra_mask_patch)

            logger.info("[%s] 🧩 Headless patch активирован", uid)
        except Exception as e:
            logger.warning("[%s] ⚠️ Ошибка headless patch: %s", uid, e)

        # 4 — Accept-Language через заголовки
        try:
            await context.set_extra_http_headers({"Accept-Language": profile["accept_language"]})
        except Exception:
            pass

        page = await context.new_page()

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
        # --- 🔄 Обновляем cookies после захода на страницу (только для текущего uid) ---
        await asyncio.sleep(jitter(2.5, variance=0.4))
        try:
            fresh_cookies = await context.cookies()
            # преобразуем в {name: value}
            cookie_dict = {c.get("name"): c.get("value") for c in fresh_cookies if "name" in c and "value" in c}

            if cookie_dict:
                # проходим по всем файлам new_data*.json и обновляем только нужный аккаунт
                for file_path in sorted(DATA_DIR.glob("new_data*.json")):
                    try:
                        with open(file_path, "r", encoding="utf-8") as fh:
                            data = json.load(fh)
                    except Exception:
                        # не получилось прочитать — пропускаем файл
                        continue

                    changed = False
                    # data — ожидается список аккаунтов
                    if isinstance(data, list):
                        for entry in data:
                            # entry — словарь вида {"mail": "...", "1100209271": {...}, ...}
                            # ищем ключ равный uid (как строка)
                            if str(uid) in entry and isinstance(entry.get(str(uid)), dict):
                                # заменяем cookies только для этого uid на {name: value}
                                entry[str(uid)] = cookie_dict
                                changed = True
                                break

                    if changed:
                        # безопасно перезаписываем файл (atomic)
                        tmp_fd, tmp_path = tempfile.mkstemp(dir=file_path.parent)
                        try:
                            with os.fdopen(tmp_fd, "w", encoding="utf-8") as tmpf:
                                json.dump(data, tmpf, ensure_ascii=False, indent=2)
                            shutil.move(tmp_path, file_path)
                            logger.info("[%s] 🔄 Cookies обновлены и сохранены в %s", uid, file_path.name)
                        except Exception as e:
                            # попытка убрать временный файл, если что-то пошло не так
                            try:
                                os.remove(tmp_path)
                            except Exception:
                                pass
                            logger.warning("[%s] ⚠️ Не удалось записать обновлённые cookies в %s: %s", uid,
                                           file_path.name, e)
                        # нашли и обновили нужный файл — больше не ищем
                        break
        except Exception as e:
            logger.warning("[%s] ⚠️ Не удалось обновить cookies: %s", uid, e)

        await asyncio.sleep(COOKIE_CAPTURE_WAIT)

        try:
            btn = page.locator("#onetrust-accept-btn-handler")
            if await btn.count() > 0:
                await asyncio.sleep(0.2 + random.random() * 0.3)
                await btn.click(timeout=2000)
        except Exception:
            pass
        # 🧩 Первый запрос lottery — проверяем, есть ли шансы
        try:
            await asyncio.sleep(jitter(DELAY_BETWEEN_LOTTERY, variance=0.5))
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
            logger.info(f"[{uid}] 🎯 Ответ lottery: {status} | {text[:200]}")

            # Разбираем JSON-ответ
            try:
                data = json.loads(text)
                err = data.get("error")
                st = data.get("status")
                # Если сервер вернул ошибку 1 — шансы закончились
                if (err == 1 or err == "1") and st == 0:
                    logger.info(f"[{uid}] 🚫 Шансы закончились — пропускаем дополнительные lottery.")
                # Если ответ успешный — делаем ещё 2 запроса
                elif st == 1:
                    logger.info(f"[{uid}] ✅ Лотерея успешна — выполняем ещё 2 запроса.")
                    for j in range(2):
                        await asyncio.sleep(jitter(DELAY_BETWEEN_LOTTERY, variance=0.5))
                        await page.evaluate(f"""
                            async () => {{
                                await fetch('{base}?action=lottery', {{
                                    method: 'GET',
                                    credentials: 'include'
                                }});
                            }}
                        """)
                        logger.info(f"[{uid}] 🔁 lottery дополнительный {j + 1}/2 выполнен")
                else:
                    logger.info(f"[{uid}] ⚠️ Неизвестный ответ lottery: {text[:150]}")
            except Exception:
                logger.warning(f"[{uid}] ⚠️ Не удалось распарсить ответ lottery, продолжаем.")
        except Exception as e:
            logger.warning(f"[{uid}] ⚠️ Ошибка при первом lottery: {e}")

        # 👀 Имитируем активность пользователя перед запросом
        await humanize_pre_action(page)
        await asyncio.sleep(1.5 + random.random() * 2.0)

        # 🧩 Проверяем куки перед запросом get_resource
        try:
            cookies_before = await context.cookies("https://event-eu-cc.igg.com/")
            if cookies_before:
                cookie_names = [c.get("name") for c in cookies_before]
                logger.info("[%s] 🍪 Найдены cookies перед get_resource: %s", uid, ", ".join(cookie_names))
            else:
                logger.warning("[%s] ⚠️ Куки перед get_resource отсутствуют!", uid)
        except Exception as e:
            logger.warning("[%s] ⚠️ Не удалось получить cookies: %s", uid, e)

        # === get_resource ===
        await asyncio.sleep(jitter(1.5, variance=1.0))  # пауза 0.5–3 сек
        try:
            # 👀 Имитируем активность пользователя
            await humanize_pre_action(page)
            await asyncio.sleep(1.0 + random.random() * 2.0)

            # 🔄 Основной запрос через JS в контексте страницы
            js_code = f"""
                async () => {{
                    const res = await fetch('{base}?action=get_resource', {{
                        method: 'POST',
                        credentials: 'include',
                        headers: {{
                            'X-Requested-With': 'XMLHttpRequest',
                            'Accept': 'application/json, text/javascript, */*; q=0.01',
                            'Referer': 'https://event-eu-cc.igg.com/event/puzzle2/'
                        }}
                    }});
                    return await res.text();
                }}
            """
            text = await page.evaluate(js_code)

            # 💾 сохраняем ответ для отладки
            # raw_path = FAIL_DIR / f"{uid}_get_resource_raw.txt"
            # raw_path.write_text(text, encoding="utf-8")
            # logger.info("[%s] 💾 Ответ get_resource сохранён в %s", uid, raw_path)

            # Проверяем — JSON ли это
            if not text.strip().startswith("{"):
                debug_path = FAIL_DIR / f"{uid}_get_resource_response.html"
                debug_path.write_text(text, encoding="utf-8")
                logger.error("[%s] ⚠️ get_resource: сервер вернул HTML, сохранено в %s", uid, debug_path)
                return

            # ✅ Парсим JSON
            data = json.loads(text)
            logger.info("[%s] ✅ get_resource получен успешно", uid)

            # Извлекаем нужные данные (с защитой от неожиданных структур)
            data_section = data.get("data", {})

            if isinstance(data_section, list) and len(data_section) > 0:
                user = data_section[0].get("user", {})
            elif isinstance(data_section, dict):
                user = data_section.get("user", {})
            else:
                user = {}

            # 🔍 Логируем весь extra_info для отладки
            extra_info = user.get("extra_info", {})
            logger.info("[%s] 🧩 EXTRA_INFO: %s", uid, json.dumps(extra_info, ensure_ascii=False))

            # 🧠 Исправляем тип, если пазлы пришли строкой
            puzzle_data = extra_info.get("puzzle", {})
            if isinstance(puzzle_data, str):
                try:
                    puzzle_data = json.loads(puzzle_data)
                    logger.info("[%s] 🧩 puzzle преобразован из строки в JSON", uid)
                except Exception:
                    logger.warning("[%s] ⚠️ puzzle не удалось преобразовать: %s", uid, puzzle_data)
                    puzzle_data = {}

            entry = {
                "iggid": user.get("iggid"),
                "ec_param": user.get("ec_param"),
                "puzzle": puzzle_data,
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            # Только дубликаты (минус 1 оригинал)
            duplicates = {}
            for pid, count in puzzle_data.items():
                try:
                    count = int(count)
                    if count >= 2:
                        duplicates[pid] = count - 1
                except Exception:
                    continue

            if duplicates:
                entry["puzzle"] = duplicates
                save_puzzle_data(entry, DATA_FILE)
                logger.info("[%s] ✅ Найдены дубликаты пазлов: %s — сохранено", uid, duplicates)
                # 🎨 Красивый вывод пазлов 3×3
                try:
                    import colorama
                    from colorama import Fore, Style
                    colorama.init()

                    grid = [[" " for _ in range(3)] for _ in range(3)]
                    for pid, count in puzzle_data.items():
                        idx = int(pid) - 1
                        row, col = divmod(idx, 3)
                        if count >= 2:
                            # выделяем дубликаты зелёным
                            grid[row][col] = f"{Fore.GREEN}{count}{Style.RESET_ALL}"
                        else:
                            grid[row][col] = str(count)

                    logger.info("[%s] 🧩 Расклад пазлов:", uid)
                    for r in grid:
                        logger.info("[%s]    %s", uid, "  ".join(r))
                except Exception as e:
                    logger.warning("[%s] ⚠️ Ошибка визуализации пазлов: %s", uid, e)

                # 💾 Сразу обновляем общий подсчёт пазлов
                try:
                    calculate_puzzle_totals(DATA_FILE)
                    logger.info("[%s] 🔄 Итоговый файл puzzle_summary.json обновлён", uid)
                except Exception as e:
                    logger.warning("[%s] ⚠️ Не удалось обновить puzzle_summary.json: %s", uid, e)
            else:
                logger.info("[%s] ❌ Дубликатов нет — пропуск", uid)

        except Exception as e:
            # 9 — сохраняем артефакты и при общей ошибке
            try:
                if page:
                    await page.screenshot(path=str(FAIL_DIR / f"{uid}_exception.png"))
                    html = await page.content()
                    (FAIL_DIR / f"{uid}_exception.html").write_text(html, encoding="utf-8")
            except Exception:
                pass
            logger.error("[%s] ❌ Общая ошибка: %s", uid, e)
    finally:
        duration = round(time.perf_counter() - start_time, 2)
        logger.info("[%s] ⏱ Завершено за %s сек.", uid, duration)
        try:
            if page:
                await page.close()
            if context:
                await context.close()
        except Exception:
            pass
        # 🧹 Автоудаление профиля браузера после завершения аккаунта
        try:
            user_data_dir_path = Path(PROFILE_BASE_DIR / f"{uid}")
            if user_data_dir_path.exists():
                shutil.rmtree(user_data_dir_path, ignore_errors=True)
                logger.info("[%s] 🧹 Папка профиля удалена: %s", uid, user_data_dir_path)
        except Exception as e:
            logger.warning("[%s] ⚠️ Не удалось удалить профиль: %s", uid, e)
        await asyncio.sleep(jitter(DELAY_BETWEEN_ACCOUNTS, variance=0.6))
# ---------------- main ----------------
async def main():
    accounts = load_accounts()
    if not accounts:
        logger.error("Аккаунты не найдены в %s", DATA_DIR)
        return
    start_time = time.perf_counter()
    stats = {"total": len(accounts), "success": 0, "fail": 0}
    logger.info("Всего аккаунтов: %d", len(accounts))
    sem = asyncio.Semaphore(CONCURRENT)

    async with async_playwright() as p:

        async def worker(acc):
            async with sem:
                try:
                    await process_account(acc, p)
                    stats["success"] += 1
                except Exception as e:
                    stats["fail"] += 1
                    logger.error(f"[{acc.get('uid')}] ❌ Ошибка: {e}")

        tasks = [asyncio.create_task(worker(acc)) for acc in accounts]
        try:
            await tqdm_asyncio.gather(*tasks, desc="Обработка аккаунтов", total=len(tasks))
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    # ✅ После обработки всех аккаунтов — пересчитываем общие итоги пазлов
    try:
        calculate_puzzle_totals(DATA_FILE)
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


if __name__ == "__main__":
    print("🚀 Запуск puzzle2_auto.py...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Работа прервана пользователем.")
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
