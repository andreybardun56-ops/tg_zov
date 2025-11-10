import asyncio
import json
import os
import time
import base64
import itertools
import sys
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable
from colorama import init
from playwright.async_api import async_playwright, Error as PWError

init(autoreset=True)

# === Настройки ===
DATA_DIR = Path("data/data_akk")
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "login_refresh.log"

CONCURRENT = 5 #Сколько аккаунтов обрабатывается одновременно
DELAY_AFTER_SUCCESS = 1 #Задержка (в секундах) после успешного логина
NAV_TIMEOUT = 30_000 #Таймаут загрузки страницы
WAIT_AFTER_LOGIN = 5 #Время ожидания после нажатия кнопки «Войти»
SLOW_MO = 0.5 #Искусственная задержка Playwright между действиями

# === Логирование ===
logger = logging.getLogger("login_refresh")
logger.setLevel(logging.INFO)
for h in logger.handlers[:]:
    logger.removeHandler(h)

file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logging.basicConfig(level=logging.INFO, handlers=[file_handler])

file_locks: Dict[str, asyncio.Lock] = {}

# === Глобальное управление остановкой массового обновления ===
STOP_EVENT = asyncio.Event()


def request_stop() -> None:
    """Запрашивает остановку текущего массового обновления."""
    STOP_EVENT.set()


def clear_stop_request() -> None:
    """Сбрасывает запрос на остановку перед новым запуском."""
    STOP_EVENT.clear()


def is_stop_requested() -> bool:
    """Проверяет, был ли запрошен останов массового обновления."""
    return STOP_EVENT.is_set()

# === JSON helpers ===
def atomic_write_json(path: Path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(str(tmp), str(path))

def load_json_safe(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 🔧 фиксируем старые UID-ключи, если они были числовыми
        if isinstance(data, list):
            for acc in data:
                if isinstance(acc, dict):
                    for k in list(acc.keys()):
                        if isinstance(k, int):
                            acc[str(k)] = acc.pop(k)
        return data
    except Exception:
        return None

def extract_accounts(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if "accounts" in data and isinstance(data["accounts"], list):
            return data["accounts"]
        accs = []
        for v in data.values():
            if isinstance(v, list):
                accs.extend(v)
        return accs
    return []

# === JWT / cookies helpers ===
def jwt_get_uid(token: str) -> Optional[str]:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        obj = json.loads(decoded.decode("utf-8"))
        for k in ("sub", "uid", "userId", "user_id", "id", "jti"):
            if k in obj and obj[k]:
                return str(obj[k])
    except Exception:
        return None
    return None

def cookies_list_to_flat_dict(cookies_list: List[Dict[str, Any]]) -> Dict[str, str]:
    out = {}
    for c in cookies_list:
        n = c.get("name")
        v = c.get("value")
        if n:
            out[n] = v
    return out

# ⚙️ Обновление куки прямо в new_data*.json
async def update_account_in_newdata(file_path: Path, original_acc: Dict[str, Any], uid: str, new_cookies: Dict[str, str]) -> bool:
    """
    Ищет в файле запись аккаунта (по mail+paswd или по UID-ключу) и обновляет
    значение по ключу UID на новый словарь cookies.
    Формат записи в new_dataX.json: {"mail": "...", "paswd": "...", "<UID>": { ...cookies... } }
    """
    key = str(file_path.resolve())
    lock = file_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        file_locks[key] = lock

    async with lock:
        data = load_json_safe(file_path)
        if data is None:
            logger.warning(f"[UPDATE] Не удалось прочитать {file_path.name}")
            return False

        changed = False
        if isinstance(data, list):
            for entry in data:
                if not isinstance(entry, dict):
                    continue

                has_uid_key = entry.get(uid) is not None
                same_creds = (
                    entry.get("mail") == original_acc.get("mail") and
                    entry.get("paswd") == original_acc.get("paswd")
                )

                if has_uid_key or same_creds:
                    entry[uid] = new_cookies
                    changed = True
                    break

        elif isinstance(data, dict) and "accounts" in data and isinstance(data["accounts"], list):
            for entry in data["accounts"]:
                if not isinstance(entry, dict):
                    continue
                has_uid_key = entry.get(uid) is not None
                same_creds = (
                    entry.get("mail") == original_acc.get("mail") and
                    entry.get("paswd") == original_acc.get("paswd")
                )
                if has_uid_key or same_creds:
                    entry[uid] = new_cookies
                    changed = True
                    break

        else:
            # возможные нетиповые структуры: попробуем найти список
            found_list = None
            if isinstance(data, dict):
                for v in data.values():
                    if isinstance(v, list):
                        found_list = v
                        break
            if isinstance(found_list, list):
                for entry in found_list:
                    if not isinstance(entry, dict):
                        continue
                    has_uid_key = entry.get(uid) is not None
                    same_creds = (
                        entry.get("mail") == original_acc.get("mail") and
                        entry.get("paswd") == original_acc.get("paswd")
                    )
                    if has_uid_key or same_creds:
                        entry[uid] = new_cookies
                        changed = True
                        break

        if not changed:
            logger.warning(f"[UPDATE] В {file_path.name} не нашли запись для обновления (uid={uid}, mail={original_acc.get('mail')})")
            return False

        try:
            atomic_write_json(file_path, data)
            logger.info(f"[UPDATE] 🔄 Cookies обновлены в {file_path.name} для UID={uid}")
            return True
        except Exception as e:
            logger.exception(f"[UPDATE] Ошибка перезаписи {file_path.name}: {e}")
            return False

# === Логин одного аккаунта ===
async def process_single_account(playwright, sem, file_path: Path, account: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if is_stop_requested():
        return None

    async with sem:
        if is_stop_requested():
            return None
        mail = account.get("mail") or account.get("email") or account.get("user")
        passwd = account.get("paswd") or account.get("password") or account.get("pass")

        if not mail or not passwd:
            logger.info(f"[SKIP] в аккаунте нет mail/paswd, пропускаем: {file_path.name} / {mail}")
            return None

        browser = None
        context = None
        try:
            logger.info(f"[START] {file_path.name} → {mail}")
            if is_stop_requested():
                return None

            browser = await playwright.chromium.launch(headless=True, slow_mo=SLOW_MO)
            context = await browser.new_context()
            page = await context.new_page()

            login_url = "https://passport.igg.com/login"
            await page.goto(login_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

            # --- Accept Cookies ---
            try:
                if await page.locator("#onetrust-accept-btn-handler").count() > 0:
                    await page.locator("#onetrust-accept-btn-handler").click(timeout=3000)
                elif await page.locator("text=Accept All").count() > 0:
                    await page.locator("text=Accept All").click(timeout=3000)
                elif await page.locator("text=Принять все").count() > 0:
                    await page.locator("text=Принять все").click(timeout=3000)
            except Exception:
                pass

            # --- Ввод email ---
            filled_email = False
            for sel in [
                'input[name="email"]', 'input[type="email"]', 'input#email',
                'input[placeholder*="Email"]', 'input[placeholder*="E-mail"]',
                'input[placeholder*="邮箱"]', 'input[autocomplete="email"]'
            ]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.fill(str(mail), timeout=4000)
                        filled_email = True
                        break
                except Exception:
                    continue

            if not filled_email:
                try:
                    first_inp = await page.query_selector("input")
                    if first_inp:
                        await first_inp.fill(str(mail))
                except Exception:
                    pass

            # --- Ввод пароля ---
            filled_pass = False
            for sel in [
                'input[name="password"]', 'input[type="password"]', 'input#password',
                'input[placeholder*="Password"]', 'input[autocomplete="current-password"]'
            ]:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.fill(str(passwd), timeout=4000)
                        filled_pass = True
                        break
                except Exception:
                    continue

            # --- Клик по кнопке ---
            clicked = False
            for sel in [
                'button[type="submit"]', 'button:has-text("Sign In")', 'button:has-text("Log In")',
                'button:has-text("登录")', 'button:has-text("Sign in")', 'button:has-text("Log in")',
                'input[type="submit"]'
            ]:
                try:
                    btn = await page.query_selector(sel)
                    if btn:
                        await btn.click()
                        clicked = True
                        break
                except Exception:
                    continue

            if not clicked:
                try:
                    await page.keyboard.press("Enter")
                except Exception:
                    pass

            await page.wait_for_timeout(WAIT_AFTER_LOGIN * 1000)

            # --- Повторное принятие cookie баннера ---
            try:
                if await page.locator("text=Accept All").count() > 0:
                    await page.locator("text=Accept All").click(timeout=3000)
                elif await page.locator("text=Принять все").count() > 0:
                    await page.locator("text=Принять все").click(timeout=3000)
            except Exception:
                pass

            # --- Сбор cookies ---
            cookie_capture_timeout = 10.0
            poll_interval = 0.5
            cookies_flat = {}
            token_value = None

            try:
                await page.wait_for_load_state("networkidle", timeout=5000)
            except Exception:
                pass

            start_time = time.time()
            while time.time() - start_time < cookie_capture_timeout:
                if is_stop_requested():
                    return None
                cookies = await context.cookies()
                cookies_flat = cookies_list_to_flat_dict(cookies)
                token_value = cookies_flat.get("gpc_sso_token")
                if token_value and "PHPSESSID" in cookies_flat and "RT" in cookies_flat:
                    break
                await asyncio.sleep(poll_interval)

            uid = jwt_get_uid(token_value) if token_value else None

            # --- Fallback если токен не найден ---
            if not uid:
                try:
                    await page.goto("https://castleclash.igg.com", timeout=15000)
                    await asyncio.sleep(2)
                    for _ in range(5):
                        if is_stop_requested():
                            return None
                        cookies = await context.cookies()
                        cookies_flat = cookies_list_to_flat_dict(cookies)
                        token_value = cookies_flat.get("gpc_sso_token")
                        if token_value:
                            uid = jwt_get_uid(token_value)
                            break
                        await asyncio.sleep(0.5)
                except Exception:
                    pass

            if not uid:
                logger.info(f"[FAIL] {mail} — не найден uid / token, пропускаем.")
                return None

            # ✅ Обновляем куки прямо в исходном new_dataX.json
            ok = await update_account_in_newdata(file_path, account, str(uid), cookies_flat)
            if ok:
                logger.info(f"[OK] {mail} uid={uid} — куки обновлены в {file_path.name}")
            await asyncio.sleep(DELAY_AFTER_SUCCESS)
            return {"uid": str(uid), "cookies": cookies_flat}

        except asyncio.CancelledError:
            logger.info(f"[CANCEL] Остановка обработки {mail} из-за отмены задачи")
            raise
        except PWError as e:
            logger.exception(f"[ERROR] {mail} — playwright error: {e}")
            return None
        except Exception as e:
            logger.exception(f"[ERROR] {mail} — {e}")
            return None
        finally:
            try:
                if context:
                    await context.close()
                if browser:
                    await browser.close()
            except Exception:
                pass

async def process_all_files(progress_callback: Optional[Callable[[float, int, int], None]] = None):
    if not DATA_DIR.exists():
        logger.error(f"Папка не найдена: {DATA_DIR}")
        return None

    # ▶️ теперь берём только new_data*.json
    files = [p for p in DATA_DIR.iterdir() if p.suffix == ".json" and p.name.startswith("new_data")]
    if not files:
        logger.error("Нет JSON-файлов вида new_data*.json для обработки.")
        return None

    if STOP_EVENT.is_set():
        logger.info("[STOP] Обновление cookies остановлено до начала обработки")
        return None

    # === собираем все аккаунты ===
    total_accounts = 0
    file_to_accounts = {}
    for file_path in files:
        data = load_json_safe(file_path)
        if data is None:
            continue

        if isinstance(data, list):
            accounts = data
        elif isinstance(data, dict) and "accounts" in data:
            accounts = data["accounts"]
        elif isinstance(data, dict):
            accounts = []
            for v in data.values():
                if isinstance(v, list):
                    accounts.extend(v)
        else:
            continue

        if accounts:
            total_accounts += len(accounts)
            file_to_accounts[file_path] = accounts

    if total_accounts == 0:
        logger.error("Нет аккаунтов для обработки.")
        return

    completed = 0
    start_time = time.time()
    spinner = itertools.cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])

    async with async_playwright() as pw:
        sem = asyncio.Semaphore(CONCURRENT)
        tasks = []
        for file_path, accounts in file_to_accounts.items():
            if is_stop_requested():
                break
            for acc in accounts:
                if is_stop_requested():
                    break
                tasks.append(asyncio.create_task(process_single_account(pw, sem, file_path, acc)))
            if STOP_EVENT.is_set():
                break

        if not tasks:
            return None

        stop_triggered = False

        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except asyncio.CancelledError:
                continue
            except Exception as exc:
                logger.exception(f"[PROCESS] Ошибка обработки аккаунта: {exc}")
            else:
                completed += 1

            percent = completed / total_accounts if total_accounts else 0
            filled = int(percent * 20)
            bar = "█" * filled + "-" * (20 - filled)

            elapsed = time.time() - start_time
            est_total = elapsed / completed * total_accounts if completed else 0
            remaining = max(est_total - elapsed, 0)
            spinner_icon = next(spinner)

            sys.stdout.write(
                f"\r{spinner_icon} [{bar}] {percent * 100:5.1f}% | {completed:3d}/{total_accounts} | Осталось ~{remaining:5.1f} сек"
            )
            sys.stdout.flush()

            if progress_callback:
                try:
                    await progress_callback(percent, completed, total_accounts)
                except Exception:
                    pass

            if is_stop_requested():
                stop_triggered = True
                for task in tasks:
                    if not task.done():
                        task.cancel()
                break

        await asyncio.gather(*tasks, return_exceptions=True)

        total_time = time.time() - start_time

        if stop_triggered:
            sys.stdout.write(
                f"\n🛑 Обновление остановлено пользователем на {completed}/{total_accounts} аккаунтах.\n"
            )
        else:
            sys.stdout.write(
                f"\r✅ [████████████████████] 100% | Все аккаунты обработаны за {total_time / 60:.1f} мин.\n"
            )
            if progress_callback:
                try:
                    await progress_callback(1.0, completed, total_accounts)
                except Exception:
                    pass

        sys.stdout.flush()

    return None

# === Запуск ===
if __name__ == "__main__":
    asyncio.run(process_all_files())
