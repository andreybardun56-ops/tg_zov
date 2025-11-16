# services/puzzle2_auto.py
import os
import asyncio
import logging
import json
import tempfile
import shutil
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientError
from yarl import URL
from tqdm.asyncio import tqdm_asyncio

from services.browser_patches import get_random_browser_profile

# === Глобальные параметры ===
EVENT_PAGE = "https://event-eu-cc.igg.com/event/puzzle2/"
EVENT_API = f"{EVENT_PAGE}ajax.req.php"
EVENT_URL = URL("https://event-eu-cc.igg.com/")
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=45)

DATA_DIR = Path("data/data_akk")
LOG_DIR = Path("data/logs")
DATA_FILE = Path("data/puzzle_data.jsonl")
FAIL_DIR = Path("data/failures")

CONCURRENT = 5
DELAY_BETWEEN_ACCOUNTS = 3
DELAY_BETWEEN_LOTTERY = 1.5

STOP_EVENT = asyncio.Event()


# === Логирование ===
LOG_DIR.mkdir(parents=True, exist_ok=True)
FAIL_DIR.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("puzzle2_auto")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(LOG_DIR / "puzzle2_auto.log", encoding="utf-8", mode="w")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S"))
logger.handlers.clear()
logger.addHandler(file_handler)


# === Управление остановкой ===
def request_stop() -> None:
    STOP_EVENT.set()


def is_stop_requested() -> bool:
    return STOP_EVENT.is_set()


def clear_stop_request() -> None:
    STOP_EVENT.clear()


# === Вспомогательные функции ===
def jitter(base: float, variance: float = 0.5) -> float:
    delta = random.uniform(-variance * base, variance * base)
    return max(0.1, base + delta)


async def humanize_pre_action(min_delay: float = 0.5, max_delay: float = 2.0) -> None:
    await asyncio.sleep(random.uniform(min_delay, max_delay))


def load_accounts() -> List[Dict[str, Any]]:
    accounts: List[Dict[str, Any]] = []
    if not DATA_DIR.exists():
        logger.error("Папка не найдена: %s", DATA_DIR)
        return accounts

    for f in sorted(DATA_DIR.glob("new_data*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, list):
                for entry in data:
                    mail = entry.get("mail")
                    for k, v in entry.items():
                        if k.isdigit() and isinstance(v, dict):
                            accounts.append({"file": f.name, "mail": mail, "uid": k, "cookies": v})
        except Exception as e:
            logger.warning("Не удалось прочитать %s: %s", f.name, e)
    return accounts


def save_puzzle_data(entry: dict, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    entry["time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    existing: List[Dict[str, Any]] = []
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
    with os.fdopen(temp_fd, "w", encoding="utf-8") as tmp:
        for obj in existing:
            json.dump(obj, tmp, ensure_ascii=False, indent=2)
            tmp.write("\n\n")
    shutil.move(temp_path, file_path)


def calculate_puzzle_totals(file_path: Path):
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
        logger.info("Пазл %s: %s шт.", pid, cnt)
    logger.info("=========================")
    logger.info("Всего дубликатов: %s", total_sum)
    logger.info("Аккаунтов обработано: %s", count_accounts)

    summary_path = Path("data/puzzle_summary.json")
    with open(summary_path, "w", encoding="utf-8") as out:
        json.dump({
            "totals": totals,
            "accounts": count_accounts,
            "all_duplicates": total_sum,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }, out, ensure_ascii=False, indent=2)

    return totals


# === Работа с cookies ===
def init_cookie_jar(cookies: Optional[Dict[str, str]]) -> aiohttp.CookieJar:
    jar = aiohttp.CookieJar(unsafe=True)
    if cookies:
        jar.update_cookies(cookies, response_url=EVENT_URL)
    return jar


def cookies_from_jar(jar: aiohttp.CookieJar) -> Dict[str, str]:
    filtered = jar.filter_cookies(EVENT_URL)
    return {k: v.value for k, v in filtered.items()}


def persist_account_cookies(uid: str, cookies: Dict[str, str]) -> None:
    if not cookies:
        return
    for file_path in sorted(DATA_DIR.glob("new_data*.json")):
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue

        changed = False
        if isinstance(data, list):
            for entry in data:
                for k in list(entry.keys()):
                    if k == str(uid) and isinstance(entry[k], dict):
                        entry[k] = cookies
                        changed = True
        if changed:
            tmp = file_path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as out:
                json.dump(data, out, ensure_ascii=False, indent=2)
            os.replace(tmp, file_path)
            logger.info("[%s] 🍪 Cookies обновлены в %s", uid, file_path.name)


def log_cookie_names(jar: aiohttp.CookieJar, uid: str, caption: str) -> None:
    cookies = jar.filter_cookies(EVENT_URL)
    if cookies:
        names = ", ".join(cookies.keys())
        logger.info("[%s] 🍪 Cookies %s: %s", uid, caption, names)
    else:
        logger.warning("[%s] ⚠️ Cookies %s отсутствуют", uid, caption)


# === HTTP-заголовки ===
def _get_accept_language(profile: Dict[str, Any]) -> str:
    return profile.get("accept_language") or "en-US,en;q=0.9"


def build_navigation_headers(profile: Dict[str, Any]) -> Dict[str, str]:
    accept_lang = _get_accept_language(profile)
    return {
        "User-Agent": profile.get("user_agent", "Mozilla/5.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": accept_lang,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "Referer": EVENT_PAGE,
    }


def build_ajax_headers(profile: Dict[str, Any]) -> Dict[str, str]:
    accept_lang = _get_accept_language(profile)
    return {
        "User-Agent": profile.get("user_agent", "Mozilla/5.0"),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": accept_lang,
        "Referer": EVENT_PAGE,
        "X-Requested-With": "XMLHttpRequest",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


# === HTTP-помощники ===
async def warmup_event_page(session: aiohttp.ClientSession, profile: Dict[str, Any], uid: str) -> None:
    headers = build_navigation_headers(profile)
    try:
        async with session.get(EVENT_PAGE, headers=headers) as resp:
            await resp.text()
            logger.info("[%s] 🌐 Прогрев puzzle2: %s", uid, resp.status)
    except ClientError as e:
        logger.warning("[%s] ⚠️ Ошибка прогрева puzzle2: %s", uid, e)


async def perform_lottery_request(session: aiohttp.ClientSession, profile: Dict[str, Any], uid: str, label: str) -> str:
    params = {"action": "lottery"}
    headers = build_ajax_headers(profile)
    async with session.get(EVENT_API, params=params, headers=headers) as resp:
        text = await resp.text()
        logger.info("[%s] 🎯 Ответ lottery (%s): %s | %s", uid, label, resp.status, text[:200].replace("\n", " "))
        return text


async def run_lottery_sequence(session: aiohttp.ClientSession, profile: Dict[str, Any], uid: str) -> None:
    try:
        await asyncio.sleep(jitter(DELAY_BETWEEN_LOTTERY, variance=0.5))
        text = await perform_lottery_request(session, profile, uid, "основной")
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            logger.warning("[%s] ⚠️ lottery: не JSON", uid)
            return

        err = data.get("error")
        status = data.get("status")
        if str(err) == "1" and str(status) == "0":
            logger.info("[%s] 🚫 Шансы закончились", uid)
            return
        if str(status) == "1":
            logger.info("[%s] ✅ Лотерея успешна — выполняем ещё 2 запроса", uid)
            for attempt in range(2):
                await asyncio.sleep(jitter(DELAY_BETWEEN_LOTTERY, variance=0.5))
                await perform_lottery_request(session, profile, uid, f"доп.{attempt + 1}")
        else:
            logger.info("[%s] ⚠️ Неизвестный ответ lottery: %s", uid, text[:150])
    except ClientError as e:
        logger.warning("[%s] ⚠️ Ошибка lottery: %s", uid, e)


async def fetch_get_resource(session: aiohttp.ClientSession, profile: Dict[str, Any], uid: str) -> Optional[str]:
    headers = build_ajax_headers(profile)
    params = {"action": "get_resource"}
    try:
        async with session.post(EVENT_API, params=params, headers=headers) as resp:
            text = await resp.text()
            logger.info("[%s] 📥 Ответ get_resource: %s", uid, resp.status)
            return text
    except ClientError as e:
        logger.error("[%s] ❌ Ошибка get_resource: %s", uid, e)
        return None


# === Основной workflow аккаунта ===
async def process_account(account: Dict[str, Any]) -> bool:
    uid = account.get("uid")
    mail = account.get("mail", "?")
    cookies = account.get("cookies", {})
    start_time = time.perf_counter()

    profile = get_random_browser_profile()
    jar = init_cookie_jar(cookies)

    logger.info("[%s] → старт обработки (mail=%s)", uid, mail)

    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(cookie_jar=jar, timeout=REQUEST_TIMEOUT, connector=connector) as session:
        try:
            await warmup_event_page(session, profile, uid)
            await asyncio.sleep(jitter(2.5, variance=0.4))
            await humanize_pre_action()

            updated_cookies = cookies_from_jar(session.cookie_jar)
            persist_account_cookies(uid, updated_cookies)
            log_cookie_names(session.cookie_jar, uid, "после warmup")

            await run_lottery_sequence(session, profile, uid)

            await humanize_pre_action(1.0, 2.5)
            log_cookie_names(session.cookie_jar, uid, "перед get_resource")

            await asyncio.sleep(jitter(1.5, variance=1.0))
            text = await fetch_get_resource(session, profile, uid)
            if not text:
                return False

            if not text.strip().startswith("{"):
                debug_path = FAIL_DIR / f"{uid}_get_resource_response.html"
                debug_path.write_text(text, encoding="utf-8")
                logger.error("[%s] ⚠️ get_resource вернул HTML, сохранено в %s", uid, debug_path)
                return False

            data = json.loads(text)
            logger.info("[%s] ✅ get_resource получен успешно", uid)

            data_section = data.get("data", {})
            if isinstance(data_section, list) and data_section:
                user = data_section[0].get("user", {})
            elif isinstance(data_section, dict):
                user = data_section.get("user", {})
            else:
                user = {}

            extra_info = user.get("extra_info", {})
            logger.info("[%s] 🧩 EXTRA_INFO: %s", uid, json.dumps(extra_info, ensure_ascii=False))

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
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

            duplicates: Dict[str, int] = {}
            for pid, count in puzzle_data.items():
                try:
                    count_int = int(count)
                except Exception:
                    continue
                if count_int >= 2:
                    duplicates[pid] = count_int - 1

            if duplicates:
                entry["puzzle"] = duplicates
                save_puzzle_data(entry, DATA_FILE)
                logger.info("[%s] ✅ Найдены дубликаты пазлов: %s — сохранено", uid, duplicates)

                try:
                    import colorama
                    from colorama import Fore, Style

                    colorama.init()
                    grid = [[" " for _ in range(3)] for _ in range(3)]
                    for pid, count in puzzle_data.items():
                        try:
                            idx = int(pid) - 1
                        except Exception:
                            continue
                        row, col = divmod(idx, 3)
                        if int(count) >= 2:
                            grid[row][col] = f"{Fore.GREEN}{count}{Style.RESET_ALL}"
                        else:
                            grid[row][col] = str(count)
                    logger.info("[%s] 🧩 Расклад пазлов:", uid)
                    for row in grid:
                        logger.info("[%s]    %s", uid, "  ".join(row))
                except Exception as e:
                    logger.warning("[%s] ⚠️ Ошибка визуализации пазлов: %s", uid, e)

                try:
                    calculate_puzzle_totals(DATA_FILE)
                    logger.info("[%s] 🔄 Итоговый файл puzzle_summary.json обновлён", uid)
                except Exception as e:
                    logger.warning("[%s] ⚠️ Не удалось обновить puzzle_summary.json: %s", uid, e)
            else:
                logger.info("[%s] ❌ Дубликатов нет — пропуск", uid)

            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            debug_path = FAIL_DIR / f"{uid}_exception.txt"
            debug_path.write_text(str(e), encoding="utf-8")
            logger.error("[%s] ❌ Общая ошибка: %s", uid, e)
            return False
        finally:
            duration = round(time.perf_counter() - start_time, 2)
            logger.info("[%s] ⏱ Завершено за %s сек.", uid, duration)
            await asyncio.sleep(jitter(DELAY_BETWEEN_ACCOUNTS, variance=0.6))


# === Основной сценарий ===
async def main():
    clear_stop_request()
    accounts = load_accounts()
    if not accounts:
        logger.error("Аккаунты не найдены в %s", DATA_DIR)
        return

    start_time = time.perf_counter()
    stats = {"total": len(accounts), "success": 0, "fail": 0}
    logger.info("Всего аккаунтов: %d", len(accounts))
    sem = asyncio.Semaphore(CONCURRENT)

    async def worker(acc: Dict[str, Any]):
        uid = acc.get("uid")
        if STOP_EVENT.is_set():
            logger.info("[%s] ⏹ Пропуск аккаунта: получен сигнал остановки", uid)
            return
        async with sem:
            if STOP_EVENT.is_set():
                logger.info("[%s] ⏹ Завершаем перед стартом обработки", uid)
                return
            try:
                success = await process_account(acc)
                if success:
                    stats["success"] += 1
                else:
                    stats["fail"] += 1
            except Exception as e:
                stats["fail"] += 1
                logger.error("[%s] ❌ Ошибка выполнения: %s", uid, e)

    tasks = [asyncio.create_task(worker(acc)) for acc in accounts]
    try:
        await tqdm_asyncio.gather(*tasks, desc="Обработка аккаунтов", total=len(tasks))
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    try:
        calculate_puzzle_totals(DATA_FILE)
        logger.info("🧮 Итоговый подсчёт пазлов выполнен успешно")
    except Exception as e:
        logger.warning("⚠️ Не удалось пересчитать итоговые пазлы: %s", e)

    logger.info("=== ✅ Итог ===")
    logger.info("Всего аккаунтов: %s", stats["total"])
    logger.info("Успешно: %s", stats["success"])
    logger.info("Ошибок: %s", stats["fail"])
    total_time = round(time.perf_counter() - start_time, 2)
    logger.info("Время выполнения: %s сек.", total_time)
    logger.info("Все аккаунты обработаны.")


if __name__ == "__main__":
    print("🚀 Запуск puzzle2_auto.py...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Работа прервана пользователем.")
    except Exception as e:
        print(f"\n❌ Неожиданная ошибка: {e}")
