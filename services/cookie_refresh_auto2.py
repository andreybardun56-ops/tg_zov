"""HTTP-обновление cookies для аккаунтов puzzle2 (aiohttp).
Создаёт отдельный скрипт без Playwright, но с тёплыми заходами,
рандомными браузерными профилями и подробными логами.
"""
import asyncio
import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientError
from yarl import URL

from services.browser_patches import get_random_browser_profile

EVENT_PAGE = "https://event-eu-cc.igg.com/event/puzzle2/"
EVENT_API = f"{EVENT_PAGE}ajax.req.php"
EVENT_URL = URL("https://event-eu-cc.igg.com/")
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=45)

DATA_DIR = Path("data/data_akk")
LOG_DIR = Path("data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "cookie_refresh2.log"

logger = logging.getLogger("cookie_refresh2")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="w")
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
logger.handlers.clear()
logger.addHandler(handler)

CONCURRENT = 6
DELAY_BETWEEN_ACCOUNTS = 2.0


def jitter(base: float, variance: float = 0.35) -> float:
    delta = random.uniform(-variance * base, variance * base)
    return max(0.15, base + delta)


async def human_delay(min_delay: float = 0.4, max_delay: float = 1.2) -> None:
    await asyncio.sleep(random.uniform(min_delay, max_delay))


# ===== Работа с аккаунтами =====
def load_accounts() -> List[Dict[str, Any]]:
    accs: List[Dict[str, Any]] = []
    if not DATA_DIR.exists():
        logger.error("Папка с аккаунтами не найдена: %s", DATA_DIR)
        return accs

    for file_path in sorted(DATA_DIR.glob("new_data*.json")):
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as e:
            logger.warning("Не удалось прочитать %s: %s", file_path.name, e)
            continue

        if not isinstance(data, list):
            continue
        for entry in data:
            if not isinstance(entry, dict):
                continue
            mail = entry.get("mail")
            for uid, cookies in entry.items():
                if uid.isdigit() and isinstance(cookies, dict):
                    accs.append({
                        "file": file_path.name,
                        "mail": mail,
                        "uid": uid,
                        "cookies": cookies,
                    })
    return accs


def init_cookie_jar(cookies: Optional[Dict[str, str]]) -> aiohttp.CookieJar:
    jar = aiohttp.CookieJar(unsafe=True)
    if cookies:
        jar.update_cookies(cookies, response_url=EVENT_URL)
    return jar


def cookies_from_jar(jar: aiohttp.CookieJar) -> Dict[str, str]:
    filtered = jar.filter_cookies(EVENT_URL)
    return {name: morsel.value for name, morsel in filtered.items()}


def persist_account_cookies(uid: str, cookies: Dict[str, str]) -> None:
    if not cookies:
        return
    for file_path in sorted(DATA_DIR.glob("new_data*.json")):
        try:
            with open(file_path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue

        modified = False
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and entry.get(uid) is not None:
                    entry[uid] = cookies
                    modified = True
                    break
        if not modified:
            continue

        tmp = file_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as out:
            json.dump(data, out, ensure_ascii=False, indent=2)
        tmp.replace(file_path)
        logger.info("[%s] 🍪 Cookies обновлены в %s", uid, file_path.name)


# ===== HTTP вспомогательные =====
def _accept_language(profile: Dict[str, Any]) -> str:
    return profile.get("accept_language") or "en-US,en;q=0.9"


def build_navigation_headers(profile: Dict[str, Any]) -> Dict[str, str]:
    return {
        "User-Agent": profile.get("user_agent", "Mozilla/5.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": _accept_language(profile),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "Referer": EVENT_PAGE,
    }


def build_ajax_headers(profile: Dict[str, Any]) -> Dict[str, str]:
    return {
        "User-Agent": profile.get("user_agent", "Mozilla/5.0"),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": _accept_language(profile),
        "Referer": EVENT_PAGE,
        "X-Requested-With": "XMLHttpRequest",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


async def warmup_event_page(session: aiohttp.ClientSession, profile: Dict[str, Any], uid: str) -> None:
    headers = build_navigation_headers(profile)
    try:
        async with session.get(EVENT_PAGE, headers=headers, allow_redirects=True) as resp:
            await resp.text()
            logger.info("[%s] 🌐 Прогрев puzzle2: %s", uid, resp.status)
    except ClientError as e:
        logger.warning("[%s] ⚠️ Ошибка прогрева puzzle2: %s", uid, e)


async def ping_ajax_action(
    session: aiohttp.ClientSession,
    profile: Dict[str, Any],
    uid: str,
    action: str,
    *,
    method: str = "get",
) -> None:
    params = {"action": action, "_": str(random.randint(10_000, 99_999))}
    headers = build_ajax_headers(profile)
    try:
        if method.lower() == "post":
            async with session.post(EVENT_API, params=params, headers=headers) as resp:
                await resp.text()
        else:
            async with session.get(EVENT_API, params=params, headers=headers) as resp:
                await resp.text()
        logger.info("[%s] 🔐 Akamai ping '%s' => %s", uid, action, resp.status)
    except ClientError as e:
        logger.warning("[%s] ⚠️ Ошибка при ajax '%s': %s", uid, action, e)


def log_cookie_inventory(jar: aiohttp.CookieJar, uid: str, tag: str) -> None:
    filtered = jar.filter_cookies(EVENT_URL)
    if filtered:
        important = [name for name in filtered if name.lower() in {"ak_bmsc", "_abck", "bm_sz", "castle_age_sess"}]
        if important:
            logger.info("[%s] 🍪 %s содержит: %s", uid, tag, ", ".join(important))
        else:
            logger.info("[%s] 🍪 %s — %d cookies", uid, tag, len(filtered))
    else:
        logger.warning("[%s] 🍪 %s — jar пуст", uid, tag)


# ===== Основной worker =====
async def refresh_account(account: Dict[str, Any]) -> bool:
    uid = account.get("uid")
    mail = account.get("mail", "?")
    cookies = account.get("cookies", {})
    profile = get_random_browser_profile()
    jar = init_cookie_jar(cookies)

    logger.info("[%s] → обновляю cookies (mail=%s)", uid, mail)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600)
    start = time.perf_counter()

    async with aiohttp.ClientSession(cookie_jar=jar, timeout=REQUEST_TIMEOUT, connector=connector) as session:
        try:
            await human_delay()
            await warmup_event_page(session, profile, uid)
            log_cookie_inventory(session.cookie_jar, uid, "после warmup")
            await asyncio.sleep(jitter(1.5))

            # небольшая серия ajax-запросов, чтобы активация Akamai прошла
            await ping_ajax_action(session, profile, uid, "get_activity_time")
            await asyncio.sleep(jitter(1.0))
            await ping_ajax_action(session, profile, uid, "lottery")
            await asyncio.sleep(jitter(1.0))
            await ping_ajax_action(session, profile, uid, "get_resource", method="post")

            await human_delay(0.5, 1.5)
            fresh = cookies_from_jar(session.cookie_jar)
            if fresh:
                persist_account_cookies(str(uid), fresh)
                logger.info("[%s] ✅ Cookies сохранены (%d шт.)", uid, len(fresh))
            else:
                logger.warning("[%s] ⚠️ Получен пустой набор cookies", uid)
            log_cookie_inventory(session.cookie_jar, uid, "финал")
            return True
        except Exception as e:
            logger.error("[%s] ❌ Ошибка обновления: %s", uid, e)
            return False
        finally:
            duration = round(time.perf_counter() - start, 2)
            logger.info("[%s] ⏱ Завершено за %s сек.", uid, duration)


async def main() -> None:
    accounts = load_accounts()
    if not accounts:
        logger.error("Нет аккаунтов для обновления")
        return

    logger.info("Всего аккаунтов: %s", len(accounts))
    stats = {"total": len(accounts), "ok": 0, "fail": 0}
    sem = asyncio.Semaphore(CONCURRENT)

    async def worker(acc: Dict[str, Any]):
        async with sem:
            ok = await refresh_account(acc)
            if ok:
                stats["ok"] += 1
            else:
                stats["fail"] += 1
            await asyncio.sleep(jitter(DELAY_BETWEEN_ACCOUNTS))

    await asyncio.gather(*(worker(acc) for acc in accounts))
    logger.info("=== Итог ===")
    logger.info("Обновлено: %s", stats["ok"])
    logger.info("Ошибок: %s", stats["fail"])


if __name__ == "__main__":
    print("🚀 Запуск cookie_refresh_auto2...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Остановлено пользователем")
