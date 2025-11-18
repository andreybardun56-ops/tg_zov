"""Работа с castleclash MVP через HTTP (aiohttp)."""

import asyncio
import json
import os
import random
import re
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp
from aiohttp import ClientError
from yarl import URL

from services.browser_patches import get_random_browser_profile
from services.cookies_io import load_all_cookies, save_all_cookies
from services.logger import logger
from config import COOKIES_FILE

MVP_ORIGIN = URL("https://castleclash.igg.com/")
CDKEY_ENDPOINT = MVP_ORIGIN / "event/cdkey/ajax.req.php"
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=45)
IMPORTANT_COOKIES = {"ak_bmsc", "_abck", "bm_sz", "castle_age_sess"}
AKAMAI_WARMUP_PATHS = [
    "/akam/11/pixel_1",
    "/akam/11/pixel_2",
    "/akam/11/pixel_3",
]


def _accept_language(profile: Dict[str, Any]) -> str:
    return profile.get("accept_language") or "en-US,en;q=0.9"


def _sec_ch_headers(profile: Dict[str, Any]) -> Dict[str, str]:
    headers: Dict[str, str] = {}
    if profile.get("sec_ch_ua"):
        headers["Sec-Ch-Ua"] = profile["sec_ch_ua"]
    if profile.get("sec_ch_ua_mobile"):
        headers["Sec-Ch-Ua-Mobile"] = profile["sec_ch_ua_mobile"]
    if profile.get("sec_ch_ua_platform"):
        headers["Sec-Ch-Ua-Platform"] = profile["sec_ch_ua_platform"]
    return headers


def build_navigation_headers(profile: Dict[str, Any], referer: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": profile.get("user_agent", "Mozilla/5.0"),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": _accept_language(profile),
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Pragma": "no-cache",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
        "Host": MVP_ORIGIN.host,
        "Sec-Fetch-Site": "same-origin" if referer else "none",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-User": "?1",
        "Sec-Fetch-Dest": "document",
    }
    headers.update(_sec_ch_headers(profile))
    if referer:
        headers["Referer"] = referer
    return headers


def build_ajax_headers(profile: Dict[str, Any], referer: str) -> Dict[str, str]:
    headers = {
        "User-Agent": profile.get("user_agent", "Mozilla/5.0"),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": _accept_language(profile),
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Origin": str(MVP_ORIGIN),
        "X-Requested-With": "XMLHttpRequest",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
        "Host": MVP_ORIGIN.host,
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }
    headers.update(_sec_ch_headers(profile))
    return headers


async def human_delay(min_delay: float = 0.4, max_delay: float = 1.2) -> None:
    await asyncio.sleep(random.uniform(min_delay, max_delay))


def init_cookie_jar(initial: Optional[Dict[str, str]] = None) -> aiohttp.CookieJar:
    jar = aiohttp.CookieJar(unsafe=True)
    if initial:
        try:
            jar.update_cookies(initial, response_url=MVP_ORIGIN)
        except Exception:
            pass
    return jar


def cookies_from_jar(jar: aiohttp.CookieJar, target_url: Optional[str] = None) -> Dict[str, str]:
    url = URL(target_url) if target_url else MVP_ORIGIN
    filtered = jar.filter_cookies(url)
    return {name: morsel.value for name, morsel in filtered.items()}


def log_cookie_inventory(jar: aiohttp.CookieJar, caption: str) -> None:
    filtered = jar.filter_cookies(MVP_ORIGIN)
    if not filtered:
        logger.info("[COOKIES] 🍪 %s — jar пуст", caption)
        return
    important = [name for name in filtered if name.lower() in IMPORTANT_COOKIES]
    if important:
        logger.info("[COOKIES] 🍪 %s содержит: %s", caption, ", ".join(important))
    else:
        logger.info("[COOKIES] 🍪 %s — %d cookies", caption, len(filtered))


async def warmup_root(session: aiohttp.ClientSession, profile: Dict[str, Any]) -> None:
    headers = build_navigation_headers(profile)
    try:
        async with session.get(str(MVP_ORIGIN), headers=headers, allow_redirects=True) as resp:
            await resp.text()
            logger.info("[COOKIES] 🌐 Прогрев castleclash: %s", resp.status)
    except ClientError as e:
        logger.warning("[COOKIES] ⚠️ Ошибка прогрева castleclash: %s", e)


async def warmup_akamai(session: aiohttp.ClientSession, profile: Dict[str, Any]) -> None:
    """Дёргаем akamai pixel-ресурсы, чтобы заранее получить ak_bmsc/bm_sz."""

    headers = build_navigation_headers(profile)
    success = False

    for path in AKAMAI_WARMUP_PATHS:
        try:
            async with session.get(str(MVP_ORIGIN.with_path(path)), headers=headers) as resp:
                await resp.read()
                if resp.status == 200:
                    success = True
                    logger.info("[COOKIES] 🛡️ Akamai pixel %s => %s", path, resp.status)
                else:
                    logger.info("[COOKIES] 🛡️ Akamai pixel %s => %s", path, resp.status)
        except ClientError as e:
            logger.warning("[COOKIES] ⚠️ Akamai pixel %s: %s", path, e)

    if success:
        log_cookie_inventory(session.cookie_jar, "после Akamai пикселей")
    else:
        logger.warning("[COOKIES] ⚠️ Не удалось прогреть Akamai пиксели")


async def warmup_ajax(session: aiohttp.ClientSession, profile: Dict[str, Any], referer: str) -> None:
    params = {"action": "get_time", "_": str(random.randint(10_000, 999_999))}
    headers = build_ajax_headers(profile, referer)
    try:
        async with session.get(str(CDKEY_ENDPOINT), params=params, headers=headers) as resp:
            await resp.text()
            logger.info("[COOKIES] 🔐 Ajax ping %s", resp.status)
    except ClientError as e:
        logger.warning("[COOKIES] ⚠️ Ошибка ajax ping: %s", e)


async def fetch_mvp_page(
    session: aiohttp.ClientSession,
    profile: Dict[str, Any],
    url: str,
) -> str:
    headers = build_navigation_headers(profile, referer=str(MVP_ORIGIN))
    async with session.get(url, headers=headers, allow_redirects=True) as resp:
        text = await resp.text()
        logger.info("[COOKIES] 📄 Загружена MVP-страница (%s)", resp.status)
        return text

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
    """Обновляет cookies, полностью обходясь без Playwright."""

    from .accounts_manager import get_all_accounts

    logger.info(f"[COOKIES] 🌐 Обновляю cookies для UID={uid} (user_id={user_id})")

    accounts = get_all_accounts(str(user_id))
    acc = next((a for a in accounts if a.get("uid") == uid), None)
    if not acc or not acc.get("mvp_url"):
        return {"success": False, "error": "MVP ссылка не найдена. Добавь аккаунт заново."}

    mvp_url = acc["mvp_url"]
    profile = get_random_browser_profile()
    jar = init_cookie_jar(load_cookies_for_account(user_id, uid))
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600)

    try:
        async with aiohttp.ClientSession(cookie_jar=jar, timeout=REQUEST_TIMEOUT, connector=connector) as session:
            await warmup_akamai(session, profile)
            await human_delay(0.2, 0.5)
            await warmup_root(session, profile)
            log_cookie_inventory(session.cookie_jar, "после прогрева")
            await human_delay()
            await warmup_ajax(session, profile, mvp_url)
            await human_delay(0.6, 1.6)
            html = await fetch_mvp_page(session, profile, mvp_url)
            await human_delay(0.3, 0.9)

            cookies_result = cookies_from_jar(session.cookie_jar, mvp_url)
            if cookies_result:
                all_data = load_all_cookies()
                all_data.setdefault(str(user_id), {})[str(uid)] = cookies_result
                save_all_cookies(all_data)
                log_cookie_inventory(session.cookie_jar, "финал")
                logger.info(f"[COOKIES] 💾 Cookies обновлены для UID={uid}")
                return {"success": True, "cookies": cookies_result, "html": html}

            logger.warning(f"[COOKIES] ⚠️ Не удалось получить cookies для UID={uid}")
            return {"success": False, "error": "Не удалось получить cookies"}

    except ClientError as e:
        logger.error(f"[COOKIES] ❌ HTTP-ошибка при обновлении: {e}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.exception(f"[COOKIES] ❌ Ошибка при обновлении cookies: {e}")
        return {"success": False, "error": str(e)}


ProgressPayload = Dict[str, Any]
ProgressCallback = Callable[[ProgressPayload], Awaitable[None]]


async def refresh_all_cookies(
    progress_callback: Optional[ProgressCallback] = None,
    sleep_between: tuple[float, float] = (0.8, 1.6),
) -> Dict[str, Any]:
    """Обновляет cookies всех аккаунтов, используя aiohttp MVP-подход."""

    from .accounts_manager import get_all_users_accounts

    accounts_by_user = get_all_users_accounts()
    total_accounts = sum(len(accs) for accs in accounts_by_user.values())

    summary: Dict[str, Any] = {
        "total": total_accounts,
        "processed": 0,
        "success": 0,
        "failed": 0,
        "skipped": [],
        "failures": [],
    }

    async def emit(payload: ProgressPayload) -> None:
        if progress_callback:
            try:
                await progress_callback(payload)
            except Exception:
                logger.exception("[COOKIES] Ошибка в progress_callback")

    for user_id, accounts in accounts_by_user.items():
        for account in accounts:
            summary["processed"] += 1
            uid = (account.get("uid") or "").strip()
            username = account.get("username") or "Игрок"
            mvp_url = (account.get("mvp_url") or "").strip()

            payload_base = {
                "user_id": user_id,
                "uid": uid,
                "username": username,
                "processed": summary["processed"],
                "total": total_accounts,
            }

            if not uid:
                reason = "Отсутствует UID"
                summary["skipped"].append({"user_id": user_id, "reason": reason})
                await emit({**payload_base, "status": "skipped", "error": reason})
                continue

            if not mvp_url:
                reason = "Нет MVP ссылки"
                summary["skipped"].append({"user_id": user_id, "uid": uid, "reason": reason})
                await emit({**payload_base, "status": "skipped", "error": reason})
                continue

            result = await refresh_cookies_mvp(user_id, uid)

            if result.get("success"):
                summary["success"] += 1
                await emit({**payload_base, "status": "success", "cookies": result.get("cookies", {})})
            else:
                summary["failed"] += 1
                error_text = result.get("error", "Неизвестная ошибка")
                summary["failures"].append({"user_id": user_id, "uid": uid, "error": error_text})
                await emit({**payload_base, "status": "failed", "error": error_text})

            await human_delay(*sleep_between)

    return summary

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

def _parse_player_info(html: str) -> Dict[str, Optional[str]]:
    result: Dict[str, Optional[str]] = {"uid": None, "username": None}

    igg_patterns = [
        r"IGG\s*ID[^0-9]{0,20}(\d{6,12})",
        r"\bigg\s*id\b[^0-9]{0,20}(\d{6,12})",
        r'"iggid"\s*:\s*"(\d{6,12})"',
        r'"uid"\s*:\s*"(\d{6,12})"',
    ]
    for pattern in igg_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            result["uid"] = match.group(1)
            break

    name_patterns = [
        r"Имя\s+игрока[:：]?\s*([^<\n]+)",
        r"Player\s+Name[:：]?\s*([^<\n]+)",
        r'"playername"\s*:\s*"([^"]+)"',
        r'"username"\s*:\s*"([^"]+)"',
        r'"name"\s*:\s*"([^"]+)"',
    ]
    for pattern in name_patterns:
        match = re.search(pattern, html, re.IGNORECASE)
        if match:
            result["username"] = match.group(1).strip()
            break

    return result


async def extract_player_info_from_page(url: str) -> dict:
    """Запрашивает MVP-ссылку через aiohttp и парсит IGG ID + имя."""

    logger.info(f"[MVP] 🌐 Открываю страницу для получения данных: {url}")
    profile = get_random_browser_profile()
    jar = init_cookie_jar()
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=600)

    try:
        async with aiohttp.ClientSession(cookie_jar=jar, timeout=REQUEST_TIMEOUT, connector=connector) as session:
            await warmup_akamai(session, profile)
            await human_delay(0.2, 0.5)
            await warmup_root(session, profile)
            await warmup_ajax(session, profile, url)
            await human_delay(0.4, 1.0)
            html = await fetch_mvp_page(session, profile, url)

    except ClientError as e:
        logger.error(f"[MVP] ❌ HTTP-ошибка при открытии страницы: {e}")
        return {"success": False, "error": str(e)}
    except Exception as e:
        logger.error(f"[MVP] ❌ Ошибка при открытии страницы: {e}")
        return {"success": False, "error": str(e)}

    parsed = _parse_player_info(html)
    if parsed.get("uid") and parsed.get("username"):
        logger.info(
            "[MVP] ✅ Найден IGG ID=%s, username=%s",
            parsed["uid"],
            parsed["username"],
        )
        return {"success": True, **parsed}

    logger.warning("[MVP] ⚠️ Не удалось извлечь IGG ID или имя")
    return {"success": False, "error": "Не удалось извлечь IGG ID или имя"}
