# tg_zov/services/event_checker.py
import asyncio
import json
import logging
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any

from playwright.async_api import async_playwright, Page, BrowserContext, Response

from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    launch_masked_persistent_context,
)

# ────────────────────────────────────────────────
# Настройки и директории
# ────────────────────────────────────────────────
COOKIES_FILE = Path("data/cookies.json")
PROFILE_DIR = Path("data/chrome_profiles")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

STATUS_FILE = Path("data/event_status.json")
FAIL_DIR = Path("data/fails/event_checker")
FAIL_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("event_checker")

EVENTS = {
    "puzzle2": {"url": "https://event-eu-cc.igg.com/event/puzzle2/"},
    "flop_pair": {"url": "https://event-eu-cc.igg.com/event/flop_pair/"},
    "blind_box": {"url": "https://event-eu-cc.igg.com/event/blind_box/"},
    "regress_10th": {"url": "https://event-eu-cc.igg.com/event/regress_10th/"},
    "thanksgiving_event": {"url": "https://event-eu-cc.igg.com/event/thanksgiving_time/"},
    "castle_machine": {"url": "https://event-eu-cc.igg.com/event/castle_machine/"},
    "lucky_wheel": {"url": "https://event-eu-cc.igg.com/event/lucky_wheel/"},
    "dragon_quest": {"url": "https://event-eu-cc.igg.com/event/dragon_quest/"},
    "gas": {"url": "https://event-eu-cc.igg.com/event/gas/"},
}

TIMED_EVENTS = {"thanksgiving_event", "castle_machine", "dragon_quest", "gas"}
INACTIVE_MARKERS = ("event has not yet begun", "has already ended", "please login again", "veuillez vous reconnecter")

UTC = timezone.utc
LOCAL_OFFSET = timedelta(hours=10)
IGG_ID = "952522571"

BLOCKED_RESOURCE_TYPES = {"image", "media", "font", "websocket"}
BLOCKED_URL_KEYWORDS = (
    "analytics", "google-analytics", "googletagmanager", "doubleclick",
    "facebook", "fbcdn", "hotjar", "clarity", "yandex", "metrika", "ads", "tracking"
)

# ────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────
def get_cookies_for_igg(igg_id: str) -> list[dict]:
    if not COOKIES_FILE.exists():
        raise RuntimeError("Файл cookies.json не найден")
    with open(COOKIES_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    outer_key = next(iter(data), None)
    if not outer_key:
        raise RuntimeError("Нет данных в cookies.json")
    igg_cookies_raw = data[outer_key].get(igg_id)
    if not igg_cookies_raw:
        raise RuntimeError(f"Нет cookies для IGG ID {igg_id}")
    return [
        {"name": name, "value": value, "domain": ".igg.com", "path": "/", "httpOnly": True, "secure": True}
        for name, value in igg_cookies_raw.items()
    ]

def _inactive_reason(text: str) -> bool:
    low = (text or "").lower()
    return any(p in low for p in INACTIVE_MARKERS)

async def route_handler(route, request):
    url = request.url.lower()
    if request.resource_type in BLOCKED_RESOURCE_TYPES or any(k in url for k in BLOCKED_URL_KEYWORDS):
        await route.abort()
    else:
        await route.continue_()

async def _read_body_text(page: Page) -> str:
    try:
        return (await page.evaluate("document.body?.innerText || document.body?.textContent || ''")).strip()
    except Exception:
        return ""
# ────────────────────────────────────────────────
# Универсальный парсер дат
# ────────────────────────────────────────────────
def parse_flexible(dt: str) -> datetime:
    """
    Парсинг даты вида D/M или M/D с HH:MM:SS.
    Определяем что день, что месяц по логике чисел.
    """
    dt = dt.replace("-", "/")
    month_day, time_str = dt.split(" ")
    a, b = map(int, month_day.split("/"))
    hour, minute, second = map(int, time_str.split(":"))

    # если первое число > 12 — это день, второе месяц
    if a > 12:
        day, month = a, b
    # если второе число > 12 — это месяц/день
    elif b > 12:
        month, day = a, b
    # если оба <=12 — предполагаем формат MM/DD по умолчанию
    else:
        month, day = a, b

    return datetime(datetime.now().year, month, day, hour, minute, second, tzinfo=UTC)

# ────────────────────────────────────────────────
# Безопасная навигация с retry
# ────────────────────────────────────────────────
async def safe_goto(page: Page, url: str, retries: int = 2) -> Response | None:
    for attempt in range(1, retries + 2):
        try:
            response = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(1)  # даем JS/XHR подгрузиться
            if response is not None and response.status in (403, 401):
                logger.warning(f"Goto {url} вернул {response.status}, попытка {attempt}")
                continue
            return response
        except Exception as e:
            logger.warning(f"Goto {url} ошибка на попытке {attempt}: {e}")
            await asyncio.sleep(1)
    raise RuntimeError(f"Не удалось загрузить {url} после {retries+1} попыток")

# ────────────────────────────────────────────────
# Проверка отдельного события
# ────────────────────────────────────────────────
async def check_event(event_name: str, page: Page) -> bool | int:
    try:
        await safe_goto(page, EVENTS[event_name]["url"])
        body_text = await _read_body_text(page)
        html_text = await page.content()

        # сохраняем HTML
        dump_dir = FAIL_DIR / "html"
        dump_dir.mkdir(parents=True, exist_ok=True)
        (dump_dir / f"{event_name}.html").write_text(html_text or "<EMPTY>", encoding="utf-8")

        if _inactive_reason(body_text):
            logger.info(f"[{event_name}] неактивна (маркер неактивности найден)")
            return False

        if event_name not in TIMED_EVENTS:
            logger.info(f"[{event_name}] активна (не таймированная акция)")
            return True

        # ───────────── парсинг таймированных дат ─────────────
        try:
            time_span = await page.query_selector("#app .event-time")
            if not time_span:
                logger.warning(f"[{event_name}] элемент .event-time не найден на странице")
                return False

            time_text = await time_span.inner_text()
            logger.info(f"[{event_name}] найден период события: {time_text}")

            match = re.match(
                r".*?(\d{1,2}/\d{1,2}\s+\d{2}:\d{2}:\d{2})\s*[~－～]\s*(\d{1,2}/\d{1,2}\s+\d{2}:\d{2}:\d{2}).*",
                time_text
            )
            if not match:
                logger.warning(f"[{event_name}] таймированные интервалы не распознаны")
                return False

            start_str, end_str = match.groups()
            start_dt = parse_flexible(start_str)
            end_dt = parse_flexible(end_str)

            logger.info(f"[{event_name}] parsed start: {start_dt}, end: {end_dt}")

            now_utc = datetime.now(UTC)
            now_server = now_utc - LOCAL_OFFSET

            if start_dt <= now_server <= end_dt:
                logger.info(f"[{event_name}] активна (попадает в интервал)")
                return True
            else:
                logger.info(f"[{event_name}] неактивна (текущее время не попадает в интервал)")
                return False

        except Exception as e:
            logger.error(f"[{event_name}] ошибка при парсинге дат: {e}")
            return False

    except Exception as e:
        # <- этот внешний except был пропущен
        logger.error(f"[{event_name}] общая ошибка: {e}")
        return False
# ────────────────────────────────────────────────
# Проверка всех акций
# ────────────────────────────────────────────────
async def check_all_events(bot=None, admin_id=None) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    async with async_playwright() as p:
        ctx_data = await launch_masked_persistent_context(
            p,
            user_data_dir=str(PROFILE_DIR / f"{IGG_ID}_events"),
            browser_path=BROWSER_PATH,
            headless=True,
            slow_mo=25,
            profile=get_random_browser_profile(),
        )
        context: BrowserContext = ctx_data["context"]
        page: Page = ctx_data["page"]

        await page.route("**/*", route_handler)

        try:
            cookies_list = get_cookies_for_igg(IGG_ID)
            await context.add_cookies(cookies_list)
            logger.info(f"[check_all_events] добавлено {len(cookies_list)} cookies")
        except Exception as e:
            logger.error(f"[check_all_events] Ошибка при добавлении cookies: {e}")

        # базовая страница
        await safe_goto(page, "https://event-eu-cc.igg.com/")

        # проверка акций
        for name in EVENTS:
            results[name] = await check_event(name, page)

        await context.close()

    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATUS_FILE.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    logger.info(f"[check_all_events] результаты проверки: {results}")

    if bot and admin_id:
        msg = "📊 <b>Проверка акций завершена</b>\n\n"
        for k, v in results.items():
            emoji = "✅" if v else "⚠️"
            msg += f"{emoji} {k}\n"
        await bot.send_message(admin_id, msg, parse_mode="HTML")

    return results

# ────────────────────────────────────────────────
# Utility
# ────────────────────────────────────────────────
async def get_event_status(event_name: str) -> bool:
    try:
        if not STATUS_FILE.exists():
            return False
        with open(STATUS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return bool(data.get(event_name))
    except Exception:
        return False

# ────────────────────────────────────────────────
# Run
# ────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(check_all_events())
