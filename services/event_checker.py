# tg_zov/services/event_checker.py
import asyncio
import json
import logging
import tempfile
import shutil
from pathlib import Path
from typing import Dict, Tuple, Optional
from playwright.async_api import async_playwright

from services.browser_patches import (
    get_random_browser_profile,
    launch_masked_persistent_context,
    cookies_to_playwright,
)

logger = logging.getLogger("event_checker")

BROWSER_PATH = r".venv/Chrome/Application/chrome.exe"
PROFILE_DIR = Path("data/chrome_profiles")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

STATUS_FILE = Path("data/event_status.json")
FAIL_DIR = Path("data/fails/event_checker")
FAIL_DIR.mkdir(parents=True, exist_ok=True)
NEW_DATA_DIR = Path("data/data_akk")

EVENTS = {
    "puzzle2": "https://event-eu-cc.igg.com/event/puzzle2/",
    "flop_pair": "https://event-eu-cc.igg.com/event/flop_pair/",
    "blind_box": "https://event-eu-cc.igg.com/event/blind_box/",
    "regress_10th": "https://event-eu-cc.igg.com/event/regress_10th/",
    "thanksgiving_event": "https://event-eu-cc.igg.com/event/thanksgiving_time/",
    "castle_machine": "https://event-eu-cc.igg.com/event/castle_machine/",
    "lucky_wheel": "https://event-eu-cc.igg.com/event/lucky_wheel/",
    "dragon_quest": "https://event-eu-cc.igg.com/event/dragon_quest/",
}

INACTIVE_MARKERS = [
    "событие еще не началось",
    "или уже завершилось",
    "event has not yet begun",
    "has already ended",
    "please login again",
    "veuillez vous reconnecter",
    "veullez vous reconnecter",
]


# ---------- Вспомогательные функции ----------
def _is_inactive_by_text(body_text: str) -> Optional[str]:
    """Проверяет текст страницы на наличие фраз неактивности"""
    if not body_text:
        return "страница пуста"
    low = body_text.lower()
    for phrase in INACTIVE_MARKERS:
        if phrase in low:
            return phrase
    return None


async def _read_body_text(page) -> str:
    try:
        text = await page.evaluate(
            "(() => document?.body?.innerText || document?.body?.textContent || '')()"
        )
        return text.strip() if isinstance(text, str) else ""
    except Exception:
        return ""


def pick_first_account_from_new_data() -> Optional[Tuple[Path, str, Dict[str, str]]]:
    """Возвращает (file_path, uid, cookies_dict) первого аккаунта"""
    if not NEW_DATA_DIR.exists():
        logger.warning("[event_checker] ⚠️ Папка new_data не найдена")
        return None

    for file in sorted(NEW_DATA_DIR.glob("new_data*.json")):
        try:
            with open(file, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, list):
                continue
            for entry in data:
                if isinstance(entry, dict):
                    for key, val in entry.items():
                        if key.isdigit() and isinstance(val, dict):
                            logger.info(f"[event_checker] ✅ Найден аккаунт {key} в {file.name}")
                            return file, key, val
        except Exception as e:
            logger.warning(f"[event_checker] ⚠️ Ошибка чтения {file.name}: {e}")
    return None


def update_cookies_in_new_data(file_path: Path, uid: str, new_cookies: Dict[str, str]):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for entry in data:
                if isinstance(entry, dict) and str(uid) in entry:
                    entry[str(uid)] = new_cookies
                    break
        tmp_fd, tmp_path = tempfile.mkstemp(dir=file_path.parent)
        with open(tmp_fd, "w", encoding="utf-8") as tmp:
            json.dump(data, tmp, ensure_ascii=False, indent=2)
        shutil.move(tmp_path, file_path)
        logger.info(f"[event_checker] 🔄 Cookies обновлены в {file_path.name} для {uid}")
    except Exception as e:
        logger.warning(f"[event_checker] ⚠️ Не удалось обновить cookies: {e}")


# ---------- Основная проверка ----------
async def check_event_active(event_name: str) -> bool:
    event_url = EVENTS.get(event_name)
    if not event_url:
        return False

    picked = pick_first_account_from_new_data()
    if not picked:
        return False

    file_path, uid, acc_cookies = picked
    html_text = ""
    body_text = ""

    async with async_playwright() as p:
        ctx = await launch_masked_persistent_context(
            p,
            user_data_dir=str(PROFILE_DIR / f"{uid}_check"),
            browser_path=BROWSER_PATH,
            headless=True,
            slow_mo=25,
            profile=get_random_browser_profile(),
        )
        context, page = ctx["context"], ctx["page"]

        try:
            if acc_cookies:
                await context.add_cookies(cookies_to_playwright(acc_cookies))
            await page.goto(event_url, wait_until="domcontentloaded", timeout=35000)

            fresh = await context.cookies()
            fresh_dict = {c["name"]: c["value"] for c in fresh if "name" in c and "value" in c}
            if fresh_dict:
                update_cookies_in_new_data(file_path, uid, fresh_dict)

            body_text = await _read_body_text(page)
            html_text = await page.content()

            dump_dir = FAIL_DIR / "html"
            dump_dir.mkdir(parents=True, exist_ok=True)
            (dump_dir / f"{event_name}.html").write_text(html_text or "<EMPTY>", encoding="utf-8")

        except Exception as e:
            logger.error(f"[{event_name}] ❌ Ошибка при загрузке: {e}")
            return False
        finally:
            try:
                await page.close()
                await context.close()
            except Exception:
                pass

    import re
    from datetime import datetime, timezone, timedelta

    UTC = timezone.utc
    SERVER_TZ = timezone.utc  # IGG сервер работает по UTC
    LOCAL_OFFSET = timedelta(hours=10)  # твой локальный +10 => сервер на 10 часов позади

    if event_name in {"thanksgiving_event", "castle_machine"}:
        matches = re.findall(
            r"(\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)\s*[~－～]\s*(\d{1,2}[-/]\d{1,2}\s+\d{1,2}:\d{1,2}(?::\d{1,2})?)",
            html_text
        )

        if matches:
            current_year = datetime.now(UTC).year

            # 💡 Серверное время = локальное − 10 часов
            local_now = datetime.now().astimezone()
            now_server = (local_now - LOCAL_OFFSET).replace(tzinfo=SERVER_TZ)

            def parse_flex(dt_str: str):
                """Парсит дату вида '15-10 00:00' как день-месяц."""
                dt_str = dt_str.replace('-', '/').strip()
                day, month_time = dt_str.split('/', 1)
                month, rest = month_time.split(' ', 1)
                fixed = f"{current_year}/{month}/{day} {rest}"  # 2025/10/15 ...
                for fmt in ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M"):
                    try:
                        return datetime.strptime(fixed, fmt).replace(tzinfo=UTC)
                    except ValueError:
                        continue
                raise ValueError(f"Не удалось разобрать дату: {dt_str}")

            active_phase = None
            segments_info = []

            for i, (start_str, end_str) in enumerate(matches, 1):
                try:
                    start_dt = parse_flex(start_str)
                    end_dt = parse_flex(end_str)
                    segments_info.append(f"{start_dt:%d/%m %H:%M}–{end_dt:%d/%m %H:%M}")

                    if start_dt <= now_server <= end_dt:
                        if event_name == "castle_machine":
                            phase_name = "Фаза 1 (Создание 🏗)" if i == 1 else "Фаза 2 (Розыгрыш призов 🎁)"
                        else:
                            phase_name = f"Фаза {i}"

                        logger.info(
                            f"[{event_name}] ✅ Акция активна — {phase_name} "
                            f"({start_dt:%d/%m %H:%M}–{end_dt:%d/%m %H:%M} UTC)"
                        )
                        active_phase = i
                        break
                except Exception as e:
                    logger.warning(f"[{event_name}] ⚠️ Ошибка парсинга сегмента {i}: {e}")

            if not active_phase:
                logger.warning(
                    f"[{event_name}] ⚠️ Все сегменты вне диапазона ({'; '.join(segments_info)}) | "
                    f"Сейчас: {now_server:%d/%m %H:%M} UTC"
                )
                return False

            return active_phase

        else:
            snippet = html_text[:400].replace("\n", " ")
            logger.warning(f"[{event_name}] ⚠️ Не найден диапазон дат в HTML. Фрагмент: {snippet}")
            return False

    # 🔍 Универсальная логика
    reason = _is_inactive_by_text(body_text)
    if reason:
        snippet = body_text[:300].replace("\n", " ")
        logger.warning(f"[{event_name}] ⚠️ Акция неактивна по тексту: '{reason}' → {snippet}")
        return False

    html_len = len(html_text)
    logger.debug(f"[{event_name}] HTML length: {html_len}")

    # 💬 если HTML короткий — логируем первые 200 символов содержимого
    if html_len < 1200:
        snippet = html_text[:200].replace("\n", " ").strip()
        logger.warning(f"[{event_name}] ⚠️ HTML короткий ({html_len} символов). Содержимое: {snippet}")
        return False

    # Проверяем наличие ключевых элементов
    key_markers = ["event-wrap", "reward", "puzzle", "lottery", "flip", "pair"]
    if not any(k in html_text for k in key_markers):
        logger.warning(f"[{event_name}] ⚠️ Нет ключевых элементов (reward/puzzle/lottery)")
        return False

    logger.info(f"[{event_name}] ✅ Акция активна (проверка пройдена)")
    return True

# ---------- Проверка всех событий ----------
async def check_all_events(bot=None, admin_id=None) -> Dict[str, bool]:
    logger.info("🚀 Начинаю проверку акций (через new_data)")
    results = {}

    for name in EVENTS.keys():
        logger.info(f"🔍 Проверяю акцию: {name}")
        try:
            active = await check_event_active(name)
            results[name] = active
        except Exception as e:
            logger.error(f"[event_checker] ❌ Ошибка при проверке {name}: {e}")
            results[name] = False
        await asyncio.sleep(1.5)

    STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info("✅ Проверка акций завершена")
    if bot and admin_id:
        summary = "📊 <b>Проверка акций завершена:</b>\n\n"
        for k, v in results.items():
            summary += f"{'✅' if v else '⚠️'} {k}\n"
        try:
            await bot.send_message(admin_id, summary, parse_mode="HTML")
        except Exception as e:
            logger.warning(f"[event_checker] ⚠️ Не удалось отправить отчёт админу: {e}")

    return results
async def get_active_events_list() -> list[str]:
    """Возвращает список активных акций по дате и HTML."""
    active = []
    for name, url in EVENTS.items():
        try:
            if await check_event_active(name):
                active.append(name)
        except Exception:
            continue
    return active
# ────────────────────────────────────────────────
# 🔍 Утилита для получения статуса акции из event_status.json
# ────────────────────────────────────────────────
async def get_event_status(event_name: str) -> bool:
    """
    Возвращает True, если указанная акция активна по данным event_status.json.
    Если файл отсутствует или формат неверный — возвращает False.
    """
    try:
        status_path = Path("data/event_status.json")
        if not status_path.exists():
            return False
        import json
        with open(status_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return bool(data.get(event_name))
    except Exception:
        return False
if __name__ == "__main__":
    import asyncio

    async def main():
        result = await check_event_active("puzzle2")
        print("Акция puzzle2 активна:" if result else "Акция puzzle2 не активна")

    asyncio.run(main())
