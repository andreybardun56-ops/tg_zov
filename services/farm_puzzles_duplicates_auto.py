# tg_zov/services/farm_puzzles_duplicates_auto.py
import asyncio
import json
import os
from contextlib import suppress
from datetime import datetime, timedelta
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from typing import Optional, Any, Dict

from aiogram import Bot
from aiogram.types import FSInputFile

from config import ADMIN_IDS
from services.event_checker import (
    STATUS_FILE as EVENT_STATUS_FILE,
    check_all_events,
    get_event_status,
)
from services.logger import logger

DUPES_SUMMARY = "data/puzzle_summary.json"
DUPES_DATA = "data/puzzle_data.jsonl"

IS_FARM_RUNNING = False
FARM_TASK: Optional[asyncio.Task] = None

STATUS_MAX_AGE = timedelta(minutes=10)
_DUPES_MODULE = None


def _load_dupes_module():
    global _DUPES_MODULE
    if _DUPES_MODULE is not None:
        return _DUPES_MODULE
    module_path = Path(__file__).with_name("puzzle3_auto.py")
    loader = SourceFileLoader("puzzle3_auto", str(module_path))
    spec = spec_from_loader(loader.name, loader)
    module = module_from_spec(spec)
    loader.exec_module(module)
    _DUPES_MODULE = module
    return module


def _is_status_fresh() -> bool:
    """Проверяет актуальность файла статуса акций."""
    status_path = Path(EVENT_STATUS_FILE)
    if not status_path.exists():
        return False

    try:
        mtime = datetime.fromtimestamp(status_path.stat().st_mtime)
    except OSError as e:
        logger.warning(f"[FARM-DUPES] ⚠️ Не удалось получить mtime event_status.json: {e}")
        return False

    return datetime.now() - mtime < STATUS_MAX_AGE


async def ensure_puzzle_event_active(bot: Optional[Bot]) -> bool:
    """Убеждаемся, что пазловая акция активна перед запуском фарма дублей."""
    is_active = await get_event_status("puzzle2")
    status_fresh = _is_status_fresh()

    if is_active and status_fresh:
        return True

    if not status_fresh:
        logger.info("[FARM-DUPES] ℹ️ Обновляю статусы акций перед запуском пазлов…")
        try:
            await check_all_events(bot=bot)
        except Exception as e:
            logger.warning(f"[FARM-DUPES] ⚠️ Не удалось обновить статусы акций: {e}")
        else:
            is_active = await get_event_status("puzzle2")
            if is_active:
                return True

    if not is_active:
        logger.info("[FARM-DUPES] ⏸ Акция Puzzle2 не активна — фарм дублей не запускаем.")

    return is_active


async def read_dupes_summary() -> dict:
    """Читает файл со статистикой дублей."""
    if not os.path.exists(DUPES_SUMMARY):
        return {}
    try:
        with open(DUPES_SUMMARY, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"[FARM-DUPES] Ошибка чтения {DUPES_SUMMARY}: {e}")
        return {}


def format_dupes_stats(data: dict) -> str:
    """Форматирует статистику дублей для Telegram."""
    totals = data.get("totals", {})
    total_accs = data.get("accounts", 0)
    all_dup = data.get("all_duplicates", 0)

    parts = []
    for i in range(1, 10):
        cnt = totals.get(str(i), 0)
        parts.append(f"{i}🧩x{cnt}")
    progress_line = " | ".join(parts)

    text = (
        f"📊 <b>Фарм дублей...</b>\n"
        f"Аккаунтов обработано: <code>{total_accs}</code>\n"
        f"Всего дубликатов: <code>{all_dup}</code>\n"
        f"{progress_line}"
    )
    return text


def is_farm_running() -> bool:
    return IS_FARM_RUNNING or (FARM_TASK is not None and not FARM_TASK.done())


async def start_farm(bot: Bot) -> bool:
    global FARM_TASK
    if is_farm_running():
        return False
    FARM_TASK = asyncio.create_task(run_farm_duplicates(bot))
    return True


async def stop_farm() -> bool:
    global FARM_TASK
    if FARM_TASK is None or FARM_TASK.done():
        FARM_TASK = None
        return False
    module = _load_dupes_module()
    module.request_stop()
    FARM_TASK.cancel()
    with suppress(asyncio.CancelledError):
        await FARM_TASK
    FARM_TASK = None
    return True


async def run_farm_duplicates(bot: Optional[Bot] = None) -> Dict[str, Any]:
    global IS_FARM_RUNNING, FARM_TASK
    current_task = asyncio.current_task()
    if FARM_TASK is None and current_task is not None:
        FARM_TASK = current_task

    is_active = await ensure_puzzle_event_active(bot)
    if not is_active:
        note = "⏸ <b>Puzzle2</b> ещё не активна. Фарм дублей не запущен."
        if bot:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, note, parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"[FARM-DUPES] ⚠️ Не удалось уведомить {admin_id}: {e}")
        FARM_TASK = None
        return {
            "success": False,
            "message": note,
            "duration_minutes": 0.0,
            "was_cancelled": False,
            "summary": {},
            "error": None,
        }

    for path in (DUPES_SUMMARY, DUPES_DATA):
        try:
            if os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    f.write("{}" if path.endswith(".json") else "")
                logger.info(f"[FARM-DUPES] 🧹 Файл {path} очищен перед запуском")
        except Exception as e:
            logger.warning(f"[FARM-DUPES] ⚠️ Не удалось очистить {path}: {e}")

    IS_FARM_RUNNING = True
    start_time = datetime.now()
    logger.info("[FARM-DUPES] 🚀 Запуск фарма дублей")

    msg_map: Dict[int, Any] = {}
    if bot:
        for admin_id in ADMIN_IDS:
            try:
                msg = await bot.send_message(admin_id, "🧩 Фарм дублей запущен! Собираем данные...")
                msg_map[admin_id] = msg
            except Exception as e:
                logger.warning(f"[FARM-DUPES] Не удалось отправить стартовое сообщение админу {admin_id}: {e}")

    last_texts: Dict[int, str] = {}

    async def progress_updater():
        while IS_FARM_RUNNING:
            if not bot or not msg_map:
                await asyncio.sleep(15)
                continue
            try:
                data = await read_dupes_summary()
                if data:
                    text = format_dupes_stats(data)
                    for admin_id, msg in msg_map.items():
                        if last_texts.get(admin_id) == text:
                            continue
                        try:
                            await bot.edit_message_text(
                                text=text,
                                chat_id=str(admin_id),
                                message_id=msg.message_id,
                                parse_mode="HTML",
                            )
                            last_texts[admin_id] = text
                        except Exception as e:
                            if "message is not modified" in str(e):
                                last_texts[admin_id] = text
                                continue
                            logger.warning(f"[FARM-DUPES] ⚠️ Не удалось обновить сообщение {admin_id}: {e}")
                await asyncio.sleep(15)
            except Exception as e:
                logger.warning(f"[FARM-DUPES] ⚠️ Ошибка в обновлении прогресса: {e}")
                await asyncio.sleep(15)

    progress_task = asyncio.create_task(progress_updater())
    was_cancelled = False
    error: Optional[Exception] = None
    result: Dict[str, Any]

    try:
        module = _load_dupes_module()
        await module.main()
    except asyncio.CancelledError:
        was_cancelled = True
        logger.info("[FARM-DUPES] 🛑 Получен сигнал на остановку фарма дублей")
    except Exception as e:
        error = e
        logger.exception(f"[FARM-DUPES] Ошибка во время фарма дублей: {e}")
    finally:
        IS_FARM_RUNNING = False
        progress_task.cancel()
        with suppress(asyncio.CancelledError):
            await progress_task

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60
        logger.info(f"[FARM-DUPES] ✅ Фарм дублей завершён за {duration:.1f} мин.")

        data = await read_dupes_summary()
        result_text = ""
        success = False

        if error is not None:
            text = (
                "❌ <b>Фарм дублей завершился с ошибкой.</b>\n"
                f"<code>{error}</code>\n\n"
            )
            if data:
                text += format_dupes_stats(data)
            else:
                text += f"⚠️ Нет данных в {DUPES_SUMMARY}"
            result_text = text
        elif was_cancelled:
            if data:
                text = (
                    "🛑 <b>Фарм дублей остановлен.</b>\n\n"
                    + format_dupes_stats(data)
                    + f"\n\n🕓 В работе до остановки: <code>{duration:.1f} мин</code>"
                )
            else:
                text = "🛑 Фарм дублей остановлен пользователем."
            result_text = text
        else:
            if data:
                text = (
                    "✅ <b>Фарм дублей завершён!</b>\n\n"
                    + format_dupes_stats(data)
                    + f"\n\n🕓 Время выполнения: <code>{duration:.1f} мин</code>"
                )
            else:
                text = f"⚠️ Фарм дублей завершён, но не удалось прочитать {DUPES_SUMMARY}"
            result_text = text
            success = True

        if bot:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, result_text, parse_mode="HTML")
                    if os.path.exists(DUPES_SUMMARY):
                        document = FSInputFile(DUPES_SUMMARY)
                        await bot.send_document(admin_id, document=document)
                except Exception as e:
                    logger.warning(f"[FARM-DUPES] Не удалось отправить итоги админу {admin_id}: {e}")

        FARM_TASK = None
        result = {
            "success": success,
            "message": result_text,
            "duration_minutes": duration,
            "was_cancelled": was_cancelled,
            "summary": data or {},
            "error": str(error) if error else None,
        }

    return result