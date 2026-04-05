# tg_zov/services/farm_puzzles_auto.py
import asyncio
from contextlib import suppress
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Any, Dict

from aiogram import Bot

from config import ADMIN_IDS
from services.logger import logger
from services import puzzle2_auto
from services.event_checker import (
    STATUS_FILE as EVENT_STATUS_FILE,
    check_all_events,
    get_event_status,
)
from services.puzzle_files import (
    clear_puzzle_runtime_files,
)

IS_FARM_RUNNING = False  # 🔒 глобальный флаг, чтобы не запускать фарм повторно
FARM_TASK: Optional[asyncio.Task] = None  # 🔗 ссылка на текущий таск фарма

STATUS_MAX_AGE = timedelta(minutes=10)


def _is_status_fresh() -> bool:
    """Проверяет актуальность файла статуса акций."""
    status_path = Path(EVENT_STATUS_FILE)
    if not status_path.exists():
        return False

    try:
        mtime = datetime.fromtimestamp(status_path.stat().st_mtime)
    except OSError as e:
        logger.warning(f"[FARM] ⚠️ Не удалось получить mtime event_status.json: {e}")
        return False

    return datetime.now() - mtime < STATUS_MAX_AGE


async def ensure_puzzle_event_active(bot: Optional[Bot]) -> bool:
    """Убеждаемся, что пазловая акция активна перед запуском фарма."""

    is_active = await get_event_status("puzzle2")
    status_fresh = _is_status_fresh()

    if is_active and status_fresh:
        return True

    if not status_fresh:
        logger.info("[FARM] ℹ️ Обновляю статусы акций перед запуском пазлов…")
        try:
            await check_all_events(bot=bot)
        except Exception as e:
            logger.warning(f"[FARM] ⚠️ Не удалось обновить статусы акций: {e}")
        else:
            is_active = await get_event_status("puzzle2")
            if is_active:
                return True

    if not is_active:
        logger.info("[FARM] ⏸ Акция Puzzle2 не активна — фарм не запускаем.")
        clear_puzzle_runtime_files(reason="puzzle2_inactive")

    return is_active


def is_farm_running() -> bool:
    """Возвращает True, если фарм уже запущен."""
    return IS_FARM_RUNNING or (FARM_TASK is not None and not FARM_TASK.done())

def has_saved_state() -> bool:
    """Проверяет, есть ли сохранённое состояние для продолжения фарма."""
    return puzzle2_auto.FARM_STATE_FILE.exists()


async def start_farm(bot: Bot, resume: bool = False) -> bool:
    """Создаёт таск фарма, если он ещё не запущен."""
    global FARM_TASK
    if is_farm_running():
        return False

    if not resume:
        puzzle2_auto.reset_farm_state()

    FARM_TASK = asyncio.create_task(run_farm_puzzles_for_all(bot, resume=resume))
    return True


async def stop_farm(save_state: bool = False) -> bool:
    """Останавливает текущий фарм, если он выполняется."""
    global FARM_TASK

    if FARM_TASK is None:
        return False

    if FARM_TASK.done():
        FARM_TASK = None
        return False

    task = FARM_TASK
    if save_state:
        logger.info("[FARM] ⏸ Остановка фарма с сохранением состояния")
        puzzle2_auto.request_stop()
        with suppress(asyncio.CancelledError):
            await task
        FARM_TASK = None
        return True

    logger.info("[FARM] ⛔️ Принудительная остановка фарма")
    puzzle2_auto.request_stop()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    puzzle2_auto.reset_farm_state()
    puzzle2_auto.clear_stop_request()
    FARM_TASK = None

    return True


async def run_farm_puzzles_for_all(
    bot: Optional[Bot] = None,
    resume: bool = False,
) -> Dict[str, Any]:
    """
    🚀 Запускает фарм пазлов:
    - вызывает puzzle2_auto.main()
    - обновляет прогресс каждые 15 сек
    - по завершении отправляет итог
    """
    global IS_FARM_RUNNING, FARM_TASK

    current_task = asyncio.current_task()
    if FARM_TASK is None and current_task is not None:
        FARM_TASK = current_task

    is_active = await ensure_puzzle_event_active(bot)
    if not is_active:
        note = "⏸ <b>Puzzle2</b> ещё не активна. Фарм не запущен."
        if bot:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, note, parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"[FARM] ⚠️ Не удалось уведомить {admin_id}: {e}")
        FARM_TASK = None
        return {
            "success": False,
            "message": note,
            "duration_minutes": 0.0,
            "was_cancelled": False,
            "stop_requested": False,
            "summary": {},
            "error": None,
        }

    if not resume:
        clear_puzzle_runtime_files(reason="new_farm_start")
    if IS_FARM_RUNNING:
        note = "⚙️ Фарм уже выполняется, подожди окончания ⏳"
        if bot:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, note)
                except Exception:
                    pass
        return {
            "success": False,
            "message": note,
            "duration_minutes": 0.0,
            "was_cancelled": False,
            "stop_requested": True,
            "summary": {},
            "error": None,
        }

    IS_FARM_RUNNING = True
    start_time = datetime.now()
    logger.info("[FARM] 🚀 Запуск фарма пазлов")

    # Сообщаем всем админам о старте
    msg_map: Dict[int, Any] = {}
    if bot:
        for admin_id in ADMIN_IDS:
            try:
                msg = await bot.send_message(admin_id, "🧩 Фарм пазлов запущен! Собираем данные...")
                logger.info(
                    f"[FARM] 📩 Стартовое сообщение отправлено админу {admin_id}: id={msg.message_id}"
                )
                msg_map[admin_id] = msg
            except Exception as e:
                logger.warning(f"[FARM] Не удалось отправить стартовое сообщение админу {admin_id}: {e}")

    was_cancelled = False
    error: Optional[Exception] = None

    result: Dict[str, Any]

    try:
        await puzzle2_auto.main()
    except asyncio.CancelledError:
        was_cancelled = True
        logger.info("[FARM] 🛑 Получен сигнал на остановку фарма")
    except Exception as e:
        error = e
        logger.exception(f"[FARM] Ошибка во время puzzle2_auto.main(): {e}")
    finally:
        IS_FARM_RUNNING = False

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60
        logger.info(f"[FARM] ✅ Фарм завершён за {duration:.1f} мин.")

        stop_requested = was_cancelled or puzzle2_auto.is_stop_requested()
        if not stop_requested:
            puzzle2_auto.reset_farm_state()

        # Итоговое сообщение
        result_text = ""
        success = False

        if error is not None:
            result_text = (
                "❌ <b>Фарм пазлов завершился с ошибкой.</b>\n"
                f"<code>{error}</code>\n\n"
            )
        elif was_cancelled or stop_requested:
            result_text = (
                "🛑 <b>Фарм пазлов остановлен.</b>\n\n"
                f"🕓 В работе до остановки: <code>{duration:.1f} мин</code>"
            )
        else:
            result_text = (
                "✅ <b>Фарм пазлов завершён!</b>\n\n"
                f"🕓 Время выполнения: <code>{duration:.1f} мин</code>"
            )
            success = True

        if bot:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(admin_id, result_text, parse_mode="HTML")
                except Exception as e:
                    logger.warning(f"[FARM] Не удалось отправить итоги админу {admin_id}: {e}")

        FARM_TASK = None
        logger.info("[FARM] 📦 Фарм пазлов полностью завершён")

        result = {
            "success": success,
            "message": result_text,
            "duration_minutes": duration,
            "was_cancelled": was_cancelled,
            "stop_requested": stop_requested,
            "summary": {},
            "error": str(error) if error else None,
        }

    return result
