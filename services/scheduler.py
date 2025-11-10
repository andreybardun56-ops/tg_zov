import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

from services.farm_puzzles_auto import run_farm_puzzles_for_all
from services.event_checker import check_all_events
from services.logger import logger
from config import ADMIN_IDS

_scheduler_started = False
_daily_enabled = False

def trigger_daily_flag(value: bool):
    """Включить/выключить автозапуск в 00:02 МСК (ставится после ручного старта впервые)."""
    global _daily_enabled
    _daily_enabled = value
    logger.info(f"[SCHED] daily_enabled={_daily_enabled}")

async def _sleep_until(dt: datetime):
    now = datetime.now(dt.tzinfo)
    seconds = max(0, (dt - now).total_seconds())
    await asyncio.sleep(seconds)

def _next_msk_0002() -> datetime:
    tz = ZoneInfo("Europe/Moscow")
    now = datetime.now(tz)
    target = now.replace(hour=8, minute=2, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target

async def _loop(bot=None):
    """Главный цикл планировщика — ежедневный фарм + проверка акций."""
    global _daily_enabled
    while True:
        target = _next_msk_0002()
        logger.info(f"[SCHED] Next run at {target.isoformat()}")
        await _sleep_until(target)

        if not _daily_enabled:
            logger.info("[SCHED] Пропуск ежедневных задач (disabled)")
            continue

        logger.info("[SCHED] 🧩 Запуск ежедневных задач...")

        # === 🧩 Проверка активных акций ===
        try:
            admin_id = ADMIN_IDS[0] if ADMIN_IDS else None
            if admin_id:
                results = await check_all_events(admin_id)
                summary = "📊 <b>Обновление статуса акций:</b>\n\n"
                for name, active in results.items():
                    emoji = "✅" if active else "⚠️"
                    summary += f"{emoji} {name}\n"

                if bot:
                    try:
                        await bot.send_message(admin_id, summary, parse_mode="HTML")
                    except Exception as e:
                        logger.warning(f"[SCHED] Не удалось отправить отчёт админу: {e}")

                logger.info("[SCHED] ✅ Проверка акций завершена")
            else:
                logger.warning("[SCHED] Нет ADMIN_IDS, пропускаю проверку акций.")
        except Exception as e:
            logger.exception(f"[SCHED] Ошибка при проверке акций: {e}")

        # === 🚀 Автоматический фарм пазлов ===
        try:
            logger.info("[SCHED] 🚀 Запуск farm_puzzles_for_all...")
            await run_farm_puzzles_for_all(bot)
            logger.info("[SCHED] ✅ Ежедневный фарм завершён")
        except Exception as e:
            logger.exception(f"[SCHED] Ошибка при запуске фарма: {e}")
        # === 🚀 Автоматический фарм пазлов ===
        try:
            logger.info("[SCHED] 🚀 Запуск farm_puzzles_for_all...")
            await run_farm_puzzles_for_all(bot)
            logger.info("[SCHED] ✅ Ежедневный фарм завершён")
        except Exception as e:
            logger.exception(f"[SCHED] Ошибка при запуске фарма: {e}")

async def ensure_scheduler_started(bot=None):
    """Гарантирует, что планировщик запущен только один раз."""
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True
    asyncio.create_task(_loop(bot))
    logger.info("[SCHED] started")
