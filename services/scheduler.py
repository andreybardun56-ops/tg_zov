import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import logging

from services.event_manager import run_full_event_cycle
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

        try:
            logger.info("[SCHED] 🚀 Полный цикл событий…")
            await run_full_event_cycle(bot=bot)
            logger.info("[SCHED] ✅ Цикл событий завершён")
        except Exception as e:
            logger.exception(f"[SCHED] Ошибка во время цикла событий: {e}")

async def ensure_scheduler_started(bot=None):
    """Гарантирует, что планировщик запущен только один раз."""
    global _scheduler_started
    if _scheduler_started:
        return
    _scheduler_started = True
    asyncio.create_task(_loop(bot))
    logger.info("[SCHED] started")