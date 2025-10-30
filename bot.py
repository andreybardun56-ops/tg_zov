# tg_zov/bot.py
import asyncio
import os
import datetime
from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties

# Всегда работаем из корня проекта
os.chdir(os.path.dirname(os.path.abspath(__file__)))

from config import BOT_TOKEN, ADMIN_IDS
from handlers import start, callback, accounts
from services.event_manager import run_full_event_cycle
from services.scheduler import ensure_scheduler_started, trigger_daily_flag
from services.event_checker import check_all_events  # ✅ для мгновенной проверки
from services.logger import logger, cleanup_old_logs, LOG_DIR  # ← добавить сюда импорт

# ────────────────────────────────────────────────
# ⚙️ Настройки автозапуска
# ────────────────────────────────────────────────
AUTO_RUN_ON_START = False      # 🚀 запускать проверку и фарм при старте
DAILY_ENABLED = False          # 🕛 включить ежедневный планировщик (00:02 МСК)

# ────────────────────────────────────────────────
# 🚀 on_startup
# ────────────────────────────────────────────────
async def on_startup(bot: Bot) -> None:
    """При старте бота уведомляет админов и включает планировщик."""
    # 🧹 Очистка старых логов (старше 3 дней)
    try:
        # гарантируем, что папка logs существует
        os.makedirs(LOG_DIR, exist_ok=True)

        cleanup_old_logs(3)
        logger.info("🧹 Очистка логов при старте: удалены файлы старше 3 дней")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка при очистке логов при старте: {e}")

    now = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
    primary_admin_id = ADMIN_IDS[0] if ADMIN_IDS else None

    # уведомление админов
    try:
        if ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                await bot.send_message(
                    admin_id,
                    f"✅ Бот успешно перезапущен и готов к работе!\n🕓 <b>{now}</b>",
                    parse_mode="HTML"
                )
            logger.info(f"✅ Бот успешно перезапущен ({now})")
        else:
            logger.warning("⚠️ Список ADMIN_IDS пуст — уведомления при старте пропущены")
    except Exception as e:
        logger.warning(f"⚠️ Ошибка уведомления админов: {e}")

    # 🔄 мгновенный автозапуск в фоне
    if AUTO_RUN_ON_START:
        async def background_start():
            try:
                logger.info("🚀 [BACKGROUND] Выполняю мгновенную проверку акций и автосбор…")

                # 🔍 Проверка акций
                await check_all_events(
                    bot=bot if primary_admin_id else None,
                    admin_id=primary_admin_id,
                )

                # 🎯 Фарм всех активных акций
                await run_full_event_cycle(bot=bot, manual=True)

                logger.info("✅ [BACKGROUND] Мгновенный автосбор завершён.")
                if primary_admin_id:
                    await bot.send_message(
                        primary_admin_id,
                        "✅ <b>Мгновенный автосбор завершён</b>\nБот готов к приёму команд.",
                        parse_mode="HTML"
                    )
            except Exception as e:
                logger.exception(f"❌ Ошибка при фоновом автозапуске: {e}")

        asyncio.create_task(background_start())
        logger.info("🧩 Мгновенный автосбор запущен в фоне.")
    else:
        logger.info("⏸️ Мгновенный автосбор при старте отключён.")

    # ежедневный планировщик (тоже в фоне)
    if DAILY_ENABLED:
        try:
            trigger_daily_flag(True)
            asyncio.create_task(ensure_scheduler_started(bot))
            logger.info("🕛 Ежедневный планировщик запущен в фоне.")
        except Exception as e:
            logger.exception(f"❌ Ошибка при запуске планировщика: {e}")
    else:
        logger.info("⏸️ Ежедневный планировщик отключён.")

# ────────────────────────────────────────────────
# 🧠 Основная функция
# ────────────────────────────────────────────────
async def main() -> None:
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    dp = Dispatcher()

    dp.include_router(start.router)
    dp.include_router(callback.router)
    dp.include_router(accounts.router)

    logger.info("✅ Бот запускается…")
    print("🚀 Бот запущен и готов к работе!")

    await on_startup(bot)
    await dp.start_polling(bot)

# ────────────────────────────────────────────────
# 🏁 Точка входа
# ────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("⏹️ Бот остановлен вручную")
