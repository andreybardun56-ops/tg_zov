# tg_zov/services/farm_puzzles_auto.py
import asyncio
import json
import os
from contextlib import suppress
from datetime import datetime
from typing import Optional

from aiogram import Bot

from config import ADMIN_IDS
from services.logger import logger
from services import puzzle2_auto

PUZZLE_SUMMARY = "data/puzzle_summary.json"
IS_FARM_RUNNING = False  # 🔒 глобальный флаг, чтобы не запускать фарм повторно
FARM_TASK: Optional[asyncio.Task] = None  # 🔗 ссылка на текущий таск фарма


async def read_puzzle_summary() -> dict:
    """Читает puzzle_summary.json и возвращает словарь статистики."""
    if not os.path.exists(PUZZLE_SUMMARY):
        return {}
    try:
        with open(PUZZLE_SUMMARY, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"[FARM] Ошибка чтения {PUZZLE_SUMMARY}: {e}")
        return {}


def format_puzzle_stats(data: dict) -> str:
    """Форматирует текст статистики пазлов для Telegram."""
    totals = data.get("totals", {})
    total_accs = data.get("accounts", 0)
    all_dup = data.get("all_duplicates", 0)

    parts = []
    for i in range(1, 10):
        cnt = totals.get(str(i), 0)
        parts.append(f"{i}🧩x{cnt}")
    progress_line = " | ".join(parts)

    text = (
        f"📊 <b>Фарм идёт...</b>\n"
        f"Аккаунтов обработано: <code>{total_accs}</code>\n"
        f"Всего дубликатов: <code>{all_dup}</code>\n"
        f"{progress_line}"
    )
    return text


def is_farm_running() -> bool:
    """Возвращает True, если фарм уже запущен."""
    return IS_FARM_RUNNING or (FARM_TASK is not None and not FARM_TASK.done())


async def start_farm(bot: Bot) -> bool:
    """Создаёт таск фарма, если он ещё не запущен."""
    global FARM_TASK
    if is_farm_running():
        return False

    FARM_TASK = asyncio.create_task(run_farm_puzzles_for_all(bot))
    return True


async def stop_farm() -> bool:
    """Останавливает текущий фарм, если он выполняется."""
    global FARM_TASK

    if FARM_TASK is None:
        return False

    if FARM_TASK.done():
        FARM_TASK = None
        return False

    FARM_TASK.cancel()
    try:
        await FARM_TASK
    except asyncio.CancelledError:
        pass
    finally:
        FARM_TASK = None

    return True


async def run_farm_puzzles_for_all(bot: Bot):
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

    # 🧹 Очистка старых данных перед новым запуском
    FILES_TO_CLEAR = [
        "data/puzzle_summary.json",
        "data/puzzle_data.jsonl",
    ]

    for path in FILES_TO_CLEAR:
        try:
            if os.path.exists(path):
                with open(path, "w", encoding="utf-8") as f:
                    f.write("{}" if path.endswith(".json") else "")
                logger.info(f"[FARM] 🧹 Файл {path} очищен перед запуском")
        except Exception as e:
            logger.warning(f"[FARM] ⚠️ Не удалось очистить {path}: {e}")
    # 🧹 Очистка старого файла статистики перед новым запуском
    try:
        if os.path.exists(PUZZLE_SUMMARY):
            with open(PUZZLE_SUMMARY, "w", encoding="utf-8") as f:
                json.dump({}, f)
            logger.info(f"[FARM] 🧹 Старый {PUZZLE_SUMMARY} очищен перед запуском")
    except Exception as e:
        logger.warning(f"[FARM] ⚠️ Не удалось очистить {PUZZLE_SUMMARY}: {e}")
    if IS_FARM_RUNNING:
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, "⚙️ Фарм уже выполняется, подожди окончания ⏳")
            except Exception:
                pass
        return

    IS_FARM_RUNNING = True
    start_time = datetime.now()
    logger.info("[FARM] 🚀 Запуск фарма пазлов")

    # Сообщаем всем админам о старте
    msg_map = {}
    for admin_id in ADMIN_IDS:
        try:
            msg = await bot.send_message(admin_id, "🧩 Фарм пазлов запущен! Собираем данные...")
            logger.info(f"[FARM] 📩 Стартовое сообщение отправлено админу {admin_id}: id={msg.message_id}")
            msg_map[admin_id] = msg
        except Exception as e:
            logger.warning(f"[FARM] Не удалось отправить стартовое сообщение админу {admin_id}: {e}")

    async def progress_updater():
        """Периодически обновляет сообщение с прогрессом."""
        first_update_done = False
        logger.info("[FARM] 🔁 Прогресс-обновитель запущен")
        while IS_FARM_RUNNING:
            logger.info("[FARM] 🔄 Проверка puzzle_summary...")
            try:
                data = await read_puzzle_summary()
                if data:
                    text = format_puzzle_stats(data)
                    logger.info(f"[FARM] 📈 Найдены данные: {data.get('accounts', 0)} аккаунтов")
                    for admin_id, msg in msg_map.items():
                        try:
                            await bot.edit_message_text(
                                text=text,
                                chat_id=str(admin_id),  # ✅ важно — теперь строка
                                message_id=msg.message_id,
                                parse_mode="HTML"
                            )
                            logger.info(f"[FARM] ✏️ Обновил сообщение для {admin_id}")
                        except Exception as e:
                            logger.warning(f"[FARM] ⚠️ Не удалось обновить сообщение {admin_id}: {e}")

                    if not first_update_done:
                        first_update_done = True
                        await asyncio.sleep(5)
                        continue

                await asyncio.sleep(15)
            except Exception as e:
                logger.warning(f"[FARM] ⚠️ Ошибка в обновлении прогресса: {e}")
                await asyncio.sleep(15)

    # 🔁 Запускаем фоновое обновление прогресса
    progress_task = asyncio.create_task(progress_updater())
    was_cancelled = False

    try:
        await puzzle2_auto.main()
    except asyncio.CancelledError:
        was_cancelled = True
        logger.info("[FARM] 🛑 Получен сигнал на остановку фарма")
    except Exception as e:
        logger.exception(f"[FARM] Ошибка во время puzzle2_auto.main(): {e}")
    finally:
        IS_FARM_RUNNING = False
        progress_task.cancel()
        with suppress(asyncio.CancelledError):
            await progress_task

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds() / 60
        logger.info(f"[FARM] ✅ Фарм завершён за {duration:.1f} мин.")

        # Итоговое сообщение
        data = await read_puzzle_summary()
        if was_cancelled:
            if data:
                text = (
                    "🛑 <b>Фарм пазлов остановлен.</b>\n\n"
                    + format_puzzle_stats(data)
                    + f"\n\n🕓 В работе до остановки: <code>{duration:.1f} мин</code>"
                )
            else:
                text = "🛑 Фарм пазлов остановлен пользователем."
        else:
            if data:
                text = (
                    "✅ <b>Фарм пазлов завершён!</b>\n\n"
                    + format_puzzle_stats(data)
                    + f"\n\n🕓 Время выполнения: <code>{duration:.1f} мин</code>"
                )
            else:
                text = f"⚠️ Фарм завершён, но не удалось прочитать {PUZZLE_SUMMARY}"

        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, text, parse_mode="HTML")
                if os.path.exists(PUZZLE_SUMMARY):
                    with open(PUZZLE_SUMMARY, "rb") as document:
                        await bot.send_document(admin_id, document=document)
            except Exception as e:
                logger.warning(f"[FARM] Не удалось отправить итоги админу {admin_id}: {e}")

        FARM_TASK = None
        logger.info("[FARM] 📦 Фарм пазлов полностью завершён")
