# tg_zov/services/logger.py
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# 📁 Абсолютный путь к папке logs
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# 📄 Имя файла по дате (основной файл)
LOG_FILE = os.path.join(LOG_DIR, f"{datetime.now():%Y-%m-%d}.log")

# ⚙️ Настройка логирования
logger = logging.getLogger("tg_zov")
logger.setLevel(logging.INFO)

# ⚠️ Добавляем хендлер только если ещё нет
if not logger.handlers:
    handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=2 * 1024 * 1024,  # 2 МБ
        backupCount=5,              # количество старых файлов
        encoding="utf-8"
    )
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

logger.propagate = False  # чтобы не дублировать в корневой логгер

import time

def cleanup_old_logs(days: int = 3):
    """🧹 Удаляет лог-файлы старше указанного количества дней"""
    now = time.time()
    deleted = 0

    if not os.path.exists(LOG_DIR):
        return

    for file in os.listdir(LOG_DIR):
        if not file.endswith(".log"):
            continue
        path = os.path.join(LOG_DIR, file)
        try:
            if os.path.isfile(path):
                age_days = (now - os.path.getmtime(path)) / 86400
                if age_days > days:
                    os.remove(path)
                    deleted += 1
        except Exception as e:
            logger.warning(f"⚠️ Не удалось удалить лог {file}: {e}")

    if deleted:
        logger.info(f"🧹 Удалено старых логов: {deleted}")
