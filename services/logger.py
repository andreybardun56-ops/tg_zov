# tg_zov/services/logger.py
import logging
import os
from datetime import datetime

# 📁 Абсолютный путь к папке logs
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# 📄 Имя файла по дате
LOG_FILE = os.path.join(LOG_DIR, f"{datetime.now():%Y-%m-%d}.log")

# ⚙️ Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        # ✅ Только в файл, без вывода в консоль
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)

logger = logging.getLogger(__name__)
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
            logging.warning(f"⚠️ Не удалось удалить лог {file}: {e}")

    if deleted:
        logging.info(f"🧹 Удалено старых логов: {deleted}")
