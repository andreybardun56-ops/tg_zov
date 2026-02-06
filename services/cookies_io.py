import json
import os
from config import COOKIES_FILE

def load_all_cookies() -> dict:
    """Загружает общий файл cookies.json"""
    if not os.path.exists(COOKIES_FILE):
        return {}
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_all_cookies(data: dict):
    """Сохраняет общий файл cookies.json"""
    os.makedirs(os.path.dirname(COOKIES_FILE), exist_ok=True)
    with open(COOKIES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
