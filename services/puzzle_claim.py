# tg_zov/services/puzzle_claim.py
"""
🎁 Выдача 30 ec_param кодов вручную (для админов)

Используется кнопкой "🎁 Получить 30 пазлов" в start.py.
Берёт 30 первых кодов из puzzle_data.jsonl, удаляет их из файла и
пишет запись в puzzle_claim.log.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger("puzzle_claim")

# Пути к файлам
PUZZLE_DATA_FILE = Path("data/puzzle_data.jsonl")
PUZZLE_CLAIM_LOG = Path("data/puzzle_claim.log")


# ─────────────────────────── helpers ───────────────────────────

def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    """Читает jsonl, где каждый блок — объект с ec_param или другим содержимым."""
    if not path.exists():
        return []
    blocks = []
    buf = ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    buf += line
                else:
                    if buf.strip():
                        try:
                            blocks.append(json.loads(buf))
                        except Exception:
                            pass
                        buf = ""
            if buf.strip():
                try:
                    blocks.append(json.loads(buf))
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"[PUZZLE_CLAIM] Ошибка чтения {path}: {e}")
    return blocks


def _write_jsonl(path: Path, blocks: List[Dict[str, Any]]):
    """Перезаписывает puzzle_data.jsonl атомарно."""
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for block in blocks:
            json.dump(block, f, ensure_ascii=False, indent=2)
            f.write("\n\n")
    os.replace(tmp, path)


def _append_log(user_id: int, count: int):
    """Добавляет запись о выдаче кодов в puzzle_claim.log"""
    PUZZLE_CLAIM_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(PUZZLE_CLAIM_LOG, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] user_id={user_id} получил {count} кодов\n")


# ─────────────────────────── main ───────────────────────────

async def issue_puzzle_codes(user_id: int) -> List[str]:
    """
    Выдаёт 30 ec_param кодов, удаляя их из puzzle_data.jsonl.
    Возвращает список выданных кодов.
    """
    if not PUZZLE_DATA_FILE.exists():
        logger.warning("[PUZZLE_CLAIM] Файл puzzle_data.jsonl не найден.")
        return []

    # 1️⃣ читаем все блоки
    blocks = _read_jsonl(PUZZLE_DATA_FILE)
    if not blocks:
        logger.info("[PUZZLE_CLAIM] Нет доступных кодов для выдачи.")
        return []

    # 2️⃣ собираем все ec_param из блоков
    all_codes: List[str] = []
    for block in blocks:
        if isinstance(block, dict):
            code = block.get("ec_param")
            if code and isinstance(code, str):
                all_codes.append(code)

    # 3️⃣ выдаём первые 30
    selected = all_codes[:30]
    if not selected:
        return []

    # 4️⃣ удаляем выданные из blocks
    remaining_blocks = [
        b for b in blocks if b.get("ec_param") not in selected
    ]

    # 5️⃣ записываем обновлённый файл
    try:
        _write_jsonl(PUZZLE_DATA_FILE, remaining_blocks)
        logger.info(f"[PUZZLE_CLAIM] Удалено {len(selected)} ec_param из puzzle_data.jsonl")
    except Exception as e:
        logger.error(f"[PUZZLE_CLAIM] Ошибка записи puzzle_data.jsonl: {e}")

    # 6️⃣ добавляем запись в лог
    _append_log(user_id, len(selected))

    return selected
