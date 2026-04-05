# tg_zov/services/puzzle_claim.py
"""
🎁 Выдача 30 ec_param кодов вручную (для админов)

Используется кнопкой "🎁 Получить 30 пазлов" в start.py.
Берёт 30 первых кодов из puzzle_data.jsonl, удаляет их из файла и
пишет запись в puzzle_claim_log.json.
"""

import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from services.puzzle_files import PUZZLE_CLAIM_LOG_FILE, PUZZLE_DATA_FILE

logger = logging.getLogger("puzzle_claim")

PUZZLE_CLAIM_LOG = PUZZLE_CLAIM_LOG_FILE


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


def _load_claim_log() -> Dict[str, Any]:
    if not PUZZLE_CLAIM_LOG.exists():
        return {}
    try:
        with open(PUZZLE_CLAIM_LOG, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_claim_log(data: Dict[str, Any]) -> None:
    PUZZLE_CLAIM_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(PUZZLE_CLAIM_LOG, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _append_log(user_id: int, count: int, user_name: str | None = None, user_tag: str | None = None):
    """Добавляет запись о выдаче кодов в puzzle_claim_log.json"""
    log_data = _load_claim_log()
    key = str(user_id)
    user_entry = log_data.setdefault(
        key,
        {
            "count": 0,
            "claimed_puzzles": [],
            "claimed_ec_params": [],
            "history": [],
            "tg_name": "",
            "tg_tag": "",
        },
    )

    if user_name:
        user_entry["tg_name"] = user_name
    if user_tag:
        user_entry["tg_tag"] = user_tag.lstrip("@")
    user_entry["count"] = int(user_entry.get("count", 0)) + int(count)
    user_entry.setdefault("history", []).append(
        {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "action": "claim_30_codes",
            "count": count,
        }
    )
    _save_claim_log(log_data)


def _append_specific_log(
    user_id: int,
    puzzle_id: int,
    ec_param: str,
    user_name: str | None = None,
    user_tag: str | None = None,
):
    log_data = _load_claim_log()
    key = str(user_id)
    user_entry = log_data.setdefault(
        key,
        {
            "count": 0,
            "claimed_puzzles": [],
            "claimed_ec_params": [],
            "history": [],
            "tg_name": "",
            "tg_tag": "",
        },
    )

    if user_name:
        user_entry["tg_name"] = user_name
    if user_tag:
        user_entry["tg_tag"] = user_tag.lstrip("@")

    claimed = user_entry.setdefault("claimed_ec_params", [])
    if ec_param not in claimed:
        claimed.append(ec_param)

    claimed_puzzles = user_entry.setdefault("claimed_puzzles", [])
    if puzzle_id not in claimed_puzzles:
        claimed_puzzles.append(puzzle_id)

    user_entry.setdefault("history", []).append(
        {
            "ts": datetime.now().isoformat(timespec="seconds"),
            "action": "claim_specific_puzzle",
            "puzzle_id": puzzle_id,
            "ec_param": ec_param,
        }
    )
    _save_claim_log(log_data)


def _has_claim_record(user_id: int, ec_param: str) -> bool:
    """Проверяет, получал ли уже пользователь конкретный ec_param."""
    data = _load_claim_log()
    user_entry = data.get(str(user_id), {})
    claimed_codes = user_entry.get("claimed_ec_params", [])
    return ec_param in claimed_codes


# ─────────────────────────── main ───────────────────────────

async def issue_puzzle_codes(
    user_id: int,
    user_name: str | None = None,
    user_tag: str | None = None,
) -> List[str]:
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
    _append_log(user_id, len(selected), user_name=user_name, user_tag=user_tag)

    return selected


async def issue_specific_puzzle(
    user_id: int,
    puzzle_id: int,
    user_name: str | None = None,
    user_tag: str | None = None,
) -> Optional[str]:
    """Выдаёт конкретный пазл (1–9) из puzzle_data.jsonl."""
    if not PUZZLE_DATA_FILE.exists():
        return None

    puzzle_key = str(puzzle_id)
    blocks = _read_jsonl(PUZZLE_DATA_FILE)
    if not blocks:
        return None

    selected_index: Optional[int] = None
    selected_block: Optional[Dict[str, Any]] = None

    for idx, block in enumerate(blocks):
        if not isinstance(block, dict):
            continue
        puzzles = block.get("puzzle")
        ec_param = block.get("ec_param")
        if not ec_param or not isinstance(puzzles, dict):
            continue
        available = puzzles.get(puzzle_key)
        if available is None:
            continue
        try:
            available_int = int(available)
        except (ValueError, TypeError):
            continue
        if available_int < 1:
            continue
        if _has_claim_record(user_id, ec_param):
            continue

        selected_index = idx
        selected_block = block
        break

    if selected_index is None or selected_block is None:
        return None

    puzzles: Dict[str, Any] = selected_block.get("puzzle", {})
    new_value = int(puzzles.get(puzzle_key, 0)) - 1
    if new_value <= 0:
        puzzles.pop(puzzle_key, None)
    else:
        puzzles[puzzle_key] = new_value

    if not puzzles:
        del blocks[selected_index]
    else:
        blocks[selected_index]["puzzle"] = puzzles

    try:
        _write_jsonl(PUZZLE_DATA_FILE, blocks)
    except Exception as e:
        logger.error(f"[PUZZLE_CLAIM] Ошибка записи puzzle_data.jsonl: {e}")
        return None

    ec_param = selected_block.get("ec_param")
    if not isinstance(ec_param, str):
        return None

    _append_specific_log(
        user_id,
        puzzle_id,
        ec_param,
        user_name=user_name,
        user_tag=user_tag,
    )
    return ec_param
