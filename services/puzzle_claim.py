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
from typing import List, Dict, Any, Optional

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


def _append_log(user_id: int, count: int, user_name: str | None = None, user_tag: str | None = None):
    """Добавляет запись о выдаче кодов в puzzle_claim.log"""
    safe_name = user_name.replace(" ", "_") if user_name else ""
    safe_tag = user_tag.lstrip("@") if user_tag else ""
    PUZZLE_CLAIM_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(PUZZLE_CLAIM_LOG, "a", encoding="utf-8") as f:
        f.write(
            f"[{datetime.now():%Y-%m-%d %H:%M:%S}] user_id={user_id} "
            f"tg_name={safe_name} tg_tag={safe_tag} получил {count} кодов\n"
        )


def _append_specific_log(
    user_id: int,
    puzzle_id: int,
    ec_param: str,
    user_name: str | None = None,
    user_tag: str | None = None,
):
    safe_name = user_name.replace(" ", "_") if user_name else ""
    safe_tag = user_tag.lstrip("@") if user_tag else ""
    PUZZLE_CLAIM_LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(PUZZLE_CLAIM_LOG, "a", encoding="utf-8") as f:
        f.write(
            f"[{datetime.now():%Y-%m-%d %H:%M:%S}] user_id={user_id} "
            f"tg_name={safe_name} tg_tag={safe_tag} получал "
            f"ec_param={ec_param} puzzle_id={puzzle_id}\n"
        )


def _parse_log_tokens(line: str) -> Dict[str, str]:
    """Извлекает пары ключ=значение из строки лога."""
    tokens: Dict[str, str] = {}
    for chunk in line.strip().split():
        if "=" not in chunk:
            continue
        key, value = chunk.split("=", 1)
        tokens[key.strip()] = value.strip()
    return tokens


def _has_claim_record(user_id: int, ec_param: str) -> bool:
    """Проверяет, получал ли уже пользователь конкретный ec_param."""
    if not PUZZLE_CLAIM_LOG.exists():
        return False

    try:
        with open(PUZZLE_CLAIM_LOG, "r", encoding="utf-8") as f:
            for line in f:
                if "ec_param=" not in line:
                    continue
                tokens = _parse_log_tokens(line)
                if (
                    tokens.get("user_id") == str(user_id)
                    and tokens.get("ec_param") == ec_param
                ):
                    return True
    except Exception as e:
        logger.warning(f"[PUZZLE_CLAIM] Ошибка чтения puzzle_claim.log: {e}")
    return False


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