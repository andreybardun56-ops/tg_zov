from __future__ import annotations

import json
from pathlib import Path

from services.logger import logger

PUZZLE_SUMMARY_FILE = Path("data/puzzle_summary.json")
PUZZLE_DATA_FILE = Path("data/puzzle_data.jsonl")
PUZZLE_CLAIM_LOG_FILE = Path("data/puzzle_claim_log.json")

# Старые/лишние варианты, которые больше не используем.
LEGACY_PUZZLE_FILES: tuple[Path, ...] = (
    Path("data/puzzle_claim.log"),
)


def canonical_puzzle_files() -> tuple[Path, Path, Path]:
    return (PUZZLE_CLAIM_LOG_FILE, PUZZLE_DATA_FILE, PUZZLE_SUMMARY_FILE)


def clear_puzzle_runtime_files(reason: str = "") -> None:
    """
    Очищает рабочие файлы пазлов.
    - puzzle_claim_log.json -> {}
    - puzzle_data.jsonl -> пусто
    - puzzle_summary.json -> {}
    """
    for path in canonical_puzzle_files():
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.suffix == ".json":
                with path.open("w", encoding="utf-8") as f:
                    json.dump({}, f, ensure_ascii=False, indent=2)
            else:
                path.write_text("", encoding="utf-8")
            logger.info("[PUZZLE-FILES] 🧹 Очищен %s (%s)", path, reason or "без причины")
        except Exception as exc:
            logger.warning("[PUZZLE-FILES] ⚠️ Не удалось очистить %s: %s", path, exc)

    _cleanup_legacy_files(reason=reason)


def _cleanup_legacy_files(reason: str = "") -> None:
    for path in LEGACY_PUZZLE_FILES:
        try:
            if path.exists():
                path.unlink()
                logger.info("[PUZZLE-FILES] 🗑️ Удалён legacy файл %s (%s)", path, reason or "без причины")
        except Exception as exc:
            logger.warning("[PUZZLE-FILES] ⚠️ Не удалось удалить legacy файл %s: %s", path, exc)
