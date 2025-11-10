# tg_zov/services/puzzle2_bundle.py
import asyncio
import logging

from services.farm_puzzles_auto import run_farm_puzzles_for_all
from services import puzzle2_auto  # это твой файл services/puzzle2_auto.py

logger = logging.getLogger("puzzle2_bundle")

async def run_puzzle2_all_sources(user_id=None, uid=None, bot=None):
    """
    Запускает оба источника сбора пазлов:
    1) run_farm_puzzles_for_all (аккаунты из базы бота)
    2) puzzle2_auto.main() (аккаунты из new_data*.json)

    user_id/uid игнорируются puzzle2_auto, но важны для run_farm_puzzles_for_all.
    """
    results = []

    # ── 1) базы бота
    try:
        logger.info("🧩 [bundle] Запуск run_farm_puzzles_for_all (база бота)")
        r1 = await run_farm_puzzles_for_all(bot)
        results.append(("farm_puzzles_for_all", True, r1.get("message", "OK")))
    except Exception as e:
        logger.exception("❌ [bundle] Ошибка run_farm_puzzles_for_all: %s", e)
        results.append(("farm_puzzles_for_all", False, f"Ошибка: {e}"))

    # ── 2) new_data*.json (глобально, для всех там указанных аккаунтов)
    try:
        logger.info("🧩 [bundle] Запуск puzzle2_auto.main() (new_data*.json)")
        await puzzle2_auto.main()
        results.append(("puzzle2_auto", True, "puzzle2_auto завершён"))
    except Exception as e:
        logger.exception("❌ [bundle] Ошибка puzzle2_auto.main(): %s", e)
        results.append(("puzzle2_auto", False, f"Ошибка: {e}"))

    # Итог
    ok = all(s for _, s, _ in results)
    txt = "\n".join([f"{'✅' if s else '⚠️'} {name}: {msg}" for name, s, msg in results])
    return {"success": ok, "message": txt}
