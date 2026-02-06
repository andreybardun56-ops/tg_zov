# tg_zov/services/puzzle_claim_auto2.py

import os
import json
import asyncio
import random
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from playwright.async_api import async_playwright
from html import escape

from services.logger import logger
from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    cookies_to_playwright,
    launch_masked_persistent_context,
    humanize_pre_action,
)

# ================== PATHS ==================
COOKIES_FILE = Path("data/cookies.json")
PUZZLE_DATA_FILE = Path("data/puzzle_data.jsonl")
PUZZLE_SUMMARY_FILE = Path("data/puzzle_summary.json")
PUZZLE_CLAIM_LOG = Path("data/puzzle_claim_log.json")
PROFILE_DIR = Path("data/chrome_profiles")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

EVENT_PAGE = "https://event-eu-cc.igg.com/event/puzzle2/"
EVENT_API = f"{EVENT_PAGE}ajax.req.php"

PUZZLE_ORDER = [1, 2, 3, 4, 5, 6, 7, 8, 9]

# ================== HELPERS ==================
def load_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default

def save_json(path: Path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)

def parse_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    blocks, buf = [], ""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            buf += line
        else:
            if buf.strip():
                blocks.append(json.loads(buf))
            buf = ""
    if buf.strip():
        blocks.append(json.loads(buf))
    return blocks

def write_jsonl(path: Path, blocks: List[Dict[str, Any]]):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for b in blocks:
            json.dump(b, f, ensure_ascii=False, indent=2)
            f.write("\n\n")
    os.replace(tmp, path)

def find_donor(puzzle_id: int, exclude: set) -> Optional[Tuple[Dict, int]]:
    blocks = parse_jsonl(PUZZLE_DATA_FILE)
    for i, b in enumerate(blocks):
        if b.get("iggid") in exclude:
            continue
        if str(puzzle_id) in b.get("puzzle", {}):
            return b, i
    return None

# ================== AUTO CLAIM PUZZLE ==================
async def auto_claim_puzzle2(user_id: str, bot, target_iggid: Optional[str] = None, amount: int = 30) -> bool:
    """
    Автоматический сбор пазлов для одного аккаунта.
    target_iggid можно указать, чтобы выбрать конкретный аккаунт.
    """
    try:
        tg_user_id = str(user_id)
        claim_log = load_json(PUZZLE_CLAIM_LOG, {})
        user_entry = claim_log.setdefault("users", {}).setdefault(tg_user_id, {})

        received_total = 0
        puzzle_idx = 0

        # Загружаем cookies пользователя
        cookies_db = load_json(COOKIES_FILE, {})
        accounts = cookies_db.get(tg_user_id)
        if not accounts:
            await bot.send_message(tg_user_id, "⚠️ Нет cookies для пользователя.")
            return False

        # Берём нужный аккаунт или первый доступный
        if target_iggid and target_iggid in accounts:
            acc_cookies = accounts[target_iggid]
        else:
            target_iggid = list(accounts.keys())[0]
            acc_cookies = accounts[target_iggid]

        amount = min(amount, 30)

        async with async_playwright() as p:
            ctx_info = await launch_masked_persistent_context(
                p,
                user_data_dir=str(PROFILE_DIR / target_iggid),
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=50,
                profile=get_random_browser_profile()
            )
            context, page = ctx_info["context"], ctx_info["page"]

            await context.add_cookies(cookies_to_playwright(acc_cookies))
            await page.goto(EVENT_PAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1.5)
            await humanize_pre_action(page)

            while received_total < amount and user_entry.get("count", 0) < 30:
                puzzle_id = PUZZLE_ORDER[puzzle_idx % 9]
                puzzle_idx += 1

                retries = 0
                success = False
                last_error = None

                while retries < 3 and not success:
                    donor_data = find_donor(puzzle_id, set(user_entry.get("donors", [])))
                    if not donor_data:
                        break

                    donor, donor_index = donor_data
                    donor_iggid = donor["iggid"]

                    url = f"{EVENT_API}?action=claim_friend_puzzle&friend_iggid={donor_iggid}&puzzle={puzzle_id}"

                    resp = await page.evaluate(f"""
                        async () => {{
                            const r = await fetch("{url}", {{
                                method: "GET",
                                credentials: "include",
                                headers: {{
                                    "X-Requested-With": "XMLHttpRequest",
                                    "Referer": "{EVENT_PAGE}"
                                }}
                            }});
                            return {{status: r.status, text: await r.text()}};
                        }}
                    """)

                    try:
                        data = json.loads(resp["text"])
                    except Exception:
                        data = {}

                    if data.get("status") == 1:
                        success = True

                        # --- update puzzle_data ---
                        blocks = parse_jsonl(PUZZLE_DATA_FILE)
                        puzzles = blocks[donor_index]["puzzle"]
                        puzzles[str(puzzle_id)] -= 1
                        if puzzles[str(puzzle_id)] <= 0:
                            puzzles.pop(str(puzzle_id))
                        if not puzzles:
                            blocks.pop(donor_index)
                        write_jsonl(PUZZLE_DATA_FILE, blocks)

                        # --- update summary ---
                        summary = load_json(PUZZLE_SUMMARY_FILE, {"totals": {}, "all_duplicates": 0})
                        summary["totals"][str(puzzle_id)] = summary["totals"].get(str(puzzle_id), 1) - 1
                        summary["all_duplicates"] = summary.get("all_duplicates", 1) - 1
                        save_json(PUZZLE_SUMMARY_FILE, summary)

                        user_entry.setdefault("donors", []).append(donor_iggid)
                        user_entry["count"] = user_entry.get("count", 0) + 1
                        save_json(PUZZLE_CLAIM_LOG, claim_log)

                        received_total += 1
                        await asyncio.sleep(random.uniform(1.5, 3.0))
                        break

                    else:
                        last_error = data.get("error")
                        user_entry.setdefault("donors", []).append(donor_iggid)
                        save_json(PUZZLE_CLAIM_LOG, claim_log)
                        retries += 1

                        if last_error == 5:
                            user_entry["count"] = 30
                            save_json(PUZZLE_CLAIM_LOG, claim_log)
                            await bot.send_message(
                                tg_user_id,
                                f"🚫 Аккаунт <code>{target_iggid}</code> достиг лимита 30 пазлов.",
                                parse_mode="HTML"
                            )
                            success = True
                            break

                if not success:
                    await bot.send_message(
                        tg_user_id,
                        f"❌ Ошибка получения пазла {puzzle_id}\nКод ошибки: {last_error}",
                        parse_mode="HTML"
                    )
                    break

            await bot.send_message(
                tg_user_id,
                f"✅ Все пазлы собраны\nПолучено: <b>{user_entry.get('count', 0)}</b> / 30",
                parse_mode="HTML"
            )

            await page.close()
            await context.close()

        return True
    except Exception as e:
        logger.error(f"[auto_claim_puzzle] Ошибка: {e}")
        return False

# ================== BATCH CLAIM ==================
async def claim_puzzles_batch(tg_user_id: str, target_iggid: str, amount: int, bot):
    """
    Массовый сбор пазлов для одного аккаунта с учётом лимитов и логов.
    """
    await auto_claim_puzzle2(tg_user_id, bot, target_iggid, amount)
