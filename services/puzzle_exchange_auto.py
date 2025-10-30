# tg_zov/services/puzzle_exchange_auto.py
import json
import asyncio
import logging
import random
from pathlib import Path
from typing import Dict, Any

from services.browser_patches import run_event_with_browser, humanize_pre_action

LOG = logging.getLogger("puzzle_exchange")
EVENT_URL = "https://event-eu-cc.igg.com/event/puzzle2/"
API_URL = f"{EVENT_URL}ajax.req.php"
FAIL_DIR = Path("data/fails")
FAIL_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------- utils ----------------------
def parse_json_text(text: str):
    try:
        return json.loads(text)
    except Exception:
        return None


# ---------------------- Handlers ----------------------
async def handle_get_fragment_count(page) -> Dict[str, Any]:
    """Handler, вызываемый внутри run_event_with_browser"""
    js_code = f"""
        async () => {{
            const res = await fetch("{API_URL}?action=get_resource", {{
                method: 'POST',
                credentials: 'include',
                headers: {{
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Referer': '{EVENT_URL}',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
                }}
            }});
            return {{status: res.status, text: await res.text()}};
        }}
    """
    resp = await page.evaluate(js_code)
    text = resp.get("text", "")
    status = resp.get("status", 0)

    raw_path = FAIL_DIR / "get_resource_raw.txt"
    raw_path.write_text(text, encoding="utf-8")

    data = parse_json_text(text)
    if not data:
        return {"success": False, "message": f"Ответ не JSON (status={status})"}

    puzzle_left = (
        data.get("puzzle_left")
        or data.get("data", {}).get("puzzle_left")
        or data.get("data", {}).get("user", {}).get("puzzle_left")
    )

    if puzzle_left is None:
        return {"success": False, "message": "Не найден puzzle_left"}

    return {"success": True, "message": f"🧩 Осталось фрагментов: {puzzle_left}"}


async def handle_exchange_item(page, item_id: str) -> Dict[str, Any]:
    """Handler для обмена предмета"""
    clean_id = str(item_id).split(":")[-1].strip()
    exchange_url = f"{API_URL}?action=exchange&id={clean_id}"

    await humanize_pre_action(page)
    await asyncio.sleep(random.uniform(1.5, 2.5))

    js_code = f"""
        async () => {{
            const res = await fetch("{exchange_url}", {{
                method: 'POST',
                credentials: 'include',
                headers: {{
                    'X-Requested-With': 'XMLHttpRequest',
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Referer': '{EVENT_URL}',
                    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
                }}
            }});
            const txt = await res.text();
            return {{status: res.status, text: txt}};
        }}
    """
    resp = await page.evaluate(js_code)
    text = resp.get("text", "")
    status = resp.get("status", 0)

    raw_path = FAIL_DIR / f"exchange_{clean_id}_raw.txt"
    raw_path.write_text(text, encoding="utf-8")

    parsed = parse_json_text(text)
    if not parsed:
        return {"success": False, "message": "Ответ не JSON"}

    if parsed.get("status") == 1:
        return {"success": True, "message": parsed.get("msg", "Обмен успешен")}
    else:
        return {"success": False, "message": parsed.get("msg", "Обмен не выполнен")}


# ---------------------- Public API ----------------------
async def get_fragment_count(user_id: str, iggid: str):
    return await run_event_with_browser(
        user_id=user_id,
        uid=iggid,
        event_url=EVENT_URL,
        event_name="Puzzle Exchange — Get Resource",
        handler_fn=handle_get_fragment_count,
    )


async def exchange_item(user_id: str, iggid: str, item_id: str):
    return await run_event_with_browser(
        user_id=user_id,
        uid=iggid,
        event_url=EVENT_URL,
        event_name=f"Puzzle Exchange — Exchange {item_id}",
        handler_fn=lambda page: handle_exchange_item(page, item_id),
    )
