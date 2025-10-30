# tg_zov/services/flop_pair.py
import os
import json
import hashlib
import asyncio
import logging
from collections import defaultdict
from datetime import datetime
import aiohttp
from services.browser_patches import run_event_with_browser
from services.accounts_manager import get_all_accounts
from services.castle_api import load_cookies_for_account

logger = logging.getLogger("flop_pair")

BASE_URL = "https://event-eu-cc.igg.com/event/flop_pair/"
AJAX_URL = "https://event-eu-cc.igg.com/event/flop_pair/ajax.req.php?action=flop&id={pair_id}"
PAIRS_FILE = os.path.join("data", "flop_pairs.json")

# === ВСПОМОГАТЕЛЬНЫЕ ===
def safe_load_json(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

async def hash_image(session: aiohttp.ClientSession, url: str, retries: int = 3) -> str | None:
    clean_url = url.split("?")[0]
    for i in range(retries):
        try:
            async with session.get(clean_url, timeout=10) as resp:
                if resp.status == 200:
                    return hashlib.md5(await resp.read()).hexdigest()
        except Exception:
            await asyncio.sleep(0.5)
    return None

# === Этап 1: поиск пар ===
async def find_flop_pairs(user_id: str, uid: str = None):
    accounts = get_all_accounts(user_id)
    if not accounts:
        return {"success": False, "message": "⚠️ У пользователя нет аккаунтов."}

    acc = next((a for a in accounts if a.get("uid") == uid), accounts[0])
    uid = acc.get("uid")
    username = acc.get("username", "Игрок")

    cookies = load_cookies_for_account(user_id, uid)
    if not cookies:
        return {"success": False, "message": f"⚠️ Cookies не найдены ({username})"}

    async def handler(page):
        # Проверяем наличие карт
        try:
            await page.wait_for_selector("li.flip", timeout=10000)
        except Exception:
            return {"success": False, "message": "⚠️ Не удалось найти элементы карт."}

        cards = await page.query_selector_all("li.flip")
        cards_data = []
        for c in cards:
            try:
                img_tag = await c.query_selector("img")
                img_url = await img_tag.get_attribute("src")
                pair_id = await c.get_attribute("pair")
                if img_url and pair_id:
                    cards_data.append({"pair_id": pair_id, "img": img_url})
            except Exception:
                continue

        if not cards_data:
            return {"success": False, "message": "⚠️ Карты не найдены."}

        # Хэшируем
        hash_map = defaultdict(list)
        async with aiohttp.ClientSession() as session:
            for card in cards_data:
                h = await hash_image(session, card["img"])
                if h:
                    hash_map[h].append(card)

        pairs = []
        for g in hash_map.values():
            if len(g) == 2:
                pairs.append({"c1": g[0]["pair_id"], "c2": g[1]["pair_id"]})

        if not pairs:
            return {"success": False, "message": f"⚠️ Совпадающих карт не найдено ({username})."}

        os.makedirs(os.path.dirname(PAIRS_FILE), exist_ok=True)
        with open(PAIRS_FILE, "w", encoding="utf-8") as f:
            json.dump({"pairs": pairs, "updated": datetime.now().isoformat()}, f, indent=2, ensure_ascii=False)

        msg = [f"✅ {username}: найдено пар — {len(pairs)}", ""]
        msg += [f"🎯 {p['c1']} + {p['c2']}" for p in pairs[:10]]
        if len(pairs) > 10:
            msg.append(f"...и ещё {len(pairs) - 10} пар.")
        return {"success": True, "message": "\n".join(msg)}

    return await run_event_with_browser(user_id, uid, BASE_URL, "Найди пару (сканирование)", handler)

# === Этап 2: открытие ===
async def run_flop_pair(user_id: str, uid: str = None):
    """
    Ежедневное открытие пар. Использует сохранённые пары из data/flop_pairs.json
    """
    stored = safe_load_json(PAIRS_FILE)
    pairs = stored.get("pairs", [])
    if not pairs:
        logger.info("[FLOP] 🔄 Нет сохранённых пар — пересканируем.")
        res = await find_flop_pairs(user_id, uid)
        if not res.get("success"):
            return res
        stored = safe_load_json(PAIRS_FILE)
        pairs = stored.get("pairs", [])

    if not pairs:
        return {"success": False, "message": "⚠️ Пары не найдены даже после пересканирования."}

    async def handler(page):
        # Проверяем статус события
        html = (await page.content()).lower()
        if any(x in html for x in ["событие еще не началось", "уже завершилось"]):
            return {"success": True, "message": "⚠️ Событие ещё не началось или уже завершилось."}

        # Получаем попытки
        attempts = 0
        try:
            text = await page.locator("#chance-left").inner_text()
            attempts = int(text.strip())
        except Exception:
            pass

        if attempts <= 0:
            return {"success": False, "message": "⚠️ Попыток не осталось."}

        opened = 0
        rewards = []

        for i, p in enumerate(pairs, 1):
            if attempts < 2:
                break
            for pid in (p["c1"], p["c2"]):
                await page.goto(AJAX_URL.format(pair_id=pid), wait_until="domcontentloaded")
                await asyncio.sleep(2)
                attempts -= 1
            opened += 1
            rewards.append(f"#{i} 🎯 {p['c1']} + {p['c2']} → Открыто")
            await asyncio.sleep(2)

        summary = [
            f"📊 Открыто пар: {opened}/{len(pairs)}",
            f"🔢 Осталось попыток: {attempts}",
            "",
        ]
        summary.extend(rewards)
        summary.append("✅ Ежедневное открытие завершено!")

        return {"success": True, "message": "\n".join(summary)}

    return await run_event_with_browser(user_id, uid, BASE_URL, "Найди пару", handler)
