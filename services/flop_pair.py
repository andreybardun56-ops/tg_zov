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
EVENT_INACTIVE_MARKERS = (
    "событие еще не началось",
    "событие ещё не началось",
    "event has not yet begun",
    "event hat noch nicht begonnen",
    "уже завершилось",
    "has already ended",
    "bereits beendet",
)


def _account_key(user_id: str | None, uid: str | None) -> str:
    """Возвращает ключ аккаунта для сохранения состояния (user_id + uid)."""
    safe_user = str(user_id) if user_id else "default_user"
    safe_uid = str(uid) if uid else "default_uid"
    return f"{safe_user}:{safe_uid}"


def _normalize_pair(c1: str, c2: str) -> tuple[str, str]:
    """Нормализуем идентификаторы пары, чтобы порядок не имел значения."""
    return tuple(sorted((str(c1), str(c2))))


def _is_event_inactive_text(text: str) -> bool:
    lowered = (text or "").lower()
    return any(marker in lowered for marker in EVENT_INACTIVE_MARKERS)


def _response_indicates_failure(body: str) -> bool:
    """
    Пытается определить, что сервер явно вернул ошибку открытия карты.
    Важно: для этого события `code/ret = 0` может быть успешным ответом,
    поэтому не считаем это ошибкой автоматически.
    """
    if not body:
        return False

    try:
        data = json.loads(body)
    except Exception:
        data = None

    if isinstance(data, dict):
        # Частые поля явной ошибки в JSON-ответах
        for key in ("error", "errno", "err_code"):
            value = data.get(key)
            if isinstance(value, bool):
                if value:
                    return True
            elif isinstance(value, (int, float)):
                if value > 0:
                    return True
            elif isinstance(value, str) and value.strip() not in ("", "0", "ok", "success"):
                return True

        success = data.get("success")
        if success in (False, 0, "0", "false", "False"):
            return True

        msg = str(data.get("msg", "")).lower()
        if msg and any(marker in msg for marker in ("error", "ошиб", "invalid", "forbid", "denied", "fail")):
            return True

        return False

    lowered = body.lower()
    fail_markers = ("ошибка", "error", "failed", "forbidden", "denied", "invalid")
    return any(marker in lowered for marker in fail_markers)


def _build_pairs_preview(cards_data: list[dict], pairs: list[dict], hash_map: dict[str, list[dict]]) -> str:
    """
    Формирует мини-превью поля 5/5/4 и легенду, чтобы было видно где лежат пары.
    """
    rows_pattern = (5, 5, 4)
    pair_labels = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    label_by_hash: dict[str, str] = {}

    ordered_hashes = [h for h, g in hash_map.items() if len(g) == 2]
    for idx, h in enumerate(ordered_hashes):
        label_by_hash[h] = pair_labels[idx] if idx < len(pair_labels) else str(idx + 1)

    cells = []
    for card in cards_data:
        card_hash = card.get("hash")
        cells.append(label_by_hash.get(card_hash, "·"))

    rows = []
    offset = 0
    for row_len in rows_pattern:
        chunk = cells[offset: offset + row_len]
        if not chunk:
            break
        rows.append(" ".join(chunk))
        offset += row_len

    label_to_pair: dict[str, tuple[str, str]] = {}
    for p in pairs:
        norm = _normalize_pair(p["c1"], p["c2"])
        for h, g in hash_map.items():
            if len(g) != 2:
                continue
            ids = _normalize_pair(g[0]["pair_id"], g[1]["pair_id"])
            if ids == norm and h in label_by_hash:
                label_to_pair[label_by_hash[h]] = norm
                break

    legend = [f"{label} → {c1} + {c2}" for label, (c1, c2) in sorted(label_to_pair.items())]

    msg = ["🪟 Мини-расклад (5/5/4):"]
    msg.extend(rows)
    if legend:
        msg.append("")
        msg.append("🧭 Обозначения:")
        msg.extend(legend)
    return "\n".join(msg)


def _load_account_storage(user_id: str | None, uid: str | None) -> tuple[dict, dict]:
    """
    Загружает общее состояние и данные конкретного аккаунта.

    Файл переиспользуется для нескольких аккаунтов, поэтому структура хранится так:
    {
        "accounts": {
            "<uid>": {
                "pairs": [...],
                "opened_pairs": [...],
                "updated": "..."
            }
        }
    }

    Старый формат (без "accounts") автоматически мигрируется.
    """

    key = _account_key(user_id, uid)
    stored = safe_load_json(PAIRS_FILE) or {}

    if "accounts" not in stored:
        # старый формат — переносим данные под текущий ключ
        legacy_pairs = stored.get("pairs", [])
        legacy_opened = stored.get("opened_pairs", [])
        legacy_updated = stored.get("updated")
        legacy_event_period = stored.get("event_period")
        stored = {"accounts": {}}
        stored["accounts"][key] = {}
        if legacy_pairs:
            stored["accounts"][key]["pairs"] = legacy_pairs
        if legacy_opened:
            stored["accounts"][key]["opened_pairs"] = legacy_opened
        if legacy_updated:
            stored["accounts"][key]["updated"] = legacy_updated
        if legacy_event_period:
            stored["accounts"][key]["event_period"] = legacy_event_period

    accounts = stored.setdefault("accounts", {})
    account_data = accounts.setdefault(key, {})
    account_data.setdefault("pairs", [])
    account_data.setdefault("opened_pairs", [])

    return stored, account_data

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
async def find_flop_pairs(user_id: str, uid: str = None, context=None):
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
        # читаем период события (если есть), чтобы сбрасывать старые пары при новом цикле
        event_period = ""
        try:
            period_locator = page.locator(".event-time").first
            if await period_locator.count() > 0:
                event_period = (await period_locator.inner_text()).strip()
        except Exception:
            event_period = ""

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
                    card["hash"] = h
                    hash_map[h].append(card)

        pairs = []
        for g in hash_map.values():
            if len(g) == 2:
                pairs.append({"c1": g[0]["pair_id"], "c2": g[1]["pair_id"]})

        if not pairs:
            return {"success": False, "message": f"⚠️ Совпадающих карт не найдено ({username})."}

        stored, account_data = _load_account_storage(user_id, uid)
        previous_period = account_data.get("event_period")
        if event_period and previous_period and event_period != previous_period:
            # Новый период события: чистим старые данные, чтобы не пытаться открыть вчерашние пары
            account_data["pairs"] = []
            account_data["opened_pairs"] = []

        account_data["pairs"] = pairs
        account_data["updated"] = datetime.now().isoformat()
        if event_period:
            account_data["event_period"] = event_period
        # сохраняем уже открытые пары, оставляя только те, что присутствуют в новом списке
        existing_opened = {
            _normalize_pair(p[0], p[1])
            for p in account_data.get("opened_pairs", [])
            if isinstance(p, (list, tuple)) and len(p) == 2
        }
        valid_pairs = {_normalize_pair(p["c1"], p["c2"]) for p in pairs}
        account_data["opened_pairs"] = [list(p) for p in sorted(existing_opened & valid_pairs)]

        os.makedirs(os.path.dirname(PAIRS_FILE), exist_ok=True)
        with open(PAIRS_FILE, "w", encoding="utf-8") as f:
            json.dump(stored, f, indent=2, ensure_ascii=False)

        msg = [f"✅ {username}: найдено пар — {len(pairs)}", ""]
        msg.append(_build_pairs_preview(cards_data, pairs, hash_map))
        return {"success": True, "message": "\n".join(msg)}

    return await run_event_with_browser(user_id, uid, BASE_URL, "Найди пару (сканирование)", handler, context=context)

# === Этап 2: открытие ===
async def run_flop_pair(user_id: str, uid: str = None, context=None):
    """
    Ежедневное открытие пар. Пропускает уже открытые пары.
    """
    stored, account_data = _load_account_storage(user_id, uid)
    pairs = account_data.get("pairs", [])
    opened_pairs = {
        _normalize_pair(x[0], x[1])
        for x in account_data.get("opened_pairs", [])
        if isinstance(x, (list, tuple)) and len(x) == 2
    }

    if not pairs:
        logger.info("[FLOP] 🔄 Нет сохранённых пар — пересканируем.")
        res = await find_flop_pairs(user_id, uid)
        if not res.get("success"):
            return res
        stored, account_data = _load_account_storage(user_id, uid)
        pairs = account_data.get("pairs", [])
        opened_pairs = {
            _normalize_pair(x[0], x[1])
            for x in account_data.get("opened_pairs", [])
            if isinstance(x, (list, tuple)) and len(x) == 2
        }

    if not pairs:
        return {"success": False, "message": "⚠️ Пары не найдены даже после пересканирования."}

    # фильтруем только неоткрытые
    pairs_to_open = [p for p in pairs if _normalize_pair(p["c1"], p["c2"]) not in opened_pairs]
    already_open = len(pairs) - len(pairs_to_open)
    if not pairs_to_open:
        async def no_open_handler(page):
            share_chance, share_points = await _read_pool_chances(page)
            lines = ["✅ Все пары уже открыты.", f"🎁 Шансы распределения: {share_chance}"]
            if share_points:
                lines.append(f"💎 Текущий пул призов: {share_points}/50000000")
            if share_chance > 0:
                lines.append("🚀 Есть шансы в пуле — пошел собирать...")
                collected, remaining, collect_details = await _collect_pool_rewards(page, share_chance)
                lines.append(f"🎁 Собрано наград из пула: {collected}")
                lines.append(f"🎯 Осталось шансов в пуле: {remaining}")
                lines.extend(collect_details)
            return {"success": True, "message": "\n".join(lines)}

        return await run_event_with_browser(user_id, uid, BASE_URL, "Найди пару", no_open_handler, context=context)

    async def handler(page):
        html = (await page.content()).lower()
        if _is_event_inactive_text(html):
            return {"success": True, "message": "⚠️ Событие ещё не началось или уже завершилось."}
        share_chance, share_points = await _read_pool_chances(page)

        # Если период события сменился — не используем старые пары
        current_period = ""
        try:
            period_locator = page.locator(".event-time").first
            if await period_locator.count() > 0:
                current_period = (await period_locator.inner_text()).strip()
        except Exception:
            current_period = ""

        saved_period = account_data.get("event_period", "")
        if current_period and saved_period and current_period != saved_period:
            logger.info("[FLOP] 🔄 Обнаружен новый период события (%s -> %s), пересканирую пары.", saved_period, current_period)
            account_data["pairs"] = []
            account_data["opened_pairs"] = []
            account_data["event_period"] = current_period
            os.makedirs(os.path.dirname(PAIRS_FILE), exist_ok=True)
            with open(PAIRS_FILE, "w", encoding="utf-8") as f:
                json.dump(stored, f, indent=2, ensure_ascii=False)
            return {
                "success": False,
                "message": "ℹ️ Обнаружен новый период события. Старые пары сброшены, запусти «🔍 Проверить пары» и затем повтори открытие.",
            }

        # Попытки
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

        for i, p in enumerate(pairs_to_open, 1):
            if attempts < 2:
                break
            pair_opened = True
            for pid in (p["c1"], p["c2"]):
                resp = await page.goto(AJAX_URL.format(pair_id=pid), wait_until="domcontentloaded")
                body = ""
                if resp:
                    try:
                        body = await resp.text()
                    except Exception:
                        body = ""

                if _is_event_inactive_text(body):
                    return {
                        "success": False,
                        "message": "⚠️ Сервер вернул: событие ещё не началось или уже завершилось. Пары не были открыты.",
                    }

                await asyncio.sleep(2)
                attempts -= 1
                if _response_indicates_failure(body):
                    pair_opened = False

            if pair_opened:
                opened += 1
                rewards.append(f"#{i} 🎯 {p['c1']} + {p['c2']} → Открыто")
                opened_pairs.add(_normalize_pair(p["c1"], p["c2"]))
            else:
                rewards.append(f"#{i} ⚠️ {p['c1']} + {p['c2']} → не подтверждено сервером")
            await asyncio.sleep(2)

        # сохраняем обновлённые открытые
        stored_account_data = stored.setdefault("accounts", {}).setdefault(_account_key(user_id, uid), {})
        stored_account_data["pairs"] = pairs
        stored_account_data["opened_pairs"] = [list(x) for x in sorted(opened_pairs)]
        with open(PAIRS_FILE, "w", encoding="utf-8") as f:
            json.dump(stored, f, indent=2, ensure_ascii=False)

        summary = [
            f"📊 Открыто пар: {opened}/{len(pairs_to_open)}",
            f"🔢 Осталось попыток: {attempts}",
        ]
        if already_open:
            summary.append(f"🔁 Пропущено пар: {already_open} (уже были открыты)")
        share_chance, share_points = await _read_pool_chances(page)
        summary.append(f"🎁 Шансы распределения: {share_chance}")
        if share_points:
            summary.append(f"💎 Текущий пул призов: {share_points}/50000000")
        if share_chance > 0:
            summary.append("🚀 Есть шансы в пуле — пошел собирать...")
            collected, remaining, collect_details = await _collect_pool_rewards(page, share_chance)
            summary.append(f"🎁 Собрано наград из пула: {collected}")
            summary.append(f"🎯 Осталось шансов в пуле: {remaining}")
            summary.extend(collect_details)
        summary.append("")
        summary.extend(rewards)
        summary.append("✅ Ежедневное открытие завершено!")
        return {"success": True, "message": "\n".join(summary)}

    return await run_event_with_browser(user_id, uid, BASE_URL, "Найди пару", handler, context=context)
