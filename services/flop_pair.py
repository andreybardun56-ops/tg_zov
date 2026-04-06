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
SHARE_URL = "https://event-eu-cc.igg.com/event/flop_pair/ajax.req.php?action=share"
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


async def _page_indicates_event_inactive(page) -> bool:
    """
    Проверяет именно видимый текст страницы, чтобы не ловить ложные совпадения
    по скрытым шаблонам/локализациям внутри HTML/JS.
    """
    try:
        cards_count = await page.locator("li.flip").count()
        if cards_count > 0:
            return False
    except Exception:
        pass

    visible_text = ""
    try:
        visible_text = await page.evaluate("() => (document.body && document.body.innerText) || ''")
    except Exception:
        visible_text = ""

    return _is_event_inactive_text(visible_text)


def _body_indicates_event_inactive(body: str) -> bool:
    """
    Для ajax-ответов проверяем только информативные поля JSON, чтобы не реагировать
    на случайные вхождения маркеров в html/js.
    """
    if not body:
        return False

    msg_candidates: list[str] = []
    try:
        payload = json.loads(body)
    except Exception:
        payload = None

    if isinstance(payload, dict):
        for key in ("msg", "message", "error"):
            value = payload.get(key)
            if isinstance(value, str):
                msg_candidates.append(value)
        data = payload.get("data")
        if isinstance(data, dict):
            for key in ("msg", "message"):
                value = data.get(key)
                if isinstance(value, str):
                    msg_candidates.append(value)
    else:
        msg_candidates.append(body)

    return any(_is_event_inactive_text(text) for text in msg_candidates)


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
        # Для flop_pair некоторые успешные ответы приходят с success=0,
        # поэтому считаем ошибкой только явное текстовое/булево отрицание.
        if success in (False, "false", "False"):
            msg = str(data.get("msg", "")).lower()
            if not msg or any(marker in msg for marker in ("error", "ошиб", "invalid", "forbid", "denied", "fail")):
                return True

        msg = str(data.get("msg", "")).lower()
        if msg and any(marker in msg for marker in ("error", "ошиб", "invalid", "forbid", "denied", "fail")):
            return True

        return False

    lowered = body.lower()
    fail_markers = ("ошибка", "error", "failed", "forbidden", "denied", "invalid")
    return any(marker in lowered for marker in fail_markers)


def _extract_first_int(text: str | None, default: int = 0) -> int:
    if not text:
        return default
    digits = "".join(ch if ch.isdigit() else " " for ch in text)
    for token in digits.split():
        try:
            return int(token)
        except Exception:
            continue
    return default


async def _read_text_by_selectors(page, selectors: list[str]) -> str:
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0:
                txt = (await locator.inner_text()).strip()
                if txt:
                    return txt
        except Exception:
            continue
    return ""


async def _read_pool_chances(page) -> tuple[int, int]:
    """
    Возвращает:
      - share_chance: число шансов на распределение
      - share_points: текущее число очков в пуле (0, если не удалось определить)
    """
    share_text = await _read_text_by_selectors(
        page,
        [
            "#share-chance",
            ".share-chance",
            "#chance-share",
            "[data-share-chance]",
            ".chance-share",
        ],
    )
    points_text = await _read_text_by_selectors(
        page,
        [
            "#share-points",
            ".share-points",
            "#pool-points",
            ".pool-points",
            "#bonus_num",
            ".bonus-num",
        ],
    )

    share_chance = _extract_first_int(share_text, default=0)
    share_points = _extract_first_int(points_text, default=0)

    # Фолбэк: иногда значения лежат только в html/js
    if share_chance == 0 or share_points == 0:
        try:
            html = await page.content()
        except Exception:
            html = ""

        lowered = html.lower()
        if share_chance == 0:
            for marker in ("share_chance", "sharechance", "chance_share"):
                idx = lowered.find(marker)
                if idx >= 0:
                    share_chance = _extract_first_int(html[idx: idx + 80], default=share_chance)
                    if share_chance > 0:
                        break
        if share_points == 0:
            for marker in ("share_points", "sharepoints", "pool_points", "bonus_num"):
                idx = lowered.find(marker)
                if idx >= 0:
                    share_points = _extract_first_int(html[idx: idx + 120], default=share_points)
                    if share_points > 0:
                        break

    return max(0, share_chance), max(0, share_points)


async def _collect_pool_rewards(page, share_chance: int) -> tuple[int, int, list[str]]:
    """
    Пытается забрать награды из пула.
    Делает максимально безопасные попытки (кнопка/запрос), не роняя основной сценарий.
    """
    if share_chance <= 0:
        return 0, 0, []

    details: list[str] = []
    collected = 0
    remaining = share_chance

    for attempt in range(1, share_chance + 1):
        prev_remaining = remaining
        body = await _fetch_event_action(page, SHARE_URL)

        await asyncio.sleep(1.0)
        new_chance, _ = await _read_pool_chances(page)

        msg = ""
        try:
            payload = json.loads(body) if body else {}
            msg = str(payload.get("msg", "")).strip()
            chance_data = payload.get("chance") if isinstance(payload, dict) else None
            if isinstance(chance_data, dict):
                remaining = int(chance_data.get("left", new_chance) or new_chance)
        except Exception:
            pass

        if remaining == prev_remaining and new_chance != remaining:
            remaining = new_chance

        if remaining < prev_remaining:
            collected += 1

        if msg:
            details.append(f"• Пул #{attempt}: {msg}")
        else:
            details.append(f"• Пул #{attempt}: выполнен запрос share")

        if remaining <= 0:
            break

    return collected, remaining, details


async def _fetch_event_action(page, url: str) -> str:
    """
    Делает ajax-запрос из контекста страницы (с текущими cookies/session),
    не уводя вкладку на JSON-эндпоинт через page.goto().
    """
    script = """
    async (targetUrl) => {
      try {
        const response = await fetch(targetUrl, {
          method: "GET",
          credentials: "include",
          headers: {
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*"
          },
          cache: "no-store"
        });
        return await response.text();
      } catch (e) {
        return "";
      }
    }
    """
    try:
        body = await page.evaluate(script, url)
        return body if isinstance(body, str) else ""
    except Exception:
        return ""


def _resolve_account(user_id: str, uid: str | None) -> dict | None:
    accounts = get_all_accounts(user_id)
    if not accounts:
        return None
    if uid:
        found = next((a for a in accounts if str(a.get("uid")) == str(uid)), None)
        if found:
            return found
    return accounts[0]


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
    acc = _resolve_account(user_id, uid)
    if not acc:
        return {"success": False, "message": "⚠️ У пользователя нет аккаунтов."}

    uid = str(acc.get("uid") or "")
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
    acc = _resolve_account(user_id, uid)
    if not acc:
        return {"success": False, "message": "⚠️ У пользователя нет аккаунтов."}

    uid = str(acc.get("uid") or "")
    username = acc.get("username", "Игрок")

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
        if await _page_indicates_event_inactive(page):
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
        open_target = pairs_to_open[:1]

        for i, p in enumerate(open_target, 1):
            if attempts < 2:
                rewards.append("⚠️ Не хватает попыток для открытия следующей пары (нужно 2).")
                break

            pair_opened = True
            pair_msgs: list[str] = []
            last_payload: dict = {}

            for card_idx, pid in enumerate((p["c1"], p["c2"]), start=1):
                body = await _fetch_event_action(page, AJAX_URL.format(pair_id=pid))

                if _body_indicates_event_inactive(body):
                    return {
                        "success": False,
                        "message": "⚠️ Сервер вернул: событие ещё не началось или уже завершилось. Пары не были открыты.",
                    }

                attempts -= 1

                parsed = {}
                try:
                    parsed = json.loads(body) if body else {}
                except Exception:
                    parsed = {}
                if isinstance(parsed, dict):
                    last_payload = parsed
                    msg = str(parsed.get("msg", "")).strip()
                    if msg:
                        pair_msgs.append(f"• Карта {card_idx}: {msg}")

                    chance_data = parsed.get("chance")
                    if isinstance(chance_data, dict):
                        left = int(chance_data.get("left", 0) or 0)
                        free = int(chance_data.get("free", 0) or 0)
                        pair_msgs.append(f"• Шансы открытия: left={left}, free={free}")

                if _response_indicates_failure(body):
                    pair_opened = False

                await asyncio.sleep(1.2)

            if pair_opened:
                opened += 1
                rewards.append(f"#{i} 🎯 {p['c1']} + {p['c2']} → Открыто")
                opened_pairs.add(_normalize_pair(p["c1"], p["c2"]))
            else:
                rewards.append(f"#{i} ⚠️ {p['c1']} + {p['c2']} → не подтверждено сервером")

            if pair_msgs:
                rewards.extend(pair_msgs)

            chance_after_pair = 0
            pool_after_pair = 0
            chance_data = last_payload.get("chance") if isinstance(last_payload, dict) else {}
            if isinstance(chance_data, dict):
                chance_after_pair = int(chance_data.get("left", 0) or 0)
            user_extra = last_payload.get("user_extra") if isinstance(last_payload, dict) else {}
            if isinstance(user_extra, dict):
                share_data = user_extra.get("share")
                if isinstance(share_data, dict):
                    chance_after_pair = int(share_data.get("left", chance_after_pair) or chance_after_pair)
                    pool_after_pair = int(share_data.get("sum", 0) or 0)

            if chance_after_pair > 0:
                rewards.append(f"🎁 После 2-й карты: шансы пула={chance_after_pair}, пул={pool_after_pair}")
                collected, remaining, collect_details = await _collect_pool_rewards(page, chance_after_pair)
                rewards.append(f"🎁 Забрано из пула: {collected}, осталось шансов: {remaining}")
                rewards.extend(collect_details)
            await asyncio.sleep(1.5)

        # сохраняем обновлённые открытые
        stored_account_data = stored.setdefault("accounts", {}).setdefault(_account_key(user_id, uid), {})
        stored_account_data["pairs"] = pairs
        stored_account_data["opened_pairs"] = [list(x) for x in sorted(opened_pairs)]
        with open(PAIRS_FILE, "w", encoding="utf-8") as f:
            json.dump(stored, f, indent=2, ensure_ascii=False)

        summary = [
            f"👤 Аккаунт: {username} ({uid})",
            f"📊 Открыто пар за запуск: {opened}/{len(open_target)}",
            f"🔢 Осталось попыток: {attempts}",
        ]
        summary.append(f"🗂 Осталось неоткрытых пар: {max(0, len(pairs_to_open) - opened)}")
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
