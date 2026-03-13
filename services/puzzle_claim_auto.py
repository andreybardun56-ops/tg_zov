# tg_zov/services/puzzle_claim_auto.py
import os
import json
import asyncio
import logging
import tempfile
import shutil
from html import escape
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List

from playwright.async_api import async_playwright
from services.logger import logger
from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    humanize_pre_action,
    cookies_to_playwright,
    launch_masked_persistent_context,
)

# === Пути и настройки ===
COOKIES_FILE = Path("data/cookies.json")
PUZZLE_DATA_FILE = Path("data/puzzle_data.jsonl")
EVENT_PAGE = "https://event-eu-cc.igg.com/event/puzzle2/"
EVENT_API = f"{EVENT_PAGE}ajax.req.php"
PROFILE_DIR = Path("data/chrome_profiles")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

log = logging.getLogger("puzzle_claim_auto")
log.setLevel(logging.INFO)

# ---------------- utilities ----------------
def load_cookies_file() -> dict:
    if not COOKIES_FILE.exists():
        return {}
    try:
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"[PUZZLE_CLAIM] Ошибка чтения cookies.json: {e}")
        return {}

def save_cookies_file(data: dict):
    COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = COOKIES_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, COOKIES_FILE)

def parse_jsonl_blocks(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    blocks, buf = [], ""
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
    return blocks

def write_jsonl_blocks(path: Path, blocks: List[Dict[str, Any]]):
    tmp = path.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        for b in blocks:
            json.dump(b, f, ensure_ascii=False, indent=2)
            f.write("\n\n")
    os.replace(tmp, path)

def find_donor_for_puzzle(puzzle_num: int) -> Optional[Tuple[Dict[str, Any], int]]:
    blocks = parse_jsonl_blocks(PUZZLE_DATA_FILE)
    for i, entry in enumerate(blocks):
        puzzle = entry.get("puzzle", {})
        if str(puzzle_num) in puzzle and int(puzzle[str(puzzle_num)]) > 0:
            return entry, i
    return None


def find_donor_for_puzzle_exclude(puzzle_num: int, exclude_iggids: set) -> Optional[
    Tuple[Dict[str, Any], int]]:
    """Ищет донора для пазла, пропуская уже использованных."""
    blocks = parse_jsonl_blocks(PUZZLE_DATA_FILE)
    for i, entry in enumerate(blocks):
        iggid = entry.get("iggid")
        if iggid in exclude_iggids:
            continue
        puzzle = entry.get("puzzle", {})
        if str(puzzle_num) in puzzle and int(puzzle[str(puzzle_num)]) > 0:
            return entry, i
    return None


# ---------------- main logic ----------------
async def claim_puzzle(
    tg_user_id: str,
    target_iggid: str,
    puzzle_num: int,
    bot,
    msg=None,
    user_name: str | None = None,
    user_tag: str | None = None,
) -> None:
    tg_user_id = str(tg_user_id)
    logger.info(f"[PUZZLE_CLAIM] 🔍 Поиск пазла {puzzle_num} для user={tg_user_id}")

    CLAIM_LOG_FILE = Path("data/puzzle_claim_log.json")

    # ===== ЛОГ =====
    def load_claim_log() -> dict:
        if CLAIM_LOG_FILE.exists():
            try:
                with open(CLAIM_LOG_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def save_claim_log(data: dict):
        CLAIM_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = CLAIM_LOG_FILE.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CLAIM_LOG_FILE)

    log_data = load_claim_log()
    users_meta = log_data.setdefault("users_meta", {})
    if user_name or user_tag:
        users_meta[str(tg_user_id)] = {
            "name": user_name or users_meta.get(str(tg_user_id), {}).get("name", ""),
            "tag": user_tag or users_meta.get(str(tg_user_id), {}).get("tag", ""),
        }

    # ===== Проверка лимитов и повторов =====
    user_entry = log_data.setdefault("users", {}).setdefault(tg_user_id, {}).setdefault(
        target_iggid, {"donors": [], "count": 0, "claimed_puzzles": [], "last_messages": {}}
    )
    if user_name or user_tag:
        save_claim_log(log_data)

    # если достигнут лимит 30 пазлов
    if user_entry["count"] >= 30:
        await bot.send_message(
            tg_user_id,
            f"⚠️ Нельзя получить больше 30 пазлов для аккаунта <code>{target_iggid}</code> в этом событии.",
            parse_mode="HTML"
        )
        return

    used_donors = set(user_entry["donors"])
    donor_data = find_donor_for_puzzle_exclude(puzzle_num, used_donors)
    if not donor_data:
        await bot.send_message(
            tg_user_id,
            f"⚠️ Нет доступных доноров для пазла {puzzle_num}. Попробуй другой номер.",
        )
        return

    donor, donor_index = donor_data
    donor_iggid = donor.get("iggid")
    if not donor_iggid:
        await bot.send_message(tg_user_id, "⚠️ Ошибка: у донора нет IGGID.")
        return

    cookies_db = load_cookies_file()
    user_cookies = cookies_db.get(tg_user_id, {})
    acc_cookies = user_cookies.get(str(target_iggid), {})
    if not acc_cookies:
        await bot.send_message(tg_user_id, "⚠️ У выбранного аккаунта нет cookies. Сначала обнови их.")
        return

  #  await bot.send_message(
  #      tg_user_id,
  #      f"🧩 Получаю пазл <b>{puzzle_num}</b> от <code>{donor_iggid}</code>...",
  #      parse_mode="HTML"
  #  )

    try:
        async with async_playwright() as p:
            profile = get_random_browser_profile()
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir=str(PROFILE_DIR / f"{target_iggid}"),
                browser_path=BROWSER_PATH,
                headless=True,
                slow_mo=50,
                profile=profile
            )
            context, page = ctx["context"], ctx["page"]

            # добавляем старые куки
            await context.add_cookies(cookies_to_playwright(acc_cookies))
            await page.goto(EVENT_PAGE, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1.5)
            await humanize_pre_action(page)

            # обновляем куки
            fresh = await context.cookies()
            fresh_map = {c["name"]: c["value"] for c in fresh if "name" in c}
            if fresh_map:
                cookies_db.setdefault(tg_user_id, {})[str(target_iggid)] = fresh_map
                save_cookies_file(cookies_db)

            # === Основной запрос ===
            claim_url = f"{EVENT_API}?action=claim_friend_puzzle&friend_iggid={donor_iggid}&puzzle={puzzle_num}"
            logger.info(f"[PUZZLE_CLAIM] 🎯 Запрос: {claim_url}")

            js = f"""
                async () => {{
                    const res = await fetch("{claim_url}", {{
                        method: 'GET',
                        credentials: 'include',
                        headers: {{
                            'X-Requested-With': 'XMLHttpRequest',
                            'Referer': '{EVENT_PAGE}'
                        }}
                    }});
                    const txt = await res.text();
                    return {{status: res.status, text: txt}};
                }}
            """
            resp = await page.evaluate(js)
            text = resp.get("text", "")
            status = resp.get("status", 0)
            logger.info(f"[PUZZLE_CLAIM] Ответ: {status} | {text[:200]}")

            # --- 🔁 Перебор доноров ---
            max_attempts = 10
            attempt = 0
            success = False
            last_error = None
            used_donors = set(user_entry["donors"])

            while attempt < max_attempts:
                attempt += 1
                try:
                    parsed_json = json.loads(text)
                except Exception:
                    parsed_json = None

                if parsed_json and isinstance(parsed_json, dict):
                    if parsed_json.get("status") == 1:
                        success = True
                        break

                    elif parsed_json.get("error") == 4:
                        logger.info(f"[PUZZLE_CLAIM] ⚠️ Донор {donor_iggid} уже использован, ищем другого...")
                        used_donors.add(donor_iggid)
                        user_entry["donors"].append(donor_iggid)
                        save_claim_log(log_data)

                        donor_data = find_donor_for_puzzle_exclude(puzzle_num, used_donors)
                        if not donor_data:
                            last_error = 4
                            break

                        donor, donor_index = donor_data
                        donor_iggid = donor.get("iggid")
                        logger.info(f"[PUZZLE_CLAIM] 🔁 Попытка #{attempt} — новый донор {donor_iggid}")

                        claim_url = f"{EVENT_API}?action=claim_friend_puzzle&friend_iggid={donor_iggid}&puzzle={puzzle_num}"
                        js = f"""
                            async () => {{
                                const res = await fetch("{claim_url}", {{
                                    method: 'GET',
                                    credentials: 'include',
                                    headers: {{
                                        'X-Requested-With': 'XMLHttpRequest',
                                        'Referer': '{EVENT_PAGE}'
                                    }}
                                }});
                                const txt = await res.text();
                                return {{status: res.status, text: txt}};
                            }}
                        """
                        resp = await page.evaluate(js)
                        text = resp.get("text", "")
                        continue


                    elif parsed_json.get("error") == 5:

                        # ✅ лимит 30 пазлов

                        logger.info(
                            f"[PUZZLE_CLAIM] 🚫 Лимит 30 пазлов достигнут для {target_iggid}. Устанавливаю count=30.")

                        user_entry["count"] = 30

                        # гарантируем наличие словаря сообщений

                        if "last_messages" not in user_entry or not isinstance(user_entry["last_messages"], dict):
                            user_entry["last_messages"] = {}

                        save_claim_log(log_data)

                        last_error = 5

                        err_text = (

                            f"🚫 <b>Лимит достигнут!</b>\n"

                            f"Аккаунт <code>{target_iggid}</code> уже получил все 30 пазлов в этом событии 🎯.\n\n"

                            f"Возвращайся в следующий раз, когда начнётся новый ивент 🧩"

                        )

                        # 💬 Отправляем красивое сообщение пользователю сразу

                        try:

                            msg_id = user_entry["last_messages"].get(str(puzzle_num))

                            if msg_id:

                                await bot.edit_message_text(

                                    chat_id=tg_user_id,

                                    message_id=msg_id,

                                    text=err_text,

                                    parse_mode="HTML"

                                )

                            else:
                                msg = await bot.send_message(tg_user_id, err_text, parse_mode="HTML")
                                user_entry["last_messages"][str(puzzle_num)] = msg.message_id
                                save_claim_log(log_data)
                        except Exception as e:
                            logger.warning(f"[PUZZLE_CLAIM] Ошибка отправки уведомления о лимите: {e}")
                        break
                    else:
                        last_error = parsed_json.get("error")
                        break

                else:
                    success = "success" in text.lower() or "获得" in text or "成功" in text or "Поздрав" in text
                    break

            # --- 📘 Обновление данных ---
            if success:
                blocks = parse_jsonl_blocks(PUZZLE_DATA_FILE)
                if 0 <= donor_index < len(blocks):
                    puzzles = blocks[donor_index].get("puzzle", {})
                    count = int(puzzles.get(str(puzzle_num), 0))
                    if count > 1:
                        puzzles[str(puzzle_num)] = count - 1
                    else:
                        puzzles.pop(str(puzzle_num), None)
                    if not puzzles:
                        blocks.pop(donor_index)
                    else:
                        blocks[donor_index]["puzzle"] = puzzles
                    write_jsonl_blocks(PUZZLE_DATA_FILE, blocks)

                if donor_iggid not in user_entry["donors"]:
                    user_entry["donors"].append(donor_iggid)
                    user_entry["count"] += 1
                if puzzle_num not in user_entry.get("claimed_puzzles", []):
                    user_entry["claimed_puzzles"].append(puzzle_num)
                save_claim_log(log_data)

                remaining = 30 - user_entry["count"]
                puzzles_list = ", ".join(map(str, user_entry["claimed_puzzles"]))
                text_out = (
                    f"✅ Получены пазлы: <b>{puzzles_list}</b>\n"
                    f"Осталось попыток: <b>{remaining}</b> / 30"
                )
                # 1) Апдейтим "стартовое" сообщение этой попытки
                if msg:
                    try:
                        await msg.edit_text(
                            f"✅ Пазл <b>{puzzle_num}</b> получен от <code>{donor_iggid}</code>.",
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass

                # 2) Апдейтим/создаём сводку (одно сообщение, которое меняется каждый раз)
                summary_id = user_entry["last_messages"].get("summary")
                if summary_id:
                    try:
                        await bot.edit_message_text(
                            chat_id=tg_user_id,
                            message_id=summary_id,
                            text=text_out,
                            parse_mode="HTML"
                        )
                    except Exception:
                        # если вдруг удалено — создаём заново
                        m = await bot.send_message(tg_user_id, text_out, parse_mode="HTML")
                        user_entry["last_messages"]["summary"] = m.message_id
                        save_claim_log(log_data)
                else:
                    m = await bot.send_message(tg_user_id, text_out, parse_mode="HTML")
                    user_entry["last_messages"]["summary"] = m.message_id
                    save_claim_log(log_data)

                if remaining <= 0:
                    await bot.send_message(
                        tg_user_id,
                        "🚫 Все 30 пазлов уже получены.\nВозвращайтесь в следующий раз!",
                        parse_mode="HTML"
                    )

            else:
                if last_error == 4:
                    err_text = f"⚠️ Все доноры уже использованы для пазла {puzzle_num}."
                elif last_error == 5:
                    err_text = f"🚫 Для аккаунта <code>{target_iggid}</code> достигнут лимит 30 пазлов."
                else:
                    safe_text = escape(text[:300]) if text else ""
                    err_text = f"❌ Не удалось получить пазл {puzzle_num}.\n<code>{safe_text}</code>"
                if msg:
                    try:
                        await msg.edit_text(err_text, parse_mode="HTML")
                        return
                    except Exception:
                        pass

                msg_id = user_entry["last_messages"].get(str(puzzle_num))
                if msg_id:
                    await bot.edit_message_text(chat_id=tg_user_id, message_id=msg_id, text=err_text, parse_mode="HTML")
                else:
                    msg = await bot.send_message(tg_user_id, err_text, parse_mode="HTML")
                    user_entry["last_messages"][str(puzzle_num)] = msg.message_id
                    save_claim_log(log_data)

    except Exception as e:
        logger.exception(f"[PUZZLE_CLAIM] Ошибка claim_puzzle: {e}")
        await bot.send_message(tg_user_id, f"❌ Ошибка при выполнении запроса: {e}")

    finally:
        try:
            if 'page' in locals():
                await page.close()
            if 'context' in locals():
                await context.close()
        except Exception:
            pass

# ---------------- Проверка активности события Puzzle2 ----------------
async def check_puzzle2_active(user_id: str) -> bool:
    """
    Проверяет, активна ли акция «Пазлы».
    Возвращает True, если страница реально содержит элементы Puzzle2.
    """
    from services.cookies_io import load_all_cookies

    EVENT_URL = "https://event-eu-cc.igg.com/event/puzzle2/"
    user_id = str(user_id)
    cookies_db = load_all_cookies()
    user_cookies = cookies_db.get(user_id, {})

    if not user_cookies:
        logger.warning(f"[puzzle_check] ⚠️ Нет cookies для user_id={user_id}")
        return False

    # Берём первый UID
    first_uid = next(iter(user_cookies.keys()))
    acc_cookies = user_cookies.get(first_uid, {})
    if not acc_cookies:
        logger.warning(f"[puzzle_check] ⚠️ Нет cookies для первого аккаунта {first_uid}")
        return False

    async with async_playwright() as p:
        ctx_info = await launch_masked_persistent_context(
            p,
            user_data_dir=str(PROFILE_DIR / first_uid),
            browser_path=BROWSER_PATH,
            headless=True,
            slow_mo=30,
            profile=get_random_browser_profile(),
        )
        context, page = ctx_info["context"], ctx_info["page"]

        try:
            await context.add_cookies(cookies_to_playwright(acc_cookies))
            await page.goto(EVENT_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1)

            current_url = page.url.lower()
            html = await page.content()

            # 💡 Новая — более точная проверка активности:
            active_markers = [
                "ajax.req.php",
                "puzzle",  # часть ID/классов
                "gift-details-it",
                "puzzle2_main",
                "event/puzzle2/ajax.req.php",
            ]
            is_active = any(marker in html for marker in active_markers)

            if "puzzle2" in current_url and is_active:
                logger.info(f"[puzzle_check] ✅ Акция Puzzle2 активна для {user_id}")
                return True
            else:
                logger.warning(f"[puzzle_check] ⚠️ Акция Puzzle2 недоступна (url={current_url})")
                return False

        except Exception as e:
            logger.warning(f"[puzzle_check] ❌ Ошибка проверки Puzzle2: {e}")
            return False

        finally:
            try:
                await page.close()
                await context.close()
            except Exception:
                pass