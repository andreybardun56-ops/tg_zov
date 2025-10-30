# tg_zov/services/recall2_auto.py
import asyncio
import json
import logging
import random
from pathlib import Path
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from services.browser_patches import (
    get_random_browser_profile,
    launch_masked_persistent_context,
    cookies_to_playwright,
)

# ─────────────────────────────
# ⚙️ Настройки
# ─────────────────────────────
logger = logging.getLogger("recall2_auto")
logger.setLevel(logging.INFO)

DATA_FILE = Path("data/data_akk/new_data0.json")
PROFILE_DIR = Path("data/chrome_profiles/recall2")
PROFILE_DIR.mkdir(parents=True, exist_ok=True)
RESULT_FILE = Path("data/recall2_results.json")
RAW_DIR = Path("data/recall2_raw")   # где сохраняем сырые ответы
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://event-eu-cc.igg.com/event/recall2/"
CODE = "F76CDD0B"  # твой код приглашения
FULL_URL = f"{BASE_URL}?code={CODE}"
AJAX_URL = f"{BASE_URL}ajax.req.php?action=init&code={CODE}"

MAX_ATTEMPTS = 3
DELAY_BETWEEN = (1.0, 2.0)

# ─────────────────────────────
# 🧩 Загрузка аккаунтов
# ─────────────────────────────
def load_accounts() -> list[tuple[str, dict]]:
    if not DATA_FILE.exists():
        logger.error(f"⚠️ Файл {DATA_FILE} не найден!")
        return []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        accs = []
        for entry in data:
            if not isinstance(entry, dict):
                continue
            for key, val in entry.items():
                if key.lower() in ("mail", "paswd"):
                    continue
                if isinstance(val, dict) and "PHPSESSID" in val:
                    accs.append((key, val))
        logger.info(f"✅ Загружено {len(accs)} аккаунтов из {DATA_FILE.name}")
        return accs
    except Exception as e:
        logger.error(f"❌ Ошибка чтения {DATA_FILE}: {e}")
        return []

# ─────────────────────────────
# 🚀 Основная логика Recall2
# ─────────────────────────────
async def handle_account(p, uid: str, cookies_dict: dict) -> tuple[str, str]:
    """
    Одна итерация: запуск браузера, переход и выполнение запроса action=init.
    Возвращает (summary_msg, raw_response_path)
    """
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            ctx = await launch_masked_persistent_context(
                p,
                user_data_dir=str(PROFILE_DIR / f"{uid}_recall"),
                headless=True,
                slow_mo=25,
                profile=get_random_browser_profile(),
            )
            context, page = ctx["context"], ctx["page"]

            # 🍪 Добавляем cookies
            try:
                await context.add_cookies(cookies_to_playwright(cookies_dict))
            except Exception as e:
                logger.warning(f"[recall2] ⚠️ {uid}: не удалось добавить cookies ({e})")
                try:
                    await context.close()
                except Exception:
                    pass
                return "Ошибка: некорректные cookies", ""

            # 🌐 Загружаем основную страницу
            await page.goto(FULL_URL, wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(random.uniform(2, 4))

            # 📡 Выполняем запрос через сам браузер (fetch)
            js_code = f"""
                async () => {{
                    try {{
                        const res = await fetch("{AJAX_URL}", {{
                            method: "GET",
                            credentials: "include",
                            headers: {{
                                "X-Requested-With": "XMLHttpRequest",
                            }},
                        }});
                        const text = await res.text();
                        // Возвращаем статус и текст в объекте, чтобы JS-ошибки были видимы
                        return JSON.stringify({{
                            status: res.status,
                            body: text
                        }});
                    }} catch (e) {{
                        return JSON.stringify({{error: String(e)}});
                    }}
                }}
            """
            try:
                eval_result = await page.evaluate(js_code)
            except Exception as e:
                eval_result = json.dumps({"error": f"Ошибка выполнения fetch: {e}"})

            # eval_result — строка JSON; попытаемся распарсить
            raw_text = ""
            try:
                parsed = json.loads(eval_result)
                if "error" in parsed:
                    raw_text = f"JS error: {parsed.get('error')}"
                else:
                    # здесь parsed['body'] — сырой текст ответа от сервера (HTML или JSON)
                    raw_text = parsed.get("body", "")
                    # сохраняем HTTP статус тоже (если есть)
                    http_status = parsed.get("status")
            except Exception:
                # В редких случаях evaluate вернёт просто plain text
                raw_text = str(eval_result)
                http_status = None

            # Сохраняем сырой ответ в файл (полный текст)
            raw_file = RAW_DIR / f"{uid}.txt"
            try:
                raw_file.write_text(raw_text, encoding="utf-8")
                logger.debug(f"[recall2] {uid}: raw ответ сохранён в {raw_file}")
            except Exception as e:
                logger.warning(f"[recall2] {uid}: не удалось сохранить raw ответ: {e}")

            # Анализируем ответ (пытаемся распарсить JSON если это JSON)
            summary_msg = ""
            try:
                maybe_json = json.loads(raw_text)
                status = str(maybe_json.get("status", "")) if isinstance(maybe_json, dict) else ""
                m = maybe_json.get("msg", "") if isinstance(maybe_json, dict) else ""
                if status == "1":
                    summary_msg = f"✅ {uid}: {m or 'Приглашение успешно засчитано'}"
                elif isinstance(m, str) and "already" in m.lower():
                    summary_msg = f"🔹 {uid}: Уже был приглашён"
                elif isinstance(m, str) and ("not yet" in m.lower() or "coming soon" in m.lower()):
                    summary_msg = f"⚠️ {uid}: Событие ещё не началось"
                else:
                    summary_msg = f"❓ {uid}: {m or 'Неизвестный JSON-ответ'}"
            except Exception:
                # не JSON — проверим по HTML/тексту
                lowered = (raw_text or "").lower()
                if "access denied" in lowered or "forbidden" in lowered:
                    summary_msg = f"⚠️ {uid}: Access Denied / нет прав"
                elif "already been invited" in lowered or "already invited" in lowered:
                    summary_msg = f"🔹 {uid}: Уже был приглашён"
                elif "thank you" in lowered or "reward" in lowered or "successfully" in lowered:
                    summary_msg = f"✅ {uid}: Приглашение засчитано (по тексту)"
                elif "not yet" in lowered or "event has not yet begun" in lowered or "coming soon" in lowered:
                    summary_msg = f"⚠️ {uid}: Событие ещё не началось"
                else:
                    snippet = (raw_text or "")[:200].replace("\n", " ")
                    summary_msg = f"❓ {uid}: неожиданный ответ (см. файл): {snippet}"

            logger.info(f"[recall2_playwright] {summary_msg}")

            # Закрываем и возвращаем
            try:
                await page.close()
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass

            return summary_msg, str(raw_file)

        except PlaywrightTimeoutError:
            logger.warning(f"[recall2] ⚠️ {uid}: таймаут загрузки (попытка {attempt})")
        except Exception as e:
            logger.warning(f"[recall2] ⚠️ {uid}: ошибка браузера (попытка {attempt}): {e}")
        await asyncio.sleep(1.5 + attempt * 0.5)

    logger.error(f"[recall2] ❌ {uid}: не удалось обработать после {MAX_ATTEMPTS} попыток")
    return "Ошибка: не удалось открыть страницу", ""

# ─────────────────────────────
# 🎯 Главная функция
# ─────────────────────────────
async def run_recall2():
    accounts = load_accounts()
    if not accounts:
        logger.warning("⚠️ Нет аккаунтов для запуска Recall2.")
        return

    results = {}
    async with async_playwright() as p:
        for idx, (uid, cookies_dict) in enumerate(accounts, start=1):
            logger.info(f"🔹 [{idx}/{len(accounts)}] Аккаунт {uid} — выполняю Recall2")
            try:
                summary, raw_path = await handle_account(p, uid, cookies_dict)
                results[uid] = {"summary": summary, "raw_file": raw_path}
            except Exception as e:
                logger.error(f"[recall2] ❌ {uid}: {e}")
                results[uid] = {"summary": f"Ошибка: {e}", "raw_file": ""}
            await asyncio.sleep(random.uniform(*DELAY_BETWEEN))

    # 💾 Сохраняем результат
    try:
        RESULT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(RESULT_FILE, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"✅ Recall2 завершён. Результаты сохранены в {RESULT_FILE.name}")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении результатов: {e}")

# ─────────────────────────────
# 💡 Запуск вручную
# ─────────────────────────────
if __name__ == "__main__":
    asyncio.run(run_recall2())
