# tg_zov/services/regress_10th1.py
import os
import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from aiogram.types import FSInputFile
from services.logger import logger
from services.accounts_manager import get_active_account
from services.castle_api import load_cookies_for_account

# 🎯 URL акции
EVENT_URL = "http://event-cc.igg.com/event/gas/"
REWARD_URL = "http://event-cc.igg.com/event/gas/ajax.req.php?action=battlepower"


async def visit_gas_event(user_id: str, idx: int = 1, call=None):
    """
    🚀 Один проход: открыть страницу Gas Event, получить награду и сделать скрин
    """
    playwright = await async_playwright().start()
    browser = None
    page = None
    screenshot_path = None

    try:
        # 🧱 Аккаунт и куки
        account = get_active_account(user_id)
        if not account:
            return {"success": False, "message": "⚠️ Активный аккаунт не найден."}

        uid = account.get("uid")
        cookies_dict = load_cookies_for_account(user_id, uid)
        if not cookies_dict:
            return {"success": False, "message": "⚠️ Cookies не найдены."}

        logger.info(f"[GAS] ▶ Запуск #{idx} для UID={uid}")

        # 📁 Папка для скринов
        screenshots_dir = os.path.join("logs", "screenshots", f"{datetime.now():%Y-%m-%d}")
        os.makedirs(screenshots_dir, exist_ok=True)

        # 🚀 Запускаем браузер
        browser = await playwright.chromium.launch(headless=False, slow_mo=200)
        context = await browser.new_context(viewport={"width": 1280, "height": 720})

        # 🍪 Добавляем куки
        for name, value in cookies_dict.items():
            await context.add_cookies([{"name": name, "value": value, "url": EVENT_URL}])

        # 🌐 Открываем страницу
        page = await context.new_page()
        await page.goto(EVENT_URL, wait_until="domcontentloaded", timeout=30000)
        logger.info(f"[GAS] 🌍 Страница открыта: {EVENT_URL}")

        # ✅ Принятие cookies
        try:
            await page.wait_for_selector('div.i-cookie__btn[data-value="all"]', timeout=5000)
            await page.click('div.i-cookie__btn[data-value="all"]')
            logger.info("[GAS] ✅ Приняты cookies")
        except Exception:
            pass

        await asyncio.sleep(3)

        # ⚡️ Отправляем запрос на получение награды
        logger.info("[GAS] 🎯 Отправляем запрос на получение награды...")
        response = await page.request.get(REWARD_URL)
        status = response.status
        text = await response.text()
        logger.info(f"[GAS] 📡 Ответ {status}: {text[:200]}")

        reward_info = f"HTTP {status}"
        if '"code":1' in text or "success" in text.lower():
            reward_info = "✅ Награда успешно получена!"
        elif '"code":0' in text:
            reward_info = "⚠️ Награду уже получали."
        else:
            reward_info = f"⚠️ Ответ: {text[:200]}"

        # 📸 Скриншот
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = os.path.join(screenshots_dir, f"gas_event_{idx}_{ts}.png")
        await page.screenshot(path=screenshot_path)
        logger.info(f"[GAS] 📸 Скриншот сохранён: {screenshot_path}")

        # 📤 Telegram
        if call:
            caption = (
                f"🎯 Gas Event #{idx}\n"
                f"👤 UID: {uid}\n"
                f"{reward_info}\n"
                f"📸 Скриншот готов!"
            )
            await call.message.answer_photo(FSInputFile(screenshot_path), caption=caption)

        await browser.close()
        await playwright.stop()
        return {"success": True, "message": reward_info}

    except Exception as e:
        logger.exception(f"[GAS] ❌ Ошибка при входе #{idx}: {e}")

        if screenshot_path is None:
            screenshots_dir = os.path.join("logs", "screenshots", f"{datetime.now():%Y-%m-%d}")
            os.makedirs(screenshots_dir, exist_ok=True)
            screenshot_path = os.path.join(screenshots_dir, f"gas_error_{idx}.png")

        try:
            if page:
                await page.screenshot(path=screenshot_path)
                logger.info(f"[GAS] 📸 Скриншот ошибки: {screenshot_path}")
        except Exception as se:
            logger.warning(f"[GAS] ⚠️ Не удалось сделать скриншот ошибки: {se}")

        if call:
            await call.message.answer(f"⚠️ Ошибка при входе #{idx}: {e}")

        if browser:
            await browser.close()
        await playwright.stop()
        return {"success": False, "message": f"❌ Ошибка #{idx}: {e}"}


async def run_mass_requests(count: int = 1, call=None):
    """
    🚀 Запуск нескольких заходов подряд (получение награды)
    """
    logger.info(f"[GAS] ▶ Старт {count} заходов")

    for i in range(count):
        await visit_gas_event(call.from_user.id, i + 1, call)
        await asyncio.sleep(2)

    logger.info(f"[GAS] ✅ Все {count} заходов завершены")
    return {"success": True, "message": f"✅ Все {count} заходов завершены"}
