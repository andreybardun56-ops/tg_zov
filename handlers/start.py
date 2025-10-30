# tg_zov/handlers/start.py
import json
import os
import asyncio
from html import escape
from services.logger import logger
from aiogram import Router, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery
)
from config import ADMIN_IDS
from services.login_and_refresh import process_all_files
from services.lucky_wheel_auto import run_lucky_wheel
from services.puzzle_claim_auto import claim_puzzle
from services.puzzle_claim import issue_puzzle_codes
from services.dragon_quest import run_dragon_quest
from services.accounts_manager import load_all_users
from services.farm_puzzles_auto import run_farm_puzzles_for_all
from services.castle_api import extract_player_info_from_page, refresh_cookies_mvp
from services.event_manager import run_full_event_cycle
from keyboards.inline import (
    get_delete_accounts_kb,
    get_puzzle_accounts_kb,
    get_puzzle_numbers_kb,
    get_exchange_accounts_kb,
    get_contact_dev_kb
)
from keyboards.inline import send_exchange_items
from services.event_checker import check_all_events
router = Router()
USER_ACCOUNTS_FILE = "data/user_accounts.json"
# ----------------------------- 👥 Главное меню -----------------------------
user_main_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👤 Управление аккаунтами")],
        [
            KeyboardButton(text="🎁 Ввод промокода"),
            KeyboardButton(text="🧩 Пазлы")
        ],
        [KeyboardButton(text="📩 Связь с разработчиком")]
    ],
    resize_keyboard=True
)

# ----------------------------- 🛡️ Админские меню -----------------------------
admin_main_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🎯 События")],
        [KeyboardButton(text="⚙️ Управление")],
        [KeyboardButton(text="🔧 Система")]
    ],
    resize_keyboard=True,
    input_field_placeholder="Выбери раздел 👑"
)

# 🎯 События
admin_events_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="━━━━━━━━━━━ 🎁 Основные 🎯 ━━━━━━━━━━━")],
        [
            KeyboardButton(text="🎁 10 дней призов"),
            KeyboardButton(text="🎡 Колесо фортуны")
        ],
        [
            KeyboardButton(text="🃏 Найди пару"),
            KeyboardButton(text="⚙️ Создающая машина")
        ],
        [KeyboardButton(text="🐉 Рыцари Драконы")],
        [KeyboardButton(text="━━━━━━━━━━━ 🧩 Пазлы ━━━━━━━━━━━")],
        [KeyboardButton(text="🧩 Пазлы (подменю)")],
        [KeyboardButton(text="🔙 Главное меню")]
    ],
    resize_keyboard=True
)

# 🧩 Подменю пазлов
admin_puzzles_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="━━━━━━━━━━━ 🧩 Пазлы ━━━━━━━━━━━")],
        [
            KeyboardButton(text="🧩 Получить пазлы"),
            KeyboardButton(text="🧩 Фарм пазлов")
        ],
        [KeyboardButton(text="🔙 Назад к событиям")]
    ],
    resize_keyboard=True
)

# ⚙️ Управление
admin_manage_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="━━━━━━━━━━━ ⚙️ Управление ━━━━━━━━━━━")],
        [
            KeyboardButton(text="👤 Управление аккаунтами"),
            KeyboardButton(text="🔍 Проверить пары")
        ],
        [
            KeyboardButton(text="📊 Проверить акции"),  # 🔥 новая кнопка
            KeyboardButton(text="🔁 Автосбор наград")
        ],
        [
            KeyboardButton(text="🔄 Обновить cookies"),
            KeyboardButton(text="🧩 Обновить cookies в базе")
        ],
        [KeyboardButton(text="🎁 Ввод промокода")],
        [KeyboardButton(text="🔙 Главное меню")]
    ],
    resize_keyboard=True
)

# 🔧 Система
admin_system_menu = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="━━━━━━━━━━━ 🔧 Система ━━━━━━━━━━━")],
        [
            KeyboardButton(text="🧪 Тест"),
            KeyboardButton(text="📊 Статистика")
        ],
        [KeyboardButton(text="♻️ Перезапустить бота")],
        [KeyboardButton(text="🔙 Главное меню")]
    ],
    resize_keyboard=True
)


accounts_kb = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="➕ Добавить аккаунт"),
            KeyboardButton(text="🗑 Удалить аккаунт")
        ],
        [
            KeyboardButton(text="📜 Список аккаунтов"),
            KeyboardButton(text="🔙 Назад")
        ]
    ],
    resize_keyboard=True
)

@router.message(F.text == "🔙 Главное меню")
async def back_to_main_admin(message: types.Message):
    if message.from_user.id in ADMIN_IDS:
        await message.answer("🏠 Главное админ-меню:", reply_markup=admin_main_menu)
    else:
        await message.answer("🚫 У тебя нет доступа.")

@router.message(F.text == "🎯 События")
async def open_events_menu(message: types.Message):
    await message.answer("🎯 Меню событий:", reply_markup=admin_events_menu)

@router.message(F.text == "🧩 Пазлы (подменю)")
async def open_puzzles_submenu(message: types.Message):
    await message.answer("🧩 Меню пазлов и мини-игр:", reply_markup=admin_puzzles_menu)

@router.message(F.text == "🔙 Назад к событиям")
async def back_to_events(message: types.Message):
    await message.answer("🎯 Меню событий:", reply_markup=admin_events_menu)

@router.message(F.text == "⚙️ Управление")
async def open_manage_menu(message: types.Message):
    await message.answer("⚙️ Меню управления:", reply_markup=admin_manage_menu)

@router.message(F.text == "🔧 Система")
async def open_system_menu(message: types.Message):
    await message.answer("🔧 Системное меню:", reply_markup=admin_system_menu)

# ------------------------------------ 🚀 /start ------------------------------------
@router.message(Command("start"))
async def start_cmd(message: types.Message):
    """Приветственное сообщение и выбор панели"""
    user_id = message.from_user.id
    is_admin = user_id in ADMIN_IDS
    kb = admin_main_menu if is_admin else user_main_kb  # ✅ заменено admin_main_kb → admin_main_menu

    text = (
        "👋 Привет! Я твой помощник по акциям Castle Clash.\n\n"
        "Используй кнопки ниже для управления своими аккаунтами 👇"
    )
    await message.answer(text, reply_markup=kb)

# ------------------------------------ 👤 Управление аккаунтами ------------------------------------
@router.message(F.text == "👤 Управление аккаунтами")
async def manage_accounts(message: types.Message):
    await message.answer("👤 Выбери действие:", reply_markup=accounts_kb)


# ➕ Добавить аккаунт
@router.message(F.text == "➕ Добавить аккаунт")
async def ask_for_mvp_link(message: types.Message):
    await message.answer("📎 Отправь свою MVP ссылку, чтобы я добавил аккаунт.")


# 🧠 Добавление аккаунта по MVP ссылке
@router.message(F.text.contains("castleclash.igg.com") & F.text.contains("signed_key"))
async def add_account_from_mvp(message: types.Message):
    user_id = str(message.from_user.id)
    url = message.text.strip()

    await message.answer("🔍 Загружаю страницу, извлекаю данные аккаунта...")

    info = await extract_player_info_from_page(url)
    if not info.get("success"):
        err = info.get("error", "Неизвестная ошибка")
        # ВАЖНО: ошибка может содержать HTML-теги — экранируем!
        safe = escape(str(err))
        await message.answer(f"❌ Не удалось получить данные: <code>{safe}</code>", parse_mode="HTML")
        return

    uid = info.get("uid")
    username = info.get("username", "Игрок")

    if not uid:
        await message.answer("⚠️ Не удалось получить IGG ID. Проверь ссылку.")
        return

    # Проверяем на дубликаты
    all_data = load_all_users()
    for other_user, acc_list in all_data.items():
        if any(acc.get("uid") == uid for acc in acc_list):
            await message.answer("⚠️ Этот IGG ID уже добавлен другим пользователем.")
            return

    # Загружаем аккаунты пользователя
    accounts = load_accounts(user_id)
    if any(acc.get("uid") == uid for acc in accounts):
        await message.answer(f"⚠️ Аккаунт <code>{uid}</code> уже есть.", parse_mode="HTML")
        return

    # ✅ Добавляем аккаунт
    new_acc = {"uid": uid, "username": username, "mvp_url": url}
    accounts.append(new_acc)
    save_accounts(user_id, accounts)

    # ♻️ Обновляем cookies
    await message.answer(f"♻️ Обновляю cookies для <b>{username}</b>...", parse_mode="HTML")
    result = await refresh_cookies_mvp(user_id, uid)

    if result.get("success"):
        msg = (
            f"✅ Аккаунт <b>{username}</b> (IGG ID: <code>{uid}</code>) добавлен!\n"
            f"🍪 Cookies обновлены ({len(result['cookies'])} шт.)"
        )
    else:
        err = escape(str(result.get("error", "неизвестно")))
        msg = (
            f"✅ Аккаунт <b>{username}</b> (IGG ID: <code>{uid}</code>) добавлен!\n"
            f"⚠️ Cookies не удалось обновить: <code>{err}</code>"
        )

    await message.answer(msg, parse_mode="HTML")

    # 🚀 Автоматически запускаем проверку акций
    await message.answer("🎯 Запускаю автоматическую проверку акций...")
    asyncio.create_task(run_full_event_cycle(bot=message.bot, manual=True))
# 🗑 Удалить аккаунт (через inline кнопки)
@router.message(F.text == "🗑 Удалить аккаунт")
async def delete_account_prompt(message: types.Message):
    user_id = str(message.from_user.id)
    accounts = load_accounts(user_id)
    if not accounts:
        await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
        return

    kb = get_delete_accounts_kb(accounts)
    await message.answer("🗑 Выбери аккаунт для удаления:", reply_markup=kb)


@router.callback_query(F.data.startswith("del:"))
async def confirm_delete_account(callback: CallbackQuery):
    user_id = str(callback.from_user.id)
    uid_to_delete = callback.data.split(":")[1]

    accounts = load_accounts(user_id)
    updated_accounts = [acc for acc in accounts if acc.get("uid") != uid_to_delete]

    if len(updated_accounts) == len(accounts):
        await callback.answer("⚠️ Аккаунт не найден.", show_alert=True)
        return

    save_accounts(user_id, updated_accounts)
    await callback.message.edit_text(f"✅ Аккаунт <code>{uid_to_delete}</code> удалён.", parse_mode="HTML")


# 📜 Список аккаунтов
@router.message(F.text == "📜 Список аккаунтов")
async def list_accounts(message: types.Message):
    user_id = str(message.from_user.id)
    accounts = load_accounts(user_id)
    if not accounts:
        await message.answer("⚠️ Аккаунты не найдены.")
        return

    text = "📜 <b>Твои аккаунты:</b>\n\n"
    for i, acc in enumerate(accounts, 1):
        text += f"{i}. 👤 <b>{acc.get('username', 'Без имени')}</b> — <code>{acc.get('uid')}</code>\n"
    await message.answer(text, parse_mode="HTML")


# 🔙 Назад
@router.message(F.text == "🔙 Назад")
async def go_back(message: types.Message):
    kb = admin_main_menu if message.from_user.id in ADMIN_IDS else user_main_kb
    await message.answer("🔙 Главное меню:", reply_markup=kb)


# 📩 Связь с разработчиком
@router.message(F.text == "📩 Связь с разработчиком")
async def contact_dev(message: types.Message):
    await message.answer(
        "📩 Если возникли вопросы или нужна помощь — напиши разработчику 👇",
        reply_markup=get_contact_dev_kb()
    )
@router.message(F.text == "♻️ Перезапустить бота")
async def restart_bot(message: types.Message):
    """Перезапускает systemd-сервис tg_zov.service (без пароля, через sudoers)."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("🚫 У тебя нет доступа.")
        return

    await message.answer("♻️ Перезапускаю бота через systemctl...")

    # выполняем безопасно через subprocess
    import subprocess
    try:
        result = subprocess.run(
            ["sudo", "systemctl", "restart", "tg_zov.service"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode == 0:
            await message.answer("🔄 Команда на перезапуск отправлена.")
        else:
            await message.answer(f"⚠️ Ошибка при выполнении restart:\n<code>{result.stderr}</code>", parse_mode="HTML")
    except Exception as e:
        await message.answer(f"❌ Не удалось выполнить restart: <code>{e}</code>", parse_mode="HTML")

@router.message(F.text == "📊 Проверить акции")
async def check_events_cmd(message: types.Message):
    """Запускает проверку всех акций в фоне, не блокируя бота."""
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("🚫 Только для администратора.")
        return

    await message.answer("🔍 Проверка акций запущена в фоне... ⏳")

    async def background_check():
        try:
            results = await check_all_events(bot=message.bot, admin_id=message.from_user.id)
            text = "📊 <b>Статус акций:</b>\n\n"
            for name, active in results.items():
                emoji = "✅" if active else "⚠️"
                text += f"{emoji} {name}\n"
            await message.answer(text, parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Ошибка при проверке акций: <code>{e}</code>", parse_mode="HTML")

    # 🔥 запускаем проверку в фоне
    asyncio.create_task(background_check())

# ------------------------------------ 🧩 ОБНОВИТЬ COOKIES В БАЗЕ ------------------------------------
@router.message(F.text == "🧩 Обновить cookies в базе")
async def refresh_cookies_in_database(message: types.Message):
    """Фоновое обновление cookies всех аккаунтов через login_and_refresh с уведомлениями."""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.answer("🚫 У тебя нет доступа к этой функции.")
        return

    status_msg = await message.answer("🧩 Начинаю обновление cookies... ⏳")

    async def progress_update(percent: float, done: int, total: int):
        """Обновляет сообщение каждые 10%."""
        if int(percent * 100) % 5 == 0:  # каждые 5 %
            try:
                await status_msg.edit_text(
                    f"🧩 Обновление cookies...\n\n"
                    f"📊 Прогресс: <b>{percent*100:.1f}%</b>\n"
                    f"✅ Обработано: <b>{done}</b> из <b>{total}</b>",
                    parse_mode="HTML",
                )
            except Exception:
                pass  # если Telegram ограничил частоту обновлений

    async def run_update():
        try:
            await process_all_files(progress_callback=progress_update)
            await status_msg.edit_text(
                "✅ Обновление cookies завершено!\n"
                "📁 Логи: <code>logs/login_refresh.log</code>",
                parse_mode="HTML"
            )
        except Exception as e:
            await status_msg.edit_text(f"❌ Ошибка при обновлении: <code>{e}</code>", parse_mode="HTML")

    asyncio.create_task(run_update())

# ------------------------------------ 🧩 Фарм пазлов ------------------------------------
@router.message(F.text == "🧩 Фарм пазлов")
async def start_farm_puzzles(message: types.Message):
    """Запуск фарма пазлов (только для админа)."""
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.answer("🚫 У тебя нет доступа к этой функции.")
        return

    await message.answer("⏳ Запускаю фарм пазлов... Это может занять несколько минут.")
    asyncio.create_task(run_farm_puzzles_for_all(message.bot))  # 🔥 асинхронно, бот не блокируется

# --- Подменю "Пазлы" (reply-кнопки) ---
puzzle_submenu = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="🧩 Получить пазлы"),
            KeyboardButton(text="♻️ Обменять пазлы")
        ],
        [KeyboardButton(text="🔙 Назад")]
    ],
    resize_keyboard=True
)
# ------------------------------------ 🧩 Пазлы ------------------------------------
@router.message(F.text == "🧩 Пазлы")
async def puzzles_menu(message: types.Message):
    from services.event_checker import get_event_status
    is_active = await get_event_status("puzzle2")
    if not is_active:
        await message.answer("⚠️ Акция «🧩 Пазлы» сейчас не активна.")
        return
    """Показывает меню выбора между получением и обменом пазлов."""
    await message.answer("🧩 Выбери действие:", reply_markup=puzzle_submenu)

# 🧩 Получить пазлы
@router.message(F.text == "🧩 Получить пазлы")
async def get_puzzles(message: types.Message):
    """Открывает выбор аккаунта для получения пазлов."""
    from services.accounts_manager import get_all_accounts
    user_id = str(message.from_user.id)
    is_admin = message.from_user.id in ADMIN_IDS
    accounts = get_all_accounts(user_id)

    if not accounts:
        await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
        return

    await message.answer(
        "🎯 Выбери аккаунт, либо получи 30 кодов (для админов):",
        reply_markup=get_puzzle_accounts_kb(accounts, is_admin)
    )


# ♻️ Обменять пазлы
@router.message(F.text == "♻️ Обменять пазлы")
async def exchange_puzzles(message: types.Message):
    """Открывает выбор аккаунта для обмена пазлов."""
    from services.accounts_manager import get_all_accounts
    user_id = str(message.from_user.id)
    accounts = get_all_accounts(user_id)

    if not accounts:
        await message.answer("⚠️ У тебя нет добавленных аккаунтов.")
        return

    # Пока просто сообщение — позже сюда добавим inline-меню выбора обмена
    await message.answer(
        "♻️ Выбери аккаунт для обмена пазлов:",
        reply_markup=get_exchange_accounts_kb(accounts)
    )
# --- ДОБАВЛЕНИЕ обработчика кнопки “🎁 Получить 30 пазлов” ---
@router.callback_query(F.data == "get_30_puzzles")
async def give_30_puzzles_cb(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in ADMIN_IDS:
        await callback.answer("🚫 У тебя нет доступа к этой функции.", show_alert=True)
        return

    await callback.answer()  # закроет "часики"
    await callback.message.answer("⏳ Собираю твои 30 кодов...")

    codes = await issue_puzzle_codes(user_id)
    if not codes:
        await callback.message.answer("⚠️ Нет доступных кодов в puzzle_data.jsonl.")
        return

    formatted = "\n".join(codes)
    await callback.message.answer(f"🎁 Твои 30 кодов:\n<code>{formatted}</code>", parse_mode="HTML")

# 🧩 После выбора аккаунта — показываем сетку пазлов 1–9
@router.callback_query(F.data.startswith("puzzle_acc:"))
async def select_puzzle_account(callback: CallbackQuery):
    uid = callback.data.split(":")[1]
    await callback.message.edit_text(
        f"🧩 Аккаунт выбран: <b>{uid}</b>\nТеперь выбери номер пазла для получения:",
        parse_mode="HTML",
        reply_markup=get_puzzle_numbers_kb(uid)
    )

# 🧩 Обработка выбора пазла
@router.callback_query(F.data.startswith("puzzle_num:"))
async def handle_puzzle_claim(callback: CallbackQuery):
    _, uid, puzzle_num = callback.data.split(":")
    user_id = str(callback.from_user.id)
    bot = callback.message.bot

    await callback.answer()
    msg = await callback.message.answer(
        f"⏳ Начинаю получение пазла <b>{puzzle_num}</b> для аккаунта <code>{uid}</code>...",
        parse_mode="HTML"
    )

    async def run_claim():
        try:
            await claim_puzzle(user_id, uid, int(puzzle_num), bot, msg)
        except Exception as e:
            await msg.edit_text(f"❌ Ошибка при получении пазла: <code>{e}</code>", parse_mode="HTML")

    asyncio.create_task(run_claim())

# ------------------------------------ ♻️ ОБМЕН ПАЗЛОВ ------------------------------------
from aiogram.types import CallbackQuery
import logging
from services.puzzle_exchange_auto import get_fragment_count, exchange_item

logger = logging.getLogger("exchange")

@router.callback_query(F.data.startswith("exchange_acc:"))
async def start_exchange(callback: CallbackQuery):
    """Начало обмена — проверяем количество фрагментов и показываем предметы"""
    uid = callback.data.split(":")[1]
    user_id = callback.from_user.id
    await callback.answer()
    msg = await callback.message.answer("🔍 Проверяю количество фрагментов...")

    try:
        # 💾 Получаем количество фрагментов через run_event_with_browser
        result = await get_fragment_count(user_id, uid)
        msg_text = result.get("message", "")
        success = result.get("success", False)

        if not success or "0" in msg_text:
            await msg.edit_text("⚠️ У тебя нет фрагментов для обмена.")
            return

        await msg.edit_text(
            f"{msg_text}\nВыбери предмет для обмена 👇",
            parse_mode="HTML",
        )

        # Показываем доступные предметы (твоя существующая функция)
        await send_exchange_items(callback.message.bot, user_id, uid)

    except Exception as e:
        safe_err = escape(str(e))
        await msg.edit_text(
            f"❌ Ошибка при открытии обмена:\n<code>{safe_err}</code>",
            parse_mode="HTML",
        )
        logger.error(f"[exchange] ❌ Ошибка открытия обмена: {e}")

@router.callback_query(F.data.startswith("exchange_item:"))
async def handle_exchange(callback: CallbackQuery):
    """Обработка выбора конкретного предмета для обмена"""
    await callback.answer()

    # callback_data = "exchange_item:<uid>:<item_id>"
    _, uid, item_id = callback.data.split(":", 2)

    user_id = callback.from_user.id
    msg = await callback.message.answer("🔁 Выполняю обмен...")

    try:
        result = await exchange_item(user_id, uid, item_id)

        if result.get("success"):
            msg_text = result.get("message", "✅ Обмен завершён успешно!")
            back_kb = InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ Назад к обмену", callback_data=f"exchange_acc:{uid}")]
                ]
            )
            await msg.edit_text(f"✅ <b>{msg_text}</b>", parse_mode="HTML", reply_markup=back_kb)
            logger.info(f"[{uid}] ✅ Успешный обмен {item_id}")
        else:
            err_msg = result.get("message", "Неизвестная ошибка")
            await msg.edit_text(f"❌ Ошибка при обмене:\n<code>{escape(err_msg)}</code>", parse_mode="HTML")
            logger.error(f"[{uid}] ❌ Ошибка обмена {item_id}: {err_msg}")

    except Exception as e:
        safe_err = escape(str(e))
        await msg.edit_text(f"❌ Ошибка при обмене:\n<code>{safe_err}</code>", parse_mode="HTML")
        logger.error(f"[exchange_handler] ❌ Ошибка при обмене: {e}")

#------------------------------ === Обработка кнопки 🎡 Колесо фортуны ===----------------------------------
@router.message(lambda m: m.text == "🎡 Колесо фортуны")
async def handle_lucky_wheel(message: types.Message):
    """Асинхронный запуск 'Колеса фортуны' — бот не блокируется."""
    user_id = message.from_user.id
    if str(user_id) not in [str(i) for i in ADMIN_IDS]:
        await message.answer("🚫 У тебя нет доступа к этой функции.")
        return

    await message.answer("🎡 Запускаю Колесо фортуны в фоне... ⏳")

    async def send_to_tg(uid: str, text: str):
        """📩 Обратная связь во время выполнения."""
        try:
            if uid == "system":
                await message.answer(text)
            else:
                await message.answer(f"[{uid}] {text}")
        except Exception:
            pass

    async def background_wheel():
        try:
            await run_lucky_wheel(send_callback=send_to_tg)
            await message.answer("✅ Колесо фортуны завершено!", parse_mode="HTML")
        except Exception as e:
            await message.answer(f"❌ Ошибка при запуске Колеса фортуны: <code>{e}</code>", parse_mode="HTML")

    # 🚀 Запускаем в фоне, не блокируя Telegram
    asyncio.create_task(background_wheel())

# ------------------------------------ 🐉 Рыцари Драконы ------------------------------------
from datetime import datetime  # ← правильный импорт

@router.message(F.text == "🐉 Рыцари Драконы")
async def handle_dragon_quest(message: types.Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_IDS:
        await message.answer("🚫 У тебя нет доступа к этой функции.")
        return

    msg = await message.answer("🐉 Запускаю событие <b>Рыцари Драконы</b> для всех аккаунтов...", parse_mode="HTML")

    all_users = load_all_users()
    results_text = ""
    total_success = total_errors = 0
    tasks = []

    async def process_account(owner_id, acc):
        nonlocal total_success, total_errors, results_text
        acc_uid = str(acc.get("uid"))
        username = acc.get("username", "Игрок")
        try:
            result = await run_dragon_quest(owner_id, acc_uid)
            success = result.get("success", False)
            if success:
                total_success += 1
                results_text += f"✅ <b>{username}</b> ({acc_uid}) — успешно\n"
            else:
                total_errors += 1
                results_text += f"⚠️ <b>{username}</b> ({acc_uid}) — ошибка\n"
        except Exception as e:
            total_errors += 1
            results_text += f"❌ <b>{username}</b> ({acc_uid}) — исключение: {e}\n"

    # 🚀 создаём задачи для всех аккаунтов
    for uid, accounts in all_users.items():
        for acc in accounts:
            tasks.append(asyncio.create_task(process_account(uid, acc)))

    await asyncio.gather(*tasks)

    summary = (
        f"🐲 <b>Рыцари Драконы — завершено</b>\n\n"
        f"✅ Успешно: <b>{total_success}</b>\n"
        f"⚠️ Ошибок: <b>{total_errors}</b>\n"
        f"🕒 {datetime.now():%Y-%m-%d %H:%M:%S}"
    )
    await msg.edit_text(summary, parse_mode="HTML")

    try:
        await message.bot.send_message(ADMIN_IDS[0], f"{summary}\n\n{results_text}", parse_mode="HTML")
    except Exception:
        pass

# ------------------------------------ ⚙️ Работа с JSON ------------------------------------
def load_all_users():
    if not os.path.exists(USER_ACCOUNTS_FILE):
        return {}
    try:
        with open(USER_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}
def load_accounts(user_id: str):
    data = load_all_users()
    return data.get(user_id, [])
def save_accounts(user_id: str, accounts: list):
    data = load_all_users()
    data[user_id] = accounts
    os.makedirs(os.path.dirname(USER_ACCOUNTS_FILE), exist_ok=True)
    with open(USER_ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
