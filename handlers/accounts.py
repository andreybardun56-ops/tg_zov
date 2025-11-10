# tg_zov/handlers/accounts.py
import json
import os

from aiogram import Router, types, F
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from config import USER_ACCOUNTS_FILE
from services.accounts_manager import (
    add_account,
    remove_account,
    get_all_accounts,
    get_active_account,
    set_active_account
)
from services.castle_api import extract_player_info_from_page, refresh_cookies_mvp

router = Router()


# =============================
# ⚙️ Состояния FSM
# =============================
class AccountManagementStates(StatesGroup):
    waiting_for_deletion_uid = State()
    waiting_for_active_uid = State()


# =============================
# 💾 Загрузка всех пользователей (для проверки дублей UID)
# =============================
def load_all_users() -> dict:
    if not os.path.exists(USER_ACCOUNTS_FILE):
        return {}
    try:
        with open(USER_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


# =============================
# 👤 Главное меню управления аккаунтами
# =============================
@router.message(F.text == "👤 Управление аккаунтами")
async def show_accounts_menu(message: types.Message):
    user_id = str(message.from_user.id)
    accounts = get_all_accounts(user_id)
    active = get_active_account(user_id)

    if not accounts:
        text = "📭 У тебя пока нет добавленных аккаунтов."
    else:
        text = "👤 Твои аккаунты:\n"
        for acc in accounts:
            mark = " ✅ (активный)" if active and acc["uid"] == active.get("uid") else ""
            username = acc.get("username", "Игрок")
            text += f"🔹 {username} — UID: <code>{acc['uid']}</code>{mark}\n"

    kb = [
        [types.KeyboardButton(text="➕ Добавить аккаунт")],
        [types.KeyboardButton(text="❌ Удалить аккаунт")],
        [types.KeyboardButton(text="🔘 Выбрать активный")],
        [types.KeyboardButton(text="⬅️ Назад")]
    ]
    markup = types.ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)
    await message.answer(text, reply_markup=markup, parse_mode="HTML")


# =============================
# ➕ Добавление через MVP ссылку
# =============================
@router.message(F.text == "➕ Добавить аккаунт")
async def add_account_prompt(message: types.Message):
    await message.answer("📎 Отправь свою MVP ссылку, чтобы я добавил аккаунт.")


@router.message(F.text.contains("castleclash.igg.com") & F.text.contains("signed_key"))
async def handle_mvp_link(message: types.Message):
    user_id = str(message.from_user.id)
    url = message.text.strip()

    await message.answer("🔍 Загружаю страницу, извлекаю данные аккаунта...")

    info = await extract_player_info_from_page(url)
    if not info["success"]:
        await message.answer(f"❌ Не удалось получить данные: {info['error']}")
        return

    uid = info.get("uid")
    username = info.get("username", "Игрок")

    if not uid:
        await message.answer("❌ Не удалось получить IGG ID. Проверь ссылку.")
        return

    # 🚫 Проверка: UID не должен повторяться у других пользователей
    all_data = load_all_users()
    for other_user, acc_list in all_data.items():
        if any(acc.get("uid") == uid for acc in acc_list):
            await message.answer(
                "⚠️ Этот IGG ID уже добавлен другим пользователем. Повторное добавление запрещено."
            )
            return

    # ➕ Добавляем аккаунт
    added = add_account(user_id, uid, username, url)
    if not added:
        await message.answer(f"⚠️ Аккаунт <code>{uid}</code> уже есть.", parse_mode="HTML")
        return

    await message.answer(f"✅ Аккаунт <b>{username}</b> (UID: <code>{uid}</code>) добавлен!", parse_mode="HTML")

    # ♻️ Обновляем cookies
    await message.answer(f"♻️ Обновляю cookies для <b>{username}</b>...", parse_mode="HTML")
    result = await refresh_cookies_mvp(user_id, uid)

    if result.get("success"):
        await message.answer(f"🍪 Cookies обновлены ({len(result['cookies'])} шт.)", parse_mode="HTML")
    else:
        await message.answer(f"⚠️ Cookies не удалось обновить: {result.get('error')}", parse_mode="HTML")


# =============================
# ❌ Удаление аккаунта
# =============================
@router.message(F.text == "❌ Удалить аккаунт")
async def remove_account_prompt(message: types.Message, state: FSMContext):
    await message.answer("🆔 Отправь IGG ID аккаунта, который хочешь удалить.")
    await state.set_state(AccountManagementStates.waiting_for_deletion_uid)


@router.message(AccountManagementStates.waiting_for_deletion_uid, F.text.regexp(r"^\d{6,12}$"))
async def handle_uid_removal(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    uid = message.text.strip()
    success = remove_account(user_id, uid)
    if success:
        await message.answer(f"🗑 Аккаунт <code>{uid}</code> удалён.", parse_mode="HTML")
    else:
        await message.answer(f"⚠️ Аккаунт <code>{uid}</code> не найден.", parse_mode="HTML")
    await state.clear()


# =============================
# 🔘 Выбор активного аккаунта
# =============================
@router.message(F.text == "🔘 Выбрать активный")
async def choose_active_account(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    accounts = get_all_accounts(user_id)
    if not accounts:
        await message.answer("⚠️ Нет доступных аккаунтов для выбора.")
        return

    text = "🔘 Отправь IGG ID аккаунта, который хочешь сделать активным:\n\n"
    for acc in accounts:
        mark = " ✅ (активный)" if acc.get("active") else ""
        text += f"• <code>{acc['uid']}</code>{mark}\n"

    await message.answer(text, parse_mode="HTML")
    await state.set_state(AccountManagementStates.waiting_for_active_uid)


@router.message(AccountManagementStates.waiting_for_active_uid, F.text.regexp(r"^\d{6,12}$"))
async def set_active(message: types.Message, state: FSMContext):
    user_id = str(message.from_user.id)
    uid = message.text.strip()
    ok = set_active_account(user_id, uid)
    if ok:
        await message.answer(f"✅ Аккаунт <code>{uid}</code> теперь активный.", parse_mode="HTML")
    else:
        await message.answer(f"⚠️ Аккаунт <code>{uid}</code> не найден.", parse_mode="HTML")
    await state.clear()
