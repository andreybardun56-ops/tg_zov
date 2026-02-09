# tg_zov/keyboards/inline.py
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram import Router, F, types, Bot
router = Router()

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

# ============================ ♻️ ОБМЕН ПАЗЛОВ ============================

async def send_exchange_items(bot: Bot, user_id: int, uid: str, items: dict):
    """
    Отправляет карточки обмена (мини-витрину) пользователю:
    🖼 Фото + описание + кнопка "♻️ Обменять"
    """
    for item_id, item in items.items():
        title = item.get("title", f"ID {item_id}")
        amount = item.get("amount", 1)
        need = item.get("need", 1)
        img_url = item.get("img", "")
        caption = (
            f"<b>{title}</b>\n"
            f"💠 Получите: <b>{amount}</b>\n"
            f"🔹 Требуется фрагментов: <b>{need}</b>"
        )
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="♻️ Обменять",
                        callback_data=f"exchange_item:{uid}:{item_id}"
                    )
                ]
            ]
        )
        try:
            await bot.send_photo(
                chat_id=user_id,
                photo=img_url or "https://img1.igg.com/1030/res/2017/07/16/211400_9899.png",
                caption=caption,
                reply_markup=kb,
                parse_mode="HTML"
            )
        except Exception as e:
            # fallback на текстовое сообщение, если фото не загрузилось
            await bot.send_message(
                chat_id=user_id,
                text=f"{caption}\n\n⚠️ <i>Не удалось загрузить изображение: {e}</i>",
                reply_markup=kb,
                parse_mode="HTML"
            )

# ============================ 🧩 ПАЗЛЫ ============================

def get_puzzle_accounts_kb(accounts: list, is_admin: bool = False) -> InlineKeyboardMarkup:
    """
    Меню выбора аккаунта для пазлов + кнопка "🎁 Получить 30 пазлов" для админа
    """
    kb = InlineKeyboardBuilder()
    for acc in accounts:
        uid = acc.get("uid")
        username = acc.get("username", "Без имени")
        kb.button(text=f"{uid} | {username}", callback_data=f"puzzle_acc:{uid}")

        kb.button(text="🎁 Получить 30 пазлов", callback_data="get_30_puzzles")

    kb.adjust(1)
    return kb.as_markup()


def get_puzzle_numbers_kb(uid: str) -> InlineKeyboardMarkup:
    """
    Сетка 3x3 с номерами пазлов 1–9
    """
    kb = InlineKeyboardBuilder()
    for i in range(1, 10):
        kb.button(text=str(i), callback_data=f"puzzle_num:{uid}:{i}")
    kb.adjust(3)
    return kb.as_markup()


def get_collect_puzzle_kb() -> InlineKeyboardMarkup:
    """Сетка выбора конкретного пазла для выдачи админам."""
    rows = []
    digits = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣"]
    for i in range(0, 9, 3):
        rows.append([
            InlineKeyboardButton(text=digits[i + j], callback_data=f"collect_puzzle:{i + j + 1}")
            for j in range(3)
        ])

    rows.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="collect_puzzle")
    ])

    return InlineKeyboardMarkup(inline_keyboard=rows)

# ============================ ♻️ ОБМЕН ПАЗЛОВ (выбор аккаунта) ============================

def get_exchange_accounts_kb(accounts: list) -> InlineKeyboardMarkup:
    """
    Меню выбора аккаунта для обмена пазлов.
    Создаёт кнопки вида: ♻️ Обмен <username> (uid)
    """
    kb = InlineKeyboardBuilder()
    for acc in accounts:
        uid = acc.get("uid")
        username = acc.get("username", "Без имени")
        kb.button(text=f"♻️ {username} ({uid})", callback_data=f"exchange_acc:{uid}")

    kb.adjust(1)
    return kb.as_markup()

# ============================ ❌ УДАЛЕНИЕ АККАУНТОВ ============================

def get_delete_accounts_kb(accounts: list) -> InlineKeyboardMarkup:
    """
    Inline-меню удаления аккаунтов
    """
    kb = InlineKeyboardBuilder()
    for acc in accounts:
        uid = acc.get("uid")
        kb.button(text=f"❌ {uid}", callback_data=f"del:{uid}")
    kb.adjust(1)
    return kb.as_markup()


# ============================ 💬 ПРОМОКОДЫ / ОБЩИЕ ============================

def get_contact_dev_kb() -> InlineKeyboardMarkup:
    """
    Кнопка связи с разработчиком
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💬 Написать разработчику", url="https://t.me/andrey56_di")]
        ]
    )
