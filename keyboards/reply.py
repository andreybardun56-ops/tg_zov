from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def _build_reply_kb(rows: list[list[str]], placeholder: str | None = None) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=text) for text in row] for row in rows],
        resize_keyboard=True,
        input_field_placeholder=placeholder,
    )


USER_MAIN_ROWS = [
    ["👤 Управление аккаунтами"],
    ["🎁 Ввод промокода", "🧩 Пазлы"],
    ["🎡 Магическое колесо"],
    ["📩 Связь с разработчиком"],
]

TESTER_MAIN_ROWS = [
    ["🎯 События"],
    ["⚙️ Управление"],
    ["👤 Управление аккаунтами", "🧩 Пазлы"],
    ["🎁 Ввод промокода"],
    ["📩 Связь с разработчиком"],
]

ADMIN_MAIN_ROWS = [
    ["🎯 События"],
    ["⚙️ Управление"],
    ["🔧 Система"],
]

ADMIN_EVENTS_ROWS = [
    ["━━━━━━━━━━━ 🎁 Основные 🎯 ━━━━━━━━━━━"],
    ["🎡 Магическое колесо", "🎡 Колесо фортуны"],
    ["🃏 Найди пару", "🐉 Рыцари Драконы", "⚙️ Создающая машина"],
    ["━━━━━━━━━━━ 🧩 Пазлы ━━━━━━━━━━━"],
    ["🧩 Пазлы (подменю)"],
    ["🔙 Главное меню"],
]

ADMIN_SYSTEM_ROWS = [
    ["━━━━━━━━━━━ 🔧 Система ━━━━━━━━━━━"],
    ["🧪 Тест", "📊 Статистика"],
    ["🧹 Очистить мусор"],
    ["♻️ Перезапустить бота"],
    ["🔙 Главное меню"],
]

ACCOUNTS_ROWS = [
    ["➕ Добавить аккаунт", "🗑 Удалить аккаунт"],
    ["📜 Список аккаунтов", "🔙 Назад"],
]

PUZZLE_SUBMENU_ROWS = [
    ["🧩 Получить пазлы", "🧩 Взять пазл"],
    ["🧩 Собрать пазл", "♻️ Обменять пазлы"],
    ["🧩 Фарм дублей"],
    ["🔙 Назад"],
]


def get_user_main_kb() -> ReplyKeyboardMarkup:
    return _build_reply_kb(USER_MAIN_ROWS)


def get_tester_main_kb() -> ReplyKeyboardMarkup:
    return _build_reply_kb(TESTER_MAIN_ROWS, placeholder="Режим тестировщика 🧪")


def get_admin_main_kb() -> ReplyKeyboardMarkup:
    return _build_reply_kb(ADMIN_MAIN_ROWS, placeholder="Выбери раздел 👑")


def get_admin_events_menu() -> ReplyKeyboardMarkup:
    return _build_reply_kb(ADMIN_EVENTS_ROWS)


def get_admin_system_menu() -> ReplyKeyboardMarkup:
    return _build_reply_kb(ADMIN_SYSTEM_ROWS)


def get_accounts_kb() -> ReplyKeyboardMarkup:
    return _build_reply_kb(ACCOUNTS_ROWS)


def get_puzzle_submenu_kb() -> ReplyKeyboardMarkup:
    return _build_reply_kb(PUZZLE_SUBMENU_ROWS)


def get_admin_manage_menu(cookie_refresh_running: bool) -> ReplyKeyboardMarkup:
    cookie_button_text = (
        "⛔️ Остановить обновление cookies"
        if cookie_refresh_running
        else "🧩 Обновить cookies в базе"
    )
    rows = [
        ["━━━━━━━━━━━ ⚙️ Управление ━━━━━━━━━━━"],
        ["👤 Управление аккаунтами", "🔍 Проверить пары"],
        ["📊 Проверить акции", "🔁 Автосбор наград"],
        ["🔄 Обновить cookies", cookie_button_text],
        ["🎁 Ввод промокода"],
        ["🔙 Главное меню"],
    ]
    return _build_reply_kb(rows)


def get_admin_puzzles_menu(
    farm_running: bool,
    has_saved_state: bool,
    duplicates_running: bool,
) -> ReplyKeyboardMarkup:
    farm_controls: list[str] = []
    if farm_running:
        farm_controls.extend(["⛔️ Остановить фарм", "⏸ Пауза фарма"])
    elif has_saved_state:
        farm_controls.extend(["▶️ Продолжить фарм", "⛔️ Остановить фарм"])
    else:
        farm_controls.append("🧩 Фарм пазлов")

    rows = [
        ["━━━━━━━━━━━ 🧩 Пазлы ━━━━━━━━━━━"],
        ["🧩 Получить пазлы", "🧩 Взять код", "🧩 Собрать пазл"],
        ["⛔️ Остановить фарм дублей" if duplicates_running else "🧩 Фарм дублей"],
    ]
    if farm_controls:
        rows.append(farm_controls)
    rows.append(["🔙 Назад к событиям"])
    return _build_reply_kb(rows)
