# tg_zov/services/accounts_manager.py
import json
import os
from typing import List, Dict, Optional

USER_ACCOUNTS_FILE = "data/user_accounts.json"


# -------------------------------
# ⚙️ Вспомогательные функции
# -------------------------------
def _load_data() -> Dict[str, list]:
    """Загружает JSON со всеми пользователями."""
    if not os.path.exists(USER_ACCOUNTS_FILE):
        return {}
    try:
        with open(USER_ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_data(data: Dict[str, list]):
    """Сохраняет JSON со всеми пользователями."""
    os.makedirs(os.path.dirname(USER_ACCOUNTS_FILE), exist_ok=True)
    with open(USER_ACCOUNTS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# -------------------------------
# 🧩 Проверка структуры аккаунта
# -------------------------------
def _ensure_account_schema(account: Dict) -> bool:
    """
    Гарантирует наличие обязательных полей в структуре аккаунта.
    Возвращает True, если запись была изменена.
    """
    changed = False

    def ensure_str_field(key: str):
        nonlocal changed
        if key not in account or account[key] is None:
            account[key] = ""
            changed = True
        elif not isinstance(account[key], str):
            account[key] = str(account[key])
            changed = True

    ensure_str_field("uid")
    ensure_str_field("username")
    ensure_str_field("mvp_url")
    ensure_str_field("gpc_sso_token")
    ensure_str_field("mail")
    ensure_str_field("paswd")

    if "active" not in account:
        account["active"] = False
        changed = True

    return changed


# -------------------------------
# 👥 Все пользователи и аккаунты
# -------------------------------
def get_all_users_accounts() -> Dict[str, List[Dict]]:
    """Возвращает словарь user_id -> нормализованный список аккаунтов."""
    data = _load_data()
    normalized: Dict[str, List[Dict]] = {}
    data_changed = False

    for user_id, raw_accounts in data.items():
        accounts_list: List[Dict] = []
        user_changed = False

        if not isinstance(raw_accounts, list):
            user_changed = True
        else:
            for acc in raw_accounts:
                if not isinstance(acc, dict):
                    user_changed = True
                    continue
                if _ensure_account_schema(acc):
                    user_changed = True
                accounts_list.append(acc)

        normalized[str(user_id)] = accounts_list
        if user_changed:
            data[user_id] = accounts_list
            data_changed = True

    if data_changed:
        _save_data(data)

    return normalized


def load_all_users() -> Dict[str, List[Dict]]:
    """Универсальная обёртка для получения всех пользователей."""
    return get_all_users_accounts()


# -------------------------------
# 👤 Работа с аккаунтами одного пользователя
# -------------------------------
def get_all_accounts(user_id: str) -> List[Dict]:
    """Возвращает список всех аккаунтов пользователя, нормализуя структуру."""
    data = _load_data()
    raw_accounts = data.get(str(user_id), [])

    if not isinstance(raw_accounts, list):
        return []

    accounts: List[Dict] = []
    changed = False

    for acc in raw_accounts:
        if isinstance(acc, dict):
            if _ensure_account_schema(acc):
                changed = True
            accounts.append(acc)
        else:
            changed = True

    if changed or len(accounts) != len(raw_accounts):
        save_accounts(user_id, accounts)

    return accounts


def save_accounts(user_id: str, accounts: List[Dict]):
    """Сохраняет список аккаунтов конкретного пользователя."""
    data = _load_data()
    data[str(user_id)] = accounts
    _save_data(data)


def add_account(
    user_id: str,
    uid: str,
    username: str,
    mvp_url: str,
    token: Optional[str] = None,
) -> bool:
    """
    Добавляет новый аккаунт пользователю.
    Если уже существует — обновляет токен и сохранённые данные профиля.
    Возвращает True, если добавлен новый, False — если обновлён.
    """
    user_id = str(user_id)
    uid = str(uid)
    accounts = get_all_accounts(user_id)

    for acc in accounts:
        if acc.get("uid") == uid:
            acc["username"] = username
            acc["mvp_url"] = mvp_url
            if token:
                acc["gpc_sso_token"] = token
            save_accounts(user_id, accounts)
            return False  # обновлён

    new_acc = {
        "uid": uid,
        "username": username,
        "mvp_url": mvp_url,
        "gpc_sso_token": token or "",
        "mail": "",
        "paswd": "",
        "active": len(accounts) == 0,
    }
    _ensure_account_schema(new_acc)
    accounts.append(new_acc)
    save_accounts(user_id, accounts)
    return True


def remove_account(user_id: str, uid: str) -> bool:
    """Удаляет аккаунт по UID."""
    user_id = str(user_id)
    uid = str(uid)
    accounts = get_all_accounts(user_id)
    if not accounts:
        return False

    new_list = [acc for acc in accounts if acc.get("uid") != uid]
    if len(new_list) == len(accounts):
        return False

    # Если удалён активный — активным сделать первый оставшийся
    removed_active = any(acc.get("uid") == uid and acc.get("active") for acc in accounts)
    if removed_active and new_list:
        new_list[0]["active"] = True

    save_accounts(user_id, new_list)
    return True


def get_account_by_uid(user_id: str, uid: str) -> Optional[Dict]:
    """Находит аккаунт по UID."""
    uid = str(uid)
    for acc in get_all_accounts(user_id):
        if acc.get("uid") == uid:
            return acc
    return None


# -------------------------------
# 🎯 Работа с активным аккаунтом
# -------------------------------
def get_active_account(user_id: str) -> Optional[Dict]:
    """Возвращает активный аккаунт пользователя."""
    for acc in get_all_accounts(user_id):
        if acc.get("active"):
            return acc
    return None


def set_active_account(user_id: str, uid: str) -> bool:
    """Устанавливает активный аккаунт по UID."""
    user_id = str(user_id)
    uid = str(uid)
    accounts = get_all_accounts(user_id)
    found = False

    for acc in accounts:
        if acc.get("uid") == uid:
            acc["active"] = True
            found = True
        else:
            acc["active"] = False

    if found:
        save_accounts(user_id, accounts)
    return found
