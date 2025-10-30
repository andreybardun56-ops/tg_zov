import argparse
import asyncio
import base64
import enum
import json
import logging
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

try:
    from playwright.async_api import (
        Frame,
        Locator,
        Page,
        Response,
        TimeoutError,
        async_playwright,
    )
    from playwright.async_api import Page, Response, TimeoutError, async_playwright
except ModuleNotFoundError as exc:  # pragma: no cover - ранний выход, если playwright не установлен
    raise SystemExit(
        "Playwright не установлен. Установите пакет 'playwright' и выполните 'playwright install'."
    ) from exc

from services.browser_patches import (
    BROWSER_PATH,
    get_random_browser_profile,
    launch_masked_persistent_context,
)

# ----------------- Настройки -----------------
BASE_DIR = Path(__file__).resolve().parent.parent
ACCOUNTS_FILE = BASE_DIR / "data/google_accounts.json"
OUT_DIR = BASE_DIR / "data/data_akk"
CAPTCHA_DIR = BASE_DIR / "data" / "captcha"
LOGS_DIR = BASE_DIR / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
CAPTCHA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "login_refresh_google.log"

CONCURRENT = max(1, int(os.getenv("CONCURRENT", "1")))
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT", "60000"))
HEADLESS = os.getenv("HEADLESS", "0") == "1"
INTERACTIVE = os.getenv("INTERACTIVE", "0") == "1"
SLOW_MO = int(float(os.getenv("SLOW_MO", "100")))
WAIT_AFTER_NEXT = max(0, int(os.getenv("WAIT_AFTER_NEXT", "5")))


def _configure_logger() -> logging.Logger:
    logger = logging.getLogger("login_refresh_google")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.addHandler(stream_handler)

    return logger


logger = _configure_logger()


class LoginStatus(enum.Enum):
    SUCCESS = "success"
    CHALLENGE = "challenge"
    FAILED = "failed"


@dataclass
class Account:
    mail: str
    password: str

    @property
    def slug(self) -> str:
        return self.mail.replace("@", "__at__")


class ResponseRecorder:
    """Фиксирует последний интересующий HTTP-ответ страницы."""

    def __init__(self) -> None:
        self.last: Optional[Response] = None

    def __call__(self, resp: Response) -> None:
        try:
            url = resp.url
        except Exception:
            return

        if url and any(host in url for host in ("igg.com", "accounts.google.com")):
            self.last = resp


FrameLike = Union[Page, Frame]


def _safe_url(target: FrameLike) -> str:
    try:
        return target.url or ""
    except Exception:
        return ""


def _iter_context_pages(main_page: Page) -> Sequence[Page]:
    try:
        context = main_page.context
    except Exception:
        return tuple()

    pages: list[Page] = []
    for candidate in context.pages:
        try:
            if candidate.is_closed():
                continue
        except Exception:
            continue
        pages.append(candidate)
    return pages


async def _pick_google_page(main_page: Page, fallback: Page, attempts: int = 20, delay: float = 0.5) -> Page:
    for _ in range(max(1, attempts)):
        for candidate in reversed(_iter_context_pages(main_page)):
            if "accounts.google.com" in _safe_url(candidate):
                return candidate
        await asyncio.sleep(delay)
    return fallback


async def _pick_google_frame(login_page: Page, attempts: int = 20, delay: float = 0.3) -> FrameLike:
    for _ in range(max(1, attempts)):
        for frame in login_page.frames:
            if "accounts.google.com" in _safe_url(frame):
                return frame
        await asyncio.sleep(delay)
    return login_page


async def _wait_for_dom_ready(target: FrameLike, timeout: int = NAV_TIMEOUT) -> None:
    try:
        await target.wait_for_load_state("domcontentloaded", timeout=timeout)
    except Exception:
        pass


async def _wait_for_network_idle(target: FrameLike, timeout: int = NAV_TIMEOUT) -> None:
    try:
        await target.wait_for_load_state("networkidle", timeout=timeout)
    except Exception:
        pass


async def _fill_with_retry(locator: Locator, value: str, attempts: int = 3) -> None:
    last_error: Optional[Exception] = None
    for _ in range(1, attempts + 1):
        try:
            await locator.wait_for(state="visible", timeout=8000)
        except Exception as exc:
            last_error = exc
            continue

        try:
            await locator.click(timeout=2000)
        except Exception:
            pass

        try:
            await locator.fill(value, timeout=4000)
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.6)
            continue

        try:
            entered = await locator.input_value(timeout=1000)
        except Exception:
            entered = ""

        if entered.strip() == value:
            return

        try:
            await locator.press("Control+A")
            await locator.press("Delete")
        except Exception:
            pass
        await asyncio.sleep(0.6)

    if last_error:
        raise last_error
    raise RuntimeError("Не удалось ввести значение в поле")


async def _click_with_retry(locator: Locator, attempts: int = 3, wait_after: float = 0.0) -> None:
    last_error: Optional[Exception] = None
    for _ in range(max(1, attempts)):
        try:
            await locator.wait_for(state="visible", timeout=5000)
        except Exception as exc:
            last_error = exc
            continue
        try:
            await locator.click(timeout=4000)
            if wait_after:
                await asyncio.sleep(wait_after)
            return
        except Exception as exc:
            last_error = exc
            await asyncio.sleep(0.7)

    if last_error:
        raise last_error


async def _maybe_select_saved_account(target: FrameLike, email: str) -> bool:
    """Если Google предлагает список аккаунтов, пробуем выбрать нужный."""

    try:
        items = target.locator("div[data-identifier]")
        count = await items.count()
    except Exception:
        return False

    if count == 0:
        return False

    try:
        candidate = items.filter(has_text=email)
    except Exception:
        candidate = items

    try:
        if await candidate.count() > 0:
            await candidate.first.click(timeout=5000)
            await asyncio.sleep(1.0)
            return True
    except Exception:
        pass

    for alt_text in ("text=Use another account", "text=Другой аккаунт", "text=Add account"):
        try:
            locator = target.locator(alt_text)
            if await locator.count() > 0:
                await locator.first.click(timeout=4000)
                await asyncio.sleep(1.0)
                return False
        except Exception:
            continue

    return False

# ----------------- Утилиты -----------------

def atomic_write(path: Path, data: Any) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def make_new_data_format(email: str, cookies: List[Dict[str, Any]], page_content: Optional[str] = None) -> Dict[str, Any]:
    simple = []
    uid = None
    for c in cookies:
        simple.append({
            "name": c.get("name"),
            "value": c.get("value"),
            "domain": c.get("domain"),
            "path": c.get("path"),
            "expires": c.get("expires"),
            "httpOnly": c.get("httpOnly"),
            "secure": c.get("secure"),
        })
        if not uid and c.get("name") and "uid" in c.get("name").lower():
            uid = c.get("value")

    out = {"email": email, "uid": uid, "cookies": simple, "raw_cookies": cookies}
    if page_content:
        out["page_snapshot"] = page_content[:3000]
    return out

# === JWT / cookies helpers ===
def jwt_get_uid(token: str) -> Optional[str]:
    try:
        parts = token.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        obj = json.loads(decoded.decode("utf-8"))
        for k in ("sub", "uid", "userId", "user_id", "id", "jti"):
            if k in obj and obj[k]:
                return str(obj[k])
    except Exception:
        return None
    return None

def cookies_list_to_flat_dict(cookies_list: List[Dict[str, Any]]) -> Dict[str, str]:
    out = {}
    for c in cookies_list:
        n = c.get("name")
        v = c.get("value")
        if n:
            out[n] = v
    return out


UID_TEXT_PATTERNS = [
    re.compile(r"IGG\s*ID\s*[:：]\s*(\d{5,})", re.IGNORECASE),
    re.compile(r"IGGID\s*[:：]?\s*(\d{5,})", re.IGNORECASE),
    re.compile(r"\buid\b\s*[:：]\s*(\d{5,})", re.IGNORECASE),
    re.compile(r"\buser[_-]?id\b\s*[:：]\s*(\d{5,})", re.IGNORECASE),
]

UID_ATTR_CANDIDATES = (
    "data-uid",
    "data-userid",
    "data-user-id",
    "data-iggid",
    "data-igg-id",
)

IGG_COOKIE_HINTS = {
    "gpc_sso_token",
    "phpsessid",
    "iggid",
    "igg_id",
    "igg_uid",
    "igguserid",
}


def _normalize_uid(value: Any) -> Optional[str]:
    if value is None:
        return None
    candidate = str(value).strip()
    if candidate.isdigit() and len(candidate) >= 5:
        return candidate
    return None


def _extract_uid_from_cookies(cookies: List[Dict[str, Any]]) -> Optional[str]:
    for cookie in cookies:
        name = (cookie.get("name") or "").lower()
        value = (cookie.get("value") or "").strip()
        if not name or not value:
            continue
        if any(key in name for key in ("iggid", "igg_id", "user_id", "userid", "uid")):
            domain = (cookie.get("domain") or "").lower()
            if "igg" in domain or name == "uid" or name.endswith("_uid"):
                normalized = _normalize_uid(value)
                if normalized:
                    return normalized
    return None


def _extract_uid_from_html(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    for pattern in UID_TEXT_PATTERNS:
        match = pattern.search(text)
        if match:
            normalized = _normalize_uid(match.group(1))
            if normalized:
                return normalized
    return None


def _search_uid_in_obj(obj: Any) -> Optional[str]:
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_lower = key.lower()
            if any(token in key_lower for token in ("uid", "user_id", "userid", "iggid", "igg_id")):
                if isinstance(value, (str, int)):
                    normalized = _normalize_uid(value)
                    if normalized:
                        return normalized
            nested = _search_uid_in_obj(value)
            if nested:
                return nested
    elif isinstance(obj, list):
        for item in obj:
            nested = _search_uid_in_obj(item)
            if nested:
                return nested
    return None


async def _extract_uid_from_response(resp: Optional[Response]) -> Optional[str]:
    if resp is None:
        return None
    try:
        data = await resp.json()
    except Exception:
        try:
            text = await resp.text()
        except Exception:
            return None
        return _extract_uid_from_html(text)
    return _search_uid_in_obj(data)


async def _extract_uid_from_page_dom(page: Page) -> Optional[str]:
    try:
        locator = page.locator(
            "*[data-uid], *[data-userid], *[data-user-id], *[data-iggid], *[data-igg-id]"
        )
        count = await locator.count()
        for idx in range(min(count, 20)):
            handle = locator.nth(idx)
            for attr in UID_ATTR_CANDIDATES:
                try:
                    value = await handle.get_attribute(attr)
                except Exception:
                    value = None
                if value:
                    normalized = _normalize_uid(value)
                    if normalized:
                        return normalized
    except Exception:
        pass

    try:
        body_text = await page.inner_text("body")
    except Exception:
        body_text = None
    return _extract_uid_from_html(body_text)


async def _detect_success_by_cookies(
    email: str, main_page: Page, pages: Sequence[Page]
) -> Optional[tuple[LoginStatus, str, Page]]:
    """Проверяет, появились ли сессионные куки IGG, и возвращает успех."""

    try:
        context = main_page.context
    except Exception:
        return None

    try:
        cookies = await context.cookies(["https://passport.igg.com", "https://igg.com"])
    except Exception as exc:
        logger.debug("[%s] не удалось получить cookies для проверки успеха: %s", email, exc)
        return None

    if not cookies:
        return None

    igg_cookies = [c for c in cookies if "igg" in (c.get("domain") or "").lower()]
    if not igg_cookies:
        return None

    cookie_map = cookies_list_to_flat_dict(igg_cookies)
    lower_cookie_names = {name.lower() for name in cookie_map}
    if not any(name in lower_cookie_names for name in IGG_COOKIE_HINTS):
        return None

    selected_page: Optional[Page] = None
    for candidate in pages:
        try:
            url = candidate.url or ""
        except Exception:
            continue

        if "passport.igg.com" in url:
            selected_page = candidate
            if "binding" in url.lower():
                break

    if selected_page is None:
        selected_page = main_page

    logger.info(
        "[%s] ✅ Обнаружены IGG cookies (%s), считаем авторизацию успешной",
        email,
        ", ".join(sorted(cookie_map.keys())),
    )
    return LoginStatus.SUCCESS, "Получены сессионные cookies IGG", selected_page


def _extract_uid_from_saved_snapshot(email: str) -> Optional[str]:
    slug = email.replace("@", "__at__")
    snapshot_path = OUT_DIR / f"{slug}.json"
    if not snapshot_path.exists():
        return None
    try:
        with snapshot_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return None
    if isinstance(data, dict):
        for key in ("uid", "user_id", "userId"):
            value = data.get(key)
            normalized = _normalize_uid(value)
            if normalized:
                return normalized
    return None


def _extract_uid_from_new_data(email: str, data: Optional[list] = None) -> Optional[str]:
    records = data if data is not None else load_json_safe(NEW_DATA_FILE)
    if not isinstance(records, list):
        return None
    for record in records:
        if not isinstance(record, dict):
            continue
        if record.get("mail") != email:
            continue
        for key in record.keys():
            if key in {"mail", "paswd"}:
                continue
            normalized = _normalize_uid(key)
            if normalized:
                return normalized
    return None


async def save_last_response(email: str, resp: Optional[Response]) -> None:
    if resp is None:
        return
    try:
        body = await resp.text()
    except Exception:
        body = None
    out = {
        "url": getattr(resp, 'url', None),
        "status": getattr(resp, 'status', None),
        "headers": dict(getattr(resp, 'headers', {})),
        "body_snippet": body[:500] if body else None,
    }
    filename = OUT_DIR / (email.replace("@", "__at__") + "_resp.json")
    atomic_write(filename, out)
    logger.info("[%s] saved last response to %s", email, filename)


# ----------------- Логика логина -----------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Обновление cookies Google-аккаунтов для авторизации через IGG"
    )
    parser.add_argument(
        "--email",
        dest="emails",
        action="append",
        help="Обрабатывать только указанный e-mail (можно передавать несколько раз)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Пропускать аккаунты, для которых уже создан json-файл в data/data_akk",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Ограничить количество обрабатываемых аккаунтов",
    )
    return parser.parse_args()


def load_accounts(path: Path) -> List[Account]:
    if not path.exists():
        raise FileNotFoundError(path)

    try:
        raw_accounts = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Не удалось разобрать {path}: {exc}") from exc

    accounts: List[Account] = []
    for entry in raw_accounts:
        if not isinstance(entry, dict):
            logger.warning("Пропускаю некорректную запись: %s", entry)
            continue
        mail = str(entry.get("mail") or "").strip()
        password = str(entry.get("paswd") or "").strip()
        if not mail or not password:
            logger.warning("Пропускаю запись без mail/paswd: %s", entry)
            continue
        accounts.append(Account(mail=mail, password=password))

    return accounts


def filter_accounts(
    accounts: List[Account],
    *,
    emails: Optional[List[str]],
    limit: Optional[int],
) -> List[Account]:
    filtered = accounts
    if emails:
        requested = {email.lower() for email in emails if email}
        filtered = [acc for acc in filtered if acc.mail.lower() in requested]
        missing = requested - {acc.mail.lower() for acc in filtered}
        for email in sorted(missing):
            logger.warning("Запрошенный аккаунт не найден в %s: %s", ACCOUNTS_FILE, email)

    if limit is not None and limit >= 0:
        filtered = filtered[:limit]

    return filtered


async def wait_for_user_confirmation(email: str) -> None:
    loop = asyncio.get_running_loop()
    prompt = f"\n[{email}] Проверь окно браузера. Когда закончишь — нажми Enter.\n> "
    await loop.run_in_executor(None, input, prompt)


async def capture_page_artifacts(page: Page, slug: str, suffix: str) -> List[Path]:
    saved: List[Path] = []

    html_path = OUT_DIR / f"{slug}_{suffix}.html"
    try:
        html = await page.content()
        html_path.write_text(html, encoding="utf-8")
        saved.append(html_path)
    except Exception as exc:
        logger.debug("[%s] не удалось сохранить HTML: %s", slug, exc)

    screenshot_path = OUT_DIR / f"{slug}_{suffix}.png"
    try:
        await page.screenshot(path=str(screenshot_path), full_page=True)
        saved.append(screenshot_path)
    except Exception as exc:
        logger.debug("[%s] не удалось сохранить скриншот: %s", slug, exc)

    return saved


async def detect_login_error(target: FrameLike) -> Optional[str]:
    selectors = [
        "div[jsname='B34EJ']",
        "div[jsname='Ux99qd']",
        "div[jsname='UYUfn']",
        "div.o6cuMc",
        "text=Wrong password",
        "text=Неверный пароль",
        "text=Не удалось найти аккаунт",
    ]
    for selector in selectors:
        try:
            locator = target.locator(selector)
            if await locator.count() > 0:
                try:
                    text = (await locator.first.inner_text()).strip()
                except Exception:
                    text = selector
                if text:
                    return text
        except Exception:
            continue
    return None


async def detect_challenge(target: FrameLike) -> Optional[str]:
    challenge_keywords = (
        "challenge",
        "idv",
        "verification",
        "reauth",
        "signin/v2/challenge",
    )
    url = _safe_url(target)
    if any(keyword in url for keyword in challenge_keywords):
        return f"Google запросил дополнительное подтверждение (url: {url})"

    selectors_with_desc = [
        ("text=Подтвердите свою личность", "Требуется подтверждение личности"),
        ("text=Введите код подтверждения", "Ожидание кода подтверждения"),
        ("text=Verify it's you", "Verify it's you"),
        ("input[name=idvPin]", "Ожидание ввода кода idvPin"),
        ("#totpPin", "Ожидание кода из приложения"),
    ]
    for selector, fallback in selectors_with_desc:
        try:
            locator = target.locator(selector)
            if await locator.count() > 0:
                try:
                    text = (await locator.first.inner_text()).strip()
                except Exception:
                    text = fallback
                return text or fallback
        except Exception:
            continue
    return None



async def perform_login_flow(page: Page, email: str, password: str) -> tuple[LoginStatus, str, Page]:
    """Логин в IGG через Google в одном браузерном контексте."""

    base_url = "https://passport.igg.com/login"
    google_url = (
        "https://passport.igg.com/login/platform?"
        "url=https%3A%2F%2Fpassport.igg.com%2Fbindings&provider=googleplus"
    )

    main_page = page
    login_page: Page = page
    popup_page: Optional[Page] = None

    # --- 1️⃣ Открываем основную страницу ---
    try:
        await main_page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
        logger.info("[%s] открыт стартовый URL: %s", email, base_url)
        await asyncio.sleep(1.5)
    except Exception as e_start:
        logger.warning("[%s] не удалось открыть стартовую страницу: %s", email, e_start)

    # --- 🧩 Accept Cookies после загрузки страницы ---
    try:
        for sel in [
            "#onetrust-accept-btn-handler",
            "text=Accept All",
            "text=Принять все",
        ]:
            locator = main_page.locator(sel)
            if await locator.count() > 0:
                await locator.click(timeout=3000)
                logger.info("[%s] закрыт баннер cookies (%s)", email, sel)
                break
    except Exception as e_cookie:
        logger.debug("[%s] не удалось закрыть баннер cookies: %s", email, e_cookie)

    # --- 2️⃣ Клик по кнопке Google и обработка pop-up ---
    try:
        google_btn = main_page.locator(".ways-item.google")
        popup_page = None
        if await google_btn.count() > 0:
            logger.info("[%s] 🔘 Найдена кнопка Google, кликаем...", email)
            try:
                async with main_page.expect_popup(timeout=5000) as popup_waiter:
                    await google_btn.first.click(timeout=5000)
                popup_page = await popup_waiter.value
                login_page = popup_page
                logger.info("[%s] открыт pop-up авторизации Google", email)
                try:
                    await login_page.wait_for_load_state("domcontentloaded", timeout=30000)
                except Exception as e_popup_load:
                    logger.debug("[%s] не удалось дождаться загрузки pop-up: %s", email, e_popup_load)
            except TimeoutError:
                logger.debug("[%s] pop-up не открылся — авторизация идёт в текущей вкладке", email)
                await asyncio.sleep(3)
            except Exception as e_click:
                logger.warning(
                    "[%s] ⚠️ Ошибка при клике по кнопке Google: %s. Пробую открыть URL напрямую.",
                    email,
                    e_click,
                )
                popup_page = None
        else:
            logger.warning("[%s] ⚠️ Кнопка Google не найдена, открываю fallback-URL напрямую", email)

        if popup_page is None:
            login_page = main_page
            try:
                current_url = login_page.url
            except Exception:
                current_url = ""
            if "accounts.google.com" not in current_url:
                logger.debug("[%s] выполняю прямой переход на страницу авторизации Google", email)
                await login_page.goto(google_url, wait_until="domcontentloaded", timeout=30000)

        login_page = await _pick_google_page(main_page, login_page)
        await _wait_for_dom_ready(login_page)
        login_surface: FrameLike = await _pick_google_frame(login_page)
        await _wait_for_dom_ready(login_surface)
    except Exception as e_nav:
        logger.warning(
            "[%s] ⚠️ Ошибка при переходе на авторизацию Google: %s. Пытаюсь открыть URL напрямую.",
            email,
            e_nav,
        )
        await main_page.goto(google_url, wait_until="domcontentloaded", timeout=30000)
        login_page = await _pick_google_page(main_page, main_page)
        login_surface = await _pick_google_frame(login_page)
        login_page = main_page

    # --- 3️⃣ Принимаем cookies (если есть) ---
    try:
        for sel in ["#onetrust-accept-btn-handler", "text=Accept All", "text=Принять все"]:
            locator = login_surface.locator(sel)
            locator = login_page.locator(sel)
            if await locator.count() > 0:
                await locator.click(timeout=3000)
                logger.info("[%s] закрыт баннер cookies (%s) на странице авторизации", email, sel)
                break
    except Exception:
        logger.debug("[%s] баннер cookies не найден на странице авторизации", email)

    try:
        is_closed = login_page.is_closed()
    except Exception:
        is_closed = True
    if is_closed:
        logger.error("[%s] Окно авторизации Google закрылось до ввода данных", email)
        return LoginStatus.FAILED, "Окно авторизации закрылось", main_page

    # --- 4️⃣ Подбор аккаунта и ввод e-mail ---
    password_locator = login_surface.locator("input[name=Passwd]")
    password_visible = False
    try:
        password_visible = await password_locator.count() > 0
    except Exception:
        password_visible = False

    if not password_visible:
        account_selected = await _maybe_select_saved_account(login_surface, email)
        identifier_locator = login_surface.locator("input#identifierId")
        if await identifier_locator.count() == 0:
            identifier_locator = login_surface.locator("input[type=email]")

        if await identifier_locator.count() == 0 and not account_selected:
            logger.error("[%s] Поле ввода e-mail не найдено", email)
            return LoginStatus.FAILED, "Поле e-mail не найдено", login_page

        if not account_selected:
            logger.info("[%s] вводим e-mail", email)
            await _fill_with_retry(identifier_locator.first, email)
            await _click_with_retry(login_surface.locator("#identifierNext"), wait_after=0.5)

        logger.debug("[%s] ожидание после identifierNext %s с", email, WAIT_AFTER_NEXT)
        if WAIT_AFTER_NEXT:
            await asyncio.sleep(WAIT_AFTER_NEXT)

        login_page = await _pick_google_page(main_page, login_page)
        login_surface = await _pick_google_frame(login_page)
        await _wait_for_dom_ready(login_surface)
    # --- 4️⃣ Ввод email ---
    logger.info("[%s] вводим e-mail", email)
    await login_page.wait_for_selector("input#identifierId", timeout=30000)
    await login_page.fill("input#identifierId", email)
    await login_page.click("#identifierNext")
    logger.debug("[%s] ожидание после identifierNext %s с", email, WAIT_AFTER_NEXT)
    if WAIT_AFTER_NEXT:
        await asyncio.sleep(WAIT_AFTER_NEXT)

    # --- Проверяем наличие капчи после логина ---
    try:
        captcha_selectors = [
            "img#captchaimg",
            "img[src*='Captcha']",
            "img[src*='captcha']",
            "div#captchaimg img",
        ]
        input_selectors = [
            "input#ca",
            "input[name='ca']",
            "input[name='captcha']",
            "input#captcha",
        ]

        for sel in captcha_selectors:
            locator = login_surface.locator(sel)
            if await locator.count() == 0:
                locator = login_page.locator(sel)
            if await locator.count() > 0:
                slug = email.replace("@", "__at__")
                captcha_path = CAPTCHA_DIR / f"{slug}_captcha.png"

                await locator.first.screenshot(path=str(captcha_path))
            if await login_page.locator(sel).count() > 0:
                slug = email.replace("@", "__at__")
                captcha_path = CAPTCHA_DIR / f"{slug}_captcha.png"

                await login_page.locator(sel).first.screenshot(path=str(captcha_path))
                logger.warning("[%s] ⚠️ Обнаружена капча! Сохранена в %s", email, captcha_path)

                try:
                    from services.captcha_solver import solve_captcha

                    text = solve_captcha(str(captcha_path))
                except Exception as e_solve:
                    logger.error("[%s] Ошибка при распознавании капчи: %s", email, e_solve)
                    text = ""

                if text:
                    logger.info("[%s] Распознана капча: %s", email, text)
                    for inp in input_selectors:
                        captcha_input = login_surface.locator(inp)
                        if await captcha_input.count() == 0:
                            captcha_input = login_page.locator(inp)
                        if await captcha_input.count() > 0:
                            await captcha_input.first.fill(text)
                            await asyncio.sleep(0.5)
                            try:
                                await captcha_input.first.press("Enter")
                            except Exception:
                                await login_surface.keyboard.press("Enter")
                        if await login_page.locator(inp).count() > 0:
                            await login_page.fill(inp, text)
                            await asyncio.sleep(0.5)
                            await login_page.keyboard.press("Enter")
                            logger.info("[%s] Ввел текст капчи и нажал Enter", email)
                            break
                else:
                    logger.warning("[%s] ❌ Капча не распознана автоматически", email)
                break
    except Exception as e:
        logger.debug("[%s] Ошибка при проверке капчи: %s", email, e)

    # --- 5️⃣ Ввод пароля ---
    try:
        await login_surface.wait_for_selector("input[name=Passwd]", timeout=35000)
    except TimeoutError:
        error_text = await detect_login_error(login_surface)
        if error_text:
            return LoginStatus.FAILED, f"Google сообщил об ошибке: {error_text}", login_page
        challenge_text = await detect_challenge(login_surface)
        if challenge_text:
            return LoginStatus.CHALLENGE, challenge_text, login_page
        logger.error("[%s] Не дождался поля пароля", email)
        return LoginStatus.FAILED, "Поле пароля не появилось", login_page

    await _fill_with_retry(login_surface.locator("input[name=Passwd]").first, password)
    await _click_with_retry(login_surface.locator("#passwordNext"), wait_after=0.5)
    await login_page.wait_for_selector("input[name=Passwd]", timeout=30000)
    await login_page.fill("input[name=Passwd]", password)
    await login_page.click("#passwordNext")
    logger.debug("[%s] нажали passwordNext, ожидаем %s с", email, WAIT_AFTER_NEXT)
    if WAIT_AFTER_NEXT:
        await asyncio.sleep(WAIT_AFTER_NEXT)

    login_page = await _pick_google_page(main_page, login_page)
    login_surface = await _pick_google_frame(login_page)
    await _wait_for_network_idle(login_surface)

    # --- 6️⃣ Нажатие «Продолжить», если требуется ---
    try:
        cont_loc = login_surface.locator("span[jsname='V67aGc']")
        if await cont_loc.count() == 0:
            cont_loc = login_page.locator("span[jsname='V67aGc']")
        cont_loc = login_page.locator("span[jsname='V67aGc']")
        if await cont_loc.count() > 0:
            await cont_loc.first.click(timeout=4000)
            logger.info("[%s] нажата кнопка 'Продолжить'", email)
            await asyncio.sleep(1)
        else:
            for txt in ("text=Продолжить", "text=Continue", "text=Continue to Google"):
                locator = login_surface.locator(txt)
                if await locator.count() == 0:
                    locator = login_page.locator(txt)
                if await locator.count() > 0:
                    await locator.first.click(timeout=4000)
                if await login_page.locator(txt).count() > 0:
                    await login_page.locator(txt).first.click(timeout=4000)
                    logger.info("[%s] нажата кнопка 'Продолжить' (текст=%s)", email, txt)
                    await asyncio.sleep(1)
                    break
    except Exception as e_click:
        logger.debug("[%s] кнопка 'Продолжить' не найдена: %s", email, e_click)

    # --- 7️⃣ Принимаем cookies снова (если вылезли повторно) ---
    try:
        for sel in ["#onetrust-accept-btn-handler", "text=Accept All", "text=Принять все"]:
            locator = login_surface.locator(sel)
            locator = login_page.locator(sel)
            if await locator.count() > 0:
                await locator.click(timeout=3000)
                logger.info("[%s] повторное закрытие баннера cookies (%s)", email, sel)
                break
    except Exception:
        pass

    def iter_open_pages() -> list[Page]:
        pages: list[Page] = []
        seen: set[int] = set()

        for candidate in _iter_context_pages(main_page):
            ident = id(candidate)
            if ident in seen:
                continue
            seen.add(ident)
            pages.append(candidate)

        for candidate in (login_page, main_page):
            if candidate is None:
                continue
            ident = id(candidate)
            if ident in seen:
                continue
        for candidate in (login_page, main_page):
            if candidate is None:
                continue
            try:
                if candidate.is_closed():
                    continue
            except Exception:
                continue
            seen.add(ident)
            pages.append(candidate)

            ident = id(candidate)
            if ident in seen:
                continue
            seen.add(ident)
            pages.append(candidate)
        return pages

    # --- 8️⃣ Ожидаем загрузку страницы с аккаунтом (bindings) ---
    for candidate in iter_open_pages():
        try:
            await candidate.wait_for_load_state("networkidle", timeout=1000)
        except Exception:
            continue

    # --- Проверяем, загрузилась ли страница bindings ---
    for _ in range(15):
        for candidate in iter_open_pages():
            try:
                current_url = candidate.url
            except Exception:
                continue

            if "passport.igg.com/bindings" in current_url:
                try:
                    if await candidate.locator("text=IGG ID").count() > 0 or await candidate.locator(
                        "div:has-text('Привязанные аккаунты')"
                    ).count() > 0:
                        logger.info("[%s] ✅ bindings страница загружена, сохраняем куки", email)
                        return LoginStatus.SUCCESS, f"bindings загружен (url: {current_url})", candidate
                except Exception:
                    pass
        await asyncio.sleep(1)

    # --- Проверяем ошибки или челлендж ---
    for candidate in iter_open_pages():
        error_text = await detect_login_error(candidate)
        if error_text:
            return LoginStatus.FAILED, f"Google сообщил об ошибке: {error_text}", candidate

    frame_error = await detect_login_error(login_surface)
    if frame_error:
        return LoginStatus.FAILED, f"Google сообщил об ошибке: {frame_error}", login_page

    for candidate in iter_open_pages():
        challenge_text = await detect_challenge(candidate)
        if challenge_text:
            return LoginStatus.CHALLENGE, challenge_text, candidate

    frame_challenge = await detect_challenge(login_surface)
    if frame_challenge:
        return LoginStatus.CHALLENGE, frame_challenge, login_page

    # --- Если bindings открыт, но без элементов ---
    for candidate in iter_open_pages():
        try:
            current_url = candidate.url
        except Exception:
            continue

        if "passport.igg.com/bindings" in current_url:
            logger.info(
                "[%s] ⚠️ На странице bindings, но элементы не найдены — считаем успехом",
                email,
            )
            return LoginStatus.SUCCESS, f"Открыта страница аккаунта IGG (url: {current_url})", candidate

    cookie_success = await _detect_success_by_cookies(email, main_page, iter_open_pages())
    if cookie_success:
        return cookie_success

    # --- 🔁 Возврат на стартовую страницу и сбор куков ---
    try:
        logger.info(
            "[%s] переходим обратно на https://passport.igg.com/bindings для финального сбора куков",
            email,
        )
        await main_page.goto(
            "https://passport.igg.com/bindings", wait_until="domcontentloaded", timeout=30000
        )
        await asyncio.sleep(5)
        logger.info("[%s] ожидание 5 сек перед сбором куков завершено", email)
    except Exception as e_final:
        logger.warning("[%s] не удалось открыть финальную страницу для сбора куков: %s", email, e_final)

    try:
        main_page_closed = main_page.is_closed()
    except Exception:
        main_page_closed = True
    fallback_page = main_page if not main_page_closed else login_page
    current_url = ""
    try:
        current_url = fallback_page.url
    except Exception:
        pass
    return LoginStatus.FAILED, f"Не удалось подтвердить авторизацию, текущий URL: {current_url}", fallback_page


async def persist_success(
    account: Account, context, page: Page, last_response: Optional[Response]
) -> None:
async def persist_success(account: Account, context, page: Page) -> None:
    """Сохраняет cookies и HTML после успешной авторизации на странице bindings."""
    try:
        logger.info("[%s] ⏳ Ожидание 6 сек перед сбором cookies для стабильной загрузки...", account.mail)
        await asyncio.sleep(6)  # ← вот здесь задержка
    except Exception:
        pass

    try:
        cookies = await context.cookies()
    except Exception as exc:
        logger.exception("[%s] не удалось получить cookies: %s", account.mail, exc)
        return

    try:
        page_snapshot = await page.content()
    except Exception as exc:
        logger.debug("[%s] не удалось получить HTML страницы: %s", account.mail, exc)
        page_snapshot = None

    cookies_flat = cookies_list_to_flat_dict(cookies)

    # создаем формат данных для записи
    new_data = make_new_data_format(account.mail, cookies, page_snapshot)

    uid_source = None
    uid_value = new_data.get("uid")

    if not uid_value:
        uid_from_cookies = _extract_uid_from_cookies(cookies)
        if uid_from_cookies:
            uid_value = uid_from_cookies
            uid_source = "cookie"

    if not uid_value:
        token_value = cookies_flat.get("gpc_sso_token")
        if token_value:
            token_uid = jwt_get_uid(token_value)
            if token_uid:
                uid_value = token_uid
                uid_source = "gpc_sso_token"

    if not uid_value and last_response is not None:
        uid_from_response = await _extract_uid_from_response(last_response)
        if uid_from_response:
            uid_value = uid_from_response
            uid_source = "response"

    if not uid_value and page_snapshot:
        uid_from_html = _extract_uid_from_html(page_snapshot)
        if uid_from_html:
            uid_value = uid_from_html
            uid_source = "html"

    if not uid_value:
        uid_from_dom = await _extract_uid_from_page_dom(page)
        if uid_from_dom:
            uid_value = uid_from_dom
            uid_source = "dom"

    if not uid_value:
        uid_from_snapshot = _extract_uid_from_saved_snapshot(account.mail)
        if uid_from_snapshot:
            uid_value = uid_from_snapshot
            uid_source = "saved_snapshot"

    new_data_records: Optional[list] = None
    if not uid_value:
        new_data_records = load_json_safe(NEW_DATA_FILE)
        uid_from_new_data = _extract_uid_from_new_data(account.mail, new_data_records)
        if uid_from_new_data:
            uid_value = uid_from_new_data
            uid_source = "new_data"

    if uid_value:
        new_data["uid"] = uid_value
        if uid_source:
            logger.info("[%s] UID определён (%s): %s", account.mail, uid_source, uid_value)
        else:
            logger.info("[%s] UID определён: %s", account.mail, uid_value)
    else:
        logger.warning("[%s] ⚠️ Не удалось определить UID", account.mail)

    filename = OUT_DIR / f"{account.slug}.json"
    atomic_write(filename, new_data)
    logger.info(
        "[%s] 💾 cookies сохранены -> %s (uid=%s, cookies=%s)",
        account.mail,
        filename,
        new_data.get("uid") or "-",
        len(new_data.get("cookies", [])),
    )

    # дополнительный снимок bindings-страницы (для логов)
    try:
        screenshot_path = OUT_DIR / f"{account.slug}_bindings.png"
        await page.screenshot(path=str(screenshot_path), full_page=True)
        logger.info("[%s] 📸 скриншот сохранён -> %s", account.mail, screenshot_path)
    except Exception as exc:
        logger.debug("[%s] не удалось сохранить скриншот: %s", account.mail, exc)

    # ✅ Обновляем основной new_data0.json
    await update_new_data_file(account.mail, cookies, new_data.get("uid"), new_data_records)

NEW_DATA_FILE = BASE_DIR / "data" / "data_akk" / "new_data0.json"

def load_json_safe(path: Path) -> list:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        logger.warning("⚠️ Не удалось прочитать %s — создаём пустой список", path)
        return []


def save_json_safe(path: Path, data: list) -> None:
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


async def update_new_data_file(
    email: str, cookies: list, uid: Optional[str], preloaded: Optional[list] = None
) -> None:
    """Обновляет куки для указанного email в new_data0.json."""
    data = preloaded if preloaded is not None else load_json_safe(NEW_DATA_FILE)

    normalized_uid = _normalize_uid(uid)
    effective_uid = normalized_uid or _extract_uid_from_new_data(email, data)
    if not effective_uid:
        logger.warning("[%s] ⚠️ UID не найден, пропускаю обновление new_data0.json", email)
        return

    # отфильтровываем нужные куки
    cookie_map = {}
    for c in cookies:
        name = c.get("name")
        value = c.get("value")
        if name in ("PHPSESSID", "gpc_sso_token", "RT", "locale_ln", "_cookie_privacy_"):
            cookie_map[name] = value

    if not cookie_map:
        logger.warning("[%s] ⚠️ Не найдены целевые куки для обновления", email)
        return

    if not isinstance(data, list):
        data = []
    updated = False

    for acc in data:
        if acc.get("mail") == email:
            if effective_uid not in acc:
                acc[effective_uid] = {}
            acc[effective_uid].update(cookie_map)
            updated = True
            break

    if not updated:
        # если аккаунта нет — создаём новую запись
        data.append({
            "mail": email,
            "paswd": "",
            effective_uid: cookie_map
        })

    save_json_safe(NEW_DATA_FILE, data)
    logger.info(
        "[%s] 🔁 обновлены куки в new_data0.json (uid=%s, %d шт.)",
        email,
        effective_uid,
        len(cookie_map),
    )
    logger.info("[%s] 🔁 обновлены куки в new_data0.json (uid=%s, %d шт.)", email, uid, len(cookie_map))

async def login_one_account(
    account: Account,
    sem: asyncio.Semaphore,
    playwright,
    *,
    skip_existing: bool,
) -> None:
    async with sem:
        email = account.mail
        output_file = OUT_DIR / f"{account.slug}.json"
        profile_dir = (BASE_DIR / "data" / "chrome_profiles" / account.slug).resolve()
        user_data_dir = str(profile_dir)

        if skip_existing and output_file.exists():
            logger.info("[%s] пропускаю — файл %s уже существует", email, output_file)
            return

        logger.info(
            "[%s] старт авторизации (headless=%s, interactive=%s)",
            email,
            HEADLESS,
            INTERACTIVE,
        )

        # --- (1) Очистка профиля перед запуском ---
        if os.getenv("CLEAR_PROFILE", "1") == "1":
            if profile_dir.exists():
                for attempt in range(3):
                    try:
                        shutil.rmtree(profile_dir, ignore_errors=False)
                        logger.info("[%s] 🔄 Профиль очищен перед запуском: %s", email, profile_dir)
                        break
                    except Exception as e_rm:
                        logger.warning("[%s] Ошибка при удалении профиля (%d попытка): %s",
                                       email, attempt + 1, e_rm)
                        time.sleep(1)
            else:
                logger.debug("[%s] Профиль отсутствует — очищать нечего", email)

        # --- (2) Запуск браузера ---
        try:
            ctx = await launch_masked_persistent_context(
                playwright,
                user_data_dir=user_data_dir,
                browser_path=BROWSER_PATH,
                headless=HEADLESS,
                slow_mo=SLOW_MO,
                profile=get_random_browser_profile(),
            )
        except Exception as exc:
            logger.exception("[%s] ❌ Не удалось запустить браузер: %s", email, exc)
            return

        context = ctx["context"]
        page = ctx["page"]
        recorder = ResponseRecorder()
        context.on("response", recorder)

        try:
            status, message, active_page = await perform_login_flow(page, email, account.password)
            logger.info("[%s] результат авторизации: %s", email, message)

            target_page = active_page if active_page is not None else page
            try:
                if target_page.is_closed():
                    target_page = page
            except Exception:
                target_page = page

            if status is LoginStatus.SUCCESS:
                await persist_success(account, context, target_page, recorder.last)
                await persist_success(account, context, target_page)
            elif status is LoginStatus.CHALLENGE:
                logger.warning("[%s] требуется ручное подтверждение: %s", email, message)
                if INTERACTIVE:
                    await wait_for_user_confirmation(email)
                    await persist_success(account, context, target_page, recorder.last)
                    await persist_success(account, context, target_page)
                else:
                    await capture_page_artifacts(target_page, account.slug, "challenge")
            else:
                logger.error("[%s] авторизация не удалась", email)
                await capture_page_artifacts(target_page, account.slug, "failed")

        except Exception as exc:
            logger.exception("[%s] непредвиденная ошибка: %s", email, exc)
            await capture_page_artifacts(page, account.slug, "exception")

        finally:
            # --- (3) Закрываем контекст и сохраняем последнюю страницу ---
            await save_last_response(email, recorder.last)
            try:
                context.off("response", recorder)
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass

            # --- (4) Удаляем профиль после завершения работы ---
            if profile_dir.exists():
                for attempt in range(3):
                    try:
                        shutil.rmtree(profile_dir, ignore_errors=False)
                        logger.info("[%s] 🧹 Профиль удалён после завершения: %s", email, profile_dir)
                        break
                    except Exception as e_rm:
                        logger.warning("[%s] Не удалось удалить профиль после завершения (%d попытка): %s",
                                       email, attempt + 1, e_rm)
                        time.sleep(1)
                else:
                    logger.error("[%s] ❌ Не удалось удалить профиль после 3 попыток: %s",
                                 email, profile_dir)
            else:
                logger.debug("[%s] Папка профиля уже отсутствует", email)

async def main_async(args: argparse.Namespace) -> int:
    try:
        accounts = load_accounts(ACCOUNTS_FILE)
    except FileNotFoundError:
        logger.error("Файл аккаунтов не найден: %s", ACCOUNTS_FILE)
        return 1
    except ValueError as exc:
        logger.error(str(exc))
        return 1

    accounts = filter_accounts(accounts, emails=args.emails, limit=args.limit)

    if not accounts:
        logger.warning("Список аккаунтов пуст — нечего обновлять")
        return 0

    sem = asyncio.Semaphore(CONCURRENT)

    async with async_playwright() as pw:
        tasks = [
            login_one_account(acc, sem, pw, skip_existing=args.skip_existing)
            for acc in accounts
        ]
        await asyncio.gather(*tasks)

    return 0


def main() -> int:
    args = parse_args()

    if sys.platform.startswith("win"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except AttributeError:
            pass
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    return asyncio.run(main_async(args))

if __name__ == "__main__":
    try:
        exit_code = main()
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")
        exit_code = 130
    sys.exit(exit_code)
