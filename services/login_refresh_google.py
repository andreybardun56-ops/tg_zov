import argparse
import asyncio
import enum
import json
import logging
import os
import sys
import base64
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
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


async def detect_login_error(page: Page) -> Optional[str]:
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
            locator = page.locator(selector)
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


async def detect_challenge(page: Page) -> Optional[str]:
    challenge_keywords = (
        "challenge",
        "idv",
        "verification",
        "reauth",
        "signin/v2/challenge",
    )
    url = page.url
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
            locator = page.locator(selector)
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
    except Exception as e_nav:
        logger.warning(
            "[%s] ⚠️ Ошибка при переходе на авторизацию Google: %s. Пытаюсь открыть URL напрямую.",
            email,
            e_nav,
        )
        await main_page.goto(google_url, wait_until="domcontentloaded", timeout=30000)
        login_page = main_page

    # --- 3️⃣ Принимаем cookies (если есть) ---
    try:
        for sel in ["#onetrust-accept-btn-handler", "text=Accept All", "text=Принять все"]:
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
    await login_page.wait_for_selector("input[name=Passwd]", timeout=30000)
    await login_page.fill("input[name=Passwd]", password)
    await login_page.click("#passwordNext")
    logger.debug("[%s] нажали passwordNext, ожидаем %s с", email, WAIT_AFTER_NEXT)
    if WAIT_AFTER_NEXT:
        await asyncio.sleep(WAIT_AFTER_NEXT)

    # --- 6️⃣ Нажатие «Продолжить», если требуется ---
    try:
        cont_loc = login_page.locator("span[jsname='V67aGc']")
        if await cont_loc.count() > 0:
            await cont_loc.first.click(timeout=4000)
            logger.info("[%s] нажата кнопка 'Продолжить'", email)
            await asyncio.sleep(1)
        else:
            for txt in ("text=Продолжить", "text=Continue", "text=Continue to Google"):
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
        for candidate in (login_page, main_page):
            if candidate is None:
                continue
            try:
                if candidate.is_closed():
                    continue
            except Exception:
                continue
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

    for candidate in iter_open_pages():
        challenge_text = await detect_challenge(candidate)
        if challenge_text:
            return LoginStatus.CHALLENGE, challenge_text, candidate

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

    # создаем формат данных для записи
    new_data = make_new_data_format(account.mail, cookies, page_snapshot)

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

    # --- ✅ Пробуем найти UID из токена (перенесено из except) ---
    cookies_flat = cookies_list_to_flat_dict(cookies)
    token_value = cookies_flat.get("gpc_sso_token")
    if not new_data.get("uid") and token_value:
        new_data["uid"] = jwt_get_uid(token_value)
        if new_data["uid"]:
            logger.info("[%s] 🔍 UID найден из токена: %s", account.mail, new_data["uid"])

    # ✅ Обновляем основной new_data0.json
    await update_new_data_file(account.mail, cookies, new_data.get("uid"))

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


async def update_new_data_file(email: str, cookies: list, uid: Optional[str]) -> None:
    """Обновляет куки для указанного email в new_data0.json."""
    if not uid:
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

    data = load_json_safe(NEW_DATA_FILE)
    updated = False

    for acc in data:
        if acc.get("mail") == email:
            if uid not in acc:
                acc[uid] = {}
            acc[uid].update(cookie_map)
            updated = True
            break

    if not updated:
        # если аккаунта нет — создаём новую запись
        data.append({
            "mail": email,
            "paswd": "",
            uid: cookie_map
        })

    save_json_safe(NEW_DATA_FILE, data)
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
                await persist_success(account, context, target_page)
            elif status is LoginStatus.CHALLENGE:
                logger.warning("[%s] требуется ручное подтверждение: %s", email, message)
                if INTERACTIVE:
                    await wait_for_user_confirmation(email)
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
