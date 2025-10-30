import argparse
import asyncio
import enum
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from playwright.async_api import Page, Response, async_playwright
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
LOGS_DIR = BASE_DIR / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "login_refresh_google.log"

CONCURRENT = max(1, int(os.getenv("CONCURRENT", "1")))
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT", "60000"))
HEADLESS = os.getenv("HEADLESS", "0") == "1"
INTERACTIVE = os.getenv("INTERACTIVE", "0") == "1"
SLOW_MO = float(os.getenv("SLOW_MO", "100"))
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


async def perform_login_flow(page: Page, email: str, password: str) -> tuple[LoginStatus, str]:
    login_url = "https://passport.igg.com/login/platform?url=https%3A%2F%2Fpassport.igg.com%2Fbindings&provider=googleplus"
    await page.goto(login_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

    try:
        await page.wait_for_load_state("domcontentloaded", timeout=15000)
    except Exception:
        pass

    try:
        for sel in ["#onetrust-accept-btn-handler", "text=Accept All", "text=Принять все"]:
            locator = page.locator(sel)
            if await locator.count() > 0:
                await locator.click(timeout=3000)
                logger.info("[%s] закрыт баннер cookies (%s)", email, sel)
                break
    except Exception:
        logger.debug("[%s] баннер cookies не найден", email)

    logger.info("[%s] вводим e-mail", email)
    await page.wait_for_selector("input#identifierId", timeout=30000)
    await page.fill("input#identifierId", email)
    await page.click("#identifierNext")
    logger.debug("[%s] ожидание после identifierNext %s с", email, WAIT_AFTER_NEXT)
    if WAIT_AFTER_NEXT:
        await asyncio.sleep(WAIT_AFTER_NEXT)

    await page.wait_for_selector("input[name=Passwd]", timeout=30000)
    await page.fill("input[name=Passwd]", password)
    await page.click("#passwordNext")
    logger.debug("[%s] ожидание после passwordNext %s с", email, WAIT_AFTER_NEXT)
    if WAIT_AFTER_NEXT:
        await asyncio.sleep(WAIT_AFTER_NEXT)

    try:
        await page.wait_for_load_state("networkidle", timeout=30000)
    except Exception:
        pass

    current_url = page.url
    error_text = await detect_login_error(page)
    if error_text:
        return LoginStatus.FAILED, f"Google сообщил об ошибке: {error_text}"

    challenge_text = await detect_challenge(page)
    if challenge_text:
        return LoginStatus.CHALLENGE, challenge_text

    if "passport.igg.com" in current_url or "igg.com" in current_url:
        return LoginStatus.SUCCESS, f"Перенаправление на IGG успешно (url: {current_url})"

    return LoginStatus.FAILED, f"Не удалось подтвердить авторизацию, текущий URL: {current_url}"


async def persist_success(account: Account, context, page: Page) -> None:
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

    new_data = make_new_data_format(account.mail, cookies, page_snapshot)
    filename = OUT_DIR / f"{account.slug}.json"
    atomic_write(filename, new_data)
    logger.info(
        "[%s] cookies сохранены -> %s (uid=%s, cookies=%s)",
        account.mail,
        filename,
        new_data.get("uid") or "-",
        len(new_data.get("cookies", [])),
    )


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
        if skip_existing and output_file.exists():
            logger.info("[%s] пропускаю — файл %s уже существует", email, output_file)
            return

        logger.info(
            "[%s] старт авторизации (headless=%s, interactive=%s)",
            email,
            HEADLESS,
            INTERACTIVE,
        )

        profile = get_random_browser_profile()
        user_data_dir = str((BASE_DIR / "data" / "chrome_profiles" / account.slug).resolve())

        try:
            ctx = await launch_masked_persistent_context(
                playwright,
                user_data_dir=user_data_dir,
                browser_path=BROWSER_PATH,
                headless=HEADLESS,
                slow_mo=SLOW_MO,
                profile=profile,
            )
        except Exception as exc:
            logger.exception("[%s] не удалось запустить браузер: %s", email, exc)
            return

        context = ctx["context"]
        page = ctx["page"]
        recorder = ResponseRecorder()
        page.on("response", recorder)

        try:
            status, message = await perform_login_flow(page, email, account.password)
            logger.info("[%s] результат авторизации: %s", email, message)

            if status is LoginStatus.SUCCESS:
                await persist_success(account, context, page)
            elif status is LoginStatus.CHALLENGE:
                logger.warning("[%s] требуется ручное подтверждение: %s", email, message)
                if INTERACTIVE:
                    await wait_for_user_confirmation(email)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=15000)
                    except Exception:
                        pass
                    await persist_success(account, context, page)
                else:
                    artifacts = await capture_page_artifacts(page, account.slug, "challenge")
                    if artifacts:
                        logger.info(
                            "[%s] сохранены артефакты для разбора: %s",
                            email,
                            ", ".join(str(p) for p in artifacts),
                        )
            else:
                logger.error("[%s] авторизация не удалась", email)
                artifacts = await capture_page_artifacts(page, account.slug, "failed")
                if artifacts:
                    logger.info(
                        "[%s] сохранены артефакты ошибки: %s",
                        email,
                        ", ".join(str(p) for p in artifacts),
                    )
        except Exception as exc:
            logger.exception("[%s] непредвиденная ошибка: %s", email, exc)
            artifacts = await capture_page_artifacts(page, account.slug, "exception")
            if artifacts:
                logger.info(
                    "[%s] сохранены артефакты после исключения: %s",
                    email,
                    ", ".join(str(p) for p in artifacts),
                )
        finally:
            await save_last_response(email, recorder.last)
            try:
                page.off("response", recorder)
            except Exception:
                pass
            try:
                await context.close()
            except Exception:
                pass


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

    return asyncio.run(main_async(args))


if __name__ == "__main__":
    try:
        exit_code = main()
    except KeyboardInterrupt:
        logger.info("Остановка по Ctrl+C")
        exit_code = 130
    sys.exit(exit_code)
