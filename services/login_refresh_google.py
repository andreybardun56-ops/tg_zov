import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional

from playwright.async_api import async_playwright, Page, Response
from services.browser_patches import launch_masked_persistent_context, get_random_browser_profile, BROWSER_PATH

# ----------------- Настройки -----------------
BASE_DIR = Path('.')
ACCOUNTS_FILE = BASE_DIR / "data/google_accounts.json"
OUT_DIR = BASE_DIR / "data/data_akk"
LOGS_DIR = BASE_DIR / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "login_refresh_google.log"

CONCURRENT = int(os.getenv("CONCURRENT", "1"))
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT", "60000"))
HEADLESS = os.getenv("HEADLESS", "0") == "1"
INTERACTIVE = os.getenv("INTERACTIVE", "0") == "1"
SLOW_MO = float(os.getenv("SLOW_MO", "100"))
WAIT_AFTER_NEXT = int(os.getenv("WAIT_AFTER_NEXT", "5"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding='utf-8')]
)

logger = logging.getLogger("login_refresh_google")

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

async def login_one_account(account: Dict[str, str], sem: asyncio.Semaphore, playwright) -> None:
    async with sem:
        email = account.get("mail")
        password = account.get("paswd")
        logger.info("[%s] start", email)

        profile = get_random_browser_profile()
        user_data_dir = str(Path("data/chrome_profiles") / (email.replace("@", "__at__")))

        ctx = await launch_masked_persistent_context(
            playwright,
            user_data_dir=user_data_dir,
            browser_path=BROWSER_PATH,
            headless=HEADLESS,
            slow_mo=SLOW_MO,
            profile=profile,
        )

        context = ctx["context"]
        page = ctx["page"]

        last_response: Optional[Response] = None

        def _on_response(resp: Response):
            nonlocal last_response
            try:
                url = resp.url
            except Exception:
                url = None
            if url and any(k in url for k in ["igg.com", "accounts.google.com"]):
                last_response = resp

        page.on("response", _on_response)

        try:
            login_url = "https://passport.igg.com/login/platform?url=https%3A%2F%2Fpassport.igg.com%2Fbindings&provider=googleplus"
            await page.goto(login_url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT)

            # === Accept Cookies ===
            try:
                for sel in ["#onetrust-accept-btn-handler", "text=Accept All", "text=Принять все"]:
                    if await page.locator(sel).count() > 0:
                        await page.locator(sel).click(timeout=3000)
                        logger.info("[%s] clicked cookie banner %s", email, sel)
                        break
            except Exception:
                logger.debug("[%s] no cookie banner found", email)

            # === Email ===
            logger.info("[%s] waiting for email field", email)
            await page.wait_for_selector("input#identifierId", timeout=30000)
            await page.fill("input#identifierId", email)
            logger.info("[%s] email entered", email)
            await page.click("#identifierNext")
            logger.info("[%s] clicked 'Next' after email", email)
            logger.info("[%s] waiting %s seconds after clicking Next...", email, WAIT_AFTER_NEXT)
            await asyncio.sleep(WAIT_AFTER_NEXT)

            # === Password ===
            await page.wait_for_selector("input[name=Passwd]", timeout=30000)
            await page.fill("input[name=Passwd]", password)
            logger.info("[%s] password entered", email)
            await page.click("#passwordNext")
            logger.info("[%s] clicked 'Next' after password", email)
            logger.info("[%s] waiting %s seconds after clicking Next...", email, WAIT_AFTER_NEXT)
            await asyncio.sleep(WAIT_AFTER_NEXT)

            # === Проверка результата ===
            try:
                await page.wait_for_url(lambda u: "passport.igg.com" in u or "igg.com" in u, timeout=60000)
                logger.info("[%s] redirected back to IGG", email)
            except Exception:
                logger.info("[%s] stay on Google — likely 2FA or captcha, manual check required", email)

            print(f"\n[{email}] Проверь окно браузера. Когда всё закончишь — нажми Enter...\n")

            await asyncio.get_event_loop().run_in_executor(None, sys.stdin.readline)

            all_cookies = await context.cookies()
            page_snapshot = await page.content()

            await save_last_response(email, last_response)
            new_data = make_new_data_format(email, all_cookies, page_snapshot)
            filename = OUT_DIR / (email.replace("@", "__at__") + ".json")
            atomic_write(filename, new_data)
            logger.info("[%s] saved new_data -> %s", email, filename)

        except Exception as e:
            logger.exception("[%s] failed: %s", email, e)
        finally:
            try:
                await context.close()
            except Exception:
                pass


async def main():
    if not ACCOUNTS_FILE.exists():
        logger.error("accounts file not found: %s", ACCOUNTS_FILE)
        sys.exit(1)

    accounts = json.loads(ACCOUNTS_FILE.read_text(encoding="utf-8"))
    cleaned = [{"mail": a.get("mail"), "paswd": a.get("paswd")} for a in accounts if a.get("mail") and a.get("paswd")]

    sem = asyncio.Semaphore(CONCURRENT)

    async with async_playwright() as pw:
        await asyncio.gather(*(login_one_account(acc, sem, pw) for acc in cleaned))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("interrupted by user")
