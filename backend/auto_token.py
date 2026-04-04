"""
NSE EDGE v5 — Automatic Kite Access Token Refresher
Headless browser login via Playwright + pyotp (bypasses Cloudflare bot detection).

Requires in backend/.env:
    KITE_API_KEY=xxx
    KITE_API_SECRET=xxx
    KITE_USER_ID=ZAxxxx          ← your Zerodha login ID
    KITE_PASSWORD=yourpassword   ← your Zerodha password
    KITE_TOTP_SECRET=BASE32xxx   ← TOTP secret from Zerodha 2FA setup

Usage (manual):
    python auto_token.py

Integrated usage (called from feed.py on auth failure):
    from auto_token import refresh_token
    ok = refresh_token()
"""

import os
import sys
import logging
import pyotp

from dotenv import load_dotenv, set_key

logger = logging.getLogger("auto_token")

_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def refresh_token(env_file: str = _ENV_FILE) -> bool:
    """
    Perform headless Kite login via Playwright and save new KITE_ACCESS_TOKEN to .env.
    Returns True on success, False on failure.
    """
    load_dotenv(env_file, override=True)

    api_key     = os.getenv("KITE_API_KEY", "").strip()
    api_secret  = os.getenv("KITE_API_SECRET", "").strip()
    user_id     = os.getenv("KITE_USER_ID", "").strip()
    password    = os.getenv("KITE_PASSWORD", "").strip()
    totp_secret = os.getenv("KITE_TOTP_SECRET", "").strip()

    missing = [k for k, v in {
        "KITE_API_KEY": api_key,
        "KITE_API_SECRET": api_secret,
        "KITE_USER_ID": user_id,
        "KITE_PASSWORD": password,
        "KITE_TOTP_SECRET": totp_secret,
    }.items() if not v]

    if missing:
        logger.error(f"auto_token: missing .env keys: {', '.join(missing)}")
        logger.error("Add them to backend/.env and restart.")
        return False

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error("auto_token: playwright not installed. Run: pip install playwright && playwright install chromium")
        return False

    connect_url = f"https://kite.trade/connect/login?api_key={api_key}&v=3"
    request_token = None

    captured_url = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/134.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            # ── Listen for all navigations to capture request_token URL ─────
            def on_request(request):
                if "request_token" in request.url:
                    captured_url.append(request.url)

            page.on("request", on_request)

            # Also intercept and abort 127.0.0.1 so browser doesn't crash
            context.route("**/127.0.0.1/**", lambda route, req: (
                captured_url.append(req.url) or route.abort()
            ))
            context.route("**localhost**", lambda route, req: (
                captured_url.append(req.url) or route.abort()
            ))

            # ── Step 1: Navigate to Kite Connect login ────────────────────────
            logger.info("auto_token: opening Kite login page...")
            page.goto(connect_url, wait_until="networkidle", timeout=30000)

            # ── Step 2: Enter user ID and password ────────────────────────────
            page.fill('input[type="text"]', user_id)
            page.fill('input[type="password"]', password)
            page.click('button[type="submit"]')
            logger.info("auto_token: password submitted")

            # ── Step 3: Wait for TOTP field and fill it ───────────────────────
            totp_selector = 'input[type="number"], input[placeholder*="TOTP"], input[placeholder*="totp"], input[autocomplete="one-time-code"]'
            page.wait_for_selector(totp_selector, timeout=15000)
            totp_value = pyotp.TOTP(totp_secret).now()
            page.fill(totp_selector, totp_value)
            logger.info(f"auto_token: TOTP {totp_value} filled")

            # Submit if button is clickable, otherwise TOTP auto-submits on 6 digits
            try:
                page.locator('button[type="submit"]').click(timeout=5000)
            except Exception:
                pass

            # ── Step 4: Wait up to 20s for redirect URL to be captured ────────
            import time as _time
            for _ in range(40):
                if captured_url:
                    break
                _time.sleep(0.5)

    except Exception as e:
        if not captured_url:
            logger.error(f"auto_token: browser login failed: {e}")
            return False

    if not captured_url:
        logger.error("auto_token: request_token URL not captured")
        return False

    final_url = captured_url[0]
    logger.info(f"auto_token: captured redirect: {final_url[:60]}...")

    from urllib.parse import urlparse, parse_qs
    params = parse_qs(urlparse(final_url).query)
    token_list = params.get("request_token")
    if not token_list:
        logger.error(f"auto_token: request_token not found in URL: {final_url}")
        return False
    request_token = token_list[0]

    logger.info(f"auto_token: got request_token ({request_token[:8]}...)")

    # ── Step 5: Exchange request_token for access_token ──────────────────────
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        sess = kite.generate_session(request_token, api_secret=api_secret)
        access_token = sess["access_token"]
        user_name    = sess.get("user_name", user_id)
    except Exception as e:
        logger.error(f"auto_token: generate_session failed: {e}")
        return False

    # ── Step 6: Persist to .env ───────────────────────────────────────────────
    set_key(env_file, "KITE_ACCESS_TOKEN", access_token)
    root_env = os.path.normpath(os.path.join(os.path.dirname(env_file), "..", ".env"))
    if os.path.isfile(root_env):
        set_key(root_env, "KITE_ACCESS_TOKEN", access_token)

    logger.info(f"auto_token: SUCCESS — {user_name} | token ...{access_token[-6:]}")
    return True


# ─── CLI usage ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  auto_token  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    print("\n" + "=" * 55)
    print("  NSE EDGE v5 — Auto Token Refresh")
    print("=" * 55)

    ok = refresh_token()
    if ok:
        print("\n  Token refreshed and saved to .env")
        print("  Restart the backend to apply the new token.")
        print("=" * 55 + "\n")
        sys.exit(0)
    else:
        print("\n  Token refresh FAILED. Check logs above.")
        print("  Ensure these are set in backend/.env:")
        print("    KITE_API_KEY, KITE_API_SECRET")
        print("    KITE_USER_ID, KITE_PASSWORD, KITE_TOTP_SECRET")
        print("=" * 55 + "\n")
        sys.exit(1)
