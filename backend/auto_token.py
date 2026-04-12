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
import re
import sys
import logging

from dotenv import load_dotenv, set_key

logger = logging.getLogger("auto_token")

_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def refresh_token(env_file: str = _ENV_FILE) -> bool:
    """
    Perform headless Kite login via Playwright and save new KITE_ACCESS_TOKEN to .env.
    Returns True on success, False on failure.
    Auto-detects asyncio context and runs in a thread if needed.
    """
    import asyncio
    try:
        asyncio.get_running_loop()
        # Inside asyncio — must run Playwright sync API in a separate thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_do_refresh, env_file)
            return future.result(timeout=300)
    except RuntimeError:
        # No event loop running — call directly (CLI / APScheduler thread)
        return _do_refresh(env_file)


def _do_refresh(env_file: str = _ENV_FILE) -> bool:
    """Internal: actual Playwright login. Always called from a plain thread."""
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
        import pyotp
    except ImportError:
        logger.error(
            "auto_token: pyotp is not installed. "
            "Run: pip install pyotp  (included in backend/requirements.txt — reinstall or redeploy the image)."
        )
        return False

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error(
            "auto_token: Playwright Python package missing. "
            "Local: pip install -r backend/requirements.txt && playwright install chromium "
            "(or: python -m playwright install chromium). "
            "Docker: rebuild the image from the repo Dockerfile (includes browser install)."
        )
        return False

    # Same entry URL as generate_token.py (kite.trade can differ and break redirects)
    connect_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"
    request_token = None

    captured_url = []

    def _capture(u: str) -> None:
        if not u or "request_token" not in u:
            return
        if u not in captured_url:
            captured_url.append(u)

    # TOTP secret: strip whitespace; pyotp accepts standard Base32
    totp_secret = "".join(totp_secret.split()).upper()

    try:
        headless = os.getenv("PLAYWRIGHT_KITE_HEADLESS", "true").strip().lower() not in (
            "0", "false", "no",
        )
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/134.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            # Capture request_token from any network activity / navigation / redirects
            page.on("request", lambda req: _capture(req.url))

            def _on_response(res):
                _capture(res.url)
                try:
                    loc = res.headers.get("location") or res.headers.get("Location")
                    if loc:
                        _capture(loc)
                except Exception:
                    pass

            page.on("response", _on_response)

            def _on_frame(frame):
                try:
                    _capture(frame.url)
                except Exception:
                    pass

            page.on("framenavigated", _on_frame)

            # Abort local redirect targets (app redirect URL) but keep the URL text.
            # Globs like **127.0.0.1** often fail to match; use regex on full URL.
            def _route_local(route, req):
                _capture(req.url)
                route.abort()

            for rx in (
                re.compile(r"https?://127\.0\.0\.1(?::\d+)?(?:/|\?|$)"),
                re.compile(r"https?://localhost(?::\d+)?(?:/|\?|$)"),
            ):
                context.route(rx, _route_local)

            # ── Step 1: Navigate to Kite Connect login ────────────────────────
            logger.info("auto_token: opening Kite login page...")
            page.goto(connect_url, wait_until="domcontentloaded", timeout=45000)

            # ── Step 2: Enter user ID and password ────────────────────────────
            user_sel = (
                'input#userid, input[name="user_id"], input[type="text"]'
            )
            page.wait_for_selector(user_sel, timeout=20000)
            page.fill(user_sel, user_id)
            page.fill('input[type="password"]', password)
            page.click('button[type="submit"]')
            logger.info("auto_token: password submitted")

            # ── Step 3: Wait for TOTP / 2FA field (avoid filling wrong text input) ─
            totp_value = pyotp.TOTP(totp_secret).now()
            totp_loc = page.locator(
                "input#totp, input[name='twofa'], input[name='twoFA'], "
                "input[autocomplete='one-time-code'], "
                "input[placeholder*='TOTP'], input[placeholder*='totp'], "
                "input[placeholder*='Authenticator']"
            ).first
            try:
                totp_loc.wait_for(state="visible", timeout=28000)
            except Exception:
                totp_loc = page.locator(
                    "form input[type='tel'], form input[type='number']"
                ).first
                totp_loc.wait_for(state="visible", timeout=8000)
            totp_loc.click(timeout=3000)
            totp_loc.fill("")
            totp_loc.fill(totp_value)
            logger.info("auto_token: TOTP submitted (6-digit code generated from secret)")

            # Submit if button exists; else Enter (some flows auto-submit)
            try:
                page.locator('button[type="submit"]').first.click(timeout=4000)
            except Exception:
                try:
                    page.keyboard.press("Enter")
                except Exception:
                    pass

            # ── Step 4: Wait for redirect URL (up to ~90s) ────────────────────
            from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

            def _url_has_token(u: str) -> bool:
                return bool(u and "request_token=" in u)

            try:
                page.wait_for_url(_url_has_token, timeout=90_000)
            except PlaywrightTimeoutError:
                logger.info("auto_token: wait_for_url timed out; polling page URL…")

            import time as _time
            for _ in range(180):
                if captured_url:
                    break
                _capture(page.url)
                if captured_url:
                    break
                _time.sleep(0.5)

            if not captured_url:
                try:
                    logger.error(f"auto_token: final page URL: {page.url}")
                    logger.error(f"auto_token: page title: {page.title()!r}")
                except Exception:
                    pass

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
    parsed = urlparse(final_url)
    params = parse_qs(parsed.query)
    token_list = params.get("request_token")
    if not token_list and parsed.fragment:
        params = parse_qs(parsed.fragment)
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

    # ── Step 6: Persist to .env + current process env (scheduler / live reload) ─
    set_key(env_file, "KITE_ACCESS_TOKEN", access_token)
    os.environ["KITE_ACCESS_TOKEN"] = access_token
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

    ok = _do_refresh()
    if ok:
        print("\n  Token refreshed and saved to .env")
        print("  If the API server is already running, apply without restart:")
        print('    curl -X POST http://127.0.0.1:8000/api/token-reload-env')
        print("  Or use the UI: token banner → LOAD .ENV (or restart the backend).")
        print("=" * 55 + "\n")
        sys.exit(0)
    else:
        print("\n  Token refresh FAILED. Check logs above.")
        print("  Ensure these are set in backend/.env:")
        print("    KITE_API_KEY, KITE_API_SECRET")
        print("    KITE_USER_ID, KITE_PASSWORD, KITE_TOTP_SECRET")
        print("=" * 55 + "\n")
        sys.exit(1)
