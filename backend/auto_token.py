"""
STOCKR.IN v5 - Automatic Kite access-token refresher.

Headless browser login via Playwright + pyotp.

Requires in backend/.env:
    KITE_API_KEY=xxx
    KITE_API_SECRET=xxx
    KITE_USER_ID=ZAxxxx
    KITE_PASSWORD=yourpassword
    KITE_TOTP_SECRET=BASE32xxx

Usage:
    python auto_token.py

If Zerodha shows a CAPTCHA, use a visible browser (solve CAPTCHA in the window; TOTP
is filled by the script). Google Chrome instead of bundled Chromium::

    $env:PLAYWRIGHT_KITE_CHANNEL='chrome'
    $env:PLAYWRIGHT_KITE_HEADLESS='false'
    python auto_token.py
"""

import logging
import os
import re
import sys
import time

from dotenv import load_dotenv, set_key

logger = logging.getLogger("auto_token")

_ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def refresh_token(env_file: str = _ENV_FILE) -> bool:
    """
    Perform headless Kite login via Playwright and save a fresh
    KITE_ACCESS_TOKEN to .env.
    """
    import asyncio

    try:
        asyncio.get_running_loop()
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_do_refresh, env_file)
            return future.result(timeout=300)
    except RuntimeError:
        return _do_refresh(env_file)


def _do_refresh(env_file: str = _ENV_FILE) -> bool:
    # Preserve shell choice for headed vs headless (load_dotenv(override=True) would
    # otherwise clobber e.g. $env:PLAYWRIGHT_KITE_HEADLESS='false' in PowerShell).
    _saved_playwright_headless = os.environ.get("PLAYWRIGHT_KITE_HEADLESS")
    _saved_playwright_channel = os.environ.get("PLAYWRIGHT_KITE_CHANNEL")
    load_dotenv(env_file, override=True)
    if _saved_playwright_headless is not None:
        os.environ["PLAYWRIGHT_KITE_HEADLESS"] = _saved_playwright_headless
    if _saved_playwright_channel is not None:
        os.environ["PLAYWRIGHT_KITE_CHANNEL"] = _saved_playwright_channel

    api_key = os.getenv("KITE_API_KEY", "").strip()
    api_secret = os.getenv("KITE_API_SECRET", "").strip()
    user_id = os.getenv("KITE_USER_ID", "").strip()
    password = os.getenv("KITE_PASSWORD", "").strip()
    totp_secret = os.getenv("KITE_TOTP_SECRET", "").strip()

    missing = [
        key
        for key, value in {
            "KITE_API_KEY": api_key,
            "KITE_API_SECRET": api_secret,
            "KITE_USER_ID": user_id,
            "KITE_PASSWORD": password,
            "KITE_TOTP_SECRET": totp_secret,
        }.items()
        if not value
    ]
    if missing:
        logger.error("auto_token: missing .env keys: %s", ", ".join(missing))
        logger.error("Add them to backend/.env and restart.")
        return False

    try:
        import pyotp
    except ImportError:
        logger.error(
            "auto_token: pyotp is not installed. Run: pip install pyotp "
            "(or reinstall backend requirements)."
        )
        return False

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        logger.error(
            "auto_token: Playwright Python package missing. "
            "Run: pip install -r backend/requirements.txt && playwright install chromium"
        )
        return False

    connect_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"
    captured_url: list[str] = []
    totp_secret = "".join(totp_secret.split()).upper()

    def _capture(url: str) -> None:
        if url and "request_token=" in url and url not in captured_url:
            captured_url.append(url)

    def _page_excerpt(page, limit: int = 700) -> str:
        try:
            body = page.locator("body").inner_text(timeout=3000)
            return " ".join(str(body or "").split())[:limit]
        except Exception:
            return ""

    def _twofa_input(page):
        return page.locator(
            "form.twofa-form input, "
            "input#totp, input[name='twofa'], input[name='twoFA'], "
            "input[autocomplete='one-time-code'], "
            "input[placeholder*='TOTP'], input[placeholder*='totp'], "
            "input[placeholder*='Authenticator'], "
            "form input[type='tel'], form input[type='number']"
        ).first

    def _login_captcha_locator(page):
        """Zerodha Kite login captcha field (same page as user id + password)."""
        return page.locator(
            "input#captcha, input[name='captcha'], input[name='Captcha'], "
            "input[placeholder*='captcha'], input[placeholder*='Captcha'], "
            "input[aria-label*='captcha'], input[aria-label*='Captcha']"
        ).first

    def _wait_for_manual_captcha_if_present(page, headless: bool) -> None:
        """
        If a captcha field is visible, headless runs cannot solve it — fail fast
        with a clear message. In headed mode, wait for the user to type the
        captcha in the browser, then continue.
        """
        cap = _login_captcha_locator(page)
        try:
            if not cap.is_visible(timeout=8000):
                return
        except Exception:
            return
        if headless:
            raise RuntimeError(
                "Kite login shows a CAPTCHA (headless Chrome cannot solve it). "
                "Re-run with PLAYWRIGHT_KITE_HEADLESS=false — a window will open; "
                "enter the captcha there, then the script continues with TOTP."
            )
        wait_sec = int(os.getenv("KITE_CAPTCHA_WAIT_SEC", "180").strip() or "180")
        wait_sec = max(30, min(wait_sec, 600))
        logger.info(
            "auto_token: CAPTCHA field detected — type it in the browser window. "
            "Waiting up to %ss, then login will submit automatically.",
            wait_sec,
        )
        deadline = time.time() + wait_sec
        while time.time() < deadline:
            try:
                val = (cap.input_value(timeout=800) or "").strip()
                if len(val) >= 4:
                    logger.info("auto_token: captcha field looks filled; submitting login")
                    return
            except Exception:
                pass
            page.wait_for_timeout(400)
        raise RuntimeError(
            f"Timed out after {wait_sec}s — captcha not detected as filled. "
            "Try again and enter the captcha in the Chromium window."
        )

    def _headed_clear_twofa_setup_wizard(page, headless: bool) -> None:
        """
        Zerodha sometimes shows 'Setup 2Factor' / method choice instead of the TOTP
        field. In headed mode, wait for the user to click through in Chrome.
        """
        if headless:
            return
        setup_markers = (
            "setup 2factor",
            "external authenticator",
            "method 1:",
            "method 2:",
            "kite mobile",
        )
        wait_sec = int(
            (os.getenv("KITE_HEADED_2FA_SETUP_WAIT_SEC", "180").strip() or "180")
        )
        wait_sec = max(30, min(wait_sec, 600))
        deadline = time.time() + wait_sec
        while time.time() < deadline:
            try:
                if _twofa_input(page).is_visible(timeout=900):
                    logger.info("auto_token: 2FA code field is visible — continuing")
                    return
            except Exception:
                pass
            try:
                ex = _page_excerpt(page).lower()
            except Exception:
                ex = ""
            if ex and not any(m in ex for m in setup_markers):
                return
            logger.info(
                "auto_token: 2FA setup / method screen — finish steps in Chrome (~%ds left)",
                max(0, int(deadline - time.time())),
            )
            page.wait_for_timeout(2500)

    def _wait_for_twofa_or_login_error(page) -> None:
        """
        After password submit, either the 2FA field should appear or the page
        should expose the actual blocker (captcha/login error).
        """
        failure_markers = (
            "invalid captcha",
            "captcha values",
            "too many attempts",
            "account is locked",
            "incorrect userid or password",
            "password is incorrect",
        )
        deadline = time.time() + 35
        while time.time() < deadline:
            locator = _twofa_input(page)
            try:
                if locator.is_visible(timeout=500):
                    return
            except Exception:
                pass
            excerpt = _page_excerpt(page).lower()
            if any(marker in excerpt for marker in failure_markers):
                raise RuntimeError(_page_excerpt(page))
            page.wait_for_timeout(500)
        excerpt = _page_excerpt(page)
        raise RuntimeError(excerpt or "2FA screen did not appear after password submit")

    def _fill_twofa_code(page, code: str) -> None:
        """
        Zerodha's current 2FA screen uses a number input inside form.twofa-form.
        Simple `fill()` can be ignored there, so use keyboard entry first and
        then force DOM events as a fallback.
        """
        locator = _twofa_input(page)
        locator.wait_for(state="visible", timeout=28000)
        locator.click(timeout=3000)
        try:
            locator.press("Control+A", timeout=1500)
        except Exception:
            pass
        try:
            locator.press("Backspace", timeout=1500)
        except Exception:
            pass
        try:
            page.keyboard.insert_text(code)
        except Exception:
            try:
                page.keyboard.type(code, delay=60)
            except Exception:
                pass
        page.wait_for_timeout(250)
        try:
            current_val = locator.input_value(timeout=1500)
        except Exception:
            current_val = ""
        if current_val != code:
            try:
                locator.evaluate(
                    """(el, val) => {
                        const setter = Object.getOwnPropertyDescriptor(
                          HTMLInputElement.prototype, "value"
                        )?.set;
                        if (setter) setter.call(el, val);
                        else el.value = val;
                        el.setAttribute("value", val);
                        el.dispatchEvent(new Event("input", { bubbles: true }));
                        el.dispatchEvent(new Event("change", { bubbles: true }));
                        el.dispatchEvent(new KeyboardEvent("keyup", { bubbles: true, key: "0" }));
                    }""",
                    code,
                )
            except Exception:
                try:
                    locator.fill(code, timeout=2000)
                except Exception:
                    pass

    def _submit_twofa(page) -> None:
        try:
            page.locator(
                "form.twofa-form button[type='submit'], button[type='submit']"
            ).first.click(timeout=4000)
        except Exception:
            try:
                page.keyboard.press("Enter")
            except Exception:
                pass

    def _wait_for_redirect_or_error(page) -> str | None:
        invalid_markers = (
            "invalid app code",
            "invalid otp",
            "incorrect app code",
            "please fill out this field",
            "expired app code",
        )

        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

        def _url_has_token(url: str) -> bool:
            return bool(url and "request_token=" in url)

        try:
            page.wait_for_url(_url_has_token, timeout=20000)
        except PlaywrightTimeoutError:
            pass
        if captured_url:
            return captured_url[0]

        deadline = time.time() + 20
        while time.time() < deadline:
            _capture(page.url)
            if captured_url:
                return captured_url[0]
            excerpt = _page_excerpt(page).lower()
            if any(marker in excerpt for marker in invalid_markers):
                return "2FA code rejected by Zerodha"
            page.wait_for_timeout(500)
        return None

    try:
        headless = os.getenv("PLAYWRIGHT_KITE_HEADLESS", "true").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        with sync_playwright() as p:
            # Use installed Google Chrome: PLAYWRIGHT_KITE_CHANNEL=chrome (see Playwright docs).
            _channel = os.getenv("PLAYWRIGHT_KITE_CHANNEL", "").strip()
            _launch: dict = {"headless": headless}
            if _channel:
                _launch["channel"] = _channel
            browser = p.chromium.launch(**_launch)
            context = browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/134.0.0.0 Safari/537.36"
                )
            )
            page = context.new_page()

            page.on("request", lambda req: _capture(req.url))

            def _on_response(res):
                _capture(res.url)
                try:
                    loc = res.headers.get("location") or res.headers.get("Location")
                    if loc:
                        _capture(loc)
                except Exception:
                    pass

            def _on_frame(frame):
                try:
                    _capture(frame.url)
                except Exception:
                    pass

            page.on("response", _on_response)
            page.on("framenavigated", _on_frame)

            def _route_local(route, req):
                _capture(req.url)
                route.abort()

            for rx in (
                re.compile(r"https?://127\.0\.0\.1(?::\d+)?(?:/|\?|$)"),
                re.compile(r"https?://localhost(?::\d+)?(?:/|\?|$)"),
            ):
                context.route(rx, _route_local)

            logger.info("auto_token: opening Kite login page...")
            page.goto(connect_url, wait_until="domcontentloaded", timeout=45000)

            # Do not use input[type="text"] — it can match the CAPTCHA field before user id.
            user_loc = page.locator('input#userid, input[name="user_id"]').first
            user_loc.wait_for(state="visible", timeout=20000)
            user_loc.fill(user_id)
            pwd_loc = page.locator(
                'input#password, input[name="password"], input[type="password"]'
            ).first
            pwd_loc.wait_for(state="visible", timeout=10000)
            pwd_loc.fill(password)
            _wait_for_manual_captcha_if_present(page, headless)
            if not headless:
                pre_sec = int(
                    (os.getenv("KITE_HEADED_PRELOGIN_WAIT_SEC", "25").strip() or "25")
                )
                pre_sec = max(0, min(pre_sec, 120))
                if pre_sec > 0:
                    logger.info(
                        "auto_token: headed mode — waiting %ss: solve CAPTCHA in the "
                        "browser if you see one, then the script will click Continue.",
                        pre_sec,
                    )
                    page.wait_for_timeout(pre_sec * 1000)
            page.click('button[type="submit"]')
            logger.info("auto_token: password submitted")
            _headed_clear_twofa_setup_wizard(page, headless)
            _wait_for_twofa_or_login_error(page)

            for attempt in range(2):
                if attempt == 1:
                    wait_s = max(1, 31 - int(time.time()) % 30)
                    logger.info("auto_token: waiting %ss for fresh 2FA code retry", wait_s)
                    page.wait_for_timeout(wait_s * 1000)

                code = pyotp.TOTP(totp_secret).now()
                _fill_twofa_code(page, code)
                logger.info("auto_token: 2FA code entered (attempt %s)", attempt + 1)
                _submit_twofa(page)

                outcome = _wait_for_redirect_or_error(page)
                if outcome and "request_token=" in outcome:
                    break
                if outcome:
                    logger.warning("auto_token: %s", outcome)
                    if attempt == 0:
                        continue
                break

            if not captured_url:
                try:
                    try:
                        from urllib.parse import urlparse, urlunparse

                        pu = urlparse(page.url)
                        safe = urlunparse(
                            (pu.scheme, pu.netloc, pu.path, "", "", "")
                        )
                        logger.error("auto_token: final page URL (query stripped): %s", safe)
                    except Exception:
                        logger.error("auto_token: final page URL: (unavailable)")
                    logger.error("auto_token: page title: %r", page.title())
                    excerpt = _page_excerpt(page)
                    if excerpt:
                        logger.error("auto_token: page excerpt: %s", excerpt)
                except Exception:
                    pass
    except Exception as exc:
        if not captured_url:
            logger.error("auto_token: browser login failed: %s", exc)
            return False

    if not captured_url:
        logger.error("auto_token: request_token URL not captured")
        return False

    final_url = captured_url[0]
    logger.info("auto_token: captured redirect: %s...", final_url[:60])

    from urllib.parse import parse_qs, urlparse

    parsed = urlparse(final_url)
    params = parse_qs(parsed.query)
    token_list = params.get("request_token")
    if not token_list and parsed.fragment:
        params = parse_qs(parsed.fragment)
        token_list = params.get("request_token")
    if not token_list:
        logger.error("auto_token: request_token not found in URL: %s", final_url)
        return False
    request_token = token_list[0]
    logger.info("auto_token: got request_token (%s...)", request_token[:8])

    try:
        from kiteconnect import KiteConnect

        kite = KiteConnect(api_key=api_key)
        sess = kite.generate_session(request_token, api_secret=api_secret)
        access_token = sess["access_token"]
        user_name = sess.get("user_name", user_id)
    except Exception as exc:
        logger.error("auto_token: generate_session failed: %s", exc)
        return False

    set_key(env_file, "KITE_ACCESS_TOKEN", access_token)
    os.environ["KITE_ACCESS_TOKEN"] = access_token

    root_env = os.path.normpath(os.path.join(os.path.dirname(env_file), "..", ".env"))
    if os.path.isfile(root_env):
        set_key(root_env, "KITE_ACCESS_TOKEN", access_token)

    logger.info("auto_token: SUCCESS - %s | token ...%s", user_name, access_token[-6:])
    return True


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  auto_token  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    print("\n" + "=" * 55)
    print("  STOCKR.IN v5 - Auto Token Refresh")
    print("=" * 55)

    ok = _do_refresh()
    if ok:
        print("\n  Token refreshed and saved to .env")
        print("  If the API server is already running, apply without restart:")
        print("    curl -X POST http://127.0.0.1:8000/api/token-reload-env")
        print("  Or use the UI: token banner -> LOAD .ENV (or restart the backend).")
        print("=" * 55 + "\n")
        sys.exit(0)

    print("\n  Token refresh FAILED. Check logs above.")
    print("  Ensure these are set in backend/.env:")
    print("    KITE_API_KEY, KITE_API_SECRET")
    print("    KITE_USER_ID, KITE_PASSWORD, KITE_TOTP_SECRET")
    print("  If you see CAPTCHA errors, run headed (solve captcha in the window):")
    print('    $env:PLAYWRIGHT_KITE_HEADLESS="false"; python auto_token.py')
    print("=" * 55 + "\n")
    sys.exit(1)
