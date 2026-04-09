"""
NSE EDGE v5 — Zerodha Kite Connect Daily Token Generator
Run this script ONCE every morning before starting the system.

Usage:
    python generate_token.py

Flow (TOTP / authenticator app):
  1. Opens Kite login in your browser
  2. You enter password, then the 6-digit code from your TOTP app (or SMS if enabled)
  3. After redirect, paste the full URL from the address bar OR only the request_token value
  4. Writes KITE_ACCESS_TOKEN to backend/.env (and repo-root .env if present)

Fully automated TOTP (no browser): set KITE_USER_ID, KITE_PASSWORD, KITE_TOTP_SECRET and run:
    python auto_token.py
"""

import os
import sys
import webbrowser
from urllib.parse import urlparse, parse_qs

try:
    from kiteconnect import KiteConnect
except ImportError:
    print("ERROR: kiteconnect not installed.")
    print("Run: pip install kiteconnect")
    sys.exit(1)

try:
    from dotenv import load_dotenv, set_key
except ImportError:
    print("ERROR: python-dotenv not installed.")
    print("Run: pip install python-dotenv")
    sys.exit(1)


ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")


def _parse_request_token(raw: str) -> str:
    """Accept full redirect URL, query string, or raw token."""
    s = raw.strip().strip('"').strip("'")
    if not s:
        return ""
    if "request_token=" not in s:
        return s
    url = s if "://" in s else f"https://127.0.0.1/{s.lstrip('?&')}"
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    toks = params.get("request_token", [])
    if not toks and parsed.fragment:
        toks = parse_qs(parsed.fragment).get("request_token", [])
    return toks[0] if toks else s


def main():
    load_dotenv(ENV_FILE)

    api_key    = os.getenv("KITE_API_KEY", "").strip()
    api_secret = os.getenv("KITE_API_SECRET", "").strip()

    if not api_key:
        print("\nKITE_API_KEY not found in .env")
        api_key = input("Enter your Kite API Key: ").strip()
        set_key(ENV_FILE, "KITE_API_KEY", api_key)

    if not api_secret:
        print("\nKITE_API_SECRET not found in .env")
        api_secret = input("Enter your Kite API Secret: ").strip()
        set_key(ENV_FILE, "KITE_API_SECRET", api_secret)

    kite      = KiteConnect(api_key=api_key)
    login_url = kite.login_url()

    print("\n" + "=" * 60)
    print("  STEP 1: Login to Kite (with TOTP)")
    print("=" * 60)
    print("\n  In the browser: User ID → Password → 6-digit TOTP from your authenticator app.")
    print("  Kite app redirect must match (e.g. https://127.0.0.1/ in developer settings).\n")
    print(f"Opening: {login_url}\n")
    webbrowser.open(login_url)

    print("After login you should be redirected to a URL like:")
    print("  https://127.0.0.1/?request_token=XXXX&action=login&status=success")
    print()
    print("Paste the FULL address-bar URL here, or paste only the request_token string:")

    request_token = _parse_request_token(input("> ").strip())
    if not request_token:
        print("ERROR: No request_token provided.")
        sys.exit(1)

    print("\nGenerating access token...")
    try:
        data         = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        user_id      = data.get("user_id", "")
        user_name    = data.get("user_name", "")
    except Exception as e:
        print(f"ERROR generating session: {e}")
        sys.exit(1)

    # Save to .env
    set_key(ENV_FILE, "KITE_ACCESS_TOKEN", access_token)
    root_env = os.path.normpath(os.path.join(os.path.dirname(ENV_FILE), "..", ".env"))
    if os.path.isfile(root_env):
        set_key(root_env, "KITE_ACCESS_TOKEN", access_token)

    print("\n" + "=" * 60)
    print("  SUCCESS")
    print("=" * 60)
    print(f"  User     : {user_name} ({user_id})")
    print(f"  Token    : {access_token[:12]}...{access_token[-4:]}")
    print(f"  Saved to : {ENV_FILE}" + (f" and {root_env}" if os.path.isfile(root_env) else ""))
    print()
    print("  You can now start the system:")
    print("  cd .. && ./start.sh")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
