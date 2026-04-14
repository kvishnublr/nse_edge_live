"""
STOCKR.IN v5 â€” Daily Token Generator
Run every morning before trading: python generate_token.py

Reads KITE_API_KEY and KITE_API_SECRET from backend/.env,
opens the Kite login page, accepts the redirect URL from you,
generates today's access token, and writes it back to backend/.env.
"""

import os
import sys
import re
import webbrowser
from urllib.parse import urlparse, parse_qs
from pathlib import Path

# â”€â”€ locate backend/.env relative to this script â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH   = SCRIPT_DIR / "backend" / ".env"


def load_env(path: Path) -> dict:
    """Parse key=value pairs from a .env file (no external deps)."""
    env = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            env[k.strip()] = v.strip()
    return env


def write_access_token(path: Path, new_token: str):
    """Replace (or append) the KITE_ACCESS_TOKEN line in backend/.env."""
    text = path.read_text(encoding="utf-8") if path.exists() else ""
    pattern = re.compile(r"^KITE_ACCESS_TOKEN\s*=.*$", re.MULTILINE)
    new_line = f"KITE_ACCESS_TOKEN={new_token}"

    if pattern.search(text):
        updated = pattern.sub(new_line, text)
    else:
        # Token line not present yet â€” append it
        updated = text.rstrip("\n") + f"\n{new_line}\n"

    path.write_text(updated, encoding="utf-8")


def main():
    print("=" * 55)
    print("  STOCKR.IN v5 â€” Kite Connect Daily Token Generator")
    print("=" * 55)

    # â”€â”€ 1. Load credentials â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if not ENV_PATH.exists():
        print(f"\nERROR: {ENV_PATH} not found.")
        print("Copy backend/.env.example to backend/.env and fill in your API key/secret.")
        sys.exit(1)

    env = load_env(ENV_PATH)
    api_key    = env.get("KITE_API_KEY", "").strip()
    api_secret = env.get("KITE_API_SECRET", "").strip()

    if not api_key or api_key in ("your_api_key_here", ""):
        print("\nERROR: KITE_API_KEY is missing or not set in backend/.env")
        sys.exit(1)
    if not api_secret or api_secret in ("your_api_secret_here", ""):
        print("\nERROR: KITE_API_SECRET is missing or not set in backend/.env")
        sys.exit(1)

    # â”€â”€ 2. Import kiteconnect â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        from kiteconnect import KiteConnect
    except ImportError:
        print("\nERROR: kiteconnect package not installed.")
        print("Run: pip install kiteconnect")
        sys.exit(1)

    kite = KiteConnect(api_key=api_key)

    # â”€â”€ 3. Open login URL in browser â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    login_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"
    print(f"\nOpening Kite login in your browser...")
    print(f"  URL: {login_url}\n")
    webbrowser.open(login_url)

    # â”€â”€ 4. Ask for the redirect URL â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("After logging in, Zerodha will redirect your browser to a URL like:")
    print("  https://127.0.0.1/?request_token=XXXX&action=login&status=success\n")
    print("Paste that FULL redirect URL here and press Enter:")
    redirect_url = input("  > ").strip()

    if not redirect_url:
        print("\nERROR: No URL entered. Aborting.")
        sys.exit(1)

    # â”€â”€ 5. Parse request_token â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    try:
        parsed = urlparse(redirect_url)
        params = parse_qs(parsed.query)
        request_token_list = params.get("request_token", [])
        if not request_token_list:
            # Try fragment as well (some setups use #)
            frag_params = parse_qs(parsed.fragment)
            request_token_list = frag_params.get("request_token", [])
        if not request_token_list:
            raise ValueError("request_token not found in URL")
        request_token = request_token_list[0]
    except Exception as e:
        print(f"\nERROR: Could not parse request_token from URL: {e}")
        print("Make sure you pasted the complete redirect URL.")
        sys.exit(1)

    print(f"\nParsed request_token: {request_token[:8]}...{request_token[-4:]}")

    # â”€â”€ 6. Generate access token â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    print("Generating access token from Kite API...")
    try:
        session = kite.generate_session(request_token, api_secret=api_secret)
        access_token = session["access_token"]
    except Exception as e:
        print(f"\nERROR: Failed to generate session: {e}")
        print("Common causes:")
        print("  - request_token already used (each token is single-use)")
        print("  - KITE_API_SECRET is wrong")
        print("  - Kite app redirect URL not configured to http://127.0.0.1/")
        sys.exit(1)

    # â”€â”€ 7. Write to backend/.env â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    write_access_token(ENV_PATH, access_token)

    print(f"\nAccess token written to {ENV_PATH}")
    print(f"  Token: {access_token[:8]}...{access_token[-4:]}")
    print("\nSTOCKR.IN is ready to start. Run:")
    print("  cd backend && python main.py")
    print("=" * 55)


if __name__ == "__main__":
    main()
