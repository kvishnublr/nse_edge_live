"""
NSE EDGE v5 — Zerodha Kite Connect Daily Token Generator
Run this script ONCE every morning before starting the system.

Usage:
    python3 generate_token.py

It will:
  1. Open the Kite login URL in your browser
  2. Ask you to paste the request_token from the redirect URL
  3. Generate the access_token
  4. Automatically update KITE_ACCESS_TOKEN in your .env file
"""

import os
import sys
import webbrowser

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
    print("  STEP 1: Login to Kite")
    print("=" * 60)
    print(f"\nOpening: {login_url}\n")
    webbrowser.open(login_url)

    print("After you log in, Kite will redirect you to a URL like:")
    print("  https://your-redirect-url/?request_token=XXXXX&action=login&status=success")
    print()

    request_token = input("Paste the request_token value here: ").strip()
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

    print("\n" + "=" * 60)
    print("  SUCCESS")
    print("=" * 60)
    print(f"  User     : {user_name} ({user_id})")
    print(f"  Token    : {access_token[:12]}...{access_token[-4:]}")
    print(f"  Saved to : {ENV_FILE}")
    print()
    print("  You can now start the system:")
    print("  cd .. && ./start.sh")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
