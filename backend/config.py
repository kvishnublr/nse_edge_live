"""
NSE EDGE v5 — Configuration (Zerodha Kite Connect only)
Edit backend/.env with your API key and daily access token.
"""

import os
import sys
import logging
import datetime
import pytz
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("config")

# ─── ENVIRONMENT VALIDATION ───────────────────────────────────────────────────
def _validate_config():
    """Validate all required configuration at startup."""
    errors = []

    # Required credentials
    if not KITE_API_KEY:
        errors.append("KITE_API_KEY missing in .env")
    if not KITE_API_SECRET:
        errors.append("KITE_API_SECRET missing in .env")
    if not KITE_ACCESS_TOKEN:
        errors.append("KITE_ACCESS_TOKEN missing in .env")

    # Port validation
    if not (1 <= PORT <= 65535):
        errors.append(f"Invalid PORT: {PORT} (must be 1-65535)")

    if errors:
        logger.error("Configuration validation failed:")
        for err in errors:
            logger.error(f"  - {err}")
        sys.exit(1)

# ─── KITE CONNECT ─────────────────────────────────────────────────────────────
KITE_API_KEY      = os.getenv("KITE_API_KEY", "").strip()
KITE_API_SECRET   = os.getenv("KITE_API_SECRET", "").strip()
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

# ─── SERVER ───────────────────────────────────────────────────────────────────
HOST = os.getenv("HOST", "0.0.0.0").strip()
try:
    PORT = int(os.getenv("PORT", "8765"))
except ValueError:
    logger.error(f"Invalid PORT value: {os.getenv('PORT')}")
    PORT = 8765

# ─── NSE HEADERS (for FII/DII — only endpoint not on Kite) ────────────────────
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
}
NSE_BASE    = "https://www.nseindia.com"
NSE_TIMEOUT = 10

# ─── INSTRUMENT TOKENS (Kite) ─────────────────────────────────────────────────
# These are permanent Kite instrument tokens — never change.
# Source: https://api.kite.trade/instruments (download once)
KITE_TOKENS = {
    "NIFTY":      256265,
    "BANKNIFTY":  260105,
    "INDIAVIX":   264969,
    "ICICIBANK":  1270529,
    "SBIN":       779521,
    "HDFCBANK":   341249,
    "AXISBANK":   1510401,
    "KOTAKBANK":  492033,
    "INDUSINDBK": 1346049,
    "LT":         2939649,
    "TCS":        2953217,
    "RELIANCE":   738561,
    "TATAMOTORS": 884737,
    "BAJFINANCE": 81153,
    "TATASTEEL":  857857,
    "INFY":       408065,
    "MARUTI":     2815745,
    "SUNPHARMA":  857857,
}
KITE_TOKEN_TO_SYMBOL = {v: k for k, v in KITE_TOKENS.items()}

# Kite exchange:symbol strings for quote()
KITE_QUOTE_KEYS = {
    "NIFTY":      "NSE:NIFTY 50",
    "BANKNIFTY":  "NSE:NIFTY BANK",
    "INDIAVIX":   "NSE:INDIA VIX",
    "ICICIBANK":  "NSE:ICICIBANK",
    "SBIN":       "NSE:SBIN",
    "HDFCBANK":   "NSE:HDFCBANK",
    "AXISBANK":   "NSE:AXISBANK",
    "KOTAKBANK":  "NSE:KOTAKBANK",
    "INDUSINDBK": "NSE:INDUSINDBK",
    "LT":         "NSE:LT",
    "TCS":        "NSE:TCS",
    "RELIANCE":   "NSE:RELIANCE",
    "TATAMOTORS": "NSE:TATAMOTORS",
    "BAJFINANCE": "NSE:BAJFINANCE",
    "TATASTEEL":  "NSE:TATASTEEL",
    "INFY":       "NSE:INFY",
    "MARUTI":     "NSE:MARUTI",
    "SUNPHARMA":  "NSE:SUNPHARMA",
}

# Lot sizes (NSE standard)
LOT_SIZES = {
    "NIFTY":      25,
    "BANKNIFTY":  15,
    "ICICIBANK":  1375,
    "SBIN":       3000,
    "HDFCBANK":   550,
    "AXISBANK":   1200,
    "KOTAKBANK":  400,
    "INDUSINDBK": 700,
    "LT":         150,
    "TCS":        300,
    "RELIANCE":   500,
    "TATAMOTORS": 1800,
    "BAJFINANCE": 125,
    "TATASTEEL":  5500,
    "INFY":       600,
    "MARUTI":     30,
    "SUNPHARMA":  350,
}

FNO_SYMBOLS = list(LOT_SIZES.keys())

# ─── UPDATE INTERVALS (seconds) ───────────────────────────────────────────────
INTERVAL_PRICES  = 1    # KiteTicker is real-time; REST quote fallback every 1s
INTERVAL_CHAIN   = 30   # option chain OI via kite.quote on NFO strikes
INTERVAL_STOCKS  = 30   # FnO stock OI refresh
INTERVAL_FII     = 300  # FII/DII from NSE (updates ~hourly anyway)
INTERVAL_SPIKES  = 10   # spike detection

# ─── GATE THRESHOLDS (tune based on your trading style) ───────────────────────
GATE = {
    # G1 — Regime
    "vix_low":           12.0,   # full size
    "vix_medium":        16.0,   # 75% size
    "vix_high":          20.0,   # 50% size / avoid
    # G2 — Smart Money
    "pcr_bullish":        1.2,
    "pcr_bearish":        0.8,
    # G4 — Trigger
    "vol_surge_min":      1.5,   # volume × average
    "oi_build_min":    5000,     # OI build contracts
    # G5 — Risk
    "rr_min_intraday":    2.0,
    "rr_min_positional":  3.0,
    "atr_multiplier":     1.5,
    # Spike detection
    "spike_price_pct":    1.5,
    "spike_vol_mult":     2.5,
    "spike_oi_pct":      12.0,
}

# ─── TELEGRAM ALERTS ──────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "").strip()

# ─── SESSION ZONES (IST minutes from midnight) ────────────────────────────────
ZONES = {
    "pre_open":  (555, 560),   # 9:15–9:20
    "discovery": (555, 600),   # 9:15–10:00
    "trend":     (600, 810),   # 10:00–13:30  ← best
    "drift":     (810, 870),   # 13:30–14:30
    "expiry":    (870, 930),   # 14:30–15:30
}

# ─── MARKET HOURS CHECK ───────────────────────────────────────────────────────
def is_market_open():
    """Check if NSE market is currently open (9:15 AM - 3:30 PM IST, weekdays only)."""
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.datetime.now(ist)

        # Check if weekday (Monday=0, Sunday=6)
        if now.weekday() >= 5:  # Saturday or Sunday
            return False

        # Market hours: 9:15 AM to 3:30 PM
        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

        return market_open <= now <= market_close
    except Exception as e:
        logger.warning(f"Error checking market hours: {e}")
        return True  # Assume open on error (safer default)

def get_market_status():
    """Get current market status for logging."""
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.datetime.now(ist)

        if now.weekday() >= 5:
            return "closed (weekend)"

        market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
        market_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

        if now < market_open:
            return f"pre-market (opens in {(market_open - now).seconds // 60} min)"
        elif now <= market_close:
            return f"OPEN (closes in {(market_close - now).seconds // 60} min)"
        else:
            return "closed (trading hours ended)"
    except Exception:
        return "open (unknown status)"

# Run validation at import
_validate_config()
