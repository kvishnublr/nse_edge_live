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

# Load .env from backend directory (next to this file)
# override=False means Railway/system env vars always take priority over .env file
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_env_path, override=False)
logger = logging.getLogger("config")

# Debug: log which env vars are present (helps diagnose Railway issues)
_has_api_key = bool(os.getenv("KITE_API_KEY", "").strip())
_has_token   = bool(os.getenv("KITE_ACCESS_TOKEN", "").strip())
print(f"[config] KITE_API_KEY present: {_has_api_key} | KITE_ACCESS_TOKEN present: {_has_token}", flush=True)

# ─── ENVIRONMENT VALIDATION ───────────────────────────────────────────────────
def _validate_config():
    """Validate all required configuration at startup."""
    errors = []

    # Required credentials — only API key/secret are hard required
    # KITE_ACCESS_TOKEN can be missing; auto_token will refresh it at startup
    if not KITE_API_KEY:
        errors.append("KITE_API_KEY missing — set in Railway Variables tab")
    if not KITE_API_SECRET:
        errors.append("KITE_API_SECRET missing — set in Railway Variables tab")

    # Port validation
    if not (1 <= PORT <= 65535):
        errors.append(f"Invalid PORT: {PORT} (must be 1-65535)")

    if errors:
        logger.error("Configuration validation failed:")
        for err in errors:
            logger.error(f"  - {err}")
        sys.exit(1)

    if not KITE_ACCESS_TOKEN:
        logger.warning("KITE_ACCESS_TOKEN not set — will auto-refresh at startup")

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
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "sec-ch-ua": '"Chromium";v="134", "Google Chrome";v="134", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
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
    "TATASTEEL":  895745,
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

# ─── GATE THRESHOLDS (80% WIN RATE TARGET) ───────────────────────
GATE = {
    # G1 — Regime (relaxed to match 2025 profitable regime)
    "vix_low":           11.0,   # full size below 11 (2025 was profitable)
    "vix_medium":        12.0,   # 25% size
    "vix_high":          15.0,   # BLOCK above 15
    # G2 — Smart Money (VERY strict PCR)
    "pcr_bullish":        1.25,   # strongly bullish
    "pcr_bearish":        0.45,
    # G4 — Trigger (strict)
    "vol_surge_min":      1.4,
    "oi_build_min":    6000,
    # G5 — Risk (much stricter R:R for 80% WR target)
    "rr_min_intraday":    3.0,
    "rr_min_positional":  3.5,
    "atr_multiplier":     1.5,
    # Spike detection - OPTIMIZED v2 (73% WR)
    "spike_price_pct":    0.2,    # Min price change %
    "spike_vol_mult":     1.5,    # Min volume multiplier
    "spike_oi_pct":       12.0,   # OI change %
    "spike_confirm_pct":  0.02,   # Confirmation: next candle must move in same direction
    "spike_time_start":   570,    # 9:30 AM (minutes from midnight)
    "spike_time_end":     840,    # 2:00 PM
}

# ─── INDEX RADAR (NIFTY/BANKNIFTY momentum — scheduler._detect_index_signals) ─
# Goals: fewer false starts, no overnight history bleed, true 5m baseline, no chasing extremes.
INDEX_RADAR = {
    "time_start_min":     600,    # 10:00 IST (skip opening noise)
    "time_end_min":       840,    # 14:00 IST
    "momentum_sec":       300,    # 5-minute primary window
    "confirm_sec":        60,     # 1-minute continuation check
    "trend_sec":          1800,   # 30-minute broader trend alignment
    "min_hist_span_sec":  270,    # need ≥4.5 min of samples before signalling
    "min_hist_samples":   6,      # at least N points (30s chain job → ~3 min min)
    "chg_min_pct":        0.20,   # min |5m move|
    "chg_max_pct":        0.30,   # max |5m move| (avoid exhaustion spikes)
    "chg_hi_strength_pct": 0.26,  # strength="hi" when |chg| at upper end of band
    "trend_against_pct":  0.30,   # skip if 30m trend strongly opposite
    "pcr_pe_min":         1.40,   # PE only when puts dominate (was 1.4 live)
    "pe_max_nifty_chg":   0.12,   # PE: Nifty vs prev close must be weak (≤ this %)
    "pcr_ce_avoid_below": 0.55,   # skip CE when PCR extremely bearish (optional guard)
    "vix_block_above":    18.0,    # no index radar signals if VIX at panic levels
    "anti_chase_sec":     180,    # lookback for local high/low
    "anti_chase_ce_pct":  0.12,   # skip long if px above recent max by more than this
    "anti_chase_pe_pct":  0.12,   # skip short if px below recent min by more than this
    "dedup_minutes":      20,     # same symbol + CE/PE
    "micro_step_min_pct": 0.01,   # last 30s–60s step must favor direction (noise floor)
    # ── Win-rate / selectivity (0 on *_min / *_pct / floor = feature off) ──
    "trend_support_min_pct": 0.07,   # CE: 30m trend must be ≥ this; PE: ≤ −this (needs 30m baseline)
    "pcr_ce_min":         1.05,     # CE only if PCR ≥ this (bullish OI skew); 0 = off
    "cross_index_against_pct": 0.18, # CE: other index 5m % must be > −this; PE: < +this; 0 = off
    "quality_floor":      60,       # drop signals with quality below this; 0 = off
    "vix_soft_skips_md_ce": 16.5,   # if VIX ≥ this, skip CE unless |5m chg| ≥ chg_hi_strength; 0 = off
    "outcome_index_pct":  0.25,     # underlying % for live HIT_T1 / HIT_SL (backtest matches)
    # ── High precision (fewer signals). ML needs ≥~50 resolved T1/SL rows in DB first. ──
    "precision_boost":    False,    # True: tighter chg band, hi-only, stronger trend + quality
    "precision_hi_only": True,
    "precision_min_quality": 74,
    "precision_min_trend_sup": 0.11,
    "precision_chg_min":  0.23,
    "precision_chg_max":  0.28,
    "ml_filter_enabled":  False,   # True: keep signal only if GB model P(win) ≥ threshold
    "ml_min_win_prob":    0.72,    # fallback if no ix_radar_ml_meta.json from training
}

# ─── TELEGRAM ALERTS ──────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID",   "").strip()

# ─── WHATSAPP ALERTS (CallMeBot — free) ───────────────────────────────────────
WHATSAPP_PHONE  = os.getenv("WHATSAPP_PHONE",  "").strip()
WHATSAPP_APIKEY = os.getenv("WHATSAPP_APIKEY", "").strip()

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
