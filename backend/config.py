"""
STOCKR.IN v5 â€” Configuration (Zerodha Kite Connect only)
Edit backend/.env with your API key and daily access token.
"""

import os
import sys
import logging
import datetime
import pytz
from dotenv import load_dotenv

# Load .env from backend directory (next to this file)
# override=False: normal deploys keep host env winning.
# Windows often has KITE_ACCESS_TOKEN="" in User env â€” that blocks .env; second pass fixes it.
_env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(_env_path, override=False)
if not os.getenv("KITE_ACCESS_TOKEN", "").strip() or not os.getenv("KITE_API_KEY", "").strip():
    load_dotenv(_env_path, override=True)
logger = logging.getLogger("config")

# Debug: log which env vars are present (helps diagnose missing credentials on deploy)
_has_api_key = bool(os.getenv("KITE_API_KEY", "").strip())
_has_token   = bool(os.getenv("KITE_ACCESS_TOKEN", "").strip())
print(f"[config] KITE_API_KEY present: {_has_api_key} | KITE_ACCESS_TOKEN present: {_has_token}", flush=True)

# â”€â”€â”€ ENVIRONMENT VALIDATION â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _validate_config():
    """Validate all required configuration at startup."""
    errors = []

    # Required credentials â€” only API key/secret are hard required
    # KITE_ACCESS_TOKEN can be missing; auto_token will refresh it at startup
    if not KITE_API_KEY:
        errors.append("KITE_API_KEY missing â€” set in environment or backend/.env")
    if not KITE_API_SECRET:
        errors.append("KITE_API_SECRET missing â€” set in environment or backend/.env")

    # Port validation
    if not (1 <= PORT <= 65535):
        errors.append(f"Invalid PORT: {PORT} (must be 1-65535)")

    if errors:
        logger.error("Configuration validation failed:")
        for err in errors:
            logger.error(f"  - {err}")
        sys.exit(1)

    if not KITE_ACCESS_TOKEN:
        logger.warning("KITE_ACCESS_TOKEN not set â€” will auto-refresh at startup")

# â”€â”€â”€ KITE CONNECT â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
KITE_API_KEY      = os.getenv("KITE_API_KEY", "").strip()
KITE_API_SECRET   = os.getenv("KITE_API_SECRET", "").strip()
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN", "").strip()

# â”€â”€â”€ SERVER â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
HOST = os.getenv("HOST", "0.0.0.0").strip()


def _resolve_listen_port() -> int:
    """
    Listen port: PORT first (cloud / uvicorn convention, usually 8000), then
    NSE_EDGE_PORT for a deliberate local override. Default 8000 so
    http://localhost:8000/ matches typical bookmarks.
    """
    for key, raw in (("PORT", os.getenv("PORT", "").strip()),
                     ("NSE_EDGE_PORT", os.getenv("NSE_EDGE_PORT", "").strip())):
        if not raw:
            continue
        try:
            p = int(raw)
            if 1 <= p <= 65535:
                if key == "NSE_EDGE_PORT":
                    logger.info("Using NSE_EDGE_PORT=%s for listen port", p)
                return p
        except ValueError:
            logger.error("Invalid %s value: %r", key, raw)
    return 8000


PORT = _resolve_listen_port()

# â”€â”€â”€ NSE HEADERS (for FII/DII â€” only endpoint not on Kite) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
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

# â”€â”€â”€ INSTRUMENT TOKENS (Kite) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# These are permanent Kite instrument tokens â€” never change.
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

# â”€â”€â”€ UPDATE INTERVALS (seconds) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
INTERVAL_PRICES  = 1    # KiteTicker is real-time; REST quote fallback every 1s
INTERVAL_CHAIN   = 30   # option chain OI via kite.quote on NFO strikes
INTERVAL_STOCKS  = 30   # FnO stock OI refresh
INTERVAL_FII     = 300  # FII/DII from NSE (updates ~hourly anyway)
INTERVAL_SPIKES  = 10   # spike detection

# â”€â”€â”€ TRADING ACCOUNT PARAMETERS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Set ACCOUNT_VALUE in .env (e.g. ACCOUNT_VALUE=500000) â€” used for Gate 5 position sizing.
ACCOUNT_VALUE     = int(os.getenv("ACCOUNT_VALUE", "500000"))
RISK_PER_TRADE    = float(os.getenv("RISK_PER_TRADE", "0.01"))   # fraction, e.g. 0.01 = 1%

# â”€â”€â”€ GATE THRESHOLDS (80% WIN RATE TARGET) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
GATE = {
    # G1 â€” Regime (relaxed to match 2025 profitable regime)
    "vix_low":           11.0,   # full size below 11 (2025 was profitable)
    "vix_medium":        12.0,   # 25% size
    "vix_high":          15.0,   # BLOCK above 15
    # G2 â€” Smart Money (VERY strict PCR)
    "pcr_bullish":        1.25,   # strongly bullish
    "pcr_bearish":        0.45,
    # G4 â€” Trigger (strict)
    "vol_surge_min":      1.4,
    "oi_build_min":    6000,
    # G5 â€” Risk (much stricter R:R for 80% WR target)
    "rr_min_intraday":    3.0,
    "rr_min_positional":  3.5,
    "atr_multiplier":     1.5,
    # Dayview / STOCK PICKS positional backtest: walk forward this many *sessions*
    # (daily bars after entry); time-stop on last bar with WIN/LOSS/NEUTRAL vs entry.
    "positional_max_hold_days": 10,
    # Spike detection - OPTIMIZED v2 (73% WR)
    "spike_price_pct":    0.55,   # Quick-move focus: minimum session move %
    "spike_vol_mult":     1.6,    # Quick-move focus: minimum volume multiplier
    "spike_oi_pct":       12.0,   # OI change %
    "spike_confirm_pct":  0.02,   # Confirmation: next candle must move in same direction
    "spike_time_start":   570,    # 9:30 AM defensive start (avoid open noise whipsaws)
    "spike_time_end":     840,    # 2:00 PM
    "spike_min_gate_pass": 0,     # Gate-independent mode (quick-move scanner)
    "spike_adaptive_gate": 0,     # 0=False: ignore gate floor adaptation
    "spike_min_gate_pass_relaxed": 0, # not used when adaptive gate is disabled
    "spike_vix_gate3_above": 20.0, # if VIX above this, enforce strict gate floor
    "spike_regime_kill_switch": 0, # gate-independent: keep scanner active across regimes
    "spike_max_per_cycle": 6,      # cap spike count each scan; keep only top quality
    "spike_telegram_dedup_minutes": 20,  # same symbol+direction: one TG/WA alert per window
    "spike_active_only": 0,        # 0=False: include full scanner universe
    "spike_active_symbols": "HDFCBANK,ICICIBANK,AXISBANK,SBIN,RELIANCE,INFY,TCS,LT,BAJFINANCE,TATAMOTORS,MARUTI,INDUSINDBK,KOTAKBANK",
    "spike_universe": "NIFTY200",  # NIFTY200 | FNO
    "spike_allow_open_relax": 0,  # 0/False = disable permissive early-session shortcut
    "spike_min_confirm_move": 0.70, # If no OI confirmation, require stronger directional move
    "spike_confirm_non_oi_requires_vol": 1, # 1=True: require both move and volume for non-OI spikes
    "spike_confirm_min_vm": 1.8,   # minimum vol multiplier when non-OI strict confirm is enabled
    "spike_live_early_fail_min": 8,          # live outcome: close fast if move goes adverse early
    "spike_live_early_fail_adverse_pct": 0.18,
    "spike_live_no_ft_min": 15,              # live outcome: no-follow-through timeout
    "spike_live_no_ft_min_fav_pct": 0.12,
}

# â”€â”€â”€ INDEX HUNT / INDEX RADAR (NIFTY/BANKNIFTY â€” scheduler._detect_index_signals) â”€
# UI name: INDEX HUNT. Same dict INDEX_RADAR everywhere (API, DB section INDEX_RADAR).
# Goals: fewer false starts, true 5m baseline, no chasing; hunt_* adds 15m trend leg (SPIKE-style confluence).
INDEX_RADAR = {
    "time_start_min":     600,    # 10:00 IST (skip opening noise)
    "time_end_min":       840,    # 14:00 IST
    "momentum_sec":       300,    # 5-minute primary window
    "confirm_sec":        60,     # 1-minute continuation check
    "trend_sec":          1800,   # 30-minute broader trend alignment
    "min_hist_span_sec":  270,    # need â‰¥4.5 min of samples before signalling
    "min_hist_samples":   6,      # at least N points (30s chain job â†’ ~3 min min)
    "chg_min_pct":        0.14,   # min |5m move| (relaxed for low-to-mid impulse days)
    "chg_max_pct":        0.45,   # max |5m move| (allow stronger intraday impulses)
    "chg_hi_strength_pct": 0.24,  # strength="hi" when |chg| at upper end of band
    "trend_against_pct":  0.30,   # skip if 30m trend strongly opposite
    "pcr_pe_min":         1.20,   # PE bias threshold (relaxed)
    "pe_max_nifty_chg":   0.12,   # PE: Nifty vs prev close must be weak (â‰¤ this %)
    "pcr_ce_avoid_below": 0.55,   # skip CE when PCR extremely bearish (optional guard)
    "vix_block_above":    28.0,    # hard block only in extreme panic regimes
    "anti_chase_sec":     180,    # lookback for local high/low
    "anti_chase_ce_pct":  0.12,   # skip long if px above recent max by more than this
    "anti_chase_pe_pct":  0.12,   # skip short if px below recent min by more than this
    "dedup_minutes":      12,     # same symbol + CE/PE
    "micro_step_min_pct": 0.01,   # last 30sâ€“60s step must favor direction (noise floor)
    # â”€â”€ Win-rate / selectivity (0 on *_min / *_pct / floor = feature off) â”€â”€
    "trend_support_min_pct": 0.03,   # CE: 30m trend must be â‰¥ this; PE: â‰¤ âˆ’this
    "pcr_ce_min":         0.95,     # CE minimum PCR (relaxed)
    "cross_index_against_pct": 0.25, # opposite-index tolerance (relaxed)
    # â”€â”€ INDEX HUNT confluence (15m same-direction leg â€” live + backtest) â”€â”€
    "hunt_15m_sec":       900,      # 0 = off; else require 15m return same sign as 5m signal
    "hunt_15m_min_pct":   0.042,    # min |15m %| on underlying (same units as chg_pct; ~0.04 index %)
    "quality_floor":      52,       # drop only lower-quality noise
    "vix_soft_skips_md_ce": 22.0,   # soft CE skip threshold under elevated VIX
    "outcome_index_pct":  0.25,     # underlying % when T1/SL use same threshold (backtest + optional index fallback)
    "outcome_t1_index_pct": None,   # None â†’ use outcome_index_pct; smaller = easier T1 (higher WR, less R)
    "outcome_sl_index_pct": None,   # None â†’ use outcome_index_pct; can be > T1 so SL is slightly less twitchy
    # Live outcomes: False = only real option LTP vs T1/SL (no synthetic "win" from index % alone).
    "outcome_use_index_fallback": False,
    "opt_sl_mult":        0.70,    # option premium SL = entry Ã— this (live plan)
    "opt_t1_mult":        1.50,    # T1 = entry Ã— this (smaller â†’ easier hit, lower â‚¹ target)
    "opt_t2_mult":        2.00,
    # â”€â”€ Strike/premium guardrails (live index radar option pick) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Keep fills near liquid strikes; avoid deep ITM and very high-premium legs
    # where bid/ask spread slippage is materially worse.
    "strike_otm_steps":   0,       # 0=ATM, 1=one-step OTM
    "option_entry_min":   35.0,    # skip illiquid tiny-premium noise
    "option_entry_max":   500.0,   # cap expensive legs to reduce spread/slippage risk
    # â”€â”€ High precision (fewer signals). ML needs â‰¥~50 resolved T1/SL rows in DB first. â”€â”€
    "precision_boost":    False,    # True: tighter chg band, hi-only, stronger trend + quality
    "precision_hi_only": True,
    "precision_min_quality": 74,
    "precision_min_trend_sup": 0.11,
    "precision_chg_min":  0.23,
    "precision_chg_max":  0.28,
    "ml_filter_enabled":  False,   # True: keep signal only if GB model P(win) â‰¥ threshold
    "ml_min_win_prob":    0.72,    # fallback if no ix_radar_ml_meta.json from training
    # â”€â”€ Independent 70%+ filters (precision_v2) â”€â”€
    # session_open_lock: CE only when price > first bar close; PE only when below.
    # confirm_bars_n: require N consecutive 1-min closes in signal direction.
    # sl_memory_min: skip re-entry if same direction SL'd within N minutes.
    "session_open_lock":  False,
    "confirm_bars_n":     1,
    "sl_memory_min":      0,
}

# Applied on top of _INDEX_RADAR_BASE for preset=high_accuracy and profile index_precision.
# Accuracy-first: tighter gates, smaller premium T1, easier index T1 vs SL (higher WR target, lower R).
# outcome_use_hl: backtest uses bar high/low touches (T1 before SL per bar, same order as close loop).
# Not merged onto balanced_v2 in backtest (see main.index_signals_backtest).
INDEX_RADAR_HIGH_ACCURACY: dict = {
    "precision_boost": True,
    "precision_hi_only": True,
    "precision_chg_min": 0.22,
    "precision_chg_max": 0.33,
    "precision_min_quality": 78,
    "precision_min_trend_sup": 0.12,
    "quality_floor": 66,
    "trend_support_min_pct": 0.09,
    "trend_against_pct": 0.18,
    "pcr_ce_min": 1.03,
    "pcr_pe_min": 1.30,
    "pe_max_nifty_chg": 0.08,
    "cross_index_against_pct": 0.11,
    "hunt_15m_min_pct": 0.055,
    "anti_chase_ce_pct": 0.07,
    "anti_chase_pe_pct": 0.07,
    "dedup_minutes": 26,
    "chg_min_pct": 0.20,
    "chg_max_pct": 0.36,
    "chg_hi_strength_pct": 0.24,
    "micro_step_min_pct": 0.014,
    "vix_soft_skips_md_ce": 18.0,
    "vix_skip_ce_above": 23.0,
    "outcome_use_hl": True,
    "opt_sl_mult": 0.76,
    "opt_t1_mult": 1.18,
    "opt_t2_mult": 1.52,
    "outcome_t1_index_pct": 0.10,
    "outcome_sl_index_pct": 0.24,
    # Enforce liquid, non-deep contracts in stricter profile too.
    "strike_otm_steps": 0,
    "option_entry_min": 35.0,
    "option_entry_max": 500.0,
}

# Extra overlay for preset=elite (backtest + optional live profile index_elite). Targets fewer, higher-WR signals.
INDEX_RADAR_ELITE: dict = {
    "precision_min_quality": 82,
    "quality_floor": 72,
    "precision_chg_min": 0.24,
    "precision_chg_max": 0.31,
    "precision_min_trend_sup": 0.135,
    "trend_support_min_pct": 0.11,
    "pcr_ce_min": 1.06,
    "pcr_pe_min": 1.34,
    "pe_max_nifty_chg": 0.06,
    "cross_index_against_pct": 0.08,
    "hunt_15m_min_pct": 0.068,
    "dedup_minutes": 32,
    "vix_skip_ce_above": 21.5,
    "vix_soft_skips_md_ce": 17.5,
    "outcome_t1_index_pct": 0.09,
    "outcome_sl_index_pct": 0.27,
    "opt_t1_mult": 1.15,
    "opt_sl_mult": 0.78,
}

# â”€â”€â”€ PRECISION V2: ranked daily pick + session lock + ML when model file exists â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# Targets high WR with ~1â€“3 signals per symbol per session day (more on strong trend days).
# ML: if ix_radar_gb.joblib missing, filter is skipped (all candidates kept).
INDEX_RADAR_PRECISION_V2: dict = {
    # Base from high_accuracy
    **INDEX_RADAR_HIGH_ACCURACY,
    # â”€â”€ Ranked daily pick (backtest): keep best 1â€“3 per symbol/day, time-spaced â”€â”€
    "daily_pick_enabled":       True,
    "daily_pick_min_per_symbol": 1,
    "daily_pick_max_per_symbol": 3,
    "daily_pick_gap_minutes":   36,
    "daily_pick_use_ml_score":  True,
    # â”€â”€ Time window: skip opening noise AND late-day theta / thin liquidity â”€â”€
    "time_start_min":  615,     # 10:15 IST â€” skip open auction; still room for 1â€“2 quality signals/day
    "time_end_min":    825,     # 13:45 IST â€” before worst theta bleed
    # â”€â”€ Session-open direction lock (single biggest WR driver) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "session_open_lock": True,  # CE only above session open, PE only below
    # â”€â”€ Consecutive-bar confirmation (1 = prior 1m rule only; 2 = stricter) â”€
    "confirm_bars_n":  1,
    # â”€â”€ Momentum band: tighter sweet spot â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "precision_boost":     True,
    "precision_hi_only":   False,  # pool all band-qualified bars; daily_pick + score favor "hi"
    # 5m % on spot is usually small; wide band + ranking â‰ˆ "best impulses of the day"
    "precision_chg_min":   0.055,
    "precision_chg_max":   0.60,
    "chg_hi_strength_pct": 0.20,
    "chg_min_pct":         0.055,
    "chg_max_pct":         0.65,
    # â”€â”€ Trend strength (30m) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "precision_min_trend_sup": 0.14,
    "trend_support_min_pct":   0.10,
    "trend_against_pct":       0.16,
    # â”€â”€ PCR gates â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "pcr_ce_min":   1.02,
    "pcr_pe_min":   1.32,
    "pe_max_nifty_chg": 0.06,
    # â”€â”€ VIX gates (20â€“21 IV is common; 19 blocked too many valid CEs) â”€â”€â”€â”€â”€â”€â”€
    "vix_skip_ce_above":    20.75,
    "vix_soft_skips_md_ce": 16.5,
    # â”€â”€ Anti-chase (tighter) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "anti_chase_ce_pct": 0.06,
    "anti_chase_pe_pct": 0.06,
    # â”€â”€ 15-min hunt confluence â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "hunt_15m_sec":    900,
    "hunt_15m_min_pct": 0.045,
    # â”€â”€ Signal dedup (no back-to-back signals on same side) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "dedup_minutes":   30,
    # â”€â”€ Quality floor â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "quality_floor":          68,
    "precision_min_quality":  76,
    # â”€â”€ Cross-index alignment â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "cross_index_against_pct": 0.09,
    # â”€â”€ R:R improvement: T1=1.28Ã— (28% gain), SL=0.80Ã— (20% loss) â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # Break-even WR = 20/(28+20) = 41.7% â†’ 70% WR gives strong edge
    "opt_t1_mult":  1.28,
    "opt_sl_mult":  0.80,
    "opt_t2_mult":  1.65,
    # â”€â”€ Backtest outcome thresholds (underlying %) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "outcome_t1_index_pct": 0.11,   # T1 hit when NIFTY/BNF moves 0.11%
    "outcome_sl_index_pct": 0.26,   # SL hit when moves 0.26% against
    "outcome_use_hl":       True,
    # â”€â”€ Micro-step: last 1-min bar must accelerate â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "micro_step_min_pct": 0.016,
    # â”€â”€ ML: hard-filter when bundle exists; ranking still uses proba for daily_pick_use_ml_score
    "ml_filter_enabled":   True,
    "ml_min_win_prob":     0.68,
}

# â”€â”€â”€ PRIME STRIKE CONFIG â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# 3-layer confirmed intraday option-buying strategy.
# Layer 1: VIX hard gate (â‰¤17 calm market), Layer 2: PCR alignment,
# Layer 3: 5m momentum (calibrated for calm-VIX days).
# Daily-pick cap (1â€“3 per symbol), window 10:00â€“13:00, session lock,
# quality tier lot sizing: qualityâ‰¥80=FULL(2Ã—), qualityâ‰¥64=HALF(1Ã—).
# T1=1.30Ã—, SL=0.78Ã—  â†’  break-even WR â‰ˆ 20.5%  â†’  60% WR = strong edge.
#
# CALIBRATION NOTE:
# On VIXâ‰¤17 (calm) days, Nifty@24000 typical 5-min move = 0.03â€“0.12%.
# Setting chg_min_pct=0.09 means ~21pt move on NIFTY â€” achievable even in quiet sessions.
# No precision_boost â€” rely on quality floor, PCR, trend, session_lock for selectivity.
PRIME_STRIKE_CONFIG: dict = {
    # â”€â”€ Base from high_accuracy (keeps opt_mult, outcome params) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    **INDEX_RADAR_HIGH_ACCURACY,
    # â”€â”€ Time window: 10:00â€“13:00 (3 quality windows) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "time_start_min": 600,   # 10:00 IST
    "time_end_min":   780,   # 13:00 IST
    # â”€â”€ VIX gate: hard block only at day level â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "vix_block_above":      17.0,   # hard kill entire day if VIX â‰¥ 17
    "vix_skip_ce_above":     0.0,   # disabled â€” day-level block already handles it
    "vix_soft_skips_md_ce": 16.8,  # soft-skip medium CE only very close to hard limit
    # â”€â”€ PCR alignment (realistic PCR range: 0.65â€“1.25 in DB) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "pcr_ce_min":        0.78,   # CE: needs moderate bull PCR
    "pcr_pe_min":        1.05,   # PE: moderate put bias (achievable most weeks)
    "pe_max_nifty_chg":  0.10,   # PE: Nifty vs prev-close â‰¤ 0.10%
    "pcr_ce_avoid_below": 0.60,  # hard skip CE only in extreme bear PCR
    # â”€â”€ Momentum band: calibrated for calm-VIX sessions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    # 0.09% on Nifty@24000 = ~22 pts/5 min â€” present even on quiet days
    # 0.40% = ~96 pts/5 min â€” excludes panic/spike outliers
    "precision_boost":    False,   # OFF â€” band below is authoritative
    "precision_hi_only":  False,
    "precision_chg_min":  0.0,
    "precision_chg_max":  0.0,
    "chg_min_pct":         0.09,
    "chg_max_pct":         0.40,
    "chg_hi_strength_pct": 0.20,
    # â”€â”€ Direction lock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "session_open_lock": True,
    "confirm_bars_n":    1,
    # â”€â”€ Trend filter (30-min) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "trend_support_min_pct": 0.03,
    "trend_against_pct":     0.20,
    # â”€â”€ 15-min confluence (10-min lookback, lower threshold) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "hunt_15m_sec":     600,    # 10-min lookback (achievable in calm sessions)
    "hunt_15m_min_pct": 0.022,  # ~5pt on NIFTY@24000 in 10 min
    # â”€â”€ Anti-chase â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "anti_chase_ce_pct": 0.09,
    "anti_chase_pe_pct": 0.09,
    "anti_chase_sec":    300,
    # â”€â”€ Cross-index alignment â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "cross_index_against_pct": 0.12,
    # â”€â”€ Micro-step â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "micro_step_min_pct": 0.010,
    # â”€â”€ Quality floor (main selectivity lever without precision_boost) â”€â”€â”€â”€â”€â”€â”€â”€
    "quality_floor":         56,
    "precision_min_quality":  0,   # disabled (precision_boost=False)
    # â”€â”€ Dedup â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "dedup_minutes": 30,
    # â”€â”€ Daily pick (best-ranked signals, time-spaced â‰¥40 min) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "daily_pick_enabled":       True,
    "daily_pick_min_per_symbol": 1,
    "daily_pick_max_per_symbol": 3,
    "daily_pick_gap_minutes":   40,
    "daily_pick_use_ml_score":  True,
    # â”€â”€ R:R â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "opt_t1_mult": 1.30,
    "opt_sl_mult": 0.78,
    "opt_t2_mult": 1.70,
    # â”€â”€ Outcome simulation â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "outcome_t1_index_pct": 0.10,
    "outcome_sl_index_pct": 0.24,
    "outcome_use_hl":       True,
    # â”€â”€ ML filter: OFF (insufficient training data) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "ml_filter_enabled": False,
    "ml_min_win_prob":   0.65,
    # â”€â”€ Daily limits â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "max_signals_per_day": 3,
    "max_consec_sl":       2,
    # â”€â”€ Tier thresholds â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    "tier_full_min_quality": 80,
    "tier_half_min_quality": 64,
}

# â”€â”€â”€ SWING RADAR (positional picks â€” scheduler â†’ log_swing_radar_triggers + UI carousel) â”€
# Tighter filters = fewer logs but higher alignment with index / PCR / R:R / weak-symbol stats.
SWING_RADAR = {
    "min_score_log": 58,              # persist floor (after weak-symbol penalty)
    "min_pc_log": 0,                  # neutral by default; stricter profiles raise this
    "min_rr": 1.72,                   # ATR template ~2.0; allow normal float noise
    "min_abs_chg_pct": 0.0,           # neutral by default
    "max_abs_chg_pct": 0.0,           # 0 = disabled
    "min_vol_ratio": 0.0,             # neutral by default
    "allowed_setups": [],             # e.g. ["Pullback"] for accuracy-first mode
    "allowed_directions": [],         # e.g. ["SHORT"] for accuracy-first mode
    "vix_strict_above": 22.0,
    "vix_extra_min_score": 3,         # was 5 â€” less empty on high-VIX days
    "nifty_against_threshold": 0.45,
    "counter_trend_min_pc": 4,
    "rs_long_min_vs_nifty": 0.06,
    "rs_short_max_vs_nifty": -0.06,
    "pcr_soft_long_min": 0.82,
    "pcr_soft_short_max": 1.22,
    "pcr_soft_min_pc": 4,
    "vol_breakout_min": 1.02,
    "vol_breakout_min_vix": 1.10,
    "oi_long_breakout_max_neg": -7.0,
    "no_trade_bypass_min_score": 76,
    "no_trade_bypass_min_pc": 3,
    "weak_symbol_penalty": 8,
    "recovery_vol_min": 1.0,
    # Precision controls: setup-aware filters for accuracy-first swing profiles.
    "breakout_long_enabled": True,
    "breakout_long_min_pc": 0,
    "breakout_long_min_rs": -9.0,
    "breakout_long_min_vol": 0.0,
    "breakout_long_max_chg": 99.0,
    "pullback_short_only": False,
    "pullback_short_min_pc": 0,
    "pullback_short_min_rank_score": 0,
    "blocked_symbols": [],
}

# â”€â”€â”€ STRATEGY PROFILES (runtime switchable) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_INDEX_RADAR_BASE = dict(INDEX_RADAR)
_SWING_RADAR_BASE = dict(SWING_RADAR)
STRATEGY_PROFILES = {
    "legacy": {
        "index_radar": {},
        "swing_radar": {},
    },
    "balanced_v2": {
        "index_radar": {
            "dedup_minutes": 18,
            "quality_floor": 60,
            "trend_support_min_pct": 0.065,
            "anti_chase_ce_pct": 0.10,
            "anti_chase_pe_pct": 0.10,
            "chg_min_pct": 0.18,
            "chg_hi_strength_pct": 0.26,
            "pcr_ce_min": 0.96,
            "hunt_15m_sec": 900,
            "hunt_15m_min_pct": 0.045,
        },
        "swing_radar": {
            "min_score_log": 62,
            "min_rr": 1.90,
            "counter_trend_min_pc": 5,
            "pcr_soft_min_pc": 5,
            "vol_breakout_min": 1.05,
        },
    },
    # Live INDEX_RADAR matches INDEX_RADAR_HIGH_ACCURACY backtest preset (high selectivity).
    "index_precision": {
        "index_radar": dict(INDEX_RADAR_HIGH_ACCURACY),
        "swing_radar": {},
    },
    "index_elite": {
        "index_radar": {**dict(INDEX_RADAR_HIGH_ACCURACY), **INDEX_RADAR_ELITE},
        "swing_radar": {},
    },
    # Independent 70%+ strategy â€” no daily gate verdict required.
    # Fewer signals (~25-35 per month), but targets â‰¥70% win rate.
    "precision_v2": {
        "index_radar": dict(INDEX_RADAR_PRECISION_V2),
        "swing_radar": {},
    },
    "precision_v3": {
        "index_radar": dict(INDEX_RADAR_PRECISION_V2),
        "swing_radar": {
            # Accuracy-first positional profile: fewer signals, cleaner setups.
            "min_score_log": 70,
            "min_pc_log": 4,
            "min_rr": 1.95,
            "min_abs_chg_pct": 0.35,
            "max_abs_chg_pct": 2.80,
            "min_vol_ratio": 1.05,
            "counter_trend_min_pc": 5,
            "pcr_soft_min_pc": 5,
            "allowed_setups": ["Pullback"],
            "allowed_directions": ["SHORT"],
            "pullback_short_only": True,
            "pullback_short_min_pc": 4,
            "pullback_short_min_rank_score": 72,
            "breakout_long_enabled": False,
            "breakout_long_min_pc": 4,
            "breakout_long_min_rs": 0.20,
            "breakout_long_min_vol": 1.50,
            "breakout_long_max_chg": 2.5,
            "blocked_symbols": ["BAJFINANCE", "TATASTEEL", "INDUSINDBK", "LT", "MARUTI", "SUNPHARMA", "TATAMOTORS"],
        },
    },
}
_ACTIVE_STRATEGY_PROFILE = ""


def apply_strategy_profile(name: str | None) -> str:
    """Apply strategy overrides in-place so live imports see updated values."""
    global _ACTIVE_STRATEGY_PROFILE
    key = str(name or "").strip().lower() or "precision_v3"
    if key not in STRATEGY_PROFILES:
        key = "precision_v3"
    profile = STRATEGY_PROFILES.get(key, {})
    INDEX_RADAR.clear()
    INDEX_RADAR.update(_INDEX_RADAR_BASE)
    INDEX_RADAR.update(profile.get("index_radar", {}))
    SWING_RADAR.clear()
    SWING_RADAR.update(_SWING_RADAR_BASE)
    SWING_RADAR.update(profile.get("swing_radar", {}))
    _ACTIVE_STRATEGY_PROFILE = key
    return key


def get_strategy_profile_name() -> str:
    return _ACTIVE_STRATEGY_PROFILE or "precision_v3"


def get_strategy_profiles() -> list[str]:
    return list(STRATEGY_PROFILES.keys())


# Apply default profile at import-time so all workers/scripts use consistent rules.
_DEFAULT_STRATEGY_PROFILE = (os.getenv("STRATEGY_PROFILE", "precision_v3") or "precision_v3").strip().lower()
try:
    apply_strategy_profile(_DEFAULT_STRATEGY_PROFILE)
except Exception:
    apply_strategy_profile("precision_v3")

# â”€â”€â”€ TELEGRAM ALERTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
# Extra recipients: comma-separated chat IDs (e.g. second account / @harshvtrade numeric id after /start bot)
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS", "").strip()
TELEGRAM_CHAT_ID_HARSHVTRADE = os.getenv("TELEGRAM_CHAT_ID_HARSHVTRADE", "").strip()

_neg = ("0", "false", "no", "off")


def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key, "").strip().lower()
    if not v:
        return default
    return v not in _neg


# Default on: SPIKE HUNT (NIFTY 200 spike TG) + ADV INDEX (on DB persist / bias throttle)
TELEGRAM_NOTIFY_ADV_SPIKES = _env_bool("TELEGRAM_NOTIFY_ADV_SPIKES", True)
TELEGRAM_NOTIFY_ADV_INDEX = _env_bool("TELEGRAM_NOTIFY_ADV_INDEX", True)
# Default off: signal-engine verdict, per-stock EXECUTE, index-radar option signals, morning digest

def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("Invalid %s=%r â€” using default %s", key, raw, default)
        return default


# ADV-IDX-OPTIONS: India VIX band for IV *proxy* rank (not true 52w IV Rank)
IV_RANK_VIX_LOW = _env_float("IV_RANK_VIX_LOW", 11.0)
IV_RANK_VIX_HIGH = _env_float("IV_RANK_VIX_HIGH", 28.0)
TELEGRAM_NOTIFY_SIGNAL_ENGINE = _env_bool("TELEGRAM_NOTIFY_SIGNAL_ENGINE", False)
TELEGRAM_NOTIFY_INDEX_RADAR = _env_bool("TELEGRAM_NOTIFY_INDEX_RADAR", False)
_morning_brief = os.getenv("MORNING_TELEGRAM_BRIEF", "0").strip().lower()
MORNING_TELEGRAM_BRIEF = _morning_brief not in _neg


def get_telegram_chat_ids() -> list[str]:
    """Distinct non-empty chat_id strings for sendMessage (numeric or @public_channel)."""
    raw: list[str] = []
    if TELEGRAM_CHAT_ID:
        raw.append(TELEGRAM_CHAT_ID)
    if TELEGRAM_CHAT_ID_HARSHVTRADE:
        raw.append(TELEGRAM_CHAT_ID_HARSHVTRADE)
    for part in TELEGRAM_CHAT_IDS.split(","):
        p = part.strip()
        if p:
            raw.append(p)
    seen: set[str] = set()
    out: list[str] = []
    for x in raw:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out

# â”€â”€â”€ WHATSAPP ALERTS (CallMeBot â€” free) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
WHATSAPP_PHONE  = os.getenv("WHATSAPP_PHONE",  "").strip()
WHATSAPP_APIKEY = os.getenv("WHATSAPP_APIKEY", "").strip()

# â”€â”€â”€ SESSION ZONES (IST minutes from midnight) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
ZONES = {
    "pre_open":  (555, 560),   # 9:15â€“9:20
    "discovery": (555, 600),   # 9:15â€“10:00
    "trend":     (600, 810),   # 10:00â€“13:30  â† best
    "drift":     (810, 870),   # 13:30â€“14:30
    "expiry":    (870, 930),   # 14:30â€“15:30
}

# â”€â”€â”€ MARKET HOURS CHECK â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
NSE_TRADING_HOLIDAYS = {
    "2025-02-26": "Mahashivratri",
    "2025-03-14": "Holi",
    "2025-03-31": "Id-Ul-Fitr (Ramadan Eid)",
    "2025-04-10": "Shri Mahavir Jayanti",
    "2025-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2025-04-18": "Good Friday",
    "2025-05-01": "Maharashtra Day",
    "2025-08-15": "Independence Day",
    "2025-08-27": "Ganesh Chaturthi",
    "2025-10-02": "Mahatma Gandhi Jayanti/Dussehra",
    "2025-10-21": "Diwali Laxmi Pujan",
    "2025-10-22": "Diwali-Balipratipada",
    "2025-11-05": "Prakash Gurpurb Sri Guru Nanak Dev",
    "2025-12-25": "Christmas",
    "2026-01-26": "Republic Day",
    "2026-03-03": "Holi",
    "2026-03-26": "Shri Ram Navami",
    "2026-03-31": "Shri Mahavir Jayanti",
    "2026-04-03": "Good Friday",
    "2026-04-14": "Dr. Baba Saheb Ambedkar Jayanti",
    "2026-05-01": "Maharashtra Day",
    "2026-05-28": "Bakri Id",
    "2026-06-26": "Muharram",
    "2026-09-14": "Ganesh Chaturthi",
    "2026-10-02": "Mahatma Gandhi Jayanti",
    "2026-10-20": "Dussehra",
    "2026-11-10": "Diwali-Balipratipada",
    "2026-11-24": "Prakash Gurpurb Sri Guru Nanak Dev",
    "2026-12-25": "Christmas",
}


def get_nse_holiday_name(target_date=None):
    try:
        if target_date is None:
            ist = pytz.timezone('Asia/Kolkata')
            target_date = datetime.datetime.now(ist).date()
        if isinstance(target_date, datetime.datetime):
            target_date = target_date.date()
        return NSE_TRADING_HOLIDAYS.get(target_date.isoformat(), "")
    except Exception:
        return ""


def is_market_session_day(target_date=None):
    try:
        if target_date is None:
            ist = pytz.timezone('Asia/Kolkata')
            target_date = datetime.datetime.now(ist).date()
        if isinstance(target_date, datetime.datetime):
            target_date = target_date.date()
        if target_date.weekday() >= 5:
            return False
        return not bool(get_nse_holiday_name(target_date))
    except Exception:
        return True


def is_market_open():
    """Check if NSE market is currently open (9:15 AM - 3:30 PM IST, trading days only)."""
    try:
        ist = pytz.timezone('Asia/Kolkata')
        now = datetime.datetime.now(ist)
        if not is_market_session_day(now.date()):
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
        holiday_name = get_nse_holiday_name(now.date())
        if holiday_name:
            return f"closed (NSE holiday: {holiday_name})"

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
apply_strategy_profile(os.getenv("STRATEGY_PROFILE", "precision_v3"))


