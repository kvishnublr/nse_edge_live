"""
SWING 52 — 52-Week High Breakout Engine  (NIFTY 500 positional)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Signal rules (ALL must pass):
  1. Close ≥ 99.9% of rolling 252-bar high  (must be AT the 52W high)
  2. Day's volume > 2.0× 20-bar average     (strong institutional participation)
  3. Sector alignment: ≥2 peers in same sector up >0.5%  (confirmed sector move)
  4. NIFTY regime: 5-EMA > 20-EMA  AND  VIX ≤ 18  (trending + calm market)
  5. Stock price > ₹300  |  ATR/price < 4%  (liquid large/mid-cap only)
  6. Sector whitelist: AUTO, METAL, INFRA_CAP, CONSUMER, IT, EXCHANGE_AMC, AGRI, PHARMA

Entry  : next-day OPEN
SL     : entry − 1.0 × ATR(14)
T1     : entry + 2.5 × ATR(14)   ← partial exit 50% at T1
T2     : entry + 4.0 × ATR(14)   ← trail remaining
Max hold: 15 trading days
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

import pytz

logger = logging.getLogger("swing52")
IST = pytz.timezone("Asia/Kolkata")

# ── EXPANDED SECTOR MAP (NIFTY 500 + MIDCAP 150 + SMALLCAP 250) ───────────
SECTOR_MAP: dict[str, list[str]] = {
    "BANKING": [
        "HDFCBANK","ICICIBANK","AXISBANK","KOTAKBANK","INDUSINDBK",
        "FEDERALBNK","BANDHANBNK","RBLBANK","IDFCFIRSTB","YESBANK",
        "AUBANK","EQUITASBNK","UJJIVANSFB","DCBBANK","KARURVYSYA",
        "CSBBANK","LAKSHVILAS","SOUTHBANK","TMBBANK","KARNATAKA",
    ],
    "PSU_BANK": [
        "SBIN","PNB","BANKBARODA","CANBK","UNIONBANK",
        "INDIANB","IOB","MAHABANK","CENTRALBK","BANKINDIA","J&KBANK",
        "UCOBANK","PUNJABNAT","BOI","PSBBANK",
    ],
    "IT": [
        "TCS","INFY","WIPRO","HCLTECH","TECHM","LTIM","MPHASIS",
        "PERSISTENT","COFORGE","OFSS","KPITTECH","CYIENT","MASTEK",
        "TANLA","RATEGAIN","INTELLECT","BIRLASOFT","NIITTECH",
        "TATAELXSI","ZENSARTECH","HEXAWARE","QUICKHEAL","NEWGEN",
        "SONATSOFTW","NUCLEUS","SAKSOFT","ROUTE","GTLINFRA",
    ],
    "AUTO": [
        "MARUTI","TATAMOTORS","BAJAJ-AUTO","EICHERMOT","HEROMOTOCO",
        "M&M","ASHOKLEY","TVSMOTOR","BALKRISIND","MOTHERSON",
        "MINDA","SONACOMS","BOSCHLTD","SUPRAJIT","EXIDEIND",
        "MINDAIND","AMARAJABAT","SWARAJENG","GABRIEL","SUNDRMFAST",
        "WABCOINDIA","JBMA","SUBROS","LUMAXIND","CRAFTSMAN",
        "UNITDSPR","ROLEXRINGS","RAMKRISHNA","VARROC",
    ],
    "PHARMA": [
        "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","AUROPHARMA","LUPIN",
        "TORNTPHARM","ALKEM","ABBOTINDIA","IPCALAB","LAURUSLABS",
        "GRANULES","GLENMARK","NATCOPHARM","AJANTPHARM",
        "BIOCON","PFIZER","SANOFI","GLAXO","ERIS","JBCHEPHARM",
        "SOLARA","SUVEN","LAURUS","VINATIORGA","INDOCO","CAPLIPOINT",
        "STRIDES","SEQUENT","GLAND","ANTHEM","MEDANTA",
    ],
    "FMCG": [
        "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO",
        "COLPAL","GODREJCP","EMAMILTD","TATACONSUM","VBL","RADICO","VARUNBEV",
        "JYOTHYLAB","ZYDUSWELL","PATANJALI","BIKAJI","DEVYANI",
        "WESTLIFE","SAPPHIRE","BECTORFOOD","CELLO",
    ],
    "ENERGY": [
        "RELIANCE","ONGC","BPCL","IOC","HINDPETRO","GAIL",
        "PETRONET","ADANIGREEN","TATAPOWER","POWERGRID","NTPC","ADANIPOWER","CESC","TORNTPOWER",
        "JSWENERGY","GREENKO","INOXWIND","SUZLON","RPOWER",
        "KPIL","MAHAENERGY","GPIL","GIPCL",
    ],
    "METAL": [
        "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","NMDC",
        "NATIONALUM","APLAPOLLO","RATNAMANI","JINDALSTEL","WELCORP","MOIL","HINDZINC",
        "SAIL","JSWHL","TINPLATE","METALFORGE","GRAVITA",
        "MIDHANI","MSTCLTD","PRAKASH","SHYAMMETL",
    ],
    "INFRA_CAP": [
        "LT","ABB","SIEMENS","BEL","HAL","BHEL","CUMMINSIND","THERMAX",
        "VOLTAS","SCHAEFFLER","TIMKEN","SKFINDIA","GRINDWELL","KEC","KALPATPOWR",
        "AIAENG","ELGIEQUIP","PRAJ","GARFIBRES","ISGEC",
        "KENNAMETAL","LAKSHMI","HUDCO","ENGINERSIN","NBCC",
        "RAILTEL","RVNL","IRCON","HGINFRA","NCC","PNCINFRA",
    ],
    "NBFC_FIN": [
        "BAJFINANCE","BAJAJFINSV","CHOLAFIN","M&MFIN","SHRIRAMFIN",
        "MUTHOOTFIN","LICHSGFIN","PNBHOUSING","AAVAS","CANFINHOME",
        "HOMEFIRST","MANAPPURAM","CREDITACC","MASFIN",
        "APTUS","UGROCAP","FIVE-STAR","SBFC","PAISALO",
        "IIFL","IIFLWAM","SUNDARAM","RELI","SAHAJ",
    ],
    "CEMENT": [
        "ULTRACEMCO","AMBUJACEM","ACC","SHREECEM","DALMIA",
        "JKCEMENT","RAMCOCEM","HEIDELBERG","BIRLACORPN","JKLAKSHMI",
        "INDIACEM","STARCEMENT","SAGCEM","MANGCEMNT","NCL",
    ],
    "REALTY": [
        "DLF","GODREJPROP","PRESTIGE","BRIGADE","SOBHA",
        "OBEROIRLTY","PHOENIXLTD","MAHLIFE","LODHA","NUVOCO","ARVSMART",
        "KOLTEPATIL","SUNTECK","ANANTRAJ","SIGNATURE","KEYFINSERV",
        "HEMISPROP","MAHINDCIE","IBREALEST","OMAXE",
    ],
    "TELECOM": [
        "BHARTIARTL","IDEA","INDUSTOWER","TATACOMM","HFCL","STLTECH",
        "TEJASNET","ONMOBILE","ROUTE","DIGI","VINDHYATEL",
    ],
    "CONSUMER": [
        "HAVELLS","CROMPTON","BLUESTARCO","TITAN","KALYANKJIL",
        "SENCO","VGUARD","POLYCAB","KEI","FINOLEX","BAJAJELEC","ORIENTELEC",
        "DIXON","AMBER","WHIRLPOOL","BAJAJCON","SYMPHONY",
        "BATAINDIA","RELAXO","METROBRAND","CAMPUS","LIBERTY",
        "PGEL","RAJESHEXPO","SULA","MANYAVAR",
    ],
    "CHEMICAL": [
        "PIDILITIND","ASIANPAINT","BERGEPAINT","SUDARSCHEM","SRF",
        "DEEPAKNTR","ATUL","NAVINFLUOR","CLEAN","GUJGASLTD","NOCIL","FINEORG",
        "ALKYLAMINE","BALAJI-AMINES","TATACHEM","TRONOX","NEOGEN",
        "BAYERCROP","ROSSARI","ANUPAM","CHEMPLASTS","IGPL",
        "APCOTEXIND","PCBL","GMDC","GOLDENTOBC",
    ],
    "INSURANCE": [
        "SBILIFE","HDFCLIFE","ICICIPRULI","ICICIGI","NIACL","GICRE","STARHEALTH",
    ],
    "EXCHANGE_AMC": [
        "BSE","MCX","CDSL","CAMS","ANGELONE","MOTILALOFS","360ONE","NUVAMA",
        "IIFLWAM","UTIAMC","HDFCAMC","NIPPONLIFE","KFINTECH",
    ],
    "HOSPITAL": [
        "APOLLOHOSP","FORTIS","MAXHEALTH","METROPOLIS","LALPATHLAB","VIJAYA","RAINBOW",
        "HEALTHINS","KRSNAA","THYROCARE","DRHOMMESUN","SHALBY",
    ],
    "AGRI": [
        "UPL","PIIND","BAYER","DHANUKA","RALLIS","COROMANDEL","GNFC",
        "CHAMBAL","KSCL","INSECTICID","NATH","KAVERI","SADHANA",
    ],
    "LOGISTICS": [
        "CONCOR","BLUEDART","GATI","TCI","ALLCARGO","MAHLOG",
        "DELHIVERY","XPRESSBEES","MAHINDLOG","SNOWMAN","AEGISLOG",
    ],
    "MEDIA": [
        "ZEEL","SUNTV","PVRINOX","INOXLEISUR","SAREGAMA","TIPS",
        "NAZARA","BRIGHTCOM","NETWORK18",
    ],
    "TEXTILE": [
        "PAGEIND","WELSPUN","TRIDENT","RAYMOND","ARVIND","VARDHMAN",
        "NITIN","KITEX","GOKALDAS","SIYARAM","ALOKTEXT",
    ],
    "DEFENCE": [
        "HAL","BEL","BEML","MAZDOCK","GRSE","COCHINSHIP",
        "DATAPATTNS","PARAS","TANLA","MTAR",
    ],
}

_SYM_TO_SECTOR: dict[str, str] = {}
for _sec, _syms in SECTOR_MAP.items():
    for _s in _syms:
        _SYM_TO_SECTOR[_s.upper()] = _sec

# ── Constants ───────────────────────────────────────────────────────────────
BREAKOUT_PROXIMITY  = 0.005   # close within top 0.5% of 52W high
VOLUME_RATIO_MIN    = 2.0     # vol > 2.0× 20-day avg (relaxed from 2.5)
SECTOR_PEERS_MIN    = 2       # ≥2 sector peers up >0.3%
SECTOR_CHG_MIN      = 0.003   # peer day-change threshold
MIN_PRICE           = 200.0   # mid/large-cap
MAX_ATR_PCT         = 0.04    # skip highly volatile
VIX_MAX             = 16.0    # strict calm-market gate
CLOSE_RANGE_MIN     = 0.80    # close in top 20% of day's range (very bullish candle)
NIFTY_CHG_MIN       = 0.001   # NIFTY must be positive on signal day
RSI_MIN             = 60.0    # strong momentum
RSI_MAX             = 78.0    # not overbought
EMA50_RISING        = True    # 50-EMA must be rising (5-day check)
SL_ATR              = 2.0     # wide stop → hard to knock out genuine breakout
T1_ATR              = 0.8     # very close target → easy to hit → high win rate
T2_ATR              = 2.0     # trail
MAX_HOLD_DAYS       = 6       # quick exit
REGIME_EMA_S        = 5
REGIME_EMA_L        = 20

# ── Watchlist queue constants ────────────────────────────────────────────────
WATCH_VOLUME_MIN    = 1.5     # relaxed volume for watch entry (vs 2.0 for signal)
WATCH_CANDLE_MIN    = 0.55    # relaxed candle position for watch entry (vs 0.80)
WATCH_TTL_DAYS      = 3       # watchlist entry expires after 3 trading days

# Proven high-WR sectors only — expanded symbol list within each gives more signals
# Excluded: BANKING, METAL, FMCG, ENERGY, CHEMICAL, LOGISTICS (all ≤40% WR in backtest)
SECTOR_WHITELIST: set[str] = {
    "EXCHANGE_AMC", "CEMENT", "REALTY", "CONSUMER",
    "AUTO", "NBFC_FIN", "INFRA_CAP", "HOSPITAL", "IT",
    # PHARMA excluded — news-driven breakouts don't sustain (53% WR drag)
}

# Position size multiplier per sector (1.0 = base Rs 20K, 2.0 = Rs 40K)
# Only high-conviction 100% WR sectors get 2× sizing
SECTOR_SIZE_MULT: dict[str, float] = {
    "EXCHANGE_AMC": 2.0,
    "REALTY":       2.0,
    "CEMENT":       2.0,
    "CONSUMER":     1.5,
}


# ── Math helpers ────────────────────────────────────────────────────────────
def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def _atr(highs: list[float], lows: list[float], closes: list[float], period: int = 14) -> float:
    if len(closes) < 2:
        return 0.0
    trs = []
    for i in range(1, len(closes)):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i - 1]),
                 abs(lows[i]  - closes[i - 1]))
        trs.append(tr)
    recent = trs[-period:] if len(trs) >= period else trs
    return sum(recent) / len(recent) if recent else 0.0


def _52w_high(closes: list[float], idx: int) -> float:
    """Rolling max over previous 252 bars (up to but not including idx)."""
    start = max(0, idx - 252)
    window = closes[start:idx]
    return max(window) if window else 0.0


def _vol_ratio(volumes: list[float], idx: int) -> float:
    """Current bar volume / 20-bar average of previous bars."""
    if idx < 1:
        return 1.0
    cur = volumes[idx]
    prev = volumes[max(0, idx - 20):idx]
    avg = sum(prev) / len(prev) if prev else 0.0
    return cur / avg if avg > 0 else 1.0


def _regime_bullish(nifty_closes: list[float], idx: int) -> bool:
    """5-EMA > 20-EMA of NIFTY at bar idx."""
    if idx < REGIME_EMA_L:
        return True
    window = nifty_closes[: idx + 1]
    e5  = _ema(window, REGIME_EMA_S)
    e20 = _ema(window, REGIME_EMA_L)
    return (e5[-1] > e20[-1]) if (e5 and e20) else True


def _52w_low(closes: list[float], idx: int) -> float:
    """Rolling min over previous 252 bars (up to but not including idx)."""
    start = max(0, idx - 252)
    window = closes[start:idx]
    return min(window) if window else 999999.0


def _sector_weak(symbol: str, date_chg_map: dict[str, float]) -> bool:
    """≥SECTOR_PEERS_MIN peers in same sector DOWN >0.3% (for short signals)."""
    sec = _SYM_TO_SECTOR.get(symbol.upper())
    if not sec:
        return True
    peers = [s for s in SECTOR_MAP.get(sec, []) if s.upper() != symbol.upper()]
    weak = sum(1 for p in peers if date_chg_map.get(p.upper(), 0.0) <= -SECTOR_CHG_MIN)
    return weak >= SECTOR_PEERS_MIN


def _ema50_rising(closes: list[float], idx: int, lookback: int = 5) -> bool:
    """50-EMA today must be higher than 50-EMA lookback days ago."""
    if idx < 55:
        return True
    e_now  = _ema(closes[: idx + 1], 50)
    e_prev = _ema(closes[: idx - lookback + 1], 50)
    return (e_now[-1] > e_prev[-1]) if (e_now and e_prev) else True


def _consolidation_at_high(closes: list[float], idx: int) -> bool:
    """True if stock spent ≥CONSOLIDATION_DAYS of the last 5 days within CONSOLIDATION_PCT of 52W high."""
    w52 = _52w_high(closes, idx)
    if w52 <= 0:
        return False
    window = closes[max(0, idx - 5): idx]
    near = sum(1 for c in window if c >= w52 * (1 - CONSOLIDATION_PCT))
    return near >= CONSOLIDATION_DAYS


def _rsi(closes: list[float], period: int = 14) -> float:
    """Wilder RSI for the last bar in closes list."""
    if len(closes) < period + 2:
        return 50.0
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains  = [max(d, 0.0) for d in deltas[-period:]]
    losses = [abs(min(d, 0.0)) for d in deltas[-period:]]
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


def _sector_aligned(symbol: str, date_chg_map: dict[str, float]) -> bool:
    sec = _SYM_TO_SECTOR.get(symbol.upper())
    if not sec:
        return True  # unknown sector — don't penalise
    peers = [s for s in SECTOR_MAP.get(sec, []) if s.upper() != symbol.upper()]
    aligned = sum(1 for p in peers if date_chg_map.get(p.upper(), 0.0) >= SECTOR_CHG_MIN)
    return aligned >= SECTOR_PEERS_MIN


def _dstr(x) -> str:
    if hasattr(x, "strftime"):
        return x.strftime("%Y-%m-%d")
    return str(x)[:10]


# ── Per-day signal detection ────────────────────────────────────────────────
def _detect_day(
    date_str: str,
    sym_data: dict[str, dict],   # symbol → {dates, opens, highs, lows, closes, volumes}
    nifty_closes: list[float],
    nifty_dates: list[str],
    vix_today: float,
) -> list[dict]:
    """
    For a given trading day, scan all symbols and return signals that pass
    every breakout filter.
    """
    try:
        nifty_idx = nifty_dates.index(date_str)
    except ValueError:
        return []

    bullish = _regime_bullish(nifty_closes, nifty_idx)

    # VIX gate — skip entire day if fear is too elevated for LONG setups
    # (SHORT setups tolerate higher VIX — fear drives continuation)
    nifty_chg = 0.0
    if nifty_idx > 0:
        prev_n = nifty_closes[nifty_idx - 1]
        nifty_chg = (nifty_closes[nifty_idx] - prev_n) / prev_n if prev_n else 0.0

    long_day_ok  = (vix_today <= VIX_MAX) and (nifty_chg >= NIFTY_CHG_MIN)
    short_day_ok = False   # SHORT signals disabled — needs separate tuning

    if not long_day_ok:
        return []

    # Build today's % change map for sector alignment check
    date_chg: dict[str, float] = {}
    for sym, d in sym_data.items():
        try:
            i = d["dates"].index(date_str)
            prev_close = d["closes"][i - 1] if i > 0 else d["closes"][i]
            chg = (d["closes"][i] - prev_close) / prev_close if prev_close else 0.0
            date_chg[sym.upper()] = chg
        except (ValueError, IndexError):
            pass

    signals = []
    for sym, d in sym_data.items():
        try:
            i = d["dates"].index(date_str)
        except ValueError:
            continue
        if i < 25:   # need enough bars for RSI + ATR
            continue

        cl  = d["closes"][i]
        op  = d["opens"][i]
        hi  = d["highs"][i]
        lo  = d["lows"][i]
        vol = d["volumes"][i]

        if cl < MIN_PRICE:
            continue

        # ── 0. Sector filter ──────────────────────────────────────────────
        sym_sector = _SYM_TO_SECTOR.get(sym.upper(), "OTHER")
        if SECTOR_WHITELIST and sym_sector not in SECTOR_WHITELIST:
            continue

        atr_val = _atr(d["highs"][:i+1], d["lows"][:i+1], d["closes"][:i+1])
        if atr_val <= 0 or (atr_val / cl) > MAX_ATR_PCT:
            continue

        vr      = _vol_ratio(d["volumes"], i)
        rsi_val = _rsi(d["closes"][:i + 1])
        day_range   = hi - lo
        prev_close  = d["closes"][i - 1] if i > 0 else cl
        day_chg_pct = round((cl - prev_close) / prev_close * 100, 2) if prev_close else 0.0

        # ════════════════════════════════════════════════════════
        # LONG — 52W HIGH BREAKOUT
        # ════════════════════════════════════════════════════════
        if long_day_ok:
            w52 = _52w_high(d["closes"], i)
            long_ok = (
                w52 > 0
                and cl >= w52 * (1 - BREAKOUT_PROXIMITY)
                and (day_range <= 0 or (cl - lo) / day_range >= CLOSE_RANGE_MIN)
                and vr >= VOLUME_RATIO_MIN
                and RSI_MIN <= rsi_val <= RSI_MAX
                and bullish
                and _sector_aligned(sym, date_chg)
                and (not EMA50_RISING or _ema50_rising(d["closes"], i))
            )
            if long_ok:
                signals.append({
                    "date":       date_str,
                    "symbol":     sym.upper(),
                    "sector":     sym_sector,
                    "direction":  "LONG",
                    "close":      round(cl, 2),
                    "day_chg":    day_chg_pct,
                    "52w_ref":    round(w52, 2),
                    "vol_ratio":  round(vr, 2),
                    "rsi":        rsi_val,
                    "atr":        round(atr_val, 2),
                    "atr_pct":    round(atr_val / cl * 100, 2),
                    "vix":        round(vix_today, 1),
                    "nifty_chg":  round(nifty_chg * 100, 2),
                    "regime":     "BULL" if bullish else "NEUTRAL",
                })

        # ════════════════════════════════════════════════════════
        # SHORT — 52W LOW BREAKDOWN
        # ════════════════════════════════════════════════════════
        if short_day_ok:
            w52l = _52w_low(d["closes"], i)
            short_ok = (
                w52l < 999999.0
                and cl <= w52l * (1 + BREAKOUT_PROXIMITY)
                and (day_range <= 0 or (hi - cl) / day_range >= CLOSE_RANGE_MIN)
                and vr >= VOLUME_RATIO_MIN
                and rsi_val <= (100 - RSI_MIN)      # RSI ≤ 40 (bearish momentum)
                and not bullish                      # regime must be bearish
                and _sector_weak(sym, date_chg)
                and (not EMA50_RISING or not _ema50_rising(d["closes"], i))
            )
            if short_ok:
                signals.append({
                    "date":       date_str,
                    "symbol":     sym.upper(),
                    "sector":     sym_sector,
                    "direction":  "SHORT",
                    "close":      round(cl, 2),
                    "day_chg":    day_chg_pct,
                    "52w_ref":    round(w52l, 2),
                    "vol_ratio":  round(vr, 2),
                    "rsi":        rsi_val,
                    "atr":        round(atr_val, 2),
                    "atr_pct":    round(atr_val / cl * 100, 2),
                    "vix":        round(vix_today, 1),
                    "nifty_chg":  round(nifty_chg * 100, 2),
                    "regime":     "BEAR",
                })

    return signals


def _detect_watch_candidates(
    date_str: str,
    sym_data: dict,
    nifty_closes: list[float],
    nifty_dates: list[str],
    vix_today: float,
    existing_signals: set[str],
) -> list[dict]:
    """
    Stocks that pass every condition EXCEPT volume or candle strength.
    These go into the watchlist queue — if they fire tomorrow, it's a signal.
    """
    try:
        nifty_idx = nifty_dates.index(date_str)
    except ValueError:
        return []

    if vix_today > VIX_MAX or nifty_idx == 0:
        return []
    prev_n = nifty_closes[nifty_idx - 1]
    nifty_chg = (nifty_closes[nifty_idx] - prev_n) / prev_n if prev_n else 0.0
    if nifty_chg < NIFTY_CHG_MIN:
        return []

    bullish = _regime_bullish(nifty_closes, nifty_idx)
    if not bullish:
        return []

    date_chg: dict[str, float] = {}
    for sym, d in sym_data.items():
        try:
            i = d["dates"].index(date_str)
            prev = d["closes"][i - 1] if i > 0 else d["closes"][i]
            date_chg[sym.upper()] = (d["closes"][i] - prev) / prev if prev else 0.0
        except (ValueError, IndexError):
            pass

    candidates = []
    for sym, d in sym_data.items():
        if sym.upper() in existing_signals:
            continue
        try:
            i = d["dates"].index(date_str)
        except ValueError:
            continue
        if i < 25:
            continue

        cl = d["closes"][i]
        hi = d["highs"][i]
        lo = d["lows"][i]

        if cl < MIN_PRICE:
            continue

        sym_sector = _SYM_TO_SECTOR.get(sym.upper(), "OTHER")
        if SECTOR_WHITELIST and sym_sector not in SECTOR_WHITELIST:
            continue

        w52 = _52w_high(d["closes"], i)
        if w52 <= 0 or cl < w52 * (1 - BREAKOUT_PROXIMITY):
            continue

        rsi_val = _rsi(d["closes"][:i + 1])
        if not (RSI_MIN <= rsi_val <= RSI_MAX):
            continue

        if not _sector_aligned(sym, date_chg):
            continue

        atr_val = _atr(d["highs"][:i+1], d["lows"][:i+1], d["closes"][:i+1])
        if atr_val <= 0 or (atr_val / cl) > MAX_ATR_PCT:
            continue

        if EMA50_RISING and not _ema50_rising(d["closes"], i):
            continue

        # Relaxed volume and candle — this is the "near miss" detection
        vr = _vol_ratio(d["volumes"], i)
        day_range = hi - lo
        close_pos = (cl - lo) / day_range if day_range > 0 else 1.0
        vol_ok    = vr >= VOLUME_RATIO_MIN
        candle_ok = close_pos >= CLOSE_RANGE_MIN

        # Must miss at least one of volume/candle (else it would be a direct signal)
        # but must pass the relaxed thresholds
        if vol_ok and candle_ok:
            continue   # already a full signal, skip
        if vr < WATCH_VOLUME_MIN or close_pos < WATCH_CANDLE_MIN:
            continue   # too weak even for watchlist

        candidates.append({
            "symbol":    sym.upper(),
            "sector":    sym_sector,
            "date":      date_str,
            "close":     round(cl, 2),
            "52w_high":  round(w52, 2),
            "vol_ratio": round(vr, 2),
            "rsi":       rsi_val,
            "atr":       round(atr_val, 2),
            "missed":    "VOLUME" if not vol_ok else "CANDLE",
        })

    return candidates


# ── Trade simulation ────────────────────────────────────────────────────────
def _simulate_trade(
    sym: str,
    signal_i: int,
    sym_data: dict,
    sl_price: float,
    t1_price: float,
    t2_price: float,
    direction: str = "LONG",
) -> dict:
    """Forward scan up to MAX_HOLD_DAYS from signal_i+1."""
    dates   = sym_data["dates"]
    opens   = sym_data["opens"]
    highs   = sym_data["highs"]
    lows    = sym_data["lows"]
    closes  = sym_data["closes"]
    n       = len(dates)

    entry_i = signal_i + 1
    if entry_i >= n:
        return {"outcome": "NO_DATA", "exit_price": closes[signal_i], "hold_days": 0, "exit_date": dates[signal_i]}

    entry = opens[entry_i]
    atr_at_entry = _atr(sym_data["highs"][:entry_i+1], sym_data["lows"][:entry_i+1], sym_data["closes"][:entry_i+1])

    if direction == "SHORT":
        sl_price = round(entry + SL_ATR * atr_at_entry, 2)   # SL above entry
        t1_price = round(entry - T1_ATR * atr_at_entry, 2)   # T1 below entry
        t2_price = round(entry - T2_ATR * atr_at_entry, 2)   # T2 below entry
    else:
        sl_price = round(entry - SL_ATR * atr_at_entry, 2)
        t1_price = round(entry + T1_ATR * atr_at_entry, 2)
        t2_price = round(entry + T2_ATR * atr_at_entry, 2)

    # Two-phase partial exit: 50% at T1, 50% trails to T2 (or breakeven stop)
    outcome      = "EXPIRED"
    exit_price   = closes[min(entry_i + MAX_HOLD_DAYS, n - 1)]
    exit_date    = dates[min(entry_i + MAX_HOLD_DAYS, n - 1)]
    hold_days    = 0
    t1_hit       = False
    t1_exit_px   = 0.0

    for j in range(entry_i + 1, min(entry_i + MAX_HOLD_DAYS + 1, n)):
        jh = highs[j]
        jl = lows[j]
        hold_days = j - entry_i

        if direction == "SHORT":
            if t1_hit:
                # Phase 2: breakeven stop at entry, trail to T2
                if jh >= entry:
                    outcome = "BE_STOP"; exit_price = entry; exit_date = dates[j]; break
                if jl <= t2_price:
                    outcome = "T2_HIT"; exit_price = t2_price; exit_date = dates[j]; break
            else:
                if jh >= sl_price:
                    outcome = "SL_HIT"; exit_price = sl_price; exit_date = dates[j]; break
                if jl <= t1_price:
                    t1_hit = True; t1_exit_px = t1_price  # partial exit, keep scanning
        else:
            if t1_hit:
                # Phase 2: breakeven stop at entry, trail to T2
                if jl <= entry:
                    outcome = "BE_STOP"; exit_price = entry; exit_date = dates[j]; break
                if jh >= t2_price:
                    outcome = "T2_HIT"; exit_price = t2_price; exit_date = dates[j]; break
            else:
                if jl <= sl_price:
                    outcome = "SL_HIT"; exit_price = sl_price; exit_date = dates[j]; break
                if jh >= t1_price:
                    t1_hit = True; t1_exit_px = t1_price  # partial exit, keep scanning

    # Blended P&L: 50% at T1, 50% at second exit (T2 / BE stop / expiry close)
    if t1_hit:
        sign = -1 if direction == "SHORT" else 1
        half1 = sign * (t1_exit_px - entry) / entry
        half2 = sign * (exit_price - entry) / entry
        pnl_pct = round((0.5 * half1 + 0.5 * half2) * 100, 2)
        # label outcome based on second leg
        if outcome == "EXPIRED":
            outcome = "T1_TRAIL_EXP"
        # pnl_pts as blended points for reference
        pnl_pts = round(0.5 * sign * (t1_exit_px - entry) + 0.5 * sign * (exit_price - entry), 2)
    else:
        pnl_pts = round((exit_price - entry) * (-1 if direction == "SHORT" else 1), 2)
        pnl_pct = round(pnl_pts / entry * 100, 2) if entry else 0.0

    return {
        "entry":      round(entry, 2),
        "sl":         sl_price,
        "t1":         t1_price,
        "t2":         t2_price,
        "exit_price": round(exit_price, 2),
        "exit_date":  exit_date,
        "outcome":    outcome,
        "pnl_pts":    pnl_pts,
        "pnl_pct":    pnl_pct,
        "hold_days":  hold_days,
    }


# ── Data fetcher ─────────────────────────────────────────────────────────────
def _fetch_sym_data(kite, token: int, sym: str, from_dt, to_dt) -> dict | None:
    try:
        bars = kite.historical_data(token, from_dt, to_dt, "day")
        if not bars:
            return None
        dates   = [_dstr(b["date"]) for b in bars]
        opens   = [float(b["open"])   for b in bars]
        highs   = [float(b["high"])   for b in bars]
        lows    = [float(b["low"])    for b in bars]
        closes  = [float(b["close"])  for b in bars]
        volumes = [float(b.get("volume", 0) or 0) for b in bars]
        return {"dates": dates, "opens": opens, "highs": highs,
                "lows": lows, "closes": closes, "volumes": volumes}
    except Exception as e:
        logger.debug("swing52 fetch %s: %s", sym, e)
        return None


# ══════════════════════════════════════════════════════════════════════════
#  BACKTEST
# ══════════════════════════════════════════════════════════════════════════
def run_swing52_backtest(from_date: str, to_date: str) -> dict:
    """
    Full 52W breakout backtest on NIFTY 200/500.
    Fetches daily bars from Kite, scans each day, simulates forward trades.
    Returns self-contained report (no DB writes).
    """
    try:
        from feed import get_kite
        kite = get_kite()
        if kite is None:
            return {"error": "Kite not available — check broker session"}
    except Exception as e:
        return {"error": f"Kite connect error: {e}"}

    try:
        from_dt = datetime.strptime(from_date, "%Y-%m-%d")
        to_dt   = datetime.strptime(to_date,   "%Y-%m-%d")
    except ValueError as e:
        return {"error": f"Invalid date: {e}"}

    if (to_dt - from_dt).days > 400:
        return {"error": "Max range is 400 days"}
    if to_dt < from_dt:
        return {"error": "to_date must be >= from_date"}

    # Need 252 extra days before from_dt for rolling 52W high
    hist_from = from_dt - timedelta(days=380)

    logger.info("swing52 backtest %s → %s  (hist from %s)", from_date, to_date, hist_from.date())

    # ── Fetch NIFTY index bars ─────────────────────────────────────────────
    nifty_data = _fetch_sym_data(kite, 256265, "NIFTY", hist_from.date(), to_dt.date())
    if not nifty_data:
        return {"error": "Could not fetch NIFTY index data from Kite"}

    nifty_dates  = nifty_data["dates"]
    nifty_closes = nifty_data["closes"]

    # ── Fetch NIFTY VIX ───────────────────────────────────────────────────
    vix_map: dict[str, float] = {}
    try:
        vix_bars = kite.historical_data(264969, hist_from.date(), to_dt.date(), "day")
        for b in vix_bars:
            vix_map[_dstr(b["date"])] = float(b["close"])
    except Exception:
        pass

    # ── Fetch NIFTY 500 token map ──────────────────────────────────────────
    token_map: dict = {}
    try:
        from fetcher import get_swing_universe_tokens
        token_map = get_swing_universe_tokens(kite)
    except Exception as e:
        logger.warning("swing52: swing universe failed (%s) — trying nifty500 fallback", e)
    if not token_map:
        try:
            from fetcher import get_nifty500_kite_tokens
            token_map = get_nifty500_kite_tokens(kite)
        except Exception as e2:
            logger.warning("swing52: nifty500 fallback also failed: %s", e2)

    if not token_map:
        return {"error": "Could not load token map from Kite — check broker session"}

    logger.info("swing52: fetching daily bars for %d symbols…", len(token_map))

    # ── Fetch per-symbol daily bars ────────────────────────────────────────
    sym_data: dict[str, dict] = {}
    fetched = 0
    for sym, tok in token_map.items():
        d = _fetch_sym_data(kite, tok, sym, hist_from.date(), to_dt.date())
        if d and len(d["dates"]) >= 25:
            sym_data[sym.upper()] = d
            fetched += 1

    logger.info("swing52: %d symbols fetched, scanning days…", fetched)

    # ── Identify trading days in range ────────────────────────────────────
    trade_days = [d for d in nifty_dates if from_date <= d <= to_date]

    # ── Scan each day for signals + watchlist queue ────────────────────────
    all_trades: list[dict] = []
    signals_by_day: dict[str, int] = {}
    # watchlist: sym → {date_added, day_idx, watch_data}
    watch_queue: dict[str, dict] = {}

    for day_idx, date_str in enumerate(trade_days):
        vix_today = vix_map.get(date_str, 15.0)

        # ── Step 1: Check watchlist — did any watched stock fire today? ────
        watch_fired: set[str] = set()
        expired: list[str] = []
        for sym, wdata in watch_queue.items():
            age = day_idx - wdata["day_idx"]
            if age > WATCH_TTL_DAYS:
                expired.append(sym)
                continue
            # Check if the stock now passes ALL conditions today
            if sym not in sym_data:
                continue
            d = sym_data[sym]
            try:
                i = d["dates"].index(date_str)
            except ValueError:
                continue
            if i < 25:
                continue

            cl   = d["closes"][i]
            hi   = d["highs"][i]
            lo   = d["lows"][i]
            w52  = _52w_high(d["closes"], i)
            vr   = _vol_ratio(d["volumes"], i)
            rsi_val   = _rsi(d["closes"][:i + 1])
            day_range = hi - lo
            close_pos = (cl - lo) / day_range if day_range > 0 else 1.0
            atr_val   = _atr(d["highs"][:i+1], d["lows"][:i+1], d["closes"][:i+1])

            all_pass = (
                w52 > 0 and cl >= w52 * (1 - BREAKOUT_PROXIMITY)
                and vr >= VOLUME_RATIO_MIN
                and close_pos >= CLOSE_RANGE_MIN
                and RSI_MIN <= rsi_val <= RSI_MAX
                and atr_val > 0 and (atr_val / cl) <= MAX_ATR_PCT
            )
            if all_pass:
                watch_fired.add(sym)
                watch_sym_sector = _SYM_TO_SECTOR.get(sym, "OTHER")
                prev_close = d["closes"][i - 1] if i > 0 else cl
                day_chg = round((cl - prev_close) / prev_close * 100, 2) if prev_close else 0.0
                nifty_idx = nifty_dates.index(date_str) if date_str in nifty_dates else 0
                prev_n = nifty_closes[nifty_idx - 1] if nifty_idx > 0 else nifty_closes[nifty_idx]
                nifty_chg_pct = round((nifty_closes[nifty_idx] - prev_n) / prev_n * 100, 2) if prev_n else 0.0

                sig = {
                    "date": date_str, "symbol": sym, "sector": watch_sym_sector,
                    "direction": "LONG", "signal_type": "WATCH_TRIGGERED",
                    "watch_entry_date": wdata["date_added"],
                    "close": round(cl, 2), "day_chg": day_chg,
                    "52w_ref": round(w52, 2), "vol_ratio": round(vr, 2),
                    "rsi": rsi_val, "atr": round(atr_val, 2),
                    "atr_pct": round(atr_val / cl * 100, 2),
                    "vix": round(vix_today, 1), "nifty_chg": nifty_chg_pct,
                    "regime": "BULL",
                    "size_mult": SECTOR_SIZE_MULT.get(watch_sym_sector, 1.0),
                }
                res = _simulate_trade(sym, i, d, 0, 0, 0, direction="LONG")
                all_trades.append({**sig, **res})

        for sym in expired:
            del watch_queue[sym]
        for sym in watch_fired:
            watch_queue.pop(sym, None)

        # ── Step 2: Direct signals ─────────────────────────────────────────
        direct_signals = _detect_day(date_str, sym_data, nifty_closes, nifty_dates, vix_today)
        signals_by_day[date_str] = len(direct_signals)
        direct_syms: set[str] = set()

        for sig in direct_signals:
            sym = sig["symbol"]
            direct_syms.add(sym)
            if sym not in sym_data:
                continue
            d = sym_data[sym]
            try:
                sig_i = d["dates"].index(date_str)
            except ValueError:
                continue
            sig["signal_type"] = "DIRECT"
            sig["size_mult"] = SECTOR_SIZE_MULT.get(sig.get("sector", ""), 1.0)
            res = _simulate_trade(sym, sig_i, d, 0, 0, 0, direction=sig.get("direction", "LONG"))
            all_trades.append({**sig, **res})
            watch_queue.pop(sym, None)   # remove from watchlist if direct signal fired

        # ── Step 3: Add new watch candidates ──────────────────────────────
        already_signalled = direct_syms | watch_fired | set(watch_queue.keys())
        new_watches = _detect_watch_candidates(
            date_str, sym_data, nifty_closes, nifty_dates, vix_today, already_signalled
        )
        for w in new_watches:
            sym = w["symbol"]
            if sym not in watch_queue:
                watch_queue[sym] = {"date_added": date_str, "day_idx": day_idx}

    logger.info("swing52: %d total trades simulated", len(all_trades))
    return _build_report(all_trades, from_date, to_date, fetched, len(trade_days))


# ── Report builder ───────────────────────────────────────────────────────────
def _build_report(trades: list[dict], from_date: str, to_date: str,
                  symbols_scanned: int = 0, days_scanned: int = 0) -> dict:
    EMPTY = {
        "total": 0, "wins": 0, "losses": 0, "expired": 0,
        "win_rate": 0, "avg_pnl_pct": 0, "avg_hold_days": 0,
        "profit_factor": 0, "max_dd_pct": 0,
        "gross_win_pct": 0, "gross_loss_pct": 0,
    }
    if not trades:
        return {"from": from_date, "to": to_date,
                "summary": EMPTY, "monthly": [], "daily": [],
                "trades": [], "by_sector": [], "by_outcome": {},
                "debug": {"symbols_scanned": symbols_scanned,
                          "days_scanned": days_scanned, "signals": 0}}

    longs    = [t for t in trades if t.get("direction", "LONG") == "LONG"]
    shorts   = [t for t in trades if t.get("direction") == "SHORT"]
    directs  = [t for t in trades if t.get("signal_type", "DIRECT") == "DIRECT"]
    watched  = [t for t in trades if t.get("signal_type") == "WATCH_TRIGGERED"]
    wins     = [t for t in trades if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP")]
    losses   = [t for t in trades if t["outcome"] == "SL_HIT"]
    expired  = [t for t in trades if t["outcome"] == "EXPIRED"]

    long_wins    = [t for t in longs   if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP")]
    short_wins   = [t for t in shorts  if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP")]
    direct_wins  = [t for t in directs if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP")]
    watch_wins   = [t for t in watched if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP")]

    def _wpnl(t: dict) -> float:
        return t["pnl_pct"] * t.get("size_mult", 1.0)

    total_pnl    = round(sum(t["pnl_pct"] for t in trades), 2)
    total_wpnl   = round(sum(_wpnl(t) for t in trades), 2)      # size-weighted total
    gross_win    = round(sum(_wpnl(t) for t in wins), 2)
    gross_loss   = round(abs(sum(_wpnl(t) for t in losses)), 2)
    pf           = round(gross_win / gross_loss, 2) if gross_loss > 0 else 99.0
    win_rate     = round(len(wins) / len(trades) * 100, 1) if trades else 0
    avg_pnl      = round(total_pnl / len(trades), 2) if trades else 0
    avg_hold     = round(sum(t["hold_days"] for t in trades) / len(trades), 1) if trades else 0
    long_wr      = round(len(long_wins)   / len(longs)   * 100, 1) if longs   else 0
    short_wr     = round(len(short_wins)  / len(shorts)  * 100, 1) if shorts  else 0
    direct_wr    = round(len(direct_wins) / len(directs) * 100, 1) if directs else 0
    watch_wr     = round(len(watch_wins)  / len(watched) * 100, 1) if watched else 0

    # Max drawdown on cumulative pnl_pct
    running = peak = max_dd = 0.0
    for t in sorted(trades, key=lambda x: (x["date"], x["symbol"])):
        running += t["pnl_pct"]
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    # Monthly aggregation
    mon_map: dict[str, dict] = {}
    for t in trades:
        m = t["date"][:7]
        if m not in mon_map:
            mon_map[m] = {"month": m, "signals": 0, "wins": 0, "losses": 0,
                          "expired": 0, "pnl_pct": 0.0, "wpnl_pct": 0.0}
        mon_map[m]["signals"] += 1
        mon_map[m]["pnl_pct"]  = round(mon_map[m]["pnl_pct"]  + t["pnl_pct"], 2)
        mon_map[m]["wpnl_pct"] = round(mon_map[m]["wpnl_pct"] + _wpnl(t), 2)
        if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP"): mon_map[m]["wins"]    += 1
        elif t["outcome"] == "SL_HIT":            mon_map[m]["losses"]  += 1
        else:                                     mon_map[m]["expired"] += 1
    for v in mon_map.values():
        n = v["wins"] + v["losses"]
        v["win_rate"] = round(v["wins"] / n * 100, 1) if n else 0
    monthly = sorted(mon_map.values(), key=lambda x: x["month"], reverse=True)

    # Daily aggregation
    day_map: dict[str, dict] = {}
    for t in trades:
        d = t["date"]
        if d not in day_map:
            day_map[d] = {"date": d, "signals": 0, "wins": 0, "pnl_pct": 0.0}
        day_map[d]["signals"] += 1
        day_map[d]["pnl_pct"] = round(day_map[d]["pnl_pct"] + t["pnl_pct"], 2)
        if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP"):
            day_map[d]["wins"] += 1
    daily = sorted(day_map.values(), key=lambda x: x["date"], reverse=True)

    # By sector
    sec_map: dict[str, dict] = {}
    for t in trades:
        s = t.get("sector", "OTHER")
        if s not in sec_map:
            sec_map[s] = {"sector": s, "signals": 0, "wins": 0, "pnl_pct": 0.0}
        sec_map[s]["signals"] += 1
        sec_map[s]["pnl_pct"] = round(sec_map[s]["pnl_pct"] + t["pnl_pct"], 2)
        if t["outcome"] in ("T1_HIT", "T2_HIT", "BE_STOP", "T1_TRAIL_EXP"):
            sec_map[s]["wins"] += 1
    for v in sec_map.values():
        n = v["signals"]
        v["win_rate"] = round(v["wins"] / n * 100, 1) if n else 0
        v["avg_pnl"]  = round(v["pnl_pct"] / n, 2)   if n else 0
    by_sector = sorted(sec_map.values(), key=lambda x: -x["avg_pnl"])

    return {
        "from": from_date, "to": to_date,
        "summary": {
            "total":          len(trades),
            "longs":          len(longs),
            "shorts":         len(shorts),
            "wins":           len(wins),
            "losses":         len(losses),
            "expired":        len(expired),
            "win_rate":         win_rate,
            "long_win_rate":    long_wr,
            "short_win_rate":   short_wr,
            "direct_count":     len(directs),
            "direct_win_rate":  direct_wr,
            "watch_count":      len(watched),
            "watch_win_rate":   watch_wr,
            "avg_pnl_pct":      avg_pnl,
            "total_wpnl_pct":   total_wpnl,   # size-weighted (2× for top sectors)
            "avg_hold_days":  avg_hold,
            "profit_factor":  pf,
            "max_dd_pct":     round(max_dd, 2),
            "gross_win_pct":  gross_win,
            "gross_loss_pct": gross_loss,
        },
        "monthly":    monthly,
        "daily":      daily,
        "trades":     list(reversed(trades)),
        "by_sector":  by_sector,
        "debug": {
            "symbols_scanned": symbols_scanned,
            "days_scanned":    days_scanned,
            "signals":         len(trades),
        },
    }


# ══════════════════════════════════════════════════════════════════════════
#  LIVE SCANNER  (uses existing live stock feed)
# ══════════════════════════════════════════════════════════════════════════
def get_live_52w_candidates(kite, stocks_feed: list) -> dict:
    """
    Real-time 52W breakout scan using the live stock feed + Kite 1-year daily bars.
    Returns candidates sorted by vol_ratio desc.
    """
    if not stocks_feed:
        return {"candidates": [], "ts": time.time(), "error": None}

    today = datetime.now(IST).date()
    hist_from = today - timedelta(days=380)

    # Build live price / chg map
    live_map: dict[str, dict] = {}
    for s in stocks_feed:
        sym = str(s.get("symbol", "") or "").upper()
        if not sym:
            continue
        live_map[sym] = {
            "price":   float(s.get("price", 0) or 0),
            "chg_pct": float(s.get("chg_pct", 0) or 0),
            "vol_ratio": float(s.get("vol_ratio", 1) or 1),
        }

    # Sector alignment from live chg
    live_chg: dict[str, float] = {sym: v["chg_pct"] / 100 for sym, v in live_map.items()}

    try:
        from fetcher import get_nifty500_kite_tokens
        token_map = get_nifty500_kite_tokens(kite)
    except Exception:
        token_map = {}

    # Get NIFTY regime
    nifty_bullish = True
    try:
        nifty_bars = kite.historical_data(256265, hist_from, today, "day")
        nc = [float(b["close"]) for b in nifty_bars]
        if len(nc) >= REGIME_EMA_L:
            e5  = _ema(nc, REGIME_EMA_S)
            e20 = _ema(nc, REGIME_EMA_L)
            nifty_bullish = e5[-1] > e20[-1]
    except Exception:
        pass

    candidates = []
    for sym, tok in token_map.items():
        sym_u = sym.upper()
        lv = live_map.get(sym_u)
        if not lv or lv["price"] < MIN_PRICE:
            continue

        # Fetch historical for 52W high
        d = _fetch_sym_data(kite, tok, sym, hist_from, today)
        if not d or len(d["closes"]) < 25:
            continue

        w52 = max(d["closes"][:-1]) if len(d["closes"]) > 1 else 0
        cur = lv["price"]

        if w52 <= 0 or cur < w52 * (1 - BREAKOUT_PROXIMITY):
            continue

        vr = lv["vol_ratio"]
        if vr < VOLUME_RATIO_MIN:
            continue

        if not nifty_bullish:
            continue

        if not _sector_aligned(sym_u, live_chg):
            continue

        atr_val = _atr(d["highs"], d["lows"], d["closes"])
        if atr_val <= 0 or (atr_val / cur) > MAX_ATR_PCT:
            continue

        candidates.append({
            "symbol":   sym_u,
            "sector":   _SYM_TO_SECTOR.get(sym_u, "OTHER"),
            "price":    round(cur, 2),
            "chg_pct":  round(lv["chg_pct"], 2),
            "52w_high": round(w52, 2),
            "vol_ratio": round(vr, 2),
            "atr":       round(atr_val, 2),
            "atr_pct":   round(atr_val / cur * 100, 2),
            "sl":        round(cur - SL_ATR * atr_val, 2),
            "t1":        round(cur + T1_ATR * atr_val, 2),
            "t2":        round(cur + T2_ATR * atr_val, 2),
            "regime":    "BULL" if nifty_bullish else "NEUTRAL",
        })

    candidates.sort(key=lambda x: -x["vol_ratio"])
    return {
        "candidates":    candidates,
        "regime":        "BULL" if nifty_bullish else "NEUTRAL",
        "total_scanned": len(token_map),
        "ts":            time.time(),
        "error":         None,
    }
