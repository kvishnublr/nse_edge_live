"""
SWING 52 — 52-Week High Breakout Engine  (NIFTY 500 positional)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Signal rules (ALL must pass):
  1. Close ≥ 99.7% of rolling 252-bar high  (fresh 52W high breakout)
  2. Day's volume > 1.5× 20-bar average     (institutional participation)
  3. Sector alignment: ≥1 peer in same sector up >0.3%  (not a lone fake)
  4. NIFTY regime: 5-EMA > 20-EMA           (market momentum behind it)
  5. Stock price > ₹150  |  ATR/price < 5%  (liquid, manageable risk)

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

# ── NIFTY 500 SECTOR MAP ───────────────────────────────────────────────────
SECTOR_MAP: dict[str, list[str]] = {
    "BANKING": [
        "HDFCBANK","ICICIBANK","AXISBANK","KOTAKBANK","INDUSINDBK",
        "FEDERALBNK","BANDHANBNK","RBLBANK","IDFCFIRSTB","YESBANK",
        "AUBANK","EQUITASBNK","UJJIVANSFB","DCBBANK","KARURVYSYA",
    ],
    "PSU_BANK": [
        "SBIN","PNB","BANKBARODA","CANBK","UNIONBANK",
        "INDIANB","IOB","MAHABANK","CENTRALBK","BANKINDIA","J&KBANK",
    ],
    "IT": [
        "TCS","INFY","WIPRO","HCLTECH","TECHM","LTIM","MPHASIS",
        "PERSISTENT","COFORGE","OFSS","KPITTECH","CYIENT","MASTEK",
        "TANLA","RATEGAIN","INTELLECT",
    ],
    "AUTO": [
        "MARUTI","TATAMOTORS","BAJAJ-AUTO","EICHERMOT","HEROMOTOCO",
        "M&M","ASHOKLEY","TVSMOTOR","BALKRISIND","MOTHERSON",
        "MINDA","SONACOMS","BOSCHLTD","SUPRAJIT","EXIDEIND",
    ],
    "PHARMA": [
        "SUNPHARMA","DRREDDY","CIPLA","DIVISLAB","AUROPHARMA","LUPIN",
        "TORNTPHARM","ALKEM","ABBOTINDIA","IPCALAB","LAURUSLABS",
        "GRANULES","GLENMARK","NATCOPHARM","AJANTPHARM",
    ],
    "FMCG": [
        "HINDUNILVR","ITC","NESTLEIND","BRITANNIA","DABUR","MARICO",
        "COLPAL","GODREJCP","EMAMILTD","TATACONSUM","VBL","RADICO","VARUNBEV",
    ],
    "ENERGY": [
        "RELIANCE","ONGC","BPCL","IOC","HINDPETRO","GAIL",
        "PETRONET","ADANIGREEN","TATAPOWER","POWERGRID","NTPC","ADANIPOWER","CESC","TORNTPOWER",
    ],
    "METAL": [
        "TATASTEEL","JSWSTEEL","HINDALCO","VEDL","NMDC",
        "NATIONALUM","APLAPOLLO","RATNAMANI","JINDALSTEL","WELCORP","MOIL","HINDZINC",
    ],
    "INFRA_CAP": [
        "LT","ABB","SIEMENS","BEL","HAL","BHEL","CUMMINSIND","THERMAX",
        "VOLTAS","SCHAEFFLER","TIMKEN","SKFINDIA","GRINDWELL","KEC","KALPATPOWR",
    ],
    "NBFC_FIN": [
        "BAJFINANCE","BAJAJFINSV","CHOLAFIN","M&MFIN","SHRIRAMFIN",
        "MUTHOOTFIN","LICHSGFIN","PNBHOUSING","AAVAS","CANFINHOME",
        "HOMEFIRST","MANAPPURAM","CREDITACC","MASFIN",
    ],
    "CEMENT": [
        "ULTRACEMCO","AMBUJACEM","ACC","SHREECEM","DALMIA",
        "JKCEMENT","RAMCOCEM","HEIDELBERG","BIRLACORPN","JKLAKSHMI",
    ],
    "REALTY": [
        "DLF","GODREJPROP","PRESTIGE","BRIGADE","SOBHA",
        "OBEROIRLTY","PHOENIXLTD","MAHLIFE","LODHA","NUVOCO","ARVSMART",
    ],
    "TELECOM": [
        "BHARTIARTL","IDEA","INDUSTOWER","TATACOMM","HFCL","STLTECH",
    ],
    "CONSUMER": [
        "HAVELLS","CROMPTON","BLUESTARCO","TITAN","KALYANKJIL",
        "SENCO","VGUARD","POLYCAB","KEI","FINOLEX","BAJAJELEC","ORIENTELEC",
    ],
    "CHEMICAL": [
        "PIDILITIND","ASIANPAINT","BERGEPAINT","SUDARSCHEM","SRF",
        "DEEPAKNTR","ATUL","NAVINFLUOR","CLEAN","GUJGASLTD","NOCIL","FINEORG",
    ],
    "INSURANCE": [
        "SBILIFE","HDFCLIFE","ICICIPRULI","ICICIGI","NIACL","GICRE","STARHEALTH",
    ],
    "EXCHANGE_AMC": [
        "BSE","MCX","CDSL","CAMS","ANGELONE","MOTILALOFS","360ONE","NUVAMA",
    ],
    "HOSPITAL": [
        "APOLLOHOSP","FORTIS","MAXHEALTH","METROPOLIS","LALPATHLAB","VIJAYA","RAINBOW",
    ],
    "AGRI": [
        "UPL","PIIND","BAYER","DHANUKA","RALLIS","COROMANDEL","GNFC",
    ],
}

_SYM_TO_SECTOR: dict[str, str] = {}
for _sec, _syms in SECTOR_MAP.items():
    for _s in _syms:
        _SYM_TO_SECTOR[_s.upper()] = _sec

# ── Constants ───────────────────────────────────────────────────────────────
BREAKOUT_PROXIMITY = 0.003   # close within 0.3% of 52W high counts as breakout
VOLUME_RATIO_MIN   = 1.5     # vol > 1.5× 20-day avg
SECTOR_PEERS_MIN   = 1       # ≥1 peer up >0.3% in same sector
SECTOR_CHG_MIN     = 0.003   # peer day-change threshold
MIN_PRICE          = 150.0   # skip sub-₹150 stocks
MAX_ATR_PCT        = 0.05    # skip if ATR/price > 5%
SL_ATR             = 1.0
T1_ATR             = 2.5
T2_ATR             = 4.0
MAX_HOLD_DAYS      = 15
REGIME_EMA_S       = 5
REGIME_EMA_L       = 20


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
        return True   # not enough data — assume neutral/bullish
    window = nifty_closes[: idx + 1]
    e5  = _ema(window, REGIME_EMA_S)
    e20 = _ema(window, REGIME_EMA_L)
    return (e5[-1] > e20[-1]) if (e5 and e20) else True


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
        if i < 22:   # not enough history
            continue

        cl  = d["closes"][i]
        op  = d["opens"][i]
        hi  = d["highs"][i]
        lo  = d["lows"][i]
        vol = d["volumes"][i]

        if cl < MIN_PRICE:
            continue

        # ── 1. 52W high breakout ──────────────────────────────────────────
        w52 = _52w_high(d["closes"], i)
        if w52 <= 0 or cl < w52 * (1 - BREAKOUT_PROXIMITY):
            continue

        # ── 2. Volume expansion ───────────────────────────────────────────
        vr = _vol_ratio(d["volumes"], i)
        if vr < VOLUME_RATIO_MIN:
            continue

        # ── 3. NIFTY regime ───────────────────────────────────────────────
        if not bullish:
            continue

        # ── 4. Sector alignment ───────────────────────────────────────────
        if not _sector_aligned(sym, date_chg):
            continue

        # ── 5. ATR / liquidity ────────────────────────────────────────────
        atr_val = _atr(d["highs"][:i+1], d["lows"][:i+1], d["closes"][:i+1])
        if atr_val <= 0 or (atr_val / cl) > MAX_ATR_PCT:
            continue

        prev_close = d["closes"][i - 1] if i > 0 else cl
        day_chg_pct = round((cl - prev_close) / prev_close * 100, 2) if prev_close else 0.0

        signals.append({
            "date":       date_str,
            "symbol":     sym.upper(),
            "sector":     _SYM_TO_SECTOR.get(sym.upper(), "OTHER"),
            "close":      round(cl, 2),
            "day_chg":    day_chg_pct,
            "52w_high":   round(w52, 2),
            "vol_ratio":  round(vr, 2),
            "atr":        round(atr_val, 2),
            "atr_pct":    round(atr_val / cl * 100, 2),
            "vix":        round(vix_today, 1),
            "regime":     "BULL" if bullish else "NEUTRAL",
        })

    return signals


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

    entry   = opens[entry_i]
    # Recalculate targets from actual entry open
    atr_at_entry = _atr(sym_data["highs"][:entry_i+1], sym_data["lows"][:entry_i+1], sym_data["closes"][:entry_i+1])
    sl_price  = round(entry - SL_ATR * atr_at_entry, 2)
    t1_price  = round(entry + T1_ATR * atr_at_entry, 2)
    t2_price  = round(entry + T2_ATR * atr_at_entry, 2)

    outcome    = "EXPIRED"
    exit_price = closes[min(entry_i + MAX_HOLD_DAYS, n - 1)]
    exit_date  = dates[min(entry_i + MAX_HOLD_DAYS, n - 1)]
    hold_days  = 0

    for j in range(entry_i + 1, min(entry_i + MAX_HOLD_DAYS + 1, n)):
        jh = highs[j]
        jl = lows[j]
        hold_days = j - entry_i

        if jl <= sl_price:
            outcome    = "SL_HIT"
            exit_price = sl_price
            exit_date  = dates[j]
            break
        if jh >= t2_price:
            outcome    = "T2_HIT"
            exit_price = t2_price
            exit_date  = dates[j]
            break
        if jh >= t1_price:
            outcome    = "T1_HIT"
            exit_price = t1_price
            exit_date  = dates[j]
            break

    pnl_pts = round(exit_price - entry, 2)
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
    try:
        from fetcher import get_nifty500_kite_tokens
        token_map = get_nifty500_kite_tokens(kite)   # {symbol: token}
    except Exception as e:
        logger.warning("swing52: nifty500 token map failed: %s", e)
        token_map = {}

    if not token_map:
        return {"error": "Could not load NIFTY 500 token map from Kite"}

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

    # ── Scan each day for signals ──────────────────────────────────────────
    all_trades: list[dict] = []
    signals_by_day: dict[str, int] = {}

    for date_str in trade_days:
        vix_today = vix_map.get(date_str, 15.0)
        day_signals = _detect_day(date_str, sym_data, nifty_closes, nifty_dates, vix_today)

        signals_by_day[date_str] = len(day_signals)

        for sig in day_signals:
            sym = sig["symbol"]
            if sym not in sym_data:
                continue
            d = sym_data[sym]
            try:
                sig_i = d["dates"].index(date_str)
            except ValueError:
                continue

            res = _simulate_trade(sym, sig_i, d, 0, 0, 0)

            trade = {**sig, **res}
            all_trades.append(trade)

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

    wins    = [t for t in trades if t["outcome"] in ("T1_HIT", "T2_HIT")]
    losses  = [t for t in trades if t["outcome"] == "SL_HIT"]
    expired = [t for t in trades if t["outcome"] == "EXPIRED"]

    total_pnl    = round(sum(t["pnl_pct"] for t in trades), 2)
    gross_win    = round(sum(t["pnl_pct"] for t in wins), 2)
    gross_loss   = round(abs(sum(t["pnl_pct"] for t in losses)), 2)
    pf           = round(gross_win / gross_loss, 2) if gross_loss > 0 else 99.0
    win_rate     = round(len(wins) / len(trades) * 100, 1) if trades else 0
    avg_pnl      = round(total_pnl / len(trades), 2) if trades else 0
    avg_hold     = round(sum(t["hold_days"] for t in trades) / len(trades), 1) if trades else 0

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
                          "expired": 0, "pnl_pct": 0.0}
        mon_map[m]["signals"] += 1
        mon_map[m]["pnl_pct"] = round(mon_map[m]["pnl_pct"] + t["pnl_pct"], 2)
        if t["outcome"] in ("T1_HIT", "T2_HIT"): mon_map[m]["wins"]    += 1
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
        if t["outcome"] in ("T1_HIT", "T2_HIT"):
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
        if t["outcome"] in ("T1_HIT", "T2_HIT"):
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
            "wins":           len(wins),
            "losses":         len(losses),
            "expired":        len(expired),
            "win_rate":       win_rate,
            "avg_pnl_pct":    avg_pnl,
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
