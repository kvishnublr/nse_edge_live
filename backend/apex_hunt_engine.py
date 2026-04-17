"""
APEX HUNT v1 — Advanced Pre-Explosion Index Options Scout
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Strategy Logic:
  1. Weighted basket sentiment from NIFTY top-15 heavyweights
  2. Heat Score: basket (40%) + PCR (20%) + VIX stability (20%) + momentum (20%)
  3. Fire when Heat Score > 62 in valid time window
  4. Trade: NIFTY ATM CE (BULL) or PE (BEAR)
  5. Target: T1=+15 pts  SL=−8 pts  T2=+25 pts (strong signals)

Backtest:
  - Fetches NIFTY 50 index 1-min OHLCV from Kite
  - Proxy signal: 5-bar EMA breakout + directional body + range expansion
  - Simulates entry at next-bar open, tracks T1/T2/SL over 90 bars max
  - Returns daily / monthly / full-period P&L and stats
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Any

import pytz

logger = logging.getLogger("apex_hunt")

IST = pytz.timezone("Asia/Kolkata")

# ── NIFTY TOP-15 HEAVYWEIGHTS (weights ~Q1 2026) ───────────────────────────
NIFTY_HEAVYWEIGHTS: list[tuple[str, float]] = [
    ("HDFCBANK",   13.2),
    ("RELIANCE",    9.8),
    ("ICICIBANK",   7.4),
    ("INFY",        6.8),
    ("TCS",         4.9),
    ("LT",          4.2),
    ("BHARTIARTL",  3.8),
    ("AXISBANK",    3.5),
    ("KOTAKBANK",   3.2),
    ("HCLTECH",     2.9),
    ("SBIN",        2.7),
    ("BAJFINANCE",  2.4),
    ("WIPRO",       1.8),
    ("MARUTI",      1.7),
    ("ULTRACEMCO",  1.5),
]
_HW_MAP         = {sym: wt for sym, wt in NIFTY_HEAVYWEIGHTS}
_TOTAL_WEIGHT   = sum(wt for _, wt in NIFTY_HEAVYWEIGHTS)

# Signal thresholds
HEAT_THRESHOLD    = 62.0    # fire signal above this
HEAT_STRONG       = 75.0    # OTM strike suggestion when above this
RE_ENTRY_COOLDOWN = 300     # seconds between re-entries on same direction
NIFTY_LOT_SIZE    = 75
NIFTY_TOKEN       = 256265  # NSE:NIFTY 50 index instrument token

# ── VALID TRADING WINDOWS ──────────────────────────────────────────────────
# For LIVE signals: 3 focused high-quality windows
_VALID_WINDOWS = [
    (9 * 60 + 15,  10 * 60 + 30),   # Opening burst
    (11 * 60 + 0,  11 * 60 + 45),   # First reversal window
    (13 * 60 + 30, 14 * 60 + 30),   # Pre-close positioning
]
_HARD_CUTOFF = 14 * 60 + 45         # no new trades after 2:45 PM

# For BACKTEST: broader window to generate meaningful sample size
# Skips only the dead-zone chop (12:30-13:00) and last 15 min
_BT_START   = 9 * 60 + 20           # 9:20 AM
_BT_END     = 14 * 60 + 45          # 2:45 PM
_BT_SKIP    = (12 * 60 + 30, 13 * 60 + 0)   # midday dead zone to avoid

# ── STATE ──────────────────────────────────────────────────────────────────
_last_signal_ts: dict[str, float] = {}   # direction → last fire timestamp
_live_signals:   list[dict]        = []  # current session signals


# ══════════════════════════════════════════════════════════════════════════
# 1.  BASKET SENTIMENT
# ══════════════════════════════════════════════════════════════════════════
def compute_basket_score(stocks: list) -> dict:
    """
    Weighted basket sentiment from top-15 NIFTY stocks.
    Returns score −100..+100, direction, and per-stock contributors.
    """
    if not stocks:
        return {"score": 0.0, "direction": "NEUTRAL", "contributors": [],
                "aligned": 0, "total": 0}

    stock_map = {str(s.get("symbol", "")).upper(): s for s in stocks}

    weighted_sum        = 0.0
    total_weight_found  = 0.0
    contributors        = []
    aligned_bull        = 0
    aligned_bear        = 0

    for sym, weight in NIFTY_HEAVYWEIGHTS:
        s = stock_map.get(sym)
        if not s:
            continue
        chg    = float(s.get("chg_pct", 0) or 0)
        capped = max(-3.0, min(3.0, chg))          # cap at ±3% per stock
        contribution = capped * weight
        weighted_sum       += contribution
        total_weight_found += weight
        contributors.append({
            "sym":          sym,
            "wt":           weight,
            "chg":          round(chg, 2),
            "contribution": round(contribution, 3),
        })
        if chg > 0.1:
            aligned_bull += 1
        elif chg < -0.1:
            aligned_bear += 1

    if total_weight_found < 5.0:
        return {"score": 0.0, "direction": "NEUTRAL", "contributors": contributors,
                "aligned": 0, "total": len(contributors)}

    # Normalise to −100..+100
    norm_score = (weighted_sum / _TOTAL_WEIGHT) * (100.0 / 3.0)
    norm_score = max(-100.0, min(100.0, norm_score))

    if norm_score > 8:
        direction = "BULL"
        aligned   = aligned_bull
    elif norm_score < -8:
        direction = "BEAR"
        aligned   = aligned_bear
    else:
        direction = "NEUTRAL"
        aligned   = max(aligned_bull, aligned_bear)

    return {
        "score":        round(norm_score, 2),
        "direction":    direction,
        "contributors": sorted(contributors,
                               key=lambda x: abs(x["contribution"]),
                               reverse=True)[:8],
        "aligned":      aligned,
        "total":        len(contributors),
    }


# ══════════════════════════════════════════════════════════════════════════
# 2.  HEAT SCORE
# ══════════════════════════════════════════════════════════════════════════
def compute_heat_score(basket: dict,
                       macro:  dict | None,
                       chain:  dict | None) -> dict:
    """
    Composite heat score 0-100:
      40 pts  basket sentiment
      20 pts  PCR alignment
      20 pts  VIX stability
      20 pts  NIFTY short-term momentum
    """
    direction  = basket.get("direction", "NEUTRAL")
    basket_raw = basket.get("score", 0.0)

    # 1. Basket (40 pts)
    basket_pts = min(40.0, abs(basket_raw) * 0.40)

    # 2. PCR alignment (20 pts)
    pcr_pts = 0.0
    pcr_val = 0.0
    if chain:
        pcr_val = float(chain.get("pcr", 0) or 0)
        if direction == "BULL":
            if pcr_val >= 1.15:   pcr_pts = 20.0
            elif pcr_val >= 1.0:  pcr_pts = 12.0
        elif direction == "BEAR":
            if pcr_val <= 0.85:   pcr_pts = 20.0
            elif pcr_val <= 1.0:  pcr_pts = 12.0
        else:
            pcr_pts = 5.0

    # 3. VIX stability (20 pts)
    vix_pts = 0.0
    vix_val = 0.0
    if macro:
        vix_val = float(macro.get("vix", 0) or 0)
        if 0 < vix_val <= 13:    vix_pts = 20.0
        elif vix_val <= 16:      vix_pts = 14.0
        elif vix_val <= 20:      vix_pts = 8.0
        else:                    vix_pts = 2.0

    # 4. NIFTY momentum (20 pts)
    mom_pts  = 0.0
    nifty_chg = 0.0
    if macro:
        nifty_chg = float(
            macro.get("nifty_chg_pct",
                macro.get("nifty_chg",
                    macro.get("chg_pct", 0))) or 0
        )
        if direction == "BULL":
            if nifty_chg > 0.3:    mom_pts = 20.0
            elif nifty_chg > 0.1:  mom_pts = 12.0
            else:                  mom_pts = 5.0
        elif direction == "BEAR":
            if nifty_chg < -0.3:   mom_pts = 20.0
            elif nifty_chg < -0.1: mom_pts = 12.0
            else:                  mom_pts = 5.0
        else:
            mom_pts = 5.0

    heat_score = max(0.0, min(100.0,
                              basket_pts + pcr_pts + vix_pts + mom_pts))

    return {
        "heat_score":  round(heat_score, 1),
        "basket_pts":  round(basket_pts, 1),
        "pcr_pts":     round(pcr_pts, 1),
        "vix_pts":     round(vix_pts, 1),
        "mom_pts":     round(mom_pts, 1),
        "pcr":         round(pcr_val, 2),
        "vix":         round(vix_val, 2),
        "nifty_chg":   round(nifty_chg, 3),
        "direction":   direction,
    }


# ══════════════════════════════════════════════════════════════════════════
# 3.  LIVE SIGNAL DETECTION
# ══════════════════════════════════════════════════════════════════════════
def _time_quality(cm: int) -> float:
    if 9*60+15 <= cm <= 9*60+45:    return 1.0
    elif cm <= 10*60+30:            return 0.90
    elif 11*60+0 <= cm <= 11*60+45: return 0.85
    elif 13*60+30 <= cm <= 14*60+30:return 0.85
    elif cm <= 11*60+0:             return 0.50   # chop
    elif cm <= 13*60+30:            return 0.50   # dead zone
    return 0.60


def _in_valid_window(cm: int) -> bool:
    return any(s <= cm <= e for s, e in _VALID_WINDOWS) and cm <= _HARD_CUTOFF


def detect_apex_signals(state: dict) -> list:
    """
    Detect APEX HUNT signals from live market state dict.
    Called every ~20 s from scheduler.
    Returns list of signal dicts (usually 0 or 1).
    """
    global _live_signals

    now_dt = datetime.now(IST)
    cm     = now_dt.hour * 60 + now_dt.minute

    if not _in_valid_window(cm):
        return []

    stocks = (state.get("stocks")
              or state.get("last_stocks")
              or [])
    macro  = state.get("last_macro") or {}
    chain  = state.get("last_chain") or {}

    basket = compute_basket_score(stocks)
    heat   = compute_heat_score(basket, macro, chain)

    direction  = heat["direction"]
    heat_score = heat["heat_score"]

    if direction == "NEUTRAL" or heat_score < HEAT_THRESHOLD:
        return []

    # Cooldown
    now_ts  = time.time()
    last_ts = _last_signal_ts.get(direction, 0.0)
    if now_ts - last_ts < RE_ENTRY_COOLDOWN:
        return []

    # Time-quality adjusted confidence
    confidence = round(heat_score * _time_quality(cm), 1)
    if confidence < 55:
        return []

    _last_signal_ts[direction] = now_ts

    # NIFTY spot price
    nifty_price = float(macro.get("nifty", macro.get("nifty50", 0)) or 0)
    if nifty_price <= 0:
        return []

    option_type = "CE" if direction == "BULL" else "PE"
    sl_pts, t1_pts, t2_pts = 8, 15, 25

    if direction == "BULL":
        sl = round(nifty_price - sl_pts, 2)
        t1 = round(nifty_price + t1_pts, 2)
        t2 = round(nifty_price + t2_pts, 2)
    else:
        sl = round(nifty_price + sl_pts, 2)
        t1 = round(nifty_price - t1_pts, 2)
        t2 = round(nifty_price - t2_pts, 2)

    atm_strike = round(nifty_price / 50) * 50
    otm_strike = (atm_strike + 50) if direction == "BULL" else (atm_strike - 50)
    strike_lbl = (f"OTM {otm_strike}{option_type}" if heat_score >= HEAT_STRONG
                  else f"ATM {atm_strike}{option_type}")

    signal = {
        "symbol":        "NIFTY",
        "type":          direction.lower(),   # "bull" | "bear"
        "signal":        f"LONG {option_type}",
        "option_type":   option_type,
        "strike_label":  strike_lbl,
        "entry":         nifty_price,
        "sl":            sl,
        "t1":            t1,
        "t2":            t2,
        "sl_pts":        sl_pts,
        "t1_pts":        t1_pts,
        "t2_pts":        t2_pts,
        "heat_score":    heat_score,
        "confidence":    confidence,
        "basket_score":  basket["score"],
        "basket_aligned":basket["aligned"],
        "basket_total":  basket["total"],
        "pcr":           heat["pcr"],
        "vix":           heat["vix"],
        "nifty_chg":     heat["nifty_chg"],
        "time":          now_dt.strftime("%H:%M"),
        "ts":            now_ts,
        "contributors":  basket.get("contributors", []),
        "strength":      "hi" if confidence >= 70 else "md",
        "rr":            round(t1_pts / sl_pts, 1),
        "outcome":       None,
    }

    _live_signals.append(signal)
    if len(_live_signals) > 50:
        _live_signals = _live_signals[-50:]

    return [signal]


def get_live_signals() -> list:
    return list(_live_signals)


def get_live_basket_state(state: dict) -> dict:
    """Return basket + heat snapshot for the live dashboard panel."""
    stocks = state.get("stocks") or state.get("last_stocks") or []
    macro  = state.get("last_macro") or {}
    chain  = state.get("last_chain") or {}
    basket = compute_basket_score(stocks)
    heat   = compute_heat_score(basket, macro, chain)
    return {
        "basket":     basket,
        "heat":       heat,
        "heavyweights": [
            {"sym": sym, "wt": wt,
             "chg": float((next((s for s in stocks
                                  if str(s.get("symbol","")).upper() == sym),
                                {}) or {}).get("chg_pct", 0) or 0)}
            for sym, wt in NIFTY_HEAVYWEIGHTS[:8]
        ],
    }


# ══════════════════════════════════════════════════════════════════════════
# 4.  BACKTEST ENGINE
# ══════════════════════════════════════════════════════════════════════════
def _ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k    = 2.0 / (period + 1)
    emas = [values[0]]
    for v in values[1:]:
        emas.append(v * k + emas[-1] * (1 - k))
    return emas


def _simulate_apex_trades(bars: list) -> list:
    """
    Proxy signal on 1-min NIFTY bars — calibrated for realistic backtest volume.

    Signal conditions (basket sentiment not available historically, so proxy):
      1. Directional candle: close > open (bull) or close < open (bear)
      2. Close breaks above/below 5-bar EMA (local momentum)
      3. Body >= 35% of bar range (genuine directional move, not doji)
      4. Bar range >= 1.10× 20-bar average range (slight expansion = attention)
      5. Minimum bar range >= 3 pts (filter micro-noise)
      6. 10-bar cooldown between same-direction entries (~10 min separation)

    Time filter: 9:20–14:45, skipping 12:30–13:00 midday dead zone.
    Entry at next-bar open | T1=+15 pts | SL=-8 pts | T2=+25 pts
    Forced exit at 15:15 PM | approx option P&L via ATM delta 0.5, lot 75
    """
    trades = []
    n = len(bars)
    if n < 25:
        return trades

    closes = [float(b["close"]) for b in bars]
    highs  = [float(b["high"])  for b in bars]
    lows   = [float(b["low"])   for b in bars]
    opens_ = [float(b["open"])  for b in bars]
    emas5  = _ema(closes, 5)
    ranges = [highs[i] - lows[i] for i in range(n)]

    # Rolling 20-bar average range — vectorised O(n)
    avg20   = [0.0] * n
    rng_sum = 0.0
    for i in range(n):
        rng_sum += ranges[i]
        if i >= 20:
            rng_sum -= ranges[i - 20]
        avg20[i] = rng_sum / min(i + 1, 20)

    last_sig_bar: dict[str, int] = {"BULL": -999, "BEAR": -999}
    COOLDOWN = 10   # bars between same-direction signals

    i = 20
    while i < n - 2:
        bar = bars[i]
        dt  = bar["date"]
        if hasattr(dt, "tzinfo"):
            dt = dt.astimezone(IST) if dt.tzinfo else IST.localize(dt)
        cm = dt.hour * 60 + dt.minute

        # Backtest time gate: 9:20–14:45 skipping 12:30–13:00
        if cm < _BT_START or cm > _BT_END:
            i += 1
            continue
        if _BT_SKIP[0] <= cm < _BT_SKIP[1]:
            i += 1
            continue

        cl   = closes[i]
        op   = opens_[i]
        hi   = highs[i]
        lo   = lows[i]
        rng  = hi - lo
        body = abs(cl - op)
        avg  = avg20[i] if avg20[i] > 0 else rng

        body_pct  = body / rng if rng > 0 else 0
        vol_ratio = rng / avg  if avg  > 0 else 1.0

        bull_sig = (
            cl > op                               # up candle
            and cl > emas5[i]                     # above local EMA
            and body_pct  >= 0.35                 # real body (not doji)
            and vol_ratio >= 1.10                 # slight range expansion
            and rng       >= 3.0                  # at least 3 pts
            and (i - last_sig_bar["BULL"]) >= COOLDOWN
        )
        bear_sig = (
            cl < op                               # down candle
            and cl < emas5[i]                     # below local EMA
            and body_pct  >= 0.35
            and vol_ratio >= 1.10
            and rng       >= 3.0
            and (i - last_sig_bar["BEAR"]) >= COOLDOWN
        )

        if not bull_sig and not bear_sig:
            i += 1
            continue

        direction = "BULL" if bull_sig else "BEAR"
        last_sig_bar[direction] = i

        if i + 1 >= n:
            i += 1
            continue

        entry     = float(bars[i + 1]["open"])
        sl_pts, t1_pts, t2_pts = 8, 15, 25

        if direction == "BULL":
            sl_price = entry - sl_pts
            t1_price = entry + t1_pts
            t2_price = entry + t2_pts
        else:
            sl_price = entry + sl_pts
            t1_price = entry - t1_pts
            t2_price = entry - t2_pts

        # Outcome scan over next 90 bars
        outcome    = "EXPIRED"
        exit_price = entry
        exit_bar   = i + 1

        for j in range(i + 2, min(i + 91, n)):
            jbar = bars[j]
            jdt  = jbar["date"]
            if hasattr(jdt, "tzinfo"):
                jdt = jdt.astimezone(IST) if jdt.tzinfo else IST.localize(jdt)
            jcm = jdt.hour * 60 + jdt.minute

            if jcm >= 15 * 60 + 15:
                exit_price = float(jbar["close"])
                exit_bar   = j
                break

            jh = float(jbar["high"])
            jl = float(jbar["low"])

            if direction == "BULL":
                if jl <= sl_price:
                    exit_price, outcome, exit_bar = sl_price, "SL", j; break
                if jh >= t2_price:
                    exit_price, outcome, exit_bar = t2_price, "T2", j; break
                if jh >= t1_price:
                    exit_price, outcome, exit_bar = t1_price, "T1", j; break
            else:
                if jh >= sl_price:
                    exit_price, outcome, exit_bar = sl_price, "SL", j; break
                if jl <= t2_price:
                    exit_price, outcome, exit_bar = t2_price, "T2", j; break
                if jl <= t1_price:
                    exit_price, outcome, exit_bar = t1_price, "T1", j; break

        pnl_pts = round((exit_price - entry) * (1 if direction == "BULL" else -1), 2)
        pnl_rs  = round(pnl_pts * 0.5 * NIFTY_LOT_SIZE, 0)   # ATM delta ~0.5

        sig_dt = bars[i]["date"]
        if hasattr(sig_dt, "tzinfo"):
            sig_dt = sig_dt.astimezone(IST) if sig_dt.tzinfo else IST.localize(sig_dt)

        trades.append({
            "date":       sig_dt.strftime("%Y-%m-%d"),
            "time":       sig_dt.strftime("%H:%M"),
            "direction":  direction,
            "entry":      round(entry, 2),
            "sl":         round(sl_price, 2),
            "t1":         round(t1_price, 2),
            "t2":         round(t2_price, 2),
            "exit":       round(exit_price, 2),
            "outcome":    outcome,
            "pnl_pts":    pnl_pts,
            "pnl_rs":     int(pnl_rs),
            "vol_ratio":  round(vol_ratio, 2),
            "body_pct":   round(body_pct * 100, 1),
        })

        i = exit_bar + 1

    return trades


def _build_report(trades: list, from_date: str, to_date: str) -> dict:
    empty_summary = {
        "total": 0, "wins": 0, "losses": 0, "expired": 0,
        "win_rate": 0, "total_pnl_pts": 0, "total_pnl_rs": 0,
        "avg_pnl_pts": 0, "profit_factor": 0, "max_dd_pts": 0,
    }
    if not trades:
        return {"from": from_date, "to": to_date,
                "summary": empty_summary,
                "monthly": [], "daily": [], "trades": []}

    wins    = [t for t in trades if t["outcome"] in ("T1", "T2")]
    losses  = [t for t in trades if t["outcome"] == "SL"]
    expired = [t for t in trades if t["outcome"] == "EXPIRED"]

    total_pnl_pts = round(sum(t["pnl_pts"] for t in trades), 2)
    total_pnl_rs  = int(sum(t["pnl_rs"]  for t in trades))
    gross_win     = sum(t["pnl_pts"] for t in wins)
    gross_loss    = abs(sum(t["pnl_pts"] for t in losses))
    pf            = round(gross_win / gross_loss, 2) if gross_loss > 0 else 99.0
    win_rate      = round(len(wins) / len(trades) * 100, 1) if trades else 0
    avg_pnl       = round(total_pnl_pts / len(trades), 2) if trades else 0

    # Max drawdown
    running = peak = max_dd = 0.0
    for t in trades:
        running += t["pnl_pts"]
        if running > peak:
            peak = running
        dd = peak - running
        if dd > max_dd:
            max_dd = dd

    # Daily aggregation
    daily_map: dict[str, dict] = {}
    for t in trades:
        d = t["date"]
        if d not in daily_map:
            daily_map[d] = {"date": d, "signals": 0, "wins": 0,
                            "losses": 0, "expired": 0,
                            "pnl_pts": 0.0, "pnl_rs": 0}
        daily_map[d]["signals"] += 1
        daily_map[d]["pnl_pts"]  = round(daily_map[d]["pnl_pts"] + t["pnl_pts"], 2)
        daily_map[d]["pnl_rs"]  += int(t["pnl_rs"])
        if t["outcome"] in ("T1", "T2"):  daily_map[d]["wins"]    += 1
        elif t["outcome"] == "SL":        daily_map[d]["losses"]  += 1
        else:                             daily_map[d]["expired"]  += 1
    daily = sorted(daily_map.values(), key=lambda x: x["date"], reverse=True)

    # Monthly aggregation
    monthly_map: dict[str, dict] = {}
    for t in trades:
        m = t["date"][:7]
        if m not in monthly_map:
            monthly_map[m] = {"month": m, "signals": 0, "wins": 0,
                              "losses": 0, "expired": 0,
                              "pnl_pts": 0.0, "pnl_rs": 0}
        monthly_map[m]["signals"] += 1
        monthly_map[m]["pnl_pts"]  = round(monthly_map[m]["pnl_pts"] + t["pnl_pts"], 2)
        monthly_map[m]["pnl_rs"]  += int(t["pnl_rs"])
        if t["outcome"] in ("T1", "T2"):  monthly_map[m]["wins"]   += 1
        elif t["outcome"] == "SL":        monthly_map[m]["losses"] += 1
        else:                             monthly_map[m]["expired"] += 1
    for v in monthly_map.values():
        v["win_rate"] = round(v["wins"] / v["signals"] * 100, 1) if v["signals"] else 0
    monthly = sorted(monthly_map.values(), key=lambda x: x["month"], reverse=True)

    return {
        "from": from_date,
        "to":   to_date,
        "summary": {
            "total":         len(trades),
            "wins":          len(wins),
            "losses":        len(losses),
            "expired":       len(expired),
            "win_rate":      win_rate,
            "total_pnl_pts": total_pnl_pts,
            "total_pnl_rs":  total_pnl_rs,
            "avg_pnl_pts":   avg_pnl,
            "profit_factor": pf,
            "max_dd_pts":    round(max_dd, 2),
        },
        "monthly": monthly,
        "daily":   daily,
        "trades":  list(reversed(trades)),  # most-recent first
    }


def run_apex_backtest(from_date: str, to_date: str) -> dict:
    """
    Public entry-point: fetch NIFTY 1-min data from Kite and run simulation.
    Kite allows 60-day chunks for minute interval; we chunk automatically.
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
        return {"error": f"Invalid date format: {e}"}

    if (to_dt - from_dt).days > 366:
        return {"error": "Max range is 366 days"}
    if to_dt < from_dt:
        return {"error": "to_date must be >= from_date"}

    # Fetch in 55-day chunks (safe margin under Kite's 60-day minute limit)
    all_bars: list = []
    chunk_start    = from_dt
    while chunk_start <= to_dt:
        chunk_end = min(chunk_start + timedelta(days=55), to_dt)
        try:
            bars = kite.historical_data(
                NIFTY_TOKEN,
                chunk_start.date(),
                chunk_end.date(),
                "minute",
            )
            all_bars.extend(bars)
            logger.info(
                "apex_backtest: fetched %d bars %s→%s",
                len(bars), chunk_start.date(), chunk_end.date(),
            )
        except Exception as e:
            logger.warning("apex_backtest fetch error %s: %s", chunk_start.date(), e)
        chunk_start = chunk_end + timedelta(days=1)

    if not all_bars:
        return {"error": "No data returned from Kite for the given range — market may have been closed"}

    logger.info("apex_backtest: total bars fetched = %d, running simulation…", len(all_bars))
    trades = _simulate_apex_trades(all_bars)
    logger.info("apex_backtest: simulation complete, trades = %d", len(trades))
    report = _build_report(trades, from_date, to_date)
    report["debug"] = {
        "bars_fetched": len(all_bars),
        "trades_found": len(trades),
        "first_bar":    str(all_bars[0]["date"])  if all_bars else None,
        "last_bar":     str(all_bars[-1]["date"]) if all_bars else None,
    }
    return report
