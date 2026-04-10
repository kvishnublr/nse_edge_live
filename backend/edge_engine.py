"""
NSE EDGE v5 — Production Edge Engine
4-Layer multiplicative signal model with GEX, IV Rank, LTQ, ADX, regime gate.
Exposes:  compute_edge_snapshot()  →  full snapshot dict
          compute_gex(chain)       →  GEX per strike
          run_edge_backtest(days)  →  historical trade log
"""

import time
import logging
import sqlite3
import statistics
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import pytz

logger = logging.getLogger("edge_engine")
_IST = pytz.timezone("Asia/Kolkata")

DB_PATH = Path(__file__).parent / "data" / "backtest.db"

# ─── SIGNAL BUS (3-min TTL window) ────────────────────────────────────────────
_signal_bus: dict = {}          # {signal_key: {score:0-1, ts:float, label:str}}
_bus_ttl = 180                  # seconds — stale signals weight → 0

def post_signal(key: str, score: float, label: str = ""):
    _signal_bus[key] = {"score": max(0.0, min(1.0, score)), "ts": time.time(), "label": label}

def _fresh(entry: dict) -> float:
    """Return weight 0-1 based on age vs TTL."""
    age = time.time() - entry.get("ts", 0)
    if age >= _bus_ttl:
        return 0.0
    return 1.0 - (age / _bus_ttl) * 0.35   # linear decay to 0.65 at TTL

# ─── LTQ TRACKER ──────────────────────────────────────────────────────────────
_ltq_history: deque = deque(maxlen=50)    # rolling 50 ticks
_ltq_last_signal: float = 0

def update_ltq(ltq: float, side: str, ltp: float, vwap: float):
    """Call on every tick from price feed. side='buy'|'sell'."""
    global _ltq_last_signal
    _ltq_history.append(ltq)
    if len(_ltq_history) < 20:
        return
    mu = statistics.mean(_ltq_history)
    if mu == 0:
        return
    try:
        sd = statistics.stdev(_ltq_history) or (mu * 0.5)
        z  = (ltq - mu) / sd
    except Exception:
        return

    now = time.time()
    if abs(z) >= 2.5 and (now - _ltq_last_signal) > 60:
        _ltq_last_signal = now
        above_vwap = ltp >= vwap if vwap else True
        if side == "buy" and above_vwap:
            post_signal("ltq_buy",  min(z / 4, 1.0), f"LTQ spike z={z:.1f} BUY above VWAP")
        elif side == "sell" and not above_vwap:
            post_signal("ltq_sell", min(z / 4, 1.0), f"LTQ spike z={z:.1f} SELL below VWAP")

# ─── VWAP σ BANDS ─────────────────────────────────────────────────────────────
def calc_vwap_bands(price_hist: list) -> dict:
    """price_hist = [(ts, price, vol), ...]"""
    if len(price_hist) < 5:
        return {"vwap": 0, "sd": 0, "band1": 0, "band2": 0}
    try:
        tp_vol = sum(p * v for _, p, v in price_hist if v > 0)
        vol    = sum(v     for _, p, v in price_hist if v > 0)
        vwap   = tp_vol / vol if vol else statistics.mean(p for _, p, v in price_hist)
        prices = [p for _, p, _ in price_hist]
        sd     = statistics.stdev(prices) if len(prices) > 1 else 0
        return {"vwap": round(vwap, 2), "sd": round(sd, 2),
                "band1": round(vwap + sd, 2), "band1d": round(vwap - sd, 2),
                "band2": round(vwap + 2*sd, 2), "band2d": round(vwap - 2*sd, 2)}
    except Exception:
        return {"vwap": 0, "sd": 0, "band1": 0, "band2": 0}

# ─── ADX CALCULATION ──────────────────────────────────────────────────────────
def calc_adx(closes: list, period: int = 14) -> float:
    """Returns ADX from list of close prices (proxy using close-to-close)."""
    if len(closes) < period + 2:
        return 20.0   # neutral default
    try:
        ups, dns = [], []
        for i in range(1, len(closes)):
            diff = closes[i] - closes[i-1]
            ups.append(max(diff, 0))
            dns.append(max(-diff, 0))
        dm_pos = ups[-(period):]
        dm_neg = dns[-(period):]
        atr    = [abs(closes[i] - closes[i-1]) for i in range(-period, 0)]
        atr_s  = sum(atr) or 1
        di_pos = 100 * sum(dm_pos) / atr_s
        di_neg = 100 * sum(dm_neg) / atr_s
        dx     = 100 * abs(di_pos - di_neg) / (di_pos + di_neg + 0.001)
        return round(dx, 1)
    except Exception:
        return 20.0

# ─── IV RANK ──────────────────────────────────────────────────────────────────
def calc_iv_rank(current_iv: float, vix_hist: list) -> dict:
    """
    vix_hist: list of daily VIX values (52 weeks).
    Returns iv_rank (0-100), iv_percentile (0-100), recommendation.
    """
    if not vix_hist or not current_iv:
        return {"iv_rank": 50, "iv_pct": 50, "action": "neutral", "label": "—"}
    try:
        lo, hi = min(vix_hist), max(vix_hist)
        rank   = round((current_iv - lo) / (hi - lo + 0.001) * 100, 1)
        pct    = round(sum(1 for v in vix_hist if v <= current_iv) / len(vix_hist) * 100, 1)
        if pct < 30:
            action, label = "BUY_OPTIONS",  f"IV cheap (pct {pct:.0f}%) — buy options"
        elif pct > 70:
            action, label = "SELL_OPTIONS", f"IV expensive (pct {pct:.0f}%) — sell/spread"
        else:
            action, label = "NEUTRAL",      f"IV normal (pct {pct:.0f}%)"
        return {"iv_rank": rank, "iv_pct": pct, "action": action, "label": label}
    except Exception:
        return {"iv_rank": 50, "iv_pct": 50, "action": "neutral", "label": "—"}

# ─── GEX COMPUTATION ──────────────────────────────────────────────────────────
def compute_gex(chain: dict) -> dict:
    """
    chain: dict from fetcher.fetch_option_chain() → {strikes:[{strike, ce_oi, pe_oi, ce_iv, pe_iv}]}
    Returns per-strike GEX, gravity strike, and explosion strikes.
    """
    if not chain or not isinstance(chain, dict):
        return {"strikes": [], "gravity": None, "explosion_up": None, "explosion_dn": None, "error": "no chain"}

    lot_size = 50   # Nifty lot size
    strikes_raw = chain.get("strikes") or chain.get("data") or []
    if not strikes_raw:
        # try flat list format
        return {"strikes": [], "gravity": None, "explosion_up": None, "explosion_dn": None, "error": "empty chain"}

    ul_price = chain.get("ul_price") or chain.get("spot") or 0

    results = []
    for row in strikes_raw:
        try:
            k      = float(row.get("strike") or row.get("s") or 0)
            ce_oi  = float(row.get("ce_oi")  or row.get("call_oi")  or 0)
            pe_oi  = float(row.get("pe_oi")  or row.get("put_oi")   or 0)
            ce_iv  = float(row.get("ce_iv")  or row.get("call_iv")  or 20) / 100
            pe_iv  = float(row.get("pe_iv")  or row.get("put_iv")   or 20) / 100
            if k == 0:
                continue
            # Simplified BS delta approximation (no time, just moneyness)
            dist   = (ul_price - k) / (ul_price * 0.01 + 1) if ul_price else 0
            ce_d   = max(0.05, min(0.95, 0.5 + dist * 0.05))
            pe_d   = max(0.05, min(0.95, 0.5 - dist * 0.05))
            # Gamma proxy: peaked at ATM, decays with distance
            ce_g   = max(0, 0.04 * (1 - abs(dist) * 0.15))
            pe_g   = max(0, 0.04 * (1 - abs(dist) * 0.15))
            # GEX = OI × Gamma × LotSize (positive = dealer long gamma, pins price)
            gex    = round((ce_oi * ce_g - pe_oi * pe_g) * lot_size / 1e6, 3)
            results.append({
                "strike": k, "ce_oi": int(ce_oi), "pe_oi": int(pe_oi),
                "gex": gex,
                "ce_iv_pct": round(ce_iv * 100, 1), "pe_iv_pct": round(pe_iv * 100, 1),
                "skew": round((pe_iv - ce_iv) * 100, 1)
            })
        except Exception:
            continue

    if not results:
        return {"strikes": [], "gravity": None, "explosion_up": None, "explosion_dn": None, "error": "parse_fail"}

    results.sort(key=lambda x: x["strike"])
    # Gravity = max positive GEX (price gets pinned here)
    pos = [r for r in results if r["gex"] > 0]
    neg = [r for r in results if r["gex"] < 0]
    gravity     = max(pos,  key=lambda x: x["gex"])  ["strike"] if pos else None
    exp_up_r    = [r for r in neg if r["strike"] > (ul_price or 0)]
    exp_dn_r    = [r for r in neg if r["strike"] < (ul_price or 0)]
    exp_up      = min(exp_up_r, key=lambda x: x["strike"])["strike"] if exp_up_r else None
    exp_dn      = max(exp_dn_r, key=lambda x: x["strike"])["strike"] if exp_dn_r else None
    total_gex   = round(sum(r["gex"] for r in results), 3)
    # Skew: avg (put IV - call IV) near ATM strikes
    atm_rows    = [r for r in results if ul_price and abs(r["strike"] - ul_price) <= ul_price * 0.02]
    avg_skew    = round(statistics.mean(r["skew"] for r in atm_rows), 2) if atm_rows else 0
    skew_signal = "FEAR" if avg_skew > 4 else "NORMAL" if avg_skew > 0 else "GREED"

    return {
        "strikes": results,
        "gravity":      gravity,
        "explosion_up": exp_up,
        "explosion_dn": exp_dn,
        "total_gex":    total_gex,
        "avg_skew":     avg_skew,
        "skew_signal":  skew_signal,
        "ul_price":     ul_price,
    }

# ─── PCR SIGNAL ───────────────────────────────────────────────────────────────
def pcr_signal(pcr: float, pcr_hist: list) -> dict:
    """
    pcr_hist: list of last N PCR values.
    Edge is in CHANGE FROM EXTREME, not level.
    """
    if not pcr or pcr <= 0:
        return {"signal": "neutral", "score": 0.5, "label": "No PCR data"}
    trend = ""
    score = 0.5
    if len(pcr_hist) >= 3:
        recent  = pcr_hist[-1]
        earlier = pcr_hist[-3]
        if earlier > 1.5 and recent < earlier:      # recovering from extreme high
            trend, score = "BULLISH_REVERSION", 0.78
        elif earlier < 0.7 and recent > earlier:    # recovering from extreme low
            trend, score = "BEARISH_REVERSION", 0.22
        elif pcr > 1.4:
            trend, score = "PUT_HEAVY", 0.62         # mild bullish (contrarian)
        elif pcr < 0.7:
            trend, score = "CALL_HEAVY", 0.38        # mild bearish
        else:
            trend, score = "NEUTRAL", 0.50
    label = f"PCR {pcr:.2f} → {trend}" if trend else f"PCR {pcr:.2f}"
    return {"signal": trend, "score": score, "label": label}

# ─── OI DELTA VELOCITY ────────────────────────────────────────────────────────
_oi_history: deque = deque(maxlen=12)   # (ts, oi_ce, oi_pe)

def update_oi(ce_oi: float, pe_oi: float):
    _oi_history.append((time.time(), ce_oi, pe_oi))

def calc_oi_velocity() -> dict:
    """How fast is OI building vs 5 minutes ago?"""
    if len(_oi_history) < 6:
        return {"ce_vel": 0, "pe_vel": 0, "signal": "neutral"}
    try:
        now_ce  = _oi_history[-1][1]
        now_pe  = _oi_history[-1][2]
        old_ce  = _oi_history[-6][1]
        old_pe  = _oi_history[-6][2]
        ce_vel  = (now_ce - old_ce) / (old_ce + 1)
        pe_vel  = (now_pe - old_pe) / (old_pe + 1)
        if pe_vel > ce_vel and pe_vel > 0.01:
            sig = "PE_BUILD"    # puts building → protect / bearish hedge
        elif ce_vel > pe_vel and ce_vel > 0.01:
            sig = "CE_BUILD"    # calls building → bullish positioning
        else:
            sig = "NEUTRAL"
        return {"ce_vel": round(ce_vel * 100, 2), "pe_vel": round(pe_vel * 100, 2), "signal": sig}
    except Exception:
        return {"ce_vel": 0, "pe_vel": 0, "signal": "neutral"}

# ─── SESSION TIME GATE ─────────────────────────────────────────────────────────
def session_gate() -> dict:
    """Returns current session window status."""
    now_ist = datetime.now(_IST)
    h, m = now_ist.hour, now_ist.minute
    mins  = h * 60 + m

    windows = [
        (0,   555,  "PRE_OPEN",   0.0,  "Before market"),
        (555, 560,  "OPEN_NOISE", 0.0,  "9:15–9:20 pure noise — skip"),
        (560, 675,  "PRIME_A",    1.0,  "9:20–11:15 ✓ Prime window A"),
        (675, 780,  "CHOP",       0.0,  "11:15–13:00 chop — skip"),
        (780, 870,  "PRIME_B",    1.0,  "13:00–14:30 ✓ Prime window B"),
        (870, 900,  "THETA_BURN", 0.0,  "14:30–15:00 theta decay — no new"),
        (900, 930,  "NO_TRADE",   0.0,  "15:00–15:30 illiquid — never"),
        (930, 9999, "CLOSED",     0.0,  "Post-close"),
    ]
    day = now_ist.weekday()
    if day >= 5:   # Saturday / Sunday
        return {"window": "CLOSED", "gate": 0.0, "label": "Weekend — market closed", "mins": mins}

    for lo, hi, name, gate, label in windows:
        if lo <= mins < hi:
            return {"window": name, "gate": gate, "label": label, "mins": mins}

    return {"window": "CLOSED", "gate": 0.0, "label": "Closed", "mins": mins}

# ─── REGIME CLASSIFIER ────────────────────────────────────────────────────────
def classify_regime(vix: float, adx: float, nifty_chg_pct: float, pcr: float) -> dict:
    """
    Returns regime dict: {regime, confidence, size_multiplier, strategy, label}
    """
    regime = "NORMAL"
    conf   = 0.6
    size   = 1.0
    strat  = "BREAKOUT + MEAN_REV"
    notes  = []

    # VIX buckets
    if vix < 13:
        notes.append(f"VIX {vix:.1f} → low-vol, breakouts clean")
        regime = "LOW_VOL"
        conf   = 0.75
        size   = 1.0
        strat  = "BREAKOUT"
    elif vix <= 18:
        notes.append(f"VIX {vix:.1f} → normal, both strategies work")
        regime = "NORMAL"
        conf   = 0.70
        size   = 1.0
        strat  = "BREAKOUT + MEAN_REV"
    elif vix <= 22:
        notes.append(f"VIX {vix:.1f} → elevated, trade with institutional flow only, size ↓50%")
        regime = "ELEVATED"
        conf   = 0.55
        size   = 0.5
        strat  = "WITH_FLOW_ONLY"
    else:
        notes.append(f"VIX {vix:.1f} → DANGER — fake moves dominate, use straddles not directional")
        regime = "HIGH_VOL"
        conf   = 0.35
        size   = 0.0   # gate = 0, no directional
        strat  = "STRADDLE_ONLY"

    # ADX overlay
    if adx > 22:
        notes.append(f"ADX {adx:.1f} → trending, prefer breakout signals")
        if regime in ("LOW_VOL", "NORMAL"):
            strat = "BREAKOUT"
            conf  = min(conf + 0.08, 0.95)
    elif adx < 18:
        notes.append(f"ADX {adx:.1f} → ranging, prefer mean reversion")
        if regime in ("LOW_VOL", "NORMAL"):
            strat = "MEAN_REVERSION"
    else:
        notes.append(f"ADX {adx:.1f} → ambiguous, require L1+L2 alignment")
        conf  = max(conf - 0.08, 0.20)

    gate = 0 if regime == "HIGH_VOL" else 1
    return {
        "regime":    regime,
        "confidence": round(conf, 2),
        "size_mult": size,
        "gate":      gate,
        "strategy":  strat,
        "vix":       round(vix, 1),
        "adx":       round(adx, 1),
        "notes":     notes,
        "label":     f"{regime} | {strat} | {int(size*100)}% size"
    }

# ─── COMPOSITE SCORE (MULTIPLICATIVE) ─────────────────────────────────────────
COMPOSITE_THRESHOLD = 0.72

def compute_composite(l1: float, l2: float, l3: float, regime_gate: int) -> dict:
    """
    final = regime_gate × (L1×0.50 + L2×0.30 + L3×0.20)
    """
    weighted = l1 * 0.50 + l2 * 0.30 + l3 * 0.20
    final    = regime_gate * weighted
    action   = "EXECUTE" if final >= COMPOSITE_THRESHOLD else \
               "WATCH"   if final >= 0.55 else \
               "SKIP"
    return {
        "l1": round(l1, 3), "l2": round(l2, 3), "l3": round(l3, 3),
        "weighted": round(weighted, 3),
        "final":    round(final, 3),
        "action":   action,
        "threshold": COMPOSITE_THRESHOLD,
        "pct":      round(final * 100, 1),
    }

# ─── RISK ENGINE ──────────────────────────────────────────────────────────────
def compute_position_size(account: float, entry: float, sl: float,
                          delta: float = 0.50, lot_size: int = 50) -> dict:
    """
    Returns optimal lots based on 1.5% risk rule.
    """
    if entry <= 0 or sl <= 0 or entry == sl:
        return {"lots": 0, "capital": 0, "risk_pct": 0, "note": "invalid inputs"}
    risk_pts      = abs(entry - sl)
    risk_per_lot  = risk_pts * delta * lot_size
    max_risk_cash = account * 0.015
    lots          = int(max_risk_cash / risk_per_lot) if risk_per_lot > 0 else 0
    lots          = max(0, min(lots, 50))   # cap at 50 lots
    actual_risk   = lots * risk_per_lot
    pct           = round(actual_risk / account * 100, 2) if account > 0 else 0
    return {
        "lots":       lots,
        "capital":    round(lots * entry * lot_size, 0),
        "risk_cash":  round(actual_risk, 0),
        "risk_pct":   pct,
        "risk_pts":   round(risk_pts, 1),
        "note":       f"{lots} lots × ₹{int(risk_per_lot)}/lot risk"
    }

# ─── ADVANCE-DECLINE BREADTH ──────────────────────────────────────────────────
NIFTY_HEAVYWEIGHTS = [
    "HDFCBANK","RELIANCE","ICICIBANK","INFY","TCS",
    "KOTAKBANK","LT","AXISBANK","SBIN","ITC",
    "BHARTIARTL","HINDUNILVR","BAJFINANCE","WIPRO","ASIANPAINT"
]

def calc_breadth(prices: dict, vwap_ref: dict = None) -> dict:
    """
    prices: {symbol: {price, ...}}
    vwap_ref: {symbol: vwap}  (if available)
    Returns: above_vwap count, breadth_score 0-1
    """
    above = 0
    total = 0
    detail = []
    for sym in NIFTY_HEAVYWEIGHTS:
        p = prices.get(sym) or {}
        if not p:
            continue
        ltp  = p.get("price", 0)
        prev = p.get("prev", ltp)
        vwap = (vwap_ref or {}).get(sym, prev)
        if ltp and vwap:
            total += 1
            up = ltp >= vwap
            if up:
                above += 1
            detail.append({"sym": sym, "above": up, "ltp": ltp})

    score = above / total if total > 0 else 0.5
    signal = "STRONG" if above >= 10 else "MODERATE" if above >= 7 else "WEAK"
    return {
        "above": above, "total": total,
        "score": round(score, 2),
        "signal": signal,
        "detail": detail,
        "label": f"{above}/{total} heavyweights above VWAP → {signal}"
    }

# ─── FULL EDGE SNAPSHOT ───────────────────────────────────────────────────────
def compute_edge_snapshot(state: dict = None, prices: dict = None, chain: dict = None) -> dict:
    """
    Main entry. Reads live signals state, prices, and chain.
    Returns complete edge snapshot dict for API/WS broadcast.
    """
    import signals as sig_mod
    from feed import price_history

    state  = state  or sig_mod.state
    prices = prices or {}

    # ── Extract live data from state ────────────────────────────────────────
    indices   = (state.get("last_macro") or {})
    chain_d   = chain or state.get("last_chain") or {}
    fii       = state.get("last_fii") or {}
    stocks_p  = prices

    vix       = float(indices.get("vix",  15) or 15)
    pcr       = float(indices.get("pcr",  1.0) or 1.0)
    nifty     = float(indices.get("nifty", 0) or 0)
    nifty_chg = float(indices.get("nifty_chg", 0) or 0)

    # ── ADX from NIFTY close history ────────────────────────────────────────
    nifty_hist = list(price_history.get("NIFTY", []))
    closes     = [h[1] for h in nifty_hist[-50:]] if nifty_hist else []
    adx        = calc_adx(closes) if len(closes) >= 16 else 20.0

    # ── VWAP bands for Nifty ────────────────────────────────────────────────
    vwap_data  = calc_vwap_bands(nifty_hist[-200:]) if len(nifty_hist) >= 10 else {}
    vwap       = vwap_data.get("vwap", 0)

    vwap_signal = "NEUTRAL"
    vwap_score  = 0.5
    if vwap and nifty:
        sd  = vwap_data.get("sd", 0) or 1
        dev = (nifty - vwap) / sd
        if abs(dev) >= 2.0:
            vwap_signal = "REVERSION" if abs(dev) >= 2.0 else "STRETCH"
            vwap_score  = 0.72 if abs(dev) >= 2.0 else 0.60
        elif abs(dev) <= 0.5:
            vwap_signal = "AT_VWAP"
            vwap_score  = 0.55

    # ── Layer 0 — Regime ────────────────────────────────────────────────────
    session  = session_gate()
    regime_d = classify_regime(vix, adx, nifty_chg, pcr)
    session_gate_val = session["gate"]
    regime_gate_val  = regime_d["gate"]
    l0_gate  = 1 if (session_gate_val == 1 and regime_gate_val == 1) else 0

    # ── Layer 1 — Order Flow ────────────────────────────────────────────────
    oi_vel   = calc_oi_velocity()
    # LTQ signals from bus
    ltq_buy  = _signal_bus.get("ltq_buy",  {"score": 0, "ts": 0})
    ltq_sell = _signal_bus.get("ltq_sell", {"score": 0, "ts": 0})
    ltq_score= max(ltq_buy["score"] * _fresh(ltq_buy),
                   ltq_sell["score"] * _fresh(ltq_sell))
    # VWAP deviation signal
    oi_score = 0.65 if oi_vel["signal"] in ("PE_BUILD", "CE_BUILD") else 0.40
    l1_raw   = ltq_score * 0.50 + oi_score * 0.30 + vwap_score * 0.20
    l1_score = round(min(l1_raw, 1.0), 3)

    l1_signals = [
        {"k": "LTQ Spike",    "v": f"z-score signal" if ltq_score > 0.4 else "—",
         "score": round(ltq_score, 2), "c": "cg" if ltq_score > 0.5 else "cm"},
        {"k": "OI Velocity",  "v": oi_vel["signal"], "score": round(oi_score, 2),
         "c": "cg" if oi_vel["signal"] != "NEUTRAL" else "cm"},
        {"k": "VWAP Dev",     "v": vwap_signal if vwap else "—",
         "score": round(vwap_score, 2), "c": "cg" if vwap_score > 0.55 else "cm"},
    ]
    if vwap:
        l1_signals.append({"k": "VWAP", "v": f"{vwap:.0f}", "score": 0, "c": "cb"})

    # ── Layer 2 — Options Surface ───────────────────────────────────────────
    # IV Rank from VIX 52-week history in DB
    vix_52w     = _get_vix_history(252)
    iv_rank_d   = calc_iv_rank(vix, vix_52w)
    iv_score    = 1.0 - abs(iv_rank_d["iv_pct"] - 50) / 100   # peaks at mid, useful signal quality

    # GEX
    gex_d = compute_gex(chain_d)
    gex_score = 0.5
    if gex_d.get("gravity") and nifty:
        dist = abs(gex_d["gravity"] - nifty) / (nifty * 0.01 + 1)
        gex_score = max(0.3, 0.85 - dist * 0.04)  # closer to gravity → higher pin probability

    # PCR signal (need history)
    pcr_hist  = _get_pcr_history(10)
    pcr_d     = pcr_signal(pcr, pcr_hist + [pcr])
    skew_score = 0.5
    if gex_d.get("avg_skew") is not None:
        sk = gex_d["avg_skew"]
        if sk > 4:
            skew_score = 0.70   # fear premium — protect short delta
        elif sk < 0:
            skew_score = 0.65   # unusual — greed premium

    l2_score = round(iv_score * 0.35 + gex_score * 0.35 + pcr_d["score"] * 0.30, 3)
    l2_signals = [
        {"k": "IV Rank",  "v": iv_rank_d["label"],  "score": round(iv_score, 2),
         "c": "cg" if iv_rank_d["action"] != "SELL_OPTIONS" else "cr"},
        {"k": "GEX",      "v": f"Gravity {gex_d.get('gravity','—')} | Skew {gex_d.get('avg_skew','—')}",
         "score": round(gex_score, 2), "c": "cg" if gex_score > 0.55 else "cm"},
        {"k": "PCR",      "v": pcr_d["label"],       "score": round(pcr_d["score"], 2),
         "c": "cg" if pcr_d["score"] > 0.6 else "cr" if pcr_d["score"] < 0.4 else "cm"},
    ]

    # ── Layer 3 — Structure ─────────────────────────────────────────────────
    breadth_d = calc_breadth(stocks_p)
    b_score   = breadth_d["score"]
    # Gate scores from existing signal engine (1-5)
    g3 = (state.get("gates") or {}).get(3, {})
    g4 = (state.get("gates") or {}).get(4, {})
    str_score = (g3.get("score", 50) / 100 + g4.get("score", 50) / 100) / 2
    l3_score  = round(str_score * 0.60 + b_score * 0.40, 3)

    l3_signals = [
        {"k": "Breadth",  "v": breadth_d["label"], "score": round(b_score, 2),
         "c": "cg" if b_score >= 0.67 else "cr" if b_score < 0.40 else "cm"},
        {"k": "Structure","v": g3.get("state","wt").upper(),
         "score": round(str_score, 2), "c": "cg" if str_score > 0.6 else "cm"},
        {"k": "Trigger",  "v": g4.get("state","wt").upper(),
         "score": round(str_score, 2), "c": "cg" if str_score > 0.6 else "cm"},
    ]

    # ── Composite ───────────────────────────────────────────────────────────
    composite = compute_composite(l1_score, l2_score, l3_score, l0_gate)

    # ── Risk defaults (account placeholder) ─────────────────────────────────
    account   = 500000   # ₹5L default — user can override in UI
    risk_info = {}
    if nifty > 0:
        sl_pts  = max(adx * 1.5, 30)   # ADX-based SL
        sl      = nifty - sl_pts if composite.get("action") == "EXECUTE" else nifty + sl_pts
        # ATM option price proxy
        entry_opt = round(vix / 100 * nifty * 0.04, 0)   # rough IV × spot × sqrt(t)
        risk_info = compute_position_size(account, max(entry_opt, 50), max(entry_opt * 0.5, 20),
                                          delta=0.50, lot_size=50)
        risk_info["sl_pts"] = round(sl_pts, 0)

    return {
        "ts": time.time(),
        "regime":    regime_d,
        "session":   session,
        "l0_gate":   l0_gate,
        "l1":        {"score": l1_score, "signals": l1_signals},
        "l2":        {"score": l2_score, "signals": l2_signals, "gex": gex_d, "iv_rank": iv_rank_d},
        "l3":        {"score": l3_score, "signals": l3_signals},
        "composite": composite,
        "risk":      risk_info,
        "breadth":   breadth_d,
        "vwap":      vwap_data,
        "oi_vel":    oi_vel,
        "meta":      {"vix": vix, "adx": adx, "pcr": pcr, "nifty": nifty},
    }

# ─── DB HELPERS ───────────────────────────────────────────────────────────────
def _get_vix_history(days: int = 252) -> list:
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(
            "SELECT vix FROM vix_daily ORDER BY date DESC LIMIT ?", (days,)
        ).fetchall()
        conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []

def _get_pcr_history(days: int = 10) -> list:
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(
            "SELECT pcr FROM chain_daily ORDER BY date DESC LIMIT ?", (days,)
        ).fetchall()
        conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []

def _get_ohlcv_history(days: int = 60) -> list:
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute(
            "SELECT date,open,high,low,close,volume FROM ohlcv ORDER BY date DESC LIMIT ?",
            (days,)
        ).fetchall()
        conn.close()
        return [{"date":r[0],"open":r[1],"high":r[2],"low":r[3],"close":r[4],"vol":r[5]} for r in rows]
    except Exception:
        return []

# ─── BACKTEST ENGINE ──────────────────────────────────────────────────────────
def run_edge_backtest(days: int = 90) -> dict:
    """
    Replay the composite model on historical DB data.
    Returns: {trades:[...], stats:{...}, equity_curve:[...]}
    Uses: ohlcv, vix_daily, chain_daily, fii_daily
    """
    try:
        conn = sqlite3.connect(str(DB_PATH))
        rows = conn.execute("""
            SELECT o.date, o.open, o.high, o.low, o.close, o.volume,
                   v.vix, v.vix_chg,
                   c.pcr, c.total_call_oi, c.total_put_oi, c.ul_price,
                   f.fii_net
            FROM ohlcv o
            LEFT JOIN vix_daily   v ON o.date = v.date
            LEFT JOIN chain_daily c ON o.date = c.date
            LEFT JOIN fii_daily   f ON o.date = f.date
            ORDER BY o.date DESC LIMIT ?
        """, (days,)).fetchall()
        conn.close()
    except Exception as e:
        return {"error": str(e), "trades": [], "stats": {}}

    if not rows:
        return {"error": "no_data", "trades": [], "stats": {},
                "hint": "Download historical data first via Settings → Download Historical Data"}

    rows = list(reversed(rows))   # chronological order
    trades = []
    equity = 0.0
    wins = losses = 0
    equity_curve = []
    pcr_buf  = []
    vix_buf  = []
    close_buf = []

    for i, r in enumerate(rows):
        dt, op, hi, lo, cl, vol, vix, vix_chg, pcr, ce_oi, pe_oi, ul, fii = r
        vix     = vix  or 15.0
        pcr     = pcr  or 1.0
        fii_net = fii  or 0
        nifty   = cl   or 0
        chg_pct = ((cl - op) / op * 100) if op else 0

        close_buf.append(cl or 0)
        vix_buf.append(vix)
        pcr_buf.append(pcr)

        if i < 20:    # warmup
            continue

        # Layer 0
        adx       = calc_adx(close_buf[-30:])
        regime_d  = classify_regime(vix, adx, chg_pct, pcr)
        l0_gate   = regime_d["gate"]

        # Layer 1 proxy (no tick data in backtest — use price action)
        hi_prev   = rows[i-1][2] if i > 0 else hi
        lo_prev   = rows[i-1][3] if i > 0 else lo
        cl_prev   = rows[i-1][4] if i > 0 else cl
        breakout  = hi > hi_prev * 1.003   # 0.3% above prev high
        breakdown = lo < lo_prev * 0.997
        vwap_d    = nifty    # use close as proxy for daily context
        ltq_proxy = 0.65 if (breakout or breakdown) else 0.40
        oi_vel_p  = 0.6 if pcr > 1.3 or pcr < 0.75 else 0.45
        dev_score = 0.60   # neutral — no intraday tick data
        l1_score  = ltq_proxy * 0.50 + oi_vel_p * 0.30 + dev_score * 0.20

        # Layer 2 proxy
        vix52_hi  = max(vix_buf)
        vix52_lo  = min(vix_buf)
        iv_pct    = (vix - vix52_lo) / (vix52_hi - vix52_lo + 0.001) * 100
        iv_score  = 1.0 - abs(iv_pct - 50) / 100
        pcr_d     = pcr_signal(pcr, pcr_buf[-5:])
        gex_proxy = 0.60 if abs(chg_pct) < 0.8 else 0.45   # low vol day = pin likely
        l2_score  = iv_score * 0.35 + gex_proxy * 0.35 + pcr_d["score"] * 0.30

        # Layer 3 proxy
        # ADX structure + trend consistency
        above_prev = cl > cl_prev
        trend_cnt  = sum(1 for j in range(max(0,i-5), i) if rows[j][4] > rows[j-1][4])
        str_score  = trend_cnt / 5
        breadth_p  = 0.65 if str_score > 0.6 else 0.40
        l3_score   = str_score * 0.60 + breadth_p * 0.40

        comp = compute_composite(l1_score, l2_score, l3_score, l0_gate)
        final = comp["final"]

        if final < COMPOSITE_THRESHOLD:
            equity_curve.append({"date": dt, "equity": round(equity, 0), "trade": False})
            continue

        # Simulate entry/exit: entry at open next day, exit at close
        if i + 1 >= len(rows):
            continue
        next_r   = rows[i + 1]
        entry_p  = next_r[1]   # next day open
        exit_p   = next_r[4]   # next day close
        direction = "BUY" if above_prev and adx > 18 else "SELL"
        pts      = (exit_p - entry_p) if direction == "BUY" else (entry_p - exit_p)
        # Assume 1 lot ATM option trade (approx 50 delta × lot 50 × points)
        pnl      = round(pts * 0.50 * 50, 0)
        # Deduct slippage (0.4 pts per side = 0.8 total)
        pnl     -= round(0.8 * 0.50 * 50, 0)
        outcome  = "WIN" if pnl > 0 else "LOSS"
        if outcome == "WIN":
            wins += 1
        else:
            losses += 1
        equity  += pnl

        trades.append({
            "date":      dt,
            "dir":       direction,
            "entry":     round(entry_p, 0),
            "exit":      round(exit_p, 0),
            "pts":       round(pts, 1),
            "pnl":       round(pnl, 0),
            "outcome":   outcome,
            "score":     round(final, 3),
            "l1":        round(l1_score, 2),
            "l2":        round(l2_score, 2),
            "l3":        round(l3_score, 2),
            "regime":    regime_d["regime"],
            "vix":       round(vix, 1),
            "adx":       round(adx, 1),
            "equity":    round(equity, 0),
        })
        equity_curve.append({"date": dt, "equity": round(equity, 0), "trade": True, "pnl": round(pnl,0)})

    total = wins + losses
    wr    = round(wins / total * 100, 1) if total > 0 else 0
    avg_win  = round(statistics.mean(t["pnl"] for t in trades if t["pnl"] > 0), 0) if wins > 0 else 0
    avg_loss = round(statistics.mean(t["pnl"] for t in trades if t["pnl"] <= 0), 0) if losses > 0 else 0
    rr       = round(abs(avg_win / avg_loss), 2) if avg_loss else 0

    # Max drawdown
    peak = 0
    max_dd = 0
    for pt in equity_curve:
        e = pt["equity"]
        peak = max(peak, e)
        dd   = peak - e
        max_dd = max(max_dd, dd)

    stats = {
        "total_trades": total,
        "wins":         wins,
        "losses":       losses,
        "win_rate":     wr,
        "avg_win":      avg_win,
        "avg_loss":     avg_loss,
        "rr":           rr,
        "total_pnl":    round(equity, 0),
        "max_drawdown": round(max_dd, 0),
        "expectancy":   round((wr/100 * avg_win) + ((1-wr/100) * avg_loss), 0),
        "days_tested":  days,
        "trades_per_month": round(total / (days / 22), 1) if days > 0 else 0,
        "profitable_regimes": _regime_breakdown(trades),
    }

    return {"trades": trades[-60:], "stats": stats, "equity_curve": equity_curve, "ts": time.time()}

def _regime_breakdown(trades: list) -> list:
    breakdown = {}
    for t in trades:
        r = t.get("regime", "NORMAL")
        if r not in breakdown:
            breakdown[r] = {"wins": 0, "losses": 0, "pnl": 0}
        if t["pnl"] > 0:
            breakdown[r]["wins"] += 1
        else:
            breakdown[r]["losses"] += 1
        breakdown[r]["pnl"] += t["pnl"]
    result = []
    for r, d in breakdown.items():
        tot = d["wins"] + d["losses"]
        result.append({
            "regime": r,
            "trades": tot,
            "wr":     round(d["wins"]/tot*100, 1) if tot > 0 else 0,
            "pnl":    round(d["pnl"], 0)
        })
    return sorted(result, key=lambda x: -x["pnl"])
