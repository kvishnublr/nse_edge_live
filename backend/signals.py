"""
NSE EDGE v5 — Signal Engine
5-gate filter driven entirely by Kite Connect live data.
"""

import time
import logging
import statistics
from typing import Optional, List
from collections import deque
from datetime import datetime
import requests as _requests
import pytz
from config import GATE as TH, LOT_SIZES

logger = logging.getLogger("signals")
_IST = pytz.timezone("Asia/Kolkata")

# ─── TELEGRAM ALERT ───────────────────────────────────────────────────────────
_last_telegram_verdict = None   # debounce — only alert on verdict change

def _send_telegram(msg: str):
    from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        _requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
            timeout=5,
        )
    except Exception as e:
        logger.debug(f"Telegram send failed: {e}")


# ─── WHATSAPP ALERT (CallMeBot) ───────────────────────────────────────────────
def _send_whatsapp(msg: str):
    from config import WHATSAPP_PHONE, WHATSAPP_APIKEY
    if not WHATSAPP_PHONE or not WHATSAPP_APIKEY:
        return
    try:
        import urllib.parse
        encoded = urllib.parse.quote(msg)
        _requests.get(
            f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}"
            f"&text={encoded}&apikey={WHATSAPP_APIKEY}",
            timeout=8,
        )
    except Exception as e:
        logger.debug(f"WhatsApp send failed: {e}")


# ─── SPIKE SCORING HELPER ─────────────────────────────────────────────────────
def _score_spike(vol_mult: float, chg_pct: float, sym: str, candle_min: int) -> int:
    """Score a spike 0-100. Higher price/vol now score higher (not penalized)."""
    score = 0
    # Volume quality (0-35): 5-8× is sweet spot; very high can still be valid
    if 5.0 <= vol_mult < 8.0:    score += 35
    elif 3.0 <= vol_mult < 5.0:  score += 30
    elif vol_mult >= 8.0:        score += 22   # exceptional — can reverse but notable
    elif 2.0 <= vol_mult < 3.0:  score += 15
    else:                         score += 5
    # Price momentum quality (0-30): bigger moves score higher, not lower
    ap = abs(chg_pct)
    if ap >= 1.5:          score += 30
    elif ap >= 1.0:        score += 28
    elif 0.5 <= ap < 1.0:  score += 25
    elif 0.4 <= ap < 0.5:  score += 18
    elif 0.3 <= ap < 0.4:  score += 10
    else:                   score += 4
    # Symbol quality (0-20) — expanded lists for NSE F&O universe
    hi_sym = {'TCS', 'TATASTEEL', 'MARUTI', 'INFY', 'RELIANCE', 'HDFCBANK', 'ICICIBANK', 'AXISBANK'}
    md_sym = {
        'BAJFINANCE', 'TATAMOTORS', 'WIPRO', 'TECHM', 'SBIN', 'KOTAKBANK',
        'LT', 'NTPC', 'POWERGRID', 'ONGC', 'COALINDIA', 'INDUSINDBK',
        'BAJAJFINSV', 'ADANIENT', 'ADANIPORTS', 'HINDUNILVR', 'BHARTIARTL',
        'SUNPHARMA', 'DRREDDY', 'DIVISLAB', 'EICHERMOT', 'HEROMOTOCO',
    }
    if sym in hi_sym:    score += 20
    elif sym in md_sym:  score += 12
    else:                score += 5
    # Time quality (0-15)
    if candle_min <= 600:    score += 15   # 09:15-10:00 — best
    elif candle_min <= 810:  score += 10   # 10:00-13:30
    elif candle_min <= 870:  score += 5    # 13:30-14:30
    # else 0 — 14:30+ is weak
    return score


# ─── SPIKE ALERT (Telegram + WhatsApp) ───────────────────────────────────────
def _send_spike_alert(spike: dict):
    """Send spike alert via Telegram and WhatsApp. Only fires for score >= 60."""
    score = spike.get("score", 0)
    if score < 60:
        return
    sym    = spike.get("symbol", "")
    price  = spike.get("price", 0)
    chg    = spike.get("chg_pct", 0)
    vm     = spike.get("vol_mult", 0)
    sig    = spike.get("signal", "")
    t      = spike.get("time", "")
    msg = (
        f"⚡ <b>SPIKE ALERT — {sym}</b>  [Score: {score}]\n"
        f"Signal: {sig}  |  {'+' if chg >= 0 else ''}{chg:.2f}%  |  Vol {vm:.1f}×\n"
        f"Price: ₹{price:.2f}  |  {t}"
    )
    _send_telegram(msg)
    _send_whatsapp(msg)


# ─── GLOBAL STATE (read by WebSocket broadcaster) ─────────────────────────────
state = {
    "gates": {
        1: {"name": "REGIME",       "state": "wt", "score": 50, "rows": []},
        2: {"name": "SMART MONEY",  "state": "wt", "score": 50, "rows": []},
        3: {"name": "STRUCTURE",    "state": "wt", "score": 50, "rows": []},
        4: {"name": "TRIGGER",      "state": "wt", "score": 50, "rows": []},
        5: {"name": "RISK VALID",   "state": "wt", "score": 50, "rows": []},
    },
    "verdict":     "WAIT",
    "verdict_sub": "Initialising...",
    "pass_count":  0,
    "confidence":  0.0,
    "spikes":      [],
    "ticker":      [],
    "last_chain":  None,
    "last_macro":  None,
    "last_stocks": [],
    "last_fii":    None,
    "last_updated": 0,
}

# ─── PRICE HISTORY HELPERS ────────────────────────────────────────────────────
def push_price(symbol: str, price: float, volume: float = 0):
    """Called by scheduler to maintain history for ATR/VWAP."""
    from feed import _hist  # shared deque in feed.py
    if price:
        _hist(symbol).append((time.time(), price, volume))


def calc_atr(symbol: str, periods: int = 14) -> float:
    """ATR from Kite price history."""
    from feed import price_history
    hist  = list(price_history.get(symbol, []))
    defaults = {"NIFTY": 90, "BANKNIFTY": 230, "ICICIBANK": 20,
                "SBIN": 12, "RELIANCE": 30}
    if len(hist) < 3:
        return defaults.get(symbol, 15)
    prices = [h[1] for h in hist]
    trs    = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    window = trs[-periods:] if len(trs) >= periods else trs
    return round(statistics.mean(window), 2)


def calc_vwap(symbol: str) -> Optional[float]:
    """Session VWAP from Kite tick history."""
    from feed import price_history
    import pytz
    from datetime import datetime
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    start_ts = now.replace(hour=9, minute=15, second=0, microsecond=0).timestamp()
    hist = [(ts, p, v) for ts, p, v in price_history.get(symbol, [])
            if ts >= start_ts and v > 0]
    if not hist:
        return None
    pv  = sum(p * v for _, p, v in hist)
    vol = sum(v for _, _, v in hist)
    return round(pv / vol, 2) if vol else None


# ─── GATE 1: REGIME ───────────────────────────────────────────────────────────
def gate1_regime(indices: dict, fii: dict) -> dict:
    vix     = indices.get("vix", 15)
    vix_chg = indices.get("vix_chg", 0)
    fii_net = fii.get("fii_net", 0) if fii else 0

    # VIX
    if vix < TH["vix_low"]:
        vix_lbl, vix_col = f"{vix:.1f} — LOW ✓ full size", "cg"
    elif vix < TH["vix_medium"]:
        vix_lbl, vix_col = f"{vix:.1f} — MODERATE 75% size", "ca"
    elif vix < TH["vix_high"]:
        vix_lbl, vix_col = f"{vix:.1f} — ELEVATED 50% size", "ca"
    else:
        vix_lbl, vix_col = f"{vix:.1f} — DANGER avoid", "cr"

    # FII (relaxed thresholds - allow some selling)
    sign    = "+" if fii_net >= 0 else ""
    fii_lbl = f"{sign}₹{abs(fii_net):.0f} Cr — {'NET BUY ✓' if fii_net > 0 else 'NET SELL'}"
    fii_col = "cg" if fii_net > 0 else "ca" if fii_net > -5000 else "cr"

    # Nifty direction
    nifty_chg = indices.get("nifty_chg", 0)
    nif_col   = "cg" if nifty_chg > 0 else "cr"
    nif_lbl   = f"{'+' if nifty_chg >= 0 else ''}{nifty_chg:.2f}%"

    score = 100
    if vix >= TH["vix_high"]:   score -= 40
    elif vix >= TH["vix_medium"]: score -= 20
    elif vix >= TH["vix_low"]:    score -= 5
    if fii_net < -10000:          score -= 20
    elif fii_net < -5000:         score -= 10
    if vix_chg > 10:              score -= 10
    elif vix_chg > 5:            score -= 5
    score = max(0, min(100, score))

    # More lenient: only fail if VIX very high OR massive FII selling
    if vix >= TH["vix_high"] + 5 or fii_net < -20000:
        st = "st"
    elif vix >= TH["vix_high"] or fii_net < -10000:
        st = "am"
    elif score >= 60:
        st = "go"
    else:
        st = "wt"

    rows = [
        {"k": "India VIX",      "v": vix_lbl,  "c": vix_col},
        {"k": "VIX change",     "v": f"{'+' if vix_chg >= 0 else ''}{vix_chg:.1f}%",
         "c": "cr" if vix_chg > 3 else "cg" if vix_chg < -3 else "cm"},
        {"k": "FII cash",       "v": fii_lbl,  "c": fii_col},
        {"k": "Nifty change",   "v": nif_lbl,  "c": nif_col},
        {"k": "Regime verdict", "v": "BULL TREND ✓" if st == "go" else
              "CAUTION" if st in ("am","wt") else "BEAR / AVOID",
         "c": "cg" if st == "go" else "ca" if st in ("am","wt") else "cr"},
    ]
    return {"name": "REGIME", "state": st, "score": score, "rows": rows}


# ─── GATE 2: SMART MONEY ──────────────────────────────────────────────────────
def gate2_smart_money(chain: dict) -> dict:
    if not chain:
        return {"name": "SMART MONEY", "state": "wt", "score": 50,
                "rows": [{"k": "Status", "v": "Fetching option chain...", "c": "cm"}]}

    pcr      = chain.get("pcr", 1.0)
    ul       = chain.get("ul_price", 0)
    mp       = chain.get("max_pain", 0)
    tc_oi    = chain.get("total_call_oi", 0)
    tp_oi    = chain.get("total_put_oi", 0)
    net      = tp_oi - tc_oi

    if pcr >= TH["pcr_bullish"]:
        pcr_lbl, pcr_col = f"{pcr:.2f} — BULLISH ✓", "cg"
    elif pcr <= TH["pcr_bearish"]:
        pcr_lbl, pcr_col = f"{pcr:.2f} — BEARISH", "cr"
    else:
        pcr_lbl, pcr_col = f"{pcr:.2f} — NEUTRAL", "ca"

    mp_dist = round(ul - mp) if mp else 0
    mp_lbl  = f"{mp:,} ({'+' if mp_dist >= 0 else ''}{mp_dist} pts away)"

    score = 60
    if pcr >= TH["pcr_bullish"]:   score += 25
    elif pcr <= TH["pcr_bearish"]: score -= 25
    if net > 100000:               score += 15
    elif net < -100000:            score -= 15
    score = max(0, min(100, score))

    if score <= 35 or pcr <= TH["pcr_bearish"]:
        st = "st"
    elif score >= 70 and pcr >= TH["pcr_bullish"]:
        st = "go"
    elif score >= 55:
        st = "am"
    else:
        st = "wt"

    rows = [
        {"k": "PCR",           "v": pcr_lbl, "c": pcr_col},
        {"k": "Total Call OI", "v": f"{tc_oi:,}", "c": "cr"},
        {"k": "Total Put OI",  "v": f"{tp_oi:,}", "c": "cg"},
        {"k": "OI net bias",   "v": f"{'+' if net >= 0 else ''}{net:,}",
         "c": "cg" if net > 0 else "cr"},
        {"k": "Max Pain",      "v": mp_lbl, "c": "ca"},
    ]
    return {"name": "SMART MONEY", "state": st, "score": score, "rows": rows}


# ─── GATE 3: STRUCTURE ────────────────────────────────────────────────────────
def gate3_structure(indices: dict) -> dict:
    nifty = indices.get("nifty", 0)
    high  = indices.get("nifty_high", nifty)
    low   = indices.get("nifty_low",  nifty)
    vwap  = calc_vwap("NIFTY")

    if vwap and vwap > 0:
        diff    = nifty - vwap
        pct     = diff / vwap * 100
        if pct > 0.1:
            vwap_lbl, vwap_col = f"ABOVE +{pct:.2f}% ✓", "cg"
        elif pct > -0.1:
            vwap_lbl, vwap_col = f"AT VWAP {pct:.2f}%", "ca"
        else:
            vwap_lbl, vwap_col = f"BELOW {pct:.2f}%", "cr"
        vwap_val = f"{vwap:,.0f}"
    else:
        vwap_lbl, vwap_col, vwap_val = "Computing...", "cm", "—"
        pct = 0

    rng     = high - low if high > low else 0
    rng_pct = rng / low * 100 if low else 0
    pos_pct = (nifty - low) / rng * 100 if rng > 0 else 50

    if pos_pct >= 60:
        pos_lbl, pos_col = f"Upper {pos_pct:.0f}% of range ✓", "cg"
    elif pos_pct >= 40:
        pos_lbl, pos_col = f"Mid {pos_pct:.0f}%", "ca"
    else:
        pos_lbl, pos_col = f"Lower {pos_pct:.0f}% — weak", "cr"

    score = 60
    if vwap and pct > 0.05:  score += 20
    elif vwap and pct < -0.10: score -= 15
    if pos_pct >= 55:        score += 15
    elif pos_pct < 35:       score -= 10
    score = max(0, min(100, score))

    st = "go" if score >= 55 else "am" if score >= 40 else "st"

    rows = [
        {"k": "vs VWAP",        "v": vwap_lbl, "c": vwap_col},
        {"k": "VWAP level",     "v": vwap_val, "c": "cb"},
        {"k": "Day H/L",        "v": f"{high:,.0f} / {low:,.0f} ({rng_pct:.2f}% range)", "c": "cm"},
        {"k": "Range position", "v": pos_lbl,  "c": pos_col},
        {"k": "Nifty price",    "v": f"{nifty:,.2f}",
         "c": "cg" if indices.get("nifty_chg", 0) > 0 else "cr"},
    ]
    return {"name": "STRUCTURE", "state": st, "score": score, "rows": rows}


# ─── GATE 4: TRIGGER ──────────────────────────────────────────────────────────
def gate4_trigger(indices: dict, chain: dict, stocks: list) -> dict:
    nifty     = indices.get("nifty", 0)
    nifty_chg = indices.get("nifty_chg", 0)
    nifty_vol = indices.get("nifty_vol", 0)  # from KiteTicker
    atr       = calc_atr("NIFTY")

    # Volume: compare current vol vs typical (use ATR-normalised move as proxy)
    mom_pct  = abs(nifty_chg)
    atr_pct  = (atr / nifty * 100) if nifty else 0.3
    vol_mult = mom_pct / atr_pct if atr_pct > 0 else 1.0
    vol_lbl  = f"{vol_mult:.1f}× ATR-normalised"
    vol_col  = "cg" if vol_mult >= TH["vol_surge_min"] else "ca" if vol_mult >= 1.0 else "cm"

    # OI build from chain
    if chain:
        oi_chg = sum(
            abs(s.get("call_oi_chg", 0)) + abs(s.get("put_oi_chg", 0))
            for s in chain.get("strikes", [])
        )
        oi_lbl = f"+{oi_chg:,} contracts"
        oi_col = "cg" if oi_chg >= TH["oi_build_min"] else "ca"
    else:
        oi_chg, oi_lbl, oi_col = 0, "Awaiting chain data", "cm"

    # ATR
    atr_lbl = f"{atr:.0f} pts ({atr_pct:.2f}% of Nifty)"
    atr_col = "ca" if atr_pct > 0.5 else "cg"

    # Momentum
    if abs(nifty_chg) >= 0.5:
        mom_lbl = f"{'+' if nifty_chg > 0 else ''}{nifty_chg:.2f}% STRONG"
        mom_col = "cg" if nifty_chg > 0 else "cr"
    else:
        mom_lbl, mom_col = f"{nifty_chg:.2f}% — dull", "cm"

    score = 50
    if vol_mult >= TH["vol_surge_min"]: score += 25
    elif vol_mult >= 0.7:               score += 10
    if oi_chg >= TH["oi_build_min"]:    score += 25
    elif oi_chg >= TH["oi_build_min"] / 2: score += 10
    if abs(nifty_chg) >= 0.3:           score += 15
    elif abs(nifty_chg) >= 0.1:         score += 5
    score = max(0, min(100, score))

    # More lenient: pass with lower score
    st = "go" if score >= 55 else "wt" if score >= 35 else "am"

    rows = [
        {"k": "ATR (14-bar)", "v": atr_lbl,  "c": atr_col},
        {"k": "Volume signal","v": vol_lbl,  "c": vol_col},
        {"k": "OI build",     "v": oi_lbl,   "c": oi_col},
        {"k": "Momentum",     "v": mom_lbl,  "c": mom_col},
        {"k": "Trigger",      "v": "FIRED ✓" if st == "go" else
              "BUILDING — watch" if st == "wt" else "WEAK — no entry",
         "c": "cg" if st == "go" else "ca" if st == "wt" else "cm"},
    ]
    return {"name": "TRIGGER", "state": st, "score": score, "rows": rows}


# ─── GATE 5: RISK VALID ───────────────────────────────────────────────────────
def gate5_risk(indices: dict, chain: dict, mode: str) -> dict:
    nifty = indices.get("nifty", 0)
    vix   = indices.get("vix", 15)
    atr   = calc_atr("NIFTY")

    # VIX size rule
    if vix < TH["vix_low"]:
        size_lbl, size_col, mult = "FULL 100% ✓", "cg", 1.0
    elif vix < TH["vix_medium"]:
        size_lbl, size_col, mult = "75% size", "ca", 0.75
    elif vix < TH["vix_high"]:
        size_lbl, size_col, mult = "50% size", "ca", 0.50
    else:
        size_lbl, size_col, mult = "25% / NO TRADE", "cr", 0.25

    stop_pts   = round(atr * TH["atr_multiplier"])
    stop_price = round(nifty - stop_pts)

    # Find nearest CE wall (target) from chain
    target = round(nifty + stop_pts * 2.5)  # default 1:2.5
    if chain:
        ce_walls = sorted(
            [(s["strike"], s["call_oi"]) for s in chain.get("strikes", [])
             if s["strike"] > nifty],
            key=lambda x: -x[1]
        )
        if ce_walls:
            target = ce_walls[0][0]

    rr      = (target - nifty) / stop_pts if stop_pts else 0
    rr_min  = TH["rr_min_intraday"] if mode == "intraday" else TH["rr_min_positional"]
    rr_lbl  = f"1:{rr:.1f} (need ≥ 1:{rr_min:.0f})"
    rr_col  = "cg" if rr >= rr_min else "ca" if rr >= rr_min * 0.8 else "cr"

    score = 60
    if rr >= rr_min:              score += 30
    elif rr >= rr_min * 0.8:      score += 10
    else:                         score -= 30
    if mult < 0.5:                score -= 20
    score = max(0, min(100, score))

    st = "go" if (rr >= rr_min and mult >= 0.5) else \
         "st" if (rr < 1.5 or mult <= 0.25) else "am"

    # Calculate position sizing
    position_size_lots = 0
    position_size_rupees = 0
    if st == "go" and rr > 0 and mult > 0:
        # Account risk parameters (can be made configurable later)
        account_value = 500000  # ₹5 lakh trading capital
        risk_per_trade = 0.01   # 1% risk per trade
        monetary_risk = account_value * risk_per_trade  # ₹5,000 risk per trade
        
        # Risk per lot = stop points * lot size
        lot_size = LOT_SIZES.get("NIFTY", 25)  # Default to NIFTY lot size
        risk_per_lot = stop_pts * lot_size
        
        if risk_per_lot > 0:
            raw_lots = monetary_risk / (risk_per_lot * mult)  # Adjust for VIX sizing
            position_size_lots = max(1, int(round(raw_lots)))  # At least 1 lot
            position_size_rupees = position_size_lots * lot_size * nifty

    rows = [
        {"k": "R:R ratio",     "v": rr_lbl,  "c": rr_col},
        {"k": "Target (CE wall)","v": f"{target:,} (+{target-nifty:.0f} pts)","c": "cg"},
        {"k": "Stop distance", "v": f"{stop_pts} pts → SL {stop_price:,}", "c": "ca"},
        {"k": "VIX sizing",    "v": size_lbl, "c": size_col},
        {"k": "Position Size", "v": f"{position_size_lots} lot{'s' if position_size_lots > 1 else ''} (₹{position_size_rupees:,})", "c": "cg" if position_size_lots > 0 else "st"},
        {"k": "Risk verdict",  "v": "VALID ✓" if st == "go" else
               "FAIL — R:R too low" if st == "st" else "MARGINAL",
          "c": "cg" if st == "go" else "cr" if st == "st" else "ca"},
    ]
    return {"name": "RISK VALID", "state": st, "score": score, "rows": rows, 
            "position_size_lots": position_size_lots, "position_size_rupees": position_size_rupees}


# ─── VERDICT ──────────────────────────────────────────────────────────────────
def compute_verdict(gates: list) -> tuple:
    states    = [g["state"] for g in gates]
    pass_cnt  = states.count("go")
    has_fail  = "st" in states

    if pass_cnt == 5:
        return "EXECUTE", "All 5 gates clear — deploy full position", pass_cnt
    elif has_fail or pass_cnt < 3:
        failed = [i + 1 for i, s in enumerate(states) if s == "st"]
        gnum = failed[0] if failed else next((i+1 for i,s in enumerate(states) if s in ("wt","am")), 1)
        return "NO TRADE", f"G{gnum} failed — stand down", pass_cnt
    else:
        waiting = [i + 1 for i, s in enumerate(states) if s in ("wt", "am")]
        lbl = {1:"regime",2:"smart money",3:"structure",4:"trigger",5:"risk"}
        gnum = waiting[0] if waiting else 1
        return "WAIT", f"G{gnum} {lbl.get(gnum,'signal')} not satisfied", pass_cnt


# ─── SPIKE DETECTOR ───────────────────────────────────────────────────────────
def detect_spikes(stocks: list, prev: dict, gates: dict | None = None, verdict: str = "WAIT") -> list:
    from datetime import datetime
    import backtest_data as bd
    import pytz
    now_dt   = datetime.now(pytz.timezone("Asia/Kolkata"))
    now      = now_dt.strftime("%H:%M")
    candle_min = now_dt.hour * 60 + now_dt.minute
    spikes   = []

    # Spike radar is fully independent of gates — fires on price/vol/OI merit only
    gate_score_floor = 50

    try:
        acc_filters = bd.get_signal_accuracy_filters()
    except Exception:
        acc_filters = {"weak_symbols": set(), "weak_buckets": set()}

    if candle_min < 570:
        time_bucket = "open_915_930"
    elif candle_min < 630:
        time_bucket = "morning_930_1030"
    elif candle_min < 780:
        time_bucket = "midday_1030_1300"
    else:
        time_bucket = "late_1300_plus"

    if time_bucket in acc_filters.get("weak_buckets", set()):
        return []

    for s in stocks:
        sym      = s.get("symbol", "")
        price    = s.get("price", 0)
        chg_pct  = s.get("chg_pct", 0)
        oi_pct   = s.get("oi_chg_pct", 0)
        vol      = s.get("volume", 1) or 1
        prev_vol = prev.get(sym, {}).get("volume", vol) or vol
        vm       = vol / prev_vol if prev_vol else 1.0
        stock_pc = int(s.get("pc", 0) or 0)
        stock_verdict = str(s.get("verdict", "WAIT") or "WAIT")
        if sym in acc_filters.get("weak_symbols", set()):
            continue

        sp_type = sig = trigger = ""

        # Time window filter: 9:30-11:00 (570-660) + 13:00-14:00 (780-840)
        # SKIP 11:00-13:00 — lunch-hour chop has <15% accuracy (data-validated)
        in_window = (570 <= candle_min < 660) or (780 <= candle_min <= 840)
        if not in_window:
            continue

        price_th = TH.get("spike_price_pct", 0.2)
        vol_th   = TH.get("spike_vol_mult", 1.5)
        oi_th    = TH.get("spike_oi_pct", 12.0)

        # Detect spike type — price+vol spike takes priority, then OI-only, then vol-only
        if abs(chg_pct) >= price_th and vm >= vol_th:
            sp_type = "buy" if chg_pct > 0 else "sell"
            sig     = "LONG" if chg_pct > 0 else "SHORT"
            trigger = f"Price {'+' if chg_pct > 0 else ''}{chg_pct:.2f}% | Vol {vm:.1f}×"
            if abs(oi_pct) >= oi_th:
                oi_sign = "+" if oi_pct > 0 else ""
                sig    += " + OI"
                trigger = f"{trigger} | OI {oi_sign}{oi_pct:.0f}%"
        elif abs(oi_pct) >= oi_th:
            sp_type = "buy" if oi_pct > 0 else "sell"
            sig     = "OI BUILD" if oi_pct > 0 else "OI UNWIND"
            trigger = f"OI {'+' if oi_pct > 0 else ''}{oi_pct:.1f}%"
        elif vm >= vol_th and abs(chg_pct) >= price_th * 0.5:
            sp_type = "vol"
            sig     = "VOL SPIKE"
            trigger = f"Vol {vm:.1f}×"

        if not sp_type:
            continue

        # Score the spike — threshold rises when gates are stopped
        score = _score_spike(vm, chg_pct, sym, candle_min)
        if score < gate_score_floor:
            continue

        # Gate quality filter — require pc>=2 (allows signals during moderate gate failures)
        if stock_pc < 2 or stock_verdict not in ("EXECUTE", "WATCH", "MONITOR"):
            continue

        # Confirmation proxy — OI-aware: skip OI check if data is likely stale (near-zero)
        oi_available = abs(oi_pct) >= 1.0
        if oi_available:
            same_dir_oi  = (chg_pct > 0 and oi_pct > 0) or (chg_pct < 0 and oi_pct < 0)
            has_confirmation = same_dir_oi or vm >= 3.0
        else:
            # OI data stale / unavailable — rely on price + volume alone
            has_confirmation = vm >= 2.5 or abs(chg_pct) >= 0.8
        if not has_confirmation:
            continue

        # Unified strength from score
        strength = "hi" if score >= 70 else "md"

        spike_dict = {
            "symbol":       sym,
            "time":         now,
            "price":        price,
            "chg_pct":      chg_pct,
            "vol_mult":     round(vm, 1),
            "oi_pct":       oi_pct,
            "type":         sp_type,
            "trigger":      trigger,
            "signal":       sig,
            "strength":     strength,
            "score":        score,
            "pc":           stock_pc,
        }
        spikes.append(spike_dict)

        # Send alert for high-quality spikes
        if score >= 60:
            _send_spike_alert(spike_dict)

    # Sort by score descending, then by abs chg_pct
    spikes.sort(key=lambda x: (-x.get("score", 0), -abs(x.get("chg_pct", 0))))
    return spikes[:15]


def _annotate_live_stocks(stocks: list, gates_dict: dict, verdict: str) -> list:
    """Attach live gate context and backend-owned scores to stock rows."""
    now_hm = datetime.now(_IST).strftime("%H:%M")
    g1 = (gates_dict.get(1) or {}).get("state", "wt")
    g2 = (gates_dict.get(2) or {}).get("state", "wt")
    for s in stocks:
        chg = float(s.get("chg_pct", 0) or 0)
        oi_pct = float(s.get("oi_chg_pct", 0) or 0)
        vol_r = float(s.get("vol_ratio", 0) or 0)
        atr_pct = float(s.get("atr_pct", 0) or 0)
        g3 = "go" if abs(chg) >= 0.8 and abs(oi_pct) >= 4 else "am" if abs(chg) >= 0.35 else "st"
        g4 = "go" if vol_r >= 1.5 and abs(chg) >= 0.6 else "wt" if vol_r >= 1.1 or abs(chg) >= 0.4 else "st"
        g5 = "go" if atr_pct > 0 and abs(chg) >= max(0.6, atr_pct * 0.35) else "am" if abs(chg) >= 0.35 else "st"
        pc = [g1, g2, g3, g4, g5].count("go")
        base_score = float(s.get("score", 40) or 40)
        score = int(min(99, max(base_score, 35 + pc * 10 + (8 if vol_r >= 1.5 else 0))))
        s.update({
            "g1": g1,
            "g2": g2,
            "g3": g3,
            "g4": g4,
            "g5": g5,
            "pc": pc,
            "score": score,
            "signal_time": now_hm if pc >= 3 else "",
            "verdict": "EXECUTE" if pc >= 3 and verdict != "NO TRADE" else "WATCH" if pc >= 2 else "WAIT",
        })
    return stocks


# ─── MASTER RUN ───────────────────────────────────────────────────────────────
def run_signal_engine(indices: dict, chain: dict, fii: dict,
                      stocks: list, mode: str = "intraday"):
    if not indices or not indices.get("nifty"):
        logger.warning("Signal engine: no index data yet")
        return

    g1 = gate1_regime(indices, fii)
    g2 = gate2_smart_money(chain)
    g3 = gate3_structure(indices)
    g4 = gate4_trigger(indices, chain, stocks)
    g5 = gate5_risk(indices, chain, mode)
    gates = [g1, g2, g3, g4, g5]

    verdict, sub, pass_cnt = compute_verdict(gates)

    gates_dict = {i + 1: g for i, g in enumerate(gates)}
    stocks = _annotate_live_stocks(stocks, gates_dict, verdict)
    prev   = {s["symbol"]: s for s in state.get("last_stocks", [])}
    new_spikes = detect_spikes(stocks, prev, gates_dict, verdict)

    # Preserve today's spikes across the session — don't wipe them when the
    # time window closes.  Merge new detections into the running list; clear
    # only when the calendar date rolls over (new trading day).
    import datetime as _dt
    today_str = _dt.date.today().isoformat()
    prev_spikes = state.get("spikes", [])
    prev_date   = state.get("spikes_date", today_str)

    if prev_date != today_str:
        # New trading day — start fresh
        merged_spikes = new_spikes
    else:
        # Same day — merge: add new spikes not already in the list
        existing_keys = {(s["symbol"], s["time"], s["type"]) for s in prev_spikes}
        merged_spikes = prev_spikes + [s for s in new_spikes
                                       if (s["symbol"], s["time"], s["type"]) not in existing_keys]
        # Keep latest 30, sorted by score
        merged_spikes.sort(key=lambda x: -x.get("score", 0))
        merged_spikes = merged_spikes[:30]

    spikes = merged_spikes
    state["spikes_date"] = today_str

    # Build intel ticker
    ticker = []
    vix    = indices.get("vix", 0)
    fii_net= fii.get("fii_net", 0) if fii else 0
    pcr    = chain.get("pcr", 0) if chain else 0

    if pass_cnt == 5:
        ticker.append("ALL 5 GATES PASS — <em>EXECUTE signal active</em>")
    elif verdict == "NO TRADE":
        ticker.append("Gate FAIL — <em>NO TRADE — stand down now</em>")
    else:
        ticker.append(f"System WAIT — {pass_cnt}/5 gates — watching trigger")

    if pcr:
        ticker.append(f"PCR {pcr:.2f} — {'<em>bullish OI</em>' if pcr > 1.2 else 'bearish OI' if pcr < 0.8 else 'neutral OI'}")
    if vix:
        ticker.append(f"VIX {vix:.1f} — {'<em>low — full size</em>' if vix < 12 else 'elevated — reduce size' if vix > 16 else 'moderate regime'}")
    if fii_net:
        ticker.append(f"FII {'<em>net buyer</em>' if fii_net > 0 else 'net seller'} ₹{abs(fii_net):.0f} Cr today")
    for sp in spikes[:3]:
        if sp["strength"] == "hi":
            ticker.append(f"{sp['symbol']} SPIKE — <em>{sp['signal']}</em> — {sp['trigger']}")

    # ── Confidence score (0-10) using gate weights from backtest analysis ──
    try:
        import gate_weights as gw
        confidence = gw.compute_confidence(gates_dict)
    except Exception:
        confidence = 0.0

    # Extract position sizing data from RISK VALID gate (gate 5)
    position_size_lots = 0
    position_size_rupees = 0
    if 5 in gates_dict:
        position_size_lots = gates_dict[5].get("position_size_lots", 0)
        position_size_rupees = gates_dict[5].get("position_size_rupees", 0)

    state.update({
        "gates":       gates_dict,
        "verdict":     verdict,
        "verdict_sub": sub,
        "pass_count":  pass_cnt,
        "confidence":  confidence,
        "spikes":      spikes,
        "ticker":      ticker,
        "last_chain":  chain,
        "last_macro":  indices,
        "last_stocks": stocks,
        "last_fii":    fii,
        "last_updated":time.time(),
        "position_size_lots": position_size_lots,
        "position_size_rupees": position_size_rupees,
    })

    # ── Telegram alert on verdict change ──
    global _last_telegram_verdict
    if verdict != _last_telegram_verdict:
        prev_verdict = _last_telegram_verdict
        _last_telegram_verdict = verdict
        if verdict == "EXECUTE":
            nifty = indices.get("nifty", 0)
            # Pull entry zone from gate5 rows if available
            g5_rows = gates_dict.get(5, {}).get("rows", [])
            entry_line = ""
            for r in g5_rows:
                if r.get("k") == "Target (CE wall)":
                    entry_line += f"Target: {r['v']}  |  "
                elif r.get("k") == "Stop distance":
                    entry_line += f"SL: {r['v']}"
            pos_lots = gates_dict.get(5, {}).get("position_size_lots", 0)
            pos_rs   = gates_dict.get(5, {}).get("position_size_rupees", 0)
            msg = (
                f"🟢 <b>NSE EDGE — EXECUTE SIGNAL</b>\n"
                f"Nifty: <b>{nifty:.0f}</b>  |  VIX: {vix:.1f}  |  PCR: {pcr:.2f}\n"
                f"Gates: {pass_cnt}/5 PASS  |  Confidence: {confidence}/10\n"
                f"FII: ₹{fii_net:.0f} Cr\n"
            )
            if entry_line:
                msg += f"{entry_line}\n"
            if pos_lots:
                msg += f"Size: {pos_lots} lot{'s' if pos_lots != 1 else ''} (₹{pos_rs:,})\n"
            msg += "<b>All gates clear — trade now</b>"
            _send_telegram(msg)
        elif verdict == "NO TRADE" and prev_verdict == "EXECUTE":
            _send_telegram("🔴 <b>NSE EDGE — EXECUTE cancelled</b>\nGate failed — stand down.")

    # ── Log to DB for backtest analysis (non-blocking) ──
    try:
        import backtest_data as bd
        bd.log_signal(gates_dict, verdict, pass_cnt, indices, chain, fii)
        bd.log_live_spikes(spikes, gates_dict, verdict, pass_cnt, indices, chain)
    except Exception:
        pass

    logger.info(f"Verdict: {verdict} ({pass_cnt}/5) | VIX={vix:.1f} | PCR={pcr:.2f} | Conf={confidence}")
