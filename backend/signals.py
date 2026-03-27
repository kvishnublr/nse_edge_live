"""
NSE EDGE v5 — Signal Engine
5-gate filter driven entirely by Kite Connect live data.
"""

import time
import logging
import statistics
from typing import Optional, List
from collections import deque
from config import GATE as TH

logger = logging.getLogger("signals")

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

    # FII
    sign    = "+" if fii_net >= 0 else ""
    fii_lbl = f"{sign}₹{abs(fii_net):.0f} Cr — {'NET BUY ✓' if fii_net > 0 else 'NET SELL'}"
    fii_col = "cg" if fii_net > 500 else "ca" if fii_net > -500 else "cr"

    # Nifty direction
    nifty_chg = indices.get("nifty_chg", 0)
    nif_col   = "cg" if nifty_chg > 0 else "cr"
    nif_lbl   = f"{'+' if nifty_chg >= 0 else ''}{nifty_chg:.2f}%"

    score = 100
    if vix >= TH["vix_high"]:   score -= 50
    elif vix >= TH["vix_medium"]: score -= 25
    elif vix >= TH["vix_low"]:    score -= 10
    if fii_net < -1000:           score -= 20
    elif fii_net < 0:             score -= 10
    if vix_chg > 5:               score -= 10
    score = max(0, min(100, score))

    if vix >= TH["vix_high"] or fii_net < -2000:
        st = "st"
    elif vix >= TH["vix_medium"] or fii_net < 0:
        st = "am"
    elif score >= 70:
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
    if vwap and pct > 0.1:  score += 20
    elif vwap and pct < -0.15: score -= 20
    if pos_pct >= 60:        score += 15
    elif pos_pct < 40:       score -= 10
    score = max(0, min(100, score))

    st = "go" if score >= 70 else "am" if score >= 50 else "st"

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
    if vol_mult >= TH["vol_surge_min"]: score += 20
    elif vol_mult >= 1.0:               score += 5
    if oi_chg >= TH["oi_build_min"]:    score += 20
    if abs(nifty_chg) >= 0.5:           score += 10
    score = max(0, min(100, score))

    st = "go" if score >= 70 else "wt" if score >= 45 else "am"

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

    rows = [
        {"k": "R:R ratio",     "v": rr_lbl,  "c": rr_col},
        {"k": "Target (CE wall)","v": f"{target:,} (+{target-nifty:.0f} pts)","c": "cg"},
        {"k": "Stop distance", "v": f"{stop_pts} pts → SL {stop_price:,}", "c": "ca"},
        {"k": "VIX sizing",    "v": size_lbl, "c": size_col},
        {"k": "Risk verdict",  "v": "VALID ✓" if st == "go" else
              "FAIL — R:R too low" if st == "st" else "MARGINAL",
         "c": "cg" if st == "go" else "cr" if st == "st" else "ca"},
    ]
    return {"name": "RISK VALID", "state": st, "score": score, "rows": rows}


# ─── VERDICT ──────────────────────────────────────────────────────────────────
def compute_verdict(gates: list) -> tuple:
    states    = [g["state"] for g in gates]
    pass_cnt  = states.count("go")
    has_fail  = "st" in states

    if pass_cnt == 5:
        return "EXECUTE", "All 5 gates clear — deploy full position", pass_cnt
    elif has_fail or pass_cnt < 3:
        failed = [i + 1 for i, s in enumerate(states) if s == "st"]
        return "NO TRADE", f"G{failed[0]} failed — stand down", pass_cnt
    else:
        waiting = [i + 1 for i, s in enumerate(states) if s in ("wt", "am")]
        lbl = {1:"regime",2:"smart money",3:"structure",4:"trigger",5:"risk"}
        return "WAIT", f"G{waiting[0]} {lbl.get(waiting[0],'signal')} not satisfied", pass_cnt


# ─── SPIKE DETECTOR ───────────────────────────────────────────────────────────
def detect_spikes(stocks: list, prev: dict) -> list:
    from datetime import datetime
    import pytz
    now = datetime.now(pytz.timezone("Asia/Kolkata")).strftime("%H:%M")
    spikes = []

    for s in stocks:
        sym      = s.get("symbol", "")
        price    = s.get("price", 0)
        chg_pct  = s.get("chg_pct", 0)
        oi_pct   = s.get("oi_chg_pct", 0)
        vol      = s.get("volume", 1) or 1
        prev_vol = prev.get(sym, {}).get("volume", vol) or vol
        vm       = vol / prev_vol if prev_vol else 1.0

        sp_type = sig = trigger = ""
        strength = "lo"

        if abs(chg_pct) >= TH["spike_price_pct"] and vm >= TH["spike_vol_mult"]:
            sp_type  = "buy" if chg_pct > 0 else "sell"
            trigger  = f"Price {'+' if chg_pct > 0 else ''}{chg_pct:.2f}% + Vol {vm:.1f}×"
            sig      = "LONG ENTRY" if chg_pct > 0 else "SHORT ENTRY"
            strength = "hi" if abs(chg_pct) >= 2 and vm >= 3 else "md"
        elif abs(oi_pct) >= TH["spike_oi_pct"]:
            sp_type  = "buy" if oi_pct > 0 else "sell"
            trigger  = f"OI spike {'+' if oi_pct > 0 else ''}{oi_pct:.1f}%"
            sig      = "LONG OI BUILD" if oi_pct > 0 else "OI UNWIND"
            strength = "hi" if abs(oi_pct) >= 20 else "md"
        elif vm >= TH["spike_vol_mult"]:
            sp_type  = "oi"
            trigger  = f"Volume {vm:.1f}× average"
            sig      = "UNUSUAL VOL"
            strength = "md"

        if sp_type:
            spikes.append({
                "symbol":   sym,
                "time":     now,
                "price":    price,
                "chg_pct":  chg_pct,
                "vol_mult": round(vm, 1),
                "oi_pct":   oi_pct,
                "type":     sp_type,
                "trigger":  trigger,
                "signal":   sig,
                "strength": strength,
            })

    order = {"hi": 0, "md": 1, "lo": 2}
    spikes.sort(key=lambda x: (order.get(x["strength"], 2), -abs(x.get("oi_pct", 0))))
    return spikes[:12]


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

    prev   = {s["symbol"]: s for s in state.get("last_stocks", [])}
    spikes = detect_spikes(stocks, prev)

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

    state.update({
        "gates":       {i + 1: g for i, g in enumerate(gates)},
        "verdict":     verdict,
        "verdict_sub": sub,
        "pass_count":  pass_cnt,
        "spikes":      spikes,
        "ticker":      ticker,
        "last_chain":  chain,
        "last_macro":  indices,
        "last_stocks": stocks,
        "last_fii":    fii,
        "last_updated":time.time(),
    })
    logger.info(f"Verdict: {verdict} ({pass_cnt}/5) | VIX={vix:.1f} | PCR={pcr:.2f}")
