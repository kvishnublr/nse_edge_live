"""
NSE EDGE v5 — Backtest Engine
Replays the 5-gate signal logic on 1-year OHLCV + VIX + PCR data.
Outcome = next day's close vs today's close (LONG simulation).
"""

import logging
import statistics
from datetime import datetime

from config import GATE as TH

logger = logging.getLogger("backtest")


# ─── HISTORICAL GATE RECONSTRUCTION ───────────────────────────────────────────
def _g1(vix: float, vix_chg: float, fii_net: float) -> dict:
    score = 100
    if vix >= TH["vix_high"]:       score -= 50
    elif vix >= TH["vix_medium"]:   score -= 25
    elif vix >= TH["vix_low"]:      score -= 10
    if fii_net < -1000:             score -= 20
    elif fii_net < 0:               score -= 10
    if vix_chg > 5:                 score -= 10
    score = max(0, min(100, score))

    if vix >= TH["vix_high"] or fii_net < -2000:
        st = "st"
    elif vix >= TH["vix_medium"] or fii_net < 0:
        st = "am"
    elif score >= 70:
        st = "go"
    else:
        st = "wt"
    return {"state": st, "score": score}


def _g2(pcr: float, call_oi: int, put_oi: int) -> dict:
    net   = put_oi - call_oi
    score = 60
    if pcr >= TH["pcr_bullish"]:   score += 25
    elif pcr <= TH["pcr_bearish"]: score -= 25
    if net > 100_000:              score += 15
    elif net < -100_000:           score -= 15
    score = max(0, min(100, score))

    if score <= 35 or pcr <= TH["pcr_bearish"]:
        st = "st"
    elif score >= 70 and pcr >= TH["pcr_bullish"]:
        st = "go"
    elif score >= 55:
        st = "am"
    else:
        st = "wt"
    return {"state": st, "score": score}


def _g3(close: float, high: float, low: float) -> dict:
    """Daily: VWAP proxy = (H+L+C)/3; range position = (C-L)/(H-L)."""
    vwap    = (high + low + close) / 3
    pct     = (close - vwap) / vwap * 100 if vwap else 0
    rng     = high - low if high > low else 1
    pos_pct = (close - low) / rng * 100

    score = 60
    if pct > 0.1:       score += 20
    elif pct < -0.15:   score -= 20
    if pos_pct >= 60:   score += 15
    elif pos_pct < 40:  score -= 10
    score = max(0, min(100, score))

    st = "go" if score >= 70 else "am" if score >= 50 else "st"
    return {"state": st, "score": score}


def _g4(close: float, prev_close: float, volume: int, avg_vol: float) -> dict:
    """Momentum from daily % change + volume vs 20-day average."""
    chg_pct  = (close - prev_close) / prev_close * 100 if prev_close else 0
    vol_mult = volume / avg_vol if avg_vol > 0 else 1.0

    score = 50
    if vol_mult >= TH["vol_surge_min"]: score += 20
    elif vol_mult >= 1.0:               score += 5
    if abs(chg_pct) >= 0.5:            score += 10
    if vol_mult >= 2.0:                 score += 10   # proxy for OI build
    score = max(0, min(100, score))

    st = "go" if score >= 70 else "wt" if score >= 45 else "am"
    return {"state": st, "score": score}


def _g5(close: float, atr: float, vix: float, mode: str = "intraday") -> dict:
    """ATR-based stop/target. No live chain → default 1:2.5 R:R."""
    if vix < TH["vix_low"]:       mult = 1.0
    elif vix < TH["vix_medium"]:  mult = 0.75
    elif vix < TH["vix_high"]:    mult = 0.50
    else:                          mult = 0.25

    stop_pts = atr * TH["atr_multiplier"]
    rr       = 2.5   # default when no live chain data
    rr_min   = TH["rr_min_intraday"] if mode == "intraday" else TH["rr_min_positional"]

    score = 60
    if rr >= rr_min:              score += 30
    elif rr >= rr_min * 0.8:      score += 10
    else:                          score -= 30
    if mult < 0.5:                 score -= 20
    score = max(0, min(100, score))

    st = "go" if (rr >= rr_min and mult >= 0.5) else \
         "st" if (rr < 1.5 or mult <= 0.25) else "am"
    return {"state": st, "score": score}


def _atr(history: list, periods: int = 14) -> float:
    if len(history) < 2:
        return 90.0
    prices = [r["close"] for r in history]
    trs    = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    window = trs[-periods:] if len(trs) >= periods else trs
    return round(statistics.mean(window), 2) if window else 90.0


def _verdict(gates: list):
    states   = [g["state"] for g in gates]
    pass_cnt = states.count("go")
    has_fail = "st" in states
    if pass_cnt == 5:
        return "EXECUTE", pass_cnt
    elif has_fail or pass_cnt < 3:
        return "NO TRADE", pass_cnt
    else:
        return "WAIT", pass_cnt


# ─── MAIN BACKTEST ────────────────────────────────────────────────────────────
def run_backtest(from_date: str, to_date: str, mode: str = "intraday") -> dict:
    """
    Simulate gate logic on historical daily data.
    Returns metrics + per-gate stats + last 100 trade records.
    """
    import backtest_data as bd
    bd.init_db()

    conn = bd.get_conn()

    ohlcv_raw = conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv "
        "WHERE date >= ? AND date <= ? ORDER BY date",
        (from_date, to_date)
    ).fetchall()

    vix_map   = {r[0]: (r[1], r[2]) for r in conn.execute(
        "SELECT date, vix, vix_chg FROM vix_daily WHERE date >= ? AND date <= ?",
        (from_date, to_date)
    ).fetchall()}

    chain_map = {r[0]: (r[1], r[2], r[3]) for r in conn.execute(
        "SELECT date, pcr, total_call_oi, total_put_oi FROM chain_daily "
        "WHERE date >= ? AND date <= ?",
        (from_date, to_date)
    ).fetchall()}

    fii_map   = {r[0]: r[1] for r in conn.execute(
        "SELECT date, fii_net FROM fii_daily WHERE date >= ? AND date <= ?",
        (from_date, to_date)
    ).fetchall()}

    conn.close()

    if not ohlcv_raw:
        return {
            "error":   "No OHLCV data. Run data download first.",
            "trades":  [],
            "metrics": {},
            "gate_stats": {},
        }

    ohlcv = [{"date": r[0], "open": r[1], "high": r[2],
               "low": r[3], "close": r[4], "volume": r[5]}
             for r in ohlcv_raw]

    trades     = []
    gate_track = {i: {"go": 0, "total": 0, "wins": 0} for i in range(1, 6)}

    for i, day in enumerate(ohlcv):
        if i < 20:
            continue
        dt    = day["date"]
        prior = ohlcv[max(0, i - 20):i]
        atr_v = _atr(prior)
        vols  = [r["volume"] for r in prior if r["volume"] > 0]
        avg_v = statistics.mean(vols) if vols else 1

        vix_r    = vix_map.get(dt, (15.0, 0.0))
        chain_r  = chain_map.get(dt, (1.0, 500_000, 500_000))
        fii_net  = fii_map.get(dt, 0.0)

        g1 = _g1(vix_r[0], vix_r[1], fii_net)
        g2 = _g2(chain_r[0], chain_r[1], chain_r[2])
        g3 = _g3(day["close"], day["high"], day["low"])
        g4 = _g4(day["close"], ohlcv[i - 1]["close"], day["volume"], avg_v)
        g5 = _g5(day["close"], atr_v, vix_r[0], mode)

        verdict, pass_cnt = _verdict([g1, g2, g3, g4, g5])

        # Outcome: next calendar day
        nxt_close   = ohlcv[i + 1]["close"] if i + 1 < len(ohlcv) else None
        outcome_pts = round(nxt_close - day["close"], 2) if nxt_close else None
        outcome     = None
        if outcome_pts is not None:
            outcome = "WIN" if outcome_pts >= 30 else "LOSS" if outcome_pts <= -30 else "NEUTRAL"

        record = {
            "date":        dt,
            "nifty":       day["close"],
            "vix":         vix_r[0],
            "pcr":         chain_r[0],
            "fii_net":     fii_net,
            "g1": g1["state"], "g1_score": g1["score"],
            "g2": g2["state"], "g2_score": g2["score"],
            "g3": g3["state"], "g3_score": g3["score"],
            "g4": g4["state"], "g4_score": g4["score"],
            "g5": g5["state"], "g5_score": g5["score"],
            "verdict":     verdict,
            "pass_count":  pass_cnt,
            "nxt_close":   nxt_close,
            "outcome_pts": outcome_pts,
            "outcome":     outcome,
            "atr":         atr_v,
        }
        trades.append(record)

        for gi, g in enumerate([g1, g2, g3, g4, g5], 1):
            gate_track[gi]["total"] += 1
            if g["state"] == "go":
                gate_track[gi]["go"] += 1
                if outcome == "WIN":
                    gate_track[gi]["wins"] += 1

    # Save to DB
    _save_to_db(trades)

    execute  = [t for t in trades if t["verdict"] == "EXECUTE"]
    wins     = [t for t in execute if t["outcome"] == "WIN"]
    losses   = [t for t in execute if t["outcome"] == "LOSS"]

    win_rate = round(len(wins) / len(execute) * 100, 1) if execute else 0.0
    avg_win  = round(statistics.mean([t["outcome_pts"] for t in wins]),   2) if wins   else 0.0
    avg_loss = round(statistics.mean([t["outcome_pts"] for t in losses]), 2) if losses else 0.0
    pf       = round(abs(avg_win / avg_loss), 2) if avg_loss and avg_win else 0.0
    total_pnl = round(sum(t["outcome_pts"] for t in execute if t["outcome_pts"] is not None), 2)

    gate_stats = {}
    for gi, st in gate_track.items():
        go  = st["go"]
        tot = st["total"]
        win = st["wins"]
        gate_stats[gi] = {
            "pass_rate":         round(go / tot * 100, 1) if tot else 0,
            "win_rate_when_go":  round(win / go * 100, 1) if go  else 0,
            "sample":            go,
        }

    metrics = {
        "total_days":      len(trades),
        "execute_signals": len(execute),
        "wins":            len(wins),
        "losses":          len(losses),
        "neutrals":        len([t for t in execute if t["outcome"] == "NEUTRAL"]),
        "win_rate_pct":    win_rate,
        "avg_win_pts":     avg_win,
        "avg_loss_pts":    avg_loss,
        "profit_factor":   pf,
        "total_pnl_pts":   total_pnl,
        "from_date":       from_date,
        "to_date":         to_date,
        "mode":            mode,
        "data_coverage":   {
            "ohlcv":  len(ohlcv),
            "vix":    len(vix_map),
            "chain":  len(chain_map),
            "fii":    len(fii_map),
        },
    }

    logger.info(
        f"Backtest {from_date}→{to_date}: {len(trades)} days, "
        f"{len(execute)} EXECUTE, WR={win_rate}%, PF={pf}"
    )

    return {
        "trades":     trades[-100:],
        "metrics":    metrics,
        "gate_stats": gate_stats,
    }


def _save_to_db(trades: list):
    import backtest_data as bd
    conn = bd.get_conn()
    conn.execute("DELETE FROM signal_log WHERE session='backtest'")
    rows = [(
        t["date"], "backtest",
        t["g1"], t["g2"], t["g3"], t["g4"], t["g5"],
        t["g1_score"], t["g2_score"], t["g3_score"], t["g4_score"], t["g5_score"],
        t["verdict"], t["pass_count"],
        t["nifty"], t["vix"], t["pcr"], t["fii_net"],
        t.get("nxt_close"), t.get("outcome_pts"), t.get("outcome"),
        0.0,
    ) for t in trades]
    conn.executemany("""
        INSERT INTO signal_log
        (date, session, g1, g2, g3, g4, g5, g1_score, g2_score, g3_score,
         g4_score, g5_score, verdict, pass_count, nifty, vix, pcr, fii_net,
         nifty_next, outcome_pts, outcome, ts)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    conn.close()
