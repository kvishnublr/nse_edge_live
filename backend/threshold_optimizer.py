"""
NSE EDGE v5 — Gate Threshold Optimizer
Grid-searches GATE config values against 3-year backtest data
to maximise profit factor. Returns best config + full results table.
"""

import logging
import statistics
from itertools import product

logger = logging.getLogger("optimizer")

# ─── SEARCH SPACE ─────────────────────────────────────────────────────────────
SEARCH_SPACE = {
    "vix_low":        [10.0, 12.0, 14.0],
    "vix_medium":     [15.0, 16.0, 18.0],
    "vix_high":       [20.0, 22.0, 25.0],
    "pcr_bullish":    [1.1, 1.2, 1.3],
    "pcr_bearish":    [0.7, 0.8, 0.9],
    "vol_surge_min":  [1.3, 1.5, 1.8],
}


def _g1(vix, vix_chg, fii_net, th):
    score = 100
    if vix >= th["vix_high"]:     score -= 50
    elif vix >= th["vix_medium"]: score -= 25
    elif vix >= th["vix_low"]:    score -= 10
    if fii_net < -1000:           score -= 20
    elif fii_net < 0:             score -= 10
    if vix_chg > 5:               score -= 10
    score = max(0, min(100, score))
    if vix >= th["vix_high"] or fii_net < -2000:
        st = "st"
    elif vix >= th["vix_medium"] or fii_net < 0:
        st = "am"
    elif score >= 70:
        st = "go"
    else:
        st = "wt"
    return st


def _g2(pcr, call_oi, put_oi, th):
    net   = put_oi - call_oi
    score = 60
    if pcr >= th["pcr_bullish"]:   score += 25
    elif pcr <= th["pcr_bearish"]: score -= 25
    if net > 100_000:              score += 15
    elif net < -100_000:           score -= 15
    score = max(0, min(100, score))
    if score <= 35 or pcr <= th["pcr_bearish"]:
        st = "st"
    elif score >= 70 and pcr >= th["pcr_bullish"]:
        st = "go"
    elif score >= 55:
        st = "am"
    else:
        st = "wt"
    return st


def _g3(close, high, low):
    vwap    = (high + low + close) / 3
    pct     = (close - vwap) / vwap * 100 if vwap else 0
    rng     = high - low if high > low else 1
    pos_pct = (close - low) / rng * 100
    score = 60
    if pct > 0.1:      score += 20
    elif pct < -0.15:  score -= 20
    if pos_pct >= 60:  score += 15
    elif pos_pct < 40: score -= 10
    score = max(0, min(100, score))
    return "go" if score >= 70 else "am" if score >= 50 else "st"


def _g4(close, prev_close, volume, avg_vol, th):
    chg_pct = abs((close - prev_close) / prev_close * 100) if prev_close else 0
    no_vol  = avg_vol == 0 or volume == 0
    if no_vol:
        score = 50
        if chg_pct >= 1.0:   score += 30
        elif chg_pct >= 0.5: score += 15
        elif chg_pct >= 0.3: score += 5
        else:                 score -= 10
    else:
        vol_mult = volume / avg_vol
        score = 50
        if vol_mult >= th["vol_surge_min"]: score += 20
        elif vol_mult >= 1.0:               score += 5
        if chg_pct >= 0.5:   score += 10
        if vol_mult >= 2.0:  score += 10
    score = max(0, min(100, score))
    return "go" if score >= 70 else "wt" if score >= 45 else "am"


def _verdict(states):
    pass_cnt = states.count("go")
    has_fail = "st" in states
    if pass_cnt == 5:              return "EXECUTE"
    elif has_fail or pass_cnt < 3: return "NO TRADE"
    else:                          return "WAIT"


def _score_config(rows, th, atr_map):
    """
    rows: list of (date, open, high, low, close, volume, vix, vix_chg,
                   pcr, call_oi, put_oi, fii_net, prev_close, avg_vol, atr)
    """
    execute_pts = []
    for r in rows:
        (dt, o, h, l, c, vol, vix, vix_chg,
         pcr, call_oi, put_oi, fii_net,
         prev_close, avg_vol, atr_v, outcome_pts) = r

        if outcome_pts is None:
            continue

        s1 = _g1(vix, vix_chg, fii_net, th)
        s2 = _g2(pcr, call_oi, put_oi, th)
        s3 = _g3(c, h, l)
        s4 = _g4(c, prev_close, vol, avg_vol, th)
        # g5 uses ATR — keep same logic regardless of config
        s5 = "go" if atr_v > 0 else "wt"

        verdict = _verdict([s1, s2, s3, s4, s5])
        if verdict == "EXECUTE":
            execute_pts.append(outcome_pts)

    if len(execute_pts) < 5:
        return {"signals": len(execute_pts), "win_rate": 0, "profit_factor": 0, "total_pnl": 0}

    threshold = 20  # simplified fixed threshold for optimizer comparison
    wins   = [p for p in execute_pts if p >= threshold]
    losses = [p for p in execute_pts if p <= -threshold]

    win_rate = round(len(wins) / len(execute_pts) * 100, 1)
    avg_win  = statistics.mean(wins)   if wins   else 0
    avg_loss = abs(statistics.mean(losses)) if losses else 0
    pf       = round(avg_win / avg_loss, 2) if avg_loss > 0 else 0
    total    = round(sum(execute_pts), 1)

    return {
        "signals":       len(execute_pts),
        "win_rate":      win_rate,
        "profit_factor": pf,
        "total_pnl":     total,
    }


def run_optimizer() -> dict:
    """
    Load 3-year data from DB, grid-search threshold combinations,
    return top 10 configs + best config.
    """
    try:
        import backtest_data as bd
        bd.init_db()
        conn = bd.get_conn()

        ohlcv_raw = conn.execute(
            "SELECT date, open, high, low, close, volume FROM ohlcv ORDER BY date"
        ).fetchall()
        vix_map   = {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT date, vix, vix_chg FROM vix_daily"
        ).fetchall()}
        chain_map = {r[0]: (r[1], r[2], r[3]) for r in conn.execute(
            "SELECT date, pcr, total_call_oi, total_put_oi FROM chain_daily"
        ).fetchall()}
        fii_map   = {r[0]: r[1] for r in conn.execute(
            "SELECT date, fii_net FROM fii_daily"
        ).fetchall()}
        conn.close()

        if len(ohlcv_raw) < 30:
            return {"error": "Not enough data. Run data download first."}

        ohlcv = [{"date": r[0], "open": r[1], "high": r[2],
                  "low": r[3], "close": r[4], "volume": r[5]}
                 for r in ohlcv_raw]

        # Pre-compute per-row derived values
        rows = []
        for i, day in enumerate(ohlcv):
            if i < 20:
                continue
            dt      = day["date"]
            prior   = ohlcv[max(0, i - 20):i]
            prices  = [r["close"] for r in prior]
            trs     = [abs(prices[j] - prices[j-1]) for j in range(1, len(prices))]
            atr_v   = round(statistics.mean(trs[-14:]), 2) if trs else 90.0
            vols    = [r["volume"] for r in prior if r["volume"] > 0]
            avg_vol = statistics.mean(vols) if vols else 1

            vix_r   = vix_map.get(dt, (15.0, 0.0))
            ch_r    = chain_map.get(dt, (1.0, 500_000, 500_000))
            fii_net = fii_map.get(dt, 0.0)

            nxt_close   = ohlcv[i + 1]["close"] if i + 1 < len(ohlcv) else None
            outcome_pts = round(nxt_close - day["close"], 2) if nxt_close else None

            rows.append((
                dt, day["open"], day["high"], day["low"], day["close"], day["volume"],
                vix_r[0], vix_r[1],
                ch_r[0], ch_r[1], ch_r[2],
                fii_net,
                ohlcv[i - 1]["close"], avg_vol, atr_v,
                outcome_pts,
            ))

        # Grid search
        keys   = list(SEARCH_SPACE.keys())
        combos = list(product(*[SEARCH_SPACE[k] for k in keys]))
        logger.info(f"Optimizer: testing {len(combos)} threshold combinations on {len(rows)} days")

        results = []
        for combo in combos:
            th = dict(zip(keys, combo))
            # Skip invalid combinations
            if th["vix_low"] >= th["vix_medium"] or th["vix_medium"] >= th["vix_high"]:
                continue
            # Keep rr and atr_multiplier from current config
            from config import GATE as BASE
            th["rr_min_intraday"]   = BASE["rr_min_intraday"]
            th["rr_min_positional"] = BASE["rr_min_positional"]
            th["atr_multiplier"]    = BASE["atr_multiplier"]
            th["oi_build_min"]      = BASE["oi_build_min"]
            th["spike_price_pct"]   = BASE.get("spike_price_pct", 1.5)
            th["spike_vol_mult"]    = BASE.get("spike_vol_mult", 2.5)
            th["spike_oi_pct"]      = BASE.get("spike_oi_pct", 12.0)

            metrics = _score_config(rows, th, {})
            results.append({**{k: th[k] for k in keys}, **metrics})

        if not results:
            return {"error": "No valid combinations found"}

        # Sort by profit factor descending, then win_rate
        results.sort(key=lambda x: (x["profit_factor"], x["win_rate"]), reverse=True)
        best = results[0]

        logger.info(
            f"Optimizer best: PF={best['profit_factor']} WR={best['win_rate']}% "
            f"signals={best['signals']} "
            f"vix={best['vix_low']}/{best['vix_medium']}/{best['vix_high']} "
            f"pcr={best['pcr_bearish']}/{best['pcr_bullish']} "
            f"vol={best['vol_surge_min']}"
        )

        return {
            "best":    best,
            "top10":   results[:10],
            "tested":  len(results),
            "days":    len(rows),
        }

    except Exception as e:
        logger.error(f"Optimizer error: {e}", exc_info=True)
        return {"error": str(e)}
