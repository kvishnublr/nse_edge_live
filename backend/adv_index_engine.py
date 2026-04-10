"""
ADV INDEX — NIFTY 50 weighted futures OI + cash move composite (live),
plus a 30D backtest using *price breadth only* (documented limitation).

Live: weights from NSE (or fallback), near-month stock futures OI change,
equity %chg from Kite ticker cache — combined into a directional score.
Backtest: no historical intraday OI in Kite → same score formula with OI=0,
i.e. weighted 5m stock returns vs next-window NIFTY path (honest label).
"""

from __future__ import annotations

import csv
import logging
import math
import statistics
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytz

from index_radar_logic import cm_from_candle

logger = logging.getLogger("adv_index_engine")
IST = pytz.timezone("Asia/Kolkata")

# Live thresholds (composite score -1..1 after tanh)
ADV_IX_LONG_TH = 0.055
ADV_IX_SHORT_TH = -0.055

# Backtest (price-breadth proxy — see module docstring)
BT_BREADTH_LONG = 0.038
BT_BREADTH_SHORT = -0.038
BT_T1_PCT = 0.11
BT_SL_PCT = 0.17
BT_MAX_FWD = 40
BT_BAR_STEP = 30
BT_CM0 = 600
BT_CM1 = 810
NIFTY_TOKEN = 256265

_DATA_DIR = Path(__file__).resolve().parent / "data"
DEFAULT_BACKTEST_CSV = _DATA_DIR / "adv_index_backtest_last.csv"


def _tanh_scale(x: float, scale: float = 12.0) -> float:
    return math.tanh((x or 0) / scale)


def compute_live_snapshot(kite) -> dict[str, Any]:
    """Build ADV INDEX snapshot; safe to call periodically from scheduler."""
    from feed import get_price
    from fetcher import fetch_nifty50_futures_quotes, get_nifty50_weights

    t0 = time.time()
    if not kite:
        return {"error": "no_kite", "ts": t0}

    try:
        futs, weights = fetch_nifty50_futures_quotes(kite)
    except Exception as e:
        logger.warning("ADV INDEX futures fetch failed: %s", e)
        return {"error": str(e), "ts": t0}

    wsum = sum(weights.values()) or 1.0
    rows: list[dict[str, Any]] = []
    breadth = 0.0
    oi_press = 0.0

    for sym, w in weights.items():
        wn = (w / wsum) * 100.0
        fq = futs.get(sym) or {}
        eq = get_price(sym) or {}
        chg = float(eq.get("chg_pct", 0) or 0)
        oi_p = float(fq.get("oi_chg_pct", 0) or 0)
        # OI build in direction of cash move → constructive; opposite → fade
        align = 1.0 if (chg >= 0) == (oi_p >= 0) or abs(oi_p) < 1.5 else -0.35
        oi_part = _tanh_scale(oi_p) * align
        px_part = _tanh_scale(chg * 2.5)
        contrib = (wn / 100.0) * (0.55 * px_part + 0.45 * oi_part)
        breadth += (wn / 100.0) * chg / 100.0
        oi_press += (wn / 100.0) * oi_part
        rows.append({
            "symbol": sym,
            "weight": round(wn, 3),
            "chg_pct": round(chg, 3),
            "oi_chg_pct": round(oi_p, 3),
            "contrib": round(contrib, 5),
        })

    rows.sort(key=lambda r: abs(r.get("contrib", 0)), reverse=True)
    composite = sum(r["contrib"] for r in rows)
    composite = max(-1.0, min(1.0, composite * 2.8))

    if composite >= ADV_IX_LONG_TH:
        bias = "LONG"
    elif composite <= ADV_IX_SHORT_TH:
        bias = "SHORT"
    else:
        bias = "NEUTRAL"

    return {
        "ts": t0,
        "bias": bias,
        "score": round(composite, 4),
        "breadth_chg": round(breadth * 100, 4),
        "oi_pressure": round(oi_press, 4),
        "n_weights": len(weights),
        "n_futures_quoted": len(futs),
        "top_contributors": rows[:12],
        "disclaimer": (
            "ADV INDEX is a composite of NIFTY 50 weights, stock %chg, and "
            "near-month futures OI change — not a guaranteed edge. "
            "Confirm with IDX RADAR / risk rules before trading."
        ),
        "backtest_note": (
            "30D API backtest uses the same breadth formula with OI term zeroed "
            "(Kite does not provide minute OI history for 50 names)."
        ),
    }


def _eq_tokens(kite) -> dict[str, int]:
    out: dict[str, int] = {}
    try:
        for i in kite.instruments("NSE"):
            if i.get("instrument_type") != "EQ":
                continue
            ts = str(i.get("tradingsymbol", "") or "").upper()
            tok = i.get("instrument_token")
            if ts and tok:
                out[ts] = int(tok)
    except Exception as e:
        logger.warning("ADV INDEX eq tokens: %s", e)
    return out


def _group_by_date(candles: list) -> dict[str, list]:
    byd: dict[str, list] = defaultdict(list)
    for c in candles or []:
        try:
            d = c["date"].date().isoformat()
        except Exception:
            continue
        byd[d].append(c)
    for d in byd:
        byd[d].sort(key=lambda x: x["date"])
    return dict(byd)


def _expectancy_stats(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Avg win/loss (NIFTY %), mean signed return per resolved trade, profit factor."""
    resolved = [t for t in trades if t.get("outcome") in ("HIT_T1", "HIT_SL")]
    wins = [t for t in resolved if t["outcome"] == "HIT_T1"]
    losses = [t for t in resolved if t["outcome"] == "HIT_SL"]
    win_rets = [float(t.get("exit_pct") or 0) for t in wins]
    loss_rets = [float(t.get("exit_pct") or 0) for t in losses]
    avg_win = round(statistics.mean(win_rets), 4) if win_rets else None
    avg_loss = round(statistics.mean(loss_rets), 4) if loss_rets else None
    all_res = [float(t.get("exit_pct") or 0) for t in resolved]
    expectancy_pct = round(statistics.mean(all_res), 4) if all_res else None
    gross_profit = sum(win_rets)
    gross_loss_abs = sum(abs(x) for x in loss_rets)
    profit_factor = round(gross_profit / gross_loss_abs, 3) if gross_loss_abs > 0 else None
    expired_n = sum(1 for t in trades if t.get("outcome") == "EXPIRED")
    return {
        "n_resolved": len(resolved),
        "n_wins": len(wins),
        "n_losses": len(losses),
        "n_expired": expired_n,
        "avg_win_pct": avg_win,
        "avg_loss_pct": avg_loss,
        "expectancy_per_trade_pct": expectancy_pct,
        "profit_factor": profit_factor,
        "gross_profit_pct_sum": round(gross_profit, 4),
        "gross_loss_pct_sum_abs": round(gross_loss_abs, 4),
    }


def export_adv_index_trades_csv(trades: list[dict[str, Any]], path: Path | str) -> str:
    """Write all trades to CSV; returns absolute path string."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    keys = ["date", "cm", "direction", "breadth", "nifty_5m", "outcome", "exit_pct", "bars"]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for t in trades:
            w.writerow({k: t.get(k) for k in keys})
    return str(path.resolve())


def _close_at_or_before(mkt: list, target_cm: int) -> tuple[float, int]:
    best_px = 0.0
    best_cm = -1
    for c in mkt:
        cm = cm_from_candle(c)
        if cm <= target_cm and cm >= best_cm:
            best_cm = cm
            best_px = float(c.get("close") or 0)
    return best_px, best_cm


def run_adv_index_backtest(
    kite,
    days: int = 30,
    export_csv: bool | str = False,
) -> dict[str, Any]:
    """
    Walk-forward on last `days` calendar days (trading days inside range).
    Signal: weighted 5m cash returns (OI term omitted — see docstring).
    Outcome: next up to BT_MAX_FWD NIFTY minutes — T1/T1 short symmetric %.
    """
    from fetcher import get_nifty50_weights

    if not kite:
        return {"error": "Kite not available", "ok": False}

    weights = get_nifty50_weights()
    syms = [s for s in weights.keys() if s and s != "NIFTY"]
    to_d = datetime.now(IST).date()
    from_d = to_d - timedelta(days=int(days) + 15)

    try:
        nifty_hist = kite.historical_data(NIFTY_TOKEN, from_d, to_d, "minute") or []
    except Exception as e:
        return {"error": f"nifty hist: {e}", "ok": False}

    nifty_by = _group_by_date(nifty_hist)
    tok_map = _eq_tokens(kite)
    stock_hist: dict[str, dict[str, list]] = {}
    for sym in syms:
        tok = tok_map.get(sym)
        if not tok:
            continue
        try:
            h = kite.historical_data(tok, from_d, to_d, "minute") or []
        except Exception:
            continue
        if h:
            stock_hist[sym] = _group_by_date(h)

    wsum = sum(weights.values()) or 1.0
    trades: list[dict[str, Any]] = []

    for d_str in sorted(nifty_by.keys()):
        nm = nifty_by[d_str]
        if len(nm) < 30:
            continue
        for cm_sig in range(BT_CM0, BT_CM1 + 1, BT_BAR_STEP):
            px_n, cmn = _close_at_or_before(nm, cm_sig)
            px_n5, _ = _close_at_or_before(nm, cm_sig - 5)
            if px_n <= 0 or px_n5 <= 0:
                continue
            nifty_5m = (px_n - px_n5) / px_n5 * 100.0

            breadth = 0.0
            w_used = 0.0
            for sym, w in weights.items():
                smap = stock_hist.get(sym, {})
                day_m = smap.get(d_str)
                if not day_m:
                    continue
                p0, _ = _close_at_or_before(day_m, cm_sig)
                p5, _ = _close_at_or_before(day_m, cm_sig - 5)
                if p0 <= 0 or p5 <= 0:
                    continue
                r = (p0 - p5) / p5 * 100.0
                wi = w / wsum
                breadth += wi * r
                w_used += wi

            if w_used < 0.65:
                continue

            direction = None
            if breadth >= BT_BREADTH_LONG and nifty_5m > -0.22:
                direction = "LONG"
            elif breadth <= BT_BREADTH_SHORT and nifty_5m < 0.22:
                direction = "SHORT"

            if not direction:
                continue

            start_i = None
            for i, c in enumerate(nm):
                if cm_from_candle(c) > cm_sig:
                    start_i = i
                    break
            if start_i is None:
                continue

            entry = px_n
            outcome = "EXPIRED"
            exit_pct = 0.0
            bars = 0
            for j in range(start_i, min(start_i + BT_MAX_FWD, len(nm))):
                bars += 1
                cl = float(nm[j].get("close") or 0)
                if cl <= 0:
                    continue
                mvp = (cl - entry) / entry * 100.0
                if direction == "LONG":
                    if mvp >= BT_T1_PCT:
                        outcome = "HIT_T1"
                        exit_pct = mvp
                        break
                    if mvp <= -BT_SL_PCT:
                        outcome = "HIT_SL"
                        exit_pct = mvp
                        break
                else:
                    if mvp <= -BT_T1_PCT:
                        outcome = "HIT_T1"
                        exit_pct = -mvp
                        break
                    if mvp >= BT_SL_PCT:
                        outcome = "HIT_SL"
                        exit_pct = -mvp
                        break

            trades.append({
                "date": d_str,
                "cm": cm_sig,
                "direction": direction,
                "breadth": round(breadth, 4),
                "nifty_5m": round(nifty_5m, 4),
                "outcome": outcome,
                "exit_pct": round(exit_pct, 4),
                "bars": bars,
            })

    resolved = [t for t in trades if t["outcome"] in ("HIT_T1", "HIT_SL")]
    wins = sum(1 for t in resolved if t["outcome"] == "HIT_T1")
    n_res = len(resolved)
    wr = round(100.0 * wins / n_res, 2) if n_res else None
    ex_stats = _expectancy_stats(trades)

    out: dict[str, Any] = {
        "ok": True,
        "days_requested": days,
        "from_date": str(from_d),
        "to_date": str(to_d),
        "n_signals": len(trades),
        "resolved_n": n_res,
        "wins": wins,
        "win_rate_pct": wr,
        "expectancy": ex_stats,
        "params": {
            "breadth_long": BT_BREADTH_LONG,
            "breadth_short": BT_BREADTH_SHORT,
            "t1_pct": BT_T1_PCT,
            "sl_pct": BT_SL_PCT,
            "bar_step_min": BT_BAR_STEP,
        },
        "methodology": (
            "Proxy backtest: weighted 5m NIFTY 50 cash returns (no historical OI). "
            "Live ADV INDEX adds futures OI alignment — expect different behaviour."
        ),
        "trades": trades,
        "trades_sample": trades[:80],
        "trades_total_stored": len(trades),
    }

    if export_csv:
        csv_path = export_adv_index_trades_csv(
            trades,
            export_csv if isinstance(export_csv, str) else DEFAULT_BACKTEST_CSV,
        )
        out["csv_path"] = csv_path

    return out
