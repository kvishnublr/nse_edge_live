"""
Parameter sweep and report for Swing/Positional stock picks.

Runs the same candidate engine used by live Swing Radar, but evaluates
multiple parameter profiles in-memory on a shared historical dataset so
the comparison is apples-to-apples.
"""

from __future__ import annotations

import argparse
import bisect
import copy
import json
import statistics
from contextlib import contextmanager
from datetime import datetime, timedelta

from backtest_data import get_conn, init_db, swing_radar_candidates
from backtest_engine import _atr, _g1, _g2, _g3, _g4, _g5, _verdict
from config import FNO_SYMBOLS, KITE_TOKENS, SWING_RADAR, apply_strategy_profile
from feed import get_kite
from swing_radar_backtest import _dstr, _simulate_daily, summarize_swing_backtest_rows


def _load_market_context(from_date: str, to_date: str) -> dict:
    init_db()
    fd = datetime.strptime(str(from_date)[:10], "%Y-%m-%d").date()
    td = datetime.strptime(str(to_date)[:10], "%Y-%m-%d").date()
    context_from = fd - timedelta(days=120)
    conn = get_conn()
    try:
        ohlcv_raw = conn.execute(
            "SELECT date, open, high, low, close, volume FROM ohlcv WHERE date >= ? AND date <= ? ORDER BY date",
            (context_from.isoformat(), td.isoformat()),
        ).fetchall()
        vix_raw = conn.execute(
            "SELECT date, vix, vix_chg FROM vix_daily WHERE date >= ? AND date <= ?",
            (context_from.isoformat(), td.isoformat()),
        ).fetchall()
        chain_raw = conn.execute(
            "SELECT date, pcr, total_call_oi, total_put_oi FROM chain_daily WHERE date >= ? AND date <= ?",
            (context_from.isoformat(), td.isoformat()),
        ).fetchall()
        fii_raw = conn.execute(
            "SELECT date, fii_net, dii_net FROM fii_daily WHERE date >= ? AND date <= ?",
            (context_from.isoformat(), td.isoformat()),
        ).fetchall()
    finally:
        conn.close()

    ohlcv = [
        {"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5] or 0}
        for r in ohlcv_raw
    ]
    return {
        "fd": fd,
        "td": td,
        "ohlcv": ohlcv,
        "date_index": {row["date"]: i for i, row in enumerate(ohlcv)},
        "vix_map": {r[0]: (r[1], r[2]) for r in vix_raw},
        "chain_map": {r[0]: (r[1], r[2], r[3]) for r in chain_raw},
        "fii_map": {r[0]: (r[1], r[2]) for r in fii_raw},
    }


def _load_symbol_history(kite, from_date: str, to_date: str) -> dict[str, list[dict]]:
    fd = datetime.strptime(str(from_date)[:10], "%Y-%m-%d").date()
    td = datetime.strptime(str(to_date)[:10], "%Y-%m-%d").date()
    context_from = fd - timedelta(days=120)
    symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ("NIFTY", "BANKNIFTY", "INDIAVIX")]
    out: dict[str, list[dict]] = {}
    for sym in symbols:
        hist = kite.historical_data(int(KITE_TOKENS[sym]), context_from, td, "day") or []
        rows = []
        for c in hist:
            rows.append(
                {
                    "date": _dstr(c["date"]),
                    "open": float(c.get("open") or 0),
                    "high": float(c.get("high") or 0),
                    "low": float(c.get("low") or 0),
                    "close": float(c.get("close") or 0),
                    "volume": int(c.get("volume") or 0),
                }
            )
        rows.sort(key=lambda x: x["date"])
        if rows:
            out[sym] = rows
    return out


def _build_daily_candidates(context: dict, sym_series: dict[str, list[dict]], from_date: str, to_date: str) -> list[dict]:
    ohlcv = context["ohlcv"]
    date_index = context["date_index"]
    vix_map = context["vix_map"]
    chain_map = context["chain_map"]
    fii_map = context["fii_map"]
    trading_days = [row["date"] for row in ohlcv if str(from_date)[:10] <= row["date"] <= str(to_date)[:10]]

    day_rows = []
    for d in trading_days:
        idx = date_index.get(d)
        if idx is None or idx < 20:
            continue
        day = ohlcv[idx]
        prev = ohlcv[idx - 1]
        nifty_close = float(day["close"] or 0)
        prev_close = float(prev["close"] or 0)
        if nifty_close <= 0 or prev_close <= 0:
            continue

        chg_pct_nifty = (nifty_close - prev_close) / prev_close * 100
        prior_n = ohlcv[max(0, idx - 20): idx]
        atr_n = _atr([{"close": float(x["close"] or 0)} for x in prior_n])

        vix_r = vix_map.get(d, (15.0, 0.0))
        vix = float(vix_r[0] or 0)
        vix_chg = float(vix_r[1] or 0)
        chain_r = chain_map.get(d, (1.0, 500_000, 500_000))
        pcr = float(chain_r[0] or 1.0)
        call_oi = int(chain_r[1] or 0)
        put_oi = int(chain_r[2] or 0)
        fii_row = fii_map.get(d, (0.0, 0.0))
        fii_net = float(fii_row[0] or 0)
        if abs(fii_net) > 10000:
            fii_net = round(fii_net / 100.0, 2)

        g1 = _g1(vix, vix_chg, fii_net)
        g2 = _g2(pcr, call_oi, put_oi)
        g3i = _g3(nifty_close, float(day["high"] or 0), float(day["low"] or 0))
        vols = [float(x["volume"] or 0) for x in prior_n if (x.get("volume") or 0) > 0]
        avg_v = statistics.mean(vols) if vols else 1.0
        g4i = _g4(nifty_close, prev_close, int(day["volume"] or 0), avg_v)
        g5i = _g5(nifty_close, atr_n, vix, "intraday")
        verdict, pass_cnt = _verdict([g1, g2, g3i, g4i, g5i])

        stocks = []
        for sym, series in sym_series.items():
            dsorted = [x["date"] for x in series]
            si = bisect.bisect_left(dsorted, d)
            if si >= len(series) or series[si]["date"] != d:
                continue
            dr = series[si]
            dc = float(dr["close"] or 0)
            if dc <= 0:
                continue
            pv = float(series[si - 1]["close"] or dc) if si > 0 else dc
            dp = (dc - pv) / pv * 100 if pv else 0.0
            dv = int(dr.get("volume") or 0)
            prev_slice = series[max(0, si - 20): si]
            prev_vols = [float(x["volume"] or 0) for x in prev_slice if (x.get("volume") or 0) > 0]
            avg_vs = statistics.mean(prev_vols) if prev_vols else 1.0
            vol_r = round(dv / avg_vs, 1) if avg_vs > 0 and dv > 0 else 0.0
            oi_proxy = round(dp * 10.0, 2)

            trs = [
                abs(float(series[j]["close"] or 0) - float(series[j - 1]["close"] or 0))
                for j in range(1, si)
            ]
            atr_v = round(statistics.mean(trs[-14:]), 4) if len(trs) >= 14 else (round(statistics.mean(trs), 4) if trs else 90.0)
            atr_pct = round(atr_v / dc * 100.0, 1) if dc > 0 else 0.0

            g3 = "go" if abs(dp) >= 0.8 and abs(oi_proxy) >= 4 else "am" if abs(dp) >= 0.35 else "st"
            g4 = "go" if vol_r >= 1.5 and abs(dp) >= 0.6 else "wt" if vol_r >= 1.1 or abs(dp) >= 0.4 else "st"
            g5 = "go" if atr_pct > 0 and abs(dp) >= max(0.6, atr_pct * 0.35) else "am" if abs(dp) >= 0.35 else "st"
            stock_pc = len([x for x in (g1["state"], g2["state"], g3, g4, g5) if x == "go"])
            stock_score = int(min(99, max(40, 35 + stock_pc * 10 + (8 if vol_r >= 1.5 else 0))))

            stocks.append(
                {
                    "symbol": sym,
                    "price": dc,
                    "chg_pct": round(dp, 4),
                    "oi_chg_pct": oi_proxy,
                    "vol_ratio": vol_r,
                    "atr_pct": atr_pct,
                    "rs_pct": round(dp - chg_pct_nifty, 2),
                    "pc": stock_pc,
                    "score": stock_score,
                }
            )

        day_rows.append(
            {
                "date": d,
                "stocks": stocks,
                "indices": {"vix": vix, "nifty_chg": round(chg_pct_nifty, 4)},
                "chain": {"pcr": pcr},
                "verdict": verdict,
                "pass_cnt": pass_cnt,
            }
        )
    return day_rows


@contextmanager
def _temporary_swing_config(overrides: dict):
    original = copy.deepcopy(SWING_RADAR)
    try:
        SWING_RADAR.clear()
        SWING_RADAR.update(original)
        SWING_RADAR.update(overrides or {})
        yield
    finally:
        SWING_RADAR.clear()
        SWING_RADAR.update(original)


def evaluate_variant(name: str, overrides: dict, day_rows: list[dict], sym_series: dict[str, list[dict]], max_forward_days: int) -> dict:
    rows = []
    daily_pick_counts = []
    unique_symbols = set()
    with _temporary_swing_config(overrides):
        for day_row in day_rows:
            picks = swing_radar_candidates(
                day_row["stocks"],
                day_row["indices"],
                day_row["chain"],
                day_row["verdict"],
                day_row["pass_cnt"],
            )
            daily_pick_counts.append(len(picks))
            for p in picks:
                sym = p["sym"]
                series = sym_series.get(sym) or []
                dates = [x["date"] for x in series]
                si = bisect.bisect_left(dates, day_row["date"])
                if si >= len(series) or series[si]["date"] != day_row["date"]:
                    continue
                outcome, exit_px, exit_d, pnl_pct, hold_steps = _simulate_daily(
                    series,
                    si,
                    p["direction"],
                    p["entry"],
                    p["stop"],
                    p["target"],
                    max_forward_days,
                )
                rows.append(
                    {
                        "trade_date": day_row["date"],
                        "symbol": sym,
                        "signal_type": p["direction"],
                        "trigger": f"BACKTEST | Swing {p['setup']} | {p['sig_lbl']}",
                        "outcome": outcome,
                        "pnl_pct": float(pnl_pct),
                        "hold_minutes": int(max(1, hold_steps) * 24 * 60),
                    }
                )
                unique_symbols.add(sym)

    report = summarize_swing_backtest_rows(rows)
    report.update(
        {
            "variant": name,
            "avg_daily_picks": round(statistics.mean(daily_pick_counts), 2) if daily_pick_counts else 0.0,
            "max_daily_picks": max(daily_pick_counts) if daily_pick_counts else 0,
            "active_days": sum(1 for x in daily_pick_counts if x > 0),
            "unique_symbols": len(unique_symbols),
            "config": copy.deepcopy(overrides),
        }
    )
    return report


def _profile_variants() -> list[tuple[str, dict]]:
    variants = []
    for profile_name in ("legacy", "balanced_v2", "precision_v3"):
        apply_strategy_profile(profile_name)
        variants.append((f"profile:{profile_name}", copy.deepcopy(SWING_RADAR)))
    return variants


def _custom_variants() -> list[tuple[str, dict]]:
    return [
        (
            "coverage_plus",
            {
                "min_score_log": 60,
                "min_pc_log": 3,
                "min_rr": 1.8,
                "min_abs_chg_pct": 0.2,
                "max_abs_chg_pct": 3.2,
                "min_vol_ratio": 0.95,
                "counter_trend_min_pc": 4,
                "pcr_soft_min_pc": 4,
                "allowed_setups": [],
                "allowed_directions": [],
                "pullback_short_only": False,
                "breakout_long_enabled": True,
                "breakout_long_min_pc": 3,
                "breakout_long_min_rs": 0.05,
                "breakout_long_min_vol": 1.1,
                "breakout_long_max_chg": 2.8,
                "recovery_vol_min": 1.0,
                "blocked_symbols": [],
            },
        ),
        (
            "coverage_guarded",
            {
                "min_score_log": 64,
                "min_pc_log": 3,
                "min_rr": 1.85,
                "min_abs_chg_pct": 0.25,
                "max_abs_chg_pct": 3.0,
                "min_vol_ratio": 1.0,
                "counter_trend_min_pc": 4,
                "pcr_soft_min_pc": 4,
                "allowed_setups": ["Pullback", "Breakout"],
                "allowed_directions": [],
                "pullback_short_only": False,
                "breakout_long_enabled": True,
                "breakout_long_min_pc": 3,
                "breakout_long_min_rs": 0.1,
                "breakout_long_min_vol": 1.2,
                "breakout_long_max_chg": 2.7,
                "recovery_vol_min": 1.02,
                "blocked_symbols": ["BAJFINANCE", "INDUSINDBK"],
            },
        ),
        (
            "balanced_plus",
            {
                "min_score_log": 66,
                "min_pc_log": 4,
                "min_rr": 1.9,
                "min_abs_chg_pct": 0.3,
                "max_abs_chg_pct": 2.8,
                "min_vol_ratio": 1.0,
                "counter_trend_min_pc": 5,
                "pcr_soft_min_pc": 5,
                "allowed_setups": ["Pullback", "Breakout"],
                "allowed_directions": [],
                "pullback_short_only": False,
                "breakout_long_enabled": True,
                "breakout_long_min_pc": 4,
                "breakout_long_min_rs": 0.15,
                "breakout_long_min_vol": 1.3,
                "breakout_long_max_chg": 2.4,
                "recovery_vol_min": 1.05,
                "blocked_symbols": ["BAJFINANCE", "INDUSINDBK", "TATAMOTORS"],
            },
        ),
        (
            "balanced_pullback_only",
            {
                "min_score_log": 68,
                "min_pc_log": 4,
                "min_rr": 1.9,
                "min_abs_chg_pct": 0.3,
                "max_abs_chg_pct": 2.6,
                "min_vol_ratio": 1.0,
                "counter_trend_min_pc": 5,
                "pcr_soft_min_pc": 5,
                "allowed_setups": ["Pullback"],
                "allowed_directions": ["SHORT"],
                "pullback_short_only": True,
                "pullback_short_min_pc": 4,
                "pullback_short_min_rank_score": 68,
                "breakout_long_enabled": False,
                "blocked_symbols": ["BAJFINANCE", "INDUSINDBK", "TATAMOTORS"],
            },
        ),
        (
            "precision_dual",
            {
                "min_score_log": 70,
                "min_pc_log": 4,
                "min_rr": 2.0,
                "min_abs_chg_pct": 0.35,
                "max_abs_chg_pct": 2.4,
                "min_vol_ratio": 1.05,
                "counter_trend_min_pc": 5,
                "pcr_soft_min_pc": 5,
                "allowed_setups": ["Pullback", "Breakout"],
                "allowed_directions": [],
                "pullback_short_only": False,
                "pullback_short_min_pc": 4,
                "pullback_short_min_rank_score": 72,
                "breakout_long_enabled": True,
                "breakout_long_min_pc": 4,
                "breakout_long_min_rs": 0.2,
                "breakout_long_min_vol": 1.4,
                "breakout_long_max_chg": 2.2,
                "blocked_symbols": ["BAJFINANCE", "INDUSINDBK", "TATAMOTORS", "TATASTEEL"],
            },
        ),
        (
            "precision_v3_plus",
            {
                "min_score_log": 72,
                "min_pc_log": 4,
                "min_rr": 2.0,
                "min_abs_chg_pct": 0.35,
                "max_abs_chg_pct": 2.5,
                "min_vol_ratio": 1.1,
                "counter_trend_min_pc": 5,
                "pcr_soft_min_pc": 5,
                "allowed_setups": ["Pullback"],
                "allowed_directions": ["SHORT"],
                "pullback_short_only": True,
                "pullback_short_min_pc": 4,
                "pullback_short_min_rank_score": 74,
                "breakout_long_enabled": False,
                "blocked_symbols": ["BAJFINANCE", "TATASTEEL", "INDUSINDBK", "LT", "MARUTI", "SUNPHARMA", "TATAMOTORS"],
            },
        ),
        (
            "precision_short_only",
            {
                "min_score_log": 72,
                "min_pc_log": 4,
                "min_rr": 2.05,
                "min_abs_chg_pct": 0.35,
                "max_abs_chg_pct": 2.5,
                "min_vol_ratio": 1.05,
                "counter_trend_min_pc": 5,
                "pcr_soft_min_pc": 5,
                "allowed_setups": ["Pullback"],
                "allowed_directions": ["SHORT"],
                "pullback_short_only": True,
                "pullback_short_min_pc": 4,
                "pullback_short_min_rank_score": 74,
                "breakout_long_enabled": False,
                "blocked_symbols": ["BAJFINANCE", "TATASTEEL", "INDUSINDBK", "LT", "MARUTI", "SUNPHARMA", "TATAMOTORS"],
            },
        ),
        (
            "elite_short_only",
            {
                "min_score_log": 76,
                "min_pc_log": 5,
                "min_rr": 2.1,
                "min_abs_chg_pct": 0.45,
                "max_abs_chg_pct": 2.3,
                "min_vol_ratio": 1.15,
                "counter_trend_min_pc": 5,
                "pcr_soft_min_pc": 5,
                "allowed_setups": ["Pullback"],
                "allowed_directions": ["SHORT"],
                "pullback_short_only": True,
                "pullback_short_min_pc": 5,
                "pullback_short_min_rank_score": 76,
                "breakout_long_enabled": False,
                "blocked_symbols": ["BAJFINANCE", "TATASTEEL", "INDUSINDBK", "LT", "MARUTI", "SUNPHARMA", "TATAMOTORS", "RELIANCE"],
            },
        ),
    ]


def _format_table(rows: list[dict]) -> str:
    headers = [
        "variant",
        "trades",
        "resolved_wr",
        "all_wr",
        "avg_pnl",
        "pf",
        "days",
        "avg/day",
        "symbols",
    ]
    data = []
    for r in rows:
        data.append(
            [
                r["variant"],
                str(r["total_trades"]),
                f"{r['win_rate_pct']:.1f}",
                f"{r['resolved_win_rate_pct']:.1f}",
                f"{r['avg_pnl_pct']:.2f}",
                f"{r['profit_factor']:.2f}",
                str(r["active_days"]),
                f"{r['avg_daily_picks']:.2f}",
                str(r["unique_symbols"]),
            ]
        )
    widths = [len(h) for h in headers]
    for row in data:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))
    lines = []
    lines.append(" | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)))
    lines.append("-+-".join("-" * widths[i] for i in range(len(headers))))
    for row in data:
        lines.append(" | ".join(row[i].ljust(widths[i]) for i in range(len(headers))))
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-date", default="2023-04-01")
    parser.add_argument("--to-date", default="2026-04-01")
    parser.add_argument("--max-forward-days", type=int, default=12)
    parser.add_argument("--json-out", default="")
    args = parser.parse_args()

    kite = get_kite()
    if not kite:
        raise SystemExit("Kite session unavailable.")

    context = _load_market_context(args.from_date, args.to_date)
    sym_series = _load_symbol_history(kite, args.from_date, args.to_date)
    day_rows = _build_daily_candidates(context, sym_series, args.from_date, args.to_date)

    results = []
    for name, overrides in [*_profile_variants(), *_custom_variants()]:
        results.append(evaluate_variant(name, overrides, day_rows, sym_series, args.max_forward_days))

    ranked = sorted(
        results,
        key=lambda x: (
            -x["win_rate_pct"],
            -x["resolved_win_rate_pct"],
            -x["profit_factor"],
            -x["avg_pnl_pct"],
            -x["total_trades"],
        ),
    )

    payload = {
        "from_date": args.from_date,
        "to_date": args.to_date,
        "max_forward_days": args.max_forward_days,
        "universe_size": len(sym_series),
        "variants": ranked,
    }

    print(json.dumps({"meta": {k: payload[k] for k in ("from_date", "to_date", "max_forward_days", "universe_size")}}, indent=2))
    print()
    print(_format_table(ranked))

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        print()
        print(f"Saved report to {args.json_out}")


if __name__ == "__main__":
    main()
