"""
Replay Swing Radar on historical dates using:
  - NIFTY / VIX / PCR / FII from backtest.db (ohlcv, vix_daily, chain_daily, fii_daily)
  - Per-stock daily OHLCV from Kite (one historical_data call per symbol)
  - Same candidate rules as live via backtest_data.swing_radar_candidates
Outcomes: forward scan on daily bars (TARGET HIT / SL HIT / EXPIRED) within max_forward_days.
Rows use signal_key BT-SWING|... and verdict SWING_RADAR so Swing History UI picks them up.
"""

from __future__ import annotations

import bisect
import logging
import statistics
import time
from datetime import datetime, timedelta

import pytz

from backtest_data import get_conn, init_db, swing_radar_candidates, _gate_snapshot_json
from backtest_engine import _atr, _g1, _g2, _g3, _g4, _g5, _verdict

logger = logging.getLogger("swing_radar_backtest")
IST = pytz.timezone("Asia/Kolkata")


def _dstr(x) -> str:
    if hasattr(x, "strftime"):
        return x.strftime("%Y-%m-%d")
    return str(x)[:10]


def _simulate_daily(sym_series: list, start_i: int, direction: str, entry: float, stop: float, target: float, max_forward: int):
    """
    sym_series: sorted list of dicts date, open, high, low, close, volume
    start_i: index of signal day; first evaluation day is start_i + 1.
    """
    n = len(sym_series)
    last_close = entry
    last_date = sym_series[start_i]["date"]
    for step in range(1, max_forward + 1):
        j = start_i + step
        if j >= n:
            break
        day = sym_series[j]
        hi = float(day["high"] or 0)
        lo = float(day["low"] or 0)
        cl = float(day["close"] or 0)
        d0 = day["date"]
        if direction == "LONG":
            if lo <= stop and stop > 0:
                pct = (stop - entry) / entry * 100 if entry else 0.0
                return "SL HIT", stop, d0, round(pct, 4), step
            if hi >= target and target > 0:
                pct = (target - entry) / entry * 100 if entry else 0.0
                return "TARGET HIT", target, d0, round(pct, 4), step
        else:
            if hi >= stop and stop > 0:
                pct = (entry - stop) / entry * 100 if entry else 0.0
                return "SL HIT", stop, d0, round(pct, 4), step
            if lo <= target and target > 0:
                pct = (entry - target) / entry * 100 if entry else 0.0
                return "TARGET HIT", target, d0, round(pct, 4), step
        last_close = cl
        last_date = d0
    pct = (last_close - entry) / entry * 100 if entry else 0.0
    if direction != "LONG":
        pct = (entry - last_close) / entry * 100 if entry else 0.0
    return "EXPIRED", round(last_close, 2), last_date, round(pct, 4), max_forward


def summarize_swing_backtest_rows(rows: list[dict]) -> dict:
    if not rows:
        return {
            "total_trades": 0,
            "resolved_trades": 0,
            "target_hits": 0,
            "sl_hits": 0,
            "expired": 0,
            "win_rate_pct": 0.0,
            "resolved_win_rate_pct": 0.0,
            "avg_pnl_pct": 0.0,
            "median_pnl_pct": 0.0,
            "profit_factor": 0.0,
            "expectancy_pct": 0.0,
            "avg_hold_days": 0.0,
            "gross_profit_pct_sum": 0.0,
            "gross_loss_pct_sum_abs": 0.0,
            "by_setup": [],
            "by_month": [],
            "top_symbols": [],
            "bottom_symbols": [],
        }

    wins = [r for r in rows if r.get("outcome") == "TARGET HIT"]
    losses = [r for r in rows if r.get("outcome") == "SL HIT"]
    expired = [r for r in rows if r.get("outcome") == "EXPIRED"]
    pnl_values = [float(r.get("pnl_pct") or 0.0) for r in rows]
    gross_profit = round(sum(v for v in pnl_values if v > 0), 4)
    gross_loss_abs = round(sum(-v for v in pnl_values if v < 0), 4)
    resolved_win_rate = round(len(wins) / len(rows) * 100, 1) if rows else 0.0
    wl_base = len(wins) + len(losses)
    win_rate = round(len(wins) / wl_base * 100, 1) if wl_base else 0.0
    avg_pnl = round(statistics.mean(pnl_values), 4) if pnl_values else 0.0
    med_pnl = round(statistics.median(pnl_values), 4) if pnl_values else 0.0
    pf = round(gross_profit / gross_loss_abs, 3) if gross_loss_abs > 0 else None
    hold_days = [max(0.0, float(r.get("hold_minutes") or 0) / 1440.0) for r in rows]

    def _bucket(key_fn):
        buckets: dict[str, dict[str, float]] = {}
        for row in rows:
            key = key_fn(row)
            bucket = buckets.setdefault(key, {"n": 0, "wins": 0, "sum_pnl": 0.0})
            bucket["n"] += 1
            if row.get("outcome") == "TARGET HIT":
                bucket["wins"] += 1
            bucket["sum_pnl"] += float(row.get("pnl_pct") or 0.0)
        return buckets

    def _setup_from_trigger(trigger: str) -> str:
        txt = str(trigger or "")
        if "Swing " in txt and " |" in txt:
            try:
                return txt.split("Swing ", 1)[1].split(" |", 1)[0].strip()
            except Exception:
                return "Unknown"
        return "Unknown"

    setup_buckets = _bucket(lambda r: _setup_from_trigger(r.get("trigger")))
    month_buckets = _bucket(lambda r: str(r.get("trade_date") or "")[:7])
    symbol_buckets = _bucket(lambda r: str(r.get("symbol") or ""))

    def _rows_from_buckets(buckets: dict[str, dict[str, float]], *, min_n: int = 1) -> list[dict]:
        out = []
        for key, data in buckets.items():
            n = int(data["n"])
            if n < min_n:
                continue
            out.append({
                "key": key,
                "trades": n,
                "win_rate_pct": round(data["wins"] / n * 100, 1) if n else 0.0,
                "avg_pnl_pct": round(data["sum_pnl"] / n, 4) if n else 0.0,
            })
        return out

    top_symbols = sorted(_rows_from_buckets(symbol_buckets, min_n=2), key=lambda x: (-x["avg_pnl_pct"], -x["trades"], x["key"]))[:5]
    bottom_symbols = sorted(_rows_from_buckets(symbol_buckets, min_n=2), key=lambda x: (x["avg_pnl_pct"], -x["trades"], x["key"]))[:5]

    return {
        "total_trades": len(rows),
        "resolved_trades": len(rows),
        "target_hits": len(wins),
        "sl_hits": len(losses),
        "expired": len(expired),
        "win_rate_pct": win_rate,
        "resolved_win_rate_pct": resolved_win_rate,
        "avg_pnl_pct": avg_pnl,
        "median_pnl_pct": med_pnl,
        "profit_factor": pf or 0.0,
        "expectancy_pct": avg_pnl,
        "avg_hold_days": round(statistics.mean(hold_days), 2) if hold_days else 0.0,
        "gross_profit_pct_sum": gross_profit,
        "gross_loss_pct_sum_abs": gross_loss_abs,
        "by_setup": sorted(_rows_from_buckets(setup_buckets), key=lambda x: (-x["avg_pnl_pct"], -x["trades"], x["key"])),
        "by_month": sorted(_rows_from_buckets(month_buckets), key=lambda x: x["key"]),
        "top_symbols": top_symbols,
        "bottom_symbols": bottom_symbols,
    }


def build_swing_backtest_report(from_date: str | None = None, to_date: str | None = None) -> dict:
    init_db()
    conn = get_conn()
    conn.row_factory = None
    where = ["signal_key LIKE 'BT-SWING|%'"]
    params: list[str] = []
    if from_date:
        where.append("trade_date >= ?")
        params.append(str(from_date)[:10])
    if to_date:
        where.append("trade_date <= ?")
        params.append(str(to_date)[:10])

    rows = conn.execute(
        f"""
        SELECT trade_date, symbol, signal_type, trigger, outcome, pnl_pct, hold_minutes
        FROM live_signal_history
        WHERE {' AND '.join(where)}
        ORDER BY trade_date ASC, id ASC
        """,
        params,
    ).fetchall()
    conn.close()

    data = [
        {
            "trade_date": r[0],
            "symbol": r[1],
            "signal_type": r[2],
            "trigger": r[3],
            "outcome": r[4],
            "pnl_pct": float(r[5] or 0.0),
            "hold_minutes": int(r[6] or 0),
        }
        for r in rows
    ]
    report = summarize_swing_backtest_rows(data)
    report.update({
        "from_date": str(from_date)[:10] if from_date else (data[0]["trade_date"] if data else None),
        "to_date": str(to_date)[:10] if to_date else (data[-1]["trade_date"] if data else None),
    })
    return report


def run_swing_radar_backtest(
    kite,
    from_date: str,
    to_date: str,
    max_forward_days: int = 12,
    max_calendar_span_days: int = 1095,
    clear_existing: bool = False,
) -> dict:
    """
    Returns {inserted, skipped_days, errors, from_date, to_date, symbols_loaded}
    """
    init_db()
    conn = get_conn()
    try:
        fd = datetime.strptime(str(from_date)[:10], "%Y-%m-%d").date()
        td = datetime.strptime(str(to_date)[:10], "%Y-%m-%d").date()
    except ValueError as e:
        conn.close()
        return {"error": f"Invalid date: {e}", "inserted": 0}

    if td < fd:
        conn.close()
        return {"error": "to_date must be >= from_date", "inserted": 0}
    if (td - fd).days > max_calendar_span_days:
        conn.close()
        return {"error": f"Range exceeds {max_calendar_span_days} days (~3y cap)", "inserted": 0}

    cleared_existing = 0
    if clear_existing:
        try:
            cur = conn.execute(
                """DELETE FROM live_signal_history
                   WHERE signal_key LIKE 'BT-SWING|%'
                     AND trade_date >= ? AND trade_date <= ?""",
                (str(from_date)[:10], str(to_date)[:10]),
            )
            cleared_existing = int(cur.rowcount or 0)
            conn.commit()
        except Exception as e:
            logger.warning("swing BT clear_existing failed: %s", e)

    context_from = fd - timedelta(days=120)
    ctx_from_s = context_from.isoformat()

    ohlcv_raw = conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv WHERE date >= ? AND date <= ? ORDER BY date",
        (ctx_from_s, str(to_date)[:10]),
    ).fetchall()
    if not ohlcv_raw:
        conn.close()
        return {"error": "No NIFTY OHLCV in backtest DB for range. Run /api/backtest/download first.", "inserted": 0}

    ohlcv = [
        {"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5] or 0}
        for r in ohlcv_raw
    ]
    date_index = {row["date"]: i for i, row in enumerate(ohlcv)}

    vix_map = {
        r[0]: (r[1], r[2])
        for r in conn.execute(
            "SELECT date, vix, vix_chg FROM vix_daily WHERE date >= ? AND date <= ?",
            (ctx_from_s, str(to_date)[:10]),
        ).fetchall()
    }
    chain_map = {
        r[0]: (r[1], r[2], r[3])
        for r in conn.execute(
            "SELECT date, pcr, total_call_oi, total_put_oi FROM chain_daily WHERE date >= ? AND date <= ?",
            (ctx_from_s, str(to_date)[:10]),
        ).fetchall()
    }
    fii_map = {
        r[0]: (r[1], r[2])
        for r in conn.execute(
            "SELECT date, fii_net, dii_net FROM fii_daily WHERE date >= ? AND date <= ?",
            (ctx_from_s, str(to_date)[:10]),
        ).fetchall()
    }
    conn.close()

    from config import FNO_SYMBOLS, KITE_TOKENS

    symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ("NIFTY", "BANKNIFTY", "INDIAVIX")]
    sym_series: dict[str, list] = {}
    to_s = str(to_date)[:10]
    for sym in symbols:
        tok = KITE_TOKENS[sym]
        try:
            hist = kite.historical_data(int(tok), context_from, datetime.strptime(to_s, "%Y-%m-%d").date(), "day")
        except Exception as e:
            logger.warning("swing BT: %s historical_data failed: %s", sym, e)
            continue
        rows = []
        for c in hist or []:
            ds = _dstr(c["date"])
            rows.append(
                {
                    "date": ds,
                    "open": float(c.get("open") or 0),
                    "high": float(c.get("high") or 0),
                    "low": float(c.get("low") or 0),
                    "close": float(c.get("close") or 0),
                    "volume": int(c.get("volume") or 0),
                }
            )
        rows.sort(key=lambda x: x["date"])
        if rows:
            sym_series[sym] = rows

    trading_days = [row["date"] for row in ohlcv if row["date"] >= str(from_date)[:10] and row["date"] <= to_s]
    inserted = 0
    skipped = 0
    errors: list[str] = []
    now_ts = time.time()

    for d in trading_days:
        idx = date_index.get(d)
        if idx is None or idx < 20:
            skipped += 1
            continue

        day = ohlcv[idx]
        prev = ohlcv[idx - 1]
        nifty_close = float(day["close"] or 0)
        prev_close = float(prev["close"] or 0)
        if nifty_close <= 0 or prev_close <= 0:
            skipped += 1
            continue

        chg_pct_nifty = (nifty_close - prev_close) / prev_close * 100
        prior_n = ohlcv[max(0, idx - 20) : idx]
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

        gates_snap = {
            1: {**g1, "name": "REGIME"},
            2: {**g2, "name": "SMART MONEY"},
            3: {**g3i, "name": "STRUCTURE"},
            4: {**g4i, "name": "TRIGGER"},
            5: {**g5i, "name": "RISK VALID"},
        }
        gate_json = _gate_snapshot_json(gates_snap)

        g1s = g1["state"]
        g2s = g2["state"]

        stocks: list = []
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
            prev_slice = series[max(0, si - 20) : si]
            _vols = [float(x["volume"] or 0) for x in prev_slice if (x.get("volume") or 0) > 0]
            avg_vs = statistics.mean(_vols) if _vols else 1.0
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
            g5 = (
                "go"
                if atr_pct > 0 and abs(dp) >= max(0.6, atr_pct * 0.35)
                else "am"
                if abs(dp) >= 0.35
                else "st"
            )
            stock_pc = len([x for x in (g1s, g2s, g3, g4, g5) if x == "go"])
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

        indices = {"vix": vix, "nifty_chg": round(chg_pct_nifty, 4)}
        chain = {"pcr": pcr}
        picks = swing_radar_candidates(stocks, indices, chain, verdict, pass_cnt)
        if not picks:
            continue

        conn = get_conn()
        try:
            for p in picks:
                sym = p["sym"]
                series = sym_series.get(sym) or []
                dsorted = [x["date"] for x in series]
                si = bisect.bisect_left(dsorted, d)
                if si >= len(series) or series[si]["date"] != d:
                    continue
                outcome, exit_px, exit_d, pnl_pct, hold_steps = _simulate_daily(
                    series, si, p["direction"], p["entry"], p["stop"], p["target"], max_forward_days
                )
                ext = "09:15"
                signal_key = f"BT-SWING|{d}|{sym}|{p['setup']}"
                trig = (
                    f"BACKTEST | Swing {p['setup']} | {p['sig_lbl']} | {p['chg']:+.2f}% | Vol×{p['vol_r']:.1f} | "
                    f"{p['pc']}/5 gates | score {p['score']}"
                )
                if p.get("rank_score", p["score"]) < p["score"]:
                    trig += f" | rank {p['rank_score']}"
                strength = "hi" if p["score"] >= 80 else "md" if p["score"] >= 65 else "lo"
                pnl_pts = (
                    (exit_px - p["entry"]) if p["direction"] == "LONG" else (p["entry"] - exit_px)
                )
                try:
                    cur = conn.execute(
                        """
                        INSERT OR IGNORE INTO live_signal_history
                        (signal_key, trade_date, symbol, signal_type, trigger, strength, signal_time,
                         entry_price, stop_loss, target_price, exit_price, exit_time, status, outcome,
                         pnl_pts, pnl_pct, hold_minutes, gate_pass_count, gate_snapshot, verdict, vix, pcr,
                         created_ts, updated_ts)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            signal_key,
                            d,
                            sym,
                            p["direction"],
                            trig,
                            strength,
                            "09:15",
                            round(p["entry"], 2),
                            round(p["stop"], 2),
                            round(p["target"], 2),
                            round(exit_px, 2),
                            f"{exit_d} {ext}",
                            "CLOSED",
                            outcome,
                            round(pnl_pts, 4),
                            float(pnl_pct),
                            int(max(1, hold_steps) * 24 * 60),
                            p["pc"],
                            gate_json,
                            "SWING_RADAR",
                            vix,
                            pcr,
                            now_ts,
                            now_ts,
                        ),
                    )
                    if cur.rowcount == 1:
                        inserted += 1
                except Exception as ex:
                    errors.append(f"{d} {sym}: {ex}")
            conn.commit()
        finally:
            conn.close()

    return {
        "ok": True,
        "inserted": inserted,
        "cleared_existing": cleared_existing,
        "skipped_days": skipped,
        "errors": errors[:50],
        "error_count": len(errors),
        "from_date": str(from_date)[:10],
        "to_date": to_s,
        "symbols_loaded": len(sym_series),
        "max_forward_days": max_forward_days,
        "report": build_swing_backtest_report(str(from_date)[:10], to_s),
    }
