"""
ADV-SWING: replay strict swing-style setups on NIFTY 500 cash (Kite NSE EQ daily bars).
Uses swing_radar_candidates with ADV_SWING_RADAR overlay; rows tagged BT-ADVS|… / verdict ADV_SWING.
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
from swing_radar_backtest import _dstr, _simulate_daily, summarize_swing_backtest_rows

logger = logging.getLogger("adv_swing_backtest")
IST = pytz.timezone("Asia/Kolkata")


def build_adv_swing_backtest_report(from_date: str | None = None, to_date: str | None = None) -> dict:
    init_db()
    conn = get_conn()
    conn.row_factory = None
    where = ["signal_key LIKE 'BT-ADVS|%'"]
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
    report.update(
        {
            "from_date": str(from_date)[:10] if from_date else (data[0]["trade_date"] if data else None),
            "to_date": str(to_date)[:10] if to_date else (data[-1]["trade_date"] if data else None),
            "universe": "NIFTY_500",
            "verdict": "ADV_SWING",
        }
    )
    return report


def run_adv_swing_radar_backtest(
    kite,
    from_date: str,
    to_date: str,
    max_forward_days: int | None = None,
    max_calendar_span_days: int | None = None,
    clear_existing: bool = False,
) -> dict:
    from config import (
        ADV_SWING_CANDIDATE_LIMIT,
        ADV_SWING_MAX_FORWARD_DAYS,
        ADV_SWING_MAX_RANGE_DAYS,
        ADV_SWING_KITE_SLEEP_SEC,
        ADV_SWING_RADAR,
    )
    import fetcher

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

    span_cap = int(max_calendar_span_days or ADV_SWING_MAX_RANGE_DAYS)
    span_cap = max(30, min(span_cap, ADV_SWING_MAX_RANGE_DAYS))
    if (td - fd).days > span_cap:
        conn.close()
        return {"error": f"Range exceeds {span_cap} days cap for ADV-SWING", "inserted": 0}

    mfd = int(max_forward_days or ADV_SWING_MAX_FORWARD_DAYS)
    mfd = max(5, min(mfd, 35))
    sleep_s = float(ADV_SWING_KITE_SLEEP_SEC or 0.1)

    cleared_existing = 0
    if clear_existing:
        try:
            cur = conn.execute(
                """DELETE FROM live_signal_history
                   WHERE signal_key LIKE 'BT-ADVS|%'
                     AND trade_date >= ? AND trade_date <= ?""",
                (str(from_date)[:10], str(to_date)[:10]),
            )
            cleared_existing = int(cur.rowcount or 0)
            conn.commit()
        except Exception as e:
            logger.warning("adv swing BT clear_existing failed: %s", e)

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

    tok_map = fetcher.get_nifty500_kite_tokens(kite)
    n500_syms = fetcher.get_nifty500_symbols()
    if not tok_map:
        return {
            "error": "No Kite NSE EQ tokens mapped for NIFTY 500 — verify Kite login and instruments download.",
            "inserted": 0,
            "nifty500_listed": len(n500_syms),
        }
    sym_series: dict[str, list] = {}
    to_s = str(to_date)[:10]
    td_dt = datetime.strptime(to_s, "%Y-%m-%d").date()
    for idx, (sym, tok) in enumerate(sorted(tok_map.items(), key=lambda x: x[0])):
        try:
            hist = kite.historical_data(int(tok), context_from, td_dt, "day")
        except Exception as e:
            logger.warning("adv swing BT: %s historical_data failed: %s", sym, e)
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
        if sleep_s > 0 and (idx + 1) % 8 == 0:
            time.sleep(sleep_s)

    if not sym_series:
        return {
            "error": "No daily history loaded for NIFTY 500 symbols in this window — widen dates or check Kite limits.",
            "inserted": 0,
            "nifty500_listed": len(n500_syms),
            "tokens_tried": len(tok_map),
        }

    cand_limit = int(ADV_SWING_CANDIDATE_LIMIT or 10)
    swr_ov = dict(ADV_SWING_RADAR)

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
        picks = swing_radar_candidates(
            stocks,
            indices,
            chain,
            verdict,
            pass_cnt,
            min_score=60,
            limit=cand_limit,
            swr_override=swr_ov,
        )
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
                    series, si, p["direction"], p["entry"], p["stop"], p["target"], mfd
                )
                ext = "09:15"
                signal_key = f"BT-ADVS|{d}|{sym}|{p['setup']}"
                trig = (
                    f"BACKTEST | Swing {p['setup']} | {p['sig_lbl']} | {p['chg']:+.2f}% | Vol×{p['vol_r']:.1f} | "
                    f"{p['pc']}/5 gates | score {p['score']} | ADV-SWING N500"
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
                            "ADV_SWING",
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

    rep = build_adv_swing_backtest_report(str(from_date)[:10], to_s)
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
        "nifty500_listed": len(n500_syms),
        "max_forward_days": mfd,
        "candidate_limit": cand_limit,
        "report": rep,
    }
