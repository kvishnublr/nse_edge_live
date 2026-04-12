"""
ADV-IDX-OPTIONS — dedicated SQLite table `adv_idx_options_daily` (separate from ohlcv/vix_daily).

Download NIFTY + India VIX daily from Kite, compute IV proxy + weekday gamma context,
and build a structured backtest report (paper executes + next-session NIFTY drift).
"""

from __future__ import annotations

import logging
import sqlite3
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional

import pytz

from adv_idx_options import IST, _confidence_blend, _iv_rank_proxy, expiry_intelligence

logger = logging.getLogger("adv_idx_options_storage")

IST_TZ = IST


def _iso_date(val: Any) -> str:
    """Normalize Kite historical `date` (date or datetime) to YYYY-MM-DD for joining NIFTY ↔ VIX."""
    from datetime import date as date_cls
    from datetime import datetime as dt_cls

    if val is None:
        return ""
    if isinstance(val, dt_cls):
        return val.date().isoformat()
    if isinstance(val, date_cls):
        return val.isoformat()
    s = str(val).strip()
    return s[:10] if len(s) >= 10 else s


def _db_path() -> str:
    from backtest_data import DB_PATH

    return str(DB_PATH)


def _last_verdict_by_date(conn: sqlite3.Connection) -> dict[str, str]:
    rows = conn.execute(
        "SELECT date, verdict, ts FROM signal_log WHERE session != 'backtest' ORDER BY ts ASC"
    ).fetchall()
    out: dict[str, str] = {}
    for d, v, _ts in rows:
        if d and v:
            out[str(d)[:10]] = str(v)
    return out


def _row_score(
    iv_zone: str,
    gamma: bool,
    verdict: Optional[str],
) -> float:
    conf = _confidence_blend(iv_zone, gamma, verdict)
    return float(conf.get("options_context_score") or 0)


def download_adv_idx_options_from_kite(kite: Any, days: int = 730) -> dict[str, Any]:
    """
    Pull NIFTY + INDIAVIX daily into `adv_idx_options_daily` (INSERT OR REPLACE), then recompute context columns.
    """
    import backtest_data as bd
    from config import KITE_TOKENS

    days = max(30, min(int(days), 3650))
    bd.init_db()

    to_dt = datetime.now(IST_TZ).date()
    from_dt = to_dt - timedelta(days=days)

    nifty_data: list = []
    vix_raw: list = []
    nifty_err: Optional[str] = None
    vix_err: Optional[str] = None
    try:
        nifty_data = list(kite.historical_data(KITE_TOKENS["NIFTY"], from_dt, to_dt, "day") or [])
    except Exception as e:
        nifty_err = str(e)
        logger.warning("adv_idx_options NIFTY historical: %s", e)
    try:
        vix_raw = list(kite.historical_data(KITE_TOKENS["INDIAVIX"], from_dt, to_dt, "day") or [])
    except Exception as e:
        vix_err = str(e)
        logger.warning("adv_idx_options INDIAVIX historical: %s", e)

    vix_sorted = sorted(vix_raw, key=lambda x: x["date"])
    vix_map: dict[str, tuple[float, float]] = {}
    prev = None
    for d in vix_sorted:
        v = float(d["close"] or 0)
        dt_str = _iso_date(d.get("date"))
        if not dt_str:
            continue
        chg = round((v - prev) / prev * 100, 2) if prev and prev > 0 else 0.0
        vix_map[dt_str] = (v, chg)
        prev = v

    now_ts = time.time()
    conn = bd.get_conn()
    merged = 0
    try:
        for d in nifty_data:
            dt_str = _iso_date(d.get("date"))
            if not dt_str or dt_str not in vix_map:
                continue
            vx, vch = vix_map[dt_str]
            conn.execute(
                """
                INSERT OR REPLACE INTO adv_idx_options_daily
                (date, nifty_open, nifty_high, nifty_low, nifty_close, nifty_volume,
                 vix, vix_chg, source, updated_ts)
                VALUES (?,?,?,?,?,?,?,?, 'kite', ?)
                """,
                (
                    dt_str,
                    float(d.get("open") or 0),
                    float(d.get("high") or 0),
                    float(d.get("low") or 0),
                    float(d.get("close") or 0),
                    int(d.get("volume") or 0),
                    vx,
                    vch,
                    now_ts,
                ),
            )
            merged += 1
        conn.commit()
    finally:
        conn.close()

    recompute_adv_idx_options_daily()

    hint = None
    if merged == 0:
        hint = (
            "No overlapping NIFTY+VIX daily bars in range. "
            "Check Kite token (login daily), INDIAVIX subscription, and config KITE_TOKENS. "
            "Historical data does not require market hours."
        )
        if nifty_err:
            hint += f" NIFTY API: {nifty_err}"
        if vix_err:
            hint += f" VIX API: {vix_err}"
        elif not vix_raw:
            hint += " VIX series empty — token or instrument 264969 (INDIAVIX)."
        elif not nifty_data:
            hint += " NIFTY series empty — token or instrument 256265."

    return {
        "ok": merged > 0,
        "rows_merged": merged,
        "nifty_bars": len(nifty_data),
        "vix_bars": len(vix_raw),
        "nifty_error": nifty_err,
        "vix_error": vix_err,
        "hint": hint,
        "from": from_dt.isoformat(),
        "to": to_dt.isoformat(),
        "table": "adv_idx_options_daily",
        "note": "Kite historical_data works when logged in; no need to wait for LIVE market.",
    }


def recompute_adv_idx_options_daily() -> int:
    """Fill iv_* / gamma / weekday / options_context_score (verdict=None) for all rows."""
    import backtest_data as bd
    from config import IV_RANK_VIX_HIGH, IV_RANK_VIX_LOW

    bd.init_db()
    conn = bd.get_conn()
    n = 0
    try:
        rows = conn.execute(
            "SELECT date, vix FROM adv_idx_options_daily ORDER BY date"
        ).fetchall()
        now_ts = time.time()
        for date_str, vix_raw in rows:
            vix = float(vix_raw or 0)
            try:
                y, m, d = [int(x) for x in str(date_str)[:10].split("-")]
                dt = IST_TZ.localize(datetime(y, m, d, 12, 0, 0))
            except Exception:
                continue
            exp = expiry_intelligence(dt)
            iv = _iv_rank_proxy(vix, IV_RANK_VIX_LOW, IV_RANK_VIX_HIGH)
            z = str(iv.get("zone") or "unknown")
            gamma = 1 if exp.get("gamma_elevated_day") else 0
            conf = _confidence_blend(z, bool(gamma), None)
            sc = float(conf.get("options_context_score") or 0)
            conn.execute(
                """
                UPDATE adv_idx_options_daily SET
                    iv_rank_proxy=?, iv_zone=?, gamma_elevated=?, weekday=?,
                    options_context_score=?, updated_ts=?
                WHERE date=?
                """,
                (
                    iv.get("iv_rank_proxy"),
                    z,
                    gamma,
                    int(exp.get("weekday") or 0),
                    sc,
                    now_ts,
                    str(date_str)[:10],
                ),
            )
            n += 1
        conn.commit()
    finally:
        conn.close()
    return n


def build_adv_idx_options_report(
    days: int = 180,
    min_score: float = 60.0,
    verdict_mode: str = "none",
) -> dict[str, Any]:
    """
    Full report from `adv_idx_options_daily`. Includes next-session NIFTY % change after paper-execute days.
    """
    import backtest_data as bd

    days = max(7, min(int(days), 3650))
    min_score = float(min_score)
    verdict_mode = (verdict_mode or "none").strip().lower()
    if verdict_mode not in ("none", "signal_log"):
        verdict_mode = "none"

    t0 = time.time()
    bd.init_db()
    to_d = datetime.now(IST_TZ).date()
    from_d = to_d - timedelta(days=days)

    conn = bd.get_conn()
    try:
        verdict_map: dict[str, str] = {}
        if verdict_mode == "signal_log":
            try:
                verdict_map = _last_verdict_by_date(conn)
            except Exception as e:
                logger.debug("verdict_map: %s", e)

        rows = conn.execute(
            """
            SELECT date, nifty_open, nifty_high, nifty_low, nifty_close, nifty_volume,
                   vix, vix_chg, iv_rank_proxy, iv_zone, gamma_elevated, weekday,
                   options_context_score
            FROM adv_idx_options_daily
            WHERE date >= ? AND date <= ?
            ORDER BY date
            """,
            (from_d.isoformat(), to_d.isoformat()),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {
            "ok": False,
            "error": "No rows in adv_idx_options_daily for this range. POST /api/adv-idx-options/download first.",
            "trades_executed": 0,
            "summary": {},
            "ts": t0,
        }

    zone_counts: dict[str, int] = defaultdict(int)
    gamma_days = 0
    executed = 0
    daily_out: list[dict[str, Any]] = []
    monthly: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"days": 0, "executed": 0, "next_up": 0, "next_n": 0}
    )

    # First pass: scores + executed
    parsed: list[dict[str, Any]] = []
    for r in rows:
        ds = str(r[0])[:10]
        iv_zone = str(r[9] or "unknown")
        gamma = bool(r[10])
        vd: Optional[str] = None
        if verdict_mode == "signal_log":
            vd = verdict_map.get(ds)
            if vd not in ("EXECUTE", "NO TRADE", "WAIT"):
                vd = None
        score = _row_score(iv_zone, gamma, vd)
        ex = score >= min_score
        if ex:
            executed += 1
        if gamma:
            gamma_days += 1
        zone_counts[iv_zone] += 1

        mo = ds[:7]
        monthly[mo]["days"] += 1
        if ex:
            monthly[mo]["executed"] += 1

        parsed.append(
            {
                "date": ds,
                "nifty_close": float(r[4] or 0) if r[4] is not None else None,
                "vix": round(float(r[6] or 0), 2),
                "iv_zone": iv_zone,
                "iv_rank_proxy": r[8],
                "gamma_elevated": gamma,
                "options_context_score": round(score, 1),
                "executed": ex,
                "verdict_applied": vd,
            }
        )

    # Next-session stats (contiguous rows = trading sessions in table)
    next_returns: list[float] = []
    for j, p in enumerate(parsed):
        if not p["executed"]:
            continue
        if j + 1 >= len(parsed):
            break
        cur = p.get("nifty_close")
        nxt = parsed[j + 1].get("nifty_close")
        if cur and nxt and cur > 0:
            pct = round((nxt - cur) / cur * 100, 3)
            next_returns.append(pct)
            mo = p["date"][:7]
            monthly[mo]["next_n"] += 1
            if pct > 0:
                monthly[mo]["next_up"] += 1

    n_next = len(next_returns)
    next_wr = round(sum(1 for x in next_returns if x > 0) / n_next * 100, 1) if n_next else None
    next_avg = round(sum(next_returns) / n_next, 3) if n_next else None

    # Monthly next-day WR
    monthly_list = []
    for mo in sorted(monthly.keys()):
        b = monthly[mo]
        nn = b["next_n"]
        wr_m = round(b["next_up"] / nn * 100, 1) if nn else None
        monthly_list.append(
            {
                "month": mo,
                "days": b["days"],
                "executed": b["executed"],
                "execute_rate_pct": round(b["executed"] / b["days"] * 100, 1) if b["days"] else 0,
                "next_session_samples": nn,
                "next_session_nifty_up_rate_pct": wr_m,
            }
        )

    executed_recent = [p for p in parsed if p["executed"]][-30:]

    total_days = len(parsed)
    return {
        "ok": True,
        "ts": t0,
        "source": "adv_idx_options_daily",
        "criteria": (
            f"Paper execute = options_context_score >= {min_score} after IV+gamma"
            + (f" + signal_log verdict ({verdict_mode})." if verdict_mode == "signal_log" else " (no gate verdict merge).")
            + " Next-session stat = NIFTY % from this row's close to the following row's close in this table (Kite daily)."
        ),
        "min_score": min_score,
        "verdict_mode": verdict_mode,
        "calendar_days_requested": days,
        "summary": {
            "days_in_sample": total_days,
            "trades_executed": executed,
            "execute_rate_pct": round(executed / total_days * 100, 1) if total_days else 0,
            "gamma_elevated_days": gamma_days,
            "iv_zone_counts": dict(zone_counts),
            "next_session_after_execute": {
                "sample_n": n_next,
                "nifty_close_to_next_close_up_rate_pct": next_wr,
                "avg_nifty_next_session_return_pct": next_avg,
            },
        },
        "monthly": monthly_list,
        "from": parsed[0]["date"],
        "to": parsed[-1]["date"],
        "executed_days_recent": executed_recent,
        "daily_tail": parsed[-45:] if len(parsed) > 45 else parsed,
    }


def populate_from_local_db(days: int = 3650) -> dict[str, Any]:
    """
    Populate `adv_idx_options_daily` from existing local `ohlcv + vix_daily` tables.
    Use this when Kite historical download isn't available.
    """
    import backtest_data as bd
    from datetime import timedelta

    bd.init_db()
    to_d = datetime.now(IST_TZ).date()
    from_d = to_d - timedelta(days=int(days))

    conn = bd.get_conn()
    merged = 0
    now_ts = time.time()
    try:
        rows = conn.execute(
            """
            SELECT o.date, o.open, o.high, o.low, o.close, o.volume, v.vix, v.vix_chg
            FROM ohlcv o
            INNER JOIN vix_daily v ON v.date = o.date
            WHERE o.date >= ? AND o.date <= ?
            ORDER BY o.date
            """,
            (from_d.isoformat(), to_d.isoformat()),
        ).fetchall()
        for r in rows:
            dt_str, nopen, nhigh, nlow, nclose, nvol, vix, vch = r
            dt_str = str(dt_str)[:10]
            conn.execute(
                """
                INSERT OR REPLACE INTO adv_idx_options_daily
                (date, nifty_open, nifty_high, nifty_low, nifty_close, nifty_volume,
                 vix, vix_chg, source, updated_ts)
                VALUES (?,?,?,?,?,?,?,?, 'local_db', ?)
                """,
                (dt_str, float(nopen or 0), float(nhigh or 0), float(nlow or 0),
                 float(nclose or 0), int(nvol or 0), float(vix or 0), float(vch or 0), now_ts),
            )
            merged += 1
        conn.commit()
    finally:
        conn.close()

    if merged > 0:
        recompute_adv_idx_options_daily()

    return {
        "ok": merged > 0,
        "rows_merged": merged,
        "source": "local_db (ohlcv + vix_daily)",
        "from": from_d.isoformat(),
        "to": to_d.isoformat(),
    }


def count_adv_idx_options_rows() -> int:
    import backtest_data as bd

    bd.init_db()
    conn = bd.get_conn()
    try:
        r = conn.execute("SELECT COUNT(*) FROM adv_idx_options_daily").fetchone()
        return int(r[0] or 0)
    finally:
        conn.close()
