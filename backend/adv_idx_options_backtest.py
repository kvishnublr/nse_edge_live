"""
Historical replay of ADV-IDX-OPTIONS *context score* (IV proxy + weekday expiry flags).

This is not live option-chain replay (needs Kite per day). It answers:
  "How many session-days would count as paper EXECUTE under the same score rule?"

Paper EXECUTE = options_context_score >= min_score (default 60), with live gate verdict
omitted unless verdict_mode='signal_log' (last verdict that day in signal_log).
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pytz

from adv_idx_options import IST, _confidence_blend, _iv_rank_proxy, expiry_intelligence

logger = logging.getLogger("adv_idx_options_backtest")

_DB = Path(__file__).resolve().parent / "data" / "backtest.db"


def _last_verdict_by_date(conn: sqlite3.Connection) -> dict[str, str]:
    """Map trade_date -> latest non-backtest verdict string."""
    rows = conn.execute(
        "SELECT date, verdict, ts FROM signal_log WHERE session != 'backtest' ORDER BY ts ASC"
    ).fetchall()
    out: dict[str, str] = {}
    for d, v, _ts in rows:
        if d and v:
            out[str(d)[:10]] = str(v)
    return out


def run_adv_idx_options_backtest(
    days: int = 180,
    min_score: float = 60.0,
    verdict_mode: str = "none",
) -> dict[str, Any]:
    """
    Prefer `adv_idx_options_daily` (Kite download) for replay; else ohlcv ∩ vix_daily.

    trades_executed = count of days with options_context_score >= min_score.
    With verdict_mode='none', score uses only IV zone + gamma weekday (same as live when gates unknown).
    """
    from adv_idx_options_storage import build_adv_idx_options_report

    from config import IV_RANK_VIX_HIGH, IV_RANK_VIX_LOW

    days = max(7, min(int(days), 3650))
    min_score = float(min_score)
    verdict_mode = (verdict_mode or "none").strip().lower()
    if verdict_mode not in ("none", "signal_log"):
        verdict_mode = "none"

    t0 = time.time()
    if not _DB.is_file():
        return {
            "error": f"backtest.db missing at {_DB}",
            "trades_executed": 0,
            "ts": t0,
        }

    rep = build_adv_idx_options_report(
        days=days, min_score=min_score, verdict_mode=verdict_mode
    )
    if not rep.get("ok"):
        # Auto-populate from local ohlcv+vix_daily if dedicated table is empty
        try:
            from adv_idx_options_storage import populate_from_local_db
            sync = populate_from_local_db(days=3650)
            if sync.get("ok"):
                logger.info("Auto-populated adv_idx_options_daily from local DB: %d rows", sync["rows_merged"])
                rep = build_adv_idx_options_report(
                    days=days, min_score=min_score, verdict_mode=verdict_mode
                )
        except Exception as _e:
            logger.warning("Auto-populate from local DB failed: %s", _e)
    if rep.get("ok"):
        s = rep["summary"]
        return {
            "ok": True,
            "ts": rep["ts"],
            "criteria": rep["criteria"],
            "data_source": "adv_idx_options_daily",
            "min_score": rep["min_score"],
            "verdict_mode": rep["verdict_mode"],
            "calendar_days_requested": rep["calendar_days_requested"],
            "days_in_sample": s["days_in_sample"],
            "trades_executed": s["trades_executed"],
            "gamma_elevated_days": s["gamma_elevated_days"],
            "iv_zone_counts": s["iv_zone_counts"],
            "from": rep["from"],
            "to": rep["to"],
            "daily_tail": rep["daily_tail"],
            "full_report": rep,
        }

    to_d = datetime.now(IST).date()
    from_d = to_d - timedelta(days=days)

    conn = sqlite3.connect(str(_DB), timeout=15)
    try:
        verdict_map: dict[str, str] = {}
        if verdict_mode == "signal_log":
            try:
                verdict_map = _last_verdict_by_date(conn)
            except Exception as e:
                logger.debug("verdict_map: %s", e)

        rows = conn.execute(
            """
            SELECT o.date, v.vix
            FROM ohlcv o
            INNER JOIN vix_daily v ON v.date = o.date
            WHERE o.date >= ? AND o.date <= ?
            ORDER BY o.date
            """,
            (from_d.isoformat(), to_d.isoformat()),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return {
            "error": (
                "No data: either POST /api/adv-idx-options/download (writes adv_idx_options_daily) "
                "or run global ⬇ DOWNLOAD DATA for ohlcv + vix_daily."
            ),
            "trades_executed": 0,
            "days_in_sample": 0,
            "from": from_d.isoformat(),
            "to": to_d.isoformat(),
            "ts": t0,
            "hint": rep.get("error"),
        }

    daily: list[dict[str, Any]] = []
    executed = 0
    zone_counts: dict[str, int] = {}
    gamma_days = 0

    for date_str, vix_raw in rows:
        vix = float(vix_raw or 0)
        try:
            y, m, d = [int(x) for x in str(date_str)[:10].split("-")]
            dt = IST.localize(datetime(y, m, d, 12, 0, 0))
        except Exception:
            continue

        exp = expiry_intelligence(dt)
        if exp.get("gamma_elevated_day"):
            gamma_days += 1

        iv = _iv_rank_proxy(vix, IV_RANK_VIX_LOW, IV_RANK_VIX_HIGH)
        z = str(iv.get("zone") or "unknown")
        zone_counts[z] = zone_counts.get(z, 0) + 1

        vd: Optional[str] = None
        if verdict_mode == "signal_log":
            vd = verdict_map.get(str(date_str)[:10])
            if vd not in ("EXECUTE", "NO TRADE", "WAIT"):
                vd = None

        conf = _confidence_blend(z, bool(exp.get("gamma_elevated_day")), vd)
        score = float(conf.get("options_context_score") or 0)
        is_ex = score >= min_score
        if is_ex:
            executed += 1

        daily.append(
            {
                "date": str(date_str)[:10],
                "vix": round(vix, 2),
                "iv_zone": z,
                "iv_rank_proxy": iv.get("iv_rank_proxy"),
                "gamma_elevated": bool(exp.get("gamma_elevated_day")),
                "options_context_score": round(score, 1),
                "executed": is_ex,
                "verdict_applied": vd,
            }
        )

    if not daily:
        return {
            "error": "Joined ohlcv+vix_daily had rows but none parsed; check date formats in DB.",
            "trades_executed": 0,
            "days_in_sample": 0,
            "from": from_d.isoformat(),
            "to": to_d.isoformat(),
            "ts": t0,
        }

    return {
        "ok": True,
        "ts": t0,
        "data_source": "ohlcv_vix_join",
        "criteria": (
            f"Paper trade day = options_context_score >= {min_score} "
            f"(IV proxy vs IV_RANK_VIX_LOW/HIGH + weekday expiry gamma; "
            f"verdict_mode={verdict_mode}). Not broker executions."
        ),
        "min_score": min_score,
        "verdict_mode": verdict_mode,
        "calendar_days_requested": days,
        "days_in_sample": len(daily),
        "trades_executed": executed,
        "gamma_elevated_days": gamma_days,
        "iv_zone_counts": zone_counts,
        "from": str(rows[0][0])[:10],
        "to": str(rows[-1][0])[:10],
        "daily_tail": daily[-40:] if len(daily) > 40 else daily,
    }
