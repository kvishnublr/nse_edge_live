"""
INTRA INDEX historical replay: one Kite minute fetch per session day, evaluate at 10:30 IST,
paper FIRE vs same-day session close (NIFTY index proxy — not options).

Breadth in backtest: NIFTY open→cutoff % change (not 15 stocks) — documented in report.
"""

from __future__ import annotations

import logging
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pytz

import intra_index_engine as ixe

logger = logging.getLogger("intra_index_backtest")

IST = pytz.timezone("Asia/Kolkata")
_DB = Path(__file__).resolve().parent / "data" / "backtest.db"


def _vix_for_date(conn: sqlite3.Connection, d: date) -> float:
    row = conn.execute("SELECT vix FROM vix_daily WHERE date = ?", (d.isoformat(),)).fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return 0.0


def _eod_close(bars: list[ixe._Bar]) -> Optional[float]:
    if not bars:
        return None
    return float(bars[-1].c)


def run_intra_index_backtest(
    kite: Any,
    days: int = 30,
    asof_cm: int = ixe.M_SIGNAL_END,
    fire_threshold: int = ixe.FIRE_CONFIDENCE_MIN,
) -> dict[str, Any]:
    """
    Walk backward `days` calendar days from today; skip weekends; skip if no minute data.

    Entry = NIFTY close of last 1m bar with cm <= asof_cm (default 10:30).
    EOD = last session 1m close same day.
    Forward % = LONG (eod-entry)/entry*100, SHORT (entry-eod)/entry*100.
    """
    t0 = time.time()
    days = max(5, min(int(days), 365))
    asof_cm = max(ixe.M_ORB_END, min(int(asof_cm), ixe.M_CLOSE))
    fire_threshold = max(40, min(int(fire_threshold), 95))

    if not kite:
        return {"ok": False, "error": "Kite not available", "ts": t0}

    to_d = datetime.now(IST).date()
    min_d = to_d - timedelta(days=max(days * 4, 120))

    conn: Optional[sqlite3.Connection] = None
    if _DB.is_file():
        try:
            conn = sqlite3.connect(str(_DB), timeout=15)
        except Exception:
            conn = None

    rows_out: list[dict[str, Any]] = []
    errors = 0
    d = to_d
    calendar_walked = 0

    while d >= min_d and len(rows_out) < days:
        calendar_walked += 1
        if d.weekday() >= 5:
            d -= timedelta(days=1)
            continue

        vix_p = _vix_for_date(conn, d) if conn else 0.0

        try:
            raw = list(kite.historical_data(ixe.NIFTY_TOKEN, d, d, "minute") or [])
        except Exception as e:
            logger.debug("intra_bt %s: %s", d, e)
            errors += 1
            d -= timedelta(days=1)
            continue

        bars = ixe._parse_minute_rows(raw)
        if len(bars) < 5:
            d -= timedelta(days=1)
            continue

        ev = [b for b in bars if ixe.M_OPEN <= b.cm <= asof_cm]
        if len(ev) < 2:
            d -= timedelta(days=1)
            continue

        day_open = ev[0].o if ev[0].o else ev[0].c
        sig_last = ev[-1]
        morning_pct = ((sig_last.c - day_open) / day_open * 100) if day_open else 0.0
        # Heuristic adv/dec for display only (confidence uses mean_chg)
        if morning_pct > 0.12:
            adv_n, dec_n = 10, 3
        elif morning_pct < -0.12:
            adv_n, dec_n = 3, 10
        else:
            adv_n, dec_n = 6, 6

        ev_inner = ixe.evaluate_intraday_slice(
            bars,
            asof_cm,
            d,
            vix_p,
            morning_pct,
            adv_n,
            dec_n,
            len(ixe.HEAVY_SYMBOLS),
            fire_threshold=fire_threshold,
            ist_time_label=f"{asof_cm // 60:02d}:{asof_cm % 60:02d}",
            use_all_bars_for_last=False,
        )

        eod = _eod_close(bars)
        entry = float(sig_last.c)
        fwd: Optional[float] = None
        win: Optional[bool] = None
        if eod and entry > 0:
            if ev_inner.get("orb_break") == "LONG":
                fwd = round((eod - entry) / entry * 100, 3)
                win = fwd > 0 if ev_inner.get("fire_signal") else None
            elif ev_inner.get("orb_break") == "SHORT":
                fwd = round((entry - eod) / entry * 100, 3)
                win = fwd > 0 if ev_inner.get("fire_signal") else None

        rows_out.append(
            {
                "date": d.isoformat(),
                "fire": bool(ev_inner.get("fire_signal")),
                "confidence": ev_inner.get("confidence"),
                "orb_break": ev_inner.get("orb_break"),
                "entry": round(entry, 2),
                "eod_close": round(eod, 2) if eod else None,
                "fwd_pct": fwd,
                "win": win,
                "vix": ev_inner.get("vix"),
                "breadth_proxy_pct": round(morning_pct, 4),
                "orb_high": ev_inner.get("orb_high"),
                "orb_low": ev_inner.get("orb_low"),
                "vwap": ev_inner.get("vwap"),
            }
        )
        d -= timedelta(days=1)

    if conn:
        try:
            conn.close()
        except Exception:
            pass

    fired = [r for r in rows_out if r.get("fire")]
    resolved = [r for r in fired if r.get("fwd_pct") is not None and r.get("win") is not None]
    wins = sum(1 for r in resolved if r.get("win"))
    wr = round(wins / len(resolved) * 100, 1) if resolved else None
    avg_fwd = (
        round(sum(float(r["fwd_pct"]) for r in resolved) / len(resolved), 3) if resolved else None
    )

    monthly: dict[str, dict[str, Any]] = {}
    for r in fired:
        fp = r.get("fwd_pct")
        if fp is None or r.get("win") is None:
            continue
        mo = r["date"][:7]
        b = monthly.setdefault(mo, {"n": 0, "w": 0, "sum_fwd": 0.0})
        b["n"] += 1
        if r.get("win"):
            b["w"] += 1
        b["sum_fwd"] += float(fp)
    monthly_list = []
    for mo in sorted(monthly.keys()):
        b = monthly[mo]
        nn = b["n"]
        monthly_list.append(
            {
                "month": mo,
                "fire_days": nn,
                "win_rate_pct": round(b["w"] / nn * 100, 1) if nn else None,
                "avg_fwd_pct": round(b["sum_fwd"] / nn, 3) if nn else None,
            }
        )

    return {
        "ok": True,
        "ts": t0,
        "calendar_days_scanned": calendar_walked,
        "session_days": len(rows_out),
        "asof_cm": asof_cm,
        "fire_threshold": fire_threshold,
        "fire_days": len(fired),
        "fire_resolved": len(resolved),
        "fire_wins": wins,
        "fire_win_rate_pct": wr,
        "avg_fwd_pct_on_fire": avg_fwd,
        "criteria": (
            f"Paper FIRE at {asof_cm // 60:02d}:{asof_cm % 60:02d} IST using same rules as live "
            f"(ORB+VWAP+VIX+confidence≥{fire_threshold}). "
            "Breadth term uses NIFTY % from session open→cutoff (not 15 cash names). "
            "Forward = index move to same-day last 1m close — not option premium."
        ),
        "monthly": monthly_list,
        "daily": rows_out[: min(120, len(rows_out))],
        "note": "One Kite call per day; long ranges take time. Weekends/holidays skipped automatically.",
    }
