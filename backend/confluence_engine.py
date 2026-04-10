"""
NSE EDGE — Intraday confluence model (experimental, removable).

Combines only data we already have: gates, PCR/OI totals, VIX, spot vs max pain,
coarse ΔOI between chain snapshots, and F&O stock breadth. No order-flow / DOM.

Not predictive of guaranteed moves; use for context and journaling only.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("confluence_engine")

DB_PATH = Path(__file__).parent / "data" / "backtest.db"

# Throttle DB writes: at most one row per _PERSIST_MIN_SEC unless grade or band changes
_PERSIST_MIN_SEC = 90.0
_last_persist_ts: float = 0.0
_last_persist_sig: str = ""


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def ensure_table() -> None:
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS confluence_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT,
                ts REAL,
                grade TEXT,
                score REAL,
                bias TEXT,
                verdict TEXT,
                pass_count INTEGER,
                vix REAL,
                pcr REAL,
                nifty REAL,
                nifty_chg REAL,
                components_json TEXT,
                reasons_json TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_conf_hist_date_ts ON confluence_history(trade_date, ts DESC)"
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("confluence ensure_table: %s", e)


def compute_confluence(
    gates: dict | None,
    verdict: str,
    pass_count: int,
    chain: dict | None,
    macro: dict | None,
    stocks: list | None,
    prev_chain_totals: dict | None,
) -> dict[str, Any]:
    """
    Returns a JSON-serializable snapshot for UI + WS.
    prev_chain_totals: optional {"total_call_oi": int, "total_put_oi": int, "pcr": float}
    """
    gates = gates or {}
    chain = chain or {}
    macro = macro or {}
    stocks = stocks or []

    pc = max(0, min(5, int(pass_count or 0)))
    vdx = str(verdict or "WAIT").upper()
    pcr = _f(chain.get("pcr"), 1.0)
    vix = _f(macro.get("vix"), 0)
    nifty = _f(macro.get("nifty"), 0)
    nifty_chg = _f(macro.get("nifty_chg"), 0)
    mp = _f(chain.get("max_pain"), 0)
    tc = int(chain.get("total_call_oi") or 0)
    tp = int(chain.get("total_put_oi") or 0)

    reasons: list[str] = []
    parts: dict[str, dict[str, float]] = {}

    # 1) Gates (0–30)
    g_pts = pc * 6.0
    parts["gates"] = {"pts": round(g_pts, 1), "max": 30, "pass": float(pc)}
    if pc >= 5:
        reasons.append("All 5 gates green — engine allows risk-on sizing.")
    elif pc >= 3:
        reasons.append(f"{pc}/5 gates pass — moderate alignment.")
    else:
        reasons.append("Gates weak — most confluence reads are low reliability.")

    # 2) PCR skew (0–18)
    dev = abs(pcr - 1.0)
    pcr_pts = _clamp((dev / 0.35) * 6.0, 0, 18)
    parts["pcr_skew"] = {"pts": round(pcr_pts, 1), "max": 18, "pcr": pcr}
    if pcr >= 1.12:
        reasons.append(f"PCR {pcr:.2f} — put-heavy OI (bullish skew for index).")
    elif pcr <= 0.88:
        reasons.append(f"PCR {pcr:.2f} — call-heavy OI (bearish skew).")
    else:
        reasons.append(f"PCR {pcr:.2f} — near balanced.")

    # 3) VIX regime (0–15)
    if vix <= 0:
        vix_pts = 7.0
        reasons.append("VIX missing — volatility score neutral.")
    elif vix < 14:
        vix_pts = 15.0
        reasons.append(f"VIX {vix:.1f} — complacency / range-friendly.")
    elif vix <= 18:
        vix_pts = 11.0
        reasons.append(f"VIX {vix:.1f} — normal intraday regime.")
    elif vix <= 22:
        vix_pts = 6.0
        reasons.append(f"VIX {vix:.1f} — elevated; cut size.")
    else:
        vix_pts = 2.0
        reasons.append(f"VIX {vix:.1f} — high fear; avoid fresh option buys.")
    parts["vix"] = {"pts": round(vix_pts, 1), "max": 15, "vix": vix}

    # 4) Spot vs max pain (0–12)
    mp_pts = 0.0
    if nifty > 0 and mp > 0:
        dist_pct = abs(nifty - mp) / nifty * 100.0
        mp_pts = _clamp(12.0 - dist_pct * 1.2, 0, 12)
        if dist_pct < 0.35:
            reasons.append(f"Spot near Max Pain {mp:,.0f} — pinning risk.")
        elif nifty > mp:
            reasons.append(f"Spot above Max Pain {mp:,.0f} — call writers under pressure.")
        else:
            reasons.append(f"Spot below Max Pain {mp:,.0f} — put writers under pressure.")
    else:
        reasons.append("Max pain / spot incomplete.")
    parts["max_pain"] = {"pts": round(mp_pts, 1), "max": 12}

    # 5) ΔOI aggregate vs previous snapshot (0–12)
    d_oi_pts = 5.0
    if prev_chain_totals and tc > 0 and tp > 0:
        ptc = int(prev_chain_totals.get("total_call_oi") or 0)
        ptp = int(prev_chain_totals.get("total_put_oi") or 0)
        if ptc > 0 and ptp > 0:
            dc = (tc - ptc) / ptc * 100.0
            dp = (tp - ptp) / ptp * 100.0
            net = dc - dp
            d_oi_pts = _clamp(6.0 + min(abs(net) * 0.25, 6.0), 0, 12)
            if net > 2:
                reasons.append(f"Call OI building faster than puts ({dc:+.1f}% vs {dp:+.1f}%).")
            elif net < -2:
                reasons.append(f"Put OI building faster than calls ({dp:+.1f}% vs {dc:+.1f}%).")
            else:
                reasons.append("Call/Put OI changes balanced vs last snapshot.")
    parts["oi_delta"] = {"pts": round(d_oi_pts, 1), "max": 12}

    # 6) Stock breadth (0–13)
    ex_n = sum(1 for s in stocks if str(s.get("verdict", "")).upper() == "EXECUTE")
    hi_n = sum(
        1
        for s in stocks
        if str(s.get("symbol", "")) not in ("NIFTY", "BANKNIFTY", "INDIAVIX")
        and float(s.get("score") or 0) >= 78
    )
    br_pts = _clamp(ex_n * 3.5 + min(hi_n, 4) * 1.5, 0, 13)
    parts["breadth"] = {"pts": round(br_pts, 1), "max": 13, "execute_n": float(ex_n), "hi_score_n": float(hi_n)}
    if ex_n:
        reasons.append(f"{ex_n} F&O name(s) on EXECUTE — directional participation.")
    elif hi_n:
        reasons.append(f"{hi_n} high-score watchlist names — wait for gate confirmation.")

    raw = g_pts + pcr_pts + vix_pts + mp_pts + d_oi_pts + br_pts
    if vdx == "NO TRADE":
        raw *= 0.55
        reasons.append("Verdict NO TRADE — confluence capped (stand down).")

    score = int(round(_clamp(raw, 0, 100)))

    if score >= 72:
        grade = "A"
    elif score >= 52:
        grade = "B"
    else:
        grade = "C"

    # Directional bias (heuristic, not a trade signal)
    bias = "NEUTRAL"
    if vdx == "NO TRADE":
        bias = "CAUTIOUS"
    elif pcr >= 1.08 and nifty_chg >= 0.15:
        bias = "LONG_LEAN"
    elif pcr >= 1.08:
        bias = "BULLISH_OI"
    elif pcr <= 0.92 and nifty_chg <= -0.15:
        bias = "SHORT_LEAN"
    elif pcr <= 0.92:
        bias = "BEARISH_OI"

    return {
        "score": score,
        "grade": grade,
        "bias": bias,
        "verdict": vdx,
        "pass_count": pc,
        "pcr": round(pcr, 4),
        "vix": round(vix, 2),
        "nifty": round(nifty, 2),
        "nifty_chg": round(nifty_chg, 3),
        "max_pain": int(mp) if mp else 0,
        "components": parts,
        "reasons": reasons[:12],
        "disclaimer": "Confluence blends gates, OI, VIX, max pain & breadth only — no order-flow. "
        "Not investment advice; for research and journaling.",
        "ts": time.time(),
    }


def chain_totals_snapshot(chain: dict | None) -> dict[str, Any]:
    c = chain or {}
    return {
        "total_call_oi": int(c.get("total_call_oi") or 0),
        "total_put_oi": int(c.get("total_put_oi") or 0),
        "pcr": _f(c.get("pcr"), 1.0),
        "ts": time.time(),
    }


def maybe_persist_snapshot(snap: dict[str, Any], trade_date: str) -> None:
    """Write one row if throttle allows or snapshot materially changed."""
    global _last_persist_ts, _last_persist_sig
    ensure_table()
    score = int(snap.get("score") or 0)
    grade = str(snap.get("grade") or "C")
    sig = f"{grade}|{score // 8}"  # banded
    now = time.time()
    if (now - _last_persist_ts) < _PERSIST_MIN_SEC and sig == _last_persist_sig:
        return
    _last_persist_ts = now
    _last_persist_sig = sig
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            """
            INSERT INTO confluence_history
            (trade_date, ts, grade, score, bias, verdict, pass_count,
             vix, pcr, nifty, nifty_chg, components_json, reasons_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                trade_date,
                now,
                grade,
                float(score),
                str(snap.get("bias") or ""),
                str(snap.get("verdict") or ""),
                int(snap.get("pass_count") or 0),
                float(snap.get("vix") or 0),
                float(snap.get("pcr") or 0),
                float(snap.get("nifty") or 0),
                float(snap.get("nifty_chg") or 0),
                json.dumps(snap.get("components") or {}),
                json.dumps(snap.get("reasons") or []),
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("confluence persist: %s", e)


def fetch_history(limit: int = 120, trade_date: str | None = None) -> list[dict[str, Any]]:
    ensure_table()
    limit = max(1, min(500, int(limit)))
    rows: list[dict[str, Any]] = []
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.row_factory = sqlite3.Row
        if trade_date:
            cur = conn.execute(
                """
                SELECT * FROM confluence_history
                WHERE trade_date = ?
                ORDER BY ts DESC LIMIT ?
                """,
                (trade_date, limit),
            )
        else:
            cur = conn.execute(
                """
                SELECT * FROM confluence_history
                ORDER BY ts DESC LIMIT ?
                """,
                (limit,),
            )
        for r in cur.fetchall():
            rows.append(
                {
                    "id": r["id"],
                    "trade_date": r["trade_date"],
                    "ts": r["ts"],
                    "grade": r["grade"],
                    "score": r["score"],
                    "bias": r["bias"],
                    "verdict": r["verdict"],
                    "pass_count": r["pass_count"],
                    "vix": r["vix"],
                    "pcr": r["pcr"],
                    "nifty": r["nifty"],
                    "nifty_chg": r["nifty_chg"],
                    "components": json.loads(r["components_json"] or "{}"),
                    "reasons": json.loads(r["reasons_json"] or "[]"),
                }
            )
        conn.close()
    except Exception as e:
        logger.warning("confluence fetch_history: %s", e)
    return rows
