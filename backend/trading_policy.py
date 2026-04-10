"""
Aggregate evidence across DB tables for a single “trading policy” view:
sample sizes, live vs backfilled spikes, Index Radar walk-forward split, EXECUTE stats.

Read-only; safe to call from /api/trading-policy.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any

# Minimum resolved trades before treating stats as informative (soft gates).
MIN_INDEX_RESOLVED = 30
MIN_SPIKE_LIVE_CLOSED = 15
MIN_EXECUTE_RESOLVED = 25
OOS_TEST_FRAC = 0.30


def _db_path() -> Path:
    return Path(__file__).resolve().parent / "data" / "backtest.db"


def _split_by_trade_date(
    rows: list[dict[str, Any]], test_frac: float
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[str]]:
    dates = sorted({str(r.get("trade_date") or "") for r in rows if r.get("trade_date")})
    if len(dates) < 4:
        return rows, [], dates
    k = max(1, int(len(dates) * (1.0 - test_frac)))
    train_dates = set(dates[:k])
    test_dates = set(dates[k:])
    train = [r for r in rows if str(r.get("trade_date") or "") in train_dates]
    test = [r for r in rows if str(r.get("trade_date") or "") in test_dates]
    return train, test, dates


def _wr_index(rows: list[dict[str, Any]]) -> tuple[float, int, int]:
    if not rows:
        return 0.0, 0, 0
    w = sum(1 for r in rows if r.get("outcome") == "HIT_T1")
    n = len(rows)
    return round(100.0 * w / n, 2), w, n


def _monthly_buckets(rows: list[dict[str, Any]], date_key: str = "trade_date") -> list[dict[str, Any]]:
    from collections import defaultdict

    buck: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        d = str(r.get(date_key) or "")[:7]
        if len(d) == 7:
            buck[d].append(r)
    out = []
    for m in sorted(buck.keys()):
        sub = buck[m]
        wr, w, n = _wr_index(sub)
        out.append({"month": m, "resolved_n": n, "win_rate_pct": wr, "hits_t1": w})
    return out


def build_trading_policy_report(db_path: str | Path | None = None) -> dict[str, Any]:
    path = Path(db_path) if db_path else _db_path()
    now = time.time()
    out: dict[str, Any] = {
        "generated_ts": now,
        "db_path": str(path),
        "thresholds": {
            "min_index_resolved": MIN_INDEX_RESOLVED,
            "min_spike_live_closed": MIN_SPIKE_LIVE_CLOSED,
            "min_execute_resolved": MIN_EXECUTE_RESOLVED,
            "oos_test_frac": OOS_TEST_FRAC,
        },
    }

    if not path.is_file():
        out["error"] = "backtest.db not found"
        out["verdict"] = _verdict_block("insufficient_data", "index_radar", [])
        return out

    conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        # ── Index Radar ─────────────────────────────────────────────────────
        ix_rows = conn.execute(
            """
            SELECT trade_date, outcome, rr, chg_pct, quality, pcr, type, ts
            FROM index_signal_history
            WHERE outcome IN ('HIT_T1', 'HIT_SL')
            ORDER BY trade_date ASC, ts ASC
            """
        ).fetchall()
        ix_list = [dict(r) for r in ix_rows]
        wr_all, w_all, n_all = _wr_index(ix_list)
        avg_rr = None
        rr_row = conn.execute(
            "SELECT AVG(rr) FROM index_signal_history WHERE outcome IN ('HIT_T1','HIT_SL') AND rr IS NOT NULL"
        ).fetchone()
        if rr_row and rr_row[0] is not None:
            avg_rr = round(float(rr_row[0]), 3)

        train, test, date_list = _split_by_trade_date(ix_list, OOS_TEST_FRAC)
        tr_wr, _, tr_n = _wr_index(train)
        te_wr, _, te_n = _wr_index(test)
        k_split = max(1, int(len(date_list) * (1.0 - OOS_TEST_FRAC))) if len(date_list) >= 4 else 0
        train_date_labels = date_list[:k_split] if k_split else date_list
        test_date_labels = date_list[k_split:] if k_split else []

        ix_warnings: list[str] = []
        if n_all < MIN_INDEX_RESOLVED:
            ix_warnings.append(
                f"Index Radar: only {n_all} resolved trades (suggest ≥{MIN_INDEX_RESOLVED} before trusting WR)."
            )
        if te_n < 6 and len(date_list) >= 4:
            ix_warnings.append(
                f"OOS slice is small (n={te_n} on {len(test_date_labels)} days) — high variance."
            )

        out["index_radar"] = {
            "resolved_n": n_all,
            "hits_t1": w_all,
            "hits_sl": n_all - w_all,
            "win_rate_pct": wr_all,
            "avg_rr": avg_rr,
            "distinct_trade_days": len(date_list),
            "walk_forward": {
                "test_frac": OOS_TEST_FRAC,
                "train_days": len(train_date_labels),
                "test_days": len(test_date_labels),
                "train_resolved_n": tr_n,
                "test_resolved_n": te_n,
                "train_win_rate_pct": tr_wr,
                "test_win_rate_pct": te_wr if te_n else None,
            },
            "by_month": _monthly_buckets(ix_list),
            "warnings": ix_warnings,
        }

        # ── Spikes: live vs backfilled (live_signal_history) ───────────────────
        live_sql = """
            SELECT outcome, status, verdict, COUNT(*) AS c
            FROM live_signal_history
            WHERE IFNULL(signal_key,'') NOT LIKE 'BACKFILL%'
              AND IFNULL(verdict,'') != 'SWING_RADAR'
            GROUP BY outcome, status, verdict
        """
        live_breakdown = [dict(r) for r in conn.execute(live_sql).fetchall()]

        live_closed = conn.execute(
            """
            SELECT outcome, COUNT(*) AS c, AVG(pnl_pct) AS avg_pnl_pct
            FROM live_signal_history
            WHERE IFNULL(signal_key,'') NOT LIKE 'BACKFILL%'
              AND IFNULL(verdict,'') != 'SWING_RADAR'
              AND status = 'CLOSED'
            GROUP BY outcome
            """
        ).fetchall()

        t_vs_sl = conn.execute(
            """
            SELECT
              SUM(CASE WHEN outcome='TARGET HIT' THEN 1 ELSE 0 END) AS t1,
              SUM(CASE WHEN outcome='SL HIT' THEN 1 ELSE 0 END) AS sl
            FROM live_signal_history
            WHERE IFNULL(signal_key,'') NOT LIKE 'BACKFILL%'
              AND IFNULL(verdict,'') != 'SWING_RADAR'
              AND outcome IN ('TARGET HIT','SL HIT')
            """
        ).fetchone()
        t1c = int(t_vs_sl[0] or 0)
        slc = int(t_vs_sl[1] or 0)
        t_sl_wr = round(100.0 * t1c / (t1c + slc), 2) if (t1c + slc) else None

        live_closed_n = sum(int(r["c"]) for r in live_closed)

        bf_n = conn.execute(
            "SELECT COUNT(*) FROM live_signal_history WHERE IFNULL(signal_key,'') LIKE 'BACKFILL%'"
        ).fetchone()[0]

        sp_warnings: list[str] = []
        if live_closed_n < MIN_SPIKE_LIVE_CLOSED:
            sp_warnings.append(
                f"Spikes (live-tracked): only {live_closed_n} closed rows "
                f"(suggest ≥{MIN_SPIKE_LIVE_CLOSED} before using WR)."
            )
        if bf_n and t_sl_wr is not None and t_sl_wr < 40:
            sp_warnings.append(
                "Live T1-vs-SL rate is weak; do not mix with BACKFILLED rows when judging edge."
            )

        out["spikes"] = {
            "live": {
                "raw_breakdown_rows": live_breakdown,
                "closed_by_outcome": [dict(zip(["outcome", "n", "avg_pnl_pct"], [r[0], r[1], round(r[2], 4) if r[2] is not None else None])) for r in live_closed],
                "closed_n": live_closed_n,
                "target_vs_sl": {"target_hits": t1c, "sl_hits": slc, "win_rate_pct": t_sl_wr},
            },
            "backfilled": {
                "row_count": int(bf_n or 0),
                "note": "Imported historical replay — not comparable to live-forward stats.",
            },
            "warnings": sp_warnings,
        }

        # ── Five-gate EXECUTE (signal_log) ───────────────────────────────────
        ex = conn.execute(
            """
            SELECT outcome, COUNT(*) AS c
            FROM signal_log
            WHERE verdict = 'EXECUTE' AND outcome IS NOT NULL
            GROUP BY outcome
            """
        ).fetchall()
        ex_map = {str(r[0]): int(r[1]) for r in ex}
        resolved = sum(v for k, v in ex_map.items() if k in ("WIN", "LOSS"))
        wins = ex_map.get("WIN", 0)
        ex_wr = round(100.0 * wins / resolved, 2) if resolved else None
        ex_warnings: list[str] = []
        if resolved < MIN_EXECUTE_RESOLVED:
            ex_warnings.append(
                f"EXECUTE resolved WIN/LOSS: {resolved} (suggest ≥{MIN_EXECUTE_RESOLVED} for stability)."
            )
        out["execute_gate"] = {
            "by_outcome": ex_map,
            "resolved_win_loss_n": resolved,
            "win_rate_pct": ex_wr,
            "warnings": ex_warnings,
        }

        # ── Swing ───────────────────────────────────────────────────────────
        sw = conn.execute(
            """
            SELECT status, outcome, COUNT(*) FROM live_signal_history
            WHERE IFNULL(verdict,'') = 'SWING_RADAR' OR IFNULL(signal_key,'') LIKE 'SWING|%'
            GROUP BY status, outcome
            """
        ).fetchall()
        out["swing_radar"] = {
            "breakdown": [dict(zip(["status", "outcome", "n"], r)) for r in sw],
            "note": "Mostly positional — wait for closed rows before ranking vs Index Radar.",
        }

        # ── Confluence (no outcomes in DB) ───────────────────────────────────
        ch_n = 0
        try:
            ch_n = int(conn.execute("SELECT COUNT(*) FROM confluence_history").fetchone()[0] or 0)
        except Exception:
            pass
        out["confluence"] = {
            "history_rows": ch_n,
            "has_labeled_outcomes": False,
            "role": "Context only until outcomes are logged.",
        }

    finally:
        conn.close()

    bullets = _policy_bullets(out)
    out["policy_bullets"] = bullets
    out["verdict"] = _compute_verdict(out)
    return out


def _verdict_block(status: str, primary: str, reasons: list[str]) -> dict[str, Any]:
    return {"status": status, "primary_system": primary, "reasons": reasons}


def _compute_verdict(data: dict[str, Any]) -> dict[str, Any]:
    reasons: list[str] = []
    primary = "index_radar"
    status = "ok"

    ix = data.get("index_radar") or {}
    sp = (data.get("spikes") or {}).get("live") or {}
    ex = data.get("execute_gate") or {}

    n_ix = int(ix.get("resolved_n") or 0)
    n_sp = int(sp.get("closed_n") or 0)
    ex_n = int(ex.get("resolved_win_loss_n") or 0)

    if n_ix < MIN_INDEX_RESOLVED and n_sp < MIN_SPIKE_LIVE_CLOSED:
        status = "insufficient_data"
        reasons.append("Very small samples for both Index Radar and live spikes.")
    elif n_ix < MIN_INDEX_RESOLVED:
        status = "weak_sample"
        reasons.append("Index Radar sample below recommended minimum.")

    wf = ix.get("walk_forward") or {}
    te_n = int(wf.get("test_resolved_n") or 0)
    te_wr = wf.get("test_win_rate_pct")
    if te_n and te_wr is not None and te_wr < 42 and n_ix >= MIN_INDEX_RESOLVED:
        reasons.append(f"Recent OOS Index WR ({te_wr}%, n={te_n}) is soft — size down or tighten filters.")

    ex_wr = ex.get("win_rate_pct")
    if ex_n >= 15 and ex_wr is not None and ex_wr < 40:
        reasons.append(f"EXECUTE gate WIN% ({ex_wr}%) is weak on n={ex_n} — do not use as primary trigger.")

    t_sl = (sp.get("target_vs_sl") or {}).get("win_rate_pct")
    if n_sp >= MIN_SPIKE_LIVE_CLOSED and t_sl is not None and t_sl < 35:
        reasons.append(f"Live spike T1-vs-SL ({t_sl}%) is weak — use spikes only with extra filters.")

    if not reasons:
        reasons.append(
            "Primary: Index Radar (labeled levels + walk-forward check). "
            "Spikes: trust live-only rows, not BACKFILLED. "
            "Confluence: context. EXECUTE: confirm with your own sample."
        )

    summary = (
        "Favour Index Radar when you have enough resolved history; "
        "size to walk-forward OOS; ignore spike backfill for win-rate claims."
    )
    return {
        "status": status,
        "primary_system": primary,
        "summary": summary,
        "reasons": reasons,
    }


def _policy_bullets(data: dict[str, Any]) -> list[str]:
    ix = data.get("index_radar") or {}
    wf = ix.get("walk_forward") or {}
    sp = (data.get("spikes") or {}).get("live") or {}
    bf = (data.get("spikes") or {}).get("backfilled") or {}
    ex = data.get("execute_gate") or {}

    te_wr = wf.get("test_win_rate_pct")
    te_wr_s = f"{te_wr}%" if te_wr is not None else "n/a"
    t_sl = sp.get("target_vs_sl") or {}
    t_sl_wr = t_sl.get("win_rate_pct")
    t_sl_s = f"{t_sl_wr}%" if t_sl_wr is not None else "n/a"
    ex_wr = ex.get("win_rate_pct")
    ex_wr_s = f"{ex_wr}%" if ex_wr is not None else "n/a"
    lines = [
        f"Index Radar: {ix.get('resolved_n', 0)} resolved (T1 vs SL), WR {ix.get('win_rate_pct')}%"
        + (f", avg R:R {ix.get('avg_rr')}" if ix.get("avg_rr") else ""),
        f"Index walk-forward (last ~{int((wf.get('test_frac') or 0) * 100)}% of trading days as OOS): "
        f"train WR {wf.get('train_win_rate_pct')}% (n={wf.get('train_resolved_n')}), "
        f"test WR {te_wr_s} (n={wf.get('test_resolved_n')})",
        f"Spikes live-closed: {sp.get('closed_n', 0)} rows; T1 vs SL WR {t_sl_s} "
        f"({t_sl.get('target_hits')}T / {t_sl.get('sl_hits')}SL)",
        f"Spikes BACKFILLED rows: {bf.get('row_count', 0)} — do not merge with live WR.",
        f"EXECUTE (5-gate) WIN/LOSS: {ex.get('resolved_win_loss_n', 0)} trades, WR {ex_wr_s}",
        f"Aim for ≥{MIN_INDEX_RESOLVED} resolved Index trades and ≥{MIN_SPIKE_LIVE_CLOSED} live-closed spikes before aggressive sizing.",
    ]
    return lines
