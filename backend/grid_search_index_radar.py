#!/usr/bin/env python3
"""
Grid search on *stored* index_signal_history rows (post-hoc filter).

Optimises train-split win rate on resolved trades (HIT_T1 vs HIT_SL) using
fields saved at signal time: quality, pcr, chg_pct, type (CE/PE).

Out-of-sample: last fraction of *trading dates* is held out; best params are
chosen on the train slice only, then reported on the test slice.

This is not identical to re-running the live detector with new thresholds (rows
were emitted under past rules). It is an honest cheap estimate of how much
extra edge a threshold policy adds on *your* history.

Usage (from repo root or backend/):
  python grid_search_index_radar.py
  python grid_search_index_radar.py --test-frac 0.30 --min-test 6
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from itertools import product
from pathlib import Path

_DB = Path(__file__).resolve().parent / "data" / "backtest.db"


@dataclass(frozen=True)
class Params:
    q_floor: int
    pcr_ce_min: float
    pcr_pe_min: float
    chg_lo: float
    chg_hi: float


def _load_resolved(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        """
        SELECT trade_date, type, chg_pct, quality, pcr, outcome, sig_id
        FROM index_signal_history
        WHERE outcome IN ('HIT_T1', 'HIT_SL')
        ORDER BY trade_date ASC, ts ASC
        """
    ).fetchall()


def _passes(r: sqlite3.Row, p: Params) -> bool:
    q = r["quality"]
    if q is None:
        return False
    if int(q) < p.q_floor:
        return False
    chg = abs(float(r["chg_pct"] or 0))
    if chg < p.chg_lo or chg > p.chg_hi:
        return False
    pcr = r["pcr"]
    if pcr is None:
        return False
    pcr = float(pcr)
    typ = str(r["type"] or "").upper()
    if typ == "CE":
        if pcr < p.pcr_ce_min:
            return False
    elif typ == "PE":
        if pcr < p.pcr_pe_min:
            return False
    else:
        return False
    return True


def _wr(rows: list[sqlite3.Row]) -> tuple[float, int, int]:
    if not rows:
        return 0.0, 0, 0
    wins = sum(1 for r in rows if r["outcome"] == "HIT_T1")
    n = len(rows)
    return round(100.0 * wins / n, 2), wins, n


def _split_by_date(
    rows: list[sqlite3.Row], test_frac: float
) -> tuple[list[sqlite3.Row], list[sqlite3.Row], list[str]]:
    dates = sorted({r["trade_date"] for r in rows})
    if len(dates) < 4:
        return rows, [], dates
    k = max(1, int(len(dates) * (1.0 - test_frac)))
    train_dates = set(dates[:k])
    test_dates = set(dates[k:])
    train = [r for r in rows if r["trade_date"] in train_dates]
    test = [r for r in rows if r["trade_date"] in test_dates]
    return train, test, dates


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--test-frac", type=float, default=0.30, help="Fraction of calendar dates held out (end)")
    ap.add_argument("--min-train", type=int, default=10, help="Min resolved trades on train after filter")
    ap.add_argument("--min-test", type=int, default=8, help="Min OOS trades after filter (raise for less variance)")
    ap.add_argument(
        "--min-test-stable",
        type=int,
        default=15,
        help="Second pass: best OOS WR among grids with at least this many OOS trades",
    )
    ap.add_argument("--db", type=Path, default=_DB)
    args = ap.parse_args()

    if not args.db.is_file():
        print(json.dumps({"error": f"DB not found: {args.db}"}, indent=2))
        return 1

    conn = sqlite3.connect(str(args.db))
    try:
        rows = _load_resolved(conn)
    finally:
        conn.close()

    if len(rows) < 20:
        print(
            json.dumps(
                {
                    "error": "Not enough resolved index rows in DB (need ~20+). Run index backtests first.",
                    "resolved_count": len(rows),
                },
                indent=2,
            )
        )
        return 1

    train, test, all_dates = _split_by_date(rows, args.test_frac)
    if not test:
        print(json.dumps({"error": "Too few distinct dates for OOS split", "dates": len(all_dates)}, indent=2))
        return 1

    q_grid = [50, 54, 58, 62, 66, 70]
    pcr_ce_grid = [0.85, 0.92, 0.98, 1.02]
    pcr_pe_grid = [1.05, 1.12, 1.18, 1.24]
    chg_lo_grid = [0.10, 0.14, 0.18, 0.22]
    chg_hi_grid = [0.32, 0.38, 0.44, 0.52]

    baseline_tr_wr, baseline_tr_w, baseline_tr_n = _wr(train)
    baseline_te_wr, baseline_te_w, baseline_te_n = _wr(test)

    ranked: list[tuple[float, float, Params, int, int]] = []
    ranked_stable: list[tuple[float, float, Params, int, int]] = []

    for qf, pce, ppe, clo, chi in product(
        q_grid, pcr_ce_grid, pcr_pe_grid, chg_lo_grid, chg_hi_grid
    ):
        if clo >= chi - 0.04:
            continue
        p = Params(qf, pce, ppe, clo, chi)
        tr_f = [r for r in train if _passes(r, p)]
        te_f = [r for r in test if _passes(r, p)]
        tr_wr, _, tr_n = _wr(tr_f)
        te_wr, _, te_n = _wr(te_f)
        if tr_n < args.min_train or te_n < args.min_test:
            continue
        ranked.append((te_wr, tr_wr, p, tr_n, te_n))
        if te_n >= args.min_test_stable and tr_n >= args.min_train:
            ranked_stable.append((te_wr, tr_wr, p, tr_n, te_n))

    ranked.sort(key=lambda x: (-x[0], -x[1], -x[3]))
    ranked_stable.sort(key=lambda x: (-x[0], -x[1], -x[4]))

    out = {
        "db": str(args.db),
        "resolved_total": len(rows),
        "unique_dates": len(all_dates),
        "test_frac": args.test_frac,
        "date_range": f"{all_dates[0]} .. {all_dates[-1]}",
        "baseline": {
            "train_wr_pct": baseline_tr_wr,
            "train_n": baseline_tr_n,
            "test_wr_pct_oos": baseline_te_wr,
            "test_n_oos": baseline_te_n,
        },
        "best_max_oos_wr": None,
        "best_stable_oos": None,
        "selection_rule": "best_max_oos_wr: max OOS WR with train_n>=min_train, test_n>=min_test. best_stable_oos: same but test_n>=min_test_stable (less variance).",
        "top_5_oos": [],
        "caution": "High OOS % on tiny N is luck; prefer best_stable_oos when available.",
    }

    def _pack(te_wr, tr_wr, p, tr_n, te_n):
        return {
            "oos_wr_pct": te_wr,
            "oos_wins": sum(1 for r in test if _passes(r, p) and r["outcome"] == "HIT_T1"),
            "oos_n": te_n,
            "train_wr_pct": tr_wr,
            "train_n_filtered": tr_n,
            "params": {
                "quality_floor": p.q_floor,
                "pcr_ce_min": p.pcr_ce_min,
                "pcr_pe_min": p.pcr_pe_min,
                "abs_chg_pct_band": [p.chg_lo, p.chg_hi],
            },
            "delta_oos_vs_baseline_pp": round(te_wr - baseline_te_wr, 2),
        }

    if ranked:
        x = ranked[0]
        out["best_max_oos_wr"] = _pack(x[0], x[1], x[2], x[3], x[4])
        if x[4] < 15:
            out["best_max_oos_wr"]["warning_small_oos_n"] = True
        out["top_5_oos"] = [
            {
                "oos_wr_pct": t[0],
                "train_wr_pct": t[1],
                "train_n": t[3],
                "oos_n": t[4],
                "quality_floor": t[2].q_floor,
                "pcr_ce_min": t[2].pcr_ce_min,
                "pcr_pe_min": t[2].pcr_pe_min,
                "chg_band": [t[2].chg_lo, t[2].chg_hi],
            }
            for t in ranked[:5]
        ]
    else:
        out["hint"] = "No grid point met min_train/min_test; lower --min-train / --min-test or widen DB history."

    if ranked_stable:
        y = ranked_stable[0]
        out["best_stable_oos"] = _pack(y[0], y[1], y[2], y[3], y[4])
    elif ranked:
        out["best_stable_oos"] = None
        out["stable_skip_reason"] = f"No grid with OOS n>={args.min_test_stable}; try --min-test-stable 10"

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
