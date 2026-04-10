"""
Persist ADV INDEX snapshots to SQLite for forward OI-inclusive analysis.

Throttled writes (default 90s) unless bias flips, so the DB grows slowly but
captures regime changes. Same DB file as backtest_data / confluence.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("adv_index_history")

DB_PATH = Path(__file__).resolve().parent / "data" / "backtest.db"

_PERSIST_MIN_SEC = 90.0
_last_persist_ts: float = 0.0
_last_persist_bias: str | None = None


def ensure_table() -> None:
    try:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS adv_index_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trade_date TEXT NOT NULL,
                ts REAL NOT NULL,
                ist_time TEXT,
                score REAL,
                bias TEXT,
                breadth_chg REAL,
                oi_pressure REAL,
                n_weights INTEGER,
                n_futures_quoted INTEGER,
                contributors_json TEXT,
                full_json TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_adv_ix_hist_date_ts ON adv_index_history(trade_date, ts DESC)"
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("adv_index_history ensure_table: %s", e)


def persist_snapshot(snap: dict[str, Any] | None, *, min_sec: float | None = None) -> bool:
    """
    Insert one row if snapshot is valid and throttle allows (or bias changed).
    Returns True if a row was written.
    """
    global _last_persist_ts, _last_persist_bias
    if not snap or snap.get("error"):
        return False

    ensure_table()
    bias = str(snap.get("bias") or "")
    now = time.time()
    gap = float(min_sec if min_sec is not None else _PERSIST_MIN_SEC)
    if (now - _last_persist_ts) < gap and bias == (_last_persist_bias or ""):
        return False

    try:
        from datetime import datetime
        import pytz

        ist = pytz.timezone("Asia/Kolkata")
        dt = datetime.fromtimestamp(float(snap.get("ts") or now), tz=ist)
        trade_date = dt.strftime("%Y-%m-%d")
        ist_time = dt.strftime("%H:%M")
    except Exception:
        trade_date = ""
        ist_time = ""

    top = snap.get("top_contributors") or []
    try:
        contributors_json = json.dumps(top[:24], ensure_ascii=False)
    except Exception:
        contributors_json = "[]"
    try:
        full_json = json.dumps(snap, ensure_ascii=False, default=str)
    except Exception:
        full_json = "{}"

    row = (
        trade_date,
        now,
        ist_time,
        float(snap.get("score") or 0),
        bias,
        float(snap.get("breadth_chg") or 0),
        float(snap.get("oi_pressure") or 0),
        int(snap.get("n_weights") or 0),
        int(snap.get("n_futures_quoted") or 0),
        contributors_json,
        full_json,
    )

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            """
            INSERT INTO adv_index_history
            (trade_date, ts, ist_time, score, bias, breadth_chg, oi_pressure,
             n_weights, n_futures_quoted, contributors_json, full_json)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            row,
        )
        conn.commit()
        conn.close()
        _last_persist_ts = now
        _last_persist_bias = bias
        return True
    except Exception as e:
        logger.warning("adv_index_history persist: %s", e)
        return False


def fetch_history(
    trade_date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    """Return recent rows newest-first. If trade_date set, filter that day only."""
    ensure_table()
    limit = max(1, min(5000, int(limit)))
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        if trade_date:
            cur = conn.execute(
                """
                SELECT id, trade_date, ts, ist_time, score, bias, breadth_chg, oi_pressure,
                       n_weights, n_futures_quoted, contributors_json, full_json
                FROM adv_index_history
                WHERE trade_date = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (str(trade_date).strip()[:10], limit),
            )
        elif from_date and to_date:
            cur = conn.execute(
                """
                SELECT id, trade_date, ts, ist_time, score, bias, breadth_chg, oi_pressure,
                       n_weights, n_futures_quoted, contributors_json, full_json
                FROM adv_index_history
                WHERE trade_date >= ? AND trade_date <= ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (str(from_date)[:10], str(to_date)[:10], limit),
            )
        else:
            cur = conn.execute(
                """
                SELECT id, trade_date, ts, ist_time, score, bias, breadth_chg, oi_pressure,
                       n_weights, n_futures_quoted, contributors_json, full_json
                FROM adv_index_history
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            )
        out = []
        for r in cur.fetchall():
            d = dict(r)
            try:
                d["contributors"] = json.loads(d.pop("contributors_json") or "[]")
            except Exception:
                d["contributors"] = []
            d.pop("full_json", None)
            out.append(d)
        return out
    finally:
        conn.close()


def fetch_history_row_full(row_id: int) -> dict[str, Any] | None:
    ensure_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        r = conn.execute(
            "SELECT * FROM adv_index_history WHERE id=?",
            (int(row_id),),
        ).fetchone()
        if not r:
            return None
        d = dict(r)
        try:
            d["contributors"] = json.loads(d.pop("contributors_json") or "[]")
        except Exception:
            d["contributors"] = []
        try:
            d["snapshot"] = json.loads(d.pop("full_json") or "{}")
        except Exception:
            d["snapshot"] = {}
        return d
    finally:
        conn.close()
