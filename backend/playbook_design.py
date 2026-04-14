"""
STOCKR.IN â€” Session playbook design layer.

- Pre-build checklist (risk / signal definition / discipline) persisted in SQLite.
- One primary intraday playbook per session from regime tags (anti-sprawl).
- Historical diagnostics: regime distribution + coarse next-day stats (not full intraday PnL).

Playbooks are *planning* tools; execution still flows through the existing gate engine.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pytz

logger = logging.getLogger("playbook_design")

IST = pytz.timezone("Asia/Kolkata")
DB_PATH = Path(__file__).parent / "data" / "backtest.db"

# â”€â”€â”€ Catalogue â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

PLAYBOOKS: dict[str, dict[str, str]] = {
    "GAMMA_EXPIRY_SCALP": {
        "title": "Expiry-day gamma",
        "focus": "ATM straddle/strangle, delta-neutral scalps; respect theta; need live Greeks/LTP.",
        "gates_hint": "Gate 3 structure + tight risk; no overnight.",
    },
    "MAX_PAIN_FADE": {
        "title": "Max pain gravity",
        "focus": "Fade large spot vs max-pain gaps into Tueâ€“Wed of expiry week; spreads, defined risk.",
        "gates_hint": "Mean-reversion bias; confirm with chain OI shifts.",
    },
    "STOP_HUNT_REVERSAL": {
        "title": "Stop hunt / chop",
        "focus": "Wick traps, absorption vs exhaustion; fade false breaks when VIX is spiking.",
        "gates_hint": "Microstructure + spike engine; smaller size.",
    },
    "OI_UNWIND_REVERSAL": {
        "title": "OI / PCR positioning",
        "focus": "Extreme PCR or fast day-over-day PCR shift as context; trade only with price confirmation.",
        "gates_hint": "Treat as filter first; Gate 4 confirmation.",
    },
    "ORB_FII_FILTER": {
        "title": "ORB + flow filter",
        "focus": "9:15â€“9:30 range; breakout only if FII/PCR context aligns (your Gate 3/4 stack).",
        "gates_hint": "Default intraday workhorse when regime is calm.",
    },
    "DEFAULT_STRUCTURE": {
        "title": "Balanced structure",
        "focus": "No dominant regime tag; use standard 5-gate process without a specialty playbook.",
        "gates_hint": "Full gate stack; avoid forcing a theme.",
    },
}

CHECKLIST_ITEMS: list[dict[str, Any]] = [
    {"item_key": "unit_signal", "label": "Signal defined (what exactly fires, on what symbol)", "sort_order": 10},
    {"item_key": "unit_instrument", "label": "Instrument chosen (future / option structure / spread)", "sort_order": 20},
    {"item_key": "unit_hold", "label": "Holding horizon defined (scalp vs day vs swing)", "sort_order": 30},
    {"item_key": "unit_invalidate", "label": "Invalidation / stop rule defined before entry", "sort_order": 40},
    {"item_key": "sig_vs_trade", "label": "Signal vs trade separated (context filter â‰  entry trigger)", "sort_order": 50},
    {"item_key": "risk_max_loss", "label": "Max loss per trade & session cap documented", "sort_order": 60},
    {"item_key": "risk_gap", "label": "Gap / event tail risk reviewed (results, RBI, global)", "sort_order": 70},
    {"item_key": "risk_liquidity", "label": "Strikes liquid enough; bid-ask acceptable for size", "sort_order": 80},
    {"item_key": "risk_broker_api", "label": "Broker API supports order type / SL you need", "sort_order": 90},
    {"item_key": "scanner_readonly", "label": "Nightly / scanner mode stays read-only until backtested", "sort_order": 100},
    {"item_key": "one_playbook", "label": "One primary playbook this session (suppress conflicting themes)", "sort_order": 110},
    {"item_key": "news_calendar", "label": "Calendar / VIX regime checked (no surprise headline dependency)", "sort_order": 120},
]


def get_conn() -> sqlite3.Connection:
    return sqlite3.connect(str(DB_PATH), timeout=15)


def ensure_schema(conn: Optional[sqlite3.Connection] = None) -> None:
    own = conn is None
    if own:
        conn = get_conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS playbook_checklist (
                item_key   TEXT PRIMARY KEY,
                label      TEXT NOT NULL,
                checked    INTEGER NOT NULL DEFAULT 0,
                sort_order INTEGER NOT NULL DEFAULT 0,
                updated_ts REAL
            );
            CREATE TABLE IF NOT EXISTS playbook_daily (
                trade_date     TEXT PRIMARY KEY,
                primary_playbook TEXT NOT NULL,
                regime_json    TEXT,
                suppressed_json TEXT,
                reasons_json   TEXT,
                edge_regime    TEXT,
                nifty_close    REAL,
                vix            REAL,
                pcr            REAL,
                created_ts     REAL
            );
            CREATE INDEX IF NOT EXISTS idx_playbook_daily_ts ON playbook_daily(created_ts);
        """)
        conn.commit()
        _seed_checklist(conn)
    finally:
        if own:
            conn.close()


def _seed_checklist(conn: sqlite3.Connection) -> None:
    ts = time.time()
    for row in CHECKLIST_ITEMS:
        conn.execute(
            """
            INSERT OR IGNORE INTO playbook_checklist (item_key, label, checked, sort_order, updated_ts)
            VALUES (?, ?, 0, ?, ?)
            """,
            (row["item_key"], row["label"], int(row["sort_order"]), ts),
        )
    conn.commit()


def days_to_next_thursday(d: date) -> int:
    """Calendar days until next NIFTY-style weekly Thursday (0 on Thursday)."""
    wd = d.weekday()
    return (3 - wd) % 7


def build_regime_tags(
    *,
    session_date: date,
    vix: float,
    vix_chg: float,
    pcr: float,
    pcr_prev: Optional[float],
    fii_net: Optional[float],
    max_pain: float,
    nifty_close: float,
) -> dict[str, Any]:
    tags: list[str] = []
    d2e = days_to_next_thursday(session_date)
    wd = session_date.weekday()

    if vix >= 20:
        tags.append("VIX_STRESS")
    elif vix >= 16:
        tags.append("VIX_ELEVATED")
    elif vix < 12:
        tags.append("VIX_COMPLACENT")

    if vix_chg >= 3.0:
        tags.append("VIX_SHOCK_UP")
    elif vix_chg <= -3.0:
        tags.append("VIX_CRUSH")

    if pcr > 1.35:
        tags.append("PCR_HIGH")
    elif pcr < 0.68:
        tags.append("PCR_LOW")

    if pcr_prev is not None:
        dp = pcr - pcr_prev
        if abs(dp) >= 0.12:
            tags.append("PCR_FAST_SHIFT")

    if fii_net is not None and fii_net <= -2500:
        tags.append("FII_HEAVY_SELL")
    elif fii_net is not None and fii_net >= 2500:
        tags.append("FII_STRONG_BUY")

    if wd == 3:
        tags.append("EXPIRY_DAY")
    if d2e <= 3:
        tags.append("NEAR_WEEKLY_EXPIRY")

    if max_pain and nifty_close and max_pain > 1000:
        gap = abs(nifty_close - max_pain) / nifty_close * 100.0
        if gap >= 0.35:
            tags.append("FAR_FROM_MAX_PAIN")
        if gap < 0.12:
            tags.append("AT_MAX_PAIN")

    return {
        "tags": tags,
        "days_to_thursday": d2e,
        "weekday": wd,
        "inputs": {
            "vix": round(vix, 2),
            "vix_chg": round(vix_chg, 2),
            "pcr": round(pcr, 3),
            "pcr_prev": round(pcr_prev, 3) if pcr_prev is not None else None,
            "fii_net": fii_net,
            "max_pain": max_pain,
            "nifty_close": nifty_close,
        },
    }


def select_primary_playbook(reg: dict[str, Any]) -> dict[str, Any]:
    """
    Return exactly one primary playbook and a list of suppressed alternates (anti-sprawl).
    Priority is explicit and stable for testing.
    """
    tags = set(reg.get("tags") or [])
    wd = reg.get("weekday", 0)
    d2e = reg.get("days_to_thursday", 7)
    inp = reg.get("inputs") or {}
    vix = float(inp.get("vix") or 0)
    vix_chg = float(inp.get("vix_chg") or 0)
    pcr = float(inp.get("pcr") or 1.0)
    max_pain = float(inp.get("max_pain") or 0)
    nifty_close = float(inp.get("nifty_close") or 0)

    candidates = [
        "GAMMA_EXPIRY_SCALP",
        "MAX_PAIN_FADE",
        "STOP_HUNT_REVERSAL",
        "OI_UNWIND_REVERSAL",
        "ORB_FII_FILTER",
    ]
    primary = "DEFAULT_STRUCTURE"
    reasons: list[str] = []

    # 1) Expiry Thursday â€” gamma dominates if vol is tradable
    if "EXPIRY_DAY" in tags and vix >= 11.5:
        primary = "GAMMA_EXPIRY_SCALP"
        reasons.append("Expiry session with VIX â‰¥ 11.5 â†’ gamma scalping is master playbook.")

    # 2) Tueâ€“Wed before Thursday, elevated vol / pain gap
    elif (
        wd in (1, 2)
        and d2e in (1, 2)
        and vix >= 11.0
        and max_pain > 1000
        and "FAR_FROM_MAX_PAIN" in tags
    ):
        primary = "MAX_PAIN_FADE"
        reasons.append("Expiry-week Tue/Wed, spot materially off max pain â†’ mean-reversion toward pain.")

    # 3) Stress / shock â†’ stop-hunt / reversal microstructure
    elif "VIX_STRESS" in tags or (vix >= 17.5 and "VIX_SHOCK_UP" in tags):
        primary = "STOP_HUNT_REVERSAL"
        reasons.append("High VIX or sharp VIX spike â†’ prioritize stop-run / absorption logic.")

    # 4) Extreme or fast-shifting PCR â†’ positioning playbook as context-first
    elif ("PCR_FAST_SHIFT" in tags and ("PCR_HIGH" in tags or "PCR_LOW" in tags)) or pcr > 1.45 or pcr < 0.58:
        primary = "OI_UNWIND_REVERSAL"
        reasons.append("Extreme or rapidly shifting PCR â†’ OI/positioning as master context.")

    # 5) Default intraday
    else:
        primary = "ORB_FII_FILTER"
        reasons.append("Calm/mixed regime â†’ ORB + flow filter as default playbook.")

    suppressed = [c for c in candidates if c != primary]
    notes: list[str] = []
    if "FII_HEAVY_SELL" in tags and primary == "ORB_FII_FILTER":
        notes.append("FII heavy sell: require stronger Gate 4 confirmation on long breakouts.")
    if "VIX_COMPLACENT" in tags and primary == "ORB_FII_FILTER":
        notes.append("Low VIX: ORB breaks may be shallow â€” tighten targets.")

    return {
        "primary": primary,
        "primary_meta": PLAYBOOKS.get(primary, {}),
        "suppressed_playbooks": suppressed,
        "reasons": reasons,
        "session_notes": notes,
        "meta_rule": "Execute one primary playbook per session; ignore conflicting themes until flat.",
    }


def snapshot_from_live_state(state: dict[str, Any], prices: dict[str, Any]) -> dict[str, Any]:
    """Build selector output from signals.state + price map (same shape as /api/state)."""
    macro = state.get("last_macro") or {}
    fii_o = state.get("last_fii") or {}
    chain = state.get("last_chain") or {}

    vix = float(macro.get("vix") or chain.get("vix") or 15.0)
    pcr = float(macro.get("pcr") or chain.get("pcr") or 1.0)
    # vix chg: macro may expose chg_pct; else 0
    vix_chg = float(macro.get("vix_chg") or macro.get("india_vix_chg") or 0.0)
    fii_net = fii_o.get("fii_net")
    try:
        fii_net = float(fii_net) if fii_net is not None else None
    except (TypeError, ValueError):
        fii_net = None

    nifty_ltp = prices.get("NIFTY") or prices.get("NIFTY 50")
    if isinstance(nifty_ltp, dict):
        nifty_ltp = nifty_ltp.get("price") or nifty_ltp.get("ltp")
    try:
        nifty_close = float(nifty_ltp or macro.get("nifty") or 0)
    except (TypeError, ValueError):
        nifty_close = 0.0

    max_pain = chain.get("max_pain") or 0
    try:
        max_pain = float(max_pain or 0)
    except (TypeError, ValueError):
        max_pain = 0.0

    session_date = datetime.now(IST).date()
    pcr_prev: Optional[float] = None
    try:
        pcr_prev = _fetch_previous_pcr(session_date.isoformat())
    except Exception as e:
        logger.debug("pcr_prev lookup: %s", e)

    reg = build_regime_tags(
        session_date=session_date,
        vix=vix,
        vix_chg=vix_chg,
        pcr=pcr,
        pcr_prev=pcr_prev,
        fii_net=fii_net,
        max_pain=max_pain,
        nifty_close=nifty_close,
    )
    pick = select_primary_playbook(reg)

    edge_regime = None
    try:
        import edge_engine as _ee

        nchg = float(macro.get("nifty_chg") or 0)
        hist = state.get("nifty_hist") or []
        if not hist:
            from feed import price_history

            hist = list(price_history.get("NIFTY", []))
        closes = [h[1] for h in hist[-50:]] if hist else []
        adx = _ee.calc_adx(closes) if len(closes) >= 16 else 20.0
        edge_regime = _ee.classify_regime(vix, adx, nchg, pcr)
    except Exception:
        pass

    return {
        "session_date": session_date.isoformat(),
        "regime": reg,
        "selection": pick,
        "edge_regime": edge_regime,
        "playbook_catalogue": PLAYBOOKS,
        "checklist_template": CHECKLIST_ITEMS,
        "ts": time.time(),
    }


def _fetch_previous_pcr(as_of: str) -> Optional[float]:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT pcr FROM chain_daily
            WHERE date < ?
            ORDER BY date DESC LIMIT 1
            """,
            (as_of,),
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else None
    finally:
        conn.close()


def get_checklist() -> list[dict[str, Any]]:
    ensure_schema()
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT item_key, label, checked, sort_order, updated_ts
            FROM playbook_checklist ORDER BY sort_order, item_key
            """
        ).fetchall()
        return [
            {
                "item_key": r[0],
                "label": r[1],
                "checked": bool(r[2]),
                "sort_order": r[3],
                "updated_ts": r[4],
            }
            for r in rows
        ]
    finally:
        conn.close()


def set_checklist_item(item_key: str, checked: bool) -> bool:
    ensure_schema()
    conn = get_conn()
    try:
        cur = conn.execute(
            """
            UPDATE playbook_checklist SET checked = ?, updated_ts = ?
            WHERE item_key = ?
            """,
            (1 if checked else 0, time.time(), item_key),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def record_playbook_day(
    trade_date: str,
    payload: dict[str, Any],
) -> None:
    ensure_schema()
    reg = payload.get("regime") or {}
    sel = payload.get("selection") or {}
    inp = reg.get("inputs") or {}
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO playbook_daily (
                trade_date, primary_playbook, regime_json, suppressed_json, reasons_json,
                edge_regime, nifty_close, vix, pcr, created_ts
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                trade_date[:10],
                sel.get("primary") or "DEFAULT_STRUCTURE",
                json.dumps(reg, separators=(",", ":")),
                json.dumps(sel.get("suppressed_playbooks") or [], separators=(",", ":")),
                json.dumps(
                    {
                        "reasons": sel.get("reasons"),
                        "session_notes": sel.get("session_notes"),
                        "meta_rule": sel.get("meta_rule"),
                    },
                    separators=(",", ":"),
                ),
                payload.get("edge_regime"),
                float(inp.get("nifty_close") or 0) or None,
                float(inp.get("vix") or 0) or None,
                float(inp.get("pcr") or 0) or None,
                time.time(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_playbook_history(limit: int = 60) -> list[dict[str, Any]]:
    ensure_schema()
    conn = get_conn()
    try:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT * FROM playbook_daily ORDER BY trade_date DESC LIMIT ?
            """,
            (max(1, min(limit, 500)),),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def run_playbook_backtest(days: int = 400) -> dict[str, Any]:
    """
    Walk calendar history in backtest.db. This does not simulate intraday fills;
    it scores how often next-day behaviour rhymes with the playbook theme (diagnostic only).
    """
    ensure_schema()
    days = max(60, min(int(days), 1200))
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT o.date as d, o.open, o.high, o.low, o.close,
                   v.vix, v.vix_chg, c.pcr, c.max_pain_proxy, c.ul_price,
                   f.fii_net
            FROM ohlcv o
            LEFT JOIN vix_daily v ON v.date = o.date
            LEFT JOIN chain_daily c ON c.date = o.date
            LEFT JOIN fii_daily f ON f.date = o.date
            ORDER BY o.date ASC
            """
        ).fetchall()
    finally:
        conn.close()

    if len(rows) < 10:
        return {"error": "insufficient_ohlcv", "rows": len(rows)}

    # Trim to last `days`
    rows = rows[-days:]
    pcr_by_date = {r[0]: float(r[7] or 1.0) for r in rows}

    stats: dict[str, dict[str, Any]] = {}
    max_pain_samples: list[bool] = []

    def _bump(pb: str, key: str, inc: float = 1.0):
        if pb not in stats:
            stats[pb] = {"n": 0, "range_sum": 0.0, "gap_sum": 0.0}
        stats[pb][key] = stats[pb].get(key, 0.0) + inc

    for i in range(len(rows) - 1):
        d_str, o, h, low, c_, vix, vix_chg, pcr, max_pain, ul_px, fii = rows[i]
        d = date.fromisoformat(d_str)
        vix = float(vix or 14.0)
        vix_chg = float(vix_chg or 0.0)
        pcr = float(pcr or 1.0)
        prev_pcr = pcr_by_date.get(_prev_date_str(rows, i))
        max_pain = float(max_pain or 0)
        nifty_close = float(c_ or 0)
        if ul_px and float(ul_px) > 1000:
            nifty_close = float(ul_px)
        try:
            fii_net = float(fii) if fii is not None else None
        except (TypeError, ValueError):
            fii_net = None

        reg = build_regime_tags(
            session_date=d,
            vix=vix,
            vix_chg=vix_chg,
            pcr=pcr,
            pcr_prev=prev_pcr,
            fii_net=fii_net,
            max_pain=max_pain,
            nifty_close=nifty_close,
        )
        pick = select_primary_playbook(reg)
        pb = pick["primary"]

        nxt = rows[i + 1]
        no, nh, nl, nc = float(nxt[1]), float(nxt[2]), float(nxt[3]), float(nxt[4])
        range_pct = ((nh - nl) / no * 100.0) if no else 0.0
        gap_abs = abs(nc - nifty_close) / nifty_close * 100.0 if nifty_close else 0.0

        _bump(pb, "n")
        _bump(pb, "range_sum", range_pct)
        _bump(pb, "gap_sum", gap_abs)

        if pb == "MAX_PAIN_FADE" and max_pain > 1000 and nifty_close:
            toward = (max_pain - nifty_close) * (nc - nifty_close) > 0
            max_pain_samples.append(toward)

    summary = []
    for pb, st in sorted(stats.items(), key=lambda x: -x[1]["n"]):
        n = int(st["n"])
        summary.append(
            {
                "playbook": pb,
                "days": n,
                "avg_next_day_range_pct": round(st["range_sum"] / n, 3) if n else 0.0,
                "avg_next_day_gap_pct": round(st["gap_sum"] / n, 3) if n else 0.0,
            }
        )

    mp_hit = (
        round(100.0 * sum(max_pain_samples) / len(max_pain_samples), 1)
        if max_pain_samples
        else None
    )

    return {
        "window_days": len(rows),
        "playbook_stats": summary,
        "max_pain_directional_hit_pct": mp_hit,
        "max_pain_sample_days": len(max_pain_samples),
        "note": (
            "Diagnostic only: uses daily bars. Next-day range proxies intraday opportunity; "
            "max-pain hit rate is directional move toward strike after MAX_PAIN_FADE days."
        ),
    }


def _prev_date_str(rows: list, idx: int) -> Optional[str]:
    if idx <= 0:
        return None
    return rows[idx - 1][0]


def checklist_completion_ratio() -> dict[str, Any]:
    items = get_checklist()
    if not items:
        return {"total": 0, "checked": 0, "ratio": 0.0}
    ck = sum(1 for x in items if x["checked"])
    return {"total": len(items), "checked": ck, "ratio": round(ck / len(items), 3)}
