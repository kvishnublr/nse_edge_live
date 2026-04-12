"""
INTRA INDEX — NIFTY 1-minute session context: ORB (9:15–9:30), VWAP, heavyweight breadth,
and a paper intraday *confidence* score with an optional FIRE flag in 9:30–10:30 IST.

This is not option strike selection or backtested option premium P&L — see tab disclaimer.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Optional

import pytz

from config import KITE_TOKENS

logger = logging.getLogger("intra_index_engine")

IST = pytz.timezone("Asia/Kolkata")
NIFTY_TOKEN = int(KITE_TOKENS.get("NIFTY") or 256265)

# IST minutes from midnight
M_OPEN = 9 * 60 + 15
M_ORB_END = 9 * 60 + 30
M_SIGNAL_END = 10 * 60 + 30
M_LUNCH0 = 13 * 60
M_LUNCH1 = 13 * 60 + 30
M_CLOSE = 15 * 60 + 30

# Heavyweight cash symbols (must exist in feed price_cache during session)
HEAVY_SYMBOLS: tuple[str, ...] = (
    "RELIANCE",
    "TCS",
    "HDFCBANK",
    "ICICIBANK",
    "INFY",
    "SBIN",
    "KOTAKBANK",
    "LT",
    "AXISBANK",
    "INDUSINDBK",
    "BAJFINANCE",
    "TATAMOTORS",
    "TATASTEEL",
    "MARUTI",
    "SUNPHARMA",
)

FIRE_CONFIDENCE_MIN = 70
VIX_NOISE = 20.0


@dataclass
class _Bar:
    cm: int
    o: float
    h: float
    low: float
    c: float
    v: float


def _to_ist(dt: Any) -> Optional[datetime]:
    if dt is None:
        return None
    if not isinstance(dt, datetime):
        try:
            dt = datetime.fromisoformat(str(dt).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        return IST.localize(dt)
    return dt.astimezone(IST)


def _session_phase(now_cm: int, weekday: int) -> str:
    if weekday >= 5:
        return "WEEKEND"
    if now_cm < M_OPEN:
        return "PRE_OPEN"
    if now_cm >= M_CLOSE:
        return "CLOSED"
    if now_cm < M_ORB_END:
        return "ORB_BUILD"
    # Inclusive 10:30 bar (cm == M_SIGNAL_END)
    if now_cm <= M_SIGNAL_END:
        return "SIGNAL_WINDOW"
    if M_LUNCH0 <= now_cm < M_LUNCH1:
        return "LUNCH_DRIFT"
    return "REGULAR"


def _parse_minute_rows(raw: list) -> list[_Bar]:
    out: list[_Bar] = []
    for row in raw or []:
        try:
            dt = _to_ist(row.get("date"))
            if not dt:
                continue
            cm = dt.hour * 60 + dt.minute
            if cm < M_OPEN or cm > M_CLOSE:
                continue
            out.append(
                _Bar(
                    cm=cm,
                    o=float(row.get("open") or 0),
                    h=float(row.get("high") or 0),
                    low=float(row.get("low") or 0),
                    c=float(row.get("close") or 0),
                    v=float(row.get("volume") or 0),
                )
            )
        except Exception:
            continue
    out.sort(key=lambda b: b.cm)
    return out


def _vwap_bars(bars: list[_Bar]) -> tuple[float, float, float]:
    """Return (vwap, cum_vol, typical_last) from session bars."""
    tpv = 0.0
    vv = 0.0
    for b in bars:
        if b.v <= 0:
            continue
        tp = (b.h + b.low + b.c) / 3.0
        tpv += tp * b.v
        vv += b.v
    vwap = tpv / vv if vv > 0 else 0.0
    return vwap, vv, tpv


def evaluate_intraday_slice(
    bars_all: list[_Bar],
    asof_cm: int,
    trade_d: date,
    vix_p: float,
    mean_chg: float,
    adv_n: int,
    dec_n: int,
    heavy_n: int,
    *,
    fire_threshold: int = FIRE_CONFIDENCE_MIN,
    ist_time_label: Optional[str] = None,
    use_all_bars_for_last: bool = False,
) -> dict[str, Any]:
    """
    Core ORB + VWAP + confidence + FIRE using bars with cm in [M_OPEN, asof_cm].
    If use_all_bars_for_last, nifty_last uses last bar of full day (live "current" print).
    """
    wd = trade_d.weekday()
    base: dict[str, Any] = {
        "trade_date": trade_d.isoformat(),
        "ist_time": ist_time_label or f"{asof_cm // 60:02d}:{asof_cm % 60:02d}",
        "session_phase": _session_phase(asof_cm, wd),
        "fire_signal": False,
        "confidence": 0,
        "fire_threshold": fire_threshold,
        "direction": "NONE",
        "orb_high": None,
        "orb_low": None,
        "orb_break": "NONE",
        "nifty_last": None,
        "vwap": None,
        "vwap_dist_pct": None,
        "vix": round(vix_p, 2) if vix_p else None,
        "heavy_breadth": {
            "mean_chg_pct": round(mean_chg, 4),
            "advancers": adv_n,
            "decliners": dec_n,
            "n": heavy_n,
        },
        "avoid_reasons": [],
        "notes": [],
    }

    if wd >= 5:
        base["notes"].append("Weekend")
        base["fire_checklist"] = []
        base["fire_checks_passed"] = 0
        return base

    ev = [b for b in bars_all if M_OPEN <= b.cm <= asof_cm]
    base["nifty_bars_slice"] = len(ev)
    base["nifty_bars_today"] = len([b for b in bars_all if b.cm >= M_OPEN])

    if len(ev) < 2:
        base["notes"].append("Insufficient minute bars through cutoff")
        base["fire_checklist"] = []
        base["fire_checks_passed"] = 0
        return base

    orb_bars = [b for b in ev if M_OPEN <= b.cm < M_ORB_END]
    session_bars = [b for b in ev if b.cm >= M_OPEN]

    if len(orb_bars) >= 2:
        orb_high = max(b.h for b in orb_bars)
        orb_low = min(b.low for b in orb_bars)
        base["orb_high"] = round(orb_high, 2)
        base["orb_low"] = round(orb_low, 2)
    else:
        base["notes"].append("ORB incomplete at cutoff")

    sig_last = ev[-1]
    last_for_px = bars_all[-1] if use_all_bars_for_last and bars_all else sig_last
    base["nifty_last"] = round(last_for_px.c, 2)
    base["nifty_at_cutoff"] = round(sig_last.c, 2)

    vwap, _, _ = _vwap_bars(session_bars)
    base["vwap"] = round(vwap, 2) if vwap else None
    if vwap and sig_last.c:
        base["vwap_dist_pct"] = round((sig_last.c - vwap) / vwap * 100, 3)

    orb_break = "NONE"
    if base["orb_high"] is not None and base["orb_low"] is not None:
        if sig_last.c > base["orb_high"]:
            orb_break = "LONG"
        elif sig_last.c < base["orb_low"]:
            orb_break = "SHORT"
    base["orb_break"] = orb_break

    phase = base["session_phase"]
    conf = 35
    avoid: list[str] = []

    if base["orb_high"] is None:
        avoid.append("ORB incomplete")
    elif orb_break == "NONE":
        avoid.append("No ORB break yet (inside opening range)")
        conf += 5
    else:
        conf += 28
        if orb_break == "LONG" and mean_chg > 0.04:
            conf += 18
        elif orb_break == "SHORT" and mean_chg < -0.04:
            conf += 18
        elif orb_break == "LONG" and mean_chg < -0.08:
            conf -= 12
            avoid.append("ORB long vs weak breadth proxy")
        elif orb_break == "SHORT" and mean_chg > 0.08:
            conf -= 12
            avoid.append("ORB short vs strong breadth proxy")

    if vwap and sig_last.c:
        if orb_break == "LONG" and sig_last.c >= vwap:
            conf += 12
        elif orb_break == "LONG" and sig_last.c < vwap:
            conf -= 10
            avoid.append("Long ORB but below VWAP")
        if orb_break == "SHORT" and sig_last.c <= vwap:
            conf += 12
        elif orb_break == "SHORT" and sig_last.c > vwap:
            conf -= 10
            avoid.append("Short ORB but above VWAP")

    if vix_p and vix_p > VIX_NOISE:
        conf -= 18
        avoid.append(f"VIX>{VIX_NOISE} (noise regime)")

    if M_LUNCH0 <= asof_cm < M_LUNCH1:
        conf -= 25
        avoid.append("Lunch window (1:00–1:30 PM)")

    if asof_cm < M_ORB_END:
        avoid.append("Opening noise window — no FIRE before 9:30")

    if phase != "SIGNAL_WINDOW":
        avoid.append("Outside primary signal window (9:30–10:30)")

    conf = int(max(0, min(100, conf)))
    base["confidence"] = conf

    fire = (
        conf >= fire_threshold
        and phase == "SIGNAL_WINDOW"
        and M_ORB_END <= asof_cm <= M_SIGNAL_END
        and orb_break in ("LONG", "SHORT")
        and not (M_LUNCH0 <= asof_cm < M_LUNCH1)
    )
    if vix_p and vix_p > VIX_NOISE:
        fire = False

    base["fire_signal"] = bool(fire)
    base["direction"] = orb_break

    if not fire:
        base["avoid_reasons"] = list(dict.fromkeys([a for a in avoid if a]))
    else:
        base["avoid_reasons"] = []

    vw_ok = False
    if orb_break == "LONG" and vwap and sig_last.c:
        vw_ok = sig_last.c >= vwap
    elif orb_break == "SHORT" and vwap and sig_last.c:
        vw_ok = sig_last.c <= vwap
    base["fire_checklist"] = [
        {"ok": len(ev) >= 2, "label": "Enough 1m bars through now"},
        {"ok": base["orb_high"] is not None, "label": "ORB range complete (9:15–9:29)"},
        {"ok": orb_break in ("LONG", "SHORT"), "label": "ORB break (close vs range)"},
        {"ok": asof_cm >= M_ORB_END, "label": "Past 9:30 (no opening-noise FIRE)"},
        {
            "ok": phase == "SIGNAL_WINDOW" and M_ORB_END <= asof_cm <= M_SIGNAL_END,
            "label": "Inside primary window 9:30–10:30",
        },
        {"ok": vw_ok, "label": "VWAP aligns with break (long above / short below)"},
        {"ok": not (vix_p and vix_p > VIX_NOISE), "label": f"VIX ≤ {VIX_NOISE} (or n/a)"},
        {"ok": not (M_LUNCH0 <= asof_cm < M_LUNCH1), "label": "Not lunch drift (1:00–1:30)"},
        {"ok": conf >= fire_threshold, "label": f"Confidence ≥ {fire_threshold}"},
    ]
    base["fire_checks_passed"] = sum(1 for x in base["fire_checklist"] if x.get("ok"))

    return base


def compute_live_snapshot(kite: Any, fire_threshold: Optional[int] = None) -> dict[str, Any]:
    """
    Build intraday context from today's NIFTY 1m candles + live heavyweight %chg + VIX level.
    fire_threshold: optional override (40–95), default FIRE_CONFIDENCE_MIN.
    """
    from feed import get_price

    t0 = time.time()
    now = datetime.now(IST)
    today = now.date()
    now_cm = now.hour * 60 + now.minute
    weekday = now.weekday()

    ft = int(fire_threshold) if fire_threshold is not None else FIRE_CONFIDENCE_MIN
    ft = max(40, min(95, ft))

    base: dict[str, Any] = {
        "module": "intra_index_engine.py",
        "ts": t0,
        "trade_date": today.isoformat(),
        "ist_time": now.strftime("%H:%M"),
        "session_phase": _session_phase(now_cm, weekday),
        "fire_signal": False,
        "confidence": 0,
        "fire_threshold": ft,
        "direction": "NONE",
        "orb_high": None,
        "orb_low": None,
        "orb_break": "NONE",
        "nifty_last": None,
        "vwap": None,
        "vwap_dist_pct": None,
        "vix": None,
        "heavy_breadth": {},
        "heavy_rows": [],
        "avoid_reasons": [],
        "notes": [],
        "fire_checklist": [],
        "fire_checks_passed": 0,
    }

    if not kite:
        base["error"] = "Kite not available"
        return base

    if weekday >= 5:
        base["notes"].append("Equity session closed (weekend).")
        return base

    if base["session_phase"] in ("PRE_OPEN", "CLOSED"):
        base["notes"].append("Outside regular cash session for ORB/VWAP stack.")
        return base

    try:
        raw = list(kite.historical_data(NIFTY_TOKEN, today, today, "minute") or [])
    except Exception as e:
        logger.warning("intra_index NIFTY minute: %s", e)
        base["error"] = str(e)
        return base

    bars = _parse_minute_rows(raw)
    base["nifty_bars_today"] = len(bars)

    if not bars:
        base["notes"].append("No NIFTY 1m bars yet (pre-open or holiday / no data).")
        return base

    vx = get_price("INDIAVIX") or {}
    vix_p = float(vx.get("price") or 0)

    rows_h: list[dict[str, Any]] = []
    chgs: list[float] = []
    for sym in HEAVY_SYMBOLS:
        q = get_price(sym) or {}
        chg = float(q.get("chg_pct") or 0)
        chgs.append(chg)
        rows_h.append(
            {
                "symbol": sym,
                "chg_pct": round(chg, 3),
                "price": float(q.get("price") or 0) or None,
            }
        )
    rows_h.sort(key=lambda r: abs(r.get("chg_pct", 0)), reverse=True)
    mean_chg = sum(chgs) / len(chgs) if chgs else 0.0
    up_n = sum(1 for x in chgs if x > 0.05)
    dn_n = sum(1 for x in chgs if x < -0.05)

    inner = evaluate_intraday_slice(
        bars,
        now_cm,
        today,
        vix_p,
        mean_chg,
        up_n,
        dn_n,
        len(HEAVY_SYMBOLS),
        fire_threshold=ft,
        ist_time_label=now.strftime("%H:%M"),
        use_all_bars_for_last=True,
    )
    base.update(inner)
    base["ts"] = t0
    base["module"] = "intra_index_engine.py"
    base["heavy_rows"] = rows_h[:15]
    base["disclaimer"] = (
        "INTRA INDEX is a session context stack (1m NIFTY ORB + VWAP + heavy cash breadth). "
        "FIRE is a paper flag when score ≥ threshold in 9:30–10:30 IST — not a broker order, "
        "not strike selection, and not backtested option premium P&L."
    )
    return base
