"""
Shared Index Radar rules (INDEX_RADAR) for 1-minute bar backtests.
Live scheduler uses tick/30s history; this module mirrors the same thresholds on OHLCV minutes.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def cm_from_candle(c: dict) -> int:
    d = c["date"]
    return d.hour * 60 + d.minute


def index_radar_quality(
    chg: float,
    is_ce: bool,
    ir: Dict[str, Any],
    vix: float,
    pcr: float,
) -> Tuple[str, int]:
    chg_lo = float(ir["chg_min_pct"])
    chg_hi = float(ir["chg_max_pct"])
    chg_str = float(ir["chg_hi_strength_pct"])
    pcr_pe = float(ir["pcr_pe_min"])
    strength = "hi" if abs(chg) >= chg_str else "md"
    quality = 52
    quality += min(
        18,
        int((abs(chg) - chg_lo) / max(chg_hi - chg_lo, 0.01) * 18),
    )
    if vix and vix < 12:
        quality += 8
    if (is_ce and pcr >= 1.0) or (not is_ce and pcr >= pcr_pe):
        quality += 7
    if strength == "hi":
        quality += 10
    quality = max(40, min(99, quality))
    return strength, quality


def passes_index_radar_1m(
    mkt: List[dict],
    i: int,
    ir: Dict[str, Any],
    *,
    vix_eod: float,
    pcr_day: float,
    nifty_day_pct_for_pe: Optional[float],
    cross_other_5m: Optional[float] = None,
) -> Tuple[bool, Optional[float], Optional[bool], str]:
    """
    Apply INDEX_RADAR filters to minute bar i in session list mkt (same day, sorted by time).

    nifty_day_pct_for_pe: Nifty % vs previous session close (live: indices.nifty_chg).
    Use None if unknown — PE signals are rejected; CE ignores this field.

    Returns: (ok, chg_pct_or_none, is_ce_or_none, reason_code)
    """
    if i < 0 or i >= len(mkt):
        return False, None, None, "bounds"

    c = mkt[i]
    cm = cm_from_candle(c)
    px = float(c.get("close") or 0)
    if not px:
        return False, None, None, "px"

    t0, t1w = int(ir["time_start_min"]), int(ir["time_end_min"])
    if not (t0 <= cm <= t1w):
        return False, None, None, "time"

    vx = float(vix_eod or 0)
    if vx and vx >= float(ir.get("vix_block_above", 99)):
        return False, None, None, "vix"

    min_span = float(ir["min_hist_span_sec"])
    min_samp = int(ir["min_hist_samples"])
    if i < min_samp:
        return False, None, None, "warmup_n"
    span = mkt[i - 1]["date"].timestamp() - mkt[0]["date"].timestamp()
    if span < min_span:
        return False, None, None, "warmup_span"

    target_cm = cm - 5
    five_ago_i = next(
        (j for j in range(i - 1, -1, -1) if cm_from_candle(mkt[j]) <= target_cm),
        None,
    )
    if five_ago_i is None:
        return False, None, None, "no5m"
    old_px = float(mkt[five_ago_i].get("close") or 0)
    if not old_px:
        return False, None, None, "oldpx"

    chg = (px - old_px) / old_px * 100
    chg_lo = float(ir["chg_min_pct"])
    chg_hi = float(ir["chg_max_pct"])
    chg_str = float(ir["chg_hi_strength_pct"])
    if ir.get("precision_boost"):
        chg_lo = max(chg_lo, float(ir.get("precision_chg_min", 0.23)))
        chg_hi = min(chg_hi, float(ir.get("precision_chg_max", 0.28)))
    if abs(chg) < chg_lo or abs(chg) > chg_hi:
        return False, None, None, "chg_band"
    is_ce = chg > 0
    if ir.get("precision_boost") and ir.get("precision_hi_only", True):
        if abs(chg) < chg_str:
            return False, None, None, "prec_hi"

    if i >= 1:
        op = float(mkt[i - 1].get("close") or 0)
        if op:
            one_chg = (px - op) / op * 100
            if is_ce and one_chg <= 0:
                return False, None, None, "1m"
            if not is_ce and one_chg >= 0:
                return False, None, None, "1m"

    tr_ag = float(ir["trend_against_pct"])
    thirty_ago_cm = cm - 30
    thirty_i = next(
        (j for j in range(i - 1, -1, -1) if cm_from_candle(mkt[j]) <= thirty_ago_cm),
        None,
    )
    trend_chg = None
    if thirty_i is not None:
        tp = float(mkt[thirty_i].get("close") or 0)
        if tp:
            trend_chg = (px - tp) / tp * 100
            if is_ce and trend_chg < -tr_ag:
                return False, None, None, "trend"
            if not is_ce and trend_chg > tr_ag:
                return False, None, None, "trend"
            tsup = float(ir.get("trend_support_min_pct", 0))
            if ir.get("precision_boost"):
                tsup = max(tsup, float(ir.get("precision_min_trend_sup", 0.10)))
            if tsup > 0:
                if is_ce and trend_chg < tsup:
                    return False, None, None, "trend_sup"
                if not is_ce and trend_chg > -tsup:
                    return False, None, None, "trend_sup"

    if i >= 2:
        micro_min = float(ir["micro_step_min_pct"])
        a = float(mkt[i - 2].get("close") or 0)
        b = float(mkt[i - 1].get("close") or 0)
        if a:
            step_pct = (b - a) / a * 100
            if is_ce and step_pct < micro_min:
                return False, None, None, "micro"
            if not is_ce and step_pct > -micro_min:
                return False, None, None, "micro"

    chase_w = int(ir["anti_chase_sec"])
    chase_ce = float(ir["anti_chase_ce_pct"])
    chase_pe = float(ir["anti_chase_pe_pct"])
    nch = max(2, (chase_w + 59) // 60)
    if i >= nch:
        seg = mkt[i - nch : i]
        closes = [float(x["close"]) for x in seg if x.get("close")]
        if len(closes) >= 2:
            recent_hi, recent_lo = max(closes), min(closes)
            if is_ce and px > recent_hi * (1.0 + chase_ce / 100.0):
                return False, None, None, "chase"
            if not is_ce and px < recent_lo * (1.0 - chase_pe / 100.0):
                return False, None, None, "chase"

    pcr = float(pcr_day or 1.0)
    pcr_ce_av = float(ir.get("pcr_ce_avoid_below", 0))
    if is_ce and pcr_ce_av > 0 and pcr < pcr_ce_av:
        return False, None, None, "pcr_ce"

    pcr_ce_min = float(ir.get("pcr_ce_min", 0))
    if is_ce and pcr_ce_min > 0 and pcr < pcr_ce_min:
        return False, None, None, "pcr_ce_min"

    vs = float(ir.get("vix_soft_skips_md_ce", 0))
    if is_ce and vs > 0 and vx and vx >= vs and abs(chg) < chg_str:
        return False, None, None, "vix_soft_ce"

    if not is_ce:
        if pcr < float(ir["pcr_pe_min"]):
            return False, None, None, "pcr_pe"
        if nifty_day_pct_for_pe is None:
            return False, None, None, "nifty_align"
        if nifty_day_pct_for_pe > float(ir["pe_max_nifty_chg"]):
            return False, None, None, "pe_day"

    cap = float(ir.get("cross_index_against_pct", 0))
    if cap > 0 and cross_other_5m is not None:
        if is_ce and cross_other_5m < -cap:
            return False, None, None, "cross"
        if not is_ce and cross_other_5m > cap:
            return False, None, None, "cross"

    return True, chg, is_ce, ""


def build_minute_close_map(day_candles: List[dict]) -> Dict[int, float]:
    """Last close per session minute for a single calendar day."""
    out: Dict[int, float] = {}
    for c in sorted(day_candles, key=lambda x: x["date"]):
        cm = cm_from_candle(c)
        cl = float(c.get("close") or 0)
        if cl:
            out[cm] = cl
    return out
