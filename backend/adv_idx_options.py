"""
ADV-IDX-OPTIONS — options context layer for index trading (NIFTY / BANKNIFTY).

Combines:
  • Expiry-weekday intelligence (Thu NIFTY weekly, Wed BANKNIFTY — holiday-adjusted only by user)
  • IV *proxy* rank from India VIX vs configurable band (not true 52w IV rank)
  • Max pain + CE/PE OI skew near ATM (GEX-style heuristic, not dealer GEX)
  • FII cash net (from live state) — options-flow interpretation is guidance only
  • Placeholders for global cues scorer + live Greeks (wire data sources later)

Safe to import from FastAPI and scheduler; failures return error dicts, never raise.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Optional

import pytz

logger = logging.getLogger("adv_idx_options")
IST = pytz.timezone("Asia/Kolkata")

_WEEKDAY_NARRATIVE: dict[int, dict[str, str]] = {
    0: {
        "label": "Monday",
        "bias": "Premium often rich early week; many systems lean sell-vol / fade opening gaps with discipline.",
        "note": "Watch opening range; institutional positioning sets tone.",
    },
    1: {
        "label": "Tuesday",
        "bias": "Direction for the week often clearer after Monday’s noise.",
        "note": "Trend-follow setups get cleaner if Monday range breaks.",
    },
    2: {
        "label": "Wednesday",
        "bias": "Mid-week drift common; conviction can be lower intraday.",
        "note": "BANKNIFTY weekly expiry — gamma and pinning risk elevated.",
    },
    3: {
        "label": "Thursday",
        "bias": "NIFTY weekly expiry — expiry crush / gamma effects strongest.",
        "note": "Reduce size on naked options; favor defined risk near expiry.",
    },
    4: {
        "label": "Friday",
        "bias": "Next-week series positioning begins; weekend theta for buyers.",
        "note": "Rolls and calendar activity pick up.",
    },
    5: {"label": "Saturday", "bias": "Market closed.", "note": ""},
    6: {"label": "Sunday", "bias": "Market closed.", "note": ""},
}


def _iv_rank_proxy(vix: float, vix_low: float, vix_high: float) -> dict[str, Any]:
    if vix_high <= vix_low:
        vix_high = vix_low + 1.0
    if vix <= 0:
        return {
            "iv_rank_proxy": None,
            "zone": "unknown",
            "hint": "India VIX unavailable — IV rank proxy skipped.",
            "vix_low_config": vix_low,
            "vix_high_config": vix_high,
        }
    r = (vix - vix_low) / (vix_high - vix_low) * 100.0
    r = max(0.0, min(100.0, r))
    if r < 30:
        zone, hint = "favor_long_vol", "Proxy <30: vol relatively cheap vs band — long-vol / buys less punished by crush (still confirm with setup)."
    elif r > 70:
        zone, hint = "favor_short_vol", "Proxy >70: vol rich vs band — directional buys face IV headwind; favor sells / spreads / smaller premium outlay."
    else:
        zone, hint = "spreads_neutral", "Mid band — spreads / reduced premium; direction alone insufficient."
    return {
        "iv_rank_proxy": round(r, 1),
        "zone": zone,
        "hint": hint,
        "vix_low_config": vix_low,
        "vix_high_config": vix_high,
        "disclaimer": "This uses India VIX vs IV_RANK_VIX_LOW/HIGH — not true 52-week IV Rank. Wire historical IV for production IV Rank.",
    }


def expiry_intelligence(now: Optional[datetime] = None) -> dict[str, Any]:
    dt = now or datetime.now(IST)
    if dt.tzinfo is None:
        dt = IST.localize(dt)
    wd = dt.weekday()
    meta = _WEEKDAY_NARRATIVE.get(wd, _WEEKDAY_NARRATIVE[0])
    # Standard weekly: NIFTY Thursday, BANKNIFTY Wednesday (exchange holidays not modeled)
    nifty_exp = wd == 3 and wd < 5
    bnf_exp = wd == 2 and wd < 5
    gamma_elevated = nifty_exp or bnf_exp
    return {
        "weekday": wd,
        "weekday_name": meta["label"],
        "narrative": meta["bias"],
        "day_note": meta["note"],
        "is_nifty_weekly_expiry_weekday": nifty_exp,
        "is_banknifty_weekly_expiry_weekday": bnf_exp,
        "gamma_elevated_day": gamma_elevated,
        "gamma_note": (
            "NIFTY and/or BANKNIFTY weekly expiry session — treat gamma as 3–5× normal for naked risk."
            if gamma_elevated
            else "No major index weekly expiry on this weekday (Thu=NIFTY, Wed=BNF) — still verify holiday calendar."
        ),
        "holiday_disclaimer": "Holiday-adjusted expiries not applied; confirm on NSE calendar.",
    }


def _gex_proxy(chain: dict[str, Any]) -> dict[str, Any]:
    strikes = chain.get("strikes") or []
    atm = int(chain.get("atm") or 0)
    ul = float(chain.get("ul_price") or 0) or float(atm)
    sym = chain.get("symbol") or "NIFTY"
    if not strikes or not atm:
        return {"sign": "unknown", "skew_score": 0.0, "note": "No chain strikes"}
    band = max(ul * 0.007, 150.0) if sym == "NIFTY" else max(ul * 0.006, 300.0)
    net = 0.0
    tot = 0.0
    for s in strikes:
        st = float(s.get("strike") or 0)
        if abs(st - atm) > band * 2.5:
            continue
        ce = float(s.get("call_oi") or 0)
        pe = float(s.get("put_oi") or 0)
        net += ce - pe
        tot += ce + pe
    if tot <= 0:
        return {"sign": "neutral", "skew_score": 0.0, "note": "No OI in ATM ring"}
    skew = net / tot
    if skew > 0.06:
        sign = "call_heavy"
        interp = "More call OI near ATM vs puts — often associated with upside convexity / call wall dynamics; not exchange GEX."
    elif skew < -0.06:
        sign = "put_heavy"
        interp = "More put OI near ATM — put support or put-wall narrative; confirm with price."
    else:
        sign = "balanced"
        interp = "Roughly balanced CE/PE OI near money."
    return {
        "sign": sign,
        "skew_score": round(skew, 4),
        "note": "Heuristic CE−PE OI skew in ATM band — not dealer gamma exposure.",
        "interpretation": interp,
    }


def _chain_summary(chain: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not chain:
        return None
    gex = _gex_proxy(chain)
    ul = float(chain.get("ul_price") or 0)
    mp = int(chain.get("max_pain") or 0)
    dist = round((ul - mp) / ul * 100, 3) if ul and mp else None
    return {
        "symbol": chain.get("symbol"),
        "expiry": chain.get("expiry"),
        "ul_price": ul,
        "pcr": chain.get("pcr"),
        "max_pain": mp,
        "atm": chain.get("atm"),
        "dist_ul_to_max_pain_pct": dist,
        "gex_proxy": gex,
    }


def _fii_options_reading(fii: Optional[dict[str, Any]]) -> dict[str, Any]:
    if not fii:
        return {
            "summary": "No FII/DII snapshot in state yet (scheduler / WS).",
            "detail": "When fii_net is available: large sustained selling often coincides with index hedging; pair with PCR and expiry context.",
        }
    fn = fii.get("fii_net")
    dn = fii.get("dii_net")
    parts = []
    if fn is not None:
        parts.append(f"FII cash net ≈ {float(fn):+.0f} Cr (lagged daily series in many feeds).")
    if dn is not None:
        parts.append(f"DII net ≈ {float(dn):+.0f} Cr.")
    parts.append(
        "True edge: FII index fut OI change + CE/PE OI writing — not in this snapshot; add NSE/BSE participant OI when available."
    )
    return {"summary": " ".join(parts), "detail": fii}


def global_cues_placeholder() -> dict[str, Any]:
    return {
        "score": None,
        "label": "Kite-only macro: NIFTY / BANKNIFTY / India VIX in the strip above — no third-party quote feeds.",
        "factors": [],
        "disclaimer": "Macro is Kite tape + FII context only; no third-party quote feeds.",
    }


def greeks_education() -> dict[str, Any]:
    return {
        "live_panel": "not_connected",
        "text": (
            "Live Delta/Theta/Vega/Gamma need position + chain from Kite holdings. "
            "After entry: monitor theta burn vs your stop; on expiry days gamma explodes — use ADV-IDX-OPTIONS expiry flags + size down."
        ),
        "theta_alert_stub": "When wired: alert if bought option loses >X% premium per hour from theta alone.",
    }


def _confidence_blend(
    iv_zone: str,
    gamma_elevated: bool,
    verdict: Optional[str],
) -> dict[str, Any]:
    score = 55
    notes = []
    if iv_zone == "favor_long_vol":
        score += 8
        notes.append("IV proxy low — long vol less penalized.")
    elif iv_zone == "favor_short_vol":
        score -= 10
        notes.append("IV proxy high — shrink long-premium size.")
    if gamma_elevated:
        score -= 12
        notes.append("Expiry weekday — reduce naked gamma.")
    if verdict == "EXECUTE":
        notes.append("Gate verdict EXECUTE — still apply options context above.")
    elif verdict == "NO TRADE":
        score -= 5
        notes.append("Verdict NO TRADE — stand down regardless of IV.")
    return {
        "options_context_score": max(0, min(100, score)),
        "notes": notes,
        "disclaimer": "Heuristic blend only — not a trade signal.",
    }


def build_snapshot(kite: Any, state: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    """
    Build full payload for UI + /api/state. Requires kite for option chains; VIX from price cache.
    """
    state = state or {}
    t0 = time.time()
    from config import IV_RANK_VIX_HIGH, IV_RANK_VIX_LOW

    try:
        from fetcher import fetch_indices, fetch_option_chain
    except Exception as e:
        return {"error": str(e), "ts": t0}

    indices = fetch_indices() or {}
    vix = float(indices.get("vix") or 0)
    iv_block = _iv_rank_proxy(vix, IV_RANK_VIX_LOW, IV_RANK_VIX_HIGH)
    exp = expiry_intelligence()

    nifty_c = bnf_c = None
    err_n = err_b = None
    if kite:
        try:
            nifty_c = fetch_option_chain(kite, "NIFTY")
        except Exception as e:
            err_n = str(e)
            logger.debug("adv_idx_options NIFTY chain: %s", e)
        try:
            bnf_c = fetch_option_chain(kite, "BANKNIFTY")
        except Exception as e:
            err_b = str(e)
            logger.debug("adv_idx_options BNF chain: %s", e)
    else:
        err_n = "Kite unavailable"

    fii = state.get("last_fii")
    verdict = state.get("verdict")
    conf = _confidence_blend(
        iv_block.get("zone") or "unknown",
        bool(exp.get("gamma_elevated_day")),
        verdict if isinstance(verdict, str) else None,
    )

    return {
        "ts": t0,
        "vix": vix,
        "vix_chg": float(indices.get("vix_chg") or 0),
        "iv": iv_block,
        "expiry": exp,
        "nifty": _chain_summary(nifty_c),
        "banknifty": _chain_summary(bnf_c),
        "chain_errors": {"nifty": err_n, "banknifty": err_b},
        "fii_options": _fii_options_reading(fii if isinstance(fii, dict) else None),
        "global_cues": global_cues_placeholder(),
        "greeks": greeks_education(),
        "confidence": conf,
        "pattern_memory": {
            "status": "planned",
            "hint": "Log setups + regime; after N trades run attribution (see user scorecard).",
        },
        "correlation_break": {
            "status": "planned",
            "hint": "Compare NIFTY vs BANKNIFTY %chg vs rolling correlation — flag sector rotation.",
        },
        "meta": {
            "module": "adv_idx_options.py",
            "version": 1,
        },
    }
