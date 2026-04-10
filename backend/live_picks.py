"""
Live stock picks from F&O scanner cache + gate context (shared by /api/live-picks and market desk).
"""

from __future__ import annotations

from typing import Any


def compute_live_picks(state: dict[str, Any]) -> dict[str, Any]:
    """Return sorted picks and total count (before top-N trim for API)."""
    stocks = state.get("last_stocks", []) or []
    indices = state.get("last_macro", {}) or {}
    vix = float(indices.get("vix") or 15.0)
    g_pass = int(state.get("pass_count") or 0)
    global_verdict = str(state.get("verdict") or "WAIT")

    picks: list[dict[str, Any]] = []
    for s in stocks:
        sym = s.get("symbol", "")
        price = s.get("price", 0) or 0
        if not price or sym in ("NIFTY", "BANKNIFTY", "INDIAVIX"):
            continue
        chg = s.get("chg_pct", 0) or 0
        vol_r = s.get("vol_ratio", 1.0) or 1.0
        oi_pct = s.get("oi_chg_pct", 0) or 0
        stock_score = 0
        if abs(chg) >= 1.5:
            stock_score += 3
        elif abs(chg) >= 0.5:
            stock_score += 1
        if vol_r >= 2.0:
            stock_score += 2
        elif vol_r >= 1.3:
            stock_score += 1
        if abs(oi_pct) >= 5:
            stock_score += 2
        elif abs(oi_pct) >= 2:
            stock_score += 1
        if stock_score < 1:
            continue

        atr = price * 0.015
        if chg >= 1.5:
            setup = "Breakout"
            entry_p = price * 1.002
        elif chg > 0:
            setup = "Pullback"
            entry_p = price - 0.2 * atr
        elif chg > -0.5:
            setup = "Recovery"
            entry_p = price + 0.1 * atr
        else:
            setup = "Momentum"
            entry_p = price

        sl_p = entry_p - 1.5 * atr
        tgt_p = entry_p + 2.5 * (entry_p - sl_p)
        tgt_pp = entry_p + 4.0 * (entry_p - sl_p)
        rr = round((tgt_p - entry_p) / max(entry_p - sl_p, 1), 1)
        rr_p = round((tgt_pp - entry_p) / max(entry_p - sl_p, 1), 1)
        score = min(99, round(40 + g_pass * 8 + stock_score * 4 + (5 if vol_r >= 1.5 else 0)))
        pc = int(s.get("pc", g_pass) or g_pass)
        stock_verdict = str(s.get("verdict", global_verdict or "WAIT"))
        signal_label = str(s.get("signal", "WATCH")).upper()
        if stock_verdict == "EXECUTE" and pc >= 5:
            conf = "CONFIRMED"
            cls = "rpk-go"
        elif stock_verdict in ("EXECUTE", "WATCH") or pc >= 3:
            conf = "HIGH CONF" if pc >= 4 else "WATCH"
            cls = "rpk-go" if pc >= 4 else "rpk-am"
        elif global_verdict == "NO TRADE" or s.get("g1") == "st" or s.get("g5") == "st":
            conf = "NO TRADE"
            cls = "rpk-st"
            stock_verdict = "NO TRADE"
        else:
            conf = "WATCH"
            cls = "rpk-am"

        sec_map = {
            "HDFCBANK": "Banking",
            "ICICIBANK": "Banking",
            "AXISBANK": "Banking",
            "KOTAKBANK": "Banking",
            "INDUSINDBK": "Banking",
            "SBIN": "PSU Bank",
            "BANKNIFTY": "Index",
            "TCS": "IT",
            "INFY": "IT",
            "MARUTI": "Auto",
            "TATAMOTORS": "Auto",
            "LT": "Infra",
            "BAJFINANCE": "NBFC",
            "RELIANCE": "Energy",
            "TATASTEEL": "Steel",
            "SUNPHARMA": "Pharma",
        }
        sector = sec_map.get(sym, "Market")
        oi_pct = s.get("oi_chg_pct", 0) or 0

        def _pf(p: float) -> str:
            return str(round(p, 1)) if p < 2000 else str(int(round(p)))

        picks.append(
            {
                "sym": sym,
                "score": score,
                "pc": pc,
                "conf": conf,
                "cls": cls,
                "setup": setup,
                "close": price,
                "chg_pct": round(chg, 2),
                "vol_ratio": round(vol_r, 1),
                "oi_chg_pct": round(oi_pct, 1),
                "entry": _pf(entry_p),
                "sl": _pf(sl_p),
                "target": _pf(tgt_p),
                "target_p": _pf(tgt_pp),
                "rr": rr,
                "rr_p": rr_p,
                "meta": f"{setup} · {sector} · Vol {vol_r:.1f}x · OI {oi_pct:+.1f}% · VIX {vix:.1f}",
                "reason": f"{stock_verdict} · {pc}/5 gates · {signal_label} · R:R 1:{rr}",
                "reason_p": f"{stock_verdict} · {pc}/5 gates · Swing target · R:R 1:{rr_p}",
                "g1": s.get("g1", "wt"),
                "g2": s.get("g2", "wt"),
                "g3": s.get("g3", "wt"),
                "g4": s.get("g4", "wt"),
                "g5": s.get("g5", "wt"),
            }
        )

    picks.sort(key=lambda x: (-x["pc"], -x["score"]))
    return {"picks": picks, "total": len(picks)}
