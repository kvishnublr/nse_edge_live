"""
Live stock picks from F&O scanner cache + gate context (shared by /api/live-picks and market desk).
"""

from __future__ import annotations

from typing import Any

from backtest_data import swing_radar_candidates

_SECTOR_MAP = {
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
_SKIP_SYMS = {"NIFTY", "BANKNIFTY", "INDIAVIX"}


def _pf(price: float) -> str:
    return str(round(price, 1)) if price < 2000 else str(int(round(price)))


def _rr(entry: float, stop: float, target: float) -> float:
    risk = abs(entry - stop)
    reward = abs(target - entry)
    return round(reward / risk, 1) if risk > 1e-9 else 0.0


def _coerce_stock(s: dict[str, Any], nifty_chg: float, fallback_pc: int) -> dict[str, Any] | None:
    sym = str(s.get("symbol", "") or "").upper()
    price = float(s.get("price", 0) or 0)
    if not sym or sym in _SKIP_SYMS or price <= 0:
        return None

    chg = float(s.get("chg_pct", 0) or 0)
    vol_r = float(s.get("vol_ratio", 0) or 0)
    oi_pct = float(s.get("oi_chg_pct", 0) or 0)
    pc = int(s.get("pc", fallback_pc) or fallback_pc)
    raw_score = int(s.get("score", 0) or 0)
    if raw_score <= 0:
        raw_score = int(min(99, max(40, 35 + pc * 10 + (8 if vol_r >= 1.5 else 0))))

    return {
        "symbol": sym,
        "price": price,
        "chg_pct": chg,
        "oi_chg_pct": oi_pct,
        "vol_ratio": vol_r,
        "atr_pct": float(s.get("atr_pct", 0) or max(abs(chg) * 0.8, 1.2)),
        "rs_pct": float(s.get("rs_pct", 0) or round(chg - nifty_chg, 2)),
        "pc": pc,
        "score": raw_score,
        "g1": s.get("g1", "wt"),
        "g2": s.get("g2", "wt"),
        "g3": s.get("g3", "wt"),
        "g4": s.get("g4", "wt"),
        "g5": s.get("g5", "wt"),
        "verdict": str(s.get("verdict", "") or "").upper(),
        "signal": str(s.get("signal", "WATCH") or "WATCH").upper(),
    }


def _confidence(sig_lbl: str, pc: int, global_verdict: str, stock: dict[str, Any]) -> tuple[str, str, str]:
    stock_verdict = str(stock.get("verdict") or global_verdict or "WATCH").upper()
    if sig_lbl == "EXECUTE" and pc >= 5:
        return "CONFIRMED", "rpk-go", stock_verdict
    if sig_lbl == "EXECUTE" or pc >= 4:
        return "HIGH CONF", "rpk-go", stock_verdict if stock_verdict != "WAIT" else "EXECUTE"
    if global_verdict == "NO TRADE" or stock.get("g1") == "st" or stock.get("g5") == "st":
        return "NO TRADE", "rpk-st", "NO TRADE"
    if sig_lbl == "WATCH" or pc >= 3:
        return "WATCH", "rpk-am", stock_verdict if stock_verdict != "WAIT" else "WATCH"
    return "SCAN", "rpk-am", stock_verdict if stock_verdict != "WAIT" else "SCAN"


def compute_live_picks(state: dict[str, Any]) -> dict[str, Any]:
    """Return sorted picks and total count (before any API trim)."""
    stocks = state.get("last_stocks", []) or []
    indices = state.get("last_macro", {}) or {}
    chain = state.get("last_chain", {}) or {}
    vix = float(indices.get("vix") or 15.0)
    nifty_chg = float(indices.get("nifty_chg") or indices.get("chg_pct") or 0.0)
    pcr = float(chain.get("pcr") or 1.0)
    g_pass = int(state.get("pass_count") or 0)
    global_verdict = str(state.get("verdict") or "WAIT").upper()

    prepared: list[dict[str, Any]] = []
    by_symbol: dict[str, dict[str, Any]] = {}
    for raw in stocks:
        item = _coerce_stock(raw, nifty_chg=nifty_chg, fallback_pc=g_pass)
        if item is None:
            continue
        prepared.append(item)
        by_symbol[item["symbol"]] = item

    candidates = swing_radar_candidates(
        prepared,
        {"vix": vix, "nifty_chg": nifty_chg},
        {"pcr": pcr},
        global_verdict,
        g_pass,
        limit=None,
    )

    picks: list[dict[str, Any]] = []
    for cand in candidates:
        sym = cand["sym"]
        stock = by_symbol.get(sym, {})
        entry = float(cand["entry"])
        stop = float(cand["stop"])
        target = float(cand["target"])
        rr = _rr(entry, stop, target)
        target_p = entry + (target - entry) * 1.6 if cand["direction"] == "LONG" else entry - (entry - target) * 1.6
        rr_p = _rr(entry, stop, target_p)
        conf, cls, stock_verdict = _confidence(cand["sig_lbl"], int(cand["pc"]), global_verdict, stock)
        score = int(cand.get("rank_score", cand["score"]))
        raw_score = int(cand.get("score", score))
        sector = _SECTOR_MAP.get(sym, "Market")
        vol_r = float(cand.get("vol_r") or stock.get("vol_ratio") or 0)
        oi_pct = float(stock.get("oi_chg_pct") or 0)
        direction = str(cand.get("direction") or "LONG")

        picks.append(
            {
                "sym": sym,
                "score": score,
                "raw_score": raw_score,
                "pc": int(cand["pc"]),
                "conf": conf,
                "cls": cls,
                "setup": cand["setup"],
                "direction": direction,
                "close": round(float(cand["price"]), 2),
                "chg_pct": round(float(cand["chg"]), 2),
                "vol_ratio": round(vol_r, 1),
                "oi_chg_pct": round(oi_pct, 1),
                "entry": _pf(entry),
                "sl": _pf(stop),
                "target": _pf(target),
                "target_p": _pf(target_p),
                "rr": rr,
                "rr_p": rr_p,
                "meta": f"{cand['setup']} | {direction} | {sector} | Vol {vol_r:.1f}x | OI {oi_pct:+.1f}% | VIX {vix:.1f}",
                "reason": f"{stock_verdict} | {cand['sig_lbl']} | {cand['pc']}/5 gates | R:R 1:{rr}",
                "reason_p": f"{stock_verdict} | Extended swing target | rank {score} | R:R 1:{rr_p}",
                "g1": stock.get("g1", "wt"),
                "g2": stock.get("g2", "wt"),
                "g3": stock.get("g3", "wt"),
                "g4": stock.get("g4", "wt"),
                "g5": stock.get("g5", "wt"),
            }
        )

    return {"picks": picks, "total": len(picks)}
