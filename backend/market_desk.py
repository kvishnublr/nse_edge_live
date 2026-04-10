"""
Market desk bundle: RSS headlines (global + India) + live picks + macro/FII hints.
Cached to avoid hammering RSS; safe when Kite is offline (picks may be empty).
"""

from __future__ import annotations

import html
import logging
import time
import xml.etree.ElementTree as ET
from typing import Any

import requests

from live_picks import compute_live_picks

logger = logging.getLogger("market_desk")

_CACHE: dict[str, Any] = {}
_CACHE_TS = 0.0
_TTL_SEC = 360.0  # 6 minutes

_RSS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Google News topic RSS — no API key; for context only (not investment advice).
URL_GLOBAL = (
    "https://news.google.com/rss/search?q=global+markets+OR+Federal+Reserve+OR+crude+oil+OR+US+stocks"
    "&hl=en-US&gl=US&ceid=US:en"
)
URL_INDIA = (
    "https://news.google.com/rss/search?q=NSE+OR+NIFTY+OR+RBI+India+OR+Sensex"
    "&hl=en-IN&gl=IN&ceid=IN:en"
)
URL_COMMODITIES = (
    "https://news.google.com/rss/search?q=crude+oil+OR+gold+price+OR+silver+OR+USD+INR+rupee"
    "&hl=en-US&gl=US&ceid=US:en"
)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def _breadth_movers(stocks: list) -> dict[str, Any]:
    """F&O stock universe breadth + leaders / laggards by % change."""
    rows: list[dict[str, Any]] = []
    for s in stocks or []:
        sym = (s.get("symbol") or "").strip()
        if not sym or sym in ("NIFTY", "BANKNIFTY", "INDIAVIX"):
            continue
        chg = _f(s.get("chg_pct"))
        rows.append({"sym": sym, "chg_pct": round(chg, 2)})
    up = sum(1 for r in rows if r["chg_pct"] > 0.15)
    down = sum(1 for r in rows if r["chg_pct"] < -0.15)
    flat = max(0, len(rows) - up - down)
    by_hi = sorted(rows, key=lambda x: -x["chg_pct"])
    by_lo = sorted(rows, key=lambda x: x["chg_pct"])
    return {
        "up": up,
        "down": down,
        "flat": flat,
        "total": len(rows),
        "leaders": by_hi[:8],
        "laggards": by_lo[:8],
    }


def _synthesize_today_brief(
    macro: dict[str, Any],
    fii: dict[str, Any],
    bm: dict[str, Any],
    confluence: dict[str, Any],
    verdict: str,
    pass_count: int,
    news_global: list,
    news_india: list,
    news_commodities: list,
) -> list[str]:
    """Short decision-support checklist lines (context only, not advice)."""
    lines: list[str] = []

    n = _f(macro.get("nifty"))
    bn = _f(macro.get("banknifty"))
    nc = macro.get("nifty_chg")
    bc = macro.get("banknifty_chg")
    if n > 0:
        t = f"Nifty spot ~{n:,.0f}"
        if nc is not None and _f(nc) != 0.0:
            t += f" ({_f(nc):+.2f}% vs prev close)."
        else:
            t += "."
        lines.append(t)
    if bn > 0:
        t = f"Bank Nifty ~{bn:,.0f}"
        if bc is not None and _f(bc) != 0.0:
            t += f" ({_f(bc):+.2f}% on day)."
        else:
            t += "."
        lines.append(t)

    vx = _f(macro.get("vix"))
    if vx > 0:
        if vx < 13:
            reg = "low — vol sellers often favored in model"
        elif vx < 17:
            reg = "moderate — balanced intraday noise"
        else:
            reg = "elevated — expect wider swings; size down if unsure"
        lines.append(f"India VIX {vx:.1f} ({reg}).")

    fn = fii.get("fii_net")
    dn = fii.get("dii_net")
    if fn is not None and _f(fn) != 0.0:
        fdir = "bought" if _f(fn) >= 0 else "sold"
        lines.append(f"FII cash (latest print): net {fdir} ~₹{abs(_f(fn)):,.0f} Cr.")
    if dn is not None and _f(dn) != 0.0:
        ddir = "bought" if _f(dn) >= 0 else "sold"
        lines.append(f"DII cash: net {ddir} ~₹{abs(_f(dn)):,.0f} Cr.")

    tot = int(bm.get("total") or 0)
    if tot > 0:
        lines.append(
            f"F&O stock breadth (this scanner): {bm['up']} up, {bm['down']} down, "
            f"{bm['flat']} flattish of {tot} names — crude risk-on/off tilt."
        )

    if confluence:
        g = str(confluence.get("grade") or "—").upper()
        sc = confluence.get("score")
        bias = confluence.get("bias") or "—"
        vdx = confluence.get("verdict") or "—"
        pc_c = confluence.get("pass_count")
        pc_use = int(pc_c) if pc_c is not None else pass_count
        lines.append(
            f"Confluence model: grade {g} ({sc}/100), bias {bias}, verdict {vdx}; "
            f"gates {pc_use}/5 aligned with engine."
        )
    else:
        lines.append(
            f"Signal engine: verdict {verdict}, {pass_count}/5 gates "
            "(refresh desk after chain tick for full confluence line)."
        )

    if news_india:
        t0 = news_india[0].get("title") or ""
        if t0:
            lines.append("India headline watch: " + (t0[:160] + "…" if len(t0) > 160 else t0))
    if news_global:
        t0 = news_global[0].get("title") or ""
        if t0:
            lines.append("Global headline watch: " + (t0[:160] + "…" if len(t0) > 160 else t0))
    if news_commodities:
        t0 = news_commodities[0].get("title") or ""
        if t0:
            lines.append("Commodities / FX watch: " + (t0[:160] + "…" if len(t0) > 160 else t0))

    lines.append(
        "Checklist only — confirm with your rules, liquidity, and risk; no guaranteed move."
    )
    return lines


def _parse_rss_items(xml_text: str, limit: int = 10) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[str] = set()
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.debug("RSS parse error: %s", e)
        return out

    channel = root.find("channel")
    if channel is None:
        return out

    for it in channel.findall("item"):
        if len(out) >= limit:
            break
        raw_title = (it.findtext("title") or "").strip()
        title = html.unescape(raw_title).replace("<b>", "").replace("</b>", "")
        link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()
        src_el = it.find("source")
        source = (src_el.text or "").strip() if src_el is not None else ""
        if not title or title in seen:
            continue
        seen.add(title)
        short_pub = pub[:48] + ("…" if len(pub) > 48 else "")
        out.append(
            {
                "title": title[:220] + ("…" if len(title) > 220 else ""),
                "link": link,
                "published": short_pub,
                "source": source,
            }
        )
    return out


def _fetch_rss(url: str, limit: int) -> list[dict[str, str]]:
    try:
        r = requests.get(url, headers=_RSS_HEADERS, timeout=14)
        r.raise_for_status()
        return _parse_rss_items(r.text, limit=limit)
    except Exception as e:
        logger.debug("RSS fetch failed %s: %s", url[:60], e)
        return []


def _live_desk_extras(
    signals_mod: Any,
    news_global: list,
    news_india: list,
    news_commodities: list,
) -> dict[str, Any]:
    macro = signals_mod.state.get("last_macro") or {}
    fii = signals_mod.state.get("last_fii") or {}
    stocks = signals_mod.state.get("last_stocks") or []
    bm = _breadth_movers(stocks)
    conf = signals_mod.state.get("confluence") or {}
    verdict = str(signals_mod.state.get("verdict") or "WAIT")
    pc = int(signals_mod.state.get("pass_count") or 0)
    brief = _synthesize_today_brief(
        macro,
        fii,
        bm,
        conf,
        verdict,
        pc,
        news_global,
        news_india,
        news_commodities,
    )
    lp = compute_live_picks(signals_mod.state)
    return {
        "picks": lp["picks"][:12],
        "picks_total": lp["total"],
        "macro": macro,
        "fii": fii,
        "breadth": {"up": bm["up"], "down": bm["down"], "flat": bm["flat"], "total": bm["total"]},
        "fno_leaders": bm["leaders"][:6],
        "fno_laggards": bm["laggards"][:6],
        "today_brief": brief,
    }


def get_market_desk(signals_mod: Any, force_refresh: bool = False) -> dict[str, Any]:
    """Build desk payload from signals.state + cached RSS."""
    global _CACHE, _CACHE_TS
    now = time.time()
    if not force_refresh and _CACHE and (now - _CACHE_TS) < _TTL_SEC:
        base = dict(_CACHE)
        ng = base.get("news_global") or []
        ni = base.get("news_india") or []
        nc = base.get("news_commodities") or []
        extra = _live_desk_extras(signals_mod, ng, ni, nc)
        base.update(extra)
        base["stale"] = True
        return base

    ng = _fetch_rss(URL_GLOBAL, 10)
    ni = _fetch_rss(URL_INDIA, 10)
    nc = _fetch_rss(URL_COMMODITIES, 8)
    extra = _live_desk_extras(signals_mod, ng, ni, nc)

    payload = {
        "news_global": ng,
        "news_india": ni,
        "news_commodities": nc,
        "cached_at": int(now),
        "ttl_sec": int(_TTL_SEC),
        "stale": False,
        "error": None,
        **extra,
    }
    if not ng and not ni and not nc:
        payload["error"] = "Headlines temporarily unavailable (network or RSS). Live picks & breadth still work."

    _CACHE = {
        "news_global": ng,
        "news_india": ni,
        "news_commodities": nc,
        "cached_at": int(now),
        "ttl_sec": int(_TTL_SEC),
        "error": payload["error"],
    }
    _CACHE_TS = now
    return payload
