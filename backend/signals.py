"""
NSE EDGE v5 — Signal Engine
5-gate filter driven entirely by Kite Connect live data.
"""

import time
import logging
import statistics
from typing import Optional, List
from collections import deque
from datetime import datetime
import requests as _requests
import pytz
from config import GATE as TH, LOT_SIZES

logger = logging.getLogger("signals")
_IST = pytz.timezone("Asia/Kolkata")

# ─── TELEGRAM ALERT ───────────────────────────────────────────────────────────
_last_telegram_verdict = None   # debounce — only alert on verdict change
_stock_exec_alert_ts: dict[str, float] = {}
STOCK_EXECUTE_TELEGRAM_COOLDOWN_SEC = 1800  # per symbol
# ADV-SPIKES Telegram/WhatsApp: last send epoch per (symbol, type) — see spike_telegram_dedup_minutes
_spike_alert_last_ts: dict[tuple[str, str], float] = {}
# Smart dedup: also track score of last sent signal so high-score spikes can override
_spike_alert_last_score: dict[tuple[str, str], int] = {}

# ─── WIN-RATE CACHE (from live_signal_history DB) ─────────────────────────────
_wr_cache: dict = {"ts": 0, "sym": {}, "bucket": {}}
_WR_CACHE_TTL = 1800  # refresh every 30 min


def _load_wr_data() -> None:
    """Load win-rate stats from DB into cache. Non-blocking — catches all errors."""
    try:
        import sqlite3
        from backtest_data import DB_PATH
        conn = sqlite3.connect(DB_PATH, timeout=3)
        cur  = conn.cursor()

        # Per-symbol WR (min 10 resolved trades)
        cur.execute("""
            SELECT symbol,
                SUM(CASE WHEN outcome='TARGET HIT' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome='SL HIT' THEN 1 ELSE 0 END)     as losses
            FROM live_signal_history
            WHERE outcome IN ('TARGET HIT','SL HIT')
            GROUP BY symbol
            HAVING (wins + losses) >= 10
        """)
        sym_adj: dict[str, int] = {}
        sym_wr:  dict[str, float] = {}
        for sym, w, l in cur.fetchall():
            total = w + l
            wr    = w / total
            sym_wr[sym] = round(wr, 3)
            if   wr < 0.50: sym_adj[sym] = +15   # weak — much harder to fire
            elif wr < 0.58: sym_adj[sym] = +8    # below-avg
            elif wr < 0.65: sym_adj[sym] = +3    # borderline
            elif wr >= 0.78: sym_adj[sym] = -5   # proven — slightly easier
            else:           sym_adj[sym] = 0

        # Per time-bucket WR (from actual signal_time HH:MM)
        cur.execute("""
            SELECT
                CASE
                    WHEN CAST(substr(signal_time,1,2) AS INTEGER)*60
                       + CAST(substr(signal_time,4,2) AS INTEGER) < 630 THEN 'open_915_930'
                    WHEN CAST(substr(signal_time,1,2) AS INTEGER)*60
                       + CAST(substr(signal_time,4,2) AS INTEGER) < 690 THEN 'morning_930_1030'
                    WHEN CAST(substr(signal_time,1,2) AS INTEGER)*60
                       + CAST(substr(signal_time,4,2) AS INTEGER) < 780 THEN 'midday_1030_1300'
                    ELSE 'late_1300_plus'
                END as bucket,
                SUM(CASE WHEN outcome='TARGET HIT' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome='SL HIT'     THEN 1 ELSE 0 END) as losses
            FROM live_signal_history
            WHERE outcome IN ('TARGET HIT','SL HIT')
              AND length(signal_time) >= 5
            GROUP BY bucket
            HAVING (wins + losses) >= 15
        """)
        bkt_adj: dict[str, int] = {}
        for bkt, w, l in cur.fetchall():
            total = w + l
            wr    = w / total
            if   wr < 0.45: bkt_adj[bkt] = +12  # terrible — big penalty
            elif wr < 0.55: bkt_adj[bkt] = +6   # below avg
            elif wr < 0.62: bkt_adj[bkt] = +3
            else:           bkt_adj[bkt] = 0

        conn.close()
        _wr_cache.update({"ts": time.time(), "sym": sym_adj, "bucket": bkt_adj,
                          "sym_wr": sym_wr})
        logger.info("WR cache loaded: %d symbols, %d buckets", len(sym_adj), len(bkt_adj))
    except Exception as e:
        logger.debug("WR cache load failed: %s", e)


def _get_wr_adj(sym: str, time_bucket: str) -> tuple[int, float]:
    """Return (score_floor_delta, win_rate) for symbol+bucket. Refreshes cache if stale."""
    now = time.time()
    if now - _wr_cache["ts"] > _WR_CACHE_TTL:
        _load_wr_data()
    sym_delta = _wr_cache.get("sym", {}).get(sym, 0)
    bkt_delta = _wr_cache.get("bucket", {}).get(time_bucket, 0)
    wr        = _wr_cache.get("sym_wr", {}).get(sym, -1.0)
    return sym_delta + bkt_delta, wr


# ─── STOCK ATR LEVELS (per-stock ATR-based T1/T2/SL) ─────────────────────────
# Fallback ATR% by symbol tier when price_history is sparse
_ATR_FALLBACK_PCT: dict[str, float] = {
    "MARUTI": 0.55, "BAJFINANCE": 0.55, "TATAMOTORS": 0.60,
    "INDUSINDBK": 0.50, "KOTAKBANK": 0.40, "AXISBANK": 0.45,
    "SBIN": 0.40, "ICICIBANK": 0.40, "HDFCBANK": 0.35,
    "RELIANCE": 0.35, "TCS": 0.30, "INFY": 0.30,
    "TATASTEEL": 0.55, "LT": 0.40, "SUNPHARMA": 0.40,
}
_ATR_SL_MULT  = 0.5   # SL  = 0.5 × ATR from entry  → keeps tight stop
_ATR_T1_MULT  = 1.0   # T1  = 1.0 × ATR             → 1:2 R:R
_ATR_T2_MULT  = 2.2   # T2  = 2.2 × ATR             → 1:4.4 R:R


def _compute_spike_levels(sym: str, price: float, sp_type: str) -> dict:
    """
    ATR-based entry/SL/T1/T2 for a stock spike.
    Uses live price_history ATR; falls back to symbol-tier % when data is sparse.
    """
    isBuy = sp_type == "buy"

    # Try live ATR first
    atr     = calc_atr(sym)
    atr_pct = (atr / price * 100) if price > 0 else 0

    # Sanity check: if ATR is unrealistically small or huge, use fallback
    expected_pct = _ATR_FALLBACK_PCT.get(sym, 0.45)
    if atr_pct < 0.08 or atr_pct > 4.0:
        atr = price * expected_pct / 100
        atr_pct = expected_pct

    # Clamp: never let SL be less than 0.2% or T1 be less than 0.4%
    sl_pts = max(price * 0.002, atr * _ATR_SL_MULT)
    t1_pts = max(price * 0.004, atr * _ATR_T1_MULT)
    t2_pts = max(price * 0.008, atr * _ATR_T2_MULT)

    entry = round(price, 2)
    if isBuy:
        sl = round(entry - sl_pts, 2)
        t1 = round(entry + t1_pts, 2)
        t2 = round(entry + t2_pts, 2)
    else:
        sl = round(entry + sl_pts, 2)
        t1 = round(entry - t1_pts, 2)
        t2 = round(entry - t2_pts, 2)

    rr = round(t1_pts / sl_pts, 2) if sl_pts else 0
    return {
        "entry": entry, "sl": sl, "t1": t1, "t2": t2,
        "atr": round(atr, 2), "atr_pct": round(atr_pct, 3), "rr": rr,
    }


def _send_telegram(msg: str):
    from config import TELEGRAM_BOT_TOKEN, get_telegram_chat_ids

    if not TELEGRAM_BOT_TOKEN:
        return
    chat_ids = get_telegram_chat_ids()
    if not chat_ids:
        return
    for cid in chat_ids:
        try:
            _requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
                timeout=5,
            )
        except Exception as e:
            logger.debug("Telegram send failed (%s): %s", cid, e)


def send_telegram_message(msg: str) -> None:
    """Public hook for scheduler / other modules (same path as verdict alerts)."""
    _send_telegram(msg)


def send_today_signals_digest(trade_date: str | None = None) -> dict:
    """
    Push an HTML digest of today's persisted signals (live_signal_history, index_signal_history,
    optional adv_index_history) to every chat in get_telegram_chat_ids() — primary + HARSHVTRADE + TELEGRAM_CHAT_IDS.
    """
    import html as _html
    import sqlite3
    from config import TELEGRAM_BOT_TOKEN, get_telegram_chat_ids
    from backtest_data import get_conn, DB_PATH

    chat_ids = get_telegram_chat_ids()
    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "error": "TELEGRAM_BOT_TOKEN not set"}
    if not chat_ids:
        return {"ok": False, "error": "No chat ids — set TELEGRAM_CHAT_ID and/or TELEGRAM_CHAT_ID_HARSHVTRADE / TELEGRAM_CHAT_IDS"}

    ist = pytz.timezone("Asia/Kolkata")
    d = (trade_date or "").strip() or datetime.now(ist).strftime("%Y-%m-%d")

    def esc(x) -> str:
        return _html.escape(str(x), quote=True)

    rows_sp: list = []
    rows_ix: list = []
    try:
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT symbol, signal_type, signal_time, COALESCE(trigger,''), COALESCE(outcome,''), "
                "COALESCE(strength,''), COALESCE(status,'') FROM live_signal_history WHERE trade_date=? ORDER BY id",
                (d,),
            )
            rows_sp = cur.fetchall()
        except Exception as e:
            logger.debug("digest live_signal_history: %s", e)
        try:
            cur.execute(
                "SELECT symbol, type, signal_time, strike, COALESCE(outcome,'') "
                "FROM index_signal_history WHERE trade_date=? ORDER BY id",
                (d,),
            )
            rows_ix = cur.fetchall()
        except Exception as e:
            logger.debug("digest index_signal_history: %s", e)
        conn.close()
    except Exception as e:
        return {"ok": False, "error": f"db: {e}"}

    adv_lines: list[str] = []
    try:
        conn2 = sqlite3.connect(str(DB_PATH), timeout=10)
        cur2 = conn2.cursor()
        cur2.execute(
            "SELECT ist_time, bias, score, breadth_chg, oi_pressure FROM adv_index_history "
            "WHERE trade_date=? ORDER BY ts DESC LIMIT 24",
            (d,),
        )
        for r in cur2.fetchall():
            adv_lines.append(f"  {esc(r[0])} · {esc(r[1])} · sc {esc(r[2])} · br {esc(r[3])} · OI {esc(r[4])}")
        conn2.close()
    except Exception:
        pass

    parts: list[str] = [f"<b>Signals digest</b> {esc(d)} IST\n"]
    parts.append(f"<b>Spikes / live history</b> ({len(rows_sp)})\n")
    if not rows_sp:
        parts.append("<i>none in DB for this date</i>\n")
    else:
        for row in rows_sp[:45]:
            sym, stype, stime, trig, out, stren, stat = row
            tail = out or stat or "—"
            parts.append(
                f"• {esc(sym)} {esc(stype)} @ {esc(stime)} · {esc(tail)} · {esc(stren)}\n"
                f"  <code>{esc(trig)[:180]}</code>\n"
            )
        if len(rows_sp) > 45:
            parts.append(f"<i>… +{len(rows_sp) - 45} more</i>\n")

    parts.append(f"\n<b>Index radar (DB)</b> ({len(rows_ix)})\n")
    if not rows_ix:
        parts.append("<i>none in DB for this date</i>\n")
    else:
        for sym, typ, stime, strike, out in rows_ix[:30]:
            parts.append(f"• {esc(sym)} {esc(typ)} {esc(strike)} @ {esc(stime)} · {esc(out)}\n")

    if adv_lines:
        parts.append(f"\n<b>ADV INDEX snapshots</b> (up to 24)\n" + "\n".join(adv_lines) + "\n")

    body = "".join(parts)
    max_chunk = 3900
    chunks: list[str] = []
    while body:
        chunks.append(body[:max_chunk])
        body = body[max_chunk:]

    failures: list[str] = []
    ok_posts = 0
    for cid in chat_ids:
        for ch in chunks:
            try:
                r = _requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": cid, "text": ch, "parse_mode": "HTML"},
                    timeout=15,
                ).json()
                if r.get("ok"):
                    ok_posts += 1
                else:
                    failures.append(f"{cid}: {r.get('description', 'error')}")
            except Exception as ex:
                failures.append(f"{cid}: {ex}")

    out = {
        "ok": ok_posts > 0,
        "date": d,
        "chats": len(chat_ids),
        "telegram_posts_ok": ok_posts,
        "rows_spikes": len(rows_sp),
        "rows_index_radar": len(rows_ix),
        "failures": failures,
    }
    if failures and ok_posts == 0:
        out["ok"] = False
        out["error"] = "; ".join(failures[:3])
    return out


def send_adv_index_telegram(snap: dict) -> None:
    """Telegram when ADV INDEX snapshot is persisted (bias change or throttle window)."""
    from config import TELEGRAM_NOTIFY_ADV_INDEX, TELEGRAM_BOT_TOKEN

    if not TELEGRAM_NOTIFY_ADV_INDEX or not TELEGRAM_BOT_TOKEN or not snap or snap.get("error"):
        return
    bias = str(snap.get("bias") or "—")
    score = snap.get("score", "—")
    br = snap.get("breadth_chg", "—")
    oi = snap.get("oi_pressure", "—")
    nf = snap.get("n_futures_quoted", "—")
    top = snap.get("top_contributors") or []
    t1 = "—"
    if top and isinstance(top[0], dict):
        t1 = str(top[0].get("symbol") or "—")
    msg = (
        f"◇ <b>ADV INDEX</b> · <b>{bias}</b>\n"
        f"Score <code>{score}</code> · Breadth <code>{br}</code>% · OI press <code>{oi}</code> · N fut <code>{nf}</code>\n"
        f"Top: <b>{t1}</b> · NIFTY50 cash + futures OI (live)"
    )
    _send_telegram(msg)


# ─── WHATSAPP ALERT (CallMeBot) ───────────────────────────────────────────────
def _send_whatsapp(msg: str):
    from config import WHATSAPP_PHONE, WHATSAPP_APIKEY
    if not WHATSAPP_PHONE or not WHATSAPP_APIKEY:
        return
    try:
        import urllib.parse
        encoded = urllib.parse.quote(msg)
        _requests.get(
            f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}"
            f"&text={encoded}&apikey={WHATSAPP_APIKEY}",
            timeout=8,
        )
    except Exception as e:
        logger.debug(f"WhatsApp send failed: {e}")


# ─── SPIKE SCORING HELPER ─────────────────────────────────────────────────────
def _score_spike(vol_mult: float, chg_pct: float, sym: str, candle_min: int,
                 sym_wr: float = -1.0) -> int:
    """Score a spike 0-100. Includes WR bonus/penalty when historical data exists."""
    score = 0
    # Volume quality (0-35): 5-8× is sweet spot; very high can still be valid
    if 5.0 <= vol_mult < 8.0:    score += 35
    elif 3.0 <= vol_mult < 5.0:  score += 30
    elif vol_mult >= 8.0:        score += 22   # exceptional — can reverse but notable
    elif 2.0 <= vol_mult < 3.0:  score += 15
    else:                         score += 5
    # Price momentum quality (0-30): bigger moves score higher, not lower
    ap = abs(chg_pct)
    if ap >= 1.5:          score += 30
    elif ap >= 1.0:        score += 28
    elif 0.5 <= ap < 1.0:  score += 25
    elif 0.4 <= ap < 0.5:  score += 18
    elif 0.3 <= ap < 0.4:  score += 10
    else:                   score += 4
    # Symbol quality (0-20) — expanded lists for NSE F&O universe
    hi_sym = {'TCS', 'TATASTEEL', 'INFY', 'RELIANCE', 'HDFCBANK', 'AXISBANK',
              'BAJFINANCE', 'TATAMOTORS', 'INDUSINDBK', 'SUNPHARMA', 'LT'}
    md_sym = {
        'WIPRO', 'TECHM', 'SBIN', 'KOTAKBANK', 'MARUTI', 'ICICIBANK',
        'NTPC', 'POWERGRID', 'ONGC', 'COALINDIA',
        'BAJAJFINSV', 'ADANIENT', 'ADANIPORTS', 'HINDUNILVR', 'BHARTIARTL',
        'DRREDDY', 'DIVISLAB', 'EICHERMOT', 'HEROMOTOCO',
    }
    if sym in hi_sym:    score += 20
    elif sym in md_sym:  score += 12
    else:                score += 5
    # Historical win-rate bonus/penalty (0-8): data-driven adjustment
    if sym_wr >= 0.78:   score += 8   # proven high-WR symbol
    elif sym_wr >= 0.70: score += 4
    elif sym_wr >= 0.60: score += 0   # neutral
    elif sym_wr >= 0.50: score -= 3   # slightly weak
    elif sym_wr >= 0:    score -= 7   # poor WR — penalise the raw score too
    # Time quality (0-15)
    if candle_min <= 600:    score += 15   # 09:15-10:00 — best
    elif candle_min <= 810:  score += 10   # 10:00-13:30
    elif candle_min <= 870:  score += 5    # 13:30-14:30
    # else 0 — 14:30+ is weak
    return score


# ─── SPIKE ALERT (Telegram + WhatsApp) ───────────────────────────────────────
def _send_spike_alert(spike: dict):
    """ADV-SPIKES (NIFTY 200 universe): Telegram + WhatsApp when score >= 60."""
    from config import TELEGRAM_NOTIFY_ADV_SPIKES, GATE as TH

    if not TELEGRAM_NOTIFY_ADV_SPIKES:
        return
    score = spike.get("score", 0)
    if score < 60:
        return
    sym     = str(spike.get("symbol", "") or "").strip().upper()
    sp_type = str(spike.get("type",   "") or "").strip().lower()
    if not sym or sp_type not in ("buy", "sell"):
        return

    dedup_min = max(1, min(120, int(TH.get("spike_telegram_dedup_minutes", 20) or 20)))
    now_ts    = time.time()
    _k        = (sym, sp_type)
    prev_ts   = _spike_alert_last_ts.get(_k, 0.0)
    prev_score = _spike_alert_last_score.get(_k, 0)

    # Smart dedup: allow override if score is significantly higher (≥12 pts) than last alert
    score_override = score >= prev_score + 12 and now_ts - prev_ts >= 5 * 60
    if not score_override and now_ts - prev_ts < dedup_min * 60:
        return

    uni = str(TH.get("spike_universe", "NIFTY200") or "").upper()
    if uni == "NIFTY200" and sym:
        try:
            from fetcher import get_nifty200_symbols
            n200 = {str(s).strip().upper() for s in (get_nifty200_symbols() or []) if s}
            if n200 and sym not in n200:
                return
        except Exception:
            pass

    price = float(spike.get("price", 0) or 0)
    chg   = spike.get("chg_pct", 0)
    vm    = spike.get("vol_mult", 0)
    sig   = spike.get("signal", "")
    t     = spike.get("time", "")

    # Use ATR-based levels from spike dict (set by detect_spikes), else compute now
    entry = float(spike.get("entry", price) or price)
    sl    = float(spike.get("sl",    0)    or 0)
    t1    = float(spike.get("t1",    0)    or 0)
    t2    = float(spike.get("t2",    0)    or 0)
    atr   = float(spike.get("atr",   0)    or 0)
    rr    = float(spike.get("rr",    0)    or 0)
    if not sl:
        lvl   = _compute_spike_levels(sym, price, sp_type)
        entry, sl, t1, t2 = lvl["entry"], lvl["sl"], lvl["t1"], lvl["t2"]
        atr, rr = lvl["atr"], lvl["rr"]

    # WR context
    _, sym_wr = _get_wr_adj(sym, "")
    wr_str = f" · Historical WR {sym_wr*100:.0f}%" if sym_wr >= 0 else ""

    msg = (
        f"⚡ <b>ADV-SPIKES (NIFTY 200) — {sym}</b>  [Score: {score}]\n"
        f"Signal: {sig}  |  {'+' if chg >= 0 else ''}{chg:.2f}%  |  Vol {vm:.1f}×{wr_str}\n"
        f"LTP: ₹{price:.2f}  |  {t}\n"
        f"<b>ATR-Model</b>  Entry ₹{entry:.2f} · SL ₹{sl:.2f} · T1 ₹{t1:.2f} · T2 ₹{t2:.2f}\n"
        f"<i>ATR ₹{atr:.2f} · R:R 1:{rr:.1f} (ATR-scaled levels)</i>"
    )
    _spike_alert_last_ts[_k]    = now_ts
    _spike_alert_last_score[_k] = score
    _send_telegram(msg)
    _send_whatsapp(_strip_html_for_whatsapp(msg))


def _strip_html_for_whatsapp(html: str) -> str:
    """CallMeBot/plain WhatsApp: drop simple HTML tags."""
    import re as _re

    s = _re.sub(r"<br\s*/?>", "\n", html, flags=_re.I)
    s = _re.sub(r"<[^>]+>", "", s)
    return s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")


# ─── GLOBAL STATE (read by WebSocket broadcaster) ─────────────────────────────
state = {
    "gates": {
        1: {"name": "REGIME",       "state": "wt", "score": 50, "rows": []},
        2: {"name": "SMART MONEY",  "state": "wt", "score": 50, "rows": []},
        3: {"name": "STRUCTURE",    "state": "wt", "score": 50, "rows": []},
        4: {"name": "TRIGGER",      "state": "wt", "score": 50, "rows": []},
        5: {"name": "RISK VALID",   "state": "wt", "score": 50, "rows": []},
    },
    "verdict":     "WAIT",
    "verdict_sub": "Initialising...",
    "pass_count":  0,
    "confidence":  0.0,
    "spikes":      [],
    "ticker":      [],
    "last_chain":  None,
    "last_macro":  None,
    "last_stocks": [],
    "last_fii":    None,
    "last_updated": 0,
    "confluence":  None,  # experimental: multi-factor intraday snapshot (confluence_engine.py)
    "adv_index":   None,  # NIFTY50-weighted OI + breadth (adv_index_engine.py)
}

# ─── PRICE HISTORY HELPERS ────────────────────────────────────────────────────
def push_price(symbol: str, price: float, volume: float = 0):
    """Called by scheduler to maintain history for ATR/VWAP."""
    from feed import _hist  # shared deque in feed.py
    if price:
        _hist(symbol).append((time.time(), price, volume))


def calc_atr(symbol: str, periods: int = 14) -> float:
    """ATR from Kite price history."""
    from feed import price_history
    hist  = list(price_history.get(symbol, []))
    defaults = {"NIFTY": 90, "BANKNIFTY": 230, "ICICIBANK": 20,
                "SBIN": 12, "RELIANCE": 30}
    if len(hist) < 3:
        return defaults.get(symbol, 15)
    prices = [h[1] for h in hist]
    trs    = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
    window = trs[-periods:] if len(trs) >= periods else trs
    return round(statistics.mean(window), 2)


def calc_vwap(symbol: str) -> Optional[float]:
    """Session VWAP from Kite tick history."""
    from feed import price_history
    import pytz
    from datetime import datetime
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.now(ist)
    start_ts = now.replace(hour=9, minute=15, second=0, microsecond=0).timestamp()
    hist = [(ts, p, v) for ts, p, v in price_history.get(symbol, [])
            if ts >= start_ts and v > 0]
    if not hist:
        return None
    pv  = sum(p * v for _, p, v in hist)
    vol = sum(v for _, _, v in hist)
    return round(pv / vol, 2) if vol else None


# ─── GATE 1: REGIME ───────────────────────────────────────────────────────────
def gate1_regime(indices: dict, fii: dict) -> dict:
    vix     = indices.get("vix", 15)
    vix_chg = indices.get("vix_chg", 0)
    fii_net = fii.get("fii_net", 0) if fii else 0

    # VIX
    if vix < TH["vix_low"]:
        vix_lbl, vix_col = f"{vix:.1f} — LOW ✓ full size", "cg"
    elif vix < TH["vix_medium"]:
        vix_lbl, vix_col = f"{vix:.1f} — MODERATE 75% size", "ca"
    elif vix < TH["vix_high"]:
        vix_lbl, vix_col = f"{vix:.1f} — ELEVATED 50% size", "ca"
    else:
        vix_lbl, vix_col = f"{vix:.1f} — DANGER avoid", "cr"

    # FII (relaxed thresholds - allow some selling)
    sign    = "+" if fii_net >= 0 else ""
    fii_lbl = f"{sign}₹{abs(fii_net):.0f} Cr — {'NET BUY ✓' if fii_net > 0 else 'NET SELL'}"
    fii_col = "cg" if fii_net > 0 else "ca" if fii_net > -5000 else "cr"

    # Nifty direction
    nifty_chg = indices.get("nifty_chg", 0)
    nif_col   = "cg" if nifty_chg > 0 else "cr"
    nif_lbl   = f"{'+' if nifty_chg >= 0 else ''}{nifty_chg:.2f}%"

    score = 100
    if vix >= TH["vix_high"]:   score -= 40
    elif vix >= TH["vix_medium"]: score -= 20
    elif vix >= TH["vix_low"]:    score -= 5
    if fii_net < -10000:          score -= 20
    elif fii_net < -5000:         score -= 10
    if vix_chg > 10:              score -= 10
    elif vix_chg > 5:            score -= 5
    score = max(0, min(100, score))

    # More lenient: only fail if VIX very high OR massive FII selling
    if vix >= TH["vix_high"] + 5 or fii_net < -20000:
        st = "st"
    elif vix >= TH["vix_high"] or fii_net < -10000:
        st = "am"
    elif score >= 60:
        st = "go"
    else:
        st = "wt"

    rows = [
        {"k": "India VIX",      "v": vix_lbl,  "c": vix_col},
        {"k": "VIX change",     "v": f"{'+' if vix_chg >= 0 else ''}{vix_chg:.1f}%",
         "c": "cr" if vix_chg > 3 else "cg" if vix_chg < -3 else "cm"},
        {"k": "FII cash",       "v": fii_lbl,  "c": fii_col},
        {"k": "Nifty change",   "v": nif_lbl,  "c": nif_col},
        {"k": "Regime verdict", "v": "BULL TREND ✓" if st == "go" else
              "CAUTION" if st in ("am","wt") else "BEAR / AVOID",
         "c": "cg" if st == "go" else "ca" if st in ("am","wt") else "cr"},
    ]
    return {"name": "REGIME", "state": st, "score": score, "rows": rows}


# ─── GATE 2: SMART MONEY ──────────────────────────────────────────────────────
def gate2_smart_money(chain: dict) -> dict:
    if not chain:
        return {"name": "SMART MONEY", "state": "wt", "score": 50,
                "rows": [{"k": "Status", "v": "Fetching option chain...", "c": "cm"}]}

    pcr      = chain.get("pcr", 1.0)
    ul       = chain.get("ul_price", 0)
    mp       = chain.get("max_pain", 0)
    tc_oi    = chain.get("total_call_oi", 0)
    tp_oi    = chain.get("total_put_oi", 0)
    net      = tp_oi - tc_oi

    if pcr >= TH["pcr_bullish"]:
        pcr_lbl, pcr_col = f"{pcr:.2f} — BULLISH ✓", "cg"
    elif pcr <= TH["pcr_bearish"]:
        pcr_lbl, pcr_col = f"{pcr:.2f} — BEARISH", "cr"
    else:
        pcr_lbl, pcr_col = f"{pcr:.2f} — NEUTRAL", "ca"

    mp_dist = round(ul - mp) if mp else 0
    mp_lbl  = f"{mp:,} ({'+' if mp_dist >= 0 else ''}{mp_dist} pts away)"

    # ATM strike OI delta — most predictive single data point in the chain
    atm_row = None
    try:
        strikes = chain.get("strikes", [])
        if strikes and ul > 0:
            # Find the strike closest to current price
            atm = min(strikes, key=lambda s: abs(s.get("strike", 0) - ul))
            atm_strike   = atm.get("strike", 0)
            atm_c_oi     = atm.get("call_oi", 0)
            atm_p_oi     = atm.get("put_oi", 0)
            atm_c_chg    = atm.get("call_oi_chg", 0)
            atm_p_chg    = atm.get("put_oi_chg", 0)
            # Net OI change at ATM: +put / -call = bullish pressure
            atm_net_chg  = atm_p_chg - atm_c_chg
            atm_col      = "cg" if atm_net_chg > 5000 else "cr" if atm_net_chg < -5000 else "ca"
            atm_lbl      = (f"{atm_strike} | CE OI {atm_c_oi:,} ({'+' if atm_c_chg>=0 else ''}{atm_c_chg:,}) "
                            f"| PE OI {atm_p_oi:,} ({'+' if atm_p_chg>=0 else ''}{atm_p_chg:,})")
            atm_row      = {"k": f"ATM {atm_strike} OI chg", "v": atm_lbl, "c": atm_col}
            # Scoring: ATM CE build (bears adding shorts) is bearish; ATM PE build is bullish
            if atm_net_chg > 10000:   net += 50000   # strong bullish ATM signal
            elif atm_net_chg < -10000: net -= 50000  # strong bearish ATM signal
    except Exception:
        pass

    # Max Pain pin zone: if price within ±0.3% of max pain near EOD, expect reversal
    import datetime as _dt
    ist_now = _dt.datetime.now(pytz.timezone("Asia/Kolkata"))
    near_eod = ist_now.hour * 60 + ist_now.minute >= 840  # after 14:00
    max_pain_pin = mp and ul and abs(ul - mp) / mp < 0.003 and near_eod

    score = 60
    if pcr >= TH["pcr_bullish"]:   score += 25
    elif pcr <= TH["pcr_bearish"]: score -= 25
    if net > 100000:               score += 15
    elif net < -100000:            score -= 15
    if max_pain_pin:               score -= 10  # pinning near max pain → reduce conviction
    score = max(0, min(100, score))

    if score <= 35 or pcr <= TH["pcr_bearish"]:
        st = "st"
    elif score >= 70 and pcr >= TH["pcr_bullish"]:
        st = "go"
    elif score >= 55:
        st = "am"
    else:
        st = "wt"

    rows = [
        {"k": "PCR",           "v": pcr_lbl, "c": pcr_col},
        {"k": "Total Call OI", "v": f"{tc_oi:,}", "c": "cr"},
        {"k": "Total Put OI",  "v": f"{tp_oi:,}", "c": "cg"},
        {"k": "OI net bias",   "v": f"{'+' if net >= 0 else ''}{net:,}",
         "c": "cg" if net > 0 else "cr"},
        {"k": "Max Pain",      "v": mp_lbl + (" ⚠ PIN ZONE" if max_pain_pin else ""), "c": "ca"},
    ]
    if atm_row:
        rows.insert(2, atm_row)
    return {"name": "SMART MONEY", "state": st, "score": score, "rows": rows}


# ─── GATE 3: STRUCTURE ────────────────────────────────────────────────────────
def gate3_structure(indices: dict) -> dict:
    nifty = indices.get("nifty", 0)
    high  = indices.get("nifty_high", nifty)
    low   = indices.get("nifty_low",  nifty)
    vwap  = calc_vwap("NIFTY")

    if vwap and vwap > 0:
        diff    = nifty - vwap
        pct     = diff / vwap * 100
        if pct > 0.1:
            vwap_lbl, vwap_col = f"ABOVE +{pct:.2f}% ✓", "cg"
        elif pct > -0.1:
            vwap_lbl, vwap_col = f"AT VWAP {pct:.2f}%", "ca"
        else:
            vwap_lbl, vwap_col = f"BELOW {pct:.2f}%", "cr"
        vwap_val = f"{vwap:,.0f}"
    else:
        vwap_lbl, vwap_col, vwap_val = "Computing...", "cm", "—"
        pct = 0

    rng     = high - low if high > low else 0
    rng_pct = rng / low * 100 if low else 0
    pos_pct = (nifty - low) / rng * 100 if rng > 0 else 50

    if pos_pct >= 60:
        pos_lbl, pos_col = f"Upper {pos_pct:.0f}% of range ✓", "cg"
    elif pos_pct >= 40:
        pos_lbl, pos_col = f"Mid {pos_pct:.0f}%", "ca"
    else:
        pos_lbl, pos_col = f"Lower {pos_pct:.0f}% — weak", "cr"

    score = 60
    if vwap and pct > 0.05:  score += 20
    elif vwap and pct < -0.10: score -= 15
    if pos_pct >= 55:        score += 15
    elif pos_pct < 35:       score -= 10
    score = max(0, min(100, score))

    st = "go" if score >= 55 else "am" if score >= 40 else "st"

    rows = [
        {"k": "vs VWAP",        "v": vwap_lbl, "c": vwap_col},
        {"k": "VWAP level",     "v": vwap_val, "c": "cb"},
        {"k": "Day H/L",        "v": f"{high:,.0f} / {low:,.0f} ({rng_pct:.2f}% range)", "c": "cm"},
        {"k": "Range position", "v": pos_lbl,  "c": pos_col},
        {"k": "Nifty price",    "v": f"{nifty:,.2f}",
         "c": "cg" if indices.get("nifty_chg", 0) > 0 else "cr"},
    ]
    return {"name": "STRUCTURE", "state": st, "score": score, "rows": rows}


# ─── GATE 4: TRIGGER ──────────────────────────────────────────────────────────
def gate4_trigger(indices: dict, chain: dict, stocks: list) -> dict:
    nifty     = indices.get("nifty", 0)
    nifty_chg = indices.get("nifty_chg", 0)
    nifty_vol = indices.get("nifty_vol", 0)  # from KiteTicker
    atr       = calc_atr("NIFTY")

    # Volume: compare current vol vs typical (use ATR-normalised move as proxy)
    mom_pct  = abs(nifty_chg)
    atr_pct  = (atr / nifty * 100) if nifty else 0.3
    vol_mult = mom_pct / atr_pct if atr_pct > 0 else 1.0
    vol_lbl  = f"{vol_mult:.1f}× ATR-normalised"
    vol_col  = "cg" if vol_mult >= TH["vol_surge_min"] else "ca" if vol_mult >= 1.0 else "cm"

    # OI build from chain
    if chain:
        oi_chg = sum(
            abs(s.get("call_oi_chg", 0)) + abs(s.get("put_oi_chg", 0))
            for s in chain.get("strikes", [])
        )
        oi_lbl = f"+{oi_chg:,} contracts"
        oi_col = "cg" if oi_chg >= TH["oi_build_min"] else "ca"
    else:
        oi_chg, oi_lbl, oi_col = 0, "Awaiting chain data", "cm"

    # ATR
    atr_lbl = f"{atr:.0f} pts ({atr_pct:.2f}% of Nifty)"
    atr_col = "ca" if atr_pct > 0.5 else "cg"

    # Momentum
    if abs(nifty_chg) >= 0.5:
        mom_lbl = f"{'+' if nifty_chg > 0 else ''}{nifty_chg:.2f}% STRONG"
        mom_col = "cg" if nifty_chg > 0 else "cr"
    else:
        mom_lbl, mom_col = f"{nifty_chg:.2f}% — dull", "cm"

    score = 50
    if vol_mult >= TH["vol_surge_min"]: score += 25
    elif vol_mult >= 0.7:               score += 10
    if oi_chg >= TH["oi_build_min"]:    score += 25
    elif oi_chg >= TH["oi_build_min"] / 2: score += 10
    if abs(nifty_chg) >= 0.3:           score += 15
    elif abs(nifty_chg) >= 0.1:         score += 5
    score = max(0, min(100, score))

    # More lenient: pass with lower score
    st = "go" if score >= 55 else "wt" if score >= 35 else "am"

    rows = [
        {"k": "ATR (14-bar)", "v": atr_lbl,  "c": atr_col},
        {"k": "Volume signal","v": vol_lbl,  "c": vol_col},
        {"k": "OI build",     "v": oi_lbl,   "c": oi_col},
        {"k": "Momentum",     "v": mom_lbl,  "c": mom_col},
        {"k": "Trigger",      "v": "FIRED ✓" if st == "go" else
              "BUILDING — watch" if st == "wt" else "WEAK — no entry",
         "c": "cg" if st == "go" else "ca" if st == "wt" else "cm"},
    ]
    return {"name": "TRIGGER", "state": st, "score": score, "rows": rows}


# ─── GATE 5: RISK VALID ───────────────────────────────────────────────────────
def gate5_risk(indices: dict, chain: dict, mode: str) -> dict:
    nifty = indices.get("nifty", 0)
    vix   = indices.get("vix", 15)
    atr   = calc_atr("NIFTY")

    # VIX size rule
    if vix < TH["vix_low"]:
        size_lbl, size_col, mult = "FULL 100% ✓", "cg", 1.0
    elif vix < TH["vix_medium"]:
        size_lbl, size_col, mult = "75% size", "ca", 0.75
    elif vix < TH["vix_high"]:
        size_lbl, size_col, mult = "50% size", "ca", 0.50
    else:
        size_lbl, size_col, mult = "25% / NO TRADE", "cr", 0.25

    stop_pts   = round(atr * TH["atr_multiplier"])
    stop_price = round(nifty - stop_pts)

    # Find nearest CE wall (target) from chain
    target = round(nifty + stop_pts * 2.5)  # default 1:2.5
    if chain:
        ce_walls = sorted(
            [(s["strike"], s["call_oi"]) for s in chain.get("strikes", [])
             if s["strike"] > nifty],
            key=lambda x: -x[1]
        )
        if ce_walls:
            target = ce_walls[0][0]

    rr      = (target - nifty) / stop_pts if stop_pts else 0
    rr_min  = TH["rr_min_intraday"] if mode == "intraday" else TH["rr_min_positional"]
    rr_lbl  = f"1:{rr:.1f} (need ≥ 1:{rr_min:.0f})"
    rr_col  = "cg" if rr >= rr_min else "ca" if rr >= rr_min * 0.8 else "cr"

    score = 60
    if rr >= rr_min:              score += 30
    elif rr >= rr_min * 0.8:      score += 10
    else:                         score -= 30
    if mult < 0.5:                score -= 20
    score = max(0, min(100, score))

    st = "go" if (rr >= rr_min and mult >= 0.5) else \
         "st" if (rr < 1.5 or mult <= 0.25) else "am"

    # Calculate position sizing
    position_size_lots = 0
    position_size_rupees = 0
    if st == "go" and rr > 0 and mult > 0:
        from config import ACCOUNT_VALUE, RISK_PER_TRADE
        monetary_risk = ACCOUNT_VALUE * RISK_PER_TRADE  # configurable, default 1% of capital
        lot_size      = LOT_SIZES.get("NIFTY", 25)
        risk_per_lot  = stop_pts * lot_size
        if risk_per_lot > 0:
            raw_lots           = monetary_risk / (risk_per_lot * mult)
            position_size_lots = max(1, int(round(raw_lots)))
            position_size_rupees = position_size_lots * lot_size * nifty

    from config import ACCOUNT_VALUE, RISK_PER_TRADE
    rows = [
        {"k": "R:R ratio",      "v": rr_lbl,  "c": rr_col},
        {"k": "Target (CE wall)","v": f"{target:,} (+{target-nifty:.0f} pts)","c": "cg"},
        {"k": "Stop distance",  "v": f"{stop_pts} pts → SL {stop_price:,}", "c": "ca"},
        {"k": "VIX sizing",     "v": size_lbl, "c": size_col},
        {"k": "Account / Risk", "v": f"₹{ACCOUNT_VALUE:,} @ {RISK_PER_TRADE*100:.1f}% = ₹{ACCOUNT_VALUE*RISK_PER_TRADE:,.0f} risk/trade",
         "c": "cb"},
        {"k": "Position Size",  "v": f"{position_size_lots} lot{'s' if position_size_lots > 1 else ''} (₹{position_size_rupees:,})",
         "c": "cg" if position_size_lots > 0 else "st"},
        {"k": "Risk verdict",   "v": "VALID ✓" if st == "go" else
               "FAIL — R:R too low" if st == "st" else "MARGINAL",
         "c": "cg" if st == "go" else "cr" if st == "st" else "ca"},
    ]
    return {"name": "RISK VALID", "state": st, "score": score, "rows": rows, 
            "position_size_lots": position_size_lots, "position_size_rupees": position_size_rupees}


# ─── VERDICT ──────────────────────────────────────────────────────────────────
def compute_verdict(gates: list) -> tuple:
    states    = [g["state"] for g in gates]
    pass_cnt  = states.count("go")
    has_fail  = "st" in states

    if pass_cnt == 5:
        return "EXECUTE", "All 5 gates clear — deploy full position", pass_cnt
    elif has_fail or pass_cnt < 3:
        failed = [i + 1 for i, s in enumerate(states) if s == "st"]
        gnum = failed[0] if failed else next((i+1 for i,s in enumerate(states) if s in ("wt","am")), 1)
        return "NO TRADE", f"G{gnum} failed — stand down", pass_cnt
    else:
        waiting = [i + 1 for i, s in enumerate(states) if s in ("wt", "am")]
        lbl = {1:"regime",2:"smart money",3:"structure",4:"trigger",5:"risk"}
        gnum = waiting[0] if waiting else 1
        return "WAIT", f"G{gnum} {lbl.get(gnum,'signal')} not satisfied", pass_cnt


# ─── SPIKE DETECTOR ───────────────────────────────────────────────────────────
def detect_spikes(stocks: list, prev: dict, gates: dict | None = None, verdict: str = "WAIT") -> list:
    from datetime import datetime
    import backtest_data as bd
    import pytz
    now_dt   = datetime.now(pytz.timezone("Asia/Kolkata"))
    now      = now_dt.strftime("%H:%M")
    candle_min = now_dt.hour * 60 + now_dt.minute
    spikes   = []
    # Optional day-level kill switch for weak/chop regimes.
    if bool(int(TH.get("spike_regime_kill_switch", 0) or 0)) and gates:
        try:
            g1_state = str((gates.get(1) or {}).get("state", "")).lower()
            g2_state = str((gates.get(2) or {}).get("state", "")).lower()
            weak_regime = (g1_state in ("st", "wt")) or (g2_state in ("st", "wt"))
            if weak_regime:
                return []
        except Exception:
            pass

    # Spike radar primarily runs on price/vol/OI merit; apply optional
    # gate confluence floors from config to reduce bad-regime churn.
    gate_score_floor = 45

    try:
        acc_filters = bd.get_signal_accuracy_filters()
    except Exception:
        acc_filters = {"weak_symbols": set(), "weak_buckets": set()}

    if candle_min < 630:
        time_bucket = "open_915_930"
    elif candle_min < 690:
        time_bucket = "morning_930_1030"
    elif candle_min < 780:
        time_bucket = "midday_1030_1300"
    else:
        time_bucket = "late_1300_plus"

    # Weak session buckets from historical accuracy: slightly tighten floor, do not zero the radar.
    weak_buckets = acc_filters.get("weak_buckets", set())
    if weak_buckets and time_bucket in weak_buckets:
        gate_score_floor = 48

    active_only = bool(int(TH.get("spike_active_only", 0) or 0))
    active_raw = str(TH.get("spike_active_symbols", "") or "")
    active_set = {x.strip().upper() for x in active_raw.split(",") if x.strip()}

    for s in stocks:
        sym       = s.get("symbol", "")
        if active_only and active_set and str(sym or "").upper() not in active_set:
            continue
        price     = s.get("price", 0)
        chg_pct   = float(s.get("chg_pct", 0) or 0)
        oi_pct    = float(s.get("oi_chg_pct", 0) or 0)
        vol       = float(s.get("volume", 0) or 0)
        prev_vol  = float((prev.get(sym) or {}).get("volume", 0) or 0)
        if vol <= 0:
            vol = 1.0
        if prev_vol <= 0:
            prev_vol = vol
        # Backtest uses 1m vol / rolling avg. Live cumulative vol / prev tick stays ~1.0 — useless.
        # Use fetcher vol_ratio (cum vs 20d daily avg) and max with tick ramp for sudden prints.
        vol_ratio = float(s.get("vol_ratio", 0) or 0)
        vm_tick   = vol / max(prev_vol, 1.0)
        vm        = max(vol_ratio, vm_tick)
        stock_pc  = int(s.get("pc", 0) or 0)

        # WR-based floor adjustment + raw WR for score bonus
        wr_adj, sym_wr = _get_wr_adj(sym, time_bucket)
        sym_floor = gate_score_floor + wr_adj + (4 if sym in acc_filters.get("weak_symbols", set()) else 0)
        sym_floor = max(35, min(75, sym_floor))  # clamp

        sp_type = sig = trigger = ""

        # Time window: GATE spike_time_start–end (default 9:30–14:00 IST)
        t_start = TH.get("spike_time_start", 570)
        t_end   = TH.get("spike_time_end",   840)
        if not (t_start <= candle_min <= t_end):
            continue
        min_gate_pass = int(TH.get("spike_min_gate_pass", 0) or 0)
        if bool(int(TH.get("spike_adaptive_gate", 0) or 0)):
            relaxed_floor = int(TH.get("spike_min_gate_pass_relaxed", min_gate_pass) or min_gate_pass)
            vix_gate3_above = float(TH.get("spike_vix_gate3_above", 18.0))
            vix_now = float(s.get("vix", 0) or 0)
            # In calmer regimes, allow gate>=2 to keep quality opportunities.
            if vix_now > 0 and vix_now <= vix_gate3_above:
                min_gate_pass = min(min_gate_pass, relaxed_floor)
        if min_gate_pass > 0 and stock_pc < min_gate_pass:
            continue

        price_th = float(TH.get("spike_price_pct", 0.2))
        vol_th   = float(TH.get("spike_vol_mult", 1.5))
        allow_open_relax = bool(int(TH.get("spike_allow_open_relax", 0) or 0))
        # Morning: cum vol vs 20d full-session avg is still tiny → vol_ratio << vol_th.
        # If price has already moved ~0.4%+, treat volume gate as met so opens aren't blank.
        if (
            allow_open_relax
            and
            candle_min <= min(t_end, 660)
            and vol_ratio < 1.2
            and abs(chg_pct) >= 0.42
        ):
            vm = max(vm, vol_th)
        oi_th    = float(TH.get("spike_oi_pct", 12.0))

        # Session chg_pct is slower than 1m bar open→close — stricter when vol surge is only mild.
        if vm >= 3.5:
            price_min = price_th
        elif vm >= vol_th * 1.25:
            price_min = max(price_th, 0.25)
        elif vm >= vol_th:
            price_min = max(price_th, 0.22)
        else:
            price_min = 0.35

        # Detect spike type — price+vol spike takes priority, then OI-only
        if abs(chg_pct) >= price_min and vm >= vol_th:
            sp_type = "buy" if chg_pct > 0 else "sell"
            sig     = "LONG" if chg_pct > 0 else "SHORT"
            trigger = f"Price {'+' if chg_pct > 0 else ''}{chg_pct:.2f}% | Vol {vm:.1f}×"
            if abs(oi_pct) >= oi_th:
                oi_sign = "+" if oi_pct > 0 else ""
                sig    += " + OI"
                trigger = f"{trigger} | OI {oi_sign}{oi_pct:.0f}%"
        elif abs(oi_pct) >= oi_th:
            sp_type = "buy" if oi_pct > 0 else "sell"
            sig     = "OI BUILD" if oi_pct > 0 else "OI UNWIND"
            trigger = f"OI {'+' if oi_pct > 0 else ''}{oi_pct:.1f}%"

        if not sp_type:
            continue

        # Score and floor — spikes fire on merit regardless of gate state
        score = _score_spike(vm, chg_pct, sym, candle_min, sym_wr=sym_wr)
        if score < sym_floor:
            continue

        # Confirmation — align with UI (vol vs avg day + direction); avoid blocking everything on tick vol ~1×.
        oi_available = abs(oi_pct) >= 1.0
        early_ist = candle_min <= 660
        min_confirm_move = float(TH.get("spike_min_confirm_move", 0.85))
        strict_non_oi = bool(int(TH.get("spike_confirm_non_oi_requires_vol", 0) or 0))
        min_confirm_vm = float(TH.get("spike_confirm_min_vm", 2.0))
        if oi_available:
            same_dir_oi = (chg_pct > 0 and oi_pct > 0) or (chg_pct < 0 and oi_pct < 0)
            has_confirmation = (
                same_dir_oi
                or vm >= 2.5
                or vol_ratio >= vol_th
                or abs(chg_pct) >= 0.70
                or (allow_open_relax and early_ist and vm >= vol_th and abs(chg_pct) >= 0.42)
            )
        else:
            if strict_non_oi:
                has_confirmation = (
                    ((vm >= min_confirm_vm or vol_ratio >= vol_th) and abs(chg_pct) >= min_confirm_move)
                    or (allow_open_relax and early_ist and vm >= vol_th and abs(chg_pct) >= 0.42)
                )
            else:
                has_confirmation = (
                    vm >= 2.5
                    or vol_ratio >= vol_th
                    or abs(chg_pct) >= min_confirm_move
                    or (allow_open_relax and early_ist and vm >= vol_th and abs(chg_pct) >= 0.42)
                )
        if not has_confirmation:
            continue

        # Unified strength from score
        strength = "hi" if score >= 70 else "md"

        # ATR-based levels for this stock
        lvl = _compute_spike_levels(sym, price, sp_type)

        spike_dict = {
            "symbol":       sym,
            "time":         now,
            "price":        price,
            "chg_pct":      chg_pct,
            "vol_mult":     round(vm, 1),
            "oi_pct":       oi_pct,
            "type":         sp_type,
            "trigger":      trigger,
            "signal":       sig,
            "strength":     strength,
            "score":        score,
            "pc":           stock_pc,
            "sym_wr":       round(sym_wr, 3) if sym_wr >= 0 else None,
            # ATR-based levels — overrides the fixed ±0.5% frontend fallback
            "entry":        lvl["entry"],
            "sl":           lvl["sl"],
            "t1":           lvl["t1"],
            "t2":           lvl["t2"],
            "atr":          lvl["atr"],
            "atr_pct":      lvl["atr_pct"],
            "rr":           lvl["rr"],
        }
        spikes.append(spike_dict)

        # Send alert for high-quality spikes
        if score >= 60:
            _send_spike_alert(spike_dict)

    # Sort by score descending, then by abs chg_pct
    spikes.sort(key=lambda x: (-x.get("score", 0), -abs(x.get("chg_pct", 0))))
    max_per_cycle = int(TH.get("spike_max_per_cycle", 15) or 15)
    max_per_cycle = max(1, min(30, max_per_cycle))
    return spikes[:max_per_cycle]


def build_today_spikes_from_kite_history(kite) -> list:
    """
    Rebuild today's spike list from 1-min OHLCV (same rules as server startup backfill).
    Populates the SPIKE tray when the server restarts after hours or after a DB reset.
    """
    if not kite:
        return []
    import statistics as _stat
    from feed import KITE_TOKENS
    from config import FNO_SYMBOLS

    today_str = datetime.now(_IST).date().isoformat()
    backfill = []
    _t_start = TH.get("spike_time_start", 570)
    _t_end = TH.get("spike_time_end", 840)
    _score_floor = 45
    _vol_th = 2.5
    _dedup_window = 20
    _last_sig_min = {}

    for sym in FNO_SYMBOLS[2:]:
        tok = KITE_TOKENS.get(sym)
        if not tok:
            continue
        try:
            candles = kite.historical_data(tok, today_str, today_str, "minute")
            if len(candles) < 10:
                continue
            vols = [c["volume"] for c in candles if c["volume"] > 0]
            if not vols:
                continue
            avg_vol = _stat.mean(vols)
            open_px = candles[0]["open"]
            for i, c in enumerate(candles):
                t = c["date"]
                cm = t.hour * 60 + t.minute
                if not (_t_start <= cm <= _t_end):
                    continue
                price = c["close"]
                vol = c["volume"] or 0
                vm = vol / avg_vol if avg_vol else 0
                chg_pct = (price - open_px) / open_px * 100 if open_px else 0
                price_min = 0.5 if vm < 4.0 else 0.2
                if abs(chg_pct) < price_min or vm < _vol_th:
                    continue
                if vm < 4.0 and abs(chg_pct) < 1.0:
                    continue
                score = _score_spike(vm, chg_pct, sym, cm)
                if score < _score_floor:
                    continue
                sp_type = "buy" if chg_pct > 0 else "sell"
                key = (sym, sp_type)
                if key in _last_sig_min and cm - _last_sig_min[key] < _dedup_window:
                    continue
                _last_sig_min[key] = cm
                sig = "LONG" if chg_pct > 0 else "SHORT"
                trigger = f"Price {'+' if chg_pct > 0 else ''}{chg_pct:.2f}% | Vol {vm:.1f}x"
                entry = price
                t1_px = entry * 1.005 if sp_type == "buy" else entry * 0.995
                sl_px = entry * 0.995 if sp_type == "buy" else entry * 1.005
                outcome = None
                for j in range(i + 1, min(i + 31, len(candles))):
                    fc = candles[j]
                    if sp_type == "buy":
                        if fc["low"] <= sl_px:
                            outcome = "HIT SL"
                            break
                        if fc["high"] >= t1_px:
                            outcome = "HIT T1"
                            break
                    else:
                        if fc["high"] >= sl_px:
                            outcome = "HIT SL"
                            break
                        if fc["low"] <= t1_px:
                            outcome = "HIT T1"
                            break
                if outcome is None:
                    outcome = "EXPIRED"
                backfill.append({
                    "symbol": sym,
                    "time": t.strftime("%H:%M"),
                    "price": round(price, 2),
                    "chg_pct": round(chg_pct, 2),
                    "vol_mult": round(vm, 1),
                    "oi_pct": 0.0,
                    "type": sp_type,
                    "trigger": trigger,
                    "signal": sig,
                    "strength": "hi" if score >= 70 else "md",
                    "score": score,
                    "pc": 3,
                    "outcome": outcome,
                })
        except Exception:
            continue

    if backfill:
        backfill.sort(key=lambda x: -x["score"])
        backfill = backfill[:30]
    return backfill


def _annotate_live_stocks(stocks: list, gates_dict: dict, verdict: str) -> list:
    """Attach live gate context and backend-owned scores to stock rows."""
    now_hm = datetime.now(_IST).strftime("%H:%M")
    g1 = (gates_dict.get(1) or {}).get("state", "wt")
    g2 = (gates_dict.get(2) or {}).get("state", "wt")
    for s in stocks:
        chg = float(s.get("chg_pct", 0) or 0)
        oi_pct = float(s.get("oi_chg_pct", 0) or 0)
        vol_r = float(s.get("vol_ratio", 0) or 0)
        atr_pct = float(s.get("atr_pct", 0) or 0)
        g3 = "go" if abs(chg) >= 0.8 and abs(oi_pct) >= 4 else "am" if abs(chg) >= 0.35 else "st"
        g4 = "go" if vol_r >= 1.5 and abs(chg) >= 0.6 else "wt" if vol_r >= 1.1 or abs(chg) >= 0.4 else "st"
        g5 = "go" if atr_pct > 0 and abs(chg) >= max(0.6, atr_pct * 0.35) else "am" if abs(chg) >= 0.35 else "st"
        pc = [g1, g2, g3, g4, g5].count("go")
        base_score = float(s.get("score", 40) or 40)
        score = int(min(99, max(base_score, 35 + pc * 10 + (8 if vol_r >= 1.5 else 0))))
        s.update({
            "g1": g1,
            "g2": g2,
            "g3": g3,
            "g4": g4,
            "g5": g5,
            "pc": pc,
            "score": score,
            "signal_time": now_hm if pc >= 3 else "",
            "verdict": "EXECUTE" if pc >= 3 and verdict != "NO TRADE" else "WATCH" if pc >= 2 else "WAIT",
        })
    return stocks


# ─── MASTER RUN ───────────────────────────────────────────────────────────────
def run_signal_engine(indices: dict, chain: dict, fii: dict,
                      stocks: list, mode: str = "intraday"):
    if not indices or not indices.get("nifty"):
        logger.warning("Signal engine: no index data yet")
        return

    g1 = gate1_regime(indices, fii)
    g2 = gate2_smart_money(chain)
    g3 = gate3_structure(indices)
    g4 = gate4_trigger(indices, chain, stocks)
    g5 = gate5_risk(indices, chain, mode)
    gates = [g1, g2, g3, g4, g5]

    verdict, sub, pass_cnt = compute_verdict(gates)

    gates_dict = {i + 1: g for i, g in enumerate(gates)}
    prev_stocks_list = state.get("last_stocks", []) or []
    prev_verdict_by_sym = {
        str(s.get("symbol", "")): str(s.get("verdict", ""))
        for s in prev_stocks_list
    }
    stocks = _annotate_live_stocks(stocks, gates_dict, verdict)
    prev   = {s["symbol"]: s for s in prev_stocks_list}
    new_spikes = detect_spikes(stocks, prev, gates_dict, verdict)

    # Preserve today's spikes across the session — don't wipe them when the
    # time window closes.  Merge new detections into the running list; clear
    # only when the calendar date rolls over (new trading day).
    import datetime as _dt
    today_str = _dt.date.today().isoformat()
    prev_spikes = state.get("spikes", [])
    prev_date   = state.get("spikes_date", today_str)

    if prev_date != today_str:
        # New trading day — start fresh
        merged_spikes = new_spikes
    else:
        # Same day — merge with 20-min deduplication per symbol+direction.
        # Prevents same stock re-triggering on the same sustained move.
        def _to_min(t):
            """Convert HH:MM string to minutes-from-midnight."""
            try:
                h, m = t.split(":")
                return int(h) * 60 + int(m)
            except Exception:
                return 0

        dedup_window = 20  # minutes — same symbol+direction won't fire again within this window
        existing_keys = {(s["symbol"], s["time"], s["type"]) for s in prev_spikes}
        to_add = []
        for ns in new_spikes:
            if (ns["symbol"], ns["time"], ns["type"]) in existing_keys:
                continue
            ns_min = _to_min(ns["time"])
            ns_dir = ns.get("type", "")
            # Check if same symbol+direction fired within dedup_window
            recent = any(
                ps.get("symbol") == ns["symbol"]
                and ps.get("type") == ns_dir
                and abs(_to_min(ps["time"]) - ns_min) < dedup_window
                for ps in prev_spikes
            )
            if not recent:
                to_add.append(ns)
        merged_spikes = prev_spikes + to_add
        # Keep latest 30, sorted by score
        merged_spikes.sort(key=lambda x: -x.get("score", 0))
        merged_spikes = merged_spikes[:30]

    spikes = merged_spikes
    state["spikes_date"] = today_str

    # Per-stock EXECUTE → Telegram (edge detect, cooldown per symbol)
    global _stock_exec_alert_ts
    now_ts = time.time()
    cd = STOCK_EXECUTE_TELEGRAM_COOLDOWN_SEC
    for s in stocks:
        sym = str(s.get("symbol", "")).strip()
        if not sym or sym in ("NIFTY", "BANKNIFTY", "INDIAVIX"):
            continue
        if s.get("verdict") != "EXECUTE":
            continue
        if prev_verdict_by_sym.get(sym) == "EXECUTE":
            continue
        last_t = _stock_exec_alert_ts.get(sym, 0)
        if now_ts - last_t < cd:
            continue
        _stock_exec_alert_ts[sym] = now_ts
        px = float(s.get("price", 0) or s.get("ltp", 0) or 0)
        chg = float(s.get("chg_pct", 0) or 0)
        pc_s = int(s.get("pc", 0) or 0)
        from config import TELEGRAM_NOTIFY_SIGNAL_ENGINE

        if not TELEGRAM_NOTIFY_SIGNAL_ENGINE:
            continue
        msg = (
            f"🎯 <b>STOCK EXECUTE — {sym}</b>\n"
            f"₹{px:.2f} ({chg:+.2f}%) · {pc_s}/5 on name · Global: {verdict}\n"
            f"<i>Confirm liquidity and your rules before entry.</i>"
        )
        _send_telegram(msg)

    # Build intel ticker
    ticker = []
    vix    = indices.get("vix", 0)
    fii_net= fii.get("fii_net", 0) if fii else 0
    pcr    = chain.get("pcr", 0) if chain else 0

    if pass_cnt == 5:
        ticker.append("ALL 5 GATES PASS — <em>EXECUTE signal active</em>")
    elif verdict == "NO TRADE":
        ticker.append("Gate FAIL — <em>NO TRADE — stand down now</em>")
    else:
        ticker.append(f"System WAIT — {pass_cnt}/5 gates — watching trigger")

    if pcr:
        ticker.append(f"PCR {pcr:.2f} — {'<em>bullish OI</em>' if pcr > 1.2 else 'bearish OI' if pcr < 0.8 else 'neutral OI'}")
    if vix:
        ticker.append(f"VIX {vix:.1f} — {'<em>low — full size</em>' if vix < 12 else 'elevated — reduce size' if vix > 16 else 'moderate regime'}")
    if fii_net:
        ticker.append(f"FII {'<em>net buyer</em>' if fii_net > 0 else 'net seller'} ₹{abs(fii_net):.0f} Cr today")
    for sp in spikes[:3]:
        if sp["strength"] == "hi":
            ticker.append(f"{sp['symbol']} SPIKE — <em>{sp['signal']}</em> — {sp['trigger']}")

    # ── Confidence score (0-10) using gate weights from backtest analysis ──
    try:
        import gate_weights as gw
        confidence = gw.compute_confidence(gates_dict)
    except Exception:
        confidence = 0.0

    # Extract position sizing data from RISK VALID gate (gate 5)
    position_size_lots = 0
    position_size_rupees = 0
    if 5 in gates_dict:
        position_size_lots = gates_dict[5].get("position_size_lots", 0)
        position_size_rupees = gates_dict[5].get("position_size_rupees", 0)

    state.update({
        "gates":       gates_dict,
        "verdict":     verdict,
        "verdict_sub": sub,
        "pass_count":  pass_cnt,
        "confidence":  confidence,
        "spikes":      spikes,
        "ticker":      ticker,
        "last_chain":  chain,
        "last_macro":  indices,
        "last_stocks": stocks,
        "last_fii":    fii,
        "last_updated":time.time(),
        "position_size_lots": position_size_lots,
        "position_size_rupees": position_size_rupees,
    })

    # ── Telegram alert on verdict change (optional — off when only ADV-SPIKES / ADV INDEX TG) ──
    global _last_telegram_verdict
    from config import TELEGRAM_NOTIFY_SIGNAL_ENGINE

    if verdict != _last_telegram_verdict:
        prev_verdict = _last_telegram_verdict
        _last_telegram_verdict = verdict
        if TELEGRAM_NOTIFY_SIGNAL_ENGINE and verdict == "EXECUTE":
            nifty = indices.get("nifty", 0)
            # Pull entry zone from gate5 rows if available
            g5_rows = gates_dict.get(5, {}).get("rows", [])
            entry_line = ""
            for r in g5_rows:
                if r.get("k") == "Target (CE wall)":
                    entry_line += f"Target: {r['v']}  |  "
                elif r.get("k") == "Stop distance":
                    entry_line += f"SL: {r['v']}"
            pos_lots = gates_dict.get(5, {}).get("position_size_lots", 0)
            pos_rs   = gates_dict.get(5, {}).get("position_size_rupees", 0)
            msg = (
                f"🟢 <b>NSE EDGE — EXECUTE SIGNAL</b>\n"
                f"Nifty: <b>{nifty:.0f}</b>  |  VIX: {vix:.1f}  |  PCR: {pcr:.2f}\n"
                f"Gates: {pass_cnt}/5 PASS  |  Confidence: {confidence}/10\n"
                f"FII: ₹{fii_net:.0f} Cr\n"
            )
            if entry_line:
                msg += f"{entry_line}\n"
            if pos_lots:
                msg += f"Size: {pos_lots} lot{'s' if pos_lots != 1 else ''} (₹{pos_rs:,})\n"
            msg += "<b>All gates clear — trade now</b>"
            _send_telegram(msg)
        elif TELEGRAM_NOTIFY_SIGNAL_ENGINE and verdict == "NO TRADE" and prev_verdict == "EXECUTE":
            _send_telegram("🔴 <b>NSE EDGE — EXECUTE cancelled</b>\nGate failed — stand down.")

    # ── Log to DB for backtest analysis (non-blocking) ──
    try:
        import backtest_data as bd
        bd.log_signal(gates_dict, verdict, pass_cnt, indices, chain, fii)
        bd.log_live_spikes(spikes, gates_dict, verdict, pass_cnt, indices, chain)
    except Exception:
        pass

    logger.info(f"Verdict: {verdict} ({pass_cnt}/5) | VIX={vix:.1f} | PCR={pcr:.2f} | Conf={confidence}")
