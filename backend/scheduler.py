"""
NSE EDGE v5 — Scheduler (Kite Connect only)
All jobs call Kite APIs or read from the Kite price cache.
"""

import logging
import os
import sqlite3
import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from config import is_market_open, INDEX_RADAR as _IXR
from fetcher import IST

_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "backtest.db")


def _ix_parse_expiry_date(expiry_text: str):
    """Best-effort parse for chain expiry labels like '13 Apr 2026'."""
    if not expiry_text:
        return None
    s = str(expiry_text).strip()
    for fmt in ("%d %b %Y", "%d-%b-%Y", "%d %B %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    return None


def _ix_expiry_week_label(expiry_text: str) -> str:
    """Return W1/W2/W3/W4 or M for monthly-like farther expiry."""
    dt = _ix_parse_expiry_date(expiry_text)
    if not dt:
        return ""
    now = datetime.now(IST)
    d = (dt.date() - now.date()).days
    if d <= 7:
        return "W1"
    if d <= 14:
        return "W2"
    if d <= 21:
        return "W3"
    if d <= 28:
        return "W4"
    return "M"


def _ix_migrate_cols():
    try:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.execute("PRAGMA table_info(index_signal_history)")
        have = {r[1] for r in cur.fetchall()}
        if "quality" not in have:
            conn.execute("ALTER TABLE index_signal_history ADD COLUMN quality REAL")
        if "pcr" not in have:
            conn.execute("ALTER TABLE index_signal_history ADD COLUMN pcr REAL")
        if "option_expiry" not in have:
            conn.execute("ALTER TABLE index_signal_history ADD COLUMN option_expiry TEXT")
        if "option_week" not in have:
            conn.execute("ALTER TABLE index_signal_history ADD COLUMN option_week TEXT")
        # Cleanup historical duplicate rows created before strict upserts:
        # keep latest id for the same business signal identity.
        conn.execute("""
            DELETE FROM index_signal_history
            WHERE id NOT IN (
                SELECT MAX(id) FROM index_signal_history
                GROUP BY trade_date, symbol, type, signal_time, strike, ROUND(COALESCE(entry,0), 2)
            )
        """)
        # Best-effort unique index on sig_id (older DBs may miss UNIQUE constraint).
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_ix_sig_id ON index_signal_history(sig_id)")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("ix_migrate: %s", e)


def _ix_db_init():
    """Ensure index_signal_history table exists."""
    try:
        conn = sqlite3.connect(_DB_PATH)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS index_signal_history (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                sig_id      TEXT UNIQUE,
                trade_date  TEXT,
                symbol      TEXT,
                type        TEXT,
                signal_time TEXT,
                ts          REAL,
                index_px    REAL,
                strike      INTEGER,
                entry       REAL,
                sl          REAL,
                t1          REAL,
                t2          REAL,
                rr          REAL,
                lot_sz      INTEGER,
                lot_pnl_t1  REAL,
                chg_pct     REAL,
                strength    TEXT,
                vix         REAL,
                option_expiry TEXT,
                option_week TEXT,
                outcome     TEXT,
                created_ts  REAL,
                updated_ts  REAL
            )
        """)
        conn.commit()
        conn.close()
        _ix_migrate_cols()
    except Exception as e:
        logger.warning(f"ix_db_init: {e}")

def _notify_index_radar_new(sig: dict) -> None:
    """Telegram alert for a newly appended live index radar signal (server-side, no duplicate on outcome updates)."""
    try:
        from signals import send_telegram_message
    except Exception:
        return
    try:
        sym = sig.get("symbol") or ""
        typ = sig.get("type") or ""
        strike = sig.get("strike", "")
        tm = sig.get("time", "")
        chg = float(sig.get("chg_pct") or 0)
        msg = (
            f"📡 <b>INDEX RADAR — {sym} {strike} {typ}</b>\n"
            f"<i>{tm} IST</i> · 5m move <b>{chg:+.2f}%</b>\n"
            f"Premium entry <b>₹{float(sig.get('entry') or 0):.2f}</b> · SL ₹{float(sig.get('sl') or 0):.2f} · "
            f"T1 ₹{float(sig.get('t1') or 0):.2f} · R:R {sig.get('rr', '—')}\n"
            f"VIX {float(sig.get('vix') or 0):.1f} · PCR {float(sig.get('pcr') or 0):.2f} · "
            f"Q {int(sig.get('quality') or 0)} · {sig.get('strength', 'md')}"
        )
        send_telegram_message(msg)
    except Exception as e:
        logger.debug("index radar telegram: %s", e)


def _ix_db_upsert(sig):
    """Insert or update an index signal in DB."""
    try:
        conn = sqlite3.connect(_DB_PATH)
        conn.execute("""
            INSERT INTO index_signal_history
              (sig_id, trade_date, symbol, type, signal_time, ts,
               index_px, strike, entry, sl, t1, t2, rr, lot_sz, lot_pnl_t1,
               chg_pct, strength, vix, quality, pcr, option_expiry, option_week,
               outcome, created_ts, updated_ts)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(sig_id) DO UPDATE SET
              outcome=excluded.outcome,
              option_expiry=COALESCE(excluded.option_expiry, option_expiry),
              option_week=COALESCE(excluded.option_week, option_week),
              updated_ts=excluded.updated_ts
        """, (
            sig["id"],
            datetime.now(IST).strftime("%Y-%m-%d"),
            sig["symbol"], sig["type"], sig["time"], sig["ts"],
            sig["index_px"], sig["strike"], sig["entry"],
            sig["sl"], sig["t1"], sig["t2"], sig["rr"],
            sig["lot_sz"], sig["lot_pnl_t1"],
            sig.get("chg_pct", 0), sig.get("strength", "md"),
            sig.get("vix", 0), sig.get("quality"), sig.get("pcr"),
            sig.get("option_expiry"), sig.get("option_week"),
            sig.get("outcome"),
            sig["ts"], time.time()
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"ix_db_upsert: {e}")

logger = logging.getLogger("scheduler")

_kite    = None   # KiteConnect instance
_fetcher = None
_signals = None
_ws      = None
_cache   = {"indices": None, "chain": None, "bn_chain": None, "fii": None,
            "stocks": [], "mode": "intraday",
            "ix_px_hist": [],   # [(ts, nifty_px, bn_px), ...] for index spike detection
            "prev_chain_totals": None}  # for confluence ΔOI (confluence_engine)

# ─── CIRCUIT BREAKER (prevent cascading failures) ──────────────────────────────
_job_errors = {}  # Track consecutive errors per job
MAX_CONSECUTIVE_ERRORS = 10


def set_dependencies(kite_instance, fetcher_mod, signals_mod, broadcast_fn):
    global _kite, _fetcher, _signals, _ws
    _kite    = kite_instance
    _fetcher = fetcher_mod
    _signals = signals_mod
    _ws      = broadcast_fn
    _ix_db_init()


def set_mode(mode: str):
    _cache["mode"] = mode


def set_initial_stocks(stocks: list):
    """Seed scheduler cache with initial stocks from startup fetch."""
    if stocks:
        _cache["stocks"] = stocks


def _check_circuit(job_name: str) -> bool:
    """Check if job should run (circuit breaker pattern)."""
    if job_name not in _job_errors:
        _job_errors[job_name] = 0

    if _job_errors[job_name] >= MAX_CONSECUTIVE_ERRORS:
        logger.error(f"Circuit breaker OPEN for {job_name} ({_job_errors[job_name]} failures) - job disabled")
        return False
    return True


def _record_error(job_name: str):
    """Record a job failure."""
    _job_errors[job_name] = _job_errors.get(job_name, 0) + 1
    logger.warning(f"{job_name} failed ({_job_errors[job_name]}/{MAX_CONSECUTIVE_ERRORS})")


def _record_success(job_name: str):
    """Clear error counter on success."""
    if _job_errors.get(job_name, 0) > 0:
        logger.info(f"{job_name} recovered (errors reset)")
    _job_errors[job_name] = 0


def refresh_confluence_broadcast(persist: bool = True) -> None:
    """
    Build confluence from current signal state + scheduler cache; broadcast on WS.
    Safe when market is closed (uses last_chain / last_macro from startup or last session).
    """
    if not _ws or not _signals:
        return
    try:
        import confluence_engine as _ce

        chain = _signals.state.get("last_chain") or _cache.get("chain")
        macro = _signals.state.get("last_macro") or _cache.get("indices") or {}
        if not chain or not macro:
            logger.debug("refresh_confluence_broadcast: missing chain or macro")
            return
        prev_tot = _cache.get("prev_chain_totals")
        cg = _signals.state.get("gates") or {}
        stocks = _cache.get("stocks") or _signals.state.get("last_stocks") or []
        co = _ce.compute_confluence(
            cg,
            str(_signals.state.get("verdict") or "WAIT"),
            int(_signals.state.get("pass_count") or 0),
            chain,
            macro,
            stocks,
            prev_tot,
        )
        _signals.state["confluence"] = co
        _ws({"type": "confluence", "data": co, "ts": time.time()})
        if persist:
            td = datetime.now(IST).strftime("%Y-%m-%d")
            _ce.maybe_persist_snapshot(co, td)
            _cache["prev_chain_totals"] = _ce.chain_totals_snapshot(chain)
    except Exception as e:
        logger.debug("refresh_confluence_broadcast: %s", e)


# ─── JOB: BROADCAST PRICES (every 1 second) ───────────────────────────────────
def job_prices():
    """Broadcast latest prices from Kite cache — KiteTicker keeps them fresh."""
    try:
        from feed import get_all_prices
        import backtest_data as bd
        prices = get_all_prices()
        if prices:
            bd.update_live_signal_outcomes(prices)
        if prices and _ws:
            _ws({"type": "prices", "data": prices, "ts": time.time()})
    except Exception as e:
        logger.warning(f"job_prices: {e}")


# ─── JOB: FETCH OPTION CHAIN + RUN GATES (every 30 seconds) ──────────────────
def job_chain():
    """Fetch option chain and run signal engine (market hours only)."""
    if not _check_circuit("job_chain"):
        return

    if not is_market_open():
        # Market closed - skip but don't count as error
        return

    try:
        # Option chain from Kite NFO
        chain = _fetcher.fetch_option_chain(_kite, "NIFTY")
        bn_chain = _fetcher.fetch_option_chain(_kite, "BANKNIFTY")
        if chain:
            # Validate Max Pain is not too stale
            ul_price = chain.get("ul_price", 0)
            max_pain = chain.get("max_pain", 0)

            if ul_price > 0 and max_pain > 0:
                deviation = abs(max_pain - ul_price) / ul_price * 100
                if deviation > 5:  # More than 5% away = stale/incorrect
                    logger.warning(
                        f"Max Pain deviation too high: {max_pain} vs price {ul_price} ({deviation:.1f}%) "
                        f"- data may be stale, recalculating..."
                    )
                    # Recalculate to ensure fresh data
                    chain = _fetcher.fetch_option_chain(_kite, "NIFTY")

            if chain:
                _cache["chain"] = chain
        if bn_chain:
            _cache["bn_chain"] = bn_chain

        # Indices from price cache (Kite live)
        indices = _fetcher.fetch_indices()
        if indices:
            _cache["indices"] = indices

        # Run signal engine
        if _cache["indices"]:
            _signals.run_signal_engine(
                indices = _cache["indices"],
                chain   = _cache["chain"],
                fii     = _cache["fii"],
                stocks  = _cache["stocks"],
                mode    = _cache["mode"],
            )

        # ── Index Spike Radar ──────────────────────────────────────────────
        if chain and indices:
            today_str = datetime.now(IST).strftime("%Y-%m-%d")
            existing_ix = _signals.state.get("index_signals", [])
            existing_date = _signals.state.get("index_signals_date", "")
            # New session: drop rolling price history so 5m momentum is intraday-only
            if existing_date and existing_date != today_str:
                _cache["ix_px_hist"] = []

            new_ix = _detect_index_signals(chain, indices, _cache.get("bn_chain"))
            # Roll over ONLY when the date actually changes (not on missing date = startup)
            if existing_date and existing_date != today_str:
                existing_ix = new_ix
                for ns in new_ix:
                    _ix_db_upsert(ns)
                    _notify_index_radar_new(ns)
            else:
                _update_index_outcomes(existing_ix, indices)
                # Persist outcome updates to DB
                for sig in existing_ix:
                    if sig.get("outcome"):
                        _ix_db_upsert(sig)
                # Dedup: skip same symbol+type within configured minutes
                _ix_dedup = int(_IXR.get("dedup_minutes", 20))
                def _ixmin(t):
                    try: h,m=t.split(":"); return int(h)*60+int(m)
                    except: return 0
                for ns in new_ix:
                    nm = _ixmin(ns["time"])
                    already = any(
                        ps["symbol"] == ns["symbol"] and ps["type"] == ns["type"]
                        and abs(_ixmin(ps["time"]) - nm) < _ix_dedup
                        for ps in existing_ix
                    )
                    if not already:
                        existing_ix.append(ns)
                        _ix_db_upsert(ns)
                        _notify_index_radar_new(ns)
                existing_ix.sort(key=lambda x: x.get("ts", 0), reverse=True)
                existing_ix = existing_ix[:25]
            _ix_attach_option_ltps(existing_ix, chain, _cache.get("bn_chain"))
            _signals.state["index_signals"]      = existing_ix
            _signals.state["index_signals_date"] = today_str

        if _ws:
            _ws({"type": "gates", "data": {
                "gates":       {str(k): v for k, v in _signals.state["gates"].items()},
                "verdict":     _signals.state["verdict"],
                "verdict_sub": _signals.state["verdict_sub"],
                "pass_count":  _signals.state["pass_count"],
            }, "timestamp": time.time()})
            if chain:
                _ws({"type": "chain",  "data": chain,              "timestamp": time.time()})
            if indices:
                _ws({"type": "macro",  "data": indices,            "timestamp": time.time()})
            ix_sigs = _signals.state.get("index_signals", [])
            _ws({"type": "index_spikes", "data": ix_sigs, "ts": time.time()})

            # Confluence (same helper as off-hours job)
            refresh_confluence_broadcast(persist=True)

        _record_success("job_chain")
    except Exception as e:
        logger.error(f"job_chain: {e}", exc_info=True)
        _record_error("job_chain")


def job_confluence_idle():
    """Market closed: still emit confluence from cached chain/macro so the tab + DB history work."""
    if is_market_open():
        return
    refresh_confluence_broadcast(persist=True)


# ─── JOB: F&O STOCKS (every 30 seconds) ──────────────────────────────────────
def job_stocks():
    """Fetch F&O stock OI."""
    if not _check_circuit("job_stocks"):
        return

    try:
        stocks = _fetcher.fetch_fno_stocks(_kite)
        if stocks:
            gates = _signals.state.get("gates", {}) or {}
            pass_count = int(_signals.state.get("pass_count", 0) or 0)
            verdict = _signals.state.get("verdict", "WAIT")
            now_hm = datetime.now(IST).strftime("%H:%M")
            g1 = (gates.get(1) or gates.get("1") or {}).get("state", "wt")
            g2 = (gates.get(2) or gates.get("2") or {}).get("state", "wt")
            for s in stocks:
                chg = float(s.get("chg_pct", 0) or 0)
                oi_pct = float(s.get("oi_chg_pct", 0) or 0)
                vol_r = float(s.get("vol_ratio", 0) or 0)
                atr_pct = float(s.get("atr_pct", 0) or 0)
                g3 = "go" if abs(chg) >= 0.8 and abs(oi_pct) >= 4 else "am" if abs(chg) >= 0.35 else "st"
                g4 = "go" if vol_r >= 1.5 and abs(chg) >= 0.6 else "wt" if vol_r >= 1.1 or abs(chg) >= 0.4 else "st"
                g5 = "go" if atr_pct > 0 and abs(chg) >= max(0.6, atr_pct * 0.35) else "am" if abs(chg) >= 0.35 else "st"
                stock_pc = [g1, g2, g3, g4, g5].count("go")
                stock_score = int(min(99, max(float(s.get("score", 40) or 40), 35 + stock_pc * 10 + (8 if vol_r >= 1.5 else 0))))
                s.update({
                    "g1": g1,
                    "g2": g2,
                    "g3": g3,
                    "g4": g4,
                    "g5": g5,
                    "pc": stock_pc,
                    "score": stock_score,
                    "signal_time": now_hm if stock_pc >= 3 else "",
                    "verdict": "EXECUTE" if stock_pc >= 3 and verdict != "NO TRADE" else "WATCH" if stock_pc >= 2 else "WAIT",
                })
            _cache["stocks"] = stocks
            if _ws:
                _ws({"type": "stocks", "data": stocks, "timestamp": time.time()})
            # Persist Swing Radar snapshots (same logic family as UI carousel) for history.
            try:
                from backtest_data import log_swing_radar_triggers

                _gates = _signals.state.get("gates", {}) or {}
                _ver = str(_signals.state.get("verdict", "WAIT") or "WAIT")
                _pc = int(_signals.state.get("pass_count", 0) or 0)
                log_swing_radar_triggers(
                    stocks, _gates, _ver, _pc,
                    _cache.get("indices"), _cache.get("chain"),
                )
            except Exception as _e:
                logger.debug("swing_radar persist: %s", _e)
        _record_success("job_stocks")
    except Exception as e:
        logger.error(f"job_stocks: {e}")
        _record_error("job_stocks")


# ─── JOB: FII/DII from NSE (every 5 minutes) ─────────────────────────────────
def job_fii():
    """Fetch FII/DII data."""
    if not _check_circuit("job_fii"):
        return

    try:
        fii = _fetcher.fetch_fii_dii()
        if fii:
            _cache["fii"] = fii
            if _ws:
                _ws({"type": "fii", "data": fii, "timestamp": time.time()})
        _record_success("job_fii")
    except Exception as e:
        logger.error(f"job_fii: {e}")
        _record_error("job_fii")


# ─── INDEX SPIKE DETECTION ────────────────────────────────────────────────────
def _ix_baseline(hist, now_ts, px_idx, window_sec):
    """Newest sample at or before (now - window_sec). None if none qualifies."""
    cutoff = now_ts - window_sec
    return next((e for e in reversed(hist) if e[0] <= cutoff), None)


def _ix_recent_range(hist, now_ts, px_idx, window_sec):
    """High/low over [now-window, now) using history only (excludes live tick)."""
    lo = now_ts - window_sec
    xs = [e[px_idx] for e in hist if lo <= e[0] < now_ts and e[px_idx]]
    if len(xs) < 2:
        return None, None
    return max(xs), min(xs)


def _detect_index_signals(chain, indices, bn_chain=None):
    """
    NIFTY/BANKNIFTY momentum radar: controlled 5m impulse, confirmations, no chasing.
    History is appended AFTER checks so windows are not polluted by the current print.
    """
    import time as _t
    ir = _IXR
    now_ts = _t.time()
    now_dt = datetime.now(IST)
    cm = now_dt.hour * 60 + now_dt.minute
    t0, t1w = ir["time_start_min"], ir["time_end_min"]
    if not (t0 <= cm <= t1w):
        return []

    nifty_px = float(indices.get("nifty", 0) or 0)
    bn_px    = float(indices.get("banknifty", 0) or 0)
    if not nifty_px:
        return []

    vix = float(indices.get("vix", 0) or 0)
    if vix and vix >= float(ir.get("vix_block_above", 99)):
        return []

    nifty_day = float(indices.get("nifty_chg", 0) or 0)
    pcr = float((chain or {}).get("pcr", 1.0) or 1.0)

    hist = _cache["ix_px_hist"]
    min_span = float(ir["min_hist_span_sec"])
    min_samp = int(ir["min_hist_samples"])
    if len(hist) < min_samp or (now_ts - hist[0][0]) < min_span:
        hist.append((now_ts, nifty_px, bn_px))
        if len(hist) > 120:
            hist.pop(0)
        return []

    mom_sec   = int(ir["momentum_sec"])
    conf_sec  = int(ir["confirm_sec"])
    trend_sec = int(ir["trend_sec"])
    chg_lo    = float(ir["chg_min_pct"])
    chg_hi    = float(ir["chg_max_pct"])
    if ir.get("precision_boost"):
        chg_lo = max(chg_lo, float(ir.get("precision_chg_min", 0.23)))
        chg_hi = min(chg_hi, float(ir.get("precision_chg_max", 0.28)))
    chg_str   = float(ir["chg_hi_strength_pct"])
    tr_against = float(ir["trend_against_pct"])
    chase_w   = int(ir["anti_chase_sec"])
    chase_ce  = float(ir["anti_chase_ce_pct"])
    chase_pe  = float(ir["anti_chase_pe_pct"])
    micro_min = float(ir["micro_step_min_pct"])
    pcr_pe    = float(ir["pcr_pe_min"])
    pe_nifty  = float(ir["pe_max_nifty_chg"])
    pcr_ce_av = float(ir.get("pcr_ce_avoid_below", 0))

    results = []
    for sym, px, lot_sz, px_idx in [("NIFTY", nifty_px, 25, 1), ("BANKNIFTY", bn_px, 15, 2)]:
        if not px:
            continue

        base = _ix_baseline(hist, now_ts, px_idx, mom_sec)
        if not base or not base[px_idx]:
            continue
        old_px = base[px_idx]
        chg = (px - old_px) / old_px * 100

        if abs(chg) < chg_lo or abs(chg) > chg_hi:
            continue
        is_ce = chg > 0

        if ir.get("precision_boost") and ir.get("precision_hi_only", True):
            if abs(chg) < chg_str:
                continue

        one_entry = _ix_baseline(hist, now_ts, px_idx, conf_sec)
        if one_entry and one_entry[px_idx]:
            one_chg = (px - one_entry[px_idx]) / one_entry[px_idx] * 100
            if is_ce and one_chg <= 0:
                continue
            if not is_ce and one_chg >= 0:
                continue

        trend_entry = _ix_baseline(hist, now_ts, px_idx, trend_sec)
        trend_chg = None
        if trend_entry and trend_entry[px_idx]:
            trend_chg = (px - trend_entry[px_idx]) / trend_entry[px_idx] * 100
            if is_ce and trend_chg < -tr_against:
                continue
            if not is_ce and trend_chg > tr_against:
                continue
            tsup = float(ir.get("trend_support_min_pct", 0))
            if ir.get("precision_boost"):
                tsup = max(tsup, float(ir.get("precision_min_trend_sup", 0.10)))
            if tsup > 0:
                if is_ce and trend_chg < tsup:
                    continue
                if not is_ce and trend_chg > -tsup:
                    continue

        cap = float(ir.get("cross_index_against_pct", 0))
        if cap > 0:
            if px_idx == 1 and base[2] and bn_px:
                o_chg = (bn_px - base[2]) / base[2] * 100
                if is_ce and o_chg < -cap:
                    continue
                if not is_ce and o_chg > cap:
                    continue
            elif px_idx == 2 and base[1] and nifty_px:
                o_chg = (nifty_px - base[1]) / base[1] * 100
                if is_ce and o_chg < -cap:
                    continue
                if not is_ce and o_chg > cap:
                    continue

        if len(hist) >= 2:
            e_old, e_new = hist[-2], hist[-1]
            if e_old[px_idx] and e_new[px_idx]:
                step_pct = (e_new[px_idx] - e_old[px_idx]) / e_old[px_idx] * 100
                if is_ce and step_pct < micro_min:
                    continue
                if not is_ce and step_pct > -micro_min:
                    continue

        recent_hi, recent_lo = _ix_recent_range(hist, now_ts, px_idx, chase_w)
        if recent_hi and recent_lo:
            if is_ce and px > recent_hi * (1.0 + chase_ce / 100.0):
                continue
            if not is_ce and px < recent_lo * (1.0 - chase_pe / 100.0):
                continue

        if is_ce and pcr_ce_av > 0 and pcr < pcr_ce_av:
            continue

        pcr_ce_min = float(ir.get("pcr_ce_min", 0))
        if is_ce and pcr_ce_min > 0 and pcr < pcr_ce_min:
            continue

        vs = float(ir.get("vix_soft_skips_md_ce", 0))
        if is_ce and vs > 0 and vix and vix >= vs and abs(chg) < chg_str:
            continue

        if not is_ce:
            if pcr < pcr_pe:
                continue
            if nifty_day > pe_nifty:
                continue

        if sym == "NIFTY" and chain:
            active_chain = chain
            atm = active_chain.get("atm", 0) or round(px / 50) * 50
            step = 50
            target = atm + step if is_ce else atm - step
            entry_px = 0.0
            for s in (active_chain.get("strikes") or []):
                if s["strike"] == target:
                    entry_px = float(s.get("call_ltp" if is_ce else "put_ltp", 0) or 0)
                    break
            if not entry_px:
                for s in (active_chain.get("strikes") or []):
                    if s.get("is_atm"):
                        entry_px = float(s.get("call_ltp" if is_ce else "put_ltp", 0) or 0)
                        target = int(s.get("strike") or target)
                        break
        else:
            # BANKNIFTY: use its own option chain LTP (fallback to proxy only if chain unavailable)
            active_chain = bn_chain
            atm = (active_chain or {}).get("atm", 0) or round(px / 100) * 100
            step = 100
            target = atm + step if is_ce else atm - step
            entry_px = 0.0
            for s in ((active_chain or {}).get("strikes") or []):
                if s["strike"] == target:
                    entry_px = float(s.get("call_ltp" if is_ce else "put_ltp", 0) or 0)
                    break
            if not entry_px:
                for s in ((active_chain or {}).get("strikes") or []):
                    if s.get("is_atm"):
                        entry_px = float(s.get("call_ltp" if is_ce else "put_ltp", 0) or 0)
                        target = int(s.get("strike") or target)
                        break
            if not entry_px:
                entry_px = round(px * 0.007, 1)

        if not entry_px:
            continue

        sl         = round(entry_px * 0.70, 2)
        t1         = round(entry_px * 1.50, 2)
        t2         = round(entry_px * 2.00, 2)
        rr         = round((t1 - entry_px) / max(entry_px - sl, 0.01), 1)
        lot_pnl_t1 = round((t1 - entry_px) * lot_sz)

        strength = "hi" if abs(chg) >= chg_str else "md"
        quality = 52
        quality += min(18, int((abs(chg) - chg_lo) / max(chg_hi - chg_lo, 0.01) * 18))
        if vix and vix < 12:
            quality += 8
        if (is_ce and pcr >= 1.0) or (not is_ce and pcr >= pcr_pe):
            quality += 7
        if strength == "hi":
            quality += 10
        quality = max(40, min(99, quality))

        qfloor = int(ir.get("quality_floor", 0))
        if ir.get("precision_boost"):
            qfloor = max(qfloor, int(ir.get("precision_min_quality", 72)))
        if qfloor > 0 and quality < qfloor:
            continue

        cand = {
            "id":            f"{sym}_{now_dt.strftime('%H%M')}_{'CE' if is_ce else 'PE'}",
            "symbol":        sym,
            "time":          now_dt.strftime("%H:%M"),
            "ts":            now_ts,
            "index_px":      round(px, 2),
            "entry_index_px":round(px, 2),
            "chg_pct":       round(chg, 2),
            "type":          "CE" if is_ce else "PE",
            "strike":        target,
            "entry":         round(entry_px, 2),
            "sl":            sl,
            "t1":            t1,
            "t2":            t2,
            "rr":            rr,
            "lot_sz":        lot_sz,
            "lot_pnl_t1":    lot_pnl_t1,
            "strength":      strength,
            "quality":       quality,
            "chg_window":    "5min",
            "outcome":       None,
            "vix":           vix,
            "pcr":           round(pcr, 3),
            "option_expiry": (active_chain or {}).get("expiry"),
            "option_week":   _ix_expiry_week_label((active_chain or {}).get("expiry")),
        }

        if ir.get("ml_filter_enabled"):
            try:
                from index_radar_ml import effective_ml_threshold, win_probability

                _p = win_probability(cand)
                _thr = effective_ml_threshold(float(ir.get("ml_min_win_prob", 0.72)))
                if _p is not None and _p < _thr:
                    continue
                cand["ml_p"] = round(_p, 4) if _p is not None else None
            except Exception as _e:
                logger.debug("index radar ml: %s", _e)

        results.append(cand)

    hist.append((now_ts, nifty_px, bn_px))
    if len(hist) > 120:
        hist.pop(0)
    return results


def _ix_attach_option_ltps(signals, chain, bn_chain):
    """Set ``ltp`` on open index radar signals from the current option chain."""
    if not signals:
        return
    for sig in signals:
        if sig.get("outcome"):
            continue
        sym = str(sig.get("symbol") or "")
        try:
            strike = int(sig.get("strike") or 0)
        except (TypeError, ValueError):
            strike = 0
        is_ce = sig.get("type") == "CE"
        ch = chain if sym == "NIFTY" else bn_chain
        if ch:
            if not sig.get("option_expiry"):
                sig["option_expiry"] = ch.get("expiry")
            if not sig.get("option_week"):
                sig["option_week"] = _ix_expiry_week_label(ch.get("expiry"))
        ltp = None
        if ch and strike:
            nearest = None
            for row in ch.get("strikes") or []:
                try:
                    st = int(row.get("strike") or 0)
                except (TypeError, ValueError):
                    continue
                raw = row.get("call_ltp" if is_ce else "put_ltp", 0)
                if st == strike:
                    ltp = float(raw or 0)
                    break
                # Fallback for stale/missing strike: use nearest listed strike quote.
                d = abs(st - strike)
                if nearest is None or d < nearest[0]:
                    nearest = (d, float(raw or 0))
            if (not ltp or ltp <= 0) and nearest:
                ltp = nearest[1]
        if ltp and ltp > 0:
            sig["ltp"] = round(ltp, 2)
        else:
            sig["ltp"] = round(float(sig.get("entry") or 0), 2)


def _update_index_outcomes(signals, indices):
    """Resolve live index signals against market prices.

    Priority:
    - Option premium (ltp vs t1/sl) when available (exact to market premium)
    - Fallback to index move threshold (legacy) if ltp missing
    """
    import time as _t
    now_ts  = _t.time()
    nifty   = float((indices or {}).get("nifty", 0) or 0)
    bn      = float((indices or {}).get("banknifty", 0) or 0)
    ix_th   = float(_IXR.get("outcome_index_pct", 0.25))
    for sig in signals:
        if sig.get("outcome") is not None:
            continue
        _otm = datetime.now(IST).strftime("%H:%M")
        # Premium-based outcome (preferred)
        try:
            ltp = float(sig.get("ltp", 0) or 0)
            t1  = float(sig.get("t1", 0) or 0)
            sl  = float(sig.get("sl", 0) or 0)
        except Exception:
            ltp = t1 = sl = 0.0
        if ltp > 0 and t1 > 0 and sl > 0:
            if ltp >= t1:
                sig["outcome"] = "HIT_T1"
                sig["outcome_time"] = _otm
                continue
            if ltp <= sl:
                sig["outcome"] = "HIT_SL"
                sig["outcome_time"] = _otm
                continue

        # Fallback: index move threshold (kept for resilience)
        entry_idx = float(sig.get("entry_index_px", 0) or 0)
        cur       = nifty if sig.get("symbol") == "NIFTY" else bn
        if entry_idx and cur:
            mv    = (cur - entry_idx) / entry_idx * 100
            is_ce = sig.get("type") == "CE"
            if   (is_ce and mv >= ix_th) or (not is_ce and mv <= -ix_th):
                sig["outcome"] = "HIT_T1"
                sig["outcome_time"] = _otm
                continue
            if (is_ce and mv <= -ix_th) or (not is_ce and mv >= ix_th):
                sig["outcome"] = "HIT_SL"
                sig["outcome_time"] = _otm
                continue

        # Time expiry
        if now_ts - sig.get("ts", now_ts) > 2700:
            sig["outcome"] = "EXPIRED"
            sig["outcome_time"] = _otm


# ─── JOB: SPIKES + TICKER (every 10 seconds) ─────────────────────────────────
def job_spikes():
    try:
        if _signals and _ws:
            _ws({"type": "spikes", "data": _signals.state.get("spikes", []), "ts": time.time()})
            _ws({"type": "ticker", "data": _signals.state.get("ticker", []), "ts": time.time()})
    except Exception as e:
        logger.warning(f"job_spikes: {e}")


# ─── JOB: DAILY KITE TOKEN REFRESH (7:55 AM IST, Mon–Fri) ─────────────────────
def job_token_refresh():
    """
    Refresh Kite access token every trading morning at 7:55 AM IST (Asia/Kolkata)
    via Playwright + pyotp. Reloads the new token into the live KiteConnect
    instance so no restart is needed.

    Requires KITE_USER_ID, KITE_PASSWORD, KITE_TOTP_SECRET, KITE_API_KEY,
    KITE_API_SECRET (Railway Variables or backend/.env). The app process must
    be running at that time (use Railway “always on” or an external wake/ping
    if your plan sleeps the service).
    """
    logger.info("=== Daily token refresh starting (scheduled Mon–Fri 07:55 IST) ===")
    try:
        from auto_token import refresh_token
        ok = refresh_token()
    except Exception as e:
        logger.error(f"Token refresh exception: {e}")
        ok = False

    if not ok:
        logger.error("Daily token refresh FAILED — will retry in 5 minutes")
        # Schedule a one-off retry
        import threading, time as _t
        def _retry():
            _t.sleep(300)
            logger.info("Token refresh retry attempt...")
            try:
                from auto_token import refresh_token as _rt
                if _rt():
                    _apply_new_token()
                    logger.info("Token refresh retry SUCCESS")
                else:
                    logger.error("Token refresh retry also FAILED — manual intervention needed")
            except Exception as e2:
                logger.error(f"Token refresh retry error: {e2}")
        threading.Thread(target=_retry, daemon=True).start()
        return

    _apply_new_token()


def _apply_new_token():
    """Reload KITE_ACCESS_TOKEN from .env and push it into the live Kite instance."""
    try:
        import os
        from dotenv import load_dotenv
        _env = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
        load_dotenv(_env, override=True)
        new_token = os.getenv("KITE_ACCESS_TOKEN", "").strip()
        if not new_token:
            logger.error("_apply_new_token: token empty after reload")
            return
        import config
        config.KITE_ACCESS_TOKEN = new_token
        from feed import get_kite, _kite as _fkite
        import feed as _feed
        _feed._kite = None          # force re-init with new token
        kite = get_kite()           # creates fresh KiteConnect with new token
        # Update scheduler reference
        global _kite
        _kite = kite
        logger.info(f"=== Token applied live — last 6: ...{new_token[-6:]} ===")
    except Exception as e:
        logger.error(f"_apply_new_token error: {e}")


def job_morning_briefing():
    """9:00 IST Mon–Fri: global/India context, movement checklist, watchlist → Telegram."""
    try:
        from config import MORNING_TELEGRAM_BRIEF, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
        if not MORNING_TELEGRAM_BRIEF or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            return
        import html as html_mod

        import signals
        from market_desk import get_market_desk

        desk = get_market_desk(signals, force_refresh=True)
        brief = desk.get("today_brief") or []
        picks = desk.get("picks") or []
        leaders = desk.get("fno_leaders") or []
        ng = desk.get("news_global") or []
        err = desk.get("error")

        lines: list[str] = [
            "🌅 <b>Pre-open desk — 9:00 IST</b>",
            "<i>Context only — not a trade signal.</i>",
        ]
        if err:
            lines.append("⚠ " + html_mod.escape(str(err)))
        for line in brief[:14]:
            lines.append("· " + html_mod.escape(line))
        pick_syms = [p.get("sym") or p.get("symbol", "") for p in picks[:12]]
        pick_syms = [x for x in pick_syms if x]
        if pick_syms:
            lines.append("")
            lines.append("<b>Stocks to watch:</b> " + html_mod.escape(", ".join(pick_syms)))
        if leaders:
            parts = []
            for x in leaders[:8]:
                sym = x.get("sym") or x.get("symbol", "")
                if not sym:
                    continue
                try:
                    cp = float(x.get("chg_pct", 0) or 0)
                except (TypeError, ValueError):
                    cp = 0.0
                parts.append(f"{sym} ({cp:+.1f}%)")
            if parts:
                lines.append("<b>F&amp;O movers:</b> " + html_mod.escape(", ".join(parts)))
        if ng:
            lines.append("")
            lines.append("<b>Global headlines:</b>")
            for it in ng[:4]:
                t = (it.get("title") or "")[:160]
                lines.append("• " + html_mod.escape(t))

        signals.send_telegram_message("\n".join(lines))
        logger.info("Morning Telegram briefing sent (9:00 IST)")
    except Exception as e:
        logger.error("job_morning_briefing failed: %s", e)


# ─── BUILD SCHEDULER ─────────────────────────────────────────────────────────
def build_scheduler() -> BackgroundScheduler:
    sched = BackgroundScheduler(
        timezone="Asia/Kolkata",
        # Default: tight misfire window for intraday jobs.
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 5},
    )
    sched.add_job(job_prices,        "interval", seconds=1,   id="prices")
    sched.add_job(job_chain,         "interval", seconds=30,  id="chain")
    sched.add_job(job_stocks,        "interval", seconds=30,  id="stocks")
    sched.add_job(job_fii,           "interval", seconds=300, id="fii")
    sched.add_job(job_spikes,        "interval", seconds=10,  id="spikes")
    sched.add_job(job_confluence_idle, "interval", seconds=45, id="confluence_idle")
    # Daily token refresh Mon–Fri 7:55 IST — before cash market open (9:15).
    # Large misfire_grace_time: if the host wakes late (e.g. Railway cold start),
    # the job still runs instead of being dropped.
    sched.add_job(
        job_token_refresh,
        "cron",
        hour=7,
        minute=55,
        day_of_week="mon-fri",
        id="token_refresh",
        misfire_grace_time=28800,
        coalesce=True,
        max_instances=1,
    )
    # Pre-open briefing: global markets, checklist, key names (Telegram)
    sched.add_job(
        job_morning_briefing,
        "cron",
        hour=9,
        minute=0,
        day_of_week="mon-fri",
        id="morning_brief",
    )

    def on_err(ev):
        logger.error(f"Scheduler job {ev.job_id} failed: {ev.exception}")
    sched.add_listener(on_err, EVENT_JOB_ERROR)
    return sched
