"""
NSE EDGE v5 — Scheduler (Kite Connect only)
All jobs call Kite APIs or read from the Kite price cache.
"""

import logging
import time
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR
from config import is_market_open
from fetcher import IST

logger = logging.getLogger("scheduler")

_kite    = None   # KiteConnect instance
_fetcher = None
_signals = None
_ws      = None
_cache   = {"indices": None, "chain": None, "fii": None,
            "stocks": [], "mode": "intraday",
            "ix_px_hist": []}   # [(ts, nifty_px, bn_px), ...] for index spike detection

# ─── CIRCUIT BREAKER (prevent cascading failures) ──────────────────────────────
_job_errors = {}  # Track consecutive errors per job
MAX_CONSECUTIVE_ERRORS = 10


def set_dependencies(kite_instance, fetcher_mod, signals_mod, broadcast_fn):
    global _kite, _fetcher, _signals, _ws
    _kite    = kite_instance
    _fetcher = fetcher_mod
    _signals = signals_mod
    _ws      = broadcast_fn


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
            new_ix = _detect_index_signals(chain, indices)
            today_str = datetime.now(IST).strftime("%Y-%m-%d")
            existing_ix = _signals.state.get("index_signals", [])
            # Roll over at new day
            if _signals.state.get("index_signals_date") != today_str:
                existing_ix = new_ix
            else:
                _update_index_outcomes(existing_ix, indices)
                # Dedup: skip same symbol+type within 15 min
                def _ixmin(t):
                    try: h,m=t.split(":"); return int(h)*60+int(m)
                    except: return 0
                for ns in new_ix:
                    nm = _ixmin(ns["time"])
                    already = any(
                        ps["symbol"] == ns["symbol"] and ps["type"] == ns["type"]
                        and abs(_ixmin(ps["time"]) - nm) < 15
                        for ps in existing_ix
                    )
                    if not already:
                        existing_ix.append(ns)
                existing_ix.sort(key=lambda x: x.get("ts", 0), reverse=True)
                existing_ix = existing_ix[:25]
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

        _record_success("job_chain")
    except Exception as e:
        logger.error(f"job_chain: {e}", exc_info=True)
        _record_error("job_chain")


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
def _detect_index_signals(chain, indices):
    """Detect NIFTY/BANKNIFTY directional moves ≥0.3% over 5 min and recommend CE/PE."""
    import time as _t
    now_ts = _t.time()
    now_dt = datetime.now(IST)
    cm = now_dt.hour * 60 + now_dt.minute
    if not (570 <= cm <= 840):
        return []

    nifty_px = float(indices.get("nifty", 0) or 0)
    bn_px    = float(indices.get("banknifty", 0) or 0)
    if not nifty_px:
        return []

    # Store in rolling price history
    hist = _cache["ix_px_hist"]
    hist.append((now_ts, nifty_px, bn_px))
    if len(hist) > 120:
        hist.pop(0)
    if len(hist) < 3:
        return []

    results = []
    for sym, px, lot_sz, px_idx in [("NIFTY", nifty_px, 25, 1), ("BANKNIFTY", bn_px, 15, 2)]:
        if not px:
            continue
        # Price 3 minutes ago (shorter window = more responsive)
        three_ago = now_ts - 180
        old_entry = next((e for e in reversed(hist) if e[0] <= three_ago), None)
        if not old_entry:
            old_entry = hist[0]
        old_px = old_entry[px_idx]
        if not old_px:
            continue
        chg = (px - old_px) / old_px * 100
        if abs(chg) < 0.15:
            continue
        is_ce = chg > 0

        # Find ATM and real option premium from chain (NIFTY only; BN estimated)
        if sym == "NIFTY" and chain:
            atm = chain.get("atm", 0) or round(px / 50) * 50
            step = 50
            target = atm + step if is_ce else atm - step
            entry_px = 0.0
            for s in (chain.get("strikes") or []):
                if s["strike"] == target:
                    entry_px = float(s.get("call_ltp" if is_ce else "put_ltp", 0) or 0)
                    break
            if not entry_px:
                # fallback: ATM itself
                for s in (chain.get("strikes") or []):
                    if s.get("is_atm"):
                        entry_px = float(s.get("call_ltp" if is_ce else "put_ltp", 0) or 0)
                        break
        else:
            atm    = round(px / 100) * 100
            step   = 100
            target = atm + step if is_ce else atm - step
            entry_px = round(px * 0.007, 1)   # ~0.7% estimate for BN OTM

        if not entry_px:
            continue

        sl         = round(entry_px * 0.70, 2)   # −30%
        t1         = round(entry_px * 1.50, 2)   # +50%
        t2         = round(entry_px * 2.00, 2)   # +100%
        rr         = round((t1 - entry_px) / max(entry_px - sl, 0.01), 1)
        lot_pnl_t1 = round((t1 - entry_px) * lot_sz)

        results.append({
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
            "strength":      "hi" if abs(chg) >= 0.4 else "md",
            "outcome":       None,
            "vix":           float(indices.get("vix", 0) or 0),
        })
    return results


def _update_index_outcomes(signals, indices):
    """Resolve live index signals: HIT_T1 / HIT_SL / EXPIRED."""
    import time as _t
    now_ts  = _t.time()
    nifty   = float((indices or {}).get("nifty", 0) or 0)
    bn      = float((indices or {}).get("banknifty", 0) or 0)
    for sig in signals:
        if sig.get("outcome") is not None:
            continue
        entry_idx = sig.get("entry_index_px", 0)
        cur       = nifty if sig["symbol"] == "NIFTY" else bn
        if not entry_idx or not cur:
            continue
        mv    = (cur - entry_idx) / entry_idx * 100
        is_ce = sig["type"] == "CE"
        if   (is_ce and mv >= 0.25) or (not is_ce and mv <= -0.25): sig["outcome"] = "HIT_T1"
        elif (is_ce and mv <= -0.25) or (not is_ce and mv >= 0.25): sig["outcome"] = "HIT_SL"
        elif now_ts - sig.get("ts", now_ts) > 2700:                  sig["outcome"] = "EXPIRED"


# ─── JOB: SPIKES + TICKER (every 10 seconds) ─────────────────────────────────
def job_spikes():
    try:
        if _signals and _ws:
            _ws({"type": "spikes", "data": _signals.state.get("spikes", []), "ts": time.time()})
            _ws({"type": "ticker", "data": _signals.state.get("ticker", []), "ts": time.time()})
    except Exception as e:
        logger.warning(f"job_spikes: {e}")


# ─── JOB: DAILY KITE TOKEN REFRESH (7:55 AM IST) ────────────────────────────
def job_token_refresh():
    """
    Refresh Kite access token every morning at 7:55 AM IST via Playwright
    headless login. Reloads the new token into the live KiteConnect instance
    so no restart is needed.
    """
    logger.info("=== Daily token refresh starting (7:55 AM IST) ===")
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


# ─── BUILD SCHEDULER ─────────────────────────────────────────────────────────
def build_scheduler() -> BackgroundScheduler:
    sched = BackgroundScheduler(
        timezone="Asia/Kolkata",
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 5},
    )
    sched.add_job(job_prices,        "interval", seconds=1,   id="prices")
    sched.add_job(job_chain,         "interval", seconds=30,  id="chain")
    sched.add_job(job_stocks,        "interval", seconds=30,  id="stocks")
    sched.add_job(job_fii,           "interval", seconds=300, id="fii")
    sched.add_job(job_spikes,        "interval", seconds=10,  id="spikes")
    # Daily token refresh at 7:55 AM IST — runs before market open (9:15 AM)
    sched.add_job(job_token_refresh, "cron", hour=7, minute=55, id="token_refresh")

    def on_err(ev):
        logger.error(f"Scheduler job {ev.job_id} failed: {ev.exception}")
    sched.add_listener(on_err, EVENT_JOB_ERROR)
    return sched
