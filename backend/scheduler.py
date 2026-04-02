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
            "stocks": [], "mode": "intraday"}

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
        prices = get_all_prices()
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


# ─── JOB: SPIKES + TICKER (every 10 seconds) ─────────────────────────────────
def job_spikes():
    try:
        if _signals and _ws:
            _ws({"type": "spikes", "data": _signals.state.get("spikes", []), "ts": time.time()})
            _ws({"type": "ticker", "data": _signals.state.get("ticker", []), "ts": time.time()})
    except Exception as e:
        logger.warning(f"job_spikes: {e}")


# ─── BUILD SCHEDULER ─────────────────────────────────────────────────────────
def build_scheduler() -> BackgroundScheduler:
    sched = BackgroundScheduler(
        timezone="Asia/Kolkata",
        job_defaults={"coalesce": True, "max_instances": 1, "misfire_grace_time": 5},
    )
    sched.add_job(job_prices, "interval", seconds=1,   id="prices")
    sched.add_job(job_chain,  "interval", seconds=30,  id="chain")
    sched.add_job(job_stocks, "interval", seconds=30,  id="stocks")
    sched.add_job(job_fii,    "interval", seconds=300, id="fii")
    sched.add_job(job_spikes, "interval", seconds=10,  id="spikes")

    def on_err(ev):
        logger.error(f"Scheduler job {ev.job_id} failed: {ev.exception}")
    sched.add_listener(on_err, EVENT_JOB_ERROR)
    return sched
