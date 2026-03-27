"""
NSE EDGE v5 — Scheduler (Kite Connect only)
All jobs call Kite APIs or read from the Kite price cache.
"""

import logging
import time
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.events import EVENT_JOB_ERROR

logger = logging.getLogger("scheduler")

_kite    = None   # KiteConnect instance
_fetcher = None
_signals = None
_ws      = None
_cache   = {"indices": None, "chain": None, "fii": None,
            "stocks": [], "mode": "intraday"}


def set_dependencies(kite_instance, fetcher_mod, signals_mod, broadcast_fn):
    global _kite, _fetcher, _signals, _ws
    _kite    = kite_instance
    _fetcher = fetcher_mod
    _signals = signals_mod
    _ws      = broadcast_fn


def set_mode(mode: str):
    _cache["mode"] = mode


# ─── JOB: BROADCAST PRICES (every 1 second) ───────────────────────────────────
def job_prices():
    """Broadcast latest prices from Kite cache — KiteTicker keeps them fresh."""
    try:
        from feed import get_all_prices
        prices = get_all_prices()
        if prices and _ws:
            _ws({"type": "prices", "data": prices, "ts": time.time()})
    except Exception as e:
        logger.debug(f"job_prices: {e}")


# ─── JOB: FETCH OPTION CHAIN + RUN GATES (every 30 seconds) ──────────────────
def job_chain():
    try:
        # Option chain from Kite NFO
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
            }, "ts": time.time()})
            if chain:
                _ws({"type": "chain",  "data": chain,              "ts": time.time()})
            if indices:
                _ws({"type": "macro",  "data": indices,            "ts": time.time()})
    except Exception as e:
        logger.error(f"job_chain: {e}", exc_info=True)


# ─── JOB: F&O STOCKS (every 30 seconds) ──────────────────────────────────────
def job_stocks():
    try:
        stocks = _fetcher.fetch_fno_stocks(_kite)
        if stocks:
            _cache["stocks"] = stocks
            if _ws:
                _ws({"type": "stocks", "data": stocks, "ts": time.time()})
    except Exception as e:
        logger.error(f"job_stocks: {e}", exc_info=True)


# ─── JOB: FII/DII from NSE (every 5 minutes) ─────────────────────────────────
def job_fii():
    try:
        fii = _fetcher.fetch_fii_dii()
        if fii:
            _cache["fii"] = fii
            if _ws:
                _ws({"type": "fii", "data": fii, "ts": time.time()})
    except Exception as e:
        logger.error(f"job_fii: {e}")


# ─── JOB: SPIKES + TICKER (every 10 seconds) ─────────────────────────────────
def job_spikes():
    try:
        if _signals and _ws:
            _ws({"type": "spikes", "data": _signals.state.get("spikes", []), "ts": time.time()})
            _ws({"type": "ticker", "data": _signals.state.get("ticker", []), "ts": time.time()})
    except Exception as e:
        logger.debug(f"job_spikes: {e}")


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
