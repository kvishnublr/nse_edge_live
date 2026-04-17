"""
STOCKR.IN v5 â€” Zerodha Kite Connect Price Feed
Real-time ticks via KiteTicker WebSocket.
REST quote fallback if ticker drops.

Every tick from KiteTicker updates the price_cache dict.
The signal engine reads from price_cache every 30 seconds.
"""

import time
import logging
import threading
from collections import deque
from typing import Dict, Optional

import config
from kiteconnect import KiteConnect, KiteTicker

from config import KITE_TOKENS, KITE_TOKEN_TO_SYMBOL, KITE_QUOTE_KEYS

logger = logging.getLogger("feed")

_token_refresh_lock = threading.Lock()
_last_auto_token_attempt_ts = 0.0
_AUTO_TOKEN_COOLDOWN_SEC = 180.0
_AUTO_RECOVER_POLL_SEC = 180.0


def maybe_refresh_kite_token(reason: str = "auth_error") -> None:
    """Non-interactive token refresh when Kite returns auth errors (throttled)."""
    global _last_auto_token_attempt_ts
    now = time.time()
    if now - _last_auto_token_attempt_ts < _AUTO_TOKEN_COOLDOWN_SEC:
        return
    with _token_refresh_lock:
        now = time.time()
        if now - _last_auto_token_attempt_ts < _AUTO_TOKEN_COOLDOWN_SEC:
            return
        _last_auto_token_attempt_ts = now

    def _bg():
        try:
            from auto_token import refresh_token

            if refresh_token():
                from scheduler import _apply_new_token

                _apply_new_token()
                logger.info("Kite access token auto-refreshed (%s)", reason)
            else:
                logger.warning("Auto token refresh skipped or failed (%s)", reason)
        except Exception as e:
            logger.warning("Auto token refresh error (%s): %s", reason, e)

    threading.Thread(target=_bg, daemon=True).start()

# â”€â”€â”€ PRICE CACHE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# {symbol: {price, chg_pct, chg_pts, prev, high, low, volume, oi, ts, source}}
price_cache: Dict[str, dict] = {}
_lock = threading.Lock()

# Price history for ATR/VWAP calculation
# {symbol: deque of (timestamp, price, volume)}
price_history: Dict[str, deque] = {}
HISTORY_LEN = 500


def _hist(symbol: str) -> deque:
    if symbol not in price_history:
        price_history[symbol] = deque(maxlen=HISTORY_LEN)
    return price_history[symbol]


def _update(symbol: str, data: dict):
    ts = time.time()
    with _lock:
        price_cache[symbol] = {**data, "ts": ts}
    # push to history for ATR/VWAP
    p = data.get("price", 0)
    v = data.get("volume", 0)
    if p:
        _hist(symbol).append((ts, p, v))


def get_price(symbol: str) -> Optional[dict]:
    with _lock:
        return price_cache.get(symbol)


def get_all_prices() -> dict:
    with _lock:
        return dict(price_cache)


# â”€â”€â”€ KITE CONNECT CLIENT â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
_kite: Optional[KiteConnect] = None
_kite_bound_token: Optional[str] = None
_ticker: Optional[KiteTicker] = None
_ticker_connected = False
_ticker_lock = threading.Lock()  # Protect _ticker_connected from race conditions
_reconnect_count = 0
_reconnect_lock = threading.Lock()  # Protect _reconnect_count


def get_kite() -> KiteConnect:
    """Return authenticated KiteConnect bound to the current config.KITE_ACCESS_TOKEN."""
    global _kite, _kite_bound_token
    tok = (config.KITE_ACCESS_TOKEN or "").strip()
    if _kite is not None and _kite_bound_token == tok:
        return _kite
    _kite = KiteConnect(api_key=config.KITE_API_KEY)
    _kite.set_access_token(tok)
    _kite_bound_token = tok
    logger.info("KiteConnect client initialised")
    return _kite


# â”€â”€â”€ REST QUOTE FALLBACK â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def fetch_quotes_rest():
    """
    Fetch quotes for all tracked symbols via kite.quote().
    Used as fallback when ticker is not connected, and for OI data
    (KiteTicker MODE_FULL includes OI for F&O; MODE_LTP for equities).
    """
    kite = get_kite()
    # Fetch in batches of 500 (Kite limit per call)
    keys = list(KITE_QUOTE_KEYS.values())
    auth_markers = (
        "token",
        "incorrect api",
        "unauthorized",
        "forbidden",
        "invalid",
        "api_key",
    )
    transient_markers = (
        "timeout",
        "timed out",
        "read timed",
        "connection reset",
        "connection aborted",
        "temporarily unavailable",
        "503",
        "502",
    )
    data = None
    for attempt in range(3):
        try:
            data = kite.quote(keys)
            break
        except Exception as e:
            err = str(e).lower()
            if any(s in err for s in auth_markers):
                maybe_refresh_kite_token("rest_quote")
                logger.warning(f"REST quote failed: {e}")
                return
            if any(s in err for s in transient_markers) and attempt < 2:
                time.sleep(0.4 * (2**attempt))
                continue
            logger.warning(f"REST quote failed: {e}")
            return
    if not data:
        return

    for symbol, qkey in KITE_QUOTE_KEYS.items():
        q = data.get(qkey)
        if not q:
            continue
        ltp   = q.get("last_price", 0)
        close = q.get("ohlc", {}).get("close", ltp) or ltp
        chg   = round(ltp - close, 2)
        chg_p = round(chg / close * 100, 2) if close else 0
        _update(symbol, {
            "symbol":  symbol,
            "price":   round(ltp, 2),
            "chg_pts": chg,
            "chg_pct": chg_p,
            "prev":    round(close, 2),
            "high":    q.get("ohlc", {}).get("high", ltp),
            "low":     q.get("ohlc", {}).get("low", ltp),
            "volume":  q.get("volume", 0),
            "oi":      q.get("oi", 0),
            "source":  "kite_rest",
        })


# â”€â”€â”€ KITE TICKER (REAL-TIME WEBSOCKET) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def _on_ticks(ws, ticks: list):
    """Called by KiteTicker on every tick. Updates price_cache instantly."""
    global _ticker_connected
    with _ticker_lock:
        _ticker_connected = True
    for tick in ticks:
        token  = tick.get("instrument_token")
        symbol = KITE_TOKEN_TO_SYMBOL.get(token)
        if not symbol:
            continue
        ltp   = tick.get("last_price", 0)
        close = tick.get("ohlc", {}).get("close", ltp) or ltp
        chg   = round(ltp - close, 2)
        chg_p = round(chg / close * 100, 2) if close else 0
        _update(symbol, {
            "symbol":   symbol,
            "price":    round(ltp, 2),
            "chg_pts":  chg,
            "chg_pct":  chg_p,
            "prev":     round(close, 2),
            "high":     tick.get("ohlc", {}).get("high", ltp),
            "low":      tick.get("ohlc", {}).get("low", ltp),
            "volume":   tick.get("volume_traded", 0),
            "oi":       tick.get("oi", 0),
            "bid":      tick.get("depth", {}).get("buy", [{}])[0].get("price", 0),
            "ask":      tick.get("depth", {}).get("sell", [{}])[0].get("price", 0),
            "source":   "kite_tick",
        })


def _on_connect(ws, response):
    global _reconnect_count
    with _reconnect_lock:
        _reconnect_count = 0
    tokens = list(KITE_TOKENS.values())
    logger.info(f"KiteTicker connected â€” subscribing {len(tokens)} instruments")
    ws.subscribe(tokens)
    # FULL mode = price + depth + OI + volume
    ws.set_mode(ws.MODE_FULL, tokens)


def _on_close(ws, code, reason):
    global _ticker_connected
    with _ticker_lock:
        _ticker_connected = False
    logger.warning(f"KiteTicker closed ({code}): {reason}")


def _on_error(ws, code, reason):
    logger.error(f"KiteTicker error ({code}): {reason}")


def _on_reconnect(ws, attempts_count):
    global _reconnect_count
    with _reconnect_lock:
        _reconnect_count = attempts_count
    logger.info(f"KiteTicker reconnecting... attempt {attempts_count}")


def _on_noreconnect(ws):
    global _ticker_connected
    with _ticker_lock:
        _ticker_connected = False
    logger.error("KiteTicker gave up reconnecting â€” falling back to REST quotes")
    # Start REST polling fallback
    threading.Thread(target=_rest_fallback_loop, daemon=True).start()


def _rest_fallback_loop():
    """Poll REST quotes every 2 seconds when ticker is down (max 10 minutes)."""
    logger.info("REST quote fallback loop started")
    max_iterations = 300  # 10 minutes (300 * 2 seconds)
    iteration = 0

    while iteration < max_iterations:
        with _ticker_lock:
            if _ticker_connected:
                logger.info("Ticker reconnected â€” stopping REST fallback")
                return

        fetch_quotes_rest()
        time.sleep(2)
        iteration += 1

    logger.error(f"REST fallback timeout after {max_iterations * 2}s â€” ticker not recovered")
    logger.warning("System will continue with stale data. Please check Kite API status and restart.")


# â”€â”€â”€ FEED MANAGER â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
class FeedManager:
    def __init__(self):
        self._running = False
        self._demo_mode = False
        self._recover_thread_started = False

    def _start_auto_recover_loop(self):
        """Keep trying headless token refresh until Kite auth succeeds."""
        if self._recover_thread_started:
            return
        self._recover_thread_started = True

        def _loop():
            logger.info("Kite auto-recover loop started (headless mode)")
            while self._running:
                try:
                    try:
                        get_kite().profile()
                        if self._demo_mode:
                            self._demo_mode = False
                        logger.info("Kite session recovered; stopping auto-recover loop")
                        return
                    except Exception:
                        pass

                    from auto_token import refresh_token

                    if refresh_token():
                        from scheduler import _apply_new_token
                        _apply_new_token()
                        try:
                            get_kite().profile()
                            self._demo_mode = False
                            logger.info("Kite auto-recover SUCCESS; live feed restored")
                            return
                        except Exception as _e:
                            logger.warning("Kite auto-recover applied token but auth still failed: %s", _e)
                    else:
                        logger.warning("Kite auto-recover refresh attempt failed")
                except Exception as e:
                    logger.warning("Kite auto-recover loop error: %s", e)

                for _ in range(int(_AUTO_RECOVER_POLL_SEC)):
                    if not self._running:
                        return
                    time.sleep(1)

        threading.Thread(target=_loop, daemon=True).start()

    def start(self):
        """Start KiteTicker real-time feed."""
        self._running = True

        if not config.KITE_API_KEY:
            raise RuntimeError("\n\n  KITE_API_KEY must be set in backend/.env\n")
        if not config.KITE_ACCESS_TOKEN:
            logger.warning("KITE_ACCESS_TOKEN missing — attempting immediate headless refresh")
            try:
                from auto_token import refresh_token
                if refresh_token():
                    from scheduler import _apply_new_token
                    _apply_new_token()
                else:
                    logger.warning("headless refresh returned false — starting in degraded mode (no Kite token)")
                    self._running = False
                    return
            except Exception as e:
                logger.warning(
                    f"KITE_ACCESS_TOKEN missing and auto-refresh failed: {e}\n"
                    "  Starting in degraded mode — use the UI to login via Kite OAuth."
                )
                self._running = False
                return

        # Verify credentials work before starting ticker
        try:
            kite = get_kite()
            profile = kite.profile()
            logger.info(f"Kite auth OK â€” logged in as: {profile.get('user_name', 'unknown')}")
        except Exception as e:
            logger.warning(f"Kite auth failed: {e}")
            # Immediate non-interactive refresh attempt (no manual browser/code flow).
            recovered = False
            try:
                from auto_token import refresh_token
                if refresh_token():
                    from scheduler import _apply_new_token
                    _apply_new_token()
                    kite = get_kite()
                    profile = kite.profile()
                    logger.info(f"Kite auth recovered â€” logged in as: {profile.get('user_name', 'unknown')}")
                    recovered = True
                else:
                    raise RuntimeError("headless refresh returned false")
            except Exception as _re:
                logger.warning(f"Immediate headless token refresh failed: {_re}")
            if recovered:
                # Continue normal startup path below (REST warmup + ticker start).
                pass
            else:
                # Kite-only: no alternate price feed â€” cache stays empty until session is valid.
                logger.warning(
                    "Kite session pending â€” price cache empty until token is valid; "
                    "auto-refresh will call restart_ticker_with_new_token() when ready."
                )
                self._demo_mode = True
                self._start_auto_recover_loop()
                return

        self._demo_mode = False

        # Do one REST fetch immediately so cache is populated on startup
        logger.info("Initial REST quote fetch...")
        fetch_quotes_rest()
        logger.info(f"  Cached {len(price_cache)} instruments")

        # Start KiteTicker WebSocket
        logger.info("Starting KiteTicker real-time feed...")
        global _ticker
        _ticker = KiteTicker(config.KITE_API_KEY, config.KITE_ACCESS_TOKEN)
        _ticker.on_ticks       = _on_ticks
        _ticker.on_connect     = _on_connect
        _ticker.on_close       = _on_close
        _ticker.on_error       = _on_error
        _ticker.on_reconnect   = _on_reconnect
        _ticker.on_noreconnect = _on_noreconnect

        # threaded=True runs the ticker in a background thread
        # reconnect_max_tries=50 handles network drops
        _ticker.connect(threaded=True)
        logger.info("KiteTicker started â€” waiting for ticks")

    def stop(self):
        self._running = False
        self._recover_thread_started = False
        global _ticker
        if _ticker:
            try:
                _ticker.close()
            except Exception:
                pass


def restart_ticker_with_new_token() -> None:
    """
    After config.KITE_ACCESS_TOKEN (and os.environ) are updated, rebuild REST client
    and KiteTicker so the live feed uses the new session. Safe after a failed/pending Kite login.
    """
    global _kite, _kite_bound_token, _ticker
    api_key = (config.KITE_API_KEY or "").strip()
    token = (config.KITE_ACCESS_TOKEN or "").strip()
    if not api_key or not token:
        logger.error("restart_ticker_with_new_token: missing KITE_API_KEY or KITE_ACCESS_TOKEN")
        return
    _kite = None
    _kite_bound_token = None
    if _ticker is not None:
        try:
            _ticker.close()
        except Exception as e:
            logger.debug("KiteTicker close during restart: %s", e)
        _ticker = None
    try:
        k = get_kite()
        k.profile()
    except Exception as e:
        logger.error("restart_ticker_with_new_token: token still rejected by Kite: %s", e)
        return
    if getattr(feed_manager, "_demo_mode", False):
        feed_manager._demo_mode = False
        logger.info("Kite session active â€” live ticker starting")
    _ticker = KiteTicker(api_key, token)
    _ticker.on_ticks = _on_ticks
    _ticker.on_connect = _on_connect
    _ticker.on_close = _on_close
    _ticker.on_error = _on_error
    _ticker.on_reconnect = _on_reconnect
    _ticker.on_noreconnect = _on_noreconnect
    try:
        fetch_quotes_rest()
    except Exception as e:
        logger.warning("restart_ticker_with_new_token: initial REST fetch: %s", e)
    _ticker.connect(threaded=True)
    logger.info("KiteTicker restarted with refreshed access token")


feed_manager = FeedManager()
