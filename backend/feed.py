"""
NSE EDGE v5 — Zerodha Kite Connect Price Feed
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

from kiteconnect import KiteConnect, KiteTicker

from config import (
    KITE_API_KEY, KITE_ACCESS_TOKEN,
    KITE_TOKENS, KITE_TOKEN_TO_SYMBOL, KITE_QUOTE_KEYS,
)

logger = logging.getLogger("feed")

# ─── PRICE CACHE ──────────────────────────────────────────────────────────────
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


# ─── KITE CONNECT CLIENT ──────────────────────────────────────────────────────
_kite: Optional[KiteConnect] = None
_ticker: Optional[KiteTicker] = None
_ticker_connected = False
_ticker_lock = threading.Lock()  # Protect _ticker_connected from race conditions
_reconnect_count = 0
_reconnect_lock = threading.Lock()  # Protect _reconnect_count


def get_kite() -> KiteConnect:
    """Return authenticated KiteConnect instance."""
    global _kite
    if _kite is None:
        _kite = KiteConnect(api_key=KITE_API_KEY)
        _kite.set_access_token(KITE_ACCESS_TOKEN)
        logger.info("KiteConnect client initialised")
    return _kite


# ─── REST QUOTE FALLBACK ──────────────────────────────────────────────────────
def fetch_quotes_rest():
    """
    Fetch quotes for all tracked symbols via kite.quote().
    Used as fallback when ticker is not connected, and for OI data
    (KiteTicker MODE_FULL includes OI for F&O; MODE_LTP for equities).
    """
    kite = get_kite()
    # Fetch in batches of 500 (Kite limit per call)
    keys = list(KITE_QUOTE_KEYS.values())
    try:
        data = kite.quote(keys)
    except Exception as e:
        logger.warning(f"REST quote failed: {e}")
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


# ─── KITE TICKER (REAL-TIME WEBSOCKET) ────────────────────────────────────────
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
    logger.info(f"KiteTicker connected — subscribing {len(tokens)} instruments")
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
    logger.error("KiteTicker gave up reconnecting — falling back to REST quotes")
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
                logger.info("Ticker reconnected — stopping REST fallback")
                return

        fetch_quotes_rest()
        time.sleep(2)
        iteration += 1

    logger.error(f"REST fallback timeout after {max_iterations * 2}s — ticker not recovered")
    logger.warning("System will continue with stale data. Please check Kite API status and restart.")


# ─── FEED MANAGER ─────────────────────────────────────────────────────────────
class FeedManager:
    def __init__(self):
        self._running = False

    def start(self):
        """Start KiteTicker real-time feed."""
        self._running = True

        if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
            raise RuntimeError(
                "\n\n  KITE_API_KEY and KITE_ACCESS_TOKEN must be set in backend/.env\n"
                "  Run: python3 generate_token.py to get today's access token.\n"
            )

        # Verify credentials work before starting ticker
        try:
            kite = get_kite()
            profile = kite.profile()
            logger.info(f"Kite auth OK — logged in as: {profile.get('user_name', 'unknown')}")
        except Exception as e:
            logger.warning(f"Kite auth failed: {e}")
            logger.warning("Using DEMO mode with yfinance data (no live trading)")
            self._demo_mode = True
            # Seed with demo data from yfinance
            self._fetch_demo_data()
            return

        # Do one REST fetch immediately so cache is populated on startup
        logger.info("Initial REST quote fetch...")
        fetch_quotes_rest()
        logger.info(f"  Cached {len(price_cache)} instruments")

        # Start KiteTicker WebSocket
        logger.info("Starting KiteTicker real-time feed...")
        global _ticker
        _ticker = KiteTicker(KITE_API_KEY, KITE_ACCESS_TOKEN)
        _ticker.on_ticks       = _on_ticks
        _ticker.on_connect     = _on_connect
        _ticker.on_close       = _on_close
        _ticker.on_error       = _on_error
        _ticker.on_reconnect   = _on_reconnect
        _ticker.on_noreconnect = _on_noreconnect

        # threaded=True runs the ticker in a background thread
        # reconnect_max_tries=50 handles network drops
        _ticker.connect(threaded=True)
        logger.info("KiteTicker started — waiting for ticks")

    def _fetch_demo_data(self):
        """Fetch demo data from yfinance when Kite auth fails."""
        try:
            import yfinance as yf
            logger.info("Fetching demo data from yfinance...")
            
            # Fetch Nifty data
            nifty = yf.Ticker("^NSEI")
            nifty_info = nifty.info
            nifty_price = nifty_info.get('regularMarketPrice', 22500)
            nifty_prev = nifty_info.get('previousClose', nifty_price)
            nifty_chg = ((nifty_price - nifty_prev) / nifty_prev * 100) if nifty_prev else 0
            
            _update("NIFTY", {
                "price": nifty_price,
                "prev": nifty_prev,
                "chg_pct": nifty_chg,
                "chg_pts": nifty_price - nifty_prev,
                "high": nifty_info.get('regularMarketDayHigh', nifty_price),
                "low": nifty_info.get('regularMarketDayLow', nifty_price),
                "volume": nifty_info.get('regularMarketVolume', 0),
                "source": "yfinance"
            })
            
            # Fetch BankNifty
            banknifty = yf.Ticker("^NSEBANK")
            bn_info = banknifty.info
            bn_price = bn_info.get('regularMarketPrice', 48000)
            bn_prev = bn_info.get('previousClose', bn_price)
            bn_chg = ((bn_price - bn_prev) / bn_prev * 100) if bn_prev else 0
            
            _update("BANKNIFTY", {
                "price": bn_price,
                "prev": bn_prev,
                "chg_pct": bn_chg,
                "chg_pts": bn_price - bn_prev,
                "high": bn_info.get('regularMarketDayHigh', bn_price),
                "low": bn_info.get('regularMarketDayLow', bn_price),
                "volume": bn_info.get('regularMarketVolume', 0),
                "source": "yfinance"
            })
            
            # Fetch India VIX (approximate using ^INDIAVIX or ^VIX)
            try:
                vix = yf.Ticker("^INDIAVIX")
                vix_info = vix.info
                vix_price = vix_info.get('regularMarketPrice', 15)
                vix_prev = vix_info.get('previousClose', vix_price)
                vix_chg = ((vix_price - vix_prev) / vix_prev * 100) if vix_prev else 0
            except:
                vix_price = 15
                vix_chg = 0
            
            _update("INDIAVIX", {
                "price": vix_price,
                "prev": vix_price / (1 + vix_chg/100) if vix_chg else vix_price,
                "chg_pct": vix_chg,
                "chg_pts": 0,
                "source": "yfinance"
            })
            
            logger.info(f"Demo data: Nifty {nifty_price} ({nifty_chg:+.2f}%), BankNifty {bn_price} ({bn_chg:+.2f}%)")
            
            # Start polling loop for demo mode
            threading.Thread(target=self._demo_poll_loop, daemon=True).start()
            
        except Exception as e:
            logger.error(f"Failed to fetch demo data: {e}")
    
    def _demo_poll_loop(self):
        """Poll yfinance every 60 seconds in demo mode."""
        import yfinance as yf
        logger.info("Demo mode: polling yfinance every 60s")
        while self._running and getattr(self, '_demo_mode', False):
            time.sleep(60)
            try:
                nifty = yf.Ticker("^NSEI")
                nifty_info = nifty.info
                nifty_price = nifty_info.get('regularMarketPrice')
                nifty_prev = nifty_info.get('previousClose')
                if nifty_price and nifty_prev:
                    nifty_chg = (nifty_price - nifty_prev) / nifty_prev * 100
                    _update("NIFTY", {
                        "price": nifty_price,
                        "prev": nifty_prev,
                        "chg_pct": nifty_chg,
                        "chg_pts": nifty_price - nifty_prev,
                        "source": "yfinance"
                    })
                    
                    banknifty = yf.Ticker("^NSEBANK")
                    bn_info = banknifty.info
                    bn_price = bn_info.get('regularMarketPrice')
                    bn_prev = bn_info.get('previousClose')
                    if bn_price and bn_prev:
                        bn_chg = (bn_price - bn_prev) / bn_prev * 100
                        _update("BANKNIFTY", {
                            "price": bn_price,
                            "prev": bn_prev,
                            "chg_pct": bn_chg,
                            "chg_pts": bn_price - bn_prev,
                            "source": "yfinance"
                        })
                    logger.info(f"Demo update: Nifty {nifty_chg:+.2f}%, BankNifty {bn_chg:+.2f}%")
            except Exception as e:
                logger.warning(f"Demo poll failed: {e}")

    def stop(self):
        self._running = False
        global _ticker
        if _ticker:
            try:
                _ticker.close()
            except Exception:
                pass


feed_manager = FeedManager()
