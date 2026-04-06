"""
NSE EDGE v5 — Data Fetcher (Zerodha Kite Connect)

All market data from Kite:
  - Option chain OI via kite.quote on NFO instruments
  - Indices (Nifty, BankNifty, India VIX) via kite.quote
  - F&O stock OI via kite.quote on NFO futures

FII/DII from NSE (Kite does not provide this data).
"""

import time
import logging
import statistics
import requests
from typing import Optional, List, Dict
from datetime import datetime, timedelta

import pytz

from config import KITE_QUOTE_KEYS, NSE_BASE, NSE_HEADERS, NSE_TIMEOUT

logger = logging.getLogger("fetcher")
IST = pytz.timezone("Asia/Kolkata")

# ─── NSE SESSION (for FII/DII only) ──────────────────────────────────────────
_nse_session = requests.Session()
_nse_session.headers.update(NSE_HEADERS)
_nse_cookie_ts = 0
_stock_analytics_cache: Dict[str, dict] = {}
_stock_analytics_ts = 0.0
_global_macro_cache: Dict[str, float] = {}
_global_macro_ts = 0.0


def _get_stock_analytics(kite, futures: List[dict]) -> Dict[str, dict]:
    """Daily analytics for live stock scanner, cached 15 minutes."""
    global _stock_analytics_cache, _stock_analytics_ts
    if _stock_analytics_cache and time.time() - _stock_analytics_ts < 900:
        return _stock_analytics_cache

    analytics = {}
    to_dt = datetime.now(IST)
    from_dt = to_dt - timedelta(days=45)

    for fut in futures:
        sym = fut.get("name")
        token = fut.get("instrument_token")
        if not sym or not token:
            continue
        try:
            hist = kite.historical_data(token, from_dt, to_dt, "day") or []
            if len(hist) < 5:
                continue
            vols = [float(r.get("volume", 0) or 0) for r in hist[-20:] if float(r.get("volume", 0) or 0) > 0]
            avg_vol20 = statistics.mean(vols) if vols else 0.0
            trs = []
            prev_close = None
            for r in hist[-15:]:
                hi = float(r.get("high", 0) or 0)
                lo = float(r.get("low", 0) or 0)
                cl = float(r.get("close", 0) or 0)
                tr = hi - lo
                if prev_close is not None:
                    tr = max(tr, abs(hi - prev_close), abs(lo - prev_close))
                trs.append(max(tr, 0.0))
                prev_close = cl
            last_close = float(hist[-1].get("close", 0) or 0)
            atr = statistics.mean(trs) if trs else 0.0
            analytics[sym] = {
                "avg_vol20": avg_vol20,
                "atr_pct14": round((atr / last_close * 100), 2) if last_close else 0.0,
            }
        except Exception as e:
            logger.debug(f"stock analytics skipped for {sym}: {e}")

    _stock_analytics_cache = analytics
    _stock_analytics_ts = time.time()
    return analytics


def fetch_global_macro() -> Dict[str, float]:
    """Fetch global macro references from Yahoo quote endpoint, cached 5 minutes."""
    global _global_macro_cache, _global_macro_ts
    if _global_macro_cache and time.time() - _global_macro_ts < 300:
        return _global_macro_cache
    try:
        out: Dict[str, float] = {}
        sym_map = {
            "^TNX": ("us10y", False),
            "DX-Y.NYB": ("dxy", False),
            "CL=F": ("crude", False),
        }
        for yahoo_sym, (out_key, divide_ten) in sym_map.items():
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yahoo_sym}?range=5d&interval=1d"
            resp = requests.get(url, timeout=6, headers={"User-Agent": NSE_HEADERS.get("User-Agent", "Mozilla/5.0")})
            if resp.status_code != 200:
                continue
            result = ((resp.json().get("chart", {}) or {}).get("result") or [None])[0] or {}
            meta = result.get("meta", {}) or {}
            px = meta.get("regularMarketPrice")
            prev_close = meta.get("chartPreviousClose") or meta.get("previousClose")
            if px is None:
                continue
            price = float(px) / 10.0 if divide_ten else float(px)
            chg = None
            if prev_close not in (None, 0):
                base_prev = float(prev_close) / 10.0 if divide_ten else float(prev_close)
                if base_prev:
                    chg = (price - base_prev) / base_prev * 100.0
            out[out_key] = round(price, 2)
            out[f"{out_key}_chg"] = round(chg, 2) if chg is not None else None
        if out:
            _global_macro_cache = out
            _global_macro_ts = time.time()
        return _global_macro_cache
    except Exception as e:
        logger.debug(f"global macro fetch failed: {e}")
        return _global_macro_cache

def _nse_refresh_cookie():
    """Refresh NSE session cookie (2-step handshake NSE requires)."""
    global _nse_cookie_ts
    try:
        # Step 1: hit homepage to get initial cookies
        resp = _nse_session.get(NSE_BASE, timeout=NSE_TIMEOUT)
        if resp.status_code != 200:
            logger.warning(f"NSE cookie refresh failed: {resp.status_code}")
            return False
        time.sleep(0.5)
        # Step 2: hit market-data page to validate session
        _nse_session.get(f"{NSE_BASE}/market-data/live-equity-market", timeout=NSE_TIMEOUT)
        _nse_cookie_ts = time.time()
        logger.debug("NSE cookie refreshed")
        return True
    except Exception as e:
        logger.warning(f"NSE cookie refresh exception: {e}")
        return False

def _nse_get(url: str, max_retries: int = 3) -> Optional[dict]:
    """Get data from NSE with automatic cookie refresh and retry logic."""
    for attempt in range(max_retries):
        # Refresh cookie if stale (every 4 minutes)
        if attempt == 0 or time.time() - _nse_cookie_ts > 240:
            _nse_refresh_cookie()

        try:
            resp = _nse_session.get(url, timeout=NSE_TIMEOUT, headers={
                "Accept": "application/json, text/plain, */*",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
            })

            # Success
            if resp.status_code == 200:
                ct = resp.headers.get("Content-Type", "")
                if "json" not in ct:
                    # NSE returned HTML (bot detection / Cloudflare challenge)
                    logger.warning(f"NSE returned non-JSON ({ct[:40]}) — bot detection, retrying with cookie refresh")
                    _nse_refresh_cookie()
                    time.sleep(1)
                    continue
                return resp.json()

            # Unauthorized - refresh and retry
            elif resp.status_code == 401 and attempt < max_retries - 1:
                logger.warning(f"NSE 401 Unauthorized (attempt {attempt + 1}/{max_retries}) - refreshing cookie")
                _nse_refresh_cookie()
                continue

            # Other HTTP errors
            else:
                logger.warning(f"NSE {resp.status_code} on {url} (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    time.sleep(1)  # Wait before retry
                continue

        except requests.Timeout:
            logger.warning(f"NSE timeout on {url} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None

        except Exception as e:
            logger.warning(f"NSE exception: {e.__class__.__name__} (attempt {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
            return None

    logger.error(f"NSE GET {url}: Failed after {max_retries} attempts")
    return None


# ─── OPTION CHAIN (via Kite NFO instruments) ──────────────────────────────────
def fetch_option_chain(kite, symbol: str = "NIFTY") -> Optional[dict]:
    """
    Build option chain using kite.quote() on NFO option instruments.

    Flow:
      1. Get all NFO instruments via kite.instruments("NFO") — cached per session
      2. Filter to weekly/nearest expiry for NIFTY or BANKNIFTY options
      3. Select 6 ITM + ATM + 6 OTM strikes
      4. Call kite.quote() on those ~13 CE+PE instruments (26 keys max)
      5. Parse OI, IV, LTP for each strike
    """
    try:
        from feed import get_price
        ul     = get_price(symbol)
        ul_px  = ul["price"] if ul else (24400 if symbol == "NIFTY" else 52000)

        # Get instruments and filter
        instruments = _get_nfo_instruments(kite, symbol)
        if not instruments:
            logger.warning(f"No NFO instruments found for {symbol}")
            return None

        # Nearest expiry
        today      = datetime.now(IST).date()
        expiries   = sorted(set(i["expiry"] for i in instruments if i["expiry"] >= today))
        if not expiries:
            return None
        expiry     = expiries[0]
        exp_str    = f"{expiry.day} {expiry.strftime('%b %Y')}" if hasattr(expiry, 'strftime') else str(expiry)

        # Strikes for this expiry
        exp_insts  = [i for i in instruments if i["expiry"] == expiry]
        all_strikes = sorted(set(int(i["strike"]) for i in exp_insts))
        if not all_strikes:
            return None

        # ATM
        atm = min(all_strikes, key=lambda s: abs(s - ul_px))
        atm_idx = all_strikes.index(atm)
        # Use ALL strikes for comprehensive PCR analysis
        selected = all_strikes

        # Build Kite quote keys: NFO:NIFTY24MAR25000CE etc.
        inst_map = {}  # (strike, type) → instrument
        for i in exp_insts:
            key = (int(i["strike"]), i["instrument_type"])  # CE or PE
            inst_map[key] = i

        kite_keys = []
        for s in selected:
            for t in ("CE", "PE"):
                inst = inst_map.get((s, t))
                if inst:
                    kite_keys.append(f"NFO:{inst['tradingsymbol']}")

        if not kite_keys:
            return None

        # Fetch quotes — max 500 per call, we have ~26
        data = kite.quote(kite_keys)

        # Parse
        strike_data = []
        total_call_oi = 0
        total_put_oi  = 0

        for s in selected:
            ce_inst = inst_map.get((s, "CE"))
            pe_inst = inst_map.get((s, "PE"))
            ce_key  = f"NFO:{ce_inst['tradingsymbol']}" if ce_inst else ""
            pe_key  = f"NFO:{pe_inst['tradingsymbol']}" if pe_inst else ""

            cq = data.get(ce_key, {})
            pq = data.get(pe_key, {})

            c_oi     = cq.get("oi", 0)
            # Use OI change from prev close if available, else daily range
            c_oi_chg = cq.get("oi") - cq.get("oi_prev_day_close", 0) if "oi" in cq else 0
            if c_oi_chg == 0:  # Fallback if prev_day_close not available
                c_oi_chg = cq.get("oi_day_high", 0) - cq.get("oi_day_low", 0)
            c_ltp    = cq.get("last_price", 0)
            c_iv     = _calc_iv_proxy(c_ltp, ul_px, s, expiry, "CE")

            p_oi     = pq.get("oi", 0)
            # Use OI change from prev close if available, else daily range
            p_oi_chg = pq.get("oi") - pq.get("oi_prev_day_close", 0) if "oi" in pq else 0
            if p_oi_chg == 0:  # Fallback if prev_day_close not available
                p_oi_chg = pq.get("oi_day_high", 0) - pq.get("oi_day_low", 0)
            p_ltp    = pq.get("last_price", 0)
            p_iv     = _calc_iv_proxy(p_ltp, ul_px, s, expiry, "PE")

            total_call_oi += c_oi
            total_put_oi  += p_oi

            strike_data.append({
                "strike":      s,
                "is_atm":      s == atm,
                "call_oi":     c_oi,
                "call_oi_chg": c_oi_chg,
                "call_iv":     c_iv,
                "call_ltp":    c_ltp,
                "put_oi":      p_oi,
                "put_oi_chg":  p_oi_chg,
                "put_iv":      p_iv,
                "put_ltp":     p_ltp,
            })

        pcr = 0
        if total_call_oi > 0 and total_put_oi > 0:
            pcr = round(total_put_oi / total_call_oi, 2)
        elif total_call_oi == 0 and total_put_oi == 0:
            logger.warning(f"fetch_option_chain({symbol}): No valid OI data for both puts and calls")
            return None
        else:
            logger.warning(f"fetch_option_chain({symbol}): Incomplete OI data (calls={total_call_oi}, puts={total_put_oi})")
            pcr = round(total_put_oi / total_call_oi, 2) if total_call_oi > 0 else 0
        max_pain = _calc_max_pain(strike_data)

        # Log Max Pain vs current price for monitoring
        if ul_px > 0 and max_pain > 0:
            deviation = abs(max_pain - ul_px) / ul_px * 100
            if deviation > 5:
                logger.warning(f"Max Pain {max_pain} deviates {deviation:.1f}% from price {ul_px} (PCR={pcr})")
            else:
                logger.debug(f"Max Pain {max_pain} vs price {ul_px} (deviation {deviation:.1f}%)")

        return {
            "symbol":        symbol,
            "expiry":        exp_str,
            "ul_price":      ul_px,
            "pcr":           pcr,
            "max_pain":      max_pain,
            "atm":           atm,
            "strikes":       strike_data,
            "total_call_oi": total_call_oi,
            "total_put_oi":  total_put_oi,
        }

    except Exception as e:
        logger.error(f"Option chain error ({symbol}): {e}", exc_info=True)
        return None


# Cache NFO instruments per session (they don't change intraday)
_nfo_cache: dict = {}
_nfo_cache_ts: float = 0

def _get_nfo_instruments(kite, symbol: str) -> List[dict]:
    """Return NFO option instruments for NIFTY or BANKNIFTY, cached for 6 hours."""
    global _nfo_cache, _nfo_cache_ts
    cache_key = symbol
    if cache_key in _nfo_cache and time.time() - _nfo_cache_ts < 21600:
        return _nfo_cache[cache_key]
    try:
        logger.info(f"Downloading NFO instrument list for {symbol}...")
        all_inst = kite.instruments("NFO")
        filtered = [
            i for i in all_inst
            if i.get("name") == symbol
            and i.get("instrument_type") in ("CE", "PE")
        ]
        _nfo_cache[cache_key] = filtered
        _nfo_cache_ts         = time.time()
        logger.info(f"  {len(filtered)} {symbol} option instruments loaded")
        return filtered
    except Exception as e:
        logger.error(f"NFO instruments download failed: {e}")
        return []


def _calc_max_pain(strikes: List[dict]) -> int:
    """Max pain strike — where total option buyer loss is maximised."""
    try:
        strike_vals = [s["strike"] for s in strikes]
        if not strike_vals:
            return 0
        min_loss   = float("inf")
        max_pain   = strike_vals[len(strike_vals) // 2]
        for test_s in strike_vals:
            loss = sum(
                s["call_oi"] * max(0, test_s - s["strike"]) +
                s["put_oi"]  * max(0, s["strike"] - test_s)
                for s in strikes
            )
            if loss < min_loss:
                min_loss = loss
                max_pain = test_s
        return max_pain
    except Exception:
        return 0


def _calc_iv_proxy(ltp: float, spot: float, strike: int, expiry, opt_type: str) -> float:
    """Rough IV proxy — real IV needs Black-Scholes; this is for display only."""
    try:
        if not ltp or not spot:
            return 0.0
        days = (expiry - datetime.now(IST).date()).days
        if days <= 0:
            return 0.0
        t = days / 365
        intrinsic = max(0, (spot - strike) if opt_type == "CE" else (strike - spot))
        time_val  = max(0, ltp - intrinsic)
        iv_proxy  = (time_val / spot) / (t ** 0.5) * 100
        return round(min(iv_proxy, 99.9), 1)
    except Exception:
        return 0.0


# ─── INDICES — from price cache (KiteTicker feeds them live) ─────────────────
def fetch_indices() -> Optional[dict]:
    """
    Read Nifty, BankNifty, VIX from the live price cache.
    These are updated every tick by KiteTicker.
    """
    from feed import get_price
    nifty     = get_price("NIFTY")
    banknifty = get_price("BANKNIFTY")
    vix       = get_price("INDIAVIX")

    if not nifty:
        return None

    out = {
        "nifty":          nifty.get("price", 0),
        "nifty_chg":      nifty.get("chg_pct", 0),
        "nifty_pts":      nifty.get("chg_pts", 0),
        "nifty_high":     nifty.get("high", 0),
        "nifty_low":      nifty.get("low", 0),
        "banknifty":      banknifty.get("price", 0) if banknifty else 0,
        "banknifty_chg":  banknifty.get("chg_pct", 0) if banknifty else 0,
        "banknifty_pts":  banknifty.get("chg_pts", 0) if banknifty else 0,
        "vix":            vix.get("price", 0) if vix else 0,
        "vix_chg":        vix.get("chg_pct", 0) if vix else 0,
    }
    out.update(fetch_global_macro())
    return out


# ─── F&O STOCK OI (via kite.quote on NFO futures) ─────────────────────────────
def fetch_fno_stocks(kite) -> List[dict]:
    """
    Fetch F&O stock data: price, OI, OI change, volume.
    Uses kite.quote on near-month futures to get OI.
    Equity price comes from price_cache (KiteTicker).
    """
    from config import FNO_SYMBOLS, LOT_SIZES
    from feed import get_price

    stocks = []
    try:
        # Get near-month futures instruments
        futures = _get_nfo_futures(kite)
        if not futures:
            logger.warning("No futures instruments available")
            return []

        # Build quote keys for futures
        fut_keys = [f"NFO:{f['tradingsymbol']}" for f in futures]
        if not fut_keys:
            return []

        # Fetch in one call
        data = kite.quote(fut_keys[:100])  # max 100 keys

        fut_map = {f["name"]: f for f in futures}
        analytics = _get_stock_analytics(kite, futures)
        nifty_eq = get_price("NIFTY") or {}
        nifty_chg = float(nifty_eq.get("chg_pct", 0) or 0)

        for sym in FNO_SYMBOLS:
            fut = fut_map.get(sym)
            if not fut:
                continue
            qkey = f"NFO:{fut['tradingsymbol']}"
            fq   = data.get(qkey, {})

            # Equity price from ticker (real-time)
            eq = get_price(sym)
            price    = eq["price"] if eq else fq.get("last_price", 0)
            chg_pct  = eq["chg_pct"] if eq else 0
            chg_pts  = eq["chg_pts"] if eq else 0

            oi       = fq.get("oi", 0)
            oi_prev  = fq.get("oi_day_low", oi)  # rough proxy for prev OI
            oi_chg   = oi - oi_prev
            oi_chg_p = round(oi_chg / oi_prev * 100, 2) if oi_prev else 0

            vol      = fq.get("volume_traded", 0) or eq.get("volume", 0) if eq else 0
            lot      = LOT_SIZES.get(sym, 1)
            stat     = analytics.get(sym, {})
            avg_vol  = float(stat.get("avg_vol20", 0) or 0)
            vol_ratio = round(vol / avg_vol, 2) if avg_vol > 0 and vol > 0 else 0.0
            atr_pct   = round(float(stat.get("atr_pct14", 0) or 0), 2)
            if atr_pct <= 0:
                atr_pct = round(max(abs(chg_pct) * 0.8, 1.2), 2)
            rs_pct = round(chg_pct - nifty_chg, 2)
            delivery_pct = max(22, min(78, round(34 + abs(oi_chg_p) * 1.0 + min(vol_ratio, 3.5) * 8 + max(rs_pct, 0) * 1.2, 1)))
            score = 40
            if abs(chg_pct) >= 1.5:
                score += 18
            elif abs(chg_pct) >= 0.5:
                score += 8
            if vol_ratio >= 2.0:
                score += 16
            elif vol_ratio >= 1.3:
                score += 8
            if abs(oi_chg_p) >= 10:
                score += 15
            elif abs(oi_chg_p) >= 4:
                score += 8
            if rs_pct >= 1.0:
                score += 10
            elif rs_pct <= -1.0:
                score += 4
            signal = "LONG OI" if chg_pct > 0 and oi_chg_p >= 0 else "SHORT OI" if chg_pct < 0 and oi_chg_p >= 0 else "MIXED"

            stocks.append({
                "symbol":     sym,
                "price":      round(price, 2),
                "chg_pct":    round(chg_pct, 2),
                "chg_pts":    round(chg_pts, 2),
                "oi":         oi,
                "oi_chg":     oi_chg,
                "oi_chg_pct": oi_chg_p,
                "volume":     vol,
                "lot_size":   lot,
                "fut_ltp":    fq.get("last_price", 0),
                "avg_vol20":  round(avg_vol, 0) if avg_vol else 0,
                "vol_ratio":  round(vol_ratio, 2),
                "atr_pct":    atr_pct,
                "rs_pct":     rs_pct,
                "delivery_pct": delivery_pct,
                "signal":     signal,
                "score":      min(99, score),
                "derived_fields": True,
            })
    except Exception as e:
        logger.error(f"FnO stocks fetch error: {e}", exc_info=True)

    return stocks


_nfo_fut_cache: list = []
_nfo_fut_cache_ts: float = 0

def _get_nfo_futures(kite) -> List[dict]:
    """Get near-month stock futures, cached 6 hours."""
    global _nfo_fut_cache, _nfo_fut_cache_ts
    if _nfo_fut_cache and time.time() - _nfo_fut_cache_ts < 21600:
        return _nfo_fut_cache
    try:
        from config import FNO_SYMBOLS
        all_inst = kite.instruments("NFO")
        today    = datetime.now(IST).date()
        # Near-month futures for our symbols
        futures  = {}
        for i in all_inst:
            name = i.get("name", "")
            if name not in FNO_SYMBOLS:
                continue
            if i.get("instrument_type") != "FUT":
                continue
            exp = i.get("expiry")
            if not exp or exp < today:
                continue
            # Keep nearest expiry per symbol
            if name not in futures or exp < futures[name]["expiry"]:
                futures[name] = i
        _nfo_fut_cache    = list(futures.values())
        _nfo_fut_cache_ts = time.time()
        logger.info(f"Near-month futures loaded: {len(_nfo_fut_cache)} symbols")
        return _nfo_fut_cache
    except Exception as e:
        logger.error(f"NFO futures instruments error: {e}")
        return []


# ─── FII / DII (from NSE — Kite does not provide this) ────────────────────────
def fetch_fii_dii() -> Optional[dict]:
    """Fetch FII and DII net cash flow from NSE participant data."""
    raw = _nse_get(f"{NSE_BASE}/api/fiidiiTradeReact")
    if not raw:
        return None
    try:
        rows = raw if isinstance(raw, list) else raw.get("data", [])
        # API returns one row per category: "FII/FPI" and "DII"
        fii_row = next((r for r in rows if "FII" in str(r.get("category", ""))), {})
        dii_row = next((r for r in rows if "DII" in str(r.get("category", ""))), {})

        def _f(row, key):
            try: return round(float(row.get(key, 0) or 0), 2)
            except: return 0.0

        return {
            "fii_net":  _f(fii_row, "netValue"),
            "fii_buy":  _f(fii_row, "buyValue"),
            "fii_sell": _f(fii_row, "sellValue"),
            "dii_net":  _f(dii_row, "netValue"),
            "dii_buy":  _f(dii_row, "buyValue"),
            "dii_sell": _f(dii_row, "sellValue"),
            "date":     fii_row.get("date") or dii_row.get("date", ""),
        }
    except Exception as e:
        logger.error(f"FII/DII parse error: {e}")
        return None
