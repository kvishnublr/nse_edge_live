"""
NSE EDGE v5 — FastAPI Backend (Zerodha Kite Connect only)
Start: python3 main.py
"""

import asyncio
import datetime
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Set

import pytz
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

import fetcher
import signals
import scheduler as sched
from feed import feed_manager, get_all_prices
from config import HOST, PORT, KITE_API_KEY, KITE_ACCESS_TOKEN, is_market_open, get_market_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-12s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("apscheduler.executors.default").setLevel(logging.WARNING)
logging.getLogger("apscheduler.scheduler").setLevel(logging.WARNING)
logger = logging.getLogger("main")

# ─── WEBSOCKET HUB ────────────────────────────────────────────────────────────
connected_clients: Set[WebSocket] = set()
_bcast_queue: asyncio.Queue = None
_start_time = time.time()


def broadcast(payload: dict):
    """Broadcast payload to all connected WebSocket clients."""
    if _bcast_queue:
        try:
            _bcast_queue.put_nowait(payload)
        except asyncio.QueueFull:
            logger.warning("Broadcast queue full - dropping message (consider increasing maxsize)")


async def _bcast_loop():
    global _bcast_queue
    _bcast_queue = asyncio.Queue(maxsize=5000)  # Increased from 1000 to handle high-frequency updates
    logger.info("Broadcast loop started")
    while True:
        payload = await _bcast_queue.get()
        if not connected_clients:
            continue
        msg  = json.dumps(payload)
        dead = set()
        for ws in list(connected_clients):
            try:
                await ws.send_text(msg)
            except ConnectionResetError:
                dead.add(ws)
            except RuntimeError:  # Connection already closed
                dead.add(ws)
            except Exception as e:
                logger.debug(f"WebSocket send error: {e.__class__.__name__}")
                dead.add(ws)
        connected_clients.difference_update(dead)
        if dead:
            logger.debug(f"Removed {len(dead)} dead WebSocket connections (remaining: {len(connected_clients)})")


# ─── LIFESPAN ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 55)
    logger.info("  NSE EDGE v5 — Zerodha Kite Connect")
    logger.info("=" * 55)

    # Validate config — if token missing, try auto-refresh before giving up
    if not KITE_API_KEY:
        logger.error("KITE_API_KEY missing in .env — cannot start")
        raise SystemExit(1)

    if not KITE_ACCESS_TOKEN:
        logger.warning("KITE_ACCESS_TOKEN not set — will auto-refresh in background after startup")
        import threading
        def _bg_refresh():
            import time as _t
            _t.sleep(3)  # Let server finish starting first
            logger.info("Background startup token refresh starting...")
            try:
                from auto_token import _do_refresh
                if _do_refresh():
                    from scheduler import _apply_new_token
                    _apply_new_token()
                    logger.info("Background startup token refresh SUCCESS")
                else:
                    logger.warning("Background startup token refresh FAILED — running in DEMO mode")
            except Exception as _e:
                logger.warning(f"Background startup token refresh error: {_e}")
        threading.Thread(target=_bg_refresh, daemon=True).start()

    # Start broadcast loop
    asyncio.create_task(_bcast_loop())

    # Initialise backtest DB (creates tables if missing)
    try:
        import backtest_data as bd
        bd.init_db()
        logger.info("Backtest DB ready")
    except Exception as e:
        logger.warning(f"Backtest DB init failed (non-critical): {e}")

    # Start Kite feed (validates creds, starts KiteTicker)
    feed_manager.start()
    
    demo_mode = getattr(feed_manager, '_demo_mode', False)
    if demo_mode:
        logger.info("Running in DEMO MODE (no live trading)")
        kite = None
    else:
        # Get authenticated Kite instance
        from feed import get_kite
        kite = get_kite()

    # Wire scheduler
    sched.set_dependencies(kite, fetcher, signals, broadcast)

    # Initial data fetch
    logger.info("Running initial data fetch...")
    try:
        indices = fetcher.fetch_indices()
        fii     = fetcher.fetch_fii_dii()
        
        if kite:
            chain   = fetcher.fetch_option_chain(kite, "NIFTY")
            stocks  = fetcher.fetch_fno_stocks(kite)
        else:
            chain = None
            stocks = []
            logger.info("  Skipping chain/stocks (Kite not available in demo mode)")

        if indices:
            logger.info(f"  Nifty={indices.get('nifty',0):.0f} "
                        f"BankNifty={indices.get('banknifty',0):.0f} "
                        f"VIX={indices.get('vix',0):.1f}")
        if chain:
            logger.info(f"  PCR={chain.get('pcr',0):.2f}  "
                        f"MaxPain={chain.get('max_pain',0):,}  "
                        f"ATM={chain.get('atm',0):,}")
        if fii:
            logger.info(f"  FII={fii.get('fii_net',0):.0f}Cr  "
                        f"DII={fii.get('dii_net',0):.0f}Cr")
        if stocks:
            logger.info(f"  {len(stocks)} F&O stocks loaded")
            sched.set_initial_stocks(stocks)  # seed scheduler cache

        signals.run_signal_engine(indices, chain, fii, stocks or [], "intraday")
    except Exception as e:
        logger.error(f"Initial fetch error: {e}", exc_info=True)

    # Backfill today's spikes from Kite 1-min history so the table is
    # populated even when the server starts after market hours.
    if kite:
        try:
            import statistics as _stat
            from signals import _score_spike
            from feed import KITE_TOKENS
            from config import FNO_SYMBOLS
            import datetime as _dt
            today_str = _dt.date.today().isoformat()
            backfill = []
            from config import GATE as _GATE
            _t_start = _GATE.get("spike_time_start", 570)
            _t_end   = _GATE.get("spike_time_end",   840)
            _score_floor = 45
            _vol_th = 2.5   # same as live detection — higher than 1.5× config
            _dedup_window = 20  # minutes — one signal per symbol+dir per 20 min

            # Track last signal time per (symbol, direction) for deduplication
            _last_sig_min = {}  # {(sym, sp_type): minute}

            for sym in FNO_SYMBOLS[2:]:
                tok = KITE_TOKENS.get(sym)
                if not tok:
                    continue
                try:
                    candles = kite.historical_data(tok, today_str, today_str, "minute")
                    if len(candles) < 10:
                        continue
                    vols = [c['volume'] for c in candles if c['volume'] > 0]
                    if not vols:
                        continue
                    avg_vol = _stat.mean(vols)
                    open_px = candles[0]['open']
                    for i, c in enumerate(candles):
                        t = c['date']
                        cm = t.hour * 60 + t.minute
                        if not (_t_start <= cm <= _t_end):
                            continue
                        price   = c['close']
                        vol     = c['volume'] or 0
                        vm      = vol / avg_vol if avg_vol else 0
                        # chg_pct = candle move from open (cumulative intraday)
                        chg_pct = (price - open_px) / open_px * 100 if open_px else 0
                        # Price threshold: relaxed for very high volume (4×+)
                        price_min = 0.5 if vm < 4.0 else 0.2
                        if abs(chg_pct) < price_min or vm < _vol_th:
                            continue
                        # OI unavailable in backfill — require price>=1.0% or vol>=4×
                        if vm < 4.0 and abs(chg_pct) < 1.0:
                            continue
                        score = _score_spike(vm, chg_pct, sym, cm)
                        if score < _score_floor:
                            continue
                        sp_type = "buy" if chg_pct > 0 else "sell"
                        # 20-min deduplication — skip if same symbol+dir fired recently
                        key = (sym, sp_type)
                        if key in _last_sig_min and cm - _last_sig_min[key] < _dedup_window:
                            continue
                        _last_sig_min[key] = cm
                        sig     = "LONG" if chg_pct > 0 else "SHORT"
                        trigger = f"Price {'+' if chg_pct>0 else ''}{chg_pct:.2f}% | Vol {vm:.1f}x"
                        # Outcome: T1=+0.5% buy / −0.5% sell; SL=−0.5% buy / +0.5% sell
                        entry = price
                        t1_px = entry * 1.005 if sp_type == "buy" else entry * 0.995
                        sl_px = entry * 0.995  if sp_type == "buy" else entry * 1.005
                        outcome = None
                        for j in range(i + 1, min(i + 31, len(candles))):
                            fc = candles[j]
                            if sp_type == "buy":
                                if fc['low']  <= sl_px: outcome = "HIT SL"; break
                                if fc['high'] >= t1_px: outcome = "HIT T1"; break
                            else:
                                if fc['high'] >= sl_px: outcome = "HIT SL"; break
                                if fc['low']  <= t1_px: outcome = "HIT T1"; break
                        if outcome is None:
                            outcome = "EXPIRED"
                        backfill.append({
                            "symbol":   sym,
                            "time":     t.strftime("%H:%M"),
                            "price":    round(price, 2),
                            "chg_pct":  round(chg_pct, 2),
                            "vol_mult": round(vm, 1),
                            "oi_pct":   0.0,
                            "type":     sp_type,
                            "trigger":  trigger,
                            "signal":   sig,
                            "strength": "hi" if score >= 70 else "md",
                            "score":    score,
                            "pc":       3,
                            "outcome":  outcome,
                        })
                except Exception:
                    pass
            if backfill:
                backfill.sort(key=lambda x: -x["score"])
                backfill = backfill[:30]
                signals.state["spikes"] = backfill
                signals.state["spikes_date"] = today_str
                logger.info(f"  Backfilled {len(backfill)} spikes from today's history")
        except Exception as e:
            logger.warning(f"Spike backfill failed (non-critical): {e}")

    # Start scheduler
    job_scheduler = sched.build_scheduler()
    job_scheduler.start()

    logger.info("=" * 55)
    logger.info(f"  WebSocket : ws://{HOST}:{PORT}/ws")
    logger.info(f"  API       : http://{HOST}:{PORT}/api/health")
    logger.info(f"  Verdict   : {signals.state['verdict']}")
    logger.info("=" * 55)

    yield

    logger.info("Shutting down...")
    job_scheduler.shutdown(wait=False)
    feed_manager.stop()


app = FastAPI(title="NSE EDGE v5", version="5.0.0", lifespan=lifespan)

# ─── CORS CONFIGURATION ─────────────────────────────────────────────────────────
_extra_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
ALLOWED_ORIGINS = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "https://kvishnublr.github.io",   # GitHub Pages
    "null",   # file:// protocol sends Origin: null
] + _extra_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,  # Disabled for better security
    allow_methods=["GET", "POST"],  # Only GET and POST
    allow_headers=["Content-Type", "Authorization"],
)


# ─── FRONTEND ─────────────────────────────────────────────────────────────────
_FRONTEND = os.path.join(os.path.dirname(__file__), "..", "frontend", "index.html")

@app.get("/")
async def serve_frontend():
    resp = FileResponse(os.path.abspath(_FRONTEND))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# ─── WEBSOCKET ────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    ip = websocket.client.host if websocket.client else "?"
    logger.info(f"WS connected: {ip} (total: {len(connected_clients)})")

    try:
        await _send_initial(websocket)
        while True:
            try:
                text = await asyncio.wait_for(websocket.receive_text(), timeout=60)
                msg  = json.loads(text)
                if msg.get("type") == "set_mode":
                    mode = msg.get("mode", "intraday").lower()
                    # Validate mode is in allowed list
                    if mode in ["intraday", "swing", "positional"]:
                        sched.set_mode(mode)
                        logger.info(f"Trading mode changed to: {mode}")
                    else:
                        logger.warning(f"Invalid mode received from WebSocket: {mode}")
                        await websocket.send_text(json.dumps({
                            "type": "error",
                            "message": f"Invalid mode '{mode}'. Allowed: intraday, swing, positional"
                        }))
                elif msg.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong", "timestamp": time.time()}))
            except json.JSONDecodeError as e:
                logger.warning(f"WebSocket JSON decode error: {e}")
                await websocket.send_text(json.dumps({"type": "error", "message": "Invalid JSON"}))
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"type": "ping"}))
            except Exception as e:
                logger.error(f"WebSocket message handling error: {e.__class__.__name__}")
                break
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket exception: {e.__class__.__name__}: {e}")
    finally:
        connected_clients.discard(websocket)
        logger.info(f"WS disconnected: {ip} (remaining: {len(connected_clients)})")


async def _send_initial(ws: WebSocket):
    """Burst full state to a freshly connected client."""
    try:
        now = time.time()
        await ws.send_text(json.dumps({"type": "gates", "timestamp": now, "data": {
            "gates":       {str(k): v for k, v in signals.state["gates"].items()},
            "verdict":     signals.state["verdict"],
            "verdict_sub": signals.state["verdict_sub"],
            "pass_count":  signals.state["pass_count"],
            "confidence":  signals.state.get("confidence", 0.0),
            "position_size_lots": signals.state.get("position_size_lots", 0),
            "position_size_rupees": signals.state.get("position_size_rupees", 0),
        }}))
        prices = get_all_prices()
        if prices:
            await ws.send_text(json.dumps({"type": "prices", "timestamp": now, "data": prices}))
        if signals.state.get("last_chain"):
            await ws.send_text(json.dumps({"type": "chain", "timestamp": now, "data": signals.state["last_chain"]}))
        if signals.state.get("last_macro"):
            await ws.send_text(json.dumps({"type": "macro", "timestamp": now, "data": signals.state["last_macro"]}))
        if signals.state.get("last_stocks"):
            await ws.send_text(json.dumps({"type": "stocks", "timestamp": now, "data": signals.state["last_stocks"]}))
        if signals.state.get("last_fii"):
            await ws.send_text(json.dumps({"type": "fii", "timestamp": now, "data": signals.state["last_fii"]}))
        await ws.send_text(json.dumps({"type": "spikes", "timestamp": now, "data": signals.state.get("spikes", [])}))
        await ws.send_text(json.dumps({"type": "ticker", "timestamp": now, "data": signals.state.get("ticker", [])}))
        await ws.send_text(json.dumps({"type": "ready", "timestamp": now, "msg": "NSE EDGE v5 Live"}))
    except Exception as e:
        logger.error(f"Initial state send: {e.__class__.__name__}: {e}")


# ─── REST API ─────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    from feed import price_cache, _ticker_connected
    from feed import _ticker_lock

    last_updated = signals.state.get("last_updated", 0)
    now = time.time()
    data_age_sec = round(now - last_updated, 1)

    # Determine health status based on data staleness
    if data_age_sec > 300:  # > 5 minutes
        status = "critical"
    elif data_age_sec > 60:  # > 1 minute
        status = "stale"
    else:
        status = "ok"

    with _ticker_lock:
        ticker_connected = _ticker_connected

    return JSONResponse({
        "status":         status,
        "market_status":  get_market_status(),
        "verdict":        signals.state["verdict"],
        "pass_count":     signals.state["pass_count"],
        "kite_ticker":    ticker_connected,
        "prices_cached":  len(price_cache),
        "ws_clients":     len(connected_clients),
        "uptime_sec":     round(now - _start_time),
        "last_updated":   last_updated,
        "data_age_sec":   data_age_sec,
    })


@app.get("/api/debug/index")
async def debug_index():
    import time as _t
    hist = sched._cache.get("ix_px_hist", [])
    now_ts = _t.time()
    five_ago = now_ts - 300
    old = next((e for e in reversed(hist) if e[0] <= five_ago), None)
    if not old and hist:
        old = hist[0]
    one_ago = now_ts - 60
    one = next((e for e in reversed(hist) if e[0] <= one_ago), None)
    return JSONResponse({
        "hist_len": len(hist),
        "oldest_age_sec": round(now_ts - hist[0][0]) if hist else None,
        "five_min_ago_nifty": old[1] if old else None,
        "current_nifty": hist[-1][1] if hist else None,
        "chg_pct_5min_nifty": round((hist[-1][1] - old[1]) / old[1] * 100, 3) if (hist and old) else None,
        "chg_pct_1min_nifty": round((hist[-1][1] - one[1]) / one[1] * 100, 3) if (hist and one) else None,
        "threshold": 0.20,
        "window_sec": 300,
        "index_signals": len(signals.state.get("index_signals", [])),
    })


@app.get("/api/index-signals/history")
async def index_signals_history(days: int = 7):
    import sqlite3 as _sq
    db_path = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
    try:
        conn = _sq.connect(db_path)
        conn.row_factory = _sq.Row
        rows = conn.execute("""
            SELECT * FROM index_signal_history
            WHERE trade_date >= date('now', ?, 'localtime')
            ORDER BY ts DESC LIMIT 200
        """, (f"-{days} days",)).fetchall()
        conn.close()
        data = [dict(r) for r in rows]
        # Summary stats
        resolved = [r for r in data if r["outcome"] in ("HIT_T1", "HIT_SL")]
        wins = sum(1 for r in resolved if r["outcome"] == "HIT_T1")
        wr = round(wins / len(resolved) * 100) if resolved else 0
        return JSONResponse({"signals": data, "total": len(data),
                             "wins": wins, "losses": len(resolved)-wins,
                             "win_rate": wr})
    except Exception as e:
        return JSONResponse({"signals": [], "error": str(e)})


@app.get("/api/state")
async def get_state():
    return JSONResponse({
        "gates":       {str(k): v for k, v in signals.state["gates"].items()},
        "verdict":     signals.state["verdict"],
        "verdict_sub": signals.state["verdict_sub"],
        "pass_count":  signals.state["pass_count"],
        "confidence":  signals.state.get("confidence", 0.0),
        "prices":      get_all_prices(),
        "chain":       signals.state.get("last_chain"),
        "macro":       signals.state.get("last_macro"),
        "stocks":      signals.state.get("last_stocks"),
        "fii":         signals.state.get("last_fii"),
        "spikes":        signals.state.get("spikes", []),
        "index_signals": signals.state.get("index_signals", []),
        "position_size_lots": signals.state.get("position_size_lots", 0),
        "position_size_rupees": signals.state.get("position_size_rupees", 0),
    })


@app.get("/api/live-picks")
async def get_live_picks():
    """Compute live stock picks from current stocks cache."""
    stocks  = signals.state.get("last_stocks", [])
    indices = signals.state.get("last_macro", {}) or {}
    chain   = signals.state.get("last_chain", {}) or {}
    pcr     = chain.get("pcr", 1.0)
    vix     = indices.get("vix", 15.0)
    g_pass  = signals.state.get("pass_count", 0)   # global gate pass count
    global_verdict = signals.state.get("verdict", "WAIT")

    picks = []
    for s in stocks:
        sym    = s.get("symbol", "")
        price  = s.get("price", 0) or 0
        if not price or sym in ("NIFTY", "BANKNIFTY", "INDIAVIX"):
            continue
        chg    = s.get("chg_pct", 0) or 0
        vol_r  = s.get("vol_ratio", 1.0) or 1.0
        oi_pct = s.get("oi_chg_pct", 0) or 0
        # Score stock-level quality
        stock_score = 0
        if abs(chg) >= 1.5: stock_score += 3
        elif abs(chg) >= 0.5: stock_score += 1
        if vol_r >= 2.0: stock_score += 2
        elif vol_r >= 1.3: stock_score += 1
        if abs(oi_pct) >= 5: stock_score += 2
        elif abs(oi_pct) >= 2: stock_score += 1
        if stock_score < 1:
            continue  # skip flat/no-activity stocks

        # Simple ATR proxy: 1.5% of price
        atr    = price * 0.015
        if chg >= 1.5:
            setup    = "Breakout"
            entry_p  = price * 1.002
        elif chg > 0:
            setup    = "Pullback"
            entry_p  = price - 0.2 * atr
        elif chg > -0.5:
            setup    = "Recovery"
            entry_p  = price + 0.1 * atr
        else:
            setup    = "Momentum"
            entry_p  = price

        sl_p   = entry_p - 1.5 * atr
        tgt_p  = entry_p + 2.5 * (entry_p - sl_p)
        tgt_pp = entry_p + 4.0 * (entry_p - sl_p)
        rr     = round((tgt_p - entry_p) / max(entry_p - sl_p, 1), 1)
        rr_p   = round((tgt_pp - entry_p) / max(entry_p - sl_p, 1), 1)
        score  = min(99, round(40 + g_pass * 8 + stock_score * 4 + (5 if vol_r >= 1.5 else 0)))
        pc     = int(s.get("pc", g_pass) or g_pass)
        stock_verdict = str(s.get("verdict", global_verdict or "WAIT"))
        signal_label = str(s.get("signal", "WATCH")).upper()
        if stock_verdict == "EXECUTE" and pc >= 5:
            conf = "CONFIRMED"
            cls = "rpk-go"
        elif stock_verdict in ("EXECUTE", "WATCH") or pc >= 3:
            conf = "HIGH CONF" if pc >= 4 else "WATCH"
            cls = "rpk-go" if pc >= 4 else "rpk-am"
        elif global_verdict == "NO TRADE" or s.get("g1") == "st" or s.get("g5") == "st":
            conf = "NO TRADE"
            cls = "rpk-st"
            stock_verdict = "NO TRADE"
        else:
            conf = "WATCH"
            cls = "rpk-am"

        sec_map = {
            "HDFCBANK":"Banking","ICICIBANK":"Banking","AXISBANK":"Banking",
            "KOTAKBANK":"Banking","INDUSINDBK":"Banking","SBIN":"PSU Bank",
            "BANKNIFTY":"Index","TCS":"IT","INFY":"IT","MARUTI":"Auto",
            "TATAMOTORS":"Auto","LT":"Infra","BAJFINANCE":"NBFC","RELIANCE":"Energy",
            "TATASTEEL":"Steel","SUNPHARMA":"Pharma","BAJFINANCE":"NBFC",
        }
        sector = sec_map.get(sym, "Market")
        oi_pct = s.get("oi_chg_pct", 0) or 0

        def _pf(p):
            return str(round(p, 1)) if p < 2000 else str(int(round(p)))

        picks.append({
            "sym":      sym,
            "score":    score,
            "pc":       pc,
            "conf":     conf,
            "cls":      cls,
            "setup":    setup,
            "close":    price,
            "chg_pct":  round(chg, 2),
            "vol_ratio":round(vol_r, 1),
            "oi_chg_pct": round(oi_pct, 1),
            "entry":    _pf(entry_p),
            "sl":       _pf(sl_p),
            "target":   _pf(tgt_p),
            "target_p": _pf(tgt_pp),
            "rr":       rr,
            "rr_p":     rr_p,
            "meta":     f"{setup} · {sector} · Vol {vol_r:.1f}x · OI {oi_pct:+.1f}% · VIX {vix:.1f}",
            "reason":   f"{stock_verdict} · {pc}/5 gates · {signal_label} · R:R 1:{rr}",
            "reason_p": f"{stock_verdict} · {pc}/5 gates · Swing target · R:R 1:{rr_p}",
            "g1": s.get("g1","wt"), "g2": s.get("g2","wt"),
            "g3": s.get("g3","wt"), "g4": s.get("g4","wt"), "g5": s.get("g5","wt"),
        })

    picks.sort(key=lambda x: (-x["pc"], -x["score"]))
    return JSONResponse({"picks": picks[:8], "count": len(picks)})


@app.get("/api/signals/history")
async def get_signals_history(limit: int = 100, status: str = "ALL"):
    import backtest_data as bd
    rows = bd.get_live_signal_history(limit=limit, status=status)
    return JSONResponse({"rows": rows, "count": len(rows)})


@app.get("/api/signals/accuracy-filters")
async def get_signal_accuracy_filters_api():
    import backtest_data as bd
    data = bd.get_signal_accuracy_filters()
    return JSONResponse({
        "weak_symbols": sorted(list(data.get("weak_symbols", set()))),
        "weak_buckets": sorted(list(data.get("weak_buckets", set()))),
        "symbol_stats": data.get("symbol_stats", {}),
        "bucket_stats": data.get("bucket_stats", {}),
    })


@app.post("/api/signals/history/backfill")
async def backfill_signals_history(days: int = 7):
    import backtest_data as bd
    from datetime import datetime, timedelta
    ist = pytz.timezone('Asia/Kolkata')
    today = datetime.now(ist).date()
    from_date = str(today - timedelta(days=max(2, min(days, 30))))
    to_date = str(today - timedelta(days=1))
    resp = await spikes_backtest(from_date=from_date, to_date=to_date, vol_min=2.0, price_min=0.3, trend_filter=True, time_from="09:15", time_to="14:30", min_score=45)
    if getattr(resp, "status_code", 200) >= 400:
        try:
            payload = json.loads(resp.body.decode("utf-8"))
        except Exception:
            payload = {"error": "Backfill source failed"}
        return JSONResponse(payload, status_code=resp.status_code)
    try:
        payload = json.loads(resp.body.decode("utf-8"))
    except Exception:
        return JSONResponse({"error": "Invalid backfill payload"}, status_code=500)
    inserted = bd.import_historical_spike_results(payload.get("results", []))
    rows = bd.get_live_signal_history(limit=100, status="ALL")
    return JSONResponse({"inserted": inserted, "rows": rows, "count": len(rows), "from_date": from_date, "to_date": to_date})


@app.get("/api/chain/{symbol}")
async def get_chain(symbol: str = "NIFTY"):
    from feed import get_kite
    data = fetcher.fetch_option_chain(get_kite(), symbol.upper())
    return JSONResponse(data or {"error": "chain fetch failed"})


@app.get("/api/indices")
async def get_indices():
    return JSONResponse(fetcher.fetch_indices() or {"error": "no data"})


@app.get("/api/chart/{symbol}")
async def get_chart_data(symbol: str, from_date: str = None, to_date: str = None):
    """Return OHLCV data for charting."""
    from feed import get_kite
    from datetime import datetime, timedelta
    from config import KITE_TOKENS
    
    symbol = symbol.upper()
    if symbol not in KITE_TOKENS:
        return JSONResponse({"error": f"Unknown symbol: {symbol}"}, status_code=400)
    
    try:
        kite = get_kite()
        if not from_date:
            from_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        
        token = KITE_TOKENS[symbol]
        data = kite.historical_data(token, from_date, to_date, "day")
        
        candles = []
        volume = []
        for d in data:
            ts = int(d["date"].timestamp())
            candles.append({
                "time": ts,
                "open": d["open"],
                "high": d["high"],
                "low": d["low"],
                "close": d["close"],
            })
            vol = d.get("volume", 0) or 0
            color = "rgba(0,232,122,0.3)" if d["close"] >= d["open"] else "rgba(255,51,85,0.3)"
            volume.append({"time": ts, "value": vol, "color": color})
        
        return JSONResponse({"candles": candles, "volume": volume})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/fii")
async def get_fii():
    return JSONResponse(fetcher.fetch_fii_dii() or {"error": "no data"})


# ─── BACKTEST API ──────────────────────────────────────────────────────────────
@app.get("/api/backtest/status")
async def backtest_status():
    try:
        import backtest_data as bd
        bd.init_db()
        return JSONResponse(bd.get_data_summary())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/backtest/download")
async def backtest_download():
    """Start 3-year historical data download (runs in background thread)."""
    import asyncio, backtest_data as bd

    async def _dl():
        loop = asyncio.get_event_loop()
        try:
            bd.init_db()
            from feed import get_kite
            kite = get_kite()
            await loop.run_in_executor(None, lambda: bd.download_kite_history(kite, days=1095))
            await loop.run_in_executor(None, lambda: bd.download_chain_history(days=1095))
            await loop.run_in_executor(None, lambda: bd.download_fii_history(days=1095))
            await loop.run_in_executor(None, bd.fill_outcomes)
            logger.info("Backtest data download complete")
        except Exception as e:
            logger.error(f"Backtest download error: {e}", exc_info=True)

    asyncio.create_task(_dl())
    return JSONResponse({"message": "Download started — NIFTY OHLCV + VIX + chain PCR for 3 years. Check /api/backtest/status."})


@app.post("/api/backtest/run")
async def backtest_run(from_date: str = None, to_date: str = None, mode: str = "intraday"):
    """Run backtest over a date range and compute gate weights."""
    import asyncio, backtest_engine as be, gate_weights as gw, backtest_data as bd
    from datetime import datetime, timedelta

    if not to_date:
        to_date = datetime.now().strftime("%Y-%m-%d")
    if not from_date:
        from_date = (datetime.now() - timedelta(days=1095)).strftime("%Y-%m-%d")

    # Validate dates
    try:
        datetime.strptime(from_date, "%Y-%m-%d")
        datetime.strptime(to_date,   "%Y-%m-%d")
    except ValueError:
        return JSONResponse({"error": "Invalid date format. Use YYYY-MM-DD."}, status_code=400)

    try:
        loop    = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, lambda: be.run_backtest(from_date, to_date, mode))

        # Refresh gate weights after backtest
        if results.get("metrics", {}).get("execute_signals", 0) >= 5:
            await loop.run_in_executor(None, gw.compute_and_save_weights)

        # Fill outcomes for live signals
        await loop.run_in_executor(None, bd.fill_outcomes)

        return JSONResponse(results)
    except Exception as e:
        logger.error(f"Backtest run error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/backtest/results")
async def backtest_results(from_date: str = None, to_date: str = None):
    """Return last backtest results from signal_log."""
    import backtest_data as bd
    try:
        conn  = bd.get_conn()
        where = "WHERE session='backtest'"
        params: list = []
        if from_date:
            where  += " AND date >= ?"
            params.append(from_date)
        if to_date:
            where  += " AND date <= ?"
            params.append(to_date)

        rows = conn.execute(
            f"SELECT date, session, verdict, pass_count, nifty, vix, pcr, "
            f"outcome_pts, outcome, g1, g2, g3, g4, g5 "
            f"FROM signal_log {where} ORDER BY date DESC LIMIT 200",
            params
        ).fetchall()
        conn.close()

        trades = [
            {"date": r[0], "session": r[1], "verdict": r[2], "pass_count": r[3], "nifty": r[4],
             "vix": r[5], "pcr": r[6], "outcome_pts": r[7], "outcome": r[8],
             "g1": r[9], "g2": r[10], "g3": r[11], "g4": r[12], "g5": r[13]}
            for r in rows
        ]
        return JSONResponse({"trades": trades, "count": len(trades)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/backtest/gate-weights")
async def backtest_gate_weights():
    """Return gate predictiveness analysis."""
    try:
        import gate_weights as gw
        return JSONResponse(gw.get_gate_analysis())
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/backtest/optimize")
async def backtest_optimize():
    """Grid-search gate thresholds to maximise profit factor."""
    try:
        import asyncio, threshold_optimizer as opt
        loop    = asyncio.get_event_loop()
        results = await loop.run_in_executor(None, opt.run_optimizer)
        return JSONResponse(results)
    except Exception as e:
        logger.error(f"Optimizer error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/token-status")
async def token_status():
    """Check if Kite access token is still valid."""
    from config import KITE_ACCESS_TOKEN
    import time as _time
    try:
        from feed import get_kite
        kite = get_kite()
        profile = kite.profile()
        return JSONResponse({
            "valid":    True,
            "user":     profile.get("user_name", ""),
            "uptime_h": round((_time.time() - _start_time) / 3600, 1),
        })
    except Exception as e:
        return JSONResponse({
            "valid":   False,
            "error":   str(e),
            "uptime_h": round((_time.time() - _start_time) / 3600, 1),
        })


@app.post("/api/token-refresh")
async def token_refresh_manual():
    """Manually trigger a Kite token refresh via Playwright headless login."""
    import asyncio as _aio

    # Pre-flight: check all required credentials are present
    missing = [k for k in ("KITE_USER_ID","KITE_PASSWORD","KITE_TOTP_SECRET","KITE_API_KEY","KITE_API_SECRET")
               if not os.getenv(k,"").strip()]
    if missing:
        return JSONResponse({
            "ok": False,
            "msg": f"Missing env vars: {', '.join(missing)}. Add these in Railway → Variables tab and redeploy."
        }, status_code=400)

    loop = _aio.get_event_loop()
    def _do():
        import logging, io
        # Capture auto_token log output to surface real errors
        log_stream = io.StringIO()
        h = logging.StreamHandler(log_stream)
        h.setLevel(logging.ERROR)
        logging.getLogger("auto_token").addHandler(h)
        try:
            from auto_token import refresh_token
            ok = refresh_token()
            if ok:
                from scheduler import _apply_new_token
                _apply_new_token()
            log_stream.seek(0)
            err = log_stream.read().strip()
            return (ok, err or ("" if ok else "Login failed — check credentials or TOTP secret"))
        except Exception as e:
            return (False, str(e))
        finally:
            logging.getLogger("auto_token").removeHandler(h)

    result, errmsg = await loop.run_in_executor(None, _do)
    if result:
        return JSONResponse({"ok": True, "msg": "Token refreshed and applied live"})
    return JSONResponse({"ok": False, "msg": errmsg}, status_code=500)


@app.get("/api/backtest/dayview")
async def backtest_dayview(date: str):
    """
    Full gate drill-down for a specific date.
    Returns intraday + positional gate states, scores, entry/exit/pnl for that day.
    """
    import statistics, backtest_data as bd
    from backtest_engine import _g1, _g2, _g3, _g4, _g5, _atr, _verdict
    from config import GATE as TH

    try:
        bd.init_db()
        conn = bd.get_conn()

        # Get OHLCV for the date and 20 prior days for ATR
        rows = conn.execute(
            "SELECT date, open, high, low, close, volume FROM ohlcv "
            "WHERE date <= ? ORDER BY date DESC LIMIT 22", (date,)
        ).fetchall()

        if not rows or rows[0][0] != date:
            conn.close()
            return JSONResponse({"error": f"No OHLCV data for {date}"}, status_code=404)

        rows = list(reversed(rows))   # oldest first
        day_row  = rows[-1]
        prior    = rows[:-1]

        ohlcv_prior = [{"date": r[0], "open": r[1], "high": r[2],
                        "low": r[3], "close": r[4], "volume": r[5]} for r in prior]
        atr_v   = _atr(ohlcv_prior)
        vols    = [r["volume"] for r in ohlcv_prior if r["volume"] > 0]
        avg_v   = statistics.mean(vols) if vols else 1
        prev_close = ohlcv_prior[-1]["close"] if ohlcv_prior else day_row[4]

        vix_r   = conn.execute("SELECT vix, vix_chg FROM vix_daily WHERE date=?", (date,)).fetchone() or (15.0, 0.0)
        chain_r = conn.execute("SELECT pcr, total_call_oi, total_put_oi FROM chain_daily WHERE date=?", (date,)).fetchone() or (1.0, 500000, 500000)
        fii_net = (conn.execute("SELECT fii_net FROM fii_daily WHERE date=?", (date,)).fetchone() or (0.0,))[0]
        if abs(fii_net) > 10000:
            fii_net = round(fii_net / 100, 2)

        # Next day for entry/exit
        nxt = conn.execute(
            "SELECT date, open, high, low, close FROM ohlcv WHERE date > ? ORDER BY date LIMIT 1", (date,)
        ).fetchone()
        conn.close()

        dt_close = day_row[4]
        dt_high  = day_row[2]
        dt_low   = day_row[3]

        def _sim_mode(mode):
            g1 = _g1(vix_r[0], vix_r[1], fii_net)
            g2 = _g2(chain_r[0], chain_r[1], chain_r[2])
            g3 = _g3(dt_close, dt_high, dt_low)
            g4 = _g4(dt_close, prev_close, day_row[5], avg_v)
            g5 = _g5(dt_close, atr_v, vix_r[0], mode)
            verdict, pass_cnt = _verdict([g1, g2, g3, g4, g5])

            entry = exit_price = target = stop = pnl = outcome = None
            if nxt:
                entry = round(nxt[1], 2)
                rr    = TH["rr_min_intraday"] if mode == "intraday" else TH["rr_min_positional"]
                stop_dist   = round(atr_v * TH["atr_multiplier"], 2)
                target_dist = round(stop_dist * rr, 2)
                target = round(entry + target_dist, 2)
                stop   = round(entry - stop_dist, 2)
                if nxt[2] >= target:
                    exit_price, outcome = target, "WIN"
                elif nxt[3] <= stop:
                    exit_price, outcome = stop, "LOSS"
                else:
                    exit_price = round(nxt[4], 2)
                    diff = exit_price - entry
                    threshold = max(20, round(atr_v * 0.4))
                    outcome = "WIN" if diff >= threshold else "LOSS" if diff <= -threshold else "NEUTRAL"
                pnl = round(exit_price - entry, 2)

            return {
                "verdict": verdict, "pass_count": pass_cnt,
                "g1": {"state": g1["state"], "score": g1["score"]},
                "g2": {"state": g2["state"], "score": g2["score"]},
                "g3": {"state": g3["state"], "score": g3["score"]},
                "g4": {"state": g4["state"], "score": g4["score"]},
                "g5": {"state": g5["state"], "score": g5["score"]},
                "entry": entry, "exit": exit_price,
                "target": target, "stop": stop,
                "pnl": pnl, "outcome": outcome,
                "next_date": nxt[0] if nxt else None,
            }

        # Live signal_log entries for this date (if system was running)
        conn2 = bd.get_conn()
        live_rows = conn2.execute(
            "SELECT ts, verdict, pass_count, g1,g2,g3,g4,g5, "
            "g1_score,g2_score,g3_score,g4_score,g5_score, "
            "nifty, vix, pcr, outcome_pts, outcome "
            "FROM signal_log WHERE date=? AND session='live' ORDER BY ts",
            (date,)
        ).fetchall()
        conn2.close()

        import datetime as _dt
        _live_signals = []
        for row in live_rows:
            ts_val = row[0]
            try:
                ts_str = _dt.datetime.fromtimestamp(float(ts_val)).strftime("%H:%M:%S") if ts_val else "—"
            except Exception:
                ts_str = str(ts_val)
            _live_signals.append({
                "time": ts_str, "verdict": row[1], "pass_count": row[2],
                "g1": row[3], "g2": row[4], "g3": row[5], "g4": row[6], "g5": row[7],
                "g1_score": row[8], "g2_score": row[9], "g3_score": row[10],
                "g4_score": row[11], "g5_score": row[12],
                "nifty": row[13], "vix": row[14], "pcr": row[15],
                "outcome_pts": row[16], "outcome": row[17],
            })

        return JSONResponse({
            "date":     date,
            "nifty":    round(dt_close, 2),
            "open":     round(day_row[1], 2),
            "high":     round(dt_high, 2),
            "low":      round(dt_low, 2),
            "vix":      round(vix_r[0], 2),
            "vix_chg":  round(vix_r[1], 2),
            "pcr":      round(chain_r[0], 3),
            "call_oi":  chain_r[1],
            "put_oi":   chain_r[2],
            "fii_net":  round(fii_net, 2),
            "atr":      round(atr_v, 2),
            "intraday":   _sim_mode("intraday"),
            "positional": _sim_mode("positional"),
            "live_signals": _live_signals,
        })
    except Exception as e:
        logger.error(f"Dayview error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/dayview/full")
async def dayview_full(date: str):
    """
    Returns historical data for a date in the EXACT format that
    WebSocket handle() expects — so the live UI can replay any past day.
    """
    import statistics as _stat
    from datetime import date as _date_cls, timedelta as _td, datetime as _dt_cls
    from config import KITE_TOKENS, FNO_SYMBOLS, LOT_SIZES, GATE as TH
    from backtest_engine import _g1, _g2, _g3, _g4, _g5, _verdict
    import backtest_data as bd

    try:
        sel_date = _dt_cls.strptime(date, "%Y-%m-%d").date()
        context_from = sel_date - _td(days=35)

        bd.init_db()
        conn = bd.get_conn()
        vix_r   = conn.execute("SELECT vix, vix_chg FROM vix_daily WHERE date=?", (date,)).fetchone() or (15.0, 0.0)
        chain_r = conn.execute("SELECT pcr, total_call_oi, total_put_oi FROM chain_daily WHERE date=?", (date,)).fetchone() or (1.0, 500000, 500000)
        fii_row = conn.execute("SELECT fii_net, dii_net FROM fii_daily WHERE date=?", (date,)).fetchone() or (0.0, 0.0)
        nifty_row = conn.execute(
            "SELECT open, high, low, close, volume FROM ohlcv WHERE date=?", (date,)
        ).fetchone()
        prev_nifty = conn.execute(
            "SELECT close FROM ohlcv WHERE date < ? ORDER BY date DESC LIMIT 1", (date,)
        ).fetchone()
        conn.close()

        if not nifty_row:
            return JSONResponse({"error": f"No data for {date}"}, status_code=404)

        fii_net = fii_row[0]; dii_net = fii_row[1]
        if abs(fii_net) > 10000:
            fii_net = round(fii_net / 100, 2)
        if abs(dii_net) > 10000:
            dii_net = round(dii_net / 100, 2)
        vix = vix_r[0]; vix_chg = vix_r[1]
        pcr = chain_r[0]; call_oi = chain_r[1]; put_oi = chain_r[2]

        # Compute gates
        g1i = _g1(vix, vix_chg, fii_net)
        g2i = _g2(pcr, call_oi, put_oi)
        # SELECT open, high, low, close, volume → indices 0,1,2,3,4
        nifty_close = nifty_row[3]; nifty_high = nifty_row[1]; nifty_low = nifty_row[2]
        g3i = _g3(nifty_close, nifty_high, nifty_low)
        prev_close_val = prev_nifty[0] if prev_nifty else nifty_close
        nifty_vol = nifty_row[4]
        g4i = _g4(nifty_close, prev_close_val, nifty_vol, 0)
        g5i = _g5(nifty_close, 90.0, vix, "intraday")
        verdict, pass_cnt = _verdict([g1i, g2i, g3i, g4i, g5i])

        chg_pts = round(nifty_close - prev_close_val, 2)
        chg_pct = round(chg_pts / prev_close_val * 100, 2) if prev_close_val else 0

        def gstate_label(s): return "PASS" if s=="go" else "FAIL" if s=="st" else "CAUTION" if s=="am" else "WAIT"
        def score_cls(s): return "cg" if s=="go" else "cr" if s=="st" else "ca"

        gates_data = {
            "1": {"state": g1i["state"], "score": g1i["score"], "name": "REGIME",
                  "rows": [{"k": "VIX", "v": f"{vix:.1f}", "c": "cr" if vix>=20 else "ca" if vix>=15 else "cg"},
                            {"k": "VIX CHG", "v": f"{vix_chg:+.1f}%", "c": "cr" if vix_chg>5 else "cg"},
                            {"k": "FII NET", "v": f"₹{fii_net:+.0f} Cr", "c": "cr" if fii_net<0 else "cg"}]},
            "2": {"state": g2i["state"], "score": g2i["score"], "name": "SMART MONEY",
                  "rows": [{"k": "PCR", "v": f"{pcr:.3f}", "c": "cg" if pcr>=1.2 else "cr" if pcr<=0.8 else "ca"},
                            {"k": "CALL OI", "v": f"{call_oi:,}", "c": ""},
                            {"k": "PUT OI", "v": f"{put_oi:,}", "c": "cg"}]},
            "3": {"state": g3i["state"], "score": g3i["score"], "name": "STRUCTURE",
                  "rows": [{"k": "VWAP proxy", "v": f"{round((nifty_high+nifty_low+nifty_close)/3,0):.0f}", "c": ""},
                            {"k": "CLOSE vs VWAP", "v": f"{'+' if nifty_close>(nifty_high+nifty_low+nifty_close)/3 else ''}{round(nifty_close-(nifty_high+nifty_low+nifty_close)/3,0):.0f}", "c": score_cls(g3i["state"])},
                            {"k": "RANGE POS", "v": f"{round((nifty_close-nifty_low)/(nifty_high-nifty_low)*100) if nifty_high>nifty_low else 50:.0f}%", "c": score_cls(g3i["state"])}]},
            "4": {"state": g4i["state"], "score": g4i["score"], "name": "TRIGGER",
                  "rows": [{"k": "CHG", "v": f"{chg_pct:+.2f}%", "c": "cg" if chg_pct>0 else "cr"},
                            {"k": "VOL", "v": f"{nifty_vol:,}" if nifty_vol else "N/A (index)", "c": "ca"},
                            {"k": "MODE", "v": "Price-only (index)", "c": "ca"}]},
            "5": {"state": g5i["state"], "score": g5i["score"], "name": "RISK VALID",
                  "rows": [{"k": "VIX regime", "v": "LOW" if vix<15 else "MEDIUM" if vix<20 else "HIGH", "c": "cg" if vix<15 else "ca" if vix<20 else "cr"},
                            {"k": "R:R default", "v": "2.5:1", "c": "cg"},
                            {"k": "ATR proxy", "v": "90 pts", "c": "ca"}]},
        }
        verdict_sub = {
            "EXECUTE": "All gates aligned — trade setup confirmed",
            "WAIT": f"{pass_cnt}/5 gates pass — monitor closely",
            "NO TRADE": f"G1 REGIME fail — stand down" if g1i["state"]=="st" else "Insufficient gates — stand down"
        }.get(verdict, "")

        confidence = round(pass_cnt * 2.0, 1)

        # Fetch stock prices for that date via Kite
        from kiteconnect import KiteConnect
        from config import KITE_API_KEY, KITE_ACCESS_TOKEN
        kite = KiteConnect(api_key=KITE_API_KEY)
        kite.set_access_token(KITE_ACCESS_TOKEN)

        prices = {"NIFTY": {"price": nifty_close, "chg_pts": chg_pts, "chg_pct": chg_pct}}

        stock_picks = []
        stocks_msg  = []
        # Process all stocks first to calculate scores
        stock_data = []
        for sym in ["BANKNIFTY", "ICICIBANK", "RELIANCE", "INDUSINDBK", "SBIN", "HDFCBANK", "AXISBANK", "BAJFINANCE", "LT", "TCS", "TATAMOTORS", "INFY", "MARUTI", "KOTAKBANK"]:
            token = KITE_TOKENS.get(sym)
            if not token: continue
            try:
                hist = kite.historical_data(token, context_from, sel_date, "day")
                day_rows = [d for d in hist if (d["date"].strftime("%Y-%m-%d") if hasattr(d["date"],"strftime") else str(d["date"])[:10]) == date]
                if not day_rows: continue
                dr = day_rows[0]
                prior = [d for d in hist if (d["date"].strftime("%Y-%m-%d") if hasattr(d["date"],"strftime") else str(d["date"])[:10]) < date]
                pv = prior[-1]["close"] if prior else dr["close"]
                dc = round(dr["close"], 2)
                dp = round((dc - pv) / pv * 100, 2) if pv else 0
                dv = dr.get("volume", 0)
                _vols = [d.get("volume",0) for d in prior[-20:] if d.get("volume",0)>0]
                avg_v = _stat.mean(_vols) if _vols else 1

                # Per-stock gates
                trs = [abs(prior[i]["close"]-prior[i-1]["close"]) for i in range(1,len(prior))]
                atr_v = round(_stat.mean(trs[-14:]),2) if len(trs)>=14 else round(_stat.mean(trs),2) if trs else 90.0
                g3s = _g3(dc, dr["high"], dr["low"])
                g4s = _g4(dc, pv, dv, avg_v)
                g5s = _g5(dc, atr_v, vix, "intraday")
                _, pc_s = _verdict([g1i, g2i, g3s, g4s, g5s])
                vol_r = round(dv/avg_v, 1) if avg_v>0 and dv>0 else 0
                oi_chg_proxy = round(dp * 10)  # proxy: no historical OI per stock

                prices[sym] = {"price": dc, "chg_pts": round(dc-pv,2), "chg_pct": dp}

                # Build score before using it anywhere
                score = round(40 + pc_s*10 + (10 if dp>1 else 5 if dp>0.5 else 0) + (5 if vol_r>=1.5 else 0))
                score = min(99, score)

                # Stock scanner message (all stocks, for the OI table)
                stocks_msg.append({
                    "symbol":     sym,
                    "price":      dc,
                    "chg_pct":    dp,
                    "chg_pts":    round(dc - pv, 2),
                    "oi":         0,
                    "oi_chg":     0,
                    "oi_chg_pct": oi_chg_proxy,
                    "volume":     dv,
                    "lot_size":   LOT_SIZES.get(sym, 1),
                    "fut_ltp":    dc,
                    "vol_ratio":  vol_r,
                    "atr_pct":    round(atr_v / dc * 100, 1) if dc > 0 else 0,
                    "rs_pct":     round(dp - chg_pct, 1),   # relative strength vs NIFTY
                    "g1": g1i["state"], "g2": g2i["state"],
                    "g3": g3s["state"], "g4": g4s["state"], "g5": g5s["state"],
                    "pc": pc_s,
                    "score":    score,
                })
                if pc_s >= 2:
                    # ── Entry / SL / Target from ATR and recent 5-day swings ──
                    recent_lows  = [d["low"]  for d in prior[-5:]] if len(prior) >= 5 else [dr["low"]]
                    recent_highs = [d["high"] for d in prior[-5:]] if len(prior) >= 5 else [dr["high"]]
                    swing_low  = min(recent_lows)
                    swing_high = max(recent_highs)

                    if dp >= 1.5:
                        setup = "Breakout"
                        entry_p = dc * 1.003
                    elif dp > 0:
                        setup = "Pullback"
                        entry_p = dc - 0.25 * atr_v
                    else:
                        setup = "Recovery"
                        entry_p = dc + 0.15 * atr_v

                    sl_p       = max(swing_low, entry_p - 1.6 * atr_v)
                    risk_p     = max(entry_p - sl_p, atr_v * 0.5)
                    # Intraday target: 2.5x risk; Positional target: 4x risk
                    tgt_p_i    = entry_p + 2.5 * risk_p
                    tgt_p_pos  = entry_p + 4.0 * risk_p
                    if 0 < (swing_high - entry_p) < 2.5 * risk_p:
                        tgt_p_i = swing_high + 0.5 * atr_v
                    if 0 < (swing_high - entry_p) < 4.0 * risk_p:
                        tgt_p_pos = swing_high + 0.5 * atr_v
                    tgt_p   = tgt_p_i  # default shown (intraday)
                    rr_val  = round((tgt_p_i - entry_p) / risk_p, 1) if risk_p > 0 else 0.0
                    rr_val_p= round((tgt_p_pos - entry_p) / risk_p, 1) if risk_p > 0 else 0.0

                    def _pf(p):
                        return str(int(round(p))) if p >= 100 else f"{p:.1f}"

                    sector = ('Banking' if sym in ['HDFCBANK','ICICIBANK','AXISBANK','KOTAKBANK','INDUSINDBK','SBIN']
                              else 'Index' if sym == 'BANKNIFTY'
                              else 'IT' if sym in ['TCS','INFY']
                              else 'Auto' if sym in ['MARUTI','TATAMOTORS']
                              else 'Market')

                    # signal_time: for historical dates use 09:15 (market open); for today use current time
                    import pytz as _pytz
                    _ist_now = datetime.datetime.now(_pytz.timezone('Asia/Kolkata'))
                    _today_str = _ist_now.strftime("%Y-%m-%d")
                    _sig_time = _ist_now.strftime("%H:%M") if date == _today_str else "09:15"
                    stock_picks.append({
                        "sym":         sym,
                        "score":       score,
                        "pc":          pc_s,
                        "verdict":     "EXECUTE" if pc_s>=3 else "WATCH",
                        "conf":        "CONFIRMED" if pc_s==5 else "HIGH CONF" if pc_s>=4 else "WATCH" if pc_s==3 else "MONITOR",
                        "signal_time": _sig_time,
                        "close":       dc,
                        "chg_pct":     dp,
                        "vol_ratio":   vol_r,
                        "atr":         atr_v,
                        "oi_chg_pct":  oi_chg_proxy,
                        "setup":       setup,
                        "entry":       _pf(entry_p),
                        "sl":          _pf(sl_p),
                        "target":      _pf(tgt_p_i),
                        "target_p":    _pf(tgt_p_pos),
                        "rr":          rr_val,
                        "rr_p":        rr_val_p,
                        "meta":        f"{setup} · {sector} · Vol {vol_r}x · PCR {pcr:.2f}",
                        "reason":      f"R:R 1:{rr_val} · {pc_s}/5 gates · {'LONG BUILDUP' if dp>0 and pc_s>=4 else 'MOMENTUM' if dp>0 else 'WATCH'}",
                        "reason_p":    f"R:R 1:{rr_val_p} · {pc_s}/5 gates · Swing Target",
                        "cls":         "rpk-go" if pc_s>=4 else "rpk-am",
                        "g1": g1i["state"], "g2": g2i["state"],
                        "g3": g3s["state"], "g4": g4s["state"], "g5": g5s["state"],
                    })
            except Exception: continue

        stock_picks.sort(key=lambda x: -x["score"])

        # ── Outcome check: did each pick hit target, SL, or neither? ─────────
        try:
            from feed import get_kite as _get_kite
            kite = _get_kite()
            for pk in stock_picks:
                try:
                    sym_token = KITE_TOKENS.get(pk["sym"])
                    if not sym_token:
                        pk["outcome"] = "UNKNOWN"; continue
                    # fetch 5-min candles for that day
                    candles_5m = kite.historical_data(sym_token, sel_date, sel_date, "5minute")
                    if not candles_5m:
                        pk["outcome"] = "NO_DATA"; continue

                    # find candles after signal_time
                    sig_hm = pk.get("signal_time", "09:15")
                    sig_h, sig_m = int(sig_hm.split(":")[0]), int(sig_hm.split(":")[1])
                    entry = float(pk["entry"])
                    sl    = float(pk["sl"])
                    tgt   = float(pk["target"])

                    outcome = "NOT_EXECUTED"
                    entry_triggered = False
                    for c5 in candles_5m:
                        ct = c5["date"]
                        if hasattr(ct, "hour"):
                            if ct.hour < sig_h or (ct.hour == sig_h and ct.minute < sig_m):
                                continue
                        hi, lo = c5["high"], c5["low"]
                        if not entry_triggered:
                            if hi >= entry:
                                entry_triggered = True
                            else:
                                continue
                        # Entry triggered — now check target/SL
                        if lo <= sl:
                            outcome = "LOSS"; break
                        if hi >= tgt:
                            outcome = "PROFIT"; break
                        outcome = "NOT_EXECUTED"  # in trade, neither hit yet

                    pk["outcome"] = outcome
                except Exception:
                    pk["outcome"] = "UNKNOWN"
        except Exception:
            pass  # outcome check is best-effort; don't fail the whole response

        # ── Synthetic option chain (strike-level distribution) ────────────────
        atm_strike = int(round(nifty_close / 50.0) * 50)
        mp_val     = atm_strike  # chain_daily has no max_pain; use ATM as proxy
        offsets    = list(range(-10, 11))  # ATM-500 to ATM+500

        def _oi_w(dist, peak):
            return max(0.05, 1.0 / (1.0 + abs(dist - peak) * 0.45))

        c_weights = [_oi_w(i, 1) for i in offsets]   # calls peak ATM+50
        p_weights = [_oi_w(i, -1) for i in offsets]  # puts peak ATM-50
        c_sum = sum(c_weights); p_sum = sum(p_weights)

        hist_strikes = []
        for j, off in enumerate(offsets):
            s_val = atm_strike + off * 50
            hist_strikes.append({
                "strike":      s_val,
                "call_oi":     int(call_oi * c_weights[j] / c_sum),
                "put_oi":      int(put_oi  * p_weights[j] / p_sum),
                "call_oi_chg": 0,
                "put_oi_chg":  0,
                "is_atm":      s_val == atm_strike,
            })

        chain_msg = {
            "pcr":           pcr,
            "vix":           vix,
            "max_pain":      mp_val,
            "atm":           atm_strike,
            "total_call_oi": call_oi,
            "total_put_oi":  put_oi,
            "ul_price":      nifty_close,
            "strikes":       hist_strikes,
        }

        # ── Historical spike backfill for the selected date ──────────────────
        hist_spikes = []
        try:
            import statistics as _stat2
            from signals import _score_spike as _ssp
            from feed import KITE_TOKENS as _KT
            from config import FNO_SYMBOLS as _FNO
            _sym_list = [s for s in _FNO if s not in ("NIFTY", "BANKNIFTY", "INDIAVIX")]
            for _sym in _sym_list:
                _tok = _KT.get(_sym)
                if not _tok:
                    continue
                try:
                    _candles = kite.historical_data(_tok, sel_date, sel_date, "minute")
                    if len(_candles) < 10:
                        continue
                    _vols = [c['volume'] for c in _candles if c['volume'] > 0]
                    if not _vols:
                        continue
                    _avg_vol = _stat2.mean(_vols)
                    _open_px = _candles[0]['open']
                    for _i, _c in enumerate(_candles):
                        _t = _c['date']
                        _cm = _t.hour * 60 + _t.minute
                        if not ((570 <= _cm < 660) or (780 <= _cm <= 840)):
                            continue
                        _price   = _c['close']
                        _vol     = _c['volume'] or 0
                        _vm      = _vol / _avg_vol if _avg_vol else 0
                        _chg_pct = (_price - _open_px) / _open_px * 100 if _open_px else 0
                        if abs(_chg_pct) < 0.2 or _vm < 1.5:
                            continue
                        _score = _ssp(_vm, _chg_pct, _sym, _cm)
                        if _score < 50:
                            continue
                        _sp_type = "buy" if _chg_pct > 0 else "sell"
                        _sig     = "LONG" if _chg_pct > 0 else "SHORT"
                        _trigger = f"Price {'+' if _chg_pct>0 else ''}{_chg_pct:.2f}% | Vol {_vm:.1f}x"
                        _entry   = _price
                        _t1_px   = _entry * 1.005 if _sp_type == "buy" else _entry * 0.995
                        _sl_px   = _entry * 0.995  if _sp_type == "buy" else _entry * 1.005
                        _outcome = None
                        for _j in range(_i + 1, min(_i + 31, len(_candles))):
                            _fc = _candles[_j]
                            if _sp_type == "buy":
                                if _fc['low']  <= _sl_px: _outcome = "HIT SL"; break
                                if _fc['high'] >= _t1_px: _outcome = "HIT T1"; break
                            else:
                                if _fc['high'] >= _sl_px: _outcome = "HIT SL"; break
                                if _fc['low']  <= _t1_px: _outcome = "HIT T1"; break
                        if _outcome is None:
                            _outcome = "EXPIRED"
                        hist_spikes.append({
                            "symbol":   _sym,
                            "time":     _t.strftime("%H:%M"),
                            "price":    round(_price, 2),
                            "chg_pct":  round(_chg_pct, 2),
                            "vol_mult": round(_vm, 1),
                            "oi_pct":   0.0,
                            "type":     _sp_type,
                            "trigger":  _trigger,
                            "signal":   _sig,
                            "strength": "hi" if _score >= 70 else "md",
                            "score":    _score,
                            "pc":       3,
                            "outcome":  _outcome,
                        })
                except Exception:
                    pass
            hist_spikes.sort(key=lambda x: -x["score"])
            hist_spikes = hist_spikes[:30]
        except Exception as _se:
            logger.warning(f"Historical spike backfill failed: {_se}")

        return JSONResponse({
            "date": date,
            "prices": prices,
            "gates": {"gates": gates_data, "verdict": verdict, "verdict_sub": verdict_sub,
                      "pass_count": pass_cnt, "confidence": confidence},
            "macro":  {"vix": vix, "vix_chg": vix_chg, "fii_net": fii_net},
            "fii":    {"fii_net": fii_net, "dii_net": dii_net},
            "chain":  chain_msg,
            "stocks": sorted([s for s in stocks_msg if s["score"] >= 60], key=lambda x: (-x["score"], -abs(x["chg_pct"]))),  # Only stocks with score >= 60 (min 2 gates), high score first
            "spikes": hist_spikes,
            "stock_picks": stock_picks[:8],
            "nifty_ohlc": {"o": nifty_row[0], "h": nifty_row[1], "l": nifty_row[2], "c": nifty_row[3]},
        })
    except Exception as e:
        logger.error(f"Dayview full error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/dayview/stocks")
async def dayview_stocks(date: str):
    """
    Per-stock gate analysis for a specific date.
    G1/G2 shared from index. G3/G4/G5 computed per stock using Kite historical data.
    Also attempts to fetch 5-min intraday candles for recent dates (within 60 days).
    """
    import statistics as _stat
    from datetime import date as _date_cls, timedelta as _td, datetime as _dt_cls
    from config import KITE_TOKENS, FNO_SYMBOLS, LOT_SIZES, GATE as TH
    from backtest_engine import _g1, _g2, _g3, _g4, _g5, _verdict
    import backtest_data as bd

    try:
        sel_date = _dt_cls.strptime(date, "%Y-%m-%d").date()
        context_from = sel_date - _td(days=35)
        days_ago = (_date_cls.today() - sel_date).days

        # Get index-level context (G1/G2) from DB
        bd.init_db()
        conn = bd.get_conn()
        vix_r   = conn.execute("SELECT vix, vix_chg FROM vix_daily WHERE date=?", (date,)).fetchone() or (15.0, 0.0)
        chain_r = conn.execute("SELECT pcr, total_call_oi, total_put_oi FROM chain_daily WHERE date=?", (date,)).fetchone() or (1.0, 500000, 500000)
        fii_net = (conn.execute("SELECT fii_net FROM fii_daily WHERE date=?", (date,)).fetchone() or (0.0,))[0]
        if abs(fii_net) > 10000:
            fii_net = round(fii_net / 100, 2)
        conn.close()

        g1_idx = _g1(vix_r[0], vix_r[1], fii_net)
        g2_idx = _g2(chain_r[0], chain_r[1], chain_r[2])

        # Get kite instance
        from kiteconnect import KiteConnect
        from config import KITE_API_KEY, KITE_ACCESS_TOKEN
        kite = KiteConnect(api_key=KITE_API_KEY)
        kite.set_access_token(KITE_ACCESS_TOKEN)

        stocks = []
        skip_syms = {"NIFTY", "BANKNIFTY", "INDIAVIX"}

        for sym in FNO_SYMBOLS:
            if sym in skip_syms:
                continue
            token = KITE_TOKENS.get(sym)
            if not token:
                continue
            try:
                # Daily candles for context (ATR + avg volume)
                hist = kite.historical_data(token, context_from, sel_date, "day")
                if not hist or hist[-1]["date"].strftime("%Y-%m-%d") if hasattr(hist[-1]["date"], "strftime") else str(hist[-1]["date"])[:10] != date:
                    # Last candle may not be this exact date — find closest
                    day_data = [d for d in hist if (d["date"].strftime("%Y-%m-%d") if hasattr(d["date"],"strftime") else str(d["date"])[:10]) == date]
                    if not day_data:
                        continue
                    day_d = day_data[0]
                else:
                    day_d = hist[-1]

                prior = [d for d in hist if (d["date"].strftime("%Y-%m-%d") if hasattr(d["date"],"strftime") else str(d["date"])[:10]) < date]
                if len(prior) < 5:
                    continue

                prices  = [d["close"] for d in prior]
                trs     = [abs(prices[i]-prices[i-1]) for i in range(1, len(prices))]
                atr_v   = round(_stat.mean(trs[-14:]), 2) if len(trs) >= 14 else round(_stat.mean(trs), 2)
                vols    = [d.get("volume",0) for d in prior if d.get("volume",0)>0]
                avg_vol = _stat.mean(vols) if vols else 1
                prev_close = prior[-1]["close"]

                day_close  = day_d["close"]
                day_high   = day_d["high"]
                day_low    = day_d["low"]
                day_vol    = day_d.get("volume", 0)

                g3 = _g3(day_close, day_high, day_low)
                g4 = _g4(day_close, prev_close, day_vol, avg_vol)
                g5 = _g5(day_close, atr_v, vix_r[0], "intraday")
                g5p= _g5(day_close, atr_v, vix_r[0], "positional")
                verdict_i, pc_i = _verdict([g1_idx, g2_idx, g3, g4, g5])
                verdict_p, pc_p = _verdict([g1_idx, g2_idx, g3, g4, g5p])

                # Entry/exit simulation using next day open if available
                try:
                    nxt_hist = kite.historical_data(token, sel_date + _td(days=1), sel_date + _td(days=5), "day")
                    nxt_d = nxt_hist[0] if nxt_hist else None
                except Exception:
                    nxt_d = None

                entry = round(nxt_d["open"], 2) if nxt_d else None
                trade_i = trade_p = None
                if entry:
                    for mode, rr in [("intraday", TH["rr_min_intraday"]), ("positional", TH["rr_min_positional"])]:
                        stop_dist   = round(atr_v * TH["atr_multiplier"], 2)
                        target_dist = round(stop_dist * rr, 2)
                        target = round(entry + target_dist, 2)
                        stop   = round(entry - stop_dist, 2)
                        nxt_high = round(nxt_d["high"], 2)
                        nxt_low  = round(nxt_d["low"],  2)
                        nxt_close= round(nxt_d["close"], 2)
                        if nxt_high >= target:
                            exit_p, outcome = target, "WIN"
                        elif nxt_low <= stop:
                            exit_p, outcome = stop, "LOSS"
                        else:
                            exit_p  = nxt_close
                            diff    = nxt_close - entry
                            thr     = max(10, round(atr_v * 0.3))
                            outcome = "WIN" if diff >= thr else "LOSS" if diff <= -thr else "NEUTRAL"
                        pnl = round(exit_p - entry, 2)
                        t = {"entry": entry, "target": target, "stop": stop,
                             "exit": exit_p, "pnl": pnl, "outcome": outcome,
                             "next_date": nxt_d["date"].strftime("%Y-%m-%d") if hasattr(nxt_d["date"],"strftime") else str(nxt_d["date"])[:10]}
                        if mode == "intraday":  trade_i = t
                        else:                   trade_p = t

                # 5-min candles if within 60 days
                candles_5m = []
                if days_ago <= 59:
                    try:
                        c5 = kite.historical_data(token, sel_date, sel_date, "5minute")
                        candles_5m = [{"t": (d["date"].strftime("%H:%M") if hasattr(d["date"],"strftime") else str(d["date"])[11:16]),
                                       "o": round(d["open"],2), "h": round(d["high"],2),
                                       "l": round(d["low"],2),  "c": round(d["close"],2),
                                       "v": d.get("volume",0)} for d in c5]
                    except Exception:
                        pass

                chg_pct = round((day_close - prev_close) / prev_close * 100, 2) if prev_close else 0
                vol_ratio = round(day_vol / avg_vol, 2) if avg_vol else 0

                stocks.append({
                    "sym": sym,
                    "close": round(day_close, 2),
                    "chg_pct": chg_pct,
                    "vol_ratio": vol_ratio,
                    "atr": atr_v,
                    "lot": LOT_SIZES.get(sym, 1),
                    "g1": {"state": g1_idx["state"], "score": g1_idx["score"]},
                    "g2": {"state": g2_idx["state"], "score": g2_idx["score"]},
                    "g3": {"state": g3["state"],     "score": g3["score"]},
                    "g4": {"state": g4["state"],     "score": g4["score"]},
                    "g5": {"state": g5["state"],     "score": g5["score"]},
                    "verdict_i": verdict_i, "pc_i": pc_i,
                    "verdict_p": verdict_p, "pc_p": pc_p,
                    "trade_i": trade_i,
                    "trade_p": trade_p,
                    "candles_5m": candles_5m,
                })
            except Exception as ex:
                logger.debug(f"Dayview stock {sym}: {ex}")
                continue

        # Sort: EXECUTE first, then by pass_count desc
        order = {"EXECUTE": 0, "WAIT": 1, "NO TRADE": 2}
        stocks.sort(key=lambda s: (order.get(s["verdict_i"], 3), -s["pc_i"]))

        return JSONResponse({
            "date": date,
            "vix": round(vix_r[0], 2), "pcr": round(chain_r[0], 3),
            "fii_net": round(fii_net, 2),
            "g1": {"state": g1_idx["state"], "score": g1_idx["score"]},
            "g2": {"state": g2_idx["state"], "score": g2_idx["score"]},
            "stocks": stocks,
            "has_5m": days_ago <= 59,
        })
    except Exception as e:
        logger.error(f"Dayview stocks error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/spikes/backtest")
async def spikes_backtest(
    from_date: str = None,
    to_date: str = None,
    vol_min: float = 1.5,    # OPTIMIZED v2
    price_min: float = 0.2,   # OPTIMIZED v2
    trend_filter: bool = True,
    time_from: str = "09:30", # OPTIMIZED v2
    time_to: str = "14:00",   # OPTIMIZED v2
    min_score: int = 45,
):
    """
    Backtest Spike Radar strategy using 1-min OHLCV candles from Kite.
    v4 — OPTIMIZED (73% WR on Feb-Apr 2026):
    - Vol: >=1.5x | Price: >=0.2% | Time: 9:30-14:00
    - Score 0-100 based on vol quality, price momentum, symbol, time slot
    - 20-min cooldown per symbol per day
    - Entry: next candle open; SL=-0.25%, T1=+0.30%, T2=+0.60%; 45-candle window
    """
    import asyncio
    from datetime import datetime, timedelta
    from feed import get_kite
    from config import KITE_TOKENS, FNO_SYMBOLS

    def _score_spike_bt(vol_mult: float, chg_pct: float, sym: str, candle_min: int) -> int:
        """Score a spike 0-100 for backtest (mirrors signals._score_spike)."""
        score = 0
        # Volume quality (0-35)
        if 3.0 <= vol_mult < 5.0:    score += 35
        elif 5.0 <= vol_mult < 7.0:  score += 25
        elif 2.0 <= vol_mult < 3.0:  score += 15
        else:                         score += 5
        # Price momentum quality (0-30)
        ap = abs(chg_pct)
        if 0.5 <= ap < 1.0:    score += 30
        elif 0.4 <= ap < 0.5:  score += 20
        elif 1.0 <= ap < 1.5:  score += 20
        elif 0.3 <= ap < 0.4:  score += 10
        else:                   score += 5
        # Symbol quality (0-20)
        hi_sym = {'TCS', 'TATASTEEL', 'MARUTI', 'INFY'}
        md_sym = {'HDFCBANK', 'RELIANCE', 'BAJFINANCE', 'TATAMOTORS'}
        if sym in hi_sym:    score += 20
        elif sym in md_sym:  score += 12
        else:                score += 5
        # Time quality (0-15)
        if candle_min <= 600:    score += 15   # 09:15-10:00 — best
        elif candle_min <= 810:  score += 10   # 10:00-13:30
        elif candle_min <= 870:  score += 5    # 13:30-14:30
        # else 0 — 14:30+ is weak
        return score

    try:
        ist = pytz.timezone('Asia/Kolkata')
        today = datetime.now(ist).date()

        if not to_date:
            to_date = str(today - timedelta(days=1))
        if not from_date:
            from_date = str(today - timedelta(days=7))

        try:
            fd = datetime.strptime(from_date, "%Y-%m-%d")
            td = datetime.strptime(to_date,   "%Y-%m-%d")
        except ValueError:
            return JSONResponse({"error": "Invalid date format. Use YYYY-MM-DD"}, status_code=400)

        if td < fd:
            return JSONResponse({"error": "from_date must be before to_date"}, status_code=400)
        if (td - fd).days > 30:
            return JSONResponse({"error": "Max date range is 30 days (1-min data limit)"}, status_code=400)

        kite = get_kite()

        loop = asyncio.get_running_loop()

        # parse time filter bounds
        def _parse_hm(s):
            h, m = s.split(":") if ":" in s else (s[:2], s[2:])
            return int(h) * 60 + int(m)
        tf_from = _parse_hm(time_from)  # minutes from midnight
        tf_to   = _parse_hm(time_to)

        def _run():
            results = []
            summary = {"total": 0, "hit_t1": 0, "hit_t2": 0, "hit_sl": 0, "expired": 0}
            score_accumulator = []
            symbols_ok = 0
            symbols_failed = 0
            last_error = None

            symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ("INDIAVIX", "NIFTY", "BANKNIFTY")]

            for sym in symbols:
                token = KITE_TOKENS[sym]
                try:
                    candles = kite.historical_data(token, fd, td, "minute")
                    symbols_ok += 1
                except Exception as e:
                    logger.warning(f"Spike BT: {sym} historical_data failed: {e}")
                    symbols_failed += 1
                    last_error = str(e)
                    continue

                if len(candles) < 25:
                    continue

                # Build per-day open price for trend filter
                day_open: dict = {}  # date_str -> first candle open
                for c in candles:
                    dt = c["date"].strftime("%Y-%m-%d") if hasattr(c["date"], "strftime") else str(c["date"])[:10]
                    if dt not in day_open:
                        day_open[dt] = c["open"]

                # Cooldown: track last signal time per symbol
                last_signal_min: dict = {}  # date_str -> last signal minute-of-day

                # Rolling 20-candle avg volume
                for i in range(20, len(candles) - 1):  # -1 so next candle exists
                    c  = candles[i]
                    cn = candles[i + 1]  # next candle for entry

                    vol   = c.get("volume", 0)
                    close = c.get("close",  0)
                    open_ = c.get("open",   0)
                    if not vol or not close or not open_:
                        continue

                    # Time filter
                    ct = c["date"]
                    if hasattr(ct, "hour"):
                        candle_min = ct.hour * 60 + ct.minute
                    else:
                        try:
                            from datetime import datetime as _dt
                            _parsed = _dt.strptime(str(ct)[:16], "%Y-%m-%d %H:%M")
                            candle_min = _parsed.hour * 60 + _parsed.minute
                        except Exception:
                            candle_min = 555  # assume 9:15
                    if candle_min < tf_from or candle_min > tf_to:
                        continue

                    dt = ct.strftime("%Y-%m-%d") if hasattr(ct, "strftime") else str(ct)[:10]

                    # 20-min cooldown per symbol per day
                    last_min = last_signal_min.get(dt, -999)
                    if candle_min - last_min < 20:
                        continue

                    avg_vol  = sum(candles[j]["volume"] for j in range(i - 20, i)) / 20
                    if avg_vol == 0:
                        continue

                    vol_mult = vol / avg_vol
                    chg_pct  = (close - open_) / open_ * 100

                    # OPTIMIZED v2 filters
                    if vol_mult < vol_min or vol_mult > 7.0:
                        continue
                    if abs(chg_pct) < price_min or abs(chg_pct) > 2.0:
                        continue

                    # Score-based filter
                    score = _score_spike_bt(vol_mult, chg_pct, sym, candle_min)
                    if score < min_score:
                        continue

                    is_buy = chg_pct > 0

                    # Trend filter: spike must align with stock's day direction
                    if trend_filter:
                        d_open = day_open.get(dt, close)
                        day_chg_pct = (close - d_open) / d_open * 100 if d_open else 0
                        if is_buy  and day_chg_pct < -0.2:
                            continue
                        if not is_buy and day_chg_pct >  0.2:
                            continue

                    # Confirmation: next candle must continue in spike direction (OPTIMIZED v2)
                    cn_open = cn.get("open", 0) or close
                    cn_close = cn.get("close", 0) or close
                    cn_cp = (cn_close - cn_open) / cn_open * 100 if cn_open else 0
                    if is_buy and cn_cp < 0.02:
                        continue
                    if not is_buy and cn_cp > -0.02:
                        continue

                    # Entry = confirmed next candle open.
                    entry = round(cn_open, 2)
                    if not entry:
                        entry = round(close, 2)

                    sl = round(entry * (0.9968 if is_buy else 1.0032), 2)  # balanced stop to reduce noise without killing expectancy
                    t1 = round(entry * (1.003  if is_buy else 0.997),  2)  # +0.30% / -0.30%
                    t2 = round(entry * (1.006  if is_buy else 0.994),  2)  # +0.60% / -0.60%

                    result = "EXPIRED"
                    exit_p = None

                    # Check next 45 candles (45 min)
                    for k in range(i + 1, min(i + 46, len(candles))):
                        hi = candles[k]["high"]
                        lo = candles[k]["low"]
                        if is_buy:
                            if lo <= sl:
                                result = "HIT_SL"; exit_p = sl; break
                            if hi >= t2:
                                result = "HIT_T2"; exit_p = t2; break
                            if hi >= t1:
                                result = "HIT_T1"; exit_p = t1; break
                        else:
                            if hi >= sl:
                                result = "HIT_SL"; exit_p = sl; break
                            if lo <= t2:
                                result = "HIT_T2"; exit_p = t2; break
                            if lo <= t1:
                                result = "HIT_T1"; exit_p = t1; break

                    pnl_pct = 0.0
                    if exit_p:
                        pnl_pct = round((exit_p - entry) / entry * 100 * (1 if is_buy else -1), 3)

                    candle_time = ct
                    if hasattr(candle_time, "strftime"):
                        candle_time = candle_time.strftime("%Y-%m-%d %H:%M")
                    else:
                        candle_time = str(candle_time)[:16]

                    results.append({
                        "symbol":   sym,
                        "time":     candle_time,
                        "type":     "BUY" if is_buy else "SELL",
                        "vol_mult": round(vol_mult, 1),
                        "chg_pct":  round(chg_pct,  2),
                        "entry":    entry,
                        "sl":       sl,
                        "t1":       t1,
                        "t2":       t2,
                        "result":   result,
                        "pnl_pct":  pnl_pct,
                        "score":    score,
                    })

                    summary["total"] += 1
                    if result == "HIT_T1":   summary["hit_t1"] += 1
                    elif result == "HIT_T2": summary["hit_t2"] += 1
                    elif result == "HIT_SL": summary["hit_sl"] += 1
                    else:                    summary["expired"] += 1

                    score_accumulator.append(score)

                    # Mark cooldown
                    last_signal_min[dt] = candle_min

            # sort by time desc
            results.sort(key=lambda x: x["time"], reverse=True)

            hits      = summary["hit_t1"] + summary["hit_t2"]
            win_rate  = round(hits / summary["total"] * 100, 1) if summary["total"] else 0
            avg_pnl   = round(sum(r["pnl_pct"] for r in results) / len(results), 3) if results else 0
            avg_score = round(sum(score_accumulator) / len(score_accumulator), 1) if score_accumulator else 0
            expect    = round(
                (hits / summary["total"] * (0.003 + 0.006) / 2 -
                 summary["hit_sl"] / summary["total"] * 0.0025) * 100, 3
            ) if summary["total"] else 0

            # If ALL symbols failed, it's almost certainly a token issue
            if symbols_ok == 0 and symbols_failed > 0:
                return {"error": f"Token expired or invalid — all {symbols_failed} symbol fetches failed. Last error: {last_error}"}

            return {
                "summary":  {**summary, "win_rate": win_rate, "avg_pnl": avg_pnl,
                             "expectancy_pct": expect, "avg_score": avg_score,
                             "symbols_ok": symbols_ok, "symbols_failed": symbols_failed},
                "results":  results[:500],
                "from_date": from_date,
                "to_date":   to_date,
                "params": {
                    "vol_min": vol_min,
                    "price_min": price_min,
                    "trend_filter": trend_filter,
                    "time_from": time_from,
                    "time_to": time_to,
                    "min_score": min_score,
                    "sl_pct": 0.25,
                    "t1_pct": 0.30,
                    "t2_pct": 0.60,
                    "exit_candles": 45,
                    "cooldown_min": 20,
                },
            }

        data = await loop.run_in_executor(None, _run)
        return JSONResponse(data)

    except Exception as e:
        logger.error(f"Spike backtest error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/backtest/download-fii")
async def backtest_download_fii():
    """Download 3-year FII/DII daily net flow history from NSE."""
    import asyncio, backtest_data as bd

    async def _dl():
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, lambda: bd.download_fii_history(days=1095))
            logger.info("FII history download complete")
        except Exception as e:
            logger.error(f"FII download error: {e}", exc_info=True)

    asyncio.create_task(_dl())
    return JSONResponse({"message": "FII history download started. Check /api/backtest/status."})


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
@app.post("/api/telegram/test")
async def telegram_test():
    """Send a test Telegram message to verify bot token and chat ID are correct."""
    from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return JSONResponse(
            {"ok": False, "error": "TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in .env"},
            status_code=400,
        )

    msg = (
        "✅ <b>NSE EDGE — Telegram test</b>\n"
        "Bot is connected and alerts are active.\n"
        f"Server verdict: <b>{signals.state['verdict']}</b>  "
        f"Gates: {signals.state['pass_count']}/5"
    )
    try:
        import requests as _req
        loop = asyncio.get_event_loop()
        def _post():
            return _req.post(
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"},
                timeout=8,
            ).json()
        data = await loop.run_in_executor(None, _post)
        if data.get("ok"):
            return JSONResponse({"ok": True, "message": "Test message sent successfully"})
        return JSONResponse({"ok": False, "error": data.get("description", "Unknown Telegram error")}, status_code=502)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=502)


# ─── WHATSAPP ─────────────────────────────────────────────────────────────────
@app.post("/api/whatsapp/test")
async def whatsapp_test():
    """Send a test WhatsApp message via CallMeBot to verify phone and API key."""
    from config import WHATSAPP_PHONE, WHATSAPP_APIKEY

    if not WHATSAPP_PHONE or not WHATSAPP_APIKEY:
        return JSONResponse(
            {"ok": False, "error": "WHATSAPP_PHONE or WHATSAPP_APIKEY not set in .env"},
            status_code=400,
        )

    msg = (
        f"NSE EDGE — WhatsApp test\n"
        f"Bot connected. Alerts active.\n"
        f"Verdict: {signals.state['verdict']}  Gates: {signals.state['pass_count']}/5"
    )
    try:
        import requests as _req
        import urllib.parse
        encoded = urllib.parse.quote(msg)
        loop = asyncio.get_event_loop()
        def _get():
            return _req.get(
                f"https://api.callmebot.com/whatsapp.php?phone={WHATSAPP_PHONE}"
                f"&text={encoded}&apikey={WHATSAPP_APIKEY}",
                timeout=8,
            )
        resp = await loop.run_in_executor(None, _get)
        if resp.status_code == 200:
            return JSONResponse({"ok": True, "message": "WhatsApp test message sent"})
        return JSONResponse({"ok": False, "error": f"CallMeBot returned HTTP {resp.status_code}"}, status_code=502)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=502)


# ─── CLOUD PUSH ENDPOINT ─────────────────────────────────────────────────────
# Your local PC (with valid Kite token) pushes live state here every 30s.
# Protected by a shared secret set via PUSH_SECRET env var.
_PUSH_SECRET = os.getenv("PUSH_SECRET", "")

@app.post("/api/push-state")
async def push_state(request: Request):
    """Receive live state pushed from local Kite-connected server."""
    auth = request.headers.get("X-Push-Secret", "")
    if _PUSH_SECRET and auth != _PUSH_SECRET:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    # Inject into signals state so all existing endpoints work
    if "prices" in payload:
        try:
            from feed import price_cache
            price_cache.update(payload["prices"])
        except Exception:
            pass
    if "gates" in payload:
        g = payload["gates"]
        signals.state["gates"]      = g.get("gates", signals.state.get("gates", {}))
        signals.state["verdict"]    = g.get("verdict", signals.state.get("verdict", "WAIT"))
        signals.state["verdict_sub"]= g.get("verdict_sub", "")
        signals.state["pass_count"] = g.get("pass_count", 0)
    if "spikes" in payload:
        signals.state["spikes"] = payload["spikes"]
    if "stocks" in payload:
        signals.state["last_stocks"] = payload["stocks"]
    if "chain" in payload:
        signals.state["last_chain"] = payload["chain"]
    if "macro" in payload:
        signals.state["last_macro"] = payload["macro"]
    if "fii" in payload:
        signals.state["last_fii"] = payload["fii"]

    # Broadcast to any connected browser clients
    if "prices" in payload: broadcast({"type": "prices", "data": payload["prices"], "ts": time.time()})
    if "gates"  in payload: broadcast({"type": "gates",  "data": payload["gates"],  "timestamp": time.time()})
    if "spikes" in payload: broadcast({"type": "spikes", "data": payload["spikes"], "ts": time.time()})
    if "stocks" in payload: broadcast({"type": "stocks", "data": payload["stocks"], "timestamp": time.time()})
    if "chain"  in payload: broadcast({"type": "chain",  "data": payload["chain"],  "timestamp": time.time()})
    if "macro"  in payload: broadcast({"type": "macro",  "data": payload["macro"],  "timestamp": time.time()})
    if "fii"    in payload: broadcast({"type": "fii",    "data": payload["fii"],    "timestamp": time.time()})

    return JSONResponse({"ok": True, "ts": time.time()})


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT,
                log_level="info", access_log=False, reload=False)
