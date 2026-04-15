"""
STOCKR.IN — FastAPI Backend (Zerodha Kite Connect only)
Start: python3 main.py
"""

import asyncio
import datetime
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional, Set

import pytz
import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

import fetcher
import signals
import scheduler as sched
from feed import feed_manager, get_all_prices
from live_picks import compute_live_picks
from modules.saas.routes import router as _saas_router, init_saas_db
from config import (
    HOST, PORT, KITE_API_KEY, KITE_ACCESS_TOKEN, is_market_open, is_market_session_day, get_market_status,
    apply_strategy_profile, get_strategy_profile_name, get_strategy_profiles,
)

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
    logger.info("  STOCKR.IN — Zerodha Kite Connect")
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
                    logger.warning(
                        "Background startup token refresh FAILED — live quotes need a valid Kite token (refresh manually or fix auto_token)"
                    )
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
        init_saas_db()
        logger.info("SaaS DB ready")
    except Exception as e:
        logger.warning(f"Backtest DB init failed (non-critical): {e}")

    # Start Kite feed (validates creds, starts KiteTicker)
    feed_manager.start()
    
    demo_mode = getattr(feed_manager, '_demo_mode', False)
    if demo_mode:
        logger.info("Kite session pending — chain/stocks/quotes use Kite only; empty until token is valid")
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
        try:
            indices = fetcher.fetch_indices()
        except Exception as _ie:
            logger.warning(f"  fetch_indices failed ({_ie.__class__.__name__}) — will use ticker fallback")
            indices = None

        try:
            fii = fetcher.fetch_fii_dii()
        except Exception:
            fii = None

        if kite:
            try:
                chain = fetcher.fetch_option_chain(kite, "NIFTY")
            except Exception as _ce:
                logger.warning(f"  fetch_option_chain failed ({_ce.__class__.__name__})")
                chain = None
            try:
                stocks = fetcher.fetch_fno_stocks(kite)
            except Exception:
                stocks = []
        else:
            chain = None
            stocks = []
            logger.info("  Skipping chain/stocks (Kite session not ready yet)")

        # Fallback: build minimal indices from KiteTicker price_cache
        if not indices:
            from feed import price_cache as _pc
            _n = _pc.get("NIFTY", {})
            _b = _pc.get("BANKNIFTY", {})
            _v = _pc.get("INDIAVIX", {})
            if _n.get("price"):
                indices = {
                    "nifty": _n["price"], "nifty_chg": _n.get("chg_pct", 0),
                    "banknifty": _b.get("price", 0), "banknifty_chg": _b.get("chg_pct", 0),
                    "vix": _v.get("price", 0), "pcr": (chain or {}).get("pcr", 1.0),
                    "_source": "ticker_fallback",
                }
                logger.info(f"  Indices from ticker: Nifty={indices['nifty']:.0f} VIX={indices['vix']:.1f}")

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

        # Seed scheduler cache so off-hours job_chain has data to push
        if chain:
            sched.set_cache("chain", chain)
        if fii:
            sched.set_cache("fii", fii)
        if indices:
            sched.set_cache("indices", indices)

        if is_market_open():
            signals.run_signal_engine(indices, chain, fii, stocks or [], "intraday")
        else:
            signals.set_market_closed_state(get_market_status())

        # Ensure last_chain/last_macro are never overwritten by off-hours job with None
        # Re-affirm after run_signal_engine in case scheduler already fired
        if chain and not signals.state.get("last_chain"):
            signals.state["last_chain"] = chain
        if indices and not signals.state.get("last_macro"):
            signals.state["last_macro"] = indices
    except Exception as e:
        logger.error(f"Initial fetch error: {e}", exc_info=True)

    # Backfill today's spikes from Kite 1-min history so the table is
    # populated even when the server starts after market hours.
    if kite and is_market_session_day():
        try:
            today_str = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).date().isoformat()
            backfill = signals.build_today_spikes_from_kite_history(kite)
            if backfill:
                signals.state["spikes"] = backfill
                signals.state["spikes_date"] = today_str
                logger.info(f"  Backfilled {len(backfill)} spikes from today's history")
        except Exception as e:
            logger.warning(f"Spike backfill failed (non-critical): {e}")

    # Restore today's index signals from DB so TODAY tab is never blank after restart
    try:
        if not is_market_session_day():
            signals.state["index_signals"] = []
            signals.state["index_signals_date"] = datetime.date.today().isoformat()
            raise RuntimeError("skip holiday/weekend index restore")
        import sqlite3 as _sq, datetime as _dt, json as _json
        _db = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
        _today = _dt.date.today().isoformat()
        _conn = _sq.connect(_db)
        _conn.row_factory = _sq.Row
        _rows = _conn.execute(
            "SELECT * FROM index_signal_history WHERE trade_date=? ORDER BY ts ASC",
            (_today,)
        ).fetchall()
        _conn.close()
        if _rows:
            _ix_raw = [dict(r) for r in _rows]
            # Deduplicate historical duplicates by business identity
            # (same day/symbol/type/time/strike/entry), keeping the most useful row.
            def _ix_key(z):
                return (
                    str(z.get("trade_date") or ""),
                    str(z.get("symbol") or ""),
                    str(z.get("type") or ""),
                    str(z.get("signal_time") or z.get("time") or ""),
                    int(z.get("strike") or 0),
                    round(float(z.get("entry") or 0), 2),
                )
            def _ix_rank(z):
                oc = 1 if z.get("outcome") else 0
                upd = float(z.get("updated_ts") or z.get("ts") or 0)
                rid = int(z.get("id") or 0)
                return (oc, upd, rid)
            _pick = {}
            for _r in _ix_raw:
                _k = _ix_key(_r)
                _old = _pick.get(_k)
                if (_old is None) or (_ix_rank(_r) > _ix_rank(_old)):
                    _pick[_k] = _r
            _ix = list(_pick.values())
            _ix.sort(key=lambda x: float(x.get("ts") or 0), reverse=True)
            import re as _re
            for s in _ix:
                sid = str(s.get("sig_id") or s.get("id") or "")
                if sid and not s.get("id"):
                    s["id"] = sid
                t = (s.get("signal_time") or s.get("time") or "")
                t = str(t).strip() if t is not None else ""
                if not t and s.get("ts"):
                    try:
                        t = datetime.datetime.fromtimestamp(
                            float(s["ts"]), tz=fetcher.IST
                        ).strftime("%H:%M")
                    except Exception:
                        t = ""
                if not t and sid:
                    _m = _re.search(r"_(\d{4})_[A-Z]", sid)
                    if _m:
                        g = _m.group(1)
                        t = f"{g[:2]}:{g[2:]}"
                s["time"] = t
                s["signal_time"] = t
            signals.state["index_signals"]      = _ix
            signals.state["index_signals_date"] = _today
            logger.info(f"  Restored {len(_ix)} index signals from DB")
    except Exception as _e:
        logger.warning(f"Index signal restore failed (non-critical): {_e}")

    # Start scheduler (includes Mon–Fri 07:55 IST Kite token auto-refresh)
    job_scheduler = sched.build_scheduler()
    job_scheduler.start()
    try:
        _tj = job_scheduler.get_job("token_refresh")
        if _tj and _tj.next_run_time:
            logger.info(
                "  Kite token auto-refresh: Mon–Fri 07:55 Asia/Kolkata (next: %s)",
                _tj.next_run_time.strftime("%Y-%m-%d %H:%M %Z"),
            )
    except Exception:
        pass

    logger.info("=" * 55)
    logger.info(f"  WebSocket : ws://{HOST}:{PORT}/ws")
    logger.info(f"  API       : http://{HOST}:{PORT}/api/health")
    logger.info(
        "  INTRA BT  : POST|GET /api/intra-index/backtest (alias /api/intra_index/backtest) — health /api/intra-index/health"
    )
    logger.info(f"  Verdict   : {signals.state['verdict']}")
    logger.info("=" * 55)

    yield

    logger.info("Shutting down...")
    job_scheduler.shutdown(wait=False)
    feed_manager.stop()


app = FastAPI(title="STOCKR.IN", version="5.0.0", lifespan=lifespan)

# ─── CORS CONFIGURATION ─────────────────────────────────────────────────────────
_extra_origins = [o.strip() for o in os.getenv("ALLOWED_ORIGINS", "").split(",") if o.strip()]
ALLOWED_ORIGINS = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "http://localhost:8765",
    "http://127.0.0.1:8765",
    # Live Server / Vite / common static UI ports (UI calls API on :8000)
    "http://localhost:5500",
    "http://127.0.0.1:5500",
    "http://localhost:5501",
    "http://127.0.0.1:5501",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "https://kvishnublr.github.io",   # GitHub Pages
    "null",   # file:// protocol sends Origin: null
] + _extra_origins

# LAN / hotspot dev: UI opened as http://192.168.x.x:PORT (Live Server, phone, etc.) calling API on 127.0.0.1:8000
# gets Origin http://192.168.x.x:PORT — not in the static list above — fetch /api/state fails while WebSocket may still work → LIVE + all WAIT.
_ALLOW_LAN_ORIGIN_REGEX = (
    r"https?://("
    r"192\.168\.\d{1,3}\.\d{1,3}|"
    r"10\.\d{1,3}\.\d{1,3}\.\d{1,3}|"
    r"172\.(1[6-9]|2\d|3[0-1])\.\d{1,3}\.\d{1,3}|"
    r"\[::1\]"
    r")(:\d+)?$"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=_ALLOW_LAN_ORIGIN_REGEX,
    allow_credentials=False,  # Disabled for better security
    allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# ─── PLAYBOOK API (early mount — playbook_routes.py) ──────────────────────────
# Routes live in a small module so they cannot be “lost” at the bottom of main.py.
try:
    from playbook_routes import router as _playbook_router

    app.include_router(_playbook_router)
    logger.info("Mounted playbook-design API (%d paths)", len(_playbook_router.routes))
except Exception as _pb_exc:
    logger.exception("FATAL: playbook_routes failed to mount: %s", _pb_exc)

try:
    app.include_router(_saas_router)
    logger.info("Mounted SaaS platform API (%d paths)", len(_saas_router.routes))
except Exception as _saas_exc:
    logger.exception("FATAL: saas platform failed to mount: %s", _saas_exc)


# ─── FRONTEND ─────────────────────────────────────────────────────────────────
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
_FRONTEND = os.path.join(FRONTEND_DIR, "index.html")
_FRONTEND_ADMIN = os.path.join(FRONTEND_DIR, "admin.html")
app.mount("/frontend-static", StaticFiles(directory=os.path.abspath(FRONTEND_DIR)), name="frontend-static")

@app.get("/")
async def serve_frontend():
    resp = FileResponse(os.path.abspath(_FRONTEND))
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


@app.get("/admin")
async def serve_admin_frontend():
    resp = FileResponse(os.path.abspath(_FRONTEND_ADMIN))
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
        await ws.send_text(json.dumps({"type": "ready", "timestamp": now, "msg": "STOCKR.IN Live"}))
    except Exception as e:
        logger.error(f"Initial state send: {e.__class__.__name__}: {e}")


# ─── REST API ─────────────────────────────────────────────────────────────────
@app.post("/api/refresh-state")
@app.get("/api/refresh-state")
async def refresh_state():
    """
    Force re-fetch of indices + chain (if Kite available) and broadcast to all WS clients.
    Called by the frontend when it detects stale/missing data. Also triggers token refresh
    if the Kite REST API starts failing.
    """
    import asyncio as _aio
    async def _do():
        try:
            from feed import get_kite, price_cache as _pc, maybe_refresh_kite_token
            _kite = get_kite()

            # Try to fetch fresh indices
            try:
                indices = fetcher.fetch_indices()
            except Exception as _e:
                logger.warning("refresh_state: fetch_indices failed (%s) — using ticker", _e.__class__.__name__)
                indices = None
                maybe_refresh_kite_token("refresh_state_timeout")

            # Ticker fallback
            if not indices:
                _n, _b, _v = _pc.get("NIFTY", {}), _pc.get("BANKNIFTY", {}), _pc.get("INDIAVIX", {})
                if _n.get("price"):
                    indices = {
                        "nifty": _n["price"], "nifty_chg": _n.get("chg_pct", 0),
                        "banknifty": _b.get("price", 0), "banknifty_chg": _b.get("chg_pct", 0),
                        "vix": _v.get("price", 0),
                        "pcr": (signals.state.get("last_chain") or {}).get("pcr", 1.0),
                        "_source": "ticker_fallback",
                    }

            if indices:
                signals.state["last_macro"] = indices

            # Try chain refresh if Kite available
            if _kite:
                try:
                    chain = fetcher.fetch_option_chain(_kite, "NIFTY")
                    if chain:
                        signals.state["last_chain"] = chain
                        sched.set_cache("chain", chain)
                        broadcast({"type": "chain", "data": chain, "timestamp": time.time()})
                except Exception as _ce:
                    logger.debug("refresh_state: fetch_option_chain: %s", _ce.__class__.__name__)
                    # Trigger token refresh in background if repeated failures
                    maybe_refresh_kite_token("chain_fetch_error")

            if indices:
                broadcast({"type": "macro", "data": indices, "timestamp": time.time()})
            broadcast({"type": "prices", "data": get_all_prices(), "ts": time.time()})

            # Re-run signal engine with best available data
            if is_market_open():
                signals.run_signal_engine(
                    indices, signals.state.get("last_chain"),
                    signals.state.get("last_fii"), signals.state.get("last_stocks") or [],
                    "intraday"
                )
            else:
                signals.set_market_closed_state(get_market_status())
            broadcast({"type": "gates", "timestamp": time.time(), "data": {
                "gates": {str(k): v for k, v in signals.state["gates"].items()},
                "verdict": signals.state["verdict"],
                "verdict_sub": signals.state.get("verdict_sub", ""),
                "pass_count": signals.state["pass_count"],
                "confidence": signals.state.get("confidence", 0.0),
                "position_size_lots": signals.state.get("position_size_lots", 0),
                "position_size_rupees": signals.state.get("position_size_rupees", 0),
            }})
            logger.info("refresh_state: pushed fresh state to %d clients", len(connected_clients))
        except Exception as e:
            logger.error("refresh_state error: %s", e)

    _aio.create_task(_do())
    return JSONResponse({"ok": True, "ts": time.time(), "ws_clients": len(connected_clients)})


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
        # If false/missing after deploy, the running process is an old build (restart backend).
        "playbook_design_api": True,
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
async def index_signals_history(
    from_date: str = None, to_date: str = None,
    days: int = None
):
    import sqlite3 as _sq
    from datetime import date, timedelta

    from trading_policy import index_hunt_walk_forward_stats

    db_path = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
    try:
        today = date.today().isoformat()
        # Support both from/to and legacy days param
        if from_date and to_date:
            f, t = str(from_date).strip()[:10], str(to_date).strip()[:10]
            try:
                fd, td = date.fromisoformat(f), date.fromisoformat(t)
                if fd > td:
                    fd, td = td, fd
                    f, t = fd.isoformat(), td.isoformat()
            except ValueError:
                pass
        elif days:
            f = (date.today() - timedelta(days=days)).isoformat()
            t = today
        else:
            f = t = today
        conn = _sq.connect(db_path)
        conn.row_factory = _sq.Row
        # Chronological by session date so early months in [from,to] are not dropped when
        # LIMIT applies (old: ORDER BY ts DESC took newest 500 ticks globally).
        rows = conn.execute("""
            SELECT * FROM index_signal_history
            WHERE trade_date BETWEEN ? AND ?
            ORDER BY trade_date ASC, ts ASC
            LIMIT 8000
        """, (f, t)).fetchall()
        wf_rows = conn.execute("""
            SELECT trade_date, outcome FROM index_signal_history
            WHERE trade_date BETWEEN ? AND ?
              AND outcome IN ('HIT_T1','HIT_SL')
            ORDER BY trade_date ASC, ts ASC
        """, (f, t)).fetchall()
        conn.close()
        data = [dict(r) for r in rows]
        walk_forward = index_hunt_walk_forward_stats([dict(r) for r in wf_rows])
        resolved = [r for r in data if r["outcome"] in ("HIT_T1", "HIT_SL")]
        wins = sum(1 for r in resolved if r["outcome"] == "HIT_T1")
        net_pnl = sum(
            (r["t1"] - r["entry"]) * r["lot_sz"] if r["outcome"] == "HIT_T1"
            else -(r["entry"] - r["sl"]) * r["lot_sz"]
            for r in resolved
        )
        wr = round(wins / len(resolved) * 100) if resolved else 0
        dates_in = sorted({str(r.get("trade_date") or "") for r in data if r.get("trade_date")})
        return JSONResponse({
            "signals": data,
            "total": len(data),
            "wins": wins,
            "losses": len(resolved) - wins,
            "win_rate": wr,
            "net_pnl": round(net_pnl),
            "walk_forward": walk_forward,
            "query_from": f,
            "query_to": t,
            "data_trade_date_min": dates_in[0] if dates_in else None,
            "data_trade_date_max": dates_in[-1] if dates_in else None,
        })
    except Exception as e:
        return JSONResponse({"signals": [], "error": str(e)})


@app.post("/api/index-signals/wipe")
async def index_signals_wipe(request: Request):
    """
    Delete all rows from `index_signal_history` (INDEX HUNT stored backtest/history).

    Body JSON (optional):
      - refresh_kite_aux: if true, refresh NIFTY daily + `vix_daily` from Kite (needs valid token).
      - kite_days: lookback for that refresh (default 500, max 4000).
    Index *minute* data for POST /api/index-signals/backtest is always fetched from Kite per run.
    PCR in `chain_daily` is filled via NSE bhavcopy (use ⬇ DOWNLOAD DATA or chain download) if gaps exist.
    """
    import asyncio

    try:
        body = await request.json()
    except Exception:
        body = {}
    import backtest_data as bd

    try:
        sched._ix_db_init()
    except Exception as e:
        logger.warning("index_signals_wipe _ix_db_init: %s", e)

    db_path = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
    deleted = 0
    try:
        import sqlite3 as _sq

        conn = _sq.connect(db_path)
        row = conn.execute("SELECT COUNT(*) FROM index_signal_history").fetchone()
        deleted = int(row[0]) if row else 0
        conn.close()
    except Exception:
        pass
    try:
        bd.wipe_index_signal_history()
    except Exception as e:
        logger.error("index_signals_wipe: %s", e, exc_info=True)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    out: dict = {"ok": True, "deleted_rows": deleted}
    if body.get("refresh_kite_aux"):
        kite_days = int(body.get("kite_days") or 500)
        kite_days = max(30, min(kite_days, 4000))
        try:
            from feed import get_kite

            kite = get_kite()
            if not kite:
                out["kite_refresh"] = "skipped_no_token"
            else:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, lambda: bd.download_kite_history(kite, days=kite_days))
                out["kite_refresh"] = f"ok_nifty_vix_{kite_days}d"
        except Exception as e:
            logger.warning("index_signals_wipe kite refresh: %s", e)
            out["kite_refresh"] = f"error:{e}"
    return JSONResponse(out)


@app.post("/api/index-radar/ml-train")
async def index_radar_ml_train(request: Request):
    """
    Train GradientBoosting filter on index_signal_history (HIT_T1 vs HIT_SL).
    Body JSON: { "target_precision": 0.80 } — picks proba threshold toward that precision on val split.
    """
    try:
        body = await request.json()
    except Exception:
        body = {}
    target = float(body.get("target_precision", 0.80))
    try:
        import index_radar_ml as _iml

        return JSONResponse(_iml.train_and_save(target_precision=target))
    except Exception as e:
        logger.error("ml-train: %s", e, exc_info=True)
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=200,
        )


@app.get("/api/index-radar/ml-status")
async def index_radar_ml_status():
    import json as _json
    import index_radar_ml as _iml
    meta = {}
    try:
        if os.path.isfile(_iml._META_PATH):
            with open(_iml._META_PATH, encoding="utf-8") as f:
                meta = _json.load(f)
    except Exception:
        pass
    b = _iml.load_bundle()
    return JSONResponse({
        "model_loaded": b is not None,
        "meta": meta,
        "model_path": _iml._DEFAULT_MODEL,
    })


@app.post("/api/index-signals/backtest")
async def index_signals_backtest(request: Request):
    """
    Run Index Radar on Kite 1-min data; rules match live INDEX_RADAR (see config.py).
    Outcome: forward scan uses outcome_t1_index_pct / outcome_sl_index_pct on the underlying
    (fallback: outcome_index_pct for both) within 45 minutes; matches live index fallback + plan mults.
    """
    import sqlite3 as _sq, datetime as _dt, time as _t
    from collections import defaultdict

    from config import INDEX_RADAR, INDEX_RADAR_HIGH_ACCURACY, INDEX_RADAR_ELITE, INDEX_RADAR_PRECISION_V2, _INDEX_RADAR_BASE
    from trading_policy import index_hunt_walk_forward_stats
    from index_radar_logic import (
        build_minute_close_map,
        cm_from_candle,
        daily_pick_select,
        index_hunt_candidate_score,
        index_radar_quality,
        passes_index_radar_1m,
    )

    body = await request.json()
    from_date = body.get("from_date", (_dt.date.today() - _dt.timedelta(days=30)).isoformat())
    to_date   = body.get("to_date",   _dt.date.today().isoformat())
    preset = str(body.get("preset") or "default").strip().lower()
    # Default: match live engine (INDEX_RADAR includes active strategy profile).
    # High accuracy: file baseline + INDEX_RADAR_HIGH_ACCURACY only — same as profile "index_precision",
    # not stacked on balanced_v2 (stacking produced impossible thresholds and zero signals).
    IR_BT = dict(INDEX_RADAR)
    if preset in ("precision_v2", "independent", "v2", "70pct"):
        IR_BT = dict(_INDEX_RADAR_BASE)
        IR_BT.update(INDEX_RADAR_PRECISION_V2)
        preset = "precision_v2"
    elif preset in ("elite", "e70", "max_precision"):
        IR_BT = dict(_INDEX_RADAR_BASE)
        IR_BT.update(INDEX_RADAR_HIGH_ACCURACY)
        IR_BT.update(INDEX_RADAR_ELITE)
        preset = "elite"
    elif preset in ("high_accuracy", "precision", "ha"):
        IR_BT = dict(_INDEX_RADAR_BASE)
        IR_BT.update(INDEX_RADAR_HIGH_ACCURACY)
        preset = "high_accuracy"
    else:
        preset = "default"

    try:
        from feed import get_kite
        kite = get_kite()
        if not kite:
            return JSONResponse({"error": "Kite not available"}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=503)

    NIFTY_TOK = 256265
    BN_TOK    = 260105
    CONFIGS = [
        ("NIFTY",     NIFTY_TOK, 25,  50),
        ("BANKNIFTY", BN_TOK,    15, 100),
    ]

    db_path = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
    conn = _sq.connect(db_path)
    try:
        import index_radar_ml as _irm
        _irm._ensure_ix_columns()
    except Exception:
        pass
    _ix_dedup_bt = int(IR_BT.get("dedup_minutes", 20))
    t_win0, t_win1 = int(IR_BT["time_start_min"]), int(IR_BT["time_end_min"])

    # Nifty minute map + prev close — PE filter uses Nifty % vs prev close (same as live)
    nifty_by_date: dict = defaultdict(list)
    try:
        _nh = kite.historical_data(NIFTY_TOK, from_date, to_date, "minute")
    except Exception:
        _nh = []
    for c in _nh or []:
        nifty_by_date[c["date"].date().isoformat()].append(c)
    nifty_days = sorted(nifty_by_date.keys())
    nifty_prev_close: dict = {}
    nifty_minute: dict = {}
    for di, d in enumerate(nifty_days):
        if di > 0:
            pd = nifty_days[di - 1]
            nifty_prev_close[d] = float(nifty_by_date[pd][-1]["close"] or 0)
        nifty_minute[d] = build_minute_close_map(nifty_by_date[d])

    bn_by_date: dict = defaultdict(list)
    try:
        _bh = kite.historical_data(BN_TOK, from_date, to_date, "minute")
    except Exception:
        _bh = []
    for c in _bh or []:
        bn_by_date[c["date"].date().isoformat()].append(c)
    bn_minute: dict = {d: build_minute_close_map(bn_by_date[d]) for d in bn_by_date}

    total_inserted = 0
    results = []
    _ix_base = float(IR_BT.get("outcome_index_pct", 0.25))
    _ix_t1_th = float(IR_BT["outcome_t1_index_pct"]) if IR_BT.get("outcome_t1_index_pct") is not None else _ix_base
    _ix_sl_th = float(IR_BT["outcome_sl_index_pct"]) if IR_BT.get("outcome_sl_index_pct") is not None else _ix_base
    filter_stats: dict = defaultdict(int)

    for sym, tok, lot_sz, step in CONFIGS:
        try:
            candles = kite.historical_data(tok, from_date, to_date, "minute")
        except Exception:
            continue
        if not candles:
            continue

        by_date = defaultdict(list)
        for c in candles:
            d = c["date"].date().isoformat()
            by_date[d].append(c)

        for day, day_c in sorted(by_date.items()):
            mkt = [c for c in day_c if t_win0 <= cm_from_candle(c) <= t_win1]
            mkt.sort(key=lambda x: x["date"])
            if len(mkt) < 15:
                continue

            vix_row = conn.execute("SELECT vix FROM vix_daily WHERE date=?", (day,)).fetchone()
            vix_d = float(vix_row[0]) if vix_row and vix_row[0] is not None else 0.0
            pcr_row = conn.execute("SELECT pcr FROM chain_daily WHERE date=?", (day,)).fetchone()
            pcr_d = float(pcr_row[0]) if pcr_row and pcr_row[0] else 1.0

            npc_day = nifty_prev_close.get(day) or 0.0
            nmin_day = nifty_minute.get(day, {})
            use_daily = bool(IR_BT.get("daily_pick_enabled"))
            last_sig_min: dict = {}

            def _finalize_one(i, c, chg, is_ce, px, quality, strength):
                nonlocal total_inserted
                sig_type = "CE" if is_ce else "PE"
                atm = round(px / step) * step
                strike = atm + step if is_ce else atm - step
                entry = round(px * 0.007, 1)
                if entry < 20:
                    entry = 20.0
                sl_m = float(IR_BT.get("opt_sl_mult", 0.70))
                t1_m = float(IR_BT.get("opt_t1_mult", 1.50))
                t2_m = float(IR_BT.get("opt_t2_mult", 2.00))
                sl = round(entry * sl_m, 2)
                t1 = round(entry * t1_m, 2)
                t2 = round(entry * t2_m, 2)
                rr = round((t1 - entry) / max(entry - sl, 0.01), 1)

                if IR_BT.get("ml_filter_enabled") and not use_daily:
                    try:
                        from index_radar_ml import effective_ml_threshold, win_probability

                        _cand_ml = {
                            "chg_pct": float(chg),
                            "type": "CE" if is_ce else "PE",
                            "symbol": sym,
                            "strength": strength,
                            "time": c["date"].strftime("%H:%M"),
                            "vix": vix_d,
                            "rr": float(rr),
                            "quality": quality,
                            "pcr": pcr_d,
                        }
                        _pr = win_probability(_cand_ml)
                        _thr = effective_ml_threshold(float(IR_BT.get("ml_min_win_prob", 0.72)))
                        if _pr is not None and _pr < _thr:
                            filter_stats["reject_ml"] += 1
                            return
                    except Exception:
                        pass

                outcome = None
                entry_idx_px = px
                use_hl = bool(IR_BT.get("outcome_use_hl", False))
                for j in range(i + 1, min(i + 46, len(mkt))):
                    fc = mkt[j]
                    if use_hl:
                        hi = float(fc.get("high") or fc.get("close") or 0)
                        lo = float(fc.get("low") or fc.get("close") or 0)
                        if not hi or not lo:
                            continue
                        if is_ce:
                            high_mv = (hi - entry_idx_px) / entry_idx_px * 100
                            low_mv = (lo - entry_idx_px) / entry_idx_px * 100
                            if high_mv >= _ix_t1_th:
                                outcome = "HIT_T1"
                                break
                            if low_mv <= -_ix_sl_th:
                                outcome = "HIT_SL"
                                break
                        else:
                            high_mv = (hi - entry_idx_px) / entry_idx_px * 100
                            low_mv = (lo - entry_idx_px) / entry_idx_px * 100
                            if low_mv <= -_ix_t1_th:
                                outcome = "HIT_T1"
                                break
                            if high_mv >= _ix_sl_th:
                                outcome = "HIT_SL"
                                break
                    else:
                        fmv = (float(fc["close"]) - entry_idx_px) / entry_idx_px * 100
                        if (is_ce and fmv >= _ix_t1_th) or (not is_ce and fmv <= -_ix_t1_th):
                            outcome = "HIT_T1"
                            break
                        if (is_ce and fmv <= -_ix_sl_th) or (not is_ce and fmv >= _ix_sl_th):
                            outcome = "HIT_SL"
                            break
                if outcome is None:
                    outcome = "EXPIRED"

                _id_sfx = "_V2" if preset == "precision_v2" else ("_EL" if preset == "elite" else ("_HA" if preset == "high_accuracy" else ""))
                sig_id = f"{sym}_{day.replace('-', '')}_{c['date'].strftime('%H%M')}_{sig_type}{_id_sfx}"
                sig_time = c["date"].strftime("%H:%M")
                lot_pnl = round((t1 - entry) * lot_sz)
                now_ts = _t.time()

                try:
                    conn.execute("""
                        INSERT OR REPLACE INTO index_signal_history
                          (sig_id, trade_date, symbol, type, signal_time, ts,
                           index_px, strike, entry, sl, t1, t2, rr, lot_sz, lot_pnl_t1,
                           chg_pct, strength, vix, quality, pcr, outcome, created_ts, updated_ts)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        sig_id, day, sym, sig_type, sig_time, c["date"].timestamp(),
                        round(px, 2), strike, entry, sl, t1, t2, rr, lot_sz, lot_pnl,
                        round(float(chg), 2), strength, vix_d, float(quality), float(pcr_d),
                        outcome, now_ts, now_ts
                    ))
                    total_inserted += 1
                    filter_stats["signals_inserted"] += 1
                    results.append({
                        "sig_id": sig_id, "date": day, "symbol": sym,
                        "type": sig_type, "time": sig_time,
                        "chg": round(float(chg), 2), "outcome": outcome,
                        "quality": quality, "pcr": round(pcr_d, 3), "vix": vix_d,
                    })
                except Exception:
                    pass

            if use_daily:
                pool: list = []
                for i, c in enumerate(mkt):
                    cm = cm_from_candle(c)
                    px = float(c.get("close") or 0)
                    if not px:
                        continue

                    filter_stats["bars_scanned"] += 1

                    npx = nmin_day.get(cm)
                    if npc_day and npx:
                        nifty_day_pe = (npx - npc_day) / npc_day * 100
                    else:
                        nifty_day_pe = None

                    cross_o = None
                    if sym == "NIFTY":
                        bm = bn_minute.get(day, {})
                        x0, x1 = bm.get(cm - 5), bm.get(cm)
                        if x0 and x1:
                            cross_o = (x1 - x0) / x0 * 100
                    else:
                        xm = nifty_minute.get(day, {})
                        x0, x1 = xm.get(cm - 5), xm.get(cm)
                        if x0 and x1:
                            cross_o = (x1 - x0) / x0 * 100

                    ok, chg, is_ce, _why = passes_index_radar_1m(
                        mkt, i, IR_BT,
                        vix_eod=vix_d,
                        pcr_day=pcr_d,
                        nifty_day_pct_for_pe=nifty_day_pe,
                        cross_other_5m=cross_o,
                    )
                    if not ok:
                        filter_stats[f"reject_{_why or 'unknown'}"] += 1
                        continue

                    strength, quality = index_radar_quality(float(chg), is_ce, IR_BT, vix_d, pcr_d)
                    _qf = int(IR_BT.get("quality_floor", 0))
                    if IR_BT.get("precision_boost"):
                        _qf = max(_qf, int(IR_BT.get("precision_min_quality", 72)))
                    if _qf > 0 and quality < _qf:
                        filter_stats["reject_quality_floor"] += 1
                        continue

                    atm = round(px / step) * step
                    entry = round(px * 0.007, 1)
                    if entry < 20:
                        entry = 20.0
                    sl_m = float(IR_BT.get("opt_sl_mult", 0.70))
                    t1_m = float(IR_BT.get("opt_t1_mult", 1.50))
                    sl = round(entry * sl_m, 2)
                    t1 = round(entry * t1_m, 2)
                    rr = round((t1 - entry) / max(entry - sl, 0.01), 1)

                    ml_for_score = None
                    if IR_BT.get("daily_pick_use_ml_score") or IR_BT.get("ml_filter_enabled"):
                        try:
                            from index_radar_ml import effective_ml_threshold, win_probability

                            _cand_ml = {
                                "chg_pct": float(chg),
                                "type": "CE" if is_ce else "PE",
                                "symbol": sym,
                                "strength": strength,
                                "time": c["date"].strftime("%H:%M"),
                                "vix": vix_d,
                                "rr": float(rr),
                                "quality": quality,
                                "pcr": pcr_d,
                            }
                            _pr = win_probability(_cand_ml)
                            if IR_BT.get("ml_filter_enabled"):
                                _thr = effective_ml_threshold(float(IR_BT.get("ml_min_win_prob", 0.72)))
                                if _pr is not None and _pr < _thr:
                                    filter_stats["reject_ml"] += 1
                                    continue
                            ml_for_score = _pr
                        except Exception:
                            pass

                    sc = index_hunt_candidate_score(float(quality), float(chg), strength, ml_p=ml_for_score)
                    pool.append({
                        "i": i,
                        "cm": cm,
                        "score": sc,
                        "sig_type": "CE" if is_ce else "PE",
                        "sym": sym,
                        "chg": chg,
                        "is_ce": is_ce,
                        "px": px,
                        "quality": quality,
                        "strength": strength,
                        "candle": c,
                    })

                chosen = daily_pick_select(pool, IR_BT)
                filter_stats["daily_pick_pool"] += len(pool)
                filter_stats["daily_pick_kept"] += len(chosen)
                if len(pool) > len(chosen):
                    filter_stats["daily_pick_dropped"] += len(pool) - len(chosen)
                for item in chosen:
                    _finalize_one(
                        item["i"],
                        item["candle"],
                        item["chg"],
                        item["is_ce"],
                        item["px"],
                        item["quality"],
                        item["strength"],
                    )
            else:
                for i, c in enumerate(mkt):
                    cm = cm_from_candle(c)
                    px = float(c.get("close") or 0)
                    if not px:
                        continue

                    filter_stats["bars_scanned"] += 1

                    npx = nmin_day.get(cm)
                    if npc_day and npx:
                        nifty_day_pe = (npx - npc_day) / npc_day * 100
                    else:
                        nifty_day_pe = None

                    cross_o = None
                    if sym == "NIFTY":
                        bm = bn_minute.get(day, {})
                        x0, x1 = bm.get(cm - 5), bm.get(cm)
                        if x0 and x1:
                            cross_o = (x1 - x0) / x0 * 100
                    else:
                        xm = nifty_minute.get(day, {})
                        x0, x1 = xm.get(cm - 5), xm.get(cm)
                        if x0 and x1:
                            cross_o = (x1 - x0) / x0 * 100

                    ok, chg, is_ce, _why = passes_index_radar_1m(
                        mkt, i, IR_BT,
                        vix_eod=vix_d,
                        pcr_day=pcr_d,
                        nifty_day_pct_for_pe=nifty_day_pe,
                        cross_other_5m=cross_o,
                    )
                    if not ok:
                        filter_stats[f"reject_{_why or 'unknown'}"] += 1
                        continue

                    sig_type = "CE" if is_ce else "PE"
                    if sig_type in last_sig_min and cm - last_sig_min[sig_type] < _ix_dedup_bt:
                        filter_stats["reject_dedup"] += 1
                        continue
                    last_sig_min[sig_type] = cm

                    strength, quality = index_radar_quality(float(chg), is_ce, IR_BT, vix_d, pcr_d)
                    _qf = int(IR_BT.get("quality_floor", 0))
                    if IR_BT.get("precision_boost"):
                        _qf = max(_qf, int(IR_BT.get("precision_min_quality", 72)))
                    if _qf > 0 and quality < _qf:
                        filter_stats["reject_quality_floor"] += 1
                        continue

                    _finalize_one(i, c, chg, is_ce, px, quality, strength)

        conn.commit()

    conn.close()

    resolved = [r for r in results if r["outcome"] in ("HIT_T1","HIT_SL")]
    wins = sum(1 for r in resolved if r["outcome"] == "HIT_T1")
    wr = round(wins/len(resolved)*100) if resolved else 0
    fs_sorted = dict(sorted(filter_stats.items(), key=lambda x: -x[1]))
    walk_forward = index_hunt_walk_forward_stats(
        [{"trade_date": r["date"], "outcome": r["outcome"]} for r in results]
    )
    return JSONResponse({
        "inserted": total_inserted,
        "total": len(results),
        "wins": wins,
        "losses": len(resolved)-wins,
        "win_rate": wr,
        "from": from_date,
        "to": to_date,
        "filter_stats": fs_sorted,
        "preset": preset,
        "walk_forward": walk_forward,
        "note": (
            "WR = HIT_T1 / (HIT_T1+HIT_SL) within 45m. preset=precision_v2: ranked daily pick per symbol "
            "(see filter_stats daily_pick_*). ML gate runs only if backend/data/ix_radar_gb.joblib exists. "
            "Not a guarantee of future performance."
        ),
    })


# ─── PRIME STRIKES BACKTEST ───────────────────────────────────────────────────
@app.post("/api/prime-strikes/backtest")
async def prime_strikes_backtest(request: Request):
    """
    PRIME STRIKE — 3-layer confirmed intraday option-buying backtest.
    VIX hard gate + PCR alignment + 5m momentum + quality-tier lot sizing.
    In-memory only — not written to DB.
    """
    import datetime as _dt, traceback as _tb, sqlite3 as _sq
    from collections import defaultdict

    body = await request.json()
    from_date = body.get("from_date", (_dt.date.today() - _dt.timedelta(days=30)).isoformat())
    to_date   = body.get("to_date",   _dt.date.today().isoformat())

    # ── imports ──
    try:
        from config import PRIME_STRIKE_CONFIG, _INDEX_RADAR_BASE
        from trading_policy import index_hunt_walk_forward_stats
        from index_radar_logic import (
            build_minute_close_map, cm_from_candle, daily_pick_select,
            index_hunt_candidate_score, index_radar_quality, passes_index_radar_1m,
        )
        PS = dict(_INDEX_RADAR_BASE)
        PS.update(PRIME_STRIKE_CONFIG)
    except Exception as e:
        logger.error("prime_strikes_backtest import/config error: %s", e)
        return JSONResponse({"error": f"Config error: {e}"}, status_code=500)

    # ── kite ──
    try:
        from feed import get_kite
        kite = get_kite()
        if not kite:
            return JSONResponse({"error": "Kite not available — check token"}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": f"Kite error: {e}"}, status_code=503)

    # ── DB (VIX + PCR lookup) ──
    try:
        db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "backtest.db")
        conn = _sq.connect(db_path)
    except Exception as e:
        return JSONResponse({"error": f"DB error: {e}"}, status_code=500)

    NIFTY_TOK = 256265
    BN_TOK    = 260105
    CONFIGS   = [("NIFTY", NIFTY_TOK, 25, 50), ("BANKNIFTY", BN_TOK, 15, 100)]

    t_win0    = int(PS.get("time_start_min", 600))
    t_win1    = int(PS.get("time_end_min",   780))
    _t1_th    = float(PS.get("outcome_t1_index_pct", 0.10))
    _sl_th    = float(PS.get("outcome_sl_index_pct", 0.24))
    use_hl    = bool(PS.get("outcome_use_hl", True))
    tier_full = int(PS.get("tier_full_min_quality", 80))
    tier_half = int(PS.get("tier_half_min_quality", 64))
    max_day   = int(PS.get("max_signals_per_day", 3))
    max_csl   = int(PS.get("max_consec_sl", 2))
    all_trades: list = []
    flt: dict = defaultdict(int)

    try:
        # ── pre-fetch Nifty 1-min for cross-index + PE day % ──
        nifty_by_date: dict = defaultdict(list)
        try:
            _nh = kite.historical_data(NIFTY_TOK, from_date, to_date, "minute")
        except Exception:
            _nh = []
        for c in (_nh or []):
            nifty_by_date[c["date"].date().isoformat()].append(c)
        nifty_days = sorted(nifty_by_date.keys())
        nifty_prev: dict = {}
        nifty_min:  dict = {}
        for di, dk in enumerate(nifty_days):
            if di > 0:
                nifty_prev[dk] = float(nifty_by_date[nifty_days[di-1]][-1]["close"] or 0)
            nifty_min[dk] = build_minute_close_map(nifty_by_date[dk])

        bn_by_date: dict = defaultdict(list)
        try:
            _bh = kite.historical_data(BN_TOK, from_date, to_date, "minute")
        except Exception:
            _bh = []
        for c in (_bh or []):
            bn_by_date[c["date"].date().isoformat()].append(c)
        bn_min: dict = {dk: build_minute_close_map(bn_by_date[dk]) for dk in bn_by_date}

        # ── main loop ──
        for sym, tok, lot_sz, step in CONFIGS:
            try:
                candles = kite.historical_data(tok, from_date, to_date, "minute")
            except Exception:
                continue
            if not candles:
                continue

            by_day: dict = defaultdict(list)
            for c in candles:
                by_day[c["date"].date().isoformat()].append(c)

            for day, day_c in sorted(by_day.items()):
                mkt = sorted(
                    [c for c in day_c if t_win0 <= cm_from_candle(c) <= t_win1],
                    key=lambda x: x["date"]
                )
                if len(mkt) < 15:
                    continue

                # VIX + PCR from DB
                vr = conn.execute("SELECT vix FROM vix_daily WHERE date=?", (day,)).fetchone()
                vix_d = float(vr[0]) if vr and vr[0] is not None else 0.0
                pr = conn.execute("SELECT pcr FROM chain_daily WHERE date=?", (day,)).fetchone()
                pcr_d = float(pr[0]) if pr and pr[0] else 1.0

                # Hard VIX day block (only when we actually have VIX data)
                vix_block = float(PS.get("vix_block_above", 17.0))
                if vix_d > 0 and vix_d >= vix_block:
                    flt["vix_day_blocked"] += 1
                    continue

                npc = nifty_prev.get(day, 0.0)
                nm  = nifty_min.get(day, {})

                pool: list = []
                for idx, c in enumerate(mkt):
                    cm_i = cm_from_candle(c)
                    px   = float(c.get("close") or 0)
                    if not px:
                        continue
                    flt["bars_scanned"] += 1

                    npx = nm.get(cm_i)
                    nifty_pe = (npx - npc) / npc * 100 if (npc and npx) else None

                    cross = None
                    if sym == "NIFTY":
                        bm = bn_min.get(day, {})
                        x0, x1 = bm.get(cm_i-5), bm.get(cm_i)
                        if x0 and x1:
                            cross = (x1-x0)/x0*100
                    else:
                        xm = nifty_min.get(day, {})
                        x0, x1 = xm.get(cm_i-5), xm.get(cm_i)
                        if x0 and x1:
                            cross = (x1-x0)/x0*100

                    ok, chg, is_ce, why = passes_index_radar_1m(
                        mkt, idx, PS,
                        vix_eod=vix_d, pcr_day=pcr_d,
                        nifty_day_pct_for_pe=nifty_pe, cross_other_5m=cross,
                    )
                    if not ok:
                        flt[f"reject_{why or 'unk'}"] += 1
                        continue

                    strength, quality = index_radar_quality(float(chg), is_ce, PS, vix_d, pcr_d)
                    qfloor = max(
                        int(PS.get("quality_floor", 68)),
                        int(PS.get("precision_min_quality", 76)) if PS.get("precision_boost") else 0,
                    )
                    if qfloor > 0 and quality < qfloor:
                        flt["reject_quality"] += 1
                        continue

                    entry = max(20.0, round(px * 0.007, 1))
                    atm   = round(px / step) * step
                    ml_p  = None
                    try:
                        if PS.get("daily_pick_use_ml_score"):
                            from index_radar_ml import win_probability
                            ml_p = win_probability({
                                "chg_pct": float(chg), "type": "CE" if is_ce else "PE",
                                "symbol": sym, "strength": strength,
                                "time": c["date"].strftime("%H:%M"),
                                "vix": vix_d, "rr": 1.67,
                                "quality": quality, "pcr": pcr_d,
                            })
                    except Exception:
                        pass

                    sc = index_hunt_candidate_score(float(quality), float(chg), strength, ml_p=ml_p)
                    pool.append({
                        "i": idx, "cm": cm_i, "score": sc,
                        "sig_type": "CE" if is_ce else "PE",
                        "chg": chg, "is_ce": is_ce, "px": px,
                        "quality": quality, "strength": strength,
                        "candle": c, "entry": entry, "atm": atm,
                    })

                chosen = daily_pick_select(pool, PS)
                flt["pool"] += len(pool)
                flt["kept"] += len(chosen)
                chosen.sort(key=lambda x: x["i"])

                day_n, csl = 0, 0
                for item in chosen:
                    if day_n >= max_day:
                        flt["cap_day"] += 1; break
                    if csl >= max_csl:
                        flt["cap_csl"] += 1; break

                    idx2     = item["i"]
                    c2       = item["candle"]
                    chg2     = item["chg"]
                    is_ce2   = item["is_ce"]
                    px2      = item["px"]
                    quality2 = item["quality"]
                    strength2= item["strength"]
                    entry2   = item["entry"]
                    atm2     = item["atm"]
                    cm2      = item["cm"]
                    stype    = item["sig_type"]

                    if float(quality2) < float(tier_half):
                        flt["skip_below_half_tier"] += 1
                        continue

                    t1m = float(PS.get("opt_t1_mult", 1.30))
                    slm = float(PS.get("opt_sl_mult", 0.78))
                    t1  = round(entry2 * t1m, 2)
                    sl  = round(entry2 * slm, 2)
                    rr  = round((t1-entry2)/max(entry2-sl, 0.01), 1)

                    tier = "FULL" if float(quality2) >= float(tier_full) else "HALF"
                    lots = 2 if tier == "FULL" else 1
                    win  = "W1" if cm2 <= 660 else ("W2" if cm2 <= 720 else "W3")

                    outcome = None
                    for jj in range(idx2+1, min(idx2+46, len(mkt))):
                        fc = mkt[jj]
                        if use_hl:
                            hi = float(fc.get("high") or fc.get("close") or 0)
                            lo = float(fc.get("low")  or fc.get("close") or 0)
                            if not hi or not lo:
                                continue
                            if is_ce2:
                                if (hi-px2)/px2*100 >= _t1_th: outcome="HIT_T1"; break
                                if (lo-px2)/px2*100 <= -_sl_th: outcome="HIT_SL"; break
                            else:
                                if (lo-px2)/px2*100 <= -_t1_th: outcome="HIT_T1"; break
                                if (hi-px2)/px2*100 >= _sl_th:  outcome="HIT_SL"; break
                        else:
                            mv = (float(fc["close"])-px2)/px2*100
                            if (is_ce2 and mv>=_t1_th) or (not is_ce2 and mv<=-_t1_th): outcome="HIT_T1"; break
                            if (is_ce2 and mv<=-_sl_th) or (not is_ce2 and mv>=_sl_th): outcome="HIT_SL"; break
                    if outcome is None:
                        outcome = "EXPIRED"

                    tot_lots = lot_sz * lots
                    if outcome == "HIT_T1":
                        pnl = round((t1-entry2)*tot_lots); csl=0
                    elif outcome == "HIT_SL":
                        pnl = round(-(entry2-sl)*tot_lots); csl+=1
                    else:
                        pnl = 0; csl=0

                    day_n += 1
                    flt["signals"] += 1
                    strike = atm2+step if is_ce2 else atm2-step

                    all_trades.append({
                        "date": day, "symbol": sym, "type": stype,
                        "time": c2["date"].strftime("%H:%M"),
                        "window": win, "chg_pct": round(float(chg2),2),
                        "vix": vix_d, "pcr": round(pcr_d,3),
                        "quality": quality2, "strength": strength2,
                        "tier": tier, "lots": lots,
                        "index_px": round(px2,2), "strike": strike,
                        "entry": entry2, "t1": t1, "sl": sl, "rr": rr,
                        "lot_sz": lot_sz, "outcome": outcome, "pnl": pnl,
                    })

    except Exception as e:
        logger.error("prime_strikes_backtest error: %s\n%s", e, _tb.format_exc())
        try:
            conn.close()
        except Exception:
            pass
        return JSONResponse({"error": f"Backtest error: {e}"}, status_code=500)

    conn.close()

    # ── aggregate ──
    resolved = [t for t in all_trades if t["outcome"] in ("HIT_T1","HIT_SL")]
    wins     = sum(1 for t in resolved if t["outcome"]=="HIT_T1")
    wr       = round(wins/len(resolved)*100) if resolved else 0
    net_pnl  = sum(t["pnl"] for t in all_trades)

    def _bs(trades):
        r=[t for t in trades if t["outcome"] in ("HIT_T1","HIT_SL")]
        w=sum(1 for t in r if t["outcome"]=="HIT_T1")
        return {"total":len(trades),"resolved":len(r),"wins":w,
                "losses":len(r)-w,"wr":round(w/len(r)*100) if r else 0,
                "pnl":sum(t["pnl"] for t in trades)}

    def _vbkt(t):
        v=t["vix"]
        if not v: return "no_data"
        if v<13:  return "<13"
        if v<16:  return "13-16"
        if v<17:  return "16-17"
        return ">17"

    wstats = {k:_bs([t for t in all_trades if t["window"]==k]) for k in ("W1","W2","W3")}
    wstats["W1"]["label"]="10:00-11:00 (best)"
    wstats["W2"]["label"]="11:00-12:00"
    wstats["W3"]["label"]="12:00-13:00"
    vstats = {k:_bs([t for t in all_trades if _vbkt(t)==k]) for k in ("<13","13-16","16-17",">17","no_data")}
    tstats = {k:_bs([t for t in all_trades if t["tier"]==k]) for k in ("FULL","HALF")}
    dstats = {k:_bs([t for t in all_trades if t["type"]==k]) for k in ("CE","PE")}

    try:
        from trading_policy import index_hunt_walk_forward_stats
        wf = index_hunt_walk_forward_stats(
            [{"trade_date":t["date"],"outcome":t["outcome"]} for t in all_trades])
    except Exception:
        wf = {"ok": False}

    return JSONResponse({
        "total":    len(all_trades),
        "resolved": len(resolved),
        "wins":     wins,
        "losses":   len(resolved)-wins,
        "win_rate": wr,
        "net_pnl":  net_pnl,
        "from":     from_date,
        "to":       to_date,
        "trades":   all_trades,
        "window_stats": wstats,
        "vix_stats":    vstats,
        "tier_stats":   tstats,
        "type_stats":   dstats,
        "walk_forward": wf,
        "filter_stats": dict(sorted(flt.items(), key=lambda x:-x[1])),
        "note": (
            "PRIME STRIKE: VIX<17 day block | PCR align | 10:00–13:00 | session lock | "
            "confirm bars | 15m hunt | trades only quality≥tier_half (64); FULL≥80=2 lots, else HALF=1. "
            "WR=HIT_T1/(HIT_T1+HIT_SL) within 45 1m bars."
        ),
    })


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
        "adv_index": signals.state.get("adv_index"),
        "adv_idx_options": signals.state.get("adv_idx_options"),
        "intra_index": signals.state.get("intra_index"),
        "strategy_profile": get_strategy_profile_name(),
        "position_size_lots": signals.state.get("position_size_lots", 0),
        "position_size_rupees": signals.state.get("position_size_rupees", 0),
    })


@app.get("/api/strategy/profile")
async def get_strategy_profile():
    return JSONResponse({
        "active": get_strategy_profile_name(),
        "available": get_strategy_profiles(),
    })


@app.post("/api/strategy/profile")
async def set_strategy_profile(name: str):
    active = apply_strategy_profile(name)
    logger.info("Strategy profile switched to: %s", active)
    return JSONResponse({
        "ok": True,
        "active": active,
        "available": get_strategy_profiles(),
    })


@app.get("/api/performance/day")
async def day_performance(date: str = None):
    """
    Day performance across all alert sections (SPIKE HUNT + swing radar + index radar).

    - For `live_signal_history`: uses stored outcomes and P&L; OPEN rows use current prices for MTM.
    - For `index_signal_history`: uses stored outcomes and plan P&L; OPEN rows use current option LTP for MTM.
    """
    import sqlite3 as _sq
    from datetime import date as _d
    try:
        day = (date or _d.today().isoformat()).strip()
    except Exception:
        day = _d.today().isoformat()

    db_path = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
    prices_now = get_all_prices() or {}
    ix_live = signals.state.get("index_signals", []) or []
    ch_nifty = signals.state.get("last_chain", {}) or {}
    ch_bank = signals.state.get("bn_chain", {}) or {}
    try:
        from config import LOT_SIZES as _LOT_SIZES
    except Exception:
        _LOT_SIZES = {}

    def _inr(n: float) -> float:
        try:
            return float(round(float(n or 0), 2))
        except Exception:
            return 0.0

    def _opt_ltp(sym: str, strike, typ: str) -> float:
        ch = ch_nifty if str(sym or "").upper() == "NIFTY" else ch_bank
        rows_ = (ch or {}).get("strikes") or []
        if not rows_:
            return 0.0
        is_ce = str(typ or "").upper() == "CE"
        key = "call_ltp" if is_ce else "put_ltp"
        try:
            target = int(strike or 0)
        except Exception:
            target = 0
        for r in rows_:
            try:
                st = int(r.get("strike") or 0)
            except Exception:
                continue
            v = float(r.get(key) or 0)
            if target and st == target and v > 0:
                return v
        return 0.0

    rows = []
    sections = {}
    # Model sizing (user-facing): fixed ₹30,000 deployed per signal → qty = floor(30_000 / entry).
    stock_notional_per_signal = 30000.0

    # ── Spikes + Swing (live_signal_history) ───────────────────────────────
    try:
        conn = _sq.connect(db_path)
        conn.row_factory = _sq.Row
        lsh = conn.execute(
            """SELECT id, trade_date, symbol, signal_type, trigger, strength, signal_time,
                      entry_price, stop_loss, target_price, exit_price, exit_time,
                      status, outcome, pnl_pts, pnl_pct, hold_minutes, verdict
               FROM live_signal_history
               WHERE trade_date = ?
               ORDER BY created_ts ASC""",
            (day,),
        ).fetchall()
        conn.close()
    except Exception:
        lsh = []

    for r in lsh:
        d = dict(r)
        sym = d.get("symbol")
        status = d.get("status") or "OPEN"
        entry = float(d.get("entry_price") or 0)
        exit_p = d.get("exit_price")
        dirn = (d.get("signal_type") or "LONG").upper()
        last_px = float(((prices_now.get(sym) or {}).get("price") or 0) or 0)
        # MTM for open rows if we can
        if status == "OPEN" and entry and last_px:
            pnl_pts = (last_px - entry) if dirn != "SHORT" else (entry - last_px)
            pnl_pct = (pnl_pts / entry) * 100
        else:
            pnl_pts = float(d.get("pnl_pts") or 0)
            pnl_pct = float(d.get("pnl_pct") or 0)
        sec = "SWING" if str(d.get("verdict") or "") == "SWING_RADAR" else "SPIKE_HUNT"
        lot_sz = int((_LOT_SIZES or {}).get(str(sym or "").upper(), 1) or 1)
        pnl_inr = float(pnl_pts or 0) * lot_sz
        qty_model = int(stock_notional_per_signal // entry) if entry > 0 else 0
        investment_inr = _inr(float(qty_model) * entry) if qty_model and entry else 0.0
        pnl_model = float(pnl_pts or 0) * float(qty_model or 0)
        row = {
            "section": sec,
            "kind": "STOCK",
            "symbol": sym,
            "time": d.get("signal_time"),
            "hit_time": d.get("exit_time"),
            "entry_time_ist": d.get("signal_time"),
            "exit_time_ist": d.get("exit_time") if status != "OPEN" else None,
            "direction": dirn,
            "trigger": d.get("trigger") or "",
            "strength": d.get("strength") or "",
            "entry": _inr(entry),
            "sl": _inr(d.get("stop_loss")),
            "t1": _inr(d.get("target_price")),
            "ltp": _inr(last_px) if status == "OPEN" and last_px else _inr(exit_p),
            "status": status,
            "outcome": d.get("outcome") or status,
            "pnl_pts": _inr(pnl_pts),
            "pnl_pct": _inr(pnl_pct),
            "lot_sz": lot_sz,
            "pnl_inr": _inr(pnl_inr),
            "qty_model": int(qty_model or 0),
            "investment_inr": float(investment_inr),
            "pnl_model_inr": _inr(pnl_model),
        }
        rows.append(row)
        sections.setdefault(sec, {"count": 0, "wins": 0, "losses": 0, "open": 0, "pnl_inr": 0.0, "pnl_model_inr": 0.0})
        sections[sec]["count"] += 1
        sections[sec]["pnl_inr"] += float(row["pnl_inr"] or 0)
        sections[sec]["pnl_model_inr"] += float(row["pnl_model_inr"] or 0)
        if status == "OPEN":
            sections[sec]["open"] += 1
        if str(row["outcome"]).upper().startswith("TARGET") or str(row["outcome"]).upper().startswith("WIN"):
            sections[sec]["wins"] += 1
        if "SL" in str(row["outcome"]).upper() or "LOSS" in str(row["outcome"]).upper():
            sections[sec]["losses"] += 1

    # ── Index Radar (index_signal_history + live MTM) ──────────────────────
    try:
        conn = _sq.connect(db_path)
        conn.row_factory = _sq.Row
        ix = conn.execute(
            """SELECT id, updated_ts, sig_id, trade_date, symbol, type, signal_time, ts, index_px,
                      strike, entry, sl, t1, t2, rr, lot_sz, outcome, exit_time, outcome_ltp,
                      option_expiry, option_week
               FROM index_signal_history
               WHERE trade_date = ?
               ORDER BY ts ASC""",
            (day,),
        ).fetchall()
        conn.close()
    except Exception:
        ix = []

    # Deduplicate DB rows first (same signal repeated from earlier inserts)
    def _ixk(z):
        return (
            str(z.get("trade_date") or ""),
            str(z.get("symbol") or ""),
            str(z.get("type") or ""),
            str(z.get("signal_time") or ""),
            int(z.get("strike") or 0),
            round(float(z.get("entry") or 0), 2),
        )
    def _ixrank(z):
        oc = 1 if z.get("outcome") else 0
        upd = float(z.get("updated_ts") or z.get("ts") or 0)
        rid = int(z.get("id") or 0)
        return (oc, upd, rid)
    ix_pick = {}
    for _r in [dict(x) for x in ix]:
        _k = _ixk(_r)
        _old = ix_pick.get(_k)
        if (_old is None) or (_ixrank(_r) > _ixrank(_old)):
            ix_pick[_k] = _r
    ix = list(ix_pick.values())
    ix.sort(key=lambda x: float(x.get("ts") or 0))

    ix_live_by_id = {str(s.get("id")): s for s in ix_live if s and s.get("id")}
    for d in ix:
        sid = str(d.get("sig_id") or "")
        live = ix_live_by_id.get(sid, {})
        status = "OPEN" if not d.get("outcome") else "CLOSED"
        lot = int(d.get("lot_sz") or 0) or (25 if d.get("symbol") == "NIFTY" else 15)
        entry = float(d.get("entry") or 0)
        ltp = 0.0
        if status != "OPEN":
            _ol0 = d.get("outcome_ltp")
            try:
                _ol0 = float(_ol0) if _ol0 is not None and _ol0 != "" else 0.0
            except Exception:
                _ol0 = 0.0
            if _ol0 > 0:
                ltp = _ol0
        if not ltp:
            ltp = float((live.get("ltp") if live else 0) or 0)
        if not ltp:
            ltp = _opt_ltp(d.get("symbol"), d.get("strike"), d.get("type"))
        if status == "OPEN" and entry and ltp:
            pnl = (ltp - entry) * lot
            unit_move = (ltp - entry)
        else:
            oc = d.get("outcome")
            if oc == "HIT_T1" or oc == "HIT_T2":
                pnl = (float(d.get("t1") or 0) - entry) * lot
                unit_move = (float(d.get("t1") or 0) - entry)
            elif oc == "HIT_SL":
                pnl = -(entry - float(d.get("sl") or 0)) * lot
                unit_move = (float(d.get("sl") or 0) - entry)
            else:
                pnl = 0.0
                unit_move = 0.0
        # User model for INDEX RADAR options:
        # always 1 lot execution; cash allocation shown as fixed ₹30K per trade.
        lot_u = max(int(lot or 0), 1)
        num_lots = 1
        qty_model = int(lot_u)
        pnl_model = float(unit_move or 0) * float(qty_model or 0)
        investment_inr = _inr(stock_notional_per_signal)
        exit_ist = (d.get("exit_time") or live.get("outcome_time") or live.get("exit_time") or "").strip() or None
        ol = d.get("outcome_ltp")
        if ol is not None:
            try:
                ol = float(ol)
            except Exception:
                ol = None
        row = {
            "section": "INDEX_RADAR",
            "kind": "OPTION",
            "symbol": d.get("symbol"),
            "time": d.get("signal_time"),
            "hit_time": exit_ist,
            "entry_time_ist": d.get("signal_time"),
            "exit_time_ist": exit_ist if status != "OPEN" else None,
            "direction": d.get("type"),
            "trigger": "INDEX HUNT",
            "strength": "",
            "entry": _inr(entry),
            "sl": _inr(d.get("sl")),
            "t1": _inr(d.get("t1")),
            "ltp": _inr(ltp if ltp else None),
            "option_expiry": d.get("option_expiry") or (live.get("option_expiry") if live else None),
            "option_week": d.get("option_week") or (live.get("option_week") if live else None),
            "status": status,
            "outcome": d.get("outcome") or "OPEN",
            "pnl_pts": _inr(pnl),
            "pnl_pct": 0.0,
            "strike": d.get("strike"),
            "outcome_ltp": _inr(ol) if ol is not None and ol > 0 else None,
            "lot_sz": lot,
            "pnl_inr": _inr(pnl),
            "qty_model": int(qty_model or 0),
            "lots_model": int(num_lots or 0),
            "investment_inr": float(investment_inr),
            "pnl_model_inr": _inr(pnl_model),
        }
        rows.append(row)
        sections.setdefault("INDEX_RADAR", {"count": 0, "wins": 0, "losses": 0, "open": 0, "pnl_inr": 0.0, "pnl_model_inr": 0.0})
        sections["INDEX_RADAR"]["count"] += 1
        sections["INDEX_RADAR"]["pnl_inr"] += float(row["pnl_inr"] or 0)
        sections["INDEX_RADAR"]["pnl_model_inr"] += float(row["pnl_model_inr"] or 0)
        if status == "OPEN":
            sections["INDEX_RADAR"]["open"] += 1
        if row["outcome"] == "HIT_T1" or row["outcome"] == "HIT_T2":
            sections["INDEX_RADAR"]["wins"] += 1
        if row["outcome"] == "HIT_SL":
            sections["INDEX_RADAR"]["losses"] += 1

    total_pnl = round(sum(v["pnl_inr"] for v in sections.values()), 2) if sections else 0.0
    total_pnl_model = round(sum(v.get("pnl_model_inr", 0.0) for v in sections.values()), 2) if sections else 0.0
    total_count = sum(v["count"] for v in sections.values()) if sections else 0
    total_open = sum(v["open"] for v in sections.values()) if sections else 0
    total_wins = sum(v["wins"] for v in sections.values()) if sections else 0
    total_losses = sum(v["losses"] for v in sections.values()) if sections else 0
    wr = round((total_wins / max(total_wins + total_losses, 1)) * 100, 1) if (total_wins + total_losses) else 0.0

    # sort rows by time (string) then section
    rows.sort(key=lambda x: (str(x.get("time") or ""), str(x.get("section") or "")))
    # Validation checks (data quality / consistency)
    issues = []
    for r in rows:
        if not r.get("time"):
            issues.append({"symbol": r.get("symbol"), "section": r.get("section"), "issue": "missing signal_time"})
        if (r.get("entry") or 0) <= 0:
            issues.append({"symbol": r.get("symbol"), "section": r.get("section"), "issue": "non_positive_entry"})
        st = str(r.get("status") or "").upper()
        if st == "OPEN" and (r.get("ltp") is None or float(r.get("ltp") or 0) <= 0):
            issues.append({"symbol": r.get("symbol"), "section": r.get("section"), "issue": "open_without_ltp"})
        out = str(r.get("outcome") or "").upper()
        p = float(r.get("pnl_inr") or 0)
        if ("SL" in out) and p > 0:
            issues.append({"symbol": r.get("symbol"), "section": r.get("section"), "issue": "sl_with_positive_pnl"})
        if (("TARGET" in out) or ("HIT_T1" in out) or ("HIT_T2" in out)) and p < 0:
            issues.append({"symbol": r.get("symbol"), "section": r.get("section"), "issue": "target_with_negative_pnl"})
        if r.get("section") == "INDEX_RADAR":
            t1v = float(r.get("t1") or 0)
            slv = float(r.get("sl") or 0)
            olv = r.get("outcome_ltp")
            olv = float(olv) if olv is not None and olv != "" else None
            if "HIT_T1" in out or "HIT_T2" in out:
                if olv is None:
                    issues.append(
                        {
                            "symbol": r.get("symbol"),
                            "section": r.get("section"),
                            "issue": "index_hit_target_no_exit_ltp",
                            "detail": "Outcome predates premium proof or index-proxy win; re-open with outcome_use_index_fallback off.",
                        }
                    )
                elif t1v > 0 and olv + 0.02 < t1v:
                    issues.append(
                        {
                            "symbol": r.get("symbol"),
                            "section": r.get("section"),
                            "issue": "index_hit_target_exit_ltp_below_t1",
                            "detail": f"exit_ltp={olv:.2f} t1={t1v:.2f}",
                        }
                    )
            if "HIT_SL" in out and olv is not None and slv > 0 and olv > slv + 0.02:
                issues.append(
                    {
                        "symbol": r.get("symbol"),
                        "section": r.get("section"),
                        "issue": "index_sl_exit_ltp_above_sl",
                        "detail": f"exit_ltp={olv:.2f} sl={slv:.2f}",
                    }
                )

    return JSONResponse({
        "date": day,
        "summary": {
            "signals": total_count,
            "open": total_open,
            "wins": total_wins,
            "losses": total_losses,
            "win_rate": wr,
            "pnl_inr": round(total_pnl),
            "pnl_model_inr": round(total_pnl_model),
        },
        "sections": {k: {**v, "pnl_inr": round(v["pnl_inr"]), "pnl_model_inr": round(v.get("pnl_model_inr", 0.0))} for k, v in sections.items()},
        "rows": rows,
        "capital_model": {
            "notional_per_signal_inr": int(stock_notional_per_signal),
            "qty_rule": (
                "STOCK: qty = floor(notional/entry); P&L = move × qty. "
                "INDEX options: fixed 1 lot per signal (qty_units = lot_sz); "
                "deploy is shown as fixed notional ₹30,000 per trade; P&L = premium_move × lot_sz."
            ),
        },
        "validation": {
            "ok": len(issues) == 0,
            "issues_count": len(issues),
            "issues": issues[:100],
        },
    })


@app.get("/api/trading-policy")
async def trading_policy():
    """One-page evidence: Index Radar walk-forward, live-only spikes vs backfill, EXECUTE stats, sample gates."""
    try:
        from trading_policy import build_trading_policy_report
    except ImportError:
        build_trading_policy_report = None
    if not build_trading_policy_report:
        return JSONResponse({"error": "trading_policy module missing"}, status_code=500)
    return JSONResponse(build_trading_policy_report())


@app.get("/api/intra-index/snapshot")
async def intra_index_snapshot(
    fire_threshold: Optional[int] = Query(None, ge=40, le=95, description="FIRE min confidence (default 70)"),
):
    """NIFTY 1m ORB + VWAP + heavyweight breadth + paper FIRE flag (9:30–10:30 IST)."""
    from feed import get_kite

    import intra_index_engine as _ixi

    kite = get_kite()
    return JSONResponse(_ixi.compute_live_snapshot(kite, fire_threshold=fire_threshold))


@app.get("/api/intra-index/health")
async def intra_index_health():
    """Cheap check that INTRA INDEX routes are mounted."""
    return JSONResponse(
        {
            "ok": True,
            "snapshot": "/api/intra-index/snapshot",
            "backtest_post_get": [
                "/api/intra-index/backtest",
                "/api/intra_index/backtest",
            ],
        }
    )


async def _intra_index_backtest_core(days: int, asof_cm: int, fire_threshold: int):
    import asyncio

    from feed import get_kite

    import intra_index_backtest as _ixi_bt

    kite = get_kite()
    if not kite:
        return JSONResponse({"ok": False, "error": "Kite not available"}, status_code=503)
    try:
        loop = asyncio.get_event_loop()
        out = await loop.run_in_executor(
            None,
            lambda: _ixi_bt.run_intra_index_backtest(
                kite,
                days=days,
                asof_cm=asof_cm,
                fire_threshold=fire_threshold,
            ),
        )
        return JSONResponse(out)
    except Exception as e:
        logger.error(f"intra_index_backtest: {e}", exc_info=True)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


async def intra_index_backtest_handle(request: Request):
    """Paper FIRE replay: POST JSON body or GET query (days, asof_cm, fire_threshold)."""
    body: dict = {}
    if request.method == "POST":
        try:
            body = await request.json()
        except Exception:
            body = {}
    else:
        body = dict(request.query_params)
    try:
        days = int(body.get("days") or 30)
        asof_cm = int(body.get("asof_cm") or 630)
        fire_threshold = int(body.get("fire_threshold") or 70)
    except (TypeError, ValueError):
        return JSONResponse({"ok": False, "error": "Invalid days/asof_cm/fire_threshold"}, status_code=400)
    return await _intra_index_backtest_core(days, asof_cm, fire_threshold)


app.add_api_route(
    "/api/intra-index/backtest",
    intra_index_backtest_handle,
    methods=["GET", "POST"],
    tags=["intra-index"],
)
app.add_api_route(
    "/api/intra_index/backtest",
    intra_index_backtest_handle,
    methods=["GET", "POST"],
    tags=["intra-index"],
)


@app.get("/api/adv-index/snapshot")
async def adv_index_snapshot():
    """Fresh NIFTY 50 weighted OI + cash composite (may take 1–2s; also cached in /api/state ~75s)."""
    from feed import get_kite
    import adv_index_engine as _aix

    kite = get_kite()
    if not kite:
        return JSONResponse({"error": "Kite not available"}, status_code=503)
    return JSONResponse(_aix.compute_live_snapshot(kite))


@app.post("/api/adv-index/backtest")
async def adv_index_backtest(request: Request):
    """
    30D (default) walk-forward: weighted 5m cash breadth → NIFTY path.
    OI term is *not* in history — see response methodology (live adds OI).
    """
    from feed import get_kite
    import adv_index_engine as _aix

    try:
        body = await request.json()
    except Exception:
        body = {}
    days = int((body or {}).get("days", 30))
    days = max(5, min(120, days))
    export_csv = bool((body or {}).get("export_csv", False))
    kite = get_kite()
    if not kite:
        return JSONResponse({"error": "Kite not available"}, status_code=503)
    return JSONResponse(_aix.run_adv_index_backtest(kite, days=days, export_csv=export_csv))


@app.get("/api/adv-index/backtest-last-csv")
async def adv_index_backtest_last_csv():
    """Download last CSV written by POST /api/adv-index/backtest with export_csv true."""
    import adv_index_engine as _aix

    p = _aix.DEFAULT_BACKTEST_CSV
    if not os.path.isfile(str(p)):
        return JSONResponse({"error": "No CSV yet — run backtest with export_csv: true"}, status_code=404)
    return FileResponse(
        str(p),
        media_type="text/csv",
        filename="adv_index_backtest_last.csv",
    )


@app.get("/api/adv-index/backtest-csv-info")
async def adv_index_backtest_csv_info():
    """Whether last backtest CSV exists (for UI download link without re-running)."""
    import adv_index_engine as _aix

    p = _aix.DEFAULT_BACKTEST_CSV
    sp = str(p)
    if not os.path.isfile(sp):
        return JSONResponse({"exists": False, "path": sp})
    try:
        st = os.stat(sp)
        return JSONResponse({
            "exists": True,
            "path": sp,
            "size_bytes": int(st.st_size),
            "modified_ts": int(st.st_mtime),
        })
    except OSError:
        return JSONResponse({"exists": False, "path": sp})


@app.get("/api/adv-index/history")
async def adv_index_history_api(
    trade_date: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 500,
):
    """Persisted ADV INDEX snapshots (OI-inclusive live path) for forward analysis."""
    import adv_index_history as _aixh

    rows = _aixh.fetch_history(
        trade_date=trade_date,
        from_date=from_date,
        to_date=to_date,
        limit=limit,
    )
    return JSONResponse({"count": len(rows), "rows": rows})


@app.get("/api/adv-index/history/{row_id}")
async def adv_index_history_one(row_id: int):
    import adv_index_history as _aixh

    row = _aixh.fetch_history_row_full(row_id)
    if not row:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse(row)


@app.get("/api/adv-idx-options/snapshot")
async def adv_idx_options_snapshot():
    """IV proxy, expiry intelligence, max pain + OI skew — fresh option chains (1–3s)."""
    from feed import get_kite
    import adv_idx_options as _adio

    kite = get_kite()
    if not kite:
        return JSONResponse({"error": "Kite not available", "ts": __import__("time").time()})
    return JSONResponse(_adio.build_snapshot(kite, signals.state))


@app.post("/api/adv-idx-options/backtest")
async def adv_idx_options_backtest(request: Request):
    """
    Context replay: prefers table `adv_idx_options_daily` (see /download); else ohlcv ∩ vix_daily.
    Response may include `full_report` when using the dedicated table.
    """
    import asyncio

    import adv_idx_options_backtest as _adio_bt

    body: dict = {}
    try:
        body = await request.json()
    except Exception:
        body = {}

    days = int(body.get("days") or 180)
    min_score = float(body.get("min_score") or 60)
    verdict_mode = str(body.get("verdict_mode") or "none")

    try:
        loop = asyncio.get_event_loop()
        out = await loop.run_in_executor(
            None,
            lambda: _adio_bt.run_adv_idx_options_backtest(
                days=days, min_score=min_score, verdict_mode=verdict_mode
            ),
        )
        return JSONResponse(out)
    except Exception as e:
        logger.error(f"adv_idx_options_backtest: {e}", exc_info=True)
        return JSONResponse({"error": str(e), "trades_executed": 0}, status_code=500)


@app.post("/api/adv-idx-options/download")
async def adv_idx_options_download(request: Request):
    """Download NIFTY + India VIX daily into `adv_idx_options_daily` (separate from ohlcv/vix_daily)."""
    import asyncio

    from feed import get_kite

    import adv_idx_options_storage as _adio_st

    kite = get_kite()
    if not kite:
        return JSONResponse({"error": "Kite not available"}, status_code=503)

    body: dict = {}
    try:
        body = await request.json()
    except Exception:
        body = {}
    days = int(body.get("days") or 730)

    try:
        loop = asyncio.get_event_loop()
        out = await loop.run_in_executor(
            None,
            lambda: _adio_st.download_adv_idx_options_from_kite(kite, days=days),
        )
        return JSONResponse(out)
    except Exception as e:
        logger.error(f"adv_idx_options_download: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/adv-idx-options/sync-local")
async def adv_idx_options_sync_local():
    """Populate adv_idx_options_daily from local ohlcv+vix_daily (no Kite needed)."""
    import asyncio
    import adv_idx_options_storage as _adio_st

    try:
        loop = asyncio.get_event_loop()
        out = await loop.run_in_executor(None, lambda: _adio_st.populate_from_local_db(days=3650))
        return JSONResponse(out)
    except Exception as e:
        logger.error(f"adv_idx_options_sync_local: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/adv-idx-options/backtest-report")
async def adv_idx_options_backtest_report(
    days: int = 180,
    min_score: float = 60,
    verdict_mode: str = "none",
):
    """Structured report from `adv_idx_options_daily`: summary, monthly, next-session stats."""
    import asyncio

    import adv_idx_options_storage as _adio_st

    try:
        loop = asyncio.get_event_loop()
        out = await loop.run_in_executor(
            None,
            lambda: _adio_st.build_adv_idx_options_report(
                days=int(days),
                min_score=float(min_score),
                verdict_mode=str(verdict_mode or "none"),
            ),
        )
        return JSONResponse(out)
    except Exception as e:
        logger.error(f"adv_idx_options_backtest_report: {e}", exc_info=True)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/live-picks")
async def get_live_picks():
    """Compute live stock picks from current stocks cache."""
    lp = compute_live_picks(signals.state)
    return JSONResponse({"picks": lp["picks"][:8], "count": lp["total"]})


@app.get("/api/signals/history")
async def get_signals_history(
    limit: int = 100,
    status: str = "ALL",
    verdict: str | None = None,
    days: int | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
):
    import backtest_data as bd
    import datetime as _dt
    try:
        limit = max(1, min(int(limit), 50_000))
    except Exception:
        limit = 100
    min_trade_date = None
    if from_date:
        min_trade_date = str(from_date).strip()[:10]
    elif days is not None:
        try:
            raw = int(days)
            if raw > 0:
                d = min(raw, 365 * 12)
                min_trade_date = (_dt.date.today() - _dt.timedelta(days=d)).isoformat()
        except Exception:
            min_trade_date = None
    rows = bd.get_live_signal_history(
        limit=limit,
        status=status,
        verdict=verdict,
        min_trade_date=min_trade_date,
    )
    if from_date:
        fd = str(from_date).strip()[:10]
        rows = [r for r in rows if str(r.get("trade_date") or "") >= fd]
    if to_date:
        td = str(to_date).strip()[:10]
        rows = [r for r in rows if str(r.get("trade_date") or "") <= td]
    return JSONResponse({"rows": rows, "count": len(rows)})


@app.post("/api/signals/history/reset-spikes-today")
async def reset_signals_history_spikes_today():
    """
    Wipe live_signal_history and index_signal_history, then repopulate:
    - Spikes: today's list from Kite 1-min rebuild (cold-start rules) or in-memory spikes.
    - Index radar: today's rows from memory if index_signals_date matches today (IST).
    """
    import backtest_data as bd
    from feed import get_kite, feed_manager

    try:
        bd.wipe_live_signal_history()
        bd.wipe_index_signal_history()
    except Exception as e:
        logger.error(f"wipe signal/index history: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)

    kite = None
    if not getattr(feed_manager, "_demo_mode", False):
        try:
            kite = get_kite()
        except Exception as e:
            logger.warning(f"reset-spikes-today: no Kite ({e})")

    spikes: list = []
    source = "none"
    if kite:
        try:
            spikes = signals.build_today_spikes_from_kite_history(kite)
            if spikes:
                source = "kite"
        except Exception as e:
            logger.warning(f"reset-spikes-today Kite rebuild failed: {e}")

    today_ist = datetime.datetime.now(pytz.timezone("Asia/Kolkata")).date().isoformat()
    if not spikes and signals.state.get("spikes_date") == today_ist:
        spikes = list(signals.state.get("spikes") or [])
        if spikes:
            source = "state"

    gates = signals.state.get("gates") or {}
    verdict = signals.state.get("verdict") or "WAIT"
    pass_cnt = int(signals.state.get("pass_count") or 0)
    indices = signals.state.get("last_macro") or {}
    chain = signals.state.get("last_chain") or {}

    try:
        inserted = bd.log_live_spikes(spikes, gates, verdict, pass_cnt, indices, chain)
    except Exception as e:
        logger.error(f"log_live_spikes after reset: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)

    if spikes:
        signals.state["spikes"] = spikes
        signals.state["spikes_date"] = today_ist
        try:
            broadcast({"type": "spikes", "data": spikes, "ts": time.time()})
        except Exception:
            pass

    index_inserted = 0
    ix_source = "none"
    if signals.state.get("index_signals_date") == today_ist:
        ix_list = list(signals.state.get("index_signals") or [])
        try:
            index_inserted = bd.replace_index_signal_rows(ix_list, today_ist)
            ix_source = "state" if ix_list else "none"
        except Exception as e:
            logger.error(f"replace_index_signal_rows after reset: {e}", exc_info=True)
            return JSONResponse({"error": str(e)}, status_code=500)
    else:
        signals.state["index_signals"] = []
        signals.state["index_signals_date"] = today_ist
    try:
        broadcast({
            "type": "index_spikes",
            "data": signals.state.get("index_signals", []),
            "ts": time.time(),
        })
    except Exception:
        pass

    return JSONResponse({
        "ok": True,
        "trade_date": today_ist,
        "cleared_all": True,
        "inserted_rows": inserted,
        "spike_count": len(spikes),
        "source": source,
        "index_inserted_rows": index_inserted,
        "index_source": ix_source,
    })


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
    days = max(2, min(int(days), 180))
    from_date = str(today - timedelta(days=days))
    to_date = str(today - timedelta(days=1))
    # spikes_backtest accepts max 30-day windows for minute data; run in chunks.
    inserted = 0
    chunk_meta = []
    cur_start = datetime.strptime(from_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(to_date, "%Y-%m-%d").date()
    while cur_start <= end_date:
        cur_end = min(cur_start + timedelta(days=29), end_date)
        resp = await spikes_backtest(
            from_date=str(cur_start),
            to_date=str(cur_end),
            vol_min=2.0,
            price_min=0.3,
            trend_filter=True,
            time_from="09:15",
            time_to="14:30",
            min_score=45,
            early_fail_min=8,
            early_fail_adverse_pct=0.18,
            no_ft_min=15,
            no_ft_min_fav_pct=0.12,
        )
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
        ins = bd.import_historical_spike_results(payload.get("results", []))
        inserted += int(ins or 0)
        chunk_meta.append({
            "from_date": str(cur_start),
            "to_date": str(cur_end),
            "inserted": int(ins or 0),
            "signals": int((payload.get("summary") or {}).get("total", 0)),
        })
        cur_start = cur_end + timedelta(days=1)
    rows = bd.get_live_signal_history(limit=100, status="ALL")
    return JSONResponse({
        "inserted": inserted,
        "rows": rows,
        "count": len(rows),
        "from_date": from_date,
        "to_date": to_date,
        "days": days,
        "chunks": chunk_meta,
    })


@app.post("/api/signals/swing-backtest")
async def swing_radar_backtest_api(request: Request):
    """
    Replay Swing Radar on historical dates using backtest.db (NIFTY/VIX/PCR/FII) + Kite daily bars.
    Inserts closed rows (signal_key BT-SWING|...) into live_signal_history with verdict SWING_RADAR.
    Requires Kite access and prior /api/backtest/download for index/macro series.
    JSON body (optional): from_date, to_date (YYYY-MM-DD), max_forward_days (3–30, default 12).
    Default range: last ~3 years ending today.
    """
    import swing_radar_backtest as srb
    from datetime import datetime, timedelta
    from feed import get_kite

    try:
        body = await request.json()
    except Exception:
        body = {}
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.now(ist).date()
    to_d = str(body.get("to_date") or today.isoformat())[:10]
    from_d = str(body.get("from_date") or (today - timedelta(days=1095)).isoformat())[:10]
    try:
        max_fd = int(body.get("max_forward_days") or 12)
    except Exception:
        max_fd = 12
    max_fd = max(3, min(max_fd, 30))

    raw_clear = body.get("clear_existing", True)
    if isinstance(raw_clear, str):
        clear_existing = raw_clear.strip().lower() not in {"0", "false", "no", "off"}
    else:
        clear_existing = bool(raw_clear)

    kite = get_kite()
    if not kite:
        return JSONResponse({"error": "Kite not available"}, status_code=503)

    loop = asyncio.get_running_loop()

    def _run():
        return srb.run_swing_radar_backtest(kite, from_d, to_d, max_forward_days=max_fd, clear_existing=clear_existing)

    result = await loop.run_in_executor(None, _run)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return JSONResponse(result)


@app.get("/api/swing-radar-backtest-report")
async def swing_radar_backtest_report(from_date: str | None = None, to_date: str | None = None):
    import swing_radar_backtest as srb

    report = srb.build_swing_backtest_report(from_date, to_date)
    return JSONResponse(report)


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
async def backtest_download(request: Request):
    """Start historical data download (runs in background thread)."""
    import asyncio, backtest_data as bd

    try:
        body = await request.json()
    except Exception:
        body = {}
    days = max(60, min(int(body.get("days") or 1095), 1825))

    async def _dl():
        loop = asyncio.get_event_loop()
        try:
            bd.init_db()
            from feed import get_kite
            kite = get_kite()
            await loop.run_in_executor(None, lambda: bd.download_kite_history(kite, days=days))
            await loop.run_in_executor(None, lambda: bd.download_chain_history(days=days))
            await loop.run_in_executor(None, lambda: bd.download_fii_history(days=days))
            await loop.run_in_executor(None, bd.fill_outcomes)
            logger.info("Backtest data download complete")
        except Exception as e:
            logger.error(f"Backtest download error: {e}", exc_info=True)

    asyncio.create_task(_dl())
    return JSONResponse({"message": f"Download started - up to {days} days of NIFTY OHLCV, VIX, chain PCR, and FII history. Check /api/backtest/status."})


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


@app.post("/api/backtest/multi-report")
async def backtest_multi_report(request: Request):
    """
    One gate-engine replay with full history, then rolling metrics for 1W / 1M / 6M calendar lookbacks.

    - refresh_kite (default True): pull NIFTY + India VIX daily from Zerodha Kite into backtest.db.
    - refresh_nse (default False): refresh PCR + FII from NSE archives (slow); use ⬇ DOWNLOAD DATA for full 3Y.
    """
    import asyncio
    from datetime import datetime, timedelta

    import backtest_data as bd
    import backtest_engine as be

    body: dict = {}
    try:
        body = await request.json()
    except Exception:
        body = {}

    mode = str(body.get("mode") or "intraday")
    if mode not in ("intraday", "positional"):
        mode = "intraday"
    refresh_kite = bool(body.get("refresh_kite", True))
    refresh_nse = bool(body.get("refresh_nse", False))
    kite_days = max(60, min(int(body.get("kite_days") or 280), 1095))
    nse_days = max(60, min(int(body.get("nse_days") or 220), 1095))
    history_span = max(120, min(int(body.get("history_span_days") or 280), 1095))

    periods_cfg = {
        "1w": {"calendar_days": int(body.get("days_1w") or 7), "label": "Last 7 calendar days"},
        "1m": {"calendar_days": int(body.get("days_1m") or 30), "label": "Last 30 calendar days"},
        "6m": {"calendar_days": int(body.get("days_6m") or 183), "label": "Last ~6 months (183d)"},
    }

    def _work() -> dict:
        bd.init_db()
        summary_before = bd.get_data_summary()
        kite_status = "skipped"
        from feed import get_kite

        kite = get_kite()
        if refresh_kite:
            if kite:
                bd.download_kite_history(kite, days=kite_days)
                kite_status = "ok"
            else:
                kite_status = "no_kite"
        if refresh_nse:
            bd.download_chain_history(days=nse_days)
            bd.download_fii_history(days=nse_days)

        to_d = datetime.now().date()
        from_d = to_d - timedelta(days=history_span)
        full = be.run_backtest(
            from_d.isoformat(),
            to_d.isoformat(),
            mode,
            persist=False,
            trim_trade_log=False,
        )

        if full.get("error"):
            return {
                "error": full["error"],
                "kite_refresh": kite_status,
                "data_summary": bd.get_data_summary(),
                "data_summary_before": summary_before,
            }

        all_trades = full.get("trades_all") or full.get("trades") or []
        dates = [t.get("date") for t in all_trades if t.get("date")]
        anchor = to_d
        if dates:
            try:
                anchor = max(datetime.strptime(d, "%Y-%m-%d").date() for d in dates)
            except ValueError:
                pass

        periods_out = {}
        for key, meta in periods_cfg.items():
            cal = max(1, meta["calendar_days"])
            cut = (anchor - timedelta(days=cal)).isoformat()
            slice_rows = [t for t in all_trades if t.get("date") and t["date"] >= cut]
            agg = be.aggregate_backtest_window(slice_rows)
            exec_rows = [
                t
                for t in sorted(slice_rows, key=lambda x: x.get("date") or "", reverse=True)
                if t.get("verdict") == "EXECUTE"
            ][:25]
            periods_out[key] = {
                "label": meta["label"],
                "window_start": cut,
                "window_end": anchor.isoformat(),
                "metrics": agg["metrics"],
                "gate_stats": agg["gate_stats"],
                "trades_sample": exec_rows,
            }

        return {
            "ok": True,
            "mode": mode,
            "anchor_date": anchor.isoformat(),
            "replay_range": {"from": from_d.isoformat(), "to": to_d.isoformat()},
            "kite_refresh": kite_status,
            "nse_refresh": "ran" if refresh_nse else "skipped",
            "data_summary": bd.get_data_summary(),
            "full_metrics": full.get("metrics"),
            "periods": periods_out,
            "disclaimer": (
                "Gate-engine daily replay (next-day open simulation). "
                "Zerodha supplies NIFTY + VIX; PCR/FII come from NSE archives in backtest.db — "
                "run ⬇ DOWNLOAD DATA if coverage is thin."
            ),
        }

    try:
        loop = asyncio.get_event_loop()
        payload = await loop.run_in_executor(None, _work)
        return JSONResponse(payload)
    except Exception as e:
        logger.error(f"backtest multi-report: {e}", exc_info=True)
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
    import config as _cfg
    import time as _time
    has_tok = bool((_cfg.KITE_ACCESS_TOKEN or "").strip())
    if not has_tok:
        return JSONResponse(
            {
                "valid": False,
                "has_token": False,
                "error": "KITE_ACCESS_TOKEN missing — set in backend/.env or use token refresh",
                "uptime_h": round((_time.time() - _start_time) / 3600, 1),
            }
        )
    try:
        from feed import get_kite
        kite = get_kite()
        profile = kite.profile()
        return JSONResponse({
            "valid": True,
            "has_token": True,
            "user": profile.get("user_name", ""),
            "uptime_h": round((_time.time() - _start_time) / 3600, 1),
        })
    except Exception as e:
        return JSONResponse({
            "valid": False,
            "has_token": True,
            "error": str(e),
            "uptime_h": round((_time.time() - _start_time) / 3600, 1),
        })


@app.post("/api/token-refresh")
async def token_refresh_manual():
    """Manually trigger a Kite token refresh with a bounded subprocess."""
    import asyncio as _aio
    import subprocess as _subprocess
    import sys as _sys
    from pathlib import Path
    from dotenv import load_dotenv

    _envp = Path(__file__).resolve().parent / ".env"
    if _envp.is_file():
        load_dotenv(_envp, override=True)

    missing = [k for k in ("KITE_USER_ID", "KITE_PASSWORD", "KITE_TOTP_SECRET", "KITE_API_KEY", "KITE_API_SECRET")
               if not os.getenv(k, "").strip()]
    if missing:
        return JSONResponse({
            "ok": False,
            "msg": f"Missing env vars: {', '.join(missing)}. Add these to backend/.env or your host environment and restart."
        }, status_code=400)

    _script = Path(__file__).resolve().parent / "auto_token.py"
    _timeout_s = max(45, int(os.getenv("TOKEN_REFRESH_TIMEOUT_SECONDS", "150") or 150))

    def _summarize_cli(output: str) -> str:
        lines = [str(line or "").strip() for line in str(output or "").splitlines()]
        lines = [
            line for line in lines
            if line
            and not set(line) <= {"="}
            and "STOCKR.IN v5" not in line
            and "auto_token  INFO" not in line
        ]
        for line in reversed(lines):
            low = line.lower()
            if "failed" in low or "missing" in low or "not installed" in low or "browser login failed" in low:
                return line
        return lines[-1] if lines else "Auto refresh did not complete. Use One-Time Kite Login or verify backend/.env credentials."

    loop = _aio.get_event_loop()

    def _do():
        try:
            proc = _subprocess.run(
                [_sys.executable, str(_script)],
                cwd=str(_script.parent),
                capture_output=True,
                text=True,
                timeout=_timeout_s,
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
        except _subprocess.TimeoutExpired as exc:
            combined = "\n".join(part for part in [exc.stdout or "", exc.stderr or ""] if part).strip()
            return (False, f"Auto refresh timed out after {_timeout_s}s. {_summarize_cli(combined)}")
        except Exception as exc:
            return (False, str(exc))
        combined = "\n".join(part for part in [proc.stdout or "", proc.stderr or ""] if part).strip()
        if proc.returncode == 0:
            return (True, _summarize_cli(combined))
        return (False, _summarize_cli(combined))

    result, errmsg = await loop.run_in_executor(None, _do)
    if result:
        try:
            from feed import fetch_quotes_rest
            from scheduler import _apply_new_token

            _apply_new_token()
            fetch_quotes_rest()
        except Exception as exc:
            logger.warning("token-refresh: fetch_quotes_rest: %s", exc)
        try:
            await _aio.wait_for(refresh_state(), timeout=20)
        except Exception as exc:
            logger.warning("token-refresh: refresh_state: %s", exc)
        return JSONResponse({"ok": True, "msg": "Token refreshed and applied live"})
    return JSONResponse({"ok": False, "msg": errmsg}, status_code=500)


@app.post("/api/token-reload-env")
async def token_reload_from_env():
    """
    Reload KITE_ACCESS_TOKEN from backend/.env into this process (no Playwright).
    Use after: python auto_token.py, generate_token.py, or pasting a new token into .env
    while the server is already running.
    """
    from pathlib import Path
    from dotenv import load_dotenv

    _envp = Path(__file__).resolve().parent / ".env"
    if not _envp.is_file():
        return JSONResponse({"ok": False, "msg": "backend/.env not found"}, status_code=404)
    load_dotenv(_envp, override=True)
    if not os.getenv("KITE_ACCESS_TOKEN", "").strip():
        return JSONResponse(
            {"ok": False, "msg": "KITE_ACCESS_TOKEN is empty in backend/.env — run auto_token or paste token"},
            status_code=400,
        )
    try:
        from scheduler import _apply_new_token

        _apply_new_token()
        from feed import get_kite

        profile = get_kite().profile()
        try:
            from feed import fetch_quotes_rest

            fetch_quotes_rest()
        except Exception as _qe:
            logger.warning("token-reload-env: fetch_quotes_rest: %s", _qe)
        await refresh_state()
        return JSONResponse(
            {
                "ok": True,
                "msg": "Token loaded from .env and applied",
                "user": profile.get("user_name", ""),
            }
        )
    except Exception as e:
        logger.error("token-reload-env: %s", e, exc_info=True)
        return JSONResponse({"ok": False, "msg": str(e)}, status_code=500)


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
    Intraday trade sim = first session after signal only. Positional = same entry/TP/SL
    walked across up to GATE["positional_max_hold_days"] daily sessions (time-stop on last bar).
    Also attempts to fetch 5-min intraday candles for recent dates (within 60 days).
    """
    import statistics as _stat
    from datetime import date as _date_cls, timedelta as _td, datetime as _dt_cls
    from config import KITE_TOKENS, FNO_SYMBOLS, LOT_SIZES, GATE as TH
    from backtest_engine import _g1, _g2, _g3, _g4, _g5, _verdict
    from dayview_trade_sim import simulate_dayview_positional_long
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

                # Entry next session open; intraday = 1st bar only, positional = multi-day path
                try:
                    nxt_hist = kite.historical_data(token, sel_date + _td(days=1), sel_date + _td(days=45), "day")
                    nxt_d = nxt_hist[0] if nxt_hist else None
                except Exception:
                    nxt_hist = []
                    nxt_d = None

                entry = round(nxt_d["open"], 2) if nxt_d else None
                trade_i = trade_p = None
                if entry:
                    stop_dist = round(atr_v * TH["atr_multiplier"], 2)
                    rr_i = TH["rr_min_intraday"]
                    target_i = round(entry + round(stop_dist * rr_i, 2), 2)
                    stop_i = round(entry - stop_dist, 2)
                    nxt_high = round(nxt_d["high"], 2)
                    nxt_low = round(nxt_d["low"], 2)
                    nxt_close = round(nxt_d["close"], 2)
                    if nxt_high >= target_i:
                        exit_p, outcome = target_i, "WIN"
                    elif nxt_low <= stop_i:
                        exit_p, outcome = stop_i, "LOSS"
                    else:
                        exit_p = nxt_close
                        diff = nxt_close - entry
                        thr = max(10, round(atr_v * 0.3))
                        outcome = "WIN" if diff >= thr else "LOSS" if diff <= -thr else "NEUTRAL"
                    pnl = round(exit_p - entry, 2)
                    nd = nxt_d["date"].strftime("%Y-%m-%d") if hasattr(nxt_d["date"], "strftime") else str(nxt_d["date"])[:10]
                    trade_i = {
                        "entry": entry, "target": target_i, "stop": stop_i,
                        "exit": exit_p, "pnl": pnl, "outcome": outcome,
                        "next_date": nd,
                    }
                    rr_p = TH["rr_min_positional"]
                    target_p = round(entry + round(stop_dist * rr_p, 2), 2)
                    stop_p = round(entry - stop_dist, 2)
                    max_hold = int(TH.get("positional_max_hold_days", 10))
                    trade_p = simulate_dayview_positional_long(
                        entry, target_p, stop_p, nxt_hist, atr_v, max_hold
                    )

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
    early_fail_min: int = 8,
    early_fail_adverse_pct: float = 0.18,
    no_ft_min: int = 15,
    no_ft_min_fav_pct: float = 0.12,
    universe: str = "AUTO",
):
    """
    Backtest Spike Radar strategy using 1-min OHLCV candles from Kite.
    v4 — OPTIMIZED (73% WR on Feb-Apr 2026):
    - Vol: >=1.5x | Price: >=0.2% | Time: 9:30-14:00
    - Score 0-100 based on vol quality, price momentum, symbol, time slot
    - 20-min cooldown per symbol per day
    - Entry: next candle open; SL=-0.25%, T1=+0.30%, T2=+0.60%; 45-candle window
    - Production exits: early fail + no-follow-through guard
    """
    import asyncio
    from datetime import datetime, timedelta
    from feed import get_kite
    from config import KITE_TOKENS, FNO_SYMBOLS, GATE as TH
    import fetcher as _fetcher

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
            summary = {
                "total": 0, "hit_t1": 0, "hit_t2": 0, "hit_sl": 0, "expired": 0,
                "early_fail": 0, "no_follow_through": 0
            }
            score_accumulator = []
            symbols_ok = 0
            symbols_failed = 0
            last_error = None

            u = str(universe or "AUTO").upper()
            if u == "AUTO":
                u = str(TH.get("spike_universe", "FNO") or "FNO").upper()
            if u == "NIFTY200":
                n200_map = _fetcher.get_nifty200_kite_tokens(kite) or {}
                symbol_tokens = {s: t for s, t in n200_map.items()}
            else:
                symbol_tokens = {
                    s: KITE_TOKENS[s]
                    for s in FNO_SYMBOLS
                    if s in KITE_TOKENS and s not in ("INDIAVIX", "NIFTY", "BANKNIFTY")
                }
            symbols = sorted(symbol_tokens.keys())

            for sym in symbols:
                token = symbol_tokens.get(sym)
                if not token:
                    continue
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
                    max_fav_pct = 0.0
                    max_adv_pct = 0.0

                    # Check next 45 candles (45 min)
                    for k in range(i + 1, min(i + 46, len(candles))):
                        hi = candles[k]["high"]
                        lo = candles[k]["low"]
                        close_k = candles[k].get("close", entry)
                        bars_in_trade = k - i
                        if is_buy:
                            fav_pct = ((hi - entry) / entry) * 100 if entry else 0.0
                            adv_pct = ((entry - lo) / entry) * 100 if entry else 0.0
                        else:
                            fav_pct = ((entry - lo) / entry) * 100 if entry else 0.0
                            adv_pct = ((hi - entry) / entry) * 100 if entry else 0.0
                        max_fav_pct = max(max_fav_pct, fav_pct)
                        max_adv_pct = max(max_adv_pct, adv_pct)
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

                        # Early-fail: if move goes adverse quickly without follow-through.
                        if bars_in_trade >= max(1, int(early_fail_min)) and max_adv_pct >= max(0.05, float(early_fail_adverse_pct)):
                            result = "EARLY_FAIL"
                            exit_p = round(close_k, 2)
                            break
                        # No-follow-through: enough time has passed but no impulse developed.
                        if bars_in_trade >= max(3, int(no_ft_min)) and max_fav_pct < max(0.05, float(no_ft_min_fav_pct)):
                            result = "NO_FOLLOW"
                            exit_p = round(close_k, 2)
                            break

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
                    elif result == "EARLY_FAIL": summary["early_fail"] += 1
                    elif result == "NO_FOLLOW": summary["no_follow_through"] += 1
                    else:                    summary["expired"] += 1

                    score_accumulator.append(score)

                    # Mark cooldown
                    last_signal_min[dt] = candle_min

            # sort by time desc
            results.sort(key=lambda x: x["time"], reverse=True)

            hits      = summary["hit_t1"] + summary["hit_t2"]
            resolved  = hits + summary["hit_sl"] + summary["early_fail"] + summary["no_follow_through"]
            win_rate  = round(hits / summary["total"] * 100, 1) if summary["total"] else 0
            resolved_win_rate = round(hits / resolved * 100, 1) if resolved else 0
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
                "summary":  {**summary, "win_rate": win_rate, "resolved_win_rate": resolved_win_rate, "avg_pnl": avg_pnl,
                             "expectancy_pct": expect, "avg_score": avg_score,
                             "symbols_ok": symbols_ok, "symbols_failed": symbols_failed},
                "results":  results[:500],
                "from_date": from_date,
                "to_date":   to_date,
                "params": {
                    "universe": u,
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
                    "early_fail_min": early_fail_min,
                    "early_fail_adverse_pct": early_fail_adverse_pct,
                    "no_ft_min": no_ft_min,
                    "no_ft_min_fav_pct": no_ft_min_fav_pct,
                },
            }

        data = await loop.run_in_executor(None, _run)
        return JSONResponse(data)

    except Exception as e:
        logger.error(f"Spike backtest error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/spikes/report")
async def spikes_report(
    days: int = 90,
    universe: str = "AUTO",
    nifty200: int = 0,
    vol_min: float = 1.5,
    price_min: float = 0.2,
    trend_filter: bool = True,
    time_from: str = "09:30",
    time_to: str = "14:00",
    min_score: int = 45,
    early_fail_min: int = 8,
    early_fail_adverse_pct: float = 0.18,
    no_ft_min: int = 15,
    no_ft_min_fav_pct: float = 0.12,
):
    """
    Run spike backtest over `days` (calendar days, ending yesterday IST) in ≤30-day chunks
    (Kite 1-min limit). Aggregates summaries across chunks; omits per-trade rows to keep payload small.
    """
    from datetime import datetime as _dt, timedelta as _td

    try:
        if days < 1 or days > 366:
            return JSONResponse({"error": "days must be between 1 and 366"}, status_code=400)

        uni = "NIFTY200" if int(nifty200) else str(universe or "AUTO")

        ist = pytz.timezone("Asia/Kolkata")
        today = _dt.now(ist).date()
        td = today - _td(days=1)
        fd = td - _td(days=days - 1)

        agg = {
            "total": 0,
            "hit_t1": 0,
            "hit_t2": 0,
            "hit_sl": 0,
            "expired": 0,
            "early_fail": 0,
            "no_follow_through": 0,
        }
        symbols_ok = 0
        symbols_failed = 0
        w_pnl = 0.0
        w_score = 0.0
        chunk_rows = []

        cur = fd
        while cur <= td:
            chunk_to = min(cur + _td(days=30), td)
            s_from = cur.strftime("%Y-%m-%d")
            s_to = chunk_to.strftime("%Y-%m-%d")

            resp = await spikes_backtest(
                from_date=s_from,
                to_date=s_to,
                vol_min=vol_min,
                price_min=price_min,
                trend_filter=trend_filter,
                time_from=time_from,
                time_to=time_to,
                min_score=min_score,
                early_fail_min=early_fail_min,
                early_fail_adverse_pct=early_fail_adverse_pct,
                no_ft_min=no_ft_min,
                no_ft_min_fav_pct=no_ft_min_fav_pct,
                universe=uni,
            )
            if resp.status_code >= 400:
                return resp
            payload = json.loads(resp.body.decode())
            if payload.get("error"):
                return JSONResponse(payload, status_code=500)

            summ = payload.get("summary") or {}
            t = int(summ.get("total") or 0)
            for k in agg:
                agg[k] += int(summ.get(k) or 0)
            symbols_ok += int(summ.get("symbols_ok") or 0)
            symbols_failed += int(summ.get("symbols_failed") or 0)
            if t:
                w_pnl += float(summ.get("avg_pnl") or 0) * t
                w_score += float(summ.get("avg_score") or 0) * t

            chunk_rows.append({
                "from_date": s_from,
                "to_date": s_to,
                "summary": summ,
            })

            cur = chunk_to + _td(days=1)

        hits = agg["hit_t1"] + agg["hit_t2"]
        resolved = hits + agg["hit_sl"] + agg["early_fail"] + agg["no_follow_through"]
        win_rate = round(hits / agg["total"] * 100, 1) if agg["total"] else 0.0
        resolved_win_rate = round(hits / resolved * 100, 1) if resolved else 0.0
        avg_pnl = round(w_pnl / agg["total"], 3) if agg["total"] else 0.0
        avg_score = round(w_score / agg["total"], 1) if agg["total"] else 0.0
        expectancy_pct = round(
            (hits / agg["total"] * (0.003 + 0.006) / 2 - agg["hit_sl"] / agg["total"] * 0.0025) * 100,
            3,
        ) if agg["total"] else 0.0

        out_summary = {
            **agg,
            "win_rate": win_rate,
            "resolved_win_rate": resolved_win_rate,
            "avg_pnl": avg_pnl,
            "expectancy_pct": expectancy_pct,
            "avg_score": avg_score,
            "symbols_ok": symbols_ok,
            "symbols_failed": symbols_failed,
        }

        return JSONResponse(
            {
                "from_date": fd.strftime("%Y-%m-%d"),
                "to_date": td.strftime("%Y-%m-%d"),
                "days_calendar": days,
                "chunks": chunk_rows,
                "summary": out_summary,
                "params": {
                    "universe": uni,
                    "vol_min": vol_min,
                    "price_min": price_min,
                    "trend_filter": trend_filter,
                    "time_from": time_from,
                    "time_to": time_to,
                    "min_score": min_score,
                    "early_fail_min": early_fail_min,
                    "early_fail_adverse_pct": early_fail_adverse_pct,
                    "no_ft_min": no_ft_min,
                    "no_ft_min_fav_pct": no_ft_min_fav_pct,
                },
            }
        )

    except Exception as e:
        logger.error(f"Spikes report error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/spikes/universe-cards")
async def spikes_universe_cards(
    days: int = 30,
    universe: str = "NIFTY200",
    include_today: int = 1,
    max_results: int = 800,
    vol_min: float = 1.5,
    price_min: float = 0.2,
    trend_filter: bool = True,
    time_from: str = "09:30",
    time_to: str = "14:00",
    min_score: int = 45,
    early_fail_min: int = 8,
    early_fail_adverse_pct: float = 0.18,
    no_ft_min: int = 15,
    no_ft_min_fav_pct: float = 0.12,
):
    """
    NIFTY 200 (or FNO) spike backtest over a calendar window, merged into one trade list for UI cards.
    Chunks to respect the 30-day Kite 1-min limit. `include_today=1` ends on today IST (partial session OK).
    """
    from datetime import datetime as _dt, timedelta as _td

    try:
        days = max(1, min(int(days), 366))
        max_results = max(50, min(int(max_results), 2500))
        uni = str(universe or "NIFTY200").upper()
        if uni not in ("NIFTY200", "FNO", "AUTO"):
            uni = "NIFTY200"

        ist = pytz.timezone("Asia/Kolkata")
        today = _dt.now(ist).date()
        td = today if int(include_today) else (today - _td(days=1))
        fd = td - _td(days=days - 1)

        merged: list = []
        seen: set = set()
        chunk_meta = []

        cur = fd
        while cur <= td:
            chunk_to = min(cur + _td(days=30), td)
            s_from = cur.strftime("%Y-%m-%d")
            s_to = chunk_to.strftime("%Y-%m-%d")

            resp = await spikes_backtest(
                from_date=s_from,
                to_date=s_to,
                vol_min=vol_min,
                price_min=price_min,
                trend_filter=trend_filter,
                time_from=time_from,
                time_to=time_to,
                min_score=min_score,
                early_fail_min=early_fail_min,
                early_fail_adverse_pct=early_fail_adverse_pct,
                no_ft_min=no_ft_min,
                no_ft_min_fav_pct=no_ft_min_fav_pct,
                universe=uni,
            )
            if resp.status_code >= 400:
                return resp
            payload = json.loads(resp.body.decode())
            if payload.get("error"):
                return JSONResponse(payload, status_code=500)

            rows = payload.get("results") or []
            for r in rows:
                key = (
                    str(r.get("symbol") or ""),
                    str(r.get("time") or ""),
                    str(r.get("type") or ""),
                )
                if key in seen:
                    continue
                seen.add(key)
                merged.append(r)

            chunk_meta.append({"from_date": s_from, "to_date": s_to, "rows": len(rows)})
            cur = chunk_to + _td(days=1)

        merged.sort(key=lambda x: str(x.get("time") or ""), reverse=True)
        merged = merged[:max_results]

        summ = {
            "total": 0,
            "hit_t1": 0,
            "hit_t2": 0,
            "hit_sl": 0,
            "expired": 0,
            "early_fail": 0,
            "no_follow_through": 0,
        }
        for r in merged:
            summ["total"] += 1
            res = str(r.get("result") or "").upper()
            if res == "HIT_T1":
                summ["hit_t1"] += 1
            elif res == "HIT_T2":
                summ["hit_t2"] += 1
            elif res == "HIT_SL":
                summ["hit_sl"] += 1
            elif res == "EXPIRED":
                summ["expired"] += 1
            elif res == "EARLY_FAIL":
                summ["early_fail"] += 1
            elif res in ("NO_FOLLOW", "NO FOLLOW"):
                summ["no_follow_through"] += 1

        hits = summ["hit_t1"] + summ["hit_t2"]
        summ["win_rate"] = round(hits / summ["total"] * 100, 1) if summ["total"] else 0.0

        return JSONResponse(
            {
                "from_date": fd.strftime("%Y-%m-%d"),
                "to_date": td.strftime("%Y-%m-%d"),
                "days_calendar": days,
                "universe": uni,
                "chunks": chunk_meta,
                "summary": summ,
                "results": merged,
                "params": {
                    "vol_min": vol_min,
                    "price_min": price_min,
                    "trend_filter": trend_filter,
                    "time_from": time_from,
                    "time_to": time_to,
                    "min_score": min_score,
                },
            }
        )

    except Exception as e:
        logger.error(f"universe-cards error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/backtest/download-fii")
async def backtest_download_fii(request: Request):
    """Download FII/DII daily net flow history from NSE."""
    import asyncio, backtest_data as bd

    try:
        body = await request.json()
    except Exception:
        body = {}
    days = max(60, min(int(body.get("days") or 1095), 1825))

    async def _dl():
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, lambda: bd.download_fii_history(days=days))
            logger.info("FII history download complete")
        except Exception as e:
            logger.error(f"FII download error: {e}", exc_info=True)

    asyncio.create_task(_dl())
    return JSONResponse({"message": f"FII history download started for up to {days} days. Check /api/backtest/status."})


# ─── TELEGRAM ─────────────────────────────────────────────────────────────────
@app.post("/api/telegram/test")
async def telegram_test():
    """Send a test Telegram message to every configured chat_id (primary + TELEGRAM_CHAT_IDS + harshvtrade)."""
    from config import TELEGRAM_BOT_TOKEN, get_telegram_chat_ids

    chat_ids = get_telegram_chat_ids()
    if not TELEGRAM_BOT_TOKEN or not chat_ids:
        return JSONResponse(
            {"ok": False, "error": "TELEGRAM_BOT_TOKEN or chat id(s) not set in .env (TELEGRAM_CHAT_ID / TELEGRAM_CHAT_IDS / TELEGRAM_CHAT_ID_HARSHVTRADE)"},
            status_code=400,
        )

    msg = (
        "✅ <b>STOCKR.IN — Telegram test</b>\n"
        "Bot is connected. SPIKE HUNT + ADV INDEX alerts go to this chat.\n"
        f"Server verdict: <b>{signals.state['verdict']}</b>  "
        f"Gates: {signals.state['pass_count']}/5"
    )
    try:
        import requests as _req
        loop = asyncio.get_event_loop()

        def _post_all():
            errs = []
            oks = 0
            for cid in chat_ids:
                try:
                    r = _req.post(
                        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                        json={"chat_id": cid, "text": msg, "parse_mode": "HTML"},
                        timeout=8,
                    ).json()
                    if r.get("ok"):
                        oks += 1
                    else:
                        errs.append(f"{cid}: {r.get('description', 'error')}")
                except Exception as ex:
                    errs.append(f"{cid}: {ex}")
            return oks, errs

        oks, errs = await loop.run_in_executor(None, _post_all)
        if oks:
            return JSONResponse(
                {
                    "ok": True,
                    "message": f"Test sent to {oks}/{len(chat_ids)} chat(s)",
                    "chats": len(chat_ids),
                    "failures": errs,
                }
            )
        return JSONResponse({"ok": False, "error": "; ".join(errs) or "All sends failed"}, status_code=502)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=502)


@app.post("/api/telegram/today-digest")
async def telegram_today_digest(date: str = None):
    """Send today's (or ?date=YYYY-MM-DD) persisted signals digest to all configured Telegram chats."""
    try:
        from signals import send_today_signals_digest

        res = send_today_signals_digest(date)
        status = 200 if res.get("ok") else 502
        return JSONResponse(res, status_code=status)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


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
        f"STOCKR.IN — WhatsApp test\n"
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


# ─── EDGE ENGINE API ──────────────────────────────────────────────────────────
import edge_engine as _ee

@app.get("/api/edge/snapshot")
async def edge_snapshot():
    """Full edge engine snapshot — regime, L1/L2/L3, composite, risk, GEX."""
    try:
        from feed import get_all_prices
        snap = _ee.compute_edge_snapshot(
            state=signals.state,
            prices=get_all_prices(),
            chain=signals.state.get("last_chain"),
        )
        return JSONResponse(snap)
    except Exception as e:
        logger.error(f"edge_snapshot error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/edge/regime")
async def edge_regime():
    """Regime classifier only — fast poll endpoint."""
    try:
        from feed import price_history
        indices = signals.state.get("last_macro") or {}
        vix     = float(indices.get("vix", 15) or 15)
        pcr     = float(indices.get("pcr", 1.0) or 1.0)
        nchg    = float(indices.get("nifty_chg", 0) or 0)
        hist    = list(price_history.get("NIFTY", []))
        closes  = [h[1] for h in hist[-50:]]
        adx     = _ee.calc_adx(closes) if len(closes) >= 16 else 20.0
        regime  = _ee.classify_regime(vix, adx, nchg, pcr)
        session = _ee.session_gate()
        return JSONResponse({"regime": regime, "session": session, "ts": time.time()})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/edge/gex")
async def edge_gex():
    """GEX computation from live option chain."""
    try:
        chain = signals.state.get("last_chain") or {}
        gex   = _ee.compute_gex(chain)
        return JSONResponse(gex)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/edge/backtest")
async def edge_backtest(days: int = 90):
    """Run edge model backtest on historical DB data."""
    try:
        days = max(30, min(days, 500))
        result = _ee.run_edge_backtest(days)
        return JSONResponse(result)
    except Exception as e:
        logger.error(f"edge_backtest error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/edge/position-size")
async def edge_position_size(request: Request):
    """Position sizing calculator."""
    try:
        body    = await request.json()
        account = float(body.get("account", 500000))
        entry   = float(body.get("entry", 100))
        sl      = float(body.get("sl", 70))
        delta   = float(body.get("delta", 0.50))
        lot_sz  = int(body.get("lot_size", 50))
        result  = _ee.compute_position_size(account, entry, sl, delta, lot_sz)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ─────────────────────────────────────────────────────────────────────────────
# HISTORY API — daily heatmap + drilldown
# ─────────────────────────────────────────────────────────────────────────────
@app.get("/api/history/calendar")
async def history_calendar(months: int = 6):
    """Daily performance summary for heatmap calendar."""
    import sqlite3 as _sq
    from datetime import date as _d, timedelta
    db_path = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
    cutoff = (_d.today() - timedelta(days=months * 31)).isoformat()
    try:
        conn = _sq.connect(db_path)
        rows = conn.execute("""
            SELECT trade_date,
                COUNT(*) as signals,
                SUM(CASE WHEN outcome LIKE '%TARGET%' OR outcome LIKE '%WIN%' OR outcome LIKE '%HIT_T%'
                         THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN outcome LIKE '%SL%' OR outcome LIKE '%LOSS%'
                         THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) as open_cnt,
                SUM(COALESCE(pnl_pts,0)) as total_pts,
                AVG(CASE WHEN pnl_pts IS NOT NULL THEN pnl_pts ELSE NULL END) as avg_pts
            FROM live_signal_history
            WHERE trade_date >= ?
            GROUP BY trade_date
            ORDER BY trade_date ASC
        """, (cutoff,)).fetchall()
        conn.close()
        days = []
        for r in rows:
            total = r[1] or 0
            wins = r[2] or 0
            losses = r[3] or 0
            executed = total - (r[4] or 0)
            wr = round(wins / executed * 100, 1) if executed > 0 else 0
            pts = round(r[5] or 0, 2)
            # Estimate INR P&L (₹1L model: ~₹100 per point for options)
            pnl_inr = round(pts * 100, 0)
            days.append({
                "date": r[0],
                "signals": total,
                "wins": wins,
                "losses": losses,
                "open": r[4] or 0,
                "win_rate": wr,
                "pts": pts,
                "pnl_inr": pnl_inr,
            })
        return JSONResponse({"days": days})
    except Exception as e:
        logger.error(f"history_calendar error: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/history/stats")
async def history_stats(months: int = 3):
    """Aggregate stats for the history page header."""
    import sqlite3 as _sq
    from datetime import date as _d, timedelta
    db_path = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
    cutoff = (_d.today() - timedelta(days=months * 31)).isoformat()
    try:
        conn = _sq.connect(db_path)
        agg = conn.execute("""
            SELECT
                COUNT(DISTINCT trade_date) as trading_days,
                COUNT(*) as total_signals,
                SUM(CASE WHEN outcome LIKE '%TARGET%' OR outcome LIKE '%WIN%' OR outcome LIKE '%HIT_T%'
                         THEN 1 ELSE 0 END) as total_wins,
                SUM(CASE WHEN outcome LIKE '%SL%' OR outcome LIKE '%LOSS%'
                         THEN 1 ELSE 0 END) as total_losses,
                SUM(COALESCE(pnl_pts,0)) as total_pts,
                MAX(CASE WHEN outcome LIKE '%TARGET%' OR outcome LIKE '%WIN%' OR outcome LIKE '%HIT_T%'
                         THEN pnl_pts ELSE 0 END) as best_pts,
                MIN(CASE WHEN outcome LIKE '%SL%' OR outcome LIKE '%LOSS%'
                         THEN pnl_pts ELSE 0 END) as worst_pts
            FROM live_signal_history WHERE trade_date >= ?
        """, (cutoff,)).fetchone()

        # Streak calculation
        day_rows = conn.execute("""
            SELECT trade_date,
                SUM(CASE WHEN outcome LIKE '%TARGET%' OR outcome LIKE '%WIN%' OR outcome LIKE '%HIT_T%' THEN 1 ELSE 0 END) as w,
                SUM(CASE WHEN outcome LIKE '%SL%' OR outcome LIKE '%LOSS%' THEN 1 ELSE 0 END) as l
            FROM live_signal_history WHERE trade_date >= ?
            GROUP BY trade_date ORDER BY trade_date DESC
        """, (cutoff,)).fetchall()
        conn.close()

        streak = 0
        streak_type = "—"
        if day_rows:
            first = day_rows[0]
            if first[1] > first[2]:
                streak_type = "WIN"
                for dr in day_rows:
                    if dr[1] > dr[2]: streak += 1
                    else: break
            else:
                streak_type = "LOSS"
                for dr in day_rows:
                    if dr[2] >= dr[1]: streak += 1
                    else: break

        total_exec = (agg[2] or 0) + (agg[3] or 0)
        wr = round((agg[2] or 0) / total_exec * 100, 1) if total_exec > 0 else 0
        return JSONResponse({
            "trading_days": agg[0] or 0,
            "total_signals": agg[1] or 0,
            "total_wins": agg[2] or 0,
            "total_losses": agg[3] or 0,
            "win_rate": wr,
            "total_pts": round(agg[4] or 0, 2),
            "pnl_inr": round((agg[4] or 0) * 100, 0),
            "best_day_pts": round(agg[5] or 0, 2),
            "worst_day_pts": round(agg[6] or 0, 2),
            "streak": streak,
            "streak_type": streak_type,
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT,
                log_level="info", access_log=False, reload=False)


