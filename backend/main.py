"""
NSE EDGE v5 — FastAPI Backend (Zerodha Kite Connect only)
Start: python3 main.py
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from typing import Set

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

import fetcher
import signals
import scheduler as sched
from feed import feed_manager, get_all_prices
from config import HOST, PORT, KITE_API_KEY, KITE_ACCESS_TOKEN

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-12s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("main")

# ─── WEBSOCKET HUB ────────────────────────────────────────────────────────────
connected_clients: Set[WebSocket] = set()
_bcast_queue: asyncio.Queue = None
_start_time = time.time()


def broadcast(payload: dict):
    if _bcast_queue:
        try:
            _bcast_queue.put_nowait(payload)
        except asyncio.QueueFull:
            pass


async def _bcast_loop():
    global _bcast_queue
    _bcast_queue = asyncio.Queue(maxsize=1000)
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
            except Exception:
                dead.add(ws)
        connected_clients.difference_update(dead)


# ─── LIFESPAN ─────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("=" * 55)
    logger.info("  NSE EDGE v5 — Zerodha Kite Connect")
    logger.info("=" * 55)

    # Validate config
    if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
        logger.error("KITE_API_KEY and KITE_ACCESS_TOKEN missing in .env")
        logger.error("Run: python3 generate_token.py to get today's token")
        raise SystemExit(1)

    # Start broadcast loop
    asyncio.create_task(_bcast_loop())

    # Start Kite feed (validates creds, starts KiteTicker)
    feed_manager.start()

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
        chain   = fetcher.fetch_option_chain(kite, "NIFTY")
        stocks  = fetcher.fetch_fno_stocks(kite)

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

        signals.run_signal_engine(indices, chain, fii, stocks or [], "intraday")
    except Exception as e:
        logger.error(f"Initial fetch error: {e}", exc_info=True)

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
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


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
                    sched.set_mode(msg.get("mode", "intraday"))
                elif msg.get("type") == "ping":
                    await websocket.send_text(json.dumps({"type": "pong"}))
            except asyncio.TimeoutError:
                await websocket.send_text(json.dumps({"type": "ping"}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        connected_clients.discard(websocket)
        logger.info(f"WS disconnected: {ip} (remaining: {len(connected_clients)})")


async def _send_initial(ws: WebSocket):
    """Burst full state to a freshly connected client."""
    try:
        await ws.send_text(json.dumps({"type": "gates", "data": {
            "gates":       {str(k): v for k, v in signals.state["gates"].items()},
            "verdict":     signals.state["verdict"],
            "verdict_sub": signals.state["verdict_sub"],
            "pass_count":  signals.state["pass_count"],
        }}))
        prices = get_all_prices()
        if prices:
            await ws.send_text(json.dumps({"type": "prices", "data": prices}))
        if signals.state.get("last_chain"):
            await ws.send_text(json.dumps({"type": "chain",  "data": signals.state["last_chain"]}))
        if signals.state.get("last_macro"):
            await ws.send_text(json.dumps({"type": "macro",  "data": signals.state["last_macro"]}))
        if signals.state.get("last_stocks"):
            await ws.send_text(json.dumps({"type": "stocks", "data": signals.state["last_stocks"]}))
        if signals.state.get("last_fii"):
            await ws.send_text(json.dumps({"type": "fii",    "data": signals.state["last_fii"]}))
        await ws.send_text(json.dumps({"type": "spikes", "data": signals.state.get("spikes", [])}))
        await ws.send_text(json.dumps({"type": "ticker", "data": signals.state.get("ticker", [])}))
        await ws.send_text(json.dumps({"type": "ready", "msg": "NSE EDGE v5 Live"}))
    except Exception as e:
        logger.error(f"Initial state send: {e}")


# ─── REST API ─────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    from feed import price_cache, _ticker_connected
    return JSONResponse({
        "status":         "ok",
        "verdict":        signals.state["verdict"],
        "pass_count":     signals.state["pass_count"],
        "kite_ticker":    _ticker_connected,
        "prices_cached":  len(price_cache),
        "ws_clients":     len(connected_clients),
        "uptime_sec":     round(time.time() - _start_time),
        "last_updated":   signals.state.get("last_updated", 0),
    })


@app.get("/api/state")
async def get_state():
    return JSONResponse({
        "gates":       {str(k): v for k, v in signals.state["gates"].items()},
        "verdict":     signals.state["verdict"],
        "verdict_sub": signals.state["verdict_sub"],
        "pass_count":  signals.state["pass_count"],
        "prices":      get_all_prices(),
        "chain":       signals.state.get("last_chain"),
        "macro":       signals.state.get("last_macro"),
        "stocks":      signals.state.get("last_stocks"),
        "fii":         signals.state.get("last_fii"),
        "spikes":      signals.state.get("spikes", []),
    })


@app.get("/api/chain/{symbol}")
async def get_chain(symbol: str = "NIFTY"):
    from feed import get_kite
    data = fetcher.fetch_option_chain(get_kite(), symbol.upper())
    return JSONResponse(data or {"error": "chain fetch failed"})


@app.get("/api/indices")
async def get_indices():
    return JSONResponse(fetcher.fetch_indices() or {"error": "no data"})


@app.get("/api/fii")
async def get_fii():
    return JSONResponse(fetcher.fetch_fii_dii() or {"error": "no data"})


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT,
                log_level="info", access_log=False, reload=False)
