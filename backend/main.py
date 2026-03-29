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
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
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

    # Validate config
    if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
        logger.error("KITE_API_KEY and KITE_ACCESS_TOKEN missing in .env")
        logger.error("Run: python3 generate_token.py to get today's token")
        raise SystemExit(1)

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
    return FileResponse(os.path.abspath(_FRONTEND))

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
        "spikes":      signals.state.get("spikes", []),
        "position_size_lots": signals.state.get("position_size_lots", 0),
        "position_size_rupees": signals.state.get("position_size_rupees", 0),
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
            f"SELECT date, verdict, pass_count, nifty, vix, pcr, "
            f"outcome_pts, outcome, g1, g2, g3, g4, g5 "
            f"FROM signal_log {where} ORDER BY date DESC LIMIT 200",
            params
        ).fetchall()
        conn.close()

        trades = [
            {"date": r[0], "verdict": r[1], "pass_count": r[2], "nifty": r[3],
             "vix": r[4], "pcr": r[5], "outcome_pts": r[6], "outcome": r[7],
             "g1": r[8], "g2": r[9], "g3": r[10], "g4": r[11], "g5": r[12]}
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

                if sym in ["BANKNIFTY", "ICICIBANK", "RELIANCE"]:
                    prices[sym] = {"price": dc, "chg_pts": round(dc-pv,2), "chg_pct": dp}

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
                })

                # Build pick entry for right panel (only strong signals)
                score = round(40 + pc_s*10 + (10 if dp>1 else 5 if dp>0.5 else 0) + (5 if vol_r>=1.5 else 0))
                score = min(99, score)
                if pc_s >= 3:
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
                        "verdict":     "EXECUTE" if pc_s==5 else "WATCH" if pc_s==4 else "WAIT",
                        "conf":        "CONFIRMED" if pc_s==5 else "HIGH CONF" if pc_s==4 else "50:50",
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

        return JSONResponse({
            "date": date,
            "prices": prices,
            "gates": {"gates": gates_data, "verdict": verdict, "verdict_sub": verdict_sub,
                      "pass_count": pass_cnt, "confidence": confidence},
            "macro":  {"vix": vix, "vix_chg": vix_chg, "fii_net": fii_net},
            "fii":    {"fii_net": fii_net, "dii_net": dii_net},
            "chain":  chain_msg,
            "stocks": stocks_msg,
            "spikes": [],
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
async def spikes_backtest(from_date: str = None, to_date: str = None):
    """
    Backtest Spike Radar strategy using 1-min OHLCV candles from Kite.
    Spike = candle where vol > 2.5x rolling avg AND abs price move >= 0.5%
    Entry at spike candle close, check next 30 candles for T1/T2/SL hit.
    T1=+0.3%, T2=+0.6%, SL=-0.25% from entry.
    """
    import asyncio
    from datetime import datetime, timedelta
    from feed import get_kite
    from config import KITE_TOKENS, FNO_SYMBOLS

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

        def _run():
            results = []
            summary = {"total": 0, "hit_t1": 0, "hit_t2": 0, "hit_sl": 0, "expired": 0}

            symbols = [s for s in FNO_SYMBOLS if s in KITE_TOKENS and s not in ("INDIAVIX",)]

            for sym in symbols:
                token = KITE_TOKENS[sym]
                try:
                    candles = kite.historical_data(token, fd, td, "minute")
                except Exception as e:
                    logger.warning(f"Spike BT: {sym} historical_data failed: {e}")
                    continue

                if len(candles) < 25:
                    continue

                # rolling 20-candle avg volume
                for i in range(20, len(candles)):
                    c = candles[i]
                    vol  = c.get("volume", 0)
                    close = c.get("close", 0)
                    open_ = c.get("open",  0)
                    if not vol or not close or not open_:
                        continue

                    avg_vol = sum(candles[j]["volume"] for j in range(i-20, i)) / 20
                    if avg_vol == 0:
                        continue

                    vol_mult  = vol / avg_vol
                    chg_pct   = (close - open_) / open_ * 100

                    # Spike condition
                    if vol_mult < 2.5 or abs(chg_pct) < 0.5:
                        continue

                    is_buy   = chg_pct > 0
                    entry    = round(close * (1.0005 if is_buy else 0.9995), 2)
                    sl       = round(entry * (0.9975 if is_buy else 1.0025), 2)
                    t1       = round(entry * (1.003  if is_buy else 0.997),  2)
                    t2       = round(entry * (1.006  if is_buy else 0.994),  2)

                    result   = "EXPIRED"
                    exit_p   = None
                    exit_i   = None

                    # check next 30 candles
                    for k in range(i+1, min(i+31, len(candles))):
                        hi = candles[k]["high"]
                        lo = candles[k]["low"]
                        if is_buy:
                            if lo <= sl:
                                result = "HIT_SL"; exit_p = sl; exit_i = k; break
                            if hi >= t2:
                                result = "HIT_T2"; exit_p = t2; exit_i = k; break
                            if hi >= t1:
                                result = "HIT_T1"; exit_p = t1; exit_i = k; break
                        else:
                            if hi >= sl:
                                result = "HIT_SL"; exit_p = sl; exit_i = k; break
                            if lo <= t2:
                                result = "HIT_T2"; exit_p = t2; exit_i = k; break
                            if lo <= t1:
                                result = "HIT_T1"; exit_p = t1; exit_i = k; break

                    pnl_pct = 0.0
                    if exit_p:
                        pnl_pct = round((exit_p - entry) / entry * 100 * (1 if is_buy else -1), 3)

                    candle_time = c["date"]
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
                    })

                    summary["total"] += 1
                    if result == "HIT_T1":  summary["hit_t1"] += 1
                    elif result == "HIT_T2": summary["hit_t2"] += 1
                    elif result == "HIT_SL": summary["hit_sl"] += 1
                    else:                    summary["expired"] += 1

            # sort by time desc
            results.sort(key=lambda x: x["time"], reverse=True)

            hits    = summary["hit_t1"] + summary["hit_t2"]
            win_rate = round(hits / summary["total"] * 100, 1) if summary["total"] else 0
            avg_pnl  = round(sum(r["pnl_pct"] for r in results) / len(results), 3) if results else 0

            return {
                "summary":  {**summary, "win_rate": win_rate, "avg_pnl": avg_pnl},
                "results":  results[:500],   # cap at 500 rows
                "from_date": from_date,
                "to_date":   to_date,
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


if __name__ == "__main__":
    uvicorn.run("main:app", host=HOST, port=PORT,
                log_level="info", access_log=False, reload=False)
