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
from config import HOST, PORT, KITE_API_KEY, KITE_ACCESS_TOKEN, is_market_open, get_market_status

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
# Only allow specific origins to prevent CSRF attacks
ALLOWED_ORIGINS = [
    "http://localhost:8080",
    "http://127.0.0.1:8080",
    "localhost:8080",
    "127.0.0.1:8080",
    "http://localhost:8000",
    "http://127.0.0.1:8000",
    "null",   # file:// protocol sends Origin: null
]
# Note: file:// protocol cannot be in CORS allow_origins, so local file:// loads are unrestricted by browser

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,  # Disabled for better security
    allow_methods=["GET", "POST"],  # Only GET and POST
    allow_headers=["Content-Type", "Authorization"],
)


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
