# NSE EDGE v5 — Live Trading System (Zerodha Kite Connect)

Real-time NSE trading terminal powered entirely by Zerodha Kite Connect.
KiteTicker WebSocket for live ticks. kite.quote for option chain OI.
NSE API only for FII/DII (Kite doesn't provide it).

---

## What you need

1. **Zerodha trading account** — zerodha.com
2. **Kite Connect developer subscription** — kite.trade (~₹2000/month)
   - Gives you: real-time ticks, option chain OI, instrument data
3. **Python 3.9+** on your machine

---

## Setup (one-time)

```bash
# 1. Extract and enter the folder
unzip nse_edge_live.zip
cd nse_edge_live

# 2. Make scripts executable
chmod +x start.sh stop.sh

# 3. Install dependencies
pip install -r backend/requirements.txt

# 4. Add your API keys
cp backend/.env.example backend/.env
# Edit backend/.env — add KITE_API_KEY and KITE_API_SECRET
```

---

## Every morning (before 9:15 AM)

```bash
cd nse_edge_live/backend
python3 generate_token.py
```

This opens your browser, logs you into Kite, and saves today's access token to `.env` automatically.
Takes 30 seconds. Token expires at midnight — do this every trading day.

Then start the system:

```bash
cd ..
./start.sh
```

Browser opens with the terminal. All data live from Kite.

---

## Data flow

```
Zerodha Kite Connect
        │
        ├── KiteTicker WebSocket ──→ feed.py ──→ price_cache
        │   (real-time ticks)        (every tick, 15+ instruments)
        │
        ├── kite.quote (NFO) ─────→ fetcher.py ──→ option chain
        │   (every 30 seconds)       (PCR, OI, Max Pain, strikes)
        │
        ├── kite.quote (NFO FUT) ──→ fetcher.py ──→ stock OI scanner
        │   (every 30 seconds)       (OI, OI change, volume)
        │
        └── kite.instruments ──────→ fetcher.py ──→ NFO instrument list
            (cached, once per day)    (for building option chain)

NSE Website (FII/DII only — Kite doesn't have it)
        └── /api/fiidiiTradeReact ──→ fetcher.py ──→ FII/DII flow

signals.py ← reads all of the above → 5-gate engine → verdict
scheduler.py ← runs all jobs → broadcasts via WebSocket
main.py ← FastAPI WebSocket hub → frontend/index.html
```

---

## Update intervals

| Data             | Interval | Source       |
|-----------------|---------|--------------|
| Prices (all)    | Real-time | KiteTicker  |
| Gates + Verdict | 30s     | signals.py   |
| Option chain    | 30s     | kite.quote NFO |
| Stock OI        | 30s     | kite.quote NFO futures |
| VIX             | Real-time | KiteTicker  |
| FII / DII       | 5 min   | NSE website  |
| Spike detection | 10s     | signals.py   |

---

## API endpoints

```
GET /api/health          System status + KiteTicker connection state
GET /api/state           Full system state as JSON
GET /api/chain/NIFTY     Live Nifty option chain
GET /api/chain/BANKNIFTY Live BankNifty option chain
GET /api/indices         Live index prices + VIX
GET /api/fii             FII/DII cash flow
```

---

## Gate thresholds (edit backend/config.py)

| Gate | Parameter        | Default |
|------|----------------|---------|
| G1   | VIX low        | 12.0    |
| G1   | VIX medium     | 16.0    |
| G2   | PCR bullish    | 1.20    |
| G2   | PCR bearish    | 0.80    |
| G4   | Volume surge   | 1.5×    |
| G4   | OI build       | 5,000   |
| G5   | R:R intraday   | 1:2.0   |
| G5   | R:R positional | 1:3.0   |
| G5   | ATR multiplier | 1.5×    |

---

## Troubleshooting

**"KITE_ACCESS_TOKEN missing"**
→ Run `python3 generate_token.py` and restart

**Token invalid / expired**
→ Access tokens expire at midnight. Run `generate_token.py` every morning.

**KiteTicker not connecting**
→ Check `curl http://localhost:8765/api/health` for `kite_ticker: false`
→ Verify your internet connection and Kite subscription status

**Option chain shows no data**
→ Ensure market hours (9:15–15:30 IST)
→ NFO instruments download on first run — takes ~10 seconds

**Port 8765 in use**
→ `./stop.sh` or `lsof -ti:8765 | xargs kill -9`

---

## Disclaimer

For educational purposes. Derivatives trading involves substantial risk.
Verify all signals independently before trading. Past performance does not
guarantee future results.
