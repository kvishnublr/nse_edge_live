# STOCKR.IN v5 â€” Live Trading System (Zerodha Kite Connect)

Real-time NSE trading terminal powered entirely by Zerodha Kite Connect.
KiteTicker WebSocket for live ticks. kite.quote for option chain OI.
NSE API only for FII/DII (Kite doesn't provide it).

---

## What you need

1. **Zerodha trading account** â€” zerodha.com
2. **Kite Connect developer subscription** â€” kite.trade (~â‚¹2000/month)
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
# Edit backend/.env â€” add KITE_API_KEY and KITE_API_SECRET
```

---

## Every morning (before 9:15 AM)

```bash
cd nse_edge_live/backend
python3 generate_token.py
```

This opens your browser, logs you into Kite, and saves today's access token to `.env` automatically.
Takes 30 seconds. Token expires at midnight â€” do this every trading day.

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
        â”‚
        â”œâ”€â”€ KiteTicker WebSocket â”€â”€â†’ feed.py â”€â”€â†’ price_cache
        â”‚   (real-time ticks)        (every tick, 15+ instruments)
        â”‚
        â”œâ”€â”€ kite.quote (NFO) â”€â”€â”€â”€â”€â†’ fetcher.py â”€â”€â†’ option chain
        â”‚   (every 30 seconds)       (PCR, OI, Max Pain, strikes)
        â”‚
        â”œâ”€â”€ kite.quote (NFO FUT) â”€â”€â†’ fetcher.py â”€â”€â†’ stock OI scanner
        â”‚   (every 30 seconds)       (OI, OI change, volume)
        â”‚
        â””â”€â”€ kite.instruments â”€â”€â”€â”€â”€â”€â†’ fetcher.py â”€â”€â†’ NFO instrument list
            (cached, once per day)    (for building option chain)

NSE Website (FII/DII only â€” Kite doesn't have it)
        â””â”€â”€ /api/fiidiiTradeReact â”€â”€â†’ fetcher.py â”€â”€â†’ FII/DII flow

signals.py â† reads all of the above â†’ 5-gate engine â†’ verdict
scheduler.py â† runs all jobs â†’ broadcasts via WebSocket
main.py â† FastAPI WebSocket hub â†’ frontend/index.html
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
| G4   | Volume surge   | 1.5Ã—    |
| G4   | OI build       | 5,000   |
| G5   | R:R intraday   | 1:2.0   |
| G5   | R:R positional | 1:3.0   |
| G5   | ATR multiplier | 1.5Ã—    |

---

## Troubleshooting

**"KITE_ACCESS_TOKEN missing"**
â†’ Run `python3 generate_token.py` and restart

**Token invalid / expired**
â†’ Access tokens expire at midnight. Run `generate_token.py` every morning.

**KiteTicker not connecting**
â†’ Check `curl http://localhost:8765/api/health` for `kite_ticker: false`
â†’ Verify your internet connection and Kite subscription status

**Option chain shows no data**
â†’ Ensure market hours (9:15â€“15:30 IST)
â†’ NFO instruments download on first run â€” takes ~10 seconds

**Port 8765 in use**
â†’ `./stop.sh` or `lsof -ti:8765 | xargs kill -9`

---

## Disclaimer

For educational purposes. Derivatives trading involves substantial risk.
Verify all signals independently before trading. Past performance does not
guarantee future results.
