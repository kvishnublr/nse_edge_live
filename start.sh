#!/bin/bash
# NSE EDGE v5 — System Launcher
# Usage: ./start.sh
# Starts the Python backend. Open frontend/index.html in your browser.

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/backend"
FRONTEND_DIR="$SCRIPT_DIR/frontend"
VENV_DIR="$SCRIPT_DIR/venv"
PID_FILE="$SCRIPT_DIR/.backend.pid"
LOG_FILE="$SCRIPT_DIR/nse_edge.log"

# ─── Colors ────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'

echo ""
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}   NSE EDGE v5 — Intelligent Trading System     ${RESET}"
echo -e "${BOLD}${GREEN}═══════════════════════════════════════════════${RESET}"
echo ""

# ─── Check Python ─────────────────────────────────────────────────────────
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}ERROR: Python 3 not found. Install Python 3.9+${RESET}"
    exit 1
fi

PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo -e "Python ${GREEN}${PY_VER}${RESET} found"

# ─── Create virtualenv if not exists ──────────────────────────────────────
if [ ! -d "$VENV_DIR" ]; then
    echo -e "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

# ─── Install dependencies ─────────────────────────────────────────────────
echo -e "Checking dependencies..."
pip install -q -r "$BACKEND_DIR/requirements.txt" 2>&1 | grep -v "^Requirement already"
echo -e "${GREEN}Dependencies OK${RESET}"

# ─── .env setup ───────────────────────────────────────────────────────────
if [ ! -f "$BACKEND_DIR/.env" ]; then
    cp "$BACKEND_DIR/.env.example" "$BACKEND_DIR/.env"
    echo -e "${YELLOW}Created .env from template — edit backend/.env to add API keys${RESET}"
    echo -e "${YELLOW}Add Kite keys + token in backend/.env — live data is Zerodha Kite only${RESET}"
fi

# ─── Stop existing instance ───────────────────────────────────────────────
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo -e "Stopping previous instance (PID $OLD_PID)..."
        kill "$OLD_PID" 2>/dev/null || true
        sleep 1
    fi
    rm -f "$PID_FILE"
fi

# ─── Start backend ────────────────────────────────────────────────────────
echo -e "Starting backend..."
cd "$BACKEND_DIR"
nohup python3 main.py > "$LOG_FILE" 2>&1 &
BACKEND_PID=$!
echo $BACKEND_PID > "$PID_FILE"

# Wait for backend to start
echo -n "Waiting for backend"
for i in $(seq 1 15); do
    sleep 1
    echo -n "."
    if curl -s "http://localhost:8765/api/health" > /dev/null 2>&1; then
        echo ""
        break
    fi
done
echo ""

# Check it's actually running
if ! kill -0 "$BACKEND_PID" 2>/dev/null; then
    echo -e "${RED}Backend failed to start. Check logs:${RESET}"
    tail -20 "$LOG_FILE"
    exit 1
fi

# ─── Open frontend ────────────────────────────────────────────────────────
FRONTEND_PATH="$FRONTEND_DIR/index.html"

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════${RESET}"
echo -e "${GREEN}  ✓ Backend running (PID $BACKEND_PID)${RESET}"
echo -e "${GREEN}  ✓ WebSocket: ws://localhost:8765/ws${RESET}"
echo -e "${GREEN}  ✓ API:       http://localhost:8765/api/health${RESET}"
echo -e "${GREEN}  ✓ Log:       $LOG_FILE${RESET}"
echo ""
echo -e "${BOLD}  Open this file in your browser:${RESET}"
echo -e "${BLUE}  $FRONTEND_PATH${RESET}"
echo -e "${GREEN}═══════════════════════════════════════════════${RESET}"
echo ""

# Auto-open browser on macOS / Linux
if command -v open &> /dev/null; then
    open "$FRONTEND_PATH"
elif command -v xdg-open &> /dev/null; then
    xdg-open "$FRONTEND_PATH" &
fi

# ─── Tail logs ────────────────────────────────────────────────────────────
echo -e "Tailing logs (Ctrl+C to stop log view — backend keeps running)..."
echo ""
tail -f "$LOG_FILE"
