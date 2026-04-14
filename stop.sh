#!/bin/bash
# STOCKR.IN v5 â€” Stop backend
PID_FILE="$(dirname "$0")/.backend.pid"
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        echo "Stopping STOCKR.IN backend (PID $PID)..."
        kill "$PID"
        rm -f "$PID_FILE"
        echo "Stopped."
    else
        echo "Backend not running (stale PID $PID)"
        rm -f "$PID_FILE"
    fi
else
    echo "No PID file found â€” backend may not be running"
    pkill -f "python3 main.py" 2>/dev/null && echo "Killed by process name" || echo "Nothing to kill"
fi
