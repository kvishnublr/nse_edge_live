"""
NSE EDGE v5 — Cloud Pusher
Run this on your local PC alongside main.py.
It reads live state from localhost:8765 and pushes it to stokr.in every 30s.

Usage:
    python push_to_cloud.py
"""

import time
import requests
import os

LOCAL  = "http://localhost:8765"
CLOUD  = "https://stokr.in"
SECRET = os.getenv("PUSH_SECRET", "nse-edge-push-2026")
INTERVAL = 30  # seconds

headers = {"X-Push-Secret": SECRET, "Content-Type": "application/json"}

print("=" * 55)
print("  NSE EDGE — Cloud Pusher")
print(f"  Local  : {LOCAL}")
print(f"  Cloud  : {CLOUD}")
print(f"  Interval: every {INTERVAL}s")
print("=" * 55)

def fetch_local(path):
    try:
        r = requests.get(f"{LOCAL}{path}", timeout=5)
        return r.json() if r.ok else None
    except Exception:
        return None

def push():
    state  = fetch_local("/api/state")
    if not state:
        print("  [WARN] Local server not responding")
        return

    payload = {}
    if state.get("prices"):  payload["prices"] = state["prices"]
    # Wrap gates into the format push endpoint expects
    payload["gates"] = {
        "gates":       state.get("gates", {}),
        "verdict":     state.get("verdict", "WAIT"),
        "verdict_sub": state.get("verdict_sub", ""),
        "pass_count":  state.get("pass_count", 0),
    }
    if state.get("spikes"):  payload["spikes"] = state["spikes"]
    if state.get("stocks"):  payload["stocks"] = state["stocks"]
    if state.get("chain"):   payload["chain"]  = state["chain"]
    if state.get("macro"):   payload["macro"]  = state["macro"]
    if state.get("fii"):     payload["fii"]    = state["fii"]

    try:
        r = requests.post(f"{CLOUD}/api/push-state", json=payload, headers=headers, timeout=10)
        if r.ok:
            print(f"  [{time.strftime('%H:%M:%S')}] Pushed OK — verdict={state.get('gates',{}).get('verdict','?')}")
        else:
            print(f"  [{time.strftime('%H:%M:%S')}] Push failed: {r.status_code} {r.text[:80]}")
    except Exception as e:
        print(f"  [{time.strftime('%H:%M:%S')}] Push error: {e}")

while True:
    push()
    time.sleep(INTERVAL)
