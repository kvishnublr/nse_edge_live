"""Index radar backtest WR: default vs high_accuracy. Run from backend/ with venv.

  python check_ix_7d_accuracy.py          # 7 trading days
  python check_ix_7d_accuracy.py 60       # 60 trading days
"""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta


def _last_weekday(d: date) -> date:
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def _n_trading_days_before(end: date, n: int) -> date:
    d = end
    c = 0
    while c < n:
        d -= timedelta(days=1)
        if d.weekday() < 5:
            c += 1
    return d


def _run_backtest_chunk(client, preset: str, d0: date, d1: date) -> dict:
    r = client.post(
        "/api/index-signals/backtest",
        json={"from_date": d0.isoformat(), "to_date": d1.isoformat(), "preset": preset},
    )
    try:
        d = r.json()
    except Exception:
        raise RuntimeError(r.text[:800]) from None
    if r.status_code >= 400 or d.get("error"):
        raise RuntimeError(d.get("error", r.text))
    return d


def _aggregate_preset(client, preset: str, start: date, end: date, chunk_cal_days: int) -> dict:
    """Kite 1-min history is ~60 calendar days; long spans must be chunked."""
    total_sig = wins = losses = inserted = 0
    chunks = 0
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_cal_days - 1), end)
        d = _run_backtest_chunk(client, preset, cur, chunk_end)
        total_sig += int(d.get("total") or 0)
        wins += int(d.get("wins") or 0)
        losses += int(d.get("losses") or 0)
        inserted += int(d.get("inserted") or 0)
        chunks += 1
        cur = chunk_end + timedelta(days=1)
    resolved = wins + losses
    wr = round(wins / resolved * 100) if resolved else 0
    return {
        "preset": "high_accuracy" if preset == "high_accuracy" else "default",
        "from": start.isoformat(),
        "to": end.isoformat(),
        "total_signals": total_sig,
        "wins_t1": wins,
        "losses_sl": losses,
        "win_rate_pct": wr,
        "inserted": inserted,
        "api_chunks": chunks,
    }


def main() -> int:
    n_td = 7
    if len(sys.argv) > 1:
        try:
            n_td = max(1, min(252, int(sys.argv[1])))
        except ValueError:
            print("Usage: python check_ix_7d_accuracy.py [trading_days]", file=sys.stderr)
            return 1
    end = _last_weekday(date.today())
    start = _n_trading_days_before(end, n_td - 1)
    # Stay under Kite minute-data window (~60d) per request
    chunk_cal = 55

    try:
        from fastapi.testclient import TestClient
    except ImportError:
        print("Install fastapi[all] or starlette for TestClient", file=sys.stderr)
        return 1

    import main as app_main

    client = TestClient(app_main.app)
    out = []
    try:
        for preset in ("default", "high_accuracy"):
            out.append(_aggregate_preset(client, preset, start, end, chunk_cal))
    except RuntimeError as e:
        print(json.dumps({"error": str(e)}, indent=2), file=sys.stderr)
        return 1

    print(
        json.dumps(
            {
                "window_trading_days": n_td,
                "calendar_span": f"{start} .. {end}",
                "chunk_calendar_days_per_request": chunk_cal,
                "note": "Win rate = T1 / (T1+SL) only; EXPIRED excluded. Totals summed across API chunks.",
                "results": out,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
