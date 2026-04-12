"""Dayview long trade path simulation (STOCK PICKS history / range report)."""


def _bar_iso(bar: dict) -> str:
    d = bar["date"]
    return d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10]


def simulate_dayview_positional_long(
    entry: float,
    target: float,
    stop: float,
    daily_bars: list,
    atr_v: float,
    max_hold_days: int,
) -> dict | None:
    """
    Long with fixed TP/SL from entry; walk up to max_hold_days daily sessions.

    Per session: gap at open (>= target → WIN at target; <= stop → LOSS at open),
    else high >= target before low <= stop (same TP-first ordering as single-day
    dayview), else on the last session in the window close vs entry with ATR band
    → WIN / LOSS / NEUTRAL (time stop).
    """
    if not daily_bars or entry is None:
        return None
    entry = round(float(entry), 2)
    target = round(float(target), 2)
    stop = round(float(stop), 2)
    thr = max(10.0, round(float(atr_v) * 0.3, 2))
    slice_bars = daily_bars[: max(1, int(max_hold_days))]
    n = len(slice_bars)
    if n == 0:
        return None

    for i, bar in enumerate(slice_bars):
        o = round(float(bar["open"]), 2)
        h = round(float(bar["high"]), 2)
        l = round(float(bar["low"]), 2)
        c = round(float(bar["close"]), 2)
        last = i == n - 1
        outcome = None
        exit_p = None

        if o >= target:
            exit_p, outcome = target, "WIN"
        elif o <= stop:
            exit_p, outcome = o, "LOSS"
        elif h >= target:
            exit_p, outcome = target, "WIN"
        elif l <= stop:
            exit_p, outcome = stop, "LOSS"
        elif last:
            diff = c - entry
            if diff >= thr:
                exit_p, outcome = c, "WIN"
            elif diff <= -thr:
                exit_p, outcome = c, "LOSS"
            else:
                exit_p, outcome = c, "NEUTRAL"

        if outcome:
            pnl = round(exit_p - entry, 2)
            first = slice_bars[0]
            return {
                "entry": entry,
                "target": target,
                "stop": stop,
                "exit": exit_p,
                "pnl": pnl,
                "outcome": outcome,
                "next_date": _bar_iso(first),
                "exit_date": _bar_iso(bar),
                "hold_days": i + 1,
            }
    return None
