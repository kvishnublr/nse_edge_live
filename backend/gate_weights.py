"""
STOCKR.IN v5 â€” Gate Weights & Confidence Scoring
Analyzes backtest + live signal history to determine which gates
are most predictive. Generates a confidence score (0â€“10) for live signals.
"""

import logging

logger = logging.getLogger("gate_weights")

GATE_NAMES = {1: "REGIME", 2: "SMART MONEY", 3: "STRUCTURE", 4: "TRIGGER", 5: "RISK VALID"}
DEFAULT_WEIGHTS = {1: 0.20, 2: 0.20, 3: 0.20, 4: 0.20, 5: 0.20}


# â”€â”€â”€ COMPUTE & SAVE WEIGHTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def compute_and_save_weights() -> dict:
    """
    Load signal_log with known outcomes, measure each gate's predictive lift,
    normalise to weights summing to 1.0, and persist to DB.
    """
    try:
        import backtest_data as bd
        bd.init_db()
        conn = bd.get_conn()

        rows = conn.execute("""
            SELECT g1, g2, g3, g4, g5, outcome
            FROM signal_log
            WHERE verdict = 'EXECUTE' AND outcome IS NOT NULL
        """).fetchall()
        conn.close()
    except Exception as e:
        logger.warning(f"Weight computation DB error: {e}")
        return DEFAULT_WEIGHTS

    if len(rows) < 10:
        logger.info(f"Insufficient data ({len(rows)} EXECUTE rows) â€” using equal weights")
        return DEFAULT_WEIGHTS

    total_wins    = sum(1 for r in rows if r[5] == "WIN")
    baseline_wr   = total_wins / len(rows) if rows else 0.5

    wins_go  = {i: 0 for i in range(1, 6)}
    total_go = {i: 0 for i in range(1, 6)}

    for row in rows:
        outcome = row[5]
        for gi in range(1, 6):
            if row[gi - 1] == "go":
                total_go[gi] += 1
                if outcome == "WIN":
                    wins_go[gi] += 1

    # Predictive lift = win_rate_when_go / baseline
    lifts = {}
    for gi in range(1, 6):
        if total_go[gi] > 0:
            wr_go = wins_go[gi] / total_go[gi]
            lifts[gi] = max(0.01, wr_go / baseline_wr if baseline_wr > 0 else 1.0)
        else:
            lifts[gi] = 1.0   # no data â†’ neutral

    total_lift = sum(lifts.values())
    weights    = {gi: round(l / total_lift, 4) for gi, l in lifts.items()}

    # Persist
    try:
        import backtest_data as bd
        conn = bd.get_conn()
        for gi in range(1, 6):
            wr = round(wins_go[gi] / total_go[gi] * 100, 1) if total_go[gi] else 0.0
            conn.execute(
                "INSERT OR REPLACE INTO gate_weights (gate, name, weight, win_rate, sample_size) "
                "VALUES (?,?,?,?,?)",
                (gi, GATE_NAMES[gi], weights[gi], wr, total_go[gi])
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"Gate weights save error: {e}")

    logger.info(f"Gate weights: {weights}  (baseline WR {baseline_wr:.1%}, n={len(rows)})")
    return weights


# â”€â”€â”€ LOAD WEIGHTS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def get_weights() -> dict:
    """Load from DB; fall back to equal weights."""
    try:
        import backtest_data as bd
        conn = bd.get_conn()
        rows = conn.execute("SELECT gate, weight FROM gate_weights").fetchall()
        conn.close()
        if len(rows) == 5:
            return {r[0]: r[1] for r in rows}
    except Exception:
        pass
    return DEFAULT_WEIGHTS


# â”€â”€â”€ LIVE CONFIDENCE SCORE â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def compute_confidence(gates: dict) -> float:
    """
    Confidence score 0â€“10 for a live signal.
    = weighted sum of (gate_score/100 Ã— state_multiplier) Ã— 10
    """
    weights = get_weights()
    state_mult = {"go": 1.0, "am": 0.6, "wt": 0.4, "st": 0.0}
    total = 0.0
    for gi in range(1, 6):
        g    = gates.get(gi, {})
        sc   = g.get("score", 50) / 100
        mult = state_mult.get(g.get("state", "wt"), 0.4)
        total += weights.get(gi, 0.2) * sc * mult
    return round(total * 10, 1)


# â”€â”€â”€ FULL GATE ANALYSIS â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
def get_gate_analysis() -> dict:
    """Return full per-gate analysis for frontend display."""
    try:
        import backtest_data as bd
        conn = bd.get_conn()
        rows = conn.execute(
            "SELECT gate, name, weight, win_rate, sample_size FROM gate_weights ORDER BY gate"
        ).fetchall()
        totals = conn.execute(
            "SELECT COUNT(*), SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) "
            "FROM signal_log WHERE verdict='EXECUTE' AND outcome IS NOT NULL"
        ).fetchone()
        conn.close()

        gate_data = {
            r[0]: {"name": r[1], "weight": r[2], "win_rate": r[3], "sample": r[4]}
            for r in rows
        }
        overall_wr = round(totals[1] / totals[0] * 100, 1) if totals and totals[0] else 0
        return {
            "gates":            gate_data,
            "overall_win_rate": overall_wr,
            "total_signals":    totals[0] if totals else 0,
        }
    except Exception as e:
        return {"gates": {}, "overall_win_rate": 0, "error": str(e)}
