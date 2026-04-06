"""
Index Radar — ML precision filter (GradientBoosting on resolved T1 vs SL).

Trains on index_signal_history rows with outcome HIT_T1 / HIT_SL.
At runtime, only emit live/backtest signals when P(win) >= ml_min_win_prob (from config).

80% win rate is not guaranteed out-of-sample; the trainer picks a probability cutoff
that hits ~80% precision on a time-split validation set when data allows.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

logger = logging.getLogger("index_radar_ml")

_INSTALL_HINT = (
    "Install:  pip install scikit-learn joblib  "
    "(from project folder: .\\venv\\Scripts\\pip install scikit-learn joblib)"
)


def _import_sklearn_stack():
    """Lazy import so FastAPI starts even if ML deps are not installed yet."""
    try:
        import joblib
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.metrics import roc_auc_score
        from sklearn.model_selection import train_test_split

        return joblib, GradientBoostingClassifier, roc_auc_score, train_test_split
    except ImportError as e:
        raise ImportError(f"{_INSTALL_HINT}  ({e})") from e


def _import_joblib():
    try:
        import joblib

        return joblib
    except ImportError as e:
        raise ImportError(f"{_INSTALL_HINT}  ({e})") from e

_DB_PATH = os.path.join(os.path.dirname(__file__), "data", "backtest.db")
_DEFAULT_MODEL = os.path.join(os.path.dirname(__file__), "data", "ix_radar_gb.joblib")
_META_PATH = os.path.join(os.path.dirname(__file__), "data", "ix_radar_ml_meta.json")


def _ensure_ix_columns() -> None:
    try:
        conn = sqlite3.connect(_DB_PATH)
        cur = conn.execute("PRAGMA table_info(index_signal_history)")
        have = {r[1] for r in cur.fetchall()}
        if "quality" not in have:
            conn.execute("ALTER TABLE index_signal_history ADD COLUMN quality REAL")
        if "pcr" not in have:
            conn.execute("ALTER TABLE index_signal_history ADD COLUMN pcr REAL")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("ix columns: %s", e)

_bundle: Optional[Dict[str, Any]] = None


def _parse_hour(sig_time: str) -> float:
    try:
        h, m = str(sig_time).split(":")
        return (int(h) + int(m) / 60.0) / 24.0
    except Exception:
        return 0.5


def row_to_features(row: Tuple[Any, ...], colnames: List[str]) -> Optional[np.ndarray]:
    """Turn sqlite row + column names into feature vector."""
    d = dict(zip(colnames, row))
    outcome = d.get("outcome")
    if outcome not in ("HIT_T1", "HIT_SL"):
        return None
    chg = float(d.get("chg_pct") or 0)
    typ = (d.get("type") or "").upper()
    sym = (d.get("symbol") or "").upper()
    st = (d.get("strength") or "md").lower()
    is_nifty = 1.0 if sym == "NIFTY" else 0.0
    vix = float(d.get("vix") or 0)
    rr = float(d.get("rr") or 1.5)
    q = d.get("quality")
    if q is None:
        q = 55.0
    pcr = d.get("pcr")
    if pcr is None:
        pcr = 1.0
    x = np.array(
        [
            chg,
            abs(chg),
            1.0 if typ == "CE" else 0.0,
            is_nifty,
            1.0 if st == "hi" else 0.0,
            _parse_hour(d.get("signal_time") or "12:00"),
            min(vix, 40) / 40.0,
            min(rr, 6.0) / 6.0,
            float(q) / 100.0,
            min(float(pcr), 2.5) / 2.5,
        ],
        dtype=np.float32,
    )
    return x


FEATURE_LABELS = [
    "chg_pct",
    "abs_chg",
    "is_ce",
    "is_nifty",
    "strength_hi",
    "hour_norm",
    "vix_norm",
    "rr_norm",
    "quality_norm",
    "pcr_norm",
]


def load_training_matrix() -> Tuple[Optional[np.ndarray], Optional[np.ndarray], int]:
    """
    Load all resolved T1/SL rows as matrices.
    Returns (X, y, n_raw) — X,y are None only on schema/DB error; n_raw is row count built.
    """
    _ensure_ix_columns()
    conn = sqlite3.connect(_DB_PATH)
    cur = conn.execute("PRAGMA table_info(index_signal_history)")
    colnames = [r[1] for r in cur.fetchall()]
    need = {"outcome", "chg_pct", "type", "symbol", "signal_time", "strength", "vix", "rr"}
    if not need.issubset(set(colnames)):
        conn.close()
        logger.warning("index_signal_history missing columns for ML")
        return None, None, 0
    rows = conn.execute(
        """
        SELECT * FROM index_signal_history
        WHERE outcome IN ('HIT_T1','HIT_SL')
        ORDER BY trade_date, signal_time
        """
    ).fetchall()
    conn.close()
    X, y = [], []
    for r in rows:
        xv = row_to_features(r, colnames)
        if xv is None:
            continue
        X.append(xv)
        d = dict(zip(colnames, r))
        y.append(1 if d.get("outcome") == "HIT_T1" else 0)
    n_raw = len(X)
    if n_raw == 0:
        return None, None, 0
    return np.vstack(X), np.array(y, dtype=np.int32), n_raw


def _find_precision_threshold(
    proba: np.ndarray, y_true: np.ndarray, target_precision: float = 0.80, min_pos: int = 6
) -> Tuple[float, float]:
    """First (highest) proba cutoff from 0.97→ with precision ≥ target; else best precision."""
    for t in np.arange(0.97, 0.38, -0.015):
        sel = proba >= t
        n = int(sel.sum())
        if n < min_pos:
            continue
        prec = float((y_true[sel] == 1).sum() / n)
        if prec >= target_precision:
            return float(t), prec
    best_t, best_prec = 0.55, 0.0
    for t in np.arange(0.97, 0.38, -0.015):
        sel = proba >= t
        n = int(sel.sum())
        if n < min_pos:
            continue
        prec = float((y_true[sel] == 1).sum() / n)
        if prec > best_prec:
            best_t, best_prec = float(t), prec
    return best_t, best_prec


def train_and_save(
    target_precision: float = 0.80,
    model_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Train GB classifier; time-ordered split; save model + recommended probability threshold.
    """
    global _bundle
    try:
        joblib, GradientBoostingClassifier, roc_auc_score, train_test_split = _import_sklearn_stack()
    except ImportError as e:
        return {"ok": False, "error": str(e)}

    _ensure_ix_columns()
    path = model_path or _DEFAULT_MODEL
    X, y, n_raw = load_training_matrix()
    if X is None or y is None:
        return {
            "ok": False,
            "error": f"No usable training data (resolved rows ≈ {n_raw}). Run Index Radar BACKTEST to fill index_signal_history with HIT_T1/HIT_SL.",
        }

    min_train = 30
    if n_raw < min_train:
        return {
            "ok": False,
            "error": f"Need at least {min_train} resolved wins+losses in DB; you have {n_raw}. Run a longer backtest.",
        }

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.25, shuffle=False, stratify=None
    )
    if len(np.unique(y_train)) < 2:
        return {"ok": False, "error": "Training set needs both wins and losses"}

    clf = GradientBoostingClassifier(
        n_estimators=100,
        max_depth=3,
        learning_rate=0.08,
        min_samples_leaf=4,
        random_state=42,
    )
    clf.fit(X_train, y_train)

    proba_val = clf.predict_proba(X_val)[:, 1]
    try:
        auc = float(roc_auc_score(y_val, proba_val))
    except Exception:
        auc = 0.0

    thr, val_prec = _find_precision_threshold(proba_val, y_val, target_precision, min_pos=6)
    sel = proba_val >= thr
    val_prec_f = float((y_val[sel] == 1).sum() / max(int(sel.sum()), 1))
    val_rec = float((y_val[sel] == 1).sum() / max(int((y_val == 1).sum()), 1))

    _bundle = {
        "model": clf,
        "threshold": thr,
        "target_precision": target_precision,
        "val_precision_at_thr": val_prec_f,
        "val_recall_at_thr": val_rec,
        "feature_labels": FEATURE_LABELS,
    }
    joblib.dump(_bundle, path)

    meta = {
        "threshold": thr,
        "val_precision": val_prec_f,
        "val_recall": val_rec,
        "auc": auc,
        "n_train": int(len(y_train)),
        "n_val": int(len(y_val)),
        "target_precision": target_precision,
        "path": path,
    }
    with open(_META_PATH, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    logger.info(
        "Index Radar ML trained: auc=%.3f thr=%.3f val_prec=%.3f val_rec=%.3f",
        auc,
        thr,
        val_prec_f,
        val_rec,
    )
    return {"ok": True, **meta}


def load_bundle(force: bool = False) -> Optional[Dict[str, Any]]:
    global _bundle
    if _bundle is not None and not force:
        return _bundle
    path = _DEFAULT_MODEL
    if not os.path.isfile(path):
        return None
    try:
        jl = _import_joblib()
        _bundle = jl.load(path)
        return _bundle
    except Exception as e:
        logger.warning("Failed to load ML bundle: %s", e)
        return None


def win_probability(sig: Dict[str, Any]) -> Optional[float]:
    """P(HIT_T1) for a proposed signal dict (same keys as live)."""
    b = load_bundle()
    if not b or "model" not in b:
        return None
    clf = b["model"]
    chg = float(sig.get("chg_pct") or 0)
    typ = (sig.get("type") or "").upper()
    sym = (sig.get("symbol") or "").upper()
    st = (sig.get("strength") or "md").lower()
    vix = float(sig.get("vix") or 0)
    rr = float(sig.get("rr") or 1.5)
    q = float(sig.get("quality") or 55)
    pcr = float(sig.get("pcr") or 1.0)
    x = np.array(
        [
            [
                chg,
                abs(chg),
                1.0 if typ == "CE" else 0.0,
                1.0 if sym == "NIFTY" else 0.0,
                1.0 if st == "hi" else 0.0,
                _parse_hour(sig.get("time") or "12:00"),
                min(vix, 40) / 40.0,
                min(rr, 6.0) / 6.0,
                q / 100.0,
                min(pcr, 2.5) / 2.5,
            ]
        ],
        dtype=np.float32,
    )
    return float(clf.predict_proba(x)[0, 1])


def ml_threshold_from_meta() -> Optional[float]:
    if os.path.isfile(_META_PATH):
        try:
            with open(_META_PATH, encoding="utf-8") as f:
                return float(json.load(f).get("threshold"))
        except Exception:
            pass
    b = load_bundle()
    if b and "threshold" in b:
        return float(b["threshold"])
    return None


def effective_ml_threshold(config_fallback: float) -> float:
    mt = ml_threshold_from_meta()
    return float(mt) if mt is not None else float(config_fallback)


def should_take_ml(sig: Dict[str, Any], config_fallback: float) -> Tuple[bool, Optional[float]]:
    p = win_probability(sig)
    if p is None:
        return True, None
    thr = effective_ml_threshold(config_fallback)
    return p >= thr, p
