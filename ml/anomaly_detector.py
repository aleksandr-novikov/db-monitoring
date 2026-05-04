"""Per-table anomaly detection using Isolation Forest.

Features per tick: row_count, null_rate, Δrow_count, Δnull_rate (4-dim).
Deltas make sudden spikes/drops stand out regardless of absolute scale.

Model lifecycle
---------------
- ``train(table)``     — fit IsolationForest + StandardScaler on 60-day history,
                         persist to ``models/<table>__anomaly.joblib``.
- ``score_table(table, window_days)``
                       — load persisted model, score every point in the window,
                         return [{ts, score, is_anomaly}].
- ``retrain_all()``    — train every monitored table; used by the nightly job.

Anomaly threshold: ``decision_function(x) < 0``  (built-in IsolationForest
convention). The raw score is stored as-is so the threshold stays stable
across queries of different widths — unlike min-max normalisation which
shifts with every request.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.metrics_storage import get_metrics


try:
    import numpy as np
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler
    _HAS_SKLEARN = True
except Exception:  # pragma: no cover
    np = None  # type: ignore
    IsolationForest = None  # type: ignore
    StandardScaler = None  # type: ignore
    _HAS_SKLEARN = False

try:
    import joblib as _joblib
    _HAS_JOBLIB = True
except Exception:  # pragma: no cover
    _joblib = None  # type: ignore
    _HAS_JOBLIB = False

logger = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
MIN_POINTS = 200
TRAIN_WINDOW_DAYS = 60


class InsufficientDataError(Exception):
    """Raised when there is not enough history to train the model."""


def _parse_ts(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _load_features(
    table: str, window_days: int
) -> tuple[list[datetime], "np.ndarray"]:
    """Load (timestamps, feature_matrix) for *table* over *window_days*.

    Features: [row_count, null_rate, Δrow_count, Δnull_rate].
    Only ticks where BOTH row_count AND null_rate are available are kept.
    The first tick is dropped after computing deltas, so n_rows = aligned - 1.
    Raises InsufficientDataError when the result has fewer than MIN_POINTS rows.
    """
    if not _HAS_SKLEARN:
        raise ImportError("scikit-learn is required for anomaly detection")

    window = timedelta(days=window_days)
    rc_rows = get_metrics(table, "row_count", window=window)
    nr_rows = get_metrics(table, "null_rate", window=window)

    rc_map = {r["ts"]: float(r["value"]) for r in rc_rows}
    nr_map = {r["ts"]: float(r["value"]) for r in nr_rows}

    # Inner join on timestamp — only ticks with both metrics present.
    common_ts = sorted(set(rc_map) & set(nr_map))
    if len(common_ts) < 2:
        raise InsufficientDataError(
            f"need at least 2 aligned ticks for {table}, got {len(common_ts)}"
        )

    rc_vals = [rc_map[ts] for ts in common_ts]
    nr_vals = [nr_map[ts] for ts in common_ts]

    # Compute deltas (drops first element).
    d_rc = [rc_vals[i] - rc_vals[i - 1] for i in range(1, len(rc_vals))]
    d_nr = [nr_vals[i] - nr_vals[i - 1] for i in range(1, len(nr_vals))]
    timestamps = [_parse_ts(ts) for ts in common_ts[1:]]

    X = np.array(
        [[rc_vals[i + 1], nr_vals[i + 1], d_rc[i], d_nr[i]] for i in range(len(d_rc))],
        dtype=float,
    )
    return timestamps, X


def _model_path(table: str) -> Path:
    safe = table.replace("/", "_").replace(" ", "_")
    return MODELS_DIR / f"{safe}__anomaly.joblib"


def train(table: str) -> dict[str, Any]:
    """Fit and persist an anomaly-detection model for *table*.

    Returns metadata dict: {n_points, trained_at}.
    Raises InsufficientDataError when history is too short.
    """
    timestamps, X = _load_features(table, window_days=TRAIN_WINDOW_DAYS)
    if len(timestamps) < MIN_POINTS:
        raise InsufficientDataError(
            f"need at least {MIN_POINTS} points for {table}, got {len(timestamps)}"
        )

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # contamination=0.01 sets the decision threshold so that ≈1% of training
    # data is flagged — conservative enough to keep FPR well below 5% on
    # normal data, while still surfacing real anomalies.
    model = IsolationForest(
        n_estimators=100,
        contamination=0.01,
        random_state=42,
    )
    model.fit(X_scaled)

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "model": model,
        "scaler": scaler,
        "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_points": len(timestamps),
    }
    if _HAS_JOBLIB:
        try:
            _joblib.dump(payload, _model_path(table))
        except Exception as exc:
            logger.warning("Failed to persist anomaly model for %s: %s", table, exc)

    return {"n_points": len(timestamps), "trained_at": payload["trained_at"]}


def _load_model(table: str) -> dict | None:
    if not _HAS_JOBLIB:
        return None
    path = _model_path(table)
    if not path.exists():
        return None
    try:
        return _joblib.load(path)
    except Exception as exc:
        logger.warning("Failed to load anomaly model %s: %s", path, exc)
        return None


def score_table(
    table: str,
    window_days: int = 14,
) -> list[dict]:
    """Score every tick in *window_days* using the persisted model.

    Returns [{ts (ISO str), score (float), is_anomaly (int 0|1)}].
    score == decision_function value; negative means anomaly.

    If no persisted model exists, attempts an on-demand train. Raises
    InsufficientDataError when there is not enough data for training.
    """
    persisted = _load_model(table)
    if persisted is None:
        train(table)
        persisted = _load_model(table)
    if persisted is None:
        raise InsufficientDataError(f"could not load or train model for {table}")

    timestamps, X = _load_features(table, window_days=window_days)
    if len(timestamps) == 0:
        return []

    scaler: StandardScaler = persisted["scaler"]
    model: IsolationForest = persisted["model"]

    X_scaled = scaler.transform(X)
    raw_scores = model.decision_function(X_scaled)
    predictions = model.predict(X_scaled)

    return [
        {
            "ts": ts.isoformat(timespec="seconds"),
            "score": round(float(raw_scores[i]), 6),
            "is_anomaly": int(predictions[i] == -1),
        }
        for i, ts in enumerate(timestamps)
    ]


def retrain_all() -> dict[str, int]:
    """Retrain anomaly models for every monitored table. Used by the nightly job."""
    from app.db import list_tables

    counts: dict[str, int] = {"trained": 0, "skipped": 0, "errors": 0}
    for t in list_tables():
        name = t["table_name"]
        try:
            train(name)
            counts["trained"] += 1
        except InsufficientDataError:
            counts["skipped"] += 1
        except Exception as exc:
            logger.exception("Anomaly retrain failed for %s: %s", name, exc)
            counts["errors"] += 1
    logger.info("Anomaly detector retrain complete: %s", counts)
    return counts
