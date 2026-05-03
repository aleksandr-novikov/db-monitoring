"""Per-table time-series forecasting for monitored metrics.

Uses Prophet when installed and the history is long enough (>= 7 days).
Falls back to ordinary least-squares linear regression for short series so
new tables still get a forecast as soon as a few points are collected.
Raises InsufficientDataError when fewer than 2 points are available.

Trained models are persisted via joblib under ``models/`` so the nightly
retrain job can refresh them without blocking request-time predictions.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from app.metrics_storage import get_metrics

try:  # pragma: no cover - optional heavy dep
    from prophet import Prophet  # type: ignore
    _HAS_PROPHET = True
except Exception:  # pragma: no cover
    Prophet = None  # type: ignore
    _HAS_PROPHET = False

try:  # pragma: no cover - optional dep
    import joblib  # type: ignore
    _HAS_JOBLIB = True
except Exception:  # pragma: no cover
    joblib = None  # type: ignore
    _HAS_JOBLIB = False

logger = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).resolve().parent.parent / "models"
MIN_PROPHET_DAYS = 7
MIN_POINTS = 2


class InsufficientDataError(Exception):
    """Raised when not enough history is available to fit any model."""


@dataclass
class LinearModel:
    """Trivial OLS fallback. Stores slope/intercept on epoch seconds and
    a Gaussian residual std for symmetric prediction intervals."""

    slope: float
    intercept: float
    sigma: float
    t0: float

    def predict(self, ts: datetime) -> tuple[float, float, float]:
        x = ts.timestamp() - self.t0
        yhat = self.slope * x + self.intercept
        # 95% interval ≈ 1.96σ
        margin = 1.96 * self.sigma
        return yhat, yhat - margin, yhat + margin


def _parse_ts(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    # SQLAlchemy may hand back tz-naive ISO strings depending on the driver.
    s = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _load_history(table: str, metric: str, days: int = 60) -> list[tuple[datetime, float]]:
    rows = get_metrics(table, metric, window=timedelta(days=days))
    return [(_parse_ts(r["ts"]), float(r["value"])) for r in rows]


def _fit_linear(points: list[tuple[datetime, float]]) -> LinearModel:
    t0 = points[0][0].timestamp()
    xs = [p[0].timestamp() - t0 for p in points]
    ys = [p[1] for p in points]
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    den = sum((x - mx) ** 2 for x in xs)
    slope = num / den if den else 0.0
    intercept = my - slope * mx
    residuals = [y - (slope * x + intercept) for x, y in zip(xs, ys)]
    var = sum(r * r for r in residuals) / max(n - 2, 1)
    sigma = math.sqrt(var) if var > 0 else 0.0
    return LinearModel(slope=slope, intercept=intercept, sigma=sigma, t0=t0)


def _fit_prophet(points: list[tuple[datetime, float]]):  # pragma: no cover - heavy
    import pandas as pd  # type: ignore
    df = pd.DataFrame({"ds": [p[0].replace(tzinfo=None) for p in points],
                       "y": [p[1] for p in points]})
    m = Prophet(interval_width=0.95, daily_seasonality=False, weekly_seasonality=True)
    m.fit(df)
    return m


def _predict_prophet(model, horizon_days: int) -> list[dict]:  # pragma: no cover
    import pandas as pd  # type: ignore
    future = model.make_future_dataframe(periods=horizon_days * 24, freq="H",
                                          include_history=False)
    fc = model.predict(future)
    out = []
    for _, row in fc.iterrows():
        ds = row["ds"]
        if hasattr(ds, "to_pydatetime"):
            ds = ds.to_pydatetime()
        out.append({
            "ts": ds.replace(tzinfo=timezone.utc).isoformat(timespec="seconds"),
            "yhat": float(row["yhat"]),
            "yhat_lower": float(row["yhat_lower"]),
            "yhat_upper": float(row["yhat_upper"]),
        })
    return out


def _predict_linear(model: LinearModel, last_ts: datetime, horizon_days: int) -> list[dict]:
    out = []
    for h in range(1, horizon_days * 24 + 1):
        ts = last_ts + timedelta(hours=h)
        yhat, lo, hi = model.predict(ts)
        out.append({
            "ts": ts.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "yhat": yhat,
            "yhat_lower": lo,
            "yhat_upper": hi,
        })
    return out


def _model_path(table: str, metric: str) -> Path:
    safe = f"{table}__{metric}".replace("/", "_")
    return MODELS_DIR / f"{safe}.joblib"


def train(table: str, metric: str = "row_count") -> dict[str, Any]:
    """Fit a forecast model for (table, metric), persist it, return metadata."""
    points = _load_history(table, metric)
    if len(points) < MIN_POINTS:
        raise InsufficientDataError(
            f"need at least {MIN_POINTS} points for {table}/{metric}, got {len(points)}"
        )
    span_days = (points[-1][0] - points[0][0]).total_seconds() / 86400.0
    if _HAS_PROPHET and span_days >= MIN_PROPHET_DAYS:
        model = _fit_prophet(points)
        kind = "prophet"
    else:
        model = _fit_linear(points)
        kind = "linear"

    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "kind": kind,
        "model": model,
        "last_ts": points[-1][0].isoformat(),
        "trained_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if _HAS_JOBLIB:
        try:
            joblib.dump(payload, _model_path(table, metric))
        except Exception as e:  # pragma: no cover
            logger.warning("Failed to persist model for %s/%s: %s", table, metric, e)
    return {"kind": kind, "points": len(points), "span_days": span_days}


def _load_persisted(table: str, metric: str) -> dict | None:
    if not _HAS_JOBLIB:
        return None
    path = _model_path(table, metric)
    if not path.exists():
        return None
    try:
        return joblib.load(path)
    except Exception as e:  # pragma: no cover
        logger.warning("Failed to load model %s: %s", path, e)
        return None


def forecast(
    table: str,
    metric: str = "row_count",
    horizon_days: int = 7,
) -> list[dict]:
    """Return forecast points for the next ``horizon_days`` days.

    Loads a persisted model if present and the history hasn't grown past it;
    otherwise refits on demand. Raises InsufficientDataError when there isn't
    enough data even for the linear fallback.
    """
    points = _load_history(table, metric)
    if len(points) < MIN_POINTS:
        raise InsufficientDataError(
            f"need at least {MIN_POINTS} points for {table}/{metric}, got {len(points)}"
        )
    last_ts = points[-1][0]

    persisted = _load_persisted(table, metric)
    fresh = (
        persisted is not None
        and _parse_ts(persisted["last_ts"]) >= last_ts - timedelta(hours=1)
    )
    if not fresh:
        train(table, metric)
        persisted = _load_persisted(table, metric) or {
            "kind": "linear",
            "model": _fit_linear(points),
        }

    if persisted["kind"] == "prophet":  # pragma: no cover - heavy
        return _predict_prophet(persisted["model"], horizon_days)
    return _predict_linear(persisted["model"], last_ts, horizon_days)


def retrain_all(metrics: tuple[str, ...] = ("row_count",)) -> dict[str, int]:
    """Retrain forecasts for every monitored table. Used by the nightly cron."""
    from app.db import list_tables  # local import to avoid app cycles

    counts = {"trained": 0, "skipped": 0, "errors": 0}
    for t in list_tables():
        name = t["table_name"]
        for m in metrics:
            try:
                train(name, m)
                counts["trained"] += 1
            except InsufficientDataError:
                counts["skipped"] += 1
            except Exception as e:  # pragma: no cover - defensive
                logger.exception("forecast retrain failed for %s/%s: %s", name, m, e)
                counts["errors"] += 1
    logger.info("Forecast retrain complete: %s", counts)
    return counts
