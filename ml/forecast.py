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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from app.metrics_storage import get_changepoints, get_metrics

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
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    # SQLAlchemy may hand back tz-naive ISO strings depending on the driver.
    s = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _load_history(
    table: str, metric: str, days: int = 60, project_id: str = "legacy"
) -> list[tuple[datetime, float]]:
    rows = get_metrics(table, metric, project_id, window=timedelta(days=days))
    return [(_parse_ts(r["ts"]), float(r["value"])) for r in rows]


def _fit_linear(points: list[tuple[datetime, float]]) -> LinearModel:
    t0 = points[0][0].timestamp()
    xs = [p[0].timestamp() - t0 for p in points]
    ys = [p[1] for p in points]
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    num = sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=False))
    den = sum((x - mx) ** 2 for x in xs)
    slope = num / den if den else 0.0
    intercept = my - slope * mx
    residuals = [y - (slope * x + intercept) for x, y in zip(xs, ys, strict=False)]
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


_SEVERE_DROP_RATIO = 0.2


def _is_severe_drop(cp: dict, threshold: float = _SEVERE_DROP_RATIO) -> bool:
    """True when the changepoint represents a large sudden decrease.

    Criterion: value_after < value_before AND value_after / value_before < threshold.
    Used to switch the training strategy: instead of a full-window fit that
    extrapolates the negative trend into the future, we use only post-CP data.
    """
    before = cp.get("value_before", 1.0)
    after = cp.get("value_after", 0.0)
    if before <= 0:
        return False
    return after < before and (after / before) < threshold


def _normalize_forecast_point(p: dict) -> dict:
    """Clamp all forecast values to [0, ∞) and restore CI ordering.

    Called unconditionally after any shift so the invariants hold regardless
    of whether an anchor shift was applied.
    """
    p["yhat"]       = max(0.0, p["yhat"])
    p["yhat_lower"] = max(0.0, p["yhat_lower"])
    p["yhat_upper"] = max(0.0, p["yhat_upper"])
    p["yhat_lower"] = min(p["yhat_lower"], p["yhat"])
    p["yhat_upper"] = max(p["yhat_upper"], p["yhat"])
    return p


def _anchor_shift(out: list[dict], last_value: float | None) -> list[dict]:
    """Shift the entire forecast so its first point lands on `last_value`.

    Prophet's trend on a series with step jumps fits a smooth curve through
    the middle of the data, so its yhat at last_ts can sit far below the
    actual last value (e.g. 70k vs 80k for our events seed). Linear OLS on
    a stepped series has the same issue. Shifting by `last_value - yhat[0]`
    keeps the trend slope and the CI width but anchors the line at the
    user's actual last-observed value, eliminating the visual gap between
    fact and forecast on the chart.
    """
    if not out:
        return out
    if last_value is not None:
        shift = last_value - out[0]["yhat"]
        if shift != 0:
            for p in out:
                p["yhat"]       = p["yhat"]       + shift
                p["yhat_lower"] = p["yhat_lower"] + shift
                p["yhat_upper"] = p["yhat_upper"] + shift
    for p in out:
        _normalize_forecast_point(p)
    return out


def _predict_prophet(model, horizon_days: int, last_value: float | None = None) -> list[dict]:  # pragma: no cover
    future = model.make_future_dataframe(periods=horizon_days * 24, freq="h",
                                          include_history=False)
    fc = model.predict(future)
    out = []
    for _, row in fc.iterrows():
        ds = row["ds"]
        if hasattr(ds, "to_pydatetime"):
            ds = ds.to_pydatetime()
        out.append({
            "ts": ds.replace(tzinfo=UTC).isoformat(timespec="seconds"),
            "yhat": float(row["yhat"]),
            "yhat_lower": max(0.0, float(row["yhat_lower"])),
            "yhat_upper": float(row["yhat_upper"]),
        })
    return _anchor_shift(out, last_value)


def _predict_linear(
    model: LinearModel,
    last_ts: datetime,
    horizon_days: int,
    last_value: float | None = None,
) -> list[dict]:
    out = []
    for h in range(1, horizon_days * 24 + 1):
        ts = last_ts + timedelta(hours=h)
        yhat, lo, hi = model.predict(ts)
        out.append({
            "ts": ts.astimezone(UTC).isoformat(timespec="seconds"),
            "yhat": yhat,
            "yhat_lower": max(0.0, lo),
            "yhat_upper": hi,
        })
    return _anchor_shift(out, last_value)


def _model_path(table: str, metric: str, project_id: str = "legacy") -> Path:
    safe = f"{table}__{metric}".replace("/", "_")
    if project_id == "legacy":
        return MODELS_DIR / f"{safe}.joblib"
    safe_project = project_id.replace("/", "_").replace(" ", "_")
    return MODELS_DIR / f"{safe_project}__{safe}.joblib"


def train(
    table: str, metric: str = "row_count", project_id: str = "legacy"
) -> dict[str, Any]:
    """Fit a forecast model for (table, metric, project_id), persist it, return metadata."""
    cps = get_changepoints(table, metric, window=timedelta(days=60), project_id=project_id)
    last_cp = cps[-1] if cps else None
    last_cp_ts: str | None = last_cp["ts"] if last_cp else None
    last_value_override: float | None = None

    if last_cp_ts is not None:
        since = _parse_ts(last_cp_ts)
        cp_age_days = (datetime.now(UTC) - since).total_seconds() / 86400.0
        if cp_age_days >= MIN_PROPHET_DAYS:
            # Old changepoint — the new regime has had time to stabilise.
            # Fit only on strictly-post-cp data so the forecast reflects
            # the new regime, not the pre-cp baseline. The simple
            # `days=days_since` window from `now` would still bleed pre-cp
            # ticks in, hence the explicit `p[0] >= since` filter.
            days_since = int(cp_age_days) + 2
            points = [
                p for p in _load_history(table, metric, days=days_since, project_id=project_id)
                if p[0] >= since
            ]
            if len(points) < MIN_POINTS:
                points = _load_history(table, metric, project_id=project_id)
        elif last_cp is not None and _is_severe_drop(last_cp):
            # Recent severe drop (e.g. 96k → 4): the full window carries a
            # strong negative trend that Prophet/OLS extrapolates into negative
            # territory after anchor-shift. Use post-CP points to capture the
            # new regime. When not enough post-CP metrics exist yet, synthesize
            # a flat series from cp.value_after — not from the last recorded
            # metric, which could still be the pre-CP value.
            full_points = _load_history(table, metric, project_id=project_id)
            post_cp_points = [p for p in full_points if p[0] >= since]
            if len(post_cp_points) >= MIN_POINTS:
                points = post_cp_points
            else:
                last_ts = full_points[-1][0] if full_points else since
                flat_val = last_cp["value_after"]
                points = [
                    (last_ts - timedelta(hours=1), flat_val),
                    (last_ts, flat_val),
                ]
                last_value_override = flat_val
        else:
            # Recent changepoint that is not a severe drop (e.g. growth or
            # moderate step). Fitting on post-cp alone would leave a short
            # flat plateau and lose the longer-term trend context. Use the
            # full window: Prophet's piecewise trend handles step shifts
            # natively, and the linear fallback carries the long-term slope.
            points = _load_history(table, metric, project_id=project_id)
    else:
        points = _load_history(table, metric, project_id=project_id)

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
        "last_changepoint_ts": last_cp_ts,
        "trained_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "last_value_override": last_value_override,
    }
    if _HAS_JOBLIB:
        try:
            joblib.dump(payload, _model_path(table, metric, project_id))
        except Exception as e:  # pragma: no cover
            logger.warning("Failed to persist model for %s/%s: %s", table, metric, e)
    return {"kind": kind, "points": len(points), "span_days": span_days}


def _load_persisted(table: str, metric: str, project_id: str = "legacy") -> dict | None:
    if not _HAS_JOBLIB:
        return None
    path = _model_path(table, metric, project_id)
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
    project_id: str = "legacy",
) -> list[dict]:
    """Return forecast points for the next ``horizon_days`` days.

    Loads a persisted model if present and the history hasn't grown past it;
    otherwise refits on demand. Raises InsufficientDataError when there isn't
    enough data even for the linear fallback.
    """
    points = _load_history(table, metric, project_id=project_id)
    if len(points) < MIN_POINTS:
        raise InsufficientDataError(
            f"need at least {MIN_POINTS} points for {table}/{metric}, got {len(points)}"
        )
    last_ts = points[-1][0]

    cps = get_changepoints(table, metric, window=timedelta(days=60), project_id=project_id)
    last_cp_ts = cps[-1]["ts"] if cps else None

    persisted = _load_persisted(table, metric, project_id)
    fresh = (
        persisted is not None
        and _parse_ts(persisted["last_ts"]) >= last_ts - timedelta(hours=1)
        and persisted.get("last_changepoint_ts") == last_cp_ts
    )
    if not fresh:
        train(table, metric, project_id)
        persisted = _load_persisted(table, metric, project_id) or {
            "kind": "linear",
            "model": _fit_linear(points),
        }

    override = persisted.get("last_value_override")
    last_value = override if override is not None else points[-1][1]
    if persisted["kind"] == "prophet":  # pragma: no cover - heavy
        return _predict_prophet(persisted["model"], horizon_days, last_value=last_value)
    return _predict_linear(persisted["model"], last_ts, horizon_days, last_value=last_value)


def retrain_all(
    metrics: tuple[str, ...] = ("row_count",),
    project_id: str = "legacy",
    tables: list[str] | None = None,
) -> dict[str, int]:
    """Retrain forecasts for every monitored table. Used by the nightly cron."""
    if tables is None:
        from app.db import list_tables  # local import to avoid app cycles
        tables = [t["table_name"] for t in list_tables()]

    counts = {"trained": 0, "skipped": 0, "errors": 0}
    for name in tables:
        for m in metrics:
            try:
                train(name, m, project_id)
                counts["trained"] += 1
            except InsufficientDataError:
                counts["skipped"] += 1
            except Exception as e:  # pragma: no cover - defensive
                logger.exception("forecast retrain failed for %s/%s: %s", name, m, e)
                counts["errors"] += 1
    logger.info("Forecast retrain complete: %s", counts)
    return counts
