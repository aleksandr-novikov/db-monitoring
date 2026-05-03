"""Offline change-point detection on stored metric series.

Wraps ``ruptures.Pelt`` with an RBF cost — robust to non-Gaussian noise, finds
abrupt mean shifts (the typical ETL-bug or migration footprint). Falls back to
a CUSUM-style scan implemented in pure NumPy when ``ruptures`` isn't installed,
so the feature degrades gracefully.

Each detection returns: ``ts`` (ISO breakpoint), ``score`` (mean shift in pre-
window standard deviations — unitless, comparable across metrics), and
``value_before`` / ``value_after`` (window means around the cut). Score is what
the UI uses to threshold "show me this annotation".
"""
from __future__ import annotations

import logging
import math
import statistics
from datetime import datetime, timedelta, timezone
from typing import Iterable

from app.metrics_storage import get_metrics, save_changepoints

try:  # pragma: no cover - optional heavy dep
    import numpy as np  # type: ignore
    import ruptures as rpt  # type: ignore
    _HAS_RUPTURES = True
except Exception:  # pragma: no cover
    np = None  # type: ignore
    rpt = None  # type: ignore
    _HAS_RUPTURES = False

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 14
MIN_POINTS = 8
MIN_SCORE = 1.5            # below this, the shift is in the noise floor
MIN_SEGMENT = 4            # don't treat first/last 4 points as a change-point
MIN_RELATIVE_SHIFT = 0.15  # secondary filter — ignore <15% shifts of the pre-mean
LOCAL_WINDOW_POINTS = 48   # ≈ 12h at 15-min ticks; scope scoring locally so an
                           # earlier shift doesn't pollute the before-window stats
DEDUPE_WINDOW_HOURS = 72   # collapse PELT's hierarchical splits around the same real shift
PELT_PENALTY = 10.0        # high enough to ignore linear trends; spikes still surface

# Cumulative metrics with natural linear growth — detrend before fitting PELT,
# otherwise the bursty insert noise produces ghost change-points along the
# growth curve. Bounded-ratio metrics (null_rate) stay raw.
_DETREND_METRICS = {"row_count", "size_bytes"}


def _parse_ts(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _score(values: list[float], idx: int) -> tuple[float, float, float]:
    """Return (score, mean_before, mean_after) for a candidate cut at ``idx``.

    Both windows are clipped to ``LOCAL_WINDOW_POINTS`` around the candidate,
    so an earlier change-point in the same series doesn't inflate the
    before-window stdev and starve this candidate of score.
    """
    lo = max(0, idx - LOCAL_WINDOW_POINTS)
    hi = min(len(values), idx + LOCAL_WINDOW_POINTS)
    before = values[lo:idx]
    after = values[idx:hi]
    if len(before) < 2 or len(after) < 2:
        return 0.0, 0.0, 0.0
    mb = statistics.fmean(before)
    ma = statistics.fmean(after)
    sd = statistics.pstdev(before) if len(before) >= 2 else 0.0
    # 1% of the pre-window mean as the noise floor — keeps the score
    # comparable across metrics whose absolute scale differs by orders of
    # magnitude (null_rate ~0.02 vs row_count ~100_000).
    floor = max(abs(mb) * 0.01, 1e-4)
    return abs(ma - mb) / max(sd, floor), mb, ma


def _detrend(values: list[float]) -> list[float]:
    """Subtract the OLS linear fit. Used for cumulative metrics so PELT only
    sees the residual — abrupt shifts stand out, gradual growth is removed."""
    if not _HAS_RUPTURES or len(values) < 2:
        return list(values)
    n = len(values)
    arr = np.asarray(values, dtype=float)
    x = np.arange(n, dtype=float)
    slope, intercept = np.polyfit(x, arr, 1)
    return list(arr - (slope * x + intercept))


def _detect_pelt(values: list[float], detrend: bool = False) -> list[int]:
    """Return candidate breakpoint indices using ruptures' PELT/RBF."""
    if not _HAS_RUPTURES or len(values) < MIN_POINTS:
        return []
    series = _detrend(values) if detrend else values
    arr = np.asarray(series, dtype=float).reshape(-1, 1)
    algo = rpt.Pelt(model="rbf", min_size=MIN_SEGMENT).fit(arr)
    # ruptures returns segment END indices; the last one is len(values).
    bkps = [b for b in algo.predict(pen=PELT_PENALTY) if b < len(values)]
    return bkps


def _detect_cusum(values: list[float]) -> list[int]:
    """NumPy-free CUSUM fallback: scan for the index that maximises the
    standardised mean shift, then iteratively look for additional shifts in
    the residual segments. Crude, but enough to flag the seeded `orders`
    null-rate spike when ruptures is missing.
    """
    n = len(values)
    if n < MIN_POINTS:
        return []
    bkps: list[int] = []
    queue: list[tuple[int, int]] = [(0, n)]
    while queue:
        lo, hi = queue.pop()
        if hi - lo < 2 * MIN_SEGMENT:
            continue
        best_idx, best_score = -1, 0.0
        for i in range(lo + MIN_SEGMENT, hi - MIN_SEGMENT):
            score, _, _ = _score(values[lo:hi], i - lo)
            if score > best_score:
                best_idx, best_score = i, score
        if best_idx < 0 or best_score < MIN_SCORE:
            continue
        bkps.append(best_idx)
        queue.append((lo, best_idx))
        queue.append((best_idx, hi))
    return sorted(bkps)


def detect_changepoints(
    table: str,
    metric: str,
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> list[dict]:
    """Return change-point events for a single (table, metric) series.

    Each event: {ts, table_name, metric_name, score, value_before, value_after}.
    Scores below ``MIN_SCORE`` are suppressed — they fall into noise and
    polluting the chart with weak annotations is worse than missing them.
    """
    rows = get_metrics(table, metric, window=timedelta(days=window_days))
    if len(rows) < MIN_POINTS:
        return []
    timestamps = [_parse_ts(r["ts"]) for r in rows]
    values = [float(r["value"]) for r in rows]

    detrend = metric in _DETREND_METRICS
    indices = (
        _detect_pelt(values, detrend=detrend) if _HAS_RUPTURES else _detect_cusum(values)
    )

    events: list[dict] = []
    for idx in indices:
        if idx < MIN_SEGMENT or idx > len(values) - MIN_SEGMENT:
            continue
        score, before, after = _score(values, idx)
        if score < MIN_SCORE or not math.isfinite(score):
            continue
        denom = max(abs(before), 1e-6)
        if abs(after - before) / denom < MIN_RELATIVE_SHIFT:
            continue
        events.append({
            "ts": timestamps[idx].isoformat(timespec="seconds"),
            "_dt": timestamps[idx],
            "table_name": table,
            "metric_name": metric,
            "score": round(score, 3),
            "value_before": round(before, 4),
            "value_after": round(after, 4),
        })
    return _dedupe(events)


def _dedupe(events: list[dict]) -> list[dict]:
    """Collapse near-duplicate detections that point at the same real shift.

    PELT often emits several breakpoints around a single regime change; we
    keep the highest-scoring one. But a temporary spike has *two* legitimate
    change-points (rise + fall) — they shift in opposite directions, so we
    preserve them even when they fall inside the dedup window.
    """
    if not events:
        return []
    events.sort(key=lambda e: e["_dt"])
    kept: list[dict] = []
    for e in events:
        same_direction = kept and (
            (e["value_after"] > e["value_before"]) ==
            (kept[-1]["value_after"] > kept[-1]["value_before"])
        )
        within_window = kept and (
            e["_dt"] - kept[-1]["_dt"] <= timedelta(hours=DEDUPE_WINDOW_HOURS)
        )
        if within_window and same_direction:
            if e["score"] > kept[-1]["score"]:
                kept[-1] = e
        else:
            kept.append(e)
    for e in kept:
        e.pop("_dt", None)
    return kept


def detect_all(
    metrics: Iterable[str] = ("row_count", "null_rate"),
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> dict[str, int]:
    """Run detection across every monitored (table, metric) and persist hits."""
    from app.db import list_tables  # local import — avoids app import cycle

    counts = {"detected": 0, "tables": 0, "errors": 0}
    for t in list_tables():
        counts["tables"] += 1
        name = t["table_name"]
        for m in metrics:
            try:
                events = detect_changepoints(name, m, window_days=window_days)
            except Exception as e:  # pragma: no cover - defensive
                logger.exception("changepoint detection failed for %s/%s: %s", name, m, e)
                counts["errors"] += 1
                continue
            if events:
                save_changepoints(events)
                counts["detected"] += len(events)
    logger.info("Change-point sweep complete: %s", counts)
    return counts
