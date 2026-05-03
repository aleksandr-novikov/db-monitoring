"""Distribution-drift detection for monitored columns.

Compares the most recent ``column_distribution`` snapshot (current window)
against the oldest snapshot in the rolling 7-day baseline. Two metrics:

- **PSI** (Population Stability Index) — categorical columns. PSI = Σ (c-b) * ln(c/b)
  over aligned bins, with Laplace smoothing so missing buckets don't blow up
  the log. Industry-standard thresholds: 0.1 stable, 0.2 warn, 0.25 critical.
- **KS** (two-sample Kolmogorov–Smirnov) — numeric columns. We work from the
  binned (value, count) sketches we store, not raw samples, so KS here is the
  empirical-CDF distance between two weighted ECDFs. The p-value uses the
  asymptotic two-sample formula `p = 2·exp(-2·D²·n_eff)`.

No scipy dependency — both metrics are short and self-contained.
"""
from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from app.metrics_storage import get_engine

# PSI thresholds — see docstring above.
PSI_WARN = 0.2
PSI_CRITICAL = 0.25
KS_PVALUE_THRESHOLD = 0.05
BASELINE_DAYS = 7
SMOOTHING = 1e-4

_NUMERIC_TYPE_FRAGMENTS = (
    "int", "numeric", "decimal", "real", "double", "float",
    "money", "serial", "bigserial",
)


def _is_numeric(data_type: str | None) -> bool:
    if not data_type:
        return False
    t = data_type.lower()
    return any(frag in t for frag in _NUMERIC_TYPE_FRAGMENTS)


def psi(baseline: dict[str, float], current: dict[str, float]) -> float:
    """Population Stability Index over aligned categorical bins.

    Inputs are {value: proportion} dicts (proportions sum to ~1). Missing
    keys are treated as zero and smoothed so the log stays finite. Returns
    a non-negative float; 0 means identical distributions.
    """
    keys = set(baseline) | set(current)
    if not keys:
        return 0.0
    out = 0.0
    for k in keys:
        b = baseline.get(k, 0.0) or 0.0
        c = current.get(k, 0.0) or 0.0
        if b <= 0:
            b = SMOOTHING
        if c <= 0:
            c = SMOOTHING
        out += (c - b) * math.log(c / b)
    return out


def ks_two_sample(
    baseline: list[tuple[float, int]],
    current: list[tuple[float, int]],
) -> tuple[float, float]:
    """Weighted two-sample KS distance + asymptotic p-value.

    Each input is a list of (numeric_value, count) pairs (the binned sketch we
    persist). Returns (D, p_value). Returns (0.0, 1.0) for empty inputs.
    """
    if not baseline or not current:
        return 0.0, 1.0
    n1 = sum(c for _, c in baseline)
    n2 = sum(c for _, c in current)
    if n1 == 0 or n2 == 0:
        return 0.0, 1.0

    xs = sorted({v for v, _ in baseline} | {v for v, _ in current})
    b_map: dict[float, int] = {}
    c_map: dict[float, int] = {}
    for v, c in baseline:
        b_map[v] = b_map.get(v, 0) + c
    for v, c in current:
        c_map[v] = c_map.get(v, 0) + c

    cum_b = cum_c = 0
    d = 0.0
    for x in xs:
        cum_b += b_map.get(x, 0)
        cum_c += c_map.get(x, 0)
        d = max(d, abs(cum_b / n1 - cum_c / n2))

    n_eff = (n1 * n2) / (n1 + n2)
    p = math.exp(-2.0 * d * d * n_eff)  # one-sided
    p = min(1.0, 2.0 * p)               # two-sided approximation
    return d, p


def _severity(psi_value: float) -> str:
    if psi_value > PSI_CRITICAL:
        return "critical"
    if psi_value > PSI_WARN:
        return "warn"
    return "ok"


def _load_distributions(table_name: str, since: datetime) -> list[dict]:
    """Pull every column_distribution snapshot for a table since `since`.

    Decodes JSON tags eagerly so callers can iterate without re-parsing.
    """
    stmt = text("""
        SELECT ts, tags
        FROM metrics
        WHERE table_name = :table_name
          AND metric_name = 'column_distribution'
          AND ts >= :since
        ORDER BY ts
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {
            "table_name": table_name,
            "since": since.isoformat(timespec="seconds"),
        }).fetchall()
    out = []
    for ts, tags in rows:
        if not tags:
            continue
        try:
            t = json.loads(tags)
        except (TypeError, ValueError):
            continue
        out.append({"ts": ts, **t})
    return out


def _split_baseline_current(
    snapshots: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    """Per-column: pick the oldest snapshot as baseline, newest as current."""
    by_col: dict[str, list[dict]] = {}
    for s in snapshots:
        by_col.setdefault(s["column"], []).append(s)
    baseline: dict[str, dict] = {}
    current: dict[str, dict] = {}
    for col, snaps in by_col.items():
        snaps.sort(key=lambda x: x["ts"])
        baseline[col] = snaps[0]
        current[col] = snaps[-1]
    return baseline, current


def _to_proportions(buckets: list[dict]) -> dict[str, float]:
    total = sum(b.get("count", 0) for b in buckets)
    if total == 0:
        return {}
    return {b["value"]: b["count"] / total for b in buckets}


def _numeric_pairs(buckets: list[dict]) -> list[tuple[float, int]]:
    out = []
    for b in buckets:
        try:
            out.append((float(b["value"]), int(b["count"])))
        except (TypeError, ValueError):
            continue
    return out


def compute_drift(
    table_name: str, baseline_days: int = BASELINE_DAYS
) -> list[dict]:
    """Return per-column drift report for `table_name`.

    Each entry: {column, data_type, psi, ks_pvalue, is_drift, severity}.
    Columns missing from baseline OR current are omitted (no comparison
    possible). When fewer than 2 snapshots exist for a column, severity is
    "insufficient_data" and is_drift is False.
    """
    since = datetime.now(timezone.utc) - timedelta(days=baseline_days)
    snapshots = _load_distributions(table_name, since)
    if not snapshots:
        return []
    baseline, current = _split_baseline_current(snapshots)

    out: list[dict] = []
    for column, cur in current.items():
        base = baseline.get(column)
        if base is None or base["ts"] == cur["ts"]:
            out.append({
                "column": column,
                "data_type": cur.get("data_type"),
                "psi": None,
                "ks_pvalue": None,
                "is_drift": False,
                "severity": "insufficient_data",
            })
            continue

        cur_buckets = cur.get("buckets") or []
        base_buckets = base.get("buckets") or []
        psi_val = psi(_to_proportions(base_buckets), _to_proportions(cur_buckets))

        ks_p: float | None = None
        if _is_numeric(cur.get("data_type")):
            _, ks_p = ks_two_sample(
                _numeric_pairs(base_buckets), _numeric_pairs(cur_buckets)
            )

        severity = _severity(psi_val)
        is_drift = severity != "ok" or (ks_p is not None and ks_p < KS_PVALUE_THRESHOLD)
        out.append({
            "column": column,
            "data_type": cur.get("data_type"),
            "psi": round(psi_val, 4),
            "ks_pvalue": round(ks_p, 4) if ks_p is not None else None,
            "is_drift": is_drift,
            "severity": severity,
        })

    out.sort(key=lambda r: -(r["psi"] or 0))
    return out
