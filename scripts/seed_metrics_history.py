"""
Seed 14 days of synthetic monitoring history into the monitor DB.

One command populates everything the demo dashboards need:
  - row_count + null_rate every 15 min   → chart "Динамика за 7 дней", forecast
  - column_distribution once per day     → drift detection (PSI/KS)

Writes to MONITOR_DB_URL via app.metrics_storage. Idempotent in the sense
that re-running adds new snapshots (timestamps differ each run); wipe with
`sqlite3 monitor.db "DELETE FROM metrics"` if you need a clean slate.

Anomalies on chart metrics (all visible in the dashboard's 7-day window):
  1. users.row_count step-up           — +15k jump on day 11 (marketing push)
  2. orders.null_rate spike            — day 10.5–11.5 (bad data load)
  3. events.ip_address null step-up    — last 7 days (logging regression)
  4. products row_count drop           — day 10 (accidental DELETE)

Drift scenarios on column_distribution:
  4. users.signup_source               — gradual web→mobile (last 7 days)
  5. orders.shipping_country           — sudden US lurch (last ~3 days)
  6. orders.amount                     — numeric mean shift ($400 → $1500)
  7. orders.items_count                — basket size 2 → 5 in last 3 days (KS)
  8. events.server_id                  — load skew toward server-1 (last 7 days)
  9. events.duration_ms                — latency 200ms → 800ms last 7d (KS)

Stable controls (severity=ok, PSI < 0.01):
  users.country, orders.status, events.device_type, products.category

Usage:
    python -m scripts.seed_metrics_history
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone

from app.metrics_storage import save_metrics

TABLES = ["users", "products", "orders", "events"]
DAYS = 14
INTERVAL_MINUTES = 15
DRIFT_SNAPSHOT_TOTAL = 1_000  # synthetic per-snapshot row count for distributions

_BASE_ROWS = {"users": 50_000, "products": 1_000, "orders": 100_000, "events": 200_000}
_GROWTH_PER_DAY = {"users": 150, "products": 2, "orders": 600, "events": 3_000}
_BASE_NULL_RATE = {"users": 0.05, "products": 0.01, "orders": 0.02, "events": 0.02}
_NULL_COLUMN = {
    "users": "email",
    "products": "price_updated_at",
    "orders": "discount",
    "events": "ip_address",
}


# ---------------------------------------------------------------------------
# Row-count + null-rate (chart + forecast input)
# ---------------------------------------------------------------------------

def _row_count(table: str, days_passed: float, ts: datetime) -> float:
    base = _BASE_ROWS[table] + _GROWTH_PER_DAY[table] * days_passed
    if table == "products" and 9.9 < days_passed < 10.9:
        base *= 0.4
    # Marketing campaign adds 15k users on day 11 — sustained step up.
    if table == "users" and days_passed >= 11.0:
        base += 15_000
    return base


def _row_count_walk(table: str, prev: float | None, target: float) -> float:
    """Walk from ``prev`` toward ``target`` with bounded per-tick variance.

    Per-tick increment is drawn from U(0, 2·expected_growth), so the mean
    matches linear growth and the cumulative sum stays close to baseline —
    no random-walk drift that PELT would mistake for regime changes.
    """
    if prev is None:
        return target
    expected_growth = max(target - prev, 0.0)
    return prev + random.uniform(0.0, 2.0 * expected_growth)


def _null_rate(table: str, days_passed: float) -> float:
    rate = max(0.0, random.gauss(_BASE_NULL_RATE[table], 0.003))
    # Bad data load — moved into the chart's 7-day visible window so the
    # change-point annotation actually shows up on /dashboard/orders.
    if table == "orders" and 10.5 < days_passed < 11.5:
        rate += 0.18
    if table == "events" and days_passed > DAYS - 7:
        rate = max(rate, 0.25)
    return round(min(rate, 1.0), 4)


# ---------------------------------------------------------------------------
# Column-distribution generators (drift input). Each returns {value: weight}.
# `progress` ranges 0.0 (window start) → 1.0 (window end).
# ---------------------------------------------------------------------------

def _users_signup_source(progress: float) -> dict[str, float]:
    if progress < 0.5:
        return {"web": 0.60, "mobile": 0.30, "api": 0.07, "referral": 0.03}
    p = (progress - 0.5) * 2
    return {"web": 0.60 - 0.45 * p, "mobile": 0.30 + 0.45 * p,
            "api": 0.07, "referral": 0.03}


def _users_country(_: float) -> dict[str, float]:
    return {"RU": 0.30, "US": 0.25, "DE": 0.15, "GB": 0.12,
            "FR": 0.08, "CN": 0.06, "BR": 0.04}


def _orders_status(_: float) -> dict[str, float]:
    return {"delivered": 0.45, "shipped": 0.20, "confirmed": 0.18,
            "pending": 0.10, "cancelled": 0.07}


def _orders_shipping_country(progress: float) -> dict[str, float]:
    if progress < 0.8:
        return {"RU": 0.25, "US": 0.20, "DE": 0.15, "GB": 0.15,
                "FR": 0.10, "CN": 0.10, "BR": 0.05}
    return {"RU": 0.10, "US": 0.65, "DE": 0.07, "GB": 0.07,
            "FR": 0.04, "CN": 0.04, "BR": 0.03}


def _orders_amount(progress: float) -> dict[str, float]:
    centers = [50 + 100 * i for i in range(20)]
    mean = 400 + 1100 * progress
    std = 200 + 400 * progress
    weights = {str(c): math.exp(-((c - mean) ** 2) / (2 * std * std)) for c in centers}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def _orders_items_count(progress: float) -> dict[str, float]:
    # Integer cart sizes 1..10. Mean shifts from ~2 to ~5 in the last ~3 days
    # (final 20% of window) — simulates an upsell or pricing-bundle change.
    if progress < 0.8:
        mean = 2.0
    else:
        mean = 2.0 + 3.0 * ((progress - 0.8) / 0.2)
    weights = {str(k): math.exp(-((k - mean) ** 2) / (2 * 1.4 * 1.4)) for k in range(1, 11)}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def _events_duration_ms(progress: float) -> dict[str, float]:
    # 100ms-wide bins from 50ms to 1950ms. Mean ramps from ~200ms (healthy)
    # to ~800ms in the last 7 days — classic "production got slower" signal.
    centers = [50 + 100 * i for i in range(20)]
    if progress < 0.5:
        mean = 200.0
    else:
        mean = 200.0 + 600.0 * ((progress - 0.5) / 0.5)
    std = 80.0 + 100.0 * progress  # spread also widens under load
    weights = {str(c): math.exp(-((c - mean) ** 2) / (2 * std * std)) for c in centers}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def _events_server_id(progress: float) -> dict[str, float]:
    p = max(0.0, (progress - 0.5) * 2)
    s1 = 0.34 + 0.45 * p
    rest = (1 - s1) / 2
    return {"server-1": s1, "server-2": rest, "server-3": rest}


def _events_device_type(_: float) -> dict[str, float]:
    return {"mobile": 0.55, "desktop": 0.35, "tablet": 0.10}


def _products_category(_: float) -> dict[str, float]:
    return {"Electronics": 0.22, "Clothing": 0.20, "Food": 0.18,
            "Books": 0.15, "Home": 0.15, "Sports": 0.10}


# (table, column, data_type, generator)
_DRIFT_COLUMNS = [
    ("users",    "signup_source",    "varchar", _users_signup_source),
    ("users",    "country",          "varchar", _users_country),
    ("orders",   "status",           "varchar", _orders_status),
    ("orders",   "shipping_country", "varchar", _orders_shipping_country),
    ("orders",   "amount",           "numeric", _orders_amount),
    ("orders",   "items_count",      "integer", _orders_items_count),
    ("events",   "server_id",        "varchar", _events_server_id),
    ("events",   "device_type",      "varchar", _events_device_type),
    ("events",   "duration_ms",      "integer", _events_duration_ms),
    ("products", "category",         "varchar", _products_category),
]


def _to_buckets(weights: dict[str, float], total: int = DRIFT_SNAPSHOT_TOTAL) -> list[dict]:
    raw = [(v, max(0.0, w + random.gauss(0, 0.005))) for v, w in weights.items()]
    s = sum(c for _, c in raw) or 1.0
    buckets = [{"value": v, "count": int(round(c / s * total))} for v, c in raw]
    buckets = [b for b in buckets if b["count"] > 0]
    buckets.sort(key=lambda b: -b["count"])
    return buckets


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def _generate():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(days=DAYS)
    t = start
    last_drift_day = -1
    prev_rc: dict[str, float | None] = {tbl: None for tbl in TABLES}
    while t <= now:
        days_passed = (t - start).total_seconds() / 86400
        for table in TABLES:
            target = _row_count(table, days_passed, t)
            in_drop = table == "products" and 9.9 < days_passed < 10.9
            # The drop window legitimately decreases row_count; outside it,
            # walk monotonically with bursty positive jitter.
            value = target if in_drop else _row_count_walk(table, prev_rc[table], target)
            prev_rc[table] = value
            yield {
                "ts": t, "table_name": table,
                "metric_name": "row_count",
                "value": int(value),
            }
            yield {
                "ts": t, "table_name": table,
                "metric_name": "null_rate",
                "value": _null_rate(table, days_passed),
                "tags": {"column": _NULL_COLUMN[table]},
            }
        # Distribution snapshot once per day at the first tick of that day.
        current_day = int(days_passed)
        if current_day != last_drift_day:
            last_drift_day = current_day
            progress = days_passed / DAYS
            for table, column, dtype, gen in _DRIFT_COLUMNS:
                buckets = _to_buckets(gen(progress))
                yield {
                    "ts": t, "table_name": table,
                    "metric_name": "column_distribution",
                    "value": float(sum(b["count"] for b in buckets)),
                    "tags": {"column": column, "data_type": dtype, "buckets": buckets},
                }
        t += timedelta(minutes=INTERVAL_MINUTES)


def main() -> None:
    random.seed(42)
    batch: list[dict] = []
    total = 0
    for row in _generate():
        batch.append(row)
        if len(batch) >= 1000:
            total += save_metrics(batch)
            batch.clear()
    total += save_metrics(batch)

    ticks = DAYS * 24 * 60 // INTERVAL_MINUTES
    drift_snaps = (DAYS + 1) * len(_DRIFT_COLUMNS)
    print(f"Seeded {total:,} metric rows into MONITOR_DB_URL:")
    print(f"  row_count + null_rate     — {len(TABLES)} tables × 2 × ~{ticks} ticks")
    print(f"  column_distribution        — {drift_snaps} snapshots ({len(_DRIFT_COLUMNS)} cols × {DAYS + 1} days)")
    print("Chart anomalies:")
    print("  users.row_count step-up    — +15k on day 11 (marketing campaign)")
    print("  orders.null_rate spike     — days 10.5–11.5")
    print("  events.ip_address step-up  — last 7 days")
    print("  products row_count drop    — day 10")
    print("Drift scenarios:")
    print("  users.signup_source        — gradual web→mobile (last 7 days)")
    print("  orders.shipping_country    — sudden US lurch (last ~3 days)")
    print("  orders.amount              — numeric mean shift ($400 → $1500)")
    print("  orders.items_count         — basket size 2 → 5 (last 3 days, KS)")
    print("  events.server_id           — server-1 load skew (last 7 days)")
    print("  events.duration_ms         — latency 200ms → 800ms (last 7d, KS)")


if __name__ == "__main__":
    main()
