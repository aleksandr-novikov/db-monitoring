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

Drift scenarios on column_distribution (PSI / KS):
  4. users.signup_source               — gradual web→mobile (last 7 days)
  5. users.age                         — younger signups, mean 35 → 27 (KS)
  6. orders.shipping_country           — sudden US lurch (last ~3 days)
  7. orders.amount                     — numeric mean shift $400 → $1500 (KS)
  8. orders.items_count                — basket size 2 → 5 in last 3 days (KS)
  9. events.server_id                  — load skew toward server-1 (last 7d)
 10. events.duration_ms                — latency 200ms → 800ms last 7d (KS)
 11. products.stock                    — capacity drain 200 → 50 (KS)
 12. products.price                    — pricing bump $50 → $80 (KS)

Stable controls (severity=ok, PSI < 0.01):
  users.country, orders.status, events.device_type,
  events.events_in_session, products.category, products.return_rate

Schema-drift events (засеваются как готовые события в monitor.schema_events):
  10. users.country         — column_added 2 дня назад (триггерит ⚠ badge)
  11. orders.discount       — type_changed 5 дней назад (numeric → numeric(6,4))
  12. events.prev_event_type — nullable_changed 10 дней назад (за пределами 7д alert)
  13. products.price_updated_at — column_removed 12 дней назад

Usage:
    python -m scripts.seed_metrics_history
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone

from app.metrics_storage import save_metrics, save_schema_events

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

# Approximate avg row size in bytes — used to fabricate `size_bytes` metrics
# during local-only seeding (the live collector would compute these from
# pg_total_relation_size). Not exact but plausible for demo charts.
_AVG_ROW_BYTES = {
    "users": 200,
    "products": 500,
    "orders": 250,
    "events": 150,
}

# Per-table null fractions for every column the live collector would touch.
# Powers the schema panel (which reads `null_count` per column tagged with
# `column` in metric tags). Columns missing here render as "—" — same
# behaviour as if the collector had never run, but with the seeder they
# stay missing on purpose (e.g. NOT NULL system columns).
_NULL_FRACTIONS: dict[str, dict[str, float]] = {
    "users": {
        "id": 0.0, "email": 0.05, "age": 0.02, "country": 0.0,
        "signup_source": 0.0, "created_at": 0.0, "updated_at": 0.0,
    },
    "products": {
        "id": 0.0, "name": 0.0, "category": 0.0, "price": 0.0,
        "cost_price": 0.0, "stock": 0.0, "avg_daily_sales": 0.0,
        "return_rate": 0.0, "price_updated_at": 0.30,
        "created_at": 0.0, "updated_at": 0.0,
    },
    "orders": {
        "id": 0.0, "user_id": 0.0, "amount": 0.0, "items_count": 0.0,
        "discount": 0.02, "shipping_country": 0.0, "status": 0.0,
        "has_prior_events": 0.0, "user_orders_last_1h": 0.0,
        "amount_vs_avg_ratio": 0.0, "created_at": 0.0,
    },
    "events": {
        "id": 0.0, "user_id": 0.0, "session_id": 0.0,
        "event_type": 0.0, "prev_event_type": 0.20,
        "prev_event_gap_s": 0.20, "duration_ms": 0.0,
        "events_in_session": 0.0, "ip_address": 0.25,
        "ip_events_last_1h": 0.0, "server_id": 0.0,
        "device_type": 0.0, "is_bot_suspected": 0.0,
        "created_at": 0.0,
    },
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


def _events_events_in_session(_: float) -> dict[str, float]:
    # Stable Gaussian around mean=5. Control series — engagement levels
    # don't change in any of our seeded scenarios.
    centers = list(range(1, 21))
    mean = 5.0
    std = 3.0
    weights = {str(c): math.exp(-((c - mean) ** 2) / (2 * std * std)) for c in centers}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def _users_age(progress: float) -> dict[str, float]:
    # Age bins every 5 years from 18 to 73. Mean drifts younger over the last
    # 7 days — simulates a marketing push that brought in younger signups.
    centers = list(range(18, 76, 5))
    if progress < 0.5:
        mean = 35.0
    else:
        mean = 35.0 - 8.0 * ((progress - 0.5) / 0.5)  # 35 → 27
    std = 10.0
    weights = {str(c): math.exp(-((c - mean) ** 2) / (2 * std * std)) for c in centers}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def _products_stock(progress: float) -> dict[str, float]:
    # Stock distribution drifts down from ~200 to ~50 in the last 7 days —
    # "running out" scenario, useful for capacity-planning demos.
    centers = list(range(0, 500, 25))
    if progress < 0.5:
        mean = 200.0
    else:
        mean = 200.0 - 150.0 * ((progress - 0.5) / 0.5)
    std = 50.0
    weights = {str(c): math.exp(-((c - mean) ** 2) / (2 * std * std)) for c in centers}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def _products_price(progress: float) -> dict[str, float]:
    # Average price ramps from $50 to $80 in last 7 days — pricing change.
    centers = [10 + 10 * i for i in range(20)]  # $10..$200
    if progress < 0.5:
        mean = 50.0
    else:
        mean = 50.0 + 30.0 * ((progress - 0.5) / 0.5)
    std = 20.0
    weights = {str(c): math.exp(-((c - mean) ** 2) / (2 * std * std)) for c in centers}
    s = sum(weights.values()) or 1.0
    return {k: v / s for k, v in weights.items()}


def _products_return_rate(_: float) -> dict[str, float]:
    # Stable return rate around 5%, bins every 1% from 0% to 12%. Numeric
    # control to balance the seeded numeric drift cases.
    centers = [round(i * 0.01, 2) for i in range(13)]
    mean = 0.05
    std = 0.025
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
    # users — categorical drift, categorical control, numeric drift
    ("users",    "signup_source",    "varchar", _users_signup_source),
    ("users",    "country",          "varchar", _users_country),
    ("users",    "age",              "integer", _users_age),
    # orders — categorical control, categorical drift, two numeric drifts
    ("orders",   "status",           "varchar", _orders_status),
    ("orders",   "shipping_country", "varchar", _orders_shipping_country),
    ("orders",   "amount",           "numeric", _orders_amount),
    ("orders",   "items_count",      "integer", _orders_items_count),
    # events — categorical drift, categorical control, numeric drift, numeric control
    ("events",   "server_id",        "varchar", _events_server_id),
    ("events",   "device_type",      "varchar", _events_device_type),
    ("events",   "duration_ms",      "integer", _events_duration_ms),
    ("events",   "events_in_session", "integer", _events_events_in_session),
    # products — categorical control, two numeric drifts, numeric control
    ("products", "category",         "varchar", _products_category),
    ("products", "stock",            "integer", _products_stock),
    ("products", "price",            "numeric", _products_price),
    ("products", "return_rate",      "double precision", _products_return_rate),
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
    final_row_counts: dict[str, int] = {}
    while t <= now:
        days_passed = (t - start).total_seconds() / 86400
        is_last_tick = t + timedelta(minutes=INTERVAL_MINUTES) > now
        for table in TABLES:
            target = _row_count(table, days_passed, t)
            in_drop = table == "products" and 9.9 < days_passed < 10.9
            # The drop window legitimately decreases row_count; outside it,
            # walk monotonically with bursty positive jitter.
            value = target if in_drop else _row_count_walk(table, prev_rc[table], target)
            prev_rc[table] = value
            row_count = int(value)
            yield {
                "ts": t, "table_name": table,
                "metric_name": "row_count",
                "value": row_count,
            }
            yield {
                "ts": t, "table_name": table,
                "metric_name": "size_bytes",
                "value": float(row_count * _AVG_ROW_BYTES[table]),
            }
            yield {
                "ts": t, "table_name": table,
                "metric_name": "null_rate",
                "value": _null_rate(table, days_passed),
                "tags": {"column": _NULL_COLUMN[table]},
            }
            if is_last_tick:
                final_row_counts[table] = row_count
                for col, frac in _NULL_FRACTIONS.get(table, {}).items():
                    yield {
                        "ts": t, "table_name": table,
                        "metric_name": "null_count",
                        "value": float(int(row_count * frac)),
                        "tags": {"column": col},
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


def _schema_drift_events() -> list[dict]:
    """Pre-baked schema_events so /dashboard/<table> shows the alert badge
    and the events list without anyone actually running ALTER TABLE on
    Supabase. Timestamps span 12 days back so the demo covers both the
    7-day alert window and the older events that only appear in the list.
    """
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    def at(days_ago: float) -> str:
        return (now - timedelta(days=days_ago)).isoformat(timespec="seconds")

    return [
        {
            "ts": at(2),
            "table_name": "users",
            "change_type": "column_added",
            "column_name": "country",
            "details": {"after": {"type": "text", "nullable": False}},
        },
        {
            "ts": at(5),
            "table_name": "orders",
            "change_type": "type_changed",
            "column_name": "discount",
            "details": {
                "before": {"type": "numeric"},
                "after": {"type": "numeric(6,4)"},
            },
        },
        {
            "ts": at(10),
            "table_name": "events",
            "change_type": "nullable_changed",
            "column_name": "prev_event_type",
            "details": {"before": {"nullable": True}, "after": {"nullable": False}},
        },
        {
            "ts": at(12),
            "table_name": "products",
            "change_type": "column_removed",
            "column_name": "price_updated_at",
            "details": {"before": {"type": "timestamptz", "nullable": True}},
        },
    ]


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

    schema_events = _schema_drift_events()
    save_schema_events(schema_events)

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
    print("  users.age                  — younger signups, mean 35 → 27 (KS)")
    print("  orders.shipping_country    — sudden US lurch (last ~3 days)")
    print("  orders.amount              — numeric mean shift ($400 → $1500)")
    print("  orders.items_count         — basket size 2 → 5 (last 3 days, KS)")
    print("  events.server_id           — server-1 load skew (last 7 days)")
    print("  events.duration_ms         — latency 200ms → 800ms (last 7d, KS)")
    print("  products.stock             — capacity drain 200 → 50 (KS)")
    print("  products.price             — pricing bump $50 → $80 (KS)")
    print("Stable controls (severity=ok):")
    print("  users.country, orders.status, events.device_type,")
    print("  events.events_in_session, products.category, products.return_rate")
    print(f"Schema drift events:        — {len(schema_events)} events seeded")
    print("  users.country              — column_added 2 days ago (⚠ badge)")
    print("  orders.discount            — type_changed 5 days ago (⚠ badge)")
    print("  events.prev_event_type     — nullable_changed 10 days ago")
    print("  products.price_updated_at  — column_removed 12 days ago")


if __name__ == "__main__":
    main()
