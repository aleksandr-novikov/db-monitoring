"""
Seed 14 days of synthetic metric history (every 15 min) for demo dashboards.

Writes to the monitoring DB (MONITOR_DB_URL) via app.metrics_storage.
Includes 3 anomalies visible on charts:
  1. null_rate spike in orders   — day 6–7  (simulates a bad data load)
  2. gradual null_rate rise in events — last 5 days (simulates a growing regression)
  3. sudden row_count drop in products  — day 10 (simulates an accidental DELETE)

Usage:
    python -m scripts.seed_metrics_history
"""
import math
import os
import random
from datetime import datetime, timedelta, timezone

os.environ.setdefault("MONITOR_DB_URL", "sqlite:///monitor.db")

from app.metrics_storage import save_metrics  # noqa: E402

TABLES = ["users", "products", "orders", "events"]
DAYS = 14
INTERVAL_MINUTES = 15

_BASE_ROWS = {
    "users": 50_000,
    "products": 1_000,
    "orders": 100_000,
    "events": 200_000,
}
_GROWTH_PER_DAY = {
    "users": 150,
    "products": 2,
    "orders": 600,
    "events": 3_000,
}
_BASE_NULL_RATE = {
    "users": 0.05,
    "products": 0.01,
    "orders": 0.02,
    "events": 0.02,
}
_NULL_COLUMN = {
    "users": "email",
    "products": "description",
    "orders": "discount",
    "events": "payload",
}


def _row_count(table: str, days_passed: float, ts: datetime) -> float:
    base = _BASE_ROWS[table] + _GROWTH_PER_DAY[table] * days_passed
    # daily seasonality ±8%, peak at 14:00 UTC
    seasonality = 1 + 0.08 * math.sin((ts.hour - 14) / 24 * 2 * math.pi)
    noise = 1 + random.uniform(-0.01, 0.01)
    # anomaly 3: sudden drop in products on day 10 (accidental delete)
    if table == "products" and 9.9 < days_passed < 10.1:
        base *= 0.4
    return base * seasonality * noise


def _null_rate(table: str, days_passed: float) -> float:
    rate = max(0.0, random.gauss(_BASE_NULL_RATE[table], 0.003))
    # anomaly 1: null_rate spike in orders around day 6–7
    if table == "orders" and 5.8 < days_passed < 7.2:
        rate += 0.18
    # anomaly 2: gradual null_rate rise in events over last 5 days
    if table == "events" and days_passed > DAYS - 5:
        rise = (days_passed - (DAYS - 5)) / 5  # 0→1 over 5 days
        rate += 0.20 * rise
    return round(min(rate, 1.0), 4)


def _generate():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(days=DAYS)
    t = start
    while t <= now:
        days_passed = (t - start).total_seconds() / 86400
        for table in TABLES:
            yield {
                "ts": t,
                "table_name": table,
                "metric_name": "row_count",
                "value": int(_row_count(table, days_passed, t)),
            }
            yield {
                "ts": t,
                "table_name": table,
                "metric_name": "null_rate",
                "value": _null_rate(table, days_passed),
                "tags": {"column": _NULL_COLUMN[table]},
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
    print(f"Seeded {total:,} metric rows:")
    print(f"  {len(TABLES)} tables × 2 metrics × ~{ticks} ticks = ~{len(TABLES) * 2 * ticks:,} rows")
    print("Anomalies injected:")
    print("  orders   null_rate spike      — days 6–7")
    print("  events   gradual null_rate ↑  — last 5 days")
    print("  products row_count drop       — day 10")


if __name__ == "__main__":
    main()
