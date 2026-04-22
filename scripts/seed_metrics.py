"""Seed 2 weeks of synthetic metrics for demo and local dev.

Usage:
    python -m scripts.seed_metrics
"""
import math
import os
import random
from datetime import datetime, timedelta, timezone

os.environ.setdefault("MONITOR_DB_URL", "sqlite:///monitor.db")

from app.metrics_storage import save_metrics  # noqa: E402

TABLES = ["users", "products", "orders", "events"]
DAYS = 14
INTERVAL_MINUTES = 5


def _rows():
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start = now - timedelta(days=DAYS)
    base_rows = {"users": 12000, "products": 800, "orders": 45000, "events": 320000}
    growth_per_day = {"users": 60, "products": 2, "orders": 500, "events": 4500}

    t = start
    while t <= now:
        days_passed = (t - start).total_seconds() / 86400
        # daily seasonality: ±10% around mean, peaks at 14:00 UTC
        hour_factor = 1 + 0.1 * math.sin((t.hour - 14) / 24 * 2 * math.pi)
        for table in TABLES:
            rows = base_rows[table] + growth_per_day[table] * days_passed
            rows *= hour_factor
            rows *= 1 + random.uniform(-0.02, 0.02)
            yield {
                "ts": t,
                "table_name": table,
                "metric_name": "row_count",
                "value": int(rows),
            }
            null_rate = max(0, random.gauss(0.03, 0.01))
            # inject a null-rate spike around day 10 for orders (demo anomaly)
            if table == "orders" and 9.5 < days_passed < 10.5:
                null_rate += 0.15
            yield {
                "ts": t,
                "table_name": table,
                "metric_name": "null_rate",
                "value": round(null_rate, 4),
                "tags": {"column": "email" if table == "users" else "discount"},
            }
        t += timedelta(minutes=INTERVAL_MINUTES)


def main() -> None:
    batch: list[dict] = []
    total = 0
    for row in _rows():
        batch.append(row)
        if len(batch) >= 1000:
            total += save_metrics(batch)
            batch.clear()
    total += save_metrics(batch)
    print(f"Seeded {total} metric rows across {len(TABLES)} tables for {DAYS} days.")


if __name__ == "__main__":
    main()
