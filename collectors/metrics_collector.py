import logging
from datetime import datetime, timezone

from app import db

logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, schema: str | None = None):
        self.schema = schema

    def collect(self, table_name: str, ts: datetime | None = None) -> list[dict]:
        ts = ts or datetime.now(timezone.utc)
        rows = []

        try:
            stats = db.table_stats(table_name, schema=self.schema)
        except Exception as exc:
            logger.error("Failed to collect stats for table %s: %s", table_name, exc)
            return []

        if stats is None:
            logger.warning("Table %s not found in schema %s, skipping", table_name, self.schema)
            return []

        rows.append({"ts": ts, "table_name": table_name, "metric_name": "row_count", "value": stats["row_count"]})
        rows.append({"ts": ts, "table_name": table_name, "metric_name": "size_bytes", "value": stats["size_bytes"]})

        last_modified = _to_epoch(stats.get("last_analyze"))
        if last_modified is not None:
            rows.append({"ts": ts, "table_name": table_name, "metric_name": "last_modified", "value": last_modified})

        try:
            null_stats = db.column_nulls(table_name, schema=self.schema)
        except Exception as exc:
            logger.error(
                "Failed to collect null stats for table %s: %s — "
                "returning partial snapshot (row_count/size_bytes only)",
                table_name, exc,
            )
            return rows

        for col in null_stats:
            rows.append({
                "ts": ts,
                "table_name": table_name,
                "metric_name": "null_count",
                "value": col["null_count"],
                "tags": {"column": col["column"]},
            })

        if null_stats:
            avg_rate = sum(c["null_rate"] for c in null_stats) / len(null_stats)
            rows.append({
                "ts": ts,
                "table_name": table_name,
                "metric_name": "null_rate",
                "value": round(avg_rate, 4),
            })

        return rows


def _to_epoch(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None
