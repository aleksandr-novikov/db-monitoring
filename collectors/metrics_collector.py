import logging
from datetime import UTC, datetime

from app import db

logger = logging.getLogger(__name__)

# #233: TABLESAMPLE percent for "sample" mode. Constant for now — exposed
# as a knob if real workloads need tuning, but 1% has been the standard
# safe default on prod-grade Postgres (multi-GB pages).
_SAMPLE_PERCENT = 1.0


class MetricsCollector:
    def __init__(
        self,
        schema: str | None = None,
        collection_mode: str = "full",
    ):
        """*collection_mode* (#233) ∈ {'full', 'sample', 'approx'}. Caller
        is responsible for downgrading non-Postgres connections to 'full'
        — by the time this runs, the mode is assumed dialect-compatible."""
        self.schema = schema
        self.collection_mode = collection_mode if collection_mode in (
            "full", "sample", "approx",
        ) else "full"

    def collect(self, table_name: str, ts: datetime | None = None) -> list[dict]:
        ts = ts or datetime.now(UTC)
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

        if self.collection_mode == "sample":
            rows.extend(self._collect_null_rate_sample(table_name, ts))
        elif self.collection_mode == "approx":
            rows.extend(self._collect_null_rate_approx(table_name, ts))
        else:
            rows.extend(self._collect_null_rate_full(table_name, ts))

        return rows

    # ------ mode-specific null/distribution paths -------------------------

    def _collect_null_rate_full(
        self, table_name: str, ts: datetime,
    ) -> list[dict]:
        rows: list[dict] = []
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

        try:
            distributions = db.column_distribution(table_name, schema=self.schema)
        except Exception as exc:
            logger.error(
                "Failed to collect column distributions for table %s: %s",
                table_name, exc,
            )
            distributions = []

        for dist in distributions:
            rows.append({
                "ts": ts,
                "table_name": table_name,
                "metric_name": "column_distribution",
                "value": float(dist["total"]),
                "tags": {
                    "column": dist["column"],
                    "data_type": dist["data_type"],
                    "buckets": dist["buckets"],
                },
            })

        return rows

    def _collect_null_rate_sample(
        self, table_name: str, ts: datetime,
    ) -> list[dict]:
        """#233: TABLESAMPLE SYSTEM(percent). No null_count, no distribution.

        Single avg null_rate per table — same shape as ``full`` mode so the
        dashboard renderer doesn't need to branch on cardinality, just on
        the ``source`` tag.
        """
        adapter = db.get_adapter()
        try:
            per_col, sample_size = adapter.column_nulls_sample(
                table_name, self.schema, percent=_SAMPLE_PERCENT,
            )
        except Exception as exc:
            logger.error(
                "Failed to collect sample null stats for %s: %s",
                table_name, exc,
            )
            return []
        if sample_size == 0:
            logger.warning(
                "sample returned 0 rows for %s, skipping null_rate",
                table_name,
            )
            return []
        if not per_col:
            return []
        avg_rate = sum(c["null_rate"] for c in per_col) / len(per_col)
        return [{
            "ts": ts,
            "table_name": table_name,
            "metric_name": "null_rate",
            "value": round(avg_rate, 4),
            "tags": {
                "source": "sample",
                "sample_size": sample_size,
                "sample_percent": _SAMPLE_PERCENT,
            },
        }]

    def _collect_null_rate_approx(
        self, table_name: str, ts: datetime,
    ) -> list[dict]:
        """#233: read null_frac from pg_stats. Zero data scan; may be stale
        (data since last ANALYZE not reflected)."""
        adapter = db.get_adapter()
        try:
            per_col = adapter.column_nulls_approx(table_name, self.schema)
        except Exception as exc:
            logger.error(
                "Failed to collect approx null stats for %s: %s",
                table_name, exc,
            )
            return []
        if not per_col:
            logger.warning(
                "no pg_stats for %s, skipping approx null_rate", table_name,
            )
            return []
        avg_rate = sum(c["null_rate"] for c in per_col) / len(per_col)
        return [{
            "ts": ts,
            "table_name": table_name,
            "metric_name": "null_rate",
            "value": round(avg_rate, 4),
            "tags": {"source": "approx"},
        }]


def _to_epoch(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None
