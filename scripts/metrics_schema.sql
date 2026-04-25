-- Storage for time-series metrics collected by the monitor.
-- SQLite for MVP; post-MVP migration path is Postgres + TimescaleDB.

CREATE TABLE IF NOT EXISTS metrics (
    ts          TEXT NOT NULL,   -- ISO 8601 UTC, e.g. "2026-04-22T07:30:00"
    table_name  TEXT NOT NULL,
    metric_name TEXT NOT NULL,   -- row_count, size_bytes, null_rate, ...
    value       REAL NOT NULL,
    tags        TEXT             -- optional JSON: {"column": "email", ...}
);

CREATE INDEX IF NOT EXISTS idx_metrics_table_ts  ON metrics (table_name, ts);
CREATE INDEX IF NOT EXISTS idx_metrics_metric_ts ON metrics (metric_name, ts);
