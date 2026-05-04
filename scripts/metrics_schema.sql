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

-- Detected change-points (PELT/RBF) — written by the hourly detection job.
CREATE TABLE IF NOT EXISTS changepoints (
    ts            TEXT NOT NULL,   -- ISO 8601 UTC of the detected breakpoint
    table_name    TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    score         REAL NOT NULL,   -- normalised severity (mean shift / pre-std)
    value_before  REAL NOT NULL,
    value_after   REAL NOT NULL,
    detected_at   TEXT NOT NULL,
    PRIMARY KEY (ts, table_name, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_changepoints_table_metric_ts
    ON changepoints (table_name, metric_name, ts);

-- Latest known column-list per table — written by the schema collector
-- after every successful snapshot. Stored as JSON so we don't have to
-- evolve a relational column list every time the source schema changes.
CREATE TABLE IF NOT EXISTS schema_snapshots (
    table_name  TEXT NOT NULL PRIMARY KEY,
    columns     TEXT NOT NULL,    -- JSON: [{"name", "type", "nullable"}, ...]
    captured_at TEXT NOT NULL
);

-- Detected schema-drift events (column added/removed/type changed/nullability
-- changed). One row per (table, change_type, column) per detection run; the
-- collector dedupes against the previous snapshot so the table grows only
-- when the source schema actually moves.
CREATE TABLE IF NOT EXISTS schema_events (
    ts            TEXT NOT NULL,    -- when the change was first observed
    table_name    TEXT NOT NULL,
    change_type   TEXT NOT NULL,    -- column_added | column_removed |
                                    -- type_changed | nullable_changed
    column_name   TEXT NOT NULL,
    details       TEXT NOT NULL     -- JSON: {"before": {...}, "after": {...}}
);

CREATE INDEX IF NOT EXISTS idx_schema_events_table_ts
    ON schema_events (table_name, ts);

-- Anomaly scores from Isolation Forest — written by the collect tick and
-- the nightly retrain job. One row per (ts, table); upsert on re-run.
CREATE TABLE IF NOT EXISTS anomaly_scores (
    ts          TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    score       REAL NOT NULL,   -- raw decision_function value; < 0 means anomaly
    is_anomaly  INTEGER NOT NULL, -- 1 if anomaly, 0 otherwise
    PRIMARY KEY (ts, table_name)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_scores_table_ts
    ON anomaly_scores (table_name, ts);
