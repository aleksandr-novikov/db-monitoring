-- Storage for time-series metrics on Postgres + TimescaleDB.
-- Mirrors metrics_schema.sql (SQLite) — same tables, columns and indexes
-- so the application code can talk to either backend through the same SQL
-- shape. Differences:
--   * TIMESTAMPTZ instead of TEXT for any timestamp column (lets us turn
--     `metrics` into a TimescaleDB hypertable).
--   * DOUBLE PRECISION instead of REAL.
--   * `notifications.id` is BIGSERIAL (vs INTEGER AUTOINCREMENT).
--   * JSON-shaped columns are kept as TEXT — the application stores
--     pre-encoded JSON strings, and TEXT keeps the read path identical
--     between dialects (no JSONB → dict auto-decode to special-case).

-- Required to make `create_hypertable` available. No-op if the extension
-- is already installed (e.g. when running on the timescale/timescaledb image).
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS metrics (
    project_id  TEXT NOT NULL,   -- tenant scope (#53). 'legacy' for pre-#53 rows.
    ts          TIMESTAMPTZ NOT NULL,
    table_name  TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    value       DOUBLE PRECISION NOT NULL,
    tags        TEXT
);

-- Convert to hypertable. Idempotent via if_not_exists. Chunk interval
-- defaults to 7 days, which matches our typical 14d/30d query windows.
SELECT create_hypertable('metrics', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_metrics_project_table_metric_ts
    ON metrics (project_id, table_name, metric_name, ts);

CREATE TABLE IF NOT EXISTS changepoints (
    project_id    TEXT NOT NULL DEFAULT 'legacy',
    ts            TIMESTAMPTZ NOT NULL,
    table_name    TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    score         DOUBLE PRECISION NOT NULL,
    value_before  DOUBLE PRECISION NOT NULL,
    value_after   DOUBLE PRECISION NOT NULL,
    detected_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, ts, table_name, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_changepoints_table_metric_ts
    ON changepoints (table_name, metric_name, ts);
CREATE INDEX IF NOT EXISTS idx_changepoints_project_ts
    ON changepoints (project_id, detected_at DESC);

CREATE TABLE IF NOT EXISTS schema_snapshots (
    table_name  TEXT NOT NULL PRIMARY KEY,
    columns     TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS schema_events (
    ts            TIMESTAMPTZ NOT NULL,
    table_name    TEXT NOT NULL,
    change_type   TEXT NOT NULL,
    column_name   TEXT NOT NULL,
    details       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schema_events_table_ts
    ON schema_events (table_name, ts);

CREATE TABLE IF NOT EXISTS anomaly_scores (
    project_id  TEXT NOT NULL DEFAULT 'legacy',
    ts          TIMESTAMPTZ NOT NULL,
    table_name  TEXT NOT NULL,
    score       DOUBLE PRECISION NOT NULL,
    is_anomaly  INTEGER NOT NULL,
    PRIMARY KEY (project_id, ts, table_name)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_scores_table_ts
    ON anomaly_scores (table_name, ts);
CREATE INDEX IF NOT EXISTS idx_anomaly_scores_project_ts
    ON anomaly_scores (project_id, ts DESC);

CREATE TABLE IF NOT EXISTS drift_reports (
    project_id  TEXT NOT NULL DEFAULT 'legacy',
    table_name  TEXT NOT NULL,
    column_name TEXT NOT NULL,
    data_type   TEXT,
    psi         DOUBLE PRECISION,
    ks_pvalue   DOUBLE PRECISION,
    is_drift    INTEGER NOT NULL,
    severity    TEXT NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_drift_reports_table
    ON drift_reports (table_name);
CREATE INDEX IF NOT EXISTS idx_drift_reports_project_ts
    ON drift_reports (project_id, computed_at DESC);

CREATE TABLE IF NOT EXISTS llm_explanations (
    table_name    TEXT NOT NULL,
    metric        TEXT NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    explanation   TEXT NOT NULL,
    suggested_fix TEXT NOT NULL,
    confidence    DOUBLE PRECISION NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (table_name, metric, ts)
);

CREATE TABLE IF NOT EXISTS telegram_throttle (
    project_id   TEXT NOT NULL DEFAULT 'legacy',
    table_name   TEXT NOT NULL,
    event_key    TEXT NOT NULL,
    last_sent_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (project_id, table_name, event_key)
);

CREATE TABLE IF NOT EXISTS notifications (
    id          BIGSERIAL PRIMARY KEY,
    ts          TIMESTAMPTZ NOT NULL,
    project_id  TEXT NOT NULL DEFAULT 'legacy',
    event_type  TEXT NOT NULL,
    table_name  TEXT,
    metric_name TEXT,
    message     TEXT NOT NULL,
    status      TEXT NOT NULL,
    error       TEXT,
    chat_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_notifications_ts
    ON notifications (ts DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_project_ts
    ON notifications (project_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_event_type_ts
    ON notifications (event_type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_table_ts
    ON notifications (table_name, ts DESC);

-- Users (#49). Mirrors the SQLite definition (id as TEXT to keep the
-- application code dialect-agnostic — `app.auth` generates UUIDs via
-- `uuid.uuid4().hex` regardless of backend). created_at / last_login_at
-- are TIMESTAMPTZ on Postgres (matches the rest of this schema).
CREATE TABLE IF NOT EXISTS users (
    id             TEXT NOT NULL PRIMARY KEY,
    email          TEXT NOT NULL UNIQUE,
    password_hash  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    last_login_at  TIMESTAMPTZ,
    -- Belt-and-braces защита поверх _normalize_email в app/auth.py — см.
    -- комментарий в metrics_schema.sql.
    CHECK (email = LOWER(email))
);

CREATE TABLE IF NOT EXISTS projects (
    id          TEXT NOT NULL PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    UNIQUE (user_id, slug),
    -- Mirror the slug CHECK from metrics_schema.sql — see the comment
    -- there.
    CHECK (slug = LOWER(slug))
);

CREATE TABLE IF NOT EXISTS connections (
    id               TEXT NOT NULL PRIMARY KEY,
    project_id       TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    dsn_encrypted    BYTEA NOT NULL,
    schema_name      TEXT NOT NULL DEFAULT 'public',
    interval_minutes INTEGER NOT NULL DEFAULT 15,
    is_active        INTEGER NOT NULL DEFAULT 1,
    created_at       TIMESTAMPTZ NOT NULL,
    CHECK (interval_minutes BETWEEN 5 AND 1440)
);

CREATE INDEX IF NOT EXISTS idx_connections_project ON connections (project_id);

CREATE TABLE IF NOT EXISTS failed_login_attempts (
    email        TEXT NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL,
    -- Mirrors the CHECK on users.email — see comment in metrics_schema.sql.
    CHECK (email = LOWER(email))
);

CREATE INDEX IF NOT EXISTS idx_failed_login_email_ts
    ON failed_login_attempts (email, attempted_at);
CREATE INDEX IF NOT EXISTS idx_failed_login_attempted_at
    ON failed_login_attempts (attempted_at);

CREATE TABLE IF NOT EXISTS project_notifications (
    project_id            TEXT NOT NULL PRIMARY KEY
                          REFERENCES projects(id) ON DELETE CASCADE,
    telegram_bot_token    BYTEA,
    telegram_chat_id      TEXT,
    throttle_minutes      INTEGER NOT NULL DEFAULT 30
                          CHECK (throttle_minutes BETWEEN 1 AND 1440),
    updated_at            TIMESTAMPTZ NOT NULL
);
