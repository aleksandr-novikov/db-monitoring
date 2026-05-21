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

-- Кешированный отчёт drift по каждой (таблица, колонка). Перезаписывается
-- целиком при пересчёте — на странице /schema читаем отсюда, а не считаем
-- заново на каждый запрос. Обновляется warmup_ml + sweep'ом коллектора.
CREATE TABLE IF NOT EXISTS drift_reports (
    table_name  TEXT NOT NULL,
    column_name TEXT NOT NULL,
    data_type   TEXT,
    psi         REAL,
    ks_pvalue   REAL,
    is_drift    INTEGER NOT NULL,
    severity    TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    PRIMARY KEY (table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_drift_reports_table
    ON drift_reports (table_name);

-- LLM-generated root-cause explanations — cached to avoid repeated NIM calls.
-- TTL is 24 h, checked at read time via created_at.
CREATE TABLE IF NOT EXISTS llm_explanations (
    table_name    TEXT NOT NULL,
    metric        TEXT NOT NULL,
    ts            TEXT NOT NULL,
    explanation   TEXT NOT NULL,
    suggested_fix TEXT NOT NULL,
    confidence    REAL NOT NULL,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (table_name, metric, ts)
);

-- Throttle table for Telegram notifications.
-- Prevents more than 1 message per (table_name, event_key) per TELEGRAM_THROTTLE_MINUTES.
-- event_key examples: "anomaly_row_count", "schema_drift", "changepoint_null_rate"
CREATE TABLE IF NOT EXISTS telegram_throttle (
    table_name   TEXT NOT NULL,
    event_key    TEXT NOT NULL,
    last_sent_at TEXT NOT NULL,
    PRIMARY KEY (table_name, event_key)
);

-- История отправленных Telegram-уведомлений (#76). Пишется на каждый
-- вызов notify_*, в т.ч. при ошибке доставки (status='failed' + error).
-- Bot token и прочие секреты сюда не попадают по дизайну.
CREATE TABLE IF NOT EXISTS notifications (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,    -- ISO 8601 UTC, момент попытки отправки
    event_type  TEXT NOT NULL,    -- anomaly | schema_drift | changepoint | forecast | root_cause
    table_name  TEXT,
    metric_name TEXT,
    message     TEXT NOT NULL,
    status      TEXT NOT NULL,    -- sent | failed
    error       TEXT,
    chat_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_notifications_ts
    ON notifications (ts DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_event_type_ts
    ON notifications (event_type, ts DESC);
CREATE INDEX IF NOT EXISTS idx_notifications_table_ts
    ON notifications (table_name, ts DESC);

-- Users (#49). Часть Sprint 3 multi-tenant эпика.
-- id — uuid4 как TEXT (Python uuid.uuid4().hex), email хранится lower-case
-- (нормализация на уровне приложения), password_hash — werkzeug
-- `pbkdf2:sha256` или `scrypt` (выбирается werkzeug на основе версии).
-- created_at / last_login_at — ISO 8601 UTC, как везде в этой схеме.
CREATE TABLE IF NOT EXISTS users (
    id             TEXT NOT NULL PRIMARY KEY,
    email          TEXT NOT NULL UNIQUE,
    password_hash  TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    last_login_at  TEXT,
    -- Belt-and-braces защита поверх _normalize_email в app/auth.py:
    -- любой raw INSERT мимо нормализации (сидеры, ручной SQL) валится здесь,
    -- а не молча создаёт дубликат, который потом не находится по email.
    CHECK (email = LOWER(email))
);
