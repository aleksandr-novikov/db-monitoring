-- Storage for time-series metrics collected by the monitor.
-- SQLite for MVP; post-MVP migration path is Postgres + TimescaleDB.

CREATE TABLE IF NOT EXISTS metrics (
    project_id  TEXT NOT NULL,   -- tenant scope (#53). 'legacy' for pre-#53 rows.
    ts          TEXT NOT NULL,   -- ISO 8601 UTC, e.g. "2026-04-22T07:30:00"
    table_name  TEXT NOT NULL,
    metric_name TEXT NOT NULL,   -- row_count, size_bytes, null_rate, ...
    value       REAL NOT NULL,
    tags        TEXT             -- optional JSON: {"column": "email", ...}
);

-- Primary read path: WHERE project_id=? AND table_name=? AND metric_name=? AND ts>=?
-- Leading project_id partitions the index cleanly per tenant.
CREATE INDEX IF NOT EXISTS idx_metrics_project_table_metric_ts
    ON metrics (project_id, table_name, metric_name, ts);

-- Detected change-points (PELT/RBF) — written by the hourly detection job.
CREATE TABLE IF NOT EXISTS changepoints (
    project_id    TEXT NOT NULL DEFAULT 'legacy',
    ts            TEXT NOT NULL,   -- ISO 8601 UTC of the detected breakpoint
    table_name    TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    score         REAL NOT NULL,   -- normalised severity (mean shift / pre-std)
    value_before  REAL NOT NULL,
    value_after   REAL NOT NULL,
    detected_at   TEXT NOT NULL,
    PRIMARY KEY (project_id, ts, table_name, metric_name)
);

CREATE INDEX IF NOT EXISTS idx_changepoints_table_metric_ts
    ON changepoints (table_name, metric_name, ts);
CREATE INDEX IF NOT EXISTS idx_changepoints_project_ts
    ON changepoints (project_id, detected_at DESC);

-- Latest known column-list per table — written by the schema collector
-- after every successful snapshot. Stored as JSON so we don't have to
-- evolve a relational column list every time the source schema changes.
CREATE TABLE IF NOT EXISTS schema_snapshots (
    project_id  TEXT NOT NULL DEFAULT 'legacy',
    table_name  TEXT NOT NULL,
    columns     TEXT NOT NULL,    -- JSON: [{"name", "type", "nullable"}, ...]
    captured_at TEXT NOT NULL,
    PRIMARY KEY (project_id, table_name)
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
    details       TEXT NOT NULL,    -- JSON: {"before": {...}, "after": {...}}
    project_id    TEXT NOT NULL DEFAULT 'legacy'
);

CREATE INDEX IF NOT EXISTS idx_schema_events_table_ts
    ON schema_events (table_name, ts);

CREATE INDEX IF NOT EXISTS idx_schema_events_project_table_ts
    ON schema_events (project_id, table_name, ts);

-- Anomaly scores from Isolation Forest — written by the collect tick and
-- the nightly retrain job. One row per (ts, table); upsert on re-run.
CREATE TABLE IF NOT EXISTS anomaly_scores (
    project_id  TEXT NOT NULL DEFAULT 'legacy',
    ts          TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    score       REAL NOT NULL,   -- raw decision_function value; < 0 means anomaly
    is_anomaly  INTEGER NOT NULL, -- 1 if anomaly, 0 otherwise
    PRIMARY KEY (project_id, ts, table_name)
);

CREATE INDEX IF NOT EXISTS idx_anomaly_scores_table_ts
    ON anomaly_scores (table_name, ts);
CREATE INDEX IF NOT EXISTS idx_anomaly_scores_project_ts
    ON anomaly_scores (project_id, ts DESC);

-- Кешированный отчёт drift по каждой (таблица, колонка). Перезаписывается
-- целиком при пересчёте — на странице /schema читаем отсюда, а не считаем
-- заново на каждый запрос. Обновляется warmup_ml + sweep'ом коллектора.
CREATE TABLE IF NOT EXISTS drift_reports (
    project_id  TEXT NOT NULL DEFAULT 'legacy',
    table_name  TEXT NOT NULL,
    column_name TEXT NOT NULL,
    data_type   TEXT,
    psi         REAL,
    ks_pvalue   REAL,
    is_drift    INTEGER NOT NULL,
    severity    TEXT NOT NULL,
    computed_at TEXT NOT NULL,
    PRIMARY KEY (project_id, table_name, column_name)
);

CREATE INDEX IF NOT EXISTS idx_drift_reports_table
    ON drift_reports (table_name);
CREATE INDEX IF NOT EXISTS idx_drift_reports_project_ts
    ON drift_reports (project_id, computed_at DESC);

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

-- Throttle table for Telegram notifications (#143 multi-tenant).
-- Prevents more than 1 message per (project_id, table_name, event_key) per
-- the project's throttle_minutes window. Throttle is per-tenant — one user
-- spamming alerts must not suppress another user's first notification.
-- event_key examples: "anomaly_row_count", "schema_drift", "changepoint_null_rate"
CREATE TABLE IF NOT EXISTS telegram_throttle (
    project_id   TEXT NOT NULL DEFAULT 'legacy',
    table_name   TEXT NOT NULL,
    event_key    TEXT NOT NULL,
    last_sent_at TEXT NOT NULL,
    PRIMARY KEY (project_id, table_name, event_key)
);

-- История отправленных Telegram-уведомлений (#76). Пишется на каждый
-- вызов notify_*, в т.ч. при ошибке доставки (status='failed' + error).
-- Bot token и прочие секреты сюда не попадают по дизайну.
CREATE TABLE IF NOT EXISTS notifications (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,    -- ISO 8601 UTC, момент попытки отправки
    project_id  TEXT NOT NULL DEFAULT 'legacy',  -- tenant scope (#137)
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
CREATE INDEX IF NOT EXISTS idx_notifications_project_ts
    ON notifications (project_id, ts DESC);
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
    -- #220: системный администратор. 1 = доступ к /admin/*. Назначается
    -- через settings.ADMIN_EMAIL при старте app, нет UI промоушена.
    is_admin       INTEGER NOT NULL DEFAULT 0,
    -- Belt-and-braces защита поверх _normalize_email в app/auth.py:
    -- любой raw INSERT мимо нормализации (сидеры, ручной SQL) валится здесь,
    -- а не молча создаёт дубликат, который потом не находится по email.
    CHECK (email = LOWER(email))
);

-- Projects (#50). Каждый юзер видит ТОЛЬКО свои проекты (фильтр по
-- user_id во всех запросах + UNIQUE(user_id, slug)). Slug per-user, чтобы
-- два юзера могли независимо назвать свой проект "default".
--
-- UNIQUE(user_id, slug) автоматически создаёт композитный индекс с
-- ведущим user_id — этого достаточно и для list_projects_for_user
-- (WHERE user_id=?), и для get_project_by_slug (WHERE user_id=? AND slug=?).
-- Отдельный индекс по user_id был бы дублированием.
CREATE TABLE IF NOT EXISTS projects (
    id          TEXT NOT NULL PRIMARY KEY,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (user_id, slug),
    -- Belt-and-braces защита поверх .lower() в app/projects.py — ловит
    -- любой raw INSERT мимо нормализации (см. зеркальный CHECK на
    -- users.email и failed_login_attempts.email).
    CHECK (slug = LOWER(slug))
);

-- Project membership (#172). Допускает несколько юзеров на один проект
-- с указанием роли. Owner — автор, создаётся автоматически при
-- create_project. Editor/viewer добавляются вручную через
-- add_project_member.
--
-- Composite PK (project_id, user_id): один пользователь — одна роль
-- на проект. Чтобы перевести из viewer в editor, делаем UPSERT.
-- FK CASCADE на projects: удаление проекта чистит membership.
-- FK CASCADE на users: удаление аккаунта чистит membership.
CREATE TABLE IF NOT EXISTS project_members (
    project_id  TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    user_id     TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role        TEXT NOT NULL CHECK (role IN ('owner', 'editor', 'viewer')),
    joined_at   TEXT NOT NULL,
    PRIMARY KEY (project_id, user_id)
);

-- Поиск "какие проекты доступны юзеру" — основной запрос на каждой
-- странице (header switcher, /projects/). Композитный PK выше
-- начинается с project_id, поэтому WHERE user_id=? делает full scan
-- без отдельного индекса.
CREATE INDEX IF NOT EXISTS idx_project_members_user
    ON project_members (user_id);

-- DB connections (#51). Каждый коннект принадлежит проекту (FK с CASCADE).
-- dsn_encrypted — Fernet ciphertext, BLOB, plaintext НИКОГДА не хранится.
-- interval_minutes — частота сбора метрик per-connection, 5..1440 минут.
CREATE TABLE IF NOT EXISTS connections (
    id               TEXT NOT NULL PRIMARY KEY,
    project_id       TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    dsn_encrypted    BLOB NOT NULL,
    schema_name      TEXT NOT NULL DEFAULT 'public',
    interval_minutes INTEGER NOT NULL DEFAULT 15,
    is_active        INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL,
    -- #232 load-safety knobs. table_allowlist / table_denylist хранятся
    -- как JSON-массив строк ('["users","orders"]'); NULL = «не задано».
    -- max_tables_per_tick — hard cap на число обработанных таблиц за тик,
    -- применяется ПОСЛЕ allow/denylist. skip_tables_larger_than_gb и
    -- statement_timeout_ms — только Postgres; для ClickHouse/Iceberg
    -- игнорируются (см. collectors/per_project.py).
    table_allowlist            TEXT,
    table_denylist             TEXT,
    max_tables_per_tick        INTEGER DEFAULT 50,
    skip_tables_larger_than_gb REAL,
    statement_timeout_ms       INTEGER DEFAULT 30000,
    -- #234 Iceberg production params. namespace/warehouse — plain TEXT,
    -- auth token шифруется отдельно через crypto.encrypt_token (НЕ
    -- encrypt_dsn — разные lifecycles для DSN-rotation и token-rotation).
    iceberg_namespace             TEXT,
    iceberg_warehouse             TEXT,
    iceberg_auth_token_encrypted  BLOB,
    -- #233 collection_mode ∈ {'full','sample','approx'}. NULL/'full'
    -- сохраняют поведение до миграции; sample/approx работают только
    -- для Postgres, для ClickHouse/Iceberg downgrade на 'full' c warning.
    collection_mode TEXT DEFAULT 'full',
    CHECK (interval_minutes BETWEEN 5 AND 1440)
);

CREATE INDEX IF NOT EXISTS idx_connections_project ON connections (project_id);

-- Persistent collector run log (#239). UI reads this in #240 to explain
-- what happened during each scheduled/manual collection tick.
CREATE TABLE IF NOT EXISTS collector_runs (
    id                TEXT PRIMARY KEY,
    project_id        TEXT NOT NULL,
    connection_id     TEXT NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    status            TEXT NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'success', 'warning', 'failed', 'skipped')),
    mode              TEXT NOT NULL DEFAULT 'scheduled'
                      CHECK (mode IN ('full', 'manual', 'scheduled')),
    tables_total      INTEGER DEFAULT 0,
    tables_checked    INTEGER DEFAULT 0,
    tables_skipped    INTEGER DEFAULT 0,
    metrics_collected INTEGER DEFAULT 0,
    duration_ms       INTEGER,
    error_message     TEXT
);

CREATE TABLE IF NOT EXISTS collector_run_tables (
    id                TEXT PRIMARY KEY,
    run_id            TEXT NOT NULL REFERENCES collector_runs(id) ON DELETE CASCADE,
    table_name        TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('success', 'skipped', 'failed')),
    metrics_collected INTEGER DEFAULT 0,
    rows_observed     INTEGER,
    duration_ms       INTEGER,
    skip_reason       TEXT CHECK (
                          skip_reason IS NULL OR skip_reason IN (
                              'denylisted', 'not_in_allowlist', 'too_large',
                              'timeout', 'max_tables_limit'
                          )
                      ),
    error_message     TEXT
);

CREATE INDEX IF NOT EXISTS idx_collector_runs_conn
    ON collector_runs (connection_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_collector_runs_project_started
    ON collector_runs (project_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_collector_run_tables_run
    ON collector_run_tables (run_id);

-- Failed login attempts (#56). Используется для per-email lockout после
-- 5 неуспешных попыток в окне 15 минут. Append-only лог: успешный логин
-- ничего не пишет, очистка — purge_old_failed_logins по retention.
CREATE TABLE IF NOT EXISTS failed_login_attempts (
    email        TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    -- Зеркалит CHECK на users.email — нормализация на app-уровне
    -- (_normalize_email), CHECK ловит raw INSERT мимо неё.
    CHECK (email = LOWER(email))
);

-- (email, attempted_at) поддерживает lockout-проверку
-- (count_recent_failed_logins). (attempted_at) — purge_old_failed_logins
-- (DELETE WHERE attempted_at < cutoff), без него full scan.
CREATE INDEX IF NOT EXISTS idx_failed_login_email_ts
    ON failed_login_attempts (email, attempted_at);
CREATE INDEX IF NOT EXISTS idx_failed_login_attempted_at
    ON failed_login_attempts (attempted_at);

-- Password reset tokens (#133).
-- Хранится ТОЛЬКО HMAC-SHA256(SECRET_KEY, raw_token) — raw token уходит
-- юзеру по email и в БД никогда не попадает. Leak метрик-БД даёт хеши,
-- но не позволяет восстановить токены (без знания SECRET_KEY). HMAC, не
-- голый SHA256, чтобы offline-перебор по словарю популярных uuid4
-- значений был бесполезен.
-- expires_at — 1 час с момента создания.
-- used_at — NULL пока токен не использован; невозможность повторного
-- использования обеспечивается атомарным UPDATE с условием used_at IS NULL.
CREATE TABLE IF NOT EXISTS password_reset_tokens (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash   TEXT NOT NULL UNIQUE,
    expires_at   TEXT NOT NULL,
    used_at      TEXT,
    created_at   TEXT NOT NULL
);

-- (user_id) поддерживает invalidate-all-tokens-for-user при создании
-- нового запроса и при успешном сбросе пароля. (expires_at) — для
-- batch cleanup просроченных токенов.
CREATE INDEX IF NOT EXISTS idx_password_reset_user
    ON password_reset_tokens (user_id);
CREATE INDEX IF NOT EXISTS idx_password_reset_expires
    ON password_reset_tokens (expires_at);

-- Project invite tokens (#222).
-- Owner генерирует токен → отдаёт коллеге → тот переходит по ссылке
-- /invite/<token> → membership row создаётся в project_members.
-- token хранится как hex (32 байта = 64-char hex от secrets.token_hex(32));
-- одноразовый (used_at IS NULL → используем атомарным UPDATE);
-- TTL 7 дней (expires_at = created_at + 7d).
-- FK CASCADE на projects: удалили проект — токены ушли.
-- FK CASCADE на users (created_by): удалили автора — приглашения тоже.
-- В отличие от password_reset_tokens, токен здесь хранится "в plain"
-- (не HMAC): он сам по себе access grant в один-единственный проект,
-- leak метрики-БД даёт уже доступ к гораздо большему.
CREATE TABLE IF NOT EXISTS project_invites (
    token        TEXT NOT NULL PRIMARY KEY,
    project_id   TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    role         TEXT NOT NULL CHECK (role IN ('editor', 'viewer')),
    created_by   TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    used_at      TEXT
);

CREATE INDEX IF NOT EXISTS idx_project_invites_project
    ON project_invites (project_id);
CREATE INDEX IF NOT EXISTS idx_project_invites_expires
    ON project_invites (expires_at);

-- Per-project Telegram notification settings (#143).
-- Bot token хранится Fernet-зашифрованным (та же схема что connections.dsn_encrypted)
-- — leak metrics-DB файла недостаточен чтобы заполучить токен.
-- PRIMARY KEY = project_id (1-to-1 — один Telegram-конфиг на проект).
-- NULLABLE telegram_bot_token / telegram_chat_id означают «настройка
-- частично заполнена» — отправка не происходит пока оба не заданы.
-- НЕТ глобального fallback: если у проекта нет записи / не заполнено —
-- уведомления молча скипаются (защита от cross-tenant leak).
CREATE TABLE IF NOT EXISTS project_notifications (
    project_id            TEXT NOT NULL PRIMARY KEY
                          REFERENCES projects(id) ON DELETE CASCADE,
    telegram_bot_token    BLOB,
    telegram_chat_id      TEXT,
    throttle_minutes      INTEGER NOT NULL DEFAULT 30
                          CHECK (throttle_minutes BETWEEN 1 AND 1440),
    updated_at            TEXT NOT NULL
);
