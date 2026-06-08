import json
import logging
import threading
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import bindparam, create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError, ProgrammingError

from app.config import settings

logger = logging.getLogger(__name__)

_SCHEMA_DIR = Path(__file__).resolve().parent.parent / "scripts"
SQLITE_SCHEMA_PATH = _SCHEMA_DIR / "metrics_schema.sql"
TIMESCALE_SCHEMA_PATH = _SCHEMA_DIR / "timescale_schema.sql"

_engine: Engine | None = None
_engine_lock = threading.Lock()
_initialized = False


def _backend() -> str:
    """Return 'sqlite' or 'postgres' for the configured metrics store."""
    url = settings.MONITOR_DB_URL
    # `postgresql://`, `postgres://`, `postgresql+psycopg2://` all start with
    # `postgres` — single prefix check is sufficient.
    if url.startswith("postgres"):
        return "postgres"
    return "sqlite"


def _is_postgres() -> bool:
    return _backend() == "postgres"


def _new_engine() -> Engine:
    url = settings.MONITOR_DB_URL
    kwargs: dict[str, Any] = {"future": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    engine = create_engine(url, **kwargs)
    if url.startswith("sqlite"):
        # SQLite parses FK clauses but enforces them only when this PRAGMA
        # is ON, and it has to be set on every new connection. Without it,
        # `ON DELETE CASCADE` (projects → users, future #51 connections →
        # projects) silently fails to fire on the MVP backend.
        @event.listens_for(engine, "connect")
        def _enable_sqlite_fk(dbapi_connection, _record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys = ON")
            cursor.close()
    return engine


def get_engine() -> Engine:
    global _engine, _initialized
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = _new_engine()
    if not _initialized:
        with _engine_lock:
            if not _initialized:
                _apply_schema(_engine)
                _initialized = True
    return _engine


def _apply_schema(engine: Engine) -> None:
    # Run column migrations BEFORE the main loop so indexes that depend on
    # newly added columns (e.g. project_id) don't fail on existing installs.
    _migrate_existing_schema(engine)

    schema_path = TIMESCALE_SCHEMA_PATH if _is_postgres() else SQLITE_SCHEMA_PATH
    sql = schema_path.read_text()
    # Strip single-line -- comments, then split on ;. Handles both SQLite and
    # Postgres (psycopg2 does not allow multiple statements per execute()).
    # Naive `;` split — assumes no statement contains a `;` inside a string
    # literal or a `$$...$$` body. Today's schema files honour that; do not
    # add triggers / PL/pgSQL functions without revisiting this loop.
    stripped = "\n".join(
        line.split("--", 1)[0] for line in sql.splitlines()
    )
    statements = [s.strip() for s in stripped.split(";") if s.strip()]
    # Each statement gets its own transaction so a failure on the optional
    # TimescaleDB extension (plain Postgres / missing privileges) doesn't
    # poison the rest of the schema — Postgres aborts the *whole* txn on the
    # first error, so we can't share one across statements here.
    for stmt in statements:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except ProgrammingError:
            # Tolerate failures only on the two TimescaleDB-specific
            # statements (CREATE EXTENSION timescaledb / SELECT
            # create_hypertable(...)) — those are no-ops on plain Postgres
            # without the extension. Any other ProgrammingError is real and
            # must propagate.
            if _is_optional_timescale_stmt(stmt):
                continue
            raise


def _existing_columns(engine: Engine, table: str) -> set[str]:
    with engine.connect() as conn:
        if _is_postgres():
            rows = conn.execute(
                text("SELECT column_name FROM information_schema.columns "
                     "WHERE table_name = :t"),
                {"t": table},
            ).fetchall()
            return {r[0] for r in rows}
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
        return {r[1] for r in rows}


def _table_exists(engine: Engine, table: str) -> bool:
    with engine.connect() as conn:
        if _is_postgres():
            row = conn.execute(
                text("SELECT 1 FROM information_schema.tables WHERE table_name = :t"),
                {"t": table},
            ).fetchone()
        else:
            row = conn.execute(
                text("SELECT 1 FROM sqlite_master WHERE type='table' AND name=:t"),
                {"t": table},
            ).fetchone()
    return row is not None


_PROJECT_SCOPED_ML_TABLES = {
    "anomaly_scores": {
        "columns": [
            "project_id", "ts", "table_name", "score", "is_anomaly",
        ],
        "select": (
            "'legacy' AS project_id, ts, table_name, score, is_anomaly"
        ),
        "sqlite_ddl": """
            CREATE TABLE anomaly_scores__new (
                project_id  TEXT NOT NULL DEFAULT 'legacy',
                ts          TEXT NOT NULL,
                table_name  TEXT NOT NULL,
                score       REAL NOT NULL,
                is_anomaly  INTEGER NOT NULL,
                PRIMARY KEY (project_id, ts, table_name)
            )
        """,
        "postgres_pk": ["project_id", "ts", "table_name"],
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_anomaly_scores_table_ts "
            "ON anomaly_scores (table_name, ts)",
            "CREATE INDEX IF NOT EXISTS idx_anomaly_scores_project_ts "
            "ON anomaly_scores (project_id, ts DESC)",
        ],
    },
    "changepoints": {
        "columns": [
            "project_id", "ts", "table_name", "metric_name", "score",
            "value_before", "value_after", "detected_at",
        ],
        "select": (
            "'legacy' AS project_id, ts, table_name, metric_name, score, "
            "value_before, value_after, detected_at"
        ),
        "sqlite_ddl": """
            CREATE TABLE changepoints__new (
                project_id    TEXT NOT NULL DEFAULT 'legacy',
                ts            TEXT NOT NULL,
                table_name    TEXT NOT NULL,
                metric_name   TEXT NOT NULL,
                score         REAL NOT NULL,
                value_before  REAL NOT NULL,
                value_after   REAL NOT NULL,
                detected_at   TEXT NOT NULL,
                PRIMARY KEY (project_id, ts, table_name, metric_name)
            )
        """,
        "postgres_pk": ["project_id", "ts", "table_name", "metric_name"],
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_changepoints_table_metric_ts "
            "ON changepoints (table_name, metric_name, ts)",
            "CREATE INDEX IF NOT EXISTS idx_changepoints_project_ts "
            "ON changepoints (project_id, detected_at DESC)",
        ],
    },
    "drift_reports": {
        "columns": [
            "project_id", "table_name", "column_name", "data_type", "psi",
            "ks_pvalue", "is_drift", "severity", "computed_at",
        ],
        "select": (
            "'legacy' AS project_id, table_name, column_name, data_type, psi, "
            "ks_pvalue, is_drift, severity, computed_at"
        ),
        "sqlite_ddl": """
            CREATE TABLE drift_reports__new (
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
            )
        """,
        "postgres_pk": ["project_id", "table_name", "column_name"],
        "indexes": [
            "CREATE INDEX IF NOT EXISTS idx_drift_reports_table "
            "ON drift_reports (table_name)",
            "CREATE INDEX IF NOT EXISTS idx_drift_reports_project_ts "
            "ON drift_reports (project_id, computed_at DESC)",
        ],
    },
}


def _sqlite_pk_columns(engine: Engine, table: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table})")).fetchall()
    return [r[1] for r in sorted((r for r in rows if r[5]), key=lambda r: r[5])]


def _postgres_pk_columns(engine: Engine, table: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a
              ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = CAST(:table_name AS regclass)
              AND i.indisprimary
            ORDER BY array_position(i.indkey, a.attnum)
        """), {"table_name": table}).fetchall()
    return [r[0] for r in rows]


def _postgres_pk_constraint(engine: Engine, table: str) -> str | None:
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = CAST(:table_name AS regclass)
              AND contype = 'p'
        """), {"table_name": table}).fetchone()
    return row[0] if row else None


def _migrate_sqlite_project_scoped_ml_table(
    engine: Engine, table: str, spec: dict
) -> None:
    desired_pk = spec["postgres_pk"]
    if (
        "project_id" in _existing_columns(engine, table)
        and _sqlite_pk_columns(engine, table) == desired_pk
    ):
        return
    columns = ", ".join(spec["columns"])
    has_project_id = "project_id" in _existing_columns(engine, table)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table}__new"))
        conn.execute(text(spec["sqlite_ddl"]))
        if has_project_id:
            select_cols = columns
        else:
            select_cols = spec["select"]
        conn.execute(text(
            f"INSERT INTO {table}__new ({columns}) "
            f"SELECT {select_cols} FROM {table}"
        ))
        conn.execute(text(f"DROP TABLE {table}"))
        conn.execute(text(f"ALTER TABLE {table}__new RENAME TO {table}"))
        for index_sql in spec["indexes"]:
            conn.execute(text(index_sql))
    logger.info("%s migrated to project-scoped primary key", table)


def _migrate_postgres_project_scoped_ml_table(
    engine: Engine, table: str, spec: dict
) -> None:
    desired_pk = spec["postgres_pk"]
    columns = _existing_columns(engine, table)
    pk_columns = _postgres_pk_columns(engine, table)
    if "project_id" in columns and pk_columns == desired_pk:
        return
    constraint = _postgres_pk_constraint(engine, table)

    with engine.begin() as conn:
        if "project_id" not in columns:
            conn.execute(text(
                f"ALTER TABLE {table} ADD COLUMN project_id TEXT NOT NULL "
                "DEFAULT 'legacy'"
            ))
        if constraint:
            conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT {constraint}"))
        conn.execute(text(
            f"ALTER TABLE {table} ADD PRIMARY KEY ({', '.join(desired_pk)})"
        ))
        for index_sql in spec["indexes"]:
            conn.execute(text(index_sql))
    logger.info("%s migrated to project-scoped primary key", table)


def _migrate_project_scoped_ml_tables(engine: Engine) -> None:
    for table, spec in _PROJECT_SCOPED_ML_TABLES.items():
        if not _table_exists(engine, table):
            continue
        if _is_postgres():
            _migrate_postgres_project_scoped_ml_table(engine, table, spec)
        else:
            _migrate_sqlite_project_scoped_ml_table(engine, table, spec)


def _migrate_existing_schema(engine: Engine) -> None:
    """ALTER pre-#53/#137 tables to match the current schema file.

    The CREATE TABLE statements are no-ops on existing installs (IF NOT EXISTS),
    so columns added after initial deploy need explicit ALTER here. Each table
    is checked independently — a partial DB (e.g. notifications without metrics)
    still gets migrated correctly.
    """
    if _table_exists(engine, "metrics") and "project_id" not in _existing_columns(engine, "metrics"):
        with engine.begin() as conn:
            # NOT NULL + DEFAULT works on SQLite (>=3.3) and Postgres; the
            # default backfills existing rows with the 'legacy' tenant id.
            conn.execute(text(
                "ALTER TABLE metrics ADD COLUMN project_id TEXT NOT NULL "
                "DEFAULT 'legacy'"
            ))
        logger.info(
            "metrics.project_id added (existing rows backfilled to 'legacy')"
        )

    if _table_exists(engine, "schema_events") and "project_id" not in _existing_columns(engine, "schema_events"):
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE schema_events ADD COLUMN project_id TEXT NOT NULL "
                "DEFAULT 'legacy'"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_schema_events_project_table_ts "
                "ON schema_events (project_id, table_name, ts)"
            ))
        logger.info(
            "schema_events.project_id added (existing rows backfilled to 'legacy')"
        )

    if _table_exists(engine, "notifications") and "project_id" not in _existing_columns(engine, "notifications"):
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE notifications ADD COLUMN project_id TEXT NOT NULL "
                "DEFAULT 'legacy'"
            ))
            # Add index in the same transaction so existing DBs get it too.
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS idx_notifications_project_ts "
                "ON notifications (project_id, ts DESC)"
            ))
        logger.info(
            "notifications.project_id added (existing rows backfilled to 'legacy')"
        )

    # #197: schema_snapshots gets project_id in the primary key. The table is
    # a cache (the next collection tick re-populates it); dropping it is safe.
    # Side effect: one tick without snapshot baseline — diff_schemas returns []
    # when before=None, so no false-positive events fire.
    if _table_exists(engine, "schema_snapshots") and "project_id" not in _existing_columns(engine, "schema_snapshots"):
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE schema_snapshots"))
        logger.info(
            "schema_snapshots dropped for multi-tenant migration "
            "(snapshot cache reset; recreated below with project_id in PK)"
        )

    # #220: users gets is_admin column. Дефолт 0 — никто не получает доступ
    # к /admin/* без явного промоушена через ADMIN_EMAIL.
    if _table_exists(engine, "users") and "is_admin" not in _existing_columns(engine, "users"):
        with engine.begin() as conn:
            conn.execute(text(
                "ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0"
            ))
        logger.info("users.is_admin added (existing rows default to 0)")

    # #143: telegram_throttle gets project_id in the primary key. The table
    # is an ephemeral cache (throttle window is typically tens of minutes),
    # so we don't try to do a careful in-place ALTER PRIMARY KEY (which
    # SQLite doesn't even support). Drop + let the schema file recreate it
    # with the new structure. Side effect: throttle cache is reset once
    # (existing throttle rows are discarded — at most one extra notification
    # per (table, event_key) may fire right after deploy).
    if _table_exists(engine, "telegram_throttle") and "project_id" not in _existing_columns(engine, "telegram_throttle"):
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE telegram_throttle"))
        logger.info(
            "telegram_throttle dropped for multi-tenant migration "
            "(throttle cache reset; recreated below with project_id in PK)"
        )

    # #232: connections gets per-connection load-safety knobs. All five columns
    # are NULL-default (or sensible defaults) so existing rows preserve the
    # pre-#232 behavior on next boot. SQLite/Postgres both honor a plain
    # ALTER TABLE … ADD COLUMN here.
    if _table_exists(engine, "connections"):
        present = _existing_columns(engine, "connections")
        _safety_columns = [
            ("table_allowlist", "TEXT"),
            ("table_denylist", "TEXT"),
            ("max_tables_per_tick", "INTEGER DEFAULT 50"),
            ("skip_tables_larger_than_gb", "REAL"),
            ("statement_timeout_ms", "INTEGER DEFAULT 30000"),
        ]
        for col, ddl in _safety_columns:
            if col not in present:
                with engine.begin() as conn:
                    conn.execute(text(
                        f"ALTER TABLE connections ADD COLUMN {col} {ddl}"
                    ))
                logger.info("connections.%s added (#232 load safety)", col)

    _migrate_project_scoped_ml_tables(engine)

    # #172: backfill project_members for projects that existed before
    # shared-access landed. Every legacy project's user_id becomes the
    # owner row. Idempotent: subsequent boots find the row already there
    # and skip. Runs AFTER the schema CREATE so the table exists.
    if _table_exists(engine, "projects") and _table_exists(engine, "project_members"):
        with engine.begin() as conn:
            existing = conn.execute(text("""
                SELECT p.id, p.user_id FROM projects p
                LEFT JOIN project_members m
                  ON m.project_id = p.id AND m.user_id = p.user_id
                WHERE m.project_id IS NULL
            """)).fetchall()
            if existing:
                now_iso = _iso(datetime.now(UTC))
                conn.execute(
                    text("""
                        INSERT INTO project_members
                            (project_id, user_id, role, joined_at)
                        VALUES (:pid, :uid, 'owner', :ts)
                    """),
                    [{"pid": pid, "uid": uid, "ts": now_iso} for pid, uid in existing],
                )
                logger.info(
                    "Backfilled %d owner row(s) into project_members", len(existing),
                )


def _is_optional_timescale_stmt(stmt: str) -> bool:
    normalized = " ".join(stmt.lower().split())
    return (
        "create extension" in normalized and "timescaledb" in normalized
    ) or normalized.startswith("select create_hypertable")


# --- Cross-dialect helpers --------------------------------------------------


def _upsert_sql(
    table: str, columns: list[str], conflict_columns: list[str]
) -> str:
    """Return UPSERT SQL appropriate for the active backend.

    SQLite uses `INSERT OR REPLACE`; Postgres uses
    `INSERT ... ON CONFLICT (...) DO UPDATE SET ...` with EXCLUDED.
    """
    cols = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    if _is_postgres():
        updates = [c for c in columns if c not in conflict_columns]
        set_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in updates)
        conflict = ", ".join(conflict_columns)
        on_conflict = (
            f"ON CONFLICT ({conflict}) DO UPDATE SET {set_clause}"
            if updates
            else f"ON CONFLICT ({conflict}) DO NOTHING"
        )
        return f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) {on_conflict}"
    return f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})"


def _normalize_ts(value: Any) -> str | None:
    """Coerce a stored timestamp into an ISO 8601 UTC string.

    Storage returns ``str`` on SQLite (TEXT) and ``datetime`` on Postgres
    (TIMESTAMPTZ). All callers downstream expect the legacy string form, so
    we collapse both at the read boundary.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.astimezone(UTC).isoformat(timespec="seconds")
    return str(value)


def save_metrics(rows: Iterable[dict], project_id: str) -> int:
    """Insert a batch of metrics scoped to ``project_id`` (#53).

    Each row: {ts, table_name, metric_name, value, tags?}. The project_id
    is supplied once at the call site rather than per-row — every batch
    that this app emits is per-connection, and a connection lives in
    exactly one project.
    """
    payload = []
    for r in rows:
        tags = r.get("tags")
        payload.append(
            {
                "project_id": project_id,
                "ts": _iso(r["ts"]),
                "table_name": r["table_name"],
                "metric_name": r["metric_name"],
                "value": float(r["value"]),
                "tags": json.dumps(tags) if tags is not None else None,
            }
        )
    if not payload:
        return 0
    stmt = text("""
        INSERT INTO metrics (project_id, ts, table_name, metric_name, value, tags)
        VALUES (:project_id, :ts, :table_name, :metric_name, :value, :tags)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_metrics(
    table_name: str,
    metric_name: str,
    project_id: str,
    window: timedelta = timedelta(days=7),
) -> list[dict]:
    """Return rows for (project, table, metric) within `window`, oldest first."""
    since = _iso(datetime.now(UTC) - window)
    stmt = text("""
        SELECT ts, value, tags
        FROM metrics
        WHERE project_id = :project_id
          AND table_name = :table_name
          AND metric_name = :metric_name
          AND ts >= :since
        ORDER BY ts
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(
            stmt,
            {
                "project_id": project_id,
                "table_name": table_name,
                "metric_name": metric_name,
                "since": since,
            },
        ).fetchall()
    return [
        {
            "ts": _normalize_ts(r[0]),
            "value": r[1],
            "tags": json.loads(r[2]) if r[2] else None,
        }
        for r in rows
    ]


def get_latest_metric(
    table_name: str, metric_name: str, project_id: str
) -> dict | None:
    """Return the most recent {ts, value, tags} for (project, table, metric)."""
    stmt = text("""
        SELECT ts, value, tags
        FROM metrics
        WHERE project_id = :project_id
          AND table_name = :table_name
          AND metric_name = :metric_name
        ORDER BY ts DESC
        LIMIT 1
    """)
    with get_engine().connect() as conn:
        row = conn.execute(
            stmt, {"project_id": project_id, "table_name": table_name,
                   "metric_name": metric_name},
        ).fetchone()
    if not row:
        return None
    return {
        "ts": _normalize_ts(row[0]),
        "value": row[1],
        "tags": json.loads(row[2]) if row[2] else None,
    }


def get_latest_null_counts(
    table_name: str, project_id: str
) -> dict[str, int]:
    """Return {column: null_count} from the latest collector run.

    Scoped to ``project_id`` (#53) — same table_name in another tenant
    can't pollute the read.
    """
    stmt = text("""
        SELECT tags, value
        FROM metrics
        WHERE project_id = :project_id
          AND table_name = :table_name
          AND metric_name = 'null_count'
          AND ts = (
              SELECT MAX(ts) FROM metrics
              WHERE project_id = :project_id
                AND table_name = :table_name
                AND metric_name = 'null_count'
          )
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(
            stmt, {"project_id": project_id, "table_name": table_name},
        ).fetchall()
    result: dict[str, int] = {}
    for tags_json, value in rows:
        if not tags_json:
            continue
        column = json.loads(tags_json).get("column")
        if column:
            result[column] = int(value)
    return result


# Mirrors DEDUPE_WINDOW_HOURS in ml/changepoint — both layers use the same
# window so within-run and cross-run deduplication behave consistently.
_CLUSTER_WINDOW_HOURS = 72


def save_changepoints(rows: Iterable[dict], project_id: str = "legacy") -> list[dict]:
    """Persist detected change-points with cross-run cluster deduplication.

    Within a ±72 h window, at most one record is kept per
    (table, metric, direction).  A new detection replaces the stored one only
    when its score is strictly higher, so the best signal for each real shift
    survives successive hourly runs.  This complements the within-run
    deduplication already done by ml/changepoint._dedupe.

    Returns the list of events that were actually written (new or replaced),
    so callers can send notifications only for genuinely new/updated points.
    """
    payload = []
    detected_at = _iso(datetime.now(UTC))
    for r in rows:
        payload.append({
            "project_id": project_id,
            "ts": _iso(r["ts"]),
            "table_name": r["table_name"],
            "metric_name": r["metric_name"],
            "score": float(r["score"]),
            "value_before": float(r["value_before"]),
            "value_after": float(r["value_after"]),
            "detected_at": detected_at,
        })
    if not payload:
        return 0

    insert_stmt = text(_upsert_sql(
        "changepoints",
        ["project_id", "ts", "table_name", "metric_name", "score",
         "value_before", "value_after", "detected_at"],
        conflict_columns=["project_id", "ts", "table_name", "metric_name"],
    ))
    # Dialect-specific time delta: julianday is SQLite-only. On Postgres the
    # ts column is TIMESTAMPTZ and we compute hours via EXTRACT(EPOCH).
    # Delete by the changepoints PK — identical on both dialects.
    delete_stmt = text("""
        DELETE FROM changepoints
        WHERE project_id = :project_id
          AND ts = :ts
          AND table_name = :t
          AND metric_name = :m
    """)
    if _is_postgres():
        cluster_query = text("""
            SELECT ts, score FROM changepoints
            WHERE project_id = :project_id
              AND table_name = :t
              AND metric_name = :m
              AND ABS(EXTRACT(EPOCH FROM (ts - CAST(:ts AS TIMESTAMPTZ))) / 3600) <= :w
              AND (CASE WHEN value_after > value_before THEN 1 ELSE 0 END) = :dir
        """)
    else:
        cluster_query = text("""
            SELECT ts, score FROM changepoints
            WHERE project_id = :project_id
              AND table_name = :t
              AND metric_name = :m
              AND ABS(julianday(ts) - julianday(:ts)) * 24 <= :w
              AND (CASE WHEN value_after > value_before THEN 1 ELSE 0 END) = :dir
        """)

    saved_events: list[dict] = []
    with get_engine().begin() as conn:
        for row in payload:
            direction = 1 if row["value_after"] > row["value_before"] else 0
            existing = conn.execute(cluster_query, {
                "project_id": project_id,
                "t": row["table_name"],
                "m": row["metric_name"],
                "ts": row["ts"],
                "w": _CLUSTER_WINDOW_HOURS,
                "dir": direction,
            }).fetchall()

            if not existing:
                conn.execute(insert_stmt, row)
                saved_events.append(row)
            elif row["score"] > max(r.score for r in existing):
                for old in existing:
                    conn.execute(
                        delete_stmt,
                        {
                            "project_id": project_id,
                            "ts": old.ts,
                            "t": row["table_name"],
                            "m": row["metric_name"],
                        },
                    )
                conn.execute(insert_stmt, row)
                saved_events.append(row)

    return saved_events


def get_changepoints(
    table_name: str,
    metric_name: str | None = None,
    window: timedelta = timedelta(days=14),
    project_id: str = "legacy",
) -> list[dict]:
    """Return change-points for a table, oldest first. `metric_name=None`
    returns all metrics; otherwise filters."""
    since = _iso(datetime.now(UTC) - window)
    base = """
        SELECT ts, table_name, metric_name, score, value_before, value_after
        FROM changepoints
        WHERE project_id = :project_id
          AND table_name = :table_name
          AND ts >= :since
    """
    params: dict[str, Any] = {
        "project_id": project_id,
        "table_name": table_name,
        "since": since,
    }
    if metric_name is not None:
        base += " AND metric_name = :metric_name"
        params["metric_name"] = metric_name
    base += " ORDER BY ts"
    with get_engine().connect() as conn:
        rows = conn.execute(text(base), params).fetchall()
    return [
        {
            "ts": _normalize_ts(r[0]),
            "table_name": r[1],
            "metric_name": r[2],
            "score": r[3],
            "value_before": r[4],
            "value_after": r[5],
        }
        for r in rows
    ]


def get_schema_snapshot(table_name: str, project_id: str = "legacy") -> list[dict] | None:
    """Latest stored column list for a table and project, or None if no snapshot yet."""
    stmt = text(
        "SELECT columns FROM schema_snapshots WHERE project_id = :pid AND table_name = :t"
    )
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"pid": project_id, "t": table_name}).fetchone()
    if not row:
        return None
    return json.loads(row[0])


def save_schema_snapshot(table_name: str, columns: list[dict], project_id: str = "legacy") -> None:
    """Replace the stored snapshot for a table and project."""
    sql = _upsert_sql(
        "schema_snapshots",
        ["project_id", "table_name", "columns", "captured_at"],
        conflict_columns=["project_id", "table_name"],
    )
    with get_engine().begin() as conn:
        conn.execute(text(sql), {
            "project_id": project_id,
            "table_name": table_name,
            "columns": json.dumps(columns),
            "captured_at": _iso(datetime.now(UTC)),
        })


def save_schema_events(events: Iterable[dict], project_id: str = "legacy") -> int:
    """Append schema-drift events. Each event: {ts, table_name, change_type,
    column_name, details}."""
    payload = []
    for e in events:
        payload.append({
            "ts": _iso(e["ts"]),
            "table_name": e["table_name"],
            "change_type": e["change_type"],
            "column_name": e["column_name"],
            "details": json.dumps(e.get("details") or {}),
            "project_id": project_id,
        })
    if not payload:
        return 0
    stmt = text("""
        INSERT INTO schema_events (ts, table_name, change_type, column_name, details, project_id)
        VALUES (:ts, :table_name, :change_type, :column_name, :details, :project_id)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_schema_events(
    table_name: str,
    project_id: str = "legacy",
    window: timedelta = timedelta(days=30),
) -> list[dict]:
    """Recent schema-drift events for a table and project, newest first."""
    since = _iso(datetime.now(UTC) - window)
    stmt = text("""
        SELECT ts, table_name, change_type, column_name, details
        FROM schema_events
        WHERE table_name = :t AND project_id = :pid AND ts >= :since
        ORDER BY ts DESC
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"t": table_name, "pid": project_id, "since": since}).fetchall()
    return [
        {
            "ts": _normalize_ts(r[0]),
            "table_name": r[1],
            "change_type": r[2],
            "column_name": r[3],
            "details": json.loads(r[4]) if r[4] else {},
        }
        for r in rows
    ]


def save_anomaly_scores(rows: Iterable[dict], project_id: str = "legacy") -> int:
    """Upsert anomaly scores. Each row: {ts, table_name, score, is_anomaly}."""
    payload = []
    for r in rows:
        payload.append({
            "project_id": project_id,
            "ts": _iso(r["ts"]),
            "table_name": r["table_name"],
            "score": float(r["score"]),
            "is_anomaly": int(r["is_anomaly"]),
        })
    if not payload:
        return 0
    stmt = text(_upsert_sql(
        "anomaly_scores",
        ["project_id", "ts", "table_name", "score", "is_anomaly"],
        conflict_columns=["project_id", "ts", "table_name"],
    ))
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_anomaly_scores(
    table_name: str,
    project_id: str = "legacy",
    window: timedelta = timedelta(days=7),
) -> list[dict]:
    """Return anomaly scores for a table within *window*, oldest first."""
    since = _iso(datetime.now(UTC) - window)
    stmt = text("""
        SELECT ts, score, is_anomaly
        FROM anomaly_scores
        WHERE project_id = :project_id
          AND table_name = :t
          AND ts >= :since
        ORDER BY ts
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(
            stmt,
            {"project_id": project_id, "t": table_name, "since": since},
        ).fetchall()
    return [
        {"ts": _normalize_ts(r[0]), "score": r[1], "is_anomaly": r[2]}
        for r in rows
    ]


def save_drift_reports(
    table_name: str, rows: Iterable[dict], project_id: str = "legacy"
) -> int:
    """Полностью переписать кеш drift для одной таблицы.

    Рассчитанный снапшот PSI/KS — слайд по 7-дневному окну, поэтому хранить
    историю не нужно: пересчёт раз в тик всё равно убивает старое значение.
    """
    payload = []
    computed_at = _iso(datetime.now(UTC))
    for r in rows:
        payload.append({
            "project_id": project_id,
            "table_name": table_name,
            "column_name": r["column"],
            "data_type": r.get("data_type"),
            "psi": float(r["psi"]) if r.get("psi") is not None else None,
            "ks_pvalue": float(r["ks_pvalue"]) if r.get("ks_pvalue") is not None else None,
            "is_drift": int(bool(r.get("is_drift"))),
            "severity": r["severity"],
            "computed_at": computed_at,
        })
    with get_engine().begin() as conn:
        conn.execute(
            text("""
                DELETE FROM drift_reports
                WHERE project_id = :project_id AND table_name = :t
            """),
            {"project_id": project_id, "t": table_name},
        )
        if payload:
            conn.execute(
                text("""
                    INSERT INTO drift_reports
                        (project_id, table_name, column_name, data_type, psi, ks_pvalue,
                         is_drift, severity, computed_at)
                    VALUES (:project_id, :table_name, :column_name, :data_type, :psi,
                            :ks_pvalue, :is_drift, :severity, :computed_at)
                """),
                payload,
            )
    return len(payload)


def get_drift_report(table_name: str, project_id: str = "legacy") -> list[dict]:
    """Кешированный drift по таблице, отсортированный по убыванию PSI."""
    stmt = text("""
        SELECT column_name, data_type, psi, ks_pvalue, is_drift, severity
        FROM drift_reports
        WHERE project_id = :project_id
          AND table_name = :t
        ORDER BY COALESCE(psi, 0) DESC
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(
            stmt, {"project_id": project_id, "t": table_name}
        ).fetchall()
    return [
        {
            "column": r[0],
            "data_type": r[1],
            "psi": r[2],
            "ks_pvalue": r[3],
            "is_drift": bool(r[4]),
            "severity": r[5],
        }
        for r in rows
    ]


def purge_old(retention_days: int = 90, project_id: str | None = None) -> int:
    """Drop metrics older than ``retention_days``.

    With ``project_id=None`` (the retention cron path) drops across all
    tenants; with an explicit project_id, scoped to one tenant — useful for
    "delete project" cleanup.

    On TimescaleDB we first call ``drop_chunks`` (whole-chunk drop — orders of
    magnitude faster than per-row DELETE) and then DELETE any straggler rows
    older than the cutoff but living in the chunk that straddles the cutoff
    boundary. ``drop_chunks`` is age-based, not tenant-scoped — so when a
    project_id is supplied, we skip the chunk drop and rely on the DELETE.
    """
    cutoff_dt = datetime.now(UTC) - timedelta(days=retention_days)
    cutoff = _iso(cutoff_dt)
    engine = get_engine()
    if _is_postgres() and project_id is None:
        # drop_chunks runs in its own connection so a failure (plain Postgres
        # without the TimescaleDB extension) doesn't poison the outer txn.
        try:
            with engine.begin() as conn:
                dropped = conn.execute(
                    text("SELECT drop_chunks('metrics', CAST(:cutoff AS TIMESTAMPTZ))"),
                    {"cutoff": cutoff},
                ).fetchall()
            if dropped:
                logger.info(
                    "purge_old: dropped %d Timescale chunks older than %s",
                    len(dropped), cutoff,
                )
        except ProgrammingError as e:
            sqlstate = getattr(getattr(e, "orig", None), "pgcode", None)
            if sqlstate != "42883":
                raise
    where = "ts < :cutoff"
    params: dict[str, Any] = {"cutoff": cutoff}
    if project_id is not None:
        where = "project_id = :project_id AND " + where
        params["project_id"] = project_id
    with engine.begin() as conn:
        result = conn.execute(text(f"DELETE FROM metrics WHERE {where}"), params)
    return result.rowcount or 0


def _iso(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="seconds")


# --- History page helpers (#41) ---

_PROBLEM_NULL_RATE = 0.10
_NULL_SPIKE_DELTA = 0.05


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pct(value: float | None) -> str:
    if value is None:
        return "—"
    # null_rate is stored as a fraction: 0.18 = 18%
    return f"{value * 100:.1f}%" if value <= 1 else f"{value:.1f}%"


def _short_ts(ts: str) -> str:
    # 2026-05-05T17:20:00+00:00 -> 2026-05-05 17:20
    return ts.replace("T", " ")[:16]


def _metric_identity(table_name: str, tags_json: str | None) -> tuple[str, str]:
    """Stable key for table-level or column-level metric."""
    if not tags_json:
        return table_name, ""
    try:
        tags = json.loads(tags_json)
    except json.JSONDecodeError:
        return table_name, tags_json
    column = tags.get("column") if isinstance(tags, dict) else None
    return table_name, column or ""


def _metric_label(table_name: str, tags_json: str | None) -> str:
    """Human-readable label: table or table.column."""
    table, column = _metric_identity(table_name, tags_json)
    return f"{table}.{column}" if column else table


def _fetch_history_metric_rows(
    project_id: str, window: timedelta | None = timedelta(days=30)
) -> list[dict]:
    """Fetch row_count/null_rate rows used by the history page, scoped to tenant."""
    params: dict[str, Any] = {"project_id": project_id}
    where = "WHERE project_id = :project_id AND metric_name IN ('row_count', 'null_rate')"
    if window is not None:
        params["since"] = (datetime.now(UTC) - window).isoformat()
        where += " AND ts >= :since"

    stmt = text(f"""
        SELECT ts, table_name, metric_name, value, tags
        FROM metrics
        {where}
        ORDER BY ts ASC
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, params).fetchall()

    # If local seed data has timestamps outside the current window, fallback to all rows.
    if not rows and window is not None:
        return _fetch_history_metric_rows(project_id=project_id, window=None)

    return [
        {
            "ts": _normalize_ts(r[0]),
            "table_name": r[1],
            "metric_name": r[2],
            "value": _safe_float(r[3]),
            "tags": r[4],
        }
        for r in rows
    ]


def _fetch_anomalies_by_ts(
    project_id: str, window: timedelta | None = timedelta(days=30)
) -> dict[str, int]:
    """Count IF anomalies per collector tick within the window."""
    params: dict[str, Any] = {"project_id": project_id}
    where = "WHERE project_id = :project_id AND is_anomaly = 1"
    if window is not None:
        params["since"] = (datetime.now(UTC) - window).isoformat()
        where += " AND ts >= :since"
    stmt = text(f"SELECT ts, COUNT(*) FROM anomaly_scores {where} GROUP BY ts")
    with get_engine().connect() as conn:
        return {
            _normalize_ts(r[0]): int(r[1])
            for r in conn.execute(stmt, params).fetchall()
        }


def _history_aggregate(
    project_id: str, window: timedelta | None = timedelta(days=30)
) -> dict:
    """Build reusable aggregates from metrics table, scoped to project (#53).

    A collector run is represented by a timestamp `ts`.
    Problems: null_rate >= 10%.
    Null spikes: null_rate jump by >= 5 pp compared with the previous run
    for the same table/column metric. Rule-based heuristic on raw NULL rate.
    Anomalies: IF model verdicts from anomaly_scores keyed by the same ts.
    """
    rows = _fetch_history_metric_rows(project_id=project_id, window=window)
    anomalies_by_ts = _fetch_anomalies_by_ts(project_id=project_id, window=window)

    tables_by_ts: dict[str, set[str]] = {}
    problems_by_ts: dict[str, int] = {}
    null_spikes_by_ts: dict[str, int] = {}
    null_rates_by_identity: dict[tuple[str, str], list[tuple[str, float, str | None]]] = {}
    timestamps: set[str] = set()
    known_tables: set[str] = set()

    for r in rows:
        ts = r["ts"]
        table_name = r["table_name"]
        metric_name = r["metric_name"]
        value = r["value"]
        tags = r["tags"]

        if not ts or not table_name or value is None:
            continue

        timestamps.add(ts)

        if metric_name == "row_count":
            known_tables.add(table_name)
            tables_by_ts.setdefault(ts, set()).add(table_name)

        if metric_name == "null_rate":
            identity = _metric_identity(table_name, tags)
            null_rates_by_identity.setdefault(identity, []).append((ts, value, tags))
            if value >= _PROBLEM_NULL_RATE:
                problems_by_ts[ts] = problems_by_ts.get(ts, 0) + 1

    timestamps.update(anomalies_by_ts.keys())

    for values in null_rates_by_identity.values():
        previous: float | None = None
        for ts, value, _tags in sorted(values, key=lambda x: x[0]):
            if previous is not None and (value - previous) >= _NULL_SPIKE_DELTA:
                null_spikes_by_ts[ts] = null_spikes_by_ts.get(ts, 0) + 1
            previous = value

    total_tables = len(known_tables)
    sorted_timestamps = sorted(timestamps)

    return {
        "rows": rows,
        "timestamps": sorted_timestamps,
        "tables_by_ts": tables_by_ts,
        "problems_by_ts": problems_by_ts,
        "null_spikes_by_ts": null_spikes_by_ts,
        "anomalies_by_ts": anomalies_by_ts,
        "total_tables": total_tables,
    }


def build_history_aggregate(
    project_id: str, window: timedelta = timedelta(days=30)
) -> dict:
    """Compute a single aggregate for the History page, scoped to project.

    Call once per request and pass the result to get_history_runs,
    get_history_daily, and get_history_insights to avoid repeated DB queries.
    """
    return _history_aggregate(project_id=project_id, window=window)


def get_history_runs(agg: dict, limit: int = 10) -> list[dict]:
    """Return latest collector runs for the History page."""
    timestamps = list(reversed(agg["timestamps"]))[:limit]
    total_tables = agg["total_tables"]

    runs: list[dict] = []
    for ts in timestamps:
        checked_tables = len(agg["tables_by_ts"].get(ts, set()))
        coverage_pct = round((checked_tables / total_tables) * 100, 1) if total_tables else 0.0
        runs.append(
            {
                "ts": ts,
                "ts_label": _short_ts(ts),
                "tables_checked": checked_tables,
                "problems": agg["problems_by_ts"].get(ts, 0),
                "null_spikes": agg["null_spikes_by_ts"].get(ts, 0),
                "anomalies": agg["anomalies_by_ts"].get(ts, 0),
                "coverage_pct": coverage_pct,
            }
        )
    return runs


def get_history_daily(agg: dict, days: int = 14) -> list[dict]:
    """Return daily trend for problems, null spikes and coverage.

    For each day we use the latest collector run of that day. `days` limits
    how many recent days are shown — filters the already-built aggregate,
    so no extra DB query is needed.
    """
    total_tables = agg["total_tables"]
    cutoff = (datetime.now(UTC) - timedelta(days=days)).date().isoformat()
    latest_ts_by_day: dict[str, str] = {}
    ticks_by_day: dict[str, list[str]] = {}

    for ts in agg["timestamps"]:
        day = ts[:10]
        if day >= cutoff:
            latest_ts_by_day[day] = ts
            ticks_by_day.setdefault(day, []).append(ts)

    daily: list[dict] = []
    for day in sorted(latest_ts_by_day):
        ts = latest_ts_by_day[day]
        ticks = ticks_by_day[day]
        checked_tables = len(agg["tables_by_ts"].get(ts, set()))
        coverage_pct = round((checked_tables / total_tables) * 100, 1) if total_tables else 0.0
        # problems / coverage — состояние «на конец дня» (последний тик).
        # null_spikes / anomalies — события, суммируем за весь день, иначе
        # короткий пик в середине дня пропадает из агрегата.
        daily.append(
            {
                "date": day,
                "problems": agg["problems_by_ts"].get(ts, 0),
                "null_spikes": sum(agg["null_spikes_by_ts"].get(t, 0) for t in ticks),
                "anomalies": sum(agg["anomalies_by_ts"].get(t, 0) for t in ticks),
                "coverage_pct": coverage_pct,
            }
        )
    return daily


def get_history_insights(agg: dict) -> list[str]:
    """Rule-based text conclusions for the History page."""
    timestamps = agg["timestamps"]
    if not timestamps:
        return ["Исторические метрики пока не собраны. Запустите коллектор или сидер истории."]

    latest_ts = timestamps[-1]
    previous_ts = timestamps[-2] if len(timestamps) > 1 else None
    rows = agg["rows"]

    latest_null_rates = [
        r for r in rows
        if r["ts"] == latest_ts
        and r["metric_name"] == "null_rate"
        and r["value"] is not None
    ]

    insights: list[str] = []

    # Coverage insight
    checked_tables = len(agg["tables_by_ts"].get(latest_ts, set()))
    total_tables = agg["total_tables"]
    coverage_pct = round((checked_tables / total_tables) * 100, 1) if total_tables else 0.0
    insights.append(f"Покрытие последней проверки: {coverage_pct:.1f}% ({checked_tables} из {total_tables} таблиц).")

    # Problem insight
    latest_problems = agg["problems_by_ts"].get(latest_ts, 0)
    if latest_problems:
        insights.append(f"В последней проверке найдено проблемных NULL-метрик: {latest_problems}.")
    else:
        insights.append("В последней проверке критичных NULL-проблем по заданным порогам не обнаружено.")

    # ML anomaly insight (IsolationForest)
    latest_anomalies = agg["anomalies_by_ts"].get(latest_ts, 0)
    if latest_anomalies:
        insights.append(f"IsolationForest пометил аномалий в последней проверке: {latest_anomalies}.")

    # Biggest current risk
    if latest_null_rates:
        worst = max(latest_null_rates, key=lambda r: r["value"] or 0)
        if worst["value"] is not None and worst["value"] >= _PROBLEM_NULL_RATE:
            insights.append(
                f"Таблица/поле {_metric_label(worst['table_name'], worst['tags'])} требует проверки: "
                f"NULL rate сейчас {_pct(worst['value'])}."
            )

    # Growth insight compared to previous run
    if previous_ts:
        prev_by_key: dict[tuple[str, str], dict] = {}
        latest_by_key: dict[tuple[str, str], dict] = {}

        for r in rows:
            if r["metric_name"] != "null_rate" or r["value"] is None:
                continue
            key = _metric_identity(r["table_name"], r["tags"])
            if r["ts"] == previous_ts:
                prev_by_key[key] = r
            elif r["ts"] == latest_ts:
                latest_by_key[key] = r

        best_growth = None
        for key, latest_r in latest_by_key.items():
            prev = prev_by_key.get(key)
            if not prev:
                continue
            delta = latest_r["value"] - prev["value"]
            if best_growth is None or delta > best_growth[0]:
                best_growth = (delta, prev, latest_r)

        if best_growth and best_growth[0] >= 0.01:
            _delta, prev, latest_r = best_growth
            insights.append(
                f"Самый заметный рост пропусков: {_metric_label(latest_r['table_name'], latest_r['tags'])} "
                f"с {_pct(prev['value'])} до {_pct(latest_r['value'])}."
            )

    return insights[:4]


# --- Telegram notification throttle (#38 → #143 multi-tenant) ---

def is_throttled(
    project_id: str, table: str, event_key: str,
    *, throttle_minutes: int | None = None,
) -> bool:
    """Return True if a notification for this (project, table, event_key)
    was sent within the throttle window.

    ``throttle_minutes`` should come from the project's own configuration
    (``project_notifications.throttle_minutes``). Falls back to the global
    ``settings.TELEGRAM_THROTTLE_MINUTES`` only for the ``'legacy'`` tenant
    where there's no per-project row by definition.
    """
    stmt = text("""
        SELECT last_sent_at FROM telegram_throttle
        WHERE project_id = :pid AND table_name = :table AND event_key = :key
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {
            "pid": project_id, "table": table, "key": event_key,
        }).fetchone()
    if not row:
        return False
    raw = row[0]
    if isinstance(raw, datetime):
        last_sent = raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    else:
        last_sent = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if last_sent.tzinfo is None:
            last_sent = last_sent.replace(tzinfo=UTC)
    window_minutes = (
        throttle_minutes
        if throttle_minutes is not None
        else settings.TELEGRAM_THROTTLE_MINUTES
    )
    age = datetime.now(UTC) - last_sent
    return age.total_seconds() < window_minutes * 60


def update_throttle(project_id: str, table: str, event_key: str) -> None:
    """Record that a notification for (project, table, event_key) was just sent."""
    stmt = text(_upsert_sql(
        "telegram_throttle",
        ["project_id", "table_name", "event_key", "last_sent_at"],
        conflict_columns=["project_id", "table_name", "event_key"],
    ))
    with get_engine().begin() as conn:
        conn.execute(stmt, {
            "project_id": project_id,
            "table_name": table,
            "event_key": event_key,
            "last_sent_at": _iso(datetime.now(UTC)),
        })


# --- Per-project Telegram configuration (#143) ---

def get_project_notifications(project_id: str) -> dict | None:
    """Return Telegram config for a project, or None if no row exists.

    Returned dict has raw ``telegram_bot_token`` bytes (Fernet ciphertext);
    callers must decrypt via ``app.crypto.decrypt_bytes`` before use.
    """
    stmt = text("""
        SELECT project_id, telegram_bot_token, telegram_chat_id,
               throttle_minutes, updated_at
        FROM project_notifications
        WHERE project_id = :pid
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"pid": project_id}).fetchone()
    if row is None:
        return None
    return {
        "project_id": row[0],
        "telegram_bot_token": _normalize_binary_or_none(row[1]),
        "telegram_chat_id": row[2],
        "throttle_minutes": int(row[3]),
        "updated_at": str(row[4]) if row[4] else None,
    }


def _normalize_binary_or_none(value) -> bytes | None:
    """Normalize DB binary values across SQLite and Postgres drivers."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    return bytes(value)


def save_project_notifications(
    project_id: str,
    *,
    telegram_bot_token: bytes | None,
    telegram_chat_id: str | None,
    throttle_minutes: int = 30,
) -> None:
    """UPSERT a project's Telegram configuration.

    ``telegram_bot_token`` is the ciphertext from ``app.crypto.encrypt_bytes``
    (caller responsibility — this function does NOT encrypt). ``None`` for
    either token or chat_id means "partially configured" — no notification
    will be sent until both are set.
    """
    stmt = text(_upsert_sql(
        "project_notifications",
        ["project_id", "telegram_bot_token", "telegram_chat_id",
         "throttle_minutes", "updated_at"],
        conflict_columns=["project_id"],
    ))
    with get_engine().begin() as conn:
        conn.execute(stmt, {
            "project_id": project_id,
            "telegram_bot_token": telegram_bot_token,
            "telegram_chat_id": telegram_chat_id,
            "throttle_minutes": throttle_minutes,
            "updated_at": _iso(datetime.now(UTC)),
        })


def delete_project_notifications(project_id: str) -> None:
    """Wipe Telegram config for a project. Used by the «Отключить» button."""
    stmt = text("DELETE FROM project_notifications WHERE project_id = :pid")
    with get_engine().begin() as conn:
        conn.execute(stmt, {"pid": project_id})


def list_project_ids_with_telegram() -> list[str]:
    """Return project_ids that have a complete Telegram configuration.

    Used by the scheduler to iterate over tenants that should receive
    per-project changepoint and schema-drift notifications (#197).
    """
    stmt = text("""
        SELECT project_id FROM project_notifications
        WHERE telegram_bot_token IS NOT NULL
          AND telegram_chat_id IS NOT NULL
          AND telegram_chat_id != ''
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).fetchall()
    return [r[0] for r in rows]


def list_metric_tables(project_id: str) -> list[str]:
    """Return distinct table names that have metrics for a given project.

    Passed to detect_all() so changepoint detection only considers tables
    that actually belong to this tenant — avoids cross-tenant noise (#197).
    """
    stmt = text("""
        SELECT DISTINCT table_name FROM metrics
        WHERE project_id = :pid
        ORDER BY table_name
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"pid": project_id}).fetchall()
    return [r[0] for r in rows]


# --- Notification history (#76) ---

_NOTIFICATION_EVENT_TYPES = {
    "anomaly", "schema_drift", "changepoint", "forecast", "root_cause", "test",
}


def save_notification(
    *,
    event_type: str,
    message: str,
    status: str,
    table_name: str | None = None,
    metric_name: str | None = None,
    error: str | None = None,
    chat_id: str | None = None,
    ts: datetime | str | None = None,
    project_id: str = "legacy",
) -> int:
    """Store a Telegram notification record. Returns the new row id.

    Called from notify_* helpers regardless of delivery outcome — failures are
    persisted with status='failed' so the UI can show a complete audit trail.
    """
    payload = {
        "ts": _iso(ts or datetime.now(UTC)),
        "project_id": project_id,
        "event_type": event_type,
        "table_name": table_name,
        "metric_name": metric_name,
        "message": message,
        "status": status,
        "error": error,
        "chat_id": str(chat_id) if chat_id is not None else None,
    }
    base = """
        INSERT INTO notifications
            (ts, project_id, event_type, table_name, metric_name, message, status, error, chat_id)
        VALUES
            (:ts, :project_id, :event_type, :table_name, :metric_name, :message, :status, :error, :chat_id)
    """
    # Postgres has no lastrowid — fetch the new id with RETURNING.
    sql = base + (" RETURNING id" if _is_postgres() else "")
    with get_engine().begin() as conn:
        result = conn.execute(text(sql), payload)
        if _is_postgres():
            new_id = result.scalar()
            return int(new_id) if new_id is not None else 0
        try:
            return int(result.lastrowid or 0)
        except AttributeError:
            return 0


def get_notifications(
    *,
    event_type: str | None = None,
    table_name: str | None = None,
    status: str | None = None,
    since: datetime | str | None = None,
    until: datetime | str | None = None,
    limit: int = 50,
    offset: int = 0,
    project_id: str | None = None,
) -> list[dict]:
    """Return notification history, newest first, with optional filters.

    Pagination via limit/offset; defaults give a sane single-page response for
    the UI without scanning the full table.
    """
    where = ["1=1"]
    params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
    if project_id is not None:
        where.append("project_id = :project_id")
        params["project_id"] = project_id
    if event_type:
        where.append("event_type = :event_type")
        params["event_type"] = event_type
    if table_name:
        where.append("table_name = :table_name")
        params["table_name"] = table_name
    if status:
        where.append("status = :status")
        params["status"] = status
    if since is not None:
        where.append("ts >= :since")
        params["since"] = _iso(since)
    if until is not None:
        where.append("ts <= :until")
        params["until"] = _iso(until)

    stmt = text(f"""
        SELECT id, ts, event_type, table_name, metric_name, message, status,
               error, chat_id, project_id
        FROM notifications
        WHERE {' AND '.join(where)}
        ORDER BY ts DESC, id DESC
        LIMIT :limit OFFSET :offset
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, params).fetchall()
    return [
        {
            "id": r[0],
            "ts": _normalize_ts(r[1]),
            "event_type": r[2],
            "table_name": r[3],
            "metric_name": r[4],
            "message": r[5],
            "status": r[6],
            "error": r[7],
            "chat_id": r[8],
            "project_id": r[9],
        }
        for r in rows
    ]


def count_notifications(
    *,
    event_type: str | None = None,
    table_name: str | None = None,
    status: str | None = None,
    since: datetime | str | None = None,
    until: datetime | str | None = None,
    project_id: str | None = None,
) -> int:
    """Total notifications matching filters — used for pagination metadata."""
    where = ["1=1"]
    params: dict[str, Any] = {}
    if project_id is not None:
        where.append("project_id = :project_id")
        params["project_id"] = project_id
    if event_type:
        where.append("event_type = :event_type")
        params["event_type"] = event_type
    if table_name:
        where.append("table_name = :table_name")
        params["table_name"] = table_name
    if status:
        where.append("status = :status")
        params["status"] = status
    if since is not None:
        where.append("ts >= :since")
        params["since"] = _iso(since)
    if until is not None:
        where.append("ts <= :until")
        params["until"] = _iso(until)
    stmt = text(f"SELECT COUNT(*) FROM notifications WHERE {' AND '.join(where)}")
    with get_engine().connect() as conn:
        return int(conn.execute(stmt, params).scalar() or 0)


# --- LLM explanation cache (#36) ---

def get_cached_explanation(
    table: str, metric: str, ts: str, ttl_hours: int = 24
) -> dict | None:
    """Return a cached LLM explanation if it exists and is within TTL, else None."""
    stmt = text("""
        SELECT explanation, suggested_fix, confidence, created_at
        FROM llm_explanations
        WHERE table_name = :table AND metric = :metric AND ts = :ts
        LIMIT 1
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"table": table, "metric": metric, "ts": ts}).fetchone()
    if not row:
        return None
    raw_created = row[3]
    if isinstance(raw_created, datetime):
        created_at = raw_created if raw_created.tzinfo else raw_created.replace(tzinfo=UTC)
    else:
        created_at = datetime.fromisoformat(str(raw_created).replace("Z", "+00:00"))
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=UTC)
    age = datetime.now(UTC) - created_at
    if age.total_seconds() > ttl_hours * 3600:
        return None
    return {
        "explanation": row[0],
        "suggested_fix": row[1],
        "confidence": row[2],
    }


def save_explanation(
    table: str,
    metric: str,
    ts: str,
    explanation: str,
    suggested_fix: str,
    confidence: float,
) -> None:
    """Upsert an LLM explanation into the cache."""
    sql = _upsert_sql(
        "llm_explanations",
        ["table_name", "metric", "ts", "explanation",
         "suggested_fix", "confidence", "created_at"],
        conflict_columns=["table_name", "metric", "ts"],
    )
    with get_engine().begin() as conn:
        conn.execute(text(sql), {
            "table_name": table,
            "metric": metric,
            "ts": ts,
            "explanation": explanation,
            "suggested_fix": suggested_fix,
            "confidence": float(confidence),
            "created_at": _iso(datetime.now(UTC)),
        })


# --- Users (#49) -----------------------------------------------------------


class UserAlreadyExists(Exception):
    """Raised by create_user when the email is already registered."""


def create_user(
    user_id: str, email: str, password_hash: str
) -> dict:
    """Insert a new user. Raises UserAlreadyExists on duplicate email.

    Email must already be normalised (lower-cased, stripped) by the caller —
    storage layer does not transform it. Returns the stored row as a dict.

    We pre-check by email before the INSERT so ``UserAlreadyExists`` stays
    meaningful even if future migrations add another ``UNIQUE`` constraint
    on this table — a blanket ``except IntegrityError`` would otherwise
    mis-label the new violation as "email taken". A benign race remains
    (two parallel registers with the same email) — only one wins the
    INSERT, the loser sees the IntegrityError and we surface it as
    ``UserAlreadyExists`` after confirming the email is now present.
    """
    if get_user_by_email(email) is not None:
        raise UserAlreadyExists(email)

    now = _iso(datetime.now(UTC))
    payload = {
        "id": user_id,
        "email": email,
        "password_hash": password_hash,
        "created_at": now,
        "last_login_at": None,
    }
    stmt = text("""
        INSERT INTO users (id, email, password_hash, created_at, last_login_at)
        VALUES (:id, :email, :password_hash, :created_at, :last_login_at)
    """)
    try:
        with get_engine().begin() as conn:
            conn.execute(stmt, payload)
    except IntegrityError:
        # Race: another concurrent register won. Re-check; if the email is
        # now present, surface UserAlreadyExists. Otherwise re-raise — the
        # IntegrityError came from a different constraint we don't own.
        if get_user_by_email(email) is not None:
            raise UserAlreadyExists(email) from None
        raise
    return {**payload, "last_login_at": None}


def _row_to_user(row) -> dict | None:
    if row is None:
        return None
    return {
        "id": row[0],
        "email": row[1],
        "password_hash": row[2],
        "created_at": _normalize_ts(row[3]),
        "last_login_at": _normalize_ts(row[4]),
        # #220: is_admin как bool — UI / current_user.is_admin сравнивает
        # с булем. SQLite даёт int 0/1, Postgres — то же при INTEGER column.
        "is_admin": bool(row[5]) if len(row) > 5 else False,
    }


def get_user_by_email(email: str) -> dict | None:
    stmt = text("""
        SELECT id, email, password_hash, created_at, last_login_at, is_admin
        FROM users WHERE email = :email
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"email": email}).fetchone()
    return _row_to_user(row)


def get_user_by_id(user_id: str) -> dict | None:
    stmt = text("""
        SELECT id, email, password_hash, created_at, last_login_at, is_admin
        FROM users WHERE id = :id
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"id": user_id}).fetchone()
    return _row_to_user(row)


def set_user_admin(user_id: str, is_admin: bool) -> bool:
    """Toggle users.is_admin (#220). Returns True если ряд обновился.

    Идемпотентна: повторный вызов с тем же значением → возвращает True
    если юзер существует, False если не найден.
    """
    with get_engine().begin() as conn:
        result = conn.execute(text(
            "UPDATE users SET is_admin = :v WHERE id = :id"
        ), {"v": 1 if is_admin else 0, "id": user_id})
    return (result.rowcount or 0) > 0


def is_system_admin(user_id: str) -> bool:
    """Quick lookup для @admin_required: только bool, без полного user dict."""
    with get_engine().connect() as conn:
        row = conn.execute(text(
            "SELECT is_admin FROM users WHERE id = :id"
        ), {"id": user_id}).fetchone()
    return bool(row[0]) if row else False


def update_last_login(user_id: str) -> None:
    """Stamp last_login_at = now for the given user. No-op if user is gone."""
    stmt = text("UPDATE users SET last_login_at = :ts WHERE id = :id")
    with get_engine().begin() as conn:
        conn.execute(stmt, {"id": user_id, "ts": _iso(datetime.now(UTC))})


def update_user_password(user_id: str, password_hash: str) -> None:
    """Replace the user's password hash. Used by the password-reset flow (#133).
    Caller is responsible for hashing — storage does NOT transform the input."""
    stmt = text("UPDATE users SET password_hash = :h WHERE id = :id")
    with get_engine().begin() as conn:
        conn.execute(stmt, {"id": user_id, "h": password_hash})


# --- Password reset tokens (#133) ------------------------------------------


def create_password_reset_token(
    user_id: str, token_hash: str, expires_at: datetime,
) -> None:
    """Persist a freshly-issued reset token row. ``token_hash`` is
    HMAC-SHA256(SECRET_KEY, raw_token) — raw token is emailed and never
    touches the DB."""
    stmt = text(
        "INSERT INTO password_reset_tokens "
        "(user_id, token_hash, expires_at, used_at, created_at) "
        "VALUES (:uid, :h, :exp, NULL, :created)"
    )
    now = datetime.now(UTC)
    with get_engine().begin() as conn:
        conn.execute(stmt, {
            "uid": user_id, "h": token_hash,
            "exp": _iso(expires_at), "created": _iso(now),
        })


def get_active_password_reset_token(token_hash: str) -> dict | None:
    """Return the row for an UNUSED, NON-EXPIRED token, or None.

    Used by GET /auth/reset-password to render the form. POST goes through
    ``consume_password_reset_token`` instead — that atomic UPDATE is what
    enforces one-time use; this read is purely for the "show form vs
    show error page" branch.
    """
    stmt = text("""
        SELECT id, user_id, token_hash, expires_at, used_at, created_at
        FROM password_reset_tokens
        WHERE token_hash = :h AND used_at IS NULL AND expires_at > :now
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {
            "h": token_hash, "now": _iso(datetime.now(UTC)),
        }).fetchone()
    if row is None:
        return None
    return {
        "id": int(row[0]),
        "user_id": row[1],
        "token_hash": row[2],
        "expires_at": _normalize_ts(row[3]),
        "used_at": _normalize_ts(row[4]) if row[4] else None,
        "created_at": _normalize_ts(row[5]),
    }


def consume_password_reset_token(token_hash: str) -> str | None:
    """Atomically mark a token as used and return its ``user_id``.

    Returns None if the token doesn't exist, has expired, or has already
    been used. The single UPDATE with ``used_at IS NULL`` + ``expires_at > now``
    guard is what makes the token one-time: a race where two POSTs arrive
    simultaneously can result in at most one successful row update (the
    other sees 0 rows affected).
    """
    now_iso = _iso(datetime.now(UTC))
    with get_engine().begin() as conn:
        # 1. Try to claim the token — atomic UPDATE conditional on still-valid.
        upd = conn.execute(text("""
            UPDATE password_reset_tokens
            SET used_at = :now
            WHERE token_hash = :h
              AND used_at IS NULL
              AND expires_at > :now
        """), {"h": token_hash, "now": now_iso})
        if (upd.rowcount or 0) == 0:
            return None
        # 2. Look up the user_id for the row we just claimed.
        row = conn.execute(text(
            "SELECT user_id FROM password_reset_tokens WHERE token_hash = :h"
        ), {"h": token_hash}).fetchone()
    return row[0] if row else None


def invalidate_password_reset_tokens(user_id: str) -> int:
    """Mark every active reset token for *user_id* as used. Called from
    /forgot-password before issuing a fresh one (so the latest email
    invalidates earlier links) and from successful /reset-password (so
    other concurrently-issued tokens can't be reused)."""
    stmt = text(
        "UPDATE password_reset_tokens "
        "SET used_at = :now "
        "WHERE user_id = :uid AND used_at IS NULL"
    )
    with get_engine().begin() as conn:
        result = conn.execute(stmt, {
            "uid": user_id, "now": _iso(datetime.now(UTC)),
        })
    return int(result.rowcount or 0)


# --- Failed login attempts (#56) -------------------------------------------


def record_failed_login(email: str) -> None:
    """Append a failed login attempt for *email*. Caller must pass a
    normalised (lower-cased) email — storage doesn't transform it."""
    stmt = text(
        "INSERT INTO failed_login_attempts (email, attempted_at) "
        "VALUES (:email, :ts)"
    )
    with get_engine().begin() as conn:
        conn.execute(stmt, {"email": email, "ts": _iso(datetime.now(UTC))})


def count_recent_failed_logins(email: str, window: timedelta) -> int:
    """How many failed login attempts for *email* in the last *window*."""
    since = _iso(datetime.now(UTC) - window)
    stmt = text(
        "SELECT COUNT(*) FROM failed_login_attempts "
        "WHERE email = :email AND attempted_at >= :since"
    )
    with get_engine().connect() as conn:
        return int(conn.execute(stmt, {"email": email, "since": since}).scalar() or 0)


def clear_failed_logins(email: str) -> int:
    """Wipe failed attempts for *email* on a successful login.

    Returns rows deleted (mostly diagnostic — callers don't act on it).
    """
    stmt = text("DELETE FROM failed_login_attempts WHERE email = :email")
    with get_engine().begin() as conn:
        result = conn.execute(stmt, {"email": email})
    return result.rowcount or 0


# --- Projects (#50) --------------------------------------------------------


class ProjectSlugTaken(Exception):
    """Raised by create_project when (user_id, slug) is already used."""


def create_project(project_id: str, user_id: str, name: str, slug: str) -> dict:
    """Insert a new project + auto-add owner row to project_members (#172).

    Raises ProjectSlugTaken on UNIQUE violation. Slug must already be
    normalised (lower-cased, URL-safe) by the caller. Both inserts run in
    one transaction — if the owner-row write fails, the project itself is
    rolled back.
    """
    # Pre-check keeps ProjectSlugTaken trustworthy if future schema adds
    # other UNIQUEs (see same pattern in create_user). Race window is benign:
    # the IntegrityError fallback re-checks before re-raising.
    if get_project_by_slug(user_id, slug) is not None:
        raise ProjectSlugTaken(slug)
    now = _iso(datetime.now(UTC))
    payload = {
        "id": project_id, "user_id": user_id,
        "name": name, "slug": slug, "created_at": now,
    }
    try:
        with get_engine().begin() as conn:
            conn.execute(text(
                "INSERT INTO projects (id, user_id, name, slug, created_at) "
                "VALUES (:id, :user_id, :name, :slug, :created_at)"
            ), payload)
            # #172: автор → owner row в project_members. Один INSERT в той
            # же транзакции. Без него new project не появится в
            # list_projects_for_user (она walks membership, не owners).
            conn.execute(text(
                "INSERT INTO project_members "
                "(project_id, user_id, role, joined_at) "
                "VALUES (:pid, :uid, 'owner', :ts)"
            ), {"pid": project_id, "uid": user_id, "ts": now})
    except IntegrityError:
        if get_project_by_slug(user_id, slug) is not None:
            raise ProjectSlugTaken(slug) from None
        raise
    return payload


def _row_to_project(row) -> dict | None:
    if row is None:
        return None
    return {
        "id": row[0],
        "user_id": row[1],
        "name": row[2],
        "slug": row[3],
        "created_at": _normalize_ts(row[4]),
    }


def get_project_by_slug(user_id: str, slug: str) -> dict | None:
    """Scoped to user — returns owned OR shared project matching the slug (#172).

    Slugs unique per OWNER, поэтому при коллизии "user_id владелец И user_id
    member of another's project with same slug" возвращаем owned-вариант
    (приоритет). Если у юзера такого собственного нет, проверяем shared.
    """
    # Шаг 1: owned shortcut.
    with get_engine().connect() as conn:
        row = conn.execute(text(
            "SELECT id, user_id, name, slug, created_at FROM projects "
            "WHERE user_id = :user_id AND slug = :slug"
        ), {"user_id": user_id, "slug": slug}).fetchone()
        if row is not None:
            return _row_to_project(row)
        # Шаг 2: shared lookup via project_members.
        row = conn.execute(text(
            "SELECT p.id, p.user_id, p.name, p.slug, p.created_at "
            "FROM projects p "
            "INNER JOIN project_members m ON m.project_id = p.id "
            "WHERE m.user_id = :user_id AND p.slug = :slug "
            "LIMIT 1"
        ), {"user_id": user_id, "slug": slug}).fetchone()
    return _row_to_project(row)


def get_project_by_id(user_id: str, project_id: str) -> dict | None:
    """Membership-scoped (#172): returns the project iff user_id is owner
    OR a member. Defends against horizontal escalation — random project_id
    guesses still don't leak data."""
    with get_engine().connect() as conn:
        row = conn.execute(text(
            "SELECT p.id, p.user_id, p.name, p.slug, p.created_at "
            "FROM projects p "
            "LEFT JOIN project_members m "
            "  ON m.project_id = p.id AND m.user_id = :user_id "
            "WHERE p.id = :id AND (p.user_id = :user_id OR m.user_id IS NOT NULL)"
        ), {"id": project_id, "user_id": user_id}).fetchone()
    return _row_to_project(row)


def list_projects_for_user(user_id: str) -> list[dict]:
    """Returns owned + shared projects (#172).

    UNION over (owner WHERE user_id) и (member via project_members) с
    DISTINCT по project_id чтобы owner-row (есть как в projects.user_id,
    так и в project_members.user_id='owner') не дублировался.

    Поле ``role`` — что у юзера в проекте: 'owner' для своего, иначе
    значение из project_members. UI потом покажет бейджик роли.
    """
    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT p.id, p.user_id, p.name, p.slug, p.created_at,
                   COALESCE(m.role, 'owner') AS role
            FROM projects p
            INNER JOIN project_members m
              ON m.project_id = p.id AND m.user_id = :user_id
            ORDER BY p.created_at
        """), {"user_id": user_id}).fetchall()
    return [
        {**(_row_to_project(r) or {}), "role": r[5]}
        for r in rows
    ]


# --- Project membership (#172) ---------------------------------------------


_MEMBER_ROLES = ("owner", "editor", "viewer")


class InvalidMemberRole(Exception):
    """Raised when add_project_member gets a role outside the allowed set."""


def add_project_member(
    project_id: str, user_id: str, role: str = "viewer",
) -> dict:
    """Upsert a (project, user, role) row in project_members.

    Идемпотентна — повторный вызов с тем же role вернёт существующую
    запись без ошибки. Смена роли через тот же вызов c новым role —
    UPSERT update path. Owner-row создаётся только через create_project,
    защищаем что нельзя downgrade owner-а через этот вход.
    """
    if role not in _MEMBER_ROLES:
        raise InvalidMemberRole(f"role must be one of {_MEMBER_ROLES}, got {role!r}")
    now = _iso(datetime.now(UTC))
    # Не позволяем перезаписать owner-row через этот entry — иначе
    # remove_project_member([role='owner']) можно отменить, чтобы юзер
    # перестал быть owner-ом. Защита: проверяем существующий role перед
    # upsert и отказываемся менять existing owner.
    existing = get_member_role(project_id, user_id)
    if existing == "owner" and role != "owner":
        raise InvalidMemberRole("cannot downgrade owner via add_project_member")
    stmt = text(_upsert_sql(
        "project_members",
        ["project_id", "user_id", "role", "joined_at"],
        conflict_columns=["project_id", "user_id"],
    ))
    with get_engine().begin() as conn:
        conn.execute(stmt, {
            "project_id": project_id, "user_id": user_id,
            "role": role, "joined_at": now,
        })
    return {"project_id": project_id, "user_id": user_id, "role": role,
            "joined_at": now}


def remove_project_member(project_id: str, user_id: str) -> bool:
    """Remove a non-owner member. Owner cannot be removed — он удаляется
    вместе с проектом через delete_project. Returns True if a row was
    removed (idempotent: False if нет такой membership)."""
    if get_member_role(project_id, user_id) == "owner":
        raise InvalidMemberRole("cannot remove project owner")
    with get_engine().begin() as conn:
        result = conn.execute(text(
            "DELETE FROM project_members "
            "WHERE project_id = :pid AND user_id = :uid"
        ), {"pid": project_id, "uid": user_id})
    return (result.rowcount or 0) > 0


def get_member_role(project_id: str, user_id: str) -> str | None:
    """Return the user's role on the project, or None if no membership.
    Используется на всех access-проверках вместо ownership."""
    with get_engine().connect() as conn:
        row = conn.execute(text(
            "SELECT role FROM project_members "
            "WHERE project_id = :pid AND user_id = :uid"
        ), {"pid": project_id, "uid": user_id}).fetchone()
    return row[0] if row else None


def list_project_members(project_id: str) -> list[dict]:
    """All members of a project with their roles + email/joined timestamp."""
    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT pm.user_id, u.email, pm.role, pm.joined_at
            FROM project_members pm
            INNER JOIN users u ON u.id = pm.user_id
            WHERE pm.project_id = :pid
            ORDER BY pm.joined_at
        """), {"pid": project_id}).fetchall()
    return [
        {
            "user_id": r[0], "email": r[1],
            "role": r[2], "joined_at": _normalize_ts(r[3]),
        }
        for r in rows
    ]


def rename_project(project_id: str, new_name: str) -> bool:
    """Update projects.name. Returns True when the row was found and changed,
    False when project_id doesn't exist (#224).

    slug is intentionally NOT touched — it's part of the URL and immutable
    after creation (changing it would break invite links, bookmarks, and
    project_members joins).
    Caller is responsible for the owner-only access check; storage is a
    plain UPDATE so misuse is at the route layer.
    """
    with get_engine().begin() as conn:
        result = conn.execute(
            text("UPDATE projects SET name = :name WHERE id = :id"),
            {"name": new_name, "id": project_id},
        )
    return (result.rowcount or 0) > 0


def delete_project(user_id: str, project_id: str) -> bool:
    """Hard delete. Returns True if a row was removed.

    Ownership is verified by the WHERE user_id clause on the projects DELETE.
    notifications are removed only when the project row was actually ours —
    no FK cascade on SQLite, so we do it manually in the same transaction.
    """
    with get_engine().begin() as conn:
        result = conn.execute(
            text("DELETE FROM projects WHERE id = :id AND user_id = :user_id"),
            {"id": project_id, "user_id": user_id},
        )
        deleted = (result.rowcount or 0) > 0
        if deleted:
            conn.execute(
                text("DELETE FROM metrics WHERE project_id = :pid"),
                {"pid": project_id},
            )
            conn.execute(
                text("DELETE FROM notifications WHERE project_id = :pid"),
                {"pid": project_id},
            )
    return deleted


# --- /Projects -------------------------------------------------------------


# --- Project invites (#222) -----------------------------------------------


INVITE_TTL = timedelta(days=7)


class InviteError(Exception):
    """Base for invite-consumption failures.

    Subclasses encode the *why* so the route layer can render a tailored
    HTTP status (404 unknown, 400 expired, 400 used) without sniffing the
    storage row from the outside.
    """


class InviteExpired(InviteError):
    """Token row exists but expires_at < now."""


class InviteAlreadyUsed(InviteError):
    """Token row exists but used_at IS NOT NULL."""


def create_invite_token(
    project_id: str, role: str, created_by: str,
    ttl: timedelta = INVITE_TTL,
) -> dict:
    """Mint and persist a new invite token. Returns the inserted row.

    role must be 'editor' or 'viewer' — owner invites are explicitly not
    supported (an owner inherits the project, not just a membership row).
    Token is 32 random bytes as hex → 64 chars. Acceptance criteria spec'd
    "32-char-hex" but secrets.token_hex(16) gives only 128 bits; double
    that for room without breaking the URL pattern.
    """
    if role not in ("editor", "viewer"):
        raise InvalidMemberRole(
            f"invite role must be editor|viewer, got {role!r}"
        )
    import secrets

    token = secrets.token_hex(16)  # 32-char hex per acceptance criteria
    now = datetime.now(UTC)
    payload = {
        "token": token,
        "project_id": project_id,
        "role": role,
        "created_by": created_by,
        "created_at": _iso(now),
        "expires_at": _iso(now + ttl),
        "used_at": None,
    }
    stmt = text("""
        INSERT INTO project_invites
            (token, project_id, role, created_by, created_at, expires_at, used_at)
        VALUES
            (:token, :project_id, :role, :created_by,
             :created_at, :expires_at, NULL)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return payload


def get_invite_by_token(token: str) -> dict | None:
    """Look up a raw invite row by token. Returns None if not found.

    Does NOT validate expiry / used_at — that's consume_invite's job.
    The route layer uses this only to render the registration redirect
    page when the user is unauthenticated (so it needs to know the token
    is well-formed, not yet whether it'd succeed).
    """
    stmt = text("""
        SELECT token, project_id, role, created_by,
               created_at, expires_at, used_at
        FROM project_invites WHERE token = :token
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"token": token}).fetchone()
    if row is None:
        return None
    return {
        "token": row[0],
        "project_id": row[1],
        "role": row[2],
        "created_by": row[3],
        "created_at": _normalize_ts(row[4]),
        "expires_at": _normalize_ts(row[5]),
        "used_at": _normalize_ts(row[6]) if row[6] else None,
    }


def consume_invite(token: str, user_id: str) -> dict:
    """Atomically claim an invite for *user_id* and add them to the project.

    Returns the invite row (with project_id + role) so the caller can
    redirect to the right project page. Raises:

    - LookupError — no such token (404).
    - InviteExpired — expires_at < now.
    - InviteAlreadyUsed — used_at already set, or lost a race for the row.

    Single-statement claim (UPDATE … WHERE used_at IS NULL AND expires_at
    > now) is what makes the token one-time: two concurrent POSTs can't
    both win the update. The membership row goes through
    add_project_member so it picks up the same idempotency + role
    validation as the manual-add path.
    """
    row = get_invite_by_token(token)
    if row is None:
        raise LookupError(token)
    now = datetime.now(UTC)
    expires = datetime.fromisoformat(row["expires_at"].replace("Z", "+00:00"))
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    # Check expiry before claiming so we surface InviteExpired even if
    # the token is also unused — the user sees the more specific error.
    if expires < now:
        raise InviteExpired(token)
    if row["used_at"] is not None:
        raise InviteAlreadyUsed(token)

    now_iso = _iso(now)
    with get_engine().begin() as conn:
        result = conn.execute(text("""
            UPDATE project_invites
            SET used_at = :now
            WHERE token = :token
              AND used_at IS NULL
              AND expires_at > :now
        """), {"now": now_iso, "token": token})
        if (result.rowcount or 0) == 0:
            # Race: someone else claimed it between get and update.
            raise InviteAlreadyUsed(token)

    # Add membership outside the claim transaction — if it fails, the
    # token is already burnt (acceptable: re-issue instead of double-use).
    add_project_member(row["project_id"], user_id, row["role"])
    return row


# --- Connections (#51) -----------------------------------------------------


def create_connection(
    *,
    connection_id: str,
    project_id: str,
    name: str,
    dsn_encrypted: bytes,
    schema_name: str = "public",
    interval_minutes: int = 15,
    is_active: bool = True,
) -> dict:
    """Insert a new DB connection. dsn_encrypted is Fernet ciphertext.

    Caller is responsible for owning the project_id (no cross-tenant check
    here — that lives in the route layer).
    """
    now = _iso(datetime.now(UTC))
    payload = {
        "id": connection_id,
        "project_id": project_id,
        "name": name,
        "dsn_encrypted": dsn_encrypted,
        "schema_name": schema_name,
        "interval_minutes": int(interval_minutes),
        "is_active": 1 if is_active else 0,
        "created_at": now,
    }
    stmt = text("""
        INSERT INTO connections
            (id, project_id, name, dsn_encrypted, schema_name,
             interval_minutes, is_active, created_at)
        VALUES
            (:id, :project_id, :name, :dsn_encrypted, :schema_name,
             :interval_minutes, :is_active, :created_at)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return payload


# Columns selected by every connection read. Centralised so a new column
# (#232 load-safety knobs, etc.) lands in one place instead of being added
# to three SELECTs that drift apart.
_CONNECTION_COLUMNS = (
    "id, project_id, name, dsn_encrypted, schema_name, "
    "interval_minutes, is_active, created_at, "
    "table_allowlist, table_denylist, max_tables_per_tick, "
    "skip_tables_larger_than_gb, statement_timeout_ms"
)


def _row_to_connection(row) -> dict | None:
    if row is None:
        return None
    return {
        "id": row[0],
        "project_id": row[1],
        "name": row[2],
        "dsn_encrypted": bytes(row[3]) if row[3] is not None else None,
        "schema_name": row[4],
        "interval_minutes": int(row[5]),
        "is_active": bool(row[6]),
        "created_at": _normalize_ts(row[7]),
        # #232: load-safety knobs. May be NULL on rows created before the
        # migration; collector code MUST tolerate missing/None values.
        "table_allowlist": row[8],
        "table_denylist": row[9],
        "max_tables_per_tick": int(row[10]) if row[10] is not None else None,
        "skip_tables_larger_than_gb": (
            float(row[11]) if row[11] is not None else None
        ),
        "statement_timeout_ms": (
            int(row[12]) if row[12] is not None else None
        ),
    }


def list_connections_for_project(project_id: str) -> list[dict]:
    stmt = text(
        f"SELECT {_CONNECTION_COLUMNS} FROM connections "
        "WHERE project_id = :pid ORDER BY created_at"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"pid": project_id}).fetchall()
    return [_row_to_connection(r) for r in rows]


def get_connection(project_id: str, connection_id: str) -> dict | None:
    """Scoped to project — never returns a connection from a different
    project even when the id is guessable. Defends against horizontal
    escalation via id-in-URL."""
    stmt = text(
        f"SELECT {_CONNECTION_COLUMNS} FROM connections "
        "WHERE id = :id AND project_id = :pid"
    )
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"id": connection_id, "pid": project_id}).fetchone()
    return _row_to_connection(row)


def delete_connection(project_id: str, connection_id: str) -> bool:
    """Hard delete. Returns True if a row was removed."""
    stmt = text(
        "DELETE FROM connections WHERE id = :id AND project_id = :pid"
    )
    with get_engine().begin() as conn:
        result = conn.execute(stmt, {"id": connection_id, "pid": project_id})
    return (result.rowcount or 0) > 0


def set_connection_active(
    project_id: str, connection_id: str, is_active: bool
) -> bool:
    """Toggle the is_active flag. Returns True if a row was updated."""
    stmt = text("""
        UPDATE connections SET is_active = :v
        WHERE id = :id AND project_id = :pid
    """)
    with get_engine().begin() as conn:
        result = conn.execute(stmt, {
            "id": connection_id, "pid": project_id,
            "v": 1 if is_active else 0,
        })
    return (result.rowcount or 0) > 0


# --- Collector run log (#239) ----------------------------------------------


def save_collector_run(
    run_id: str,
    project_id: str,
    connection_id: str,
    started_at: datetime,
    mode: str = "scheduled",
) -> None:
    """Create a persistent collector run row in ``running`` state."""
    stmt = text("""
        INSERT INTO collector_runs
            (id, project_id, connection_id, started_at, status, mode)
        VALUES
            (:id, :project_id, :connection_id, :started_at, 'running', :mode)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, {
            "id": run_id,
            "project_id": project_id,
            "connection_id": connection_id,
            "started_at": _iso(started_at),
            "mode": mode,
        })


def save_run_table(
    run_id: str,
    table_name: str,
    status: str,
    **kwargs,
) -> None:
    """Append one table-level result for a collector run."""
    payload = {
        "id": kwargs.get("id") or uuid.uuid4().hex,
        "run_id": run_id,
        "table_name": table_name,
        "status": status,
        "metrics_collected": int(kwargs.get("metrics_collected") or 0),
        "rows_observed": kwargs.get("rows_observed"),
        "duration_ms": kwargs.get("duration_ms"),
        "skip_reason": kwargs.get("skip_reason"),
        "error_message": kwargs.get("error_message"),
    }
    stmt = text("""
        INSERT INTO collector_run_tables
            (id, run_id, table_name, status, metrics_collected, rows_observed,
             duration_ms, skip_reason, error_message)
        VALUES
            (:id, :run_id, :table_name, :status, :metrics_collected,
             :rows_observed, :duration_ms, :skip_reason, :error_message)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)


def update_collector_run(
    run_id: str,
    *,
    status: str,
    finished_at: datetime,
    **kwargs,
) -> None:
    """Finish or update a collector run row."""
    payload = {
        "id": run_id,
        "status": status,
        "finished_at": _iso(finished_at),
        "tables_total": int(kwargs.get("tables_total") or 0),
        "tables_checked": int(kwargs.get("tables_checked") or 0),
        "tables_skipped": int(kwargs.get("tables_skipped") or 0),
        "metrics_collected": int(kwargs.get("metrics_collected") or 0),
        "duration_ms": kwargs.get("duration_ms"),
        "error_message": kwargs.get("error_message"),
    }
    stmt = text("""
        UPDATE collector_runs
        SET status = :status,
            finished_at = :finished_at,
            tables_total = :tables_total,
            tables_checked = :tables_checked,
            tables_skipped = :tables_skipped,
            metrics_collected = :metrics_collected,
            duration_ms = :duration_ms,
            error_message = :error_message
        WHERE id = :id
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)


def _parse_stored_ts(value: Any) -> datetime:
    normalized = _normalize_ts(value)
    if normalized is None:
        return datetime.now(UTC)
    parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def cleanup_stale_collector_runs(now: datetime | None = None) -> int:
    """Mark process-crashed ``running`` collector runs as failed."""
    now = now or datetime.now(UTC)
    stmt = text("""
        SELECT id, started_at
        FROM collector_runs
        WHERE status = 'running'
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt).fetchall()
    if not rows:
        return 0

    payload = []
    for run_id, started_at in rows:
        started = _parse_stored_ts(started_at)
        duration_ms = max(0, int((now - started).total_seconds() * 1000))
        payload.append({
            "id": run_id,
            "finished_at": _iso(now),
            "duration_ms": duration_ms,
            "error_message": "collector process stopped before finishing the run",
        })

    with get_engine().begin() as conn:
        conn.execute(text("""
            UPDATE collector_runs
            SET status = 'failed',
                finished_at = :finished_at,
                duration_ms = :duration_ms,
                error_message = :error_message
            WHERE id = :id AND status = 'running'
        """), payload)
    return len(payload)


def list_collector_runs(
    project_id: str,
    connection_id: str,
    limit: int = 10,
) -> list[dict]:
    """Return newest collector runs with nested table rows for the UI."""
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 10
    limit = max(1, min(limit, 100))
    runs_stmt = text("""
        SELECT id, project_id, connection_id, started_at, finished_at, status,
               mode, tables_total, tables_checked, tables_skipped,
               metrics_collected, duration_ms, error_message
        FROM collector_runs
        WHERE project_id = :project_id
          AND connection_id = :connection_id
        ORDER BY started_at DESC, id DESC
        LIMIT :limit
    """)
    with get_engine().connect() as conn:
        run_rows = conn.execute(runs_stmt, {
            "project_id": project_id,
            "connection_id": connection_id,
            "limit": limit,
        }).fetchall()
        run_ids = [r[0] for r in run_rows]
        table_rows = []
        if run_ids:
            table_rows = conn.execute(text("""
                SELECT run_id, id, table_name, status, metrics_collected,
                       rows_observed, duration_ms, skip_reason, error_message
                FROM collector_run_tables
                WHERE run_id IN :run_ids
                ORDER BY table_name
            """).bindparams(bindparam("run_ids", expanding=True)), {
                "run_ids": run_ids,
            }).fetchall()

    tables_by_run: dict[str, list[dict]] = {run_id: [] for run_id in run_ids}
    for row in table_rows:
        tables_by_run[row[0]].append({
            "id": row[1],
            "table_name": row[2],
            "status": row[3],
            "metrics_collected": int(row[4] or 0),
            "rows_observed": row[5],
            "duration_ms": row[6],
            "skip_reason": row[7],
            "error_message": row[8],
        })

    return [
        {
            "id": row[0],
            "project_id": row[1],
            "connection_id": row[2],
            "started_at": _normalize_ts(row[3]),
            "finished_at": _normalize_ts(row[4]),
            "status": row[5],
            "mode": row[6],
            "tables_total": int(row[7] or 0),
            "tables_checked": int(row[8] or 0),
            "tables_skipped": int(row[9] or 0),
            "metrics_collected": int(row[10] or 0),
            "duration_ms": row[11],
            "error_message": row[12],
            "tables": tables_by_run.get(row[0], []),
        }
        for row in run_rows
    ]


def record_successful_login(user_id: str, email: str) -> None:
    """Atomic side-effects of a successful login: stamp last_login_at AND
    clear the email's failed-login counter, both in one transaction.

    Doing this in a single ``begin()`` avoids a phantom-lockout window
    where one UPDATE commits and the other fails — the user could
    otherwise end up logged in with a stale counter that locks them out
    on their next visit.
    """
    now = _iso(datetime.now(UTC))
    with get_engine().begin() as conn:
        conn.execute(
            text("UPDATE users SET last_login_at = :ts WHERE id = :id"),
            {"id": user_id, "ts": now},
        )
        conn.execute(
            text("DELETE FROM failed_login_attempts WHERE email = :email"),
            {"email": email},
        )


def purge_old_failed_logins(retention: timedelta = timedelta(days=30)) -> int:
    """Drop failed-login records older than *retention*. Run from cron."""
    cutoff = _iso(datetime.now(UTC) - retention)
    stmt = text("DELETE FROM failed_login_attempts WHERE attempted_at < :cutoff")
    with get_engine().begin() as conn:
        result = conn.execute(stmt, {"cutoff": cutoff})
    return result.rowcount or 0
