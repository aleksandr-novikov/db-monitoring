import json
import logging
import threading
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, event, text
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


def save_metrics(rows: Iterable[dict]) -> int:
    """Insert a batch of metrics. Each row: {ts, table_name, metric_name, value, tags?}."""
    payload = []
    for r in rows:
        tags = r.get("tags")
        payload.append(
            {
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
        INSERT INTO metrics (ts, table_name, metric_name, value, tags)
        VALUES (:ts, :table_name, :metric_name, :value, :tags)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_metrics(
    table_name: str,
    metric_name: str,
    window: timedelta = timedelta(days=7),
) -> list[dict]:
    """Return rows for (table, metric) within the last `window`, oldest first."""
    since = _iso(datetime.now(UTC) - window)
    stmt = text("""
        SELECT ts, value, tags
        FROM metrics
        WHERE table_name = :table_name
          AND metric_name = :metric_name
          AND ts >= :since
        ORDER BY ts
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(
            stmt,
            {"table_name": table_name, "metric_name": metric_name, "since": since},
        ).fetchall()
    return [
        {
            "ts": _normalize_ts(r[0]),
            "value": r[1],
            "tags": json.loads(r[2]) if r[2] else None,
        }
        for r in rows
    ]


def get_latest_metric(table_name: str, metric_name: str) -> dict | None:
    """Return the most recent {ts, value, tags} for (table, metric), or None."""
    stmt = text("""
        SELECT ts, value, tags
        FROM metrics
        WHERE table_name = :table_name AND metric_name = :metric_name
        ORDER BY ts DESC
        LIMIT 1
    """)
    with get_engine().connect() as conn:
        row = conn.execute(
            stmt, {"table_name": table_name, "metric_name": metric_name}
        ).fetchone()
    if not row:
        return None
    return {
        "ts": _normalize_ts(row[0]),
        "value": row[1],
        "tags": json.loads(row[2]) if row[2] else None,
    }


def get_latest_null_counts(table_name: str) -> dict[str, int]:
    """Return {column: null_count} from the most recent collector run for a table.

    Reads stored `null_count` metrics tagged by column — never live-scans the
    monitored DB. Returns an empty dict when the collector has not run yet.
    """
    stmt = text("""
        SELECT tags, value
        FROM metrics
        WHERE table_name = :table_name
          AND metric_name = 'null_count'
          AND ts = (
              SELECT MAX(ts) FROM metrics
              WHERE table_name = :table_name AND metric_name = 'null_count'
          )
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"table_name": table_name}).fetchall()
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


def save_changepoints(rows: Iterable[dict]) -> int:
    """Persist detected change-points with cross-run cluster deduplication.

    Within a ±72 h window, at most one record is kept per
    (table, metric, direction).  A new detection replaces the stored one only
    when its score is strictly higher, so the best signal for each real shift
    survives successive hourly runs.  This complements the within-run
    deduplication already done by ml/changepoint._dedupe.
    """
    payload = []
    detected_at = _iso(datetime.now(UTC))
    for r in rows:
        payload.append({
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
        ["ts", "table_name", "metric_name", "score",
         "value_before", "value_after", "detected_at"],
        conflict_columns=["ts", "table_name", "metric_name"],
    ))
    # Dialect-specific time delta: julianday is SQLite-only. On Postgres the
    # ts column is TIMESTAMPTZ and we compute hours via EXTRACT(EPOCH).
    # Delete by the changepoints PK — identical on both dialects.
    delete_stmt = text("""
        DELETE FROM changepoints
        WHERE ts = :ts AND table_name = :t AND metric_name = :m
    """)
    if _is_postgres():
        cluster_query = text("""
            SELECT ts, score FROM changepoints
            WHERE table_name = :t
              AND metric_name = :m
              AND ABS(EXTRACT(EPOCH FROM (ts - CAST(:ts AS TIMESTAMPTZ))) / 3600) <= :w
              AND (CASE WHEN value_after > value_before THEN 1 ELSE 0 END) = :dir
        """)
    else:
        cluster_query = text("""
            SELECT ts, score FROM changepoints
            WHERE table_name = :t
              AND metric_name = :m
              AND ABS(julianday(ts) - julianday(:ts)) * 24 <= :w
              AND (CASE WHEN value_after > value_before THEN 1 ELSE 0 END) = :dir
        """)

    saved = 0
    with get_engine().begin() as conn:
        for row in payload:
            direction = 1 if row["value_after"] > row["value_before"] else 0
            existing = conn.execute(cluster_query, {
                "t": row["table_name"],
                "m": row["metric_name"],
                "ts": row["ts"],
                "w": _CLUSTER_WINDOW_HOURS,
                "dir": direction,
            }).fetchall()

            if not existing:
                conn.execute(insert_stmt, row)
                saved += 1
            elif row["score"] > max(r.score for r in existing):
                for old in existing:
                    conn.execute(
                        delete_stmt,
                        {"ts": old.ts, "t": row["table_name"], "m": row["metric_name"]},
                    )
                conn.execute(insert_stmt, row)
                saved += 1

    return saved


def get_changepoints(
    table_name: str,
    metric_name: str | None = None,
    window: timedelta = timedelta(days=14),
) -> list[dict]:
    """Return change-points for a table, oldest first. `metric_name=None`
    returns all metrics; otherwise filters."""
    since = _iso(datetime.now(UTC) - window)
    base = """
        SELECT ts, table_name, metric_name, score, value_before, value_after
        FROM changepoints
        WHERE table_name = :table_name AND ts >= :since
    """
    params: dict[str, Any] = {"table_name": table_name, "since": since}
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


def get_schema_snapshot(table_name: str) -> list[dict] | None:
    """Latest stored column list for a table, or None if no snapshot yet."""
    stmt = text("SELECT columns FROM schema_snapshots WHERE table_name = :t")
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"t": table_name}).fetchone()
    if not row:
        return None
    return json.loads(row[0])


def save_schema_snapshot(table_name: str, columns: list[dict]) -> None:
    """Replace the stored snapshot for a table."""
    sql = _upsert_sql(
        "schema_snapshots",
        ["table_name", "columns", "captured_at"],
        conflict_columns=["table_name"],
    )
    with get_engine().begin() as conn:
        conn.execute(text(sql), {
            "table_name": table_name,
            "columns": json.dumps(columns),
            "captured_at": _iso(datetime.now(UTC)),
        })


def save_schema_events(events: Iterable[dict]) -> int:
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
        })
    if not payload:
        return 0
    stmt = text("""
        INSERT INTO schema_events (ts, table_name, change_type, column_name, details)
        VALUES (:ts, :table_name, :change_type, :column_name, :details)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_schema_events(
    table_name: str, window: timedelta = timedelta(days=30)
) -> list[dict]:
    """Recent schema-drift events for a table, newest first."""
    since = _iso(datetime.now(UTC) - window)
    stmt = text("""
        SELECT ts, table_name, change_type, column_name, details
        FROM schema_events
        WHERE table_name = :t AND ts >= :since
        ORDER BY ts DESC
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"t": table_name, "since": since}).fetchall()
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


def save_anomaly_scores(rows: Iterable[dict]) -> int:
    """Upsert anomaly scores. Each row: {ts, table_name, score, is_anomaly}."""
    payload = []
    for r in rows:
        payload.append({
            "ts": _iso(r["ts"]),
            "table_name": r["table_name"],
            "score": float(r["score"]),
            "is_anomaly": int(r["is_anomaly"]),
        })
    if not payload:
        return 0
    stmt = text(_upsert_sql(
        "anomaly_scores",
        ["ts", "table_name", "score", "is_anomaly"],
        conflict_columns=["ts", "table_name"],
    ))
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_anomaly_scores(
    table_name: str, window: timedelta = timedelta(days=7)
) -> list[dict]:
    """Return anomaly scores for a table within *window*, oldest first."""
    since = _iso(datetime.now(UTC) - window)
    stmt = text("""
        SELECT ts, score, is_anomaly
        FROM anomaly_scores
        WHERE table_name = :t AND ts >= :since
        ORDER BY ts
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"t": table_name, "since": since}).fetchall()
    return [
        {"ts": _normalize_ts(r[0]), "score": r[1], "is_anomaly": r[2]}
        for r in rows
    ]


def save_drift_reports(table_name: str, rows: Iterable[dict]) -> int:
    """Полностью переписать кеш drift для одной таблицы.

    Рассчитанный снапшот PSI/KS — слайд по 7-дневному окну, поэтому хранить
    историю не нужно: пересчёт раз в тик всё равно убивает старое значение.
    """
    payload = []
    computed_at = _iso(datetime.now(UTC))
    for r in rows:
        payload.append({
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
            text("DELETE FROM drift_reports WHERE table_name = :t"),
            {"t": table_name},
        )
        if payload:
            conn.execute(
                text("""
                    INSERT INTO drift_reports
                        (table_name, column_name, data_type, psi, ks_pvalue,
                         is_drift, severity, computed_at)
                    VALUES (:table_name, :column_name, :data_type, :psi,
                            :ks_pvalue, :is_drift, :severity, :computed_at)
                """),
                payload,
            )
    return len(payload)


def get_drift_report(table_name: str) -> list[dict]:
    """Кешированный drift по таблице, отсортированный по убыванию PSI."""
    stmt = text("""
        SELECT column_name, data_type, psi, ks_pvalue, is_drift, severity
        FROM drift_reports
        WHERE table_name = :t
        ORDER BY COALESCE(psi, 0) DESC
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"t": table_name}).fetchall()
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


def purge_old(retention_days: int = 90) -> int:
    """Drop metrics older than ``retention_days``.

    On TimescaleDB we first call ``drop_chunks`` (whole-chunk drop — orders of
    magnitude faster than per-row DELETE) and then DELETE any straggler rows
    older than the cutoff but living in the chunk that straddles the cutoff
    boundary. On plain Postgres or SQLite only the DELETE runs.

    Returns the row count from the trailing DELETE only — ``drop_chunks``
    does not expose a row count, so the return value undercounts on
    TimescaleDB. The retention contract is "no metrics older than
    ``retention_days`` remain after the call", not "the integer equals total
    rows removed".

    Two separate transactions: drop_chunks commits, then DELETE runs. A
    concurrent writer can insert a row with ``ts < cutoff`` in between and
    survive; the next purge picks it up.
    """
    cutoff_dt = datetime.now(UTC) - timedelta(days=retention_days)
    cutoff = _iso(cutoff_dt)
    engine = get_engine()
    if _is_postgres():
        # drop_chunks runs in its own connection so a failure (plain Postgres
        # without the TimescaleDB extension) doesn't poison the outer txn.
        try:
            with engine.begin() as conn:
                dropped = conn.execute(
                    text("SELECT drop_chunks('metrics', CAST(:cutoff AS TIMESTAMPTZ))"),
                    {"cutoff": cutoff},
                ).fetchall()
            # drop_chunks returns one row per dropped chunk (regclass name).
            # The trailing DELETE rowcount alone undercounts on Timescale,
            # so log the chunk count for ops visibility.
            if dropped:
                logger.info(
                    "purge_old: dropped %d Timescale chunks older than %s",
                    len(dropped), cutoff,
                )
        except ProgrammingError as e:
            # Only swallow "function does not exist" (SQLSTATE 42883) — that
            # means the TimescaleDB extension is not installed and we fall
            # back to the plain DELETE. Any other ProgrammingError (locks,
            # continuous-aggregate dependencies, …) must propagate so it
            # gets surfaced instead of silently skipped.
            sqlstate = getattr(getattr(e, "orig", None), "pgcode", None)
            if sqlstate != "42883":
                raise
    with engine.begin() as conn:
        result = conn.execute(
            text("DELETE FROM metrics WHERE ts < :cutoff"),
            {"cutoff": cutoff},
        )
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


def _fetch_history_metric_rows(window: timedelta | None = timedelta(days=30)) -> list[dict]:
    """Fetch row_count/null_rate rows used by the history page."""
    params: dict[str, Any] = {}
    where = "WHERE metric_name IN ('row_count', 'null_rate')"
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
        return _fetch_history_metric_rows(window=None)

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


def _fetch_anomalies_by_ts(window: timedelta | None = timedelta(days=30)) -> dict[str, int]:
    """Count IF anomalies per collector tick within the window."""
    params: dict[str, Any] = {}
    where = "WHERE is_anomaly = 1"
    if window is not None:
        params["since"] = (datetime.now(UTC) - window).isoformat()
        where += " AND ts >= :since"
    stmt = text(f"SELECT ts, COUNT(*) FROM anomaly_scores {where} GROUP BY ts")
    with get_engine().connect() as conn:
        return {
            _normalize_ts(r[0]): int(r[1])
            for r in conn.execute(stmt, params).fetchall()
        }


def _history_aggregate(window: timedelta | None = timedelta(days=30)) -> dict:
    """Build reusable aggregates from metrics table.

    A collector run is represented by a timestamp `ts`.
    Problems: null_rate >= 10%.
    Null spikes: null_rate jump by >= 5 pp compared with the previous run
    for the same table/column metric. Rule-based heuristic on raw NULL rate.
    Anomalies: IF model verdicts from anomaly_scores keyed by the same ts.
    Coverage: checked tables / known tables.
    """
    rows = _fetch_history_metric_rows(window=window)
    anomalies_by_ts = _fetch_anomalies_by_ts(window=window)

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


def build_history_aggregate(window: timedelta = timedelta(days=30)) -> dict:
    """Compute a single aggregate for the History page.

    Call once per request and pass the result to get_history_runs,
    get_history_daily, and get_history_insights to avoid repeated DB queries.
    """
    return _history_aggregate(window=window)


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


# --- Telegram notification throttle (#38) ---

def is_throttled(table: str, event_key: str) -> bool:
    """Return True if a notification for (table, event_key) was sent within the throttle window."""
    stmt = text("""
        SELECT last_sent_at FROM telegram_throttle
        WHERE table_name = :table AND event_key = :key
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"table": table, "key": event_key}).fetchone()
    if not row:
        return False
    raw = row[0]
    if isinstance(raw, datetime):
        last_sent = raw if raw.tzinfo else raw.replace(tzinfo=UTC)
    else:
        last_sent = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        if last_sent.tzinfo is None:
            last_sent = last_sent.replace(tzinfo=UTC)
    age = datetime.now(UTC) - last_sent
    return age.total_seconds() < settings.TELEGRAM_THROTTLE_MINUTES * 60


def update_throttle(table: str, event_key: str) -> None:
    """Record that a notification for (table, event_key) was just sent."""
    stmt = text(_upsert_sql(
        "telegram_throttle",
        ["table_name", "event_key", "last_sent_at"],
        conflict_columns=["table_name", "event_key"],
    ))
    with get_engine().begin() as conn:
        conn.execute(stmt, {
            "table_name": table,
            "event_key": event_key,
            "last_sent_at": _iso(datetime.now(UTC)),
        })


# --- Notification history (#76) ---

_NOTIFICATION_EVENT_TYPES = {
    "anomaly", "schema_drift", "changepoint", "forecast", "root_cause",
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
) -> int:
    """Store a Telegram notification record. Returns the new row id.

    Called from notify_* helpers regardless of delivery outcome — failures are
    persisted with status='failed' so the UI can show a complete audit trail.
    """
    payload = {
        "ts": _iso(ts or datetime.now(UTC)),
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
            (ts, event_type, table_name, metric_name, message, status, error, chat_id)
        VALUES
            (:ts, :event_type, :table_name, :metric_name, :message, :status, :error, :chat_id)
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
) -> list[dict]:
    """Return notification history, newest first, with optional filters.

    Pagination via limit/offset; defaults give a sane single-page response for
    the UI without scanning the full table.
    """
    where = ["1=1"]
    params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
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
        SELECT id, ts, event_type, table_name, metric_name, message, status, error, chat_id
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
) -> int:
    """Total notifications matching filters — used for pagination metadata."""
    where = ["1=1"]
    params: dict[str, Any] = {}
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
    }


def get_user_by_email(email: str) -> dict | None:
    stmt = text("""
        SELECT id, email, password_hash, created_at, last_login_at
        FROM users WHERE email = :email
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"email": email}).fetchone()
    return _row_to_user(row)


def get_user_by_id(user_id: str) -> dict | None:
    stmt = text("""
        SELECT id, email, password_hash, created_at, last_login_at
        FROM users WHERE id = :id
    """)
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"id": user_id}).fetchone()
    return _row_to_user(row)


def update_last_login(user_id: str) -> None:
    """Stamp last_login_at = now for the given user. No-op if user is gone."""
    stmt = text("UPDATE users SET last_login_at = :ts WHERE id = :id")
    with get_engine().begin() as conn:
        conn.execute(stmt, {"id": user_id, "ts": _iso(datetime.now(UTC))})


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
    """Insert a new project. Raises ProjectSlugTaken on UNIQUE violation.

    Slug must already be normalised (lower-cased, URL-safe) by the caller.
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
    stmt = text(
        "INSERT INTO projects (id, user_id, name, slug, created_at) "
        "VALUES (:id, :user_id, :name, :slug, :created_at)"
    )
    try:
        with get_engine().begin() as conn:
            conn.execute(stmt, payload)
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
    """Scoped to user — never returns another user's project even on slug match."""
    stmt = text(
        "SELECT id, user_id, name, slug, created_at FROM projects "
        "WHERE user_id = :user_id AND slug = :slug"
    )
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"user_id": user_id, "slug": slug}).fetchone()
    return _row_to_project(row)


def get_project_by_id(user_id: str, project_id: str) -> dict | None:
    """Scoped to user. Returns None if the project belongs to someone else
    even when the id is correct — defence against horizontal escalation."""
    stmt = text(
        "SELECT id, user_id, name, slug, created_at FROM projects "
        "WHERE id = :id AND user_id = :user_id"
    )
    with get_engine().connect() as conn:
        row = conn.execute(stmt, {"id": project_id, "user_id": user_id}).fetchone()
    return _row_to_project(row)


def list_projects_for_user(user_id: str) -> list[dict]:
    stmt = text(
        "SELECT id, user_id, name, slug, created_at FROM projects "
        "WHERE user_id = :user_id ORDER BY created_at"
    )
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"user_id": user_id}).fetchall()
    return [_row_to_project(r) for r in rows]


def delete_project(user_id: str, project_id: str) -> bool:
    """Hard delete. Returns True if a row was removed (i.e. the project
    existed and belonged to this user)."""
    stmt = text(
        "DELETE FROM projects WHERE id = :id AND user_id = :user_id"
    )
    with get_engine().begin() as conn:
        result = conn.execute(stmt, {"id": project_id, "user_id": user_id})
    return (result.rowcount or 0) > 0


# --- /Projects -------------------------------------------------------------


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
    }


def list_connections_for_project(project_id: str) -> list[dict]:
    stmt = text("""
        SELECT id, project_id, name, dsn_encrypted, schema_name,
               interval_minutes, is_active, created_at
        FROM connections WHERE project_id = :pid ORDER BY created_at
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"pid": project_id}).fetchall()
    return [_row_to_connection(r) for r in rows]


def get_connection(project_id: str, connection_id: str) -> dict | None:
    """Scoped to project — never returns a connection from a different
    project even when the id is guessable. Defends against horizontal
    escalation via id-in-URL."""
    stmt = text("""
        SELECT id, project_id, name, dsn_encrypted, schema_name,
               interval_minutes, is_active, created_at
        FROM connections WHERE id = :id AND project_id = :pid
    """)
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

