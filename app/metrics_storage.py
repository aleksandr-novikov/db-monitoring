import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import settings

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "scripts" / "metrics_schema.sql"

_engine: Engine | None = None
_engine_lock = threading.Lock()
_initialized = False


def _new_engine() -> Engine:
    url = settings.MONITOR_DB_URL
    kwargs: dict[str, Any] = {"future": True}
    if url.startswith("sqlite"):
        kwargs["connect_args"] = {"check_same_thread": False}
    return create_engine(url, **kwargs)


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
    sql = SCHEMA_PATH.read_text()
    # Strip single-line -- comments, then split on ;. Handles both SQLite and
    # Postgres (psycopg2 does not allow multiple statements per execute()).
    stripped = "\n".join(
        line.split("--", 1)[0] for line in sql.splitlines()
    )
    statements = [s.strip() for s in stripped.split(";") if s.strip()]
    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


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
    since = _iso(datetime.now(timezone.utc) - window)
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
            "ts": r[0],
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
    return {"ts": row[0], "value": row[1], "tags": json.loads(row[2]) if row[2] else None}


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


def purge_old(retention_days: int = 90) -> int:
    """Delete metrics older than `retention_days`. Returns deleted row count."""
    cutoff = _iso(datetime.now(timezone.utc) - timedelta(days=retention_days))
    stmt = text("DELETE FROM metrics WHERE ts < :cutoff")
    with get_engine().begin() as conn:
        result = conn.execute(stmt, {"cutoff": cutoff})
    return result.rowcount or 0


def _iso(value: datetime | str) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")
