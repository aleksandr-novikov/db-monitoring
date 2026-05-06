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


def save_changepoints(rows: Iterable[dict]) -> int:
    """Upsert detected change-points. Each row: {ts, table_name, metric_name,
    score, value_before, value_after}.

    The PRIMARY KEY (ts, table, metric) collapses repeats from successive
    detection runs, so the table grows linearly with distinct events rather
    than with hourly polls.
    """
    payload = []
    detected_at = _iso(datetime.now(timezone.utc))
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
    # `INSERT OR REPLACE` is the SQLite spelling; Postgres has the same
    # behaviour via `ON CONFLICT DO UPDATE`. We only run on SQLite today.
    stmt = text("""
        INSERT OR REPLACE INTO changepoints
            (ts, table_name, metric_name, score, value_before, value_after, detected_at)
        VALUES (:ts, :table_name, :metric_name, :score, :value_before, :value_after, :detected_at)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_changepoints(
    table_name: str,
    metric_name: str | None = None,
    window: timedelta = timedelta(days=14),
) -> list[dict]:
    """Return change-points for a table, oldest first. `metric_name=None`
    returns all metrics; otherwise filters."""
    since = _iso(datetime.now(timezone.utc) - window)
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
            "ts": r[0],
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
    stmt = text("""
        INSERT OR REPLACE INTO schema_snapshots (table_name, columns, captured_at)
        VALUES (:t, :cols, :ts)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, {
            "t": table_name,
            "cols": json.dumps(columns),
            "ts": _iso(datetime.now(timezone.utc)),
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
    since = _iso(datetime.now(timezone.utc) - window)
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
            "ts": r[0],
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
    stmt = text("""
        INSERT OR REPLACE INTO anomaly_scores (ts, table_name, score, is_anomaly)
        VALUES (:ts, :table_name, :score, :is_anomaly)
    """)
    with get_engine().begin() as conn:
        conn.execute(stmt, payload)
    return len(payload)


def get_anomaly_scores(
    table_name: str, window: timedelta = timedelta(days=7)
) -> list[dict]:
    """Return anomaly scores for a table within *window*, oldest first."""
    since = _iso(datetime.now(timezone.utc) - window)
    stmt = text("""
        SELECT ts, score, is_anomaly
        FROM anomaly_scores
        WHERE table_name = :t AND ts >= :since
        ORDER BY ts
    """)
    with get_engine().connect() as conn:
        rows = conn.execute(stmt, {"t": table_name, "since": since}).fetchall()
    return [{"ts": r[0], "score": r[1], "is_anomaly": r[2]} for r in rows]


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


# --- History page helpers (#41) ---

_PROBLEM_NULL_RATE = 0.10
_CRITICAL_NULL_RATE = 0.30
_ANOMALY_NULL_RATE_DELTA = 0.05


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
        params["since"] = (datetime.now(timezone.utc) - window).isoformat()
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
            "ts": r[0],
            "table_name": r[1],
            "metric_name": r[2],
            "value": _safe_float(r[3]),
            "tags": r[4],
        }
        for r in rows
    ]


def _history_aggregate(window: timedelta | None = timedelta(days=30)) -> dict:
    """Build reusable aggregates from metrics table.

    A collector run is represented by a timestamp `ts`.
    Problems: null_rate >= 10%.
    Anomalies: null_rate jump by >= 5 percentage points compared with the previous run
    for the same table/column metric.
    Coverage: checked tables / known tables.
    """
    rows = _fetch_history_metric_rows(window=window)

    tables_by_ts: dict[str, set[str]] = {}
    problems_by_ts: dict[str, int] = {}
    anomalies_by_ts: dict[str, int] = {}
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

    for values in null_rates_by_identity.values():
        previous: float | None = None
        for ts, value, _tags in sorted(values, key=lambda x: x[0]):
            if previous is not None and (value - previous) >= _ANOMALY_NULL_RATE_DELTA:
                anomalies_by_ts[ts] = anomalies_by_ts.get(ts, 0) + 1
            previous = value

    total_tables = len(known_tables)
    sorted_timestamps = sorted(timestamps)

    return {
        "rows": rows,
        "timestamps": sorted_timestamps,
        "tables_by_ts": tables_by_ts,
        "problems_by_ts": problems_by_ts,
        "anomalies_by_ts": anomalies_by_ts,
        "total_tables": total_tables,
    }


def get_history_runs(limit: int = 10) -> list[dict]:
    """Return latest collector runs for the History page."""
    agg = _history_aggregate(window=timedelta(days=30))
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
                "anomalies": agg["anomalies_by_ts"].get(ts, 0),
                "coverage_pct": coverage_pct,
            }
        )
    return runs


def get_history_daily(days: int = 14) -> list[dict]:
    """Return daily trend for problems, anomalies and coverage.

    For each day we use the latest collector run of that day, so the chart shows
    end-of-day state rather than a raw count of all hourly/15-min checks.
    """
    agg = _history_aggregate(window=timedelta(days=days))
    total_tables = agg["total_tables"]
    latest_ts_by_day: dict[str, str] = {}

    for ts in agg["timestamps"]:
        day = ts[:10]
        latest_ts_by_day[day] = ts

    daily: list[dict] = []
    for day in sorted(latest_ts_by_day):
        ts = latest_ts_by_day[day]
        checked_tables = len(agg["tables_by_ts"].get(ts, set()))
        coverage_pct = round((checked_tables / total_tables) * 100, 1) if total_tables else 0.0
        daily.append(
            {
                "date": day,
                "problems": agg["problems_by_ts"].get(ts, 0),
                "anomalies": agg["anomalies_by_ts"].get(ts, 0),
                "coverage_pct": coverage_pct,
            }
        )
    return daily


def get_history_insights() -> list[str]:
    """Rule-based text conclusions for the History page."""
    agg = _history_aggregate(window=timedelta(days=30))
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

    # Biggest current risk
    if latest_null_rates:
        worst = max(latest_null_rates, key=lambda r: r["value"] or 0)
        if worst["value"] is not None and worst["value"] >= _PROBLEM_NULL_RATE:
            insights.append(
                f"Таблица/поле { _metric_label(worst['table_name'], worst['tags']) } требует проверки: "
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
        for key, latest in latest_by_key.items():
            prev = prev_by_key.get(key)
            if not prev:
                continue
            delta = latest["value"] - prev["value"]
            if best_growth is None or delta > best_growth[0]:
                best_growth = (delta, prev, latest)

        if best_growth and best_growth[0] >= 0.01:
            _delta, prev, latest = best_growth
            insights.append(
                f"Самый заметный рост пропусков: { _metric_label(latest['table_name'], latest['tags']) } "
                f"с {_pct(prev['value'])} до {_pct(latest['value'])}."
            )

    return insights[:4]

