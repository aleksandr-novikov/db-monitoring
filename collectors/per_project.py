"""Per-connection scheduled metric collection (#54).

Replaces the single global ``collect_all_tables`` job (which writes to the
'legacy' tenant) with one APScheduler job per active connection. Each job:

- Lives at ``collect:<project_id>:<connection_id>`` so admin tooling and
  logs can identify the tenant at a glance.
- Runs at the connection's own ``interval_minutes`` (5..1440 from #51).
- Uses ``app.db.using_engine`` to override the global engine/adapter for
  the duration of the tick — the existing collector + adapter code does
  not need to know about per-project mode.
- Tags every metric with the correct ``project_id`` via #53.

Lifecycle hooks:
- ``register_jobs_for_all_active_connections(scheduler)`` — called once at
  app startup from ``collectors/scheduler.py``.
- ``add_job_for_connection(scheduler, project_id, connection)`` — called by
  the connections blueprint on create / activate / interval change.
- ``remove_job_for_connection(scheduler, project_id, connection_id)`` —
  called on delete / deactivate.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime

from apscheduler.schedulers.base import BaseScheduler

from app import crypto
from app.db import make_adapter_for_url, using_engine
from app.metrics_storage import (
    get_connection,
    list_projects_for_user,
    save_collector_run,
    save_metrics,
    save_run_table,
    update_collector_run,
)
from app.security import scrub_value
from collectors.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)


# --- #232 load-safety helpers ---------------------------------------------


def _parse_table_list(raw: str | None) -> list[str]:
    """Decode a stored allow/denylist into a list of table names.

    The column is plain TEXT holding a JSON array — kept as JSON rather than
    a comma-split string so a future name with a comma (legal in Postgres
    via quoting) doesn't silently split into two entries. Any failure (None,
    empty string, bad JSON, non-list, non-string entries) collapses to ``[]``
    so an operator typo in the DB doesn't take the collector down — a
    malformed list reads as "no constraint" and we log once.
    """
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        logger.warning(
            "invalid JSON in connection safety list, treating as empty: %r", raw,
        )
        return []
    if not isinstance(parsed, list):
        logger.warning(
            "connection safety list must be a JSON array, got %s", type(parsed).__name__,
        )
        return []
    return [str(x).strip() for x in parsed if isinstance(x, str)]


def _apply_table_filters(
    tables: list[dict], conn_row: dict,
) -> tuple[list[dict], list[tuple[str, str]]]:
    """Apply allowlist → denylist → max_tables_per_tick.

    Returns (kept, skipped) where *skipped* is a list of (table_name, reason)
    so the caller can log every drop with a stable reason code:
    ``not_in_allowlist`` | ``denylisted`` | ``max_tables_limit``.

    Order matters and is fixed by spec:
      1. Sort by table_name for deterministic output across ticks.
      2. Drop names not in allowlist (empty allowlist = no constraint).
      3. Drop names in denylist (empty denylist = no constraint).
      4. Cap at max_tables_per_tick AFTER both lists — the cap should
         apply to the user-intended set, not to the catalog-order prefix.
    """
    allowlist = set(_parse_table_list(conn_row.get("table_allowlist")))
    denylist = set(_parse_table_list(conn_row.get("table_denylist")))
    max_tables = conn_row.get("max_tables_per_tick")

    ordered = sorted(tables, key=lambda t: t["table_name"])
    kept: list[dict] = []
    skipped: list[tuple[str, str]] = []
    for t in ordered:
        name = t["table_name"]
        if allowlist and name not in allowlist:
            skipped.append((name, "not_in_allowlist"))
            continue
        if name in denylist:
            skipped.append((name, "denylisted"))
            continue
        kept.append(t)

    if max_tables is not None and max_tables >= 0 and len(kept) > max_tables:
        for t in kept[max_tables:]:
            skipped.append((t["table_name"], "max_tables_limit"))
        kept = kept[:max_tables]
    return kept, skipped


def _build_engine(dsn: str, statement_timeout_ms: int | None):
    """Create the per-tick SQLAlchemy engine.

    Extracted so tests can introspect the connect_args without driving a
    full collection tick. ``statement_timeout_ms`` is applied via the
    libpq ``options`` parameter (session-scoped on every connection of
    this engine) on Postgres only — for ClickHouse / MySQL / Iceberg the
    parameter is meaningless and we leave the connect_args minimal.

    Note on SET LOCAL: the issue spec suggested ``SET LOCAL
    statement_timeout`` from a separate transaction, but SET LOCAL is
    scoped to its transaction and the adapter opens its own connections
    per query — so a one-shot SET LOCAL never reaches them. ``-c
    statement_timeout=...`` in the connect options applies session-wide
    and survives across the adapter's queries.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    connect_args: dict = {"connect_timeout": 5}
    if dsn.lower().startswith(("postgres://", "postgresql://", "postgresql+")) \
            and statement_timeout_ms and statement_timeout_ms > 0:
        connect_args["options"] = (
            f"-c statement_timeout={int(statement_timeout_ms)}"
        )
    return create_engine(dsn, poolclass=NullPool, connect_args=connect_args)


def _is_postgres_dsn(dsn: str) -> bool:
    return dsn.lower().startswith(("postgres://", "postgresql://", "postgresql+"))

# `prefix` so admin tooling and grep can spot a per-connection job at sight
# without parsing the id structure.
JOB_PREFIX = "collect:"


def job_id_for(project_id: str, connection_id: str) -> str:
    """Stable APScheduler id for the (project, connection) pair."""
    return f"{JOB_PREFIX}{project_id}:{connection_id}"


def parse_job_id(job_id: str) -> tuple[str, str] | None:
    """Reverse ``job_id_for`` — returns (project_id, connection_id) or None
    if the id isn't one of ours (e.g. global jobs from collectors/scheduler.py).
    """
    if not job_id.startswith(JOB_PREFIX):
        return None
    parts = job_id[len(JOB_PREFIX):].split(":", 1)
    if len(parts) != 2:
        return None
    return parts[0], parts[1]


# --- The job body ---------------------------------------------------------


def _inc_collector_error_counter() -> None:
    """Best-effort Prometheus error counter bump."""
    try:
        from app.instrumentation import collector_runs_total
        collector_runs_total.labels(result="error").inc()
    except ImportError:
        pass


def collect_for_connection(project_id: str, connection_id: str) -> None:
    """Single tick: enumerate tables on the connection's DSN, save metrics
    tagged with the project_id.

    Errors are caught + logged (the job runs hourly-ish; one bad tick must
    not poison the whole scheduler). Plaintext DSN never reaches a log —
    ``app.security.DSNFilter`` from #56 scrubs at LogRecord construction.
    """
    started = time.monotonic()
    run_started_at = datetime.now(UTC)
    run_id = uuid.uuid4().hex
    run_created = False
    engine = None
    rows_saved = 0
    tables_seen = 0
    tables_checked = 0
    tables_skipped = 0
    degraded = False
    run_status = "success"
    run_error: str | None = None
    table_names: list[str] = []

    conn_row = get_connection(project_id, connection_id)
    if conn_row is None:
        logger.info("[project=%s][conn=%s] skipped — inactive/deleted",
                    project_id, connection_id)
        return

    save_collector_run(
        run_id,
        project_id=project_id,
        connection_id=connection_id,
        started_at=run_started_at,
        mode="scheduled",
    )
    run_created = True

    if not conn_row["is_active"]:
        logger.info("[project=%s][conn=%s] skipped — inactive/deleted",
                    project_id, connection_id)
        update_collector_run(
            run_id,
            status="skipped",
            finished_at=datetime.now(UTC),
            duration_ms=int((time.monotonic() - started) * 1000),
            error_message="connection inactive or deleted",
        )
        return

    try:
        dsn = crypto.decrypt_dsn(conn_row["dsn_encrypted"])
    except crypto.InvalidToken:
        run_error = scrub_value(
            "DSN ciphertext invalid — Fernet key rotated? Re-save the connection."
        )
        logger.warning("[project=%s][conn=%s] %s",
                       project_id, connection_id, run_error)
        update_collector_run(
            run_id,
            status="failed",
            finished_at=datetime.now(UTC),
            duration_ms=int((time.monotonic() - started) * 1000),
            error_message=run_error,
        )
        _inc_collector_error_counter()
        return

    # #234: decrypt Iceberg auth token (if stored) and pass with warehouse
    # so the adapter uses the production-rotated value, not just the DSN.
    iceberg_token: str | None = None
    if conn_row.get("iceberg_auth_token_encrypted"):
        try:
            iceberg_token = crypto.decrypt_token(
                conn_row["iceberg_auth_token_encrypted"],
            )
        except crypto.InvalidToken:
            run_error = scrub_value(
                "Iceberg auth token ciphertext invalid",
            )
            logger.warning(
                "[project=%s][conn=%s] %s",
                project_id, connection_id, run_error,
            )
            update_collector_run(
                run_id,
                status="failed",
                finished_at=datetime.now(UTC),
                duration_ms=int((time.monotonic() - started) * 1000),
                error_message=run_error,
            )
            _inc_collector_error_counter()
            return

    try:
        # Iceberg uses a catalog API (not SQLAlchemy) — skip engine creation.
        # For all other dialects, create a per-tick NullPool engine so
        # connections don't leak across scheduler runs. _build_engine wires
        # the #232 statement_timeout for Postgres at the libpq options layer;
        # ClickHouse / Iceberg ignore the value (spec).
        if not dsn.lower().startswith("iceberg+"):
            engine = _build_engine(dsn, conn_row.get("statement_timeout_ms"))
        adapter = make_adapter_for_url(
            dsn,
            warehouse=conn_row.get("iceberg_warehouse"),
            auth_token=iceberg_token,
        )
    except Exception as exc:
        run_error = scrub_value(exc)
        logger.warning("[project=%s][conn=%s] %s",
                       project_id, connection_id, run_error)
        update_collector_run(
            run_id,
            status="failed",
            finished_at=datetime.now(UTC),
            duration_ms=int((time.monotonic() - started) * 1000),
            error_message=run_error,
        )
        if engine:
            engine.dispose()
        _inc_collector_error_counter()
        return

    run_ts = datetime.now(UTC)
    # #234: for Iceberg, iceberg_namespace overrides schema_name.
    # effective_namespace = iceberg_namespace or schema_name — preserves
    # back-compat for legacy connections without the new field.
    schema = conn_row.get("iceberg_namespace") or conn_row["schema_name"]
    # #232: cache dialect + threshold once — applied to every table below.
    is_postgres = _is_postgres_dsn(dsn)
    skip_larger_than_gb = conn_row.get("skip_tables_larger_than_gb")
    try:
        with using_engine(engine, adapter):
            from collectors.schema_collector import collect_table_schema

            # #232: apply allow/deny/cap filters and log each skipped table
            # to the #239 run-log so the operator sees WHY it was skipped.
            raw_tables = adapter.list_tables(schema)
            tables, skipped = _apply_table_filters(raw_tables, conn_row)
            tables_seen = len(tables) + len(skipped)
            for name, reason in skipped:
                logger.info(
                    "[project=%s][conn=%s] skip %s: %s",
                    project_id, connection_id, name, reason,
                )
                tables_skipped += 1
                save_run_table(
                    run_id, name, "skipped",
                    skip_reason=reason, duration_ms=0,
                )

            collector = MetricsCollector(schema=schema)
            for table in tables:
                table_name = table["table_name"]
                table_names.append(table_name)
                table_started = time.monotonic()
                metrics_collected = 0
                rows_observed = None
                table_status = "success"
                table_error = None
                table_skip_reason: str | None = None

                # #232 large-table early-skip (Postgres only). Cheap
                # pg_stat_user_tables read; if over threshold, never enter
                # the heavy column_nulls path. Recorded as skipped in the
                # #239 run log with skip_reason='too_large'.
                if is_postgres and skip_larger_than_gb is not None:
                    stats = adapter.table_stats(table_name, schema)
                    threshold_bytes = float(skip_larger_than_gb) * 1e9
                    if stats and stats["size_bytes"] > threshold_bytes:
                        logger.info(
                            "[project=%s][conn=%s] skip %s: too_large "
                            "(%.2f GB > %.2f GB)",
                            project_id, connection_id, table_name,
                            stats["size_bytes"] / 1e9,
                            float(skip_larger_than_gb),
                        )
                        table_status = "skipped"
                        table_skip_reason = "too_large"
                        tables_skipped += 1
                        save_run_table(
                            run_id, table_name, "skipped",
                            skip_reason=table_skip_reason,
                            duration_ms=int(
                                (time.monotonic() - table_started) * 1000,
                            ),
                        )
                        continue

                try:
                    metrics = collector.collect(table_name, ts=run_ts)
                    metrics_collected = len(metrics)
                    row_count_metric = next(
                        (m for m in metrics if m.get("metric_name") == "row_count"),
                        None,
                    )
                    if row_count_metric is not None:
                        rows_observed = int(row_count_metric["value"])
                    if metrics:
                        rows_saved += save_metrics(metrics, project_id)
                    try:
                        collect_table_schema(
                            table_name, schema=schema, project_id=project_id,
                        )
                    except Exception as schema_exc:
                        degraded = True
                        table_status = "failed"
                        table_error = scrub_value(schema_exc)
                        logger.warning(
                            "[project=%s][conn=%s] schema collection failed for %s: %s",
                            project_id, connection_id, table_name, table_error,
                        )
                except Exception as table_exc:
                    degraded = True
                    table_status = "failed"
                    table_error = scrub_value(table_exc)
                    logger.warning(
                        "[project=%s][conn=%s] table collection failed for %s: %s",
                        project_id, connection_id, table_name, table_error,
                    )
                finally:
                    if table_status != "skipped":
                        tables_checked += 1
                    save_run_table(
                        run_id,
                        table_name,
                        table_status,
                        metrics_collected=metrics_collected,
                        rows_observed=rows_observed,
                        duration_ms=int((time.monotonic() - table_started) * 1000),
                        error_message=table_error,
                    )
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        run_status = "failed"
        run_error = scrub_value(exc)
        logger.warning(
            "[project=%s][conn=%s] collection failed after %dms: %s",
            project_id, connection_id, elapsed_ms, run_error,
        )
        # #101: count the failed tick. Late import so a missing
        # prometheus-client install doesn't break collection itself.
        _inc_collector_error_counter()
    finally:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        try:
            if run_created:
                final_status = run_status
                if run_status == "success" and degraded:
                    final_status = "warning"
                update_collector_run(
                    run_id,
                    status=final_status,
                    finished_at=datetime.now(UTC),
                    tables_total=tables_seen,
                    tables_checked=tables_checked,
                    tables_skipped=tables_skipped,
                    metrics_collected=rows_saved,
                    duration_ms=elapsed_ms,
                    error_message=run_error,
                )
        finally:
            if engine:
                engine.dispose()

    if run_status == "failed":
        return

    logger.info(
        "[project=%s][conn=%s] collected %d metrics across %d tables in %dms",
        project_id, connection_id, rows_saved, tables_seen,
        int((time.monotonic() - started) * 1000),
    )
    try:
        from app.instrumentation import collector_runs_total
        collector_runs_total.labels(result="ok").inc()
    except ImportError:
        pass

    # #154 post-tick: per-project anomaly alerts. Only fires when (a) we
    # actually wrote metrics this tick AND (b) the tenant has Telegram
    # configured via /settings/notifications (#143). No global fallback —
    # silence is the default.
    if rows_saved > 0:
        _maybe_notify_anomalies(project_id, table_names)


def _load_telegram_config(project_id: str) -> tuple[str, str, int] | None:
    """Thin wrapper around the shared public helper in app.notifications.telegram."""
    from app.notifications.telegram import load_project_telegram_config
    return load_project_telegram_config(project_id)


def _passes_alert_quality_gate(
    table: str, anomaly: dict, project_id: str,
) -> bool:
    """Drop borderline anomalies before they reach Telegram (#171).

    Two independent checks, both must pass:

    1. ``|score| >= ANOMALY_NOTIFY_MIN_SCORE_MAGNITUDE`` — отбрасывает
       borderline IsolationForest predictions (score ≈ -0.003 при
       стабильных данных). Score из ``decision_function`` — отрицательный
       означает аномалия; чем больше |score|, тем увереннее модель.

    2. ``|value - baseline_median| / baseline_median >= ANOMALY_NOTIFY_MIN_DELTA_RATIO``
       — отбрасывает алерты на мелких колебаниях (день недели, нагрузочные
       циклы), даже если IF их пометил. baseline = 7-day median.
       Если baseline не вычисляется (<3 точки в окне) — gate пропускает
       (нет данных = доверяем детектору).

    Возвращает True если алерт ПРОЙТИ, False если ОТСЕЧЬ.
    """
    from app.config import settings

    score = float(anomaly.get("score", 0.0))
    if abs(score) < settings.ANOMALY_NOTIFY_MIN_SCORE_MAGNITUDE:
        logger.debug(
            "[project=%s][table=%s] anomaly score %.4f below magnitude "
            "threshold (%.4f); dropping alert",
            project_id, table, score,
            settings.ANOMALY_NOTIFY_MIN_SCORE_MAGNITUDE,
        )
        return False

    # Delta vs 7-day median of row_count (наиболее частая аномальная
    # метрика). Если медианы нет (свежая таблица) — пропускаем gate.
    baseline = _baseline_median_row_count(table, project_id)
    if baseline is None or baseline == 0:
        return True
    latest_value = _latest_row_count_at(table, project_id, anomaly["ts"])
    if latest_value is None:
        return True
    delta_ratio = abs(latest_value - baseline) / baseline
    if delta_ratio < settings.ANOMALY_NOTIFY_MIN_DELTA_RATIO:
        logger.debug(
            "[project=%s][table=%s] delta_ratio %.3f below threshold "
            "%.3f (baseline=%.1f, latest=%.1f); dropping alert",
            project_id, table, delta_ratio,
            settings.ANOMALY_NOTIFY_MIN_DELTA_RATIO,
            baseline, latest_value,
        )
        return False
    return True


def _baseline_median_row_count(table: str, project_id: str) -> float | None:
    """Median row_count за последние 7 дней (excluding the anomaly point itself).

    Возвращает None если в окне меньше 3 точек — недостаточно для
    repeatable median. Чисто defensive: на свежей таблице gate
    пропускает алерт без расчёта delta.
    """
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import text

    from app.metrics_storage import get_engine

    since = (datetime.now(UTC) - timedelta(days=7)).isoformat(timespec="seconds")
    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT value FROM metrics
            WHERE project_id = :pid
              AND table_name = :table
              AND metric_name = 'row_count'
              AND ts >= :since
            ORDER BY value
        """), {"pid": project_id, "table": table, "since": since}).fetchall()
    values = [float(r[0]) for r in rows]
    if len(values) < 3:
        return None
    # SQLite не имеет PERCENTILE_CONT, считаем в Python.
    n = len(values)
    return values[n // 2] if n % 2 else (values[n // 2 - 1] + values[n // 2]) / 2


def _latest_row_count_at(table: str, project_id: str, ts: str) -> float | None:
    """row_count в момент аномальной точки. Если такой записи нет — None."""
    from sqlalchemy import text

    from app.metrics_storage import get_engine

    with get_engine().connect() as conn:
        row = conn.execute(text("""
            SELECT value FROM metrics
            WHERE project_id = :pid
              AND table_name = :table
              AND metric_name = 'row_count'
              AND ts = :ts
        """), {"pid": project_id, "table": table, "ts": ts}).fetchone()
    return float(row[0]) if row else None


def _maybe_notify_anomalies(project_id: str, table_names: list[str]) -> None:
    """Score the last day of metrics for each table just collected, send a
    Telegram alert for the most recent anomaly per table.

    Mirrors the legacy ``collectors/scheduler.py::_score_recent_anomalies``
    but scoped to the current tenant — both ``score_table`` and the
    notification path take ``project_id`` explicitly. Anomaly model is
    persisted per-(project_id, table); first tick after install trains
    on the fly. ``InsufficientDataError`` is the "we don't have enough
    history yet" signal, treated as silent skip.
    """
    cfg = _load_telegram_config(project_id)
    if cfg is None:
        logger.debug(
            "[project=%s] no Telegram config — skipping anomaly notifications",
            project_id,
        )
        return
    bot_token, chat_id, throttle = cfg

    # Late imports keep the cold-start cost out of `import collectors.per_project`
    # — anomaly_detector pulls sklearn (heavy), and most ticks won't notify.
    from app.metrics_storage import save_anomaly_scores
    from app.notifications.telegram import notify_anomaly
    from ml.anomaly_detector import InsufficientDataError, score_table

    for name in table_names:
        try:
            scores = score_table(name, window_days=1, project_id=project_id)
            if not scores:
                continue
            save_anomaly_scores(
                [{**s, "table_name": name} for s in scores],
                project_id,
            )
            anomalies = [s for s in scores if s["is_anomaly"]]
            if not anomalies:
                continue
            latest = max(anomalies, key=lambda s: s["ts"])
            # #171 quality gate: IsolationForest помечает is_anomaly=1
            # для borderline точек со score=-0.003, что на стабильных
            # данных даёт false positives. Здесь — два независимых порога:
            # (1) magnitude самого score, (2) насколько метрика реально
            # сдвинулась относительно 7-дневной медианы. Оба должны
            # пройти, иначе alert не идёт.
            if not _passes_alert_quality_gate(name, latest, project_id):
                continue
            try:
                notify_anomaly(
                    project_id, name, latest["ts"], latest["score"],
                    bot_token=bot_token, chat_id=chat_id,
                    throttle_minutes=throttle,
                )
            except Exception as exc:
                logger.warning(
                    "[project=%s][table=%s] anomaly notification failed: %s",
                    project_id, name, exc,
                )
        except InsufficientDataError:
            # Not enough history to train — first few ticks. Quiet skip;
            # no metric or log noise, this is expected for fresh tables.
            pass
        except Exception as exc:
            logger.warning(
                "[project=%s][table=%s] anomaly scoring skipped: %s",
                project_id, name, exc,
            )


# --- Scheduler hooks ------------------------------------------------------


_JOB_OPTS = {
    # APScheduler defaults that match the spec:
    "misfire_grace_time": 60,  # tolerate 60 s late firing on busy worker
    "max_instances": 1,        # one tick at a time — long DB doesn't stack
    "coalesce": True,          # if multiple firings missed, run once
    "replace_existing": True,  # idempotent add_job_for_connection on re-register
}


def add_job_for_connection(
    scheduler: BaseScheduler, project_id: str, connection: dict,
    *, run_immediately: bool = False,
) -> None:
    """Idempotent: re-adding overwrites the existing job (same id).

    Connection must be the dict shape returned by ``metrics_storage.
    get_connection`` — needs id, interval_minutes.

    ``run_immediately`` schedules the very first tick at "now" instead of
    "now + interval". Set when the user has just added/toggled-on the
    connection so they see metrics right away rather than waiting up to
    24 h on a daily interval. Not set during boot-time re-registration,
    where firing every active job at once would thunder the target DBs.
    """
    if scheduler is None or not scheduler.running:
        logger.debug("scheduler not running, deferring job for conn=%s",
                     connection["id"])
        return
    extra: dict = {}
    if run_immediately:
        extra["next_run_time"] = datetime.now(UTC)
    scheduler.add_job(
        collect_for_connection,
        "interval",
        minutes=int(connection["interval_minutes"]),
        args=[project_id, connection["id"]],
        id=job_id_for(project_id, connection["id"]),
        name=f"collect project={project_id} conn={connection['id']}",
        **_JOB_OPTS,
        **extra,
    )
    logger.info(
        "[project=%s][conn=%s] registered (every %d min%s)",
        project_id, connection["id"], connection["interval_minutes"],
        ", first tick now" if run_immediately else "",
    )


def remove_job_for_connection(
    scheduler: BaseScheduler, project_id: str, connection_id: str
) -> None:
    """No-op if the job isn't registered — covers the "toggle off while
    already disabled" UX path.
    """
    if scheduler is None or not scheduler.running:
        return
    jid = job_id_for(project_id, connection_id)
    if scheduler.get_job(jid) is not None:
        scheduler.remove_job(jid)
        logger.info("[project=%s][conn=%s] unregistered", project_id, connection_id)


def register_jobs_for_all_active_connections(scheduler: BaseScheduler) -> int:
    """Boot-time enumeration: every active connection gets a scheduled job.

    Called from ``collectors/scheduler.py::start_scheduler`` after the
    scheduler starts. Returns the number of jobs registered.
    """
    from sqlalchemy import text

    from app.metrics_storage import get_engine

    # We don't have a "list ALL users" helper (no need so far), so we walk
    # connections directly. Each active row gets a job tagged with its
    # owning project_id.
    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT c.id, c.project_id, c.interval_minutes
            FROM connections AS c
            WHERE c.is_active = 1
        """)).fetchall()

    n = 0
    for cid, pid, interval in rows:
        add_job_for_connection(
            scheduler, pid,
            {"id": cid, "interval_minutes": int(interval)},
        )
        n += 1
    logger.info("registered %d per-connection collection jobs", n)
    return n


# --- Helpers for /admin ---------------------------------------------------


def list_jobs_for_user(scheduler: BaseScheduler, user_id: str) -> list[dict]:
    """Per-user scoping for /admin/jobs: only jobs whose project_id is
    owned by ``user_id``. Global / non-prefixed jobs are excluded — they're
    process-wide concerns (forecast retrain, drift sweep, …) and don't
    belong on a tenant's admin page.
    """
    if scheduler is None or not scheduler.running:
        return []
    owned_project_ids = {p["id"] for p in list_projects_for_user(user_id)}
    out: list[dict] = []
    for job in scheduler.get_jobs():
        parsed = parse_job_id(job.id)
        if parsed is None:
            continue
        project_id, conn_id = parsed
        if project_id not in owned_project_ids:
            continue
        out.append({
            "id": job.id,
            "name": job.name,
            "project_id": project_id,
            "connection_id": conn_id,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        })
    return out


def user_owns_job(user_id: str, job_id: str) -> bool:
    """True iff ``job_id`` is one of ours AND its project_id belongs to ``user_id``.

    Used by ``/admin/jobs/<id>/run`` to 404 on cross-tenant trigger attempts.
    """
    parsed = parse_job_id(job_id)
    if parsed is None:
        return False
    project_id, _ = parsed
    owned: Iterable[dict] = list_projects_for_user(user_id)
    return any(p["id"] == project_id for p in owned)


__all__ = [
    "add_job_for_connection",
    "collect_for_connection",
    "job_id_for",
    "list_jobs_for_user",
    "parse_job_id",
    "register_jobs_for_all_active_connections",
    "remove_job_for_connection",
    "user_owns_job",
]
