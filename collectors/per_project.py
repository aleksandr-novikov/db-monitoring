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

import logging
import time
from collections.abc import Iterable
from datetime import UTC, datetime

from apscheduler.schedulers.base import BaseScheduler

from app import crypto
from app.db import make_adapter_for_url, using_engine
from app.metrics_storage import (
    get_connection,
    list_projects_for_user,
    save_metrics,
)
from collectors.metrics_collector import MetricsCollector

logger = logging.getLogger(__name__)

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


def collect_for_connection(project_id: str, connection_id: str) -> None:
    """Single tick: enumerate tables on the connection's DSN, save metrics
    tagged with the project_id.

    Errors are caught + logged (the job runs hourly-ish; one bad tick must
    not poison the whole scheduler). Plaintext DSN never reaches a log —
    ``app.security.DSNFilter`` from #56 scrubs at LogRecord construction.
    """
    started = time.monotonic()
    conn_row = get_connection(project_id, connection_id)
    if conn_row is None or not conn_row["is_active"]:
        logger.info("[project=%s][conn=%s] skipped — inactive/deleted",
                    project_id, connection_id)
        return

    try:
        dsn = crypto.decrypt_dsn(conn_row["dsn_encrypted"])
    except crypto.InvalidToken:
        logger.warning("[project=%s][conn=%s] DSN ciphertext invalid — "
                       "Fernet key rotated? Re-save the connection.",
                       project_id, connection_id)
        return

    # Iceberg uses a catalog API (not SQLAlchemy) — skip engine creation.
    # For all other dialects, create a per-tick NullPool engine so connections
    # don't leak across scheduler runs.
    engine = None
    if not dsn.lower().startswith("iceberg+"):
        from sqlalchemy import create_engine
        from sqlalchemy.pool import NullPool

        engine = create_engine(
            dsn, poolclass=NullPool, connect_args={"connect_timeout": 5},
        )

    try:
        adapter = make_adapter_for_url(dsn)
    except ValueError as exc:
        logger.warning("[project=%s][conn=%s] %s", project_id, connection_id, exc)
        if engine:
            engine.dispose()
        return

    run_ts = datetime.now(UTC)
    schema = conn_row["schema_name"]
    rows_saved = 0
    tables_seen = 0
    try:
        with using_engine(engine, adapter):
            tables = adapter.list_tables(schema)
            collector = MetricsCollector(schema=schema)
            for table in tables:
                tables_seen += 1
                metrics = collector.collect(table["table_name"], ts=run_ts)
                if metrics:
                    rows_saved += save_metrics(metrics, project_id)
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - started) * 1000)
        logger.warning(
            "[project=%s][conn=%s] collection failed after %dms: %s",
            project_id, connection_id, elapsed_ms, exc,
        )
        if engine:
            engine.dispose()
        # #101: count the failed tick. Late import so a missing
        # prometheus-client install doesn't break collection itself.
        try:
            from app.instrumentation import collector_runs_total
            collector_runs_total.labels(result="error").inc()
        except ImportError:
            pass
        return

    if engine:
        engine.dispose()
    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "[project=%s][conn=%s] collected %d metrics across %d tables in %dms",
        project_id, connection_id, rows_saved, tables_seen, elapsed_ms,
    )
    try:
        from app.instrumentation import collector_runs_total
        collector_runs_total.labels(result="ok").inc()
    except ImportError:
        pass

    # #154 post-tick: per-project anomaly alerts. Only fires when (a) we
    # actually wrote metrics this tick AND (b) the tenant has Telegram
    # configured via /settings/notifications (#143). No global fallback —
    # silence is the default. Schema-drift and change-point alerts still
    # need their snapshot/event tables to grow a project_id column
    # before they can run per-tenant; that's a separate ticket.
    if rows_saved > 0:
        _maybe_notify_anomalies(project_id, [t["table_name"] for t in tables])


def _load_telegram_config(project_id: str) -> tuple[str, str, int] | None:
    """Return ``(bot_token, chat_id, throttle_minutes)`` or None if the
    project has not configured Telegram. Decryption failure is treated
    as "not configured" with a loud warning — we never silently fall back."""
    from app import crypto
    from app.metrics_storage import get_project_notifications

    cfg = get_project_notifications(project_id)
    if cfg is None:
        return None
    token_encrypted = cfg.get("telegram_bot_token")
    chat_id = cfg.get("telegram_chat_id")
    if not token_encrypted or not chat_id:
        return None
    try:
        bot_token = crypto.decrypt_token(token_encrypted)
    except crypto.InvalidToken:
        logger.warning(
            "[project=%s] telegram_bot_token failed to decrypt — Fernet key "
            "rotated without re-encrypt? Re-save the config in Settings.",
            project_id,
        )
        return None
    return bot_token, chat_id, int(cfg.get("throttle_minutes") or 30)


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
