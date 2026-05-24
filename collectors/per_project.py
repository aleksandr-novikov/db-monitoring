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

    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    # NullPool — per-tick engine, no pool to leak across runs. connect_timeout
    # bounds the establish phase; OperationalError on a dead target bubbles
    # up to our try/except below.
    engine = create_engine(
        dsn, poolclass=NullPool, connect_args={"connect_timeout": 5},
    )
    try:
        adapter = make_adapter_for_url(dsn)
    except ValueError as exc:
        logger.warning("[project=%s][conn=%s] %s", project_id, connection_id, exc)
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
        engine.dispose()
        return

    engine.dispose()
    elapsed_ms = int((time.monotonic() - started) * 1000)
    logger.info(
        "[project=%s][conn=%s] collected %d metrics across %d tables in %dms",
        project_id, connection_id, rows_saved, tables_seen, elapsed_ms,
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
    scheduler: BaseScheduler, project_id: str, connection: dict
) -> None:
    """Idempotent: re-adding overwrites the existing job (same id).

    Connection must be the dict shape returned by ``metrics_storage.
    get_connection`` — needs id, interval_minutes.
    """
    if scheduler is None or not scheduler.running:
        logger.debug("scheduler not running, deferring job for conn=%s",
                     connection["id"])
        return
    scheduler.add_job(
        collect_for_connection,
        "interval",
        minutes=int(connection["interval_minutes"]),
        args=[project_id, connection["id"]],
        id=job_id_for(project_id, connection["id"]),
        name=f"collect project={project_id} conn={connection['id']}",
        **_JOB_OPTS,
    )
    logger.info("[project=%s][conn=%s] registered (every %d min)",
                project_id, connection["id"], connection["interval_minutes"])


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
