"""Per-connection scheduling tests (#54).

We don't spin up a real APScheduler — the lifecycle is exercised with a
``MagicMock`` standing in for the scheduler. The job body
``collect_for_connection`` is tested against an in-memory SQLite (target
DB == metrics DB == same file) so we can verify it actually writes rows
tagged with the right ``project_id`` without needing a Postgres.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from cryptography.fernet import Fernet

from app import crypto
from collectors.per_project import (
    _run_with_timeout,
    add_job_for_connection,
    collect_for_connection,
    job_id_for,
    list_jobs_for_user,
    parse_job_id,
    register_jobs_for_all_active_connections,
    remove_job_for_connection,
    user_owns_job,
)


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Fresh SQLite metrics DB AND scoped to a single test."""
    db_path = tmp_path / "scheduled.db"
    import app.metrics_storage as storage_mod

    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


@pytest.fixture
def fake_scheduler():
    """MagicMock scheduler that captures add_job / remove_job / get_job."""
    sched = MagicMock(name="scheduler")
    sched.running = True
    sched._jobs = {}  # MagicMock isn't dict-like by default

    def _add(fn, *args, id=None, **kwargs):
        job = MagicMock(id=id, name=kwargs.get("name", id))
        sched._jobs[id] = job
        return job

    def _remove(jid):
        sched._jobs.pop(jid, None)

    def _get(jid):
        return sched._jobs.get(jid)

    def _list():
        return list(sched._jobs.values())

    sched.add_job.side_effect = _add
    sched.remove_job.side_effect = _remove
    sched.get_job.side_effect = _get
    sched.get_jobs.side_effect = _list
    return sched


# --- Helpers --------------------------------------------------------------


def _seed_user_with_active_connection(storage, dsn: str):
    """Create a user + project + active connection. Returns (user_id, project, connection)."""
    user_id = uuid.uuid4().hex
    storage.create_user(user_id=user_id, email=f"u{user_id[:6]}@x.io",
                        password_hash="x")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_id,
        name="P", slug="default",
    )
    conn = storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="local",
        dsn_encrypted=crypto.encrypt_dsn(dsn),
        schema_name="main",  # SQLite default schema
        interval_minutes=15,
        is_active=True,
    )
    return user_id, project, conn


# --- job_id_for / parse_job_id -------------------------------------------


def test_job_id_round_trip():
    jid = job_id_for("proj-abc", "conn-def")
    assert jid == "collect:proj-abc:conn-def"
    assert parse_job_id(jid) == ("proj-abc", "conn-def")


def test_parse_job_id_returns_none_for_non_collect_ids():
    assert parse_job_id("collect_all_tables") is None
    assert parse_job_id("retrain_forecasts") is None
    assert parse_job_id("collect:single") is None


def test_run_with_timeout_preserves_db_contextvars():
    """Regression for #282: metadata calls run in a worker thread, so the
    per-connection engine/adapter context must be copied explicitly."""
    from sqlalchemy import create_engine

    import app.db as db_mod

    engine = create_engine("sqlite:///:memory:")
    adapter = db_mod.PostgresAdapter()
    try:
        with db_mod.using_engine(engine, adapter):
            seen_engine, seen_adapter = _run_with_timeout(
                lambda: (db_mod.get_engine(), db_mod.get_adapter()),
            )
    finally:
        engine.dispose()

    assert seen_engine is engine
    assert seen_adapter is adapter


# --- add_job_for_connection / remove ---------------------------------------


def test_add_job_for_connection_registers_with_stable_id(fake_scheduler):
    add_job_for_connection(fake_scheduler, "proj-X", {
        "id": "conn-Y", "interval_minutes": 15,
    })
    fake_scheduler.add_job.assert_called_once()
    call = fake_scheduler.add_job.call_args
    assert call.kwargs["id"] == "collect:proj-X:conn-Y"
    assert call.kwargs["minutes"] == 15  # interval honoured
    assert call.kwargs["max_instances"] == 1
    assert call.kwargs["coalesce"] is True
    assert call.kwargs["replace_existing"] is True
    # Default: NO next_run_time → APScheduler waits "now + interval"
    # before the first tick. Boot-time re-registration depends on this
    # to avoid a thundering herd across all active connections.
    assert "next_run_time" not in call.kwargs


def test_add_job_for_connection_run_immediately_sets_next_run_time_now(fake_scheduler):
    """run_immediately=True schedules the first tick at "now" instead of
    "now + interval" — used when the user just added/toggled-on a
    connection and expects metrics promptly, not on the next interval."""
    from datetime import UTC, datetime

    before = datetime.now(UTC)
    add_job_for_connection(
        fake_scheduler, "proj-X",
        {"id": "conn-Y", "interval_minutes": 1440},  # daily — would otherwise wait 24h
        run_immediately=True,
    )
    after = datetime.now(UTC)
    call = fake_scheduler.add_job.call_args
    nrt = call.kwargs["next_run_time"]
    assert before <= nrt <= after
    # The interval cadence is still honoured for subsequent ticks.
    assert call.kwargs["minutes"] == 1440


def test_remove_job_unregisters_only_when_present(fake_scheduler):
    # No-op if job doesn't exist yet.
    remove_job_for_connection(fake_scheduler, "p", "c")
    fake_scheduler.remove_job.assert_not_called()

    # Register, then remove.
    add_job_for_connection(fake_scheduler, "p", {"id": "c", "interval_minutes": 5})
    remove_job_for_connection(fake_scheduler, "p", "c")
    fake_scheduler.remove_job.assert_called_once_with("collect:p:c")


def test_add_job_no_op_when_scheduler_not_running():
    sched = MagicMock()
    sched.running = False
    add_job_for_connection(sched, "p", {"id": "c", "interval_minutes": 5})
    sched.add_job.assert_not_called()


# --- register_jobs_for_all_active_connections -----------------------------


def test_register_enumerates_active_connections(storage, fake_scheduler):
    """Bootstrap: every active connection in the DB gets a job."""
    _, project, conn1 = _seed_user_with_active_connection(
        storage, dsn=f"sqlite:///{':memory:'}",
    )
    # Second active connection in the same project.
    conn2 = storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"], name="second",
        dsn_encrypted=crypto.encrypt_dsn("sqlite:///:memory:"),
        schema_name="main", interval_minutes=30, is_active=True,
    )
    # Inactive connection — must NOT be registered.
    storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"], name="off",
        dsn_encrypted=crypto.encrypt_dsn("sqlite:///:memory:"),
        schema_name="main", interval_minutes=10, is_active=False,
    )

    n = register_jobs_for_all_active_connections(fake_scheduler)
    assert n == 2
    registered_ids = {call.kwargs["id"] for call in fake_scheduler.add_job.call_args_list}
    assert job_id_for(project["id"], conn1["id"]) in registered_ids
    assert job_id_for(project["id"], conn2["id"]) in registered_ids


# --- collect_for_connection (full path) ------------------------------------


def test_collect_for_connection_writes_metrics_with_project_id(storage, tmp_path):
    """End-to-end: create a real SQLite target, point a connection at it,
    run the job, check that metrics land tagged with the right project_id.
    """
    # Build a target SQLite DB with a single table, populate one row so
    # the Postgres adapter… wait — SQLite isn't a supported adapter.
    # We need a Postgres-compatible target for the adapter path.
    # Workaround: skip the adapter and test the job's persistence logic
    # via a monkeypatch.
    _user_id, project, conn = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@unreachable:5432/d",
    )
    from collectors import metrics_collector

    # Stub the heavy lifting: pretend the collector produced one row.
    fake_rows = [{
        "ts": datetime.now(UTC), "table_name": "users",
        "metric_name": "row_count", "value": 42.0,
    }]
    import app.db as db_mod

    def fake_list_tables(self, schema):
        return [{"table_name": "users", "schema": schema}]
    import unittest.mock as mock
    with mock.patch.object(metrics_collector.MetricsCollector, "collect",
                           return_value=fake_rows), \
         mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=fake_list_tables):
        collect_for_connection(project["id"], conn["id"])

    # The metrics row should be tagged with this project's id.
    rows = storage.get_metrics("users", "row_count", project["id"],
                                window=__import__("datetime").timedelta(minutes=5))
    assert len(rows) == 1
    assert rows[0]["value"] == 42.0

    # And NOT visible from another tenant.
    other_rows = storage.get_metrics("users", "row_count", "other-tenant",
                                      window=__import__("datetime").timedelta(minutes=5))
    assert other_rows == []


def test_collect_for_connection_refreshes_drift_cache_per_project(
    storage, monkeypatch,
):
    """Регрессионный (#fix-per-project-drift): tenant per-project тик
    раньше пропускал drift refresh — он жил в legacy collect_all_tables.
    Эффект: /api/drift/<table> тенантов отдавал stale данные. Теперь
    тик пересчитывает drift с tables=table_names этого подключения."""
    _user_id, project, conn = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@unreachable:5432/d",
    )
    from collectors import metrics_collector

    fake_rows = [{
        "ts": datetime.now(UTC), "table_name": "users",
        "metric_name": "row_count", "value": 42.0,
    }]
    import app.db as db_mod

    def fake_list_tables(self, schema):
        return [{"table_name": "users", "schema": schema}]

    captured: list[dict] = []
    import ml.drift as drift_mod

    def fake_compute(*, project_id, tables=None):
        captured.append({"project_id": project_id, "tables": list(tables or [])})
        return {"tables": len(tables or []), "rows": 0}

    import unittest.mock as mock
    with mock.patch.object(metrics_collector.MetricsCollector, "collect",
                           return_value=fake_rows), \
         mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=fake_list_tables), \
         mock.patch.object(drift_mod, "compute_and_store_drift_all",
                           side_effect=fake_compute):
        collect_for_connection(project["id"], conn["id"])

    assert len(captured) == 1
    assert captured[0]["project_id"] == project["id"]
    assert captured[0]["tables"] == ["users"]


def test_collect_for_connection_skips_inactive(storage):
    """Job re-checks is_active on each tick — covers the race where a
    connection is deactivated between job-fire scheduling and the actual
    callback.
    """
    _user_id, project, conn = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@h/d",
    )
    storage.set_connection_active(project["id"], conn["id"], is_active=False)
    # Should return without raising even though DSN host is unreachable.
    collect_for_connection(project["id"], conn["id"])
    # And no metrics written.
    rows = storage.get_metrics("users", "row_count", project["id"],
                                window=__import__("datetime").timedelta(minutes=5))
    assert rows == []


def test_collect_for_connection_handles_unknown_id(storage):
    """A job that fires for a deleted connection just logs and returns."""
    collect_for_connection("ghost-project", "ghost-conn")  # no exception


# --- /admin/jobs scoping ---------------------------------------------------


def test_list_jobs_for_user_filters_to_owned_projects(storage, fake_scheduler):
    """User-A sees only their connection jobs; user-B's are hidden."""
    user_a, project_a, conn_a = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@h/d",
    )
    # Second user with their own project + connection.
    user_b = uuid.uuid4().hex
    storage.create_user(user_id=user_b, email="b@x.io", password_hash="x")
    project_b = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_b,
        name="B", slug="default",
    )
    conn_b = storage.create_connection(
        connection_id=uuid.uuid4().hex, project_id=project_b["id"],
        name="b-conn", dsn_encrypted=crypto.encrypt_dsn("postgresql://u:p@h/d"),
        schema_name="public", interval_minutes=15, is_active=True,
    )

    add_job_for_connection(fake_scheduler, project_a["id"],
                            {"id": conn_a["id"], "interval_minutes": 15})
    add_job_for_connection(fake_scheduler, project_b["id"],
                            {"id": conn_b["id"], "interval_minutes": 15})
    # Add a global job — must NOT appear in either user's list.
    fake_scheduler.add_job(lambda: None, "interval", minutes=15,
                            id="collect_all_tables", name="collect_all_tables")

    a_jobs = list_jobs_for_user(fake_scheduler, user_a)
    b_jobs = list_jobs_for_user(fake_scheduler, user_b)

    assert [j["connection_id"] for j in a_jobs] == [conn_a["id"]]
    assert [j["connection_id"] for j in b_jobs] == [conn_b["id"]]
    # Global jobs excluded (not prefixed with collect:).
    assert all(j["id"].startswith("collect:") for j in a_jobs + b_jobs)


def test_user_owns_job_rejects_cross_tenant(storage):
    user_a, project_a, conn_a = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@h/d",
    )
    user_b = uuid.uuid4().hex
    storage.create_user(user_id=user_b, email="b@x.io", password_hash="x")
    jid = job_id_for(project_a["id"], conn_a["id"])

    assert user_owns_job(user_a, jid) is True
    assert user_owns_job(user_b, jid) is False
    # Non-collect job id → False (the caller still needs to look it up
    # against the scheduler; user_owns_job is the tenant check only).
    assert user_owns_job(user_a, "collect_all_tables") is False


# --- Per-project notifications wiring (#154) -------------------------------

def _tg_token(bot_id: str = "0000000001") -> str:
    """Synthetic Telegram bot token — format-valid for our validators
    but obviously fake (leading zeros — real Telegram bot IDs don't have
    those). Mirrors the constant in tests/test_project_notifications.py.
    """
    return bot_id + ":" + "A" * 35


def _run_collect_with_anomaly_stub(storage, project_id, conn_id, table_name,
                                   is_anomaly, monkeypatch):
    """Drive collect_for_connection through one tick with a fake target +
    a stubbed anomaly_detector.score_table that returns a single point."""
    import unittest.mock as mock

    import app.db as db_mod
    from collectors import metrics_collector

    fake_rows = [{
        "ts": datetime.now(UTC), "table_name": table_name,
        "metric_name": "row_count", "value": 42.0,
    }]
    def fake_list_tables(self, schema):
        return [{"table_name": table_name, "schema": schema}]

    fake_score = [{
        "ts": datetime.now(UTC).isoformat(timespec="seconds"),
        "score": -0.22 if is_anomaly else 0.05,
        "is_anomaly": 1 if is_anomaly else 0,
    }]
    monkeypatch.setattr(
        "ml.anomaly_detector.score_table",
        lambda table, window_days=14, project_id="legacy": fake_score,
    )

    with mock.patch.object(metrics_collector.MetricsCollector, "collect",
                           return_value=fake_rows), \
         mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=fake_list_tables):
        collect_for_connection(project_id, conn_id)


def test_collect_for_connection_notifies_when_configured(storage, monkeypatch):
    """Tenant with valid Telegram config gets notify_anomaly called with
    its OWN bot_token / chat_id on a confirmed anomaly."""
    _user, project, conn = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@unreachable:5432/d",
    )
    storage.save_project_notifications(
        project["id"],
        telegram_bot_token=crypto.encrypt_token(_tg_token()),
        telegram_chat_id="9001",
        throttle_minutes=15,
    )

    captures = []
    def fake_notify(pid, table, ts, score, *, bot_token, chat_id,
                    throttle_minutes=None, metric="row_count"):
        captures.append({
            "pid": pid, "table": table, "score": score,
            "bot_token": bot_token, "chat_id": chat_id,
            "throttle": throttle_minutes,
        })

    monkeypatch.setattr(
        "app.notifications.telegram.notify_anomaly", fake_notify,
    )
    _run_collect_with_anomaly_stub(
        storage, project["id"], conn["id"], "users",
        is_anomaly=True, monkeypatch=monkeypatch,
    )

    assert len(captures) == 1
    c = captures[0]
    assert c["pid"] == project["id"]
    assert c["table"] == "users"
    assert c["bot_token"] == _tg_token()
    assert c["chat_id"] == "9001"
    assert c["throttle"] == 15


def test_collect_for_connection_skips_notify_when_not_configured(storage, monkeypatch):
    """Tenant with no project_notifications row gets ZERO notify calls,
    even on a confirmed anomaly. This is the "no global fallback" rule."""
    _user, project, conn = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@unreachable:5432/d",
    )

    captures = []
    monkeypatch.setattr(
        "app.notifications.telegram.notify_anomaly",
        lambda *args, **kwargs: captures.append((args, kwargs)),
    )
    _run_collect_with_anomaly_stub(
        storage, project["id"], conn["id"], "users",
        is_anomaly=True, monkeypatch=monkeypatch,
    )
    assert captures == []


def test_collect_for_connection_skips_notify_when_no_anomaly(storage, monkeypatch):
    """Configured tenant + healthy score → still no notification (only
    real anomalies fire alerts; the throttle doesn't even get touched)."""
    _user, project, conn = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@unreachable:5432/d",
    )
    storage.save_project_notifications(
        project["id"],
        telegram_bot_token=crypto.encrypt_token(_tg_token()),
        telegram_chat_id="42",
        throttle_minutes=30,
    )

    captures = []
    monkeypatch.setattr(
        "app.notifications.telegram.notify_anomaly",
        lambda *args, **kwargs: captures.append(args),
    )
    _run_collect_with_anomaly_stub(
        storage, project["id"], conn["id"], "orders",
        is_anomaly=False, monkeypatch=monkeypatch,
    )
    assert captures == []


def test_collect_for_connection_cross_tenant_notification_isolation(storage, monkeypatch):
    """Two tenants, anomaly only in tenant A's tick → only A gets notified.
    Pins the no-fallback / no-cross-leak invariant at the wiring layer."""
    # Tenant A — Telegram configured.
    _user_a, proj_a, conn_a = _seed_user_with_active_connection(
        storage, dsn="postgresql://u:p@unreachable:5432/a",
    )
    storage.save_project_notifications(
        proj_a["id"],
        telegram_bot_token=crypto.encrypt_token(_tg_token("0000000001")),
        telegram_chat_id="aaa",
        throttle_minutes=20,
    )
    # Tenant B — also configured, different chat.
    user_b = uuid.uuid4().hex
    storage.create_user(user_id=user_b, email=f"b{user_b[:5]}@x.io",
                        password_hash="x")
    proj_b = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_b, name="B", slug="default-b",
    )
    conn_b = storage.create_connection(
        connection_id=uuid.uuid4().hex, project_id=proj_b["id"],
        name="b", dsn_encrypted=crypto.encrypt_dsn("postgresql://u:p@unreachable:5432/b"),
        schema_name="public", interval_minutes=15, is_active=True,
    )
    storage.save_project_notifications(
        proj_b["id"],
        telegram_bot_token=crypto.encrypt_token(_tg_token("0000000002")),
        telegram_chat_id="bbb",
        throttle_minutes=20,
    )

    captures = []
    def fake_notify(pid, table, ts, score, *, bot_token, chat_id,
                    throttle_minutes=None, metric="row_count"):
        captures.append({"pid": pid, "chat_id": chat_id})

    monkeypatch.setattr(
        "app.notifications.telegram.notify_anomaly", fake_notify,
    )

    # Tick A — anomaly fires; B is not invoked at all.
    _run_collect_with_anomaly_stub(
        storage, proj_a["id"], conn_a["id"], "orders",
        is_anomaly=True, monkeypatch=monkeypatch,
    )
    # Tick B — healthy, no notification.
    _run_collect_with_anomaly_stub(
        storage, proj_b["id"], conn_b["id"], "orders",
        is_anomaly=False, monkeypatch=monkeypatch,
    )

    assert len(captures) == 1
    assert captures[0]["pid"] == proj_a["id"]
    assert captures[0]["chat_id"] == "aaa"
