from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app import crypto


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    db_path = tmp_path / "metrics.db"
    import app.metrics_storage as storage_mod

    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


def _seed_connection(storage, *, project_id: str | None = None, active: bool = True):
    user_id = uuid.uuid4().hex
    storage.create_user(
        user_id=user_id,
        email=f"u{user_id[:8]}@example.test",
        password_hash="x",
    )
    project = storage.create_project(
        project_id=project_id or uuid.uuid4().hex,
        user_id=user_id,
        name="Project",
        slug=f"p-{user_id[:8]}",
    )
    conn = storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="main",
        dsn_encrypted=crypto.encrypt_dsn("postgresql://user:pass@db/app"),
        schema_name="public",
        is_active=active,
    )
    return project, conn


def test_schema_tables_indexes_and_checks_exist(storage):
    engine = storage.get_engine()
    with engine.connect() as conn:
        tables = {
            r[0]
            for r in conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ))
        }
        indexes = {
            r[0]
            for r in conn.execute(text(
                "SELECT name FROM sqlite_master WHERE type = 'index'"
            ))
        }

    assert "collector_runs" in tables
    assert "collector_run_tables" in tables
    assert "idx_collector_runs_conn" in indexes
    assert "idx_collector_runs_project_started" in indexes
    assert "idx_collector_run_tables_run" in indexes

    project, conn_row = _seed_connection(storage)
    with pytest.raises(IntegrityError):
        storage.save_collector_run(
            uuid.uuid4().hex,
            project["id"],
            conn_row["id"],
            datetime.now(UTC),
            mode="bad-mode",
        )


def test_collector_run_helpers_list_and_tenant_isolation(storage):
    project_a, conn_a = _seed_connection(storage, project_id="proj-a")
    project_b, conn_b = _seed_connection(storage, project_id="proj-b")
    started = datetime.now(UTC)

    run_a = uuid.uuid4().hex
    storage.save_collector_run(run_a, project_a["id"], conn_a["id"], started)
    storage.save_run_table(
        run_a,
        "users",
        "success",
        metrics_collected=3,
        rows_observed=42,
        duration_ms=11,
    )
    storage.update_collector_run(
        run_a,
        status="success",
        finished_at=started + timedelta(seconds=1),
        tables_total=1,
        tables_checked=1,
        metrics_collected=3,
        duration_ms=1000,
    )

    run_b = uuid.uuid4().hex
    storage.save_collector_run(run_b, project_b["id"], conn_b["id"], started)
    storage.update_collector_run(
        run_b,
        status="success",
        finished_at=started,
    )

    runs_a = storage.list_collector_runs(project_a["id"], conn_a["id"])
    runs_b = storage.list_collector_runs(project_b["id"], conn_b["id"])

    assert [r["id"] for r in runs_a] == [run_a]
    assert [r["id"] for r in runs_b] == [run_b]
    assert runs_a[0]["tables"][0]["table_name"] == "users"
    assert runs_a[0]["tables"][0]["rows_observed"] == 42
    assert storage.list_collector_runs(project_b["id"], conn_a["id"]) == []
    assert storage.list_collector_runs(project_a["id"], conn_a["id"], limit="bad")[0]["id"] == run_a


def test_update_connection_probe_scrubs_and_scopes(storage):
    project_a, conn_a = _seed_connection(storage, project_id="proj-a")
    project_b, _ = _seed_connection(storage, project_id="proj-b")

    assert storage.update_connection_probe(
        project_b["id"],
        conn_a["id"],
        status="ok",
        tables_found=3,
    ) is False

    assert storage.update_connection_probe(
        project_a["id"],
        conn_a["id"],
        status="error",
        error="failed postgresql://user:secret@db/app?token=abc",
    ) is True

    conn = storage.get_connection(project_a["id"], conn_a["id"])
    assert conn["last_probe_status"] == "error"
    assert conn["last_probe_at"] is not None
    assert "secret" not in conn["last_probe_error"]
    assert "token=abc" not in conn["last_probe_error"]
    assert "abc" not in conn["last_probe_error"]


def test_list_last_runs_for_connections_returns_latest_per_connection(storage):
    project_a, conn_a = _seed_connection(storage, project_id="proj-a")
    conn_b = storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project_a["id"],
        name="replica",
        dsn_encrypted=crypto.encrypt_dsn("postgresql://user:pass@db/replica"),
        schema_name="public",
        is_active=True,
    )
    project_b, conn_other = _seed_connection(storage, project_id="proj-b")
    old_started = datetime.now(UTC) - timedelta(minutes=10)
    new_started = datetime.now(UTC)

    old_run = uuid.uuid4().hex
    storage.save_collector_run(old_run, project_a["id"], conn_a["id"], old_started)
    storage.update_collector_run(old_run, status="failed", finished_at=old_started)

    new_run = uuid.uuid4().hex
    storage.save_collector_run(new_run, project_a["id"], conn_a["id"], new_started)
    storage.update_collector_run(new_run, status="success", finished_at=new_started)

    conn_b_run = uuid.uuid4().hex
    storage.save_collector_run(conn_b_run, project_a["id"], conn_b["id"], new_started)
    storage.update_collector_run(conn_b_run, status="warning", finished_at=new_started)

    other_run = uuid.uuid4().hex
    storage.save_collector_run(other_run, project_b["id"], conn_other["id"], new_started)
    storage.update_collector_run(other_run, status="success", finished_at=new_started)

    runs = storage.list_last_runs_for_connections(
        project_a["id"],
        [conn_a["id"], conn_b["id"], conn_other["id"]],
    )

    assert runs[conn_a["id"]]["id"] == new_run
    assert runs[conn_a["id"]]["status"] == "success"
    assert runs[conn_b["id"]]["id"] == conn_b_run
    assert conn_other["id"] not in runs


def test_get_collector_run_detail_scopes_sorts_filters_scrubs_and_limits(storage):
    project_a, conn_a = _seed_connection(storage, project_id="proj-a")
    project_b, conn_b = _seed_connection(storage, project_id="proj-b")
    started = datetime.now(UTC)
    run_id = uuid.uuid4().hex
    storage.save_collector_run(run_id, project_a["id"], conn_a["id"], started)
    storage.update_collector_run(
        run_id,
        status="warning",
        finished_at=started + timedelta(seconds=2),
        tables_total=104,
        tables_checked=103,
        tables_skipped=1,
        metrics_collected=101,
        duration_ms=2000,
        error_message="run failed postgresql://u:secret@db/app?token=abc",
    )
    storage.save_run_table(
        run_id,
        "Z_success",
        "success",
        metrics_collected=1,
        rows_observed=42,
        duration_ms=12,
    )
    storage.save_run_table(
        run_id,
        "A_failed",
        "failed",
        error_message="table failed postgresql://u:secret@db/app?token=abc",
    )
    storage.save_run_table(
        run_id,
        "b_skipped",
        "skipped",
        skip_reason="too_large",
    )
    for i in range(101):
        storage.save_run_table(run_id, f"filler_{i:03d}", "success")

    storage.save_collector_run(uuid.uuid4().hex, project_b["id"], conn_b["id"], started)

    detail = storage.get_collector_run_detail(project_a["id"], conn_a["id"], run_id)
    failed = storage.get_collector_run_detail(
        project_a["id"],
        conn_a["id"],
        run_id,
        status="failed",
    )

    assert storage.get_collector_run_detail(project_b["id"], conn_a["id"], run_id) is None
    assert storage.get_collector_run_detail(project_a["id"], conn_b["id"], run_id) is None
    assert detail is not None
    assert detail["rows_total"] == 104
    assert detail["rows_limit"] == 100
    assert len(detail["rows"]) == 100
    assert [row["table_name"] for row in detail["rows"][:3]] == [
        "A_failed",
        "b_skipped",
        "filler_000",
    ]
    assert "secret" not in detail["error_message"]
    assert "token=abc" not in detail["error_message"]
    assert "secret" not in detail["rows"][0]["error_message"]
    assert failed["rows_total"] == 1
    assert [row["status"] for row in failed["rows"]] == ["failed"]


def test_get_collector_run_detail_filter_applies_before_limit(storage):
    project, conn_row = _seed_connection(storage)
    started = datetime.now(UTC)
    run_id = uuid.uuid4().hex
    storage.save_collector_run(run_id, project["id"], conn_row["id"], started)
    storage.update_collector_run(run_id, status="warning", finished_at=started)
    for i in range(101):
        storage.save_run_table(run_id, f"success_{i:03d}", "success")
    storage.save_run_table(run_id, "late_failed", "failed")

    detail = storage.get_collector_run_detail(
        project["id"],
        conn_row["id"],
        run_id,
        status="failed",
    )

    assert detail["rows_total"] == 1
    assert [row["table_name"] for row in detail["rows"]] == ["late_failed"]


def test_cleanup_stale_collector_runs(storage):
    project, conn_row = _seed_connection(storage)
    started = datetime.now(UTC) - timedelta(seconds=2)
    run_id = uuid.uuid4().hex
    storage.save_collector_run(run_id, project["id"], conn_row["id"], started)

    updated = storage.cleanup_stale_collector_runs(now=datetime.now(UTC))

    assert updated == 1
    run = storage.list_collector_runs(project["id"], conn_row["id"])[0]
    assert run["status"] == "failed"
    assert run["finished_at"] is not None
    assert run["duration_ms"] >= 0
    assert "stopped before finishing" in run["error_message"]


def test_cascade_delete_connection_removes_run_log(storage):
    project, conn_row = _seed_connection(storage)
    run_id = uuid.uuid4().hex
    storage.save_collector_run(run_id, project["id"], conn_row["id"], datetime.now(UTC))
    storage.save_run_table(run_id, "users", "success")

    assert storage.delete_connection(project["id"], conn_row["id"]) is True

    with storage.get_engine().connect() as conn:
        run_count = conn.execute(text("SELECT COUNT(*) FROM collector_runs")).scalar()
        table_count = conn.execute(text("SELECT COUNT(*) FROM collector_run_tables")).scalar()
    assert run_count == 0
    assert table_count == 0


def test_collect_for_connection_success_creates_run_log(storage):
    import app.db as db_mod
    from collectors import metrics_collector, per_project

    project, conn_row = _seed_connection(storage)
    fake_rows = [{
        "ts": datetime.now(UTC),
        "table_name": "users",
        "metric_name": "row_count",
        "value": 42,
    }]

    def fake_list_tables(self, schema):
        return [{"table_name": "users", "schema": schema}]

    with mock.patch.object(
        metrics_collector.MetricsCollector, "collect", return_value=fake_rows,
    ), mock.patch.object(
        db_mod.PostgresAdapter, "list_tables", autospec=True, side_effect=fake_list_tables,
    ), mock.patch(
        "collectors.schema_collector.collect_table_schema", return_value=None,
    ):
        per_project.collect_for_connection(project["id"], conn_row["id"])

    run = storage.list_collector_runs(project["id"], conn_row["id"])[0]
    assert run["status"] == "success"
    assert run["tables_checked"] == 1
    assert run["metrics_collected"] == 1
    assert run["tables"][0]["status"] == "success"
    assert run["tables"][0]["rows_observed"] == 42


def test_collect_for_connection_inactive_logged_as_skipped(storage):
    from collectors import per_project

    project, conn_row = _seed_connection(storage, active=False)

    per_project.collect_for_connection(project["id"], conn_row["id"])

    run = storage.list_collector_runs(project["id"], conn_row["id"])[0]
    assert run["status"] == "skipped"
    assert run["error_message"] == "connection inactive or deleted"


def test_collect_for_connection_table_failure_sets_warning_and_scrubs(storage):
    import app.db as db_mod
    from collectors import metrics_collector, per_project

    project, conn_row = _seed_connection(storage)

    def fake_list_tables(self, schema):
        return [{"table_name": "users", "schema": schema}]

    with mock.patch.object(
        metrics_collector.MetricsCollector,
        "collect",
        side_effect=RuntimeError("boom postgresql://u:secret@db/app?token=abc"),
    ), mock.patch.object(
        db_mod.PostgresAdapter, "list_tables", autospec=True, side_effect=fake_list_tables,
    ):
        per_project.collect_for_connection(project["id"], conn_row["id"])

    run = storage.list_collector_runs(project["id"], conn_row["id"])[0]
    assert run["status"] == "warning"
    assert run["tables"][0]["status"] == "failed"
    assert "secret" not in run["tables"][0]["error_message"]
    assert "token=abc" not in run["tables"][0]["error_message"]
    assert "abc" not in run["tables"][0]["error_message"]


def test_collect_for_connection_invalid_engine_url_logged_as_failed(storage):
    from app.instrumentation import collector_runs_total
    from collectors import per_project
    from tests.test_prometheus_metrics import _get_value

    project, conn_row = _seed_connection(storage)
    with storage.get_engine().begin() as db_conn:
        db_conn.execute(
            text("UPDATE connections SET dsn_encrypted = :dsn WHERE id = :id"),
            {
                "id": conn_row["id"],
                "dsn": crypto.encrypt_dsn("not-a-url"),
            },
        )

    before = _get_value(collector_runs_total, result="error")
    per_project.collect_for_connection(project["id"], conn_row["id"])
    after = _get_value(collector_runs_total, result="error")

    run = storage.list_collector_runs(project["id"], conn_row["id"])[0]
    assert run["status"] == "failed"
    assert run["finished_at"] is not None
    assert run["error_message"]
    assert after - before == 1
