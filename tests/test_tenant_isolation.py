"""Cross-tenant isolation tests for #53.

The acceptance criterion: user Y must not see user X's metrics, even
when the same ``table_name`` appears in both projects. These tests
exercise the full path from save_metrics → DB read → API/dashboard
render, with two project_ids holding identically-named tables.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest


@pytest.fixture
def storage(tmp_path, monkeypatch):
    db_path = tmp_path / "iso.db"
    import app.metrics_storage as storage_mod

    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


@pytest.fixture
def seeded_two_tenants(storage):
    """Project A and project B both store a 'users' table — different values."""
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now - timedelta(hours=1), "table_name": "users",
         "metric_name": "row_count", "value": 100},
        {"ts": now - timedelta(hours=1), "table_name": "users",
         "metric_name": "null_count", "value": 5, "tags": {"column": "email"}},
        {"ts": now, "table_name": "users",
         "metric_name": "row_count", "value": 110},
    ], "project-A")
    storage.save_metrics([
        {"ts": now, "table_name": "users",
         "metric_name": "row_count", "value": 9999},
        {"ts": now, "table_name": "users",
         "metric_name": "null_count", "value": 999, "tags": {"column": "email"}},
    ], "project-B")
    return storage


# --- Storage-layer isolation ----------------------------------------------


def test_save_metrics_writes_project_id(storage):
    """Inserted rows carry the supplied project_id, not a default."""
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now, "table_name": "users",
         "metric_name": "row_count", "value": 42},
    ], "tenant-x")
    from sqlalchemy import text
    with storage.get_engine().connect() as conn:
        rows = conn.execute(
            text("SELECT project_id FROM metrics WHERE table_name = 'users'")
        ).fetchall()
    assert [r[0] for r in rows] == ["tenant-x"]


def test_get_metrics_isolates_by_project(seeded_two_tenants):
    a_rows = seeded_two_tenants.get_metrics(
        "users", "row_count", "project-A", window=timedelta(days=1),
    )
    b_rows = seeded_two_tenants.get_metrics(
        "users", "row_count", "project-B", window=timedelta(days=1),
    )
    assert [r["value"] for r in a_rows] == [100.0, 110.0]
    assert [r["value"] for r in b_rows] == [9999.0]


def test_get_latest_metric_isolates_by_project(seeded_two_tenants):
    a = seeded_two_tenants.get_latest_metric("users", "row_count", "project-A")
    b = seeded_two_tenants.get_latest_metric("users", "row_count", "project-B")
    assert a["value"] == 110.0
    assert b["value"] == 9999.0


def test_get_latest_null_counts_isolates_by_project(seeded_two_tenants):
    a = seeded_two_tenants.get_latest_null_counts("users", "project-A")
    b = seeded_two_tenants.get_latest_null_counts("users", "project-B")
    assert a == {"email": 5}
    assert b == {"email": 999}


def test_history_aggregate_isolates_by_project(seeded_two_tenants):
    agg_a = seeded_two_tenants.build_history_aggregate(
        "project-A", window=timedelta(days=7),
    )
    agg_b = seeded_two_tenants.build_history_aggregate(
        "project-B", window=timedelta(days=7),
    )
    # Each tenant sees only their own row_count rows.
    assert all(
        r["value"] in (100, 110) for r in agg_a["rows"]
        if r["metric_name"] == "row_count"
    )
    assert all(
        r["value"] == 9999 for r in agg_b["rows"]
        if r["metric_name"] == "row_count"
    )


def test_unknown_project_returns_empty(seeded_two_tenants):
    """A project_id that wasn't ever written to behaves like an empty store."""
    rows = seeded_two_tenants.get_metrics(
        "users", "row_count", "never-existed", window=timedelta(days=1),
    )
    assert rows == []


# --- purge_old scoping -----------------------------------------------------


def test_purge_old_without_project_id_purges_all(seeded_two_tenants):
    """The retention cron path (project_id=None) clears across all tenants."""
    # Insert old rows in both tenants.
    long_ago = datetime.now(UTC) - timedelta(days=120)
    seeded_two_tenants.save_metrics([
        {"ts": long_ago, "table_name": "old", "metric_name": "row_count", "value": 1},
    ], "project-A")
    seeded_two_tenants.save_metrics([
        {"ts": long_ago, "table_name": "old", "metric_name": "row_count", "value": 2},
    ], "project-B")
    deleted = seeded_two_tenants.purge_old(retention_days=90)
    assert deleted >= 2


def test_purge_old_with_project_id_scopes_to_one_tenant(seeded_two_tenants):
    long_ago = datetime.now(UTC) - timedelta(days=120)
    seeded_two_tenants.save_metrics([
        {"ts": long_ago, "table_name": "old", "metric_name": "row_count", "value": 1},
    ], "project-A")
    seeded_two_tenants.save_metrics([
        {"ts": long_ago, "table_name": "old", "metric_name": "row_count", "value": 2},
    ], "project-B")
    deleted = seeded_two_tenants.purge_old(
        retention_days=90, project_id="project-A",
    )
    assert deleted == 1
    # Project B's old row survives the scoped purge.
    surviving = seeded_two_tenants.get_metrics(
        "old", "row_count", "project-B", window=timedelta(days=365),
    )
    assert [r["value"] for r in surviving] == [2.0]


# --- HTTP isolation through the real Flask app ----------------------------


@pytest.fixture
def isolated_app(tmp_path, monkeypatch):
    db_path = tmp_path / "iso_http.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    # Patch list_tables EVERYWHERE it's been re-bound via `from .db import
    # list_tables`. Patching only app.db.list_tables leaves the api and
    # dashboard modules with their original references.
    def fake_list(schema=None):
        return [{"table_name": "users", "schema": "public"}]
    import app.api
    import app.dashboard
    import app.db
    monkeypatch.setattr(app.db, "list_tables", fake_list)
    monkeypatch.setattr(app.api, "list_tables", fake_list)

    from app.app import create_app
    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })
    # No limiter.reset() — create_app sets RATELIMIT_ENABLED=False when
    # TESTING=True, so the storage backend is never initialised and
    # limiter.reset() would raise AssertionError (#125).
    return app


def _register_user_a(client):
    client.post("/auth/register", data={
        "email": "a@example.com", "password": "supersecret1",
        "confirm": "supersecret1",
    })
    from app.projects import create_default_project_for
    from app.metrics_storage import get_user_by_email
    user = get_user_by_email("a@example.com")
    if user:
        create_default_project_for(user["id"])


def _register_user_b(client):
    client.post("/auth/register", data={
        "email": "b@example.com", "password": "supersecret1",
        "confirm": "supersecret1",
    })
    from app.projects import create_default_project_for
    from app.metrics_storage import get_user_by_email
    user = get_user_by_email("b@example.com")
    if user:
        create_default_project_for(user["id"])


def _project_id_of(email: str) -> str:
    from app.metrics_storage import get_user_by_email, list_projects_for_user
    user = get_user_by_email(email)
    return list_projects_for_user(user["id"])[0]["id"]


def test_api_metrics_isolates_users(isolated_app):
    """User A writes row_count=42 in their default project; user B sees nothing."""
    client = isolated_app.test_client()

    _register_user_a(client)
    a_project_id = _project_id_of("a@example.com")
    # Seed A's tenant directly via storage — the test harness has no
    # collector running.
    from app.metrics_storage import save_metrics
    save_metrics([
        {"ts": datetime.now(UTC), "table_name": "users",
         "metric_name": "row_count", "value": 42},
    ], a_project_id)

    # While A is logged in, /api/metrics/users returns A's row.
    resp_a = client.get("/api/metrics/users?range=24h").get_json()
    assert any(point["value"] == 42 for point in resp_a)

    # Switch sessions: log A out, register B.
    client.post("/auth/logout")
    _register_user_b(client)

    resp_b = client.get("/api/metrics/users?range=24h").get_json()
    assert resp_b == [], f"User B leaked A's data: {resp_b}"


def test_api_tables_isolates_latest_row_count(isolated_app):
    """/api/tables exposes the latest row_count per (tenant, table)."""
    client = isolated_app.test_client()

    _register_user_a(client)
    a_id = _project_id_of("a@example.com")
    from app.metrics_storage import save_metrics
    save_metrics([
        {"ts": datetime.now(UTC), "table_name": "users",
         "metric_name": "row_count", "value": 100},
    ], a_id)

    client.post("/auth/logout")
    _register_user_b(client)
    b_id = _project_id_of("b@example.com")
    save_metrics([
        {"ts": datetime.now(UTC), "table_name": "users",
         "metric_name": "row_count", "value": 9999},
    ], b_id)

    # B is currently logged in — should see 9999 only.
    resp_b = client.get("/api/tables").get_json()
    users_row_b = next(r for r in resp_b if r["table_name"] == "users")
    assert users_row_b["row_count"] == 9999

    # Switch back to A and read again.
    client.post("/auth/logout")
    client.post("/auth/login", data={
        "email": "a@example.com", "password": "supersecret1",
    })
    resp_a = client.get("/api/tables").get_json()
    users_row_a = next(r for r in resp_a if r["table_name"] == "users")
    assert users_row_a["row_count"] == 100
