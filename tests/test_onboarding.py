"""Onboarding flow + landing tests for #55.

Covers the acceptance bullet-list:
- Anonymous ``/`` renders the marketing landing
- Authenticated ``/`` redirects to /dashboard
- Register lands on the onboarding wizard (not an empty dashboard)
- First-connection POST auto-tests the DSN
- /dashboard without active connections shows the empty-state banner
"""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from app import crypto


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def app_(tmp_path, monkeypatch):
    db_path = tmp_path / "ob.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    # Empty target — onboarding tests never need real tables.
    import app.api
    import app.dashboard
    import app.db
    def fake_list(schema=None):
        return []
    monkeypatch.setattr(app.db, "list_tables", fake_list)
    monkeypatch.setattr(app.api, "list_tables", fake_list)

    from app.app import create_app
    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })
    # No limiter reset — TESTING disables RATELIMIT_ENABLED, so the
    # storage backend isn't initialised and ``limiter.reset()`` would
    # raise on the inner assert. Tests below don't exhaust limits anyway.
    return app


@pytest.fixture
def client(app_):
    return app_.test_client()


def _register(client, email="u@example.com"):
    return client.post(
        "/auth/register",
        data={"email": email, "password": "supersecret1", "confirm": "supersecret1"},
        follow_redirects=False,
    )


# --- Public landing -------------------------------------------------------


def test_anonymous_root_renders_landing(client):
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "DB Monitor" in body
    # CTA links are present.
    assert "/auth/register" in body
    assert "/auth/login" in body


def test_authenticated_root_redirects_to_dashboard(client):
    _register(client)
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/dashboard/")


# --- Register lands on onboarding wizard ----------------------------------


def test_register_redirects_to_onboarding_wizard(client):
    resp = _register(client, "new@example.com")
    assert resp.status_code == 302
    # Slug for the auto-created Default project is "default".
    assert resp.headers["Location"].endswith("/projects/default/connections/new")


def test_onboarding_template_rendered_when_project_empty(client):
    _register(client, "fresh@example.com")
    resp = client.get("/projects/default/connections/new")
    body = resp.get_data(as_text=True)
    # Wizard-specific copy is in onboarding/add_connection.html only.
    assert "Где взять DSN" in body
    assert "Сохранить и проверить" in body


def test_regular_template_rendered_when_project_has_connections(client, monkeypatch):
    """A power-user adding their second connection sees the standard form,
    not the wizard."""
    _register(client, "poweruser@example.com")
    # First connection — wizard mode. Stub probe so it doesn't try to
    # connect to a real DB during the auto-test path.
    import app.connections as conn_mod
    monkeypatch.setattr(
        conn_mod, "probe_connection",
        lambda dsn: {"status": "ok", "database": "x", "version": "y", "latency_ms": 1},
    )
    client.post("/projects/default/connections/new", data={
        "name": "First", "dsn": "postgresql://u:p@h:5432/d",
        "schema_name": "public", "interval_minutes": 15, "is_active": "y",
    })
    # Second GET should now hit the non-wizard form.
    resp = client.get("/projects/default/connections/new")
    body = resp.get_data(as_text=True)
    assert "Где взять DSN" not in body  # wizard sidebar absent
    assert "Новое подключение" in body  # plain form heading


# --- Auto-test after first connection -------------------------------------


def test_first_connection_auto_test_success_redirects_to_dashboard(client, monkeypatch):
    _register(client, "ok@example.com")
    import app.connections as conn_mod
    monkeypatch.setattr(
        conn_mod, "probe_connection",
        lambda dsn: {
            "status": "ok", "database": "appdb",
            "version": "PostgreSQL 16.1", "latency_ms": 4,
        },
    )
    resp = client.post(
        "/projects/default/connections/new",
        data={
            "name": "Prod", "dsn": "postgresql://u:p@h:5432/d",
            "schema_name": "public", "interval_minutes": 15, "is_active": "y",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/dashboard/")


def test_first_connection_auto_test_failure_redirects_to_connections_list(client, monkeypatch):
    _register(client, "bad@example.com")
    import app.connections as conn_mod
    monkeypatch.setattr(
        conn_mod, "probe_connection",
        lambda dsn: {
            "status": "error", "code": "auth_failed",
            "message": "Неверный логин или пароль.", "latency_ms": 12,
        },
    )
    resp = client.post(
        "/projects/default/connections/new",
        data={
            "name": "Bad", "dsn": "postgresql://u:wrong@h:5432/d",
            "schema_name": "public", "interval_minutes": 15, "is_active": "y",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    # url_for resolves to the trailing-slash form of list_connections;
    # strip before comparing so we don't depend on the rule order.
    assert resp.headers["Location"].rstrip("/").endswith("/projects/default/connections")

    # Connection was still saved (so the user can edit/delete from the list).
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    user = get_user_by_email("bad@example.com")
    project = list_projects_for_user(user["id"])[0]
    assert len(list_connections_for_project(project["id"])) == 1


# --- /dashboard empty state -----------------------------------------------


def test_dashboard_empty_state_when_no_connections(client):
    _register(client, "empty@example.com")
    resp = client.get("/dashboard/")
    body = resp.get_data(as_text=True)
    assert "Нет подключений" in body
    assert "/projects/default/connections/new" in body


def test_dashboard_no_empty_state_with_at_least_one_connection(client, monkeypatch):
    _register(client, "stocked@example.com")
    import app.connections as conn_mod
    monkeypatch.setattr(
        conn_mod, "probe_connection",
        lambda dsn: {"status": "ok", "database": "x", "version": "y", "latency_ms": 1},
    )
    # Add one connection; redirect goes to /dashboard (success path).
    client.post("/projects/default/connections/new", data={
        "name": "Prod", "dsn": "postgresql://u:p@h:5432/d",
        "schema_name": "public", "interval_minutes": 15, "is_active": "y",
    })
    resp = client.get("/dashboard/")
    body = resp.get_data(as_text=True)
    assert "Нет подключений" not in body
