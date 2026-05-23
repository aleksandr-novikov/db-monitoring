"""Tests for the /test endpoint + probe_connection (#52).

The happy-path probe goes through ``sqlalchemy.create_engine`` — that part
is faked at the module-attribute boundary so unit tests don't need a real
Postgres. A live-Postgres regression test lives in
``tests/integration/test_full_cycle.py`` (#44 framework).
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.exc import OperationalError

from app import connections, crypto
from app.app import create_app


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def conn_app(tmp_path, monkeypatch):
    db_path = tmp_path / "tc.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })
    return app


@pytest.fixture
def client(conn_app):
    return conn_app.test_client()


# --- Pure unit tests for probe_connection ---------------------------------


def test_probe_classifies_invalid_url():
    """Malformed URL → SQLAlchemy make_url raises → invalid_dsn code."""
    result = connections.probe_connection("not a url")
    assert result["status"] == "error"
    assert result["code"] == "invalid_dsn"


def test_probe_classifies_unsupported_dialect():
    result = connections.probe_connection(
        "mysql+pymysql://u:p@h:3306/d"
    )
    assert result["status"] == "error"
    assert result["code"] == "unsupported_dialect"


@pytest.mark.parametrize("err_text, expected_code", [
    ('FATAL:  password authentication failed for user "admin"', "auth_failed"),
    ("connection timeout expired", "timeout"),
    ("connect_timeout expired during startup", "timeout"),
    ("could not translate host name \"db.example.com\" to address", "network"),
    ("could not connect to server: Connection refused", "network"),
    ("Name or service not known", "network"),
    ("some other weird error from the future", "error"),
])
def test_probe_classifies_exception_text(monkeypatch, err_text, expected_code):
    """probe_connection wraps SQLAlchemy errors → coded responses."""
    def boom(*_args, **_kwargs):
        # Build a SQLAlchemy-style OperationalError. The wrapped message
        # is what _classify_error inspects.
        return _engine_that_raises(OperationalError("SELECT 1", {}, Exception(err_text)))

    monkeypatch.setattr(connections, "create_engine", boom)
    result = connections.probe_connection("postgresql://u:p@h:5432/d")
    assert result["status"] == "error"
    assert result["code"] == expected_code
    # User-facing message must NOT contain the original DSN.
    assert "u:p@h" not in result["message"]


def test_probe_returns_database_and_version_on_success(monkeypatch):
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_returns_metadata("testdb", "PostgreSQL 16.1 on x86_64-pc-linux-gnu"),
    )
    result = connections.probe_connection("postgresql://u:p@h/d")
    assert result["status"] == "ok"
    assert result["database"] == "testdb"
    assert result["version"] == "PostgreSQL 16.1"  # trims " on x86_64-..."
    assert isinstance(result["latency_ms"], int)


# --- Route-level tests ----------------------------------------------------


def _register(client, email="u@example.com"):
    client.post("/auth/register", data={
        "email": email, "password": "supersecret1", "confirm": "supersecret1",
    })


def _logout(client):
    client.post("/auth/logout")


def _add_conn(client, slug="default", dsn="postgresql://u:p@h:5432/d"):
    return client.post(f"/projects/{slug}/connections/new", data={
        "name": "Probe", "dsn": dsn,
        "schema_name": "public", "interval_minutes": 15, "is_active": "y",
    })


def _conn_id(email):
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    user = get_user_by_email(email)
    project = list_projects_for_user(user["id"])[0]
    return project["slug"], list_connections_for_project(project["id"])[0]["id"]


def test_test_route_returns_ok_payload(monkeypatch, client):
    _register(client)
    _add_conn(client)
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_returns_metadata("appdb", "PostgreSQL 16.1"),
    )
    slug, conn_id = _conn_id("u@example.com")
    resp = client.post(f"/projects/{slug}/connections/{conn_id}/test")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body == {
        "status": "ok",
        "database": "appdb",
        "version": "PostgreSQL 16.1",
        "latency_ms": body["latency_ms"],  # any int
    }


def test_test_route_returns_422_with_code_on_failure(monkeypatch, client):
    _register(client)
    _add_conn(client)
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_raises(
            OperationalError("SELECT 1", {}, Exception("password authentication failed"))
        ),
    )
    slug, conn_id = _conn_id("u@example.com")
    resp = client.post(f"/projects/{slug}/connections/{conn_id}/test")
    assert resp.status_code == 422
    assert resp.get_json()["code"] == "auth_failed"


def test_test_route_requires_login(client):
    resp = client.post(
        "/projects/default/connections/abc/test", follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


def test_test_route_rejects_strangers_conn_id(monkeypatch, client):
    _register(client, "owner@example.com")
    _add_conn(client)
    _, owner_conn_id = _conn_id("owner@example.com")
    _logout(client)

    _register(client, "stranger@example.com")
    # stranger's slug=default, but owner_conn_id belongs to another project.
    resp = client.post(f"/projects/default/connections/{owner_conn_id}/test")
    assert resp.status_code == 404


def test_test_route_handles_corrupted_ciphertext(monkeypatch, client):
    """Connection saved with a key the current process can't read."""
    _register(client)
    _add_conn(client)
    slug, conn_id = _conn_id("u@example.com")
    # Rotate Fernet key so the stored ciphertext becomes unreadable.
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()
    resp = client.post(f"/projects/{slug}/connections/{conn_id}/test")
    assert resp.status_code == 422
    assert resp.get_json()["code"] == "invalid_ciphertext"


def test_test_route_never_leaks_dsn_in_response(monkeypatch, client):
    _register(client)
    _add_conn(client, dsn="postgresql://leakyuser:leakypass@leakyhost:5432/d")
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_raises(
            OperationalError(
                "SELECT 1", {},
                Exception("FATAL: password authentication failed for user \"leakyuser\""),
            )
        ),
    )
    slug, conn_id = _conn_id("u@example.com")
    resp = client.post(f"/projects/{slug}/connections/{conn_id}/test")
    body = resp.get_data(as_text=True)
    assert "leakyuser" not in body
    assert "leakypass" not in body
    assert "leakyhost" not in body


def test_test_route_never_leaks_dsn_in_logs(monkeypatch, client, caplog):
    import logging
    _register(client)
    _add_conn(client, dsn="postgresql://logleak:supersecret@h:5432/d")
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_raises(
            OperationalError("SELECT 1", {}, Exception("connection failed for postgresql://logleak:supersecret@h:5432/d"))
        ),
    )
    slug, conn_id = _conn_id("u@example.com")
    with caplog.at_level(logging.DEBUG):
        client.post(f"/projects/{slug}/connections/{conn_id}/test")
    full_log = "\n".join(r.getMessage() for r in caplog.records)
    assert "supersecret" not in full_log


# --- Fakes -----------------------------------------------------------------


def _engine_that_returns_metadata(database: str, version: str):
    """Build a Mock engine whose .connect().execute(text(...)).fetchone()
    returns the metadata row on the second call (SELECT current_database, version()).
    """
    engine = MagicMock(name="engine")
    conn = MagicMock(name="conn")
    select1 = MagicMock(name="select1")
    metadata = MagicMock(name="metadata")
    metadata.fetchone.return_value = (database, version)
    # First execute: SELECT 1 — value irrelevant. Second: metadata row.
    conn.execute.side_effect = [select1, metadata]

    @contextmanager
    def _connect():
        yield conn

    engine.connect.side_effect = _connect
    engine.dispose = MagicMock()
    return engine


def _engine_that_raises(exc):
    engine = MagicMock(name="engine")
    engine.connect.side_effect = exc
    engine.dispose = MagicMock()
    return engine
