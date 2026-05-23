"""Tests for DB connections + Fernet encryption (#51)."""

from __future__ import annotations

import pytest
from cryptography.fernet import Fernet

from app.app import create_app


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    """Pin a deterministic Fernet key per test run.

    Without this, app/crypto.py's dev-fallback would generate a new key
    AND try to write .env.local in the repo root — undesirable in tests.
    """
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    # Reset the cached singleton so the new key takes effect.
    from app import crypto
    crypto.reset_for_tests()


@pytest.fixture
def conn_app(tmp_path, monkeypatch):
    db_path = tmp_path / "conn.db"
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


# --- Crypto layer ----------------------------------------------------------


def test_encrypt_decrypt_roundtrip():
    from app import crypto

    plain = "postgresql://user:s3cret@host:5432/db"
    ciphertext = crypto.encrypt_dsn(plain)
    assert ciphertext != plain.encode()
    assert isinstance(ciphertext, bytes)
    assert crypto.decrypt_dsn(ciphertext) == plain


def test_decrypt_with_wrong_key_raises(monkeypatch):
    from app import crypto

    ciphertext = crypto.encrypt_dsn("postgresql://u:p@h/d")
    # Rotate the key and try to decrypt with the new one.
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()
    with pytest.raises(crypto.InvalidToken):
        crypto.decrypt_dsn(ciphertext)


def test_encrypt_empty_dsn_raises():
    from app import crypto
    with pytest.raises(ValueError):
        crypto.encrypt_dsn("")


def test_fernet_key_missing_in_production_raises(monkeypatch):
    from app import crypto

    monkeypatch.delenv("FERNET_KEY", raising=False)
    monkeypatch.setenv("FLASK_ENV", "production")
    crypto.reset_for_tests()
    with pytest.raises(crypto.FernetKeyMissing):
        crypto.encrypt_dsn("postgresql://u:p@h/d")


# --- Helpers ---------------------------------------------------------------


def _register(client, email="u@example.com"):
    client.post("/auth/register", data={
        "email": email, "password": "supersecret1", "confirm": "supersecret1",
    })


def _logout(client):
    client.post("/auth/logout")


def _make_project(client, slug="prod"):
    return client.post("/projects/new", data={"name": "Prod", "slug": slug})


def _add_connection(client, slug="default", name="Local", dsn="postgresql://u:p@h:5432/d"):
    return client.post(
        f"/projects/{slug}/connections/new",
        data={
            "name": name,
            "dsn": dsn,
            "schema_name": "public",
            "interval_minutes": 15,
            "is_active": "y",
        },
    )


# --- CRUD happy paths ------------------------------------------------------


def test_create_connection_persists_ciphertext_not_plaintext(client):
    _register(client)
    # The "Default" project (slug=default) was auto-created by the register
    # flow in #50; we just need to point our connection at it.
    resp = _add_connection(client, slug="default", dsn="postgresql://u:secretpw@h:5432/d")
    assert resp.status_code == 302

    # Read directly from the storage layer — ciphertext, not plain.
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    user = get_user_by_email("u@example.com")
    project = list_projects_for_user(user["id"])[0]
    conns = list_connections_for_project(project["id"])

    assert len(conns) == 1
    stored = conns[0]["dsn_encrypted"]
    assert b"secretpw" not in stored  # not plaintext on disk
    assert isinstance(stored, bytes)
    assert len(stored) > 50  # Fernet ciphertext is base64'd ~50+ chars


def test_list_connections_shows_masked_dsn(client):
    _register(client)
    _add_connection(client, dsn="postgresql://admin:topsecret@db.example.com:5432/app")

    resp = client.get("/projects/default/connections")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    # Password is masked; everything else visible.
    assert "topsecret" not in body
    assert "***" in body
    assert "db.example.com" in body


def test_project_detail_lists_connections(client):
    _register(client)
    _add_connection(client, name="Production DB", dsn="postgresql://u:p@h/d")
    resp = client.get("/projects/default")
    body = resp.get_data(as_text=True)
    assert "Production DB" in body
    assert "В проекте пока нет подключений" not in body


def test_toggle_flips_is_active(client):
    _register(client)
    _add_connection(client)

    # Connection was created active; first toggle → inactive.
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    user = get_user_by_email("u@example.com")
    project = list_projects_for_user(user["id"])[0]
    conn_id = list_connections_for_project(project["id"])[0]["id"]

    client.post(f"/projects/default/connections/{conn_id}/toggle")
    conns = list_connections_for_project(project["id"])
    assert conns[0]["is_active"] is False

    client.post(f"/projects/default/connections/{conn_id}/toggle")
    conns = list_connections_for_project(project["id"])
    assert conns[0]["is_active"] is True


def test_delete_removes_connection(client):
    _register(client)
    _add_connection(client)

    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    user = get_user_by_email("u@example.com")
    project = list_projects_for_user(user["id"])[0]
    conn_id = list_connections_for_project(project["id"])[0]["id"]

    client.post(f"/projects/default/connections/{conn_id}/delete")
    assert list_connections_for_project(project["id"]) == []


# --- Validation ------------------------------------------------------------


@pytest.mark.parametrize("interval", [4, 0, -1, 1441, 9999])
def test_interval_out_of_range_rejected(client, interval):
    _register(client)
    resp = client.post("/projects/default/connections/new", data={
        "name": "x", "dsn": "postgresql://u:p@h/d",
        "schema_name": "public", "interval_minutes": interval,
    })
    # Form re-renders 200; no row created.
    assert resp.status_code == 200
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    user = get_user_by_email("u@example.com")
    project = list_projects_for_user(user["id"])[0]
    assert list_connections_for_project(project["id"]) == []


# --- Ownership isolation ---------------------------------------------------


def test_stranger_cannot_list_connections(client):
    _register(client, "owner@example.com")
    _add_connection(client, name="Hidden")
    _logout(client)

    _register(client, "stranger@example.com")
    # The stranger's session has its own auto-created "default" project,
    # but they shouldn't see the owner's connection even when both share
    # the slug "default" — slug is per-user.
    resp = client.get("/projects/default/connections")
    body = resp.get_data(as_text=True)
    assert "Hidden" not in body


def test_stranger_cannot_toggle_others_connection(client):
    _register(client, "owner2@example.com")
    _add_connection(client, name="Mine")

    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    owner = get_user_by_email("owner2@example.com")
    owner_project = list_projects_for_user(owner["id"])[0]
    owner_conn_id = list_connections_for_project(owner_project["id"])[0]["id"]
    _logout(client)

    _register(client, "stranger2@example.com")
    resp = client.post(f"/projects/default/connections/{owner_conn_id}/toggle")
    # Stranger's "default" project doesn't contain owner_conn_id → 404.
    assert resp.status_code == 404


def test_stranger_cannot_delete_others_connection(client):
    _register(client, "owner3@example.com")
    _add_connection(client)
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    owner = get_user_by_email("owner3@example.com")
    owner_project = list_projects_for_user(owner["id"])[0]
    owner_conn_id = list_connections_for_project(owner_project["id"])[0]["id"]
    _logout(client)

    _register(client, "stranger3@example.com")
    resp = client.post(f"/projects/default/connections/{owner_conn_id}/delete")
    assert resp.status_code == 404


# --- Gating ----------------------------------------------------------------


def test_connections_require_login(client):
    resp = client.get("/projects/default/connections", follow_redirects=False)
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


# --- DSN never reaches logs ------------------------------------------------


def test_plaintext_dsn_does_not_appear_in_logs(client, caplog):
    """The DSNFilter from #56 + storing encrypted at rest should leave
    no place where the plaintext could leak into a log line."""
    import logging
    _register(client)
    with caplog.at_level(logging.DEBUG):
        _add_connection(client, dsn="postgresql://u:supersecretpw@h:5432/d")
    full_log = "\n".join(r.getMessage() for r in caplog.records)
    assert "supersecretpw" not in full_log
