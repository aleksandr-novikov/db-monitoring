"""Tests for #256 — UI safety editor.

Covers:
- GET pre-fill from DB (JSON-list → textarea, defaults shown).
- POST writes via ``update_connection_safety``.
- Iceberg block shown/honored only for iceberg+ DSN.
- Postgres-only fields disabled for non-Postgres (server discards too).
- sample/approx mode rejected for non-Postgres connections.
- Auth-token rotation: empty = leave alone, value = re-encrypt, "clear" = NULL.
- Owner/editor only; stranger gets 403.
"""
from __future__ import annotations

import json

import pytest
from cryptography.fernet import Fernet

from app.app import create_app


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    from app import crypto
    crypto.reset_for_tests()


@pytest.fixture
def app_(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    db_path = tmp_path / "safety.db"
    monkeypatch.setattr(ms.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    return create_app({
        "TESTING": True, "LOGIN_DISABLED": False, "WTF_CSRF_ENABLED": False,
    })


@pytest.fixture
def client(app_):
    return app_.test_client()


def _register(client, email="u@example.com"):
    client.post("/auth/register", data={
        "email": email, "password": "supersecret1", "confirm": "supersecret1",
    })


def _add_pg(client, dsn="postgresql://u:p@h:5432/d"):
    client.post(
        "/projects/default/connections/new",
        data={
            "name": "PG", "dsn": dsn,
            "schema_name": "public", "interval_minutes": 15, "is_active": "y",
        },
    )


def _add_iceberg(client):
    client.post(
        "/projects/default/connections/new",
        data={
            "name": "Lake", "dsn": "iceberg+rest://cat:8181?warehouse=s3://b/w",
            "schema_name": "default", "interval_minutes": 15, "is_active": "y",
            "iceberg_namespace": "lakehouse",
            "iceberg_auth_token": "initial-token",
        },
    )


def _conn_id(client, email="u@example.com"):
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    u = get_user_by_email(email)
    p = list_projects_for_user(u["id"])[0]
    return p["id"], list_connections_for_project(p["id"])[0]["id"]


# --- GET pre-fill ----------------------------------------------------------


def test_edit_get_renders_form_with_db_values(client):
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)
    # Pre-set some safety values directly so we can assert pre-fill.
    from sqlalchemy import text

    from app.metrics_storage import get_engine
    with get_engine().begin() as c:
        c.execute(text(
            "UPDATE connections SET table_allowlist = :al, "
            "max_tables_per_tick = 7, collection_mode = 'sample' "
            "WHERE id = :id"
        ), {"al": json.dumps(["users", "orders"]), "id": cid})

    resp = client.get(f"/projects/default/connections/{cid}/edit")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "users" in body and "orders" in body
    assert ">7<" in body or "value=\"7\"" in body
    assert "sample" in body


def test_edit_redirects_with_flash_when_dsn_ciphertext_unreadable(client):
    """If the stored DSN ciphertext can't be decrypted (Fernet key rotated
    between envs), the edit page used to render with every dialect-gated
    field silently disabled — operator saw "только Postgres" hints and
    had no idea the connection itself was the problem. Now we redirect to
    the list with a clear flash so they know to delete + re-create."""
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)

    # Corrupt the ciphertext directly — simulates Fernet-key rotation:
    # what's on disk doesn't decrypt under the current key.
    from sqlalchemy import text

    from app.metrics_storage import get_engine
    with get_engine().begin() as c:
        c.execute(
            text("UPDATE connections SET dsn_encrypted = :bad WHERE id = :id"),
            {"bad": b"not-a-valid-fernet-token", "id": cid},
        )

    resp = client.get(
        f"/projects/default/connections/{cid}/edit", follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers["Location"].rstrip("/").endswith(
        "/projects/default/connections",
    )
    # Follow the redirect and assert the flash actually rendered.
    body = client.get(
        f"/projects/default/connections/{cid}/edit",
        follow_redirects=True,
    ).get_data(as_text=True)
    assert "не расшифровывается" in body


def test_edit_iceberg_block_only_for_iceberg_dsn(client):
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)
    body_pg = client.get(f"/projects/default/connections/{cid}/edit").get_data(as_text=True)
    assert "Iceberg" not in body_pg or "iceberg_namespace_allowlist" not in body_pg


def test_edit_iceberg_block_visible_for_iceberg(client):
    _register(client)
    _add_iceberg(client)
    pid, cid = _conn_id(client)
    body = client.get(f"/projects/default/connections/{cid}/edit").get_data(as_text=True)
    assert "iceberg_namespace_allowlist" in body
    assert "metadata_only_mode" in body


# --- POST persistence ------------------------------------------------------


def test_post_persists_safety_fields(client):
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)
    resp = client.post(
        f"/projects/default/connections/{cid}/edit",
        data={
            "table_allowlist": "users\norders",
            "table_denylist": "audit_logs",
            "max_tables_per_tick": 5,
            "skip_tables_larger_than_gb": "10",
            "statement_timeout_ms": 12_000,
            "collection_mode": "full",
        },
    )
    assert resp.status_code == 302

    from app.metrics_storage import get_connection
    row = get_connection(pid, cid)
    assert row["table_allowlist"] == json.dumps(["users", "orders"])
    assert row["table_denylist"] == json.dumps(["audit_logs"])
    assert row["max_tables_per_tick"] == 5
    assert row["skip_tables_larger_than_gb"] == 10.0
    assert row["statement_timeout_ms"] == 12_000
    assert row["collection_mode"] == "full"


def test_post_empty_skip_size_clears_to_null(client):
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)
    client.post(
        f"/projects/default/connections/{cid}/edit",
        data={
            "max_tables_per_tick": 50,
            "skip_tables_larger_than_gb": "",  # explicit empty → NULL
            "statement_timeout_ms": 30_000,
            "collection_mode": "full",
        },
    )
    from app.metrics_storage import get_connection
    assert get_connection(pid, cid)["skip_tables_larger_than_gb"] is None


def test_post_rejects_sample_for_non_postgres(client, monkeypatch):
    """Saving sample/approx on a ClickHouse connection must fail loudly,
    not silently downgrade. We stub the onboarding probe + scheduler so
    the test doesn't need to reach a live ClickHouse instance."""
    monkeypatch.setattr(
        "app.connections.probe_connection",
        lambda dsn, **_: {"status": "ok", "database": "x", "version": "y",
                          "latency_ms": 0},
    )
    monkeypatch.setattr(
        "collectors.per_project.add_job_for_connection",
        lambda *a, **kw: None,
    )
    _register(client)
    client.post(
        "/projects/default/connections/new",
        data={
            "name": "CH", "dsn": "clickhouse+native://u:p@h:9000/d",
            "schema_name": "default", "interval_minutes": 15, "is_active": "y",
        },
    )
    pid, cid = _conn_id(client)
    resp = client.post(
        f"/projects/default/connections/{cid}/edit",
        data={
            "max_tables_per_tick": 50,
            "statement_timeout_ms": 30_000,
            "collection_mode": "sample",
        },
        follow_redirects=False,
    )
    # 200 with re-rendered form (not 302) — flash carries the error.
    assert resp.status_code == 200
    assert b"Postgres" in resp.data
    from app.metrics_storage import get_connection
    # Mode wasn't changed.
    assert get_connection(pid, cid)["collection_mode"] in (None, "full")


# --- Iceberg token rotation -----------------------------------------------


def test_token_empty_leaves_existing_alone(client):
    """Empty iceberg_auth_token field → existing ciphertext unchanged."""
    _register(client)
    _add_iceberg(client)
    pid, cid = _conn_id(client)
    from app.metrics_storage import get_connection
    before = get_connection(pid, cid)["iceberg_auth_token_encrypted"]
    assert before is not None  # initial-token was stored

    client.post(
        f"/projects/default/connections/{cid}/edit",
        data={
            "max_tables_per_tick": 50,
            "statement_timeout_ms": 30_000,
            "collection_mode": "full",
            "iceberg_namespace": "lakehouse",
            "iceberg_auth_token": "",   # untouched
        },
    )
    after = get_connection(pid, cid)["iceberg_auth_token_encrypted"]
    assert after == before


def test_token_clear_nukes_ciphertext(client):
    _register(client)
    _add_iceberg(client)
    pid, cid = _conn_id(client)

    client.post(
        f"/projects/default/connections/{cid}/edit",
        data={
            "max_tables_per_tick": 50,
            "statement_timeout_ms": 30_000,
            "collection_mode": "full",
            "iceberg_namespace": "lakehouse",
            "iceberg_auth_token_clear": "y",
        },
    )
    from app.metrics_storage import get_connection
    assert get_connection(pid, cid)["iceberg_auth_token_encrypted"] is None


def test_token_new_value_reencrypts(client):
    _register(client)
    _add_iceberg(client)
    pid, cid = _conn_id(client)

    client.post(
        f"/projects/default/connections/{cid}/edit",
        data={
            "max_tables_per_tick": 50,
            "statement_timeout_ms": 30_000,
            "collection_mode": "full",
            "iceberg_namespace": "lakehouse",
            "iceberg_auth_token": "rotated-secret",
        },
    )
    from app import crypto
    from app.metrics_storage import get_connection
    ct = get_connection(pid, cid)["iceberg_auth_token_encrypted"]
    assert ct is not None
    assert crypto.decrypt_token(ct) == "rotated-secret"


def test_iceberg_fields_ignored_for_non_iceberg(client):
    """Hand-crafted POST with Iceberg fields on a PG connection: server
    discards them, no leak into iceberg_* columns."""
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)
    client.post(
        f"/projects/default/connections/{cid}/edit",
        data={
            "max_tables_per_tick": 50,
            "statement_timeout_ms": 30_000,
            "collection_mode": "full",
            "iceberg_namespace": "leak-attempt",
            "iceberg_auth_token": "should-not-store",
        },
    )
    from app.metrics_storage import get_connection
    row = get_connection(pid, cid)
    assert row["iceberg_namespace"] is None
    assert row["iceberg_auth_token_encrypted"] is None


# --- ACL -------------------------------------------------------------------


def test_stranger_cannot_edit(client):
    _register(client, email="owner@x.io")
    _add_pg(client)
    pid, cid = _conn_id(client, email="owner@x.io")

    client.post("/auth/logout")
    _register(client, email="stranger@x.io")
    resp = client.get(f"/projects/default/connections/{cid}/edit")
    # 404 because the slug under the stranger's account has no such project.
    assert resp.status_code in (403, 404)


# --- Storage helper allow-list -------------------------------------------


def test_update_connection_safety_drops_unknown_columns(client):
    """Hand-crafted update with a dangerous column name (e.g. dsn_encrypted)
    must be silently dropped — only whitelisted safety columns reach SQL."""
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)
    from app.metrics_storage import get_connection, update_connection_safety
    before_dsn = get_connection(pid, cid)["dsn_encrypted"]

    update_connection_safety(
        project_id=pid, connection_id=cid,
        updates={
            "dsn_encrypted": b"injected",
            "project_id": "other",
            "max_tables_per_tick": 9,
        },
    )
    row = get_connection(pid, cid)
    assert row["max_tables_per_tick"] == 9
    assert row["dsn_encrypted"] == before_dsn  # NOT overwritten
    assert row["project_id"] == pid


def test_update_with_no_allowed_columns_is_noop(client):
    _register(client)
    _add_pg(client)
    pid, cid = _conn_id(client)
    from app.metrics_storage import update_connection_safety
    assert update_connection_safety(
        project_id=pid, connection_id=cid,
        updates={"created_at": "evil", "name": "evil"},
    ) is False
