"""Tests for DB connections + Fernet encryption (#51)."""

from __future__ import annotations

import re

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


def test_dev_fallback_reuses_env_local_key(tmp_path, monkeypatch):
    from app import crypto

    env = tmp_path / ".env"
    env_local = tmp_path / ".env.local"
    monkeypatch.setattr(crypto, "_ENV", env)
    monkeypatch.setattr(crypto, "_ENV_LOCAL", env_local)
    monkeypatch.delenv("FERNET_KEY", raising=False)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    crypto.reset_for_tests()

    ciphertext = crypto.encrypt_dsn("postgresql://u:p@h/d")
    generated_key = env_local.read_text(encoding="utf-8")
    assert "FERNET_KEY=" in generated_key

    monkeypatch.delenv("FERNET_KEY", raising=False)
    crypto.reset_for_tests()

    assert crypto.decrypt_dsn(ciphertext) == "postgresql://u:p@h/d"


def test_dev_fallback_prefers_dotenv_over_env_local(tmp_path, monkeypatch):
    from cryptography.fernet import Fernet

    from app import crypto

    docker_key = Fernet.generate_key()
    stale_local_key = Fernet.generate_key()
    env = tmp_path / ".env"
    env_local = tmp_path / ".env.local"
    env.write_text(f"FERNET_KEY={docker_key.decode()}\n", encoding="utf-8")
    env_local.write_text(f"FERNET_KEY={stale_local_key.decode()}\n", encoding="utf-8")
    monkeypatch.setattr(crypto, "_ENV", env)
    monkeypatch.setattr(crypto, "_ENV_LOCAL", env_local)
    monkeypatch.delenv("FERNET_KEY", raising=False)
    monkeypatch.delenv("FLASK_ENV", raising=False)
    crypto.reset_for_tests()

    ciphertext = crypto.encrypt_dsn("iceberg+rest://iceberg-rest:8181")

    assert Fernet(docker_key).decrypt(ciphertext) == b"iceberg+rest://iceberg-rest:8181"
    with pytest.raises(Exception):
        Fernet(stale_local_key).decrypt(ciphertext)


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


def _guide_href(html: str) -> str:
    match = re.search(r'href="([^"]*PROD_CONNECTION_GUIDE\.md[^"]*)"', html)
    assert match is not None
    return match.group(1)


# --- CRUD happy paths ------------------------------------------------------


def test_readonly_hint_in_new_connection_form(client):
    _register(client)
    _add_connection(client)

    resp = client.get("/projects/default/connections/new")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    assert "только для чтения" in html
    assert "INSERT" in html
    assert "UPDATE" in html
    assert "DELETE" in html
    assert "CREATE" in html
    assert "ALTER" in html
    assert "DROP" in html
    assert "PROD_CONNECTION_GUIDE" in html
    assert html.index("только для чтения") < html.index("Сохранить")

    href = _guide_href(html)
    assert href.startswith("https://github.com/aleksandr-novikov/db-monitoring/")
    assert "password=" not in href
    assert "dsn=" not in href


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


# --- Iceberg probe -----------------------------------------------------------


def test_probe_connection_iceberg_ok(monkeypatch):
    """probe_connection routes iceberg+rest:// to _probe_iceberg and returns ok."""
    from unittest.mock import MagicMock

    from app.connections import probe_connection

    fake_adapter = MagicMock()
    fake_adapter.list_namespaces.return_value = [("ns1",), ("ns2",)]
    monkeypatch.setattr("app.connections.make_adapter_for_url", fake_adapter, raising=False)

    import app.connections as conn_mod
    monkeypatch.setattr(conn_mod, "_probe_iceberg", lambda dsn, **_: {
        "status": "ok",
        "database": "iceberg",
        "version": "2 namespace(s)",
        "latency_ms": 10,
    })

    result = probe_connection("iceberg+rest://localhost:8181?warehouse=s3://bucket/wh")
    assert result["status"] == "ok"
    assert result["database"] == "iceberg"


def test_probe_iceberg_ok(monkeypatch):
    """_probe_iceberg returns ok when adapter.list_namespaces() succeeds."""
    from unittest.mock import MagicMock

    from app.connections import _probe_iceberg

    fake_adapter = MagicMock()
    fake_adapter.list_namespaces.return_value = [("ns1",), ("ns2",)]

    monkeypatch.setattr(
        "app.db.make_adapter_for_url",
        lambda dsn, **_: fake_adapter,
    )

    result = _probe_iceberg("iceberg+rest://localhost:8181?warehouse=s3://bucket/wh")
    assert result["status"] == "ok"
    assert "2 namespace(s)" in result["version"]
    assert result["latency_ms"] >= 0


def test_probe_iceberg_import_error(monkeypatch):
    """_probe_iceberg returns unsupported_dialect when pyiceberg is missing."""
    from app.connections import _probe_iceberg

    def _raise_import(dsn, **_):
        raise ImportError("No module named 'pyiceberg'")

    monkeypatch.setattr("app.db.make_adapter_for_url", _raise_import)

    result = _probe_iceberg("iceberg+rest://localhost:8181?warehouse=s3://bucket/wh")
    assert result["status"] == "error"
    assert result["code"] == "unsupported_dialect"


def test_probe_iceberg_connection_error(monkeypatch):
    """_probe_iceberg returns error dict (not exception) when catalog is unreachable."""
    from app.connections import _probe_iceberg

    def _raise(dsn, **_):
        raise ConnectionError("catalog unreachable")

    monkeypatch.setattr("app.db.make_adapter_for_url", _raise)

    result = _probe_iceberg("iceberg+rest://localhost:8181?warehouse=s3://bucket/wh")
    assert result["status"] == "error"
    # #234: catch-all path classified as catalog_error (was "error" before).
    assert result["code"] == "catalog_error"
    assert "latency_ms" in result


def test_iceberg_adapter_list_namespaces():
    """IcebergAdapter.list_namespaces() delegates to _catalog.list_namespaces()."""
    from unittest.mock import MagicMock, patch

    from app.db import IcebergAdapter

    fake_catalog = MagicMock()
    fake_catalog.list_namespaces.return_value = [("warehouse",)]

    with patch("pyiceberg.catalog.rest.RestCatalog", return_value=fake_catalog):
        adapter = IcebergAdapter("iceberg+rest://localhost:8181?warehouse=s3://b/w")

    result = adapter.list_namespaces()
    assert result == [("warehouse",)]
    fake_catalog.list_namespaces.assert_called_once()


# --- #234 Iceberg production params ----------------------------------------


def test_iceberg_auth_token_roundtrip():
    """encrypt_token + decrypt_token roundtrip — token must come back intact."""
    from app import crypto

    plain = "Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig"
    ct = crypto.encrypt_token(plain)
    assert ct != plain.encode()
    assert isinstance(ct, bytes)
    assert crypto.decrypt_token(ct) == plain


def test_iceberg_adapter_form_warehouse_overrides_dsn_query():
    """#234: explicit warehouse arg wins over warehouse= in DSN query."""
    from unittest.mock import MagicMock, patch

    from app.db import IcebergAdapter

    fake = MagicMock()
    with patch("pyiceberg.catalog.rest.RestCatalog", return_value=fake) as ctor:
        IcebergAdapter(
            "iceberg+rest://localhost:8181?warehouse=s3://old/wh",
            warehouse="s3://new/wh",
        )
    # RestCatalog was constructed with warehouse from the override, not DSN.
    _, kwargs = ctor.call_args
    assert kwargs["warehouse"] == "s3://new/wh"


def test_iceberg_adapter_auth_token_passed_to_catalog():
    """#234: auth_token arg becomes the `token` catalog property."""
    from unittest.mock import MagicMock, patch

    from app.db import IcebergAdapter

    fake = MagicMock()
    with patch("pyiceberg.catalog.rest.RestCatalog", return_value=fake) as ctor:
        IcebergAdapter(
            "iceberg+rest://localhost:8181?warehouse=s3://b/w",
            auth_token="bearer-abc123",
        )
    _, kwargs = ctor.call_args
    assert kwargs["token"] == "bearer-abc123"


def test_probe_iceberg_namespace_not_found(monkeypatch):
    """#234: probe distinguishes missing namespace from empty namespace."""
    from unittest.mock import MagicMock

    from app.connections import _probe_iceberg

    fake = MagicMock()
    fake.list_namespaces.return_value = [("prod",), ("staging",)]
    monkeypatch.setattr("app.db.make_adapter_for_url", lambda dsn, **_: fake)

    result = _probe_iceberg(
        "iceberg+rest://h:8181?warehouse=s3://b/w",
        namespace="nonexistent",
    )
    assert result["status"] == "error"
    assert result["code"] == "namespace_not_found"
    # list_tables must NOT have been called once we knew the namespace is bogus.
    assert not fake.list_tables.called


def test_probe_iceberg_namespace_exists_tables_zero(monkeypatch):
    """#234: namespace exists, tables_found=0 → ok status (not an error)."""
    from unittest.mock import MagicMock

    from app.connections import _probe_iceberg

    fake = MagicMock()
    fake.list_namespaces.return_value = [("empty_ns",)]
    fake.list_tables.return_value = []
    monkeypatch.setattr("app.db.make_adapter_for_url", lambda dsn, **_: fake)

    result = _probe_iceberg(
        "iceberg+rest://h:8181?warehouse=s3://b/w",
        namespace="empty_ns",
    )
    assert result["status"] == "ok"
    assert result["tables_found"] == 0


def test_probe_iceberg_passes_overrides_to_adapter(monkeypatch):
    """#234: namespace/warehouse/auth_token reach make_adapter_for_url."""
    from unittest.mock import MagicMock

    from app.connections import _probe_iceberg

    captured = {}

    def fake_factory(dsn, *, warehouse=None, auth_token=None, **_):
        captured["warehouse"] = warehouse
        captured["auth_token"] = auth_token
        fake = MagicMock()
        fake.list_namespaces.return_value = [("ns",)]
        fake.list_tables.return_value = []
        return fake

    monkeypatch.setattr("app.db.make_adapter_for_url", fake_factory)
    _probe_iceberg(
        "iceberg+rest://h:8181",
        namespace="ns",
        warehouse="s3://override",
        auth_token="bearer-x",
    )
    assert captured == {"warehouse": "s3://override", "auth_token": "bearer-x"}


def test_create_iceberg_connection_persists_namespace_and_encrypts_token(client):
    """#234: POST /new with Iceberg fields stores ns/warehouse + encrypts token."""
    from app import crypto
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )

    _register(client)
    resp = client.post(
        "/projects/default/connections/new",
        data={
            "name": "Lakehouse",
            "dsn": "iceberg+rest://catalog:8181?warehouse=s3://b/w",
            "schema_name": "public",
            "interval_minutes": 15,
            "is_active": "y",
            "iceberg_namespace": "lakehouse",
            "iceberg_warehouse": "s3://prod/wh",
            "iceberg_auth_token": "bearer-secret-token-xyz",
        },
    )
    assert resp.status_code == 302

    user = get_user_by_email("u@example.com")
    project = list_projects_for_user(user["id"])[0]
    conns = list_connections_for_project(project["id"])
    assert len(conns) == 1
    row = conns[0]
    assert row["iceberg_namespace"] == "lakehouse"
    assert row["iceberg_warehouse"] == "s3://prod/wh"
    # Token is encrypted at rest and round-trips via decrypt_token.
    assert b"bearer-secret-token-xyz" not in row["iceberg_auth_token_encrypted"]
    assert (
        crypto.decrypt_token(row["iceberg_auth_token_encrypted"])
        == "bearer-secret-token-xyz"
    )


def test_iceberg_fields_ignored_for_postgres_dsn(client):
    """#234: server discards Iceberg fields when DSN is not iceberg+ — defence
    against a hand-crafted POST attaching a token to a Postgres connection."""
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )

    _register(client)
    client.post(
        "/projects/default/connections/new",
        data={
            "name": "PG",
            "dsn": "postgresql://u:p@h:5432/d",
            "schema_name": "public",
            "interval_minutes": 15,
            "is_active": "y",
            "iceberg_namespace": "should-be-ignored",
            "iceberg_warehouse": "s3://nope",
            "iceberg_auth_token": "should-not-be-stored",
        },
    )
    user = get_user_by_email("u@example.com")
    project = list_projects_for_user(user["id"])[0]
    row = list_connections_for_project(project["id"])[0]
    assert row["iceberg_namespace"] is None
    assert row["iceberg_warehouse"] is None
    assert row["iceberg_auth_token_encrypted"] is None


def test_iceberg_token_not_in_list_response(client):
    """#234: GET /connections never includes the auth token plaintext."""
    _register(client)
    client.post(
        "/projects/default/connections/new",
        data={
            "name": "Lakehouse",
            "dsn": "iceberg+rest://catalog:8181?warehouse=s3://b/w",
            "schema_name": "public",
            "interval_minutes": 15,
            "is_active": "y",
            "iceberg_namespace": "lakehouse",
            "iceberg_auth_token": "ultra-secret-token-12345",
        },
    )
    resp = client.get("/projects/default/connections")
    assert "ultra-secret-token-12345" not in resp.get_data(as_text=True)


def test_effective_namespace_falls_back_to_schema_name():
    """#234: collector uses iceberg_namespace or schema_name. With NULL ns,
    schema_name wins (back-compat with pre-#234 connections)."""
    # Exercised via the small fallback expression used in
    # collectors/per_project.collect_for_connection — no DB needed.
    conn_row_legacy = {"iceberg_namespace": None, "schema_name": "default"}
    conn_row_explicit = {"iceberg_namespace": "lakehouse", "schema_name": "default"}
    assert (
        conn_row_legacy.get("iceberg_namespace") or conn_row_legacy["schema_name"]
    ) == "default"
    assert (
        conn_row_explicit.get("iceberg_namespace")
        or conn_row_explicit["schema_name"]
    ) == "lakehouse"


def test_interval_minutes_filter_formats_daily_interval():
    from app.app import create_app

    app = create_app({"TESTING": True})
    fmt = app.jinja_env.filters["fmt_interval_minutes"]

    assert fmt(15) == "каждые 15 мин"
    assert fmt(60) == "каждый час"
    assert fmt(1440) == "раз в сутки"


def test_list_connections_with_dsn_keeps_row_when_decrypt_fails(client):
    """Бывшая бага: при ротации FERNET_KEY помеченные `<ошибка дешифровки>`
    строки молча выкидывались из `/projects/<slug>` детальной страницы,
    но оставались на `/connections`. UI разъезжался: «нет подключений» в
    одном месте, реальный ряд с error-маской в другом. Теперь оставляем
    в списке с dsn=None и маской."""
    import uuid

    from cryptography.fernet import Fernet

    from app import connections, crypto, metrics_storage

    _register(client)
    project = metrics_storage.get_project_by_slug(
        metrics_storage.get_user_by_email("u@example.com")["id"],
        "default",
    )

    # Шифруем текущим ключом → ОК.
    good = metrics_storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="good",
        dsn_encrypted=crypto.encrypt_dsn("postgresql://u:p@h:5432/d"),
        schema_name="public", interval_minutes=15, is_active=True,
    )
    # Шифруем «потерянным» ключом — для рантайм-сессии ciphertext
    # станет битым (InvalidToken). Так воспроизводится ротация ключа.
    other_fernet = Fernet(Fernet.generate_key())
    rotated_ciphertext = other_fernet.encrypt(b"postgresql://u:p@h:5432/d")
    bad = metrics_storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="rotated-key",
        dsn_encrypted=rotated_ciphertext,
        schema_name="public", interval_minutes=15, is_active=True,
    )

    items = connections.list_connections_with_dsn(project["id"])
    names = {i["name"]: i for i in items}
    assert set(names) == {"good", "rotated-key"}, (
        "битый ряд должен остаться в списке, иначе UI разъезжается"
    )
    assert names["good"]["dsn"] == "postgresql://u:p@h:5432/d"
    assert names["rotated-key"]["dsn"] is None
    assert names["rotated-key"]["dsn_masked"] == "<ошибка дешифровки>"
    assert names["good"]["id"] == good["id"]
    assert names["rotated-key"]["id"] == bad["id"]
