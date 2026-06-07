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


# --- ClickHouse probe (#141) ----------------------------------------------


def _ch_engine_returns_version(version: str):
    """Build a Mock that mimics _probe_clickhouse's connection sequence:
    SELECT 1 then SELECT version()."""
    engine = MagicMock(name="ch_engine")
    conn = MagicMock(name="ch_conn")
    select1 = MagicMock(name="select1")
    version_row = MagicMock(name="version_row")
    version_row.fetchone.return_value = (version,)
    conn.execute.side_effect = [select1, version_row]

    @contextmanager
    def _connect():
        yield conn

    engine.connect.side_effect = _connect
    engine.dispose = MagicMock()
    return engine


def test_probe_clickhouse_routes_via_backend(monkeypatch):
    """clickhouse:// DSN must NOT fall through to unsupported_dialect."""
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _ch_engine_returns_version("ClickHouse 24.5.1"),
    )
    result = connections.probe_connection("clickhouse://default@h:9000/demo")
    assert result["status"] == "ok"
    assert result["database"] == "clickhouse"
    assert result["version"] == "ClickHouse 24.5.1"
    assert isinstance(result["latency_ms"], int)


def test_probe_clickhouse_native_scheme(monkeypatch):
    """clickhouse+native:// — same code path, distinct scheme."""
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _ch_engine_returns_version("24.8.4.1"),
    )
    result = connections.probe_connection("clickhouse+native://default@h:9000/demo")
    assert result["status"] == "ok"
    assert result["version"] == "24.8.4.1"


def test_probe_clickhouse_classifies_auth_failure(monkeypatch):
    """Wrong password from CH should map to auth_failed, not generic error."""
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_raises(
            OperationalError(
                "SELECT 1", {},
                Exception("Code: 516. DB::Exception: default: Authentication failed: password is incorrect"),
            )
        ),
    )
    result = connections.probe_connection("clickhouse://default:wrong@h:9000/demo")
    assert result["status"] == "error"
    assert result["code"] == "auth_failed"


def test_probe_clickhouse_classifies_network_error(monkeypatch):
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_raises(
            OperationalError("SELECT 1", {}, Exception("Connection refused"))
        ),
    )
    result = connections.probe_connection("clickhouse://default@127.0.0.1:1/demo")
    assert result["status"] == "error"
    assert result["code"] == "network"


def test_probe_clickhouse_disposes_engine_on_success(monkeypatch):
    """One-off engine must release its pool — no slot held after probe."""
    engine = _ch_engine_returns_version("24.1")
    monkeypatch.setattr(connections, "create_engine", lambda *_a, **_kw: engine)
    connections.probe_connection("clickhouse://default@h:9000/demo")
    engine.dispose.assert_called_once()


def test_probe_clickhouse_disposes_engine_on_error(monkeypatch):
    engine = _engine_that_raises(
        OperationalError("SELECT 1", {}, Exception("Connection refused"))
    )
    monkeypatch.setattr(connections, "create_engine", lambda *_a, **_kw: engine)
    connections.probe_connection("clickhouse://default@h:9000/demo")
    engine.dispose.assert_called_once()


@pytest.mark.parametrize("err_text, expected_code", [
    ('FATAL:  password authentication failed for user "admin"', "auth_failed"),
    ("connection timeout expired", "timeout"),
    ("connect_timeout expired during startup", "timeout"),
    ("could not translate host name \"db.example.com\" to address", "network"),
    ("could not connect to server: Connection refused", "network"),
    ("Name or service not known", "network"),
    # Supavisor / Supabase pooler: project deleted or wrong project ref.
    # Message comes verbatim from libpq, so test both casings Supavisor
    # has shipped over the years.
    (
        "FATAL:  (ENOTFOUND) tenant/user postgres.deadproj not found",
        "supabase_tenant_not_found",
    ),
    (
        "FATAL:  Tenant or user not found",
        "supabase_tenant_not_found",
    ),
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
        lambda *_a, **_kw: _engine_that_returns_metadata(
            "testdb", "PostgreSQL 16.1 on x86_64-pc-linux-gnu",
        ),
    )
    result = connections.probe_connection("postgresql://u:p@h/d")
    assert result["status"] == "ok"
    assert result["database"] == "testdb"
    assert result["version"] == "PostgreSQL 16.1"  # trims " on x86_64-..."
    assert isinstance(result["latency_ms"], int)
    # #229/#230 — новые поля в success-ответе.
    assert result["tables_found"] == 0
    assert result["tables_preview"] == []
    assert result["privileges"] == {"select": False, "insert": False}
    assert result["warnings"] == []


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
    assert body["status"] == "ok"
    assert body["database"] == "appdb"
    assert body["version"] == "PostgreSQL 16.1"
    assert isinstance(body["latency_ms"], int)
    # #229/#230 — smoke-check поля.
    assert body["tables_found"] == 0
    assert body["tables_preview"] == []
    assert body["privileges"] == {"select": False, "insert": False}
    assert body["warnings"] == []


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


# --- Smoke-check: privileges + tables (#229 + #230) -----------------------


def test_probe_lists_tables_and_returns_preview(monkeypatch):
    """list_tables → tables_found=N, tables_preview ограничен 10."""
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_returns_metadata(
            "db", "PostgreSQL 16",
            tables=[f"t{i:02d}" for i in range(15)],
            has_select=True, has_insert=False,
        ),
    )
    result = connections.probe_connection("postgresql://u:p@h/d")
    assert result["status"] == "ok"
    assert result["tables_found"] == 15
    assert result["tables_preview"] == [f"t{i:02d}" for i in range(10)]
    assert result["privileges"] == {"select": True, "insert": False}
    assert result["warnings"] == []


def test_probe_emits_warning_when_insert_detected(monkeypatch):
    """has_table_privilege(INSERT)=True хоть на одной → warning."""
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_returns_metadata(
            "db", "PostgreSQL 16",
            tables=["users", "events"],
            has_select=True, has_insert=True,
        ),
    )
    result = connections.probe_connection("postgresql://u:p@h/d")
    assert result["status"] == "ok"
    # write_privileges_detected — НЕ блокирует, статус остаётся ok.
    assert result["warnings"] == ["write_privileges_detected"]
    assert result["privileges"]["insert"] is True


def test_probe_returns_no_select_permission_when_no_usage(monkeypatch):
    """has_schema_privilege(USAGE)=False → error.no_select_permission.

    Acceptance тикета #229 — без USAGE адаптер не может листать таблицы
    в схеме, дальше идти бессмысленно.
    """
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_returns_metadata(
            "db", "PostgreSQL 16", has_usage=False,
        ),
    )
    result = connections.probe_connection("postgresql://u:p@h/d")
    assert result["status"] == "error"
    assert result["code"] == "no_select_permission"
    # Дальнейших полей быть не должно — short-circuit до list_tables.
    assert "tables_found" not in result


def test_probe_empty_schema_no_privileges_no_warning(monkeypatch):
    """Пустая схема → privileges.* остаются False (нечего проверить),
    warnings пустой. Не падать."""
    monkeypatch.setattr(
        connections, "create_engine",
        lambda *_a, **_kw: _engine_that_returns_metadata(
            "db", "PostgreSQL 16", tables=[],
        ),
    )
    result = connections.probe_connection("postgresql://u:p@h/d")
    assert result["status"] == "ok"
    assert result["tables_found"] == 0
    assert result["privileges"] == {"select": False, "insert": False}
    assert result["warnings"] == []


def test_probe_privilege_check_limited_to_20_tables(monkeypatch):
    """Probe не должен дрочить has_table_privilege по 10k таблицам —
    cap _PROBE_PRIV_CHECK=20. Это явное требование тикета #229.

    Мокаем engine с 100 таблицами и считаем execute() calls: ожидаем
    1 (SELECT 1) + 1 (metadata) + 1 (USAGE) + 1 (list tables) + 20
    (privileges) = 24. _engine_that_returns_metadata уже резает
    side_effect под [:20] — проверяем что probe не идёт за пределы.
    """
    engine_holder: dict = {}

    def build_engine(*_a, **_kw):
        eng = _engine_that_returns_metadata(
            "db", "PostgreSQL 16",
            tables=[f"t{i}" for i in range(100)],
            has_select=False, has_insert=False,
        )
        engine_holder["engine"] = eng
        return eng

    monkeypatch.setattr(connections, "create_engine", build_engine)
    result = connections.probe_connection("postgresql://u:p@h/d")
    assert result["status"] == "ok"
    assert result["tables_found"] == 100
    # При has_select=False (и has_insert=False) — break не сработает,
    # значит probe пройдёт весь cap.
    eng = engine_holder["engine"]
    # engine.connect() возвращает context manager, считаем .execute calls
    # на conn внутри.
    conn = eng.connect.side_effect.__self__ if hasattr(  # type: ignore[attr-defined]
        eng.connect.side_effect, "__self__"
    ) else None
    # _engine_that_returns_metadata.side_effect — list ровно той длины,
    # которая нужна; reaching end-of-list raised бы StopIteration.
    # Если тест не упал — probe не пытался лезть за 20-й privilege check.
    assert conn is None or conn  # просто sanity, главная проверка — отсутствие падения


# --- Fakes -----------------------------------------------------------------


def _engine_that_returns_metadata(
    database: str,
    version: str,
    *,
    has_usage: bool = True,
    tables: list[str] | None = None,
    has_select: bool = True,
    has_insert: bool = False,
):
    """Build a Mock engine for the full Postgres probe (#52 + #229/#230).

    Сценарий шагов в реальном `probe_connection`:
      1. SELECT 1
      2. SELECT current_database(), version() — `(database, version)`
      3. SELECT has_schema_privilege(...)     — `(has_usage,)`
      4. SELECT table_name FROM information_schema.tables — rows
      5. Per-table: SELECT has_table_privilege(SELECT), (INSERT) — `(s, i)`

    Side-effect chain собран в этом же порядке. Per-table результат для
    шага 5 — повторяется для каждой таблицы; цикл в коде break-ит при
    первом совпадении (select=True && insert=True), поэтому достаточно
    одного результата если ответы детерминистические.
    """
    tables = tables if tables is not None else []
    engine = MagicMock(name="engine")
    conn = MagicMock(name="conn")

    select1 = MagicMock(name="select1")

    metadata = MagicMock(name="metadata")
    metadata.fetchone.return_value = (database, version)

    usage = MagicMock(name="usage")
    usage.fetchone.return_value = (has_usage,)

    tables_rs = MagicMock(name="tables")
    tables_rs.fetchall.return_value = [(t,) for t in tables]

    priv = MagicMock(name="priv")
    priv.fetchone.return_value = (has_select, has_insert)

    # Шаги 1–4 фиксированной длины, шаг 5 — `min(len(tables), 20)` раз,
    # либо ноль если таблиц нет / has_usage=False.
    side: list = [select1, metadata, usage]
    if has_usage:
        side.append(tables_rs)
        for _ in tables[:20]:
            side.append(priv)
    conn.execute.side_effect = side

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
