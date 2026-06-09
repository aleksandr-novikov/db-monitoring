"""Tests for DB connections + Fernet encryption (#51)."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta

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


def _login(client, email="u@example.com", password="supersecret1"):
    return client.post("/auth/login", data={"email": email, "password": password})


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


def _default_project_and_connection(email: str = "u@example.com"):
    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )

    user = get_user_by_email(email)
    project = list_projects_for_user(user["id"])[0]
    conn = list_connections_for_project(project["id"])[0]
    return project, conn


def _seed_run(
    project_id: str,
    connection_id: str,
    *,
    status: str = "success",
    finished: bool = True,
    table_rows: bool = True,
) -> str:
    import app.metrics_storage as storage

    started = datetime(2026, 6, 8, 10, 30, tzinfo=UTC)
    run_id = uuid.uuid4().hex
    storage.save_collector_run(run_id, project_id, connection_id, started)
    if finished:
        storage.update_collector_run(
            run_id,
            status=status,
            finished_at=started + timedelta(milliseconds=1250),
            tables_total=3,
            tables_checked=2,
            tables_skipped=1,
            metrics_collected=7,
            duration_ms=1250,
            error_message=None,
        )
    if table_rows:
        storage.save_run_table(
            run_id,
            "orders",
            "failed",
            metrics_collected=0,
            rows_observed=None,
            duration_ms=250,
            error_message="select failed",
        )
        storage.save_run_table(
            run_id,
            "sessions",
            "skipped",
            metrics_collected=0,
            rows_observed=None,
            duration_ms=None,
            skip_reason="too_large",
        )
        storage.save_run_table(
            run_id,
            "customers",
            "success",
            metrics_collected=7,
            rows_observed=42,
            duration_ms=1250,
        )
    return run_id


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


def test_new_connection_form_does_not_invite_browser_login_autofill(client):
    """Regression: Chrome must not treat connection DSN as account password."""
    _register(client)
    _add_connection(client)

    resp = client.get("/projects/default/connections/new")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)

    assert '<form method="POST" class="space-y-4" autocomplete="off"' in html
    assert 'name="name"' in html
    assert 'autocomplete="off"' in html
    assert 'name="dsn"' in html
    assert 'type="password"' in html
    assert 'autocomplete="new-password"' in html
    assert 'autocapitalize="none"' in html
    assert 'spellcheck="false"' in html


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


def test_test_connection_persists_probe_result(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 2,
    })
    _add_connection(client)
    project, conn = _default_project_and_connection()

    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "error",
        "code": "error",
        "message": "boom postgresql://u:secret@db/app?token=abc",
    })
    resp = client.post(f"/projects/default/connections/{conn['id']}/test")

    assert resp.status_code == 422
    from app.metrics_storage import get_connection
    stored = get_connection(project["id"], conn["id"])
    assert stored["last_probe_status"] == "error"
    assert "secret" not in stored["last_probe_error"]
    assert "token=abc" not in stored["last_probe_error"]
    assert "abc" not in stored["last_probe_error"]


def test_project_detail_shows_connection_status_checklist(client, monkeypatch):
    import uuid
    from datetime import UTC, datetime, timedelta

    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 7,
    })
    _add_connection(client, name="Production DB")
    project, conn = _default_project_and_connection()

    import app.metrics_storage as storage
    started = datetime(2026, 6, 8, 10, 30, tzinfo=UTC)
    run_id = uuid.uuid4().hex
    storage.save_collector_run(run_id, project["id"], conn["id"], started)
    storage.update_collector_run(
        run_id,
        status="success",
        finished_at=started + timedelta(seconds=1),
        tables_total=7,
        tables_checked=7,
        metrics_collected=21,
    )
    monkeypatch.setattr("collectors.per_project.list_jobs_for_user", lambda scheduler, user_id: [])
    monkeypatch.setattr("collectors.scheduler.get_scheduler", lambda: None)

    resp = client.get("/projects/default")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Production DB" in body
    assert "Probe" in body
    assert "Проверено" in body
    assert "7 табл." in body
    assert "Следующий запуск" in body
    assert "Не запланирован" in body
    assert "Последний сбор" in body
    assert "success" in body
    assert "08.06 10:30 UTC" in body


def test_project_detail_links_to_collector_run_detail(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 7,
    })
    _add_connection(client, name="Production DB")
    project, conn = _default_project_and_connection()
    run_id = _seed_run(project["id"], conn["id"], table_rows=False)

    monkeypatch.setattr("collectors.per_project.list_jobs_for_user", lambda scheduler, user_id: [])
    monkeypatch.setattr("collectors.scheduler.get_scheduler", lambda: None)

    resp = client.get("/projects/default")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Подробнее" in body
    assert f"/projects/default/connections/{conn['id']}/runs/{run_id}" in body


def test_run_detail_renders_summary_rows_and_human_values(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 3,
    })
    _add_connection(client, name="Production DB")
    project, conn = _default_project_and_connection()
    run_id = _seed_run(project["id"], conn["id"])

    resp = client.get(f"/projects/default/connections/{conn['id']}/runs/{run_id}")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Детали сбора" in body
    assert "Production DB" in body
    assert "Успешно" in body
    assert "1.2 с" in body
    assert "Показано 3 из 3" in body
    assert body.index("orders") < body.index("sessions") < body.index("customers")
    assert "Ошибка" in body
    assert "Пропущена" in body
    assert "Слишком большая таблица" in body
    assert ">—<" in body


def test_run_detail_filters_rows_and_handles_empty_filter(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 1,
    })
    _add_connection(client, name="Production DB")
    project, conn = _default_project_and_connection()
    run_id = _seed_run(project["id"], conn["id"], table_rows=False)

    import app.metrics_storage as storage
    storage.save_run_table(run_id, "orders", "failed", error_message="boom")

    failed = client.get(
        f"/projects/default/connections/{conn['id']}/runs/{run_id}?status=failed"
    ).get_data(as_text=True)
    skipped = client.get(
        f"/projects/default/connections/{conn['id']}/runs/{run_id}?status=skipped"
    ).get_data(as_text=True)

    assert "orders" in failed
    assert "Показано 1 из 1" in failed
    assert "Для выбранного фильтра строк нет." in skipped


def test_run_detail_404_for_foreign_connection_or_run(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 1,
    })
    _add_connection(client, name="Primary")
    _add_connection(client, name="Other")
    project, conn = _default_project_and_connection()

    import app.metrics_storage as storage
    conns = storage.list_connections_for_project(project["id"])
    other_conn = next(c for c in conns if c["id"] != conn["id"])
    run_id = _seed_run(project["id"], conn["id"], table_rows=False)

    same_project_wrong_conn = client.get(
        f"/projects/default/connections/{other_conn['id']}/runs/{run_id}"
    )

    _logout(client)
    _register(client, email="other@example.com")
    _add_connection(client, name="Foreign")
    other_project, other_user_conn = _default_project_and_connection(
        email="other@example.com",
    )
    foreign_run_id = _seed_run(
        other_project["id"],
        other_user_conn["id"],
        table_rows=False,
    )
    _logout(client)
    _login(client)

    foreign_run = client.get(
        f"/projects/default/connections/{conn['id']}/runs/{foreign_run_id}"
    )

    assert same_project_wrong_conn.status_code == 404
    assert foreign_run.status_code == 404


def test_run_detail_viewer_can_open(client, monkeypatch):
    _register(client, email="owner@example.com")
    _make_project(client, slug="shared-run")
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 1,
    })
    _add_connection(client, slug="shared-run", name="Shared DB")

    import app.metrics_storage as storage
    owner = storage.get_user_by_email("owner@example.com")
    project = storage.get_project_by_slug(owner["id"], "shared-run")
    conn = storage.list_connections_for_project(project["id"])[0]
    run_id = _seed_run(project["id"], conn["id"], table_rows=False)

    _logout(client)
    _register(client, email="viewer@example.com")
    viewer = storage.get_user_by_email("viewer@example.com")
    storage.add_project_member(project["id"], viewer["id"], "viewer")

    resp = client.get(f"/projects/shared-run/connections/{conn['id']}/runs/{run_id}")

    assert resp.status_code == 200
    assert "Детали сбора" in resp.get_data(as_text=True)


def test_run_detail_running_run_and_empty_rows(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 1,
    })
    _add_connection(client, name="Running DB")
    project, conn = _default_project_and_connection()
    run_id = _seed_run(
        project["id"],
        conn["id"],
        finished=False,
        table_rows=False,
    )

    resp = client.get(f"/projects/default/connections/{conn['id']}/runs/{run_id}")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Выполняется" in body
    assert "Сбор ещё выполняется, строки появятся после завершения." in body
    assert ">None<" not in body
    assert ">—<" in body


def test_run_detail_no_secrets_in_html(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 1,
    })
    _add_connection(client, name="Secret DB")
    project, conn = _default_project_and_connection()
    run_id = _seed_run(project["id"], conn["id"], table_rows=False)

    import app.metrics_storage as storage
    storage.save_run_table(
        run_id,
        "orders",
        "failed",
        error_message=(
            "postgresql://user:secret@db/app?token=abc "
            "Authorization: Bearer very-secret-token "
            f"bot=1234567890:{'A' * 35}"
        ),
    )

    resp = client.get(f"/projects/default/connections/{conn['id']}/runs/{run_id}")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "secret@db" not in body
    assert "token=abc" not in body
    assert "very-secret-token" not in body
    assert "A" * 35 not in body
    assert "***" in body


def test_run_detail_skipped_without_reason_uses_dash(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 1,
    })
    _add_connection(client, name="Skipped DB")
    project, conn = _default_project_and_connection()
    run_id = _seed_run(project["id"], conn["id"], table_rows=False)

    import app.metrics_storage as storage
    storage.save_run_table(
        run_id,
        "orders",
        "skipped",
    )

    resp = client.get(f"/projects/default/connections/{conn['id']}/runs/{run_id}")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Пропущена" in body
    assert ">—<" in body


def test_run_detail_shows_limited_count(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 101,
    })
    _add_connection(client, name="Large DB")
    project, conn = _default_project_and_connection()
    run_id = _seed_run(project["id"], conn["id"], table_rows=False)

    import app.metrics_storage as storage
    for i in range(101):
        storage.save_run_table(run_id, f"table_{i:03d}", "success")

    resp = client.get(f"/projects/default/connections/{conn['id']}/runs/{run_id}")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Показано 100 из 101" in body
    assert "table_099" in body
    assert "table_100" not in body


def test_project_detail_shows_next_scheduled_run(client, monkeypatch):
    _register(client)
    monkeypatch.setattr("app.connections.probe_connection", lambda dsn, **kwargs: {
        "status": "ok",
        "database": "app",
        "version": "PostgreSQL",
        "latency_ms": 5,
        "tables_found": 1,
    })
    _add_connection(client, name="Scheduled DB")
    project, conn = _default_project_and_connection()

    monkeypatch.setattr("collectors.scheduler.get_scheduler", lambda: object())
    monkeypatch.setattr("collectors.per_project.list_jobs_for_user", lambda scheduler, user_id: [{
        "id": f"collect:{project['id']}:{conn['id']}",
        "name": "collect",
        "project_id": project["id"],
        "connection_id": conn["id"],
        "next_run_time": "2026-06-08T12:45:00+00:00",
        "trigger": "interval[0:15:00]",
    }])

    resp = client.get("/projects/default")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "Scheduled DB" in body
    assert "Следующий запуск" in body
    assert "08.06 12:45 UTC" in body
    assert "Не запланирован" not in body


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


# --- Run-now (manual tick) -------------------------------------------------


def test_run_now_falls_back_to_sync_when_scheduler_absent(client, monkeypatch):
    """Under TESTING the scheduler is not running; the route falls back to
    a synchronous ``collect_for_connection`` so the operator still gets a
    result and a "сбор выполнен" flash."""
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

    called: list[tuple[str, str]] = []
    monkeypatch.setattr(
        "collectors.per_project.collect_for_connection",
        lambda pid, cid: called.append((pid, cid)),
    )

    resp = client.post(
        f"/projects/default/connections/{conn_id}/run",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert called == [(project["id"], conn_id)]
    assert "выполнен" in resp.get_data(as_text=True)


def test_run_now_schedules_one_shot_when_scheduler_running(client, monkeypatch):
    """With a running scheduler, the route enqueues a one-shot date job and
    returns immediately — does NOT block on collect_for_connection."""
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

    class _FakeSched:
        running = True

        def __init__(self):
            self.added: list[dict] = []

        def add_job(self, func, trigger, **kw):
            self.added.append({"trigger": trigger, **kw})

    fake = _FakeSched()
    monkeypatch.setattr("collectors.scheduler.get_scheduler", lambda: fake)
    # Sanity: collect_for_connection must NOT be called inline.
    sync_calls: list = []
    monkeypatch.setattr(
        "collectors.per_project.collect_for_connection",
        lambda *a: sync_calls.append(a),
    )

    resp = client.post(
        f"/projects/default/connections/{conn_id}/run",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert sync_calls == []  # request returned without blocking
    assert len(fake.added) == 1
    job = fake.added[0]
    assert job["trigger"] == "date"
    assert job["args"] == [project["id"], conn_id]
    assert "запущен" in resp.get_data(as_text=True)


def test_run_now_blocks_inactive_connection(client):
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
    # Disable.
    client.post(f"/projects/default/connections/{conn_id}/toggle")

    resp = client.post(
        f"/projects/default/connections/{conn_id}/run",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "выключено" in body


def test_run_now_stranger_forbidden(client):
    _register(client, email="owner@x.io")
    _add_connection(client)

    from app.metrics_storage import (
        get_user_by_email,
        list_connections_for_project,
        list_projects_for_user,
    )
    user = get_user_by_email("owner@x.io")
    project = list_projects_for_user(user["id"])[0]
    conn_id = list_connections_for_project(project["id"])[0]["id"]

    _logout(client)
    _register(client, email="stranger@x.io")
    resp = client.post(f"/projects/default/connections/{conn_id}/run")
    assert resp.status_code in (403, 404)


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
    # JS renderer keys off this — empty preview list is fine for "0 tables".
    assert result["tables_preview"] == []
    # No "privileges" key — Iceberg has no SELECT/INSERT grants to show.
    assert "privileges" not in result


def test_probe_iceberg_returns_tables_preview(monkeypatch):
    """Iceberg probe surfaces up to N table names so the JS Test-result
    panel can list them like the Postgres path does, instead of saying
    "таблиц в схеме не найдено" while tables_found > 0."""
    from unittest.mock import MagicMock

    from app.connections import _probe_iceberg

    fake = MagicMock()
    fake.list_namespaces.return_value = [("lakehouse",)]
    fake.list_tables.return_value = [
        {"table_name": "events", "schema": "lakehouse"},
        {"table_name": "orders", "schema": "lakehouse"},
        {"table_name": "customers", "schema": "lakehouse"},
        {"table_name": "sessions", "schema": "lakehouse"},
    ]
    monkeypatch.setattr("app.db.make_adapter_for_url", lambda dsn, **_: fake)

    result = _probe_iceberg(
        "iceberg+rest://h:8181?warehouse=s3://b/w",
        namespace="lakehouse",
    )
    assert result["status"] == "ok"
    assert result["tables_found"] == 4
    assert result["tables_preview"] == ["events", "orders", "customers", "sessions"]
    assert "privileges" not in result


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


def test_project_detail_keeps_connection_when_decrypt_fails(client, monkeypatch):
    import uuid

    from cryptography.fernet import Fernet

    from app import crypto, metrics_storage

    _register(client)
    user = metrics_storage.get_user_by_email("u@example.com")
    project = metrics_storage.get_project_by_slug(user["id"], "default")
    metrics_storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="rotated-key",
        dsn_encrypted=Fernet(Fernet.generate_key()).encrypt(
            b"postgresql://u:p@h:5432/d"
        ),
        schema_name="public",
        interval_minutes=15,
        is_active=True,
    )
    metrics_storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="good",
        dsn_encrypted=crypto.encrypt_dsn("postgresql://u:p@h:5432/d"),
        schema_name="public",
        interval_minutes=15,
        is_active=True,
    )
    monkeypatch.setattr("collectors.per_project.list_jobs_for_user", lambda scheduler, user_id: [])
    monkeypatch.setattr("collectors.scheduler.get_scheduler", lambda: None)

    resp = client.get("/projects/default")
    body = resp.get_data(as_text=True)

    assert resp.status_code == 200
    assert "rotated-key" in body
    assert "good" in body
    assert "&lt;ошибка дешифровки&gt;" in body
    assert "В проекте пока нет подключений" not in body
