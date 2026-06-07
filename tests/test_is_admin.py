"""System admin flag tests (#220).

Acceptance:
- Колонка ``is_admin`` существует в users после старта (миграция)
- ``ADMIN_EMAIL=foo@example.com`` → юзер с этим email получает is_admin=1
- ``GET /admin/jobs`` от обычного юзера → 403
- ``GET /admin/jobs`` от admin → 200
- Admin-роуты под decorator: /jobs, /jobs/<id>/run, /rollback-checklist, /feature-flags
"""

from __future__ import annotations

import uuid

import pytest
from cryptography.fernet import Fernet

from app import crypto


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.config import settings as cfg

    db_path = tmp_path / "admin.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    return ms


# ── Schema + storage helpers ──────────────────────────────────────────────


def test_users_table_has_is_admin_column(storage):
    """Колонка is_admin создаётся в новой БД (schema file)."""
    cols = storage._existing_columns(storage.get_engine(), "users")
    assert "is_admin" in cols


def test_create_user_defaults_is_admin_false(storage):
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email="user@x.io", password_hash="x")
    user = storage.get_user_by_id(uid)
    assert user["is_admin"] is False


def test_set_user_admin_idempotent_true_then_false(storage):
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email="admin@x.io", password_hash="x")

    assert storage.set_user_admin(uid, True) is True
    assert storage.is_system_admin(uid) is True
    assert storage.get_user_by_id(uid)["is_admin"] is True

    # Идемпотентно: True → True снова → всё ок.
    assert storage.set_user_admin(uid, True) is True

    # Demote обратно.
    assert storage.set_user_admin(uid, False) is True
    assert storage.is_system_admin(uid) is False


def test_set_user_admin_returns_false_for_missing_user(storage):
    assert storage.set_user_admin("ghost-user-id", True) is False


def test_is_system_admin_false_for_missing_user(storage):
    assert storage.is_system_admin("ghost") is False


def test_get_user_by_email_returns_is_admin(storage):
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email="lookup@x.io", password_hash="x")
    storage.set_user_admin(uid, True)
    user = storage.get_user_by_email("lookup@x.io")
    assert user["is_admin"] is True


# ── Migration backfill — existing DB без is_admin column ─────────────────


def test_migration_adds_is_admin_column(tmp_path, monkeypatch):
    """Симулируем pre-#220 БД: users table без is_admin → миграция
    добавляет колонку с DEFAULT 0 для существующих рядов."""
    from sqlalchemy import create_engine, text

    import app.metrics_storage as ms
    from app.config import settings as cfg

    db_path = tmp_path / "legacy.db"
    eng = create_engine(f"sqlite:///{db_path}")

    # Создаём pre-#220 users table без is_admin column.
    with eng.begin() as conn:
        conn.execute(text("""
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_login_at TEXT
            )
        """))
        conn.execute(text(
            "INSERT INTO users (id, email, password_hash, created_at) "
            "VALUES ('u1', 'legacy@x.io', 'h', '2026-01-01T00:00:00')"
        ))
    eng.dispose()

    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)

    # Триггерим _apply_schema → _migrate_existing_schema.
    eng2 = ms.get_engine()
    cols = ms._existing_columns(eng2, "users")
    assert "is_admin" in cols

    # Existing row дефолтится в False.
    user = ms.get_user_by_email("legacy@x.io")
    assert user["is_admin"] is False


# ── Route enforcement — @admin_required ──────────────────────────────────


@pytest.fixture
def auth_client(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "auth.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })
    return app.test_client()


def _register_and_login(client, email="u@x.io", password="supersecret1"):
    client.post("/auth/register", data={
        "email": email, "password": password, "confirm": password,
    })
    # Register уже логинит, но если нужен фреш — re-login.


def test_non_admin_gets_403_on_admin_jobs(auth_client):
    """Acceptance из тикета: GET /admin/jobs от обычного user → 403."""
    _register_and_login(auth_client, "regular@x.io")
    resp = auth_client.get("/admin/jobs")
    assert resp.status_code == 403


def test_non_admin_gets_403_on_feature_flags(auth_client):
    _register_and_login(auth_client, "regular2@x.io")
    resp = auth_client.get("/admin/feature-flags")
    assert resp.status_code == 403


def test_non_admin_gets_403_on_rollback_checklist(auth_client):
    _register_and_login(auth_client, "regular3@x.io")
    resp = auth_client.get("/admin/rollback-checklist")
    assert resp.status_code == 403


def test_admin_gets_200_on_admin_jobs(auth_client):
    from app.metrics_storage import get_user_by_email, set_user_admin

    _register_and_login(auth_client, "admin@x.io")
    user = get_user_by_email("admin@x.io")
    set_user_admin(user["id"], True)

    # Re-login чтобы фреш session с обновлённым is_admin.
    auth_client.post("/auth/logout")
    auth_client.post("/auth/login", data={
        "email": "admin@x.io", "password": "supersecret1",
    })
    resp = auth_client.get("/admin/jobs")
    assert resp.status_code == 200


def test_anon_redirects_not_403(auth_client):
    """Anon user → 302 на /auth/login (login gate _require_login_for_html),
    не 403. Разные семантики: 'не залогинен' vs 'залогинен но не админ'."""
    resp = auth_client.get("/admin/jobs", follow_redirects=False)
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


# ── Auto-promote по ADMIN_EMAIL ──────────────────────────────────────────


def test_auto_promote_makes_existing_user_admin(tmp_path, monkeypatch):
    """ADMIN_EMAIL=existing@x.io → юзер получает is_admin=True после
    create_app. Это шаг promote из create_app::_auto_promote_admin."""
    import app.metrics_storage as ms
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "promote.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    # Создаём юзера ДО app start.
    create_app({"TESTING": True})  # init schema
    ms.create_user(user_id="admin-uid", email="boss@x.io", password_hash="x")
    assert ms.get_user_by_email("boss@x.io")["is_admin"] is False

    # Set ADMIN_EMAIL и реинит app.
    monkeypatch.setattr(cfg, "ADMIN_EMAIL", "boss@x.io")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    create_app({"TESTING": True})

    assert ms.get_user_by_email("boss@x.io")["is_admin"] is True


def test_auto_promote_silently_skips_when_user_missing(tmp_path, monkeypatch):
    """ADMIN_EMAIL установлен, но юзер ещё не зарегистрирован.
    Не должно падать boot — promote применится после регистрации."""
    import app.metrics_storage as ms
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "no-user.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    monkeypatch.setattr(cfg, "ADMIN_EMAIL", "future@x.io")
    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    # Не падать — это main acceptance.
    app = create_app({"TESTING": True})
    assert app is not None


def test_auto_promote_noop_when_admin_email_empty(tmp_path, monkeypatch):
    """Empty ADMIN_EMAIL → ничего не делать (intended dev path)."""
    import app.metrics_storage as ms
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "empty.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    monkeypatch.setattr(cfg, "ADMIN_EMAIL", "")
    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    create_app({"TESTING": True})  # init schema
    ms.create_user(user_id="u1", email="someone@x.io", password_hash="x")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    create_app({"TESTING": True})  # second start

    user = ms.get_user_by_email("someone@x.io")
    assert user["is_admin"] is False
