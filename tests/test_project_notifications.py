"""Tests for #143 per-project Telegram configuration.

Covers:
- ``project_notifications`` CRUD with Fernet-encrypted bot_token
- Settings blueprint GET/POST/test/disable
- DSNFilter scrubs Telegram bot tokens from logs
- Cross-tenant isolation — one project's config does not leak to another
"""
from __future__ import annotations

import logging

import pytest
from cryptography.fernet import Fernet

from app import crypto

# Synthetic Telegram bot tokens — bot_id is "0000000001"+ (real Telegram
# bot IDs are non-zero integers; leading zeros are syntactically invalid),
# hash is monotone placeholder. Format matches the regex our DSNFilter
# and form validator enforce (\d{8,12}:[A-Za-z0-9_-]{35}) so the code
# under test exercises the real path, but GitHub's secret-scanner won't
# flag the pattern as a credible leaked token. Centralised at module
# level so the test surface has exactly one place to audit.
_FAKE_BOT_ID = "0000000001"
_FAKE_BOT_ID_B = "0000000002"
_FAKE_TG_TOKEN = _FAKE_BOT_ID + ":" + "A" * 35
_FAKE_TG_TOKEN_B = _FAKE_BOT_ID_B + ":" + "B" * 35


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


# ── CRUD ───────────────────────────────────────────────────────────────────

@pytest.fixture
def storage(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.config import settings
    db_path = tmp_path / "metrics.db"
    monkeypatch.setattr(settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    return ms


def _seed_project(storage, *, slug="default", name="Default") -> str:
    """Create a user + project and return the project id."""
    import uuid

    from app.metrics_storage import create_project, create_user
    user = create_user(
        user_id=uuid.uuid4().hex,
        email=f"u-{uuid.uuid4().hex[:6]}@example.com",
        password_hash="x",
    )
    project = create_project(
        project_id=uuid.uuid4().hex,
        user_id=user["id"],
        name=name,
        slug=slug,
    )
    return project["id"]


def test_get_missing_returns_none(storage):
    pid = _seed_project(storage)
    assert storage.get_project_notifications(pid) is None


def test_save_then_get_round_trip(storage):
    pid = _seed_project(storage)
    token = crypto.encrypt_token(_FAKE_TG_TOKEN)
    storage.save_project_notifications(
        pid,
        telegram_bot_token=token,
        telegram_chat_id="987654321",
        throttle_minutes=15,
    )
    row = storage.get_project_notifications(pid)
    assert row is not None
    assert row["telegram_chat_id"] == "987654321"
    assert row["throttle_minutes"] == 15
    # Stored ciphertext decrypts back to the original.
    assert crypto.decrypt_token(row["telegram_bot_token"]) == \
        _FAKE_TG_TOKEN


def test_get_project_notifications_normalizes_postgres_memoryview(storage, monkeypatch):
    token = crypto.encrypt_token(_FAKE_TG_TOKEN)

    class _Result:
        def fetchone(self):
            return ("pid", memoryview(token), "987654321", 15, "2026-06-06T00:00:00+00:00")

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return None

        def execute(self, _stmt, params):
            assert params == {"pid": "pid"}
            return _Result()

    class _Engine:
        def connect(self):
            return _Conn()

    monkeypatch.setattr(storage, "get_engine", lambda: _Engine())

    row = storage.get_project_notifications("pid")

    assert isinstance(row["telegram_bot_token"], bytes)
    assert crypto.decrypt_token(row["telegram_bot_token"]) == _FAKE_TG_TOKEN


def test_save_is_upsert(storage):
    pid = _seed_project(storage)
    tok1 = crypto.encrypt_token(_FAKE_TG_TOKEN)
    tok2 = crypto.encrypt_token(_FAKE_TG_TOKEN_B)
    storage.save_project_notifications(
        pid, telegram_bot_token=tok1, telegram_chat_id="111", throttle_minutes=30,
    )
    storage.save_project_notifications(
        pid, telegram_bot_token=tok2, telegram_chat_id="222", throttle_minutes=60,
    )
    row = storage.get_project_notifications(pid)
    assert row["telegram_chat_id"] == "222"
    assert row["throttle_minutes"] == 60
    assert crypto.decrypt_token(row["telegram_bot_token"]).startswith(_FAKE_BOT_ID_B + ":")


def test_delete_wipes_row(storage):
    pid = _seed_project(storage)
    storage.save_project_notifications(
        pid,
        telegram_bot_token=crypto.encrypt_token(_FAKE_TG_TOKEN),
        telegram_chat_id="42",
    )
    assert storage.get_project_notifications(pid) is not None
    storage.delete_project_notifications(pid)
    assert storage.get_project_notifications(pid) is None


def test_cross_tenant_isolation(storage):
    """Project A's config does not appear in project B's lookups."""
    pid_a = _seed_project(storage, slug="alice", name="Alice")
    pid_b = _seed_project(storage, slug="bob", name="Bob")
    storage.save_project_notifications(
        pid_a,
        telegram_bot_token=crypto.encrypt_token(_FAKE_TG_TOKEN),
        telegram_chat_id="aaa",
    )
    assert storage.get_project_notifications(pid_a) is not None
    assert storage.get_project_notifications(pid_b) is None


def test_cascade_on_project_delete(storage):
    """If the parent project is removed, its notifications row goes too —
    matches the connections / metrics CASCADE pattern (#51, #53)."""
    from sqlalchemy import text
    pid = _seed_project(storage)
    storage.save_project_notifications(
        pid,
        telegram_bot_token=crypto.encrypt_token(_FAKE_TG_TOKEN),
        telegram_chat_id="42",
    )
    # PRAGMA on SQLite so FK actually cascades (Sprint 3 enables it on
    # engine setup; here we depend on that wiring being live).
    with storage.get_engine().begin() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON"))
        conn.execute(text("DELETE FROM projects WHERE id = :pid"), {"pid": pid})
    assert storage.get_project_notifications(pid) is None


# ── Migration: existing single-tenant throttle table ───────────────────────

def test_old_telegram_throttle_table_gets_recreated(tmp_path, monkeypatch):
    """Pre-#143 DBs have telegram_throttle without project_id. The migration
    must rebuild the table with the new PK without crashing."""
    from sqlalchemy import create_engine, text

    import app.metrics_storage as ms
    from app.config import settings

    db_path = tmp_path / "old.db"
    # Manually create old-style throttle table.
    eng = create_engine(f"sqlite:///{db_path}")
    with eng.begin() as conn:
        conn.execute(text(
            "CREATE TABLE telegram_throttle ("
            "  table_name TEXT NOT NULL,"
            "  event_key TEXT NOT NULL,"
            "  last_sent_at TEXT NOT NULL,"
            "  PRIMARY KEY (table_name, event_key))"
        ))
    eng.dispose()

    # Now boot the app's storage layer pointing at this DB.
    monkeypatch.setattr(settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)

    # Forcing engine init runs _apply_schema → _migrate_existing_schema.
    eng2 = ms.get_engine()
    cols = ms._existing_columns(eng2, "telegram_throttle")
    assert "project_id" in cols


# ── DSNFilter / log scrubber ───────────────────────────────────────────────

def test_dsn_filter_scrubs_telegram_token():
    """#143: bot tokens in log lines must be masked, keeping the public
    bot_id but redacting the secret hash half."""
    from app.security import _scrub
    msg = "Sending via 0000000001:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA to chat 42"
    scrubbed = _scrub(msg)
    assert "AAAA" not in scrubbed
    assert "0000000001:***" in scrubbed
    # The chat_id and surrounding context survive unchanged.
    assert "chat 42" in scrubbed


def test_dsn_filter_scrubs_token_in_exception():
    """Exceptions logged via %s reach the formatter as str(exc); _scrub
    must catch BaseException too. We construct a plausible OperationalError
    string that contains a token."""
    from app.security import _scrub
    err = RuntimeError("Telegram replied 401: token=0000000001:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA invalid")
    scrubbed = _scrub(err)
    assert "AAAA" not in scrubbed
    assert "0000000001:***" in scrubbed


def test_dsn_filter_does_not_touch_short_numbers():
    """The regex requires bot_id 8–12 digits + ':' + exact 35 hash chars.
    Random short numbers like ports or IDs must NOT trigger."""
    from app.security import _scrub
    assert _scrub("connecting to host:5432 with user:42") == \
        "connecting to host:5432 with user:42"


def test_dsnfilter_via_logging_pipeline(caplog):
    """End-to-end through the real logging pipeline — install_log_record_scrubber
    rewrites msg/args at record construction so caplog sees the scrubbed value."""
    from app.security import install_log_record_scrubber
    install_log_record_scrubber()

    logger = logging.getLogger("test_143_token")
    with caplog.at_level(logging.WARNING, logger="test_143_token"):
        logger.warning(
            "bot send failed for %s",
            _FAKE_TG_TOKEN,
        )

    rendered = caplog.records[-1].getMessage()
    assert "AAAA" not in rendered
    assert "0000000001:***" in rendered


# ── Settings blueprint ─────────────────────────────────────────────────────

@pytest.fixture
def app_(tmp_path, monkeypatch):
    import app.metrics_storage as ms_
    from app.app import create_app
    from app.config import settings
    db_path = tmp_path / "ob.db"
    monkeypatch.setattr(settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms_, "_engine", None)
    monkeypatch.setattr(ms_, "_initialized", False)

    import app.api
    import app.dashboard
    import app.db
    def fake_list(schema=None):
        return []
    monkeypatch.setattr(app.db, "list_tables", fake_list)
    monkeypatch.setattr(app.api, "list_tables", fake_list)

    return create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })


@pytest.fixture
def client(app_):
    return app_.test_client()


def _register(client, email="u@example.com"):
    resp = client.post(
        "/auth/register",
        data={"email": email, "password": "supersecret1", "confirm": "supersecret1"},
        follow_redirects=False,
    )
    from app.projects import create_default_project_for
    from app.metrics_storage import get_user_by_email
    user = get_user_by_email(email)
    if user:
        create_default_project_for(user["id"])
    return resp


def test_settings_page_renders_for_authed_owner(client):
    _register(client)
    resp = client.get("/projects/default/settings/notifications")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Telegram" in body
    assert "Bot Token" in body
    assert "Chat ID" in body


def test_settings_page_requires_login(client):
    """Anon hits /auth/login redirect, not the form."""
    resp = client.get("/projects/default/settings/notifications", follow_redirects=False)
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


def test_settings_page_404_for_stranger_slug(client):
    _register(client, "owner@example.com")
    # /projects/<unknown>/settings/notifications must 404 (ownership check).
    resp = client.get("/projects/nonexistent/settings/notifications")
    assert resp.status_code == 404


def test_save_encrypts_token_and_persists(client):
    _register(client, "save@example.com")
    resp = client.post(
        "/projects/default/settings/notifications",
        data={
            "telegram_bot_token": _FAKE_TG_TOKEN,
            "telegram_chat_id": "42",
            "throttle_minutes": "30",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302  # redirect after save

    # Pull project_id from session-backed ownership chain to look it up.
    from app.metrics_storage import get_project_by_slug, get_user_by_email
    user = get_user_by_email("save@example.com")
    project = get_project_by_slug(user["id"], "default")
    from app.metrics_storage import get_project_notifications
    row = get_project_notifications(project["id"])
    assert row is not None
    assert row["telegram_chat_id"] == "42"
    assert crypto.decrypt_token(row["telegram_bot_token"]).startswith(_FAKE_BOT_ID + ":")


def test_save_with_empty_token_keeps_old(client):
    """Editing chat_id without re-entering token preserves the saved token."""
    _register(client, "keep@example.com")
    # First save with token.
    client.post(
        "/projects/default/settings/notifications",
        data={
            "telegram_bot_token": _FAKE_TG_TOKEN,
            "telegram_chat_id": "111",
            "throttle_minutes": "30",
        },
    )
    # Second save with empty token, new chat_id.
    client.post(
        "/projects/default/settings/notifications",
        data={
            "telegram_bot_token": "",
            "telegram_chat_id": "999",
            "throttle_minutes": "60",
        },
    )
    from app.metrics_storage import (
        get_project_by_slug,
        get_project_notifications,
        get_user_by_email,
    )
    user = get_user_by_email("keep@example.com")
    project = get_project_by_slug(user["id"], "default")
    row = get_project_notifications(project["id"])
    assert row["telegram_chat_id"] == "999"
    assert row["throttle_minutes"] == 60
    # Token unchanged from initial save.
    assert crypto.decrypt_token(row["telegram_bot_token"]).startswith(_FAKE_BOT_ID + ":")


def test_disable_wipes_config(client):
    _register(client, "disable@example.com")
    client.post(
        "/projects/default/settings/notifications",
        data={
            "telegram_bot_token": _FAKE_TG_TOKEN,
            "telegram_chat_id": "42",
            "throttle_minutes": "30",
        },
    )
    resp = client.post("/projects/default/settings/notifications/disable",
                       follow_redirects=False)
    assert resp.status_code == 302
    from app.metrics_storage import (
        get_project_by_slug,
        get_project_notifications,
        get_user_by_email,
    )
    user = get_user_by_email("disable@example.com")
    project = get_project_by_slug(user["id"], "default")
    assert get_project_notifications(project["id"]) is None


def test_test_button_calls_send_message_without_persisting(client, monkeypatch):
    """The Test action submits form values to /test, calls send_message
    once, audits the delivery attempt, and does NOT write a
    project_notifications row."""
    _register(client, "test@example.com")
    sent = {}

    def fake_send(text, *, bot_token, chat_id):
        sent["text"] = text
        sent["bot_token"] = bot_token
        sent["chat_id"] = chat_id
        return True, None

    monkeypatch.setattr("app.notifications.telegram.send_message", fake_send)

    resp = client.post(
        "/projects/default/settings/notifications/test",
        data={
            "telegram_bot_token": _FAKE_TG_TOKEN,
            "telegram_chat_id": "42",
            "throttle_minutes": "30",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert sent["bot_token"] == _FAKE_TG_TOKEN
    assert sent["chat_id"] == "42"
    assert "DB Monitor" in sent["text"]
    # No project_notifications row written — Test is non-destructive for
    # saved settings. Delivery is still audited in notifications history.
    from app.metrics_storage import (
        get_notifications,
        get_project_by_slug,
        get_project_notifications,
        get_user_by_email,
    )
    user = get_user_by_email("test@example.com")
    project = get_project_by_slug(user["id"], "default")
    assert get_project_notifications(project["id"]) is None
    rows = get_notifications(project_id=project["id"])
    assert len(rows) == 1
    assert rows[0]["event_type"] == "test"
    assert rows[0]["status"] == "sent"
    assert rows[0]["chat_id"] == "42"


def test_test_button_audits_failed_send(client, monkeypatch):
    _register(client, "test-failed@example.com")

    def fake_send(text, *, bot_token, chat_id):
        return False, "telegram_error: bad"

    monkeypatch.setattr("app.notifications.telegram.send_message", fake_send)

    resp = client.post(
        "/projects/default/settings/notifications/test",
        data={
            "telegram_bot_token": _FAKE_TG_TOKEN,
            "telegram_chat_id": "42",
            "throttle_minutes": "30",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302

    from app.metrics_storage import (
        get_notifications,
        get_project_by_slug,
        get_user_by_email,
    )
    user = get_user_by_email("test-failed@example.com")
    project = get_project_by_slug(user["id"], "default")
    rows = get_notifications(project_id=project["id"])
    assert len(rows) == 1
    assert rows[0]["event_type"] == "test"
    assert rows[0]["status"] == "failed"
    assert rows[0]["error"] == "telegram_error: bad"
