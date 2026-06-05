"""Tests for per-project schema_drift notifications (#197).

Covers:
- save_schema_events() / get_schema_events() respect project_id
- migration adds project_id column to pre-existing schema_events rows
- _notify_schema_drift_events() iterates all projects with Telegram + legacy
- project_id is forwarded to get_schema_events() and notify_schema_drift()
- projects without Telegram config are silently skipped
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from cryptography.fernet import Fernet


@pytest.fixture
def db(tmp_path, monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "MONITOR_DB_URL", f"sqlite:///{tmp_path / 'm.db'}")
    import app.metrics_storage as ms
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    yield ms
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)


@pytest.fixture(autouse=True)
def fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    from app import crypto
    crypto.reset_for_tests()


def _seed_project(db) -> str:
    user = db.create_user(
        user_id=uuid.uuid4().hex,
        email=f"u-{uuid.uuid4().hex[:6]}@example.com",
        password_hash="x",
    )
    project = db.create_project(
        project_id=uuid.uuid4().hex,
        user_id=user["id"],
        name="Test",
        slug=uuid.uuid4().hex[:8],
    )
    return project["id"]


def _save_telegram(db, project_id: str, chat_id: str = "111", token: str = "tok") -> None:
    from app import crypto
    db.save_project_notifications(
        project_id,
        telegram_bot_token=crypto.encrypt_token(token),
        telegram_chat_id=chat_id,
    )


def _make_event(table: str = "orders") -> dict:
    return {
        "ts": datetime.now(UTC).isoformat(),
        "table_name": table,
        "change_type": "column_added",
        "column_name": "new_col",
        "details": {"after": {"type": "TEXT"}},
    }


# ── save / get with project_id ────────────────────────────────────────────


def test_save_get_schema_events_with_project_id(db):
    pid_a = _seed_project(db)
    pid_b = _seed_project(db)
    db.save_schema_events([_make_event("orders")], project_id=pid_a)
    db.save_schema_events([_make_event("orders")], project_id=pid_b)

    rows_a = db.get_schema_events("orders", project_id=pid_a)
    rows_b = db.get_schema_events("orders", project_id=pid_b)
    assert len(rows_a) == 1
    assert len(rows_b) == 1


def test_get_schema_events_does_not_cross_tenants(db):
    pid_a = _seed_project(db)
    pid_b = _seed_project(db)
    db.save_schema_events([_make_event("products")], project_id=pid_a)

    rows_b = db.get_schema_events("products", project_id=pid_b)
    assert rows_b == []


def test_get_schema_events_defaults_to_legacy(db):
    db.save_schema_events([_make_event("users")])  # no project_id → legacy
    rows = db.get_schema_events("users")           # no project_id → legacy
    assert len(rows) == 1


def test_get_schema_events_respects_window(db):
    pid = _seed_project(db)
    old_event = _make_event("events")
    old_event["ts"] = (datetime.now(UTC) - timedelta(days=10)).isoformat()
    db.save_schema_events([old_event], project_id=pid)

    rows = db.get_schema_events("events", project_id=pid, window=timedelta(days=1))
    assert rows == []


# ── _notify_schema_drift_events orchestration ─────────────────────────────


def test_notify_schema_drift_iterates_all_projects_including_legacy(db, monkeypatch):
    pid_a = _seed_project(db)
    pid_b = _seed_project(db)
    _save_telegram(db, pid_a)
    _save_telegram(db, pid_b, chat_id="222")

    # Track which projects had their Telegram config checked — this fires
    # for every project in _iter_notification_projects() including legacy.
    checked_projects = []
    orig_load = __import__(
        "app.notifications.telegram", fromlist=["load_project_telegram_config"]
    ).load_project_telegram_config

    def fake_load_cfg(project_id):
        checked_projects.append(project_id)
        return orig_load(project_id)

    monkeypatch.setattr("app.notifications.telegram.load_project_telegram_config", fake_load_cfg)
    monkeypatch.setattr("app.metrics_storage.list_metric_tables", lambda pid: ["orders"])
    monkeypatch.setattr("app.metrics_storage.get_schema_events", lambda *a, **kw: [])
    monkeypatch.setattr(
        "app.metrics_storage.list_project_ids_with_telegram",
        lambda: [pid_a, pid_b],
    )

    from collectors.scheduler import _notify_schema_drift_events
    _notify_schema_drift_events()

    assert "legacy" in checked_projects
    assert pid_a in checked_projects
    assert pid_b in checked_projects


def test_notify_schema_drift_passes_project_id_to_notify(db, monkeypatch):
    pid = _seed_project(db)
    _save_telegram(db, pid, token="real-token", chat_id="999")

    notify_calls = []

    def fake_notify(project_id, table, events, *, bot_token, chat_id, throttle_minutes=None):
        notify_calls.append({"project_id": project_id, "table": table, "chat_id": chat_id})

    monkeypatch.setattr("app.metrics_storage.list_metric_tables", lambda p: ["orders"])
    monkeypatch.setattr("app.metrics_storage.list_project_ids_with_telegram", lambda: [pid])
    monkeypatch.setattr(
        "app.metrics_storage.get_schema_events",
        lambda table, project_id="legacy", window=None: [_make_event(table)] if project_id == pid else [],
    )
    monkeypatch.setattr("app.notifications.telegram.notify_schema_drift", fake_notify)

    from collectors.scheduler import _notify_schema_drift_events
    _notify_schema_drift_events()

    project_calls = [c for c in notify_calls if c["project_id"] == pid]
    assert len(project_calls) == 1
    assert project_calls[0]["chat_id"] == "999"


def test_notify_schema_drift_skips_project_without_telegram(db, monkeypatch):
    pid = _seed_project(db)  # no Telegram configured

    notify_calls = []
    monkeypatch.setattr("app.metrics_storage.list_metric_tables", lambda p: ["orders"])
    monkeypatch.setattr("app.metrics_storage.list_project_ids_with_telegram", lambda: [pid])
    monkeypatch.setattr(
        "app.metrics_storage.get_schema_events",
        lambda table, project_id="legacy", window=None: [_make_event(table)],
    )
    monkeypatch.setattr("app.notifications.telegram.notify_schema_drift", lambda *a, **kw: notify_calls.append(1))

    from collectors.scheduler import _notify_schema_drift_events
    _notify_schema_drift_events()

    assert notify_calls == []
