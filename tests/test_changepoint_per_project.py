"""Tests for per-project changepoint notifications (#197).

Covers:
- list_project_ids_with_telegram() filters correctly
- list_metric_tables(project_id) returns only tables for that tenant
- detect_changepoints() iterates all projects with Telegram + legacy
- project_id is forwarded to detect_all() and notify_changepoint()
- projects without Telegram config are silently skipped
"""
from __future__ import annotations

import uuid

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
    """Создать user + project, вернуть project_id."""
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


# ── list_project_ids_with_telegram ────────────────────────────────────────


def test_list_project_ids_with_telegram_returns_configured(db):
    pid_a = _seed_project(db)
    pid_b = _seed_project(db)
    _save_telegram(db, pid_a)
    _save_telegram(db, pid_b, chat_id="222")

    result = db.list_project_ids_with_telegram()
    assert set(result) == {pid_a, pid_b}


def test_list_project_ids_with_telegram_excludes_empty_chat_id(db):
    pid = _seed_project(db)
    from app import crypto
    db.save_project_notifications(
        pid,
        telegram_bot_token=crypto.encrypt_token("tok"),
        telegram_chat_id="",
    )
    assert db.list_project_ids_with_telegram() == []


def test_list_project_ids_with_telegram_excludes_null_token(db):
    pid = _seed_project(db)
    db.save_project_notifications(pid, telegram_bot_token=None, telegram_chat_id=None)
    assert db.list_project_ids_with_telegram() == []


# ── list_metric_tables ────────────────────────────────────────────────────


def test_list_metric_tables_returns_only_own_tables(db):
    from datetime import UTC, datetime
    now = datetime.now(UTC)

    db.save_metrics([
        {"ts": now, "table_name": "users",  "metric_name": "row_count", "value": 1},
        {"ts": now, "table_name": "orders", "metric_name": "row_count", "value": 2},
    ], project_id="proj-a")
    db.save_metrics([
        {"ts": now, "table_name": "events", "metric_name": "row_count", "value": 3},
    ], project_id="proj-b")

    assert set(db.list_metric_tables("proj-a")) == {"users", "orders"}
    assert db.list_metric_tables("proj-b") == ["events"]
    assert db.list_metric_tables("proj-unknown") == []


# ── load_project_telegram_config ──────────────────────────────────────────


def test_load_project_telegram_config_returns_tuple(db):
    from app import crypto
    pid = _seed_project(db)
    db.save_project_notifications(
        pid,
        telegram_bot_token=crypto.encrypt_token("mytoken"),
        telegram_chat_id="999",
        throttle_minutes=15,
    )
    from app.notifications.telegram import load_project_telegram_config
    result = load_project_telegram_config(pid)
    assert result is not None
    bot_token, chat_id, throttle = result
    assert bot_token == "mytoken"
    assert chat_id == "999"
    assert throttle == 15


def test_load_project_telegram_config_returns_none_if_missing(db):
    from app.notifications.telegram import load_project_telegram_config
    assert load_project_telegram_config("nonexistent") is None


# ── detect_changepoints — оркестрация ────────────────────────────────────


@pytest.fixture
def stub_changepoint(monkeypatch):
    calls: list[dict] = []

    def _detect_all(project_id="legacy", tables=None):
        calls.append({"project_id": project_id, "tables": tables})
        if project_id != "legacy":
            return {
                "detected": 1, "tables": 1, "errors": 0,
                "events": [{
                    "table_name": "users",
                    "metric_name": "row_count",
                    "value_before": 1000.0,
                    "value_after": 2000.0,
                    "ts": "2026-06-04T10:00:00+00:00",
                }],
            }
        return {"detected": 0, "tables": 0, "errors": 0, "events": []}

    monkeypatch.setattr("ml.changepoint.detect_all", _detect_all)
    return calls


def test_detect_changepoints_iterates_all_projects_including_legacy(
    db, stub_changepoint, monkeypatch
):
    pid = _seed_project(db)
    _save_telegram(db, pid)

    monkeypatch.setattr(
        "app.notifications.telegram.load_project_telegram_config",
        lambda p: ("tok", "111", 30) if p == pid else None,
    )
    monkeypatch.setattr("app.notifications.telegram.notify_changepoint", lambda *a, **kw: None)

    from collectors.scheduler import detect_changepoints
    detect_changepoints()

    called_ids = [c["project_id"] for c in stub_changepoint]
    assert "legacy" in called_ids
    assert pid in called_ids


def test_detect_changepoints_passes_project_tables_to_detect_all(
    db, stub_changepoint, monkeypatch
):
    from datetime import UTC, datetime
    pid = _seed_project(db)
    _save_telegram(db, pid)

    db.save_metrics([
        {"ts": datetime.now(UTC), "table_name": "orders", "metric_name": "row_count", "value": 1},
    ], project_id=pid)

    monkeypatch.setattr(
        "app.notifications.telegram.load_project_telegram_config",
        lambda p: None,
    )

    from collectors.scheduler import detect_changepoints
    detect_changepoints()

    call = next(c for c in stub_changepoint if c["project_id"] == pid)
    assert call["tables"] == ["orders"]


def test_detect_changepoints_sends_notification_with_correct_project_id(
    db, stub_changepoint, monkeypatch
):
    pid = _seed_project(db)
    _save_telegram(db, pid)

    notify_calls: list[dict] = []

    def _notify(project_id, table, metric, before, after, ts, **kw):
        notify_calls.append({"project_id": project_id, "table": table})

    monkeypatch.setattr(
        "app.notifications.telegram.load_project_telegram_config",
        lambda p: ("tok", "111", 30) if p == pid else None,
    )
    monkeypatch.setattr("app.notifications.telegram.notify_changepoint", _notify)

    from collectors.scheduler import detect_changepoints
    detect_changepoints()

    assert any(c["project_id"] == pid and c["table"] == "users" for c in notify_calls)


def test_detect_changepoints_skips_project_without_telegram(
    db, stub_changepoint, monkeypatch
):
    notify_calls: list = []

    monkeypatch.setattr(
        "app.notifications.telegram.load_project_telegram_config",
        lambda p: None,
    )
    monkeypatch.setattr(
        "app.notifications.telegram.notify_changepoint",
        lambda *a, **kw: notify_calls.append(a),
    )
    monkeypatch.setattr(
        "app.metrics_storage.list_project_ids_with_telegram",
        lambda: [],
    )

    from collectors.scheduler import detect_changepoints
    detect_changepoints()

    assert notify_calls == []


# ── save_changepoints deduplication → no repeat notifications ─────────────


def test_save_changepoints_returns_only_new_events(db):
    """Second save of the same changepoint returns [] — no duplicate notifications."""
    from datetime import UTC, datetime

    from app.metrics_storage import save_changepoints

    event = {
        "ts": datetime(2026, 6, 5, 10, 0, 0, tzinfo=UTC).isoformat(),
        "table_name": "orders",
        "metric_name": "row_count",
        "score": 1.5,
        "value_before": 100.0,
        "value_after": 200.0,
    }
    pid = _seed_project(db)

    first = save_changepoints([event], project_id=pid)
    assert len(first) == 1, "first save should return the event"

    second = save_changepoints([event], project_id=pid)
    assert second == [], "duplicate save should return empty — no re-notification"


def test_detect_changepoints_no_notification_on_repeat_run(db, monkeypatch):
    """Running detect_changepoints twice for same ML output fires notify only once.

    Mocks detect_changepoints (ML layer) but lets detect_all + save_changepoints
    run for real so cross-run deduplication is exercised.
    """
    from datetime import UTC, datetime
    notify_calls: list = []
    pid = _seed_project(db)
    _save_telegram(db, pid)

    raw_event = {
        "ts": datetime(2026, 6, 5, 10, 0, 0, tzinfo=UTC),
        "table_name": "orders",
        "metric_name": "row_count",
        "score": 1.5,
        "value_before": 100.0,
        "value_after": 200.0,
    }

    # Mock at the ML level — detect_changepoints returns the same event both runs
    monkeypatch.setattr(
        "ml.changepoint.detect_changepoints",
        lambda table, metric, window_days=30, project_id="legacy": (
            [raw_event] if project_id == pid and table == "orders" and metric == "row_count"
            else []
        ),
    )
    monkeypatch.setattr(
        "app.metrics_storage.list_metric_tables",
        lambda p: ["orders"] if p == pid else [],
    )
    monkeypatch.setattr(
        "app.notifications.telegram.load_project_telegram_config",
        lambda p: ("tok", "111", 0) if p == pid else None,
    )
    monkeypatch.setattr(
        "app.notifications.telegram.notify_changepoint",
        lambda *a, **kw: notify_calls.append(1),
    )
    monkeypatch.setattr(
        "app.metrics_storage.list_project_ids_with_telegram",
        lambda: [pid],
    )
    # Prevent legacy path from connecting to the real target DB
    monkeypatch.setattr("app.db.list_tables", lambda: [])

    from collectors.scheduler import detect_changepoints
    detect_changepoints()
    first_count = len(notify_calls)
    assert first_count == 1, "first run should notify once"

    # Second run with identical ML output — save_changepoints deduplicates
    detect_changepoints()
    assert len(notify_calls) == first_count, (
        "second run with same changepoint must not send another notification"
    )
