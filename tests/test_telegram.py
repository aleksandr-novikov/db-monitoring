"""Tests for app/notifications/telegram.py (#38 → #143 multi-tenant).

After #143 notify_* functions take ``project_id`` + explicit ``bot_token`` /
``chat_id`` / ``throttle_minutes``. There is no global env fallback —
unconfigured tenants get silent skips.
"""
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from app.notifications.telegram import (
    notify_anomaly,
    notify_changepoint,
    notify_schema_drift,
    send_message,
)

_PID = "tproj"  # short test-project id used across cases


@pytest.fixture
def storage(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.config import settings
    db_path = tmp_path / "metrics.db"
    monkeypatch.setattr(settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    yield ms
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)


def _ts(hours_ago: int = 0) -> str:
    return (datetime.now(UTC) - timedelta(hours=hours_ago)).isoformat(timespec="seconds")


# ── send_message — explicit-config API ─────────────────────────────────────

def test_send_message_no_token_returns_not_configured():
    ok, error = send_message("hello", bot_token=None, chat_id="123")
    assert ok is False
    assert error == "not_configured"


def test_send_message_no_chat_returns_not_configured():
    ok, error = send_message("hello", bot_token="tok", chat_id=None)
    assert ok is False
    assert error == "not_configured"


def test_send_message_success():
    with patch("app.notifications.telegram.asyncio.run",
               side_effect=lambda coro: coro.close()) as mock_run:
        ok, error = send_message("test", bot_token="tok", chat_id="123")
    assert ok is True
    assert error is None
    mock_run.assert_called_once()


def test_send_message_telegram_error_returns_false():
    from telegram.error import TelegramError

    def _raise(coro):
        coro.close()
        raise TelegramError("bad")

    with patch("app.notifications.telegram.asyncio.run", side_effect=_raise):
        ok, error = send_message("test", bot_token="tok", chat_id="123")
    assert ok is False
    assert error and "telegram_error" in error


def test_send_message_network_error_returns_false():
    def _raise(coro):
        coro.close()
        raise OSError("conn")

    with patch("app.notifications.telegram.asyncio.run", side_effect=_raise):
        ok, error = send_message("test", bot_token="tok", chat_id="123")
    assert ok is False
    assert error and "error" in error


# ── is_throttled / update_throttle — now per-project ───────────────────────

def test_is_throttled_no_entry(storage):
    from app.metrics_storage import is_throttled
    assert is_throttled(_PID, "orders", "anomaly") is False


def test_is_throttled_recent_entry(storage):
    from app.metrics_storage import is_throttled, update_throttle
    update_throttle(_PID, "orders", "anomaly")
    assert is_throttled(_PID, "orders", "anomaly") is True


def test_is_throttled_isolated_across_projects(storage):
    """#143: project_id is part of the throttle PK — one tenant must not
    suppress another's notifications."""
    from app.metrics_storage import is_throttled, update_throttle
    update_throttle("proj-A", "orders", "anomaly")
    # proj-B has never sent — must NOT be throttled.
    assert is_throttled("proj-B", "orders", "anomaly") is False
    assert is_throttled("proj-A", "orders", "anomaly") is True


def test_is_throttled_respects_per_project_window(storage):
    """``throttle_minutes`` arg overrides the global TELEGRAM_THROTTLE_MINUTES.
    A row that's 10 min old is throttled with window=15 but not with window=5."""
    from sqlalchemy import text

    from app.metrics_storage import is_throttled

    old_ts = (datetime.now(UTC) - timedelta(minutes=10)).isoformat(timespec="seconds")
    with storage.get_engine().begin() as conn:
        conn.execute(text(
            "INSERT OR REPLACE INTO telegram_throttle "
            "(project_id, table_name, event_key, last_sent_at) "
            "VALUES (:pid, 'orders', 'anomaly', :ts)"
        ), {"pid": _PID, "ts": old_ts})

    assert is_throttled(_PID, "orders", "anomaly", throttle_minutes=15) is True
    assert is_throttled(_PID, "orders", "anomaly", throttle_minutes=5) is False


def test_throttle_is_per_table_and_key(storage):
    from app.metrics_storage import is_throttled, update_throttle
    update_throttle(_PID, "orders", "anomaly")
    assert is_throttled(_PID, "users", "anomaly") is False
    assert is_throttled(_PID, "orders", "schema_drift") is False


# ── notify_anomaly ─────────────────────────────────────────────────────────

def _explain_stub(monkeypatch, *, confidence=0.85, explanation="ETL сбой"):
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": explanation, "suggested_fix": "", "confidence": confidence},
    )


def test_notify_anomaly_sends_message(storage, monkeypatch):
    _explain_stub(monkeypatch)
    with patch("app.notifications.telegram.send_message",
               return_value=(True, None)) as mock_send:
        notify_anomaly(_PID, "orders", _ts(), -0.14,
                       bot_token="tok", chat_id="42")
    mock_send.assert_called_once()
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "row_count" in text
    assert "UTC" in text
    assert "ETL сбой" in text
    # bot_token/chat_id flow through to send_message.
    assert mock_send.call_args.kwargs["bot_token"] == "tok"
    assert mock_send.call_args.kwargs["chat_id"] == "42"


def test_notify_anomaly_no_config_records_failure_no_send(storage, monkeypatch):
    """#143: if bot_token/chat_id is None, send_message is still called
    (it shorts to 'not_configured'), and the failure is audited. No global
    fallback — no notification leaves the system."""
    from app.metrics_storage import get_notifications
    _explain_stub(monkeypatch, confidence=0.3)
    notify_anomaly(_PID, "orders", _ts(), -0.2,
                   bot_token=None, chat_id=None)
    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["error"] == "not_configured"


def test_notify_anomaly_fallback_message(storage, monkeypatch):
    _explain_stub(monkeypatch, confidence=0.3,
                  explanation="Требуется ручная проверка.")
    with patch("app.notifications.telegram.send_message",
               return_value=(True, None)) as mock_send:
        notify_anomaly(_PID, "orders", _ts(), -0.14,
                       bot_token="tok", chat_id="42")
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "Требуется ручная проверка данных." in text


def test_notify_anomaly_passes_raw_ts_and_metric_to_explain(storage, monkeypatch):
    received = {}

    def capture(t, m, ts):
        received["ts"] = ts
        received["metric"] = m
        return {"explanation": "ok", "suggested_fix": "", "confidence": 0.85}

    monkeypatch.setattr("app.notifications.telegram.explain_anomaly", capture)
    raw_ts = "2026-05-12T02:36:00+00:00"
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_anomaly(_PID, "orders", raw_ts, -0.14,
                       bot_token="tok", chat_id="42", metric="null_rate")
    assert received["ts"] == raw_ts
    assert received["metric"] == "null_rate"


def test_notify_anomaly_message_contains_score(storage, monkeypatch):
    _explain_stub(monkeypatch, confidence=0.3)
    with patch("app.notifications.telegram.send_message",
               return_value=(True, None)) as mock_send:
        notify_anomaly(_PID, "orders", _ts(), -0.25,
                       bot_token="tok", chat_id="42")
    assert "-0.2500" in mock_send.call_args[0][0]


def test_notify_anomaly_throttled_skips_send(storage, monkeypatch):
    from app.metrics_storage import update_throttle
    update_throttle(_PID, "orders", "anomaly")
    explain_mock = MagicMock()
    monkeypatch.setattr("app.notifications.telegram.explain_anomaly", explain_mock)
    with patch("app.notifications.telegram.send_message") as mock_send:
        notify_anomaly(_PID, "orders", _ts(), -0.14,
                       bot_token="tok", chat_id="42")
    mock_send.assert_not_called()
    explain_mock.assert_not_called()


# ── notify_schema_drift ────────────────────────────────────────────────────

def test_notify_schema_drift_sends_message(storage):
    events = [{"change_type": "column_added", "column_name": "email",
               "details": {"after": {"type": "text"}}}]
    with patch("app.notifications.telegram.send_message",
               return_value=(True, None)) as mock_send:
        notify_schema_drift(_PID, "orders", events,
                            bot_token="tok", chat_id="42")
    mock_send.assert_called_once()
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "column_added" in text
    assert "email" in text


def test_notify_schema_drift_multiple_events_single_message(storage):
    events = [
        {"change_type": "column_added", "column_name": "email",
         "details": {"after": {"type": "text"}}},
        {"change_type": "column_removed", "column_name": "age",
         "details": {"before": {"type": "integer"}}},
    ]
    with patch("app.notifications.telegram.send_message",
               return_value=(True, None)) as mock_send:
        notify_schema_drift(_PID, "orders", events,
                            bot_token="tok", chat_id="42")
    assert mock_send.call_count == 1
    text = mock_send.call_args[0][0]
    assert "email" in text
    assert "age" in text


def test_notify_schema_drift_empty_events_skips(storage):
    with patch("app.notifications.telegram.asyncio.run") as mock_run:
        notify_schema_drift(_PID, "orders", [],
                            bot_token="tok", chat_id="42")
    mock_run.assert_not_called()


# ── notify_changepoint ─────────────────────────────────────────────────────

def test_notify_changepoint_sends_message(storage):
    with patch("app.notifications.telegram.send_message",
               return_value=(True, None)) as mock_send:
        notify_changepoint(_PID, "orders", "row_count", 1000.0, 2500.0, _ts(),
                           bot_token="tok", chat_id="42")
    mock_send.assert_called_once()
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "row_count" in text


def test_notify_changepoint_null_rate_format(storage):
    captured = []
    with patch(
        "app.notifications.telegram.send_message",
        side_effect=lambda t, **kw: (captured.append(t) or True, None),
    ):
        notify_changepoint(_PID, "orders", "null_rate", 0.02, 0.18, _ts(),
                           bot_token="tok", chat_id="42")

    assert captured, "expected a message"
    assert "2.0%" in captured[0]
    assert "18.0%" in captured[0]


# ── Notification persistence (#76) ─────────────────────────────────────────

def test_notify_anomaly_persists_sent_record(storage, monkeypatch):
    from app.metrics_storage import get_notifications
    _explain_stub(monkeypatch, confidence=0.3)
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_anomaly(_PID, "orders", _ts(), -0.14,
                       bot_token="tok", chat_id="42")

    rows = get_notifications()
    assert len(rows) == 1
    rec = rows[0]
    assert rec["event_type"] == "anomaly"
    assert rec["table_name"] == "orders"
    assert rec["metric_name"] == "row_count"
    assert rec["status"] == "sent"
    assert rec["error"] is None
    assert rec["project_id"] == _PID  # #143: tenant tag on the audit row.


def test_notify_anomaly_persists_failed_record_on_send_error(storage, monkeypatch):
    from app.metrics_storage import get_notifications
    _explain_stub(monkeypatch, confidence=0.3)
    with patch("app.notifications.telegram.send_message",
               return_value=(False, "telegram_error: bad")):
        notify_anomaly(_PID, "orders", _ts(), -0.2,
                       bot_token="tok", chat_id="42")

    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert "telegram_error" in (rows[0]["error"] or "")


def test_notify_anomaly_failed_send_does_not_throttle(storage, monkeypatch):
    from app.metrics_storage import is_throttled
    _explain_stub(monkeypatch, confidence=0.3)
    with patch("app.notifications.telegram.send_message", return_value=(False, "oops")):
        notify_anomaly(_PID, "orders", _ts(), -0.2,
                       bot_token="tok", chat_id="42")
    assert is_throttled(_PID, "orders", "anomaly") is False


def test_notify_schema_drift_persists_record(storage):
    from app.metrics_storage import get_notifications
    events = [{"change_type": "column_added", "column_name": "email",
               "details": {"after": {"type": "text"}}}]
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_schema_drift(_PID, "orders", events,
                            bot_token="tok", chat_id="42")
    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["event_type"] == "schema_drift"
    assert rows[0]["table_name"] == "orders"
    assert rows[0]["status"] == "sent"


def test_notify_changepoint_persists_record(storage):
    from app.metrics_storage import get_notifications
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_changepoint(_PID, "orders", "row_count", 1000.0, 2500.0, _ts(),
                           bot_token="tok", chat_id="42")
    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["event_type"] == "changepoint"
    assert rows[0]["metric_name"] == "row_count"


def test_save_notification_omits_secrets(storage, monkeypatch):
    """chat_id is recorded but bot token is never persisted (acceptance)."""
    from app.metrics_storage import get_notifications
    _explain_stub(monkeypatch, confidence=0.3)
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_anomaly(_PID, "orders", _ts(), -0.2,
                       bot_token="super-secret-token", chat_id="42")

    rows = get_notifications()
    assert rows[0]["chat_id"] == "42"
    blob = " ".join(str(v) for v in rows[0].values() if v is not None)
    assert "super-secret-token" not in blob


def test_flood_throttle_10_anomalies(storage, monkeypatch):
    _explain_stub(monkeypatch, confidence=0.3)
    with patch("app.notifications.telegram.send_message",
               return_value=(True, None)) as mock_send:
        for _ in range(10):
            notify_anomaly(_PID, "orders", _ts(), -0.2,
                           bot_token="tok", chat_id="42")
    assert mock_send.call_count == 1


# ── get_notifications filters ──────────────────────────────────────────────

def test_get_notifications_filters_by_event_type(storage):
    from app.metrics_storage import get_notifications, save_notification
    save_notification(event_type="anomaly", message="m1", status="sent", table_name="orders")
    save_notification(event_type="schema_drift", message="m2", status="sent", table_name="orders")
    save_notification(event_type="anomaly", message="m3", status="failed",
                      table_name="users", error="e")

    only_anomaly = get_notifications(event_type="anomaly")
    assert len(only_anomaly) == 2
    assert {r["table_name"] for r in only_anomaly} == {"orders", "users"}

    only_failed = get_notifications(status="failed")
    assert len(only_failed) == 1
    assert only_failed[0]["table_name"] == "users"

    by_table = get_notifications(table_name="orders")
    assert len(by_table) == 2


def test_get_notifications_pagination(storage):
    from app.metrics_storage import count_notifications, get_notifications, save_notification
    for i in range(7):
        save_notification(event_type="anomaly", message=f"m{i}", status="sent", table_name="t")
    assert count_notifications() == 7
    page1 = get_notifications(limit=3, offset=0)
    page2 = get_notifications(limit=3, offset=3)
    assert len(page1) == 3
    assert len(page2) == 3
    ids1 = {r["id"] for r in page1}
    ids2 = {r["id"] for r in page2}
    assert ids1.isdisjoint(ids2)
