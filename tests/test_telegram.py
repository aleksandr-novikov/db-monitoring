"""Tests for app/notifications/telegram.py (#38)."""
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from app.notifications.telegram import (
    notify_anomaly,
    notify_changepoint,
    notify_schema_drift,
    send_message,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------

def test_send_message_no_token_returns_false(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")
    ok, error = send_message("hello")
    assert ok is False
    assert error == "not_configured"


def test_send_message_no_chat_id_returns_false(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "")
    ok, error = send_message("hello")
    assert ok is False
    assert error == "not_configured"


def test_send_message_success(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")
    with patch("app.notifications.telegram.asyncio.run",
               side_effect=lambda coro: coro.close()) as mock_run:
        ok, error = send_message("test")
    assert ok is True
    assert error is None
    mock_run.assert_called_once()


def test_send_message_telegram_error_returns_false(monkeypatch):
    from telegram.error import TelegramError
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")

    def _raise(coro):
        coro.close()
        raise TelegramError("bad")

    with patch("app.notifications.telegram.asyncio.run", side_effect=_raise):
        ok, error = send_message("test")
    assert ok is False
    assert error and "telegram_error" in error


def test_send_message_network_error_returns_false(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")

    def _raise(coro):
        coro.close()
        raise OSError("conn")

    with patch("app.notifications.telegram.asyncio.run", side_effect=_raise):
        ok, error = send_message("test")
    assert ok is False
    assert error and "error" in error


# ---------------------------------------------------------------------------
# is_throttled / update_throttle
# ---------------------------------------------------------------------------

def test_is_throttled_no_entry(storage):
    from app.metrics_storage import is_throttled
    assert is_throttled("orders", "anomaly") is False


def test_is_throttled_recent_entry(storage):
    from app.metrics_storage import is_throttled, update_throttle
    update_throttle("orders", "anomaly")
    assert is_throttled("orders", "anomaly") is True


def test_is_throttled_expired_entry(storage, monkeypatch):
    from sqlalchemy import text

    from app.metrics_storage import is_throttled

    old_ts = (datetime.now(UTC) - timedelta(minutes=31)).isoformat(timespec="seconds")
    with storage.get_engine().begin() as conn:
        conn.execute(text(
            "INSERT OR REPLACE INTO telegram_throttle (table_name, event_key, last_sent_at)"
            " VALUES ('orders', 'anomaly', :ts)"
        ), {"ts": old_ts})

    assert is_throttled("orders", "anomaly") is False


def test_throttle_is_per_table_and_key(storage):
    from app.metrics_storage import is_throttled, update_throttle
    update_throttle("orders", "anomaly")
    assert is_throttled("users", "anomaly") is False
    assert is_throttled("orders", "schema_drift") is False


# ---------------------------------------------------------------------------
# notify_anomaly
# ---------------------------------------------------------------------------

def test_notify_anomaly_sends_message(storage, monkeypatch):
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "ETL сбой", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message", return_value=(True, None)) as mock_send:
        notify_anomaly("orders", _ts(), -0.14)
    mock_send.assert_called_once()
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "ETL сбой" in text


def test_notify_anomaly_message_contains_score(storage, monkeypatch):
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "причина", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message", return_value=(True, None)) as mock_send:
        notify_anomaly("orders", _ts(), -0.25)
    text = mock_send.call_args[0][0]
    assert "-0.2500" in text


def test_notify_anomaly_throttled_skips_send(storage, monkeypatch):
    from app.metrics_storage import update_throttle
    update_throttle("orders", "anomaly")
    explain_mock = MagicMock()
    monkeypatch.setattr("app.notifications.telegram.explain_anomaly", explain_mock)
    with patch("app.notifications.telegram.send_message") as mock_send:
        notify_anomaly("orders", _ts(), -0.14)
    mock_send.assert_not_called()
    explain_mock.assert_not_called()


# ---------------------------------------------------------------------------
# notify_schema_drift
# ---------------------------------------------------------------------------

def test_notify_schema_drift_sends_message(storage, monkeypatch):
    events = [
        {"change_type": "column_added", "column_name": "email",
         "details": {"after": {"type": "text"}}},
    ]
    with patch("app.notifications.telegram.send_message", return_value=(True, None)) as mock_send:
        notify_schema_drift("orders", events)
    mock_send.assert_called_once()
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "column_added" in text
    assert "email" in text


def test_notify_schema_drift_multiple_events_single_message(storage, monkeypatch):
    events = [
        {"change_type": "column_added", "column_name": "email",
         "details": {"after": {"type": "text"}}},
        {"change_type": "column_removed", "column_name": "age",
         "details": {"before": {"type": "integer"}}},
    ]
    with patch("app.notifications.telegram.send_message", return_value=(True, None)) as mock_send:
        notify_schema_drift("orders", events)
    assert mock_send.call_count == 1
    text = mock_send.call_args[0][0]
    assert "email" in text
    assert "age" in text


def test_notify_schema_drift_empty_events_skips(storage, monkeypatch):
    with patch("app.notifications.telegram.asyncio.run") as mock_run:
        notify_schema_drift("orders", [])
    mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# notify_changepoint
# ---------------------------------------------------------------------------

def test_notify_changepoint_sends_message(storage, monkeypatch):
    with patch("app.notifications.telegram.send_message", return_value=(True, None)) as mock_send:
        notify_changepoint("orders", "row_count", 1000.0, 2500.0, _ts())
    mock_send.assert_called_once()
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "row_count" in text


def test_notify_changepoint_null_rate_format(storage, monkeypatch):
    captured = []
    with patch("app.notifications.telegram.send_message",
               side_effect=lambda t: (captured.append(t) or True, None)):
        notify_changepoint("orders", "null_rate", 0.02, 0.18, _ts())

    assert captured, "expected a message"
    assert "2.0%" in captured[0]
    assert "18.0%" in captured[0]


# ---------------------------------------------------------------------------
# Flood test: 10 anomaly calls → only 1 send
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Notification persistence (#76)
# ---------------------------------------------------------------------------

def test_notify_anomaly_persists_sent_record(storage, monkeypatch):
    from app.metrics_storage import get_notifications
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "ETL сбой", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_anomaly("orders", _ts(), -0.14)

    rows = get_notifications()
    assert len(rows) == 1
    rec = rows[0]
    assert rec["event_type"] == "anomaly"
    assert rec["table_name"] == "orders"
    assert rec["metric_name"] == "row_count"
    assert rec["status"] == "sent"
    assert rec["error"] is None
    assert "orders" in rec["message"]


def test_notify_anomaly_persists_failed_record_on_send_error(storage, monkeypatch):
    """Acceptance: failed sends still create a record with status='failed' + error."""
    from app.metrics_storage import get_notifications
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "x", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message",
               return_value=(False, "telegram_error: bad")):
        notify_anomaly("orders", _ts(), -0.2)

    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert "telegram_error" in (rows[0]["error"] or "")


def test_notify_anomaly_failed_send_does_not_throttle(storage, monkeypatch):
    """If send failed, throttle is NOT updated — next attempt should go through."""
    from app.metrics_storage import is_throttled
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "x", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message", return_value=(False, "oops")):
        notify_anomaly("orders", _ts(), -0.2)
    assert is_throttled("orders", "anomaly") is False


def test_notify_schema_drift_persists_record(storage, monkeypatch):
    from app.metrics_storage import get_notifications
    events = [{"change_type": "column_added", "column_name": "email",
               "details": {"after": {"type": "text"}}}]
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_schema_drift("orders", events)
    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["event_type"] == "schema_drift"
    assert rows[0]["table_name"] == "orders"
    assert rows[0]["status"] == "sent"


def test_notify_changepoint_persists_record(storage, monkeypatch):
    from app.metrics_storage import get_notifications
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_changepoint("orders", "row_count", 1000.0, 2500.0, _ts())
    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["event_type"] == "changepoint"
    assert rows[0]["metric_name"] == "row_count"


def test_get_notifications_filters_by_event_type(storage, monkeypatch):
    from app.metrics_storage import get_notifications, save_notification
    save_notification(event_type="anomaly", message="m1", status="sent", table_name="orders")
    save_notification(event_type="schema_drift", message="m2", status="sent", table_name="orders")
    save_notification(event_type="anomaly", message="m3", status="failed", table_name="users", error="e")

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
    # Newest-first ordering — offset slice should not overlap.
    ids1 = {r["id"] for r in page1}
    ids2 = {r["id"] for r in page2}
    assert ids1.isdisjoint(ids2)


def test_save_notification_omits_secrets(storage, monkeypatch):
    """chat_id is recorded but bot token is never persisted (acceptance)."""
    from app.metrics_storage import get_notifications
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "super-secret-token")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "42")
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "x", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message", return_value=(True, None)):
        notify_anomaly("orders", _ts(), -0.2)

    rows = get_notifications()
    assert rows[0]["chat_id"] == "42"
    blob = " ".join(str(v) for v in rows[0].values() if v is not None)
    assert "super-secret-token" not in blob


def test_flood_throttle_10_anomalies(storage, monkeypatch):
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "x", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message", return_value=(True, None)) as mock_send:
        for _ in range(10):
            notify_anomaly("orders", _ts(), -0.2)
    assert mock_send.call_count == 1, f"expected 1 send, got {mock_send.call_count}"


# ---------------------------------------------------------------------------
# End-to-end: send_message stub failure path is recorded
# ---------------------------------------------------------------------------

def test_notify_persists_failure_when_telegram_not_configured(storage, monkeypatch):
    """No token → send_message returns ('not_configured'); record still saved."""
    from app.metrics_storage import get_notifications
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "")
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "x", "suggested_fix": "", "confidence": 0.3},
    )
    notify_anomaly("orders", _ts(), -0.2)

    rows = get_notifications()
    assert len(rows) == 1
    assert rows[0]["status"] == "failed"
    assert rows[0]["error"] == "not_configured"
