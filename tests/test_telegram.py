"""Tests for app/notifications/telegram.py (#38)."""
from datetime import datetime, timedelta, timezone
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
    return (datetime.now(timezone.utc) - timedelta(hours=hours_ago)).isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# send_message
# ---------------------------------------------------------------------------

def test_send_message_no_token_returns_false(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")
    assert send_message("hello") is False


def test_send_message_no_chat_id_returns_false(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "")
    assert send_message("hello") is False


def test_send_message_success(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")
    with patch("app.notifications.telegram.asyncio.run",
               side_effect=lambda coro: coro.close()) as mock_run:
        result = send_message("test")
    assert result is True
    mock_run.assert_called_once()


def test_send_message_telegram_error_returns_false(monkeypatch):
    from telegram.error import TelegramError
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")

    def _raise(coro):
        coro.close()
        raise TelegramError("bad")

    with patch("app.notifications.telegram.asyncio.run", side_effect=_raise):
        result = send_message("test")
    assert result is False


def test_send_message_network_error_returns_false(monkeypatch):
    monkeypatch.setattr("app.config.settings.TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setattr("app.config.settings.TELEGRAM_CHAT_ID", "123")

    def _raise(coro):
        coro.close()
        raise OSError("conn")

    with patch("app.notifications.telegram.asyncio.run", side_effect=_raise):
        result = send_message("test")
    assert result is False


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
    from app.metrics_storage import is_throttled
    from sqlalchemy import text

    old_ts = (datetime.now(timezone.utc) - timedelta(minutes=31)).isoformat(timespec="seconds")
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
    with patch("app.notifications.telegram.send_message", return_value=True) as mock_send:
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
    with patch("app.notifications.telegram.send_message", return_value=True) as mock_send:
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
    with patch("app.notifications.telegram.send_message", return_value=True) as mock_send:
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
    with patch("app.notifications.telegram.send_message", return_value=True) as mock_send:
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
    with patch("app.notifications.telegram.send_message", return_value=True) as mock_send:
        notify_changepoint("orders", "row_count", 1000.0, 2500.0, _ts())
    mock_send.assert_called_once()
    text = mock_send.call_args[0][0]
    assert "orders" in text
    assert "row_count" in text


def test_notify_changepoint_null_rate_format(storage, monkeypatch):
    captured = []
    with patch("app.notifications.telegram.send_message",
               side_effect=lambda t: captured.append(t) or True):
        notify_changepoint("orders", "null_rate", 0.02, 0.18, _ts())

    assert captured, "expected a message"
    assert "2.0%" in captured[0]
    assert "18.0%" in captured[0]


# ---------------------------------------------------------------------------
# Flood test: 10 anomaly calls → only 1 send
# ---------------------------------------------------------------------------

def test_flood_throttle_10_anomalies(storage, monkeypatch):
    monkeypatch.setattr(
        "app.notifications.telegram.explain_anomaly",
        lambda t, m, ts: {"explanation": "x", "suggested_fix": "", "confidence": 0.3},
    )
    with patch("app.notifications.telegram.send_message", return_value=True) as mock_send:
        for _ in range(10):
            notify_anomaly("orders", _ts(), -0.2)
    assert mock_send.call_count == 1, f"expected 1 send, got {mock_send.call_count}"
