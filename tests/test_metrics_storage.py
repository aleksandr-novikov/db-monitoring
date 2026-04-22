from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Fresh storage backed by a temporary SQLite file per test."""
    db_path = tmp_path / "metrics.db"

    import app.metrics_storage as storage_mod
    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


def test_save_and_get_metrics(storage):
    now = datetime.now(timezone.utc)
    rows = [
        {"ts": now - timedelta(hours=2), "table_name": "users", "metric_name": "row_count", "value": 100},
        {"ts": now - timedelta(hours=1), "table_name": "users", "metric_name": "row_count", "value": 110},
        {"ts": now, "table_name": "users", "metric_name": "row_count", "value": 120},
    ]
    assert storage.save_metrics(rows) == 3

    result = storage.get_metrics("users", "row_count", window=timedelta(days=1))

    assert len(result) == 3
    assert [r["value"] for r in result] == [100.0, 110.0, 120.0]


def test_get_metrics_respects_window(storage):
    now = datetime.now(timezone.utc)
    storage.save_metrics([
        {"ts": now - timedelta(days=10), "table_name": "orders", "metric_name": "null_rate", "value": 0.05},
        {"ts": now - timedelta(days=2), "table_name": "orders", "metric_name": "null_rate", "value": 0.06},
        {"ts": now, "table_name": "orders", "metric_name": "null_rate", "value": 0.07},
    ])

    result = storage.get_metrics("orders", "null_rate", window=timedelta(days=7))

    assert [r["value"] for r in result] == [0.06, 0.07]


def test_get_metrics_filters_by_table_and_metric(storage):
    now = datetime.now(timezone.utc)
    storage.save_metrics([
        {"ts": now, "table_name": "users", "metric_name": "row_count", "value": 100},
        {"ts": now, "table_name": "orders", "metric_name": "row_count", "value": 500},
        {"ts": now, "table_name": "users", "metric_name": "null_rate", "value": 0.03},
    ])

    users_rows = storage.get_metrics("users", "row_count", window=timedelta(hours=1))

    assert len(users_rows) == 1
    assert users_rows[0]["value"] == 100.0


def test_get_metrics_empty_when_no_data(storage):
    assert storage.get_metrics("ghost", "row_count", window=timedelta(days=1)) == []


def test_tags_roundtrip(storage):
    now = datetime.now(timezone.utc)
    storage.save_metrics([
        {
            "ts": now,
            "table_name": "users",
            "metric_name": "null_rate",
            "value": 0.04,
            "tags": {"column": "email"},
        }
    ])

    result = storage.get_metrics("users", "null_rate", window=timedelta(hours=1))

    assert result[0]["tags"] == {"column": "email"}


def test_purge_old_deletes_beyond_retention(storage):
    now = datetime.now(timezone.utc)
    storage.save_metrics([
        {"ts": now - timedelta(days=100), "table_name": "users", "metric_name": "row_count", "value": 50},
        {"ts": now - timedelta(days=120), "table_name": "users", "metric_name": "row_count", "value": 40},
        {"ts": now - timedelta(days=30), "table_name": "users", "metric_name": "row_count", "value": 80},
    ])

    deleted = storage.purge_old(retention_days=90)

    assert deleted == 2
    remaining = storage.get_metrics("users", "row_count", window=timedelta(days=365))
    assert [r["value"] for r in remaining] == [80.0]


def test_save_empty_batch_is_noop(storage):
    assert storage.save_metrics([]) == 0
