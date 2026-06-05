from datetime import timedelta
from unittest.mock import patch

import pytest
from sqlalchemy.exc import OperationalError

from collectors.metrics_collector import MetricsCollector, _to_epoch

# ---------------------------------------------------------------------------
# Shared test data
# Uses the master-branch column_nulls() shape (includes data_type)
# ---------------------------------------------------------------------------

FAKE_STATS = {
    "table_name": "users",
    "schema": "public",
    "row_count": 1500,
    "size_bytes": 65536,
    "last_analyze": "2026-04-24 03:00:00",
}

FAKE_STATS_NO_ANALYZE = {**FAKE_STATS, "last_analyze": None}

FAKE_COLS = [
    {"column": "email", "data_type": "text", "null_count": 75, "null_rate": 0.05},
    {"column": "age", "data_type": "integer", "null_count": 0, "null_rate": 0.0},
]

FAKE_DISTS = [
    {
        "column": "country",
        "data_type": "text",
        "total": 1500,
        "buckets": [{"value": "RU", "count": 900}, {"value": "US", "count": 600}],
    },
]


@pytest.fixture
def collector():
    return MetricsCollector(schema="public")


# ---------------------------------------------------------------------------
# Unit tests — MetricsCollector.collect()
# ---------------------------------------------------------------------------

def test_collect_emits_all_expected_metric_names(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS), \
         patch("collectors.metrics_collector.db.column_distribution", return_value=FAKE_DISTS):
        rows = collector.collect("users")

    names = {r["metric_name"] for r in rows}
    assert names == {
        "row_count", "size_bytes", "last_modified",
        "null_count", "null_rate", "column_distribution",
    }


def test_collect_row_count_and_size_bytes_values(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users")

    untagged = {r["metric_name"]: r for r in rows if "tags" not in r}
    assert untagged["row_count"]["value"] == 1500
    assert untagged["size_bytes"]["value"] == 65536


def test_collect_null_count_emitted_per_column_with_tags(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users")

    null_counts = [r for r in rows if r["metric_name"] == "null_count"]
    assert len(null_counts) == 2
    assert {r["tags"]["column"] for r in null_counts} == {"email", "age"}
    email_row = next(r for r in null_counts if r["tags"]["column"] == "email")
    assert email_row["value"] == 75


def test_collect_null_rate_is_aggregate_without_tags(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users")

    null_rate_rows = [r for r in rows if r["metric_name"] == "null_rate"]
    assert len(null_rate_rows) == 1
    assert "tags" not in null_rate_rows[0]
    # avg(0.05, 0.0) = 0.025
    assert null_rate_rows[0]["value"] == pytest.approx(0.025, abs=1e-4)


def test_collect_last_modified_is_epoch_float(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users")

    lm = next(r for r in rows if r["metric_name"] == "last_modified")
    assert isinstance(lm["value"], float)
    assert lm["value"] > 0


def test_collect_skips_last_modified_when_analyze_is_none(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS_NO_ANALYZE), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users")

    assert not any(r["metric_name"] == "last_modified" for r in rows)


def test_collect_table_not_found_returns_empty_list(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=None):
        rows = collector.collect("nonexistent")

    assert rows == []


def test_collect_connection_error_on_stats_returns_empty_list(collector):
    with patch("collectors.metrics_collector.db.table_stats",
               side_effect=OperationalError("conn", {}, None)):
        rows = collector.collect("users")

    assert rows == []


def test_collect_no_columns_omits_null_metrics(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=[]):
        rows = collector.collect("users")

    names = {r["metric_name"] for r in rows}
    assert "null_count" not in names
    assert "null_rate" not in names
    assert "row_count" in names
    assert "size_bytes" in names


def test_collect_column_nulls_error_returns_partial_rows(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls",
               side_effect=OperationalError("conn", {}, None)):
        rows = collector.collect("users")

    names = {r["metric_name"] for r in rows}
    assert "row_count" in names
    assert "size_bytes" in names
    assert "null_count" not in names
    assert "null_rate" not in names


def test_collect_all_rows_share_same_timestamp(collector):
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users")

    timestamps = {r["ts"] for r in rows}
    assert len(timestamps) == 1


def test_collect_uses_provided_ts(collector):
    from datetime import UTC, datetime
    fixed_ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users", ts=fixed_ts)

    assert all(r["ts"] == fixed_ts for r in rows)


def test_collect_all_tables_uses_single_run_ts(storage):
    # All tables collected in one scheduler run must share the same ts so
    # _history_aggregate groups them into a single run (100% coverage).
    from unittest.mock import MagicMock, patch

    import collectors.scheduler as sched_mod

    collected_timestamps: list = []

    def fake_collect(table_name, ts=None):
        collected_timestamps.append(ts)
        return [{"ts": ts, "table_name": table_name,
                 "metric_name": "row_count", "value": 1}]

    fake_collector = MagicMock()
    fake_collector.collect.side_effect = fake_collect

    tables = [{"table_name": "orders"}, {"table_name": "users"}, {"table_name": "events"}]

    with patch("app.db.list_tables", return_value=tables), \
         patch("collectors.metrics_collector.MetricsCollector", return_value=fake_collector), \
         patch("app.metrics_storage.save_metrics", return_value=1), \
         patch("collectors.schema_collector.collect_all_schemas", return_value={"events": 0}), \
         patch("ml.drift.compute_and_store_drift_all", return_value={}), \
         patch("collectors.scheduler._score_recent_anomalies"), \
         patch("collectors.scheduler._notify_schema_drift_events"):
        sched_mod.collect_all_tables()

    assert len(collected_timestamps) == 3
    assert len(set(collected_timestamps)) == 1, (
        "all tables must share the same run_ts — got multiple timestamps: "
        f"{collected_timestamps}"
    )


# ---------------------------------------------------------------------------
# Unit tests — _to_epoch()
# ---------------------------------------------------------------------------

def test_to_epoch_converts_datetime_string_to_float():
    result = _to_epoch("2026-04-24 03:00:00")
    assert isinstance(result, float)
    assert result > 0


def test_to_epoch_returns_none_for_none():
    assert _to_epoch(None) is None


def test_to_epoch_returns_none_for_invalid_string():
    assert _to_epoch("not-a-date") is None


def test_to_epoch_handles_tz_aware_string():
    result = _to_epoch("2026-04-24T03:00:00+00:00")
    assert isinstance(result, float)


# ---------------------------------------------------------------------------
# Integration test — collect → save_metrics → get_metrics round-trip
# ---------------------------------------------------------------------------

@pytest.fixture
def storage(tmp_path, monkeypatch):
    db_path = tmp_path / "metrics.db"
    import app.metrics_storage as storage_mod
    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


def test_integration_collect_and_save_round_trip(storage):
    collector = MetricsCollector(schema="public")

    with patch("collectors.metrics_collector.db.table_stats", return_value=FAKE_STATS), \
         patch("collectors.metrics_collector.db.column_nulls", return_value=FAKE_COLS):
        rows = collector.collect("users")

    pid = "test-project"
    assert storage.save_metrics(rows, pid) == len(rows)

    rc = storage.get_metrics("users", "row_count", pid, window=timedelta(minutes=5))
    assert len(rc) == 1
    assert rc[0]["value"] == 1500.0

    nr = storage.get_metrics("users", "null_rate", pid, window=timedelta(minutes=5))
    assert len(nr) == 1
    assert nr[0]["tags"] is None
    assert nr[0]["value"] == pytest.approx(0.025, abs=1e-4)

    nc = storage.get_metrics("users", "null_count", pid, window=timedelta(minutes=5))
    assert len(nc) == 2
    assert {r["tags"]["column"] for r in nc} == {"email", "age"}


# ---------------------------------------------------------------------------
# Scheduler — double-start guard
# ---------------------------------------------------------------------------

def test_scheduler_does_not_start_twice(monkeypatch):
    from unittest.mock import MagicMock

    import collectors.scheduler as sched_mod

    fake_scheduler = MagicMock()
    fake_scheduler.running = False

    monkeypatch.setattr(sched_mod, "_scheduler", None)
    monkeypatch.setattr("collectors.scheduler.BackgroundScheduler", lambda: fake_scheduler)

    fake_app = MagicMock()
    fake_app.config.get.return_value = 15

    sched_mod.start_scheduler(fake_app)
    assert fake_scheduler.start.call_count == 1

    # Simulate second call — scheduler is now marked as running
    fake_scheduler.running = True
    monkeypatch.setattr(sched_mod, "_scheduler", fake_scheduler)
    sched_mod.start_scheduler(fake_app)

    assert fake_scheduler.start.call_count == 1  # still 1, not 2
