from datetime import UTC, datetime, timedelta

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


def _ts(offset_hours: int = 0) -> datetime:
    # Anchor on now() rather than a hardcoded date — the daily-aggregate
    # helpers filter by a 30-day window relative to now(), so a frozen base
    # falls off the window the moment the calendar passes that point + 30d.
    base = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    return base + timedelta(hours=offset_hours)


def _seed(storage, rows):
    storage.save_metrics([
        {"ts": r["ts"], "table_name": r["table"], "metric_name": r["metric"], "value": r["value"], "tags": r.get("tags")}
        for r in rows
    ])


# ---------------------------------------------------------------------------
# build_history_aggregate
# ---------------------------------------------------------------------------

def test_aggregate_empty_returns_empty_structure(storage):
    agg = storage.build_history_aggregate()
    assert agg["timestamps"] == []
    assert agg["total_tables"] == 0
    assert agg["rows"] == []


def test_aggregate_counts_known_tables(storage):
    _seed(storage, [
        {"ts": _ts(0), "table": "users",  "metric": "row_count", "value": 100},
        {"ts": _ts(0), "table": "orders", "metric": "row_count", "value": 200},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    assert agg["total_tables"] == 2


def test_aggregate_detects_null_spikes(storage):
    """null_rate jump of >=5 pp between two ticks must appear in null_spikes_by_ts."""
    t0, t1 = _ts(0), _ts(1)
    _seed(storage, [
        {"ts": t0, "table": "orders", "metric": "null_rate", "value": 0.02},
        {"ts": t1, "table": "orders", "metric": "null_rate", "value": 0.20},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    spike_ts = [ts for ts in agg["null_spikes_by_ts"] if agg["null_spikes_by_ts"][ts] > 0]
    assert len(spike_ts) == 1


def test_aggregate_no_spike_for_small_delta(storage):
    t0, t1 = _ts(0), _ts(1)
    _seed(storage, [
        {"ts": t0, "table": "orders", "metric": "null_rate", "value": 0.02},
        {"ts": t1, "table": "orders", "metric": "null_rate", "value": 0.03},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    assert sum(agg["null_spikes_by_ts"].values()) == 0


# ---------------------------------------------------------------------------
# get_history_runs
# ---------------------------------------------------------------------------

def test_get_history_runs_returns_correct_fields(storage):
    _seed(storage, [
        {"ts": _ts(0), "table": "users",  "metric": "row_count", "value": 100},
        {"ts": _ts(0), "table": "orders", "metric": "row_count", "value": 200},
        {"ts": _ts(0), "table": "orders", "metric": "null_rate",  "value": 0.02},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    runs = storage.get_history_runs(agg, limit=10)
    assert len(runs) == 1
    run = runs[0]
    assert "ts_label" in run
    assert "tables_checked" in run
    assert "problems" in run
    assert "null_spikes" in run
    assert "coverage_pct" in run
    assert run["tables_checked"] == 2
    assert run["coverage_pct"] == 100.0


def test_get_history_runs_limit(storage):
    for h in range(5):
        _seed(storage, [{"ts": _ts(h), "table": "users", "metric": "row_count", "value": 100 + h}])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    runs = storage.get_history_runs(agg, limit=3)
    assert len(runs) == 3


def test_get_history_runs_problems_counted(storage):
    _seed(storage, [
        {"ts": _ts(0), "table": "orders", "metric": "row_count", "value": 500},
        {"ts": _ts(0), "table": "orders", "metric": "null_rate",  "value": 0.15},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    runs = storage.get_history_runs(agg)
    assert runs[0]["problems"] == 1


# ---------------------------------------------------------------------------
# get_history_daily
# ---------------------------------------------------------------------------

def test_get_history_daily_returns_one_entry_per_day(storage):
    # Two ticks on the same day — only the latest should appear
    _seed(storage, [
        {"ts": _ts(0), "table": "users", "metric": "row_count", "value": 100},
        {"ts": _ts(1), "table": "users", "metric": "row_count", "value": 110},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    daily = storage.get_history_daily(agg, days=30)
    assert len(daily) == 1
    assert daily[0]["date"] == _ts(0).date().isoformat()


def test_get_history_daily_filters_by_days(storage):
    # Two entries on different days: one 20 days ago, one 3 days ago (relative to seed base)
    old_ts = datetime.now(UTC) - timedelta(days=20)
    new_ts = datetime.now(UTC) - timedelta(days=3)
    storage.save_metrics([
        {"ts": old_ts, "table_name": "users", "metric_name": "row_count", "value": 50, "tags": None},
        {"ts": new_ts, "table_name": "users", "metric_name": "row_count", "value": 60, "tags": None},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    daily_7 = storage.get_history_daily(agg, days=7)
    daily_30 = storage.get_history_daily(agg, days=30)
    assert len(daily_7) == 1
    assert len(daily_30) == 2


# ---------------------------------------------------------------------------
# get_history_insights
# ---------------------------------------------------------------------------

def test_get_history_insights_empty(storage):
    agg = storage.build_history_aggregate()
    insights = storage.get_history_insights(agg)
    assert len(insights) == 1
    assert "не собраны" in insights[0]


def test_get_history_insights_returns_coverage(storage):
    _seed(storage, [
        {"ts": _ts(0), "table": "users",  "metric": "row_count", "value": 100},
        {"ts": _ts(0), "table": "orders", "metric": "row_count", "value": 200},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    insights = storage.get_history_insights(agg)
    assert any("Покрытие" in i for i in insights)


def test_get_history_insights_flags_high_null_rate(storage):
    _seed(storage, [
        {"ts": _ts(0), "table": "orders", "metric": "row_count", "value": 500},
        {"ts": _ts(0), "table": "orders", "metric": "null_rate",  "value": 0.25},
    ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    insights = storage.get_history_insights(agg)
    assert any("требует проверки" in i for i in insights)


def test_get_history_insights_max_four(storage):
    for h in range(3):
        _seed(storage, [
            {"ts": _ts(h),     "table": "a", "metric": "row_count", "value": 100},
            {"ts": _ts(h),     "table": "a", "metric": "null_rate",  "value": 0.20 + h * 0.10},
            {"ts": _ts(h),     "table": "b", "metric": "null_rate",  "value": 0.01},
        ])
    agg = storage.build_history_aggregate(window=timedelta(days=30))
    insights = storage.get_history_insights(agg)
    assert len(insights) <= 4
