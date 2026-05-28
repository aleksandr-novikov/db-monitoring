from datetime import UTC, datetime, timedelta

import pytest

# #53: every metrics row carries a project_id. Pinning a constant here keeps
# the test bodies focused on the assertions, not the tenant scaffolding.
PID = "test-project"


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
    now = datetime.now(UTC)
    rows = [
        {"ts": now - timedelta(hours=2), "table_name": "users", "metric_name": "row_count", "value": 100},
        {"ts": now - timedelta(hours=1), "table_name": "users", "metric_name": "row_count", "value": 110},
        {"ts": now, "table_name": "users", "metric_name": "row_count", "value": 120},
    ]
    assert storage.save_metrics(rows, PID) == 3

    result = storage.get_metrics("users", "row_count", PID, window=timedelta(days=1))

    assert len(result) == 3
    assert [r["value"] for r in result] == [100.0, 110.0, 120.0]


def test_get_metrics_respects_window(storage):
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now - timedelta(days=10), "table_name": "orders", "metric_name": "null_rate", "value": 0.05},
        {"ts": now - timedelta(days=2), "table_name": "orders", "metric_name": "null_rate", "value": 0.06},
        {"ts": now, "table_name": "orders", "metric_name": "null_rate", "value": 0.07},
    ], PID)

    result = storage.get_metrics("orders", "null_rate", PID, window=timedelta(days=7))

    assert [r["value"] for r in result] == [0.06, 0.07]


def test_get_metrics_filters_by_table_and_metric(storage):
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now, "table_name": "users", "metric_name": "row_count", "value": 100},
        {"ts": now, "table_name": "orders", "metric_name": "row_count", "value": 500},
        {"ts": now, "table_name": "users", "metric_name": "null_rate", "value": 0.03},
    ], PID)

    users_rows = storage.get_metrics("users", "row_count", PID, window=timedelta(hours=1))

    assert len(users_rows) == 1
    assert users_rows[0]["value"] == 100.0


def test_get_metrics_empty_when_no_data(storage):
    assert storage.get_metrics("ghost", "row_count", PID, window=timedelta(days=1)) == []


def test_tags_roundtrip(storage):
    now = datetime.now(UTC)
    storage.save_metrics([
        {
            "ts": now,
            "table_name": "users",
            "metric_name": "null_rate",
            "value": 0.04,
            "tags": {"column": "email"},
        }
    ], PID)

    result = storage.get_metrics("users", "null_rate", PID, window=timedelta(hours=1))

    assert result[0]["tags"] == {"column": "email"}


def test_purge_old_deletes_beyond_retention(storage):
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now - timedelta(days=100), "table_name": "users", "metric_name": "row_count", "value": 50},
        {"ts": now - timedelta(days=120), "table_name": "users", "metric_name": "row_count", "value": 40},
        {"ts": now - timedelta(days=30), "table_name": "users", "metric_name": "row_count", "value": 80},
    ], PID)

    deleted = storage.purge_old(retention_days=90)

    assert deleted == 2
    remaining = storage.get_metrics("users", "row_count", PID, window=timedelta(days=365))
    assert [r["value"] for r in remaining] == [80.0]


def test_save_empty_batch_is_noop(storage):
    assert storage.save_metrics([], PID) == 0


def test_get_latest_null_counts_returns_most_recent_run(storage):
    """Returns per-column null_count from the latest collector run only."""
    older = datetime.now(UTC) - timedelta(hours=1)
    newer = datetime.now(UTC)
    storage.save_metrics([
        # older run — should be ignored
        {"ts": older, "table_name": "users", "metric_name": "null_count", "value": 9, "tags": {"column": "email"}},
        {"ts": older, "table_name": "users", "metric_name": "null_count", "value": 1, "tags": {"column": "phone"}},
        # newer run — should be returned
        {"ts": newer, "table_name": "users", "metric_name": "null_count", "value": 50, "tags": {"column": "email"}},
        {"ts": newer, "table_name": "users", "metric_name": "null_count", "value": 0, "tags": {"column": "phone"}},
        # a different table — should be ignored
        {"ts": newer, "table_name": "orders", "metric_name": "null_count", "value": 7, "tags": {"column": "email"}},
        # a different metric — should be ignored
        {"ts": newer, "table_name": "users", "metric_name": "row_count", "value": 1500},
    ], PID)

    counts = storage.get_latest_null_counts("users", PID)

    assert counts == {"email": 50, "phone": 0}


def test_get_latest_null_counts_empty_when_no_data(storage):
    assert storage.get_latest_null_counts("users", PID) == {}


# --- drift_reports cache ---


def test_save_and_get_drift_report(storage):
    rows = [
        {"column": "country", "data_type": "varchar", "psi": 0.42,
         "ks_pvalue": None, "is_drift": True, "severity": "critical"},
        {"column": "status", "data_type": "varchar", "psi": 0.05,
         "ks_pvalue": None, "is_drift": False, "severity": "ok"},
    ]
    assert storage.save_drift_reports("orders", rows) == 2

    result = storage.get_drift_report("orders")

    # Сортировка по убыванию PSI.
    assert [r["column"] for r in result] == ["country", "status"]
    assert result[0]["psi"] == 0.42
    assert result[0]["is_drift"] is True
    assert result[0]["severity"] == "critical"
    assert result[1]["is_drift"] is False


def test_save_drift_reports_replaces_previous(storage):
    storage.save_drift_reports("orders", [
        {"column": "country", "data_type": "varchar", "psi": 0.42,
         "ks_pvalue": None, "is_drift": True, "severity": "critical"},
    ])
    storage.save_drift_reports("orders", [
        {"column": "status", "data_type": "varchar", "psi": 0.01,
         "ks_pvalue": None, "is_drift": False, "severity": "ok"},
    ])

    result = storage.get_drift_report("orders")
    assert [r["column"] for r in result] == ["status"]


def test_save_drift_reports_empty_clears_table(storage):
    storage.save_drift_reports("orders", [
        {"column": "country", "data_type": "varchar", "psi": 0.42,
         "ks_pvalue": None, "is_drift": True, "severity": "critical"},
    ])
    assert storage.save_drift_reports("orders", []) == 0
    assert storage.get_drift_report("orders") == []


def test_save_drift_reports_per_table_isolation(storage):
    storage.save_drift_reports("orders", [
        {"column": "country", "data_type": "varchar", "psi": 0.4,
         "ks_pvalue": None, "is_drift": True, "severity": "critical"},
    ])
    storage.save_drift_reports("users", [
        {"column": "email", "data_type": "varchar", "psi": 0.05,
         "ks_pvalue": None, "is_drift": False, "severity": "ok"},
    ])

    # Перезапись orders не должна затронуть users.
    storage.save_drift_reports("orders", [])
    assert storage.get_drift_report("orders") == []
    assert len(storage.get_drift_report("users")) == 1


def test_get_drift_report_empty_when_no_cache(storage):
    assert storage.get_drift_report("ghost") == []


def test_save_drift_reports_handles_numeric_ks(storage):
    storage.save_drift_reports("events", [
        {"column": "amount", "data_type": "numeric", "psi": 0.12,
         "ks_pvalue": 0.003, "is_drift": True, "severity": "warn"},
    ])
    result = storage.get_drift_report("events")
    assert result[0]["ks_pvalue"] == pytest.approx(0.003)


def test_drift_reports_are_scoped_by_project(storage):
    storage.save_drift_reports("orders", [
        {"column": "country", "data_type": "varchar", "psi": 0.42,
         "ks_pvalue": None, "is_drift": True, "severity": "critical"},
    ], project_id="proj-a")
    storage.save_drift_reports("orders", [
        {"column": "status", "data_type": "varchar", "psi": 0.01,
         "ks_pvalue": None, "is_drift": False, "severity": "ok"},
    ], project_id="proj-b")

    assert [r["column"] for r in storage.get_drift_report("orders", "proj-a")] == ["country"]
    assert [r["column"] for r in storage.get_drift_report("orders", "proj-b")] == ["status"]


def test_anomaly_scores_are_scoped_by_project(storage):
    now = datetime.now(UTC)
    row = {"ts": now, "table_name": "orders", "score": -0.1, "is_anomaly": 1}
    storage.save_anomaly_scores([row], project_id="proj-a")
    storage.save_anomaly_scores([{**row, "score": 0.2, "is_anomaly": 0}], project_id="proj-b")

    a_scores = storage.get_anomaly_scores("orders", project_id="proj-a")
    b_scores = storage.get_anomaly_scores("orders", project_id="proj-b")

    assert a_scores[0]["score"] == -0.1
    assert a_scores[0]["is_anomaly"] == 1
    assert b_scores[0]["score"] == 0.2
    assert b_scores[0]["is_anomaly"] == 0


def test_history_anomalies_are_scoped_by_project(storage):
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now, "table_name": "orders", "metric_name": "row_count", "value": 10},
        {"ts": now, "table_name": "orders", "metric_name": "null_rate", "value": 0.01},
    ], "proj-a")
    storage.save_anomaly_scores([
        {"ts": now, "table_name": "orders", "score": -0.2, "is_anomaly": 1},
    ], project_id="proj-b")

    agg = storage.build_history_aggregate("proj-a")

    assert agg["anomalies_by_ts"] == {}


def test_migrates_legacy_ml_tables_to_project_scoped_pk(tmp_path, monkeypatch):
    import sqlite3

    db_path = tmp_path / "legacy.db"
    with sqlite3.connect(db_path) as conn:
        conn.executescript("""
            CREATE TABLE anomaly_scores (
                ts TEXT NOT NULL,
                table_name TEXT NOT NULL,
                score REAL NOT NULL,
                is_anomaly INTEGER NOT NULL,
                PRIMARY KEY (ts, table_name)
            );
            CREATE TABLE changepoints (
                ts TEXT NOT NULL,
                table_name TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                score REAL NOT NULL,
                value_before REAL NOT NULL,
                value_after REAL NOT NULL,
                detected_at TEXT NOT NULL,
                PRIMARY KEY (ts, table_name, metric_name)
            );
            CREATE TABLE drift_reports (
                table_name TEXT NOT NULL,
                column_name TEXT NOT NULL,
                data_type TEXT,
                psi REAL,
                ks_pvalue REAL,
                is_drift INTEGER NOT NULL,
                severity TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                PRIMARY KEY (table_name, column_name)
            );
            INSERT INTO anomaly_scores VALUES ('2026-01-01T00:00:00+00:00', 'orders', -0.1, 1);
            INSERT INTO changepoints VALUES (
                '2026-01-01T00:00:00+00:00', 'orders', 'row_count',
                3.0, 10.0, 20.0, '2026-01-01T00:01:00+00:00'
            );
            INSERT INTO drift_reports VALUES (
                'orders', 'country', 'varchar', 0.3, NULL, 1,
                'critical', '2026-01-01T00:02:00+00:00'
            );
        """)

    import app.metrics_storage as storage_mod

    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    engine = storage_mod.get_engine()

    assert "project_id" in storage_mod._existing_columns(engine, "anomaly_scores")
    assert storage_mod._sqlite_pk_columns(engine, "anomaly_scores") == [
        "project_id", "ts", "table_name",
    ]
    assert storage_mod._sqlite_pk_columns(engine, "changepoints") == [
        "project_id", "ts", "table_name", "metric_name",
    ]
    assert storage_mod._sqlite_pk_columns(engine, "drift_reports") == [
        "project_id", "table_name", "column_name",
    ]
    assert storage_mod.get_anomaly_scores(
        "orders", project_id="legacy", window=timedelta(days=3650)
    )
    assert storage_mod.get_changepoints(
        "orders", project_id="legacy", window=timedelta(days=3650)
    )
    assert storage_mod.get_drift_report("orders", project_id="legacy")
