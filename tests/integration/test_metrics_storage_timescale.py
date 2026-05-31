"""Run the metrics_storage SQLite-aimed test cases against TimescaleDB (#40).

Validates the second half of #40's acceptance criteria — "full test suite
green on TimescaleDB" — by replaying the same set of round-trip,
upsert, and cluster-replace scenarios from ``tests/test_metrics_storage.py``
on a real Timescale container. The container also exercises the
``create_hypertable``/``drop_chunks`` paths that the SQLite suite cannot.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text

from tests.integration.conftest import use_metrics_db

pytestmark = pytest.mark.integration

testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
PostgresContainer = testcontainers_postgres.PostgresContainer


# The community Timescale image bundles Postgres 16 + TimescaleDB.
_TIMESCALE_IMAGE = "timescale/timescaledb:latest-pg16"


@pytest.fixture(scope="module")
def timescale_container():
    with PostgresContainer(_TIMESCALE_IMAGE) as ts:
        yield ts


@pytest.fixture(scope="module")
def timescale_url(timescale_container) -> str:
    return timescale_container.get_connection_url().replace(
        "postgresql+psycopg2", "postgresql"
    )


@pytest.fixture(autouse=True)
def _clean_schema(timescale_url):
    """Drop tables between tests so each one starts on a fresh schema.

    metrics_storage.get_engine() reapplies the schema on first access via
    ``_initialized`` — clearing it lets the next test trigger a clean
    bootstrap (including ``create_hypertable``).
    """
    engine = create_engine(timescale_url, future=True)
    with engine.begin() as conn:
        for t in (
            "metrics", "changepoints", "schema_snapshots", "schema_events",
            "anomaly_scores", "drift_reports", "llm_explanations",
            "telegram_throttle", "notifications",
        ):
            conn.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE"))
    engine.dispose()
    yield


def test_schema_creates_hypertable(timescale_url):
    with use_metrics_db(timescale_url):
        from app.metrics_storage import get_engine

        get_engine()  # triggers _apply_schema
        engine = create_engine(timescale_url, future=True)
        with engine.connect() as conn:
            count = conn.execute(text(
                "SELECT count(*) FROM timescaledb_information.hypertables "
                "WHERE hypertable_name = 'metrics'"
            )).scalar()
        engine.dispose()
    assert count == 1, "metrics must be registered as a Timescale hypertable"


def test_save_and_get_metrics_roundtrip(timescale_url):
    with use_metrics_db(timescale_url):
        from app.metrics_storage import get_metrics, save_metrics

        now = datetime.now(UTC)
        rows = [
            {"ts": now - timedelta(hours=2), "table_name": "users",
             "metric_name": "row_count", "value": 100},
            {"ts": now - timedelta(hours=1), "table_name": "users",
             "metric_name": "row_count", "value": 110},
            {"ts": now, "table_name": "users",
             "metric_name": "row_count", "value": 120,
             "tags": {"column": "email"}},
        ]
        assert save_metrics(rows, "legacy") == 3
        result = get_metrics("users", "row_count", "legacy", window=timedelta(days=1))

    assert [r["value"] for r in result] == [100.0, 110.0, 120.0]
    # ts must be coerced to an ISO string on read, even though Timescale
    # stores a TIMESTAMPTZ — callers downstream rely on the string form.
    assert all(isinstance(r["ts"], str) for r in result)
    assert result[-1]["tags"] == {"column": "email"}


def test_get_metrics_respects_window(timescale_url):
    with use_metrics_db(timescale_url):
        from app.metrics_storage import get_metrics, save_metrics

        now = datetime.now(UTC)
        save_metrics([
            {"ts": now - timedelta(days=10), "table_name": "orders",
             "metric_name": "null_rate", "value": 0.05},
            {"ts": now - timedelta(days=2), "table_name": "orders",
             "metric_name": "null_rate", "value": 0.06},
            {"ts": now, "table_name": "orders",
             "metric_name": "null_rate", "value": 0.07},
        ], "legacy")
        result = get_metrics("orders", "null_rate", "legacy", window=timedelta(days=7))

    assert [r["value"] for r in result] == [0.06, 0.07]


def test_anomaly_scores_upsert(timescale_url):
    """ON CONFLICT (ts, table_name) DO UPDATE — replaces score on re-run."""
    with use_metrics_db(timescale_url):
        from app.metrics_storage import get_anomaly_scores, save_anomaly_scores

        ts = datetime.now(UTC)
        save_anomaly_scores([
            {"ts": ts, "table_name": "users", "score": -0.3, "is_anomaly": 1}
        ])
        save_anomaly_scores([
            {"ts": ts, "table_name": "users", "score": -0.9, "is_anomaly": 1}
        ])
        scores = get_anomaly_scores("users", window=timedelta(hours=1))

    assert len(scores) == 1
    assert scores[0]["score"] == -0.9


def test_changepoints_cluster_replaces_lower_score(timescale_url):
    """Within the 72h cluster window the higher-score detection wins."""
    with use_metrics_db(timescale_url):
        from app.metrics_storage import get_changepoints, save_changepoints

        now = datetime.now(UTC)
        save_changepoints([{
            "ts": now - timedelta(hours=24), "table_name": "users",
            "metric_name": "row_count", "score": 5.0,
            "value_before": 100, "value_after": 200,
        }])
        save_changepoints([{
            "ts": now - timedelta(hours=12), "table_name": "users",
            "metric_name": "row_count", "score": 7.5,
            "value_before": 100, "value_after": 200,
        }])
        cps = get_changepoints("users", window=timedelta(days=7))

    assert len(cps) == 1
    assert cps[0]["score"] == 7.5


def test_save_notification_returns_id(timescale_url):
    """BIGSERIAL + RETURNING id (Postgres branch of save_notification)."""
    with use_metrics_db(timescale_url):
        from app.metrics_storage import get_notifications, save_notification

        nid = save_notification(
            event_type="anomaly", message="boom", status="sent",
            table_name="users", metric_name="row_count",
        )
        rows = get_notifications(table_name="users")

    assert nid > 0
    assert len(rows) == 1
    assert rows[0]["id"] == nid
    assert rows[0]["message"] == "boom"


def test_purge_old_drops_timescale_chunks(timescale_url):
    """drop_chunks deletes the old chunk; recent rows survive."""
    with use_metrics_db(timescale_url):
        from app.metrics_storage import get_metrics, purge_old, save_metrics

        now = datetime.now(UTC)
        save_metrics([
            {"ts": now - timedelta(days=200), "table_name": "users",
             "metric_name": "row_count", "value": 1},
            {"ts": now, "table_name": "users",
             "metric_name": "row_count", "value": 2},
        ], "legacy")
        purge_old(retention_days=90)
        remaining = get_metrics("users", "row_count", "legacy", window=timedelta(days=365))

    assert [r["value"] for r in remaining] == [2.0]


def test_throttle_roundtrip(timescale_url):
    """is_throttled returns False then True after update_throttle."""
    with use_metrics_db(timescale_url):
        from app.metrics_storage import is_throttled, update_throttle

        assert not is_throttled("legacy", "users", "anomaly_row_count")
        update_throttle("legacy", "users", "anomaly_row_count")
        assert is_throttled("legacy", "users", "anomaly_row_count")
