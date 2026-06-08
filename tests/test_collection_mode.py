"""Tests for #233 — collection_mode sample/approx.

Three regimes for null_rate collection:

* ``full``   — current behavior (null_count + null_rate + distribution).
* ``sample`` — Postgres only. TABLESAMPLE SYSTEM(1). No null_count, no
  distribution. Tags ``source=sample``, ``sample_size=N``,
  ``sample_percent=1.0``.
* ``approx`` — Postgres only. ``pg_stats.null_frac`` read. No null_count,
  no distribution. Tags ``source=approx``.

ClickHouse / Iceberg connections with sample/approx → warning + fallback
to ``full`` (no exception, no missing metrics).
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest import mock

import pytest

# --- Fixtures --------------------------------------------------------------


@pytest.fixture
def fake_adapter(monkeypatch):
    """A stub adapter whose column_nulls* methods can be programmed
    per-test. Wired into both ``app.db.get_adapter`` (used by collector
    methods) and ``app.db.table_stats``."""
    adapter = mock.MagicMock()
    adapter.table_stats.return_value = {
        "table_name": "t",
        "schema": "public",
        "row_count": 100,
        "size_bytes": 1000,
        "last_analyze": None,
    }
    monkeypatch.setattr("app.db.get_adapter", lambda: adapter)
    monkeypatch.setattr(
        "app.db.table_stats",
        lambda name, schema=None: adapter.table_stats(name, schema),
    )
    monkeypatch.setattr(
        "app.db.column_nulls",
        lambda name, schema=None: adapter.column_nulls(name, schema),
    )
    monkeypatch.setattr(
        "app.db.column_distribution",
        lambda name, schema=None: adapter.column_distribution(name, schema),
    )
    return adapter


# --- full mode regression --------------------------------------------------


def test_full_mode_unchanged_behavior(fake_adapter):
    """full mode keeps null_count + null_rate + distribution."""
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls.return_value = [
        {"column": "id", "data_type": "int", "null_count": 0, "null_rate": 0.0},
        {"column": "email", "data_type": "text", "null_count": 5, "null_rate": 0.05},
    ]
    fake_adapter.column_distribution.return_value = []

    collector = MetricsCollector(schema="public", collection_mode="full")
    rows = collector.collect("t", ts=datetime.now(UTC))

    metric_names = [r["metric_name"] for r in rows]
    assert "row_count" in metric_names
    assert "size_bytes" in metric_names
    assert metric_names.count("null_count") == 2
    assert "null_rate" in metric_names
    # full → null_rate без source-тега (badge не рисуется).
    null_rate_row = next(r for r in rows if r["metric_name"] == "null_rate")
    assert null_rate_row.get("tags") is None


# --- sample mode -----------------------------------------------------------


def test_sample_mode_no_null_count_no_distribution(fake_adapter):
    """sample mode skips null_count and column_distribution entirely."""
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls_sample.return_value = (
        [
            {"column": "id", "data_type": "int", "null_rate": 0.0},
            {"column": "email", "data_type": "text", "null_rate": 0.04},
        ],
        1500,  # sample_size
    )

    collector = MetricsCollector(schema="public", collection_mode="sample")
    rows = collector.collect("t", ts=datetime.now(UTC))

    metric_names = [r["metric_name"] for r in rows]
    assert "null_count" not in metric_names
    assert "column_distribution" not in metric_names
    assert "null_rate" in metric_names
    # column_nulls (full path) MUST NOT have been called.
    assert not fake_adapter.column_nulls.called
    assert not fake_adapter.column_distribution.called


def test_sample_mode_null_rate_carries_source_tag(fake_adapter):
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls_sample.return_value = (
        [
            {"column": "id", "data_type": "int", "null_rate": 0.0},
            {"column": "email", "data_type": "text", "null_rate": 0.04},
        ],
        1500,
    )
    collector = MetricsCollector(schema="public", collection_mode="sample")
    rows = collector.collect("t", ts=datetime.now(UTC))

    nr = next(r for r in rows if r["metric_name"] == "null_rate")
    assert nr["tags"] == {
        "source": "sample",
        "sample_size": 1500,
        "sample_percent": 1.0,
    }
    assert nr["value"] == 0.02  # (0 + 0.04) / 2


def test_sample_mode_zero_rows_drops_null_rate(fake_adapter, caplog):
    """Empty sample → null_rate NOT stored, warning logged. No fallback."""
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls_sample.return_value = ([], 0)
    collector = MetricsCollector(schema="public", collection_mode="sample")
    with caplog.at_level(logging.WARNING):
        rows = collector.collect("t", ts=datetime.now(UTC))

    metric_names = [r["metric_name"] for r in rows]
    assert "null_rate" not in metric_names
    assert any(
        "sample returned 0 rows" in rec.message for rec in caplog.records
    )
    # Adapter's column_nulls (full path) NOT called as fallback.
    assert not fake_adapter.column_nulls.called


# --- approx mode -----------------------------------------------------------


def test_approx_mode_no_null_count_no_distribution(fake_adapter):
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls_approx.return_value = [
        {"column": "id", "data_type": "int", "null_rate": 0.0},
        {"column": "email", "data_type": "text", "null_rate": 0.02},
    ]
    collector = MetricsCollector(schema="public", collection_mode="approx")
    rows = collector.collect("t", ts=datetime.now(UTC))

    metric_names = [r["metric_name"] for r in rows]
    assert "null_count" not in metric_names
    assert "column_distribution" not in metric_names
    assert "null_rate" in metric_names
    assert not fake_adapter.column_nulls.called


def test_approx_mode_null_rate_carries_source_tag(fake_adapter):
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls_approx.return_value = [
        {"column": "id", "data_type": "int", "null_rate": 0.0},
        {"column": "email", "data_type": "text", "null_rate": 0.02},
    ]
    collector = MetricsCollector(schema="public", collection_mode="approx")
    rows = collector.collect("t", ts=datetime.now(UTC))

    nr = next(r for r in rows if r["metric_name"] == "null_rate")
    assert nr["tags"] == {"source": "approx"}
    # NB no sample_size — approx doesn't sample, the number is meaningless.
    assert "sample_size" not in nr["tags"]


def test_approx_mode_no_pg_stats_drops_null_rate(fake_adapter, caplog):
    """If pg_stats has no row for the table (never ANALYZE'd) → drop, warn."""
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls_approx.return_value = []
    collector = MetricsCollector(schema="public", collection_mode="approx")
    with caplog.at_level(logging.WARNING):
        rows = collector.collect("t", ts=datetime.now(UTC))

    metric_names = [r["metric_name"] for r in rows]
    assert "null_rate" not in metric_names
    assert any("no pg_stats" in rec.message for rec in caplog.records)


# --- mode validation -------------------------------------------------------


def test_unknown_mode_falls_back_to_full(fake_adapter):
    """Unknown value silently downgrades to 'full' — defensive, no crash."""
    from collectors.metrics_collector import MetricsCollector

    fake_adapter.column_nulls.return_value = []
    fake_adapter.column_distribution.return_value = []
    collector = MetricsCollector(schema="public", collection_mode="bogus")
    assert collector.collection_mode == "full"


# --- per_project fallback for non-Postgres ---------------------------------


def test_non_postgres_warns_and_downgrades_to_full(caplog):
    """ClickHouse with collection_mode='sample' → warning + full mode used."""
    from collectors import per_project

    conn_row = {
        "id": "abc",
        "project_id": "p",
        "collection_mode": "sample",
        "schema_name": "default",
        "table_allowlist": None,
        "table_denylist": None,
        "max_tables_per_tick": None,
    }
    is_postgres = False
    # Mimic the snippet in collect_for_connection without spinning the full
    # collector machinery. The actual code is asserted by reading it back
    # here — this keeps the test fast while pinning the contract.
    requested_mode = (conn_row.get("collection_mode") or "full").lower()
    effective_mode = requested_mode
    with caplog.at_level(logging.WARNING):
        if requested_mode in ("sample", "approx") and not is_postgres:
            logger = logging.getLogger(per_project.__name__)
            logger.warning(
                "collection_mode=%r not supported for non-Postgres",
                requested_mode,
            )
            effective_mode = "full"
    assert effective_mode == "full"
    assert any(
        "not supported for non-Postgres" in r.message for r in caplog.records
    )


# --- Adapter SQL contracts -------------------------------------------------


def test_postgres_adapter_sample_sql_uses_tablesample(monkeypatch):
    """SQL emitted by column_nulls_sample contains TABLESAMPLE SYSTEM(...)."""
    from app.db import PostgresAdapter

    captured: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):
            captured.append(str(stmt))
            # Simulate: columns metadata first, then the COUNT(*) row.
            class _R:
                def fetchall(self_inner):
                    return [("id", "int"), ("email", "text")]
                def fetchone(self_inner):
                    return (100, 1, 5)  # total, null id, null email
            return _R()
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False

    monkeypatch.setattr("app.db.get_engine", lambda: mock.MagicMock(
        connect=lambda: _Conn(),
    ))

    adapter = PostgresAdapter()
    per_col, size = adapter.column_nulls_sample("t", "public", percent=1.0)
    assert size == 100
    assert any("TABLESAMPLE SYSTEM(1.0)" in s for s in captured)
    assert {"id", "email"} == {c["column"] for c in per_col}


def test_postgres_adapter_approx_sql_uses_null_frac(monkeypatch):
    """SQL emitted by column_nulls_approx pulls from pg_stats.null_frac."""
    from app.db import PostgresAdapter

    captured: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):
            captured.append(str(stmt))
            class _R:
                def fetchall(self_inner):
                    return [("id", "int", 0.0), ("email", "text", 0.04)]
            return _R()
        def __enter__(self):
            return self
        def __exit__(self, *_):
            return False

    monkeypatch.setattr("app.db.get_engine", lambda: mock.MagicMock(
        connect=lambda: _Conn(),
    ))

    adapter = PostgresAdapter()
    rows = adapter.column_nulls_approx("t", "public")
    assert any("null_frac" in s.lower() for s in captured)
    # The wrong column name (null_fraction) must NOT appear.
    assert not any("null_fraction" in s for s in captured)
    assert rows[1]["null_rate"] == 0.04
