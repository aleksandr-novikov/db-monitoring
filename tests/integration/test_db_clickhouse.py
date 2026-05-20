"""End-to-end ClickHouse adapter tests against a real container (#44).

Exercises ClickHouseAdapter — `system.tables`/`system.columns` introspection,
backtick quoting, and the Nullable(...) detection in `table_schema`.

NULL semantics on ClickHouse differ from Postgres/MySQL: only
``Nullable(T)`` columns can contain NULL, so the seeded table mixes one
nullable and one plain column to verify both branches of the generic
``COUNT(*) - COUNT(col)`` NULL counter.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.integration.conftest import use_target_db

pytestmark = pytest.mark.integration

testcontainers_clickhouse = pytest.importorskip("testcontainers.clickhouse")
ClickHouseContainer = testcontainers_clickhouse.ClickHouseContainer


def _to_sqlalchemy_url(url: str) -> str:
    """Normalise the testcontainers URL to the clickhouse-sqlalchemy form.

    ``ClickHouseContainer.get_connection_url()`` returns ``clickhouse://...``
    which clickhouse-sqlalchemy maps to its native driver by default — but
    being explicit (``clickhouse+native://...``) is more robust across
    driver versions.
    """
    if url.startswith("clickhouse+"):
        return url
    return url.replace("clickhouse://", "clickhouse+native://", 1)


@pytest.fixture(scope="module")
def ch_container():
    with ClickHouseContainer("clickhouse/clickhouse-server:24.8") as ch:
        yield ch


@pytest.fixture(scope="module")
def ch_url(ch_container):
    return _to_sqlalchemy_url(ch_container.get_connection_url())


@pytest.fixture(scope="module")
def ch_database(ch_container) -> str:
    """ClickHouse "database" — analogous to a Postgres schema."""
    return ch_container.get_connection_url().rsplit("/", 1)[-1]


@pytest.fixture(scope="module", autouse=True)
def _seed(ch_container):
    raw_url = _to_sqlalchemy_url(ch_container.get_connection_url())
    engine = create_engine(raw_url, future=True)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE users (
                id      UInt64,
                email   Nullable(String),
                country String
            ) ENGINE = MergeTree() ORDER BY id
        """))
        conn.execute(text(
            "INSERT INTO users (id, email, country) VALUES "
            "(1, 'a@x.io', 'RU'), "
            "(2, NULL,     'US'), "
            "(3, 'c@x.io', 'RU'), "
            "(4, NULL,     'DE')"
        ))
    engine.dispose()


def test_list_tables_returns_seeded_table(ch_url, ch_database):
    with use_target_db(ch_url):
        from app.db import list_tables
        tables = list_tables(schema=ch_database)
    names = [t["table_name"] for t in tables]
    assert "users" in names


def test_table_stats_reports_row_count_and_size(ch_url, ch_database):
    with use_target_db(ch_url):
        from app.db import table_stats
        stats = table_stats("users", schema=ch_database)
    assert stats is not None
    assert stats["table_name"] == "users"
    assert stats["row_count"] == 4
    assert stats["size_bytes"] > 0


def test_table_stats_returns_none_for_missing_table(ch_url, ch_database):
    with use_target_db(ch_url):
        from app.db import table_stats
        assert table_stats("does_not_exist", schema=ch_database) is None


def test_table_schema_detects_nullable(ch_url, ch_database):
    with use_target_db(ch_url):
        from app.db import table_schema
        cols = table_schema("users", schema=ch_database)
    by_name = {c["name"]: c for c in cols}
    assert by_name["email"]["nullable"] is True
    # Non-Nullable columns must report nullable=False even though the dialect
    # has no SQL-standard NOT NULL keyword.
    assert by_name["id"]["nullable"] is False
    assert by_name["country"]["nullable"] is False


def test_column_nulls_counts_only_nullable_columns(ch_url, ch_database):
    with use_target_db(ch_url):
        from app.db import column_nulls
        cols = column_nulls("users", schema=ch_database)
    by_name = {c["column"]: c for c in cols}
    # 2 of 4 emails are NULL (the column is Nullable(String)).
    assert by_name["email"]["null_count"] == 2
    assert by_name["email"]["null_rate"] == 0.5
    # Non-Nullable columns can't contain NULL — must report zero.
    assert by_name["id"]["null_count"] == 0
    assert by_name["country"]["null_count"] == 0
