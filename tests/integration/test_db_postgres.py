"""End-to-end Postgres adapter tests against a real container (#44).

Runs the production adapter against a real Postgres so dialect-specific
SQL (information_schema, pg_stat_user_tables, FILTER, quote_ident) is
verified — not just shapes-of-mocks. The container is session-scoped to
amortise the ~3-5s startup over all Postgres-adapter tests.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.integration.conftest import use_target_db

pytestmark = pytest.mark.integration

testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
PostgresContainer = testcontainers_postgres.PostgresContainer


@pytest.fixture(scope="module")
def pg_container():
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="module")
def pg_url(pg_container):
    # Normalise to plain `postgresql://` so app.db's backend detection
    # routes to PostgresAdapter (rather than `postgresql+psycopg2://`,
    # which works but adds noise to URL assertions).
    url = pg_container.get_connection_url()
    return url.replace("postgresql+psycopg2", "postgresql")


@pytest.fixture(scope="module", autouse=True)
def _seed(pg_container):
    """Create one table with a NULL and one fully-populated column."""
    raw_url = pg_container.get_connection_url()
    engine = create_engine(raw_url, future=True)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE users (
                id      SERIAL PRIMARY KEY,
                email   TEXT,
                country TEXT NOT NULL
            )
        """))
        conn.execute(text("""
            INSERT INTO users (email, country) VALUES
                ('a@x.io', 'RU'),
                (NULL,     'US'),
                ('c@x.io', 'RU'),
                (NULL,     'DE')
        """))
        # pg_stat_user_tables.n_live_tup is populated by autovacuum; for a
        # fresh table we need an explicit ANALYZE so table_stats returns a
        # non-zero row_count.
        conn.execute(text("ANALYZE users"))
    engine.dispose()


def test_list_tables_returns_seeded_table(pg_url):
    with use_target_db(pg_url):
        from app.db import list_tables
        tables = list_tables(schema="public")
    names = [t["table_name"] for t in tables]
    assert "users" in names
    assert all(t["schema"] == "public" for t in tables)


def test_table_stats_reports_row_count_and_size(pg_url):
    with use_target_db(pg_url):
        from app.db import table_stats
        stats = table_stats("users", schema="public")
    assert stats is not None
    assert stats["table_name"] == "users"
    assert stats["row_count"] == 4
    assert stats["size_bytes"] > 0


def test_table_stats_returns_none_for_missing_table(pg_url):
    with use_target_db(pg_url):
        from app.db import table_stats
        assert table_stats("does_not_exist", schema="public") is None


def test_table_schema_returns_columns_with_nullability(pg_url):
    with use_target_db(pg_url):
        from app.db import table_schema
        cols = table_schema("users", schema="public")
    by_name = {c["name"]: c for c in cols}
    assert by_name["id"]["nullable"] is False
    assert by_name["email"]["nullable"] is True
    assert by_name["country"]["nullable"] is False
    assert "integer" in by_name["id"]["type"]


def test_column_nulls_counts_email_nulls(pg_url):
    with use_target_db(pg_url):
        from app.db import column_nulls
        cols = column_nulls("users", schema="public")
    by_name = {c["column"]: c for c in cols}
    # 2 of 4 emails are NULL → null_rate 0.5
    assert by_name["email"]["null_count"] == 2
    assert by_name["email"]["null_rate"] == 0.5
    # NOT NULL columns must report zero
    assert by_name["country"]["null_count"] == 0
    assert by_name["id"]["null_count"] == 0
