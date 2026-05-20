"""End-to-end MySQL adapter tests against a real container (#44).

Mirrors test_db_postgres but exercises MySQLAdapter — backtick identifier
quoting, `information_schema.tables` for stats, and the generic
``COUNT(*) - COUNT(col)`` NULL-count path.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.integration.conftest import use_target_db

pytestmark = pytest.mark.integration

testcontainers_mysql = pytest.importorskip("testcontainers.mysql")
MySqlContainer = testcontainers_mysql.MySqlContainer


@pytest.fixture(scope="module")
def mysql_container():
    with MySqlContainer("mysql:8.4") as my:
        yield my


def _to_pymysql(url: str) -> str:
    """Force the PyMySQL driver — MySqlContainer's default is `mysql+mysqldb`,
    but the project pins PyMySQL (and MySQLdb isn't installed).
    """
    for prefix in ("mysql+mysqldb://", "mysql+mysqlconnector://", "mysql://"):
        if url.startswith(prefix):
            return "mysql+pymysql://" + url[len(prefix):]
    return url


@pytest.fixture(scope="module")
def mysql_url(mysql_container):
    # Keep the `+pymysql` driver suffix — without it SQLAlchemy falls back to
    # the MySQLdb (mysqlclient) driver, which isn't installed. Backend
    # detection still routes to MySQLAdapter: `make_url(url).get_backend_name()`
    # returns "mysql" for any `mysql[+driver]://...` form.
    return _to_pymysql(mysql_container.get_connection_url())


@pytest.fixture(scope="module")
def mysql_schema(mysql_container) -> str:
    """The MySqlContainer creates a default DB; return its name."""
    # SQLAlchemy URL path includes the DB name (after the slash, before query).
    return mysql_container.get_connection_url().rsplit("/", 1)[-1].split("?", 1)[0]


@pytest.fixture(scope="module", autouse=True)
def _seed(mysql_container):
    engine = create_engine(_to_pymysql(mysql_container.get_connection_url()), future=True)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE users (
                id      INT AUTO_INCREMENT PRIMARY KEY,
                email   VARCHAR(255),
                country VARCHAR(8) NOT NULL
            )
        """))
        conn.execute(text("""
            INSERT INTO users (email, country) VALUES
                ('a@x.io', 'RU'),
                (NULL,     'US'),
                ('c@x.io', 'RU'),
                (NULL,     'DE')
        """))
        # information_schema.tables.table_rows is filled by ANALYZE on InnoDB.
        conn.execute(text("ANALYZE TABLE users"))
    engine.dispose()


def test_list_tables_returns_seeded_table(mysql_url, mysql_schema):
    with use_target_db(mysql_url):
        from app.db import list_tables
        tables = list_tables(schema=mysql_schema)
    names = [t["table_name"] for t in tables]
    assert "users" in names


def test_table_stats_reports_row_count_and_size(mysql_url, mysql_schema):
    with use_target_db(mysql_url):
        from app.db import table_stats
        stats = table_stats("users", schema=mysql_schema)
    assert stats is not None
    assert stats["table_name"] == "users"
    # InnoDB row count is an estimate — for 4 rows it may report any small
    # number, but should not be 0 after ANALYZE.
    assert stats["row_count"] >= 1
    assert stats["size_bytes"] > 0


def test_table_stats_returns_none_for_missing_table(mysql_url, mysql_schema):
    with use_target_db(mysql_url):
        from app.db import table_stats
        assert table_stats("does_not_exist", schema=mysql_schema) is None


def test_table_schema_returns_columns_with_nullability(mysql_url, mysql_schema):
    with use_target_db(mysql_url):
        from app.db import table_schema
        cols = table_schema("users", schema=mysql_schema)
    by_name = {c["name"]: c for c in cols}
    assert by_name["id"]["nullable"] is False
    assert by_name["email"]["nullable"] is True
    assert by_name["country"]["nullable"] is False


def test_column_nulls_counts_email_nulls(mysql_url, mysql_schema):
    with use_target_db(mysql_url):
        from app.db import column_nulls
        cols = column_nulls("users", schema=mysql_schema)
    by_name = {c["column"]: c for c in cols}
    assert by_name["email"]["null_count"] == 2
    assert by_name["email"]["null_rate"] == 0.5
    assert by_name["country"]["null_count"] == 0
    assert by_name["id"]["null_count"] == 0
