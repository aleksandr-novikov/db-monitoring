"""Per-dialect adapter tests with mocked SQLAlchemy engines.

Covers MySQL and ClickHouse adapters end-to-end at the unit-test level
(Postgres is also exercised via the module-level helpers in test_db_utils.py
since it is the default adapter).

Each test inspects the actual SQL string passed to engine.connect().execute()
so we catch dialect-specific regressions (wrong table source, wrong quoting)
without needing a live MySQL/ClickHouse instance.
"""

from unittest.mock import MagicMock, patch

import pytest

from app.db import ClickHouseAdapter, MySQLAdapter


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    with patch("app.db.get_engine", return_value=mock_engine):
        yield conn


def _executed_sql(conn) -> list[str]:
    """Collect the SQL string from each engine.execute() call."""
    return [str(call.args[0]) for call in conn.execute.call_args_list]


# --- MySQL ------------------------------------------------------------------


def test_mysql_list_tables_uses_information_schema(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = [("users",), ("orders",)]

    result = MySQLAdapter().list_tables("appdb")

    assert result == [
        {"table_name": "users", "schema": "appdb"},
        {"table_name": "orders", "schema": "appdb"},
    ]
    sql = _executed_sql(mock_conn)[0]
    assert "information_schema.tables" in sql
    assert "BASE TABLE" in sql


def test_mysql_table_stats_combines_data_and_index_length(mock_conn):
    mock_conn.execute.return_value.fetchone.return_value = (
        500,
        16384,
        "2026-04-29 12:00:00",
    )

    result = MySQLAdapter().table_stats("users", "appdb")

    assert result == {
        "table_name": "users",
        "schema": "appdb",
        "row_count": 500,
        "size_bytes": 16384,
        "last_analyze": "2026-04-29 12:00:00",
    }
    sql = _executed_sql(mock_conn)[0]
    assert "data_length" in sql
    assert "index_length" in sql
    assert "update_time" in sql


def test_mysql_table_stats_handles_null_update_time(mock_conn):
    mock_conn.execute.return_value.fetchone.return_value = (0, 0, None)

    result = MySQLAdapter().table_stats("partitioned_t", "appdb")

    assert result["last_analyze"] is None
    assert result["row_count"] == 0


def test_mysql_table_stats_not_found(mock_conn):
    mock_conn.execute.return_value.fetchone.return_value = None
    assert MySQLAdapter().table_stats("missing", "appdb") is None


def test_mysql_column_nulls_uses_count_diff_and_backtick_quoting(mock_conn):
    cols_result = MagicMock()
    cols_result.fetchall.return_value = [
        ("email", "varchar", "YES"),
        ("age", "int", "YES"),
    ]
    count_result = MagicMock()
    count_result.fetchone.return_value = (200, 20, 0)
    mock_conn.execute.side_effect = [cols_result, count_result]

    result = MySQLAdapter().column_nulls("users", "appdb")

    assert result == [
        {"column": "email", "data_type": "varchar", "null_count": 20, "null_rate": 0.1},
        {"column": "age", "data_type": "int", "null_count": 0, "null_rate": 0.0},
    ]
    count_sql = _executed_sql(mock_conn)[1]
    assert "COUNT(*) - COUNT(`email`)" in count_sql
    assert "`appdb`.`users`" in count_sql


def test_mysql_quote_ident_doubles_backtick():
    assert MySQLAdapter().quote_ident("name`with`tick") == "`name``with``tick`"


# --- ClickHouse -------------------------------------------------------------


def test_clickhouse_list_tables_filters_views_and_temp(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = [("events",)]

    result = ClickHouseAdapter().list_tables("default")

    assert result == [{"table_name": "events", "schema": "default"}]
    sql = _executed_sql(mock_conn)[0]
    assert "system.tables" in sql
    assert "View" in sql
    assert "is_temporary" in sql


def test_clickhouse_table_stats_reads_total_rows_and_parts(mock_conn):
    mock_conn.execute.return_value.fetchone.return_value = (
        1_000_000,
        52_428_800,
        "2026-04-29 18:00:00",
    )

    result = ClickHouseAdapter().table_stats("events", "default")

    assert result == {
        "table_name": "events",
        "schema": "default",
        "row_count": 1_000_000,
        "size_bytes": 52_428_800,
        "last_analyze": "2026-04-29 18:00:00",
    }
    sql = _executed_sql(mock_conn)[0]
    assert "system.tables" in sql
    assert "system.parts" in sql
    assert "modification_time" in sql


def test_clickhouse_table_schema_detects_nullable(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = [
        ("id", "UInt64", 0),
        ("email", "Nullable(String)", 1),
    ]

    result = ClickHouseAdapter().table_schema("users", "default")

    assert result == [
        {"name": "id", "type": "UInt64", "nullable": False},
        {"name": "email", "type": "Nullable(String)", "nullable": True},
    ]
    sql = _executed_sql(mock_conn)[0]
    assert "system.columns" in sql
    assert "startsWith" in sql


def test_clickhouse_column_nulls_uses_generic_count_diff(mock_conn):
    cols_result = MagicMock()
    cols_result.fetchall.return_value = [
        ("id", "UInt64", 0),
        ("email", "Nullable(String)", 1),
    ]
    count_result = MagicMock()
    # All ids non-null (UInt64 cannot be NULL); 5/100 emails are NULL.
    count_result.fetchone.return_value = (100, 0, 5)
    mock_conn.execute.side_effect = [cols_result, count_result]

    result = ClickHouseAdapter().column_nulls("users", "default")

    assert result == [
        {"column": "id", "data_type": "UInt64", "null_count": 0, "null_rate": 0.0},
        {"column": "email", "data_type": "Nullable(String)", "null_count": 5, "null_rate": 0.05},
    ]
    count_sql = _executed_sql(mock_conn)[1]
    assert "COUNT(*) - COUNT(`id`)" in count_sql
    assert "`default`.`users`" in count_sql


def test_clickhouse_quote_ident_escapes_backtick():
    assert ClickHouseAdapter().quote_ident("we`ird") == "`we\\`ird`"
