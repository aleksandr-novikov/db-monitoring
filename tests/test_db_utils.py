from unittest.mock import MagicMock, patch

import pytest

from app import db
from app.db import (
    ClickHouseAdapter,
    MySQLAdapter,
    PostgresAdapter,
    column_nulls,
    get_adapter,
    list_tables,
    table_stats,
)


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    with patch("app.db.get_engine", return_value=mock_engine):
        yield conn


@pytest.fixture(autouse=True)
def _reset_adapter_cache():
    db._adapter = None
    yield
    db._adapter = None


def test_list_tables(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = [
        ("users",),
        ("orders",),
        ("products",),
    ]

    result = list_tables(schema="public")

    assert len(result) == 3
    assert result[0] == {"table_name": "users", "schema": "public"}
    assert result[2] == {"table_name": "products", "schema": "public"}


def test_list_tables_empty(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = []

    result = list_tables(schema="public")

    assert result == []


def test_table_stats(mock_conn):
    mock_conn.execute.return_value.fetchone.return_value = (
        1500,
        65536,
        "2026-04-17 03:00:00",
        None,
    )

    result = table_stats("users", schema="public")

    assert result["table_name"] == "users"
    assert result["row_count"] == 1500
    assert result["size_bytes"] == 65536
    assert result["last_analyze"] == "2026-04-17 03:00:00"


def test_table_stats_autoanalyze_fallback(mock_conn):
    mock_conn.execute.return_value.fetchone.return_value = (
        200,
        8192,
        None,
        "2026-04-16 01:00:00",
    )

    result = table_stats("orders", schema="public")

    assert result["last_analyze"] == "2026-04-16 01:00:00"


def test_table_stats_not_found(mock_conn):
    mock_conn.execute.return_value.fetchone.return_value = None

    result = table_stats("nonexistent", schema="public")

    assert result is None


def test_column_nulls(mock_conn):
    cols_result = MagicMock()
    cols_result.fetchall.return_value = [
        ("email", "text"),
        ("age", "integer"),
        ("name", "text"),
    ]

    count_result = MagicMock()
    count_result.fetchone.return_value = (1000, 50, 0, 10)

    mock_conn.execute.side_effect = [cols_result, count_result]

    result = column_nulls("users", schema="public")

    assert len(result) == 3
    assert result[0] == {"column": "email", "data_type": "text", "null_count": 50, "null_rate": 0.05}
    assert result[1] == {"column": "age", "data_type": "integer", "null_count": 0, "null_rate": 0.0}
    assert result[2] == {"column": "name", "data_type": "text", "null_count": 10, "null_rate": 0.01}


def test_column_nulls_empty_table(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = []

    result = column_nulls("empty_table", schema="public")

    assert result == []


# --- factory ----------------------------------------------------------------


@pytest.mark.parametrize(
    "url, expected_cls",
    [
        ("postgresql://u:p@h/d", PostgresAdapter),
        ("postgresql+psycopg2://u:p@h/d", PostgresAdapter),
        ("mysql://u:p@h/d", MySQLAdapter),
        ("mysql+pymysql://u:p@h/d", MySQLAdapter),
        ("clickhouse://u:p@h/d", ClickHouseAdapter),
        ("clickhouse+native://u:p@h/d", ClickHouseAdapter),
    ],
)
def test_get_adapter_dispatch(url, expected_cls):
    with patch.object(db.settings, "DATABASE_URL", url):
        assert isinstance(get_adapter(), expected_cls)


def test_get_adapter_unsupported_raises():
    with patch.object(db.settings, "DATABASE_URL", "oracle://u:p@h/d"):
        with pytest.raises(ValueError, match="Unsupported database backend"):
            get_adapter()


# --- quoting ----------------------------------------------------------------


def test_postgres_quote_doubles_double_quote():
    assert PostgresAdapter().quote_ident('we"ird') == '"we""ird"'


def test_mysql_quote_doubles_backtick():
    assert MySQLAdapter().quote_ident("we`ird") == "`we``ird`"


def test_clickhouse_quote_escapes_backtick():
    assert ClickHouseAdapter().quote_ident("we`ird") == "`we\\`ird`"
