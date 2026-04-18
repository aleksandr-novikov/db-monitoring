import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import pytest

from app.db import column_nulls, list_tables, table_stats


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    mock_engine = MagicMock()
    mock_engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    with patch("app.db.get_engine", return_value=mock_engine):
        yield conn


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

    assert result["row_count"] == 0
    assert result["size_bytes"] == 0
    assert result["last_analyze"] is None


def test_column_nulls(mock_conn):
    cols_result = MagicMock()
    cols_result.fetchall.return_value = [("email",), ("age",), ("name",)]

    count_result = MagicMock()
    count_result.fetchone.return_value = (1000, 50, 0, 10)

    mock_conn.execute.side_effect = [cols_result, count_result]

    result = column_nulls("users", schema="public")

    assert len(result) == 3
    assert result[0] == {"column": "email", "null_count": 50, "null_rate": 0.05}
    assert result[1] == {"column": "age", "null_count": 0, "null_rate": 0.0}
    assert result[2] == {"column": "name", "null_count": 10, "null_rate": 0.01}


def test_column_nulls_empty_table(mock_conn):
    mock_conn.execute.return_value.fetchall.return_value = []

    result = column_nulls("empty_table", schema="public")

    assert result == []
