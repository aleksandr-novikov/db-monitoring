from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from app.app import create_app


@pytest.fixture
def client():
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


# ---------------------------------------------------------------------------
# GET /api/tables
# ---------------------------------------------------------------------------

def test_tables_returns_list(client):
    tables = [{"table_name": "users", "schema": "public"}]
    rc = {"ts": "2026-04-25T10:00:00+00:00", "value": 50000.0, "tags": None}
    nr = {"ts": "2026-04-25T10:00:00+00:00", "value": 0.05, "tags": None}

    with patch("app.api.list_tables", return_value=tables), \
         patch("app.api.get_latest_metric", side_effect=[rc, nr]):
        resp = client.get("/api/tables")

    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["table_name"] == "users"
    assert data[0]["row_count"] == 50000.0
    assert data[0]["null_rate"] == 0.05
    assert data[0]["last_check"] == "2026-04-25T10:00:00+00:00"


def test_tables_no_metrics_returns_nulls(client):
    tables = [{"table_name": "empty_table", "schema": "public"}]

    with patch("app.api.list_tables", return_value=tables), \
         patch("app.api.get_latest_metric", return_value=None):
        resp = client.get("/api/tables")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data[0]["row_count"] is None
    assert data[0]["null_rate"] is None
    assert data[0]["last_check"] is None


def test_tables_empty_db(client):
    with patch("app.api.list_tables", return_value=[]), \
         patch("app.api.get_latest_metric", return_value=None):
        resp = client.get("/api/tables")

    assert resp.status_code == 200
    assert resp.get_json() == []


# ---------------------------------------------------------------------------
# GET /api/metrics/<table>
# ---------------------------------------------------------------------------

def test_metrics_default_params(client):
    rows = [
        {"ts": "2026-04-24T10:00:00+00:00", "value": 100.0, "tags": None},
        {"ts": "2026-04-25T10:00:00+00:00", "value": 110.0, "tags": None},
    ]
    with patch("app.api.get_metrics", return_value=rows) as mock_get:
        resp = client.get("/api/metrics/users")

    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    assert data[0] == {"ts": "2026-04-24T10:00:00+00:00", "value": 100.0}
    from datetime import timedelta
    mock_get.assert_called_once_with("users", "row_count", window=timedelta(hours=24))


def test_metrics_custom_params(client):
    with patch("app.api.get_metrics", return_value=[]) as mock_get:
        resp = client.get("/api/metrics/orders?metric=null_rate&range=7d")

    assert resp.status_code == 200
    from datetime import timedelta
    mock_get.assert_called_once_with("orders", "null_rate", window=timedelta(days=7))


def test_metrics_invalid_metric(client):
    resp = client.get("/api/metrics/users?metric=bad_metric")
    assert resp.status_code == 400
    assert "error" in resp.get_json()


def test_metrics_invalid_range(client):
    resp = client.get("/api/metrics/users?range=999d")
    assert resp.status_code == 400
    assert "error" in resp.get_json()


# ---------------------------------------------------------------------------
# GET /api/schema/<table>
# ---------------------------------------------------------------------------

def test_schema_returns_columns(client):
    columns = [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "email", "type": "character varying", "nullable": True},
    ]
    with patch("app.api.table_schema", return_value=columns):
        resp = client.get("/api/schema/users")

    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 2
    assert data[0] == {"name": "id", "type": "integer", "nullable": False}
    assert data[1]["nullable"] is True


def test_schema_table_not_found(client):
    with patch("app.api.table_schema", return_value=[]):
        resp = client.get("/api/schema/nonexistent")

    assert resp.status_code == 404
    assert "error" in resp.get_json()
