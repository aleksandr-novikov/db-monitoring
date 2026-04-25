from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from app.app import create_app
from app.dashboard import status_class


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "monitor.db"
    import app.metrics_storage as storage
    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


def test_status_class_buckets():
    assert status_class(None) == "ok"
    assert status_class(0.0) == "ok"
    assert status_class(0.05) == "ok"
    assert status_class(0.10) == "warn"
    assert status_class(0.29) == "warn"
    assert status_class(0.30) == "crit"
    assert status_class(0.95) == "crit"


def test_root_redirects_to_dashboard(client):
    resp = client.get("/")
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/dashboard")


def test_overview_renders_kpis_and_table(client):
    fake_tables = [
        {"table_name": "users", "schema": "public"},
        {"table_name": "orders", "schema": "public"},
    ]
    fake_stats = {
        "users": {"table_name": "users", "schema": "public", "row_count": 1500, "size_bytes": 65536, "last_analyze": "2026-04-24 03:00:00"},
        "orders": {"table_name": "orders", "schema": "public", "row_count": 4200, "size_bytes": 262144, "last_analyze": None},
    }
    fake_cols = {
        "users": [{"column": "email", "null_count": 50, "null_rate": 0.05}],
        "orders": [{"column": "discount", "null_count": 1500, "null_rate": 0.36}],
    }
    with patch("app.dashboard.db.list_tables", return_value=fake_tables), \
         patch("app.dashboard.db.table_stats", side_effect=lambda name, schema=None: fake_stats[name]), \
         patch("app.dashboard.db.column_nulls", side_effect=lambda name, schema=None: fake_cols[name]):
        resp = client.get("/dashboard")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Обзор" in body
    assert "users" in body and "orders" in body
    assert "1 500" in body  # ru-style thousand separator
    # crit dot for orders' 36% null
    assert "bg-crit" in body
    # warn or ok for users' 5%
    assert "bg-ok" in body


def test_overview_handles_missing_stats(client):
    """table_stats returning None must not crash overview; the table is skipped."""
    with patch("app.dashboard.db.list_tables", return_value=[{"table_name": "ghost", "schema": "public"}]), \
         patch("app.dashboard.db.table_stats", return_value=None), \
         patch("app.dashboard.db.column_nulls", return_value=[]):
        resp = client.get("/dashboard")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "ghost" not in body
    assert "Нет таблиц для мониторинга" in body


def test_table_detail_renders(client):
    stats = {"table_name": "users", "schema": "public", "row_count": 1500, "size_bytes": 65536, "last_analyze": "2026-04-24"}
    cols = [
        {"column": "email", "data_type": "text", "null_count": 50, "null_rate": 0.05},
        {"column": "phone", "data_type": "text", "null_count": 600, "null_rate": 0.40},
    ]
    with patch("app.dashboard.db.table_stats", return_value=stats), \
         patch("app.dashboard.db.column_nulls", return_value=cols):
        resp = client.get("/dashboard/users")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "users" in body
    assert "email" in body and "phone" in body
    assert "Plotly" in body  # plotly cdn loaded


def test_schema_page_renders(client):
    fake_tables = [
        {"table_name": "users", "schema": "public"},
        {"table_name": "orders", "schema": "public"},
    ]
    fake_cols = {
        "users": [
            {"column": "id", "data_type": "uuid", "null_count": 0, "null_rate": 0.0},
            {"column": "email", "data_type": "text", "null_count": 248, "null_rate": 0.0496},
        ],
        "orders": [
            {"column": "amount", "data_type": "numeric", "null_count": 0, "null_rate": 0.0},
        ],
    }
    with patch("app.dashboard.db.list_tables", return_value=fake_tables), \
         patch("app.dashboard.db.column_nulls", side_effect=lambda name, schema=None: fake_cols[name]):
        resp = client.get("/dashboard/schema")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "users" in body and "orders" in body
    assert "uuid" in body and "text" in body and "numeric" in body
    assert "4.96%" in body or "5.0%" in body  # email null rate rendered


def test_table_detail_404_when_not_found(client):
    with patch("app.dashboard.db.table_stats", return_value=None):
        resp = client.get("/dashboard/nonexistent")
    assert resp.status_code == 404


def test_metrics_json_returns_history(client):
    import app.metrics_storage as storage
    now = datetime.now(timezone.utc)
    storage.save_metrics([
        {"ts": now - timedelta(hours=2), "table_name": "users", "metric_name": "row_count", "value": 100},
        {"ts": now - timedelta(hours=1), "table_name": "users", "metric_name": "row_count", "value": 110},
        {"ts": now, "table_name": "users", "metric_name": "null_rate", "value": 0.04},
    ])

    resp = client.get("/dashboard/users/metrics.json")
    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data["row_count"]) == 2
    assert len(data["null_rate"]) == 1
    assert data["row_count"][0]["value"] == 100.0


def test_metrics_json_empty_for_unknown(client):
    resp = client.get("/dashboard/nonexistent/metrics.json")
    assert resp.status_code == 200
    assert resp.get_json() == {"row_count": [], "null_rate": []}


def test_healthz_still_works(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}
