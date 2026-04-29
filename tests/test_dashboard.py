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


def _latest_factory(values: dict):
    """Build a side_effect for get_latest_metric from {(table, metric): value or dict}."""
    def _side_effect(table_name, metric_name):
        v = values.get((table_name, metric_name))
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        return {"ts": "2026-04-29T10:00:00+00:00", "value": v, "tags": None}
    return _side_effect


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


def test_overview_renders_kpis_and_table_from_storage(client):
    """Overview reads the latest stored metrics — never live-scans the monitored DB."""
    fake_tables = [
        {"table_name": "users", "schema": "public"},
        {"table_name": "orders", "schema": "public"},
    ]
    metrics = {
        ("users", "row_count"): 1500,
        ("users", "null_rate"): 0.05,
        ("users", "size_bytes"): 65536,
        ("orders", "row_count"): 4200,
        ("orders", "null_rate"): 0.36,
        ("orders", "size_bytes"): 262144,
    }
    with patch("app.dashboard.db.list_tables", return_value=fake_tables), \
         patch("app.dashboard.get_latest_metric", side_effect=_latest_factory(metrics)), \
         patch("app.dashboard.db.column_nulls") as mock_col_nulls, \
         patch("app.dashboard.db.table_stats") as mock_stats:
        resp = client.get("/dashboard")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Обзор" in body
    assert "users" in body and "orders" in body
    assert "1 500" in body  # ru-style thousand separator
    # crit dot for orders' 36% null
    assert "bg-crit" in body
    # ok dot for users' 5%
    assert "bg-ok" in body
    # CRITICAL: dashboard must NOT live-scan the monitored DB
    mock_col_nulls.assert_not_called()
    mock_stats.assert_not_called()


def test_overview_handles_no_collected_metrics(client):
    """Tables with no stored metrics still render — values show as em-dash placeholders."""
    with patch("app.dashboard.db.list_tables", return_value=[{"table_name": "fresh", "schema": "public"}]), \
         patch("app.dashboard.get_latest_metric", return_value=None):
        resp = client.get("/dashboard")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "fresh" in body
    # Em-dash placeholders for missing metrics
    assert "—" in body


def test_overview_empty_when_no_tables(client):
    with patch("app.dashboard.db.list_tables", return_value=[]), \
         patch("app.dashboard.get_latest_metric", return_value=None):
        resp = client.get("/dashboard")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Нет таблиц для мониторинга" in body


def test_table_detail_renders_from_storage_and_schema(client):
    """Detail page uses stored metrics + cheap info_schema, never column_nulls()."""
    fake_tables = [{"table_name": "users", "schema": "public"}]
    metrics = {
        ("users", "row_count"): 1500,
        ("users", "null_rate"): 0.05,
        ("users", "size_bytes"): 65536,
    }
    cols = [
        {"name": "id", "type": "uuid", "nullable": False},
        {"name": "email", "type": "text", "nullable": True},
    ]
    null_counts = {"id": 0, "email": 75}  # 75/1500 = 5%
    with patch("app.dashboard.db.list_tables", return_value=fake_tables), \
         patch("app.dashboard.get_latest_metric", side_effect=_latest_factory(metrics)), \
         patch("app.dashboard.get_latest_null_counts", return_value=null_counts), \
         patch("app.dashboard.db.table_schema", return_value=cols), \
         patch("app.dashboard.db.column_nulls") as mock_col_nulls:
        resp = client.get("/dashboard/users")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "users" in body
    assert "id" in body and "email" in body
    assert "uuid" in body and "text" in body
    assert "75 NULL" in body  # per-column null count from storage
    assert "5.0%" in body  # 75/1500 rendered
    assert "Plotly" in body  # plotly cdn loaded
    mock_col_nulls.assert_not_called()


def test_table_detail_404_when_not_listed(client):
    with patch("app.dashboard.db.list_tables", return_value=[{"table_name": "users", "schema": "public"}]):
        resp = client.get("/dashboard/nonexistent")
    assert resp.status_code == 404


def test_schema_page_renders_from_information_schema(client):
    """Schema page uses cheap information_schema — no full-table scans."""
    fake_tables = [
        {"table_name": "users", "schema": "public"},
        {"table_name": "orders", "schema": "public"},
    ]
    schemas = {
        "users": [
            {"name": "id", "type": "uuid", "nullable": False},
            {"name": "email", "type": "text", "nullable": True},
        ],
        "orders": [
            {"name": "amount", "type": "numeric", "nullable": False},
        ],
    }
    with patch("app.dashboard.db.list_tables", return_value=fake_tables), \
         patch("app.dashboard.db.table_schema", side_effect=lambda name, schema=None: schemas[name]), \
         patch("app.dashboard.get_latest_metric", return_value=None), \
         patch("app.dashboard.get_latest_null_counts", return_value={}), \
         patch("app.dashboard.db.column_nulls") as mock_col_nulls:
        resp = client.get("/dashboard/schema")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "users" in body and "orders" in body
    assert "uuid" in body and "text" in body and "numeric" in body
    mock_col_nulls.assert_not_called()


def test_healthz_still_works(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}
