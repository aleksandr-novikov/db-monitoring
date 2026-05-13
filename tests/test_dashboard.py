from unittest.mock import patch

import pytest

from app.app import _fmt_iso_in_text, create_app
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
        resp = client.get("/dashboard/schema/users")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "users" in body
    assert "id" in body and "email" in body
    assert "uuid" in body and "text" in body
    # Detail page uses stored null_count (75) → derives null_rate (75/1500 = 5%)
    # and renders the rate. Live column_nulls() is intentionally not called.
    assert "5.0%" in body
    assert "Plotly" in body  # plotly cdn loaded
    mock_col_nulls.assert_not_called()


# ---------------------------------------------------------------------------
# /dashboard/notifications  (#76)
# ---------------------------------------------------------------------------

def test_notifications_page_empty(client):
    resp = client.get("/dashboard/notifications")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Уведомления" in body
    assert "Нет уведомлений" in body


def test_notifications_page_lists_records(client):
    from app.metrics_storage import save_notification
    save_notification(event_type="anomaly", message="орёл взлетел",
                      status="sent", table_name="orders", metric_name="row_count")
    save_notification(event_type="schema_drift", message="колонка добавлена",
                      status="failed", table_name="users", error="boom")

    resp = client.get("/dashboard/notifications")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "орёл взлетел" in body
    assert "колонка добавлена" in body
    assert "Доставлено" in body
    assert "Ошибка" in body


def test_notifications_page_event_type_filter(client):
    from app.metrics_storage import save_notification
    save_notification(event_type="anomaly", message="ANOMALY_MARKER_ZZZ",
                      status="sent", table_name="orders")
    save_notification(event_type="schema_drift", message="SCHEMA_MARKER_QQQ",
                      status="sent", table_name="orders")

    resp = client.get("/dashboard/notifications?event_type=anomaly")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "ANOMALY_MARKER_ZZZ" in body
    assert "SCHEMA_MARKER_QQQ" not in body


def test_notifications_page_in_sidebar(client):
    """Sidebar (rendered from base.html) should expose the new tab on every page."""
    resp = client.get("/dashboard/notifications")
    body = resp.get_data(as_text=True)
    assert "/dashboard/notifications" in body
    assert "Уведомления" in body


def test_table_detail_404_when_not_listed(client):
    with patch("app.dashboard.db.list_tables", return_value=[{"table_name": "users", "schema": "public"}]):
        resp = client.get("/dashboard/schema/nonexistent")
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


# ---------------------------------------------------------------------------
# _fmt_iso_in_text Jinja2 filter (#89)
# ---------------------------------------------------------------------------

def test_fmt_iso_in_text_single_timestamp():
    result = _fmt_iso_in_text("аномалия в 2026-05-12T09:21:00+00:00 обнаружена")
    assert "2026-05-12 09:21 UTC" in result
    assert "T09:21:00+00:00" not in result


def test_fmt_iso_in_text_multiple_timestamps():
    text = "с 2026-05-12T09:21:00+00:00 до 2026-05-12T10:21:00+00:00"
    result = _fmt_iso_in_text(text)
    assert "2026-05-12 09:21 UTC" in result
    assert "2026-05-12 10:21 UTC" in result
    assert "+00:00" not in result


def test_fmt_iso_in_text_z_suffix():
    result = _fmt_iso_in_text("время: 2026-05-12T09:21:00Z")
    assert "2026-05-12 09:21 UTC" in result
    assert "T09:21:00Z" not in result


def test_fmt_iso_in_text_with_milliseconds():
    result = _fmt_iso_in_text("ts: 2026-05-12T20:26:40.856465+00:00")
    assert "2026-05-12 20:26 UTC" in result
    assert ".856465" not in result


def test_fmt_iso_in_text_no_timestamps_passthrough():
    text = "обычный текст без временных меток"
    assert _fmt_iso_in_text(text) == text


def test_fmt_iso_in_text_empty_string():
    assert _fmt_iso_in_text("") == ""


# ---------------------------------------------------------------------------
# Overview: last_check rendered as 'YYYY-MM-DD HH:MM UTC' (#89)
# ---------------------------------------------------------------------------

def test_overview_last_check_formatted(client):
    fake_tables = [{"table_name": "users", "schema": "public"}]
    metrics = {
        ("users", "row_count"): 1000,
        ("users", "null_rate"): 0.01,
        ("users", "size_bytes"): 32768,
    }
    with patch("app.dashboard.db.list_tables", return_value=fake_tables), \
         patch("app.dashboard.get_latest_metric", side_effect=_latest_factory(metrics)):
        resp = client.get("/dashboard")

    body = resp.get_data(as_text=True)
    assert "2026-04-29 10:00 UTC" in body
    assert "2026-04-29T10:00:00+00:00" not in body


# ---------------------------------------------------------------------------
# Notifications: ISO timestamps in message body replaced on render (#89)
# ---------------------------------------------------------------------------

def test_notifications_iso_timestamps_in_body_are_formatted(client):
    from app.metrics_storage import save_notification
    raw_msg = (
        "Изменение row_count с 3145 до 4995 "
        "(с 2026-05-12T09:21:00+00:00 до 2026-05-12T10:21:00+00:00)"
    )
    save_notification(event_type="anomaly", message=raw_msg,
                      status="sent", table_name="users")

    resp = client.get("/dashboard/notifications")
    body = resp.get_data(as_text=True)
    assert "2026-05-12 09:21 UTC" in body
    assert "2026-05-12 10:21 UTC" in body
    assert "T09:21:00+00:00" not in body
    assert "T10:21:00+00:00" not in body
