from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet

from app import crypto
from app.app import _fmt_iso_in_text, create_app
from app.dashboard import status_class


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def client(tmp_path, monkeypatch):
    db_path = tmp_path / "monitor.db"
    import app.metrics_storage as storage
    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    app = create_app({"TESTING": True})
    return app.test_client()


def _latest_factory(values: dict):
    """Build a side_effect for get_latest_metric from {(table, metric): value or dict}.

    #53 added project_id as a third positional arg; the side_effect accepts
    it for signature compatibility but ignores it (tests don't care which
    tenant is queried — they're already scoped to a single in-memory DB).
    """
    def _side_effect(table_name, metric_name, project_id=None):
        v = values.get((table_name, metric_name))
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        return {"ts": "2026-04-29T10:00:00+00:00", "value": v, "tags": None}
    return _side_effect


def _login_with_project_connection(
    client,
    *,
    email: str = "tenant@example.com",
    dsn: str = "postgresql://u:p@tenant-db:5432/app",
    schema_name: str = "analytics",
) -> dict:
    client.post("/auth/register", data={
        "email": email,
        "password": "supersecret1",
        "confirm": "supersecret1",
    })

    from app.metrics_storage import (
        create_connection,
        get_user_by_email,
        list_projects_for_user,
    )

    user = get_user_by_email(email)
    project = list_projects_for_user(user["id"])[0]
    create_connection(
        connection_id=f"conn-{email}",
        project_id=project["id"],
        name="Tenant DB",
        dsn_encrypted=crypto.encrypt_dsn(dsn),
        schema_name=schema_name,
        interval_minutes=15,
        is_active=True,
    )
    return project


def test_status_class_buckets():
    assert status_class(None) == "ok"
    assert status_class(0.0) == "ok"
    assert status_class(0.05) == "ok"
    assert status_class(0.10) == "warn"
    assert status_class(0.29) == "warn"
    assert status_class(0.30) == "crit"
    assert status_class(0.95) == "crit"


def test_root_renders_landing_for_anonymous(client):
    # #55: / shows the public landing for anonymous visitors; authed users
    # get the dashboard redirect (covered in test_onboarding.py).
    resp = client.get("/")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "DB Monitor" in body
    assert "/auth/login" in body


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


class _FakeTenantAdapter:
    def __init__(self):
        self.list_schema = None
        self.schema_calls = []

    def list_tables(self, schema):
        self.list_schema = schema
        return [{"table_name": "tenant_orders", "schema": schema}]

    def table_schema(self, table_name, schema):
        self.schema_calls.append((table_name, schema))
        return [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "customer", "type": "text", "nullable": True},
        ]


class _FakeEngine:
    def __init__(self):
        self.disposed = False

    def dispose(self):
        self.disposed = True


def test_project_overview_uses_connection_dsn_not_global_database_url(client):
    _login_with_project_connection(client)
    adapter = _FakeTenantAdapter()
    engine = _FakeEngine()

    with patch(
        "app.dashboard.db.list_tables",
        side_effect=AssertionError("global DATABASE_URL list_tables leaked"),
    ) as global_list, \
         patch("app.dashboard.db.make_adapter_for_url", return_value=adapter) as make_adapter, \
         patch("app.dashboard.create_engine", return_value=engine), \
         patch("app.dashboard.get_latest_metric", return_value=None), \
         patch("app.dashboard._ml_last_runs", return_value={}):
        resp = client.get("/dashboard/")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "tenant_orders" in body
    assert adapter.list_schema == "analytics"
    assert engine.disposed is True
    global_list.assert_not_called()
    make_adapter.assert_called_once_with("postgresql://u:p@tenant-db:5432/app")


def test_project_table_detail_uses_connection_schema_not_global_database_url(client):
    _login_with_project_connection(client, email="detail@example.com")
    adapter = _FakeTenantAdapter()

    with patch(
        "app.dashboard.db.list_tables",
        side_effect=AssertionError("global DATABASE_URL list_tables leaked"),
    ) as global_list, \
         patch(
             "app.dashboard.db.table_schema",
             side_effect=AssertionError("global DATABASE_URL table_schema leaked"),
         ) as global_schema, \
         patch("app.dashboard.db.make_adapter_for_url", return_value=adapter), \
         patch("app.dashboard.create_engine", return_value=_FakeEngine()), \
         patch("app.dashboard.get_latest_metric", side_effect=_latest_factory({
             ("tenant_orders", "row_count"): 10,
         })), \
         patch("app.dashboard.get_latest_null_counts", return_value={"customer": 1}):
        resp = client.get("/dashboard/schema/tenant_orders")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "tenant_orders" in body
    assert "customer" in body
    assert adapter.schema_calls == [("tenant_orders", "analytics")]
    global_list.assert_not_called()
    global_schema.assert_not_called()


def test_project_overview_bad_connection_does_not_fallback_to_global_database_url(client):
    _login_with_project_connection(
        client,
        email="broken@example.com",
        dsn="unknown://u:p@broken-host/db",
    )

    with patch(
        "app.dashboard.db.list_tables",
        side_effect=AssertionError("global DATABASE_URL list_tables leaked"),
    ) as global_list, \
         patch("app.dashboard.get_latest_metric", return_value=None), \
         patch("app.dashboard._ml_last_runs", return_value={}):
        resp = client.get("/dashboard/")

    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Нет таблиц для мониторинга" in body
    assert "Нет подключений" not in body
    global_list.assert_not_called()


def test_healthz_still_works(client):
    resp = client.get("/healthz")
    # Structured payload (#100): 200 when deps reachable, 503 when any
    # are down. We only check the route exists and the response carries
    # the new shape. Full semantics live in tests/test_health.py.
    assert resp.status_code in (200, 503)
    body = resp.get_json()
    assert "status" in body
    assert "checks" in body


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


# --- Resilience: list_tables failure -----------------------------------------


def test_overview_survives_list_tables_exception(client):
    """overview() returns 200 even when db.list_tables() raises — no 500."""
    with patch("app.dashboard.db.list_tables", side_effect=Exception("DB unreachable")), \
         patch("app.dashboard.get_latest_metric", return_value=None):
        resp = client.get("/dashboard")
    assert resp.status_code == 200


def test_schema_view_survives_list_tables_exception(client):
    """schema_view() returns 200 even when db.list_tables() raises — no 500."""
    with patch("app.dashboard.db.list_tables", side_effect=Exception("DB unreachable")), \
         patch("app.dashboard.get_latest_metric", return_value=None):
        resp = client.get("/dashboard/schema")
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# #137 — no-project guards: authenticated user with zero projects
# ---------------------------------------------------------------------------


@pytest.fixture
def logged_in_no_project_client(tmp_path, monkeypatch):
    """Client logged in as a user who has NO projects (simulates deleted-project state)."""
    db_path = tmp_path / "monitor.db"
    import app.metrics_storage as storage
    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    app = create_app({"TESTING": True, "WTF_CSRF_ENABLED": False})
    with app.test_client() as c:
        # Register + immediately delete the auto-created default project so the
        # user is left authenticated but with zero projects.
        c.post("/auth/register", data={
            "email": "noproj@example.com",
            "password": "supersecret1",
            "confirm": "supersecret1",
        })
        from app.metrics_storage import (
            delete_project,
            get_user_by_email,
            list_projects_for_user,
        )
        user = get_user_by_email("noproj@example.com")
        for p in list_projects_for_user(user["id"]):
            delete_project(user["id"], p["id"])
        yield c


def test_overview_no_project_skips_list_tables(logged_in_no_project_client):
    """overview() must not call db.list_tables() when the user has no projects."""
    with patch("app.dashboard.db.list_tables") as mock_list, \
         patch("app.dashboard._ml_last_runs") as mock_ml:
        resp = logged_in_no_project_client.get("/dashboard")
    assert resp.status_code == 200
    mock_list.assert_not_called()
    mock_ml.assert_not_called()
    body = resp.get_data(as_text=True)
    assert "Создать проект" in body


def test_schema_no_project_skips_list_tables(logged_in_no_project_client):
    """schema_view() must not call db.list_tables() when user has no projects."""
    with patch("app.dashboard.db.list_tables") as mock_list:
        resp = logged_in_no_project_client.get("/dashboard/schema")
    assert resp.status_code == 200
    mock_list.assert_not_called()
    body = resp.get_data(as_text=True)
    assert "Создать проект" in body


def test_history_no_project_redirects_to_new_project(logged_in_no_project_client):
    """history_view() redirects to /projects/new when user has no projects."""
    resp = logged_in_no_project_client.get("/dashboard/history", follow_redirects=False)
    assert resp.status_code == 302
    assert "/projects/new" in resp.headers["Location"]


def test_notifications_no_project_shows_empty(logged_in_no_project_client):
    """notifications_view() shows empty list (not legacy data) when user has no projects."""
    from app.metrics_storage import save_notification
    # Write a notification with legacy project_id — must NOT appear.
    save_notification(event_type="anomaly", message="LEGACY_MARKER",
                      status="sent", project_id="legacy")

    resp = logged_in_no_project_client.get("/dashboard/notifications")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "LEGACY_MARKER" not in body
    assert "Нет уведомлений" in body


def test_delete_project_cascades_notifications(tmp_path, monkeypatch):
    """delete_project() removes the project's notifications but not other projects'."""
    import app.metrics_storage as storage
    db_path = tmp_path / "cascade.db"
    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    from app.metrics_storage import (
        create_project,
        create_user,
        delete_project,
        get_notifications,
        save_notification,
    )

    # Projects require an existing user row (FK on user_id).
    user = create_user("user-cascade-id", "user-cascade@example.com", "hash")
    proj_a = create_project("proj-a", user["id"], "A", "proj-a")
    proj_b = create_project("proj-b", user["id"], "B", "proj-b")

    save_notification(event_type="anomaly", message="A_MSG",
                      status="sent", project_id=proj_a["id"])
    save_notification(event_type="anomaly", message="B_MSG",
                      status="sent", project_id=proj_b["id"])

    delete_project(user["id"], proj_a["id"])

    remaining = get_notifications()
    messages = [n["message"] for n in remaining]
    assert "A_MSG" not in messages
    assert "B_MSG" in messages


def test_delete_project_cascades_metrics(tmp_path, monkeypatch):
    """delete_project() removes the project's metrics but not other projects'."""
    import app.metrics_storage as storage
    db_path = tmp_path / "cascade_metrics.db"
    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    from datetime import UTC, datetime

    from app.metrics_storage import (
        create_project,
        create_user,
        delete_project,
        get_metrics,
        save_metrics,
    )

    user = create_user("user-metrics-cascade", "metrics-cascade@example.com", "hash")
    proj_a = create_project("proj-metrics-a", user["id"], "A", "proj-metrics-a")
    proj_b = create_project("proj-metrics-b", user["id"], "B", "proj-metrics-b")

    ts = datetime.now(UTC)
    save_metrics([{"ts": ts, "table_name": "orders", "metric_name": "row_count", "value": 100}], proj_a["id"])
    save_metrics([{"ts": ts, "table_name": "orders", "metric_name": "row_count", "value": 200}], proj_b["id"])

    delete_project(user["id"], proj_a["id"])

    assert get_metrics("orders", "row_count", proj_a["id"]) == []
    assert len(get_metrics("orders", "row_count", proj_b["id"])) == 1
