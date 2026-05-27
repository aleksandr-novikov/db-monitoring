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
    # #53: third positional arg is the tenant project_id. In tests without
    # an authenticated session it falls back to "legacy".
    mock_get.assert_called_once_with("users", "row_count", "legacy", window=timedelta(hours=24))


def test_metrics_custom_params(client):
    with patch("app.api.get_metrics", return_value=[]) as mock_get:
        resp = client.get("/api/metrics/orders?metric=null_rate&range=7d")

    assert resp.status_code == 200
    from datetime import timedelta
    mock_get.assert_called_once_with("orders", "null_rate", "legacy", window=timedelta(days=7))


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


# ---------------------------------------------------------------------------
# GET /api/notifications  (#76)
# ---------------------------------------------------------------------------

@pytest.fixture
def notifications_storage(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    db_path = tmp_path / "metrics.db"
    monkeypatch.setattr(ms.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    yield ms
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)


def test_notifications_returns_paginated_envelope(client, notifications_storage):
    from app.metrics_storage import save_notification
    for i in range(3):
        save_notification(event_type="anomaly", message=f"m{i}",
                          status="sent", table_name="orders")

    resp = client.get("/api/notifications")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["total"] == 3
    assert body["limit"] == 50
    assert body["offset"] == 0
    assert len(body["items"]) == 3
    assert {it["message"] for it in body["items"]} == {"m0", "m1", "m2"}


def test_notifications_filters_by_event_type(client, notifications_storage):
    from app.metrics_storage import save_notification
    save_notification(event_type="anomaly", message="a", status="sent", table_name="orders")
    save_notification(event_type="schema_drift", message="s", status="sent", table_name="orders")

    resp = client.get("/api/notifications?event_type=anomaly")
    body = resp.get_json()
    assert body["total"] == 1
    assert body["items"][0]["event_type"] == "anomaly"


def test_notifications_filters_by_status_and_table(client, notifications_storage):
    from app.metrics_storage import save_notification
    save_notification(event_type="anomaly", message="ok", status="sent", table_name="orders")
    save_notification(event_type="anomaly", message="bad", status="failed",
                      table_name="users", error="boom")

    resp = client.get("/api/notifications?status=failed&table=users")
    body = resp.get_json()
    assert body["total"] == 1
    assert body["items"][0]["error"] == "boom"


def test_notifications_pagination(client, notifications_storage):
    from app.metrics_storage import save_notification
    for i in range(5):
        save_notification(event_type="anomaly", message=f"m{i}",
                          status="sent", table_name="orders")

    page1 = client.get("/api/notifications?limit=2&offset=0").get_json()
    page2 = client.get("/api/notifications?limit=2&offset=2").get_json()
    assert page1["total"] == 5 and page2["total"] == 5
    assert len(page1["items"]) == 2 and len(page2["items"]) == 2
    ids1 = {it["id"] for it in page1["items"]}
    ids2 = {it["id"] for it in page2["items"]}
    assert ids1.isdisjoint(ids2)


def test_notifications_invalid_event_type(client):
    resp = client.get("/api/notifications?event_type=garbage")
    assert resp.status_code == 400


def test_notifications_invalid_status(client):
    resp = client.get("/api/notifications?status=delivered")
    assert resp.status_code == 400


def test_notifications_invalid_limit(client):
    resp = client.get("/api/notifications?limit=0")
    assert resp.status_code == 400
    resp2 = client.get("/api/notifications?limit=9999")
    assert resp2.status_code == 400


# GET /api/notifications — tenant isolation (#137)
# ---------------------------------------------------------------------------

def test_notifications_api_scoped_to_active_project(notifications_storage):
    """Active project sees only its own notifications, not another project's."""
    from flask import g

    from app.app import create_app
    from app.metrics_storage import save_notification

    save_notification(event_type="anomaly", message="mine",
                      status="sent", table_name="t", project_id="proj-a")
    save_notification(event_type="anomaly", message="theirs",
                      status="sent", table_name="t", project_id="proj-b")

    scoped_app = create_app({"TESTING": True})

    @scoped_app.before_request
    def _inject_project():
        g.current_project = {"id": "proj-a"}

    with scoped_app.test_client() as c:
        body = c.get("/api/notifications").get_json()

    assert body["total"] == 1
    assert body["items"][0]["message"] == "mine"


def test_notifications_api_no_project_user_sees_empty(client, notifications_storage):
    """Authenticated user with no project gets an empty notifications list."""
    from unittest.mock import MagicMock, patch

    from app.metrics_storage import save_notification

    save_notification(event_type="anomaly", message="legacy msg",
                      status="sent", table_name="t", project_id="legacy")

    mock_user = MagicMock()
    mock_user.is_authenticated = True

    with patch("app.api.current_user", mock_user):
        body = client.get("/api/notifications").get_json()

    assert body["total"] == 0
    assert body["items"] == []
