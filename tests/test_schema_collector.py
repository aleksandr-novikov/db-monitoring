from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from app.app import create_app
from app.metrics_storage import (
    get_schema_events,
    get_schema_snapshot,
    save_schema_events,
    save_schema_snapshot,
)
from collectors.schema_collector import (
    collect_table_schema,
    diff_schemas,
)


# ---------------------------------------------------------------------------
# diff_schemas
# ---------------------------------------------------------------------------

_BASE = [
    {"name": "id", "type": "uuid", "nullable": False},
    {"name": "email", "type": "text", "nullable": True},
    {"name": "age", "type": "integer", "nullable": True},
]


def test_no_changes_no_events():
    assert diff_schemas("users", _BASE, _BASE) == []


def test_first_observation_emits_no_events():
    # Treating before=None as "everything is new" would spam alerts; the
    # collector explicitly returns nothing on the first sighting.
    assert diff_schemas("users", None, _BASE) == []


def test_column_added():
    after = _BASE + [{"name": "country", "type": "text", "nullable": False}]
    events = diff_schemas("users", _BASE, after)
    assert len(events) == 1
    e = events[0]
    assert e["change_type"] == "column_added"
    assert e["column_name"] == "country"
    assert e["details"]["after"]["type"] == "text"


def test_column_removed():
    after = [c for c in _BASE if c["name"] != "age"]
    events = diff_schemas("users", _BASE, after)
    assert len(events) == 1
    assert events[0]["change_type"] == "column_removed"
    assert events[0]["column_name"] == "age"


def test_type_changed():
    after = [
        {**c, "type": "bigint"} if c["name"] == "age" else c
        for c in _BASE
    ]
    events = diff_schemas("users", _BASE, after)
    assert len(events) == 1
    e = events[0]
    assert e["change_type"] == "type_changed"
    assert e["column_name"] == "age"
    assert e["details"] == {"before": {"type": "integer"}, "after": {"type": "bigint"}}


def test_nullable_changed():
    after = [
        {**c, "nullable": False} if c["name"] == "email" else c
        for c in _BASE
    ]
    events = diff_schemas("users", _BASE, after)
    assert len(events) == 1
    assert events[0]["change_type"] == "nullable_changed"
    assert events[0]["column_name"] == "email"


def test_multiple_changes_at_once():
    after = [
        {"name": "id", "type": "uuid", "nullable": False},
        # email removed
        {"name": "age", "type": "bigint", "nullable": True},   # type changed
        {"name": "country", "type": "text", "nullable": False},  # added
    ]
    events = diff_schemas("users", _BASE, after)
    types = {e["change_type"] for e in events}
    assert types == {"column_added", "column_removed", "type_changed"}


# ---------------------------------------------------------------------------
# Storage round-trip + collector integration (real SQLite engine)
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_metrics(tmp_path, monkeypatch):
    from app.config import settings
    monkeypatch.setattr(settings, "MONITOR_DB_URL", f"sqlite:///{tmp_path/'m.db'}")
    import app.metrics_storage as ms
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    yield
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)


def test_snapshot_round_trip(clean_metrics):
    assert get_schema_snapshot("users") is None
    save_schema_snapshot("users", _BASE)
    assert get_schema_snapshot("users") == _BASE


def test_save_and_get_events(clean_metrics):
    save_schema_events([{
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "table_name": "orders",
        "change_type": "column_added",
        "column_name": "discount",
        "details": {"after": {"type": "numeric"}},
    }])
    events = get_schema_events("orders")
    assert len(events) == 1
    assert events[0]["change_type"] == "column_added"
    assert events[0]["details"]["after"]["type"] == "numeric"


def test_collect_table_schema_first_run_silent(clean_metrics):
    with patch("app.db.table_schema", return_value=_BASE):
        events = collect_table_schema("users")
    assert events == []
    # Snapshot is still saved so the next run has a baseline.
    assert get_schema_snapshot("users") == _BASE


def test_collect_table_schema_detects_change(clean_metrics):
    save_schema_snapshot("users", _BASE)
    after = _BASE + [{"name": "country", "type": "text", "nullable": False}]
    with patch("app.db.table_schema", return_value=after):
        events = collect_table_schema("users")
    assert len(events) == 1
    assert events[0]["change_type"] == "column_added"
    # Snapshot updated to the new shape.
    assert get_schema_snapshot("users") == after
    # Event persisted.
    assert len(get_schema_events("users")) == 1


def test_collect_table_schema_idempotent_on_no_change(clean_metrics):
    save_schema_snapshot("users", _BASE)
    with patch("app.db.table_schema", return_value=_BASE):
        events = collect_table_schema("users")
    assert events == []
    assert get_schema_events("users") == []


# ---------------------------------------------------------------------------
# /api/schema/<table>/changes
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_changes_endpoint_returns_events(client):
    payload = [{"ts": "2026-05-01T10:00:00+00:00", "table_name": "users",
                "change_type": "column_added", "column_name": "country",
                "details": {"after": {"type": "text"}}}]
    with patch("app.api.get_schema_events", return_value=payload):
        resp = client.get("/api/schema/users/changes")
    assert resp.status_code == 200
    assert resp.get_json() == payload


def test_changes_endpoint_rejects_invalid_range(client):
    resp = client.get("/api/schema/users/changes?range=999d")
    assert resp.status_code == 400
