from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from app.app import create_app
from app.metrics_storage import get_changepoints, save_changepoints
from ml import changepoint as cp_mod


def _series(values, start=None, step_minutes=15):
    # Anchor to now so timestamps always fall within the default 14-day query window.
    base = start or datetime.now(UTC) - timedelta(minutes=step_minutes * len(values))
    return [
        {"ts": (base + timedelta(minutes=step_minutes * i)).isoformat(timespec="seconds"),
         "value": v, "tags": None}
        for i, v in enumerate(values)
    ]


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


# ---------------------------------------------------------------------------
# Detection — synthetic spike (mirrors orders.null_rate seed scenario)
# ---------------------------------------------------------------------------

def test_detects_step_shift_in_synthetic_series():
    values = [0.02] * 30 + [0.20] * 30  # mean shift 0.02 → 0.20
    rows = _series(values)
    with patch.object(cp_mod, "get_metrics", return_value=rows):
        events = cp_mod.detect_changepoints("orders", "null_rate", window_days=14)
    assert events, "expected at least one change-point"
    e = events[0]
    assert e["score"] > cp_mod.MIN_SCORE
    assert e["value_before"] < 0.05
    assert e["value_after"] > 0.15


def test_no_changepoint_on_stable_series():
    # Constant-mean series with small Gaussian noise — what stable monitoring
    # data looks like. PELT should not flag any change-points here.
    import random
    random.seed(42)
    rows = _series([100.0 + random.gauss(0, 0.5) for _ in range(60)])
    with patch.object(cp_mod, "get_metrics", return_value=rows):
        events = cp_mod.detect_changepoints("users", "row_count")
    assert events == []


def test_too_few_points_returns_empty():
    rows = _series([1.0, 2.0, 3.0])
    with patch.object(cp_mod, "get_metrics", return_value=rows):
        assert cp_mod.detect_changepoints("t", "row_count") == []


def test_detect_all_persists_events(clean_metrics):
    spike = _series([0.02] * 30 + [0.20] * 30)
    flat = _series([100.0] * 60)

    def fake_get(table, metric, project_id=None, **_):
        return spike if (table, metric) == ("orders", "null_rate") else flat

    tables = [{"table_name": "orders"}, {"table_name": "users"}]
    with patch.object(cp_mod, "get_metrics", side_effect=fake_get), \
         patch("app.db.list_tables", return_value=tables):
        counts = cp_mod.detect_all()

    assert counts["detected"] >= 1
    saved = get_changepoints("orders", metric_name="null_rate")
    assert saved, "expected persisted change-point row for orders/null_rate"
    assert saved[0]["score"] > cp_mod.MIN_SCORE


# ---------------------------------------------------------------------------
# Storage upsert behaviour
# ---------------------------------------------------------------------------

def test_save_changepoints_upserts_on_repeat(clean_metrics):
    ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat(timespec="seconds")
    e = {
        "ts": ts,
        "table_name": "orders",
        "metric_name": "null_rate",
        "score": 5.0, "value_before": 0.02, "value_after": 0.20,
    }
    save_changepoints([e])
    save_changepoints([{**e, "score": 6.5}])  # same key, updated score
    rows = get_changepoints("orders")
    assert len(rows) == 1
    assert rows[0]["score"] == 6.5


def test_save_changepoints_deduplicates_cross_run_cluster(clean_metrics):
    # Simulates the hourly-scheduler bug: the same real shift is detected
    # across multiple runs with slightly different timestamps (±hours apart).
    # Only the highest-score record should survive.
    base = datetime.now(UTC) - timedelta(hours=10)
    def _e(offset_hours: float, score: float) -> dict:
        return {
            "ts": (base + timedelta(hours=offset_hours)).isoformat(timespec="seconds"),
            "table_name": "orders",
            "metric_name": "row_count",
            "score": score,
            "value_before": 1000.0,
            "value_after": 5000.0,  # all rising — same direction
        }

    save_changepoints([_e(0, 49.7)])   # run 1
    save_changepoints([_e(1, 50.7)])   # run 2 — better, replaces
    save_changepoints([_e(2, 51.7)])   # run 3 — better, replaces
    save_changepoints([_e(3, 7.0)])    # run 4 — worse, ignored

    rows = get_changepoints("orders")
    assert len(rows) == 1, f"expected 1 record, got {len(rows)}"
    assert rows[0]["score"] == 51.7


def test_save_changepoints_preserves_opposite_directions(clean_metrics):
    # A spike has two legitimate events: rise then fall. Both must be kept
    # even though they fall within the 72 h dedup window.
    base = datetime.now(UTC) - timedelta(hours=5)
    save_changepoints([{
        "ts": base.isoformat(timespec="seconds"),
        "table_name": "orders", "metric_name": "row_count",
        "score": 50.0, "value_before": 1000.0, "value_after": 5000.0,  # rise
    }])
    save_changepoints([{
        "ts": (base + timedelta(hours=2)).isoformat(timespec="seconds"),
        "table_name": "orders", "metric_name": "row_count",
        "score": 50.0, "value_before": 5000.0, "value_after": 1000.0,  # fall
    }])

    rows = get_changepoints("orders")
    assert len(rows) == 2, "rise and fall are separate events and must both be kept"


def test_changepoints_are_scoped_by_project(clean_metrics):
    ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat(timespec="seconds")
    e = {
        "ts": ts,
        "table_name": "orders",
        "metric_name": "null_rate",
        "score": 5.0,
        "value_before": 0.02,
        "value_after": 0.20,
    }

    save_changepoints([e], project_id="proj-a")
    save_changepoints([{**e, "score": 9.0}], project_id="proj-b")

    assert get_changepoints("orders", project_id="proj-a")[0]["score"] == 5.0
    assert get_changepoints("orders", project_id="proj-b")[0]["score"] == 9.0


def test_changepoint_dedupe_does_not_cross_project(clean_metrics):
    base = datetime.now(UTC) - timedelta(hours=10)

    def _e(offset_hours: float, score: float) -> dict:
        return {
            "ts": (base + timedelta(hours=offset_hours)).isoformat(timespec="seconds"),
            "table_name": "orders",
            "metric_name": "row_count",
            "score": score,
            "value_before": 1000.0,
            "value_after": 5000.0,
        }

    save_changepoints([_e(0, 20.0)], project_id="proj-a")
    save_changepoints([_e(1, 30.0)], project_id="proj-b")

    assert len(get_changepoints("orders", project_id="proj-a")) == 1
    assert len(get_changepoints("orders", project_id="proj-b")) == 1


# ---------------------------------------------------------------------------
# /api/changepoints/<table>
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_changepoints_endpoint_returns_payload(client):
    payload = [{"ts": "2026-04-25T14:00:00+00:00", "table_name": "orders",
                "metric_name": "null_rate", "score": 6.5,
                "value_before": 0.02, "value_after": 0.20}]
    with patch("app.api.get_changepoints", return_value=payload):
        resp = client.get("/api/changepoints/orders")
    assert resp.status_code == 200
    assert resp.get_json() == payload


def test_changepoints_endpoint_filters_by_metric(client):
    with patch("app.api.get_changepoints", return_value=[]) as mock_get:
        resp = client.get("/api/changepoints/orders?metric=row_count&range=7d")
    assert resp.status_code == 200
    args, kwargs = mock_get.call_args
    assert args[0] == "orders"
    assert kwargs["metric_name"] == "row_count"
    assert kwargs["window"] == timedelta(days=7)


def test_changepoints_endpoint_rejects_invalid_metric(client):
    resp = client.get("/api/changepoints/orders?metric=bogus")
    assert resp.status_code == 400


def test_changepoints_endpoint_rejects_invalid_range(client):
    resp = client.get("/api/changepoints/orders?range=999d")
    assert resp.status_code == 400
