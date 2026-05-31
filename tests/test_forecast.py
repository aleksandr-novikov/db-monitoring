from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from app.app import create_app
from ml import forecast as fc_mod


def _series(n: int, step_hours: int = 1, slope: float = 10.0, start: float = 100.0):
    base = datetime(2026, 4, 1, tzinfo=UTC)
    return [
        {"ts": (base + timedelta(hours=i * step_hours)).isoformat(timespec="seconds"),
         "value": start + slope * i,
         "tags": None}
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# ml.forecast core
# ---------------------------------------------------------------------------

def test_forecast_uses_linear_fallback_for_short_history(tmp_path, monkeypatch):
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(fc_mod, "_HAS_PROPHET", False)
    rows = _series(10, step_hours=1, slope=5.0, start=0.0)
    with patch.object(fc_mod, "get_metrics", return_value=rows), \
         patch.object(fc_mod, "get_changepoints", return_value=[]):
        out = fc_mod.forecast("t", "row_count", horizon_days=1)
    assert len(out) == 24
    for p in out:
        assert "ts" in p and "yhat" in p and "yhat_lower" in p and "yhat_upper" in p
        assert p["yhat_lower"] <= p["yhat"] <= p["yhat_upper"]
    # linear with slope=5/h should keep extrapolating upward
    assert out[-1]["yhat"] > out[0]["yhat"]


def test_forecast_raises_when_too_few_points(tmp_path, monkeypatch):
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)
    with patch.object(fc_mod, "get_metrics", return_value=_series(1)), \
         patch.object(fc_mod, "get_changepoints", return_value=[]):
        with pytest.raises(fc_mod.InsufficientDataError):
            fc_mod.forecast("t", "row_count")


def test_retrain_all_skips_empty_tables(tmp_path, monkeypatch):
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(fc_mod, "_HAS_PROPHET", False)
    tables = [{"table_name": "a"}, {"table_name": "b"}]
    series_map = {"a": _series(5), "b": _series(1)}
    with patch.object(fc_mod, "get_metrics", side_effect=lambda t, m, p=None, **_: series_map[t]), \
         patch.object(fc_mod, "get_changepoints", return_value=[]), \
         patch("app.db.list_tables", return_value=tables):
        counts = fc_mod.retrain_all()
    assert counts["trained"] == 1
    assert counts["skipped"] == 1


# ---------------------------------------------------------------------------
# /api/forecast/<table>
# ---------------------------------------------------------------------------

@pytest.fixture
def client(monkeypatch):
    # /api/forecast is gated by FF_FORECAST (#104). Forecast tests
    # exercise the endpoint, so opt in here once for the whole fixture
    # rather than annotating every case.
    monkeypatch.setenv("FF_FORECAST", "1")
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_forecast_endpoint_returns_points(client):
    payload = [{"ts": "2026-05-01T00:00:00+00:00", "yhat": 1.0, "yhat_lower": 0.5, "yhat_upper": 1.5}]
    with patch("ml.forecast.forecast", return_value=payload):
        resp = client.get("/api/forecast/users?metric=row_count&horizon=7d")
    assert resp.status_code == 200
    assert resp.get_json() == payload


def test_forecast_endpoint_invalid_metric(client):
    resp = client.get("/api/forecast/users?metric=null_rate")
    assert resp.status_code == 400


def test_forecast_endpoint_invalid_horizon(client):
    resp = client.get("/api/forecast/users?horizon=99d")
    assert resp.status_code == 400


def test_forecast_endpoint_insufficient_data(client):
    with patch("ml.forecast.forecast", side_effect=fc_mod.InsufficientDataError("nope")):
        resp = client.get("/api/forecast/users")
    assert resp.status_code == 422
    assert resp.get_json()["error"] == "insufficient_data"


# ---------------------------------------------------------------------------
# Changepoint-aware training and cache invalidation
# ---------------------------------------------------------------------------

def test_train_uses_post_changepoint_window_when_cp_is_old(tmp_path, monkeypatch):
    """If the last changepoint is older than MIN_PROPHET_DAYS, train() should
    load only post-cp data — the new regime has had time to stabilise and the
    forecast should reflect it, not the pre-cp baseline."""
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(fc_mod, "_HAS_PROPHET", False)

    now = datetime.now(UTC).replace(microsecond=0)
    cp_dt = now - timedelta(days=10)  # well past MIN_PROPHET_DAYS=7
    fake_cp = [{"ts": cp_dt.isoformat(), "metric_name": "row_count", "score": 10.0,
                "value_before": 100.0, "value_after": 200.0}]

    post_cp_rows = [
        {"ts": (cp_dt + timedelta(hours=i + 1)).isoformat(timespec="seconds"),
         "value": 200.0 + i, "tags": None}
        for i in range(20)
    ]

    captured_windows = []

    def fake_get_metrics(table, metric, project_id, window):
        captured_windows.append(window)
        return post_cp_rows

    with patch.object(fc_mod, "get_changepoints", return_value=fake_cp), \
         patch.object(fc_mod, "get_metrics", side_effect=fake_get_metrics):
        fc_mod.train("t", "row_count")

    # Single load with a window shorter than the 60-day default.
    assert len(captured_windows) == 1
    assert captured_windows[0] < timedelta(days=60)


def test_train_uses_full_history_when_cp_is_recent(tmp_path, monkeypatch):
    """Recent changepoint case — fitting on post-cp alone would leave a tiny
    flat plateau and the forecast would lose the longer-term growth context.
    train() must fall back to the full window so the user-visible trend
    survives. This is the regression that produced a flat 10K → 10K forecast
    on orders right after step3, and previously a runaway 5K → 25K forecast
    on users when the post-cp window straddled the step itself."""
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(fc_mod, "_HAS_PROPHET", False)

    now = datetime.now(UTC).replace(microsecond=0)
    cp_dt = now - timedelta(hours=10)  # well below MIN_PROPHET_DAYS=7
    fake_cp = [{"ts": cp_dt.isoformat(), "metric_name": "row_count", "score": 10.0,
                "value_before": 3000.0, "value_after": 5000.0}]

    pre = [
        {"ts": (cp_dt - timedelta(days=14) + timedelta(hours=i)).isoformat(timespec="seconds"),
         "value": 1000.0 + 200 * i / 14.0, "tags": None}
        for i in range(14 * 24)
    ]
    post = [
        {"ts": (cp_dt + timedelta(hours=i + 1)).isoformat(timespec="seconds"),
         "value": 5000.0, "tags": None} for i in range(10)
    ]
    rows = pre + post

    captured_windows = []

    def fake_get_metrics(table, metric, project_id, window):
        captured_windows.append(window)
        return rows

    with patch.object(fc_mod, "get_changepoints", return_value=fake_cp), \
         patch.object(fc_mod, "get_metrics", side_effect=fake_get_metrics):
        fc_mod.train("t", "row_count")

    # Single load, default 60-day window — full history, no post-cp filter.
    assert len(captured_windows) == 1
    assert captured_windows[0] == timedelta(days=60)


def test_forecast_invalidates_cache_on_new_changepoint(tmp_path, monkeypatch):
    """forecast() must retrain when a new changepoint appears since last train."""
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(fc_mod, "_HAS_PROPHET", False)

    rows = _series(10, step_hours=1, slope=1.0, start=100.0)

    # Persist a model that was trained with no changepoint
    old_model = fc_mod._fit_linear([(fc_mod._parse_ts(r["ts"]), float(r["value"])) for r in rows])
    import joblib as _jl
    _jl.dump({
        "kind": "linear",
        "model": old_model,
        "last_ts": rows[-1]["ts"],
        "last_changepoint_ts": None,
        "trained_at": "2026-04-01T00:00:00",
    }, fc_mod._model_path("t", "row_count"))

    # Now a changepoint has appeared
    new_cp = [{"ts": rows[5]["ts"], "metric_name": "row_count", "score": 5.0,
               "value_before": 100.0, "value_after": 150.0}]

    train_calls = []

    original_train = fc_mod.train
    def spy_train(table, metric="row_count", project_id="legacy"):
        train_calls.append((table, metric))
        return original_train(table, metric, project_id)

    with patch.object(fc_mod, "get_metrics", return_value=rows), \
         patch.object(fc_mod, "get_changepoints", return_value=new_cp), \
         patch.object(fc_mod, "train", side_effect=spy_train):
        fc_mod.forecast("t", "row_count", horizon_days=1)

    assert len(train_calls) == 1, "forecast() must retrain when changepoint is new"


def test_model_path_isolated_per_project(tmp_path, monkeypatch):
    """Two projects with the same table must not share a model file."""
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)

    path_a = fc_mod._model_path("orders", "row_count", project_id="project-a")
    path_b = fc_mod._model_path("orders", "row_count", project_id="project-b")
    path_legacy = fc_mod._model_path("orders", "row_count", project_id="legacy")

    assert path_a != path_b, "different projects must get different model paths"
    assert path_a != path_legacy, "non-legacy project must not share path with legacy"
    assert "project-a" in path_a.name
    assert "project-b" in path_b.name
    assert "project" not in path_legacy.name  # legacy keeps old filename format


def test_train_writes_to_project_scoped_path(tmp_path, monkeypatch):
    """train() with a real project_id must write to the project-scoped file."""
    monkeypatch.setattr(fc_mod, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(fc_mod, "_HAS_PROPHET", False)
    rows = _series(10, step_hours=1, slope=5.0, start=100.0)

    with patch.object(fc_mod, "get_metrics", return_value=rows), \
         patch.object(fc_mod, "get_changepoints", return_value=[]):
        fc_mod.train("orders", "row_count", project_id="tenant-x")

    expected = fc_mod._model_path("orders", "row_count", project_id="tenant-x")
    legacy = fc_mod._model_path("orders", "row_count", project_id="legacy")
    assert expected.exists(), "project-scoped model file must be created"
    assert not legacy.exists(), "legacy model file must NOT be created for non-legacy project"
