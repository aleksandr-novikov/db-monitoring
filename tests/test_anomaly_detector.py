"""Tests for ml.anomaly_detector."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from app.app import create_app
from ml import anomaly_detector as ad


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _series(n: int, step_minutes: int = 15, slope: float = 10.0, base: float = 1000.0):
    """Return fake metrics rows for get_metrics mock."""
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "ts": (t0 + timedelta(minutes=i * step_minutes)).isoformat(timespec="seconds"),
            "value": base + slope * i,
            "tags": None,
        }
        for i in range(n)
    ]


def _series_with_spike(n: int, spike_start: int, spike_end: int, spike_value: float = 0.5):
    """null_rate series: near-zero with a clear spike in [spike_start, spike_end)."""
    t0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(n):
        v = spike_value if spike_start <= i < spike_end else 0.02
        rows.append({
            "ts": (t0 + timedelta(minutes=i * 15)).isoformat(timespec="seconds"),
            "value": v,
            "tags": None,
        })
    return rows


def _make_side_effect(rc_rows, nr_rows):
    """Return a side_effect for get_metrics that dispatches by metric_name."""
    def _side_effect(table, metric_name, window=None):
        if metric_name == "row_count":
            return rc_rows
        if metric_name == "null_rate":
            return nr_rows
        return []
    return _side_effect


# ---------------------------------------------------------------------------
# _load_features
# ---------------------------------------------------------------------------

def test_load_features_raises_with_single_point():
    # _load_features needs at least 2 aligned ticks to compute deltas.
    rc = _series(1)
    nr = _series(1)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        with pytest.raises(ad.InsufficientDataError):
            ad._load_features("t", window_days=7)


def test_load_features_returns_correct_shape():
    n = 50
    rc = _series(n)
    nr = _series(n, base=0.05, slope=0.0)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        timestamps, X = ad._load_features("t", window_days=7)
    assert len(timestamps) == n - 1  # first row dropped for delta
    assert X.shape == (n - 1, 4)     # row_count, null_rate, Δrow_count, Δnull_rate


# ---------------------------------------------------------------------------
# train
# ---------------------------------------------------------------------------

def test_train_raises_when_too_few_points_after_delta(tmp_path, monkeypatch):
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    # 150 rows → 149 after delta, below MIN_POINTS=200
    rc = _series(150)
    nr = _series(150, base=0.05, slope=0.0)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        with pytest.raises(ad.InsufficientDataError):
            ad.train("t")


def test_train_persists_model(tmp_path, monkeypatch):
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(ad, "MIN_POINTS", 10)  # lower threshold for test speed
    rc = _series(50)
    nr = _series(50, base=0.05, slope=0.0)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        meta = ad.train("t")
    assert (tmp_path / "t__anomaly.joblib").exists()
    assert meta["n_points"] == 49


def test_train_returns_metadata(tmp_path, monkeypatch):
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(ad, "MIN_POINTS", 10)
    rc = _series(50)
    nr = _series(50, base=0.02, slope=0.0)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        meta = ad.train("t")
    assert "trained_at" in meta
    assert meta["n_points"] > 0


# ---------------------------------------------------------------------------
# score_table
# ---------------------------------------------------------------------------

def test_score_table_returns_list_of_dicts(tmp_path, monkeypatch):
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(ad, "MIN_POINTS", 10)
    n = 60
    rc = _series(n)
    nr = _series(n, base=0.02, slope=0.0)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        ad.train("t")
        results = ad.score_table("t", window_days=14)
    assert len(results) == n - 1
    for r in results:
        assert "ts" in r
        assert "score" in r
        assert r["is_anomaly"] in (0, 1)


def test_score_table_detects_spike(tmp_path, monkeypatch):
    """A clear null_rate spike should produce at least one is_anomaly=1 point."""
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(ad, "MIN_POINTS", 10)
    n = 300
    spike_start, spike_end = 250, 270
    rc = _series(n)
    nr = _series_with_spike(n, spike_start=spike_start, spike_end=spike_end)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        ad.train("t")
        results = ad.score_table("t", window_days=60)
    anomalous = [r for r in results if r["is_anomaly"] == 1]
    assert len(anomalous) >= 1


def test_score_table_low_fpr_on_stable_data(tmp_path, monkeypatch):
    """On stable data, fewer than 5% of points should be flagged as anomalies."""
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(ad, "MIN_POINTS", 10)
    n = 300
    rc = _series(n, slope=10.0)
    nr = _series(n, base=0.02, slope=0.0)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        ad.train("t")
        results = ad.score_table("t", window_days=60)
    fpr = sum(1 for r in results if r["is_anomaly"] == 1) / len(results)
    assert fpr < 0.05


def test_score_table_trains_on_demand_if_no_model(tmp_path, monkeypatch):
    """score_table should train automatically when no persisted model exists."""
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(ad, "MIN_POINTS", 10)
    n = 50
    rc = _series(n)
    nr = _series(n, base=0.02, slope=0.0)
    with patch.object(ad, "get_metrics", side_effect=_make_side_effect(rc, nr)):
        results = ad.score_table("t", window_days=14)
    assert len(results) > 0


# ---------------------------------------------------------------------------
# retrain_all
# ---------------------------------------------------------------------------

def test_retrain_all_counts(tmp_path, monkeypatch):
    monkeypatch.setattr(ad, "MODELS_DIR", tmp_path)
    monkeypatch.setattr(ad, "MIN_POINTS", 10)
    tables = [{"table_name": "a"}, {"table_name": "b"}, {"table_name": "c"}]
    rc_long = _series(50)
    nr_long = _series(50, base=0.02, slope=0.0)
    rc_short = _series(5)
    nr_short = _series(5, base=0.02, slope=0.0)

    series_map = {
        "a": (rc_long, nr_long),
        "b": (rc_long, nr_long),
        "c": (rc_short, nr_short),  # too short → skipped
    }

    def _get(table, metric, window=None):
        rc, nr = series_map[table]
        return rc if metric == "row_count" else nr

    with patch.object(ad, "get_metrics", side_effect=_get), \
         patch("app.db.list_tables", return_value=tables):
        counts = ad.retrain_all()

    assert counts["trained"] == 2
    assert counts["skipped"] == 1
    assert counts["errors"] == 0


# ---------------------------------------------------------------------------
# API endpoint
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_anomalies_endpoint_returns_list(client):
    payload = [{"ts": "2026-05-01T00:00:00", "score": -0.1, "is_anomaly": 1}]
    with patch("app.api.get_anomaly_scores", return_value=payload):
        resp = client.get("/api/anomalies/orders?range=7d")
    assert resp.status_code == 200
    assert resp.get_json() == payload


def test_anomalies_endpoint_invalid_range(client):
    resp = client.get("/api/anomalies/orders?range=99d")
    assert resp.status_code == 400


def test_anomalies_endpoint_returns_empty_list_when_no_data(client):
    with patch("app.api.get_anomaly_scores", return_value=[]):
        resp = client.get("/api/anomalies/orders")
    assert resp.status_code == 200
    assert resp.get_json() == []
