import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from sqlalchemy import text

from app.app import create_app
from app.metrics_storage import get_engine
from ml import drift as drift_mod
from ml.drift import (
    compute_drift,
    ks_two_sample,
    psi,
)

# ---------------------------------------------------------------------------
# PSI
# ---------------------------------------------------------------------------

def test_psi_zero_for_identical_distributions():
    p = {"a": 0.5, "b": 0.3, "c": 0.2}
    assert psi(p, p) < 1e-9


def test_psi_low_for_minor_shift():
    base = {"a": 0.5, "b": 0.3, "c": 0.2}
    cur = {"a": 0.48, "b": 0.32, "c": 0.20}
    assert psi(base, cur) < 0.1


def test_psi_above_critical_for_major_shift():
    base = {"a": 0.7, "b": 0.2, "c": 0.1}
    cur = {"a": 0.2, "b": 0.3, "c": 0.5}
    assert psi(base, cur) > drift_mod.PSI_CRITICAL


def test_psi_handles_new_categories_via_smoothing():
    base = {"a": 1.0}
    cur = {"a": 0.5, "b": 0.5}  # brand-new bucket "b"
    val = psi(base, cur)
    assert val > 0
    assert math_isfinite(val)


# ---------------------------------------------------------------------------
# KS
# ---------------------------------------------------------------------------

def test_ks_zero_distance_for_identical_samples():
    pairs = [(1.0, 100), (2.0, 200), (3.0, 50)]
    d, p = ks_two_sample(pairs, pairs)
    assert d == 0.0
    assert p == 1.0


def test_ks_detects_shifted_numeric_distribution():
    base = [(1.0, 500), (2.0, 500)]
    cur = [(10.0, 500), (20.0, 500)]
    d, p = ks_two_sample(base, cur)
    assert d == 1.0
    assert p < 0.05


def test_ks_empty_inputs_return_no_evidence():
    d, p = ks_two_sample([], [(1.0, 10)])
    assert d == 0.0 and p == 1.0


# ---------------------------------------------------------------------------
# compute_drift integration (via the real metrics SQLite engine)
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_metrics(tmp_path, monkeypatch):
    # Settings is loaded once at import; patch the live value instead of the
    # env var so a fresh sqlite file backs each test.
    from app.config import settings
    monkeypatch.setattr(settings, "MONITOR_DB_URL", f"sqlite:///{tmp_path/'m.db'}")
    import app.metrics_storage as ms
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    yield
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)


def _seed(table: str, column: str, dtype: str, ts: datetime, buckets: list[dict]):
    tags = json.dumps({"column": column, "data_type": dtype, "buckets": buckets})
    total = float(sum(b["count"] for b in buckets))
    with get_engine().begin() as conn:
        conn.execute(
            text("""INSERT INTO metrics (ts, table_name, metric_name, value, tags)
                    VALUES (:ts, :t, 'column_distribution', :v, :tags)"""),
            {"ts": ts.isoformat(timespec="seconds"), "t": table, "v": total, "tags": tags},
        )


def test_compute_drift_flags_shifted_column(clean_metrics):
    now = datetime.now(UTC)
    _seed("orders", "source", "varchar", now - timedelta(days=5),
          [{"value": "ads", "count": 700}, {"value": "organic", "count": 300}])
    _seed("orders", "source", "varchar", now,
          [{"value": "ads", "count": 200}, {"value": "organic", "count": 800}])
    report = compute_drift("orders")
    assert len(report) == 1
    row = report[0]
    assert row["column"] == "source"
    assert row["is_drift"] is True
    assert row["severity"] == "critical"
    assert row["psi"] > drift_mod.PSI_CRITICAL


def test_compute_drift_clean_for_stable_column(clean_metrics):
    now = datetime.now(UTC)
    buckets = [{"value": "ads", "count": 700}, {"value": "organic", "count": 300}]
    _seed("orders", "source", "varchar", now - timedelta(days=5), buckets)
    _seed("orders", "source", "varchar", now, buckets)
    report = compute_drift("orders")
    assert report[0]["severity"] == "ok"
    assert report[0]["is_drift"] is False
    assert report[0]["psi"] < 0.1


def test_compute_and_store_drift_all_writes_cache(clean_metrics, monkeypatch):
    from app.metrics_storage import get_drift_report
    from ml.drift import compute_and_store_drift_all

    now = datetime.now(UTC)
    _seed("orders", "source", "varchar", now - timedelta(days=5),
          [{"value": "ads", "count": 700}, {"value": "organic", "count": 300}])
    _seed("orders", "source", "varchar", now,
          [{"value": "ads", "count": 200}, {"value": "organic", "count": 800}])

    monkeypatch.setattr(
        "app.db.list_tables",
        lambda schema=None: [{"table_name": "orders", "schema": "public"}],
    )

    counts = compute_and_store_drift_all()
    assert counts == {"tables": 1, "rows": 1}

    cached = get_drift_report("orders")
    assert len(cached) == 1
    assert cached[0]["column"] == "source"
    assert cached[0]["severity"] == "critical"
    assert cached[0]["is_drift"] is True


def test_compute_and_store_drift_all_empty_table_clears_cache(clean_metrics, monkeypatch):
    """Перезапуск при пустых снапшотах должен зачищать предыдущий кеш."""
    from app.metrics_storage import get_drift_report, save_drift_reports
    from ml.drift import compute_and_store_drift_all

    save_drift_reports("orders", [{
        "column": "source", "data_type": "varchar", "psi": 0.5,
        "ks_pvalue": None, "is_drift": True, "severity": "critical",
    }])
    assert len(get_drift_report("orders")) == 1

    monkeypatch.setattr(
        "app.db.list_tables",
        lambda schema=None: [{"table_name": "orders", "schema": "public"}],
    )
    compute_and_store_drift_all()
    assert get_drift_report("orders") == []


def test_compute_drift_marks_insufficient_data(clean_metrics):
    now = datetime.now(UTC)
    _seed("orders", "source", "varchar", now,
          [{"value": "ads", "count": 100}])
    report = compute_drift("orders")
    assert report[0]["severity"] == "insufficient_data"
    assert report[0]["is_drift"] is False


# ---------------------------------------------------------------------------
# /api/drift/<table>
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


def test_drift_endpoint_returns_payload(client):
    payload = [{"column": "source", "data_type": "varchar", "psi": 0.3,
                "ks_pvalue": None, "is_drift": True, "severity": "critical"}]
    # Эндпоинт читает уже посчитанный кеш — патчим storage, а не compute_drift.
    with patch("app.metrics_storage.get_drift_report", return_value=payload):
        resp = client.get("/api/drift/orders")
    assert resp.status_code == 200
    assert resp.get_json() == payload


def test_drift_endpoint_empty_when_no_snapshots(client):
    with patch("app.metrics_storage.get_drift_report", return_value=[]):
        resp = client.get("/api/drift/orders")
    assert resp.status_code == 200
    assert resp.get_json() == []


def math_isfinite(x: float) -> bool:
    import math
    return math.isfinite(x)
