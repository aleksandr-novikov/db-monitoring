"""Tests for scripts/demo_prepare.py (#176).

End-to-end на SQLite metrics store — без поднятия Postgres / Docker.
Покрывает acceptance:
- 14-day history backfilled for the project_id
- ML warmup populates anomaly_scores, changepoints, drift_reports
- verify_project returns non-zero counters
- forecast model files appear in models/<project>__*.joblib
"""

from __future__ import annotations

import uuid

import pytest

from app import crypto, metrics_storage
from scripts import demo_prepare


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    from cryptography.fernet import Fernet
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Fresh SQLite metrics + isolated MODELS_DIR per test."""
    import app.metrics_storage as ms
    from app.config import settings as cfg

    db_path = tmp_path / "demo.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)

    # Force scripts.demo_prepare's MODELS_DIR pointer to tmp so persisted
    # joblibs don't leak between tests / repo.
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    monkeypatch.setattr(demo_prepare, "MODELS_DIR", models_dir)

    # The actual writers live in ml.forecast and ml.anomaly_detector.
    import ml.anomaly_detector as anomaly_mod
    import ml.forecast as forecast_mod
    monkeypatch.setattr(forecast_mod, "MODELS_DIR", models_dir)
    monkeypatch.setattr(anomaly_mod, "MODELS_DIR", models_dir)

    return ms


def _make_project(slug: str = "retail-postgres") -> str:
    uid = uuid.uuid4().hex
    metrics_storage.create_user(
        user_id=uid, email=f"u-{uid[:6]}@x.io", password_hash="x",
    )
    project = metrics_storage.create_project(
        project_id=uuid.uuid4().hex, user_id=uid, name="P", slug=slug,
    )
    return project["id"]


# ── _resolve_project_ids ───────────────────────────────────────────────────


def test_resolve_project_ids_returns_mapping(storage):
    pid = _make_project("retail-postgres")
    out = demo_prepare._resolve_project_ids(("retail-postgres",))
    assert out == {"retail-postgres": pid}


def test_resolve_project_ids_missing_slug_omitted(storage):
    _make_project("retail-postgres")
    out = demo_prepare._resolve_project_ids(("retail-postgres", "ghost"))
    assert "retail-postgres" in out
    assert "ghost" not in out


# ── verify_project on empty + populated DB ─────────────────────────────────


def test_verify_project_zeros_on_empty(storage):
    pid = _make_project()
    v = demo_prepare.verify_project(pid)
    assert v == {
        "metrics": 0,
        "anomaly_scores": 0,
        "changepoints": 0,
        "drift_reports": 0,
        "notifications": 0,
        "forecast_models": 0,
    }


def test_verify_project_counts_metrics_and_models(storage, tmp_path):
    """Insert synthetic rows directly + drop a fake joblib → counts non-zero."""
    from datetime import UTC, datetime

    pid = _make_project()

    # Insert one metric row directly through the storage API.
    metrics_storage.save_metrics([{
        "ts": datetime.now(UTC),
        "table_name": "users",
        "metric_name": "row_count",
        "value": 100.0,
    }], pid)

    # Touch a fake forecast model file matching the naming convention
    # used by ml/forecast.py::_model_path.
    safe = pid.replace("/", "_").replace(" ", "_")
    (demo_prepare.MODELS_DIR / f"{safe}__users__row_count.joblib").touch()
    # And one anomaly file.
    (demo_prepare.MODELS_DIR / f"{safe}__users__anomaly.joblib").touch()

    v = demo_prepare.verify_project(pid)
    assert v["metrics"] == 1
    assert v["forecast_models"] == 2


def test_verify_project_isolated_across_tenants(storage):
    """Project A's counts must not bleed into project B's verify_project."""
    from datetime import UTC, datetime

    pid_a = _make_project("retail-postgres")
    pid_b = _make_project("other-project")
    metrics_storage.save_metrics([{
        "ts": datetime.now(UTC), "table_name": "users",
        "metric_name": "row_count", "value": 1.0,
    }], pid_a)
    assert demo_prepare.verify_project(pid_a)["metrics"] == 1
    assert demo_prepare.verify_project(pid_b)["metrics"] == 0


# ── prepare_one (orchestrator → seed + warmup) ─────────────────────────────


def test_prepare_one_calls_seed_then_warmup(storage, monkeypatch):
    """End-to-end stub: prepare_one(pid) → seed_metrics_db.main + warmup_ml.main
    called in order with the right project_id."""
    pid = _make_project()
    calls = []

    def fake_seed(days, interval_minutes, reset, project_id):
        calls.append(("seed", project_id, days))
        return {"snapshots": 1, "rows": 100}

    def fake_warmup(project_id="legacy"):
        calls.append(("warmup", project_id))
        return {"forecasts": {}, "anomalies": {}, "changepoints": {}, "drift": {}}

    from scripts import seed_metrics_db, warmup_ml
    monkeypatch.setattr(seed_metrics_db, "main", fake_seed)
    monkeypatch.setattr(warmup_ml, "main", fake_warmup)

    demo_prepare.prepare_one(pid, days=14, interval_minutes=60)

    assert calls == [("seed", pid, 14), ("warmup", pid)]


# ── main() orchestration ───────────────────────────────────────────────────


def test_main_runs_per_slug_and_returns_verify_per_slug(storage, monkeypatch):
    """main() — orchestrates all configured slugs and returns the
    verification dict keyed by slug. Stubs out the heavy ML lifting so
    the test stays fast and DB-only."""
    pid_postgres = _make_project("retail-postgres")
    pid_ch = _make_project("events-clickhouse")

    monkeypatch.setattr(
        "scripts.seed_metrics_db.main",
        lambda **kw: {"rows": 0},
    )
    monkeypatch.setattr(
        "scripts.warmup_ml.main",
        lambda project_id="legacy": {},
    )

    out = demo_prepare.main(
        slugs=("retail-postgres", "events-clickhouse"),
        days=1,
        interval_minutes=60,
        skip_workspace=True,
    )

    assert set(out.keys()) == {"retail-postgres", "events-clickhouse"}
    assert all("metrics" in v for v in out.values())
    # Both project_ids resolved successfully — no SystemExit on missing slug.
    _ = pid_postgres, pid_ch


def test_main_exits_when_slug_missing(storage, monkeypatch):
    """If a requested slug doesn't exist in DB, exit(2). The caller must
    run seed_demo_workspace before demo_prepare for new slugs."""
    monkeypatch.setattr(
        "scripts.seed_metrics_db.main",
        lambda **kw: {"rows": 0},
    )
    monkeypatch.setattr(
        "scripts.warmup_ml.main",
        lambda project_id="legacy": {},
    )

    with pytest.raises(SystemExit) as exc_info:
        demo_prepare.main(
            slugs=("never-seeded",),
            skip_workspace=True,
        )
    assert exc_info.value.code == 2
