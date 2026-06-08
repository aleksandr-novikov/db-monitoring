"""Multi-tenant ML retrain in scheduler (#fix-list_connections_with_dsn followup).

Before: `retrain_forecasts` hardcoded `project_id="legacy"` and
`retrain_anomaly_detectors` used no-arg (defaults to "legacy") AND
queried tables via `app.db.list_tables()` (global DATABASE_URL). Result:
nightly ML retrain never touched tenant projects; their forecast charts
forced sync retrain on every user request.

Now: scheduler iterates over `_iter_retrain_projects()` (legacy + every
project with metric rows) and uses `list_metric_tables(project_id)` to
scope tables per-tenant.
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

import pytest


@pytest.fixture
def storage(tmp_path, monkeypatch):
    db_path = tmp_path / "sched_ml.db"
    import app.metrics_storage as storage_mod

    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    storage_mod.get_engine()
    return storage_mod


def _seed_metrics_for_project(storage, project_id: str, table: str = "users"):
    from datetime import UTC, datetime, timedelta

    rows = []
    now = datetime.now(UTC)
    # Just enough rows so the project appears in list_project_ids_with_metrics.
    for h in range(4):
        rows.append({
            "ts": now - timedelta(hours=h),
            "table_name": table,
            "metric_name": "row_count",
            "value": 100 + h,
        })
    storage.save_metrics(rows, project_id)


# --- list_project_ids_with_metrics -----------------------------------------


def test_list_project_ids_with_metrics_returns_distinct_tenants(storage):
    """Helper that the scheduler uses to enumerate projects for retrain."""
    p1 = uuid.uuid4().hex
    p2 = uuid.uuid4().hex
    _seed_metrics_for_project(storage, p1)
    _seed_metrics_for_project(storage, p2)
    _seed_metrics_for_project(storage, p1, "orders")  # дубль не плодит p1

    ids = storage.list_project_ids_with_metrics()
    assert set(ids) == {p1, p2}


def test_list_project_ids_with_metrics_empty_when_no_metrics(storage):
    assert storage.list_project_ids_with_metrics() == []


# --- _iter_retrain_projects -----------------------------------------------


def test_iter_retrain_projects_always_includes_legacy(storage):
    """Even на пустой БД scheduler должен прокрутить legacy — иначе
    одно-тенантные деплои перестанут переобучать модели."""
    from collectors.scheduler import _iter_retrain_projects

    ids = _iter_retrain_projects()
    assert "legacy" in ids
    assert len(ids) == 1


def test_iter_retrain_projects_includes_tenants_plus_legacy(storage):
    from collectors.scheduler import _iter_retrain_projects

    p1 = uuid.uuid4().hex
    p2 = uuid.uuid4().hex
    _seed_metrics_for_project(storage, p1)
    _seed_metrics_for_project(storage, p2)

    ids = _iter_retrain_projects()
    assert set(ids) >= {"legacy", p1, p2}


# --- retrain_forecasts -----------------------------------------------------


def test_retrain_forecasts_iterates_per_project(storage):
    """Регрессионный: жёсткий project_id="legacy" в scheduler был багом —
    tenant projects не переобучались, ON-demand retrain бил по UX."""
    from collectors import scheduler

    p1 = uuid.uuid4().hex
    p2 = uuid.uuid4().hex
    _seed_metrics_for_project(storage, p1, "users")
    _seed_metrics_for_project(storage, p2, "orders")

    captured: list[dict] = []

    def _fake_retrain_all(*, project_id, tables=None, **_kw):
        captured.append({"project_id": project_id, "tables": tables})
        return {"trained": 1, "skipped": 0, "errors": 0}

    with patch("ml.forecast.retrain_all", side_effect=_fake_retrain_all):
        scheduler.retrain_forecasts()

    by_pid = {c["project_id"]: c for c in captured}
    # Tenant projects вызывались.
    assert p1 in by_pid
    assert p2 in by_pid
    assert by_pid[p1]["tables"] == ["users"]
    assert by_pid[p2]["tables"] == ["orders"]


def test_retrain_forecasts_skips_empty_tenant_but_keeps_legacy(storage):
    """Tenant без метрик → не вызываем retrain_all с пустым списком таблиц
    (no-op + дорогой коннект). Legacy всегда вызывается — `retrain_all`
    внутри может фоллбэкнуться на app.db.list_tables()."""
    from collectors import scheduler

    captured: list[dict] = []

    def _fake_retrain_all(*, project_id, tables=None, **_kw):
        captured.append({"project_id": project_id, "tables": tables})
        return {"trained": 0, "skipped": 0, "errors": 0}

    with patch("ml.forecast.retrain_all", side_effect=_fake_retrain_all):
        scheduler.retrain_forecasts()

    pids = [c["project_id"] for c in captured]
    assert pids == ["legacy"]


# --- retrain_anomaly_detectors --------------------------------------------


def test_retrain_anomaly_detectors_iterates_per_project_and_scopes_scoring(
    storage,
):
    """Anomaly retrain страдал той же болезнью: дефолтный
    project_id="legacy" + app.db.list_tables() для post-retrain scoring.
    Теперь — per-project."""
    from collectors import scheduler

    p1 = uuid.uuid4().hex
    _seed_metrics_for_project(storage, p1, "users")

    retrain_calls: list[dict] = []
    score_calls: list[dict] = []

    def _fake_retrain(*, project_id, tables=None, **_kw):
        retrain_calls.append({"project_id": project_id, "tables": tables})
        return {"trained": 1, "skipped": 0, "errors": 0}

    def _fake_score(name, window_days=14, project_id="legacy"):
        score_calls.append({"name": name, "project_id": project_id})
        return []  # пустой результат — save_anomaly_scores не вызовется

    with patch("ml.anomaly_detector.retrain_all", side_effect=_fake_retrain), \
         patch("ml.anomaly_detector.score_table", side_effect=_fake_score):
        scheduler.retrain_anomaly_detectors()

    retrain_pids = {c["project_id"] for c in retrain_calls}
    assert p1 in retrain_pids
    # Scoring тоже scoped к проекту.
    score_pids = {c["project_id"] for c in score_calls}
    assert p1 in score_pids
    # На tenant project_id используются именно его таблицы.
    p1_score_targets = {c["name"] for c in score_calls if c["project_id"] == p1}
    assert p1_score_targets == {"users"}
