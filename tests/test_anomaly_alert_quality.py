"""Anomaly alert quality gates (#171, supersedes #180).

Two layers под испытанием:
1. ``_passes_alert_quality_gate`` — отсекает borderline anomalies до
   того как они дойдут до telegram.notify_anomaly:
   - |score| < MIN_SCORE_MAGNITUDE → drop
   - |Δ / baseline_median| < MIN_DELTA_RATIO → drop
2. ``_build_prompt`` — содержит исторический контекст и явную инструкцию
   LLM не оспаривать факт аномалии (alignment с detector).
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from cryptography.fernet import Fernet

from app import crypto


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.config import settings as cfg

    db_path = tmp_path / "quality.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    return ms


def _new_project(storage) -> str:
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email=f"u-{uid[:6]}@x.io", password_hash="x")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=uid, name="P", slug=f"p-{uid[:6]}",
    )
    return project["id"]


# ── _passes_alert_quality_gate ─────────────────────────────────────────────


def test_gate_drops_borderline_score(storage):
    """Score=-0.003 (стабильный baseline) → не уведомляем, даже если
    IsolationForest пометил точку как anomaly."""
    from collectors.per_project import _passes_alert_quality_gate

    pid = _new_project(storage)
    anomaly = {
        "ts": datetime.now(UTC).isoformat(timespec="seconds"),
        "score": -0.003,
        "is_anomaly": 1,
    }
    assert _passes_alert_quality_gate("users", anomaly, pid) is False


def test_gate_passes_strong_score_no_baseline(storage):
    """Без baseline (свежая таблица) и при сильном score → пропускаем."""
    from collectors.per_project import _passes_alert_quality_gate

    pid = _new_project(storage)
    anomaly = {
        "ts": datetime.now(UTC).isoformat(timespec="seconds"),
        "score": -0.25,
        "is_anomaly": 1,
    }
    # Нет metrics в БД → baseline=None → gate пропускает.
    assert _passes_alert_quality_gate("users", anomaly, pid) is True


def test_gate_drops_small_delta(storage):
    """Score сильный, но текущее значение в пределах 5% от 7-day median
    → drop (delta_ratio < 10%)."""
    from collectors.per_project import _passes_alert_quality_gate

    pid = _new_project(storage)

    # Засеять 7 точек со значениями около 100 (median=100).
    now = datetime.now(UTC)
    rows = [
        {"ts": now - timedelta(hours=h), "table_name": "users",
         "metric_name": "row_count", "value": 100.0 + (h % 5)}
        for h in range(1, 8)
    ]
    storage.save_metrics(rows, pid)

    # Аномальная точка с value=103 (3% от baseline).
    anomaly_ts = now.isoformat(timespec="seconds")
    storage.save_metrics([{
        "ts": now, "table_name": "users",
        "metric_name": "row_count", "value": 103.0,
    }], pid)

    anomaly = {"ts": anomaly_ts, "score": -0.40, "is_anomaly": 1}
    assert _passes_alert_quality_gate("users", anomaly, pid) is False


def test_gate_passes_large_delta(storage):
    """Score сильный + delta 50% → пропускаем (реальный инцидент)."""
    from collectors.per_project import _passes_alert_quality_gate

    pid = _new_project(storage)

    now = datetime.now(UTC)
    rows = [
        {"ts": now - timedelta(hours=h), "table_name": "users",
         "metric_name": "row_count", "value": 100.0}
        for h in range(1, 8)
    ]
    storage.save_metrics(rows, pid)

    anomaly_ts = now.isoformat(timespec="seconds")
    storage.save_metrics([{
        "ts": now, "table_name": "users",
        "metric_name": "row_count", "value": 150.0,
    }], pid)

    anomaly = {"ts": anomaly_ts, "score": -0.20, "is_anomaly": 1}
    assert _passes_alert_quality_gate("users", anomaly, pid) is True


def test_gate_respects_settings_overrides(storage, monkeypatch):
    """Поднимаем порог через settings — borderline score проходит."""
    from app.config import settings
    from collectors.per_project import _passes_alert_quality_gate

    monkeypatch.setattr(settings, "ANOMALY_NOTIFY_MIN_SCORE_MAGNITUDE", 0.001)
    monkeypatch.setattr(settings, "ANOMALY_NOTIFY_MIN_DELTA_RATIO", 0.0)

    pid = _new_project(storage)
    anomaly = {
        "ts": datetime.now(UTC).isoformat(timespec="seconds"),
        "score": -0.003,
        "is_anomaly": 1,
    }
    assert _passes_alert_quality_gate("users", anomaly, pid) is True


# ── _baseline_median_row_count ─────────────────────────────────────────────


def test_baseline_returns_none_for_sparse_history(storage):
    """< 3 точек → None (защита от шумной median)."""
    from collectors.per_project import _baseline_median_row_count

    pid = _new_project(storage)
    storage.save_metrics([
        {"ts": datetime.now(UTC), "table_name": "users",
         "metric_name": "row_count", "value": 100.0},
        {"ts": datetime.now(UTC) - timedelta(hours=1), "table_name": "users",
         "metric_name": "row_count", "value": 110.0},
    ], pid)
    assert _baseline_median_row_count("users", pid) is None


def test_baseline_computes_median(storage):
    from collectors.per_project import _baseline_median_row_count

    pid = _new_project(storage)
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now - timedelta(hours=h), "table_name": "orders",
         "metric_name": "row_count", "value": v}
        for h, v in zip(range(1, 8), [10, 20, 30, 40, 50, 60, 70])
    ], pid)
    # 7 значений, median = 40
    assert _baseline_median_row_count("orders", pid) == 40.0


# ── End-to-end: _maybe_notify_anomalies respects the gate ──────────────────


def test_notify_skipped_when_gate_drops_anomaly(storage, monkeypatch):
    """Borderline anomaly (score=-0.001) с настроенным Telegram → notify
    НЕ вызывается. Проверяет что gate сидит на правильном слое и не
    позволяет ML-флюктуации добраться до пользователя."""
    from collectors import per_project

    pid = _new_project(storage)
    storage.save_project_notifications(
        pid,
        telegram_bot_token=crypto.encrypt_token("0000000001:" + "A" * 35),
        telegram_chat_id="42",
        throttle_minutes=15,
    )

    captures = []
    monkeypatch.setattr(
        "app.notifications.telegram.notify_anomaly",
        lambda *a, **kw: captures.append((a, kw)),
    )
    monkeypatch.setattr(
        "ml.anomaly_detector.score_table",
        lambda table, window_days=14, project_id="legacy": [
            {"ts": datetime.now(UTC).isoformat(timespec="seconds"),
             "score": -0.001, "is_anomaly": 1}
        ],
    )

    per_project._maybe_notify_anomalies(pid, ["users"])
    assert captures == []


def test_notify_fires_when_gate_passes(storage, monkeypatch):
    from collectors import per_project

    pid = _new_project(storage)
    storage.save_project_notifications(
        pid,
        telegram_bot_token=crypto.encrypt_token("0000000001:" + "A" * 35),
        telegram_chat_id="42",
        throttle_minutes=15,
    )

    captures = []
    monkeypatch.setattr(
        "app.notifications.telegram.notify_anomaly",
        lambda *a, **kw: captures.append(a),
    )
    # Сильный score, baseline отсутствует → gate пропускает.
    monkeypatch.setattr(
        "ml.anomaly_detector.score_table",
        lambda table, window_days=14, project_id="legacy": [
            {"ts": datetime.now(UTC).isoformat(timespec="seconds"),
             "score": -0.30, "is_anomaly": 1}
        ],
    )

    per_project._maybe_notify_anomalies(pid, ["users"])
    assert len(captures) == 1


# ── LLM prompt alignment (#171 пункт 3) ────────────────────────────────────


def test_prompt_contains_detector_alignment_instruction(storage):
    """Промпт явно велит LLM не оспаривать факт аномалии — устраняет
    'данные стабильны, аномалии нет' внутри alert об аномалии."""
    from app.llm import _build_prompt

    pid = _new_project(storage)
    # Положим минимум данных чтобы prompt сгенерился.
    storage.save_metrics([{
        "ts": datetime.now(UTC), "table_name": "users",
        "metric_name": "row_count", "value": 100.0,
    }], pid)

    prompt = _build_prompt("users", "row_count",
                            datetime.now(UTC).isoformat(timespec="seconds"),
                            project_id=pid)
    assert "CONFIRMED this point is an anomaly" in prompt
    assert "do NOT write" in prompt or "do not write" in prompt.lower()


def test_prompt_contains_historical_block_when_data_available(storage):
    """С достаточной историей промпт включает median/min/max сводку."""
    from app.llm import _build_prompt

    pid = _new_project(storage)
    now = datetime.now(UTC)
    storage.save_metrics([
        {"ts": now - timedelta(minutes=m), "table_name": "users",
         "metric_name": "row_count", "value": 100.0 + m}
        for m in range(0, 60, 10)  # 6 точек
    ], pid)

    prompt = _build_prompt("users", "row_count",
                            now.isoformat(timespec="seconds"),
                            project_id=pid)
    assert "Historical context" in prompt
    assert "median" in prompt
    # Должна быть x-multiple — самое читаемое для LLM.
    assert "x of median" in prompt


def test_prompt_skips_historical_block_when_sparse(storage):
    """1 точка — мало для статистики, блок пропускается чтобы не дать
    LLM шумные числа («median 100 на 1 наблюдении»)."""
    from app.llm import _build_prompt

    pid = _new_project(storage)
    now = datetime.now(UTC)
    storage.save_metrics([{
        "ts": now, "table_name": "users",
        "metric_name": "row_count", "value": 100.0,
    }], pid)

    prompt = _build_prompt("users", "row_count",
                            now.isoformat(timespec="seconds"),
                            project_id=pid)
    assert "Historical context" not in prompt
