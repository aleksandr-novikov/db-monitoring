from unittest.mock import patch

import pytest

import scripts.warmup_ml as warmup


@pytest.fixture
def stub_models(monkeypatch):
    """Заглушаем тяжёлые ML-модули — тестируем оркестрацию, не их потроха."""
    calls: dict[str, int] = {
        "detect_all": 0,
        "anomaly_retrain": 0,
        "score_table": 0,
        "save_anomaly_scores": 0,
        "forecast_retrain": 0,
        "drift_store": 0,
    }

    def _track(key, ret):
        def _wrapped(*args, **kwargs):
            calls[key] += 1
            return ret
        return _wrapped

    monkeypatch.setattr(
        "ml.changepoint.detect_all",
        _track("detect_all", {"detected": 3, "tables": 4, "errors": 0}),
    )
    monkeypatch.setattr(
        "ml.anomaly_detector.retrain_all",
        _track("anomaly_retrain", {"trained": 4, "skipped": 0, "errors": 0}),
    )
    monkeypatch.setattr(
        "ml.forecast.retrain_all",
        _track("forecast_retrain", {"trained": 4, "skipped": 0, "errors": 0}),
    )
    monkeypatch.setattr(
        "ml.drift.compute_and_store_drift_all",
        _track("drift_store", {"tables": 4, "rows": 12}),
    )
    monkeypatch.setattr(
        "app.db.list_tables",
        lambda schema=None: [
            {"table_name": "users", "schema": "public"},
            {"table_name": "events", "schema": "public"},
        ],
    )
    monkeypatch.setattr(
        "ml.anomaly_detector.score_table",
        _track("score_table", [{"ts": "2026-05-10T00:00:00+00:00",
                                 "score": -0.1, "is_anomaly": 1}]),
    )
    monkeypatch.setattr(
        "app.metrics_storage.save_anomaly_scores",
        _track("save_anomaly_scores", 1),
    )
    return calls


def test_warmup_changepoints_runs_detect_all(stub_models):
    result = warmup.warmup_changepoints()
    assert stub_models["detect_all"] == 1
    assert result == {"detected": 3, "tables": 4, "errors": 0}


def test_warmup_forecasts_runs_retrain_all(stub_models):
    result = warmup.warmup_forecasts()
    assert stub_models["forecast_retrain"] == 1
    assert result["trained"] == 4


def test_warmup_drift_calls_compute_and_store(stub_models):
    result = warmup.warmup_drift()
    assert stub_models["drift_store"] == 1
    assert result == {"tables": 4, "rows": 12}


def test_warmup_anomalies_trains_then_scores_each_table(stub_models):
    result = warmup.warmup_anomalies()
    assert stub_models["anomaly_retrain"] == 1
    # 2 таблицы × 1 score → save один раз на таблицу.
    assert stub_models["score_table"] == 2
    assert stub_models["save_anomaly_scores"] == 2
    assert result["scored"] == 2  # каждый score_table вернул 1 точку
    assert result["trained"] == 4


def test_warmup_anomalies_skips_table_on_insufficient_data(monkeypatch, stub_models):
    from ml.anomaly_detector import InsufficientDataError

    def _raise(*args, **kwargs):
        stub_models["score_table"] += 1
        raise InsufficientDataError("nope")

    monkeypatch.setattr("ml.anomaly_detector.score_table", _raise)

    result = warmup.warmup_anomalies()
    # save_anomaly_scores ни разу не вызывается, scored=0.
    assert stub_models["save_anomaly_scores"] == 0
    assert result["scored"] == 0


def test_main_runs_all_four_stages(stub_models, capsys):
    out = warmup.main()
    assert set(out) == {"changepoints", "anomalies", "forecasts", "drift"}
    assert stub_models["detect_all"] == 1
    assert stub_models["anomaly_retrain"] == 1
    assert stub_models["forecast_retrain"] == 1
    assert stub_models["drift_store"] == 1

    captured = capsys.readouterr()
    assert "[1/4]" in captured.out
    assert "[4/4]" in captured.out
    assert "ML warmup complete" in captured.out
