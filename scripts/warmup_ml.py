"""
Прогрев ML-пайплайнов после `seed_metrics_db`.

Сидер бэкфилит метрики, но без этого скрипта дашборды:
  * не покажут change-point аннотации (таблица `changepoints` пустая);
  * не покажут anomaly-маркеры (модель не обучена → InsufficientDataError);
  * на каждый запрос forecast будут переобучать Prophet (медленно);
  * не покажут drift, потому что кеш `drift_reports` пуст.

Скрипт идемпотентен — можно перезапускать когда угодно.

Использование:
    python -m scripts.warmup_ml
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def warmup_changepoints() -> dict:
    """Прогон PELT/CUSUM по всем (таблица × метрика) и запись в changepoints."""
    from ml.changepoint import detect_all
    return detect_all()


def warmup_anomalies() -> dict:
    """Тренируем IsolationForest на каждой таблице и сразу скорим 14-дневное окно.

    Ценим обе вещи: persisted-модель, чтобы тики коллектора могли скорить
    дальше (`_score_recent_anomalies`), и таблицу `anomaly_scores`, чтобы
    дашборд показал маркеры сразу после сидинга.
    """
    from app.db import list_tables
    from app.metrics_storage import save_anomaly_scores
    from ml.anomaly_detector import (
        InsufficientDataError, retrain_all, score_table,
    )

    train_counts = retrain_all()

    scored = 0
    for t in list_tables():
        name = t["table_name"]
        try:
            scores = score_table(name, window_days=14)
        except InsufficientDataError:
            continue
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("Anomaly scoring failed for %s: %s", name, exc)
            continue
        if scores:
            save_anomaly_scores([{**s, "table_name": name} for s in scores])
            scored += len(scores)
    return {**train_counts, "scored": scored}


def warmup_forecasts() -> dict:
    """Тренируем Prophet/linear по row_count для всех таблиц, кладём в models/."""
    from ml.forecast import retrain_all
    return retrain_all()


def warmup_drift() -> dict:
    """Считаем PSI/KS на column_distribution и пишем в кеш drift_reports."""
    from ml.drift import compute_and_store_drift_all
    return compute_and_store_drift_all()


def main() -> dict:
    logging.basicConfig(level=logging.INFO)

    print("[1/4] change-point sweep...")
    cps = warmup_changepoints()
    print(f"       {cps}")

    print("[2/4] anomaly retrain + scoring...")
    an = warmup_anomalies()
    print(f"       {an}")

    print("[3/4] forecast retrain...")
    fc = warmup_forecasts()
    print(f"       {fc}")

    print("[4/4] drift cache refresh...")
    dr = warmup_drift()
    print(f"       {dr}")

    print("\nML warmup complete.")
    return {"changepoints": cps, "anomalies": an, "forecasts": fc, "drift": dr}


if __name__ == "__main__":
    main()
