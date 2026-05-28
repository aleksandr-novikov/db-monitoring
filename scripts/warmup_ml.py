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

import argparse
import logging

logger = logging.getLogger(__name__)


def _tables_from_metrics(project_id: str) -> list[str]:
    from sqlalchemy import text

    from app.metrics_storage import get_engine

    with get_engine().connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT table_name
            FROM metrics
            WHERE project_id = :project_id
            ORDER BY table_name
        """), {"project_id": project_id}).fetchall()
    return [r[0] for r in rows]


def warmup_changepoints(project_id: str = "legacy", tables: list[str] | None = None) -> dict:
    """Прогон PELT/CUSUM по всем (таблица × метрика) и запись в changepoints."""
    from ml.changepoint import detect_all
    return detect_all(project_id=project_id, tables=tables)


def warmup_anomalies(project_id: str = "legacy", tables: list[str] | None = None) -> dict:
    """Тренируем IsolationForest на каждой таблице и сразу скорим 14-дневное окно.

    Ценим обе вещи: persisted-модель, чтобы тики коллектора могли скорить
    дальше (`_score_recent_anomalies`), и таблицу `anomaly_scores`, чтобы
    дашборд показал маркеры сразу после сидинга.
    """
    from app.metrics_storage import save_anomaly_scores
    from ml.anomaly_detector import (
        InsufficientDataError,
        retrain_all,
        score_table,
    )

    if tables is None:
        from app.db import list_tables

        table_names = [t["table_name"] for t in list_tables()]
    else:
        table_names = tables

    train_counts = retrain_all(project_id=project_id, tables=table_names)
    scored = 0
    for name in table_names:
        try:
            scores = score_table(name, window_days=14, project_id=project_id)
        except InsufficientDataError:
            continue
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning("Anomaly scoring failed for %s: %s", name, exc)
            continue
        if scores:
            save_anomaly_scores(
                [{**s, "table_name": name} for s in scores],
                project_id=project_id,
            )
            scored += len(scores)
    return {**train_counts, "scored": scored}


def warmup_forecasts() -> dict:
    """Тренируем Prophet/linear по row_count для всех таблиц, кладём в models/."""
    from ml.forecast import retrain_all
    return retrain_all()


def warmup_drift(project_id: str = "legacy", tables: list[str] | None = None) -> dict:
    """Считаем PSI/KS на column_distribution и пишем в кеш drift_reports."""
    from ml.drift import compute_and_store_drift_all
    return compute_and_store_drift_all(project_id=project_id, tables=tables)


def main(project_id: str = "legacy") -> dict:
    logging.basicConfig(level=logging.INFO)
    tables = _tables_from_metrics(project_id)
    table_scope = tables or (None if project_id == "legacy" else [])

    print("[1/4] change-point sweep...")
    cps = warmup_changepoints(project_id=project_id, tables=table_scope)
    print(f"       {cps}")

    print("[2/4] anomaly retrain + scoring...")
    an = warmup_anomalies(project_id=project_id, tables=table_scope)
    print(f"       {an}")

    print("[3/4] forecast retrain...")
    fc = warmup_forecasts()
    print(f"       {fc}")

    print("[4/4] drift cache refresh...")
    dr = warmup_drift(project_id=project_id, tables=table_scope)
    print(f"       {dr}")

    print("\nML warmup complete.")
    return {"changepoints": cps, "anomalies": an, "forecasts": fc, "drift": dr}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-id", default="legacy")
    args = parser.parse_args()
    main(project_id=args.project_id)
