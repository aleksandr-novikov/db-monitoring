"""Demo 3.2 — backfill 14-day history + ML warmup для demo-проектов (#176).

Один проход, который оставляет дашборд готовым к показу:

  scripts.seed_demo_workspace  →  пользователи / проекты / коннекты
                ↓
  scripts.seed_metrics_db      →  14 дней синтетической истории, помеченной project_id
                ↓
  scripts.warmup_ml            →  Prophet / IsolationForest / PELT / drift по project_id
                ↓
  verification                 →  COUNT(*) по metrics / anomaly_scores / changepoints /
                                  drift_reports / forecast-моделей

После прогона:
- table_detail показывает 14-дневный график
- forecast endpoint отдаёт точки
- anomaly KPI на overview не пустой (есть подготовленный incident)
- /dashboard/history полон notifications
- ml_last_runs не пустой

Usage:
    python -m scripts.demo_prepare
    python -m scripts.demo_prepare --slug retail-postgres
    python -m scripts.demo_prepare --skip-workspace  # workspace уже создан
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from sqlalchemy import text

from app.metrics_storage import get_engine

logger = logging.getLogger(__name__)

# Слаги demo-проектов из seed_demo_workspace. Порядок = порядок прогона.
# По умолчанию готовим только Postgres-проект — seed_metrics_db.py читает
# схему из глобального DATABASE_URL (см. _capture_snapshots), поэтому для
# CH/Iceberg нужна отдельная инфра. Postgres-проекта достаточно чтобы
# выполнить весь acceptance из #176; CH/Iceberg — bonus, делается через
# реальные тики коллектора после `make seed-clickhouse` etc.
DEFAULT_SLUGS = ("retail-postgres",)
MODELS_DIR = Path(__file__).resolve().parent.parent / "models"


def _resolve_project_ids(slugs: tuple[str, ...]) -> dict[str, str]:
    """Map slug → project_id, опираясь на (user_id, slug) UNIQUE.

    Не вызываем get_project_by_slug т.к. он требует user_id; в БД slug
    уникален в пределах юзера, но для demo-проектов слаги тоже уникальны
    глобально (seed_demo_workspace зашивает их жёстко). Делаем один
    SELECT и матчим по slug — этого достаточно.
    """
    placeholders = ", ".join(f":s{i}" for i in range(len(slugs)))
    params = {f"s{i}": slug for i, slug in enumerate(slugs)}
    with get_engine().connect() as conn:
        rows = conn.execute(
            text(f"SELECT id, slug FROM projects WHERE slug IN ({placeholders})"),
            params,
        ).fetchall()
    return {row[1]: row[0] for row in rows}


def _count(query: str, **params) -> int:
    with get_engine().connect() as conn:
        return int(conn.execute(text(query), params).scalar() or 0)


def verify_project(project_id: str) -> dict:
    """Простой COUNT(*) per artefact для подтверждения что warmup отработал.

    Возвращает dict с полями для readable-печати + acceptance-asserts в тестах.
    """
    metrics = _count(
        "SELECT COUNT(*) FROM metrics WHERE project_id = :pid",
        pid=project_id,
    )
    anomalies = _count(
        "SELECT COUNT(*) FROM anomaly_scores WHERE project_id = :pid",
        pid=project_id,
    )
    changepoints = _count(
        "SELECT COUNT(*) FROM changepoints WHERE project_id = :pid",
        pid=project_id,
    )
    drift = _count(
        "SELECT COUNT(*) FROM drift_reports WHERE project_id = :pid",
        pid=project_id,
    )
    notifications = _count(
        "SELECT COUNT(*) FROM notifications WHERE project_id = :pid",
        pid=project_id,
    )
    # ML модели лежат плоско в models/ с именами вида
    # ``<safe_project_id>__<table>__<metric>.joblib`` (forecast) и
    # ``<safe_project_id>__<table>__anomaly.joblib`` (IsolationForest) —
    # см. ml/forecast.py::_model_path и ml/anomaly_detector.py::_model_path.
    safe = project_id.replace("/", "_").replace(" ", "_")
    forecast_models = (
        sum(1 for _ in MODELS_DIR.glob(f"{safe}__*.joblib"))
        if MODELS_DIR.exists() else 0
    )
    return {
        "metrics": metrics,
        "anomaly_scores": anomalies,
        "changepoints": changepoints,
        "drift_reports": drift,
        "notifications": notifications,
        "forecast_models": forecast_models,
    }


def prepare_one(project_id: str, *, days: int, interval_minutes: int) -> dict:
    """Один проход seed_metrics_db + warmup_ml для конкретного project_id.

    Не закладываем idempotency через --reset — оператор сам решает. Если
    хочешь чистый прогон, удали данные через storage или передай reset=True
    в seed_metrics_db.main (тут вызываем с reset=True по умолчанию — demo
    подготовка обычно чистая).
    """
    from scripts import seed_metrics_db, warmup_ml

    logger.info("[%s] seed_metrics_db: %d days × %d min", project_id, days, interval_minutes)
    seed_result = seed_metrics_db.main(
        days=days,
        interval_minutes=interval_minutes,
        reset=True,
        project_id=project_id,
    )
    logger.info("[%s] warmup_ml", project_id)
    warmup_result = warmup_ml.main(project_id=project_id)
    return {"seed": seed_result, "warmup": warmup_result}


def main(
    slugs: tuple[str, ...] = DEFAULT_SLUGS,
    *,
    days: int = 14,
    interval_minutes: int = 60,
    skip_workspace: bool = False,
) -> dict:
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not skip_workspace:
        from scripts.seed_demo_workspace import seed_demo_workspace

        logger.info("Ensuring demo workspace (users / projects / connections)...")
        seed_demo_workspace()

    resolved = _resolve_project_ids(slugs)
    missing = [s for s in slugs if s not in resolved]
    if missing:
        logger.error("Slugs not found in DB: %s — run seed_demo_workspace first", missing)
        sys.exit(2)

    results = {}
    for slug, project_id in resolved.items():
        logger.info("\n=== Preparing %s (project_id=%s) ===", slug, project_id)
        prepare_one(project_id, days=days, interval_minutes=interval_minutes)
        results[slug] = verify_project(project_id)

    # Summary table.
    print()
    print(f"{'Project':<25} {'metrics':>9} {'anomaly':>9} {'cps':>5} "
          f"{'drift':>6} {'notif':>6} {'models':>7}")
    print("-" * 70)
    for slug, v in results.items():
        print(
            f"{slug:<25} {v['metrics']:>9} {v['anomaly_scores']:>9} "
            f"{v['changepoints']:>5} {v['drift_reports']:>6} "
            f"{v['notifications']:>6} {v['forecast_models']:>7}"
        )
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument(
        "--slug", action="append", dest="slugs",
        help="Slug demo-проекта (по умолчанию: retail-postgres). Можно повторять.",
    )
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--interval-minutes", type=int, default=60)
    parser.add_argument(
        "--skip-workspace", action="store_true",
        help="Пропустить seed_demo_workspace — если уверен, что user/project/conn уже есть.",
    )
    args = parser.parse_args()

    slugs = tuple(args.slugs) if args.slugs else DEFAULT_SLUGS
    main(
        slugs=slugs,
        days=args.days,
        interval_minutes=args.interval_minutes,
        skip_workspace=args.skip_workspace,
    )
