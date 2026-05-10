"""
Reset both DBs to a clean demo-ready state.

Sequence:
  1. TRUNCATE + reseed the target Postgres (DATABASE_URL) via seed_target_db
  2. Drop monitor tables (metrics, changepoints, schema_*) and re-apply schema
  3. Run the live collector once → first real snapshot in monitor.db
  4. Run change-point detection on whatever real history exists

Usage:
    python -m scripts.reset_db
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from app.metrics_storage import _apply_schema, get_engine
from ml.anomaly_detector import MODELS_DIR as ANOMALY_MODELS_DIR
from ml.forecast import MODELS_DIR as FORECAST_MODELS_DIR

logger = logging.getLogger(__name__)


def _clear_model_cache() -> None:
    # Stale joblibs were trained against the previous data shape. Clear both
    # forecast and anomaly models so the nightly jobs retrain from scratch.
    removed = 0
    for models_dir in {FORECAST_MODELS_DIR, ANOMALY_MODELS_DIR}:
        if not models_dir.exists():
            continue
        for path in models_dir.glob("*.joblib"):
            path.unlink()
            removed += 1
    print(f"[*] cleared {removed} stale model(s)")


def _reset_target() -> None:
    print("[1/4] resetting target Postgres (TRUNCATE + reseed)...")
    from scripts.seed_target_db import main as seed_target_main

    seed_target_main(reset=True)
    print("       target DB reseeded")


def _drop_monitor() -> None:
    with get_engine().begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS metrics"))
        conn.execute(text("DROP TABLE IF EXISTS changepoints"))
        conn.execute(text("DROP TABLE IF EXISTS schema_snapshots"))
        conn.execute(text("DROP TABLE IF EXISTS schema_events"))
        conn.execute(text("DROP TABLE IF EXISTS anomaly_scores"))
        conn.execute(text("DROP TABLE IF EXISTS drift_reports"))
    _apply_schema(get_engine())
    print("[2/4] monitor tables dropped + schema reapplied")


def _run_collector() -> None:
    print("[3/4] running live collector against DATABASE_URL...")
    from collectors.scheduler import collect_all_tables

    collect_all_tables()
    print("       first snapshot written")


def _detect_changepoints() -> None:
    print("[4/4] running change-point sweep...")
    from ml.changepoint import detect_all

    counts = detect_all()
    print(f"       {counts['detected']} change-points across {counts['tables']} tables")


def main() -> None:
    logging.basicConfig(level=logging.WARNING)
    _clear_model_cache()
    _reset_target()
    _drop_monitor()
    _run_collector()
    _detect_changepoints()
    print("\nReset complete.")


if __name__ == "__main__":
    main()
