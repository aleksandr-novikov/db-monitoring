"""
Reset both DBs to a clean demo-ready state.

Sequence:
  1. TRUNCATE + reseed the target Postgres (DATABASE_URL) via seed_target_db
  2. Drop monitor tables (metrics, changepoints) and re-apply schema
  3. Seed 14 days of synthetic history (row_count, null_rate, column_distribution)
  4. Run the live collector once → populates per-column null_count snapshots
  5. Run change-point detection so the chart picks up the seeded anomalies

With --local-only, steps 1 and 4 are skipped — leaves Supabase untouched and
gives you a fast monitor-only reset (~5s vs ~10min). The schema panel will
show "—" for null_count until the next live-collector tick.

Usage:
    python -m scripts.reset_db
    python -m scripts.reset_db --local-only
"""

from __future__ import annotations

import argparse
import logging

from sqlalchemy import text

from app.metrics_storage import _apply_schema, get_engine
from ml.forecast import MODELS_DIR

logger = logging.getLogger(__name__)


def _clear_forecast_cache() -> None:
    # Stale joblibs were trained against the previous data shape; the freshness
    # check in ml.forecast only compares timestamps, not values, so it would
    # happily serve nonsense predictions until the 03:00 retrain. Wipe them.
    if not MODELS_DIR.exists():
        return
    removed = 0
    for path in MODELS_DIR.glob("*.joblib"):
        path.unlink()
        removed += 1
    print(f"[*] cleared {removed} stale forecast model(s) from {MODELS_DIR}")


def _reset_target() -> None:
    print("[1/5] resetting target Postgres (TRUNCATE + reseed)...")
    from scripts.seed_target_db import main as seed_target_main

    seed_target_main(reset=True)
    print("       target DB reseeded")


def _drop_monitor() -> None:
    with get_engine().begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS metrics"))
        conn.execute(text("DROP TABLE IF EXISTS changepoints"))
        conn.execute(text("DROP TABLE IF EXISTS schema_snapshots"))
        conn.execute(text("DROP TABLE IF EXISTS schema_events"))
    _apply_schema(get_engine())
    print("[2/5] monitor tables dropped + schema reapplied")


def _seed_history() -> None:
    from scripts.seed_metrics_history import main as seed_main

    print("[3/5] seeding 14 days of metric history...")
    seed_main()


def _run_collector() -> None:
    print("[4/5] running live collector against DATABASE_URL...")
    from collectors.scheduler import collect_all_tables

    collect_all_tables()
    print("       null_count rows populated")


def _detect_changepoints() -> None:
    print("[5/5] running change-point sweep...")
    from ml.changepoint import detect_all

    counts = detect_all()
    print(f"       {counts['detected']} change-points across {counts['tables']} tables")


def main(local_only: bool = False) -> None:
    logging.basicConfig(level=logging.WARNING)
    _clear_forecast_cache()
    if not local_only:
        _reset_target()
    _drop_monitor()
    _seed_history()
    if not local_only:
        _run_collector()
    _detect_changepoints()
    print("\nReset complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Reset monitor DB (and optionally target).")
    parser.add_argument(
        "--local-only", action="store_true",
        help="Skip Supabase reseed and live collector — monitor.db only.",
    )
    args = parser.parse_args()
    main(local_only=args.local_only)
