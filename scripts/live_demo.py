"""Live demo pipeline (#75): stream synthetic events into Postgres,
collect metrics + run ML on every tick, show the dashboard updating in
real time.

Designed for presentations: one command brings up the full pipeline,
keeps it ticking at a configurable cadence, and can inject a synthetic
incident on demand so anomaly / change-point detectors visibly react in
the UI.

Prerequisites
-------------
- Target Postgres seeded with the standard demo schema and at least a
  few users (``make seed`` or ``python -m scripts.seed_target_db``).
- Flask app running (``make server`` or ``python -m app.app``).
- ``DATABASE_URL`` pointing at the same Postgres as the app.

Usage
-----
    # 20 ticks, 5 s each, normal traffic
    python -m scripts.live_demo --ticks 20 --interval 5

    # Stream forever (Ctrl-C to stop)
    python -m scripts.live_demo --interval 10

    # Inject incident at tick 8 (10× normal volume + ip_address NULL spike)
    python -m scripts.live_demo --ticks 20 --interval 5 --incident-at 8

    # Pure preview without writing — sanity-check args
    python -m scripts.live_demo --ticks 3 --interval 1 --dry-run
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Normal event mix matches the seeder so distribution drift only triggers
# when we explicitly inject an incident, not from baseline noise.
_EVENT_TYPES = ["login", "view", "add_to_cart", "checkout", "purchase", "logout"]
_EVENT_WEIGHTS = [0.15, 0.45, 0.15, 0.10, 0.10, 0.05]
_SERVER_IDS = ["server-1", "server-2", "server-3"]
_DEVICE_TYPES = ["mobile", "desktop", "tablet"]

_BASE_ROWS_PER_TICK = 200
_INCIDENT_ROW_MULTIPLIER = 10
_BASE_NULL_RATE = 0.02
_INCIDENT_NULL_RATE = 0.40
# Incident also skews server distribution — server-3 dominates traffic,
# which is what PSI/KS drift detection should pick up on column
# `server_id`.
_INCIDENT_SERVER_WEIGHTS = [0.05, 0.05, 0.90]


def _fetch_user_ids(engine, limit: int = 500) -> list[str]:
    """Cache a pool of real user_ids for FK-valid inserts."""
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id FROM users ORDER BY random() LIMIT :limit"),
            {"limit": limit},
        ).fetchall()
    return [str(r[0]) for r in rows]


def _generate_event_row(user_ids: list[str], incident: bool) -> dict:
    null_rate = _INCIDENT_NULL_RATE if incident else _BASE_NULL_RATE
    server_weights = _INCIDENT_SERVER_WEIGHTS if incident else None
    return {
        "id": str(uuid4()),
        "user_id": random.choice(user_ids),
        "session_id": str(uuid4()),
        "event_type": random.choices(_EVENT_TYPES, weights=_EVENT_WEIGHTS, k=1)[0],
        "duration_ms": random.randint(50, 5_000),
        "events_in_session": random.randint(1, 30),
        "ip_address": None if random.random() < null_rate else f"10.0.{random.randint(0, 255)}.{random.randint(0, 255)}",
        "ip_events_last_1h": random.randint(0, 50),
        "server_id": random.choices(_SERVER_IDS, weights=server_weights, k=1)[0],
        "device_type": random.choice(_DEVICE_TYPES),
        "is_bot_suspected": False,
        "created_at": datetime.now(UTC),
    }


_INSERT_SQL = text("""
    INSERT INTO events (
        id, user_id, session_id, event_type, duration_ms, events_in_session,
        ip_address, ip_events_last_1h, server_id, device_type,
        is_bot_suspected, created_at
    ) VALUES (
        :id, :user_id, :session_id, :event_type, :duration_ms, :events_in_session,
        :ip_address, :ip_events_last_1h, :server_id, :device_type,
        :is_bot_suspected, :created_at
    )
""")


def _insert_batch(engine, rows: list[dict]) -> None:
    with engine.begin() as conn:
        conn.execute(_INSERT_SQL, rows)


def _run_collector_tick() -> int:
    """Re-import inside the tick so each call sees the latest module state.

    Returns the row_count metric stored for `events` after collection — gives
    a printable per-tick progress signal without an extra DB round-trip.
    """
    from collectors.scheduler import collect_all_tables

    collect_all_tables()

    from app.metrics_storage import get_latest_metric
    # #53: live_demo runs against the global collector → 'legacy' tenant.
    latest = get_latest_metric("events", "row_count", "legacy")
    return int(latest["value"]) if latest else 0


def _run_changepoint_pass() -> dict:
    """Optional: detect change-points after collection so the UI's red
    dashed lines update during the demo (the scheduled job runs hourly —
    too slow for a live walk-through)."""
    from ml.changepoint import detect_all
    return detect_all()


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def _install_sigint_handler() -> None:
    # signal.signal callbacks must accept (signum, frame); both unused here.
    def _bye(_signum, _frame):
        logger.info("Stopping live demo (SIGINT)")
        sys.exit(0)
    signal.signal(signal.SIGINT, _bye)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--ticks", type=int, default=None,
                        help="Stop after N ticks (default: run until Ctrl-C).")
    parser.add_argument("--interval", type=float, default=10.0,
                        help="Seconds between ticks (default: 10).")
    parser.add_argument("--rows-per-tick", type=int, default=_BASE_ROWS_PER_TICK,
                        help=f"Normal-traffic batch size (default: {_BASE_ROWS_PER_TICK}).")
    parser.add_argument("--incident-at", type=int, default=None, metavar="TICK",
                        help="Inject a synthetic incident on this tick "
                             "(10× rows, 40%% ip_address NULL, server-3 skew).")
    parser.add_argument("--changepoints", action="store_true",
                        help="Also run change-point detection on every tick.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan and exit without touching the DB.")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    _setup_logging(args.verbose)
    _install_sigint_handler()

    if args.dry_run:
        logger.info(
            "DRY RUN: would run %s ticks at %.1fs interval, "
            "%s rows/tick (incident at tick %s, changepoints=%s)",
            args.ticks or "∞", args.interval, args.rows_per_tick,
            args.incident_at, args.changepoints,
        )
        return

    from app.db import get_engine

    engine = get_engine()
    user_ids = _fetch_user_ids(engine)
    if not user_ids:
        logger.error(
            "No users found in target DB — seed the schema first "
            "(make seed or python -m scripts.seed_target_db)."
        )
        sys.exit(1)
    logger.info("Loaded %d user_ids for FK-valid inserts", len(user_ids))

    tick = 0
    while args.ticks is None or tick < args.ticks:
        tick += 1
        is_incident = (args.incident_at is not None and tick == args.incident_at)
        n_rows = (
            args.rows_per_tick * _INCIDENT_ROW_MULTIPLIER
            if is_incident else args.rows_per_tick
        )
        rows = [_generate_event_row(user_ids, is_incident) for _ in range(n_rows)]
        _insert_batch(engine, rows)

        stored_row_count = _run_collector_tick()
        cp_summary = ""
        if args.changepoints:
            cp = _run_changepoint_pass()
            cp_summary = f"  cp={cp.get('detected', 0)}"

        flag = "  ★ INCIDENT" if is_incident else ""
        logger.info(
            "tick %3d  inserted=%4d  events.row_count=%d%s%s",
            tick, n_rows, stored_row_count, cp_summary, flag,
        )

        if args.ticks is None or tick < args.ticks:
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
