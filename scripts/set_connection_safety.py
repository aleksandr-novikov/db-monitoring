"""CLI to configure per-connection load-safety knobs (#232).

UI editor for these fields is intentionally a separate task — until then
operators bump the values via this script (or a SQL session). Usage:

    python -m scripts.set_connection_safety <connection_id> [options]

Examples:
    # Restrict to two tables, cap at 5 per tick.
    python -m scripts.set_connection_safety abc123 \\
        --allowlist users,orders --max 5

    # Drop noisy audit tables, skip anything over 10 GB.
    python -m scripts.set_connection_safety abc123 \\
        --denylist audit_logs,events_raw --max-size-gb 10

    # Tighten Postgres statement timeout to 10 s.
    python -m scripts.set_connection_safety abc123 --timeout-ms 10000

    # Clear allowlist (back to "all tables").
    python -m scripts.set_connection_safety abc123 --allowlist ''

Lookups are by *connection id* (UUID hex) — owner-scoping is left to whoever
runs the script. The collector picks up new values on its next tick; no
restart needed.
"""

from __future__ import annotations

import argparse
import json
import sys

from sqlalchemy import text

from app.metrics_storage import get_engine


def _parse_csv_or_empty(raw: str | None) -> str | None:
    """Comma-list → JSON array. Empty string clears the field (NULL)."""
    if raw is None:
        return None  # flag not provided — don't touch
    raw = raw.strip()
    if raw == "":
        return ""  # sentinel: clear to NULL
    items = [s.strip() for s in raw.split(",") if s.strip()]
    return json.dumps(items)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("connection_id", help="Connection UUID hex")
    parser.add_argument(
        "--allowlist",
        help="Comma-separated table names. Empty string clears (back to "
             "'all tables').",
    )
    parser.add_argument(
        "--denylist",
        help="Comma-separated table names to skip. Empty string clears.",
    )
    parser.add_argument(
        "--max", dest="max_tables", type=int,
        help="max_tables_per_tick — hard cap after allow/denylist.",
    )
    parser.add_argument(
        "--max-size-gb", dest="max_size_gb", type=float,
        help="Postgres only: skip tables larger than this many GB. "
             "Negative value clears.",
    )
    parser.add_argument(
        "--timeout-ms", dest="timeout_ms", type=int,
        help="Postgres only: SET statement_timeout for collector queries. "
             "0 clears.",
    )
    args = parser.parse_args()

    updates: dict[str, object] = {}
    if args.allowlist is not None:
        v = _parse_csv_or_empty(args.allowlist)
        updates["table_allowlist"] = None if v == "" else v
    if args.denylist is not None:
        v = _parse_csv_or_empty(args.denylist)
        updates["table_denylist"] = None if v == "" else v
    if args.max_tables is not None:
        updates["max_tables_per_tick"] = args.max_tables
    if args.max_size_gb is not None:
        updates["skip_tables_larger_than_gb"] = (
            None if args.max_size_gb < 0 else args.max_size_gb
        )
    if args.timeout_ms is not None:
        updates["statement_timeout_ms"] = (
            None if args.timeout_ms <= 0 else args.timeout_ms
        )

    if not updates:
        print("No fields supplied — nothing to update.", file=sys.stderr)
        return 2

    set_clause = ", ".join(f"{col} = :{col}" for col in updates)
    params = dict(updates)
    params["id"] = args.connection_id

    with get_engine().begin() as conn:
        result = conn.execute(
            text(f"UPDATE connections SET {set_clause} WHERE id = :id"),
            params,
        )
    if (result.rowcount or 0) == 0:
        print(
            f"FATAL: no connection with id={args.connection_id}",
            file=sys.stderr,
        )
        return 2

    for col, val in updates.items():
        print(f"  {col} = {val!r}")
    print(f"Updated connection {args.connection_id}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
