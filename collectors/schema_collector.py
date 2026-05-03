"""Schema-drift collector — snapshots column lists, diffs against the
previously stored snapshot, and emits ``schema_events`` rows for any
add / remove / type / nullability change.

Idempotent: if the schema hasn't moved, no events are written and the
snapshot is left alone (so ``captured_at`` reflects the last *change*,
not the last poll). The first time we see a table the snapshot is
saved without producing any events — there's no baseline to compare
against, and treating the first observation as "everything was added"
would spam alerts.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from app import db
from app.metrics_storage import (
    get_schema_snapshot,
    save_schema_events,
    save_schema_snapshot,
)

logger = logging.getLogger(__name__)


def _by_name(columns: list[dict]) -> dict[str, dict]:
    return {c["name"]: c for c in columns}


def diff_schemas(
    table_name: str,
    before: list[dict] | None,
    after: list[dict],
    ts: datetime | None = None,
) -> list[dict]:
    """Return a list of schema_events rows for the differences.

    ``before=None`` is treated as "no baseline yet" → empty list, not
    "everything is new".
    """
    if before is None:
        return []
    ts_iso = (ts or datetime.now(timezone.utc)).isoformat(timespec="seconds")

    before_map = _by_name(before)
    after_map = _by_name(after)

    events: list[dict] = []

    for name, col in after_map.items():
        if name not in before_map:
            events.append({
                "ts": ts_iso,
                "table_name": table_name,
                "change_type": "column_added",
                "column_name": name,
                "details": {"after": col},
            })

    for name, col in before_map.items():
        if name not in after_map:
            events.append({
                "ts": ts_iso,
                "table_name": table_name,
                "change_type": "column_removed",
                "column_name": name,
                "details": {"before": col},
            })

    for name, before_col in before_map.items():
        after_col = after_map.get(name)
        if after_col is None:
            continue
        if (before_col.get("type") or "") != (after_col.get("type") or ""):
            events.append({
                "ts": ts_iso,
                "table_name": table_name,
                "change_type": "type_changed",
                "column_name": name,
                "details": {
                    "before": {"type": before_col.get("type")},
                    "after": {"type": after_col.get("type")},
                },
            })
        if bool(before_col.get("nullable")) != bool(after_col.get("nullable")):
            events.append({
                "ts": ts_iso,
                "table_name": table_name,
                "change_type": "nullable_changed",
                "column_name": name,
                "details": {
                    "before": {"nullable": bool(before_col.get("nullable"))},
                    "after": {"nullable": bool(after_col.get("nullable"))},
                },
            })

    return events


def collect_table_schema(table_name: str, schema: str | None = None) -> list[dict]:
    """Snapshot one table's columns, diff vs stored snapshot, persist both.

    Returns the list of new schema_events that were written (empty if no
    changes — including the first-snapshot case).
    """
    try:
        current = db.table_schema(table_name, schema=schema)
    except Exception as exc:
        logger.error("Failed to read schema for %s: %s", table_name, exc)
        return []
    if not current:
        return []

    previous = get_schema_snapshot(table_name)
    events = diff_schemas(table_name, previous, current)
    if events:
        save_schema_events(events)
        logger.info("Schema drift on %s: %d event(s)", table_name, len(events))
    if previous is None or events:
        # Persist the snapshot on first observation and after every change
        # — leave it untouched on no-op polls so captured_at remains
        # informative ("last actual change").
        save_schema_snapshot(table_name, current)
    return events


def collect_all_schemas() -> dict[str, Any]:
    """Run schema-drift detection across every monitored table."""
    counts: dict[str, Any] = {"tables": 0, "events": 0, "errors": 0}
    for entry in db.list_tables():
        counts["tables"] += 1
        try:
            events = collect_table_schema(entry["table_name"], entry.get("schema"))
            counts["events"] += len(events)
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Schema collection failed for %s: %s",
                             entry["table_name"], exc)
            counts["errors"] += 1
    logger.info("Schema sweep finished: %s", counts)
    return counts
