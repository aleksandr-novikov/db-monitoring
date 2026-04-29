from pathlib import Path

from flask import Blueprint, abort, render_template

from app import db
from app.metrics_storage import get_latest_metric, get_latest_null_counts

ROOT = Path(__file__).resolve().parent.parent

bp = Blueprint(
    "dashboard",
    __name__,
    template_folder=str(ROOT / "templates"),
    static_folder=str(ROOT / "static"),
    static_url_path="/static",
    url_prefix="/dashboard",
)


@bp.route("")
@bp.route("/")
def overview():
    tables = []
    total_rows = 0
    null_rates = []
    for entry in db.list_tables():
        name = entry["table_name"]
        snapshot = _table_snapshot(name, entry["schema"])
        if snapshot["row_count"] is None and snapshot["null_rate"] is None:
            # No stored metrics yet — show the row but with empty values.
            tables.append(snapshot)
            continue
        if snapshot["row_count"] is not None:
            total_rows += snapshot["row_count"]
        if snapshot["null_rate"] is not None:
            null_rates.append(snapshot["null_rate"])
        tables.append(snapshot)
    summary = {
        "table_count": len(tables),
        "total_rows": total_rows,
        "avg_null_rate": sum(null_rates) / len(null_rates) if null_rates else 0.0,
    }
    return render_template("overview.html", tables=tables, summary=summary)


@bp.route("/schema")
def schema_view():
    schemas = []
    for entry in db.list_tables():
        name = entry["table_name"]
        cols = _columns_with_nulls(name, entry["schema"], _table_snapshot(name, entry["schema"])["row_count"])
        schemas.append({**entry, "columns": cols})
    return render_template("schema.html", schemas=schemas)


@bp.route("/<table_name>")
def table_detail(table_name: str):
    entries = {t["table_name"]: t for t in db.list_tables()}
    if table_name not in entries:
        abort(404)
    schema = entries[table_name]["schema"]
    snapshot = _table_snapshot(table_name, schema)
    columns = _columns_with_nulls(table_name, schema, snapshot["row_count"])
    return render_template(
        "table_detail.html",
        stats=snapshot,
        columns=columns,
    )


def _columns_with_nulls(table_name: str, schema: str, row_count: int | None) -> list[dict]:
    """Combine info_schema column list with stored per-column null counts."""
    cols = db.table_schema(table_name, schema=schema)
    null_counts = get_latest_null_counts(table_name)
    result = []
    for c in cols:
        nc = null_counts.get(c["name"])
        nr = (nc / row_count) if (nc is not None and row_count) else None
        result.append({**c, "null_count": nc, "null_rate": nr})
    return result


def _table_snapshot(table_name: str, schema: str) -> dict:
    """Build a per-table dashboard row from stored metrics only (no live scans)."""
    rc = get_latest_metric(table_name, "row_count")
    nr = get_latest_metric(table_name, "null_rate")
    sz = get_latest_metric(table_name, "size_bytes")
    candidates = [m["ts"] for m in (rc, nr, sz) if m]
    last_check = max(candidates) if candidates else None
    return {
        "table_name": table_name,
        "schema": schema,
        "row_count": int(rc["value"]) if rc else None,
        "null_rate": nr["value"] if nr else None,
        "size_bytes": int(sz["value"]) if sz else None,
        "last_check": last_check,
    }


def status_class(null_rate: float | None) -> str:
    """Visual status bucket: ok / warn / crit. Used by template filter."""
    if null_rate is None:
        return "ok"
    if null_rate >= 0.30:
        return "crit"
    if null_rate >= 0.10:
        return "warn"
    return "ok"
