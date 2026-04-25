from datetime import timedelta
from pathlib import Path

from flask import Blueprint, abort, jsonify, render_template

from app import db, metrics_storage

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
        stats = db.table_stats(entry["table_name"], schema=entry["schema"])
        avg_null = _avg_null_rate(entry["table_name"], schema=entry["schema"])
        if stats:
            total_rows += stats["row_count"]
            tables.append({**stats, "avg_null_rate": avg_null})
        if avg_null is not None:
            null_rates.append(avg_null)
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
        cols = db.column_nulls(entry["table_name"], schema=entry["schema"])
        schemas.append({**entry, "columns": cols})
    return render_template("schema.html", schemas=schemas)


@bp.route("/<table_name>")
def table_detail(table_name: str):
    stats = db.table_stats(table_name)
    if stats is None:
        abort(404)
    columns = db.column_nulls(table_name)
    return render_template(
        "table_detail.html",
        stats=stats,
        columns=columns,
    )


@bp.route("/<table_name>/metrics.json")
def table_metrics_json(table_name: str):
    window = timedelta(days=7)
    return jsonify({
        "row_count": metrics_storage.get_metrics(table_name, "row_count", window),
        "null_rate": metrics_storage.get_metrics(table_name, "null_rate", window),
    })


def _avg_null_rate(table_name: str, schema: str | None = None) -> float | None:
    rows = db.column_nulls(table_name, schema=schema)
    rates = [r["null_rate"] for r in rows]
    return sum(rates) / len(rates) if rates else None


def status_class(null_rate: float | None) -> str:
    """Visual status bucket: ok / warn / crit. Used by template filter."""
    if null_rate is None:
        return "ok"
    if null_rate >= 0.30:
        return "crit"
    if null_rate >= 0.10:
        return "warn"
    return "ok"
