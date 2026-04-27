from datetime import timedelta

from flask import Blueprint, jsonify, request

from .db import list_tables, table_schema
from .metrics_storage import get_latest_metric, get_metrics

api = Blueprint("api", __name__, url_prefix="/api")

_VALID_METRICS = {"row_count", "null_rate"}
_RANGES = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
    "30d": timedelta(days=30),
}


@api.route("/tables")
def tables():
    """List monitored tables with their latest collected metrics.

    Returns row_count, null_rate and last_check as null until the metrics
    collector has run at least once. Schedule: every COLLECT_INTERVAL_MINUTES
    (default 15 min) via APScheduler.
    """
    result = []
    for t in list_tables():
        name = t["table_name"]
        rc = get_latest_metric(name, "row_count")
        nr = get_latest_metric(name, "null_rate")
        candidates = [x["ts"] for x in (rc, nr) if x]
        last_check = max(candidates) if candidates else None
        result.append({
            "table_name": name,
            "row_count": rc["value"] if rc else None,
            "null_rate": nr["value"] if nr else None,
            "last_check": last_check,
        })
    return jsonify(result)


@api.route("/metrics/<table_name>")
def metrics(table_name: str):
    """Return time-series data for a single metric of a table.

    Query params:
      metric — one of the values in _VALID_METRICS (default: row_count)
      range  — one of 1h | 6h | 24h | 7d | 14d | 30d (default: 24h)

    Returns [] when no data exists for the requested window.
    """
    metric = request.args.get("metric", "row_count")
    range_str = request.args.get("range", "24h")

    if metric not in _VALID_METRICS:
        return jsonify({"error": f"metric must be one of {sorted(_VALID_METRICS)}"}), 400
    if range_str not in _RANGES:
        return jsonify({"error": f"range must be one of {sorted(_RANGES)}"}), 400

    rows = get_metrics(table_name, metric, window=_RANGES[range_str])
    return jsonify([{"ts": r["ts"], "value": r["value"]} for r in rows])


@api.route("/schema/<table_name>")
def schema(table_name: str):
    """Return column schema for a table: [{name, type, nullable}].

    Returns 404 when the table does not exist in the monitored schema.
    """
    columns = table_schema(table_name)
    if not columns:
        return jsonify({"error": "table not found"}), 404
    return jsonify(columns)
