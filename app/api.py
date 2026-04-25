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
    columns = table_schema(table_name)
    if not columns:
        return jsonify({"error": "table not found"}), 404
    return jsonify(columns)
