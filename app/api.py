from datetime import UTC, timedelta

from flask import Blueprint, g, jsonify, request
from flask_login import current_user

from .db import list_tables, table_schema
from .feature_flags import require_flag
from .metrics_storage import (
    count_notifications,
    get_anomaly_scores,
    get_cached_explanation,
    get_changepoints,
    get_latest_metric,
    get_metrics,
    get_notifications,
    get_schema_events,
    save_explanation,
)


def _current_project_id() -> str:
    """Return ``g.current_project["id"]`` or fall back to ``'legacy'`` (#53).

    During the Sprint 3 multi-tenant rollout, anonymous /api callers (and
    legacy tests that don't register a user) still need a deterministic
    project scope. The 'legacy' bucket is what the migration backfills
    pre-existing rows into, so legacy-mode reads stay coherent. Once all
    /api callers are guaranteed to be authenticated, this can collapse to
    ``g.current_project["id"]`` and 401 on miss.
    """
    project = getattr(g, "current_project", None)
    return project["id"] if project else "legacy"

api = Blueprint("api", __name__, url_prefix="/api")

_VALID_METRICS = {"row_count", "null_rate", "null_count", "size_bytes", "last_modified"}
_FORECAST_METRICS = {"row_count", "size_bytes"}
_EXPLAIN_METRICS = {"row_count", "null_rate"}
_RANGES = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
    "30d": timedelta(days=30),
}
_HORIZONS = {"1d": 1, "3d": 3, "7d": 7, "14d": 14, "30d": 30}
_NOTIFICATION_EVENT_TYPES = {
    "anomaly", "schema_drift", "changepoint", "forecast", "root_cause", "test",
}
_NOTIFICATION_STATUSES = {"sent", "failed"}
_MAX_NOTIFICATIONS_LIMIT = 200


def _has_project_table_metrics(table_name: str, project_id: str) -> bool:
    """Return true when the current project has monitored data for a table."""
    if get_latest_metric(table_name, "row_count", project_id):
        return True
    return bool(get_anomaly_scores(table_name, project_id=project_id, window=_RANGES["14d"]))


@api.route("/tables")
def tables():
    """List monitored tables with their latest collected metrics.

    Returns row_count, null_rate and last_check as null until the metrics
    collector has run at least once. Schedule: every COLLECT_INTERVAL_MINUTES
    (default 15 min) via APScheduler.
    """
    project_id = _current_project_id()
    result = []
    for t in list_tables():
        name = t["table_name"]
        rc = get_latest_metric(name, "row_count", project_id)
        nr = get_latest_metric(name, "null_rate", project_id)
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

    rows = get_metrics(table_name, metric, _current_project_id(), window=_RANGES[range_str])
    return jsonify([{"ts": r["ts"], "value": r["value"]} for r in rows])


@api.route("/forecast/<table_name>")
@require_flag("forecast")
def forecast_endpoint(table_name: str):
    """Return forecast points for a table metric over the requested horizon.

    Query params:
      metric  — row_count (default) or size_bytes
      horizon — 1d | 3d | 7d (default) | 14d | 30d

    Gated by ``FF_FORECAST`` (#104) — when the flag is off the route 404s,
    indistinguishable from a never-deployed endpoint. Useful for emergency
    cut-off if the Prophet pipeline starts misbehaving in production.
    """
    from ml.forecast import InsufficientDataError
    from ml.forecast import forecast as run_forecast

    metric = request.args.get("metric", "row_count")
    horizon = request.args.get("horizon", "7d")

    if metric not in _FORECAST_METRICS:
        return jsonify({"error": f"metric must be one of {sorted(_FORECAST_METRICS)}"}), 400
    if horizon not in _HORIZONS:
        return jsonify({"error": f"horizon must be one of {sorted(_HORIZONS)}"}), 400

    try:
        points = run_forecast(table_name, metric, horizon_days=_HORIZONS[horizon], project_id=_current_project_id())
    except InsufficientDataError as e:
        return jsonify({"error": "insufficient_data", "message": str(e)}), 422
    return jsonify(points)


@api.route("/drift/<table_name>")
def drift(table_name: str):
    """Per-column drift report against the rolling 7-day baseline.

    Reads from the `drift_reports` cache populated by warmup_ml + the collect
    tick. Empty list when nothing has been computed yet.

    Returns [{column, data_type, psi, ks_pvalue, is_drift, severity}].
    """
    from app.metrics_storage import get_drift_report

    return jsonify(get_drift_report(table_name, _current_project_id()))


@api.route("/changepoints/<table_name>")
def changepoints(table_name: str):
    """Return persisted change-points for a table, oldest first.

    Query params:
      metric — optional filter; defaults to all metrics
      range  — 7d | 14d (default) | 30d
    """
    metric = request.args.get("metric")
    range_str = request.args.get("range", "14d")
    if metric is not None and metric not in _VALID_METRICS:
        return jsonify({"error": f"metric must be one of {sorted(_VALID_METRICS)}"}), 400
    if range_str not in _RANGES:
        return jsonify({"error": f"range must be one of {sorted(_RANGES)}"}), 400
    rows = get_changepoints(
        table_name,
        project_id=_current_project_id(),
        metric_name=metric,
        window=_RANGES[range_str],
    )
    return jsonify(rows)


@api.route("/schema/<table_name>")
def schema(table_name: str):
    """Return column schema for a table: [{name, type, nullable}].

    Returns 404 when the table does not exist in the monitored schema.
    """
    columns = table_schema(table_name)
    if not columns:
        return jsonify({"error": "table not found"}), 404
    return jsonify(columns)


@api.route("/anomalies/<table_name>")
def anomalies(table_name: str):
    """Anomaly scores for a table, oldest first.

    Query params:
      range — 1h | 6h | 24h | 7d (default) | 14d | 30d

    Returns [{ts, score, is_anomaly}]. score is the raw IsolationForest
    decision_function value; negative values indicate anomalies.
    Returns [] when no scores have been computed yet (model not trained).
    """
    range_str = request.args.get("range", "7d")
    if range_str not in _RANGES:
        return jsonify({"error": f"range must be one of {sorted(_RANGES)}"}), 400
    window = _RANGES[range_str]
    project_id = _current_project_id()
    scores = get_anomaly_scores(table_name, project_id=project_id, window=window)
    if any(s["is_anomaly"] for s in scores):
        # Attach per-feature values + z-scores so the UI can show which of
        # the 4 dimensions (row_count / null_rate / Δrow_count / Δnull_rate)
        # actually drove each anomaly. Computed lazily (no DB migration).
        from ml.anomaly_detector import feature_breakdown

        details = feature_breakdown(table_name, window=window, project_id=project_id)
        for s in scores:
            if s["is_anomaly"] and s["ts"] in details:
                s["features"] = details[s["ts"]]
    return jsonify(scores)


@api.route("/explain", methods=["POST"])
def explain():
    """LLM root-cause explanation for an anomaly point.

    Body (JSON): {table, metric, ts}
    Returns: {explanation, suggested_fix, confidence}

    Checks the 24 h cache before calling NIM. On NIM failure, falls back
    to a rule-based explanation (confidence=0.3).
    """
    from .llm import explain_anomaly

    body = request.get_json(silent=True) or {}
    table = body.get("table", "").strip()
    metric = body.get("metric", "").strip()
    ts = body.get("ts", "").strip()

    if not table or not metric or not ts:
        return jsonify({"error": "table, metric and ts are required"}), 400
    if metric not in _EXPLAIN_METRICS:
        return jsonify({"error": f"metric must be one of {sorted(_EXPLAIN_METRICS)}"}), 400

    cached = get_cached_explanation(table, metric, ts)
    if cached:
        return jsonify(cached)

    project_id = _current_project_id()
    if not _has_project_table_metrics(table, project_id):
        return jsonify({"error": "table not found"}), 404

    result = explain_anomaly(table, metric, ts, project_id=project_id)
    save_explanation(
        table=table,
        metric=metric,
        ts=ts,
        explanation=result["explanation"],
        suggested_fix=result["suggested_fix"],
        confidence=result["confidence"],
    )
    return jsonify(result)


@api.route("/notifications")
def notifications():
    """Return Telegram notification history with pagination + filters (#76).

    Query params:
      event_type — anomaly | schema_drift | changepoint | forecast | root_cause
      table      — filter by table_name
      status     — sent | failed
      range      — 1h | 6h | 24h | 7d | 14d | 30d (optional time window)
      limit      — page size, 1..200 (default 50)
      offset     — pagination offset (default 0)

    Response: {items: [...], total, limit, offset}.
    """
    event_type = request.args.get("event_type")
    table = request.args.get("table")
    status = request.args.get("status")
    range_str = request.args.get("range")

    if event_type and event_type not in _NOTIFICATION_EVENT_TYPES:
        return jsonify({"error": f"event_type must be one of {sorted(_NOTIFICATION_EVENT_TYPES)}"}), 400
    if status and status not in _NOTIFICATION_STATUSES:
        return jsonify({"error": f"status must be one of {sorted(_NOTIFICATION_STATUSES)}"}), 400
    if range_str and range_str not in _RANGES:
        return jsonify({"error": f"range must be one of {sorted(_RANGES)}"}), 400

    try:
        limit = int(request.args.get("limit", 50))
        offset = int(request.args.get("offset", 0))
    except ValueError:
        return jsonify({"error": "limit and offset must be integers"}), 400
    if limit < 1 or limit > _MAX_NOTIFICATIONS_LIMIT:
        return jsonify({"error": f"limit must be 1..{_MAX_NOTIFICATIONS_LIMIT}"}), 400
    if offset < 0:
        return jsonify({"error": "offset must be >= 0"}), 400

    since = None
    if range_str:
        from datetime import datetime
        since = datetime.now(UTC) - _RANGES[range_str]

    project = getattr(g, "current_project", None)
    if current_user.is_authenticated and project is None:
        notif_project_id: str | None = "__no_project__"
    else:
        notif_project_id = project["id"] if project else "legacy"

    filters = {
        "event_type": event_type,
        "table_name": table,
        "status": status,
        "since": since,
    }
    items = get_notifications(limit=limit, offset=offset,
                              project_id=notif_project_id, **filters)
    total = count_notifications(project_id=notif_project_id, **filters)
    return jsonify({"items": items, "total": total, "limit": limit, "offset": offset})


@api.route("/schema/<table_name>/changes")
def schema_changes(table_name: str):
    """Recent schema-drift events for a table, newest first.

    Query params:
      range — 1h | 6h | 24h | 7d | 14d | 30d (default 30d)
    """
    range_str = request.args.get("range", "30d")
    if range_str not in _RANGES:
        return jsonify({"error": f"range must be one of {sorted(_RANGES)}"}), 400
    return jsonify(get_schema_events(table_name, project_id=_current_project_id(), window=_RANGES[range_str]))
