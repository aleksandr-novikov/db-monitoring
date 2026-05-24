from datetime import UTC, datetime, timedelta
from pathlib import Path

from flask import Blueprint, abort, g, render_template

from app import db
from app.metrics_storage import (
    build_history_aggregate,
    count_notifications,
    get_history_daily,
    get_history_insights,
    get_history_runs,
    get_latest_metric,
    get_latest_null_counts,
    get_notifications,
    get_schema_events,
)


def _current_project_id() -> str:
    """g.current_project["id"] with a 'legacy' fallback — see api._current_project_id."""
    project = getattr(g, "current_project", None)
    return project["id"] if project else "legacy"

_NOTIFICATION_EVENT_LABELS = {
    "anomaly": "Аномалия",
    "schema_drift": "Дрейф схемы",
    "changepoint": "Change-point",
    "forecast": "Прогноз",
    "root_cause": "Root cause",
}
_NOTIFICATION_PAGE_SIZE = 25

_RECENT_SCHEMA_DAYS = 7

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
    # Onboarding empty state (#55): if the current project has zero
    # connections yet, render the dashboard with a CTA banner instead of
    # an empty table grid. Anonymous / legacy paths fall through to the
    # legacy global view.
    from app.metrics_storage import list_connections_for_project

    has_connections = False
    project = getattr(g, "current_project", None)
    if project is not None:
        has_connections = bool(list_connections_for_project(project["id"]))

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
    return render_template(
        "overview.html",
        tables=tables,
        summary=summary,
        ml_last_runs=_ml_last_runs(),
        needs_first_connection=project is not None and not has_connections,
    )


def _ml_last_runs() -> dict[str, str | None]:
    """Last-run timestamp (UTC, "YYYY-MM-DD HH:MM") per ML model."""
    from sqlalchemy import text

    from app.metrics_storage import get_engine
    from ml.forecast import MODELS_DIR

    out: dict[str, str | None] = {
        "isolation_forest": None,
        "prophet": None,
        "pelt": None,
        "drift": None,
    }
    with get_engine().connect() as conn:
        out["isolation_forest"] = conn.execute(text("SELECT MAX(ts) FROM anomaly_scores")).scalar()
        out["pelt"] = conn.execute(text("SELECT MAX(detected_at) FROM changepoints")).scalar()
        out["drift"] = conn.execute(text("SELECT MAX(computed_at) FROM drift_reports")).scalar()
    # Prophet не пишет в БД — обученные модели лежат в models/*.joblib,
    # mtime самого свежего файла = время последнего ночного переобучения.
    if MODELS_DIR.exists():
        mtimes = [p.stat().st_mtime for p in MODELS_DIR.glob("*.joblib")]
        if mtimes:
            out["prophet"] = datetime.fromtimestamp(max(mtimes), tz=UTC).isoformat()
    return {k: _fmt_ts(v) for k, v in out.items()}


def _fmt_ts(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("T", " ")[:16] + " UTC"


@bp.route("/schema")
def schema_view():
    from app.metrics_storage import get_drift_report

    cutoff = datetime.now(UTC) - timedelta(days=_RECENT_SCHEMA_DAYS)
    schemas = []
    for entry in db.list_tables():
        name = entry["table_name"]
        snapshot = _table_snapshot(name, entry["schema"])
        cols = _columns_with_nulls(name, entry["schema"], snapshot["row_count"])
        drift_by_col = {d["column"]: d for d in get_drift_report(name)}
        for c in cols:
            d = drift_by_col.get(c["name"])
            c["drift"] = d  # None when no snapshots exist for this column
        schema_events = get_schema_events(name, window=timedelta(days=30))
        recent_count = sum(
            1 for e in schema_events if _parse_event_ts(e["ts"]) >= cutoff
        )
        schemas.append({
            **entry,
            "columns": cols,
            "schema_events": schema_events,
            "recent_schema_changes": recent_count,
        })
    return render_template("schema.html", schemas=schemas)


def _parse_event_ts(value: str) -> datetime:
    s = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)



@bp.route("/history")
def history_view():
    agg = build_history_aggregate(project_id=_current_project_id())
    runs = get_history_runs(agg, limit=12)
    daily_history = get_history_daily(agg, days=14)
    insights = get_history_insights(agg)
    return render_template(
        "history.html",
        runs=runs,
        daily_history=daily_history,
        insights=insights,
    )

@bp.route("/notifications")
def notifications_view():
    """История Telegram-уведомлений с фильтрами и пагинацией (#76)."""
    from flask import request

    event_type = request.args.get("event_type") or None
    status = request.args.get("status") or None
    table = request.args.get("table") or None
    if event_type and event_type not in _NOTIFICATION_EVENT_LABELS:
        event_type = None
    if status and status not in {"sent", "failed"}:
        status = None

    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1

    offset = (page - 1) * _NOTIFICATION_PAGE_SIZE
    filters = {"event_type": event_type, "status": status, "table_name": table}
    items = get_notifications(limit=_NOTIFICATION_PAGE_SIZE, offset=offset, **filters)
    total = count_notifications(**filters)
    pages = max(1, (total + _NOTIFICATION_PAGE_SIZE - 1) // _NOTIFICATION_PAGE_SIZE)

    return render_template(
        "notifications.html",
        items=items,
        total=total,
        page=page,
        pages=pages,
        page_size=_NOTIFICATION_PAGE_SIZE,
        filters={"event_type": event_type or "", "status": status or "", "table": table or ""},
        event_labels=_NOTIFICATION_EVENT_LABELS,
    )


@bp.route("/schema/<table_name>")
def table_detail(table_name: str):
    from app.metrics_storage import get_drift_report

    entries = {t["table_name"]: t for t in db.list_tables()}
    if table_name not in entries:
        abort(404)
    schema = entries[table_name]["schema"]
    snapshot = _table_snapshot(table_name, schema)
    columns = _columns_with_nulls(table_name, schema, snapshot["row_count"])
    drift_by_col = {d["column"]: d for d in get_drift_report(table_name)}
    for c in columns:
        c["drift"] = drift_by_col.get(c["name"])
    schema_events = get_schema_events(table_name, window=timedelta(days=30))
    return render_template(
        "table_detail.html",
        stats=snapshot,
        columns=columns,
        schema_events=schema_events,
    )


def _columns_with_nulls(table_name: str, schema: str, row_count: int | None) -> list[dict]:
    """Combine info_schema column list with stored per-column null counts."""
    cols = db.table_schema(table_name, schema=schema)
    null_counts = get_latest_null_counts(table_name, _current_project_id())
    result = []
    for c in cols:
        nc = null_counts.get(c["name"])
        nr = (nc / row_count) if (nc is not None and row_count) else None
        result.append({**c, "null_count": nc, "null_rate": nr})
    return result


def _table_snapshot(table_name: str, schema: str) -> dict:
    """Build a per-table dashboard row from stored metrics only (no live scans)."""
    project_id = _current_project_id()
    rc = get_latest_metric(table_name, "row_count", project_id)
    nr = get_latest_metric(table_name, "null_rate", project_id)
    sz = get_latest_metric(table_name, "size_bytes", project_id)
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
