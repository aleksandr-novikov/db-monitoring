import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from flask import Blueprint, abort, g, redirect, render_template, url_for
from flask_login import current_user

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

logger = logging.getLogger(__name__)

# Sentinel used in notification filters when the user has no project — yields
# an empty result set without a special-case branch in the query builder.
_NO_PROJECT_ID = "__no_project__"


def _onboarding_redirect():
    """Return a redirect Response when an authenticated user has no projects.

    Call at the top of any dashboard route that must not fall through to the
    legacy global-DSN data path. Returns None when no redirect is needed.
    """
    if current_user.is_authenticated and getattr(g, "current_project", None) is None:
        return redirect(url_for("projects.new_project"))
    return None


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
    from app.metrics_storage import list_connections_for_project

    project = getattr(g, "current_project", None)
    # #137: authenticated user with no projects → onboarding, not legacy data.
    needs_first_project = current_user.is_authenticated and project is None
    has_connections = bool(
        list_connections_for_project(project["id"]) if project else False
    )
    needs_first_connection = project is not None and not has_connections

    tables: list = []
    total_rows = 0
    null_rates: list = []
    skip_tables = needs_first_project or needs_first_connection
    try:
        schema_entries = [] if skip_tables else db.list_tables()
    except Exception as exc:
        logger.warning("list_tables failed in overview: %s", exc)
        schema_entries = []
    for entry in schema_entries:
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
        ml_last_runs={} if (needs_first_project or needs_first_connection) else _ml_last_runs(),
        needs_first_project=needs_first_project,
        needs_first_connection=needs_first_connection,
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
    from app.metrics_storage import get_drift_report, list_connections_for_project

    project = getattr(g, "current_project", None)
    needs_first_project = current_user.is_authenticated and project is None
    has_connections = bool(
        list_connections_for_project(project["id"]) if project else False
    )
    needs_first_connection = project is not None and not has_connections

    cutoff = datetime.now(UTC) - timedelta(days=_RECENT_SCHEMA_DAYS)
    schemas = []
    skip_tables = needs_first_project or needs_first_connection
    if not skip_tables:
        try:
            _schema_entries = db.list_tables()
        except Exception as exc:
            logger.warning("list_tables failed in schema_view: %s", exc)
            _schema_entries = []
        for entry in _schema_entries:
            name = entry["table_name"]
            snapshot = _table_snapshot(name, entry["schema"])
            cols = _columns_with_nulls(name, entry["schema"], snapshot["row_count"])
            drift_by_col = {d["column"]: d for d in get_drift_report(name)}
            for c in cols:
                d = drift_by_col.get(c["name"])
                c["drift"] = d
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
    return render_template(
        "schema.html",
        schemas=schemas,
        needs_first_project=needs_first_project,
        needs_first_connection=needs_first_connection,
    )


def _parse_event_ts(value: str) -> datetime:
    s = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)



@bp.route("/history")
def history_view():
    redir = _onboarding_redirect()
    if redir:
        return redir
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

    project = getattr(g, "current_project", None)
    # #137: authenticated user with no projects → sentinel yields empty results.
    if current_user.is_authenticated and project is None:
        notif_project_id: str | None = _NO_PROJECT_ID
    else:
        notif_project_id = project["id"] if project else None

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
    items = get_notifications(limit=_NOTIFICATION_PAGE_SIZE, offset=offset,
                              project_id=notif_project_id, **filters)
    total = count_notifications(project_id=notif_project_id, **filters)
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
    from app.metrics_storage import get_drift_report, list_connections_for_project

    project = getattr(g, "current_project", None)
    # #137: auth'd user with no projects → 404 (no legacy data leak).
    # Anonymous users keep the legacy path (project is None, not authenticated).
    if current_user.is_authenticated and project is None:
        abort(404)
    has_connections = bool(
        list_connections_for_project(project["id"]) if project else False
    )
    if project is not None and not has_connections:
        abort(404)

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
