"""Shared fixtures for Playwright dashboard E2E tests (#45).

Runs the real Flask app in a background thread (Werkzeug's
``make_server`` — no test client mock) so Playwright can drive the
dashboard exactly as a user would. Database introspection is stubbed at
the ``app.db`` module level: tests don't need a live Postgres / MySQL /
ClickHouse, just the metrics SQLite that the app already speaks.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


def _free_port() -> int:
    """Bind to port 0 and read the OS-assigned port back."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# --- Seed data for the fake monitored database -----------------------------

# Two tables: `users` has full metric history (chart + KPI cards), `events`
# is registered as monitored but has no stored metrics — used to verify the
# "Нет исторических метрик" empty-state on the chart.
SEEDED_TABLES = [
    {"table_name": "users", "schema": "public"},
    {"table_name": "events", "schema": "public"},
]

SEEDED_SCHEMAS = {
    "users": [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "email", "type": "text", "nullable": True},
        {"name": "country", "type": "text", "nullable": False},
    ],
    "events": [
        {"name": "id", "type": "integer", "nullable": False},
        {"name": "event_type", "type": "text", "nullable": False},
    ],
}


def _seed_metrics(metrics_url: str) -> None:
    from app.config import settings

    settings.MONITOR_DB_URL = metrics_url

    from app import metrics_storage

    metrics_storage._engine = None
    metrics_storage._initialized = False
    metrics_storage.get_engine()  # apply schema

    now = datetime.now(UTC)
    # 14 days of hourly row_count and null_rate for `users` so the chart
    # renders a real series. `events` is left empty.
    rows: list[dict] = []
    for hours_ago in range(14 * 24, 0, -1):
        ts = now - timedelta(hours=hours_ago)
        rows.append({
            "ts": ts, "table_name": "users",
            "metric_name": "row_count", "value": 1000 + hours_ago,
        })
        rows.append({
            "ts": ts, "table_name": "users",
            "metric_name": "null_rate", "value": 0.05,
            "tags": {"column": "email"},
        })
    rows.append({
        "ts": now, "table_name": "users",
        "metric_name": "size_bytes", "value": 65536,
    })
    metrics_storage.save_metrics(rows)


def _patch_db_module() -> dict:
    """Replace app.db introspection with fixture data.

    Returns the originals so the teardown can restore them. The patch lives
    at the module-attribute level (not via pytest's monkeypatch) so the
    Flask thread, which imports app.db lazily on every request, sees the
    stub regardless of when the request fires.
    """
    from app import db

    originals = {
        "list_tables": db.list_tables,
        "table_schema": db.table_schema,
        "table_stats": db.table_stats,
        "column_nulls": db.column_nulls,
        "column_distribution": db.column_distribution,
    }

    db.list_tables = lambda schema=None: list(SEEDED_TABLES)
    db.table_schema = lambda t, schema=None: list(SEEDED_SCHEMAS.get(t, []))
    db.table_stats = lambda t, schema=None: (
        {"table_name": t, "schema": "public", "row_count": 1000,
         "size_bytes": 65536, "last_analyze": None}
        if t in SEEDED_SCHEMAS else None
    )
    db.column_nulls = lambda t, schema=None: []
    db.column_distribution = lambda t, schema=None, top_n=20: []
    return originals


def _restore_db_module(originals: dict) -> None:
    from app import db

    for name, fn in originals.items():
        setattr(db, name, fn)


# --- Server fixture --------------------------------------------------------


@pytest.fixture(scope="session")
def live_dashboard(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """Start the Flask app in a background thread; yield base URL.

    Scope is session so 6+ Playwright tests share one app — startup adds
    ~200ms which we don't want to pay per test.
    """
    metrics_db = tmp_path_factory.mktemp("e2e") / "metrics.db"
    _seed_metrics(f"sqlite:///{metrics_db}")
    originals = _patch_db_module()

    # Import create_app *after* the patch so the blueprints that read
    # settings at import time see the override.
    from app.app import create_app

    app = create_app({"TESTING": True})
    port = _free_port()

    from werkzeug.serving import make_server

    server = make_server("127.0.0.1", port, app, threaded=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        _restore_db_module(originals)


@pytest.fixture(scope="session")
def browser_context_args(browser_context_args: dict) -> dict:
    """Extend pytest-playwright's default context with a stable viewport."""
    return {
        **browser_context_args,
        "viewport": {"width": 1280, "height": 800},
    }


# --- Screenshot artifact on failure ----------------------------------------


SCREENSHOT_DIR = Path(__file__).resolve().parent / "_artifacts"


def pytest_runtest_makereport(item, call):  # noqa: D401 — pytest hook signature
    """Drop a screenshot for any failing test that has a ``page`` fixture.

    Implemented inline in the hook (instead of a fixture that tests have to
    opt into) so coverage is automatic — adding a new e2e test never
    requires remembering a fixture argument. The CI workflow uploads the
    `_artifacts/` directory as `e2e-screenshots` when this job fails.
    """
    if call.when != "call" or call.excinfo is None:
        return
    page = item.funcargs.get("page")
    if page is None:
        return
    SCREENSHOT_DIR.mkdir(exist_ok=True)
    try:
        page.screenshot(path=str(SCREENSHOT_DIR / f"{item.name}.png"))
    except Exception:  # pragma: no cover - best-effort artifact
        # If the page itself is what failed (closed context, navigation crash)
        # we still want the original test failure to surface, not the
        # screenshot error.
        pass
