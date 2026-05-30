"""Prometheus /metrics endpoint + counter wiring tests (#101).

Counters live on the prometheus_client default REGISTRY, which is process-
global. We snapshot the current value with ``_get_value`` and assert the
delta caused by the action under test — avoids order-dependent flakes
between cases that share the registry.
"""

from __future__ import annotations

import re

import pytest

from app.instrumentation import (
    collector_runs_total,
    failed_login_attempts_total,
    http_requests_total,
)

# ── Helpers ────────────────────────────────────────────────────────────────


def _get_value(counter, **labels) -> float:
    """Read the current sample value for a labelled counter. Returns 0.0
    if the label combination hasn't been observed yet."""
    if labels:
        try:
            return counter.labels(**labels)._value.get()
        except (KeyError, AttributeError):
            return 0.0
    return counter._value.get()


# ── Fixture ────────────────────────────────────────────────────────────────


@pytest.fixture
def app_(tmp_path, monkeypatch):
    """Auth-enabled app pointed at a fresh SQLite metrics DB."""
    import app.metrics_storage as storage
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "metrics.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    return create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })


@pytest.fixture
def client(app_):
    return app_.test_client()


# ── Endpoint shape ─────────────────────────────────────────────────────────


def test_metrics_endpoint_returns_prometheus_text_format(client):
    resp = client.get("/metrics")
    assert resp.status_code == 200
    # Prometheus exposition is text/plain with a version tag. prometheus-client
    # >=0.20 uses "version=1.0.0"; older scrapers/integrations issue "0.0.4".
    # Either is valid; we only assert the family.
    ctype = resp.headers["Content-Type"]
    assert ctype.startswith("text/plain")
    assert "version=" in ctype

    body = resp.get_data(as_text=True)
    # The standard expositions start each metric with a HELP and TYPE line.
    assert re.search(r"^# HELP http_requests_total ", body, re.MULTILINE)
    assert re.search(r"^# TYPE http_requests_total counter", body, re.MULTILINE)


def test_metrics_endpoint_does_not_require_login(client):
    """Scrapers shouldn't be sent through the login flow. /metrics is open
    by convention (sits behind a network ACL)."""
    resp = client.get("/metrics", follow_redirects=False)
    assert resp.status_code == 200
    # If login was enforced we'd get a 302 to /auth/login here.


def test_metrics_endpoint_csrf_exempt(app_):
    """A scraper that fires POST against /metrics still gets a normal
    405 (route is GET-only) rather than a CSRF rejection — confirms the
    endpoint sits outside the CSRF protection envelope."""
    c = app_.test_client()
    resp = c.post("/metrics")
    # Werkzeug returns 405 for unsupported method; we shouldn't see 400/403.
    assert resp.status_code == 405


# ── http_requests_total wiring ─────────────────────────────────────────────


def test_http_requests_total_ticks_per_request(client):
    """Counter goes up by exactly 1 for each successful request, with the
    correct (method, endpoint, status) labels.

    Use ``/auth/login`` (GET) — it always returns 200 in TESTING. Don't
    use ``/healthz`` here because target_db may be unreachable in CI,
    flipping the status to 503 and confusing the assertion.
    """
    before = _get_value(http_requests_total,
                        method="GET", endpoint="auth.login", status="200")
    client.get("/auth/login")
    client.get("/auth/login")
    after = _get_value(http_requests_total,
                       method="GET", endpoint="auth.login", status="200")
    assert after - before == 2


def test_http_requests_total_records_4xx_separately(client):
    """A 404 increments under status=404, not status=200."""
    before_404 = _get_value(http_requests_total,
                            method="GET", endpoint="unknown", status="404")
    client.get("/this-does-not-exist")
    after_404 = _get_value(http_requests_total,
                           method="GET", endpoint="unknown", status="404")
    assert after_404 - before_404 == 1


def test_http_request_duration_histogram_observes(client):
    """Histogram count goes up on each request — value depends on env so
    we only assert the bucket got an observation, not its magnitude."""
    from app.instrumentation import http_request_duration_seconds

    samples_before = http_request_duration_seconds.labels(
        method="GET", endpoint="auth.login",
    )._sum.get()
    client.get("/auth/login")
    samples_after = http_request_duration_seconds.labels(
        method="GET", endpoint="auth.login",
    )._sum.get()
    assert samples_after > samples_before


# ── collector_runs_total wiring ────────────────────────────────────────────


def test_collector_runs_total_ok_on_success(app_, monkeypatch):
    """A successful collect_for_connection bumps {result="ok"}."""
    import unittest.mock as mock
    import uuid
    from datetime import UTC, datetime

    import app.db as db_mod
    import app.metrics_storage as storage
    from app import crypto
    from collectors import metrics_collector, per_project

    # Seed user + project + active connection.
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email=f"u{uid[:5]}@x.io", password_hash="x")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=uid, name="P", slug="d",
    )
    conn = storage.create_connection(
        connection_id=uuid.uuid4().hex, project_id=project["id"],
        name="local", dsn_encrypted=crypto.encrypt_dsn("postgresql://u:p@h/d"),
        schema_name="public", interval_minutes=15, is_active=True,
    )

    fake_rows = [{
        "ts": datetime.now(UTC), "table_name": "users",
        "metric_name": "row_count", "value": 1.0,
    }]

    def fake_list_tables(self, schema):
        return [{"table_name": "users", "schema": schema}]

    before = _get_value(collector_runs_total, result="ok")
    with mock.patch.object(metrics_collector.MetricsCollector, "collect",
                           return_value=fake_rows), \
         mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=fake_list_tables):
        per_project.collect_for_connection(project["id"], conn["id"])
    after = _get_value(collector_runs_total, result="ok")
    assert after - before == 1


def test_collector_runs_total_error_on_failure(app_, monkeypatch):
    """When the adapter throws, the counter bumps {result="error"}."""
    import unittest.mock as mock
    import uuid

    import app.db as db_mod
    import app.metrics_storage as storage
    from app import crypto
    from collectors import per_project

    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email=f"u{uid[:5]}@x.io", password_hash="x")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=uid, name="P", slug="d",
    )
    conn = storage.create_connection(
        connection_id=uuid.uuid4().hex, project_id=project["id"],
        name="local", dsn_encrypted=crypto.encrypt_dsn("postgresql://u:p@h/d"),
        schema_name="public", interval_minutes=15, is_active=True,
    )

    def boom(self, schema):
        raise RuntimeError("adapter boom")

    before = _get_value(collector_runs_total, result="error")
    with mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=boom):
        per_project.collect_for_connection(project["id"], conn["id"])
    after = _get_value(collector_runs_total, result="error")
    assert after - before == 1


# ── failed_login_attempts_total wiring ─────────────────────────────────────


def test_failed_login_attempts_total_bumps_on_wrong_password(client):
    """Wrong password → counter +1. Successful login → no bump."""
    # Register so the email exists.
    client.post("/auth/register", data={
        "email": "u@example.com",
        "password": "supersecret1",
        "confirm": "supersecret1",
    })
    client.post("/auth/logout")

    before = _get_value(failed_login_attempts_total)
    client.post("/auth/login",
                data={"email": "u@example.com", "password": "wrong-pass"})
    after_bad = _get_value(failed_login_attempts_total)
    assert after_bad - before == 1

    # Successful login: counter does NOT move.
    client.post("/auth/login",
                data={"email": "u@example.com", "password": "supersecret1"})
    after_good = _get_value(failed_login_attempts_total)
    assert after_good == after_bad


def test_failed_login_attempts_bumps_on_unknown_email(client):
    """Counter still bumps for a non-existent email — same UI message
    means we shouldn't distinguish counters either, otherwise scrape
    becomes an oracle for email-enumeration."""
    before = _get_value(failed_login_attempts_total)
    client.post("/auth/login",
                data={"email": "ghost@example.com", "password": "x" * 12})
    after = _get_value(failed_login_attempts_total)
    assert after - before == 1
