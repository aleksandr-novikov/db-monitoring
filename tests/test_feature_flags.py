"""Feature flags tests (#104).

Covers:
- ``is_enabled`` truthy-value matrix and default-OFF behaviour
- ``require_flag`` decorator: 404 when off, normal response when on
- Real wiring on ``/api/forecast/<table>`` — FF_FORECAST controls visibility
- ``/admin/feature-flags`` lists known flags with current values
"""

from __future__ import annotations

import pytest
from flask import Flask, jsonify

from app.feature_flags import is_enabled, require_flag, snapshot

# ── is_enabled matrix ──────────────────────────────────────────────────────


@pytest.mark.parametrize("raw, expected", [
    ("1", True),
    ("true", True),
    ("TRUE", True),
    ("yes", True),
    ("on", True),
    ("0", False),
    ("false", False),
    ("no", False),
    ("off", False),
    ("", False),
    ("   ", False),
    ("maybe", False),  # unknown → off
])
def test_is_enabled_truthy_matrix(monkeypatch, raw, expected):
    monkeypatch.setenv("FF_DEMO", raw)
    assert is_enabled("demo") is expected


def test_is_enabled_default_off_when_env_missing(monkeypatch):
    """Missing env var = OFF. Never-set flag must not accidentally expose
    half-finished functionality."""
    monkeypatch.delenv("FF_NOT_SET", raising=False)
    assert is_enabled("not_set") is False


def test_is_enabled_normalises_name(monkeypatch):
    """``forecast``, ``FORECAST``, ``fore-cast`` all read FF_FORECAST."""
    monkeypatch.setenv("FF_FORE_CAST", "1")
    assert is_enabled("fore-cast") is True
    assert is_enabled("FORE_CAST") is True
    assert is_enabled("fore_cast") is True


# ── require_flag decorator on a synthetic Flask app ────────────────────────


def _make_app_with_gated_route():
    app = Flask(__name__)

    @app.route("/secret")
    @require_flag("secret")
    def secret():
        return jsonify({"ok": True})

    return app


def test_require_flag_404s_when_off(monkeypatch):
    """OFF: not 503, not 501, not a CORS pre-flight — exactly 404."""
    monkeypatch.delenv("FF_SECRET", raising=False)
    app = _make_app_with_gated_route()
    resp = app.test_client().get("/secret")
    assert resp.status_code == 404


def test_require_flag_200_when_on(monkeypatch):
    monkeypatch.setenv("FF_SECRET", "1")
    app = _make_app_with_gated_route()
    resp = app.test_client().get("/secret")
    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True}


def test_require_flag_lookup_is_per_request(monkeypatch):
    """Toggling the env var between requests flips the route without
    rebuilding the app — covers the "restart-free" UX implicitly."""
    app = _make_app_with_gated_route()
    client = app.test_client()

    monkeypatch.setenv("FF_SECRET", "1")
    assert client.get("/secret").status_code == 200

    monkeypatch.setenv("FF_SECRET", "0")
    assert client.get("/secret").status_code == 404

    monkeypatch.setenv("FF_SECRET", "true")
    assert client.get("/secret").status_code == 200


def test_require_flag_attaches_marker():
    """The wrapper carries the flag name so admin tooling can introspect."""
    @require_flag("foo")
    def view():
        return "x"

    assert getattr(view, "_feature_flag") == "foo"


# ── snapshot / admin ───────────────────────────────────────────────────────


def test_snapshot_returns_known_flags(monkeypatch):
    monkeypatch.setenv("FF_FORECAST", "1")
    snap = snapshot()
    names = {row["name"] for row in snap}
    assert "forecast" in names
    forecast_row = next(r for r in snap if r["name"] == "forecast")
    assert forecast_row["env_key"] == "FF_FORECAST"
    assert forecast_row["enabled"] is True


def test_snapshot_off_when_env_missing(monkeypatch):
    monkeypatch.delenv("FF_FORECAST", raising=False)
    forecast_row = next(r for r in snapshot() if r["name"] == "forecast")
    assert forecast_row["enabled"] is False


# ── Integration: real /api/forecast + /admin/feature-flags ────────────────


@pytest.fixture
def app_(tmp_path, monkeypatch):
    """Full app with auth disabled (TESTING default) for endpoint tests."""
    import app.metrics_storage as storage
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "ff.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    return create_app({"TESTING": True})


def test_forecast_endpoint_404_when_flag_off(app_, monkeypatch):
    """The integration test the issue asks for explicitly: FF_FORECAST=false
    → /api/forecast/<table> returns 404, indistinguishable from a missing route."""
    monkeypatch.delenv("FF_FORECAST", raising=False)
    resp = app_.test_client().get("/api/forecast/users")
    assert resp.status_code == 404


def test_forecast_endpoint_reachable_when_flag_on(app_, monkeypatch):
    """Flag ON: the route is reachable. We don't care about the actual
    forecast logic here — anything other than 404 confirms the gate opened."""
    monkeypatch.setenv("FF_FORECAST", "1")
    resp = app_.test_client().get("/api/forecast/users")
    # The route can legitimately respond 422 (insufficient data — no metrics
    # seeded in this app fixture) or 200 (forecast computed). Both prove
    # the flag didn't intercept.
    assert resp.status_code != 404


def test_admin_feature_flags_lists_state(app_, monkeypatch):
    monkeypatch.setenv("FF_FORECAST", "1")
    resp = app_.test_client().get("/admin/feature-flags")
    assert resp.status_code == 200
    body = resp.get_json()
    assert isinstance(body, list)
    forecast = next(row for row in body if row["name"] == "forecast")
    assert forecast["env_key"] == "FF_FORECAST"
    assert forecast["enabled"] is True

    monkeypatch.delenv("FF_FORECAST", raising=False)
    body2 = app_.test_client().get("/admin/feature-flags").get_json()
    forecast2 = next(row for row in body2 if row["name"] == "forecast")
    assert forecast2["enabled"] is False
