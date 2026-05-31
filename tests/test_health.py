"""Tests for the structured /healthz endpoint (#100).

Covers the issue acceptance:
- All ok → 200 + status:"ok"
- monitor_db down → 503 + status:"degraded" + checks.monitor_db.status:"down"
- ratelimit_storage n/a default vs strict=true
- JSON shape (status / checks / version fields)
- Per-check timeout (1 s budget) is honored
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from app.app import create_app
from app.health import HEALTH_TIMEOUT_S, build_health_payload


@pytest.fixture
def health_app(tmp_path, monkeypatch):
    """App with a real SQLite metrics DB so the monitor_db ping succeeds.

    target_db is stubbed via monkeypatch on app.db.get_engine so the test
    doesn't depend on a real Postgres being reachable.
    """
    db_path = tmp_path / "health.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    # Stub the target-DB engine so SELECT 1 succeeds against the same
    # SQLite file (cheap to spin up, no Postgres dependency in unit tests).
    from sqlalchemy import create_engine
    stub_engine = create_engine(f"sqlite:///{db_path}")
    monkeypatch.setattr("app.db._engine", stub_engine)

    app = create_app({"TESTING": True})
    return app


@pytest.fixture
def client(health_app):
    return health_app.test_client()


# --- Happy path ------------------------------------------------------------


def test_healthz_returns_200_when_all_ok(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    body = resp.get_json()
    assert body["status"] == "ok"
    assert body["checks"]["monitor_db"]["status"] == "ok"
    assert body["checks"]["target_db"]["status"] == "ok"
    # RATELIMIT_STORAGE_URI not set in tests → memory:// → n/a
    assert body["checks"]["ratelimit_storage"]["status"] == "n/a"


def test_healthz_payload_shape(client):
    resp = client.get("/healthz")
    body = resp.get_json()
    assert set(body.keys()) == {"status", "checks", "version"}
    for name in ("monitor_db", "target_db", "ratelimit_storage"):
        assert name in body["checks"], f"check {name!r} missing"
    # Per-check fields
    monitor = body["checks"]["monitor_db"]
    assert "status" in monitor
    assert "elapsed_ms" in monitor
    assert isinstance(monitor["elapsed_ms"], int)


def test_healthz_version_field_is_a_string(client):
    body = client.get("/healthz").get_json()
    # APP_VERSION env unset → falls back to git SHA or "dev"
    assert isinstance(body["version"], str)
    assert body["version"]  # non-empty


def test_healthz_version_honours_app_version_env(client, monkeypatch):
    """#105: Docker release builds inject APP_VERSION=<tag> via Dockerfile
    ARG → ENV. /healthz must surface that exact value so an operator can
    confirm which image is running before a rollback.
    """
    # Reset the per-process cache so the new env var is picked up.
    import app.health as health_mod
    monkeypatch.setattr(health_mod, "_VERSION_CACHE", None)
    monkeypatch.setenv("APP_VERSION", "v9.9.9-test")
    body = client.get("/healthz").get_json()
    assert body["version"] == "v9.9.9-test"


# --- Failure modes ---------------------------------------------------------


def test_healthz_returns_503_when_monitor_db_down(client, monkeypatch):
    """A broken monitor_db engine makes status='degraded' and HTTP 503."""
    def boom(*_a, **_kw):
        raise RuntimeError("simulated metrics-store connection failure")
    # Patch where the health check imports it: late import in _check_monitor_db
    monkeypatch.setattr("app.metrics_storage.get_engine", boom)

    resp = client.get("/healthz")
    assert resp.status_code == 503
    body = resp.get_json()
    assert body["status"] == "degraded"
    assert body["checks"]["monitor_db"]["status"] == "down"
    assert "simulated metrics-store connection failure" in body["checks"]["monitor_db"]["error"]
    # Other checks still ran — target_db should still be ok.
    assert body["checks"]["target_db"]["status"] == "ok"


def test_healthz_returns_503_when_target_db_down(client, monkeypatch):
    def boom(*_a, **_kw):
        raise RuntimeError("target DB unreachable")
    monkeypatch.setattr("app.db.get_engine", boom)

    resp = client.get("/healthz")
    assert resp.status_code == 503
    assert resp.get_json()["checks"]["target_db"]["status"] == "down"


def test_healthz_returns_200_with_na_default(client):
    """RATELIMIT_STORAGE_URI=memory:// → ratelimit_storage n/a → still 200."""
    resp = client.get("/healthz")
    body = resp.get_json()
    assert resp.status_code == 200
    assert body["checks"]["ratelimit_storage"]["status"] == "n/a"


def test_healthz_strict_treats_na_as_failure(client):
    """`?strict=true` flips n/a → 503 for paranoid k8s liveness."""
    resp = client.get("/healthz?strict=true")
    assert resp.status_code == 503
    body = resp.get_json()
    assert body["status"] == "degraded"
    assert body["checks"]["ratelimit_storage"]["status"] == "n/a"


@pytest.mark.parametrize("flag", ["1", "true", "yes", "TRUE", "Yes"])
def test_strict_query_param_accepts_common_truthy_values(client, flag, monkeypatch):
    # Pin DB checks to "ok" — this test is about strict-param parsing, not
    # DB connectivity. Without this, a slow ThreadPoolExecutor startup in CI
    # can push a check past HEALTH_TIMEOUT_S and flip it to "down" → 503
    # regardless of strict, making the assertion trivially true for wrong reasons.
    monkeypatch.setattr("app.health._check_monitor_db", lambda: {"status": "ok"})
    monkeypatch.setattr("app.health._check_target_db", lambda: {"status": "ok"})
    resp = client.get(f"/healthz?strict={flag}")
    assert resp.status_code == 503  # strict=True + ratelimit_storage n/a → 503


@pytest.mark.parametrize("flag", ["0", "false", "no", "", "off", "blah"])
def test_strict_query_param_falsy_values_keep_200(client, flag, monkeypatch):
    # Pin DB checks to "ok" — this test is about strict-param parsing, not
    # DB connectivity. A flaky "down" from a slow CI thread would cause 503
    # even with strict=False, masking the real assertion (#127).
    monkeypatch.setattr("app.health._check_monitor_db", lambda: {"status": "ok"})
    monkeypatch.setattr("app.health._check_target_db", lambda: {"status": "ok"})
    resp = client.get(f"/healthz?strict={flag}")
    assert resp.status_code == 200


# --- Timeout enforcement ---------------------------------------------------


@contextmanager
def _patched_check(name: str, fn):
    with patch(f"app.health._check_{name}", side_effect=fn):
        yield


def test_check_timeout_is_enforced():
    """A slow probe (> HEALTH_TIMEOUT_S) is killed and reported as 'down'.

    Exercises ``build_health_payload`` directly because client routing
    adds noise; we want to measure the wall-clock from the check runner.
    """
    def slow(*_a, **_kw):
        time.sleep(HEALTH_TIMEOUT_S + 0.5)
        return {"status": "ok"}

    started = time.monotonic()
    with patch("app.health._check_monitor_db", side_effect=slow):
        payload, status = build_health_payload(
            strict=False, ratelimit_storage_uri="memory://",
        )
    elapsed = time.monotonic() - started

    assert status == 503
    assert payload["checks"]["monitor_db"]["status"] == "down"
    assert "timeout" in payload["checks"]["monitor_db"]["error"]
    # Total wall-clock should be close to the per-check budget, not 5 s.
    # We allow a generous 2 s ceiling — the executor cleanup itself takes
    # a few hundred ms in CI.
    assert elapsed < HEALTH_TIMEOUT_S + 2.0, f"endpoint hung for {elapsed:.2f}s"


# --- Header / route plumbing ----------------------------------------------


def test_healthz_does_not_require_login(client):
    """Anonymous GET — no redirect to /auth/login."""
    resp = client.get("/healthz", follow_redirects=False)
    assert resp.status_code != 302
    assert resp.status_code in (200, 503)


def test_healthz_response_is_json(client):
    resp = client.get("/healthz")
    assert resp.content_type.startswith("application/json")
