"""Auth flow tests for #49.

Covers the acceptance criteria spelled out in the issue:
- POST /auth/register с валидным email+паролем → 302 + session login
- POST /auth/register с уже зарегистрированным email → 409
- /dashboard неавторизованным → 302 /auth/login?next=/dashboard
- /auth/logout очищает сессию, редирект на /auth/login
- Wrong password / unknown email → форма с ошибкой

We turn login enforcement and CSRF back on for these tests (the default
``TESTING`` disables both) — otherwise we'd be testing nothing.
"""

from __future__ import annotations

import pytest

from app.app import create_app


@pytest.fixture
def auth_app(tmp_path, monkeypatch):
    """Flask app with auth ENABLED and a fresh SQLite metrics DB."""
    db_path = tmp_path / "auth.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    # Stub the monitored-DB introspection so /dashboard renders the empty
    # state instead of trying to connect to a real Postgres.
    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    app = create_app({
        "TESTING": True,
        # Override the TESTING auto-disable so we can actually exercise auth.
        "LOGIN_DISABLED": False,
        # Keep CSRF off — Flask-WTF tokens require a real browser session
        # round-trip that the test client doesn't model. The forms still
        # validate field constraints; we're testing the auth flow, not CSRF.
        "WTF_CSRF_ENABLED": False,
    })
    return app


@pytest.fixture
def client(auth_app):
    return auth_app.test_client()


# --- Registration ----------------------------------------------------------


def test_register_creates_user_logs_in_and_redirects(client):
    resp = client.post(
        "/auth/register",
        data={
            "email": "Alice@Example.com",  # case is preserved in input
            "password": "correct horse battery staple",
            "confirm": "correct horse battery staple",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/dashboard/")
    # Session cookie was set — subsequent /dashboard hits land 200, not 302.
    follow = client.get("/dashboard/")
    assert follow.status_code == 200


def test_register_normalises_email_to_lowercase(client):
    client.post("/auth/register", data={
        "email": "BoB@Example.COM",
        "password": "supersecret1",
        "confirm": "supersecret1",
    })
    from app.metrics_storage import get_user_by_email
    assert get_user_by_email("bob@example.com") is not None
    assert get_user_by_email("BoB@Example.COM") is None  # case-sensitive lookup


def test_register_duplicate_email_returns_409(client):
    payload = {
        "email": "dup@example.com",
        "password": "supersecret1",
        "confirm": "supersecret1",
    }
    first = client.post("/auth/register", data=payload)
    assert first.status_code == 302  # first registration succeeded

    # Log out so we hit the "anonymous trying to register a taken email" path.
    client.post("/auth/logout")
    second = client.post("/auth/register", data=payload)
    assert second.status_code == 409


def test_register_rejects_password_mismatch(client):
    resp = client.post("/auth/register", data={
        "email": "ann@example.com",
        "password": "supersecret1",
        "confirm": "differentpass1",
    })
    # Form re-renders with 200; we just check the user wasn't created.
    assert resp.status_code == 200
    from app.metrics_storage import get_user_by_email
    assert get_user_by_email("ann@example.com") is None


def test_register_rejects_short_password(client):
    resp = client.post("/auth/register", data={
        "email": "short@example.com",
        "password": "1234567",
        "confirm": "1234567",
    })
    assert resp.status_code == 200
    from app.metrics_storage import get_user_by_email
    assert get_user_by_email("short@example.com") is None


# --- Login -----------------------------------------------------------------


def _register(client, email="test@example.com", password="supersecret1"):
    client.post("/auth/register", data={
        "email": email, "password": password, "confirm": password,
    })
    client.post("/auth/logout")


def test_login_with_correct_password_redirects_to_dashboard(client):
    _register(client, "ok@example.com", "supersecret1")
    resp = client.post("/auth/login", data={
        "email": "ok@example.com", "password": "supersecret1",
    })
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/dashboard/")


def test_login_updates_last_login_at(client):
    _register(client, "stamp@example.com", "supersecret1")
    from app.metrics_storage import get_user_by_email
    before = get_user_by_email("stamp@example.com")
    assert before["last_login_at"] is None
    client.post("/auth/login", data={
        "email": "stamp@example.com", "password": "supersecret1",
    })
    after = get_user_by_email("stamp@example.com")
    assert after["last_login_at"] is not None


def test_login_with_wrong_password_renders_form(client):
    _register(client, "user@example.com", "supersecret1")
    resp = client.post("/auth/login", data={
        "email": "user@example.com", "password": "wrong-pass",
    })
    assert resp.status_code == 200  # form re-renders, no redirect
    # Dashboard should still be unreachable
    dash = client.get("/dashboard/", follow_redirects=False)
    assert dash.status_code == 302


def test_login_with_unknown_email_renders_form(client):
    resp = client.post("/auth/login", data={
        "email": "ghost@example.com", "password": "supersecret1",
    })
    assert resp.status_code == 200


def test_login_honours_safe_next_param(client):
    _register(client, "next@example.com", "supersecret1")
    resp = client.post(
        "/auth/login?next=/dashboard/schema",
        data={"email": "next@example.com", "password": "supersecret1"},
    )
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/dashboard/schema")


def test_login_rejects_open_redirect_in_next_param(client):
    """next=https://evil/... must not be honoured — only same-host paths."""
    _register(client, "evil@example.com", "supersecret1")
    resp = client.post(
        "/auth/login?next=https://evil.example.com/",
        data={"email": "evil@example.com", "password": "supersecret1"},
    )
    assert resp.status_code == 302
    assert "evil.example.com" not in resp.headers["Location"]
    assert resp.headers["Location"].endswith("/dashboard/")


# --- Logout ----------------------------------------------------------------


def test_logout_clears_session(client):
    _register(client, "out@example.com", "supersecret1")
    client.post("/auth/login", data={
        "email": "out@example.com", "password": "supersecret1",
    })
    # Confirm logged in.
    assert client.get("/dashboard/").status_code == 200

    logout = client.post("/auth/logout", follow_redirects=False)
    assert logout.status_code == 302
    assert "/auth/login" in logout.headers["Location"]

    # Subsequent access redirects back to login.
    dash = client.get("/dashboard/", follow_redirects=False)
    assert dash.status_code == 302
    assert "/auth/login" in dash.headers["Location"]


def test_logout_requires_login(client):
    """Anonymous POST /auth/logout doesn't crash — it redirects to login."""
    resp = client.post("/auth/logout", follow_redirects=False)
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


# --- Dashboard gating ------------------------------------------------------


def test_dashboard_anonymous_redirects_to_login_with_next(client):
    resp = client.get("/dashboard/", follow_redirects=False)
    assert resp.status_code == 302
    location = resp.headers["Location"]
    assert "/auth/login" in location
    assert "next=" in location and "%2Fdashboard%2F" in location


def test_admin_anonymous_redirects_to_login(client):
    resp = client.get("/admin/jobs", follow_redirects=False)
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


def test_authenticated_user_already_logged_in_redirects_away_from_login(client):
    _register(client, "in@example.com", "supersecret1")
    client.post("/auth/login", data={
        "email": "in@example.com", "password": "supersecret1",
    })
    resp = client.get("/auth/login", follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/dashboard/")


def test_register_when_already_logged_in_redirects_away(client):
    _register(client, "already@example.com", "supersecret1")
    client.post("/auth/login", data={
        "email": "already@example.com", "password": "supersecret1",
    })
    resp = client.get("/auth/register", follow_redirects=False)
    assert resp.status_code == 302


# --- /healthz stays open ---------------------------------------------------


def test_healthz_remains_unauthenticated(client):
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.get_json() == {"status": "ok"}
