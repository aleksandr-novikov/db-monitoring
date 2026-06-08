"""Tests for the password-reset flow (#133).

Covers acceptance criteria from the issue:
- DB stores only token_hash, raw token never persisted
- Token expires after 1h, expired tokens reject with generic error
- Token is one-time (consume is atomic; double-POST fails the second time)
- Invalid / expired / used tokens all yield the same generic error
- New forgot-password request invalidates earlier active tokens
- Successful reset invalidates all other active tokens of the user
- After reset, old password no longer works
- /forgot-password responds the same for known/unknown email
  (no leak of email existence)
- Rate limit on /forgot-password binds to (email, IP)
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

import app.email as email_mod
from app.auth import _hash_reset_token


@pytest.fixture
def app_(tmp_path, monkeypatch):
    """Auth ENABLED app with a fresh metrics SQLite + memory email backend."""
    import app.metrics_storage as storage
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "pwreset.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    # Memory backend — SMTP_HOST empty → app.email captures into outbox.
    monkeypatch.setattr(cfg, "SMTP_HOST", "")
    monkeypatch.setattr(cfg, "APP_BASE_URL", "http://test.local")

    # Stub the monitored-DB introspection so /dashboard renders the empty
    # state instead of trying to connect to a real Postgres after login.
    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    email_mod.clear_outbox()

    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })
    return app


@pytest.fixture
def client(app_):
    return app_.test_client()


def _register(client, email="user@example.com", password="supersecret1"):
    client.post("/auth/register", data={
        "email": email, "password": password, "confirm": password,
    })
    client.post("/auth/logout")


# ── Storage layer ──────────────────────────────────────────────────────────

def test_token_hash_is_stored_not_raw(client):
    """Raw token never lands in the DB — only its HMAC-SHA256(SECRET_KEY, …)."""
    from sqlalchemy import text

    from app.metrics_storage import get_engine

    _register(client, "raw@example.com")
    resp = client.post("/auth/forgot-password",
                       data={"email": "raw@example.com"})
    assert resp.status_code == 302  # redirect to /auth/login

    # Pull the raw token out of the email we just "sent".
    assert email_mod.outbox, "expected a reset email"
    msg = email_mod.outbox[-1]
    assert "/auth/reset-password/" in msg.body
    raw_token = msg.body.split("/auth/reset-password/", 1)[1].split()[0]

    with get_engine().connect() as conn:
        rows = conn.execute(text(
            "SELECT token_hash FROM password_reset_tokens"
        )).fetchall()
    assert len(rows) == 1
    stored_hash = rows[0][0]
    assert stored_hash != raw_token
    # Hash matches what the route computes.
    assert stored_hash == _hash_reset_token(raw_token)


def test_token_consume_is_one_time(client):
    """Two concurrent POSTs with the same token: second one fails — atomic
    UPDATE with used_at IS NULL guard."""
    from app.metrics_storage import consume_password_reset_token

    _register(client, "once@example.com")
    client.post("/auth/forgot-password", data={"email": "once@example.com"})
    raw_token = _extract_token_from_last_email()

    h = _hash_reset_token(raw_token)
    first = consume_password_reset_token(h)
    second = consume_password_reset_token(h)
    assert first is not None
    assert second is None


def test_expired_token_rejected(client):
    """A token whose expires_at has passed yields the generic error,
    not the form."""
    from sqlalchemy import text

    from app.metrics_storage import get_engine

    _register(client, "expired@example.com")
    client.post("/auth/forgot-password",
                data={"email": "expired@example.com"})
    raw_token = _extract_token_from_last_email()

    # Backdate the token to 2 hours ago — past the 1h TTL.
    past = (datetime.now(UTC) - timedelta(hours=2)).isoformat(timespec="seconds")
    with get_engine().begin() as conn:
        conn.execute(text(
            "UPDATE password_reset_tokens SET expires_at = :ts"
        ), {"ts": past})

    resp = client.get(f"/auth/reset-password/{raw_token}")
    assert resp.status_code == 400
    body = resp.get_data(as_text=True)
    assert "Ссылка недействительна или истекла" in body


def test_invalid_token_returns_same_generic_error(client):
    """Garbage token → same error as expired/used. No oracle."""
    resp = client.get("/auth/reset-password/totally-bogus-token-xxxxxx")
    assert resp.status_code == 400
    body = resp.get_data(as_text=True)
    assert "Ссылка недействительна или истекла" in body


# ── Route behaviour ────────────────────────────────────────────────────────

def test_forgot_unknown_email_still_returns_302_no_email_sent(client):
    """No leak of email existence — same redirect, just nothing actually sent."""
    resp = client.post("/auth/forgot-password",
                       data={"email": "ghost@example.com"})
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]
    assert email_mod.outbox == []


def test_forgot_known_email_sends_message(client):
    _register(client, "real@example.com")
    resp = client.post("/auth/forgot-password",
                       data={"email": "real@example.com"})
    assert resp.status_code == 302
    assert len(email_mod.outbox) == 1
    msg = email_mod.outbox[-1]
    assert msg.to == "real@example.com"
    assert "DB Monitor" in msg.subject
    # Link is built from APP_BASE_URL, not request.host.
    assert msg.body.startswith("Здравствуйте")
    assert "http://test.local/auth/reset-password/" in msg.body


def test_send_email_memory_backend_logs_warning(client, caplog):
    from app.config import settings as cfg

    cfg.SMTP_HOST = ""

    with caplog.at_level("WARNING"):
        assert email_mod.send_email("pii@example.com", "Subject", "Body") is True

    assert "SMTP not configured; using memory email backend" in caplog.text
    assert "pii@example.com" not in caplog.text
    assert "Body" not in caplog.text


def test_forgot_password_same_response_known_unknown(client):
    _register(client, "registered@example.com")

    known = client.post("/auth/forgot-password",
                        data={"email": "registered@example.com"})
    unknown = client.post("/auth/forgot-password",
                          data={"email": "ghost@example.com"})

    assert known.status_code == unknown.status_code == 302
    assert known.headers["Location"] == unknown.headers["Location"]


def test_forgot_password_logs_warning_no_pii(client, caplog):
    _register(client, "warn@example.com")

    with caplog.at_level("WARNING"):
        resp = client.post("/auth/forgot-password",
                           data={"email": "warn@example.com"})

    assert resp.status_code == 302
    assert "forgot-password: SMTP not configured" in caplog.text
    assert "warn@example.com" not in caplog.text


def test_smtp_send_calls_smtplib(client, monkeypatch):
    from app.config import settings as cfg

    monkeypatch.setattr(cfg, "SMTP_HOST", "smtp.example.com")
    monkeypatch.setattr(cfg, "SMTP_PORT", 587)
    monkeypatch.setattr(cfg, "SMTP_USER", "user")
    monkeypatch.setattr(cfg, "SMTP_PASSWORD", "pass")
    monkeypatch.setattr(cfg, "SMTP_USE_TLS", True)

    smtp = MagicMock()
    smtp_cls = MagicMock()
    smtp_cls.return_value.__enter__.return_value = smtp

    with patch("app.email.smtplib.SMTP", smtp_cls):
        assert email_mod.send_email("to@example.com", "Subject", "Body") is True

    smtp_cls.assert_called_once_with("smtp.example.com", 587, timeout=10)
    smtp.starttls.assert_called_once()
    smtp.login.assert_called_once_with("user", "pass")
    smtp.send_message.assert_called_once()


def test_forgot_new_request_invalidates_previous_tokens(client):
    """Second /forgot-password should make the first link unusable."""
    from app.metrics_storage import consume_password_reset_token

    _register(client, "two@example.com")
    client.post("/auth/forgot-password", data={"email": "two@example.com"})
    first_token = _extract_token_from_last_email()

    client.post("/auth/forgot-password", data={"email": "two@example.com"})
    second_token = _extract_token_from_last_email()

    # The first token must not consume.
    assert consume_password_reset_token(_hash_reset_token(first_token)) is None
    # The second one still works.
    assert consume_password_reset_token(_hash_reset_token(second_token)) is not None


def test_successful_reset_changes_password(client):
    _register(client, "reset@example.com", password="old-password-1")
    client.post("/auth/forgot-password", data={"email": "reset@example.com"})
    raw_token = _extract_token_from_last_email()

    resp = client.post(f"/auth/reset-password/{raw_token}", data={
        "password": "new-password-2", "confirm": "new-password-2",
    })
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]

    # Old password no longer logs in.
    bad = client.post("/auth/login", data={
        "email": "reset@example.com", "password": "old-password-1",
    })
    assert bad.status_code == 200  # form re-renders with error
    assert client.get("/dashboard/", follow_redirects=False).status_code == 302

    # New one does.
    ok = client.post("/auth/login", data={
        "email": "reset@example.com", "password": "new-password-2",
    })
    assert ok.status_code == 302
    assert client.get("/dashboard/").status_code == 200


def test_successful_reset_invalidates_other_active_tokens(client):
    """User who issued multiple resets and used one — the other must die."""
    from app.metrics_storage import consume_password_reset_token

    _register(client, "multi@example.com", password="orig-password-1")

    # Send two forgot-password requests with a small interval — both create
    # tokens, but the second invalidates the first. Issue a THIRD after that
    # to get a fresh second active token without the auto-invalidation.
    client.post("/auth/forgot-password", data={"email": "multi@example.com"})
    # Manually mint a second active token by directly writing through storage,
    # bypassing the /forgot route (which would auto-invalidate).
    from app.metrics_storage import create_password_reset_token, get_user_by_email
    user = get_user_by_email("multi@example.com")
    parallel_raw = "second-token-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    create_password_reset_token(
        user_id=user["id"],
        token_hash=_hash_reset_token(parallel_raw),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
    )

    primary_raw = _extract_token_from_last_email()

    # Successfully reset via the primary token.
    resp = client.post(f"/auth/reset-password/{primary_raw}", data={
        "password": "fresh-password-2", "confirm": "fresh-password-2",
    })
    assert resp.status_code == 302

    # The parallel token must no longer work — invalidated as a side effect.
    assert consume_password_reset_token(_hash_reset_token(parallel_raw)) is None


def test_used_token_rejected_with_generic_error(client):
    """After successful reset, the same link returns the generic error
    (NOT 'token already used' — that would be an oracle)."""
    _register(client, "used@example.com", password="first-pass-1")
    client.post("/auth/forgot-password", data={"email": "used@example.com"})
    raw_token = _extract_token_from_last_email()

    client.post(f"/auth/reset-password/{raw_token}", data={
        "password": "second-pass-2", "confirm": "second-pass-2",
    })
    # Replay attempt.
    resp = client.get(f"/auth/reset-password/{raw_token}")
    assert resp.status_code == 400
    assert "Ссылка недействительна или истекла" in resp.get_data(as_text=True)


def test_forgot_password_link_on_login_page(client):
    """Acceptance: 'Забыли пароль?' link is visible on /auth/login."""
    resp = client.get("/auth/login")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Забыли пароль" in body
    assert "/auth/forgot-password" in body


def test_authed_user_redirected_away_from_forgot_password(client):
    """Already logged-in users go to /dashboard instead of seeing the form."""
    _register(client, "in@example.com")
    client.post("/auth/login", data={
        "email": "in@example.com", "password": "supersecret1",
    })
    resp = client.get("/auth/forgot-password", follow_redirects=False)
    assert resp.status_code == 302
    assert "/dashboard" in resp.headers["Location"]


def test_authed_user_redirected_away_from_reset_password(client):
    _register(client, "in2@example.com")
    client.post("/auth/login", data={
        "email": "in2@example.com", "password": "supersecret1",
    })
    resp = client.get("/auth/reset-password/anything",
                      follow_redirects=False)
    assert resp.status_code == 302
    assert "/dashboard" in resp.headers["Location"]


# ── Rate limit (smoke) ─────────────────────────────────────────────────────

def test_forgot_password_rate_limit(app_, monkeypatch):
    """1/minute per (email, IP) — the 2nd consecutive POST hits 429."""
    # Re-create app with rate limiting ON (TESTING usually disables it).
    from app.app import create_app
    from app.config import settings as cfg

    monkeypatch.setattr(cfg, "SMTP_HOST", "")
    monkeypatch.setattr(cfg, "APP_BASE_URL", "http://test.local")

    app2 = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
        "RATELIMIT_ENABLED": True,
        "RATELIMIT_STORAGE_URI": "memory://",
    })
    c2 = app2.test_client()
    # Same email, same IP (test client) — second POST should 429.
    r1 = c2.post("/auth/forgot-password", data={"email": "rate@example.com"})
    r2 = c2.post("/auth/forgot-password", data={"email": "rate@example.com"})
    assert r1.status_code == 302
    assert r2.status_code == 429


# ── Helpers ────────────────────────────────────────────────────────────────

def _extract_token_from_last_email() -> str:
    assert email_mod.outbox, "expected an email in the outbox"
    body = email_mod.outbox[-1].body
    marker = "/auth/reset-password/"
    assert marker in body
    return body.split(marker, 1)[1].split()[0]
