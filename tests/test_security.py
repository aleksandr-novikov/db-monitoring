"""Tests for #56 — security hygiene helpers.

Covers:
- ``mask_dsn`` unit cases (with/without password, URL-encoded, edge cases)
- ``DSNFilter`` rewrites passwords inside log records (regression for the
  parser check in the issue acceptance)
- Rate limiter trips after the configured threshold on /login and /register
- Per-email lockout: 5 failed logins → 6th request gets 429 even with the
  rate limiter off
"""

from __future__ import annotations

import logging
import re

import pytest

from app.app import create_app
from app.security import DSNFilter, mask_dsn

# --- mask_dsn --------------------------------------------------------------


@pytest.mark.parametrize("dsn,expected", [
    (
        "postgresql://user:secret@host:5432/db",
        "postgresql://user:***@host:5432/db",
    ),
    (
        "postgresql+psycopg2://user:complex%21pwd@host/db",
        "postgresql+psycopg2://user:***@host/db",
    ),
    (
        "mysql+pymysql://u:p@h:3306/d?ssl=true",
        "mysql+pymysql://u:***@h:3306/d?ssl=true",
    ),
    (
        # No password → unchanged
        "postgresql://user@host/db",
        "postgresql://user@host/db",
    ),
    (
        # No userinfo at all → unchanged
        "sqlite:///monitor.db",
        "sqlite:///monitor.db",
    ),
    (
        # Non-URL string → returned as-is (calling on a stray value is safe)
        "just some text without a dsn",
        "just some text without a dsn",
    ),
    (
        "",
        "",
    ),
])
def test_mask_dsn_replaces_password_only(dsn, expected):
    assert mask_dsn(dsn) == expected


def test_mask_dsn_handles_supabase_pooler_form():
    """Supabase pooler URLs have dots in the username — must not split there."""
    src = "postgresql://postgres.proj:p4ssw0rd@aws-0-eu.pooler.supabase.com:5432/postgres"
    masked = mask_dsn(src)
    assert "p4ssw0rd" not in masked
    assert "postgres.proj:***@aws-0-eu.pooler.supabase.com" in masked


# --- DSNFilter -------------------------------------------------------------


def test_dsn_filter_scrubs_passwords_in_log_msg(caplog):
    """Regression for the acceptance: parser must not find `:password@`
    after `://user:` in any captured log line."""
    logger = logging.getLogger("test_dsn_scrub")
    logger.addFilter(DSNFilter())
    logger.setLevel(logging.INFO)

    with caplog.at_level(logging.INFO, logger="test_dsn_scrub"):
        logger.info("connecting to postgresql://admin:s3cret@db.example.com:5432/app")
        logger.info(
            "two dsns: %s and %s",
            "postgresql://a:b@h1/x", "mysql://c:d@h2/y",
        )

    full_log = "\n".join(r.getMessage() for r in caplog.records)
    # No raw password should survive (`s3cret`, `b`, `d`).
    assert "s3cret" not in full_log
    # The placeholder must appear in place of every original password.
    assert full_log.count("***") >= 3
    # The acceptance regex: parser should NOT find `://user:[^/@]+@`.
    assert not re.search(r"://[^/@\s:]+:[^*][^/@\s]*@", full_log), full_log


def test_dsn_filter_passes_non_dsn_strings_through(caplog):
    logger = logging.getLogger("test_dsn_passthrough")
    logger.addFilter(DSNFilter())
    logger.setLevel(logging.INFO)
    with caplog.at_level(logging.INFO, logger="test_dsn_passthrough"):
        logger.info("hello world, no secrets here")
        logger.info("contains a colon: 42")
    msgs = [r.getMessage() for r in caplog.records]
    assert msgs == ["hello world, no secrets here", "contains a colon: 42"]


# --- Rate limiting & lockout ------------------------------------------------


@pytest.fixture
def rate_app(tmp_path, monkeypatch):
    """App with auth enabled AND rate limiting / lockout enabled.

    We override the TESTING auto-disables so we can actually exercise the
    limits in tests.
    """
    db_path = tmp_path / "rate.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
        "RATELIMIT_ENABLED": True,
    })

    # Reset the Flask-Limiter storage between tests so per-IP counters
    # don't carry across; the limiter holds them in module-level state.
    # Done *after* init_app, otherwise `.storage` asserts on a None backend.
    with app.app_context():
        from app.auth import limiter
        limiter.reset()
    return app


@pytest.fixture
def rate_client(rate_app):
    return rate_app.test_client()


def _register(client, email="rl@example.com", password="supersecret1"):
    client.post("/auth/register", data={
        "email": email, "password": password, "confirm": password,
    })
    client.post("/auth/logout")


def test_login_rate_limit_returns_429_after_threshold(rate_client):
    """11th POST in a minute (limit: 10/min) gets 429."""
    _register(rate_client, "limited@example.com", "supersecret1")
    # 10 attempts allowed; we use wrong passwords so we don't trip lockout
    # at the same time (lockout threshold is 5 — see lockout test below;
    # by mixing emails we keep this test focused on per-IP).
    statuses = []
    for i in range(11):
        resp = rate_client.post("/auth/login", data={
            # Each request uses a unique email so per-email lockout doesn't
            # fire — we want to isolate the per-IP rate-limit signal here.
            "email": f"u{i}@example.com",
            "password": "wrong",
        })
        statuses.append(resp.status_code)
    # First 10 → 200 (form re-renders with error); 11th → 429.
    assert statuses[-1] == 429, statuses


def test_register_rate_limit_returns_429_after_threshold(rate_client):
    """6th register in a minute (limit: 5/min) gets 429."""
    statuses = []
    for i in range(6):
        resp = rate_client.post("/auth/register", data={
            "email": f"r{i}@example.com",
            "password": "supersecret1",
            "confirm": "supersecret1",
        })
        statuses.append(resp.status_code)
    assert statuses[-1] == 429, statuses


def test_per_email_lockout_after_five_failed_attempts(rate_client):
    """5 wrong-password attempts on one email → 6th → 429 + lockout message.

    Distinct from the per-IP rate limit: even if the attacker rotates IPs,
    the same email keeps getting rejected for 15 minutes.
    """
    _register(rate_client, "lock@example.com", "supersecret1")

    # Disable the per-IP rate limit for this test by raising the threshold
    # would be ideal, but the limiter is decorator-baked. Instead we keep
    # to 5 wrong-password POSTs (under the 10/min IP limit) and verify the
    # *email-level* counter triggers.
    statuses = []
    for _ in range(5):
        r = rate_client.post("/auth/login", data={
            "email": "lock@example.com", "password": "wrong-pass",
        })
        statuses.append(r.status_code)

    # 6th attempt — now blocked by lockout (even with the correct password!).
    r = rate_client.post("/auth/login", data={
        "email": "lock@example.com", "password": "supersecret1",
    })
    assert r.status_code == 429, (statuses, r.status_code)


def test_successful_login_clears_failed_attempts(rate_client):
    """A correct password mid-streak resets the email's failure counter."""
    _register(rate_client, "reset@example.com", "supersecret1")

    # 4 bad attempts (one below the threshold)
    for _ in range(4):
        rate_client.post("/auth/login", data={
            "email": "reset@example.com", "password": "wrong",
        })

    # Correct login — clears the slate
    r = rate_client.post("/auth/login", data={
        "email": "reset@example.com", "password": "supersecret1",
    })
    assert r.status_code == 302
    rate_client.post("/auth/logout")

    # Now another 4 bad attempts must still be allowed (counter was reset
    # by the successful login). If clear_failed_logins didn't run, the
    # 4 old + 1 new attempts would lock the account on attempt #2 here.
    statuses = []
    for _ in range(4):
        r = rate_client.post("/auth/login", data={
            "email": "reset@example.com", "password": "wrong",
        })
        statuses.append(r.status_code)
    assert all(s == 200 for s in statuses), statuses
