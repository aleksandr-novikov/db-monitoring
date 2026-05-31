"""Sentry init + scrubbing tests (#103).

We don't talk to Sentry's network — ``sentry_sdk.init`` accepts a custom
``transport`` callable, so we capture outgoing envelopes locally and
assert on them. That covers the full path: init → exception captured →
before_send scrub → transport.send.
"""

from __future__ import annotations

import logging

import pytest

from app.sentry import _scrub, before_send, init_sentry

# ── before_send / _scrub unit tests ────────────────────────────────────────


def test_scrub_masks_dsn_in_string():
    """Top-level string with a DSN gets the password redacted."""
    out = _scrub("postgresql://user:topsecret@host/db")
    assert "topsecret" not in out
    assert "user:***@host" in out


def test_scrub_recurses_into_dict():
    event = {
        "extra": {"connection": "postgresql://u:hidden@h:5432/d"},
    }
    out = _scrub(event)
    assert "hidden" not in str(out)


def test_scrub_redacts_secret_keyed_dict_values():
    """Keys containing password/token/secret/api_key/authorization
    have their values wholesale replaced — even before recursing."""
    event = {
        "extra": {
            "password": "Pa$$w0rd-1234",
            "api_key": "sk-1234",
            "bearer_token": "Bearer eyJ…",
            "AUTHORIZATION": "Basic dXNlcjo=",
            "username": "alice",  # NOT secret — preserved
        },
    }
    out = _scrub(event)
    assert out["extra"]["password"] == "[REDACTED]"
    assert out["extra"]["api_key"] == "[REDACTED]"
    assert out["extra"]["bearer_token"] == "[REDACTED]"
    assert out["extra"]["AUTHORIZATION"] == "[REDACTED]"
    assert out["extra"]["username"] == "alice"


def test_scrub_handles_list_and_tuple():
    event = {"breadcrumbs": [
        "no secret here",
        "postgresql://u:leaked@h/d",
        ("nested", "postgresql://u:tupled@h/d"),
    ]}
    out = _scrub(event)
    serialised = str(out)
    assert "leaked" not in serialised
    assert "tupled" not in serialised


def test_before_send_returns_dict_not_none():
    """We never drop events — scrub-or-pass-through, never silent drop."""
    out = before_send({"extra": {"trace": "ok"}}, {})
    assert out is not None
    assert isinstance(out, dict)


def test_before_send_swallows_scrub_errors(monkeypatch):
    """Pathological event that breaks _scrub still ships SOMETHING."""
    def boom(_):
        raise RuntimeError("scrub broke")

    monkeypatch.setattr("app.sentry._scrub", boom)
    event = {"raw": "data"}
    out = before_send(event, {})
    # Falls back to the raw event — operator sees the error, just less clean.
    assert out is event


# ── init_sentry behaviour ──────────────────────────────────────────────────


def test_init_sentry_noop_when_dsn_empty(monkeypatch):
    """Empty SENTRY_DSN → don't even import sentry_sdk, return False."""
    from app.config import settings
    monkeypatch.setattr(settings, "SENTRY_DSN", "")
    assert init_sentry() is False


def test_init_sentry_initialises_when_dsn_present(monkeypatch):
    """With a DSN configured we actually call sentry_sdk.init."""
    from app.config import settings as cfg
    monkeypatch.setattr(cfg, "SENTRY_DSN",
                        "https://public@sentry.example.com/1")
    monkeypatch.setattr(cfg, "SENTRY_ENVIRONMENT", "test-env")

    captured = {}

    def fake_init(**kwargs):
        captured.update(kwargs)

    import sentry_sdk
    monkeypatch.setattr(sentry_sdk, "init", fake_init)

    assert init_sentry() is True
    assert captured["dsn"] == "https://public@sentry.example.com/1"
    assert captured["environment"] == "test-env"
    assert callable(captured["before_send"])
    assert captured["send_default_pii"] is False


def test_init_sentry_against_real_sdk_does_not_crash(monkeypatch):
    """Regression: ``sentry_sdk.init`` rejects unknown options with a
    TypeError. Tests that monkeypatch ``init`` away never exercise the
    real option whitelist, so SDK 2.x renames (e.g. ``request_bodies`` →
    ``max_request_body_size``) slip through. This test calls the REAL
    init and asserts it returns cleanly — if a future SDK bump removes
    or renames an option we use, this fails immediately.
    """
    from app.config import settings as cfg
    monkeypatch.setattr(cfg, "SENTRY_DSN",
                        "https://public@sentry.example.com/1")
    monkeypatch.setattr(cfg, "SENTRY_ENVIRONMENT", "test-real-init")
    # The DSN is fake — no events will ever be sent (the SDK only
    # validates DSN format, not reachability, during init).
    assert init_sentry() is True


# ── End-to-end with fake transport ─────────────────────────────────────────


def _make_fake_transport(events: list):
    """Build a Transport subclass that captures every envelope's event
    JSON into ``events``. Sentry 2.x dropped the "transport callable"
    shim, so we have to subclass ``sentry_sdk.transport.Transport``.
    """
    from sentry_sdk.transport import Transport

    class _Captor(Transport):
        def __init__(self, options=None):
            super().__init__(options or {})

        def capture_envelope(self, envelope):
            for item in envelope.items:
                payload = item.payload
                # Item payloads can be raw bytes or a JsonPayload object
                # depending on the item type — try both.
                if hasattr(payload, "json"):
                    events.append(payload.json)

        def flush(self, *_a, **_kw):
            pass

        def kill(self, *_a, **_kw):
            pass

    return _Captor


def test_dsn_password_is_scrubbed_in_captured_event():
    """Exception with a DSN in the message → Sentry sees the masked form."""
    import sentry_sdk

    events: list[dict] = []
    sentry_sdk.init(
        dsn="https://public@sentry.example.com/1",
        transport=_make_fake_transport(events),
        before_send=before_send,
        send_default_pii=False,
        traces_sample_rate=0.0,
        # Don't auto-attach the Flask integration — this test is about
        # the scrub itself, not request context capture.
        integrations=[],
    )
    try:
        raise RuntimeError("connect failed: postgresql://u:topsecret@h:5432/d")
    except RuntimeError:
        sentry_sdk.capture_exception()

    sentry_sdk.flush(timeout=2)

    assert events, "expected at least one event captured"
    blob = repr(events[-1])
    assert "topsecret" not in blob
    assert "u:***@h" in blob


def test_password_key_in_extra_is_redacted_end_to_end():
    """capture_message with secret-named scope extras → Sentry event
    has REDACTED placeholders, not the raw values."""
    import sentry_sdk

    events: list[dict] = []
    sentry_sdk.init(
        dsn="https://public@sentry.example.com/1",
        transport=_make_fake_transport(events),
        before_send=before_send,
        send_default_pii=False,
        traces_sample_rate=0.0,
        integrations=[],
    )
    with sentry_sdk.new_scope() as scope:
        scope.set_extra("password", "Pa$$w0rd-1234")
        scope.set_tag("api_key", "sk-secret")
        sentry_sdk.capture_message("test event")

    sentry_sdk.flush(timeout=2)

    assert events
    blob = repr(events[-1])
    assert "Pa$$w0rd-1234" not in blob
    assert "sk-secret" not in blob
    assert "[REDACTED]" in blob


# Suppress noisy ``app.sentry`` log lines from polluting unrelated test output.
@pytest.fixture(autouse=True)
def _quiet_sentry_logger():
    logging.getLogger("app.sentry").setLevel(logging.ERROR)
    yield
