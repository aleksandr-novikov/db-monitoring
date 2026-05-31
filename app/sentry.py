"""Sentry initialisation + scrubbing (#103).

Init is **gated on ``SENTRY_DSN``** — empty value means the SDK is never
loaded, so dev/CI never spam the project quota. Production gets the
FlaskIntegration (auto request/response context, breadcrumbs, exception
capture).

Two scrubbing layers stack with the existing #56 ``setLogRecordFactory``:

1. ``scrub_value`` (from ``app.security``) walks the entire event dict
   recursively, rewriting any PostgreSQL/MySQL/ClickHouse DSN found
   inside strings AND any Telegram bot token from #143. Uses a regex
   substitution (not ``urlparse``) so a DSN buried in a source-code
   context line or an exception message is still caught.
2. A keyword-based scrub flips any dict value whose key matches
   ``password`` / ``token`` / ``api_key`` (case-insensitive) to a fixed
   placeholder — protects ``extra={"api_key": "…"}`` patterns.

Both run on every event in ``before_send``.
"""

from __future__ import annotations

import logging
from typing import Any

from app.config import settings
from app.security import scrub_value

logger = logging.getLogger(__name__)

# Substring match on dict keys. Anything containing these tokens (case-
# insensitive) gets its value redacted. Conservative on purpose — false
# positives are cheap (a tag with "tokenizer" in the name loses its value
# in Sentry, but Sentry is for traces, not for ML pipeline tags).
_SECRET_KEY_HINTS = ("password", "token", "secret", "api_key", "authorization")
_REDACTED = "[REDACTED]"


def _key_is_secret(key: Any) -> bool:
    if not isinstance(key, str):
        return False
    lowered = key.lower()
    return any(hint in lowered for hint in _SECRET_KEY_HINTS)


def _scrub(value: Any) -> Any:
    """Recursively rewrite DSNs and redact secret-named dict values.

    Order matters: we redact suspicious keys BEFORE recursing into their
    values, so a ``{"password": "<long DSN-shaped string>"}`` doesn't
    leak the DSN even though ``scrub_value`` would also handle it.
    For pure strings we delegate to ``security.scrub_value`` — its regex
    finds DSNs inside larger blobs (source-code context lines, traceback
    snippets) where ``urlparse``-based ``mask_dsn`` doesn't.
    """
    if isinstance(value, dict):
        scrubbed = {}
        for k, v in value.items():
            if _key_is_secret(k):
                scrubbed[k] = _REDACTED
            else:
                scrubbed[k] = _scrub(v)
        return scrubbed
    if isinstance(value, list):
        return [_scrub(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_scrub(v) for v in value)
    # str / BaseException / anything else: defer to security.scrub_value
    # which handles strings via regex AND stringifies exceptions safely.
    return scrub_value(value)


def before_send(event: dict, _hint: dict) -> dict | None:
    """Sentry ``before_send`` hook — sanitises every outgoing event.

    Never drops events (returns the scrubbed dict, not None) — operators
    rely on the event count for monitoring. Returning None would silently
    eat the error.
    """
    try:
        return _scrub(event)
    except Exception:  # pragma: no cover - defence in depth
        # If scrubbing itself fails for some pathological event shape,
        # we still want SOMETHING in Sentry — better a less-clean event
        # than no signal at all.
        logger.exception("Sentry before_send scrub failed; sending raw event")
        return event


def init_sentry() -> bool:
    """Initialise the Sentry SDK if ``SENTRY_DSN`` is configured.

    Returns True on initialisation, False when DSN is empty (intended
    for dev / CI). Safe to call multiple times — ``sentry_sdk.init`` is
    itself idempotent within a single process.
    """
    dsn = settings.SENTRY_DSN.strip()
    if not dsn:
        return False

    # Late import: keep the cold-start cost of the auth/main modules low,
    # and let environments without sentry-sdk installed run the rest of
    # the app without crashing at import time.
    try:
        import sentry_sdk
        from sentry_sdk.integrations.flask import FlaskIntegration
    except ImportError:  # pragma: no cover - dependency missing in slim builds
        logger.warning(
            "SENTRY_DSN set but sentry-sdk not installed; skipping init"
        )
        return False

    sentry_sdk.init(
        dsn=dsn,
        environment=settings.SENTRY_ENVIRONMENT or settings.FLASK_ENV,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
        profiles_sample_rate=settings.SENTRY_PROFILES_SAMPLE_RATE,
        integrations=[FlaskIntegration()],
        before_send=before_send,
        # Don't auto-attach the request body — bodies can contain DSN
        # input from the connections form (#51) and our scrub runs only
        # on event fields, not the raw body capture. send_default_pii
        # also drops User-Agent / cookies / IP from request context.
        send_default_pii=False,
        # Sentry SDK 2.x renamed request_bodies → max_request_body_size.
        # "never" — don't capture POST bodies at all.
        max_request_body_size="never",
        max_breadcrumbs=50,
    )
    logger.info("Sentry initialised (environment=%s)",
                settings.SENTRY_ENVIRONMENT or settings.FLASK_ENV)
    return True


__all__ = ["before_send", "init_sentry"]
