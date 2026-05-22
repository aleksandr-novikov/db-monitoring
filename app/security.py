"""Security helpers (#56).

- ``mask_dsn`` — scrub the password from a SQLAlchemy/PostgreSQL-style URL
  for safe logging and UI display.
- ``DSNFilter`` — a ``logging.Filter`` that walks log records and rewrites
  any plaintext DSN found in the message or args via ``mask_dsn``.
- ``init_logging_filter(logger)`` — install ``DSNFilter`` on a logger and
  its handlers.

Keeping these in one module makes the audit story simple: "what touches
secrets in logs?" → grep this file.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import urlparse, urlunparse

__all__ = ["DSNFilter", "init_logging_filter", "mask_dsn"]

_PASSWORD_PLACEHOLDER = "***"

# Fallback regex for DSN substrings inside larger log messages (where the
# whole record.msg isn't itself a URL). Matches `scheme://user:secret@host`
# and rewrites only the password part.
#
# Password class: `[^@/]+?` (non-greedy, only ``@`` and ``/`` bound it).
# We explicitly do NOT exclude `\s` from the password — otherwise a
# password that contains a literal newline (or any control char) survives
# the scrub. Real DSNs almost never have whitespace in passwords, but
# stack traces / multiline log messages can wrap awkwardly; better to
# over-mask than to leak.
_DSN_IN_TEXT = re.compile(
    r"(?P<prefix>[a-zA-Z][a-zA-Z0-9+\-.]*://[^:/@\s]+:)"
    r"(?P<password>[^@/]+?)"
    r"(?P<suffix>@)"
)


def mask_dsn(url: str) -> str:
    """Return *url* with the password component replaced by ``***``.

    Accepts any URL with a userinfo password (``scheme://user:pw@host/...``).
    URLs without a password are returned unchanged. Non-URLs (no scheme +
    netloc) are also returned unchanged — calling this on a stray string
    must be safe.

    Bytes input is returned unchanged: we don't decode arbitrary log
    payloads (encoding might be wrong), and any DSN that lands in a log
    line will go through ``DSNFilter._scrub`` which only touches str args.
    """
    if not isinstance(url, str):
        return url
    if not url or "://" not in url:
        return url
    try:
        parsed = urlparse(url)
    except (ValueError, AttributeError):
        return url
    if not parsed.netloc or "@" not in parsed.netloc:
        return url
    userinfo, host = parsed.netloc.rsplit("@", 1)
    if ":" not in userinfo:
        return url  # username only, no password to mask
    user, _ = userinfo.split(":", 1)
    masked_netloc = f"{user}:{_PASSWORD_PLACEHOLDER}@{host}"
    return urlunparse(parsed._replace(netloc=masked_netloc))


def _scrub(value):
    """Recursively rewrite DSN passwords inside any string/tuple/dict/list."""
    if isinstance(value, str):
        return _DSN_IN_TEXT.sub(
            lambda m: f"{m.group('prefix')}{_PASSWORD_PLACEHOLDER}{m.group('suffix')}",
            value,
        )
    if isinstance(value, tuple):
        return tuple(_scrub(v) for v in value)
    if isinstance(value, list):
        return [_scrub(v) for v in value]
    if isinstance(value, dict):
        return {k: _scrub(v) for k, v in value.items()}
    return value


class DSNFilter(logging.Filter):
    """Strip plaintext DSN passwords from log records.

    The filter rewrites ``record.msg`` and ``record.args`` before any
    formatter sees them, so it works regardless of which handler the
    record ends up on. Returns ``True`` (the record is always allowed
    through — we only mutate it).
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.msg, str):
            record.msg = _scrub(record.msg)
        if record.args:
            record.args = _scrub(record.args)
        return True


def init_logging_filter(logger: logging.Logger | None = None) -> DSNFilter:
    """Install ``DSNFilter`` on a logger and its already-attached handlers.

    Defaults to the root logger so every application-emitted line is
    covered. Returns the filter instance for tests / introspection.
    """
    logger = logger if logger is not None else logging.getLogger()
    f = DSNFilter()
    logger.addFilter(f)
    for handler in logger.handlers:
        handler.addFilter(f)
    return f
