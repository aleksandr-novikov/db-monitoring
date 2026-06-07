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

__all__ = ["DSNFilter", "init_logging_filter", "mask_dsn", "scrub_value"]

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

# Telegram bot token (#143). Format from BotFather: <bot_id>:<hash> where
# bot_id is 8–12 digits and hash is exactly 35 chars from [A-Za-z0-9_-].
# Spec is stable since 2015; we match conservatively with word boundaries
# so substrings of unrelated hex blobs don't trigger.
#
# Why scrub these? Any caller can log "token=%s" with the plaintext bot
# token at debug time; leaking the token lets anyone impersonate the bot
# and exfiltrate notifications to attacker-controlled chats.
_TELEGRAM_TOKEN_IN_TEXT = re.compile(
    r"\b(?P<bot_id>\d{8,12}):(?P<hash>[A-Za-z0-9_\-]{35})\b"
)

# AWS Access Key ID (#231). Format: AKIA[0-9A-Z]{16} (regular keys),
# ASIA[0-9A-Z]{16} (temporary STS keys). Spec is stable — Amazon documents
# the prefixes как identity. Match as a whole token so substrings of unrelated
# uppercase hex blobs don't trigger.
_AWS_ACCESS_KEY_IN_TEXT = re.compile(
    r"\b(?P<aws_key>(?:AKIA|ASIA)[0-9A-Z]{16})\b"
)

# AWS Secret Access Key + S3/Iceberg/etc secret in key=value form. Не
# имеет фиксированного формата (любая base64-ish строка 30-128 chars),
# поэтому ловим по ключам ``aws_secret_access_key``, ``secret_access_key``,
# ``s3.secret-access-key`` (Iceberg REST), ``access_key_secret`` (несколько
# вендоров) — case-insensitive. Группа `value` — всё до пробела/quote/&/?/;
# чтобы не выйти за границы значения в URL-encoded query или JSON.
_AWS_SECRET_IN_TEXT = re.compile(
    r"(?i)"
    r"(?P<key>(?:aws[._-]?)?(?:secret[._-]?access[._-]?key|access[._-]?key[._-]?secret)"
    r"|s3\.secret[._-]access[._-]key)"
    r"\s*[=:]\s*"
    r"(?P<quote>[\"']?)(?P<value>[A-Za-z0-9/+=_\-]{16,128})(?P=quote)"
)

# Bearer-токен в Authorization header / iceberg-rest auth (#231).
# Формат HTTP-standard: ``Authorization: Bearer <token>`` либо
# inline ``token=<...>`` для query-style configs. Не пытаемся валидировать
# JWT-форму — любая последовательность base64-safe символов длиной >=10
# considered секретом.
_BEARER_TOKEN_IN_TEXT = re.compile(
    r"(?i)\b(?P<scheme>Bearer|Token)\s+(?P<value>[A-Za-z0-9._\-]{10,})"
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


def scrub_value(value):
    """Public alias for ``_scrub`` — same recursive sanitiser, used by
    code outside the logging path (Sentry ``before_send`` in #103)."""
    return _scrub(value)


def _scrub(value):
    """Recursively rewrite DSN passwords inside any string/tuple/dict/list.

    Exceptions are str()-ed and scrubbed: when an Exception is passed to a
    ``logger.warning('x: %s', exc)`` call, the eventual ``%s`` format-time
    ``str(exc)`` would otherwise resurrect the unscrubbed DSN. Replacing
    the exception arg with a pre-scrubbed string keeps ``%s`` happy AND
    closes the leak.
    """
    if isinstance(value, str):
        scrubbed = _DSN_IN_TEXT.sub(
            lambda m: f"{m.group('prefix')}{_PASSWORD_PLACEHOLDER}{m.group('suffix')}",
            value,
        )
        # Telegram bot token (#143). Keep the bot_id (it's effectively a
        # public identifier — anyone in the chat can read it from the bot's
        # profile), scrub only the secret hash half.
        scrubbed = _TELEGRAM_TOKEN_IN_TEXT.sub(
            lambda m: f"{m.group('bot_id')}:{_PASSWORD_PLACEHOLDER}",
            scrubbed,
        )
        # AWS Access Key ID (#231). Public identifier, но всё равно
        # маскируем — пара AKIA + Secret даёт полный доступ.
        scrubbed = _AWS_ACCESS_KEY_IN_TEXT.sub(_PASSWORD_PLACEHOLDER, scrubbed)
        # AWS Secret + любой ключ ``*_secret_access_key`` /
        # ``access_key_secret`` / ``s3.secret-access-key`` в виде ``key=value``.
        scrubbed = _AWS_SECRET_IN_TEXT.sub(
            lambda m: f"{m.group('key')}={m.group('quote')}{_PASSWORD_PLACEHOLDER}{m.group('quote')}",
            scrubbed,
        )
        # Bearer / Token <value> в Authorization-headers + iceberg auth.
        scrubbed = _BEARER_TOKEN_IN_TEXT.sub(
            lambda m: f"{m.group('scheme')} {_PASSWORD_PLACEHOLDER}",
            scrubbed,
        )
        return scrubbed
    if isinstance(value, BaseException):
        return _scrub(str(value))
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


_OLD_RECORD_FACTORY = None  # set by install_log_record_scrubber


def install_log_record_scrubber() -> None:
    """Scrub DSN passwords at LogRecord construction time.

    Why not just a Filter on root? Filters on a Logger only fire for
    records *originating* on that logger — child loggers (``app.foo``)
    don't inherit parent filters. They DO inherit ancestor *handlers*,
    so filtered handlers still scrub on the way out, but anyone reading
    records by other means (pytest caplog, Sentry's SDK handler, a custom
    JSON formatter on a different logger) gets the unscrubbed payload.

    ``logging.setLogRecordFactory`` wraps EVERY record creation across
    every logger, so the scrub happens once, at the source. Idempotent:
    re-installing replaces the previous wrapper without double-wrapping.
    """
    global _OLD_RECORD_FACTORY

    # Capture the original factory exactly once. Subsequent calls compose
    # on top of OUR wrapper, which would scrub twice — cheap but wasteful.
    # The idempotent guard in app/app.py (_ensure_dsn_logging_filter) means
    # this normally runs once per process anyway.
    if _OLD_RECORD_FACTORY is None:
        _OLD_RECORD_FACTORY = logging.getLogRecordFactory()
    base_factory = _OLD_RECORD_FACTORY

    def _scrubbing_factory(*args, **kwargs):
        record = base_factory(*args, **kwargs)
        if isinstance(record.msg, str):
            record.msg = _scrub(record.msg)
        if record.args:
            record.args = _scrub(record.args)
        return record

    logging.setLogRecordFactory(_scrubbing_factory)


def init_logging_filter(logger: logging.Logger | None = None) -> DSNFilter:
    """Install DSN scrubbing — record factory + ``DSNFilter`` on root.

    The factory is the actual defence (covers child loggers, custom
    handlers, pytest caplog, future Sentry SDK). The Filter on root is
    kept as belt-and-braces for any code path that bypasses the factory
    (e.g. someone constructing a ``LogRecord`` by hand).

    Returns the filter instance — tests use it to verify behaviour
    without poking at private globals.
    """
    install_log_record_scrubber()
    logger = logger if logger is not None else logging.getLogger()
    f = DSNFilter()
    logger.addFilter(f)
    for handler in logger.handlers:
        handler.addFilter(f)
    return f
