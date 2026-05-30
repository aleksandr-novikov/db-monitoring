"""JSON-log tests (#102).

Cover the explicit acceptance criteria:
- LOG_FORMAT=json → every log line is valid JSON with all required fields
- request_id flows from g.request_id into every record within the request
- DSN scrubbing from #56 stays effective (regression guard)
"""

from __future__ import annotations

import io
import json
import logging
import re

import pytest

from app.logging_setup import JsonFormatter, configure_logging


def _capture_records_via(formatter: logging.Formatter, *,
                         logger_name: str, msg: str, **extra) -> str:
    """Render one log record through ``formatter`` and return the line.

    A dedicated logger + handler stack keeps the test from depending on
    whatever the global root handler currently is.
    """
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(formatter)
    logger = logging.getLogger(logger_name)
    logger.handlers = [handler]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    try:
        logger.info(msg, extra=extra) if extra else logger.info(msg)
    finally:
        logger.handlers = []
    return buf.getvalue().strip()


# ── JsonFormatter shape ────────────────────────────────────────────────────

def test_json_formatter_required_fields():
    """Every line includes timestamp, level, logger, message, request_id."""
    line = _capture_records_via(
        JsonFormatter(),
        logger_name="test_required",
        msg="hello",
    )
    record = json.loads(line)
    for field in ("timestamp", "level", "logger", "message", "request_id"):
        assert field in record
    assert record["level"] == "INFO"
    assert record["logger"] == "test_required"
    assert record["message"] == "hello"
    # No flask app context → request_id stays null but the field is present.
    assert record["request_id"] is None


def test_json_formatter_includes_iso_utc_timestamp():
    line = _capture_records_via(
        JsonFormatter(), logger_name="ts", msg="x",
    )
    record = json.loads(line)
    # 2026-05-30T12:34:56.789+00:00 — millisecond precision, UTC offset.
    assert re.match(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\+00:00",
        record["timestamp"],
    )


def test_json_formatter_includes_extra_fields():
    """logger.info(..., extra={'k': v}) → JSON payload has 'k'."""
    line = _capture_records_via(
        JsonFormatter(), logger_name="ex", msg="x",
        project_id="proj-A", connection_id="conn-X",
    )
    record = json.loads(line)
    assert record["project_id"] == "proj-A"
    assert record["connection_id"] == "conn-X"


def test_json_formatter_includes_exc_info():
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger("exc_test")
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    try:
        try:
            raise ValueError("boom")
        except ValueError:
            logger.exception("caught")
    finally:
        logger.handlers = []
    record = json.loads(buf.getvalue().strip())
    assert record["message"] == "caught"
    assert "ValueError" in record["exc_info"]
    assert "boom" in record["exc_info"]


def test_json_formatter_handles_unserialisable_extra():
    """An object that isn't JSON-serialisable must NOT crash the formatter.
    We fall back to str(value)."""
    class NotSerializable:
        def __repr__(self):
            return "<NotSerializable instance>"

    line = _capture_records_via(
        JsonFormatter(), logger_name="bad", msg="x",
        widget=NotSerializable(),
    )
    record = json.loads(line)
    assert record["widget"] == "<NotSerializable instance>"


# ── configure_logging integration ──────────────────────────────────────────

def test_configure_logging_idempotent_handler_count():
    """Repeated configure_logging() must not stack handlers on root —
    tests do create_app() dozens of times per session, would otherwise
    leak into a 50-line log per record."""
    root = logging.getLogger()
    before = len([h for h in root.handlers if getattr(h, "_dbmon_managed", False)])
    configure_logging("text")
    configure_logging("json")
    configure_logging("text")
    after = len([h for h in root.handlers if getattr(h, "_dbmon_managed", False)])
    # Exactly one managed handler regardless of how many times we called.
    assert before in (0, 1)
    assert after == 1


def test_configure_logging_json_mode_attached(monkeypatch):
    configure_logging("json")
    root = logging.getLogger()
    managed = [h for h in root.handlers if getattr(h, "_dbmon_managed", False)]
    assert len(managed) == 1
    assert isinstance(managed[0].formatter, JsonFormatter)


# ── Flask request_id propagation ───────────────────────────────────────────

@pytest.fixture
def app_(tmp_path, monkeypatch):
    import app.metrics_storage as storage
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "log.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)
    monkeypatch.setattr(cfg, "LOG_FORMAT", "json")

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    return create_app({"TESTING": True})


def test_request_id_is_set_and_echoed_back(app_):
    """Each request gets a unique uuid4-shaped request_id, echoed on the
    response header so clients can correlate."""
    c = app_.test_client()
    r1 = c.get("/healthz")
    r2 = c.get("/healthz")
    rid1 = r1.headers.get("X-Request-Id")
    rid2 = r2.headers.get("X-Request-Id")
    assert rid1 and rid2
    assert rid1 != rid2
    assert re.fullmatch(r"[0-9a-f]{32}", rid1)


def test_request_id_honours_upstream_header(app_):
    """A reverse proxy (or test harness) can pre-seed the id — we honour
    it so a request can be traced through multiple services."""
    c = app_.test_client()
    rid = "trace-from-edge-1234567890abcdef"
    r = c.get("/healthz", headers={"X-Request-Id": rid})
    assert r.headers["X-Request-Id"] == rid


def test_request_id_appears_in_json_logs_within_request(app_, caplog):
    """JsonFormatter must observe g.request_id while inside a request."""
    rid = "deterministic-test-rid"
    # We don't intercept the actual JSON stream; instead we render a
    # record manually inside a synthetic app context to confirm the
    # JsonFormatter pulls g.request_id.
    formatter = JsonFormatter()
    with app_.test_request_context("/healthz",
                                    headers={"X-Request-Id": rid}):
        # Simulate the before_request hook (test_request_context doesn't
        # auto-fire app-level hooks).
        from flask import g
        g.request_id = rid
        rec = logging.makeLogRecord({
            "name": "ctx", "msg": "hi", "levelname": "INFO",
            "levelno": 20, "args": (), "created": 1700000000.123,
        })
        rec.message = "hi"
        line = formatter.format(rec)
    payload = json.loads(line)
    assert payload["request_id"] == rid
    # Suppress unused warning from caplog parameter.
    assert caplog is not None
    # Sanity: outside the context, request_id is None.
    rec_out = logging.makeLogRecord({
        "name": "ctx", "msg": "hi", "levelname": "INFO",
        "levelno": 20, "args": (), "created": 1700000000.123,
    })
    rec_out.message = "hi"
    assert json.loads(formatter.format(rec_out))["request_id"] is None


# ── DSN scrubbing regression (from #56) ────────────────────────────────────

def test_dsn_scrub_still_works_under_json_formatter():
    """Logging a DSN-containing message must NOT include the password
    in the JSON payload. The record factory in app.security scrubs at
    LogRecord construction, so the formatter never sees the secret.
    """
    from app.security import install_log_record_scrubber

    install_log_record_scrubber()  # idempotent
    logger = logging.getLogger("scrub_under_json")
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    try:
        logger.warning("connect failed: %s",
                       "postgresql://u:topsecret@h:5432/d")
    finally:
        logger.handlers = []

    line = buf.getvalue().strip()
    payload = json.loads(line)
    assert "topsecret" not in payload["message"]
    assert "u:***@h" in payload["message"]
