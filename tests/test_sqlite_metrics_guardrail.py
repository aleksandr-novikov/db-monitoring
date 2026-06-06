"""Tests for the unsafe-SQLite-metrics-store warning (#214).

Acceptance: `_warn_unsafe_sqlite_metrics_store` LOGS a WARNING when
MONITOR_DB_URL = sqlite:/// in production-like runtime (Docker container
OR FLASK_ENV != development). In intended local-dev path (FLASK_ENV =
development AND not in Docker) — silent.
"""

from __future__ import annotations

import logging

import pytest


def _has_warning(caplog, snippet: str) -> bool:
    """True if any caplog record at WARNING contains the substring."""
    return any(
        r.levelno >= logging.WARNING and snippet in r.getMessage()
        for r in caplog.records
    )


@pytest.fixture
def warn_fn():
    """Late-import the warning function under test — keeps import cheap."""
    from app.app import _warn_unsafe_sqlite_metrics_store
    return _warn_unsafe_sqlite_metrics_store


def test_sqlite_in_production_runtime_logs_warning(warn_fn, monkeypatch, caplog):
    """FLASK_ENV != development AND SQLite → WARNING."""
    from app.config import settings
    monkeypatch.setattr(settings, "MONITOR_DB_URL", "sqlite:///monitor.db")
    monkeypatch.setattr(settings, "FLASK_ENV", "production")
    with caplog.at_level(logging.WARNING):
        warn_fn()
    assert _has_warning(caplog, "MONITOR_DB_URL is SQLite")


def test_sqlite_in_docker_container_logs_warning(
    warn_fn, monkeypatch, caplog, tmp_path,
):
    """/.dockerenv present + SQLite + FLASK_ENV=development → WARNING.
    Сценарий: dev-режим в Docker — всё равно опасно. Мокаем os.path.exists
    чтобы симулировать наличие /.dockerenv."""
    import os as os_mod

    from app.config import settings
    monkeypatch.setattr(settings, "MONITOR_DB_URL", "sqlite:///monitor.db")
    monkeypatch.setattr(settings, "FLASK_ENV", "development")
    real_exists = os_mod.path.exists
    monkeypatch.setattr(
        "os.path.exists",
        lambda p: True if p == "/.dockerenv" else real_exists(p),
    )
    with caplog.at_level(logging.WARNING):
        warn_fn()
    assert _has_warning(caplog, "MONITOR_DB_URL is SQLite")


def test_sqlite_in_local_dev_is_silent(warn_fn, monkeypatch, caplog):
    """FLASK_ENV=development AND no /.dockerenv → silent (intended usage)."""
    import os as os_mod

    from app.config import settings
    monkeypatch.setattr(settings, "MONITOR_DB_URL", "sqlite:///monitor.db")
    monkeypatch.setattr(settings, "FLASK_ENV", "development")
    real_exists = os_mod.path.exists
    monkeypatch.setattr(
        "os.path.exists",
        lambda p: False if p == "/.dockerenv" else real_exists(p),
    )
    with caplog.at_level(logging.WARNING):
        warn_fn()
    assert not _has_warning(caplog, "MONITOR_DB_URL is SQLite")


def test_postgres_in_any_environment_is_silent(warn_fn, monkeypatch, caplog):
    """Postgres backend → silent regardless of FLASK_ENV / Docker."""
    from app.config import settings
    monkeypatch.setattr(
        settings, "MONITOR_DB_URL",
        "postgresql://postgres:dev@timescaledb:5432/metrics",
    )
    monkeypatch.setattr(settings, "FLASK_ENV", "production")
    with caplog.at_level(logging.WARNING):
        warn_fn()
    assert not _has_warning(caplog, "MONITOR_DB_URL is SQLite")


def test_warning_includes_actionable_pointer(warn_fn, monkeypatch, caplog):
    """Message должен указывать куда смотреть — иначе operator не починит."""
    from app.config import settings
    monkeypatch.setattr(settings, "MONITOR_DB_URL", "sqlite:///monitor.db")
    monkeypatch.setattr(settings, "FLASK_ENV", "production")
    with caplog.at_level(logging.WARNING):
        warn_fn()
    msg = " ".join(
        r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING
    )
    assert "Postgres/Timescale" in msg
    assert ".env.example" in msg or "README" in msg
