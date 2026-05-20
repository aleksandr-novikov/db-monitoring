"""Shared helpers for integration tests (#44).

Each test in this directory spins up a real database via testcontainers and
exercises the production code path end-to-end (no SQL is mocked). Default
``pytest`` runs skip everything in this directory — opt in with
``pytest -m integration``.

Helpers here centralise the bookkeeping that's identical across the
adapter tests: pointing ``settings.DATABASE_URL`` at the live container and
clearing the cached engine/adapter in ``app.db`` so the next call rebuilds
them against the new DSN.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager


@contextmanager
def use_target_db(url: str) -> Iterator[None]:
    """Point app.db at *url* and reset its cached engine/adapter.

    Restores the previous DSN on exit. Use as a fixture or in tests that
    only need one container DSN.
    """
    from app import db
    from app.config import settings

    original_url = settings.DATABASE_URL
    original_engine = db._engine
    original_adapter = db._adapter

    settings.DATABASE_URL = url
    db._engine = None
    db._adapter = None
    try:
        yield
    finally:
        if db._engine is not None:
            db._engine.dispose()
        settings.DATABASE_URL = original_url
        db._engine = original_engine
        db._adapter = original_adapter


@contextmanager
def use_metrics_db(url: str) -> Iterator[None]:
    """Point app.metrics_storage at *url* and reset its cached state.

    Mirrors ``use_target_db`` for the monitoring-side storage.
    """
    from app import metrics_storage
    from app.config import settings

    original_url = settings.MONITOR_DB_URL
    original_engine = metrics_storage._engine
    original_initialized = metrics_storage._initialized

    settings.MONITOR_DB_URL = url
    metrics_storage._engine = None
    metrics_storage._initialized = False
    try:
        yield
    finally:
        if metrics_storage._engine is not None:
            metrics_storage._engine.dispose()
        settings.MONITOR_DB_URL = original_url
        metrics_storage._engine = original_engine
        metrics_storage._initialized = original_initialized
