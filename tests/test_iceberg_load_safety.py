"""Tests for #235 — Iceberg load safety.

Three guard-rails on top of the production-params work in #234:

1. **Namespace traversal.** By default the tick walks ONLY
   ``effective_namespace`` — ``list_namespaces()`` is never called from the
   regular collection path (catalogs with thousands of namespaces would
   make the discovery itself a load attack). With
   ``iceberg_namespace_allowlist=['prod','staging']`` the tick walks only
   those namespaces.

2. **Table filtering reused from #232.** ``_apply_table_filters`` runs
   BEFORE ``collect_table_schema`` / ``table_stats`` / metric calls — for
   Iceberg too, not just Postgres.

3. **metadata_only_mode.** Only schema is collected; metric calls are
   skipped entirely. ``max_tables_per_tick`` still applies so a runaway
   catalog can't flood schema collection either.

Plus a per-metadata-call timeout so one stuck ``collect_table_schema``
doesn't sink the whole tick.
"""
from __future__ import annotations

import json
import logging
import time
import uuid
from unittest import mock

import pytest
from cryptography.fernet import Fernet

from app import crypto
from collectors.per_project import (
    _iceberg_namespaces_to_scan,
    _run_with_timeout,
    collect_for_connection,
)

# --- Fixtures (mirror the load-safety / per_project test style) -----------


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    """Per-test SQLite metrics store. Returns the storage module."""
    import app.metrics_storage as ms

    db_path = tmp_path / "metrics.db"
    monkeypatch.setattr(ms.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    ms.get_engine()  # forces _ensure_schema / migrations to run.
    return ms


def _make_iceberg_conn(storage, *, namespace=None, allowlist=None, metadata_only=False):
    """Insert a minimal Iceberg connection row and return its id."""
    user_id = uuid.uuid4().hex
    storage.create_user(
        user_id=user_id, email=f"u{user_id[:6]}@x.io", password_hash="x",
    )
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_id, name="P",
        slug=f"p-{user_id[:6]}",
    )

    conn_id = uuid.uuid4().hex
    dsn_ct = crypto.encrypt_dsn("iceberg+rest://catalog:8181?warehouse=s3://b/w")
    storage.create_connection(
        connection_id=conn_id,
        project_id=project["id"],
        name="ice",
        dsn_encrypted=dsn_ct,
        schema_name=namespace or "default",
        interval_minutes=15,
        is_active=True,
        iceberg_namespace=namespace,
    )
    if allowlist is not None or metadata_only:
        from sqlalchemy import text
        with storage.get_engine().begin() as c:
            c.execute(text(
                "UPDATE connections SET "
                "iceberg_namespace_allowlist = :al, metadata_only_mode = :mm "
                "WHERE id = :id"
            ), {
                "al": json.dumps(allowlist) if allowlist else None,
                "mm": 1 if metadata_only else 0,
                "id": conn_id,
            })
    return project["id"], conn_id


# --- _iceberg_namespaces_to_scan -------------------------------------------


def test_default_iterates_only_effective_namespace():
    """No allowlist → just [effective_namespace], no list_namespaces call."""
    assert _iceberg_namespaces_to_scan({}, "lakehouse") == ["lakehouse"]
    assert _iceberg_namespaces_to_scan(
        {"iceberg_namespace_allowlist": None}, "lakehouse",
    ) == ["lakehouse"]
    # Empty JSON list also collapses to single-namespace path.
    assert _iceberg_namespaces_to_scan(
        {"iceberg_namespace_allowlist": "[]"}, "lakehouse",
    ) == ["lakehouse"]


def test_explicit_allowlist_overrides_effective_namespace():
    """With allowlist → iterate exactly those namespaces, ignore effective_ns."""
    result = _iceberg_namespaces_to_scan(
        {"iceberg_namespace_allowlist": '["prod","staging"]'},
        "ignored",
    )
    assert result == ["prod", "staging"]


def test_empty_when_no_namespace_and_no_allowlist():
    """No effective_namespace AND no allowlist → empty list (caller bails)."""
    assert _iceberg_namespaces_to_scan({}, None) == []


def test_malformed_allowlist_falls_back_to_effective(caplog):
    """Bad JSON in allowlist is logged + treated as empty (falls back to effective_ns)."""
    with caplog.at_level(logging.WARNING):
        result = _iceberg_namespaces_to_scan(
            {"iceberg_namespace_allowlist": "not-json"},
            "default",
        )
    assert result == ["default"]


# --- _run_with_timeout -----------------------------------------------------


def test_run_with_timeout_returns_value_fast():
    assert _run_with_timeout(lambda: 42, timeout=1) == 42


def test_run_with_timeout_raises_on_slow():
    """A callable that exceeds the budget raises TimeoutError."""

    def _slow():
        time.sleep(2)
        return "never"

    with pytest.raises(TimeoutError):
        _run_with_timeout(_slow, timeout=0.2)


# --- End-to-end (mock adapter) ---------------------------------------------


def _patched_adapter(monkeypatch, *, list_tables, table_stats=None):
    """Replace make_adapter_for_url with a stub returning a mock adapter."""
    adapter = mock.MagicMock()
    adapter.list_tables.side_effect = list_tables
    if table_stats is not None:
        adapter.table_stats.side_effect = table_stats
    else:
        adapter.table_stats.return_value = {
            "table_name": "t", "schema": "n",
            "row_count": 0, "size_bytes": 0, "last_analyze": None,
        }
    monkeypatch.setattr(
        "collectors.per_project.make_adapter_for_url",
        lambda dsn, **_: adapter,
    )
    return adapter


def test_collector_walks_only_effective_namespace_by_default(storage, monkeypatch):
    """No allowlist → adapter.list_tables called once with effective_ns."""
    project_id, conn_id = _make_iceberg_conn(storage, namespace="lakehouse")
    adapter = _patched_adapter(monkeypatch, list_tables=lambda ns: [
        {"table_name": "orders", "schema": ns},
    ])
    # Stub schema collection — we only assert namespace traversal.
    monkeypatch.setattr(
        "collectors.schema_collector.collect_table_schema",
        lambda *a, **kw: None,
    )
    # Stub MetricsCollector.collect to avoid actually touching DB schema.
    monkeypatch.setattr(
        "collectors.metrics_collector.MetricsCollector.collect",
        lambda self, table, ts=None: [],
    )
    monkeypatch.setattr(
        "collectors.per_project.using_engine",
        lambda engine, adapter: _NullCtx(),
    )

    collect_for_connection(project_id, conn_id)

    # list_namespaces MUST NOT have been called in regular tick.
    assert not adapter.list_namespaces.called
    # list_tables called exactly once with the effective namespace.
    assert adapter.list_tables.call_count == 1
    assert adapter.list_tables.call_args[0][0] == "lakehouse"


def test_collector_walks_allowlisted_namespaces(storage, monkeypatch):
    project_id, conn_id = _make_iceberg_conn(
        storage, namespace="default", allowlist=["prod", "staging"],
    )
    calls: list[str] = []

    def _list(ns):
        calls.append(ns)
        return []

    _patched_adapter(monkeypatch, list_tables=_list)
    monkeypatch.setattr(
        "collectors.schema_collector.collect_table_schema",
        lambda *a, **kw: None,
    )
    monkeypatch.setattr(
        "collectors.per_project.using_engine",
        lambda engine, adapter: _NullCtx(),
    )
    collect_for_connection(project_id, conn_id)
    assert calls == ["prod", "staging"]


def test_metadata_only_skips_metrics_collect(storage, monkeypatch):
    """metadata_only_mode=True → MetricsCollector.collect NOT called, but
    collect_table_schema IS called."""
    project_id, conn_id = _make_iceberg_conn(
        storage, namespace="lakehouse", metadata_only=True,
    )
    _patched_adapter(monkeypatch, list_tables=lambda ns: [
        {"table_name": "orders", "schema": ns},
        {"table_name": "users", "schema": ns},
    ])
    schema_calls: list[str] = []
    monkeypatch.setattr(
        "collectors.schema_collector.collect_table_schema",
        lambda name, **kw: schema_calls.append(name),
    )
    metrics_calls: list[str] = []
    monkeypatch.setattr(
        "collectors.metrics_collector.MetricsCollector.collect",
        lambda self, table, ts=None: metrics_calls.append(table) or [],
    )
    monkeypatch.setattr(
        "collectors.per_project.using_engine",
        lambda engine, adapter: _NullCtx(),
    )
    collect_for_connection(project_id, conn_id)

    assert sorted(schema_calls) == ["orders", "users"]
    assert metrics_calls == []  # metric collection short-circuited


def test_metadata_only_respects_max_tables(storage, monkeypatch):
    """max_tables_per_tick=1 + metadata_only → only 1 schema call."""
    project_id, conn_id = _make_iceberg_conn(
        storage, namespace="lakehouse", metadata_only=True,
    )
    # Lower max_tables_per_tick after creation.
    from sqlalchemy import text as _text
    with storage.get_engine().begin() as c:
        c.execute(_text(
            "UPDATE connections SET max_tables_per_tick = 1 WHERE id = :id"
        ), {"id": conn_id})

    _patched_adapter(monkeypatch, list_tables=lambda ns: [
        {"table_name": f"t{i}", "schema": ns} for i in range(5)
    ])
    schema_calls: list[str] = []
    monkeypatch.setattr(
        "collectors.schema_collector.collect_table_schema",
        lambda name, **kw: schema_calls.append(name),
    )
    monkeypatch.setattr(
        "collectors.per_project.using_engine",
        lambda engine, adapter: _NullCtx(),
    )
    collect_for_connection(project_id, conn_id)
    assert len(schema_calls) == 1


def test_table_allowlist_filters_iceberg_tables(storage, monkeypatch):
    """#232 _apply_table_filters works for Iceberg too — schema NOT collected
    for non-allowlisted tables."""
    project_id, conn_id = _make_iceberg_conn(storage, namespace="lakehouse")
    from sqlalchemy import text as _text
    with storage.get_engine().begin() as c:
        c.execute(_text(
            "UPDATE connections SET table_allowlist = :al WHERE id = :id"
        ), {"al": json.dumps(["orders"]), "id": conn_id})

    _patched_adapter(monkeypatch, list_tables=lambda ns: [
        {"table_name": "orders", "schema": ns},
        {"table_name": "events", "schema": ns},
        {"table_name": "logs", "schema": ns},
    ])
    schema_calls: list[str] = []
    monkeypatch.setattr(
        "collectors.schema_collector.collect_table_schema",
        lambda name, **kw: schema_calls.append(name),
    )
    monkeypatch.setattr(
        "collectors.metrics_collector.MetricsCollector.collect",
        lambda self, table, ts=None: [],
    )
    monkeypatch.setattr(
        "collectors.per_project.using_engine",
        lambda engine, adapter: _NullCtx(),
    )
    collect_for_connection(project_id, conn_id)
    assert schema_calls == ["orders"]


def test_timeout_does_not_kill_tick(storage, monkeypatch, caplog):
    """One stuck collect_table_schema must not stop the rest."""
    project_id, conn_id = _make_iceberg_conn(storage, namespace="lakehouse")
    _patched_adapter(monkeypatch, list_tables=lambda ns: [
        {"table_name": "fast", "schema": ns},
        {"table_name": "stuck", "schema": ns},
        {"table_name": "fast2", "schema": ns},
    ])
    collected: list[str] = []

    def _schema(name, **kw):
        if name == "stuck":
            time.sleep(2)
        collected.append(name)

    monkeypatch.setattr(
        "collectors.schema_collector.collect_table_schema", _schema,
    )
    monkeypatch.setattr(
        "collectors.metrics_collector.MetricsCollector.collect",
        lambda self, table, ts=None: [],
    )
    # Shrink the timeout for the test so it actually fires.
    monkeypatch.setattr(
        "collectors.per_project._METADATA_CALL_TIMEOUT_S", 0.3,
    )
    monkeypatch.setattr(
        "collectors.per_project.using_engine",
        lambda engine, adapter: _NullCtx(),
    )
    with caplog.at_level(logging.WARNING):
        collect_for_connection(project_id, conn_id)

    # fast + fast2 completed; stuck was dropped via timeout. The tick
    # finishes regardless.
    assert "fast" in collected
    assert "fast2" in collected
    # After the #239 run-log integration, a hung collect_table_schema is
    # logged as "schema collection timed out for <name>" (not the older
    # "skip <name>: timeout" wording). Either phrasing is acceptable — the
    # invariant is that *stuck* appears in a warning and the tick still
    # finishes the rest.
    assert any(
        "stuck" in rec.message and "time" in rec.message.lower()
        for rec in caplog.records
    )


# --- Helpers ---------------------------------------------------------------


class _NullCtx:
    """Drop-in for ``using_engine(engine, adapter)`` context manager —
    bypasses the global-adapter override stack in tests that
    monkeypatch the adapter directly."""

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False
