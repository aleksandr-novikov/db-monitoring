"""Integration tests for #232 — Postgres load-safety knobs.

Covers:
- _parse_table_list edge cases (None, "", invalid JSON, non-list, non-string entries)
- _apply_table_filters: allowlist → denylist → max ordering & determinism
- collect_for_connection: large-table skip via table_stats BEFORE column_nulls
- _build_engine wires statement_timeout into Postgres connect_args, not into
  ClickHouse / Iceberg

The integration tests reuse the same monkeypatch pattern as test_per_project.py:
real SQLite metrics DB + stubbed adapter so we never touch a live Postgres.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime
from unittest import mock

import pytest
from cryptography.fernet import Fernet

from app import crypto
from collectors.per_project import (
    _apply_table_filters,
    _build_engine,
    _parse_table_list,
    collect_for_connection,
)


# --- Fixtures (mirror test_per_project.py) --------------------------------


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    db_path = tmp_path / "safety.db"
    import app.metrics_storage as storage_mod
    monkeypatch.setattr(
        storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}",
    )
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


def _seed(storage, dsn: str):
    user_id = uuid.uuid4().hex
    storage.create_user(
        user_id=user_id, email=f"u{user_id[:6]}@x.io", password_hash="x",
    )
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_id, name="P", slug="default",
    )
    conn = storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="local",
        dsn_encrypted=crypto.encrypt_dsn(dsn),
        schema_name="public",
        interval_minutes=15,
        is_active=True,
    )
    return user_id, project, conn


def _set_safety(storage, conn_id, **kwargs):
    """Patch safety columns directly via SQL — there's no UI/route yet."""
    from sqlalchemy import text
    set_clause = ", ".join(f"{c} = :{c}" for c in kwargs)
    params = {**kwargs, "id": conn_id}
    with storage.get_engine().begin() as conn:
        conn.execute(
            text(f"UPDATE connections SET {set_clause} WHERE id = :id"),
            params,
        )


# --- _parse_table_list ----------------------------------------------------


def test_parse_table_list_none_or_empty():
    assert _parse_table_list(None) == []
    assert _parse_table_list("") == []


def test_parse_table_list_valid_array():
    assert _parse_table_list('["users","orders"]') == ["users", "orders"]


def test_parse_table_list_strips_entries():
    assert _parse_table_list('["  users  ","orders"]') == ["users", "orders"]


def test_parse_table_list_invalid_json_logs_and_returns_empty(caplog):
    with caplog.at_level(logging.WARNING):
        assert _parse_table_list("not-json") == []
    assert any("invalid JSON" in r.message for r in caplog.records)


def test_parse_table_list_non_list_logs_and_returns_empty(caplog):
    with caplog.at_level(logging.WARNING):
        assert _parse_table_list('"users"') == []
    assert any("must be a JSON array" in r.message for r in caplog.records)


def test_parse_table_list_drops_non_string_entries():
    """A mixed-type array silently drops the non-strings rather than
    crashing — defensive in case someone seeds the column from Python."""
    assert _parse_table_list('["users", 42, "orders"]') == ["users", "orders"]


# --- _apply_table_filters -------------------------------------------------


def _tt(*names):
    return [{"table_name": n, "schema": "public"} for n in names]


def test_allowlist_filters_tables():
    tables = _tt("users", "orders", "events")
    conn = {"table_allowlist": '["users"]', "max_tables_per_tick": 50}
    kept, skipped = _apply_table_filters(tables, conn)
    assert [t["table_name"] for t in kept] == ["users"]
    assert ("orders", "not_in_allowlist") in skipped
    assert ("events", "not_in_allowlist") in skipped


def test_denylist_skips_tables():
    tables = _tt("users", "orders", "audit_logs")
    conn = {"table_denylist": '["audit_logs"]', "max_tables_per_tick": 50}
    kept, skipped = _apply_table_filters(tables, conn)
    assert [t["table_name"] for t in kept] == ["orders", "users"]
    assert ("audit_logs", "denylisted") in skipped


def test_max_tables_applied_after_filters():
    tables = _tt("users", "orders", "events")
    conn = {
        "table_allowlist": '["users","orders","events"]',
        "max_tables_per_tick": 2,
    }
    kept, skipped = _apply_table_filters(tables, conn)
    # Alphabetical: events, orders, users → first 2 kept.
    assert [t["table_name"] for t in kept] == ["events", "orders"]
    assert ("users", "max_tables_limit") in skipped


def test_max_tables_does_not_clip_before_allowlist():
    """Cap is applied AFTER allowlist filtering — not to the catalog prefix."""
    tables = _tt("aaa", "bbb", "ccc", "users")
    conn = {"table_allowlist": '["users"]', "max_tables_per_tick": 2}
    kept, _ = _apply_table_filters(tables, conn)
    # Even though "users" is alphabetically 4th, allowlist filters first.
    assert [t["table_name"] for t in kept] == ["users"]


def test_deterministic_order():
    """Two calls with the same input yield the same ordering."""
    tables = _tt("z", "a", "m", "b")
    conn = {"max_tables_per_tick": 2}
    k1, _ = _apply_table_filters(tables, conn)
    k2, _ = _apply_table_filters(tables, conn)
    assert [t["table_name"] for t in k1] == [t["table_name"] for t in k2]
    assert [t["table_name"] for t in k1] == ["a", "b"]


def test_empty_allowlist_processes_all():
    """allowlist='[]' must NOT exclude everything — it means "not set"."""
    tables = _tt("users", "orders")
    conn = {"table_allowlist": "[]", "max_tables_per_tick": 50}
    kept, _ = _apply_table_filters(tables, conn)
    assert {t["table_name"] for t in kept} == {"users", "orders"}


def test_empty_denylist_excludes_nothing():
    tables = _tt("users", "orders")
    conn = {"table_denylist": "[]", "max_tables_per_tick": 50}
    kept, _ = _apply_table_filters(tables, conn)
    assert {t["table_name"] for t in kept} == {"users", "orders"}


def test_invalid_json_allowlist_treated_as_empty(caplog):
    """A typo in the DB shouldn't kill the collector. Malformed allowlist
    reads as 'no constraint' so the tick still produces metrics."""
    tables = _tt("users", "orders")
    conn = {"table_allowlist": "not-json", "max_tables_per_tick": 50}
    with caplog.at_level(logging.WARNING):
        kept, _ = _apply_table_filters(tables, conn)
    assert {t["table_name"] for t in kept} == {"users", "orders"}


def test_exact_match_case_sensitive():
    """allowlist entries are exact-match — 'Users' ≠ 'users'."""
    tables = _tt("users", "Users")
    conn = {"table_allowlist": '["users"]', "max_tables_per_tick": 50}
    kept, _ = _apply_table_filters(tables, conn)
    assert [t["table_name"] for t in kept] == ["users"]


def test_negative_max_tables_does_not_truncate():
    """max_tables_per_tick=0 means "skip everything"; we cap at 0 cleanly."""
    tables = _tt("a", "b")
    conn = {"max_tables_per_tick": 0}
    kept, skipped = _apply_table_filters(tables, conn)
    assert kept == []
    assert {n for n, _ in skipped} == {"a", "b"}


# --- _build_engine --------------------------------------------------------


def _capture_create_engine_call(monkeypatch):
    """Patch sqlalchemy.create_engine in the collectors module, capture the
    connect_args passed in. Returns a dict reference that the test populates.
    """
    import sqlalchemy
    captured: dict = {}
    real = sqlalchemy.create_engine

    def fake_create_engine(dsn, **kwargs):
        captured["dsn"] = dsn
        captured["kwargs"] = kwargs
        # Return a stub — _build_engine's caller doesn't actually use it.
        return mock.MagicMock(name="engine_stub")
    monkeypatch.setattr(sqlalchemy, "create_engine", fake_create_engine)
    return captured, real


def test_build_engine_postgres_includes_statement_timeout(monkeypatch):
    captured, _ = _capture_create_engine_call(monkeypatch)
    _build_engine("postgresql://u:p@h/d", statement_timeout_ms=10000)
    opts = captured["kwargs"]["connect_args"].get("options", "")
    assert "statement_timeout=10000" in opts


def test_build_engine_postgres_omits_timeout_when_none(monkeypatch):
    captured, _ = _capture_create_engine_call(monkeypatch)
    _build_engine("postgresql://u:p@h/d", statement_timeout_ms=None)
    assert "options" not in captured["kwargs"]["connect_args"]


def test_build_engine_postgres_omits_timeout_when_zero(monkeypatch):
    captured, _ = _capture_create_engine_call(monkeypatch)
    _build_engine("postgresql://u:p@h/d", statement_timeout_ms=0)
    assert "options" not in captured["kwargs"]["connect_args"]


def test_build_engine_clickhouse_ignores_statement_timeout(monkeypatch):
    """statement_timeout is a Postgres-only knob — never leaks into other
    dialects' connect_args."""
    captured, _ = _capture_create_engine_call(monkeypatch)
    _build_engine("clickhouse+native://u:p@h:9000/d", statement_timeout_ms=10000)
    assert "options" not in captured["kwargs"]["connect_args"]


def test_build_engine_mysql_ignores_statement_timeout(monkeypatch):
    captured, _ = _capture_create_engine_call(monkeypatch)
    _build_engine("mysql+pymysql://u:p@h:3306/d", statement_timeout_ms=10000)
    assert "options" not in captured["kwargs"]["connect_args"]


# --- collect_for_connection: large-table skip -----------------------------


def _stub_collector(rows):
    from collectors import metrics_collector
    return mock.patch.object(
        metrics_collector.MetricsCollector, "collect", return_value=rows,
    )


def test_skip_large_table_before_column_nulls(storage, caplog):
    """When size_bytes > threshold the collector's `.collect()` is never
    invoked for that table — verified by asserting on the mock."""
    _, project, conn = _seed(storage, dsn="postgresql://u:p@h/d")
    _set_safety(storage, conn["id"], skip_tables_larger_than_gb=1.0)

    import app.db as db_mod

    def fake_list_tables(self, schema):
        return [
            {"table_name": "small", "schema": schema},
            {"table_name": "huge", "schema": schema},
        ]

    def fake_stats(self, table_name, schema):
        if table_name == "huge":
            return {"table_name": "huge", "schema": schema,
                    "row_count": 0, "size_bytes": int(2e9), "last_analyze": None}
        return {"table_name": "small", "schema": schema,
                "row_count": 0, "size_bytes": int(100), "last_analyze": None}

    collected: list[str] = []

    def fake_collect(self, table_name, ts=None):
        collected.append(table_name)
        return [{"ts": datetime.now(UTC), "table_name": table_name,
                 "metric_name": "row_count", "value": 1.0}]

    from collectors import metrics_collector
    with mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=fake_list_tables), \
         mock.patch.object(db_mod.PostgresAdapter, "table_stats",
                           autospec=True, side_effect=fake_stats), \
         mock.patch.object(metrics_collector.MetricsCollector, "collect",
                           autospec=True, side_effect=fake_collect), \
         caplog.at_level(logging.INFO):
        collect_for_connection(project["id"], conn["id"])

    assert collected == ["small"]
    assert any("too_large" in r.message for r in caplog.records)


def test_skip_large_table_not_triggered_when_null(storage):
    _, project, conn = _seed(storage, dsn="postgresql://u:p@h/d")
    # No skip_tables_larger_than_gb set — default NULL.

    import app.db as db_mod

    def fake_list_tables(self, schema):
        return [{"table_name": "huge", "schema": schema}]

    stats_calls: list[str] = []

    def fake_stats(self, table_name, schema):
        stats_calls.append(table_name)
        return {"table_name": table_name, "schema": schema,
                "row_count": 0, "size_bytes": int(99e9), "last_analyze": None}

    collected: list[str] = []

    def fake_collect(self, table_name, ts=None):
        collected.append(table_name)
        return []

    from collectors import metrics_collector
    with mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=fake_list_tables), \
         mock.patch.object(db_mod.PostgresAdapter, "table_stats",
                           autospec=True, side_effect=fake_stats), \
         mock.patch.object(metrics_collector.MetricsCollector, "collect",
                           autospec=True, side_effect=fake_collect):
        collect_for_connection(project["id"], conn["id"])

    # collect() still runs; table_stats() not invoked when the skip is off.
    assert collected == ["huge"]
    assert stats_calls == []


def test_allowlist_drives_what_collect_runs(storage):
    """End-to-end: allowlist=['users'] → MetricsCollector.collect called
    only for 'users', skipped rows logged with 'not_in_allowlist'."""
    _, project, conn = _seed(storage, dsn="postgresql://u:p@h/d")
    _set_safety(storage, conn["id"], table_allowlist=json.dumps(["users"]))

    import app.db as db_mod

    def fake_list_tables(self, schema):
        return [
            {"table_name": "users", "schema": schema},
            {"table_name": "orders", "schema": schema},
        ]

    collected: list[str] = []

    def fake_collect(self, table_name, ts=None):
        collected.append(table_name)
        return []

    from collectors import metrics_collector
    with mock.patch.object(db_mod.PostgresAdapter, "list_tables",
                           autospec=True, side_effect=fake_list_tables), \
         mock.patch.object(metrics_collector.MetricsCollector, "collect",
                           autospec=True, side_effect=fake_collect):
        collect_for_connection(project["id"], conn["id"])

    assert collected == ["users"]
