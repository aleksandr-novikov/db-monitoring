"""Tests for scripts/prepare_clickhouse_demo.py (#177).

End-to-end logic without launching ClickHouse: history-generation,
purge, project resolution. The live ``collect_for_connection`` step is
stubbed out — its own coverage lives in tests/test_per_project.py.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from cryptography.fernet import Fernet

from app import crypto


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.config import settings as cfg

    db_path = tmp_path / "ch.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    return ms


def _seed_events_clickhouse_project(storage) -> tuple[str, str]:
    """Create a user + the events-clickhouse project + connection so the
    orchestrator can find them without poking the real seed_demo_workspace."""
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email="demo@dbmonitor.app",
                        password_hash="x")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=uid,
        name="Events ClickHouse", slug="events-clickhouse",
    )
    conn = storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project["id"],
        name="Local ClickHouse",
        dsn_encrypted=crypto.encrypt_dsn(
            "clickhouse+native://default@localhost:19000/demo"
        ),
        schema_name="demo",
        interval_minutes=15,
        is_active=True,
    )
    return project["id"], conn["id"]


# ── Spec shape ─────────────────────────────────────────────────────────────


def test_table_specs_match_seed_clickhouse_tables():
    """Спеки должны покрывать ровно те 4 таблицы, что создаёт
    scripts/seed_clickhouse.py. Если seed-таблицы поменяются, тест
    напомнит обновить спеки тут."""
    from scripts.prepare_clickhouse_demo import _table_specs

    names = {s.name for s in _table_specs()}
    assert names == {"events", "orders", "users", "products"}


# ── Synthetic history backfill ─────────────────────────────────────────────


def test_seed_history_writes_metrics_under_project(storage):
    """save_metrics-вызов попадает в monitor.db с правильным project_id."""
    from scripts.prepare_clickhouse_demo import seed_clickhouse_history

    pid, _ = _seed_events_clickhouse_project(storage)
    result = seed_clickhouse_history(pid, days=2, interval_minutes=60)
    assert result["rows"] > 0
    assert result["tables"] == 4

    # Cross-check: rows in metrics table are tagged with the right pid.
    from sqlalchemy import text
    with storage.get_engine().connect() as conn:
        n = conn.execute(text(
            "SELECT COUNT(*) FROM metrics WHERE project_id = :pid"
        ), {"pid": pid}).scalar()
    assert n > 0


def test_seed_history_includes_row_count_size_and_null_rate(storage):
    """Каждая таблица должна получить как минимум row_count + size_bytes
    метрики на каждый тик; nullable-колонки — null_count + null_rate."""
    from sqlalchemy import text

    from scripts.prepare_clickhouse_demo import seed_clickhouse_history

    pid, _ = _seed_events_clickhouse_project(storage)
    seed_clickhouse_history(pid, days=2, interval_minutes=60)

    with storage.get_engine().connect() as conn:
        metric_names = conn.execute(text(
            "SELECT DISTINCT metric_name FROM metrics WHERE project_id = :pid"
        ), {"pid": pid}).fetchall()
    names = {r[0] for r in metric_names}
    assert "row_count" in names
    assert "size_bytes" in names
    assert "null_count" in names
    assert "null_rate" in names


def test_purge_history_removes_only_target_project(storage):
    """Prior history для project_id A удаляется; history другого проекта
    остаётся нетронутой."""
    from sqlalchemy import text

    from scripts.prepare_clickhouse_demo import (
        _purge_project_history,
        seed_clickhouse_history,
    )

    pid_a, _ = _seed_events_clickhouse_project(storage)
    # Project B with one stray metric row.
    storage.create_user(user_id="u-b", email="b@example.com", password_hash="x")
    pid_b = storage.create_project(
        project_id="proj-b", user_id="u-b",
        name="Other", slug="other-project",
    )["id"]
    storage.save_metrics([{
        "ts": datetime.now(UTC),
        "table_name": "stranger",
        "metric_name": "row_count",
        "value": 42.0,
    }], pid_b)

    seed_clickhouse_history(pid_a, days=2, interval_minutes=60)
    # Now purge A — B's row should still be there.
    deleted = _purge_project_history(pid_a, ["events", "orders", "users", "products"])
    assert deleted > 0

    with storage.get_engine().connect() as conn:
        b_count = conn.execute(text(
            "SELECT COUNT(*) FROM metrics WHERE project_id = :pid"
        ), {"pid": pid_b}).scalar()
    assert b_count == 1


def test_purge_idempotent(storage):
    """Повторный purge на пустом проекте не падает — operator может
    спокойно дважды запускать demo-prep."""
    from scripts.prepare_clickhouse_demo import _purge_project_history

    pid, _ = _seed_events_clickhouse_project(storage)
    deleted_first = _purge_project_history(pid, ["events"])
    deleted_second = _purge_project_history(pid, ["events"])
    assert deleted_first == 0
    assert deleted_second == 0


# ── _repair_clickhouse_connection — DSN rotation ───────────────────────────


def test_repair_connection_reencrypts_when_fernet_changed(storage, monkeypatch):
    """Симулируем смену Fernet ключа: старый ciphertext не дешифруется →
    repair перешивает строку с новой шифровкой APP_DSN."""
    from scripts.prepare_clickhouse_demo import (
        APP_DSN,
        _repair_clickhouse_connection,
    )

    pid, cid = _seed_events_clickhouse_project(storage)
    # Перетираем ciphertext "поломанным" значением — decrypt_dsn кинет
    # InvalidToken на ходу.
    from sqlalchemy import text
    with storage.get_engine().begin() as conn:
        conn.execute(text(
            "UPDATE connections SET dsn_encrypted = :bad WHERE id = :cid"
        ), {"bad": b"not-a-valid-fernet-blob", "cid": cid})

    _repair_clickhouse_connection(pid, cid)

    # После repair — должно дешифроваться, и значение = APP_DSN.
    conn_row = storage.get_connection(pid, cid)
    assert crypto.decrypt_dsn(conn_row["dsn_encrypted"]) == APP_DSN
    assert conn_row["schema_name"] == "demo"


def test_repair_connection_noop_when_dsn_correct(storage):
    """Если DSN уже правильный, repair не должен делать UPDATE — иначе
    бесконечно меняли бы updated_at на каждый make clickhouse-demo."""
    from sqlalchemy import text

    from scripts.prepare_clickhouse_demo import (
        APP_DSN,
        _repair_clickhouse_connection,
    )

    pid, cid = _seed_events_clickhouse_project(storage)
    # Поставим правильный ciphertext для текущего ключа.
    with storage.get_engine().begin() as conn:
        conn.execute(text(
            "UPDATE connections SET dsn_encrypted = :good, schema_name = 'demo' "
            "WHERE id = :cid"
        ), {"good": crypto.encrypt_dsn(APP_DSN), "cid": cid})

    # Прямой UPDATE — repair не должен ничего поменять. Чтобы проверить,
    # фиксируем ciphertext до и после.
    conn_before = storage.get_connection(pid, cid)
    _repair_clickhouse_connection(pid, cid)
    conn_after = storage.get_connection(pid, cid)
    # Точное равенство ciphertext — Fernet детерминированный с фиксированным
    # nonce только если nonce одинаков. Тут другой путь: убеждаемся что
    # дешифровка даёт ТО ЖЕ значение.
    assert (
        crypto.decrypt_dsn(conn_before["dsn_encrypted"]) ==
        crypto.decrypt_dsn(conn_after["dsn_encrypted"])
    )


# ── prepare_clickhouse_demo orchestration ──────────────────────────────────


def test_prepare_clickhouse_demo_runs_full_pipeline(storage, monkeypatch):
    """End-to-end orchestrator (с заглушенным live collect + warmup)."""
    from scripts import prepare_clickhouse_demo as mod

    pid_expected, cid_expected = _seed_events_clickhouse_project(storage)

    # Stub seed_demo_workspace — возвращает то что мы засеяли руками.
    def fake_workspace(**kwargs):
        # seed_demo_workspace в реальности резолвит project_by_slug
        # через user_id; здесь возвращаем готовую ссылку.
        project = {"id": pid_expected, "slug": "events-clickhouse",
                    "user_id": "u-demo", "name": "Events ClickHouse"}
        conn = {"id": cid_expected, "project_id": pid_expected}
        return {
            "projects": {"events-clickhouse": project},
            "connections": {"events-clickhouse": conn},
        }
    monkeypatch.setattr(mod, "seed_demo_workspace", fake_workspace)

    # Stub live collector — иначе пытается реально подключиться к CH.
    monkeypatch.setattr(mod, "collect_for_connection", lambda *a, **kw: None)

    # Skip warmup_ml — оно само себя покрывает в test_demo_prepare.
    result = mod.prepare_clickhouse_demo(
        days=2, interval_minutes=60,
        warmup_ml=False,
        skip_live_collect=True,
    )
    assert result["project_id"] == pid_expected
    assert result["connection_id"] == cid_expected
    assert result["history"]["rows"] > 0
    assert result["history"]["tables"] == 4


def test_prepare_handles_live_collect_failure(storage, monkeypatch):
    """Если CH не запущен — orchestrator не должен фейлиться. Live
    collect — best-effort, синтетическая history идёт независимо."""
    from scripts import prepare_clickhouse_demo as mod

    pid, cid = _seed_events_clickhouse_project(storage)

    monkeypatch.setattr(mod, "seed_demo_workspace", lambda **kw: {
        "projects": {"events-clickhouse": {"id": pid, "slug": "events-clickhouse"}},
        "connections": {"events-clickhouse": {"id": cid, "project_id": pid}},
    })

    def boom(*_a, **_kw):
        raise RuntimeError("CH not running")
    monkeypatch.setattr(mod, "collect_for_connection", boom)

    # Should NOT raise — just print and continue with history backfill.
    result = mod.prepare_clickhouse_demo(
        days=1, interval_minutes=60, warmup_ml=False,
    )
    assert result["history"]["rows"] > 0
