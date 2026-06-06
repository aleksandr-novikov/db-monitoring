"""Tests for scripts/recover_monitor_db.py (#213).

End-to-end за исключением docker / app-up — те шаги мокаются.
Главные инварианты:
- preserve переименовывает в monitor.db.broken.<ts>, не удаляет
- повторный preserve на отсутствующем файле — no-op
- broken-файлы суффиксированы timestamp-ом и не перезаписывают друг друга
- verify_via_healthz возвращает True только если backend = postgresql
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from unittest.mock import patch

import pytest


@pytest.fixture
def recover_module(monkeypatch, tmp_path):
    """Изолируем MONITOR_DB на tmp_path чтобы не трогать репо-файл."""
    import scripts.recover_monitor_db as mod
    monkeypatch.setattr(mod, "MONITOR_DB", tmp_path / "monitor.db")
    return mod


# ── preserve_broken_db ────────────────────────────────────────────────────


def test_preserve_renames_existing_file(recover_module):
    monitor = recover_module.MONITOR_DB
    monitor.write_bytes(b"corrupt-fake-bytes")

    preserved = recover_module.preserve_broken_db()

    assert preserved is not None
    assert preserved.exists()
    assert preserved.read_bytes() == b"corrupt-fake-bytes"
    assert not monitor.exists()
    assert preserved.name.startswith("monitor.db.broken.")


def test_preserve_returns_none_when_no_file(recover_module):
    assert not recover_module.MONITOR_DB.exists()
    assert recover_module.preserve_broken_db() is None


def test_preserve_does_not_overwrite_existing_broken_file(recover_module):
    """Два последовательных запуска recovery должны создавать разные
    broken-файлы (timestamp в суффиксе). Иначе теряем post-mortem."""
    monitor = recover_module.MONITOR_DB
    monitor.write_bytes(b"first-broken")
    first = recover_module.preserve_broken_db()

    monitor.write_bytes(b"second-broken")
    # Симулируем что во второй раз timestamp другой.
    fake_now = datetime.now(UTC).timestamp() + 5
    with patch(
        "scripts.recover_monitor_db.datetime",
    ) as fake_dt:
        fake_dt.now.return_value.timestamp.return_value = fake_now
        fake_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        second = recover_module.preserve_broken_db()

    assert first is not None
    assert second is not None
    assert first != second
    assert first.exists()
    assert second.exists()
    assert first.read_bytes() == b"first-broken"
    assert second.read_bytes() == b"second-broken"


# ── _container_health ────────────────────────────────────────────────────


def test_container_health_returns_absent_when_docker_missing(monkeypatch):
    """Если docker не установлен — возвращаем 'absent', не падаем."""
    import scripts.recover_monitor_db as mod

    def boom(*_a, **_kw):
        raise FileNotFoundError("docker not in PATH")
    monkeypatch.setattr("subprocess.run", boom)
    assert mod._container_health("anything") == "absent"


def test_container_health_returns_absent_when_container_missing(monkeypatch):
    """docker inspect на отсутствующий контейнер → returncode != 0."""
    import subprocess

    import scripts.recover_monitor_db as mod

    def fake_run(*_a, **_kw):
        return subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="",
        )
    monkeypatch.setattr("subprocess.run", fake_run)
    assert mod._container_health("nope") == "absent"


def test_container_health_returns_status_when_present(monkeypatch):
    import subprocess

    import scripts.recover_monitor_db as mod

    def fake_run(*_a, **_kw):
        return subprocess.CompletedProcess(
            args=[], returncode=0, stdout="healthy\n", stderr="",
        )
    monkeypatch.setattr("subprocess.run", fake_run)
    assert mod._container_health("anything") == "healthy"


# ── ensure_timescale_running ─────────────────────────────────────────────


def test_ensure_timescale_noop_when_already_healthy(monkeypatch):
    import scripts.recover_monitor_db as mod

    monkeypatch.setattr(mod, "_container_health", lambda name: "healthy")
    called = []
    monkeypatch.setattr(
        "subprocess.run",
        lambda *a, **kw: called.append(("run", a, kw)),
    )
    mod.ensure_timescale_running()
    assert called == []  # docker compose up не вызывается


def test_ensure_timescale_raises_on_timeout(monkeypatch):
    """Если timescaledb не становится healthy за timeout — SystemExit."""
    import scripts.recover_monitor_db as mod

    health_seq = iter(["starting", "starting", "starting"])
    monkeypatch.setattr(
        mod, "_container_health",
        lambda name: next(health_seq, "absent"),
    )
    monkeypatch.setattr("subprocess.run", lambda *a, **kw: None)
    monkeypatch.setattr(time, "sleep", lambda _: None)  # ускоряем

    with pytest.raises(SystemExit, match="не стал healthy"):
        mod.ensure_timescale_running(timeout_s=1)


# ── verify_via_healthz ───────────────────────────────────────────────────


def test_verify_returns_true_on_postgresql_backend(monkeypatch):
    import json
    from io import BytesIO

    import scripts.recover_monitor_db as mod

    body = json.dumps({
        "status": "ok",
        "checks": {"monitor_db": {"status": "ok", "backend": "postgresql"}},
    }).encode()

    class FakeResp:
        def read(self):
            return body
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *a, **kw: FakeResp(),
    )
    assert mod.verify_via_healthz("http://x/healthz") is True
    _ = BytesIO  # silence import


def test_verify_returns_false_on_sqlite_backend(monkeypatch):
    """Если recovery не закончился — backend всё ещё sqlite."""
    import json

    import scripts.recover_monitor_db as mod

    body = json.dumps({
        "checks": {"monitor_db": {"status": "ok", "backend": "sqlite"}},
    }).encode()

    class FakeResp:
        def read(self):
            return body
        def __enter__(self):
            return self
        def __exit__(self, *a):
            pass

    monkeypatch.setattr(
        "urllib.request.urlopen",
        lambda *a, **kw: FakeResp(),
    )
    assert mod.verify_via_healthz("http://x/healthz") is False


def test_verify_returns_false_when_app_unreachable(monkeypatch):
    """App не поднят / wrong port — verify не падает, возвращает False."""
    import urllib.error

    import scripts.recover_monitor_db as mod

    def boom(*_a, **_kw):
        raise urllib.error.URLError("Connection refused")

    monkeypatch.setattr("urllib.request.urlopen", boom)
    monkeypatch.setattr(time, "sleep", lambda _: None)
    assert mod.verify_via_healthz("http://x/healthz", timeout_s=1) is False
