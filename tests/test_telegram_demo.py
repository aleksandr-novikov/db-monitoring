"""Tests for scripts/telegram_demo.py (#182 + #202).

#202 specifically: ensure the ClickHouse demo project (``events-clickhouse``)
is included in ``DEFAULT_PROJECTS`` so ``configure`` saves Telegram
settings for all three demo bases (Postgres + Iceberg + ClickHouse).
"""

from __future__ import annotations

import uuid

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

    db_path = tmp_path / "tgdemo.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    return ms


def _seed_demo_projects(storage) -> dict[str, str]:
    """Минимальная имитация seed_demo_workspace для трёх demo-проектов
    из DEFAULT_PROJECTS. Возвращает {slug: project_id}."""
    specs = [
        ("demo@dbmonitor.app", "retail-postgres", "Retail Postgres"),
        ("lake@dbmonitor.app", "iceberg-lakehouse", "Iceberg Lakehouse"),
        ("demo@dbmonitor.app", "events-clickhouse", "Events ClickHouse"),
    ]
    # Уникальные юзеры по email (demo@ один для двух проектов).
    user_by_email: dict[str, str] = {}
    project_ids: dict[str, str] = {}
    for email, slug, name in specs:
        if email not in user_by_email:
            uid = uuid.uuid4().hex
            storage.create_user(user_id=uid, email=email, password_hash="x")
            user_by_email[email] = uid
        project = storage.create_project(
            project_id=uuid.uuid4().hex,
            user_id=user_by_email[email],
            name=name,
            slug=slug,
        )
        project_ids[slug] = project["id"]
    return project_ids


# ── DEFAULT_PROJECTS shape ────────────────────────────────────────────────


def test_default_projects_includes_all_three_backends():
    """Acceptance из #202: configure должен сохранять конфиг для всех
    трёх demo-бэкендов — Postgres, Iceberg, ClickHouse."""
    from scripts.telegram_demo import DEFAULT_PROJECTS

    slugs = {p.slug for p in DEFAULT_PROJECTS}
    assert "retail-postgres" in slugs
    assert "iceberg-lakehouse" in slugs
    assert "events-clickhouse" in slugs, (
        "ClickHouse demo project (events-clickhouse) missing from "
        "DEFAULT_PROJECTS — добавьте через DemoTelegramProject"
    )


def test_clickhouse_spec_has_strong_enough_score_for_quality_gate():
    """#171 quality gate отбрасывает алерты с |score| < 0.05. Demo-spec
    должен быть выше — иначе на сцене alert не уйдёт."""
    from app.config import settings
    from scripts.telegram_demo import DEFAULT_PROJECTS

    ch = next(p for p in DEFAULT_PROJECTS if p.slug == "events-clickhouse")
    assert abs(ch.score) >= settings.ANOMALY_NOTIFY_MIN_SCORE_MAGNITUDE


# ── configure end-to-end with three projects ─────────────────────────────


def test_configure_saves_notifications_for_all_default_projects(
    storage, monkeypatch,
):
    """End-to-end: configure(DEFAULT_PROJECTS) → у каждого из трёх demo
    проектов есть row в project_notifications с расшифрованным токеном."""
    from app.config import settings as cfg
    from scripts.telegram_demo import DEFAULT_PROJECTS, configure

    monkeypatch.setattr(cfg, "TELEGRAM_BOT_TOKEN",
                        "0000000001:" + "A" * 35)
    monkeypatch.setattr(cfg, "TELEGRAM_CHAT_ID", "42")

    pids = _seed_demo_projects(storage)
    configure(DEFAULT_PROJECTS, throttle_minutes=15)

    for slug in ("retail-postgres", "iceberg-lakehouse", "events-clickhouse"):
        row = storage.get_project_notifications(pids[slug])
        assert row is not None, f"missing config for {slug}"
        assert row["telegram_chat_id"] == "42"
        decrypted = crypto.decrypt_token(row["telegram_bot_token"])
        assert decrypted.startswith("0000000001:")


def test_configure_missing_clickhouse_project_raises(storage, monkeypatch):
    """Если seed_demo_workspace не создал events-clickhouse,
    _resolve_project поднимает SystemExit с понятным сообщением.
    Защита от тихих skip-ов."""
    from app.config import settings as cfg
    from scripts.telegram_demo import DEFAULT_PROJECTS, configure

    monkeypatch.setattr(cfg, "TELEGRAM_BOT_TOKEN",
                        "0000000001:" + "A" * 35)
    monkeypatch.setattr(cfg, "TELEGRAM_CHAT_ID", "42")

    # Создаём только два первых проекта; events-clickhouse — нет.
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email="demo@dbmonitor.app",
                        password_hash="x")
    storage.create_project(project_id=uuid.uuid4().hex, user_id=uid,
                            name="P1", slug="retail-postgres")
    lake_uid = uuid.uuid4().hex
    storage.create_user(user_id=lake_uid, email="lake@dbmonitor.app",
                        password_hash="x")
    storage.create_project(project_id=uuid.uuid4().hex, user_id=lake_uid,
                            name="P2", slug="iceberg-lakehouse")

    with pytest.raises(SystemExit) as exc_info:
        configure(DEFAULT_PROJECTS, throttle_minutes=15)
    assert "events-clickhouse" in str(exc_info.value)
