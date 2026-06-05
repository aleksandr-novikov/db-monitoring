"""Prepare and verify the Demo 3.2 Telegram path (#182).

The script never prints Telegram secrets. It reads ``TELEGRAM_BOT_TOKEN`` and
``TELEGRAM_CHAT_ID`` from ``.env``/environment, saves project-level settings,
sends test messages, and can send a real anomaly alert through the same
``notify_anomaly`` path used by collectors.

Usage:
    python -m scripts.telegram_demo configure
    python -m scripts.telegram_demo test
    python -m scripts.telegram_demo alert
    python -m scripts.telegram_demo schema_drift [--delay N]
    python -m scripts.telegram_demo changepoint [--delay N]
    python -m scripts.telegram_demo fallback
    python -m scripts.telegram_demo all
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime

from app import crypto, metrics_storage
from app.config import settings
from app.notifications.telegram import (
    notify_anomaly,
    notify_changepoint,
    notify_schema_drift,
    send_message,
)


@dataclass(frozen=True)
class DemoTelegramProject:
    email: str
    slug: str
    table: str
    metric: str
    score: float
    changepoint_before: float = 75_000.0
    changepoint_after: float = 95_000.0


DEFAULT_PROJECTS = (
    DemoTelegramProject(
        email="demo@dbmonitor.app",
        slug="retail-postgres",
        table="events",
        metric="null_rate",
        score=-0.42,
        changepoint_before=0.02,
        changepoint_after=0.18,
    ),
    DemoTelegramProject(
        email="lake@dbmonitor.app",
        slug="iceberg-lakehouse",
        table="sessions",
        metric="row_count",
        score=-0.37,
        changepoint_before=976_000.0,
        changepoint_after=1_237_000.0,
    ),
    # #202: ClickHouse demo проект. seed_demo_workspace создаёт его как
    # events-clickhouse под demo@dbmonitor.app — issue упоминает slug
    # 'clickhouse-demo' / 'clickhouse@dbmonitor.app', но на master реально
    # events-clickhouse / demo@. На сцене показывает что per-project alert
    # работает для всех трёх бэкендов (Postgres + Iceberg + ClickHouse)
    # одинаково. ``events`` — самая bursty CH-таблица; score выше
    # ANOMALY_NOTIFY_MIN_SCORE_MAGNITUDE из #171, проходит quality gate.
    DemoTelegramProject(
        email="demo@dbmonitor.app",
        slug="events-clickhouse",
        table="events",
        metric="row_count",
        score=-0.35,
    ),
)


def _require_env() -> tuple[str, str]:
    token = settings.TELEGRAM_BOT_TOKEN.strip()
    chat_id = settings.TELEGRAM_CHAT_ID.strip()
    if not token or not chat_id:
        msg = (
            "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required in .env "
            "or the environment."
        )
        raise SystemExit(msg)
    return token, chat_id


def _resolve_project(spec: DemoTelegramProject) -> dict:
    user = metrics_storage.get_user_by_email(spec.email)
    if user is None:
        raise SystemExit(f"Demo user not found: {spec.email}")
    project = metrics_storage.get_project_by_slug(user["id"], spec.slug)
    if project is None:
        raise SystemExit(f"Demo project not found: {spec.email}/{spec.slug}")
    return project


def _load_cfg(project: dict) -> tuple[str, str, int]:
    """Return (bot_token, chat_id, throttle_minutes) or raise SystemExit."""
    cfg = metrics_storage.get_project_notifications(project["id"])
    if not cfg or not cfg.get("telegram_bot_token") or not cfg.get("telegram_chat_id"):
        raise SystemExit(
            f"Telegram settings are not configured for {project['name']}."
        )
    try:
        token = crypto.decrypt_token(cfg["telegram_bot_token"])
    except crypto.InvalidToken as exc:
        raise SystemExit(
            f"Saved Telegram token cannot be decrypted for {project['name']}."
        ) from exc
    return token, cfg["telegram_chat_id"], int(cfg.get("throttle_minutes") or 30)


def configure(projects: tuple[DemoTelegramProject, ...], throttle_minutes: int) -> None:
    token, chat_id = _require_env()
    for spec in projects:
        project = _resolve_project(spec)
        metrics_storage.save_project_notifications(
            project["id"],
            telegram_bot_token=crypto.encrypt_token(token),
            telegram_chat_id=chat_id,
            throttle_minutes=throttle_minutes,
        )
        print(f"configured: {project['name']} ({project['slug']})")


def test(projects: tuple[DemoTelegramProject, ...]) -> None:
    token, chat_id = _require_env()
    failed = False
    for spec in projects:
        project = _resolve_project(spec)
        text = (
            "✅ DB Monitor demo Telegram test\n"
            f"Project: {project['name']} ({project['slug']})"
        )
        ok, error = send_message(text, bot_token=token, chat_id=chat_id)
        if ok:
            print(f"test sent: {project['name']} ({project['slug']})")
        else:
            failed = True
            print(f"test failed: {project['name']} ({project['slug']}): {error}")
    if failed:
        raise SystemExit(1)


def alert(projects: tuple[DemoTelegramProject, ...], *, respect_throttle: bool) -> None:
    for spec in projects:
        project = _resolve_project(spec)
        token, chat_id, throttle_cfg = _load_cfg(project)
        throttle = throttle_cfg if respect_throttle else 0
        notify_anomaly(
            project["id"],
            spec.table,
            datetime.now(UTC).isoformat(timespec="seconds"),
            spec.score,
            bot_token=token,
            chat_id=chat_id,
            throttle_minutes=throttle,
            metric=spec.metric,
        )
        print(
            "alert attempted: "
            f"{project['name']} ({project['slug']}) {spec.table}/{spec.metric}"
        )


def schema_drift(
    projects: tuple[DemoTelegramProject, ...], *, delay: int
) -> None:
    """Send a synthetic schema_drift notification per project with a pause between them."""
    for i, spec in enumerate(projects):
        project = _resolve_project(spec)
        token, chat_id, _ = _load_cfg(project)
        synthetic_events = [{
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "table_name": spec.table,
            "change_type": "column_added",
            "column_name": "revenue",
            "details": {"after": {"type": "numeric"}},
        }]
        notify_schema_drift(
            project["id"],
            spec.table,
            synthetic_events,
            bot_token=token,
            chat_id=chat_id,
            throttle_minutes=0,
        )
        print(f"schema_drift sent: {project['name']} ({project['slug']}) {spec.table}")
        if delay > 0 and i < len(projects) - 1:
            time.sleep(delay)


def changepoint(
    projects: tuple[DemoTelegramProject, ...], *, delay: int
) -> None:
    """Send a synthetic changepoint notification per project with a pause between them."""
    for i, spec in enumerate(projects):
        project = _resolve_project(spec)
        token, chat_id, _ = _load_cfg(project)
        value_before, value_after = spec.changepoint_before, spec.changepoint_after
        notify_changepoint(
            project["id"],
            spec.table,
            spec.metric,
            value_before,
            value_after,
            datetime.now(UTC).isoformat(timespec="seconds"),
            bot_token=token,
            chat_id=chat_id,
            throttle_minutes=0,
        )
        print(f"changepoint sent: {project['name']} ({project['slug']}) {spec.table}/{spec.metric}")
        if delay > 0 and i < len(projects) - 1:
            time.sleep(delay)


def fallback(projects: tuple[DemoTelegramProject, ...]) -> None:
    for spec in projects:
        project = _resolve_project(spec)
        message = (
            "🚨 DB Monitor fallback: Telegram API недоступен на демо.\n"
            f"Проект: {project['name']} ({project['slug']})\n"
            f"Таблица: {spec.table}\n"
            f"Метрика: {spec.metric}\n"
            "Статус: fallback notification row для audit trail."
        )
        metrics_storage.save_notification(
            project_id=project["id"],
            event_type="anomaly",
            table_name=spec.table,
            metric_name=spec.metric,
            message=message,
            status="failed",
            error="fallback_only",
            chat_id=settings.TELEGRAM_CHAT_ID.strip() or None,
        )
        print(f"fallback row: {project['name']} ({project['slug']})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("configure", "test", "alert", "schema_drift", "changepoint", "fallback", "all"),
        help="Demo Telegram action to run.",
    )
    parser.add_argument(
        "--throttle-minutes",
        type=int,
        default=30,
        help="Throttle saved by configure (default: 30).",
    )
    parser.add_argument(
        "--respect-throttle",
        action="store_true",
        help="Do not bypass throttle for the alert command.",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=8,
        help="Seconds between per-project notifications for schema_drift/changepoint (default: 8).",
    )
    args = parser.parse_args()

    projects = DEFAULT_PROJECTS
    if args.command in {"configure", "all"}:
        configure(projects, args.throttle_minutes)
    if args.command in {"test", "all"}:
        test(projects)
    if args.command in {"alert", "all"}:
        alert(projects, respect_throttle=args.respect_throttle)
    if args.command == "schema_drift":
        schema_drift(projects, delay=args.delay)
    if args.command == "changepoint":
        changepoint(projects, delay=args.delay)
    if args.command == "fallback":
        fallback(projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
