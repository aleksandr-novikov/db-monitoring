"""Prepare and verify the Demo 3.2 Telegram path (#182).

The script never prints Telegram secrets. It reads ``TELEGRAM_BOT_TOKEN`` and
``TELEGRAM_CHAT_ID`` from ``.env``/environment, saves project-level settings,
sends test messages, and can send a real anomaly alert through the same
``notify_anomaly`` path used by collectors.

Usage:
    python -m scripts.telegram_demo configure
    python -m scripts.telegram_demo test
    python -m scripts.telegram_demo alert
    python -m scripts.telegram_demo fallback
    python -m scripts.telegram_demo all
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import UTC, datetime

from app import crypto, metrics_storage
from app.config import settings
from app.notifications.telegram import notify_anomaly, send_message


@dataclass(frozen=True)
class DemoTelegramProject:
    email: str
    slug: str
    table: str
    metric: str
    score: float


DEFAULT_PROJECTS = (
    DemoTelegramProject(
        email="demo@dbmonitor.app",
        slug="retail-postgres",
        table="events",
        metric="null_rate",
        score=-0.42,
    ),
    DemoTelegramProject(
        email="lake@dbmonitor.app",
        slug="iceberg-lakehouse",
        table="sessions",
        metric="row_count",
        score=-0.37,
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

        throttle = int(cfg.get("throttle_minutes") or 30) if respect_throttle else 0
        notify_anomaly(
            project["id"],
            spec.table,
            datetime.now(UTC).isoformat(timespec="seconds"),
            spec.score,
            bot_token=token,
            chat_id=cfg["telegram_chat_id"],
            throttle_minutes=throttle,
            metric=spec.metric,
        )
        print(
            "alert attempted: "
            f"{project['name']} ({project['slug']}) {spec.table}/{spec.metric}"
        )


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
        choices=("configure", "test", "alert", "fallback", "all"),
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
    args = parser.parse_args()

    projects = DEFAULT_PROJECTS
    if args.command in {"configure", "all"}:
        configure(projects, args.throttle_minutes)
    if args.command in {"test", "all"}:
        test(projects)
    if args.command in {"alert", "all"}:
        alert(projects, respect_throttle=args.respect_throttle)
    if args.command == "fallback":
        fallback(projects)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
