"""Telegram Bot notifications for anomalies, schema drift, and change-points
(#38 → #143 multi-tenant).

Per-tenant in #143: every notify_* function takes the project's Telegram
configuration explicitly (``bot_token``, ``chat_id``, ``throttle_minutes``).
Loading that config is the caller's job (collectors/scheduler.py loads it
once per tick via ``metrics_storage.get_project_notifications``). This
module deliberately does NOT read ``settings.TELEGRAM_*`` — there is no
global fallback. If a tenant has not configured Telegram, notifications
for that tenant are silently dropped, never routed to the admin's chat.

Public entry points called from collectors/scheduler.py:
  notify_anomaly(project_id, bot_token, chat_id, table, ts, score, ...)
  notify_schema_drift(project_id, bot_token, chat_id, table, events, ...)
  notify_changepoint(project_id, bot_token, chat_id, table, metric, ...)

All functions are silent on errors — notification failures never propagate
to the caller. Each delivery attempt (success or failure) is persisted via
metrics_storage.save_notification so the UI can show a full audit trail (#76).
"""

import asyncio
import logging

from sqlalchemy import text
from telegram import Bot
from telegram.error import TelegramError

from app.llm import explain_anomaly
from app.metrics_storage import (
    get_engine,
    is_throttled,
    save_notification,
    update_throttle,
)

logger = logging.getLogger(__name__)


def send_message(
    text: str, *, bot_token: str | None, chat_id: str | None,
) -> tuple[bool, str | None]:
    """Send a plain-text message via Bot API.

    Returns (ok, error). When ok is False, error is a short reason string
    suitable for storing alongside the notification record. Returns
    (False, "not_configured") if token/chat are not set — the call is still
    audited as a failed attempt by the caller.
    """
    if not bot_token or not chat_id:
        logger.debug("Telegram not configured for this caller, skipping")
        return False, "not_configured"

    async def _send() -> None:
        async with Bot(bot_token) as bot:
            await bot.send_message(chat_id=chat_id, text=text)

    try:
        asyncio.run(_send())
        return True, None
    except TelegramError as exc:
        logger.warning("Telegram send failed: %s", exc)
        return False, f"telegram_error: {exc}"
    except Exception as exc:
        logger.warning("Telegram send error: %s", exc)
        return False, f"error: {exc}"


def _record(
    *,
    project_id: str,
    event_type: str,
    message: str,
    ok: bool,
    error: str | None,
    chat_id: str | None,
    table: str | None = None,
    metric: str | None = None,
) -> None:
    """Persist a notification attempt. Never raises — auditing is best-effort."""
    try:
        save_notification(
            event_type=event_type,
            message=message,
            status="sent" if ok else "failed",
            table_name=table,
            metric_name=metric,
            error=error,
            chat_id=chat_id,
            project_id=project_id,
        )
    except Exception as exc:  # pragma: no cover - storage failure shouldn't break alerts
        logger.warning("Failed to persist notification audit: %s", exc)


_RULE_BASED_CONFIDENCE: float = 0.3


def _fmt_ts(ts: str) -> str:
    """Format ISO timestamp to '2026-05-11 19:13 UTC'."""
    return ts.replace("T", " ")[:16] + " UTC"


def _project_label(project_id: str) -> str:
    """Best-effort human-readable project label for Telegram text."""
    try:
        with get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT name, slug FROM projects WHERE id = :project_id"),
                {"project_id": project_id},
            ).fetchone()
        if row:
            name = str(row[0] or "").strip()
            slug = str(row[1] or "").strip()
            if name and slug:
                return f"{name} ({slug})"
            return name or slug or project_id
    except Exception as exc:  # pragma: no cover - label lookup is best-effort
        logger.debug("Project label lookup failed for %s: %s", project_id, exc)
    return project_id


def notify_anomaly(
    project_id: str,
    table: str,
    ts: str,
    score: float,
    *,
    bot_token: str | None,
    chat_id: str | None,
    throttle_minutes: int | None = None,
    metric: str = "row_count",
) -> None:
    """Send anomaly alert. Throttled per (project, table, event_key)."""
    event_key = "anomaly"
    if is_throttled(project_id, table, event_key, throttle_minutes=throttle_minutes):
        return

    project_label = _project_label(project_id)
    result = explain_anomaly(table, metric, ts, project_id=project_id)
    is_llm = result.get("confidence", 0) > _RULE_BASED_CONFIDENCE
    body = result.get("explanation", "Требуется ручная проверка данных.") if is_llm else "Требуется ручная проверка данных."

    text = (
        f"\U0001f6a8 DB Monitor: аномалия\n"
        f"Проект: {project_label}\n"
        f"Таблица: {table}\n"
        f"Метрика: {metric}\n"
        f"Время: {_fmt_ts(ts)}\n"
        f"Score: {score:.4f}\n\n"
        f"{body}"
    )
    ok, error = send_message(text, bot_token=bot_token, chat_id=chat_id)
    _record(project_id=project_id, event_type="anomaly", message=text,
            ok=ok, error=error, chat_id=chat_id,
            table=table, metric=metric)
    if ok:
        update_throttle(project_id, table, event_key)


def load_project_telegram_config(project_id: str) -> tuple[str, str, int] | None:
    """Return ``(bot_token, chat_id, throttle_minutes)`` for a project, or None.

    Shared by per_project.py and scheduler.py so both paths use identical
    decryption logic (#197). Returns None when Telegram is not configured or
    the token fails to decrypt (logged as a warning).
    """
    from app import crypto
    from app.metrics_storage import get_project_notifications

    cfg = get_project_notifications(project_id)
    if cfg is None:
        return None
    token_encrypted = cfg.get("telegram_bot_token")
    chat_id = cfg.get("telegram_chat_id")
    if not token_encrypted or not chat_id:
        return None
    try:
        bot_token = crypto.decrypt_token(token_encrypted)
    except crypto.InvalidToken:
        logger.warning(
            "[project=%s] telegram_bot_token failed to decrypt — Fernet key "
            "rotated without re-encrypt? Re-save the config in Settings.",
            project_id,
        )
        return None
    return bot_token, chat_id, int(cfg.get("throttle_minutes") or 30)


def notify_schema_drift(
    project_id: str,
    table: str,
    events: list[dict],
    *,
    bot_token: str | None,
    chat_id: str | None,
    throttle_minutes: int | None = None,
) -> None:
    """Send schema-drift alert for a batch of events on one table."""
    if not events:
        return

    event_key = "schema_drift"
    if is_throttled(project_id, table, event_key, throttle_minutes=throttle_minutes):
        return

    lines = []
    for e in events:
        change_type = e.get("change_type", "")
        column = e.get("column_name", "")
        details = e.get("details", {})
        col_type = (
            (details.get("after") or details.get("before") or {}).get("type", "")
        )
        line = f"  • {change_type} — {column}"
        if col_type:
            line += f" ({col_type})"
        lines.append(line)

    project_label = _project_label(project_id)
    text = f"\U0001f4cb [{table}] Дрейф схемы:\nПроект: {project_label}\n" + "\n".join(lines)
    ok, error = send_message(text, bot_token=bot_token, chat_id=chat_id)
    _record(project_id=project_id, event_type="schema_drift", message=text,
            ok=ok, error=error, chat_id=chat_id, table=table)
    if ok:
        update_throttle(project_id, table, event_key)


def notify_changepoint(
    project_id: str,
    table: str,
    metric: str,
    value_before: float,
    value_after: float,
    ts: str,
    *,
    bot_token: str | None,
    chat_id: str | None,
    throttle_minutes: int | None = None,
) -> None:
    """Send change-point alert."""
    event_key = f"changepoint_{metric}"
    if is_throttled(project_id, table, event_key, throttle_minutes=throttle_minutes):
        return

    if metric == "null_rate":
        change_str = f"{value_before:.1%} → {value_after:.1%}"
    else:
        change_str = f"{int(value_before):,} → {int(value_after):,}"

    text = (
        f"\U0001f4c8 [{table}] Change-point: {metric} {change_str} ({ts[:10]})"
    )
    ok, error = send_message(text, bot_token=bot_token, chat_id=chat_id)
    _record(project_id=project_id, event_type="changepoint", message=text,
            ok=ok, error=error, chat_id=chat_id,
            table=table, metric=metric)
    if ok:
        update_throttle(project_id, table, event_key)
