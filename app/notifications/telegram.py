"""Telegram Bot notifications for anomalies, schema drift, and change-points (#38).

Public entry points called from collectors/scheduler.py:
  notify_anomaly(table, ts, score, metric="row_count")
  notify_schema_drift(table, events)
  notify_changepoint(table, metric, value_before, value_after, ts)

All functions are silent on errors — notification failures never propagate
to the caller. Each delivery attempt (success or failure) is persisted via
metrics_storage.save_notification so the UI can show a full audit trail (#76).
"""

import asyncio
import logging

from telegram import Bot
from telegram.error import TelegramError

from app.config import settings
from app.llm import explain_anomaly
from app.metrics_storage import is_throttled, save_notification, update_throttle

logger = logging.getLogger(__name__)


def send_message(text: str) -> tuple[bool, str | None]:
    """Send a plain-text message via Bot API.

    Returns (ok, error). When ok is False, error is a short reason string
    suitable for storing alongside the notification record. Returns
    (False, "not_configured") if token/chat are not set — the call is still
    audited as a failed attempt by the caller.
    """
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        logger.debug("Telegram not configured, skipping")
        return False, "not_configured"

    async def _send() -> None:
        async with Bot(token) as bot:
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
    event_type: str,
    message: str,
    ok: bool,
    error: str | None,
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
            chat_id=settings.TELEGRAM_CHAT_ID or None,
        )
    except Exception as exc:  # pragma: no cover - storage failure shouldn't break alerts
        logger.warning("Failed to persist notification audit: %s", exc)


_RULE_BASED_CONFIDENCE: float = 0.3


def _fmt_ts(ts: str) -> str:
    """Format ISO timestamp to '2026-05-11 19:13 UTC'."""
    return ts.replace("T", " ")[:16] + " UTC"


def notify_anomaly(table: str, ts: str, score: float, metric: str = "row_count") -> None:
    """Send anomaly alert. Throttled per (table, event_key)."""
    event_key = "anomaly"
    if is_throttled(table, event_key):
        return

    result = explain_anomaly(table, metric, ts)
    is_llm = result.get("confidence", 0) > _RULE_BASED_CONFIDENCE
    body = result.get("explanation", "Требуется ручная проверка данных.") if is_llm else "Требуется ручная проверка данных."

    text = (
        f"\U0001f6a8 [{table}] Аномалия (score: {score:.4f})\n"
        f"Обнаружена аномалия в таблице {table} по метрике {metric}"
        f" в момент {_fmt_ts(ts)}. {body}"
    )
    ok, error = send_message(text)
    _record(event_type="anomaly", message=text, ok=ok, error=error,
            table=table, metric=metric)
    if ok:
        update_throttle(table, event_key)


def notify_schema_drift(table: str, events: list[dict]) -> None:
    """Send schema-drift alert for a batch of events on one table."""
    if not events:
        return

    event_key = "schema_drift"
    if is_throttled(table, event_key):
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

    text = f"\U0001f4cb [{table}] Дрейф схемы:\n" + "\n".join(lines)
    ok, error = send_message(text)
    _record(event_type="schema_drift", message=text, ok=ok, error=error, table=table)
    if ok:
        update_throttle(table, event_key)


def notify_changepoint(
    table: str,
    metric: str,
    value_before: float,
    value_after: float,
    ts: str,
) -> None:
    """Send change-point alert."""
    event_key = f"changepoint_{metric}"
    if is_throttled(table, event_key):
        return

    if metric == "null_rate":
        change_str = f"{value_before:.1%} → {value_after:.1%}"
    else:
        change_str = f"{int(value_before):,} → {int(value_after):,}"

    text = (
        f"\U0001f4c8 [{table}] Change-point: {metric} {change_str} ({ts[:10]})"
    )
    ok, error = send_message(text)
    _record(event_type="changepoint", message=text, ok=ok, error=error,
            table=table, metric=metric)
    if ok:
        update_throttle(table, event_key)
