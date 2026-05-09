"""Telegram Bot notifications for anomalies, schema drift, and change-points (#38).

Public entry points called from collectors/scheduler.py:
  notify_anomaly(table, ts)
  notify_schema_drift(table, events)
  notify_changepoint(table, metric, value_before, value_after, ts)

All functions are silent on errors — notification failures never propagate
to the caller.
"""

import asyncio
import logging

from telegram import Bot
from telegram.error import TelegramError

from app.config import settings
from app.llm import explain_anomaly
from app.metrics_storage import is_throttled, update_throttle

logger = logging.getLogger(__name__)


def send_message(text: str) -> bool:
    """Send a plain-text message via Bot API. Returns True on success."""
    token = settings.TELEGRAM_BOT_TOKEN
    chat_id = settings.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        logger.debug("Telegram not configured, skipping")
        return False

    async def _send() -> None:
        async with Bot(token) as bot:
            await bot.send_message(chat_id=chat_id, text=text)

    try:
        asyncio.run(_send())
        return True
    except TelegramError as exc:
        logger.warning("Telegram send failed: %s", exc)
        return False
    except Exception as exc:
        logger.warning("Telegram send error: %s", exc)
        return False


def notify_anomaly(table: str, ts: str, score: float) -> None:
    """Send anomaly alert. Throttled per (table, event_key)."""
    event_key = "anomaly"
    if is_throttled(table, event_key):
        return

    result = explain_anomaly(table, "row_count", ts)
    explanation = result.get("explanation", "")

    text = (
        f"\U0001f6a8 [{table}] Аномалия (score: {score:.4f})\n"
        f"Объяснение: {explanation}"
    )
    if send_message(text):
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
    if send_message(text):
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
    if send_message(text):
        update_throttle(table, event_key)
