"""Email abstraction for transactional messages (#133).

Two backends:

- ``smtp`` — real delivery via stdlib ``smtplib``. Activated when
  ``settings.SMTP_HOST`` is non-empty. Reads SMTP_HOST/PORT/USER/PASSWORD/
  FROM/USE_TLS from env. Failures are logged + swallowed so a misconfigured
  SMTP server doesn't 500 the /forgot-password endpoint (which would also
  leak that the email exists).

- ``memory`` — captures sent messages in a module-level list. Activated
  when SMTP_HOST is empty. Used for tests and local dev without a relay.
  Inspect via ``app.email.outbox``.

Why not a Flask extension (Flask-Mail / Flask-Mailman)? Reset emails are
the only transactional surface for now; an extra dependency for one
templateless plain-text email isn't worth it. Easy to swap later.
"""

from __future__ import annotations

import logging
import smtplib
import threading
from dataclasses import dataclass
from email.message import EmailMessage

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class SentMessage:
    """In-memory record of an email handed to the ``memory`` backend.

    Tests assert against ``app.email.outbox`` to verify routing logic
    without spinning up an SMTP server.
    """
    to: str
    subject: str
    body: str


outbox: list[SentMessage] = []
_outbox_lock = threading.Lock()


def _smtp_enabled() -> bool:
    return settings.smtp_configured


def _send_smtp(to: str, subject: str, body: str) -> bool:
    """Send via stdlib smtplib. Returns True on success, False on failure.

    Failures are logged but never re-raised — the caller (/forgot-password)
    must keep the same HTTP response for known/unknown emails so attackers
    can't enumerate registered emails via timing or status-code differences.
    """
    msg = EmailMessage()
    msg["From"] = settings.SMTP_FROM
    msg["To"] = to
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        if settings.SMTP_USE_TLS:
            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=10) as smtp:
                smtp.starttls()
                if settings.SMTP_USER:
                    smtp.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(settings.SMTP_HOST, settings.SMTP_PORT, timeout=10) as smtp:
                if settings.SMTP_USER:
                    smtp.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                smtp.send_message(msg)
        return True
    except (OSError, smtplib.SMTPException) as exc:
        logger.warning("SMTP send failed: %s", exc)
        return False


def send_email(to: str, subject: str, body: str) -> bool:
    """Dispatch one email through the active backend.

    Plain-text only — no HTML. Reset emails read fine in any client and
    sidestep the templating / image / tracker-pixel rabbit-hole.
    """
    if _smtp_enabled():
        return _send_smtp(to, subject, body)
    with _outbox_lock:
        outbox.append(SentMessage(to=to, subject=subject, body=body))
    logger.warning(
        "SMTP not configured; using memory email backend; email NOT delivered"
    )
    return True


def clear_outbox() -> None:
    """Reset the in-memory outbox. Tests should call this in their setup
    so prior cases don't leak SentMessage instances."""
    with _outbox_lock:
        outbox.clear()


def send_password_reset_email(to: str, reset_url: str) -> bool:
    """Compose and dispatch the password-reset email.

    Body is intentionally short and in Russian to match the UI surface.
    Link contains the raw token — anyone with the link can reset the
    password until the token expires (1h) or is consumed.
    """
    body = (
        "Здравствуйте!\n\n"
        "Кто-то (надеемся, что вы) запросил сброс пароля в DB Monitor.\n"
        "Если это были вы, перейдите по ссылке ниже в течение 1 часа:\n\n"
        f"{reset_url}\n\n"
        "Если это были не вы — просто проигнорируйте это письмо. "
        "Пароль останется прежним.\n\n"
        "— DB Monitor"
    )
    return send_email(to, "Сброс пароля — DB Monitor", body)


__all__ = [
    "SentMessage",
    "clear_outbox",
    "outbox",
    "send_email",
    "send_password_reset_email",
]
