"""Per-project settings blueprint (#143).

Currently houses one section — Telegram notifications — but the URL prefix
``/projects/<slug>/settings/`` is laid out so future panels (general,
danger zone, members) drop in without route migration.

The blueprint is independent from ``app.projects`` to avoid bloating that
module; ownership of the slug is still enforced via
``projects._require_owned_project``.
"""

from __future__ import annotations

import logging
import re

from flask import (
    Blueprint,
    flash,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import login_required
from flask_wtf import FlaskForm
from wtforms import IntegerField, StringField, SubmitField
from wtforms.validators import (
    DataRequired,
    Length,
    NumberRange,
    Optional,
    Regexp,
)

from app import crypto, metrics_storage
from app.projects import _require_owned_project

logger = logging.getLogger(__name__)

bp = Blueprint("settings", __name__, url_prefix="/projects/<slug>/settings")


# Telegram bot token format from BotFather:
#     <bot_id>:<hash>
# bot_id is 8–12 digits; hash is exactly 35 chars from [A-Za-z0-9_-].
_TELEGRAM_TOKEN_RE = re.compile(r"^\d{8,12}:[A-Za-z0-9_\-]{35}$")

# chat_id can be: positive integer (personal chat), negative integer
# (group), or negative integer starting with -100 (supergroup/channel).
# Telegram has hit ~14-digit IDs at scale; allow a generous range.
_CHAT_ID_RE = re.compile(r"^-?\d{1,16}$")


class NotificationsForm(FlaskForm):
    # Token is Optional() so the user can keep the existing one when only
    # updating chat_id or throttle. Empty submission means "no change".
    telegram_bot_token = StringField(
        "Bot Token",
        validators=[
            Optional(),
            Length(min=44, max=60),  # bot_id(8-12) + ':' + hash(35) = 44-48
            Regexp(_TELEGRAM_TOKEN_RE,
                   message="Формат токена: 1234567890:AAFx... (8–12 цифр, ':', "
                           "35 символов)"),
        ],
        render_kw={
            "type": "password",
            "autocomplete": "off",
            "placeholder": "1234567890:AAFxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        },
    )
    telegram_chat_id = StringField(
        "Chat ID",
        validators=[
            DataRequired(),
            Regexp(_CHAT_ID_RE,
                   message="Chat ID — целое число (личный чат) или начинается "
                           "с '-100' (супергруппа/канал)."),
        ],
        render_kw={"autocomplete": "off", "placeholder": "123456789"},
    )
    throttle_minutes = IntegerField(
        "Минут между алертами",
        validators=[
            DataRequired(),
            NumberRange(min=1, max=1440, message="От 1 минуты до суток."),
        ],
        default=30,
    )
    submit = SubmitField("Сохранить")


def _mask_token(plaintext: str) -> str:
    """Show first 7 chars of bot_id + ': •••' so the UI can render a hint
    of what's saved without re-leaking the secret hash."""
    if not plaintext or ":" not in plaintext:
        return "•••"
    bot_id, _ = plaintext.split(":", 1)
    return f"{bot_id}:•••"


@bp.route("/notifications", methods=["GET", "POST"])
@login_required
def notifications(slug: str):
    project = _require_owned_project(slug)
    existing = metrics_storage.get_project_notifications(project["id"])

    form = NotificationsForm()

    # Pre-fill non-secret fields on GET; never echo the token.
    if request.method == "GET" and existing:
        form.telegram_chat_id.data = existing.get("telegram_chat_id") or ""
        form.throttle_minutes.data = existing.get("throttle_minutes") or 30

    if form.validate_on_submit():
        # If user left token blank AND we already have one stored — keep
        # the old ciphertext. Otherwise re-encrypt the freshly typed one.
        new_token = form.telegram_bot_token.data
        if new_token:
            token_encrypted = crypto.encrypt_token(new_token)
        elif existing and existing.get("telegram_bot_token"):
            token_encrypted = existing["telegram_bot_token"]
        else:
            flash("Bot Token обязателен при первой настройке.", "error")
            return _render_notifications(project, form, existing)

        metrics_storage.save_project_notifications(
            project["id"],
            telegram_bot_token=token_encrypted,
            telegram_chat_id=form.telegram_chat_id.data.strip(),
            throttle_minutes=form.throttle_minutes.data,
        )
        flash("Настройки Telegram сохранены.", "success")
        return redirect(url_for("settings.notifications", slug=slug))

    return _render_notifications(project, form, existing)


def _render_notifications(project: dict, form: NotificationsForm,
                          existing: dict | None):
    token_hint = None
    if existing and existing.get("telegram_bot_token"):
        try:
            plaintext = crypto.decrypt_token(existing["telegram_bot_token"])
            token_hint = _mask_token(plaintext)
        except crypto.InvalidToken:
            token_hint = "•••  (ошибка дешифровки)"
    return render_template(
        "projects/settings_notifications.html",
        project=project,
        form=form,
        token_hint=token_hint,
        configured=bool(
            existing
            and existing.get("telegram_bot_token")
            and existing.get("telegram_chat_id")
        ),
    )


@bp.route("/notifications/test", methods=["POST"])
@login_required
def test_notification(slug: str):
    """Send a test message using the values currently typed in the form.

    Does NOT save them — that's an explicit Save action. Lets the user
    verify the token/chat work before committing them to the DB.
    """
    from app.notifications.telegram import send_message

    project = _require_owned_project(slug)
    raw_token = (request.form.get("telegram_bot_token") or "").strip()
    chat_id = (request.form.get("telegram_chat_id") or "").strip()

    # Empty token in the form means "use the saved one" — same as Save.
    if not raw_token:
        existing = metrics_storage.get_project_notifications(project["id"])
        if existing and existing.get("telegram_bot_token"):
            try:
                raw_token = crypto.decrypt_token(existing["telegram_bot_token"])
            except crypto.InvalidToken:
                flash("Не удалось расшифровать сохранённый токен — введите заново.", "error")
                return redirect(url_for("settings.notifications", slug=slug))
        if not chat_id and existing:
            chat_id = existing.get("telegram_chat_id") or ""

    if not raw_token or not chat_id:
        flash("Заполните Bot Token и Chat ID перед тестом.", "error")
        return redirect(url_for("settings.notifications", slug=slug))

    ok, error = send_message(
        f"✅ Тестовое сообщение из DB Monitor для проекта «{project['name']}».",
        bot_token=raw_token, chat_id=chat_id,
    )
    if ok:
        flash("Тестовое сообщение отправлено.", "success")
    else:
        flash(f"Не удалось отправить ({error or 'unknown'}).", "error")
    return redirect(url_for("settings.notifications", slug=slug))


@bp.route("/notifications/disable", methods=["POST"])
@login_required
def disable_notifications(slug: str):
    project = _require_owned_project(slug)
    metrics_storage.delete_project_notifications(project["id"])
    flash("Telegram-уведомления отключены.", "info")
    return redirect(url_for("settings.notifications", slug=slug))
