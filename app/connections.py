"""DB connections blueprint for #51 (Sprint 3 multi-tenant epic).

A connection is the unit a project actually monitors — DSN + schema +
collect interval. DSNs are encrypted at rest via Fernet (``app/crypto.py``)
so a leak of the metrics DB file alone is not enough to recover credentials.

Routes (all nested under a project the current user owns):
- ``GET  /projects/<slug>/connections``                       — list
- ``GET  /projects/<slug>/connections/new``                   — add form
- ``POST /projects/<slug>/connections/new``                   — create
- ``POST /projects/<slug>/connections/<conn_id>/toggle``      — flip is_active
- ``POST /projects/<slug>/connections/<conn_id>/delete``      — hard delete

Ownership chain: every route resolves the slug to a project via
``projects._require_owned_project`` (404 on stranger's slug), then
``connections.get_connection(project_id, conn_id)`` (404 on stranger's
conn_id even when the slug check passed).

UI shows ``mask_dsn(decrypted)`` — plaintext password never reaches the
template. Logs never see the plaintext either (DSNFilter from #56
scrubs as belt-and-braces).
"""

from __future__ import annotations

import uuid

from flask import Blueprint, abort, flash, redirect, render_template, url_for
from flask_login import login_required
from flask_wtf import FlaskForm
from wtforms import BooleanField, IntegerField, StringField, SubmitField
from wtforms.validators import DataRequired, Length, NumberRange

from app import crypto, metrics_storage
from app.projects import _require_owned_project
from app.security import mask_dsn

bp = Blueprint("connections", __name__, url_prefix="/projects/<slug>/connections")


class ConnectionForm(FlaskForm):
    name = StringField(
        "Имя",
        validators=[DataRequired(), Length(min=1, max=80)],
        render_kw={"autocomplete": "off", "autofocus": True},
    )
    # DSN as password input — browsers don't auto-fill and don't surface
    # the value in dev-tools "input.value" tooltips. The actual privacy
    # comes from Fernet at rest, but this stops casual shoulder-surfing.
    dsn = StringField(
        "DSN",
        validators=[DataRequired(), Length(min=10, max=2000)],
        render_kw={
            "type": "password",
            "autocomplete": "off",
            "placeholder": "postgresql://user:password@host:5432/dbname",
        },
    )
    schema_name = StringField(
        "Схема",
        validators=[DataRequired(), Length(min=1, max=64)],
        default="public",
    )
    interval_minutes = IntegerField(
        "Интервал сбора (минуты)",
        validators=[
            DataRequired(),
            NumberRange(min=5, max=1440,
                        message="От 5 минут (минимум) до 1440 (раз в сутки)."),
        ],
        default=15,
    )
    is_active = BooleanField("Активен", default=True)
    submit = SubmitField("Сохранить")


# --- Routes ----------------------------------------------------------------


def _require_owned_connection(slug: str, conn_id: str) -> tuple[dict, dict]:
    """Two-step ownership check: project belongs to user, AND connection
    belongs to project. Returns (project, connection)."""
    project = _require_owned_project(slug)
    conn = metrics_storage.get_connection(project["id"], conn_id)
    if conn is None:
        abort(404)
    return project, conn


@bp.route("")
@bp.route("/")
@login_required
def list_connections(slug: str):
    project = _require_owned_project(slug)
    raw = metrics_storage.list_connections_for_project(project["id"])
    # Project the list for the template — decrypt + mask for display only.
    # The full ciphertext never goes anywhere near the rendered page.
    items = []
    for c in raw:
        try:
            dsn_masked = mask_dsn(crypto.decrypt_dsn(c["dsn_encrypted"]))
        except crypto.InvalidToken:
            dsn_masked = "<ошибка дешифровки>"
        items.append({**c, "dsn_masked": dsn_masked})
    return render_template(
        "connections/list.html", project=project, connections=items,
    )


@bp.route("/new", methods=["GET", "POST"])
@login_required
def new_connection(slug: str):
    project = _require_owned_project(slug)
    form = ConnectionForm()
    if form.validate_on_submit():
        metrics_storage.create_connection(
            connection_id=uuid.uuid4().hex,
            project_id=project["id"],
            name=form.name.data.strip(),
            dsn_encrypted=crypto.encrypt_dsn(form.dsn.data),
            schema_name=form.schema_name.data.strip(),
            interval_minutes=form.interval_minutes.data,
            is_active=form.is_active.data,
        )
        flash("Подключение добавлено.", "success")
        return redirect(url_for(
            "connections.list_connections", slug=slug,
        ))
    return render_template(
        "connections/new.html", project=project, form=form,
    )


@bp.route("/<conn_id>/delete", methods=["POST"])
@login_required
def delete(slug: str, conn_id: str):
    project, conn = _require_owned_connection(slug, conn_id)
    metrics_storage.delete_connection(project["id"], conn["id"])
    flash(f"Подключение «{conn['name']}» удалено.", "info")
    return redirect(url_for("connections.list_connections", slug=slug))


@bp.route("/<conn_id>/toggle", methods=["POST"])
@login_required
def toggle(slug: str, conn_id: str):
    project, conn = _require_owned_connection(slug, conn_id)
    metrics_storage.set_connection_active(
        project["id"], conn["id"], is_active=not conn["is_active"],
    )
    flash(
        f"Подключение «{conn['name']}» {'выключено' if conn['is_active'] else 'включено'}.",
        "info",
    )
    return redirect(url_for("connections.list_connections", slug=slug))


# --- Helper for other modules ----------------------------------------------


def list_connections_with_dsn(project_id: str) -> list[dict]:
    """Same as ``metrics_storage.list_connections_for_project`` but with
    decrypted DSN injected (key ``dsn``) and masked DSN (key ``dsn_masked``).

    Use this from the per-project APScheduler (#54) when it lands. Caller
    must still respect ownership — this helper assumes ``project_id`` was
    validated against ``current_user`` upstream.
    """
    items: list[dict] = []
    for c in metrics_storage.list_connections_for_project(project_id):
        try:
            plain = crypto.decrypt_dsn(c["dsn_encrypted"])
        except crypto.InvalidToken:
            continue
        items.append({**c, "dsn": plain, "dsn_masked": mask_dsn(plain)})
    return items
