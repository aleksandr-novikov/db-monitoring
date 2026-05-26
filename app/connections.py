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

import logging
import time
import uuid

from flask import Blueprint, abort, flash, jsonify, redirect, render_template, url_for
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from sqlalchemy import create_engine, make_url, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool
from wtforms import BooleanField, IntegerField, StringField, SubmitField
from wtforms.validators import DataRequired, Length, NumberRange

from app import crypto, metrics_storage
from app.auth import limiter
from app.projects import _require_owned_project
from app.security import mask_dsn

logger = logging.getLogger(__name__)

bp = Blueprint("connections", __name__, url_prefix="/projects/<slug>/connections")

# Connection-test budget — caps the wall-clock for the whole probe.
_TEST_CONNECT_TIMEOUT_S = 5


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
    # Onboarding mode (#55): zero existing connections → render the wizard
    # template (DSN-format hints) and auto-probe after save. Once a project
    # has ≥1 connection, the route reverts to the plain power-user form.
    is_first = not metrics_storage.list_connections_for_project(project["id"])
    form = ConnectionForm()
    if form.validate_on_submit():
        raw_dsn = form.dsn.data
        conn_row = metrics_storage.create_connection(
            connection_id=uuid.uuid4().hex,
            project_id=project["id"],
            name=form.name.data.strip(),
            dsn_encrypted=crypto.encrypt_dsn(raw_dsn),
            schema_name=form.schema_name.data.strip(),
            interval_minutes=form.interval_minutes.data,
            is_active=form.is_active.data,
        )
        # #54: register the APScheduler job immediately if the connection
        # is active. The scheduler is process-wide (started at app boot);
        # add_job_for_connection no-ops if the scheduler isn't running
        # (e.g. under TESTING).
        if conn_row["is_active"]:
            from collectors.per_project import add_job_for_connection
            from collectors.scheduler import get_scheduler

            add_job_for_connection(get_scheduler(), project["id"], conn_row)

        # Onboarding auto-test (#55): on the FIRST connection, probe the
        # DSN immediately so the user gets instant feedback instead of
        # waiting for the next collector tick. OK → land on /dashboard
        # with a positive flash; failure → /connections with the code so
        # they can edit/delete and retry.
        if is_first:
            result = probe_connection(raw_dsn)
            if result["status"] == "ok":
                flash(
                    "Подключение проверено. Сбор метрик запустится через "
                    f"{conn_row['interval_minutes']} мин.",
                    "success",
                )
                return redirect(url_for("dashboard.overview"))
            flash(
                "Подключение сохранено, но автоматический тест не прошёл "
                f"({result.get('code', 'error')}). Откройте список подключений и нажмите «Тест».",
                "error",
            )
            return redirect(url_for("connections.list_connections", slug=slug))

        flash("Подключение добавлено.", "success")
        return redirect(url_for(
            "connections.list_connections", slug=slug,
        ))
    template = (
        "onboarding/add_connection.html" if is_first else "connections/new.html"
    )
    return render_template(template, project=project, form=form)


@bp.route("/<conn_id>/delete", methods=["POST"])
@login_required
def delete(slug: str, conn_id: str):
    project, conn = _require_owned_connection(slug, conn_id)
    metrics_storage.delete_connection(project["id"], conn["id"])
    # #54: drop the scheduled job AFTER the row is gone — the job body
    # re-checks the DB and would no-op if it fires between delete and
    # remove_job_for_connection.
    from collectors.per_project import remove_job_for_connection
    from collectors.scheduler import get_scheduler

    remove_job_for_connection(get_scheduler(), project["id"], conn["id"])
    flash(f"Подключение «{conn['name']}» удалено.", "info")
    return redirect(url_for("connections.list_connections", slug=slug))


def _user_key() -> str:
    """Per-user rate-limit key — matches #56's `/test`: 30/min per user.

    Falls back to IP for anonymous (defence in depth — the route is
    @login_required so anonymous can't reach it, but a misconfiguration
    shouldn't degrade to "no rate limit").
    """
    from flask_limiter.util import get_remote_address

    if current_user.is_authenticated:
        return f"user:{current_user.id}"
    return f"ip:{get_remote_address()}"


@bp.route("/<conn_id>/test", methods=["POST"])
@limiter.limit("30 per minute", key_func=_user_key)
@login_required
def test_connection(slug: str, conn_id: str):
    """Live-probe the stored DSN. Per-user-throttled (#56)."""
    _project, conn = _require_owned_connection(slug, conn_id)
    try:
        plain = crypto.decrypt_dsn(conn["dsn_encrypted"])
    except crypto.InvalidToken:
        return jsonify({
            "status": "error", "code": "invalid_ciphertext",
            "message": "Сохранённый DSN не расшифровывается. Пересохрани подключение.",
        }), 422
    result = probe_connection(plain)
    status_code = 200 if result["status"] == "ok" else 422
    return jsonify(result), status_code


@bp.route("/<conn_id>/toggle", methods=["POST"])
@login_required
def toggle(slug: str, conn_id: str):
    project, conn = _require_owned_connection(slug, conn_id)
    new_active = not conn["is_active"]
    metrics_storage.set_connection_active(
        project["id"], conn["id"], is_active=new_active,
    )
    # #54: keep the scheduler in sync with the row's is_active flag.
    from collectors.per_project import (
        add_job_for_connection,
        remove_job_for_connection,
    )
    from collectors.scheduler import get_scheduler

    sched = get_scheduler()
    if new_active:
        add_job_for_connection(sched, project["id"], {**conn, "is_active": True})
    else:
        remove_job_for_connection(sched, project["id"], conn["id"])
    flash(
        f"Подключение «{conn['name']}» {'выключено' if conn['is_active'] else 'включено'}.",
        "info",
    )
    return redirect(url_for("connections.list_connections", slug=slug))


# --- Connection probe (#52) ------------------------------------------------


# Error-code mapping: rough match on exception text. Order matters — auth
# is more specific than the generic "could not connect" patterns and must
# be checked first. Each phrase is a substring of the lower-cased message.
_AUTH_HINTS = (
    "password authentication failed",
    "authentication failed",
    "access denied for user",  # MySQL phrasing (future-proofing)
)
_TIMEOUT_HINTS = (
    "timeout expired",
    "connection timed out",
    "connect_timeout expired",
)
_NETWORK_HINTS = (
    "could not translate host name",
    "could not connect to server",
    "connection refused",
    "no route to host",
    "network is unreachable",
    "name or service not known",
    "temporary failure in name resolution",
    "unable to connect",
)


def _classify_error(exc: BaseException) -> tuple[str, str]:
    """Map an exception to (code, user-safe message).

    User-safe message must NOT leak DSN content — the DSNFilter (#56)
    will scrub on the way to logs, but the JSON response goes straight
    to the browser without the filter. Phrasing is deliberately generic.
    """
    msg = str(exc).lower()
    if any(h in msg for h in _AUTH_HINTS):
        return "auth_failed", "Неверный логин или пароль."
    if any(h in msg for h in _TIMEOUT_HINTS):
        return "timeout", f"Подключение не удалось за {_TEST_CONNECT_TIMEOUT_S} c."
    if any(h in msg for h in _NETWORK_HINTS):
        return "network", "Хост недоступен или DNS не разрешается."
    return "error", "Ошибка подключения (см. логи сервера)."


def _probe_iceberg(dsn: str) -> dict:
    """Lightweight probe for iceberg+rest:// and iceberg+glue:// DSNs.

    Calls list_namespaces() on the catalog — no data scan, just a metadata
    round-trip.  Falls back gracefully if pyiceberg is not installed.
    """
    started = time.monotonic()
    try:
        from app.db import make_adapter_for_url
        adapter = make_adapter_for_url(dsn)
        namespaces = adapter.list_namespaces()
        latency_ms = int((time.monotonic() - started) * 1000)
        return {
            "status": "ok",
            "database": "iceberg",
            "version": f"{len(namespaces)} namespace(s)",
            "latency_ms": latency_ms,
        }
    except ImportError:
        return {
            "status": "error", "code": "unsupported_dialect",
            "message": "pyiceberg не установлен на сервере.",
        }
    except Exception as exc:
        logger.warning("iceberg probe failed: %s", exc, exc_info=True)
        latency_ms = int((time.monotonic() - started) * 1000)
        return {
            "status": "error", "code": "error",
            "message": "Iceberg catalog недоступен или DSN неверен.",
            "latency_ms": latency_ms,
        }


def probe_connection(dsn: str) -> dict:
    """Try connecting and reading a couple of harmless metadata bits.

    Postgres-only at the moment — other dialects return ``unsupported_dialect``
    until a per-dialect probe lands (the adapters in #42 know how to
    introspect tables but not how to phrase a self-test in a way that
    works across MySQL / ClickHouse). The JSON response is identical
    shape across success and failure so the UI never has to branch on
    keys, only on ``status``.
    """
    try:
        backend = make_url(dsn).get_backend_name()
    except Exception:
        return {
            "status": "error", "code": "invalid_dsn",
            "message": "DSN не парсится как URL.",
        }

    if backend.startswith("iceberg"):
        return _probe_iceberg(dsn)

    if backend != "postgresql":
        return {
            "status": "error", "code": "unsupported_dialect",
            "message": f"Тест для диалекта {backend!r} ещё не реализован.",
        }

    # NullPool: do NOT keep the connection alive after the probe — we don't
    # want a one-off test to occupy a pool slot for the rest of the process.
    # connect_args.connect_timeout: psycopg2 / libpq honours this for the
    # initial TCP+startup phase, which is exactly what we want to bound.
    engine = create_engine(
        dsn,
        poolclass=NullPool,
        connect_args={"connect_timeout": _TEST_CONNECT_TIMEOUT_S},
    )
    started = time.monotonic()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            row = conn.execute(
                text("SELECT current_database(), version()")
            ).fetchone()
        latency_ms = int((time.monotonic() - started) * 1000)
        return {
            "status": "ok",
            "database": row[0],
            "version": row[1].split(" on ", 1)[0],  # trim "on x86_64-..."
            "latency_ms": latency_ms,
        }
    except SQLAlchemyError as exc:
        # Full traceback (with masked DSN — DSNFilter scrubs the password
        # before it reaches any handler) goes to server logs; user sees
        # only the classified code.
        logger.warning("connection probe failed: %s", exc, exc_info=True)
        code, user_msg = _classify_error(exc)
        return {
            "status": "error", "code": code, "message": user_msg,
            "latency_ms": int((time.monotonic() - started) * 1000),
        }
    finally:
        engine.dispose()


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
