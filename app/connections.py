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

from flask import (
    Blueprint,
    abort,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from sqlalchemy import create_engine, make_url, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import NullPool
from wtforms import (
    BooleanField,
    IntegerField,
    PasswordField,
    SelectField,
    StringField,
    SubmitField,
    TextAreaField,
)
from wtforms.validators import (
    DataRequired,
    Length,
    NumberRange,
    Optional,
)

from app import crypto, metrics_storage
from app.auth import limiter
from app.projects import _require_owned_project, _require_role
from app.security import mask_dsn

logger = logging.getLogger(__name__)

bp = Blueprint("connections", __name__, url_prefix="/projects/<slug>/connections")

# Connection-test budget — caps the wall-clock for the whole probe.
_TEST_CONNECT_TIMEOUT_S = 5

# #230: сколько таблиц возвращать UI как preview.
_PROBE_TABLES_PREVIEW = 10
# #229: сколько таблиц проверять на INSERT-права. Полный скан всей
# схемы бессмысленно дорог (десятки тыщ таблиц у крупных пользователей);
# первые 20 — практический компромисс для smoke-check.
_PROBE_PRIV_CHECK = 20


def _textarea_to_jsonlist(raw: str | None) -> str | None:
    """Convert a textarea-blob (one name per line, or comma-separated) into
    the JSON-array TEXT shape used by the collector's ``_parse_table_list``.
    Empty/whitespace-only input returns ``None`` so the column becomes NULL
    (== "no constraint")."""
    if not raw:
        return None
    parts = []
    for chunk in raw.replace(",", "\n").splitlines():
        s = chunk.strip()
        if s:
            parts.append(s)
    if not parts:
        return None
    import json
    return json.dumps(parts)


def _jsonlist_to_textarea(raw: str | None) -> str:
    """Inverse of ``_textarea_to_jsonlist`` for form pre-fill."""
    if not raw:
        return ""
    import json
    try:
        items = json.loads(raw)
    except (ValueError, TypeError):
        return ""
    if not isinstance(items, list):
        return ""
    return "\n".join(str(x) for x in items if isinstance(x, str))


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
    # #234: Iceberg production params. Поля опциональны — для не-Iceberg
    # DSN остаются пустыми (JS прячет блок при вводе non-iceberg+ DSN).
    # Token идёт через PasswordField — браузер не подсказывает значение
    # из истории и не показывает plaintext в tooltip dev-tools.
    iceberg_namespace = StringField(
        "Iceberg namespace",
        validators=[Optional(), Length(max=256)],
        render_kw={"placeholder": "lakehouse"},
    )
    iceberg_warehouse = StringField(
        "Iceberg warehouse",
        validators=[Optional(), Length(max=256)],
        render_kw={"placeholder": "s3://bucket/warehouse"},
    )
    iceberg_auth_token = PasswordField(
        "Iceberg auth token",
        validators=[Optional(), Length(max=2000)],
        render_kw={"autocomplete": "off", "placeholder": "Bearer token"},
    )
    submit = SubmitField("Сохранить")


class ConnectionSafetyForm(FlaskForm):
    """#256: один редактор load-safety, collection_mode и Iceberg-настроек
    существующего подключения. Имя/DSN/schema тут НЕ редактируются —
    их смена ломает сбор метрик и должна идти через delete + new.

    Empty string у numeric/optional полей = NULL в БД (== «без ограничения»
    / «default»). Это интерпретируется в роуте, не в форме, чтобы валидация
    оставалась узкой.
    """
    # #232 load safety — для всех диалектов.
    table_allowlist = TextAreaField(
        "Allowlist таблиц",
        validators=[Optional(), Length(max=10_000)],
        render_kw={
            "rows": 4,
            "placeholder": "users\norders\n(пусто = все таблицы)",
        },
    )
    table_denylist = TextAreaField(
        "Denylist таблиц",
        validators=[Optional(), Length(max=10_000)],
        render_kw={"rows": 4, "placeholder": "audit_logs\nevents_raw"},
    )
    max_tables_per_tick = IntegerField(
        "Max таблиц за тик",
        validators=[Optional(), NumberRange(min=1, max=500)],
        default=50,
    )
    skip_tables_larger_than_gb = StringField(
        "Skip таблиц > N GB (Postgres)",
        validators=[Optional(), Length(max=16)],
        render_kw={"placeholder": "10 (пусто = без ограничения)"},
    )
    statement_timeout_ms = IntegerField(
        "statement_timeout (мс, Postgres)",
        validators=[Optional(), NumberRange(min=1000, max=600_000)],
        default=30_000,
    )
    # #233 collection mode.
    collection_mode = SelectField(
        "Режим сбора",
        choices=[
            ("full", "full — точные null_count + null_rate + distribution"),
            ("sample", "sample — TABLESAMPLE 1% (Postgres only)"),
            ("approx", "approx — pg_stats.null_frac (Postgres only)"),
        ],
        default="full",
    )
    # #235 Iceberg load safety.
    iceberg_namespace_allowlist = TextAreaField(
        "Iceberg namespace allowlist",
        validators=[Optional(), Length(max=10_000)],
        render_kw={
            "rows": 3,
            "placeholder": "prod\nstaging\n(пусто = только effective_namespace)",
        },
    )
    metadata_only_mode = BooleanField(
        "Iceberg: только schema, без metrics",
        default=False,
    )
    # #234 Iceberg production params (edit-сторона).
    iceberg_namespace = StringField(
        "Iceberg namespace", validators=[Optional(), Length(max=256)],
    )
    iceberg_warehouse = StringField(
        "Iceberg warehouse", validators=[Optional(), Length(max=256)],
    )
    iceberg_auth_token = PasswordField(
        "Iceberg auth token (пусто = оставить как есть)",
        validators=[Optional(), Length(max=2000)],
        render_kw={"autocomplete": "off"},
    )
    iceberg_auth_token_clear = BooleanField(
        "Очистить сохранённый токен", default=False,
    )

    submit = SubmitField("Сохранить настройки")


# --- Routes ----------------------------------------------------------------


def _require_owned_connection(slug: str, conn_id: str) -> tuple[dict, dict]:
    """Two-step ownership check: project belongs to user, AND connection
    belongs to project. Returns (project, connection)."""
    project = _require_owned_project(slug)
    conn = metrics_storage.get_connection(project["id"], conn_id)
    if conn is None:
        abort(404)
    return project, conn


def _persist_probe_result(project_id: str, connection_id: str, result: dict) -> None:
    """Store the latest probe outcome for an already saved connection."""
    status = "ok" if result.get("status") == "ok" else "error"
    error = None
    if status == "error":
        error = result.get("message") or result.get("code") or "probe failed"
    metrics_storage.update_connection_probe(
        project_id,
        connection_id,
        status=status,
        tables_found=result.get("tables_found"),
        error=error,
    )


@bp.route("")
@bp.route("/")
@login_required
def list_connections(slug: str):
    project = _require_owned_project(slug)
    project["role"] = metrics_storage.get_member_role(project["id"], current_user.id)
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
    project = _require_role(slug, "owner", "editor")
    # Onboarding mode (#55): zero existing connections → render the wizard
    # template (DSN-format hints) and auto-probe after save. Once a project
    # has ≥1 connection, the route reverts to the plain power-user form.
    is_first = not metrics_storage.list_connections_for_project(project["id"])
    form = ConnectionForm()
    if form.validate_on_submit():
        raw_dsn = form.dsn.data
        # #234: Iceberg fields are only meaningful for iceberg+ DSNs. JS
        # hides them otherwise, but the server has to ignore them too —
        # a hand-crafted POST shouldn't be able to attach an Iceberg
        # token to a Postgres connection.
        is_iceberg = raw_dsn.lower().startswith("iceberg+")
        iceberg_ns = (form.iceberg_namespace.data or "").strip() or None
        iceberg_wh = (form.iceberg_warehouse.data or "").strip() or None
        iceberg_token = (form.iceberg_auth_token.data or "").strip()
        token_ct = (
            crypto.encrypt_token(iceberg_token)
            if is_iceberg and iceberg_token
            else None
        )
        conn_row = metrics_storage.create_connection(
            connection_id=uuid.uuid4().hex,
            project_id=project["id"],
            name=form.name.data.strip(),
            dsn_encrypted=crypto.encrypt_dsn(raw_dsn),
            schema_name=form.schema_name.data.strip(),
            interval_minutes=form.interval_minutes.data,
            is_active=form.is_active.data,
            iceberg_namespace=iceberg_ns if is_iceberg else None,
            iceberg_warehouse=iceberg_wh if is_iceberg else None,
            iceberg_auth_token_encrypted=token_ct,
        )
        # #54: register the APScheduler job immediately if the connection
        # is active. The scheduler is process-wide (started at app boot);
        # add_job_for_connection no-ops if the scheduler isn't running
        # (e.g. under TESTING).
        if conn_row["is_active"]:
            from collectors.per_project import add_job_for_connection
            from collectors.scheduler import get_scheduler

            # First tick immediately so the user sees metrics on /dashboard
            # right after save instead of waiting up to interval_minutes.
            add_job_for_connection(
                get_scheduler(), project["id"], conn_row, run_immediately=True,
            )

        # Onboarding auto-test (#55): on the FIRST connection, probe the
        # DSN immediately so the user gets instant feedback instead of
        # waiting for the next collector tick. OK → land on /dashboard
        # with a positive flash; failure → /connections with the code so
        # they can edit/delete and retry.
        if is_first:
            result = probe_connection(
                raw_dsn,
                iceberg_namespace=iceberg_ns if is_iceberg else None,
                iceberg_warehouse=iceberg_wh if is_iceberg else None,
                iceberg_auth_token=iceberg_token if is_iceberg else None,
            )
            _persist_probe_result(project["id"], conn_row["id"], result)
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


@bp.route("/<conn_id>/edit", methods=["GET", "POST"])
@login_required
def edit_safety(slug: str, conn_id: str):
    """#256: один редактор для load-safety, collection_mode и Iceberg-настроек.

    GET — pre-fill из БД, POST — валидация + UPDATE.

    Iceberg-блок виден только для iceberg+ DSN. Postgres-only поля
    (skip_size_gb / statement_timeout / sample/approx) дополнительно
    дисейблятся в шаблоне для не-Postgres подключений; сервер
    одновременно отбрасывает их значения если диалект не подходит.
    """
    project, conn = _require_owned_connection(slug, conn_id)
    role = metrics_storage.get_member_role(project["id"], current_user.id)
    if role not in ("owner", "editor"):
        abort(403)

    # Decode DSN once to know the dialect. Failure to decrypt doesn't kill
    # the edit form — operator may need to clear the bad ciphertext via the
    # CLI; pre-fill is best-effort.
    try:
        plain_dsn = crypto.decrypt_dsn(conn["dsn_encrypted"])
    except crypto.InvalidToken:
        plain_dsn = ""
    is_iceberg = plain_dsn.lower().startswith("iceberg+")
    is_postgres = plain_dsn.lower().startswith(
        ("postgres://", "postgresql://", "postgresql+"),
    )

    form = ConnectionSafetyForm()

    if request.method == "GET":
        form.table_allowlist.data = _jsonlist_to_textarea(conn.get("table_allowlist"))
        form.table_denylist.data = _jsonlist_to_textarea(conn.get("table_denylist"))
        form.max_tables_per_tick.data = conn.get("max_tables_per_tick")
        sk = conn.get("skip_tables_larger_than_gb")
        form.skip_tables_larger_than_gb.data = "" if sk is None else f"{sk}"
        form.statement_timeout_ms.data = conn.get("statement_timeout_ms")
        form.collection_mode.data = conn.get("collection_mode") or "full"
        form.iceberg_namespace_allowlist.data = _jsonlist_to_textarea(
            conn.get("iceberg_namespace_allowlist"),
        )
        form.metadata_only_mode.data = bool(conn.get("metadata_only_mode"))
        form.iceberg_namespace.data = conn.get("iceberg_namespace") or ""
        form.iceberg_warehouse.data = conn.get("iceberg_warehouse") or ""

    if form.validate_on_submit():
        updates: dict[str, object] = {}
        updates["table_allowlist"] = _textarea_to_jsonlist(form.table_allowlist.data)
        updates["table_denylist"] = _textarea_to_jsonlist(form.table_denylist.data)
        updates["max_tables_per_tick"] = form.max_tables_per_tick.data
        # Optional float-or-blank. Validate manually so the form-level
        # validator stays simple (no custom float parser there).
        sk_raw = (form.skip_tables_larger_than_gb.data or "").strip()
        if not sk_raw:
            updates["skip_tables_larger_than_gb"] = None
        else:
            try:
                sk_val = float(sk_raw)
                if sk_val <= 0:
                    raise ValueError
                updates["skip_tables_larger_than_gb"] = sk_val
            except ValueError:
                flash("skip_tables_larger_than_gb: ожидается положительное число.",
                      "error")
                return render_template(
                    "connections/edit.html",
                    project=project, conn=conn, form=form,
                    is_iceberg=is_iceberg, is_postgres=is_postgres,
                )
        updates["statement_timeout_ms"] = form.statement_timeout_ms.data
        # Mode: don't let an operator silently set sample/approx on a non-
        # Postgres connection — the collector would downgrade with a warning,
        # but blocking at save-time is clearer.
        mode = form.collection_mode.data or "full"
        if mode in ("sample", "approx") and not is_postgres:
            flash(
                f"Режим {mode!r} доступен только для Postgres-подключений.",
                "error",
            )
            return render_template(
                "connections/edit.html",
                project=project, conn=conn, form=form,
                is_iceberg=is_iceberg, is_postgres=is_postgres,
            )
        updates["collection_mode"] = mode

        # Iceberg-side fields. For non-Iceberg DSNs we leave the existing
        # row values alone (skip the update keys entirely) — defence
        # against a hand-crafted POST attaching tokens to Postgres rows.
        if is_iceberg:
            updates["iceberg_namespace_allowlist"] = _textarea_to_jsonlist(
                form.iceberg_namespace_allowlist.data,
            )
            updates["metadata_only_mode"] = 1 if form.metadata_only_mode.data else 0
            updates["iceberg_namespace"] = (
                (form.iceberg_namespace.data or "").strip() or None
            )
            updates["iceberg_warehouse"] = (
                (form.iceberg_warehouse.data or "").strip() or None
            )
            new_token = (form.iceberg_auth_token.data or "").strip()
            if form.iceberg_auth_token_clear.data:
                updates["iceberg_auth_token_encrypted"] = None
            elif new_token:
                updates["iceberg_auth_token_encrypted"] = crypto.encrypt_token(new_token)
            # else: leave existing token alone (no key added → no UPDATE clause).

        metrics_storage.update_connection_safety(
            project_id=project["id"], connection_id=conn["id"], updates=updates,
        )
        flash("Настройки сохранены.", "success")
        return redirect(url_for("connections.list_connections", slug=slug))

    return render_template(
        "connections/edit.html",
        project=project, conn=conn, form=form,
        is_iceberg=is_iceberg, is_postgres=is_postgres,
    )


@bp.route("/<conn_id>/delete", methods=["POST"])
@login_required
def delete(slug: str, conn_id: str):
    project, conn = _require_owned_connection(slug, conn_id)
    role = metrics_storage.get_member_role(project["id"], current_user.id)
    if role not in ("owner", "editor"):
        abort(403)
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
    project, conn = _require_owned_connection(slug, conn_id)
    role = metrics_storage.get_member_role(project["id"], current_user.id)
    if role not in ("owner", "editor"):
        abort(403)
    try:
        plain = crypto.decrypt_dsn(conn["dsn_encrypted"])
    except crypto.InvalidToken:
        result = {
            "status": "error", "code": "invalid_ciphertext",
            "message": "Сохранённый DSN не расшифровывается. Пересохрани подключение.",
        }
        _persist_probe_result(project["id"], conn["id"], result)
        return jsonify(result), 422
    # #234: decrypt the Iceberg auth token (if any) and pass all Iceberg
    # fields into probe_connection so the catalog smoke-test uses the
    # same config the collector will use. decrypt_token raises on key
    # mismatch — caught with the same code as DSN invalidation.
    iceberg_token: str | None = None
    if conn.get("iceberg_auth_token_encrypted"):
        try:
            iceberg_token = crypto.decrypt_token(
                conn["iceberg_auth_token_encrypted"],
            )
        except crypto.InvalidToken:
            result = {
                "status": "error", "code": "invalid_ciphertext",
                "message": (
                    "Сохранённый Iceberg auth token не расшифровывается. "
                    "Пересохрани подключение."
                ),
            }
            _persist_probe_result(project["id"], conn["id"], result)
            return jsonify(result), 422
    result = probe_connection(
        plain,
        iceberg_namespace=conn.get("iceberg_namespace"),
        iceberg_warehouse=conn.get("iceberg_warehouse"),
        iceberg_auth_token=iceberg_token,
    )
    _persist_probe_result(project["id"], conn["id"], result)
    status_code = 200 if result["status"] == "ok" else 422
    return jsonify(result), status_code


@bp.route("/<conn_id>/toggle", methods=["POST"])
@login_required
def toggle(slug: str, conn_id: str):
    project, conn = _require_owned_connection(slug, conn_id)
    role = metrics_storage.get_member_role(project["id"], current_user.id)
    if role not in ("owner", "editor"):
        abort(403)
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
        # Same as create: kick the first tick now — toggling on is a
        # deliberate user action expecting fresh metrics promptly.
        add_job_for_connection(
            sched, project["id"], {**conn, "is_active": True},
            run_immediately=True,
        )
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
# Supavisor (Supabase's connection pooler) replies with this when the
# tenant slug in the username doesn't match a live project — typically
# means the project was deleted or the project ref is wrong. Distinct
# from auth_failed: the password isn't even evaluated, the tenant just
# doesn't exist.
_SUPABASE_TENANT_HINTS = (
    "tenant or user not found",
    "tenant/user not found",
    "(enotfound) tenant",
)


def _classify_error(exc: BaseException) -> tuple[str, str]:
    """Map an exception to (code, user-safe message).

    User-safe message must NOT leak DSN content — the DSNFilter (#56)
    will scrub on the way to logs, but the JSON response goes straight
    to the browser without the filter. Phrasing is deliberately generic.
    """
    msg = str(exc).lower()
    if any(h in msg for h in _SUPABASE_TENANT_HINTS):
        return (
            "supabase_tenant_not_found",
            "Supabase project не найден. Проверьте, что проект активен "
            "и project ref в username верный.",
        )
    if any(h in msg for h in _AUTH_HINTS):
        return "auth_failed", "Неверный логин или пароль."
    if any(h in msg for h in _TIMEOUT_HINTS):
        return "timeout", f"Подключение не удалось за {_TEST_CONNECT_TIMEOUT_S} c."
    if any(h in msg for h in _NETWORK_HINTS):
        return "network", "Хост недоступен или DNS не разрешается."
    return "error", "Ошибка подключения (см. логи сервера)."


def _probe_iceberg(
    dsn: str,
    *,
    namespace: str | None = None,
    warehouse: str | None = None,
    auth_token: str | None = None,
) -> dict:
    """Lightweight probe for iceberg+rest:// and iceberg+glue:// DSNs.

    Calls list_namespaces() on the catalog — no data scan, just a metadata
    round-trip. If *namespace* is given:
      - checked against the catalog's namespace list →
        ``namespace_not_found`` if missing,
      - else list_tables(namespace) is called and ``tables_found`` returned.

    *warehouse* / *auth_token* override any same-named values inside the
    DSN query string (#234 — form values win so the operator can rotate
    a token without re-saving the DSN). Empty/None means "use whatever the
    DSN already has".
    """
    started = time.monotonic()
    try:
        from app.db import make_adapter_for_url
        adapter = make_adapter_for_url(
            dsn, warehouse=warehouse, auth_token=auth_token,
        )
        namespaces = adapter.list_namespaces()
        if namespace:
            # Normalise: pyiceberg returns ((ns,),) or ((parent, child),).
            existing = {".".join(ns) if isinstance(ns, tuple | list)
                        else str(ns) for ns in namespaces}
            if namespace not in existing:
                latency_ms = int((time.monotonic() - started) * 1000)
                return {
                    "status": "error",
                    "code": "namespace_not_found",
                    "message": (
                        f"Namespace {namespace!r} не найден в catalog. "
                        f"Доступны: {sorted(existing) or '—'}."
                    ),
                    "latency_ms": latency_ms,
                }
            tables = adapter.list_tables(namespace)
            latency_ms = int((time.monotonic() - started) * 1000)
            return {
                "status": "ok",
                "database": "iceberg",
                "version": f"namespace={namespace}",
                "tables_found": len(tables),
                "latency_ms": latency_ms,
            }
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
        # Token must never leak into the user-facing message. The catch-all
        # text is intentionally generic; full traceback (scrubbed via
        # DSNFilter) goes to server logs only.
        logger.warning("iceberg probe failed: %s", exc, exc_info=True)
        latency_ms = int((time.monotonic() - started) * 1000)
        return {
            "status": "error", "code": "catalog_error",
            "message": "Iceberg catalog недоступен или DSN неверен.",
            "latency_ms": latency_ms,
        }


def _probe_clickhouse(dsn: str) -> dict:
    """Probe a ClickHouse server. Supports clickhouse://, clickhouse+native://
    and clickhouse+http:// DSNs handled by clickhouse-sqlalchemy.

    Mirrors the Postgres path: NullPool (one-off engine, no slot held),
    bounded connect timeout, ``SELECT 1`` + ``SELECT version()`` to confirm
    the server actually accepts a query (not just a TCP handshake). Same
    error classification — auth failures, timeouts, network errors all map
    to the same codes the UI already knows how to render.
    """
    # clickhouse-sqlalchemy passes connect_args straight to clickhouse-driver.
    # Native protocol uses `connect_timeout` (TCP-level handshake bound);
    # the HTTP dialect ignores it but doesn't error on unknown kwargs.
    engine = create_engine(
        dsn,
        poolclass=NullPool,
        connect_args={"connect_timeout": _TEST_CONNECT_TIMEOUT_S},
    )
    started = time.monotonic()
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
            row = conn.execute(text("SELECT version()")).fetchone()
        latency_ms = int((time.monotonic() - started) * 1000)
        return {
            "status": "ok",
            "database": "clickhouse",
            "version": str(row[0]) if row and row[0] else "unknown",
            "latency_ms": latency_ms,
        }
    except SQLAlchemyError as exc:
        logger.warning("clickhouse probe failed: %s", exc, exc_info=True)
        code, user_msg = _classify_error(exc)
        return {
            "status": "error", "code": code, "message": user_msg,
            "latency_ms": int((time.monotonic() - started) * 1000),
        }
    finally:
        engine.dispose()


def probe_connection(
    dsn: str,
    *,
    iceberg_namespace: str | None = None,
    iceberg_warehouse: str | None = None,
    iceberg_auth_token: str | None = None,
) -> dict:
    """Try connecting and reading a couple of harmless metadata bits.

    Dialect support: PostgreSQL (full), ClickHouse (full, #141),
    Iceberg REST/Glue (#111, expanded in #234). Other dialects return
    ``unsupported_dialect``. The JSON response is identical shape across
    success and failure so the UI never has to branch on keys, only on
    ``status``.

    Iceberg-specific params (#234) are no-ops for non-Iceberg backends.
    """
    try:
        backend = make_url(dsn).get_backend_name()
    except Exception:
        return {
            "status": "error", "code": "invalid_dsn",
            "message": "DSN не парсится как URL.",
        }

    if backend.startswith("iceberg"):
        return _probe_iceberg(
            dsn,
            namespace=iceberg_namespace,
            warehouse=iceberg_warehouse,
            auth_token=iceberg_auth_token,
        )

    if backend == "clickhouse":
        return _probe_clickhouse(dsn)

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

            # #229/#230 smoke-check. Все запросы — SELECT-only, никакой
            # записи. Schema берётся из dsn-query-param ``schema`` если
            # передан, иначе settings.MONITORED_SCHEMA — то же значение,
            # с которым реальный сборщик будет ходить в БД.
            from app.config import settings as _settings
            try:
                target_schema = (
                    make_url(dsn).query.get("schema")  # SQLAlchemy URL.query
                    or _settings.MONITORED_SCHEMA
                )
            except Exception:
                target_schema = _settings.MONITORED_SCHEMA

            # #229: USAGE на схему — без него адаптер ничего не увидит.
            # has_schema_privilege возвращает NULL для несуществующей
            # схемы → coalesce, чтобы не упасть на None.
            usage_row = conn.execute(text(
                "SELECT COALESCE(has_schema_privilege(current_user, "
                ":schema, 'USAGE'), false)"
            ), {"schema": target_schema}).fetchone()
            has_usage = bool(usage_row[0]) if usage_row else False

            if not has_usage:
                latency_ms = int((time.monotonic() - started) * 1000)
                return {
                    "status": "error",
                    "code": "no_select_permission",
                    "message": (
                        f"Нет прав USAGE на схему {target_schema!r}. "
                        "Дайте read-only роли GRANT USAGE ON SCHEMA "
                        f"{target_schema} TO <user>."
                    ),
                    "latency_ms": latency_ms,
                }

            # #230: list tables — берём первые
            # _PROBE_TABLES_PREVIEW для UI-preview, считаем total.
            tables_rows = conn.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema "
                "  AND table_type = 'BASE TABLE' "
                "ORDER BY table_name"
            ), {"schema": target_schema}).fetchall()
            all_tables = [r[0] for r in tables_rows]
            tables_preview = all_tables[:_PROBE_TABLES_PREVIEW]
            tables_found = len(all_tables)

            # #229: проверяем INSERT/SELECT на первых _PROBE_PRIV_CHECK
            # таблицах. has_table_privilege принимает имя как
            # schema.table (quoted). Если таблиц нет — has_select=False,
            # has_insert=False, warning не выставляем.
            has_select = False
            has_insert = False
            for tbl in all_tables[:_PROBE_PRIV_CHECK]:
                qualified = f'"{target_schema}"."{tbl}"'
                row_p = conn.execute(text(
                    "SELECT "
                    "  COALESCE(has_table_privilege(current_user, "
                    "    :q, 'SELECT'), false), "
                    "  COALESCE(has_table_privilege(current_user, "
                    "    :q, 'INSERT'), false)"
                ), {"q": qualified}).fetchone()
                if row_p:
                    if row_p[0]:
                        has_select = True
                    if row_p[1]:
                        has_insert = True
                if has_insert and has_select:
                    break  # дальше проверять нечего

            warnings: list[str] = []
            if has_insert:
                warnings.append("write_privileges_detected")

        latency_ms = int((time.monotonic() - started) * 1000)
        return {
            "status": "ok",
            "database": row[0],
            "version": row[1].split(" on ", 1)[0],  # trim "on x86_64-..."
            "latency_ms": latency_ms,
            "tables_found": tables_found,
            "tables_preview": tables_preview,
            "privileges": {"select": has_select, "insert": has_insert},
            "warnings": warnings,
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

    Connections whose ciphertext can't be decrypted (rotated/lost
    ``FERNET_KEY``) are returned with ``dsn=None`` and
    ``dsn_masked="<ошибка дешифровки>"`` — they MUST be visible in the UI
    so the user can delete or re-create them. Silently dropping them
    makes the project look empty while the row is still in the DB,
    which produces confusing "В проекте пока нет подключений" states
    next to a populated /connections list.

    Caller must still respect ownership — this helper assumes
    ``project_id`` was validated against ``current_user`` upstream.
    DSN-consuming callers (collectors etc.) must filter ``dsn is not None``.
    """
    items: list[dict] = []
    for c in metrics_storage.list_connections_for_project(project_id):
        try:
            plain = crypto.decrypt_dsn(c["dsn_encrypted"])
        except crypto.InvalidToken:
            items.append({**c, "dsn": None, "dsn_masked": "<ошибка дешифровки>"})
            continue
        items.append({**c, "dsn": plain, "dsn_masked": mask_dsn(plain)})
    return items
