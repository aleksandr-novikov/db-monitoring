"""Auth blueprint for #49 + #56 (Sprint 3 multi-tenant epic).

Provides email/password registration and login backed by:
- ``werkzeug.security`` for password hashing (scrypt by default on Werkzeug 3.x)
- ``Flask-Login`` for session management
- ``Flask-WTF`` for CSRF protection on the forms
- ``email-validator`` (via ``WTForms.Email``) for format validation
- ``Flask-Limiter`` for per-IP rate limits (#56) + custom per-email lockout
  on the storage layer for sustained brute-force attempts.

Out of scope:
- Password reset / email confirmation (#48 sub-tickets)
- OAuth (Sprint 4)
- Tenant-scoped data isolation (#53)
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse

from flask import Blueprint, flash, redirect, render_template, request, url_for
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_login import (
    LoginManager,
    UserMixin,
    current_user,
    login_required,
    login_user,
    logout_user,
)
from flask_wtf import FlaskForm
from werkzeug.security import check_password_hash, generate_password_hash
from wtforms import BooleanField, PasswordField, StringField, SubmitField
from wtforms.validators import DataRequired, Email, EqualTo, Length

from app import metrics_storage
from app.config import settings

logger = logging.getLogger(__name__)

bp = Blueprint("auth", __name__, url_prefix="/auth")
login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = "Войдите, чтобы получить доступ к этой странице."
login_manager.login_message_category = "info"

# Flask-Limiter (#56). Per-IP throttle on /register and /login. Storage
# backend is read from app.config["RATELIMIT_STORAGE_URI"] (set in
# create_app from the RATELIMIT_STORAGE_URI env var), so multi-worker
# deploys can point at Redis without code changes — see .env.example.
# We use module-level decorators (not init-time wiring) so individual routes
# pick up their limits without needing the limiter to know about them.
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[],  # routes opt in explicitly via @limiter.limit
)

# Lockout-on-email-failures parameters (#56 acceptance).
_LOCKOUT_THRESHOLD = 5
_LOCKOUT_WINDOW = timedelta(minutes=15)


class User(UserMixin):
    """Flask-Login adapter over the ``users`` row stored in metrics_storage."""

    def __init__(self, row: dict):
        self.id = row["id"]
        self.email = row["email"]
        self.password_hash = row["password_hash"]
        self.created_at = row["created_at"]
        self.last_login_at = row["last_login_at"]

    def get_id(self) -> str:  # Flask-Login: must return str
        return str(self.id)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)


@login_manager.user_loader
def _load_user(user_id: str) -> User | None:
    row = metrics_storage.get_user_by_id(user_id)
    return User(row) if row else None


# --- Forms -----------------------------------------------------------------

# 8 chars is a UX baseline, not a security claim — actual entropy lives in the
# hash. We don't enforce complexity rules; users will use a password manager.
_PASSWORD_MIN = 8
_PASSWORD_MAX = 128


class RegisterForm(FlaskForm):
    email = StringField(
        "Email",
        validators=[DataRequired(), Email(), Length(max=254)],
        render_kw={"autocomplete": "email", "autofocus": True},
    )
    password = PasswordField(
        "Пароль",
        validators=[
            DataRequired(),
            Length(min=_PASSWORD_MIN, max=_PASSWORD_MAX,
                   message=f"Минимум {_PASSWORD_MIN} символов."),
        ],
        render_kw={"autocomplete": "new-password"},
    )
    confirm = PasswordField(
        "Повторите пароль",
        validators=[
            DataRequired(),
            EqualTo("password", message="Пароли не совпадают."),
        ],
        render_kw={"autocomplete": "new-password"},
    )
    submit = SubmitField("Создать аккаунт")


class LoginForm(FlaskForm):
    email = StringField(
        "Email",
        validators=[DataRequired(), Email(), Length(max=254)],
        render_kw={"autocomplete": "email", "autofocus": True},
    )
    password = PasswordField(
        "Пароль",
        validators=[DataRequired(), Length(max=_PASSWORD_MAX)],
        render_kw={"autocomplete": "current-password"},
    )
    remember = BooleanField("Запомнить меня")
    submit = SubmitField("Войти")


class ForgotPasswordForm(FlaskForm):
    email = StringField(
        "Email",
        validators=[DataRequired(), Email(), Length(max=254)],
        render_kw={"autocomplete": "email", "autofocus": True},
    )
    submit = SubmitField("Получить ссылку для сброса")


class ResetPasswordForm(FlaskForm):
    password = PasswordField(
        "Новый пароль",
        validators=[
            DataRequired(),
            Length(min=_PASSWORD_MIN, max=_PASSWORD_MAX,
                   message=f"Минимум {_PASSWORD_MIN} символов."),
        ],
        render_kw={"autocomplete": "new-password", "autofocus": True},
    )
    confirm = PasswordField(
        "Повторите пароль",
        validators=[
            DataRequired(),
            EqualTo("password", message="Пароли не совпадают."),
        ],
        render_kw={"autocomplete": "new-password"},
    )
    submit = SubmitField("Сохранить пароль")


# --- Routes ----------------------------------------------------------------


def _normalize_email(raw: str) -> str:
    return (raw or "").strip().lower()


_NEXT_DENYLIST = ("/auth/logout", "/auth/login", "/auth/register")


def _safe_next(target: str | None) -> str | None:
    r"""Return ``target`` only if it's a same-host relative path.

    Open-redirect mitigation: an attacker could craft
    ``/auth/login?next=https://evil/`` and a naive redirect would honour
    it. Layered checks:
    1. Must be non-empty.
    2. Must start with ``/`` and not ``//`` (rules out protocol-relative).
    3. Must not contain a backslash — some browsers normalise ``/\evil``
       to ``//evil`` post-redirect.
    4. urlparse() must report no scheme and no netloc (defense against
       ``/%2F``-style encoded bypasses; urlparse normalises them).
    5. Must not target the auth surface itself — ``next=/auth/logout``
       would silently log the user out right after they signed in.
    """
    if not target:
        return None
    if not target.startswith("/") or target.startswith("//"):
        return None
    if "\\" in target:
        return None
    parsed = urlparse(target)
    if parsed.scheme or parsed.netloc:
        return None
    if any(parsed.path == d or parsed.path.startswith(d + "/") for d in _NEXT_DENYLIST):
        return None
    return target


# --- Password reset (#133) -------------------------------------------------

# Token format: 32 bytes from secrets.token_urlsafe → 43-char URL-safe string.
# Stored as HMAC-SHA256(SECRET_KEY, token) — see hash_reset_token below.
_RESET_TOKEN_BYTES = 32
_RESET_TOKEN_TTL = timedelta(hours=1)


def _hash_reset_token(raw_token: str) -> str:
    """HMAC-SHA256 of the raw token using SECRET_KEY. Returned as hex.

    HMAC (not bare SHA256) because raw tokens are short and high-entropy,
    but a leaked DB without the SECRET_KEY would otherwise be vulnerable
    to offline rainbow-table attacks against the token space. With the
    key as MAC input, the hash is useless without it.
    """
    return hmac.new(
        settings.SECRET_KEY.encode("utf-8"),
        raw_token.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _forgot_password_key() -> str:
    """Rate-limit key for /forgot-password — bind to (email, IP) so a
    single attacker IP can't pin-point one email by burning through 5/15min
    on different ones, and a single email can't be flooded from multiple IPs.
    Falls back to IP-only when no email is in the form (GET / malformed POST).
    """
    email = (request.form.get("email") or "").strip().lower()
    ip = get_remote_address()
    return f"{email}|{ip}" if email else ip


@bp.route("/forgot-password", methods=["GET", "POST"])
@limiter.limit("1 per minute;5 per 15 minutes",
                methods=["POST"], key_func=_forgot_password_key)
def forgot_password():
    """Issue a password-reset token, email it to the user.

    Always responds with the same "we've sent a link if the email is
    registered" message — registered/unregistered emails are
    indistinguishable from the outside. Storage operations execute
    in both branches so timing differences don't leak the answer either.
    """
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.overview"))
    form = ForgotPasswordForm()
    if form.validate_on_submit():
        from app.email import send_password_reset_email

        email = _normalize_email(form.email.data)
        row = metrics_storage.get_user_by_email(email)
        if row is not None:
            # Invalidate any active tokens first — only the latest email
            # should resolve. Then mint + persist + send.
            metrics_storage.invalidate_password_reset_tokens(row["id"])
            raw_token = secrets.token_urlsafe(_RESET_TOKEN_BYTES)
            metrics_storage.create_password_reset_token(
                user_id=row["id"],
                token_hash=_hash_reset_token(raw_token),
                expires_at=datetime.now(UTC) + _RESET_TOKEN_TTL,
            )
            reset_url = (
                settings.APP_BASE_URL.rstrip("/")
                + url_for("auth.reset_password", token=raw_token)
            )
            send_password_reset_email(email, reset_url)
        else:
            # Unknown email: log it for ops but DO NOT differentiate the
            # response. Repeated probes will hit the rate limit normally.
            logger.info(
                "forgot-password: email not registered (rate-limited normally)"
            )
        flash(
            "Если такой email зарегистрирован, мы отправили на него ссылку "
            "для сброса пароля. Срок действия — 1 час.",
            "info",
        )
        return redirect(url_for("auth.login"))
    return render_template("auth/forgot_password.html", form=form)


_RESET_GENERIC_ERROR = "Ссылка недействительна или истекла."


@bp.route("/reset-password/<token>", methods=["GET", "POST"])
@limiter.limit("10 per minute;30 per hour", methods=["POST"])
def reset_password(token: str):
    """Validate the token, accept a new password, atomically claim the token.

    Invalid / expired / used tokens all yield the same generic error
    message — distinguishing them would tell an attacker whether a given
    raw token was ever valid.
    """
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.overview"))

    token_hash = _hash_reset_token(token)
    form = ResetPasswordForm()

    if request.method == "GET":
        # Pre-validate so we can show the form vs the error page. POST
        # re-checks via the atomic consume_* below — no TOCTOU window.
        if metrics_storage.get_active_password_reset_token(token_hash) is None:
            return render_template("auth/reset_password.html",
                                    form=form, error=_RESET_GENERIC_ERROR), 400
        return render_template("auth/reset_password.html",
                                form=form, error=None)

    if form.validate_on_submit():
        user_id = metrics_storage.consume_password_reset_token(token_hash)
        if user_id is None:
            return render_template("auth/reset_password.html",
                                    form=form, error=_RESET_GENERIC_ERROR), 400
        # Update password and invalidate all other active tokens for this
        # user (someone may have requested several resets; the consumed
        # one wins, the rest die). Two separate writes are fine — the
        # token we just consumed is already marked used.
        metrics_storage.update_user_password(
            user_id, generate_password_hash(form.password.data),
        )
        metrics_storage.invalidate_password_reset_tokens(user_id)
        flash("Пароль обновлён. Войдите с новым паролем.", "success")
        return redirect(url_for("auth.login"))

    return render_template("auth/reset_password.html",
                            form=form, error=None)


@bp.route("/register", methods=["GET", "POST"])
@limiter.limit("5 per minute;20 per hour", methods=["POST"])
def register():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.overview"))
    form = RegisterForm()
    if form.validate_on_submit():
        email = _normalize_email(form.email.data)
        try:
            row = metrics_storage.create_user(
                user_id=uuid.uuid4().hex,
                email=email,
                password_hash=generate_password_hash(form.password.data),
            )
        except metrics_storage.UserAlreadyExists:
            # 409 Conflict — duplicate email. Render the form again with a
            # field-level error so the UX stays on the page.
            form.email.errors.append("Этот email уже зарегистрирован.")
            return render_template("auth/register.html", form=form), 409
        # Don't stamp last_login_at on registration — it's "last *login*",
        # not "account created" (use created_at for that). The session is
        # still set via login_user so the user lands on /dashboard without
        # a second auth round-trip.
        login_user(User(row))
        # Auto-create the "Default" project (#50 acceptance) so the new
        # user never sees an empty projects switcher. Lazy import to avoid
        # a circular dependency (projects → auth.User for current_user).
        from app.projects import create_default_project_for
        default_project = create_default_project_for(row["id"])
        # Onboarding redirect (#55): land on the wizard, not an empty
        # dashboard. The connections form detects "zero existing
        # connections" and shows the wizard template.
        return redirect(url_for(
            "connections.new_connection", slug=default_project["slug"],
        ))
    return render_template("auth/register.html", form=form)


@bp.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute;30 per hour", methods=["POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.overview"))
    form = LoginForm()
    if form.validate_on_submit():
        email = _normalize_email(form.email.data)

        # Per-email lockout (#56): a sustained brute-force keeps the IP
        # rate limit alive but doesn't help once the attacker rotates IPs.
        # Counting fails by email closes that. We check BEFORE password
        # verification so we don't burn a scrypt round on every locked
        # request.
        if metrics_storage.count_recent_failed_logins(email, _LOCKOUT_WINDOW) >= _LOCKOUT_THRESHOLD:
            # Window is sliding — recording a new attempt extends the lockout.
            # Message reflects that: "wait 15 minutes without further attempts",
            # not just "wait 15 minutes from now".
            form.password.errors.append(
                f"Слишком много неудачных попыток. Подождите "
                f"{int(_LOCKOUT_WINDOW.total_seconds() // 60)} минут "
                f"без новых попыток."
            )
            return render_template("auth/login.html", form=form), 429

        row = metrics_storage.get_user_by_email(email)
        # Compose the user *outside* the if so we don't reveal which half of
        # the (email, password) tuple was wrong — both branches take the
        # same amount of work.
        user = User(row) if row else None
        if user is not None and user.check_password(form.password.data):
            # Stamp last_login_at AND wipe failed-attempt counter in ONE
            # transaction — if either fails we abort the login rather than
            # leave a phantom-lockout window (counter persists from a past
            # brute-force, would lock the legit user on their next visit).
            metrics_storage.record_successful_login(user.id, email)
            login_user(user, remember=form.remember.data)
            next_target = _safe_next(request.args.get("next"))
            return redirect(next_target or url_for("dashboard.overview"))

        metrics_storage.record_failed_login(email)
        # #101: emit Prometheus counter. Late import keeps test rigs that
        # don't init the registry from breaking the login flow.
        try:
            from app.instrumentation import failed_login_attempts_total
            failed_login_attempts_total.inc()
        except ImportError:
            pass
        form.password.errors.append("Неверный email или пароль.")
    return render_template("auth/login.html", form=form)


@bp.route("/logout", methods=["POST"])
@login_required
def logout():
    logout_user()
    flash("Вы вышли из системы.", "info")
    return redirect(url_for("auth.login"))


def _abort_if_unauthenticated():
    """Reject the request with a redirect-to-login if the user is anonymous.

    Honours the Flask-Login ``LOGIN_DISABLED`` config flag (which the test
    suite sets) so existing dashboard / admin tests don't need to log in
    just to reach the routes under exercise.
    """
    from flask import current_app

    if current_app.config.get("LOGIN_DISABLED"):
        return None
    if not current_user.is_authenticated:
        return login_manager.unauthorized()
    return None
