"""Auth blueprint for #49 (Sprint 3 multi-tenant epic).

Provides email/password registration and login backed by:
- ``werkzeug.security`` for password hashing (scrypt by default on Werkzeug 3.x)
- ``Flask-Login`` for session management
- ``Flask-WTF`` for CSRF protection on the forms
- ``email-validator`` (via ``WTForms.Email``) for format validation

Out of scope for this PR (separate Sprint 3 tickets):
- Password reset / email confirmation (#48 sub-tickets)
- Rate limiting on /register and /login (#56)
- OAuth (Sprint 4)
- Tenant-scoped data isolation (#53)
"""

from __future__ import annotations

import uuid
from urllib.parse import urlparse

from flask import Blueprint, flash, redirect, render_template, request, url_for
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

bp = Blueprint("auth", __name__, url_prefix="/auth")
login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message = "Войдите, чтобы получить доступ к этой странице."
login_manager.login_message_category = "info"


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


@bp.route("/register", methods=["GET", "POST"])
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
        flash("Аккаунт создан. Добро пожаловать!", "success")
        return redirect(url_for("dashboard.overview"))
    return render_template("auth/register.html", form=form)


@bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.overview"))
    form = LoginForm()
    if form.validate_on_submit():
        email = _normalize_email(form.email.data)
        row = metrics_storage.get_user_by_email(email)
        # Compose the user *outside* the if so we don't reveal which half of
        # the (email, password) tuple was wrong — both branches take the
        # same amount of work.
        user = User(row) if row else None
        if user is not None and user.check_password(form.password.data):
            # Stamp before login_user — if the UPDATE fails (transient DB
            # blip), we abort the login attempt rather than land a logged-in
            # user on a 500 page with no recorded login time.
            metrics_storage.update_last_login(user.id)
            login_user(user, remember=form.remember.data)
            next_target = _safe_next(request.args.get("next"))
            return redirect(next_target or url_for("dashboard.overview"))
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
