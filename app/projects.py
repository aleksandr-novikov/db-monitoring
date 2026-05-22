"""Projects blueprint for #50 (Sprint 3 multi-tenant epic).

Each authenticated user owns a list of projects; a project is a container
for the DB connections that will land in #51. Every storage helper
filters by ``user_id`` so URL-guessing a sibling user's slug can't escalate.

Routes:
- ``GET  /projects``                — list the user's projects
- ``GET  /projects/new``            — create form
- ``POST /projects/new``            — create
- ``GET  /projects/<slug>``         — detail (placeholder until #51)
- ``POST /projects/<slug>/delete``  — hard delete
- ``POST /projects/<slug>/switch``  — set session.current_project_id

Cross-cutting concerns:
- ``g.current_project`` is populated in a before_request hook (see
  ``app/app.py``); templates use it for the header switcher.
- New users get a "Default" project auto-created on register; the
  create_user_default_project helper is invoked from ``app/auth.py``.
"""

from __future__ import annotations

import re
import uuid

from flask import (
    Blueprint,
    abort,
    flash,
    g,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from flask_login import current_user, login_required
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired, Length, Regexp

from app import metrics_storage

bp = Blueprint("projects", __name__, url_prefix="/projects")

_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?$")


class ProjectForm(FlaskForm):
    name = StringField(
        "Название",
        validators=[DataRequired(), Length(min=1, max=80)],
        render_kw={"autocomplete": "off", "autofocus": True},
    )
    slug = StringField(
        "Слаг",
        validators=[
            DataRequired(),
            Length(min=1, max=40),
            Regexp(
                _SLUG_RE,
                message="Слаг: латиница, цифры, дефисы; начинается и "
                "заканчивается на буквой или цифрой.",
            ),
        ],
        render_kw={"autocomplete": "off"},
    )
    submit = SubmitField("Создать")


def _slugify(value: str) -> str:
    """Best-effort slug from a name (used as a default for the form)."""
    s = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return s[:40] or "project"


# --- Routes ---------------------------------------------------------------


@bp.route("")
@bp.route("/")
@login_required
def list_projects():
    projects = metrics_storage.list_projects_for_user(current_user.id)
    return render_template("projects/list.html", projects=projects)


@bp.route("/new", methods=["GET", "POST"])
@login_required
def new_project():
    form = ProjectForm()
    if form.validate_on_submit():
        name = form.name.data.strip()
        slug = form.slug.data.strip().lower()
        try:
            project = metrics_storage.create_project(
                project_id=uuid.uuid4().hex,
                user_id=current_user.id,
                name=name,
                slug=slug,
            )
        except metrics_storage.ProjectSlugTaken:
            form.slug.errors.append("Проект с таким слагом уже существует.")
            return render_template("projects/new.html", form=form), 409
        # New project becomes the current one — saves a click.
        session["current_project_id"] = project["id"]
        flash(f"Проект «{name}» создан.", "success")
        return redirect(url_for("projects.detail", slug=slug))
    return render_template("projects/new.html", form=form)


def _require_owned_project(slug: str) -> dict:
    """Lookup-or-404 scoped to the current user. Centralises the ownership
    check so every route gets it right by default."""
    project = metrics_storage.get_project_by_slug(current_user.id, slug)
    if project is None:
        abort(404)
    return project


@bp.route("/<slug>")
@login_required
def detail(slug: str):
    project = _require_owned_project(slug)
    # Placeholder: real content lands with #51 (DB connections).
    return render_template("projects/detail.html", project=project)


@bp.route("/<slug>/delete", methods=["POST"])
@login_required
def delete(slug: str):
    project = _require_owned_project(slug)
    metrics_storage.delete_project(current_user.id, project["id"])
    # If we just deleted the current project, fall back to the first
    # remaining one (or clear the slot — list view will pick again).
    if session.get("current_project_id") == project["id"]:
        session.pop("current_project_id", None)
    flash(f"Проект «{project['name']}» удалён.", "info")
    return redirect(url_for("projects.list_projects"))


@bp.route("/<slug>/switch", methods=["POST"])
@login_required
def switch(slug: str):
    project = _require_owned_project(slug)
    session["current_project_id"] = project["id"]
    # Honour ?next= if present and safe — same allowlist policy as
    # auth.login. Falls back to dashboard.
    from app.auth import _safe_next

    target = _safe_next(request.args.get("next")) or url_for("dashboard.overview")
    return redirect(target)


# --- Helpers used by other blueprints -------------------------------------


def create_default_project_for(user_id: str) -> dict:
    """Auto-create the "Default" project on user registration (#50 acceptance).

    Called from ``app/auth.py::register``. Idempotent: if the user somehow
    already has a "default" project (e.g. retry after a crash), returns it.
    """
    existing = metrics_storage.get_project_by_slug(user_id, "default")
    if existing is not None:
        return existing
    return metrics_storage.create_project(
        project_id=uuid.uuid4().hex,
        user_id=user_id,
        name="Default",
        slug="default",
    )


def load_current_project_into_g() -> None:
    """before_request hook: populates ``g.current_project`` so templates
    can render the header switcher without each route doing the lookup.

    Pure-read: never mutates the session beyond the implicit pop-on-stale
    that happens when the stored id no longer belongs to the user.
    """
    g.current_project = None
    g.user_projects = []
    if not current_user.is_authenticated:
        return
    g.user_projects = metrics_storage.list_projects_for_user(current_user.id)
    if not g.user_projects:
        session.pop("current_project_id", None)
        return
    stored_id = session.get("current_project_id")
    if stored_id:
        for p in g.user_projects:
            if p["id"] == stored_id:
                g.current_project = p
                return
        # Stored id doesn't belong to this user any more (deleted, or the
        # session is reused across accounts). Drop it.
        session.pop("current_project_id", None)
    # Default: pick the first project so the header switcher has something
    # to show even on first visit after registration.
    g.current_project = g.user_projects[0]
    session["current_project_id"] = g.current_project["id"]
