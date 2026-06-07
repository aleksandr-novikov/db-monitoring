from datetime import UTC, datetime
from functools import wraps

from flask import Blueprint, abort, jsonify, render_template
from flask_login import current_user

from collectors.per_project import list_jobs_for_user, parse_job_id, user_owns_job
from collectors.scheduler import get_scheduler

from .feature_flags import snapshot as _ff_snapshot

bp = Blueprint("admin", __name__, url_prefix="/admin")


def admin_required(view):
    """403 для не-admin пользователей (#220).

    До этого decorator-а каждый аутентифицированный юзер имел доступ к
    /admin/* — это было дырой, потому что rollback-checklist и feature-
    flags toggle — это операторские действия, не для tenant-юзеров.

    Anon-юзера ловит ``_require_login_for_html`` в app.app до этого
    decorator-а — здесь нам гарантировано current_user.is_authenticated.

    Honours ``LOGIN_DISABLED`` (test bypass) тем же способом что и
    ``_require_login_for_html`` — иначе все pre-#220 admin тесты,
    написанные с LOGIN_DISABLED=True, упали бы 403. Тесты, явно
    проверяющие #220 acceptance, поднимают LOGIN_DISABLED=False.
    """
    @wraps(view)
    def wrapper(*args, **kwargs):
        from flask import current_app

        if current_app.config.get("LOGIN_DISABLED"):
            return view(*args, **kwargs)
        if not getattr(current_user, "is_admin", False):
            abort(403)
        return view(*args, **kwargs)
    return wrapper


@bp.route("/jobs")
@admin_required
def list_jobs():
    """Per-user collection jobs (#54).

    Returns jobs whose id matches ``collect:<project_id>:<connection_id>``
    where ``project_id`` belongs to the current user. Process-wide jobs
    (forecast retrain, drift sweep, …) are intentionally excluded —
    they're operator concerns, not tenant ones.

    Anonymous callers (e.g. health checks, TESTING) get the full list as
    a fallback — useful for diagnostics when the user context isn't set.
    """
    scheduler = get_scheduler()
    if scheduler is None:
        return jsonify([])

    if current_user.is_authenticated:
        return jsonify(list_jobs_for_user(scheduler, current_user.id))

    return jsonify([
        {
            "id": job.id,
            "name": job.name,
            "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        }
        for job in scheduler.get_jobs()
    ])


@bp.route("/jobs/<job_id>/run", methods=["POST"])
@admin_required
def run_job(job_id: str):
    """Trigger a job immediately by setting its next_run_time to now.

    Per-connection collection jobs (``collect:<pid>:<cid>``) require the
    job's ``project_id`` to belong to ``current_user`` — 404 otherwise so
    a guessed id leaks nothing about which jobs exist. Global ops jobs
    (no ``collect:`` prefix) stay accessible to authenticated users.
    """
    scheduler = get_scheduler()
    if scheduler is None:
        return jsonify({"error": "scheduler not running"}), 503

    if (
        current_user.is_authenticated
        and parse_job_id(job_id) is not None
        and not user_owns_job(current_user.id, job_id)
    ):
        return jsonify({"error": f"job '{job_id}' not found"}), 404

    job = scheduler.get_job(job_id)
    if job is None:
        return jsonify({"error": f"job '{job_id}' not found"}), 404

    job.modify(next_run_time=datetime.now(UTC))
    return jsonify({
        "status": "triggered",
        "job_id": job_id,
        "next_run_time": job.next_run_time.isoformat() if job.next_run_time else None,
    })


@bp.route("/rollback-checklist")
@admin_required
def rollback_checklist():
    """Static checklist for the operator at incident time (#107).

    Cribbed straight from docs/runbooks/rollback.md but as a checklist with
    checkboxes — dejavu UX so a frazzled on-call doesn't have to scroll
    through Mermaid diagrams. Pure server-side render, no JS dependencies.
    """
    return render_template("admin/rollback_checklist.html")


@bp.route("/feature-flags")
@admin_required
def feature_flags():
    """List every known feature flag and its current value (#104).

    Read-only — toggling still requires changing the env var + restart.
    No write API on purpose: a runtime mutation surface would need its
    own auth/audit story, which is out of scope for an env-only flag
    system. Operators flip flags via deploy config.

    Anonymous callers are tolerated (TESTING / health-check diagnostics)
    so this stays consistent with /admin/jobs.
    """
    return jsonify(_ff_snapshot())
