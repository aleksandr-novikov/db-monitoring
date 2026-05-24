from datetime import UTC, datetime

from flask import Blueprint, jsonify
from flask_login import current_user

from collectors.per_project import list_jobs_for_user, parse_job_id, user_owns_job
from collectors.scheduler import get_scheduler

bp = Blueprint("admin", __name__, url_prefix="/admin")


@bp.route("/jobs")
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
