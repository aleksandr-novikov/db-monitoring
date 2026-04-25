from datetime import datetime, timezone

from flask import Blueprint, jsonify

from collectors.scheduler import get_scheduler

bp = Blueprint("admin", __name__, url_prefix="/admin")


@bp.route("/jobs")
def list_jobs():
    scheduler = get_scheduler()
    if scheduler is None:
        return jsonify([])

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
    scheduler = get_scheduler()
    if scheduler is None:
        return jsonify({"error": "scheduler not running"}), 503

    job = scheduler.get_job(job_id)
    if job is None:
        return jsonify({"error": f"job '{job_id}' not found"}), 404

    job.modify(next_run_time=datetime.now(timezone.utc))
    return jsonify({"status": "triggered", "job_id": job_id})
