from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from app.app import create_app


@pytest.fixture
def client(monkeypatch):
    import collectors.scheduler as sched_mod
    monkeypatch.setattr(sched_mod, "_scheduler", None)
    app = create_app({"TESTING": True})
    with app.test_client() as c:
        yield c


@pytest.fixture
def fake_job():
    job = MagicMock()
    job.id = "collect_all_tables"
    job.name = "collect_all_tables"
    job.next_run_time = datetime(2026, 4, 25, 10, 15, tzinfo=timezone.utc)
    job.trigger = MagicMock(__str__=lambda self: "interval[0:15:00]")
    return job


# ---------------------------------------------------------------------------
# GET /admin/jobs
# ---------------------------------------------------------------------------

def test_list_jobs_returns_job_info(client, monkeypatch, fake_job):
    import collectors.scheduler as sched_mod
    fake_scheduler = MagicMock()
    fake_scheduler.get_jobs.return_value = [fake_job]
    monkeypatch.setattr(sched_mod, "_scheduler", fake_scheduler)

    resp = client.get("/admin/jobs")

    assert resp.status_code == 200
    data = resp.get_json()
    assert len(data) == 1
    assert data[0]["id"] == "collect_all_tables"
    assert data[0]["name"] == "collect_all_tables"
    assert data[0]["next_run_time"] == "2026-04-25T10:15:00+00:00"
    assert data[0]["trigger"] == "interval[0:15:00]"


def test_list_jobs_when_scheduler_not_running(client, monkeypatch):
    import collectors.scheduler as sched_mod
    monkeypatch.setattr(sched_mod, "_scheduler", None)

    resp = client.get("/admin/jobs")

    assert resp.status_code == 200
    assert resp.get_json() == []


def test_list_jobs_next_run_time_none(client, monkeypatch, fake_job):
    import collectors.scheduler as sched_mod
    fake_job.next_run_time = None
    fake_scheduler = MagicMock()
    fake_scheduler.get_jobs.return_value = [fake_job]
    monkeypatch.setattr(sched_mod, "_scheduler", fake_scheduler)

    resp = client.get("/admin/jobs")

    assert resp.status_code == 200
    assert resp.get_json()[0]["next_run_time"] is None


# ---------------------------------------------------------------------------
# POST /admin/jobs/<job_id>/run
# ---------------------------------------------------------------------------

def test_run_job_triggers_immediately(client, monkeypatch, fake_job):
    import collectors.scheduler as sched_mod
    fake_scheduler = MagicMock()
    fake_scheduler.get_job.return_value = fake_job
    monkeypatch.setattr(sched_mod, "_scheduler", fake_scheduler)

    resp = client.post("/admin/jobs/collect_all_tables/run")

    assert resp.status_code == 200
    data = resp.get_json()
    assert data["status"] == "triggered"
    assert data["job_id"] == "collect_all_tables"
    fake_job.modify.assert_called_once()
    _, kwargs = fake_job.modify.call_args
    assert isinstance(kwargs["next_run_time"], datetime)


def test_run_job_scheduler_not_running(client, monkeypatch):
    import collectors.scheduler as sched_mod
    monkeypatch.setattr(sched_mod, "_scheduler", None)

    resp = client.post("/admin/jobs/collect_all_tables/run")

    assert resp.status_code == 503
    assert "error" in resp.get_json()


def test_run_job_not_found(client, monkeypatch):
    import collectors.scheduler as sched_mod
    fake_scheduler = MagicMock()
    fake_scheduler.get_job.return_value = None
    monkeypatch.setattr(sched_mod, "_scheduler", fake_scheduler)

    resp = client.post("/admin/jobs/unknown_job/run")

    assert resp.status_code == 404
    assert "error" in resp.get_json()
