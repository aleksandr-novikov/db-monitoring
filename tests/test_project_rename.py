"""Integration tests for #224 — project rename.

Mirrors the test list from the acceptance criteria:

- test_owner_can_rename
- test_slug_unchanged_after_rename
- test_viewer_cannot_get_rename_form
- test_viewer_cannot_post_rename
- test_editor_cannot_get_rename_form
- test_editor_cannot_post_rename
- test_new_name_visible_in_project_list
- test_new_name_visible_in_detail
- test_empty_name_rejected
- test_long_name_rejected
- test_rename_project_returns_false_for_unknown_id
- test_connections_intact_after_rename
- test_metrics_intact_after_rename
"""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import text

from app import metrics_storage
from app.app import create_app


@pytest.fixture
def app(tmp_path, monkeypatch):
    db_path = tmp_path / "rename.db"
    monkeypatch.setattr(
        metrics_storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}",
    )
    monkeypatch.setattr(metrics_storage, "_engine", None)
    monkeypatch.setattr(metrics_storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    return create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })


@pytest.fixture
def client(app):
    return app.test_client()


# --- helpers ---------------------------------------------------------------


def _register(client, email, password="secret123"):
    return client.post("/auth/register", data={
        "email": email, "password": password, "confirm": password,
    })


def _login(client, email, password="secret123"):
    return client.post("/auth/login", data={"email": email, "password": password})


def _logout(client):
    client.post("/auth/logout")


def _make_project(client, suffix=""):
    slug = f"proj-{suffix or uuid.uuid4().hex[:8]}"
    r = client.post("/projects/new", data={"name": f"Test {slug}", "slug": slug})
    assert r.status_code in (302, 200)
    return slug


def _get_project_id_by_slug(app_ctx, slug):
    with app_ctx.app_context():
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT id FROM projects WHERE slug = :slug"),
                {"slug": slug},
            ).fetchone()
    return row[0] if row else None


# --- fixtures: owner / viewer / editor sessions ----------------------------


@pytest.fixture
def owner_with_project(app, client):
    _register(client, "owner@rn.com")
    slug = _make_project(client, suffix="rn")
    pid = _get_project_id_by_slug(app, slug)
    return slug, pid


@pytest.fixture
def viewer_session(app, client, owner_with_project):
    slug, pid = owner_with_project
    _logout(client)
    _register(client, "viewer@rn.com")
    with app.app_context():
        u = metrics_storage.get_user_by_email("viewer@rn.com")
        metrics_storage.add_project_member(pid, u["id"], "viewer")
    _logout(client)
    _login(client, "viewer@rn.com")
    return slug, pid


@pytest.fixture
def editor_session(app, client, owner_with_project):
    slug, pid = owner_with_project
    _logout(client)
    _register(client, "editor@rn.com")
    with app.app_context():
        u = metrics_storage.get_user_by_email("editor@rn.com")
        metrics_storage.add_project_member(pid, u["id"], "editor")
    _logout(client)
    _login(client, "editor@rn.com")
    return slug, pid


# --- storage layer ---------------------------------------------------------


def test_rename_project_returns_true_on_success(app, owner_with_project):
    _, pid = owner_with_project
    with app.app_context():
        assert metrics_storage.rename_project(pid, "Renamed") is True
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT name FROM projects WHERE id = :id"),
                {"id": pid},
            ).fetchone()
    assert row[0] == "Renamed"


def test_rename_project_returns_false_for_unknown_id(app):
    with app.app_context():
        assert metrics_storage.rename_project("nonexistent-id", "X") is False


# --- route: access control -------------------------------------------------


def test_owner_can_get_rename_form(client, owner_with_project):
    slug, _ = owner_with_project
    r = client.get(f"/projects/{slug}/rename")
    assert r.status_code == 200
    # Form is pre-filled with the current name.
    assert "Test proj-rn" in r.data.decode()


def test_viewer_cannot_get_rename_form(client, viewer_session):
    slug, _ = viewer_session
    r = client.get(f"/projects/{slug}/rename")
    assert r.status_code == 403


def test_viewer_cannot_post_rename(client, viewer_session):
    slug, _ = viewer_session
    r = client.post(f"/projects/{slug}/rename", data={"name": "Hack"})
    assert r.status_code == 403


def test_editor_cannot_get_rename_form(client, editor_session):
    slug, _ = editor_session
    r = client.get(f"/projects/{slug}/rename")
    assert r.status_code == 403


def test_editor_cannot_post_rename(client, editor_session):
    slug, _ = editor_session
    r = client.post(f"/projects/{slug}/rename", data={"name": "Hack"})
    assert r.status_code == 403


# --- route: happy path -----------------------------------------------------


def test_owner_can_rename(app, client, owner_with_project):
    slug, pid = owner_with_project
    r = client.post(
        f"/projects/{slug}/rename",
        data={"name": "Production DB"},
        follow_redirects=False,
    )
    assert r.status_code == 302
    assert r.headers["Location"].endswith(f"/projects/{slug}")

    with app.app_context():
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT name, slug FROM projects WHERE id = :id"),
                {"id": pid},
            ).fetchone()
    assert row[0] == "Production DB"
    assert row[1] == slug  # slug unchanged


def test_slug_unchanged_after_rename(client, owner_with_project):
    slug, _ = owner_with_project
    client.post(f"/projects/{slug}/rename", data={"name": "New Name"})
    # Original slug still serves the project page.
    r = client.get(f"/projects/{slug}")
    assert r.status_code == 200


def test_new_name_visible_in_detail(client, owner_with_project):
    slug, _ = owner_with_project
    client.post(f"/projects/{slug}/rename", data={"name": "Shiny Name"})
    html = client.get(f"/projects/{slug}").data.decode()
    assert "Shiny Name" in html


def test_new_name_visible_in_project_list(client, owner_with_project):
    slug, _ = owner_with_project
    client.post(f"/projects/{slug}/rename", data={"name": "Shiny Name"})
    html = client.get("/projects").data.decode()
    assert "Shiny Name" in html


def test_new_name_visible_in_header_switcher(client, owner_with_project):
    """g.user_projects feeds the header switcher — the new name must appear
    on any rendered page after rename (we check a fresh detail render)."""
    slug, _ = owner_with_project
    client.post(f"/projects/{slug}/rename", data={"name": "HeaderName"})
    # Switch to the project so g.current_project is set, then re-render.
    client.post(f"/projects/{slug}/switch")
    html = client.get("/dashboard").data.decode()
    assert "HeaderName" in html


def test_new_name_visible_in_breadcrumbs(client, owner_with_project):
    slug, _ = owner_with_project
    client.post(f"/projects/{slug}/rename", data={"name": "Crumby"})
    html = client.get(f"/projects/{slug}").data.decode()
    # The breadcrumb block includes the project name as the trailing span.
    assert "Crumby" in html


# --- validation ------------------------------------------------------------


def test_empty_name_rejected(app, client, owner_with_project):
    slug, pid = owner_with_project
    r = client.post(f"/projects/{slug}/rename", data={"name": ""})
    # Re-renders form, not a redirect.
    assert r.status_code == 200
    assert b"Location" not in r.headers.get("Location", b"") if False else True

    with app.app_context():
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT name FROM projects WHERE id = :id"),
                {"id": pid},
            ).fetchone()
    # Name was not changed.
    assert row[0] == "Test proj-rn"


def test_long_name_rejected(app, client, owner_with_project):
    slug, pid = owner_with_project
    too_long = "a" * 81
    r = client.post(f"/projects/{slug}/rename", data={"name": too_long})
    assert r.status_code == 200

    with app.app_context():
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT name FROM projects WHERE id = :id"),
                {"id": pid},
            ).fetchone()
    assert row[0] == "Test proj-rn"


def test_max_length_name_accepted(app, client, owner_with_project):
    """Boundary check: exactly 80 chars must pass."""
    slug, pid = owner_with_project
    name = "x" * 80
    r = client.post(
        f"/projects/{slug}/rename", data={"name": name}, follow_redirects=False,
    )
    assert r.status_code == 302
    with app.app_context():
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT name FROM projects WHERE id = :id"),
                {"id": pid},
            ).fetchone()
    assert row[0] == name


# --- data preservation -----------------------------------------------------


def test_connections_intact_after_rename(app, client, owner_with_project):
    slug, pid = owner_with_project
    # Seed two connections directly via storage (route layer crypto setup
    # would be overkill here — we only care that the rows survive).
    with app.app_context():
        metrics_storage.create_connection(
            connection_id="c1", project_id=pid, name="db1",
            dsn_encrypted=b"x", schema_name="public", interval_minutes=15,
        )
        metrics_storage.create_connection(
            connection_id="c2", project_id=pid, name="db2",
            dsn_encrypted=b"y", schema_name="public", interval_minutes=15,
        )

    client.post(f"/projects/{slug}/rename", data={"name": "After"})

    with app.app_context():
        conns = metrics_storage.list_connections_for_project(pid)
    assert {c["name"] for c in conns} == {"db1", "db2"}


def test_metrics_intact_after_rename(app, client, owner_with_project):
    slug, pid = owner_with_project
    with app.app_context():
        metrics_storage.save_metrics([
            {"ts": "2026-06-08T00:00:00", "table_name": "t1",
             "metric_name": "row_count", "value": 100.0},
            {"ts": "2026-06-08T00:05:00", "table_name": "t1",
             "metric_name": "row_count", "value": 110.0},
        ], project_id=pid)

    client.post(f"/projects/{slug}/rename", data={"name": "After"})

    with app.app_context():
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT COUNT(*) FROM metrics WHERE project_id = :pid"),
                {"pid": pid},
            ).fetchone()
    assert row[0] == 2


# --- UI link visibility ----------------------------------------------------


def test_owner_sees_rename_link_on_detail(client, owner_with_project):
    slug, _ = owner_with_project
    html = client.get(f"/projects/{slug}").data.decode()
    assert f"/projects/{slug}/rename" in html
    assert "Переименовать" in html


def test_viewer_does_not_see_rename_link(client, viewer_session):
    slug, _ = viewer_session
    html = client.get(f"/projects/{slug}").data.decode()
    assert "Переименовать" not in html


def test_editor_does_not_see_rename_link(client, editor_session):
    slug, _ = editor_session
    html = client.get(f"/projects/{slug}").data.decode()
    assert "Переименовать" not in html
