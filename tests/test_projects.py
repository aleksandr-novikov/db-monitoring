"""Projects CRUD + ownership-isolation tests for #50.

Covers the acceptance criteria from the issue:
- Юзер видит ТОЛЬКО свои проекты (URL-guessing другого юзера → 404)
- При регистрации auto-создаётся "Default" проект
- Switcher хранит current_project_id в сессии
"""

from __future__ import annotations

import pytest

from app.app import create_app


@pytest.fixture
def projects_app(tmp_path, monkeypatch):
    """App with auth enabled (LOGIN_DISABLED=False) and CSRF off for ergonomics.
    Real users, real projects table, in-memory SQLite under tmp_path.
    """
    db_path = tmp_path / "projects.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    app = create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })
    return app


@pytest.fixture
def client(projects_app):
    return projects_app.test_client()


def _register(client, email="u@example.com", password="supersecret1"):
    """Register a user; the post-register session is left authenticated."""
    return client.post("/auth/register", data={
        "email": email, "password": password, "confirm": password,
    })


def _logout(client):
    client.post("/auth/logout")


def _login(client, email="u@example.com", password="supersecret1"):
    return client.post("/auth/login", data={"email": email, "password": password})


# --- Auto-create Default on register ---------------------------------------


def test_register_creates_default_project(client):
    _register(client, "default@example.com")
    # Hit /projects — Default should be the only entry.
    resp = client.get("/projects")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "Default" in body
    assert "default" in body  # slug


def test_default_project_is_set_as_current(client):
    _register(client, "current@example.com")
    # Dashboard renders the switcher; it must show "Default".
    resp = client.get("/dashboard/")
    assert b"Default" in resp.data


# --- Create / list / detail ------------------------------------------------


def test_create_project_redirects_to_detail(client):
    _register(client)
    resp = client.post("/projects/new", data={
        "name": "Production", "slug": "prod",
    }, follow_redirects=False)
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/projects/prod")


def test_detail_page_shows_placeholder_for_connections(client):
    _register(client)
    client.post("/projects/new", data={"name": "Prod", "slug": "prod"})
    resp = client.get("/projects/prod")
    assert resp.status_code == 200
    assert "В проекте пока нет подключений" in resp.get_data(as_text=True)


def test_list_shows_default_plus_new_project(client):
    _register(client)
    client.post("/projects/new", data={"name": "Staging", "slug": "staging"})
    resp = client.get("/projects")
    body = resp.get_data(as_text=True)
    assert "Default" in body
    assert "Staging" in body


# --- Slug rules ------------------------------------------------------------


def test_duplicate_slug_per_same_user_returns_409(client):
    _register(client, "dup@example.com")
    # Default already takes "default" — try to create another with same slug.
    resp = client.post("/projects/new", data={"name": "Other", "slug": "default"})
    assert resp.status_code == 409
    assert "уже существует" in resp.get_data(as_text=True)


def test_two_different_users_can_share_a_slug(client):
    _register(client, "user1@example.com")
    client.post("/projects/new", data={"name": "Shared", "slug": "shared"})
    _logout(client)

    _register(client, "user2@example.com")
    resp = client.post("/projects/new", data={"name": "Shared", "slug": "shared"})
    # Should succeed — slug uniqueness is per-user.
    assert resp.status_code == 302


def test_invalid_slug_rejected(client):
    _register(client)
    bad = ["UPPER", "with spaces", "-leading", "trailing-", "with/slash", "a" * 41]
    for slug in bad:
        resp = client.post("/projects/new", data={"name": "n", "slug": slug})
        # Form re-renders 200 (no project created); 409 would mean uniqueness,
        # which we don't exercise here.
        assert resp.status_code == 200, f"slug {slug!r} should be rejected"


# --- Ownership isolation ---------------------------------------------------


def test_user_cannot_see_another_users_project(client):
    _register(client, "owner@example.com")
    client.post("/projects/new", data={"name": "Secret", "slug": "secret"})
    _logout(client)

    _register(client, "stranger@example.com")
    # Guess the other user's slug.
    resp = client.get("/projects/secret")
    assert resp.status_code == 404


def test_user_cannot_delete_another_users_project(client):
    _register(client, "victim@example.com")
    client.post("/projects/new", data={"name": "Mine", "slug": "mine"})
    _logout(client)

    _register(client, "attacker@example.com")
    resp = client.post("/projects/mine/delete")
    assert resp.status_code == 404

    # And the victim's project must still exist after re-login.
    _logout(client)
    _login(client, "victim@example.com", "supersecret1")
    resp = client.get("/projects/mine")
    assert resp.status_code == 200


def test_user_cannot_switch_to_another_users_project(client):
    _register(client, "a@example.com")
    client.post("/projects/new", data={"name": "Theirs", "slug": "theirs"})
    _logout(client)

    _register(client, "b@example.com")
    resp = client.post("/projects/theirs/switch")
    assert resp.status_code == 404


# --- Delete + switcher state -----------------------------------------------


def test_delete_clears_current_if_it_was_the_current_project(client):
    _register(client)
    # Create a second project; switch to it; delete it.
    client.post("/projects/new", data={"name": "Other", "slug": "other"})
    # `new_project` auto-switches; deleting it should fall back gracefully.
    resp = client.post("/projects/other/delete", follow_redirects=True)
    assert resp.status_code == 200
    # The remaining "Default" project should now be picked up as current.
    assert b"Default" in resp.data


def test_switch_honours_safe_next(client):
    """Switching with ?next=/projects redirects there (same-host relative path)."""
    _register(client)
    client.post("/projects/new", data={"name": "Production", "slug": "prod"})
    resp = client.post(
        "/projects/default/switch?next=/projects",
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert resp.headers["Location"].endswith("/projects")


def test_switch_rejects_open_redirect_next(client):
    """`?next=https://evil/` must not leak the user off-site."""
    _register(client)
    resp = client.post(
        "/projects/default/switch?next=https://evil.example.com/",
        follow_redirects=False,
    )
    assert resp.status_code == 302
    assert "evil.example.com" not in resp.headers["Location"]


def test_switch_changes_current_project(client):
    _register(client)
    client.post("/projects/new", data={"name": "Production", "slug": "prod"})
    # We're already on Production (new_project auto-switches). Switch back
    # to Default.
    client.post("/projects/default/switch")
    resp = client.get("/dashboard/")
    body = resp.get_data(as_text=True)
    # The switcher's summary shows the current project's name as the first
    # render — Default must appear before Production in the dropdown order
    # since list_projects orders by created_at, but the *current* one is in
    # the <summary>.
    assert "Default" in body


# --- Gating ----------------------------------------------------------------


def test_projects_requires_login(client):
    """Unauthenticated /projects → 302 to /auth/login."""
    resp = client.get("/projects", follow_redirects=False)
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


def test_new_project_requires_login(client):
    resp = client.get("/projects/new", follow_redirects=False)
    assert resp.status_code == 302
    assert "/auth/login" in resp.headers["Location"]


# --- #258: slug routes sync session.current_project_id ---------------------


def _current_project_id(client) -> str | None:
    with client.session_transaction() as sess:
        return sess.get("current_project_id")


def test_detail_route_syncs_current_project_with_url_slug(client):
    """Open /projects/default after switcher is on Production →
    current_project_id flips back to Default."""
    _register(client)
    client.post("/projects/new", data={"name": "Production", "slug": "prod"})
    # new_project auto-switched to "prod". Navigate to /projects/default
    # without using the switch route.
    resp = client.get("/projects/default")
    assert resp.status_code == 200
    # Body shows Default's switcher.
    body = resp.get_data(as_text=True)
    assert "Default" in body
    # Session updated server-side.
    with client.session_transaction() as sess:
        assert sess.get("current_project_id") is not None
    # The switcher (rendered from g.current_project) should now show
    # Default, not Production.
    assert ">Default<" in body


def test_connections_list_route_syncs_current_project_with_url_slug(client):
    _register(client)
    client.post("/projects/new", data={"name": "Production", "slug": "prod"})
    prod_pid = _current_project_id(client)
    # Navigate to /projects/default/connections — should sync back.
    client.get("/projects/default/connections")
    default_pid = _current_project_id(client)
    assert default_pid is not None
    assert default_pid != prod_pid


def test_notifications_route_syncs_current_project_with_url_slug(client):
    _register(client)
    client.post("/projects/new", data={"name": "Production", "slug": "prod"})
    prod_pid = _current_project_id(client)
    client.get("/projects/default/settings/notifications")
    default_pid = _current_project_id(client)
    assert default_pid is not None
    assert default_pid != prod_pid


def test_404_on_stranger_slug_does_not_change_current_project(client):
    """abort(404) fires before the sync — stranger's slug must not leak
    its ID into session.current_project_id."""
    _register(client, "a@example.com")
    client.post("/projects/new", data={"name": "Theirs", "slug": "theirs"})
    _logout(client)

    _register(client, "b@example.com")
    # Normalize: hit /dashboard so load_current_project_into_g seeds b's
    # Default into session (otherwise stale a's id may linger).
    client.get("/dashboard/")
    pid_before = _current_project_id(client)
    assert pid_before is not None

    resp = client.get("/projects/theirs")
    assert resp.status_code == 404
    # Session unchanged — the 404 short-circuited before the sync write.
    assert _current_project_id(client) == pid_before


def test_projects_list_no_longer_renders_make_current_button(client):
    """#258 cleanup: 'Сделать текущим' кнопка убрана. Бейдж «текущий» остаётся."""
    _register(client)
    client.post("/projects/new", data={"name": "Production", "slug": "prod"})
    resp = client.get("/projects")
    body = resp.get_data(as_text=True)
    assert "Сделать текущим" not in body
    assert "текущий" in body  # badge
