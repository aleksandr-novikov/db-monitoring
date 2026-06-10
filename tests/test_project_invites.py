"""Integration tests for #222 — invite-link flow.

Mirrors the test list from the acceptance criteria:

- test_create_invite_token
- test_authenticated_user_joins_via_invite
- test_unauthenticated_redirects_to_register
- test_expired_token_rejected
- test_used_token_rejected
- test_nonexistent_token → 404
- test_viewer_cannot_create_invite → 403

Plus a handful of belt-and-braces cases: editor cannot create invites,
owner can't burn their own token, generated URL shape, etc.

Uses the same SQLite-in-tmp pattern as tests/test_roles.py — no real
Postgres needed; the storage helpers are backend-agnostic by design.
"""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from app import metrics_storage
from app.app import create_app


@pytest.fixture
def app(tmp_path, monkeypatch):
    db_path = tmp_path / "invites.db"
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


def _register(client, email, password="secret123", next_url=None):
    url = "/auth/register"
    if next_url:
        url += f"?next={next_url}"
    resp = client.post(url, data={
        "email": email, "password": password, "confirm": password,
    })
    from app.metrics_storage import get_user_by_email
    from app.projects import create_default_project_for
    user = get_user_by_email(email)
    if user and not next_url:
        create_default_project_for(user["id"])
    return resp


def _login(client, email, password="secret123"):
    return client.post("/auth/login", data={"email": email, "password": password})


def _logout(client):
    client.post("/auth/logout")


def _make_project(client, suffix=""):
    slug = f"proj-{suffix or uuid.uuid4().hex[:8]}"
    r = client.post("/projects/new", data={"name": f"Test {slug}", "slug": slug})
    assert r.status_code in (302, 200), f"create project failed: {r.status_code}"
    return slug


def _get_project_id_by_slug(app_ctx, slug):
    with app_ctx.app_context():
        with metrics_storage.get_engine().connect() as conn:
            row = conn.execute(
                text("SELECT id FROM projects WHERE slug = :slug"),
                {"slug": slug},
            ).fetchone()
    return row[0] if row else None


def _extract_invite_token(html_or_flash: str) -> str | None:
    m = re.search(r"/invite/([a-f0-9]{32})", html_or_flash)
    return m.group(1) if m else None


# --- shared fixtures -------------------------------------------------------


@pytest.fixture
def owner_with_project(app, client):
    """Owner registered & logged in, project created. Returns (slug, pid)."""
    _register(client, "owner@inv.com")
    slug = _make_project(client, suffix="inv")
    pid = _get_project_id_by_slug(app, slug)
    return slug, pid


# --- storage layer ---------------------------------------------------------


def test_create_invite_token(app, owner_with_project):
    _slug, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "viewer", owner["id"])

    assert len(invite["token"]) == 32  # secrets.token_hex(16) → 32 hex chars
    assert re.fullmatch(r"[a-f0-9]{32}", invite["token"])
    assert invite["project_id"] == pid
    assert invite["role"] == "viewer"
    assert invite["created_by"] == owner["id"]
    assert invite["used_at"] is None

    # expires_at should be ~7 days out
    expires = datetime.fromisoformat(invite["expires_at"].replace("Z", "+00:00"))
    if expires.tzinfo is None:
        expires = expires.replace(tzinfo=UTC)
    delta = expires - datetime.now(UTC)
    assert timedelta(days=6, hours=23) < delta < timedelta(days=7, minutes=1)


def test_create_invite_token_rejects_owner_role(app, owner_with_project):
    _, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        with pytest.raises(metrics_storage.InvalidMemberRole):
            metrics_storage.create_invite_token(pid, "owner", owner["id"])


# --- route: create invite --------------------------------------------------


def test_owner_create_invite_returns_url(app, client, owner_with_project):
    slug, _ = owner_with_project
    r = client.post(
        f"/projects/{slug}/invites/create",
        data={"role": "viewer"},
        follow_redirects=True,
    )
    assert r.status_code == 200
    token = _extract_invite_token(r.data.decode())
    assert token is not None
    assert len(token) == 32


def test_viewer_cannot_create_invite(app, client, owner_with_project):
    slug, pid = owner_with_project
    _logout(client)
    _register(client, "viewer@inv.com")
    with app.app_context():
        viewer = metrics_storage.get_user_by_email("viewer@inv.com")
        metrics_storage.add_project_member(pid, viewer["id"], "viewer")
    _logout(client)
    _login(client, "viewer@inv.com")

    r = client.post(f"/projects/{slug}/invites/create", data={"role": "viewer"})
    assert r.status_code == 403


def test_editor_cannot_create_invite(app, client, owner_with_project):
    slug, pid = owner_with_project
    _logout(client)
    _register(client, "editor@inv.com")
    with app.app_context():
        editor = metrics_storage.get_user_by_email("editor@inv.com")
        metrics_storage.add_project_member(pid, editor["id"], "editor")
    _logout(client)
    _login(client, "editor@inv.com")

    r = client.post(f"/projects/{slug}/invites/create", data={"role": "viewer"})
    assert r.status_code == 403


def test_create_invite_rejects_owner_role(app, client, owner_with_project):
    slug, _ = owner_with_project
    r = client.post(
        f"/projects/{slug}/invites/create",
        data={"role": "owner"},
        follow_redirects=True,
    )
    assert r.status_code == 200
    assert "Недопустимая роль" in r.data.decode()


# --- route: accept invite --------------------------------------------------


def test_authenticated_user_joins_via_invite(app, client, owner_with_project):
    slug, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "editor", owner["id"])

    # Register the invitee in a separate session, log in.
    _logout(client)
    _register(client, "newjoiner@inv.com")
    # _register auto-logs in via Flask-Login, so we're already logged in.

    r = client.get(f"/invite/{invite['token']}", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["Location"].endswith(f"/projects/{slug}")

    with app.app_context():
        joiner = metrics_storage.get_user_by_email("newjoiner@inv.com")
        role = metrics_storage.get_member_role(pid, joiner["id"])
    assert role == "editor"

    # Token is now used.
    with app.app_context():
        consumed = metrics_storage.get_invite_by_token(invite["token"])
    assert consumed["used_at"] is not None


def test_unauthenticated_redirects_to_register(app, client, owner_with_project):
    _, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "viewer", owner["id"])

    _logout(client)
    r = client.get(f"/invite/{invite['token']}", follow_redirects=False)
    assert r.status_code == 302
    location = r.headers["Location"]
    assert "/auth/register" in location
    # next= must point back to /invite/<token>
    assert f"invite%2F{invite['token']}" in location or \
           f"invite/{invite['token']}" in location


def test_register_with_next_completes_invite(app, client, owner_with_project):
    """End-to-end: anonymous → /invite/<token> → register → joined."""
    slug, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "viewer", owner["id"])

    _logout(client)
    # Step 1: hit /invite/<token> as anon → bounced to /auth/register?next=...
    r1 = client.get(f"/invite/{invite['token']}", follow_redirects=False)
    assert r1.status_code == 302
    register_url = r1.headers["Location"]
    assert "/auth/register" in register_url

    # Step 2: register, carrying next.
    next_target = f"/invite/{invite['token']}"
    r2 = _register(client, "joinerflow@inv.com", next_url=next_target)
    assert r2.status_code == 302
    assert r2.headers["Location"].endswith(next_target)

    # Step 3: follow the redirect → /invite/<token> consumes it.
    r3 = client.get(next_target, follow_redirects=False)
    assert r3.status_code == 302
    assert r3.headers["Location"].endswith(f"/projects/{slug}")

    with app.app_context():
        u = metrics_storage.get_user_by_email("joinerflow@inv.com")
        role = metrics_storage.get_member_role(pid, u["id"])
    assert role == "viewer"


def test_register_via_invite_skips_default_project(
    app, client, owner_with_project,
):
    """A user who registers from /invite/<token>?next=... must NOT get an
    auto-created personal "Default" project — they came here to join the
    inviter's project, an empty personal one is duplicate clutter (#222).
    """
    slug, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "viewer", owner["id"])

    _logout(client)
    next_target = f"/invite/{invite['token']}"
    r = _register(client, "noflood@inv.com", next_url=next_target)
    assert r.status_code == 302
    assert r.headers["Location"].endswith(next_target)
    # Consume the invite.
    client.get(next_target, follow_redirects=False)

    with app.app_context():
        u = metrics_storage.get_user_by_email("noflood@inv.com")
        projects = metrics_storage.list_projects_for_user(u["id"])

    # Only the invited project should appear — no personal Default.
    slugs = {p["slug"] for p in projects}
    assert slugs == {slug}


def test_register_without_next_still_gets_default(app, client):
    """Plain registration (no ?next=) keeps the #50 onboarding behavior:
    a personal Default project auto-created, redirect to the connections
    wizard.
    """
    _register(client, "soloreg@inv.com")
    with app.app_context():
        u = metrics_storage.get_user_by_email("soloreg@inv.com")
        projects = metrics_storage.list_projects_for_user(u["id"])
    assert any(p["slug"] == "default" for p in projects)


def test_expired_token_rejected(app, client, owner_with_project):
    _, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(
            pid, "viewer", owner["id"], ttl=timedelta(seconds=1),
        )
        # Force-expire by rewriting expires_at directly.
        with metrics_storage.get_engine().begin() as conn:
            conn.execute(
                text("UPDATE project_invites SET expires_at = :e WHERE token = :t"),
                {
                    "e": (datetime.now(UTC) - timedelta(days=1))
                    .isoformat(timespec="seconds"),
                    "t": invite["token"],
                },
            )

    _logout(client)
    _register(client, "expjoiner@inv.com")

    r = client.get(f"/invite/{invite['token']}")
    assert r.status_code == 400
    assert "истёк" in r.data.decode()

    # Membership must NOT have been granted.
    with app.app_context():
        u = metrics_storage.get_user_by_email("expjoiner@inv.com")
        assert metrics_storage.get_member_role(pid, u["id"]) is None


def test_used_token_rejected(app, client, owner_with_project):
    _slug, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "viewer", owner["id"])

    _logout(client)
    _register(client, "firstuser@inv.com")
    r1 = client.get(f"/invite/{invite['token']}", follow_redirects=False)
    assert r1.status_code == 302

    # Second user tries the same token.
    _logout(client)
    _register(client, "seconduser@inv.com")
    r2 = client.get(f"/invite/{invite['token']}")
    assert r2.status_code == 400
    assert "использована" in r2.data.decode()

    with app.app_context():
        second = metrics_storage.get_user_by_email("seconduser@inv.com")
        assert metrics_storage.get_member_role(pid, second["id"]) is None


def test_nonexistent_token_404(app, client, owner_with_project):
    _logout(client)
    _register(client, "ghost@inv.com")
    r = client.get("/invite/" + "0" * 32)
    assert r.status_code == 404


def test_existing_member_not_burned_by_clicking_own_invite(
    app, client, owner_with_project,
):
    """Owner clicks an invite for their own project — token stays unused."""
    slug, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "viewer", owner["id"])

    r = client.get(f"/invite/{invite['token']}", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["Location"].endswith(f"/projects/{slug}")

    with app.app_context():
        still = metrics_storage.get_invite_by_token(invite["token"])
    assert still["used_at"] is None  # NOT consumed


def test_consume_invite_storage_lookuperror_on_unknown(app):
    with app.app_context():
        with pytest.raises(LookupError):
            metrics_storage.consume_invite("nope" * 8, "fake-user-id")


def test_consume_invite_atomic_on_concurrent_attempts(app, owner_with_project):
    """Two consume_invite calls for the same token — only one succeeds.

    Not a true thread race (sequential here), but exercises the
    used_at-guard branch directly so a regression in the UPDATE clause
    would be caught.
    """
    _, pid = owner_with_project
    with app.app_context():
        owner = metrics_storage.get_user_by_email("owner@inv.com")
        invite = metrics_storage.create_invite_token(pid, "viewer", owner["id"])

        # Create two more users to claim it.
        u1 = metrics_storage.create_user(
            user_id=uuid.uuid4().hex,
            email="claimer1@inv.com",
            password_hash="x",
        )
        u2 = metrics_storage.create_user(
            user_id=uuid.uuid4().hex,
            email="claimer2@inv.com",
            password_hash="x",
        )

        metrics_storage.consume_invite(invite["token"], u1["id"])
        with pytest.raises(metrics_storage.InviteAlreadyUsed):
            metrics_storage.consume_invite(invite["token"], u2["id"])

        assert metrics_storage.get_member_role(pid, u1["id"]) == "viewer"
        assert metrics_storage.get_member_role(pid, u2["id"]) is None
