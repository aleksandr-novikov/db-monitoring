"""Shared project access tests (#172).

Covers acceptance:
- User A creates project → owner row автоматом в project_members
- User B added via add_project_member → видит проект в своём списке
- User B видит project detail и connections
- User C без membership получает 404
- Owner row защищён: нельзя удалить через remove_project_member
- Non-owner не может удалить проект (route 403)
- Backfill миграция вставляет owner row для legacy projects
"""

from __future__ import annotations

import uuid

import pytest
from cryptography.fernet import Fernet

from app import crypto


@pytest.fixture(autouse=True)
def _fernet_key(monkeypatch):
    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    crypto.reset_for_tests()


@pytest.fixture
def storage(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.config import settings as cfg

    db_path = tmp_path / "shared.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    return ms


def _user(storage, email: str) -> str:
    uid = uuid.uuid4().hex
    storage.create_user(user_id=uid, email=email, password_hash="x")
    return uid


# ── create_project auto-adds owner row ─────────────────────────────────────


def test_create_project_inserts_owner_membership(storage):
    user_id = _user(storage, "owner@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_id,
        name="P", slug="my-project",
    )
    assert storage.get_member_role(project["id"], user_id) == "owner"


def test_list_projects_for_user_includes_self_owned(storage):
    user_id = _user(storage, "owner@example.com")
    storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_id,
        name="P", slug="my-project",
    )
    projects = storage.list_projects_for_user(user_id)
    assert len(projects) == 1
    assert projects[0]["slug"] == "my-project"
    assert projects[0]["role"] == "owner"


# ── add_project_member + shared listing ────────────────────────────────────


def test_add_member_makes_project_visible_to_target(storage):
    """User A создает → User B added → User B видит проект в списке."""
    a_id = _user(storage, "a@example.com")
    b_id = _user(storage, "b@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id,
        name="Retail", slug="retail",
    )
    storage.add_project_member(project["id"], b_id, role="viewer")

    b_projects = storage.list_projects_for_user(b_id)
    assert len(b_projects) == 1
    assert b_projects[0]["slug"] == "retail"
    assert b_projects[0]["role"] == "viewer"


def test_user_without_membership_does_not_see_project(storage):
    """User C без membership → пустой list, 404 на slug lookup."""
    a_id = _user(storage, "a@example.com")
    c_id = _user(storage, "c@example.com")
    storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id,
        name="Retail", slug="retail",
    )

    assert storage.list_projects_for_user(c_id) == []
    assert storage.get_project_by_slug(c_id, "retail") is None


def test_get_project_by_slug_finds_shared_project(storage):
    """Shared lookup: User B нашёл проект A по slug несмотря на то что
    у B этого slug в собственных проектах нет."""
    a_id = _user(storage, "a@example.com")
    b_id = _user(storage, "b@example.com")
    a_project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id,
        name="Shared", slug="shared-slug",
    )
    storage.add_project_member(a_project["id"], b_id, role="editor")

    found = storage.get_project_by_slug(b_id, "shared-slug")
    assert found is not None
    assert found["id"] == a_project["id"]


def test_get_project_by_slug_prefers_owned_when_collision(storage):
    """User B owns 'default' AND was added to A's 'default' — owned wins."""
    a_id = _user(storage, "a@example.com")
    b_id = _user(storage, "b@example.com")
    a_project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id,
        name="A's default", slug="default",
    )
    b_project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=b_id,
        name="B's default", slug="default",
    )
    storage.add_project_member(a_project["id"], b_id, role="viewer")

    # B looks up 'default' → должен получить свой, не A's.
    found = storage.get_project_by_slug(b_id, "default")
    assert found is not None
    assert found["id"] == b_project["id"]


def test_get_project_by_id_membership_aware(storage):
    """get_project_by_id (user, pid) проходит для shared, отказывает для outsider."""
    a_id = _user(storage, "a@example.com")
    b_id = _user(storage, "b@example.com")
    c_id = _user(storage, "c@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id,
        name="Shared", slug="shared",
    )
    storage.add_project_member(project["id"], b_id, role="editor")

    assert storage.get_project_by_id(a_id, project["id"]) is not None
    assert storage.get_project_by_id(b_id, project["id"]) is not None
    assert storage.get_project_by_id(c_id, project["id"]) is None


# ── role validation + safety guards ────────────────────────────────────────


def test_add_member_rejects_invalid_role(storage):
    user_id = _user(storage, "owner@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=user_id,
        name="P", slug="p",
    )
    target = _user(storage, "target@example.com")
    with pytest.raises(storage.InvalidMemberRole):
        storage.add_project_member(project["id"], target, role="admin")


def test_add_member_cannot_downgrade_owner(storage):
    """Owner row защищён: попытка перевести owner → editor отклоняется."""
    a_id = _user(storage, "owner@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id, name="P", slug="p",
    )
    with pytest.raises(storage.InvalidMemberRole):
        storage.add_project_member(project["id"], a_id, role="editor")


def test_remove_member_cannot_remove_owner(storage):
    a_id = _user(storage, "owner@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id, name="P", slug="p",
    )
    with pytest.raises(storage.InvalidMemberRole):
        storage.remove_project_member(project["id"], a_id)


def test_remove_member_returns_false_if_absent(storage):
    """Идемпотентность remove: повторный/несуществующий → False, не Error."""
    a_id = _user(storage, "owner@example.com")
    other = _user(storage, "other@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id, name="P", slug="p",
    )
    assert storage.remove_project_member(project["id"], other) is False


def test_add_member_upserts_role_change(storage):
    """Повторный add с другим role → UPSERT обновляет role."""
    a_id = _user(storage, "owner@example.com")
    b_id = _user(storage, "b@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id, name="P", slug="p",
    )
    storage.add_project_member(project["id"], b_id, role="viewer")
    assert storage.get_member_role(project["id"], b_id) == "viewer"
    storage.add_project_member(project["id"], b_id, role="editor")
    assert storage.get_member_role(project["id"], b_id) == "editor"


def test_list_project_members_returns_owner_and_members(storage):
    """list_project_members показывает всех с ролями + email."""
    a_id = _user(storage, "owner@example.com")
    b_id = _user(storage, "editor@example.com")
    project = storage.create_project(
        project_id=uuid.uuid4().hex, user_id=a_id, name="P", slug="p",
    )
    storage.add_project_member(project["id"], b_id, role="editor")
    members = storage.list_project_members(project["id"])
    assert len(members) == 2
    by_role = {m["role"]: m["email"] for m in members}
    assert by_role == {"owner": "owner@example.com", "editor": "editor@example.com"}


# ── Migration backfill для legacy projects ─────────────────────────────────


def test_migration_backfills_owner_row_for_legacy_projects(tmp_path, monkeypatch):
    """Legacy projects (без project_members ряда) после миграции должны
    получить owner-row автоматически. Симулируем: создаём проект, удаляем
    owner-row, реинициализируем engine — повторный _apply_schema должен
    восстановить."""
    from sqlalchemy import text

    import app.metrics_storage as ms
    from app.config import settings as cfg

    db_path = tmp_path / "legacy.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)

    uid = uuid.uuid4().hex
    ms.create_user(user_id=uid, email=f"u-{uid[:6]}@x.io", password_hash="x")
    project = ms.create_project(
        project_id=uuid.uuid4().hex, user_id=uid,
        name="Legacy", slug="legacy",
    )

    # Симулируем legacy state: удалить owner-row напрямую.
    with ms.get_engine().begin() as conn:
        conn.execute(text(
            "DELETE FROM project_members WHERE project_id = :pid"
        ), {"pid": project["id"]})
    assert ms.get_member_role(project["id"], uid) is None

    # Force re-init → _apply_schema → _migrate_existing_schema → backfill.
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)
    ms.get_engine()  # triggers _apply_schema

    assert ms.get_member_role(project["id"], uid) == "owner"


# ── Route-level: User C получает 404, non-owner — 403 на delete ────────────


@pytest.fixture
def app_(tmp_path, monkeypatch):
    import app.metrics_storage as ms
    from app.app import create_app
    from app.config import settings as cfg

    db_path = tmp_path / "route.db"
    monkeypatch.setattr(cfg, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(ms, "_engine", None)
    monkeypatch.setattr(ms, "_initialized", False)

    import app.db
    monkeypatch.setattr(app.db, "list_tables", lambda schema=None: [])

    return create_app({
        "TESTING": True,
        "LOGIN_DISABLED": False,
        "WTF_CSRF_ENABLED": False,
    })


def _register(client, email):
    client.post("/auth/register", data={
        "email": email, "password": "supersecret1", "confirm": "supersecret1",
    })


def _login(client, email):
    client.post("/auth/login", data={
        "email": email, "password": "supersecret1",
    })


def test_outsider_gets_404_on_someone_elses_project(app_):
    """User C логинится, лезет в проект A → 404."""
    c_a = app_.test_client()
    _register(c_a, "a@example.com")  # creates default project
    # Logout A, login C.
    c_a.post("/auth/logout")
    _register(c_a, "c@example.com")
    # C is now logged in. A's project slug is 'default' (auto-created).
    # But C also has their own 'default'. To force testing outsider path,
    # use a unique slug — create one as A first.
    c_a.post("/auth/logout")
    _login(c_a, "a@example.com")
    # Create custom slug as A.
    c_a.post("/projects/new", data={"name": "Secret A", "slug": "secret-a"})
    c_a.post("/auth/logout")

    # Login as C, try to access A's project.
    _login(c_a, "c@example.com")
    resp = c_a.get("/projects/secret-a")
    assert resp.status_code == 404


def test_member_can_view_shared_project(app_):
    """User A creates → adds User B → B logs in → видит проект на /projects/."""
    from app.metrics_storage import (
        add_project_member,
        get_project_by_slug,
        get_user_by_email,
    )
    c = app_.test_client()
    _register(c, "a@example.com")
    c.post("/projects/new", data={"name": "Shared A", "slug": "shared-a"})
    c.post("/auth/logout")
    _register(c, "b@example.com")
    c.post("/auth/logout")
    # Add B as member of A's project.
    a = get_user_by_email("a@example.com")
    b = get_user_by_email("b@example.com")
    project = get_project_by_slug(a["id"], "shared-a")
    add_project_member(project["id"], b["id"], role="viewer")

    # Login as B and check.
    _login(c, "b@example.com")
    list_resp = c.get("/projects/")
    assert list_resp.status_code == 200
    assert "Shared A" in list_resp.get_data(as_text=True)

    detail_resp = c.get("/projects/shared-a")
    assert detail_resp.status_code == 200
    assert "Shared A" in detail_resp.get_data(as_text=True)


def test_non_owner_cannot_delete_project_via_route(app_):
    """Editor member видит проект, но POST /delete → 403 (не 404, чтобы
    дать понятную ошибку owner-only действия — отличается от 'not found')."""
    from app.metrics_storage import (
        add_project_member,
        get_project_by_slug,
        get_user_by_email,
    )
    c = app_.test_client()
    _register(c, "a@example.com")
    c.post("/projects/new", data={"name": "Owned A", "slug": "owned-a"})
    c.post("/auth/logout")
    _register(c, "b@example.com")
    c.post("/auth/logout")
    a = get_user_by_email("a@example.com")
    b = get_user_by_email("b@example.com")
    project = get_project_by_slug(a["id"], "owned-a")
    add_project_member(project["id"], b["id"], role="editor")

    _login(c, "b@example.com")
    resp = c.post("/projects/owned-a/delete", follow_redirects=False)
    assert resp.status_code == 403

    # Sanity: проект всё ещё существует у A.
    assert get_project_by_slug(a["id"], "owned-a") is not None


def test_owner_can_still_delete_project(app_):
    """Owner-flow не должен сломаться от #172."""
    from app.metrics_storage import (
        get_project_by_slug,
        get_user_by_email,
    )
    c = app_.test_client()
    _register(c, "a@example.com")
    c.post("/projects/new", data={"name": "Owned A", "slug": "owned-a"})
    resp = c.post("/projects/owned-a/delete", follow_redirects=False)
    assert resp.status_code == 302  # redirect to /projects/

    a = get_user_by_email("a@example.com")
    assert get_project_by_slug(a["id"], "owned-a") is None
