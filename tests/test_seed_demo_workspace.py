from __future__ import annotations

from cryptography.fernet import Fernet
from werkzeug.security import check_password_hash, generate_password_hash


def _setup_storage(tmp_path, monkeypatch):
    db_path = tmp_path / "demo_workspace.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    from app import crypto

    crypto.reset_for_tests()
    return storage


def test_seed_demo_workspace_creates_users_projects_connections(tmp_path, monkeypatch):
    storage = _setup_storage(tmp_path, monkeypatch)

    from app import crypto
    from scripts.seed_demo_workspace import seed_demo_workspace

    result = seed_demo_workspace(
        password="demo12345",
        postgres_dsn="postgresql://postgres:dev@localhost:5432/monitor",
        clickhouse_dsn="clickhouse+native://default@localhost:19000/demo",
        iceberg_dsn="iceberg+rest://localhost:8181?warehouse=s3://w",
    )

    demo = storage.get_user_by_email("demo@dbmonitor.app")
    lake = storage.get_user_by_email("lake@dbmonitor.app")
    assert demo is not None
    assert lake is not None

    demo_projects = storage.list_projects_for_user(demo["id"])
    lake_projects = storage.list_projects_for_user(lake["id"])
    assert {p["slug"] for p in demo_projects} == {
        "retail-postgres",
        "events-clickhouse",
    }
    assert {p["slug"] for p in lake_projects} == {"iceberg-lakehouse"}

    retail = result["projects"]["retail-postgres"]
    retail_connections = storage.list_connections_for_project(retail["id"])
    assert len(retail_connections) == 1
    encrypted = retail_connections[0]["dsn_encrypted"]
    assert b"postgres:dev" not in encrypted
    assert crypto.decrypt_dsn(encrypted) == (
        "postgresql://postgres:dev@localhost:5432/monitor"
    )


def test_seed_demo_workspace_is_idempotent(tmp_path, monkeypatch):
    storage = _setup_storage(tmp_path, monkeypatch)

    from scripts.seed_demo_workspace import seed_demo_workspace

    kwargs = {
        "password": "demo12345",
        "postgres_dsn": "postgresql://postgres:dev@localhost:5432/monitor",
        "clickhouse_dsn": "clickhouse+native://default@localhost:19000/demo",
        "iceberg_dsn": "iceberg+rest://localhost:8181?warehouse=s3://w",
    }
    first = seed_demo_workspace(**kwargs)
    second = seed_demo_workspace(**kwargs)

    assert first["users"]["demo@dbmonitor.app"]["id"] == (
        second["users"]["demo@dbmonitor.app"]["id"]
    )
    assert first["projects"]["retail-postgres"]["id"] == (
        second["projects"]["retail-postgres"]["id"]
    )
    assert first["connections"]["retail-postgres"]["id"] == (
        second["connections"]["retail-postgres"]["id"]
    )

    demo = storage.get_user_by_email("demo@dbmonitor.app")
    demo_projects = storage.list_projects_for_user(demo["id"])
    assert len(demo_projects) == 2
    for project in demo_projects:
        assert len(storage.list_connections_for_project(project["id"])) == 1


def test_seed_demo_workspace_can_reset_existing_demo_password(tmp_path, monkeypatch):
    storage = _setup_storage(tmp_path, monkeypatch)

    from scripts.seed_demo_workspace import seed_demo_workspace

    storage.create_user(
        user_id="existing-demo",
        email="demo@dbmonitor.app",
        password_hash=generate_password_hash("old-password"),
    )

    kwargs = {
        "password": "demo12345",
        "postgres_dsn": "postgresql://postgres:dev@localhost:5432/monitor",
        "clickhouse_dsn": "clickhouse+native://default@localhost:19000/demo",
        "iceberg_dsn": "iceberg+rest://localhost:8181?warehouse=s3://w",
    }
    seed_demo_workspace(**kwargs)
    demo = storage.get_user_by_email("demo@dbmonitor.app")
    assert check_password_hash(demo["password_hash"], "old-password")

    seed_demo_workspace(**kwargs, reset_password=True)

    demo = storage.get_user_by_email("demo@dbmonitor.app")
    assert demo["id"] == "existing-demo"
    assert check_password_hash(demo["password_hash"], "demo12345")
