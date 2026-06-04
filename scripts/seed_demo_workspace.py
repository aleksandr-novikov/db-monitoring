"""Create the Demo 3.2 workspace in the monitoring DB.

This script prepares application-level demo entities only: users, projects,
and encrypted DB connections. It does not seed target databases and does not
run collectors/ML warmup. Those steps stay explicit because they depend on
local Docker services being available.

Usage:
    python -m scripts.seed_demo_workspace
    python -m scripts.seed_demo_workspace --reset-password
    python -m scripts.seed_demo_workspace --password demo12345
    python -m scripts.seed_demo_workspace --postgres-dsn postgresql://...
"""

from __future__ import annotations

import argparse
import uuid
from dataclasses import dataclass
from urllib.parse import urlencode

from werkzeug.security import generate_password_hash

from app import crypto, metrics_storage
from app.config import settings
from app.metrics_storage import add_project_member

DEFAULT_PASSWORD = "demo12345"
DEFAULT_CLICKHOUSE_DSN = "clickhouse+native://default@localhost:19000/demo"


def default_iceberg_dsn() -> str:
    params = urlencode({
        "warehouse": "s3://iceberg-smoke/warehouse",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
    })
    return f"iceberg+rest://localhost:8181?{params}"


@dataclass(frozen=True)
class DemoConnectionSpec:
    name: str
    dsn: str
    schema_name: str
    interval_minutes: int = 15
    is_active: bool = True


@dataclass(frozen=True)
class DemoProjectSpec:
    user_email: str
    name: str
    slug: str
    connection: DemoConnectionSpec


def ensure_user(email: str, password: str, *, reset_password: bool = False) -> dict:
    email = email.strip().lower()
    existing = metrics_storage.get_user_by_email(email)
    if existing is not None:
        if reset_password:
            metrics_storage.update_user_password(
                existing["id"],
                generate_password_hash(password),
            )
            updated = metrics_storage.get_user_by_email(email)
            if updated is None:
                msg = f"User disappeared after password reset: {email}"
                raise RuntimeError(msg)
            return updated
        return existing
    return metrics_storage.create_user(
        user_id=uuid.uuid4().hex,
        email=email,
        password_hash=generate_password_hash(password),
    )


def ensure_project(user_id: str, name: str, slug: str) -> dict:
    slug = slug.strip().lower()
    existing = metrics_storage.get_project_by_slug(user_id, slug)
    if existing is not None:
        return existing
    return metrics_storage.create_project(
        project_id=uuid.uuid4().hex,
        user_id=user_id,
        name=name,
        slug=slug,
    )


def ensure_connection(project_id: str, spec: DemoConnectionSpec) -> dict:
    for conn in metrics_storage.list_connections_for_project(project_id):
        if conn["name"] == spec.name:
            return conn
    return metrics_storage.create_connection(
        connection_id=uuid.uuid4().hex,
        project_id=project_id,
        name=spec.name,
        dsn_encrypted=crypto.encrypt_dsn(spec.dsn),
        schema_name=spec.schema_name,
        interval_minutes=spec.interval_minutes,
        is_active=spec.is_active,
    )


def build_project_specs(
    *,
    postgres_dsn: str,
    clickhouse_dsn: str,
    iceberg_dsn: str,
) -> list[DemoProjectSpec]:
    return [
        DemoProjectSpec(
            user_email="demo@dbmonitor.app",
            name="Retail Postgres",
            slug="retail-postgres",
            connection=DemoConnectionSpec(
                name="Local Postgres",
                dsn=postgres_dsn,
                schema_name="public",
            ),
        ),
        DemoProjectSpec(
            user_email="demo@dbmonitor.app",
            name="Events ClickHouse",
            slug="events-clickhouse",
            connection=DemoConnectionSpec(
                name="Local ClickHouse",
                dsn=clickhouse_dsn,
                schema_name="demo",
            ),
        ),
        DemoProjectSpec(
            user_email="lake@dbmonitor.app",
            name="Iceberg Lakehouse",
            slug="iceberg-lakehouse",
            connection=DemoConnectionSpec(
                name="Local Iceberg REST",
                dsn=iceberg_dsn,
                schema_name="smoke_ns",
            ),
        ),
    ]


SHARED_DEMO_EMAIL = "guest@dbmonitor.app"


def seed_demo_workspace(
    *,
    password: str = DEFAULT_PASSWORD,
    reset_password: bool = False,
    postgres_dsn: str | None = None,
    clickhouse_dsn: str = DEFAULT_CLICKHOUSE_DSN,
    iceberg_dsn: str | None = None,
) -> dict:
    postgres_dsn = postgres_dsn or settings.DATABASE_URL
    iceberg_dsn = iceberg_dsn or default_iceberg_dsn()

    users: dict[str, dict] = {}
    projects: dict[str, dict] = {}
    connections: dict[str, dict] = {}

    # #172: SHARED_DEMO_EMAIL добавляется как viewer в retail-postgres.
    # Демонстрирует "User B видит проект User A в своём списке" из acceptance.
    for email in ("demo@dbmonitor.app", "lake@dbmonitor.app", SHARED_DEMO_EMAIL):
        users[email] = ensure_user(
            email,
            password,
            reset_password=reset_password,
        )

    for spec in build_project_specs(
        postgres_dsn=postgres_dsn,
        clickhouse_dsn=clickhouse_dsn,
        iceberg_dsn=iceberg_dsn,
    ):
        user = users[spec.user_email]
        project = ensure_project(user["id"], spec.name, spec.slug)
        connection = ensure_connection(project["id"], spec.connection)
        projects[spec.slug] = project
        connections[spec.slug] = connection

    # #172: guest@dbmonitor.app — viewer на retail-postgres (проект demo@).
    # Идемпотентно: add_project_member через UPSERT — повторный вызов
    # просто оставляет существующий role.
    try:
        retail = projects["retail-postgres"]
        guest_user = users[SHARED_DEMO_EMAIL]
        add_project_member(retail["id"], guest_user["id"], role="viewer")
    except KeyError:
        # На случай если build_project_specs убрал retail-postgres из spec'а
        # — silently skip, не блокируем seed остальных проектов.
        pass

    return {
        "password": password,
        "users": users,
        "projects": projects,
        "connections": connections,
    }


def _print_summary(result: dict) -> None:
    print("Demo users:")
    for email in ("demo@dbmonitor.app", "lake@dbmonitor.app"):
        print(f"  {email} / {result['password']}")

    project_env_names = {
        "retail-postgres": "RETAIL_POSTGRES",
        "events-clickhouse": "CLICKHOUSE",
        "iceberg-lakehouse": "ICEBERG",
    }
    print("\nProjects and connections:")
    for slug, prefix in project_env_names.items():
        project = result["projects"][slug]
        connection = result["connections"][slug]
        print(f"  {project['name']}:")
        print(f"    {prefix}_PROJECT_ID={project['id']}")
        print(f"    {prefix}_CONNECTION_ID={connection['id']}")

    retail = result["projects"]["retail-postgres"]
    retail_conn = result["connections"]["retail-postgres"]
    print("\nNext Postgres demo commands:")
    print(
        "  python -m scripts.seed_metrics_db --reset "
        f"--project-id {retail['id']}"
    )
    print(f"  python -m scripts.warmup_ml --project-id {retail['id']}")
    print(
        "  python -m scripts.live_demo "
        f"--project-id {retail['id']} "
        f"--connection-id {retail_conn['id']} "
        "--ticks 20 --interval 5 --incident-at 8 --changepoints"
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create Demo 3.2 users, projects, and encrypted connections.",
    )
    parser.add_argument("--password", default=DEFAULT_PASSWORD)
    parser.add_argument(
        "--reset-password",
        action="store_true",
        help=(
            "Reset demo users' password if they already exist. "
            "Useful for preparing a repeatable demo stand."
        ),
    )
    parser.add_argument(
        "--postgres-dsn",
        default=None,
        help="Postgres DSN for Retail Postgres. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--clickhouse-dsn",
        default=DEFAULT_CLICKHOUSE_DSN,
        help=f"ClickHouse DSN (default: {DEFAULT_CLICKHOUSE_DSN}).",
    )
    parser.add_argument(
        "--iceberg-dsn",
        default=None,
        help="Iceberg DSN. Defaults to local Iceberg REST + MinIO.",
    )
    args = parser.parse_args()

    result = seed_demo_workspace(
        password=args.password,
        reset_password=args.reset_password,
        postgres_dsn=args.postgres_dsn,
        clickhouse_dsn=args.clickhouse_dsn,
        iceberg_dsn=args.iceberg_dsn,
    )
    _print_summary(result)


if __name__ == "__main__":
    main()
