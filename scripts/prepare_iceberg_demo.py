"""Prepare the Demo 3.2 Iceberg Lakehouse path.

The script keeps two concerns deliberately separate:

* live Iceberg catalog state: real namespace/tables in local REST + MinIO;
* dashboard history: synthetic 14-day metrics in the monitoring DB.

This gives a repeatable demo without pretending that 14 days passed in the
local Iceberg catalog.
"""

from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

from sqlalchemy import text

from app import crypto
from app.metrics_storage import (
    get_engine as get_monitor_engine,
)
from app.metrics_storage import (
    save_metrics,
    save_schema_events,
)
from collectors.per_project import collect_for_connection
from scripts.seed_demo_workspace import seed_demo_workspace

logger = logging.getLogger(__name__)

BUCKET = "iceberg-smoke"
WAREHOUSE = f"s3://{BUCKET}/warehouse"
NAMESPACE = "lakehouse"
REST_HOST = "localhost:8181"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_USER = "minioadmin"
MINIO_PASSWORD = "minioadmin"


def default_iceberg_dsn() -> str:
    return _iceberg_dsn(REST_HOST, MINIO_ENDPOINT)


def default_app_iceberg_dsn() -> str:
    return _iceberg_dsn("iceberg-rest:8181", "http://minio:9000")


def _iceberg_dsn(rest_host: str, minio_endpoint: str) -> str:
    params = urlencode({
        "warehouse": WAREHOUSE,
        "s3.endpoint": minio_endpoint,
        "s3.access-key-id": MINIO_USER,
        "s3.secret-access-key": MINIO_PASSWORD,
        "s3.path-style-access": "true",
    })
    return f"iceberg+rest://{rest_host}?{params}"


@dataclass(frozen=True)
class DemoColumn:
    name: str
    iceberg_type: object
    arrow_type: object
    nullable: bool
    null_rate: float


@dataclass(frozen=True)
class DemoTable:
    name: str
    row_count: int
    size_bytes: int
    columns: tuple[DemoColumn, ...]
    rows: dict[str, list]


def _table_specs() -> list[DemoTable]:
    import pyarrow as pa
    from pyiceberg.types import DoubleType, IntegerType, StringType

    return [
        DemoTable(
            name="events",
            row_count=4_250_000,
            size_bytes=1_740_800_000,
            columns=(
                DemoColumn("event_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("device_id", StringType(), pa.string(), True, 0.16),
                DemoColumn("ip_address", StringType(), pa.string(), True, 0.11),
                DemoColumn("event_type", StringType(), pa.string(), False, 0.0),
                DemoColumn("value", DoubleType(), pa.float64(), True, 0.03),
            ),
            rows={
                "event_id": ["evt-001", "evt-002", "evt-003", "evt-004", "evt-005"],
                "device_id": ["ios-1", None, "web-3", None, "android-5"],
                "ip_address": ["10.0.0.1", None, "10.0.0.3", "10.0.0.4", None],
                "event_type": ["checkout", "view", "click", "view", "purchase"],
                "value": [12.0, None, 3.5, 9.1, 42.0],
            },
        ),
        DemoTable(
            name="orders",
            row_count=620_000,
            size_bytes=308_400_000,
            columns=(
                DemoColumn("order_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("customer_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("status", StringType(), pa.string(), True, 0.02),
                DemoColumn("amount", DoubleType(), pa.float64(), True, 0.01),
            ),
            rows={
                "order_id": ["ord-001", "ord-002", "ord-003", "ord-004"],
                "customer_id": ["cus-001", "cus-002", "cus-003", "cus-004"],
                "status": ["paid", "paid", None, "refunded"],
                "amount": [120.0, 75.5, 210.0, None],
            },
        ),
        DemoTable(
            name="customers",
            row_count=96_000,
            size_bytes=84_800_000,
            columns=(
                DemoColumn("customer_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("email", StringType(), pa.string(), True, 0.04),
                DemoColumn("segment", StringType(), pa.string(), True, 0.02),
                DemoColumn("lifetime_value", DoubleType(), pa.float64(), True, 0.01),
            ),
            rows={
                "customer_id": ["cus-001", "cus-002", "cus-003", "cus-004"],
                "email": ["a@example.com", None, "c@example.com", "d@example.com"],
                "segment": ["retail", "vip", None, "retail"],
                "lifetime_value": [1500.0, 4200.0, None, 230.0],
            },
        ),
        DemoTable(
            name="sessions",
            row_count=1_380_000,
            size_bytes=512_900_000,
            columns=(
                DemoColumn("session_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("user_id", StringType(), pa.string(), True, 0.06),
                DemoColumn("source", StringType(), pa.string(), True, 0.03),
                DemoColumn("duration_sec", IntegerType(), pa.int32(), True, 0.02),
            ),
            rows={
                "session_id": ["ses-001", "ses-002", "ses-003", "ses-004"],
                "user_id": ["usr-1", None, "usr-3", "usr-4"],
                "source": ["mobile", "web", None, "partner"],
                "duration_sec": [35, 280, None, 42],
            },
        ),
    ]


def _catalog():
    from pyiceberg.catalog.rest import RestCatalog

    return RestCatalog(
        "rest",
        uri=f"http://{REST_HOST}",
        warehouse=WAREHOUSE,
        **{
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_USER,
            "s3.secret-access-key": MINIO_PASSWORD,
            "s3.path-style-access": "true",
        },
    )


def _ensure_bucket() -> None:
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=BUCKET)
        print(f"  bucket {BUCKET!r} created")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
            print(f"  bucket {BUCKET!r} already exists")
            return
        raise


def _ensure_namespace(catalog) -> None:
    from pyiceberg.exceptions import NoSuchNamespaceError

    try:
        catalog.load_namespace_properties(NAMESPACE)
        print(f"  namespace {NAMESPACE!r} already exists")
    except NoSuchNamespaceError:
        catalog.create_namespace(NAMESPACE)
        print(f"  namespace {NAMESPACE!r} created")


def _table_exists(catalog, table_name: str) -> bool:
    from pyiceberg.exceptions import NoSuchTableError

    try:
        catalog.load_table((NAMESPACE, table_name))
        return True
    except NoSuchTableError:
        return False


def _create_table(catalog, spec: DemoTable) -> bool:
    import pyarrow as pa
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    if _table_exists(catalog, spec.name):
        print(f"  table {NAMESPACE}.{spec.name} already exists")
        return False

    fields = [
        NestedField(i + 1, col.name, col.iceberg_type, required=not col.nullable)
        for i, col in enumerate(spec.columns)
    ]
    for attempt in range(1, 6):
        try:
            catalog.create_table(
                identifier=(NAMESPACE, spec.name),
                schema=Schema(*fields),
                location=f"{WAREHOUSE}/{NAMESPACE}/{spec.name}",
                properties={"write.target-file-size-bytes": "536870912"},
            )
            break
        except Exception:
            if _table_exists(catalog, spec.name):
                print(f"  table {NAMESPACE}.{spec.name} appeared after retry")
                return False
            if attempt == 5:
                raise
            time.sleep(float(attempt))
    table = catalog.load_table((NAMESPACE, spec.name))
    arrow_schema = pa.schema([
        pa.field(col.name, col.arrow_type, nullable=col.nullable)
        for col in spec.columns
    ])
    arrow_table = pa.table(
        {
            col.name: pa.array(spec.rows[col.name], type=col.arrow_type)
            for col in spec.columns
        },
        schema=arrow_schema,
    )
    table.append(arrow_table)
    print(f"  table {NAMESPACE}.{spec.name} created")
    return True


def ensure_iceberg_tables() -> dict:
    print("[1/4] Iceberg catalog")
    _ensure_bucket()
    catalog = _catalog()
    _ensure_namespace(catalog)
    created = 0
    for spec in _table_specs():
        created += int(_create_table(catalog, spec))
    return {"tables": len(_table_specs()), "created": created}


def _get_demo_project_and_connection() -> tuple[str, str]:
    result = seed_demo_workspace(
        reset_password=True,
        iceberg_dsn=default_iceberg_dsn(),
    )
    project = result["projects"]["iceberg-lakehouse"]
    connection = result["connections"]["iceberg-lakehouse"]
    _repair_iceberg_connection(project["id"], connection["id"], default_iceberg_dsn())
    return project["id"], connection["id"]


def _repair_iceberg_connection(
    project_id: str,
    connection_id: str,
    desired_dsn: str,
    *,
    interval_minutes: int = 15,
) -> None:
    """Re-save the demo DSN when a local Fernet key changed.

    `seed_demo_workspace` is intentionally idempotent and does not mutate an
    existing connection. For demo preparation we need the stronger guarantee:
    the Iceberg connection must be decryptable in the current environment.
    """
    needs_update = False

    from app.metrics_storage import get_connection

    conn = get_connection(project_id, connection_id)
    if conn is None:
        return
    try:
        current_dsn = crypto.decrypt_dsn(conn["dsn_encrypted"])
        needs_update = current_dsn != desired_dsn or conn["schema_name"] != NAMESPACE
    except crypto.InvalidToken:
        needs_update = True

    if not needs_update:
        return

    with get_monitor_engine().begin() as db_conn:
        db_conn.execute(
            text("""
                UPDATE connections
                SET dsn_encrypted = :dsn,
                    schema_name = :schema,
                    interval_minutes = :interval_minutes,
                    is_active = 1
                WHERE project_id = :project_id AND id = :connection_id
            """),
            {
                "dsn": crypto.encrypt_dsn(desired_dsn),
                "schema": NAMESPACE,
                "interval_minutes": interval_minutes,
                "project_id": project_id,
                "connection_id": connection_id,
            },
        )
    print("  Iceberg connection DSN re-saved for current Fernet key")


def _purge_project_history(project_id: str, table_names: list[str]) -> int:
    scoped_tables = (
        "metrics", "notifications", "anomaly_scores", "changepoints",
        "drift_reports",
    )
    deleted = 0
    with get_monitor_engine().begin() as conn:
        for table_name in scoped_tables:
            result = conn.execute(
                text(f"DELETE FROM {table_name} WHERE project_id = :pid"),
                {"pid": project_id},
            )
            deleted += result.rowcount or 0
        for table in table_names:
            result = conn.execute(
                text("DELETE FROM schema_events WHERE table_name = :table_name"),
                {"table_name": table},
            )
            deleted += result.rowcount or 0
    return deleted


def _timestamps(days: int, interval_minutes: int) -> list[datetime]:
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    step = timedelta(minutes=interval_minutes)
    ticks = max(1, (days * 24 * 60) // interval_minutes)
    return [end - step * (ticks - 1 - i) for i in range(ticks)]


def _row_count(spec: DemoTable, progress: float) -> int:
    base = spec.row_count * (0.58 + 0.28 * progress)
    steps = {
        "events": ((0.52, 680_000), (0.75, 720_000), (0.97, 910_000)),
        "orders": ((0.54, 85_000), (0.76, 95_000), (0.97, 125_000)),
        "customers": ((0.50, 12_000), (0.73, 18_000), (0.96, 21_000)),
        "sessions": ((0.53, 210_000), (0.76, 240_000), (0.98, 320_000)),
    }.get(spec.name, ())
    base += sum(value for point, value in steps if progress >= point)
    return min(spec.row_count, max(0, round(base)))


def _null_rate(spec: DemoTable, col: DemoColumn, progress: float, tick_idx: int) -> float:
    if spec.name == "events" and col.name == "device_id":
        rate = 0.015 if progress < 0.50 else col.null_rate
    else:
        rate = col.null_rate
    if spec.name == "events" and tick_idx == 68:
        rate = min(1.0, rate + 0.18)
    return rate


def _distribution_rows(spec: DemoTable, days: int, end: datetime) -> list[dict]:
    rows: list[dict] = []
    for day in range(days):
        ts = end - timedelta(days=days - 1 - day)
        progress = day / (days - 1) if days > 1 else 1.0
        for col in spec.columns:
            if not col.nullable:
                continue
            drift = 1.0 if spec.name == "events" and col.name == "device_id" else 0.0
            buckets = []
            for i, label in enumerate(("ios", "android", "web", "partner", "unknown")):
                baseline = (0.34, 0.28, 0.22, 0.12, 0.04)[i]
                target = (0.12, 0.16, 0.20, 0.22, 0.30)[i]
                weight = baseline + (target - baseline) * drift * progress
                buckets.append({"value": label, "count": round(weight * 1000)})
            rows.append({
                "ts": ts,
                "table_name": spec.name,
                "metric_name": "column_distribution",
                "value": float(sum(b["count"] for b in buckets)),
                "tags": {
                    "column": col.name,
                    "data_type": "string",
                    "buckets": buckets,
                },
            })
    return rows


def _metric_rows(spec: DemoTable, timestamps: list[datetime]) -> list[dict]:
    rows: list[dict] = []
    avg_row_size = spec.size_bytes / spec.row_count
    for i, ts in enumerate(timestamps):
        progress = i / (len(timestamps) - 1) if len(timestamps) > 1 else 1.0
        rc = _row_count(spec, progress)
        rows.extend([
            {"ts": ts, "table_name": spec.name, "metric_name": "row_count", "value": rc},
            {
                "ts": ts,
                "table_name": spec.name,
                "metric_name": "size_bytes",
                "value": int(rc * avg_row_size),
            },
            {
                "ts": ts,
                "table_name": spec.name,
                "metric_name": "last_modified",
                "value": ts.timestamp(),
            },
        ])
        rates: list[float] = []
        for col in spec.columns:
            if not col.nullable:
                continue
            rate = _null_rate(spec, col, progress, i)
            rates.append(rate)
            rows.append({
                "ts": ts,
                "table_name": spec.name,
                "metric_name": "null_count",
                "value": round(rc * rate),
                "tags": {"column": col.name},
            })
        if rates:
            rows.append({
                "ts": ts,
                "table_name": spec.name,
                "metric_name": "null_rate",
                "value": round(sum(rates) / len(rates), 4),
            })
    return rows


def _schema_events(days: int) -> list[dict]:
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    return [
        {
            "ts": end - timedelta(days=days * 0.50),
            "table_name": "events",
            "change_type": "nullable_changed",
            "column_name": "device_id",
            "details": {
                "before": {"name": "device_id", "type": "string", "nullable": False},
                "after": {"name": "device_id", "type": "string", "nullable": True},
            },
        },
        {
            "ts": end - timedelta(days=days * 0.42),
            "table_name": "sessions",
            "change_type": "column_added",
            "column_name": "source",
            "details": {"after": {"name": "source", "type": "string", "nullable": True}},
        },
    ]


def seed_iceberg_history(
    project_id: str,
    *,
    days: int = 14,
    interval_minutes: int = 60,
    reset: bool = True,
) -> dict:
    print("[3/4] Iceberg demo history")
    specs = _table_specs()
    table_names = [s.name for s in specs]
    deleted = _purge_project_history(project_id, table_names) if reset else 0
    timestamps = _timestamps(days, interval_minutes)
    rows: list[dict] = []
    end = timestamps[-1] if timestamps else datetime.now(UTC)
    for spec in specs:
        rows.extend(_metric_rows(spec, timestamps))
        rows.extend(_distribution_rows(spec, days, end))
    saved = save_metrics(rows, project_id)
    schema_events_saved = save_schema_events(_schema_events(days))
    print(
        f"  saved {saved} metrics, {schema_events_saved} schema-events "
        f"for {len(specs)} tables; deleted {deleted} old rows"
    )
    return {
        "rows": saved,
        "schema_events": schema_events_saved,
        "deleted": deleted,
        "tables": len(specs),
        "ticks": len(timestamps),
    }


def prepare_iceberg_demo(
    days: int = 14,
    interval_minutes: int = 60,
    *,
    warmup_ml: bool = True,
) -> dict:
    catalog_result = ensure_iceberg_tables()
    print("[2/5] Demo workspace + collector")
    project_id, connection_id = _get_demo_project_and_connection()
    collect_for_connection(project_id, connection_id)
    print(f"  collected live Iceberg snapshot for project={project_id}")
    history_result = seed_iceberg_history(
        project_id,
        days=days,
        interval_minutes=interval_minutes,
        reset=True,
    )
    _repair_iceberg_connection(
        project_id,
        connection_id,
        default_app_iceberg_dsn(),
        interval_minutes=1440,
    )
    warmup_result = None
    if warmup_ml:
        print("[4/5] ML warmup")
        from scripts.warmup_ml import main as warmup_main

        warmup_result = warmup_main(project_id=project_id)
    print("[5/5] Demo ready")
    print("  URL: http://localhost:5001/dashboard/")
    print("  Login: lake@dbmonitor.app / demo12345")
    print(f"  PROJECT_ID={project_id}")
    print(f"  CONNECTION_ID={connection_id}")
    return {
        "project_id": project_id,
        "connection_id": connection_id,
        "catalog": catalog_result,
        "history": history_result,
        "warmup": warmup_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare Iceberg Lakehouse demo tables and history.",
    )
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--interval-minutes", type=int, default=60)
    parser.add_argument(
        "--skip-warmup-ml",
        action="store_true",
        help="Only seed Iceberg tables/history; do not run ML warmup.",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    result = prepare_iceberg_demo(
        days=args.days,
        interval_minutes=args.interval_minutes,
        warmup_ml=not args.skip_warmup_ml,
    )
    print("\nIceberg demo prepared:")
    print(f"  ICEBERG_PROJECT_ID={result['project_id']}")
    print(f"  ICEBERG_CONNECTION_ID={result['connection_id']}")


if __name__ == "__main__":
    main()
