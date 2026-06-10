"""Prepare the Demo Iceberg Lakehouse path.

The script keeps two concerns deliberately separate:

* live Iceberg catalog state: real namespace/tables in local REST + MinIO
  with enough rows that the collector reads realistic numbers;
* dashboard history: synthetic 14-day metrics in the monitoring DB.

Tables and the features they showcase:
  customers   50K   — distribution drift on segment; schema event: country added
  products     5K   — stable baseline, almost no anomalies
  orders     200K   — changepoint (campaign step-up); anomaly on amount null_rate
  order_items 600K  — steady linear growth, good Prophet forecast
  events     500K   — anomaly (tracking outage drop); drift on device_id
  sessions   150K   — changepoint; schema event: utm_source added
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


def _table_specs() -> list[DemoTable]:
    import pyarrow as pa
    from pyiceberg.types import DoubleType, IntegerType, StringType

    return [
        DemoTable(
            name="customers",
            row_count=50_000,
            size_bytes=44_000_000,
            columns=(
                DemoColumn("customer_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("email", StringType(), pa.string(), True, 0.04),
                DemoColumn("segment", StringType(), pa.string(), True, 0.02),
                DemoColumn("lifetime_value", DoubleType(), pa.float64(), True, 0.01),
                DemoColumn("country", StringType(), pa.string(), True, 0.03),
            ),
        ),
        DemoTable(
            name="products",
            row_count=5_000,
            size_bytes=2_000_000,
            columns=(
                DemoColumn("product_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("name", StringType(), pa.string(), False, 0.0),
                DemoColumn("category", StringType(), pa.string(), True, 0.01),
                DemoColumn("price", DoubleType(), pa.float64(), True, 0.005),
                DemoColumn("stock_qty", IntegerType(), pa.int32(), True, 0.02),
            ),
        ),
        DemoTable(
            name="orders",
            row_count=200_000,
            size_bytes=80_000_000,
            columns=(
                DemoColumn("order_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("customer_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("status", StringType(), pa.string(), True, 0.02),
                DemoColumn("amount", DoubleType(), pa.float64(), True, 0.01),
            ),
        ),
        DemoTable(
            name="order_items",
            row_count=600_000,
            size_bytes=180_000_000,
            columns=(
                DemoColumn("item_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("order_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("product_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("quantity", IntegerType(), pa.int32(), False, 0.0),
                DemoColumn("unit_price", DoubleType(), pa.float64(), True, 0.005),
            ),
        ),
        DemoTable(
            name="events",
            row_count=500_000,
            size_bytes=205_000_000,
            columns=(
                DemoColumn("event_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("device_id", StringType(), pa.string(), True, 0.16),
                DemoColumn("ip_address", StringType(), pa.string(), True, 0.11),
                DemoColumn("event_type", StringType(), pa.string(), False, 0.0),
                DemoColumn("value", DoubleType(), pa.float64(), True, 0.03),
            ),
        ),
        DemoTable(
            name="sessions",
            row_count=150_000,
            size_bytes=53_000_000,
            columns=(
                DemoColumn("session_id", StringType(), pa.string(), False, 0.0),
                DemoColumn("user_id", StringType(), pa.string(), True, 0.06),
                DemoColumn("source", StringType(), pa.string(), True, 0.03),
                DemoColumn("utm_source", StringType(), pa.string(), True, 0.08),
                DemoColumn("duration_sec", IntegerType(), pa.int32(), True, 0.02),
            ),
        ),
    ]


# ── Data generators ────────────────────────────────────────────────────────

def _gen_customers(n: int, rng) -> object:
    import pyarrow as pa

    segments = ["retail", "vip", "wholesale", "partner"]
    countries = ["RU", "US", "DE", "GB", "FR", "CN", "BR"]
    null_email = rng.random(n) < 0.04
    null_seg = rng.random(n) < 0.02
    null_ltv = rng.random(n) < 0.01
    null_country = rng.random(n) < 0.03
    ltv = rng.uniform(50, 5000, n)
    return pa.table({
        "customer_id": pa.array([f"cus-{i:06d}" for i in range(n)]),
        "email": pa.array(
            [f"user{i}@example.com" if not null_email[i] else None for i in range(n)]
        ),
        "segment": pa.array(
            [segments[i % len(segments)] if not null_seg[i] else None for i in range(n)]
        ),
        "lifetime_value": pa.array(
            [float(ltv[i]) if not null_ltv[i] else None for i in range(n)],
            type=pa.float64(),
        ),
        "country": pa.array(
            [countries[i % len(countries)] if not null_country[i] else None for i in range(n)]
        ),
    })


def _gen_products(n: int, rng) -> object:
    import pyarrow as pa

    categories = ["Electronics", "Clothing", "Food", "Books", "Home", "Sports"]
    names = ["Widget", "Gadget", "Gizmo", "Doohickey", "Thingamajig"]
    null_cat = rng.random(n) < 0.01
    null_price = rng.random(n) < 0.005
    null_stock = rng.random(n) < 0.02
    prices = rng.uniform(1, 999, n)
    stocks = rng.integers(0, 10000, n)
    return pa.table({
        "product_id": pa.array([f"prd-{i:05d}" for i in range(n)]),
        "name": pa.array([f"{names[i % len(names)]}-{i:04d}" for i in range(n)]),
        "category": pa.array(
            [categories[i % len(categories)] if not null_cat[i] else None for i in range(n)]
        ),
        "price": pa.array(
            [round(float(prices[i]), 2) if not null_price[i] else None for i in range(n)],
            type=pa.float64(),
        ),
        "stock_qty": pa.array(
            [int(stocks[i]) if not null_stock[i] else None for i in range(n)],
            type=pa.int32(),
        ),
    })


def _gen_orders(n: int, rng) -> object:
    import pyarrow as pa

    statuses = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
    null_status = rng.random(n) < 0.02
    null_amount = rng.random(n) < 0.01
    amounts = rng.uniform(10, 2000, n)
    return pa.table({
        "order_id": pa.array([f"ord-{i:07d}" for i in range(n)]),
        "customer_id": pa.array([f"cus-{(i % 50_000):06d}" for i in range(n)]),
        "status": pa.array(
            [statuses[i % len(statuses)] if not null_status[i] else None for i in range(n)]
        ),
        "amount": pa.array(
            [round(float(amounts[i]), 2) if not null_amount[i] else None for i in range(n)],
            type=pa.float64(),
        ),
    })


def _gen_order_items(n: int, rng) -> object:
    import pyarrow as pa

    null_price = rng.random(n) < 0.005
    prices = rng.uniform(1, 500, n)
    qtys = rng.integers(1, 10, n)
    return pa.table({
        "item_id": pa.array([f"itm-{i:08d}" for i in range(n)]),
        "order_id": pa.array([f"ord-{(i // 3):07d}" for i in range(n)]),
        "product_id": pa.array([f"prd-{(i % 5_000):05d}" for i in range(n)]),
        "quantity": pa.array(qtys.tolist(), type=pa.int32()),
        "unit_price": pa.array(
            [round(float(prices[i]), 2) if not null_price[i] else None for i in range(n)],
            type=pa.float64(),
        ),
    })


def _gen_events(n: int, rng) -> object:
    import pyarrow as pa

    event_types = ["login", "view", "add_to_cart", "checkout", "purchase", "error", "logout"]
    null_device = rng.random(n) < 0.16
    null_ip = rng.random(n) < 0.11
    null_value = rng.random(n) < 0.03
    values = rng.uniform(0, 500, n)
    return pa.table({
        "event_id": pa.array([f"evt-{i:08d}" for i in range(n)]),
        "device_id": pa.array(
            [f"dev-{i % 10000:05d}" if not null_device[i] else None for i in range(n)]
        ),
        "ip_address": pa.array(
            [f"10.{i % 256}.{(i // 256) % 256}.{(i // 65536) % 256}"
             if not null_ip[i] else None for i in range(n)]
        ),
        "event_type": pa.array([event_types[i % len(event_types)] for i in range(n)]),
        "value": pa.array(
            [round(float(values[i]), 2) if not null_value[i] else None for i in range(n)],
            type=pa.float64(),
        ),
    })


def _gen_sessions(n: int, rng) -> object:
    import pyarrow as pa

    sources = ["mobile", "web", "partner", "email", "organic"]
    utm_sources = ["google", "facebook", "email", "direct", "partner"]
    null_user = rng.random(n) < 0.06
    null_source = rng.random(n) < 0.03
    null_utm = rng.random(n) < 0.08
    null_dur = rng.random(n) < 0.02
    durations = rng.integers(5, 1800, n)
    return pa.table({
        "session_id": pa.array([f"ses-{i:07d}" for i in range(n)]),
        "user_id": pa.array(
            [f"usr-{i % 50_000:06d}" if not null_user[i] else None for i in range(n)]
        ),
        "source": pa.array(
            [sources[i % len(sources)] if not null_source[i] else None for i in range(n)]
        ),
        "utm_source": pa.array(
            [utm_sources[i % len(utm_sources)] if not null_utm[i] else None for i in range(n)]
        ),
        "duration_sec": pa.array(
            [int(durations[i]) if not null_dur[i] else None for i in range(n)],
            type=pa.int32(),
        ),
    })


_GENERATORS = {
    "customers": _gen_customers,
    "products": _gen_products,
    "orders": _gen_orders,
    "order_items": _gen_order_items,
    "events": _gen_events,
    "sessions": _gen_sessions,
}

_WRITE_BATCH = 100_000  # rows per PyIceberg append call


def _generate_rows(spec: DemoTable) -> object:
    import numpy as np

    rng = np.random.default_rng(42)
    return _GENERATORS[spec.name](spec.row_count, rng)


# ── Catalog helpers ────────────────────────────────────────────────────────

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


def _drop_table(catalog, table_name: str) -> None:
    if _table_exists(catalog, table_name):
        catalog.drop_table((NAMESPACE, table_name), purge_requested=True)
        print(f"  table {NAMESPACE}.{table_name} dropped")


def _create_table(catalog, spec: DemoTable, *, reset: bool = False) -> bool:
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField

    if reset:
        _drop_table(catalog, spec.name)
    elif _table_exists(catalog, spec.name):
        print(f"  table {NAMESPACE}.{spec.name} already exists, skipping")
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
                properties={"write.target-file-size-bytes": "134217728"},
            )
            break
        except Exception:
            if _table_exists(catalog, spec.name):
                print(f"  table {NAMESPACE}.{spec.name} appeared after retry")
                return False
            if attempt == 5:
                raise
            time.sleep(float(attempt))

    print(f"  generating {spec.row_count:,} rows for {spec.name}...", end=" ", flush=True)
    t0 = time.monotonic()
    arrow_data = _generate_rows(spec)

    table = catalog.load_table((NAMESPACE, spec.name))
    # Write in batches to keep memory reasonable for large tables.
    for start in range(0, len(arrow_data), _WRITE_BATCH):
        batch = arrow_data.slice(start, _WRITE_BATCH)
        table.append(batch)

    elapsed = time.monotonic() - t0
    print(f"done ({elapsed:.1f}s)")
    return True


def ensure_iceberg_tables(*, reset: bool = False) -> dict:
    print("[1/5] Iceberg catalog")
    _ensure_bucket()
    catalog = _catalog()
    _ensure_namespace(catalog)
    created = 0
    for spec in _table_specs():
        created += int(_create_table(catalog, spec, reset=reset))
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
    """Re-save demo connection when DSN, schema, or interval changed."""
    from app.metrics_storage import get_connection

    conn = get_connection(project_id, connection_id)
    if conn is None:
        return

    needs_update = False
    try:
        current_dsn = crypto.decrypt_dsn(conn["dsn_encrypted"])
        needs_update = (
            current_dsn != desired_dsn
            or conn["schema_name"] != NAMESPACE
            or int(conn.get("interval_minutes", 0)) != interval_minutes
        )
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
    print(f"  Iceberg connection updated (interval={interval_minutes} min)")


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
    # Linear growth from 58% to 86% of spec.row_count, plus step-ups that
    # create visible changepoints for PELT to detect. Steps sum to ~14% of
    # row_count so the final value equals spec.row_count exactly.
    base = spec.row_count * (0.58 + 0.28 * progress)
    steps = {
        "customers":   ((0.50, 2_000), (0.73, 2_500), (0.96, 2_500)),
        "products":    ((0.60,   700),),
        "orders":      ((0.54, 8_000), (0.76, 10_000), (0.97, 10_000)),
        "order_items": ((0.55, 28_000), (0.77, 28_000), (0.98, 28_000)),
        "events":      ((0.52, 20_000), (0.75, 25_000), (0.97, 25_000)),
        "sessions":    ((0.53, 7_000), (0.76, 7_000), (0.98, 7_000)),
    }.get(spec.name, ())
    base += sum(value for point, value in steps if progress >= point)
    return min(spec.row_count, max(0, round(base)))


def _null_rate(spec: DemoTable, col: DemoColumn, progress: float, tick_idx: int) -> float:
    rate = col.null_rate

    # events.device_id: null_rate starts low then jumps at ~50% (nullable_changed event)
    if spec.name == "events" and col.name == "device_id":
        rate = 0.015 if progress < 0.50 else col.null_rate

    # orders.amount: spike at tick 68 — simulates a payment processor outage
    if spec.name == "orders" and col.name == "amount" and tick_idx == 68:
        rate = min(1.0, rate + 0.12)

    # customers.segment: drift — vip share grows over time (marketing effect)
    if spec.name == "customers" and col.name == "segment":
        rate = col.null_rate * (1 - 0.3 * progress)

    return rate


def _distribution_rows(spec: DemoTable, days: int, end: datetime) -> list[dict]:
    rows: list[dict] = []
    for day in range(days):
        ts = end - timedelta(days=days - 1 - day)
        progress = day / (days - 1) if days > 1 else 1.0
        for col in spec.columns:
            if not col.nullable:
                continue

            # events.device_id: ios/android → web/unknown drift
            if spec.name == "events" and col.name == "device_id":
                drift = progress
                buckets = []
                for i, label in enumerate(("ios", "android", "web", "partner", "unknown")):
                    baseline = (0.34, 0.28, 0.22, 0.12, 0.04)[i]
                    target = (0.12, 0.16, 0.20, 0.22, 0.30)[i]
                    weight = baseline + (target - baseline) * drift
                    buckets.append({"value": label, "count": round(weight * 1000)})
                rows.append({
                    "ts": ts,
                    "table_name": spec.name,
                    "metric_name": "column_distribution",
                    "value": float(sum(b["count"] for b in buckets)),
                    "tags": {"column": col.name, "data_type": "string", "buckets": buckets},
                })

            # customers.segment: retail → vip/wholesale shift
            elif spec.name == "customers" and col.name == "segment":
                drift = progress
                buckets = []
                for i, label in enumerate(("retail", "vip", "wholesale", "partner")):
                    baseline = (0.60, 0.20, 0.12, 0.08)[i]
                    target = (0.35, 0.35, 0.20, 0.10)[i]
                    weight = baseline + (target - baseline) * drift
                    buckets.append({"value": label, "count": round(weight * 1000)})
                rows.append({
                    "ts": ts,
                    "table_name": spec.name,
                    "metric_name": "column_distribution",
                    "value": float(sum(b["count"] for b in buckets)),
                    "tags": {"column": col.name, "data_type": "string", "buckets": buckets},
                })

    return rows


def _schema_events(days: int) -> list[dict]:
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    return [
        # events.device_id became nullable mid-period
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
        # customers got a country column
        {
            "ts": end - timedelta(days=days * 0.45),
            "table_name": "customers",
            "change_type": "column_added",
            "column_name": "country",
            "details": {"after": {"name": "country", "type": "string", "nullable": True}},
        },
        # sessions got utm_source column later
        {
            "ts": end - timedelta(days=days * 0.28),
            "table_name": "sessions",
            "change_type": "column_added",
            "column_name": "utm_source",
            "details": {"after": {"name": "utm_source", "type": "string", "nullable": True}},
        },
    ]


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


def seed_iceberg_history(
    project_id: str,
    *,
    days: int = 14,
    interval_minutes: int = 60,
    reset: bool = True,
) -> dict:
    print("[3/5] Iceberg demo history")
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
    reset: bool = True,
) -> dict:
    catalog_result = ensure_iceberg_tables(reset=reset)
    print("[2/5] Demo workspace")
    project_id, connection_id = _get_demo_project_and_connection()
    print("[3/5] Seeding history")
    history_result = seed_iceberg_history(
        project_id,
        days=days,
        interval_minutes=interval_minutes,
        reset=True,
    )
    # Set interval to 1440 min so scheduled collector doesn't overwrite demo data.
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
        "--no-reset",
        action="store_true",
        help="Skip dropping existing Iceberg tables (faster, but may have stale data).",
    )
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
        reset=not args.no_reset,
    )
    print("\nIceberg demo prepared:")
    print(f"  ICEBERG_PROJECT_ID={result['project_id']}")
    print(f"  ICEBERG_CONNECTION_ID={result['connection_id']}")


if __name__ == "__main__":
    main()
