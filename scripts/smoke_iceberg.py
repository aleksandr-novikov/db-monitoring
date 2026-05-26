"""Live smoke test for IcebergAdapter (#128).

Verifies the full adapter stack against a real Iceberg REST catalog + MinIO.
All assertions mirror what the scheduled collector would see in production.

Prerequisites — start with ``make iceberg-up``:
  - MinIO on localhost:9000       (MINIO_ROOT_USER/PASSWORD = minioadmin)
  - Iceberg REST on localhost:8181 (warehouse → s3://iceberg-smoke/warehouse)

Usage::

    make smoke-iceberg
    # or directly:
    python -m scripts.smoke_iceberg
"""

from __future__ import annotations

import os
import sys
import time
from urllib.parse import urlencode

# ── Config ────────────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
BUCKET = "iceberg-smoke"
WAREHOUSE = f"s3://{BUCKET}/warehouse"

REST_HOST = os.getenv("ICEBERG_REST_HOST", "localhost:8181")

# S3 properties forwarded to IcebergAdapter so file I/O hits MinIO, not AWS.
_DSN_PARAMS = urlencode({
    "warehouse": WAREHOUSE,
    "s3.endpoint": MINIO_ENDPOINT,
    "s3.access-key-id": MINIO_USER,
    "s3.secret-access-key": MINIO_PASSWORD,
    "s3.path-style-access": "true",
})
DSN = f"iceberg+rest://{REST_HOST}?{_DSN_PARAMS}"

NAMESPACE = "smoke_ns"
TABLE = "orders"

# Rows: id, customer (1 NULL), amount (1 NULL)
ROWS = {
    "id":       [1,       2,     3,     4,      5],
    "customer": ["Alice", "Bob", None,  "Dave", "Eve"],
    "amount":   [100,     200,   150,   None,   300],
}

# ── Helpers ───────────────────────────────────────────────────────────────────


def _ok(msg: str) -> None:
    print(f"  ✓ {msg}")


def _fail(msg: str) -> None:
    print(f"  ✗ {msg}", file=sys.stderr)
    sys.exit(1)


# ── Step 1: create MinIO bucket ───────────────────────────────────────────────


def create_bucket() -> None:
    print("\n[1/3] MinIO bucket")
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        _fail("boto3 not installed — run: pip install boto3")

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=BUCKET)
        _ok(f"bucket {BUCKET!r} created")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            _ok(f"bucket {BUCKET!r} already exists")
        else:
            _fail(f"MinIO error: {e}")


# ── Step 2: create namespace + table, write data ──────────────────────────────


def setup_catalog():
    print("\n[2/3] Iceberg catalog setup")
    try:
        import pyarrow as pa
        from pyiceberg.catalog.rest import RestCatalog
        from pyiceberg.schema import Schema
        from pyiceberg.types import IntegerType, LongType, NestedField, StringType
    except ImportError:
        _fail("pyiceberg[pyarrow] not installed — run: pip install 'pyiceberg[pyarrow]>=0.7.0'")

    s3_props = {
        "s3.endpoint": MINIO_ENDPOINT,
        "s3.access-key-id": MINIO_USER,
        "s3.secret-access-key": MINIO_PASSWORD,
        "s3.path-style-access": "true",
    }
    catalog = RestCatalog("rest", uri=f"http://{REST_HOST}", warehouse=WAREHOUSE, **s3_props)

    # Namespace
    existing_ns = [ns[0] for ns in catalog.list_namespaces()]
    if NAMESPACE not in existing_ns:
        catalog.create_namespace(NAMESPACE)
        _ok(f"namespace {NAMESPACE!r} created")
    else:
        _ok(f"namespace {NAMESPACE!r} already exists")

    # Table
    existing_tables = [t[-1] for t in catalog.list_tables(NAMESPACE)]
    if TABLE not in existing_tables:
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "customer", StringType(), required=False),
            NestedField(3, "amount", IntegerType(), required=False),
        )
        catalog.create_table(
            identifier=(NAMESPACE, TABLE),
            schema=schema,
            location=f"{WAREHOUSE}/{NAMESPACE}/{TABLE}",
            properties={"write.target-file-size-bytes": "536870912"},
        )
        _ok(f"table {NAMESPACE}.{TABLE!r} created")
    else:
        _ok(f"table {NAMESPACE}.{TABLE!r} already exists")

    # Write data (creates snapshot + manifest metadata)
    table = catalog.load_table((NAMESPACE, TABLE))
    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("customer", pa.string(), nullable=True),
        pa.field("amount", pa.int32(), nullable=True),
    ])
    arrow_table = pa.table({
        "id":       pa.array(ROWS["id"], type=pa.int64()),
        "customer": pa.array(ROWS["customer"], type=pa.string()),
        "amount":   pa.array(ROWS["amount"], type=pa.int32()),
    }, schema=arrow_schema)
    table.append(arrow_table)
    _ok(f"wrote {len(ROWS['id'])} rows (1 NULL in customer, 1 NULL in amount)")


# ── Step 3: run adapter and assert ───────────────────────────────────────────


def run_adapter() -> None:
    print("\n[3/3] IcebergAdapter assertions")
    print(f"  DSN: {DSN}")

    # Point PyIceberg at local MinIO (env vars used by RestCatalog file I/O)
    os.environ.setdefault("AWS_ACCESS_KEY_ID", MINIO_USER)
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", MINIO_PASSWORD)
    os.environ.setdefault("AWS_REGION", "us-east-1")

    from app.db import make_adapter_for_url
    adapter = make_adapter_for_url(DSN)

    # list_tables
    tables = adapter.list_tables(NAMESPACE)
    print(f"\n  list_tables({NAMESPACE!r}) → {tables}")
    if not any(t["table_name"] == TABLE for t in tables):
        _fail(f"table {TABLE!r} not found in list_tables result")
    _ok("list_tables ✓")

    # table_schema
    schema = adapter.table_schema(TABLE, NAMESPACE)
    print(f"  table_schema → {schema}")
    if len(schema) != 3:
        _fail(f"expected 3 fields, got {len(schema)}")
    names = {f["name"] for f in schema}
    if names != {"id", "customer", "amount"}:
        _fail(f"unexpected field names: {names}")
    _ok("table_schema ✓")

    # table_stats
    stats = adapter.table_stats(TABLE, NAMESPACE)
    print(f"  table_stats  → {stats}")
    if stats is None:
        _fail("table_stats returned None")
    if stats["row_count"] != len(ROWS["id"]):
        _fail(f"expected row_count={len(ROWS['id'])}, got {stats['row_count']}")
    if stats["size_bytes"] <= 0:
        _fail(f"expected size_bytes > 0, got {stats['size_bytes']}")
    if stats["last_analyze"] is None:
        _fail("last_analyze is None (no snapshot?)")
    _ok("table_stats ✓")

    # column_nulls
    nulls = adapter.column_nulls(TABLE, NAMESPACE)
    print(f"  column_nulls → {nulls}")
    if not nulls:
        _fail("column_nulls returned empty list")
    customer_stats = next((c for c in nulls if c["column"] == "customer"), None)
    amount_stats = next((c for c in nulls if c["column"] == "amount"), None)
    if customer_stats is None or customer_stats["null_count"] != 1:
        _fail(f"customer null_count: expected 1, got {customer_stats}")
    if amount_stats is None or amount_stats["null_count"] != 1:
        _fail(f"amount null_count: expected 1, got {amount_stats}")
    _ok("column_nulls ✓")

    # column_distribution — must return [] (no scan)
    dist = adapter.column_distribution(TABLE, NAMESPACE)
    print(f"  column_distribution → {dist}")
    if dist != []:
        _fail(f"expected [], got {dist}")
    _ok("column_distribution ✓ (empty — no scan by design)")


# ── Entrypoint ────────────────────────────────────────────────────────────────


def main() -> None:
    print("=" * 60)
    print("Iceberg adapter live smoke test")
    print(f"  REST catalog : http://{REST_HOST}")
    print(f"  MinIO        : {MINIO_ENDPOINT}")
    print(f"  Warehouse    : {WAREHOUSE}")
    print("=" * 60)

    t0 = time.monotonic()
    create_bucket()
    setup_catalog()
    run_adapter()
    elapsed = time.monotonic() - t0

    print(f"\n{'=' * 60}")
    print(f"✅  All checks passed in {elapsed:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
