"""Integration tests for IcebergAdapter against real MinIO + REST catalog (#124).

Spins up two containers via testcontainers (4.10.0 API):
  - MinIO (S3-compatible storage for Iceberg data files)
  - tabulario/iceberg-rest (REST catalog that points at MinIO)

Both share a Docker network so iceberg-rest can reach MinIO via the alias
``minio``. The Python test code (and IcebergAdapter) connect via host-mapped
ports on localhost.

Prerequisites: Docker daemon running. Runs only on push to master — same CI
gate as the other integration tests (#44).
"""

from __future__ import annotations

import time
import urllib.request
from urllib.parse import urlencode

import pytest

pytestmark = pytest.mark.integration

# Skip the whole module cleanly if any runtime dep is missing.
# Each importorskip produces a clear SKIP rather than an ImportError inside a
# fixture, which would be reported as an ERROR.
pytest.importorskip("testcontainers.core.container")
pytest.importorskip("testcontainers.core.network")
pytest.importorskip("pyiceberg")
pytest.importorskip("pyarrow")
pytest.importorskip("boto3")

from testcontainers.core.container import DockerContainer  # noqa: E402
from testcontainers.core.network import Network  # noqa: E402

# ── Constants ─────────────────────────────────────────────────────────────────

MINIO_IMAGE = "minio/minio:RELEASE.2024-01-16T16-07-38Z"
ICEBERG_REST_IMAGE = "tabulario/iceberg-rest:0.10.0"

BUCKET = "iceberg-test"
WAREHOUSE = f"s3://{BUCKET}/warehouse"
NAMESPACE = "test_ns"
TABLE = "orders"

# 5 rows: 1 NULL in customer (row 3), 1 NULL in amount (row 4)
ROWS = {
    "id":       [1,      2,      3,     4,      5],
    "customer": ["Alice", "Bob",  None,  "Dave", "Eve"],
    "amount":   [100,    200,    150,   None,   300],
}


# ── Helpers ───────────────────────────────────────────────────────────────────


def _wait_http(url: str, timeout: int = 60) -> None:
    """Poll ``url`` until 200 or timeout (seconds). Raises on timeout."""
    deadline = time.monotonic() + timeout
    last_exc: Exception = RuntimeError("timeout before first attempt")
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(url, timeout=2)
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(1)
    raise TimeoutError(f"{url} not ready after {timeout}s: {last_exc}") from last_exc


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def iceberg_network():
    with Network() as net:
        yield net


@pytest.fixture(scope="module")
def minio_container(iceberg_network):
    with (
        DockerContainer(MINIO_IMAGE)
        .with_network(iceberg_network)
        .with_network_aliases("minio")
        .with_env("MINIO_ROOT_USER", "minioadmin")
        .with_env("MINIO_ROOT_PASSWORD", "minioadmin")
        .with_exposed_ports(9000)
        .with_command("server /data --console-address :9001")
    ) as container:
        minio_port = container.get_exposed_port(9000)
        _wait_http(f"http://localhost:{minio_port}/minio/health/live")
        yield container


@pytest.fixture(scope="module")
def rest_container(iceberg_network, minio_container):
    # iceberg-rest uses the Docker-internal alias "minio:9000" to reach MinIO.
    # The Python test code connects via the host-mapped port instead.
    with (
        DockerContainer(ICEBERG_REST_IMAGE)
        .with_network(iceberg_network)
        .with_env("CATALOG_WAREHOUSE", WAREHOUSE)
        .with_env("CATALOG_IO__IMPL", "org.apache.iceberg.aws.s3.S3FileIO")
        .with_env("CATALOG_S3_ENDPOINT", "http://minio:9000")
        .with_env("CATALOG_S3_ACCESS__KEY__ID", "minioadmin")
        .with_env("CATALOG_S3_SECRET__ACCESS__KEY", "minioadmin")
        .with_env("CATALOG_S3_PATH__STYLE__ACCESS", "true")
        .with_env("AWS_ACCESS_KEY_ID", "minioadmin")
        .with_env("AWS_SECRET_ACCESS_KEY", "minioadmin")
        .with_env("AWS_REGION", "us-east-1")
        .with_exposed_ports(8181)
    ) as container:
        rest_port = container.get_exposed_port(8181)
        # tabulario/iceberg-rest has no curl/nc — wait from outside via HTTP
        _wait_http(f"http://localhost:{rest_port}/v1/config")
        yield container


@pytest.fixture(scope="module")
def iceberg_dsn(minio_container, rest_container) -> str:
    """IcebergAdapter DSN that routes S3 file I/O to the local MinIO container."""
    minio_port = minio_container.get_exposed_port(9000)
    rest_port = rest_container.get_exposed_port(8181)
    params = urlencode({
        "warehouse": WAREHOUSE,
        "s3.endpoint": f"http://localhost:{minio_port}",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
    })
    return f"iceberg+rest://localhost:{rest_port}?{params}"


@pytest.fixture(scope="module", autouse=True)
def _seed(minio_container, rest_container, iceberg_dsn) -> None:
    """Create bucket + namespace + table with known null distribution.

    Drops stale state first (NoSuchTableError / NoSuchNamespaceError) so
    the fixture is safe to reuse across partial test runs.
    """
    import boto3
    import pyarrow as pa
    from pyiceberg.catalog.rest import RestCatalog
    from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, LongType, NestedField, StringType

    minio_port = minio_container.get_exposed_port(9000)
    rest_port = rest_container.get_exposed_port(8181)
    minio_endpoint = f"http://localhost:{minio_port}"

    # Bucket
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=BUCKET)

    s3_props = {
        "s3.endpoint": minio_endpoint,
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin",
        "s3.path-style-access": "true",
    }
    catalog = RestCatalog(
        "rest",
        uri=f"http://localhost:{rest_port}",
        warehouse=WAREHOUSE,
        **s3_props,
    )

    # Drop stale state so the seed is idempotent on re-runs
    try:
        catalog.drop_table((NAMESPACE, TABLE))
    except NoSuchTableError:
        pass
    try:
        catalog.drop_namespace(NAMESPACE)
    except NoSuchNamespaceError:
        pass

    catalog.create_namespace(NAMESPACE)
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "customer", StringType(), required=False),
        NestedField(3, "amount", IntegerType(), required=False),
    )
    catalog.create_table(
        identifier=(NAMESPACE, TABLE),
        schema=schema,
        location=f"{WAREHOUSE}/{NAMESPACE}/{TABLE}",
    )

    table = catalog.load_table((NAMESPACE, TABLE))
    arrow_schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("customer", pa.string(), nullable=True),
        pa.field("amount", pa.int32(), nullable=True),
    ])
    arrow_table = pa.table(
        {
            "id":       pa.array(ROWS["id"], type=pa.int64()),
            "customer": pa.array(ROWS["customer"], type=pa.string()),
            "amount":   pa.array(ROWS["amount"], type=pa.int32()),
        },
        schema=arrow_schema,
    )
    table.append(arrow_table)


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_list_tables_returns_fixture_table(iceberg_dsn):
    from app.db import make_adapter_for_url
    tables = make_adapter_for_url(iceberg_dsn).list_tables(NAMESPACE)
    names = [t["table_name"] for t in tables]
    assert TABLE in names
    assert all(t["schema"] == NAMESPACE for t in tables)


def test_list_tables_unknown_namespace_returns_empty(iceberg_dsn):
    from app.db import make_adapter_for_url
    assert make_adapter_for_url(iceberg_dsn).list_tables("no_such_ns") == []


def test_table_schema_columns_and_nullability(iceberg_dsn):
    from app.db import make_adapter_for_url
    cols = make_adapter_for_url(iceberg_dsn).table_schema(TABLE, NAMESPACE)
    by_name = {c["name"]: c for c in cols}
    assert set(by_name) == {"id", "customer", "amount"}
    assert by_name["id"]["nullable"] is False
    assert by_name["customer"]["nullable"] is True
    assert by_name["amount"]["nullable"] is True


def test_table_stats_row_count_size_and_timestamp(iceberg_dsn):
    from app.db import make_adapter_for_url
    stats = make_adapter_for_url(iceberg_dsn).table_stats(TABLE, NAMESPACE)
    assert stats is not None
    assert stats["row_count"] == len(ROWS["id"])
    assert stats["size_bytes"] > 0
    assert stats["last_analyze"] is not None


def test_table_stats_missing_table_returns_none(iceberg_dsn):
    from app.db import make_adapter_for_url
    assert make_adapter_for_url(iceberg_dsn).table_stats("missing", NAMESPACE) is None


def test_column_nulls_counts_and_rates_from_manifest(iceberg_dsn):
    from app.db import make_adapter_for_url
    nulls = make_adapter_for_url(iceberg_dsn).column_nulls(TABLE, NAMESPACE)
    by_col = {c["column"]: c for c in nulls}
    # id: required — no NULLs
    assert by_col["id"]["null_count"] == 0
    assert by_col["id"]["null_rate"] == 0.0
    # customer: 1 of 5 NULLs
    assert by_col["customer"]["null_count"] == 1
    assert by_col["customer"]["null_rate"] == pytest.approx(0.2)
    # amount: 1 of 5 NULLs
    assert by_col["amount"]["null_count"] == 1
    assert by_col["amount"]["null_rate"] == pytest.approx(0.2)


def test_column_distribution_returns_empty(iceberg_dsn):
    from app.db import make_adapter_for_url
    assert make_adapter_for_url(iceberg_dsn).column_distribution(TABLE, NAMESPACE) == []
