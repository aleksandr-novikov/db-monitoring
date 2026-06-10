"""Unit tests for IcebergAdapter (#111).

All tests mock the PyIceberg catalog — no real Iceberg cluster needed.
Integration tests (MinIO + testcontainers) live in #124.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REST_DSN = "iceberg+rest://localhost:8181?warehouse=s3://bucket/wh"
GLUE_DSN = "iceberg+glue://?warehouse=s3://bucket/wh"


def _make_field(field_id: int, name: str, type_str: str, optional: bool = True):
    f = MagicMock()
    f.field_id = field_id
    f.name = name
    f.field_type = type_str
    f.optional = optional
    return f


def _make_manifest_entry(null_value_counts: dict, value_counts: dict, record_count: int = 100):
    from pyiceberg.manifest import ManifestEntryStatus

    entry = MagicMock()
    entry.status = ManifestEntryStatus.EXISTING
    entry.data_file.null_value_counts = null_value_counts
    entry.data_file.value_counts = value_counts
    entry.data_file.record_count = record_count
    return entry


# ---------------------------------------------------------------------------
# _adapter_key
# ---------------------------------------------------------------------------

def test_adapter_key_rest():
    from app.db import _adapter_key
    assert _adapter_key(REST_DSN) == "iceberg+rest"


def test_adapter_key_glue():
    from app.db import _adapter_key
    assert _adapter_key(GLUE_DSN) == "iceberg+glue"


def test_adapter_key_postgres():
    from app.db import _adapter_key
    assert _adapter_key("postgresql://user:pass@host/db") == "postgresql"


def test_adapter_key_clickhouse():
    from app.db import _adapter_key
    assert _adapter_key("clickhouse+native://user:pass@host:9000/db") == "clickhouse"


# ---------------------------------------------------------------------------
# IcebergAdapter construction
# ---------------------------------------------------------------------------

def test_rest_adapter_init():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        mock_cls.assert_called_once_with(
            "rest", uri="http://localhost:8181", warehouse="s3://bucket/wh"
        )
        assert adapter._catalog is mock_cls.return_value


def test_rest_adapter_init_remote_defaults_to_https():
    """Remote host → HTTPS by default (#260)."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from app.db import IcebergAdapter
        IcebergAdapter("iceberg+rest://catalog.example.com:8181?warehouse=s3://b/w")
        mock_cls.assert_called_once_with(
            "rest", uri="https://catalog.example.com:8181", warehouse="s3://b/w"
        )


def test_rest_adapter_init_localhost_defaults_to_http():
    """localhost / 127.0.0.1 / ::1 → HTTP (dev default, #260)."""
    from app.db import IcebergAdapter
    for host in ("localhost", "127.0.0.1", "[::1]"):
        with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
            IcebergAdapter(f"iceberg+rest://{host}:8181?warehouse=s3://b/w")
            called_uri = mock_cls.call_args[1]["uri"]
            assert called_uri.startswith("http://"), f"expected http:// for host {host!r}"


def test_rest_adapter_docker_hostname_defaults_to_http():
    """Docker-сервисы без точки в имени (iceberg-rest, minio) → HTTP (#290)."""
    from app.db import IcebergAdapter
    for host in ("iceberg-rest", "minio", "catalog-service"):
        with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
            IcebergAdapter(f"iceberg+rest://{host}:8181?warehouse=s3://b/w")
            called_uri = mock_cls.call_args[1]["uri"]
            assert called_uri.startswith("http://"), f"expected http:// for Docker host {host!r}"


def test_rest_adapter_init_ssl_false_overrides():
    """?ssl=false on a remote host forces HTTP; ssl must not reach PyIceberg (#260)."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from app.db import IcebergAdapter
        IcebergAdapter(
            "iceberg+rest://catalog.example.com:8181?warehouse=s3://b/w&ssl=false"
        )
        called_kwargs = mock_cls.call_args[1]
        assert called_kwargs["uri"].startswith("http://")
        assert "ssl" not in called_kwargs


def test_rest_adapter_init_ssl_true_overrides_localhost():
    """?ssl=true on localhost forces HTTPS; ssl must not reach PyIceberg (#260)."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from app.db import IcebergAdapter
        IcebergAdapter(
            "iceberg+rest://localhost:8181?warehouse=s3://b/w&ssl=true"
        )
        called_kwargs = mock_cls.call_args[1]
        assert called_kwargs["uri"].startswith("https://")
        assert "ssl" not in called_kwargs


def test_glue_adapter_init():
    with patch("pyiceberg.catalog.glue.GlueCatalog") as mock_cls:
        from app.db import IcebergAdapter
        IcebergAdapter(GLUE_DSN)
        mock_cls.assert_called_once_with("glue", warehouse="s3://bucket/wh")


def test_unsupported_catalog_type_raises():
    from app.db import IcebergAdapter
    with pytest.raises(ValueError, match="Unsupported Iceberg catalog type"):
        IcebergAdapter("iceberg+hive://host:9083")


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------

def test_list_tables_rest():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_catalog = mock_cls.return_value
        mock_catalog.list_tables.return_value = [
            ("myns", "orders"),
            ("myns", "users"),
        ]
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.list_tables("myns")

    assert result == [
        {"table_name": "orders", "schema": "myns"},
        {"table_name": "users", "schema": "myns"},
    ]


def test_list_tables_unknown_namespace_returns_empty():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from pyiceberg.exceptions import NoSuchNamespaceError

        mock_catalog = mock_cls.return_value
        mock_catalog.list_tables.side_effect = NoSuchNamespaceError("myns")
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.list_tables("myns")

    assert result == []


# ---------------------------------------------------------------------------
# table_schema
# ---------------------------------------------------------------------------

def test_table_schema():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_catalog = mock_cls.return_value
        mock_table = MagicMock()
        mock_catalog.load_table.return_value = mock_table
        mock_table.schema.return_value.fields = [
            _make_field(1, "id", "long", optional=False),
            _make_field(2, "email", "string", optional=True),
        ]
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.table_schema("users", "myns")

    assert result == [
        {"name": "id", "type": "long", "nullable": False},
        {"name": "email", "type": "string", "nullable": True},
    ]
    mock_catalog.load_table.assert_called_once_with(("myns", "users"))


def test_table_schema_unknown_table_returns_empty():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from pyiceberg.exceptions import NoSuchTableError

        mock_cls.return_value.load_table.side_effect = NoSuchTableError("users")
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.table_schema("users", "myns")

    assert result == []


# ---------------------------------------------------------------------------
# table_stats
# ---------------------------------------------------------------------------

def test_table_stats_from_snapshot_summary():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_table = mock_cls.return_value.load_table.return_value
        mock_snapshot = MagicMock()
        mock_snapshot.timestamp_ms = 1_700_000_000_000
        mock_snapshot.summary.get.side_effect = lambda k, d=0: {
            "total-records": "42",
            "total-files-size": "1024",
        }.get(k, d)
        mock_table.current_snapshot.return_value = mock_snapshot

        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.table_stats("orders", "myns")

    assert result["table_name"] == "orders"
    assert result["row_count"] == 42
    assert result["size_bytes"] == 1024
    assert result["last_analyze"] is not None


def test_table_stats_no_snapshot_returns_zeros():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_table = mock_cls.return_value.load_table.return_value
        mock_table.current_snapshot.return_value = None

        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.table_stats("orders", "myns")

    assert result["row_count"] == 0
    assert result["size_bytes"] == 0
    assert result["last_analyze"] is None


def test_table_stats_unknown_table_returns_none():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from pyiceberg.exceptions import NoSuchTableError

        mock_cls.return_value.load_table.side_effect = NoSuchTableError("orders")
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.table_stats("orders", "myns")

    assert result is None


def test_table_stats_mor_subtracts_delete_records():
    """MoR tables: live row_count = total-records minus positional + equality deletes."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_table = mock_cls.return_value.load_table.return_value
        mock_snapshot = MagicMock()
        mock_snapshot.timestamp_ms = 1_700_000_000_000
        mock_snapshot.summary.get.side_effect = lambda k, d=0: {
            "total-records": "1000",
            "total-files-size": "2048",
            "total-position-deletes": "300",
            "total-equality-deletes": "150",
        }.get(k, d)
        mock_table.current_snapshot.return_value = mock_snapshot

        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.table_stats("events", "myns")

    assert result["row_count"] == 550  # 1000 - 300 - 150


def test_table_stats_mor_row_count_never_negative():
    """Stale metadata edge-case: deletes > total-records clamps to 0, not negative."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_table = mock_cls.return_value.load_table.return_value
        mock_snapshot = MagicMock()
        mock_snapshot.timestamp_ms = 1_700_000_000_000
        mock_snapshot.summary.get.side_effect = lambda k, d=0: {
            "total-records": "10",
            "total-files-size": "512",
            "total-position-deletes": "15",
        }.get(k, d)
        mock_table.current_snapshot.return_value = mock_snapshot

        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.table_stats("events", "myns")

    assert result["row_count"] == 0


# ---------------------------------------------------------------------------
# column_nulls — manifest metadata path (no full scan)
# ---------------------------------------------------------------------------

def test_column_nulls_from_manifest_metadata():
    """Null counts come from manifest entries, not a data scan."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_table = mock_cls.return_value.load_table.return_value
        mock_snapshot = MagicMock()
        mock_table.current_snapshot.return_value = mock_snapshot

        # Schema: two fields with field_ids 1 and 2
        mock_table.schema.return_value.fields = [
            _make_field(1, "id", "long", optional=False),
            _make_field(2, "email", "string", optional=True),
        ]

        # One manifest with one entry: 5 nulls in field 2, 0 in field 1
        entry = _make_manifest_entry(
            null_value_counts={1: 0, 2: 5},
            value_counts={1: 100, 2: 100},
        )
        mock_manifest = MagicMock()
        mock_manifest.fetch_manifest_entry.return_value = [entry]
        mock_snapshot.manifests.return_value = [mock_manifest]

        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.column_nulls("users", "myns")

    assert len(result) == 2
    id_stats = next(r for r in result if r["column"] == "id")
    email_stats = next(r for r in result if r["column"] == "email")
    assert id_stats["null_count"] == 0
    assert id_stats["null_rate"] == 0.0
    assert email_stats["null_count"] == 5
    assert email_stats["null_rate"] == 0.05


def test_column_nulls_skips_deleted_manifest_entries():
    """DELETED manifest entries must not contribute to null counts."""
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        from pyiceberg.manifest import ManifestEntryStatus

        mock_table = mock_cls.return_value.load_table.return_value
        mock_snapshot = MagicMock()
        mock_table.current_snapshot.return_value = mock_snapshot
        mock_table.schema.return_value.fields = [_make_field(1, "id", "long")]

        deleted_entry = _make_manifest_entry({1: 99}, {1: 100})
        deleted_entry.status = ManifestEntryStatus.DELETED
        live_entry = _make_manifest_entry({1: 2}, {1: 50})

        mock_manifest = MagicMock()
        mock_manifest.fetch_manifest_entry.return_value = [deleted_entry, live_entry]
        mock_snapshot.manifests.return_value = [mock_manifest]

        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        result = adapter.column_nulls("users", "myns")

    assert result[0]["null_count"] == 2  # only live_entry counted


def test_column_nulls_no_snapshot_returns_empty():
    with patch("pyiceberg.catalog.rest.RestCatalog") as mock_cls:
        mock_cls.return_value.load_table.return_value.current_snapshot.return_value = None
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        assert adapter.column_nulls("users", "myns") == []


# ---------------------------------------------------------------------------
# column_distribution — graceful skip
# ---------------------------------------------------------------------------

def test_column_distribution_returns_empty():
    with patch("pyiceberg.catalog.rest.RestCatalog"):
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        assert adapter.column_distribution("orders", "myns") == []


# ---------------------------------------------------------------------------
# quote_ident — no SQL quoting for Iceberg
# ---------------------------------------------------------------------------

def test_quote_ident_passthrough():
    with patch("pyiceberg.catalog.rest.RestCatalog"):
        from app.db import IcebergAdapter
        adapter = IcebergAdapter(REST_DSN)
        assert adapter.quote_ident("my_table") == "my_table"


# ---------------------------------------------------------------------------
# make_adapter_for_url integration
# ---------------------------------------------------------------------------

def test_make_adapter_for_url_returns_iceberg_adapter():
    with patch("pyiceberg.catalog.rest.RestCatalog"):
        from app.db import IcebergAdapter, make_adapter_for_url
        adapter = make_adapter_for_url(REST_DSN)
        assert isinstance(adapter, IcebergAdapter)


def test_make_adapter_for_url_glue():
    with patch("pyiceberg.catalog.glue.GlueCatalog"):
        from app.db import IcebergAdapter, make_adapter_for_url
        adapter = make_adapter_for_url(GLUE_DSN)
        assert isinstance(adapter, IcebergAdapter)


# ---------------------------------------------------------------------------
# using_engine with engine=None — Iceberg path
# ---------------------------------------------------------------------------

def test_using_engine_none_does_not_set_engine_override():
    """engine=None must leave _engine_override untouched; adapter override must work."""
    with patch("pyiceberg.catalog.rest.RestCatalog"):
        from app.db import IcebergAdapter, _engine_override, get_adapter, using_engine
        adapter = IcebergAdapter(REST_DSN)

    original_engine = _engine_override.get()
    with using_engine(None, adapter):
        assert _engine_override.get() is original_engine, (
            "_engine_override should not be set when engine=None"
        )
        assert get_adapter() is adapter, "adapter override must be active inside the block"

    from app.db import _adapter_override
    assert _adapter_override.get() is None, "adapter override must be cleared after the block"
