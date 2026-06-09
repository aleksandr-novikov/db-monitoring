import threading
from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

from sqlalchemy import create_engine, make_url, text
from sqlalchemy.engine import Engine

from app.config import settings

_engine: Engine | None = None
_engine_lock = threading.Lock()
_adapter: "DBAdapter | None" = None

# Per-job engine + adapter override (#54). When a per-connection scheduled
# job runs, it sets these ContextVars to the target DSN's engine for the
# duration of the call — every adapter method that goes through get_engine()
# / get_adapter() picks up the override automatically. ContextVar is
# thread-safe AND task-scoped (asyncio-friendly for future async paths).
_engine_override: ContextVar[Engine | None] = ContextVar("_engine_override", default=None)
_adapter_override: ContextVar["DBAdapter | None"] = ContextVar("_adapter_override", default=None)


@contextmanager
def using_engine(engine: "Engine | None", adapter: "DBAdapter"):
    """Run a block with a per-call engine + adapter override.

    Used by collectors/per_project.py to make the existing adapter helpers
    talk to a connection's DSN instead of the global ``DATABASE_URL``.
    ``engine`` may be None for catalog-based adapters (e.g. IcebergAdapter)
    that don't use SQLAlchemy — only the adapter override is set in that case.
    """
    e_token = _engine_override.set(engine) if engine is not None else None
    a_token = _adapter_override.set(adapter)
    try:
        yield
    finally:
        _adapter_override.reset(a_token)
        if e_token is not None:
            _engine_override.reset(e_token)


def get_engine() -> Engine:
    override = _engine_override.get()
    if override is not None:
        return override
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = create_engine(
                    settings.DATABASE_URL,
                    pool_size=5,
                    max_overflow=2,
                    pool_pre_ping=True,
                    connect_args=connect_args_for_url(settings.DATABASE_URL),
                )
    return _engine


def connect_args_for_url(url: str) -> dict:
    """SQLAlchemy connect_args used for short metadata connections."""
    backend = make_url(url).get_backend_name()
    if backend == "postgresql":
        return {"connect_timeout": 5}
    if backend == "mysql":
        return {"connect_timeout": 5}
    return {}


class DBAdapter(ABC):
    """Dialect-specific access to the monitored database.

    Each adapter encapsulates the SQL needed to introspect tables and compute
    NULL statistics for one DB engine. Module-level helpers delegate here via
    get_adapter().
    """

    @abstractmethod
    def quote_ident(self, identifier: str) -> str: ...

    @abstractmethod
    def list_tables(self, schema: str) -> list[dict]: ...

    @abstractmethod
    def table_stats(self, table_name: str, schema: str) -> dict | None: ...

    @abstractmethod
    def table_schema(self, table_name: str, schema: str) -> list[dict]: ...

    @abstractmethod
    def column_nulls(self, table_name: str, schema: str) -> list[dict]: ...

    def column_distribution(
        self, table_name: str, schema: str, top_n: int = 20
    ) -> list[dict]:
        """Top-N value frequencies per column. Default uses dialect-agnostic SQL.

        Returns a list of {column, data_type, total, buckets: [{value, count}, ...]}.
        Columns with types unsuitable for grouping (text, json, blob, bytea) are
        skipped — drift on free-form text rarely makes sense and the GROUP BY
        cost grows with cardinality.
        """
        return _column_distribution_generic(self, table_name, schema, top_n)


def _column_nulls_generic(
    adapter: DBAdapter, table_name: str, schema: str
) -> list[dict]:
    """Default implementation using `COUNT(*) - COUNT(col)` (works in all dialects).

    Adapters can override this if the dialect supports something cheaper
    (e.g. Postgres `FILTER (WHERE col IS NULL)`).
    """
    cols = adapter.table_schema(table_name, schema)
    if not cols:
        return []

    fqn = f"{adapter.quote_ident(schema)}.{adapter.quote_ident(table_name)}"
    parts = ", ".join(
        f"COUNT(*) - COUNT({adapter.quote_ident(c['name'])})"
        for c in cols
    )
    query = text(f"SELECT COUNT(*), {parts} FROM {fqn}")
    with get_engine().connect() as conn:
        row = conn.execute(query).fetchone()

    total = int(row[0]) if row else 0
    return [
        {
            "column": c["name"],
            "data_type": c["type"],
            "null_count": int(row[i + 1]) if row else 0,
            "null_rate": round((row[i + 1] or 0) / total, 4) if total else 0.0,
        }
        for i, c in enumerate(cols)
    ]


_SKIP_DIST_TYPE_FRAGMENTS = ("text", "json", "jsonb", "bytea", "blob", "clob", "xml", "array", "uuid",)


def _is_distribution_skippable(data_type: str | None) -> bool:
    if not data_type:
        return True
    t = data_type.lower()
    return any(frag in t for frag in _SKIP_DIST_TYPE_FRAGMENTS)


def _column_distribution_generic(
    adapter: "DBAdapter", table_name: str, schema: str, top_n: int = 20
) -> list[dict]:
    cols = adapter.table_schema(table_name, schema)
    if not cols:
        return []
    fqn = f"{adapter.quote_ident(schema)}.{adapter.quote_ident(table_name)}"
    out: list[dict] = []
    with get_engine().connect() as conn:
        for c in cols:
            if _is_distribution_skippable(c.get("type")):
                continue
            qname = adapter.quote_ident(c["name"])
            query = text(
                f"SELECT {qname} AS v, COUNT(*) AS c FROM {fqn} "
                f"WHERE {qname} IS NOT NULL "
                f"GROUP BY {qname} ORDER BY c DESC LIMIT :top_n"
            )
            try:
                rows = conn.execute(query, {"top_n": top_n}).fetchall()
            except Exception:  # pragma: no cover - dialect-specific failures
                continue
            buckets = [{"value": _to_str(r[0]), "count": int(r[1])} for r in rows]
            total = sum(b["count"] for b in buckets)
            out.append({
                "column": c["name"],
                "data_type": c["type"],
                "total": total,
                "buckets": buckets,
            })
    return out


def _to_str(v) -> str:
    if v is None:
        return ""
    return str(v)


class PostgresAdapter(DBAdapter):
    def quote_ident(self, identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def list_tables(self, schema: str) -> list[dict]:
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        with get_engine().connect() as conn:
            rows = conn.execute(query, {"schema": schema}).fetchall()
        return [{"table_name": r[0], "schema": schema} for r in rows]

    def table_stats(self, table_name: str, schema: str) -> dict | None:
        query = text("""
            SELECT
                n_live_tup,
                pg_total_relation_size(
                    quote_ident(:schema) || '.' || quote_ident(:table_name)
                ),
                last_analyze,
                last_autoanalyze
            FROM pg_stat_user_tables
            WHERE schemaname = :schema
              AND relname = :table_name
        """)
        with get_engine().connect() as conn:
            row = conn.execute(
                query, {"schema": schema, "table_name": table_name}
            ).fetchone()
        if not row:
            return None
        last_analyze = row[2] or row[3]
        return {
            "table_name": table_name,
            "schema": schema,
            "row_count": int(row[0]),
            "size_bytes": int(row[1]),
            "last_analyze": str(last_analyze) if last_analyze else None,
        }

    def table_schema(self, table_name: str, schema: str) -> list[dict]:
        query = text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        with get_engine().connect() as conn:
            rows = conn.execute(
                query, {"schema": schema, "table_name": table_name}
            ).fetchall()
        return [
            {"name": r[0], "type": r[1], "nullable": r[2] == "YES"} for r in rows
        ]

    def column_nulls(self, table_name: str, schema: str) -> list[dict]:
        # Postgres-specific: FILTER is more idiomatic and lets the planner skip
        # non-null rows on indexed columns. Functionally identical to the
        # generic `COUNT(*) - COUNT(col)` form.
        cols_query = text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        with get_engine().connect() as conn:
            cols = [
                (r[0], r[1])
                for r in conn.execute(
                    cols_query, {"schema": schema, "table_name": table_name}
                ).fetchall()
            ]
            if not cols:
                return []

            fqn = f"{self.quote_ident(schema)}.{self.quote_ident(table_name)}"
            parts = ", ".join(
                f"COUNT(*) FILTER (WHERE {self.quote_ident(name)} IS NULL)"
                f" AS {self.quote_ident(name)}"
                for name, _ in cols
            )
            row = conn.execute(text(f"SELECT COUNT(*) AS total, {parts} FROM {fqn}")).fetchone()

        total = row[0] if row else 0
        return [
            {
                "column": name,
                "data_type": dtype,
                "null_count": int(row[i + 1]) if row else 0,
                "null_rate": round(row[i + 1] / total, 4) if total else 0.0,
            }
            for i, (name, dtype) in enumerate(cols)
        ]

    def column_nulls_sample(
        self, table_name: str, schema: str, percent: float = 1.0,
    ) -> tuple[list[dict], int]:
        """#233 sample mode: null_rate over a TABLESAMPLE SYSTEM(%) slice.

        Returns ``(per_column_rates, sample_size)``. Empty sample (table too
        small, or BERNOULLI skipped every row) → empty list + ``sample_size=0``;
        caller decides whether to drop the metric or log a warning.

        TABLESAMPLE SYSTEM is page-level (cheap) and intentionally imprecise —
        good enough for null-rate estimation, never used for null_count.
        """
        cols_query = text("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        with get_engine().connect() as conn:
            cols = [
                (r[0], r[1])
                for r in conn.execute(
                    cols_query, {"schema": schema, "table_name": table_name},
                ).fetchall()
            ]
            if not cols:
                return [], 0

            fqn = f"{self.quote_ident(schema)}.{self.quote_ident(table_name)}"
            parts = ", ".join(
                f"COUNT(*) FILTER (WHERE {self.quote_ident(name)} IS NULL)"
                f" AS {self.quote_ident(name)}"
                for name, _ in cols
            )
            # SYSTEM(percent) is interpolated, NOT bound — TABLESAMPLE only
            # accepts a literal. percent has been clamped above so injection
            # is not possible here; float-format it explicitly.
            pct = max(0.001, min(100.0, float(percent)))
            row = conn.execute(text(
                f"SELECT COUNT(*) AS total, {parts} "
                f"FROM {fqn} TABLESAMPLE SYSTEM({pct})"
            )).fetchone()

        sample_size = int(row[0]) if row else 0
        if sample_size == 0:
            return [], 0
        return [
            {
                "column": name,
                "data_type": dtype,
                "null_rate": round(row[i + 1] / sample_size, 4),
            }
            for i, (name, dtype) in enumerate(cols)
        ], sample_size

    def column_nulls_approx(
        self, table_name: str, schema: str,
    ) -> list[dict]:
        """#233 approx mode: read null_frac from pg_stats (last ANALYZE).

        No data scan — zero load. Empty list if the table has never been
        ANALYZE'd (no pg_stats row); caller logs a warning.

        ``null_frac`` is the correct column name (not ``null_fraction``).
        """
        query = text("""
            SELECT s.attname, c.data_type, s.null_frac
            FROM pg_stats s
            JOIN information_schema.columns c
              ON c.table_schema = s.schemaname
             AND c.table_name = s.tablename
             AND c.column_name = s.attname
            WHERE s.schemaname = :schema
              AND s.tablename = :table_name
            ORDER BY c.ordinal_position
        """)
        with get_engine().connect() as conn:
            rows = conn.execute(
                query, {"schema": schema, "table_name": table_name},
            ).fetchall()
        return [
            {
                "column": r[0],
                "data_type": r[1],
                # null_frac is REAL in pg_stats; round to match the 4 dp
                # convention used elsewhere.
                "null_rate": round(float(r[2]), 4) if r[2] is not None else 0.0,
            }
            for r in rows
        ]


class MySQLAdapter(DBAdapter):
    def quote_ident(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "``") + "`"

    def list_tables(self, schema: str) -> list[dict]:
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        with get_engine().connect() as conn:
            rows = conn.execute(query, {"schema": schema}).fetchall()
        return [{"table_name": r[0], "schema": schema} for r in rows]

    def table_stats(self, table_name: str, schema: str) -> dict | None:
        # table_rows in InnoDB is an estimate from the optimizer — accurate
        # enough for monitoring trends but not for exact counts. update_time
        # may be NULL on partitioned/InnoDB tables.
        query = text("""
            SELECT
                table_rows,
                COALESCE(data_length, 0) + COALESCE(index_length, 0),
                update_time
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_name = :table_name
        """)
        with get_engine().connect() as conn:
            row = conn.execute(
                query, {"schema": schema, "table_name": table_name}
            ).fetchone()
        if not row:
            return None
        return {
            "table_name": table_name,
            "schema": schema,
            "row_count": int(row[0] or 0),
            "size_bytes": int(row[1] or 0),
            "last_analyze": str(row[2]) if row[2] else None,
        }

    def table_schema(self, table_name: str, schema: str) -> list[dict]:
        query = text("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table_name
            ORDER BY ordinal_position
        """)
        with get_engine().connect() as conn:
            rows = conn.execute(
                query, {"schema": schema, "table_name": table_name}
            ).fetchall()
        return [
            {"name": r[0], "type": r[1], "nullable": r[2] == "YES"} for r in rows
        ]

    def column_nulls(self, table_name: str, schema: str) -> list[dict]:
        return _column_nulls_generic(self, table_name, schema)


class ClickHouseAdapter(DBAdapter):
    """ClickHouse adapter.

    `schema` here maps to ClickHouse *database*. Identifier quoting uses
    backticks. NULL counting uses the dialect-agnostic `COUNT(*) - COUNT(col)`
    form, which on non-Nullable columns yields zero (correct: those columns
    cannot contain NULL).
    """

    def quote_ident(self, identifier: str) -> str:
        return "`" + identifier.replace("`", "\\`") + "`"

    def list_tables(self, schema: str) -> list[dict]:
        query = text("""
            SELECT name
            FROM system.tables
            WHERE database = :schema
              AND engine NOT LIKE '%View%'
              AND is_temporary = 0
            ORDER BY name
        """)
        with get_engine().connect() as conn:
            rows = conn.execute(query, {"schema": schema}).fetchall()
        return [{"table_name": r[0], "schema": schema} for r in rows]

    def table_stats(self, table_name: str, schema: str) -> dict | None:
        query = text("""
            SELECT
                t.total_rows,
                t.total_bytes,
                (SELECT max(modification_time)
                 FROM system.parts
                 WHERE database = :schema AND table = :table_name AND active)
            FROM system.tables AS t
            WHERE t.database = :schema AND t.name = :table_name
        """)
        with get_engine().connect() as conn:
            row = conn.execute(
                query, {"schema": schema, "table_name": table_name}
            ).fetchone()
        if not row:
            return None
        return {
            "table_name": table_name,
            "schema": schema,
            "row_count": int(row[0] or 0),
            "size_bytes": int(row[1] or 0),
            "last_analyze": str(row[2]) if row[2] else None,
        }

    def table_schema(self, table_name: str, schema: str) -> list[dict]:
        query = text("""
            SELECT name, type, startsWith(type, 'Nullable(') AS is_nullable
            FROM system.columns
            WHERE database = :schema AND table = :table_name
            ORDER BY position
        """)
        with get_engine().connect() as conn:
            rows = conn.execute(
                query, {"schema": schema, "table_name": table_name}
            ).fetchall()
        return [{"name": r[0], "type": r[1], "nullable": bool(r[2])} for r in rows]

    def column_nulls(self, table_name: str, schema: str) -> list[dict]:
        return _column_nulls_generic(self, table_name, schema)


class IcebergAdapter(DBAdapter):
    """Apache Iceberg adapter — reads metadata from Iceberg catalogs.

    Supports REST (iceberg+rest://) and AWS Glue (iceberg+glue://) backends.
    Uses PyIceberg catalog API instead of SQLAlchemy — never calls get_engine().

    DSN format:
        iceberg+rest://host:port?warehouse=s3://bucket/path
        iceberg+glue://?warehouse=s3://bucket/path

    ``null_count`` and ``row_count`` are read from Iceberg snapshot/manifest
    metadata — no full table scan, even on billion-row tables.
    """

    def __init__(
        self,
        url: str,
        *,
        warehouse: str | None = None,
        auth_token: str | None = None,
    ):
        """*warehouse* and *auth_token* (#234) override matching DSN query
        params. The form value wins — operators can rotate a token without
        re-saving the whole DSN. None/empty means "keep DSN value as-is"."""
        parsed = urlparse(url)
        catalog_type = parsed.scheme.split("+", 1)[1]  # "rest" or "glue"
        qs = parse_qs(parsed.query)

        # All query params become catalog props (warehouse, s3.endpoint, etc.)
        props: dict = {key: values[0] for key, values in qs.items()}

        # #234: explicit form overrides win over DSN query params.
        if warehouse:
            props["warehouse"] = warehouse
        if auth_token:
            # PyIceberg REST catalog reads bearer tokens from the "token"
            # property. Keep "credential" as-is if the DSN already has one
            # (basic auth); only set the bearer token field.
            props["token"] = auth_token

        if catalog_type == "rest":
            from pyiceberg.catalog.rest import RestCatalog

            ssl_param = props.pop("ssl", None)
            if ssl_param is not None:
                use_tls = ssl_param.lower() in ("true", "1", "yes")
            else:
                # hostname is None for hostless URLs (invalid anyway) → HTTPS
                use_tls = parsed.hostname not in ("localhost", "127.0.0.1", "::1")
            scheme = "https" if use_tls else "http"
            props["uri"] = f"{scheme}://{parsed.netloc}"
            self._catalog = RestCatalog("rest", **props)
        elif catalog_type == "glue":
            from pyiceberg.catalog.glue import GlueCatalog

            props.pop("uri", None)  # uri is REST-only; drop it if accidentally passed
            self._catalog = GlueCatalog("glue", **props)
        else:
            raise ValueError(
                f"Unsupported Iceberg catalog type: {catalog_type!r}. "
                "Use iceberg+rest:// or iceberg+glue://"
            )

    def list_namespaces(self) -> list:
        return self._catalog.list_namespaces()

    def quote_ident(self, identifier: str) -> str:
        return identifier  # PyIceberg uses Python API, no SQL quoting

    def list_tables(self, schema: str) -> list[dict]:
        from pyiceberg.exceptions import NoSuchNamespaceError

        try:
            identifiers = self._catalog.list_tables(schema)
        except NoSuchNamespaceError:
            return []
        # list_tables returns [(namespace, table_name), ...]
        return [{"table_name": ident[-1], "schema": schema} for ident in identifiers]

    def table_stats(self, table_name: str, schema: str) -> dict | None:
        from pyiceberg.exceptions import NoSuchTableError

        try:
            table = self._catalog.load_table((schema, table_name))
        except NoSuchTableError:
            return None

        snapshot = table.current_snapshot()
        if snapshot is None:
            return {
                "table_name": table_name, "schema": schema,
                "row_count": 0, "size_bytes": 0, "last_analyze": None,
            }

        summary = snapshot.summary
        # MoR tables accumulate delete files before compaction; subtract both
        # positional and equality deletes so row_count reflects live rows only.
        # total-equality-deletes counts delete entries, not matched rows — approximate for equality-delete tables (e.g. CDC).
        total = int(summary.get("total-records", 0) or 0)
        deletes = int(summary.get("total-position-deletes", 0) or 0) + \
                  int(summary.get("total-equality-deletes", 0) or 0)
        row_count = max(0, total - deletes)
        size_bytes = int(summary.get("total-files-size", 0) or 0)
        last_analyze = datetime.fromtimestamp(
            snapshot.timestamp_ms / 1000, tz=UTC
        ).isoformat()
        return {
            "table_name": table_name,
            "schema": schema,
            "row_count": row_count,
            "size_bytes": size_bytes,
            "last_analyze": last_analyze,
        }

    def table_schema(self, table_name: str, schema: str) -> list[dict]:
        from pyiceberg.exceptions import NoSuchTableError

        try:
            table = self._catalog.load_table((schema, table_name))
        except NoSuchTableError:
            return []
        return [
            {
                "name": field.name,
                "type": str(field.field_type),
                "nullable": field.optional,
            }
            for field in table.schema().fields
        ]

    def column_nulls(self, table_name: str, schema: str) -> list[dict]:
        """Read null counts from manifest metadata — no full data scan."""
        from pyiceberg.exceptions import NoSuchTableError
        from pyiceberg.manifest import ManifestEntryStatus

        try:
            table = self._catalog.load_table((schema, table_name))
        except NoSuchTableError:
            return []

        snapshot = table.current_snapshot()
        if snapshot is None:
            return []

        iceberg_schema = table.schema()
        null_counts: dict[int, int] = {}   # field_id → total null count
        value_counts: dict[int, int] = {}  # field_id → total value count

        for manifest in snapshot.manifests(table.io):
            for entry in manifest.fetch_manifest_entry(table.io):
                if entry.status == ManifestEntryStatus.DELETED:
                    continue
                df = entry.data_file
                for fid, cnt in (df.null_value_counts or {}).items():
                    null_counts[fid] = null_counts.get(fid, 0) + cnt
                for fid, cnt in (df.value_counts or {}).items():
                    value_counts[fid] = value_counts.get(fid, 0) + cnt

        result = []
        for field in iceberg_schema.fields:
            fid = field.field_id
            nulls = null_counts.get(fid, 0)
            total = value_counts.get(fid, 0)
            result.append({
                "column": field.name,
                "data_type": str(field.field_type),
                "null_count": nulls,
                "null_rate": round(nulls / total, 4) if total else 0.0,
            })
        return result

    def column_distribution(
        self, table_name: str, schema: str, top_n: int = 20
    ) -> list[dict]:
        # Iceberg manifest metadata has no distribution info. A full PyArrow
        # scan would defeat the no-scan value prop on large tables.
        return []


def _adapter_key(url: str) -> str:
    """Return the _ADAPTERS registry key for ``url``.

    Standard SQLAlchemy URLs delegate to make_url().get_backend_name().
    Iceberg URLs (iceberg+rest://, iceberg+glue://) are handled separately
    because SQLAlchemy doesn't know the dialect and get_backend_name() would
    return only "iceberg", losing the catalog-type suffix we need.
    """
    if url.lower().startswith("iceberg+"):
        return url.split("://")[0].lower()  # "iceberg+rest" or "iceberg+glue"
    return make_url(url).get_backend_name()


_ADAPTERS: dict[str, type[DBAdapter]] = {
    "postgresql": PostgresAdapter,
    "mysql": MySQLAdapter,
    "clickhouse": ClickHouseAdapter,
    "iceberg+rest": IcebergAdapter,
    "iceberg+glue": IcebergAdapter,
}


def _make_adapter(
    cls: type[DBAdapter],
    url: str,
    *,
    warehouse: str | None = None,
    auth_token: str | None = None,
) -> DBAdapter:
    """Instantiate an adapter, passing ``url`` for catalog-based adapters.

    Catalog-based adapters (IcebergAdapter) need the raw DSN to connect to
    their catalog API — they don't use SQLAlchemy. SQL adapters take no args.
    If you add a new catalog-based adapter, add it to this condition.

    *warehouse* / *auth_token* (#234) are only used by IcebergAdapter and
    silently ignored for SQL adapters.
    """
    if issubclass(cls, IcebergAdapter):
        return cls(url, warehouse=warehouse, auth_token=auth_token)
    return cls()


def get_adapter() -> DBAdapter:
    override = _adapter_override.get()
    if override is not None:
        return override
    global _adapter
    if _adapter is None:
        url = settings.DATABASE_URL
        key = _adapter_key(url)
        cls = _ADAPTERS.get(key)
        if cls is None:
            raise ValueError(
                f"Unsupported database backend: {key!r}. "
                f"Supported: {sorted(_ADAPTERS)}"
            )
        _adapter = _make_adapter(cls, url)
    return _adapter


def make_adapter_for_url(
    url: str,
    *,
    warehouse: str | None = None,
    auth_token: str | None = None,
) -> DBAdapter:
    """Build a fresh adapter for a given DSN — for the per-project scheduler.

    Bypasses the module-level singleton (which is keyed off ``settings.
    DATABASE_URL``). Cheap operation — adapters hold no state.

    *warehouse* / *auth_token* (#234) are forwarded to IcebergAdapter and
    silently ignored for SQL adapters.
    """
    key = _adapter_key(url)
    cls = _ADAPTERS.get(key)
    if cls is None:
        raise ValueError(
            f"Unsupported database backend: {key!r}. "
            f"Supported: {sorted(_ADAPTERS)}"
        )
    return _make_adapter(cls, url, warehouse=warehouse, auth_token=auth_token)


def list_tables(schema: str | None = None) -> list[dict]:
    return get_adapter().list_tables(schema or settings.MONITORED_SCHEMA)


def table_stats(table_name: str, schema: str | None = None) -> dict | None:
    return get_adapter().table_stats(table_name, schema or settings.MONITORED_SCHEMA)


def table_schema(table_name: str, schema: str | None = None) -> list[dict]:
    return get_adapter().table_schema(table_name, schema or settings.MONITORED_SCHEMA)


def column_nulls(table_name: str, schema: str | None = None) -> list[dict]:
    return get_adapter().column_nulls(table_name, schema or settings.MONITORED_SCHEMA)


def column_distribution(
    table_name: str, schema: str | None = None, top_n: int = 20
) -> list[dict]:
    return get_adapter().column_distribution(
        table_name, schema or settings.MONITORED_SCHEMA, top_n
    )
