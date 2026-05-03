import threading
from abc import ABC, abstractmethod

from sqlalchemy import create_engine, make_url, text
from sqlalchemy.engine import Engine

from app.config import settings

_engine: Engine | None = None
_engine_lock = threading.Lock()
_adapter: "DBAdapter | None" = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                _engine = create_engine(
                    settings.DATABASE_URL,
                    pool_size=5,
                    max_overflow=2,
                    pool_pre_ping=True,
                    connect_args=_connect_args(settings.DATABASE_URL),
                )
    return _engine


def _connect_args(url: str) -> dict:
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


_SKIP_DIST_TYPE_FRAGMENTS = (
    "text", "json", "jsonb", "bytea", "blob", "clob", "xml", "array",
)


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


_ADAPTERS: dict[str, type[DBAdapter]] = {
    "postgresql": PostgresAdapter,
    "mysql": MySQLAdapter,
    "clickhouse": ClickHouseAdapter,
}


def get_adapter() -> DBAdapter:
    global _adapter
    if _adapter is None:
        backend = make_url(settings.DATABASE_URL).get_backend_name()
        cls = _ADAPTERS.get(backend)
        if cls is None:
            raise ValueError(
                f"Unsupported database backend: {backend!r}. "
                f"Supported: {sorted(_ADAPTERS)}"
            )
        _adapter = cls()
    return _adapter


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
