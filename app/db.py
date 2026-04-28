import threading

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from app.config import settings

_engine: Engine | None = None
_engine_lock = threading.Lock()


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
                    connect_args={"connect_timeout": 5},
                )
    return _engine


def _qi(identifier: str) -> str:
    """Quote a SQL identifier (schema, table, column) to prevent injection."""
    return '"' + identifier.replace('"', '""') + '"'


def list_tables(schema: str | None = None) -> list[dict]:
    schema = schema or settings.MONITORED_SCHEMA
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


def table_stats(table_name: str, schema: str | None = None) -> dict | None:
    schema = schema or settings.MONITORED_SCHEMA
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


def table_schema(table_name: str, schema: str | None = None) -> list[dict]:
    schema = schema or settings.MONITORED_SCHEMA
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
    return [{"name": r[0], "type": r[1], "nullable": r[2] == "YES"} for r in rows]


def column_nulls(table_name: str, schema: str | None = None) -> list[dict]:
    schema = schema or settings.MONITORED_SCHEMA
    fqn = f"{_qi(schema)}.{_qi(table_name)}"

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

        parts = ", ".join(
            f"COUNT(*) FILTER (WHERE {_qi(name)} IS NULL) AS {_qi(name)}"
            for name, _ in cols
        )
        count_query = text(
            f"SELECT COUNT(*) AS total, {parts} FROM {fqn}"
        )
        row = conn.execute(count_query).fetchone()

    total = row[0] if row else 0
    results = []
    for i, (name, dtype) in enumerate(cols):
        null_count = row[i + 1] if row else 0
        results.append(
            {
                "column": name,
                "data_type": dtype,
                "null_count": int(null_count),
                "null_rate": round(null_count / total, 4) if total else 0.0,
            }
        )
    return results
