"""Full pipeline smoke (#44): real Postgres → collector → SQLite metrics → API.

Spins up a real Postgres, seeds one table, runs ``MetricsCollector.collect``,
persists via ``save_metrics`` to a temp SQLite file, then queries
``/api/metrics/<table>`` through the Flask test client. Verifies the whole
chain wires together — not just any single layer.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, text

from tests.integration.conftest import use_metrics_db, use_target_db

pytestmark = pytest.mark.integration

testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
PostgresContainer = testcontainers_postgres.PostgresContainer


@pytest.fixture(scope="module")
def pg_container():
    with PostgresContainer("postgres:16") as pg:
        yield pg


@pytest.fixture(scope="module")
def pg_url(pg_container) -> str:
    return pg_container.get_connection_url().replace(
        "postgresql+psycopg2", "postgresql"
    )


@pytest.fixture(scope="module", autouse=True)
def _seed_monitored_db(pg_container):
    engine = create_engine(pg_container.get_connection_url(), future=True)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE orders (
                id    SERIAL PRIMARY KEY,
                total NUMERIC(10, 2) NOT NULL,
                email TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO orders (total, email) VALUES
                (10.00, 'a@x.io'),
                (20.00, NULL),
                (30.00, 'c@x.io')
        """))
        # Populate pg_stat_user_tables so table_stats returns row_count=3.
        conn.execute(text("ANALYZE orders"))
    engine.dispose()


def test_collector_to_api_full_cycle(pg_url, tmp_path):
    """Real Postgres → collector → SQLite metrics_storage → /api/metrics."""
    metrics_url = f"sqlite:///{tmp_path / 'metrics.db'}"

    with use_target_db(pg_url), use_metrics_db(metrics_url):
        # 1. Collect metrics for the seeded `orders` table.
        from collectors.metrics_collector import MetricsCollector

        rows = MetricsCollector().collect("orders")
        assert rows, "collector returned no rows for seeded table"

        metric_names = {r["metric_name"] for r in rows}
        assert {"row_count", "null_rate"}.issubset(metric_names)

        # 2. Persist into the SQLite metrics store.
        from app.metrics_storage import save_metrics

        saved = save_metrics(rows, "legacy")
        assert saved == len(rows)

        # 3. Read back via the public REST API using the Flask test client.
        from app.app import create_app

        app = create_app({"TESTING": True})
        client = app.test_client()

        resp = client.get("/api/metrics/orders?metric=row_count&range=24h")
        assert resp.status_code == 200
        payload = resp.get_json()
        assert isinstance(payload, list) and len(payload) == 1
        assert payload[0]["value"] == 3.0

        # 4. /api/tables should also list the table with its freshest metrics.
        tables_resp = client.get("/api/tables")
        assert tables_resp.status_code == 200
        tables = tables_resp.get_json()
        orders = next((t for t in tables if t["table_name"] == "orders"), None)
        assert orders is not None
        assert orders["row_count"] == 3.0
