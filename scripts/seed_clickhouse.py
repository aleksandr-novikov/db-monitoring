"""Seed the local ClickHouse demo target with realistic test data (#141).

Creates a ``demo`` database with 4 tables analogous to the Postgres demo
seed (``scripts/seed_target_db.py``): ``users``, ``products``, ``orders``,
``events``. Schemas are ClickHouse-native (MergeTree engine, no NULL by
default, UInt32 / DateTime types) so the adapter's ``list_tables`` /
``table_stats`` / ``null_count`` paths see real data shaped how a CH
production database actually looks.

Defaults are small (~10k rows) — designed for "demo on a laptop" speed.
Bump with flags for stress-tests.

Usage:
    python -m scripts.seed_clickhouse
    python -m scripts.seed_clickhouse --reset
    python -m scripts.seed_clickhouse --events 200000 --reset

Connection:
    Defaults to clickhouse+native://default@localhost:19000/demo to match
    `make clickhouse-up`. Override via CLICKHOUSE_URL env var.
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

DEFAULT_URL = "clickhouse+native://default@localhost:19000/demo"

# Demo enum-like dimensions — match the Postgres seed (#42, #75) so cross-
# database demos use the same vocabulary.
_CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Home", "Sports"]
_COUNTRIES = ["RU", "US", "DE", "GB", "FR", "CN", "BR"]
_SIGNUP_SOURCES = ["web", "mobile", "api", "referral"]
_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
_EVENT_TYPES = ["login", "view", "add_to_cart", "checkout", "purchase",
                "error", "logout"]
_SERVER_IDS = ["server-1", "server-2", "server-3"]
_DEVICE_TYPES = ["mobile", "desktop", "tablet"]


# ── Schema ────────────────────────────────────────────────────────────────

_SCHEMA_STATEMENTS = [
    # Database is created by docker-compose CLICKHOUSE_DB; keep an IF NOT
    # EXISTS guard so the script works against an existing CH outside Docker.
    "CREATE DATABASE IF NOT EXISTS demo",

    # users — flat profile data, MergeTree ordered by signup_date so any
    # WHERE created_at <= T snapshot scan stays sequential.
    """
    CREATE TABLE IF NOT EXISTS demo.users (
        user_id      UInt32,
        email        String,
        name         String,
        country      LowCardinality(String),
        signup_date  DateTime,
        signup_source LowCardinality(String),
        is_active    UInt8
    ) ENGINE = MergeTree() ORDER BY (signup_date, user_id)
    """,

    # products — small dimension table.
    """
    CREATE TABLE IF NOT EXISTS demo.products (
        product_id   UInt32,
        name         String,
        category     LowCardinality(String),
        price        Decimal(10, 2),
        created_at   DateTime
    ) ENGINE = MergeTree() ORDER BY (created_at, product_id)
    """,

    # orders — main fact-table for null-rate / changepoint demos.
    """
    CREATE TABLE IF NOT EXISTS demo.orders (
        order_id     UInt64,
        user_id      UInt32,
        product_id   UInt32,
        status       LowCardinality(String),
        quantity     UInt16,
        total_price  Decimal(12, 2),
        created_at   DateTime
    ) ENGINE = MergeTree() ORDER BY (created_at, order_id)
    """,

    # events — append-only stream, the bread-and-butter of ClickHouse demos.
    # event_id as String (UUID-as-text) — CH has a UUID type but String
    # plays nicer with the SQLAlchemy round-trip for the simple seed here.
    """
    CREATE TABLE IF NOT EXISTS demo.events (
        event_id     String,
        user_id      UInt32,
        event_type   LowCardinality(String),
        server_id    LowCardinality(String),
        device_type  LowCardinality(String),
        created_at   DateTime
    ) ENGINE = MergeTree() ORDER BY (created_at, event_id)
    """,
]


def _apply_schema(engine) -> None:
    """ClickHouse does not allow multi-statement execute; run one at a time."""
    with engine.begin() as conn:
        for stmt in _SCHEMA_STATEMENTS:
            clean = stmt.strip()
            if clean:
                conn.execute(text(clean))


def _reset_tables(engine) -> None:
    """TRUNCATE all demo tables — fast for MergeTree (drops parts directly).

    Idempotent: missing tables (first run after schema drop) are silently
    tolerated; CH errors with code 60 for unknown table.
    """
    from sqlalchemy.exc import DatabaseError
    for table in ("users", "products", "orders", "events"):
        try:
            with engine.begin() as conn:
                conn.execute(text(f"TRUNCATE TABLE demo.{table}"))
        except DatabaseError as exc:
            logger.debug("TRUNCATE demo.%s skipped: %s", table, exc)


# ── Seeders ───────────────────────────────────────────────────────────────

def _seed_users(engine, count: int) -> None:
    """Insert *count* users with signup_date spread over the last 365 days."""
    now = datetime.now(UTC)
    rng = random.Random(42)
    rows = []
    for i in range(1, count + 1):
        days_ago = rng.randint(0, 365)
        rows.append({
            "user_id": i,
            "email": f"user{i}@example.com",
            "name": f"User {i}",
            "country": rng.choice(_COUNTRIES),
            "signup_date": now - timedelta(days=days_ago,
                                           seconds=rng.randint(0, 86399)),
            "signup_source": rng.choice(_SIGNUP_SOURCES),
            "is_active": 1 if rng.random() > 0.05 else 0,
        })
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO demo.users "
            "(user_id, email, name, country, signup_date, signup_source, is_active) "
            "VALUES (:user_id, :email, :name, :country, :signup_date, "
            ":signup_source, :is_active)"
        ), rows)
    logger.info("Seeded %d users", count)


def _seed_products(engine, count: int) -> None:
    now = datetime.now(UTC)
    rng = random.Random(43)
    rows = [
        {
            "product_id": i,
            "name": f"Product {i}",
            "category": rng.choice(_CATEGORIES),
            "price": round(rng.uniform(5, 500), 2),
            "created_at": now - timedelta(days=rng.randint(0, 730)),
        }
        for i in range(1, count + 1)
    ]
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO demo.products "
            "(product_id, name, category, price, created_at) "
            "VALUES (:product_id, :name, :category, :price, :created_at)"
        ), rows)
    logger.info("Seeded %d products", count)


def _seed_orders(engine, count: int, max_user_id: int, max_product_id: int) -> None:
    now = datetime.now(UTC)
    rng = random.Random(44)
    rows = []
    for i in range(1, count + 1):
        qty = rng.randint(1, 5)
        price = round(rng.uniform(5, 500) * qty, 2)
        rows.append({
            "order_id": i,
            "user_id": rng.randint(1, max_user_id),
            "product_id": rng.randint(1, max_product_id),
            "status": rng.choice(_STATUSES),
            "quantity": qty,
            "total_price": price,
            "created_at": now - timedelta(days=rng.randint(0, 90),
                                          seconds=rng.randint(0, 86399)),
        })
    # ClickHouse executemany via clickhouse-sqlalchemy works fine for batches
    # in this range; for >>100k rows you'd switch to Native protocol bulk insert.
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO demo.orders "
            "(order_id, user_id, product_id, status, quantity, total_price, created_at) "
            "VALUES (:order_id, :user_id, :product_id, :status, :quantity, "
            ":total_price, :created_at)"
        ), rows)
    logger.info("Seeded %d orders", count)


def _seed_events(engine, count: int, max_user_id: int) -> None:
    now = datetime.now(UTC)
    rng = random.Random(45)
    rows = [
        {
            "event_id": uuid.uuid4().hex,
            "user_id": rng.randint(1, max_user_id),
            "event_type": rng.choice(_EVENT_TYPES),
            "server_id": rng.choice(_SERVER_IDS),
            "device_type": rng.choice(_DEVICE_TYPES),
            "created_at": now - timedelta(seconds=rng.randint(0, 14 * 86400)),
        }
        for _ in range(count)
    ]
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO demo.events "
            "(event_id, user_id, event_type, server_id, device_type, created_at) "
            "VALUES (:event_id, :user_id, :event_type, :server_id, "
            ":device_type, :created_at)"
        ), rows)
    logger.info("Seeded %d events", count)


# ── CLI ────────────────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description="Seed local ClickHouse demo target.")
    parser.add_argument("--users", type=int, default=2_000)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument("--orders", type=int, default=5_000)
    parser.add_argument("--events", type=int, default=10_000)
    parser.add_argument(
        "--reset", action="store_true",
        help="TRUNCATE all demo tables before seeding (destructive!)",
    )
    parser.add_argument(
        "--url", default=os.environ.get("CLICKHOUSE_URL", DEFAULT_URL),
        help=f"ClickHouse SQLAlchemy URL (default: {DEFAULT_URL})",
    )
    args = parser.parse_args()

    engine = create_engine(args.url)
    try:
        _apply_schema(engine)
        logger.info("Schema applied.")
        if args.reset:
            _reset_tables(engine)
            logger.info("Tables truncated.")
        _seed_users(engine, args.users)
        _seed_products(engine, args.products)
        _seed_orders(engine, args.orders, args.users, args.products)
        _seed_events(engine, args.events, args.users)
        logger.info(
            "Done. demo db now has ~%d rows total across users/products/orders/events.",
            args.users + args.products + args.orders + args.events,
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
