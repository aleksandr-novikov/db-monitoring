"""
Seed the monitored DB (DATABASE_URL) with realistic test data.

Creates tables: users, products, orders, events — with controlled defects
for monitoring demos (NULL-rate, duplicates, growing null_rate in events).

Usage:
    python -m scripts.seed_target_db
    python -m scripts.seed_target_db --users 10000 --orders 20000
"""
import argparse
import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from sqlalchemy import text

from app.db import get_engine

fake = Faker()
random.seed(42)
Faker.seed(42)

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id          SERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        email       TEXT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        id          SERIAL PRIMARY KEY,
        name        TEXT NOT NULL,
        category    TEXT NOT NULL,
        price       NUMERIC(10, 2) NOT NULL,
        stock       INT NOT NULL DEFAULT 0
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        id          SERIAL PRIMARY KEY,
        user_id     INT NOT NULL,
        product_id  INT NOT NULL,
        quantity    INT NOT NULL DEFAULT 1,
        total       NUMERIC(10, 2) NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id          SERIAL PRIMARY KEY,
        user_id     INT,
        event_type  TEXT NOT NULL,
        payload     TEXT,
        occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
    """,
]

_CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Home", "Sports"]
_EVENT_TYPES = ["login", "logout", "purchase", "view", "search", "error"]


def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _create_schema(engine) -> None:
    with engine.begin() as conn:
        for stmt in _DDL:
            conn.execute(text(stmt))


def _truncate(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE users, products, orders, events RESTART IDENTITY CASCADE"
        ))


def _get_ids(engine, table: str) -> list[int]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT id FROM {table} ORDER BY id")).fetchall()
    return [r[0] for r in rows]


def _seed_users(engine, n: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = text(
        "INSERT INTO users (name, email, created_at) VALUES (:name, :email, :created_at)"
    )
    rows = [
        {
            "name": fake.name(),
            # ~5% NULL in email — controlled defect
            "email": None if random.random() < 0.05 else fake.email(),
            "created_at": now - timedelta(days=random.randint(0, 365)),
        }
        for _ in range(n)
    ]
    with engine.begin() as conn:
        for chunk in _chunks(rows, 1000):
            conn.execute(stmt, chunk)


def _seed_products(engine, n: int) -> None:
    stmt = text(
        "INSERT INTO products (name, category, price, stock)"
        " VALUES (:name, :category, :price, :stock)"
    )
    rows = [
        {
            "name": fake.catch_phrase(),
            "category": random.choice(_CATEGORIES),
            "price": round(random.uniform(1.0, 1000.0), 2),
            "stock": random.randint(0, 500),
        }
        for _ in range(n)
    ]
    with engine.begin() as conn:
        for chunk in _chunks(rows, 500):
            conn.execute(stmt, chunk)


def _seed_orders(engine, user_ids: list[int], product_ids: list[int], n: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = text(
        "INSERT INTO orders (user_id, product_id, quantity, total, created_at)"
        " VALUES (:user_id, :product_id, :quantity, :total, :created_at)"
    )
    rows = []
    for _ in range(n):
        qty = random.randint(1, 5)
        rows.append(
            {
                "user_id": random.choice(user_ids),
                "product_id": random.choice(product_ids),
                "quantity": qty,
                "total": round(qty * random.uniform(1.0, 500.0), 2),
                "created_at": now - timedelta(days=random.randint(0, 180)),
            }
        )

    # inject a few exact duplicates (same payload) — controlled defect
    for _ in range(5):
        rows.append(random.choice(rows).copy())

    with engine.begin() as conn:
        for chunk in _chunks(rows, 1000):
            conn.execute(stmt, chunk)


def _seed_events(engine, user_ids: list[int], n: int) -> None:
    """Events where payload NULL-rate grows in the last 7 days (simulates a recent regression)."""
    now = datetime.now(timezone.utc)
    stmt = text(
        "INSERT INTO events (user_id, event_type, payload, occurred_at)"
        " VALUES (:user_id, :event_type, :payload, :occurred_at)"
    )
    rows = []
    for _ in range(n):
        age_days = random.randint(0, 90)
        # older events: ~2% NULL; last 7 days: ~25% NULL — growing null_rate defect
        null_prob = 0.25 if age_days < 7 else 0.02
        rows.append(
            {
                "user_id": random.choice(user_ids) if random.random() > 0.05 else None,
                "event_type": random.choice(_EVENT_TYPES),
                "payload": (
                    None
                    if random.random() < null_prob
                    else f'{{"action": "{fake.word()}", "value": {random.randint(1, 100)}}}'
                ),
                "occurred_at": now - timedelta(
                    days=age_days, seconds=random.randint(0, 86400)
                ),
            }
        )
    with engine.begin() as conn:
        for chunk in _chunks(rows, 1000):
            conn.execute(stmt, chunk)


def main(
    n_users: int = 50_000,
    n_products: int = 1_000,
    n_orders: int = 100_000,
    n_events: int = 200_000,
) -> None:
    engine = get_engine()

    print("Creating schema...")
    _create_schema(engine)
    print("Truncating existing data...")
    _truncate(engine)

    print(f"Seeding {n_users:,} users  (~5 % email NULL)...")
    _seed_users(engine, n_users)

    print(f"Seeding {n_products:,} products...")
    _seed_products(engine, n_products)

    user_ids = _get_ids(engine, "users")
    product_ids = _get_ids(engine, "products")

    print(f"Seeding {n_orders:,} orders  (+ 5 duplicate rows)...")
    _seed_orders(engine, user_ids, product_ids, n_orders)

    print(f"Seeding {n_events:,} events  (growing payload NULL-rate in last 7 days)...")
    _seed_events(engine, user_ids, n_events)

    print("\nDone! Seeded target DB:")
    print(f"  users    — {n_users:,} rows  (~5 % email NULL)")
    print(f"  products — {n_products:,} rows")
    print(f"  orders   — {n_orders + 5:,} rows  (5 duplicates injected)")
    print(f"  events   — {n_events:,} rows  (payload NULL-rate ~2 % old / ~25 % last 7 days)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed monitored DB with test data.")
    parser.add_argument("--users", type=int, default=50_000)
    parser.add_argument("--products", type=int, default=1_000)
    parser.add_argument("--orders", type=int, default=100_000)
    parser.add_argument("--events", type=int, default=200_000)
    args = parser.parse_args()
    main(args.users, args.products, args.orders, args.events)
