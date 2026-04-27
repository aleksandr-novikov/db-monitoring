"""
Seed the monitored DB (DATABASE_URL) with realistic test data.

Uses scripts/schema.sql — does not redefine tables inline.
Run with --reset to truncate before seeding (destructive!).

Usage:
    python -m scripts.seed_target_db
    python -m scripts.seed_target_db --reset
    python -m scripts.seed_target_db --users 50000 --orders 100000 --reset
    # Full demo dataset (~350k rows, ~10 min on Supabase free tier):
    python -m scripts.seed_target_db --users 50000 --products 1000 --orders 100000 --events 200000 --reset
"""
import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker
from sqlalchemy import text

from app.db import get_engine

SCHEMA_PATH = Path(__file__).resolve().parent / "schema.sql"

_CATEGORIES = ["Electronics", "Clothing", "Food", "Books", "Home", "Sports"]
_COUNTRIES = ["RU", "US", "DE", "GB", "FR", "CN", "BR"]
_SIGNUP_SOURCES = ["web", "mobile", "api", "referral"]
_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
_EVENT_TYPES = ["login", "view", "add_to_cart", "checkout", "purchase", "error", "logout"]
_SERVER_IDS = ["server-1", "server-2", "server-3"]
_DEVICE_TYPES = ["mobile", "desktop", "tablet"]


def _chunks(lst: list, n: int):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _apply_schema(engine) -> None:
    sql = SCHEMA_PATH.read_text()
    with engine.begin() as conn:
        for stmt in sql.split(";"):
            lines = [l for l in stmt.splitlines() if not l.strip().startswith("--")]
            clean = "\n".join(lines).strip()
            if clean:
                conn.execute(text(clean))


def _truncate(engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "TRUNCATE events, orders, products, users RESTART IDENTITY CASCADE"
        ))


def _get_ids(engine, table: str) -> list[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT id FROM {table}")).fetchall()
    return [str(r[0]) for r in rows]


def _seed_users(engine, fake: Faker, n: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = text("""
        INSERT INTO users (email, age, country, signup_source, created_at, updated_at)
        VALUES (:email, :age, :country, :signup_source, :created_at, :updated_at)
    """)
    rows = []
    for _ in range(n):
        created = now - timedelta(days=random.randint(0, 365))
        rows.append({
            "email": None if random.random() < 0.05 else fake.email(),  # ~5% NULL defect
            "age": random.randint(18, 75),
            "country": random.choice(_COUNTRIES),
            "signup_source": random.choice(_SIGNUP_SOURCES),
            "created_at": created,
            "updated_at": created,
        })
    with engine.begin() as conn:
        for chunk in _chunks(rows, 500):
            conn.execute(stmt, chunk)


def _seed_products(engine, fake: Faker, n: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = text("""
        INSERT INTO products
            (name, category, price, cost_price, stock, avg_daily_sales,
             return_rate, price_updated_at, created_at, updated_at)
        VALUES
            (:name, :category, :price, :cost_price, :stock, :avg_daily_sales,
             :return_rate, :price_updated_at, :created_at, :updated_at)
    """)
    rows = []
    for _ in range(n):
        price = round(random.uniform(10.0, 2000.0), 2)
        created = now - timedelta(days=random.randint(30, 365))
        rows.append({
            "name": fake.catch_phrase(),
            "category": random.choice(_CATEGORIES),
            "price": price,
            "cost_price": round(price * random.uniform(0.3, 0.8), 2),
            "stock": random.randint(0, 500),
            "avg_daily_sales": round(random.uniform(0.1, 50.0), 2),
            "return_rate": round(random.uniform(0.0, 0.12), 4),
            "price_updated_at": (
                now - timedelta(days=random.randint(0, 60))
                if random.random() > 0.3 else None
            ),
            "created_at": created,
            "updated_at": now - timedelta(days=random.randint(0, 30)),
        })
    with engine.begin() as conn:
        for chunk in _chunks(rows, 500):
            conn.execute(stmt, chunk)


def _seed_orders(engine, user_ids: list[str], n: int) -> None:
    now = datetime.now(timezone.utc)
    stmt = text("""
        INSERT INTO orders
            (user_id, amount, items_count, discount, shipping_country, status,
             has_prior_events, user_orders_last_1h, amount_vs_avg_ratio, created_at)
        VALUES
            (:user_id, :amount, :items_count, :discount, :shipping_country, :status,
             :has_prior_events, :user_orders_last_1h, :amount_vs_avg_ratio, :created_at)
    """)
    rows = []
    for _ in range(n):
        rows.append({
            "user_id": random.choice(user_ids),
            "amount": round(random.uniform(10.0, 2000.0), 2),
            "items_count": random.randint(1, 10),
            "discount": round(random.uniform(0.0, 0.3), 4),
            "shipping_country": random.choice(_COUNTRIES),
            "status": random.choice(_STATUSES),
            "has_prior_events": random.random() > 0.1,
            "user_orders_last_1h": random.randint(0, 3),
            "amount_vs_avg_ratio": round(random.uniform(0.3, 3.0), 4),
            "created_at": now - timedelta(days=random.randint(0, 180)),
        })
    # inject 5 exact duplicates — controlled defect
    for _ in range(5):
        rows.append(random.choice(rows).copy())
    with engine.begin() as conn:
        for chunk in _chunks(rows, 500):
            conn.execute(stmt, chunk)


def _seed_events(engine, fake: Faker, user_ids: list[str], n: int) -> None:
    """ip_address NULL-rate: ~2% for old events, ~25% for last 7 days — simulates logging regression."""
    now = datetime.now(timezone.utc)
    stmt = text("""
        INSERT INTO events
            (user_id, session_id, event_type, prev_event_type, prev_event_gap_s,
             duration_ms, events_in_session, ip_address, ip_events_last_1h,
             server_id, device_type, is_bot_suspected, created_at)
        VALUES
            (:user_id, :session_id, :event_type, :prev_event_type, :prev_event_gap_s,
             :duration_ms, :events_in_session, :ip_address, :ip_events_last_1h,
             :server_id, :device_type, :is_bot_suspected, :created_at)
    """)
    rows = []
    for _ in range(n):
        age_days = random.randint(0, 90)
        null_prob = 0.25 if age_days < 7 else 0.02
        prev_event = random.choice(_EVENT_TYPES) if random.random() > 0.2 else None
        rows.append({
            "user_id": random.choice(user_ids),
            "session_id": str(uuid.uuid4()),
            "event_type": random.choice(_EVENT_TYPES),
            "prev_event_type": prev_event,
            "prev_event_gap_s": round(random.uniform(0.01, 300.0), 3) if prev_event else None,
            "duration_ms": random.randint(10, 5000),
            "events_in_session": random.randint(1, 20),
            "ip_address": None if random.random() < null_prob else fake.ipv4(),
            "ip_events_last_1h": random.randint(0, 50),
            "server_id": random.choice(_SERVER_IDS),
            "device_type": random.choice(_DEVICE_TYPES),
            "is_bot_suspected": random.random() < 0.02,
            "created_at": now - timedelta(days=age_days, seconds=random.randint(0, 86400)),
        })
    with engine.begin() as conn:
        for chunk in _chunks(rows, 500):
            conn.execute(stmt, chunk)


def main(
    n_users: int = 5_000,
    n_products: int = 500,
    n_orders: int = 10_000,
    n_events: int = 20_000,
    reset: bool = False,
) -> None:
    random.seed(42)
    fake = Faker()
    Faker.seed(42)

    engine = get_engine()

    print("Applying schema from scripts/schema.sql...")
    _apply_schema(engine)

    if reset:
        print("Truncating existing data...")
        _truncate(engine)

    print(f"Seeding {n_users:,} users  (~5% email NULL)...")
    _seed_users(engine, fake, n_users)

    print(f"Seeding {n_products:,} products...")
    _seed_products(engine, fake, n_products)

    user_ids = _get_ids(engine, "users")

    print(f"Seeding {n_orders:,} orders  (+ 5 duplicates)...")
    _seed_orders(engine, user_ids, n_orders)

    print(f"Seeding {n_events:,} events  (growing ip_address NULL-rate in last 7 days)...")
    _seed_events(engine, fake, user_ids, n_events)

    print("\nDone! Seeded target DB:")
    print(f"  users    — {n_users:,} rows  (~5% email NULL)")
    print(f"  products — {n_products:,} rows")
    print(f"  orders   — {n_orders + 5:,} rows  (5 duplicates injected)")
    print(f"  events   — {n_events:,} rows  (ip_address NULL-rate ~2% old / ~25% last 7 days)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed monitored DB with test data.")
    parser.add_argument("--users", type=int, default=5_000)
    parser.add_argument("--products", type=int, default=500)
    parser.add_argument("--orders", type=int, default=10_000)
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument(
        "--reset", action="store_true",
        help="Truncate all tables before seeding (destructive — requires explicit flag)",
    )
    args = parser.parse_args()
    main(args.users, args.products, args.orders, args.events, reset=args.reset)
