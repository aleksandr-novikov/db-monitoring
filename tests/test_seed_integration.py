"""
Integration test: seed_target_db → seed_metrics_history alignment.

Verifies that after seeding the target DB and running seed_metrics_history
with live_engine, the last row_count point in the synthetic history is
within ±5% of the real table counts.

This test catches the class of bug introduced in commit b7fb39b (Apr 23 2026)
where seed_target_db.py defaults were reduced 10x but seed_metrics_history.py
_BASE_ROWS were not updated — causing a visible cliff on the chart.
"""
from __future__ import annotations

import random
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, text

from scripts.seed_metrics_history import DAYS, TABLES, _compute_base_rows, _row_count


# Minimal SQLite-compatible schema (seed_target_db uses Postgres types; we
# replicate only the columns that _seed_* functions actually INSERT).
_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    email   TEXT,
    age     INTEGER,
    country TEXT,
    signup_source TEXT,
    created_at TEXT,
    updated_at TEXT
);
CREATE TABLE IF NOT EXISTS products (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT,
    category        TEXT,
    price           REAL,
    cost_price      REAL,
    stock           INTEGER,
    avg_daily_sales REAL,
    return_rate     REAL,
    price_updated_at TEXT,
    created_at      TEXT,
    updated_at      TEXT
);
CREATE TABLE IF NOT EXISTS orders (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id               INTEGER,
    amount                REAL,
    items_count           INTEGER,
    discount              REAL,
    shipping_country      TEXT,
    status                TEXT,
    has_prior_events      INTEGER,
    user_orders_last_1h   INTEGER,
    amount_vs_avg_ratio   REAL,
    created_at            TEXT
);
CREATE TABLE IF NOT EXISTS events (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id             INTEGER,
    session_id          TEXT,
    event_type          TEXT,
    prev_event_type     TEXT,
    prev_event_gap_s    REAL,
    duration_ms         INTEGER,
    events_in_session   INTEGER,
    ip_address          TEXT,
    ip_events_last_1h   INTEGER,
    server_id           TEXT,
    device_type         TEXT,
    is_bot_suspected    INTEGER,
    created_at          TEXT
);
"""

_N_USERS = 5_000
_N_PRODUCTS = 500
_N_ORDERS = 10_000
_N_EVENTS = 20_000


@pytest.fixture(scope="module")
def target_engine():
    """SQLite in-memory DB seeded with default seed_target_db row counts."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    with engine.begin() as conn:
        for stmt in _SQLITE_SCHEMA.split(";"):
            clean = stmt.strip()
            if clean:
                conn.execute(text(clean))

    # Insert rows directly — matches what seed_target_db.py would produce.
    random.seed(42)
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO users (email, age, country, signup_source, created_at, updated_at) "
                 "VALUES (:email, :age, :country, :signup_source, :created_at, :updated_at)"),
            [{"email": f"u{i}@x.com", "age": 30, "country": "US", "signup_source": "web",
              "created_at": "2026-01-01", "updated_at": "2026-01-01"}
             for i in range(_N_USERS)],
        )
        conn.execute(
            text("INSERT INTO products (name, category, price, cost_price, stock, "
                 "avg_daily_sales, return_rate, created_at, updated_at) "
                 "VALUES (:name, :category, :price, :cost_price, :stock, "
                 ":avg_daily_sales, :return_rate, :created_at, :updated_at)"),
            [{"name": f"p{i}", "category": "Electronics", "price": 100.0, "cost_price": 50.0,
              "stock": 100, "avg_daily_sales": 1.0, "return_rate": 0.05,
              "created_at": "2026-01-01", "updated_at": "2026-01-01"}
             for i in range(_N_PRODUCTS)],
        )
        user_ids = list(range(1, _N_USERS + 1))
        conn.execute(
            text("INSERT INTO orders (user_id, amount, items_count, discount, shipping_country, "
                 "status, has_prior_events, user_orders_last_1h, amount_vs_avg_ratio, created_at) "
                 "VALUES (:user_id, :amount, :items_count, :discount, :shipping_country, "
                 ":status, :has_prior_events, :user_orders_last_1h, :amount_vs_avg_ratio, :created_at)"),
            [{"user_id": random.choice(user_ids), "amount": 100.0, "items_count": 2,
              "discount": 0.0, "shipping_country": "US", "status": "delivered",
              "has_prior_events": 1, "user_orders_last_1h": 0, "amount_vs_avg_ratio": 1.0,
              "created_at": "2026-01-01"}
             for _ in range(_N_ORDERS)],
        )
        conn.execute(
            text("INSERT INTO events (user_id, session_id, event_type, duration_ms, "
                 "events_in_session, ip_events_last_1h, server_id, device_type, "
                 "is_bot_suspected, created_at) "
                 "VALUES (:user_id, :session_id, :event_type, :duration_ms, "
                 ":events_in_session, :ip_events_last_1h, :server_id, :device_type, "
                 ":is_bot_suspected, :created_at)"),
            [{"user_id": random.choice(user_ids), "session_id": f"s{i}", "event_type": "view",
              "duration_ms": 200, "events_in_session": 3, "ip_events_last_1h": 1,
              "server_id": "server-1", "device_type": "mobile", "is_bot_suspected": 0,
              "created_at": "2026-01-01"}
             for i in range(_N_EVENTS)],
        )
    return engine


@pytest.fixture(scope="module")
def computed_base(target_engine):
    return _compute_base_rows(target_engine)


@pytest.fixture(scope="module")
def live_counts(target_engine):
    with target_engine.connect() as conn:
        return {t: conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar() for t in TABLES}


def test_live_counts_match_seed(live_counts):
    assert live_counts["users"] == _N_USERS
    assert live_counts["products"] == _N_PRODUCTS
    assert live_counts["orders"] == _N_ORDERS
    assert live_counts["events"] == _N_EVENTS


def test_computed_base_is_positive(computed_base):
    for table in TABLES:
        assert computed_base[table] > 0, f"{table} base <= 0"


def test_history_end_within_5pct_of_live(computed_base, live_counts):
    """Core alignment check — the bug this test guards against:
    history ends at ~50k while Supabase has ~5k → cliff on the chart.
    """
    ts = datetime.now(timezone.utc)
    for table in TABLES:
        history_end = _row_count(table, float(DAYS), ts, computed_base)
        live = live_counts[table]
        tolerance = live * 0.05
        assert abs(history_end - live) <= tolerance, (
            f"{table}: history end {history_end:.0f} vs live {live} — "
            f"diff {abs(history_end - live):.0f} exceeds 5% tolerance ({tolerance:.0f}). "
            f"If seed_target_db defaults changed, check _BASE_ROWS in seed_metrics_history.py."
        )
