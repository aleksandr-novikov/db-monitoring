from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from faker import Faker

from scripts.seed_target_db import (
    _CATEGORIES,
    _COUNTRIES,
    _DEVICE_TYPES,
    _EVENT_TYPES,
    _SERVER_IDS,
    _SIGNUP_SOURCES,
    _STATUSES,
    _chunks,
    _seed_events,
    _seed_orders,
    _seed_products,
    _seed_users,
)

fake = Faker()


@pytest.fixture
def mock_engine():
    """Mock engine that captures all rows passed to conn.execute(stmt, rows)."""
    captured: list[dict] = []

    conn = MagicMock()

    def _capture(stmt, data=None):
        if isinstance(data, list):
            captured.extend(data)
        return MagicMock()

    conn.execute.side_effect = _capture

    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)

    engine = MagicMock()
    engine.begin.return_value = ctx
    engine.connect.return_value = ctx

    return engine, captured


# --- _chunks ---

def test_chunks_basic():
    assert list(_chunks(list(range(7)), 3)) == [[0, 1, 2], [3, 4, 5], [6]]


def test_chunks_exact_multiple():
    assert list(_chunks(list(range(6)), 3)) == [[0, 1, 2], [3, 4, 5]]


def test_chunks_empty():
    assert list(_chunks([], 5)) == []


def test_chunks_larger_than_list():
    assert list(_chunks([1, 2], 10)) == [[1, 2]]


# --- _seed_users ---

def test_seed_users_row_count(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, fake, 500)
    assert len(captured) == 500


def test_seed_users_null_email_rate(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, fake, 2000)
    null_count = sum(1 for r in captured if r["email"] is None)
    # ~5% of 2000 = 100, ±2σ ≈ ±14 → expect 70–130
    assert 70 < null_count < 130


def test_seed_users_valid_signup_source(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, fake, 100)
    assert all(r["signup_source"] in _SIGNUP_SOURCES for r in captured)


def test_seed_users_valid_country(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, fake, 100)
    assert all(r["country"] in _COUNTRIES for r in captured)


def test_seed_users_age_in_range(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, fake, 100)
    assert all(18 <= r["age"] <= 75 for r in captured)


def test_seed_users_timestamps_are_datetimes(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, fake, 50)
    for row in captured:
        assert isinstance(row["created_at"], datetime)
        assert isinstance(row["updated_at"], datetime)


# --- _seed_products ---

def test_seed_products_row_count(mock_engine):
    engine, captured = mock_engine
    _seed_products(engine, fake, 200)
    assert len(captured) == 200


def test_seed_products_valid_categories(mock_engine):
    engine, captured = mock_engine
    _seed_products(engine, fake, 100)
    assert all(r["category"] in _CATEGORIES for r in captured)


def test_seed_products_cost_price_below_price(mock_engine):
    engine, captured = mock_engine
    _seed_products(engine, fake, 200)
    assert all(r["cost_price"] < r["price"] for r in captured)


def test_seed_products_return_rate_bounded(mock_engine):
    engine, captured = mock_engine
    _seed_products(engine, fake, 200)
    assert all(0.0 <= r["return_rate"] <= 1.0 for r in captured)


# --- _seed_orders ---

def test_seed_orders_injects_duplicates(mock_engine):
    engine, captured = mock_engine
    user_ids = [str(i) for i in range(1, 11)]
    _seed_orders(engine, user_ids, 100)
    assert len(captured) == 105  # 100 + 5 duplicates


def test_seed_orders_valid_user_ids(mock_engine):
    engine, captured = mock_engine
    user_ids = ["u1", "u2", "u3"]
    _seed_orders(engine, user_ids, 50)
    assert all(r["user_id"] in user_ids for r in captured)


def test_seed_orders_valid_status(mock_engine):
    engine, captured = mock_engine
    _seed_orders(engine, ["u1"], 50)
    assert all(r["status"] in _STATUSES for r in captured)


def test_seed_orders_positive_amount(mock_engine):
    engine, captured = mock_engine
    _seed_orders(engine, ["u1", "u2"], 50)
    assert all(r["amount"] > 0 for r in captured)


# --- _seed_events ---

def test_seed_events_row_count(mock_engine):
    engine, captured = mock_engine
    _seed_events(engine, fake, ["u1", "u2", "u3"], 300)
    assert len(captured) == 300


def test_seed_events_valid_event_types(mock_engine):
    engine, captured = mock_engine
    _seed_events(engine, fake, ["u1", "u2"], 200)
    assert all(r["event_type"] in _EVENT_TYPES for r in captured)


def test_seed_events_valid_server_ids(mock_engine):
    engine, captured = mock_engine
    _seed_events(engine, fake, ["u1", "u2"], 200)
    assert all(r["server_id"] in _SERVER_IDS for r in captured)


def test_seed_events_valid_device_types(mock_engine):
    engine, captured = mock_engine
    _seed_events(engine, fake, ["u1", "u2"], 200)
    assert all(r["device_type"] in _DEVICE_TYPES for r in captured)


def test_seed_events_recent_have_higher_null_rate(mock_engine):
    """Events from last 7 days must have significantly more NULL ip_address than older ones."""
    engine, captured = mock_engine
    _seed_events(engine, fake, list(range(1, 20)), 2000)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)

    recent = [r for r in captured if r["created_at"] >= cutoff]
    old = [r for r in captured if r["created_at"] < cutoff]

    if not recent or not old:
        pytest.skip("Not enough data in one of the age buckets")

    recent_null_rate = sum(1 for r in recent if r["ip_address"] is None) / len(recent)
    old_null_rate = sum(1 for r in old if r["ip_address"] is None) / len(old)

    # recent ~25%, old ~2% → recent should be at least 5× higher
    assert recent_null_rate > old_null_rate * 5
