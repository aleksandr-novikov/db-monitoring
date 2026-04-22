from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from scripts.seed_target_db import (
    _CATEGORIES,
    _EVENT_TYPES,
    _chunks,
    _seed_events,
    _seed_orders,
    _seed_products,
    _seed_users,
)


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
    _seed_users(engine, 500)
    assert len(captured) == 500


def test_seed_users_null_email_rate(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, 2000)
    null_count = sum(1 for r in captured if r["email"] is None)
    # expect ~5% ± 3% (100 ± 60)
    assert 20 < null_count < 200


def test_seed_users_required_fields(mock_engine):
    engine, captured = mock_engine
    _seed_users(engine, 50)
    for row in captured:
        assert "name" in row and row["name"]
        assert "created_at" in row
        assert isinstance(row["created_at"], datetime)


# --- _seed_products ---

def test_seed_products_row_count(mock_engine):
    engine, captured = mock_engine
    _seed_products(engine, 200)
    assert len(captured) == 200


def test_seed_products_valid_categories(mock_engine):
    engine, captured = mock_engine
    _seed_products(engine, 200)
    assert all(r["category"] in _CATEGORIES for r in captured)


def test_seed_products_positive_price(mock_engine):
    engine, captured = mock_engine
    _seed_products(engine, 100)
    assert all(r["price"] > 0 for r in captured)


# --- _seed_orders ---

def test_seed_orders_injects_duplicates(mock_engine):
    engine, captured = mock_engine
    user_ids = list(range(1, 11))
    product_ids = list(range(1, 6))
    _seed_orders(engine, user_ids, product_ids, 100)
    # 100 base + 5 duplicates
    assert len(captured) == 105


def test_seed_orders_valid_user_and_product_ids(mock_engine):
    engine, captured = mock_engine
    user_ids = [10, 20, 30]
    product_ids = [1, 2]
    _seed_orders(engine, user_ids, product_ids, 50)
    assert all(r["user_id"] in user_ids for r in captured)
    assert all(r["product_id"] in product_ids for r in captured)


def test_seed_orders_positive_totals(mock_engine):
    engine, captured = mock_engine
    _seed_orders(engine, [1, 2], [1, 2], 50)
    assert all(r["total"] > 0 for r in captured)


# --- _seed_events ---

def test_seed_events_row_count(mock_engine):
    engine, captured = mock_engine
    _seed_events(engine, [1, 2, 3], 300)
    assert len(captured) == 300


def test_seed_events_valid_event_types(mock_engine):
    engine, captured = mock_engine
    _seed_events(engine, [1, 2, 3], 200)
    assert all(r["event_type"] in _EVENT_TYPES for r in captured)


def test_seed_events_recent_have_higher_null_rate(mock_engine):
    """Events from last 7 days must have significantly more NULL payloads than older ones."""
    engine, captured = mock_engine
    _seed_events(engine, list(range(1, 20)), 2000)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=7)

    recent = [r for r in captured if r["occurred_at"] >= cutoff]
    old = [r for r in captured if r["occurred_at"] < cutoff]

    if not recent or not old:
        pytest.skip("Not enough data in one of the age buckets")

    recent_null_rate = sum(1 for r in recent if r["payload"] is None) / len(recent)
    old_null_rate = sum(1 for r in old if r["payload"] is None) / len(old)

    # recent ~25%, old ~2% → recent should be at least 5× higher
    assert recent_null_rate > old_null_rate * 5
