from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from scripts.seed_metrics_history import (
    DAYS,
    INTERVAL_MINUTES,
    TABLES,
    _null_rate,
    _row_count,
    _generate,
    main,
)


# --- _row_count ---

def test_row_count_positive():
    ts = datetime.now(timezone.utc)
    assert _row_count("users", 7.0, ts) > 0


def test_row_count_grows_over_time():
    ts = datetime.now(timezone.utc)
    early = _row_count("orders", 1.0, ts)
    late = _row_count("orders", 13.0, ts)
    assert late > early


def test_row_count_products_drop_at_day_10():
    ts = datetime.now(timezone.utc)
    before = _row_count("products", 9.5, ts)
    at_drop = _row_count("products", 9.95, ts)
    assert at_drop < before * 0.6


# --- _null_rate ---

def test_null_rate_bounded():
    for table in TABLES:
        for day in range(DAYS + 1):
            rate = _null_rate(table, float(day))
            assert 0.0 <= rate <= 1.0


def test_orders_spike_in_window():
    normal = _null_rate("orders", 3.0)
    spike = _null_rate("orders", 6.5)
    assert spike > normal + 0.10


def test_orders_no_spike_outside_window():
    rate = _null_rate("orders", 2.0)
    assert rate < 0.10


def test_events_null_rate_step_up_in_last_7_days():
    early = _null_rate("events", 2.0)
    late = _null_rate("events", float(DAYS) - 1)
    # early ≈ 0.02, late ≈ 0.25 (step-up after day 7) → difference should be clearly visible
    assert late - early > 0.15


def test_events_stable_at_start():
    rate = _null_rate("events", 2.0)
    assert rate < 0.10


# --- _generate ---

@pytest.fixture(scope="module")
def all_rows():
    return list(_generate())


@pytest.fixture(scope="module")
def gen_start():
    return datetime.now(timezone.utc) - timedelta(days=DAYS)


def test_generate_covers_all_tables(all_rows):
    assert {r["table_name"] for r in all_rows} == set(TABLES)


def test_generate_both_metrics(all_rows):
    assert {r["metric_name"] for r in all_rows} == {"row_count", "null_rate"}


def test_generate_approx_row_count(all_rows):
    expected = DAYS * 24 * 60 // INTERVAL_MINUTES * len(TABLES) * 2
    assert abs(len(all_rows) - expected) <= len(TABLES) * 4


def test_generate_null_rate_rows_have_column_tag(all_rows):
    null_rate_rows = [r for r in all_rows if r["metric_name"] == "null_rate"]
    assert all("tags" in r and "column" in r["tags"] for r in null_rate_rows)


def test_generate_all_row_counts_positive(all_rows):
    counts = [r for r in all_rows if r["metric_name"] == "row_count"]
    assert all(r["value"] > 0 for r in counts)


def test_generate_orders_spike_visible(all_rows, gen_start):
    spike = [
        r for r in all_rows
        if r["table_name"] == "orders"
        and r["metric_name"] == "null_rate"
        and 5.8 < (r["ts"] - gen_start).total_seconds() / 86400 < 7.2
    ]
    assert spike, "No spike rows found in window"
    assert all(r["value"] > 0.15 for r in spike)


def test_generate_events_null_rate_rises(all_rows, gen_start):
    early = [
        r for r in all_rows
        if r["table_name"] == "events" and r["metric_name"] == "null_rate"
        and (r["ts"] - gen_start).total_seconds() / 86400 < 5
    ]
    late = [
        r for r in all_rows
        if r["table_name"] == "events" and r["metric_name"] == "null_rate"
        and (r["ts"] - gen_start).total_seconds() / 86400 > DAYS - 2
    ]
    avg_early = sum(r["value"] for r in early) / len(early)
    avg_late = sum(r["value"] for r in late) / len(late)
    # early ≈ 0.02, late ≈ 0.25 (step-up after day 7) → difference should be clearly visible
    assert avg_late - avg_early > 0.15


def test_generate_products_row_count_drop(all_rows, gen_start):
    before = [
        r for r in all_rows
        if r["table_name"] == "products" and r["metric_name"] == "row_count"
        and 9.0 < (r["ts"] - gen_start).total_seconds() / 86400 < 9.8
    ]
    at_drop = [
        r for r in all_rows
        if r["table_name"] == "products" and r["metric_name"] == "row_count"
        and 9.9 < (r["ts"] - gen_start).total_seconds() / 86400 < 10.05
    ]
    if not before or not at_drop:
        pytest.skip("Not enough rows near the drop point")
    avg_before = sum(r["value"] for r in before) / len(before)
    avg_drop = sum(r["value"] for r in at_drop) / len(at_drop)
    assert avg_drop < avg_before * 0.6


# --- main ---

def test_main_calls_save_metrics():
    # batch.clear() mutates the list in-place after each save_metrics call,
    # so we capture sizes via side_effect rather than inspecting call_args_list afterwards
    saved_counts: list[int] = []

    def capture(batch):
        saved_counts.append(len(batch))
        return len(batch)

    with patch("scripts.seed_metrics_history.save_metrics", side_effect=capture):
        main()

    assert saved_counts, "save_metrics was never called"
    total_saved = sum(saved_counts)
    expected = DAYS * 24 * 60 // INTERVAL_MINUTES * len(TABLES) * 2
    assert abs(total_saved - expected) <= len(TABLES) * 4
