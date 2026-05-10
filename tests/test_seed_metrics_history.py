from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from scripts.seed_metrics_history import (
    DAYS,
    INTERVAL_MINUTES,
    TABLES,
    _BASE_ROWS,
    _DRIFT_COLUMNS,
    _GROWTH_PER_DAY,
    _MARKETING_STEP_UP,
    _NULL_FRACTIONS,
    _compute_base_rows,
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
    spike = _null_rate("orders", 11.0)
    assert spike > normal + 0.10


def test_orders_no_spike_outside_window():
    # Days 2 (before spike) and 13 (after spike) should both be at baseline.
    assert _null_rate("orders", 2.0) < 0.10
    assert _null_rate("orders", 13.0) < 0.10


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


def test_generate_emits_all_metric_types(all_rows):
    assert {r["metric_name"] for r in all_rows} == {
        "row_count", "null_rate", "size_bytes",
        "column_distribution", "null_count",
    }


def test_generate_approx_row_count(all_rows):
    ts_count = DAYS * 24 * 60 // INTERVAL_MINUTES
    # row_count + null_rate + size_bytes per (table, tick)
    expected_chart = ts_count * len(TABLES) * 3
    expected_drift = (DAYS + 1) * len(_DRIFT_COLUMNS)
    # null_count rows only emitted on the final tick
    expected_null_counts = sum(len(cols) for cols in _NULL_FRACTIONS.values())
    expected = expected_chart + expected_drift + expected_null_counts
    assert abs(len(all_rows) - expected) <= len(TABLES) * 4 + len(_DRIFT_COLUMNS)


def test_generate_distribution_rows_have_buckets(all_rows):
    dist = [r for r in all_rows if r["metric_name"] == "column_distribution"]
    assert dist, "no distribution rows generated"
    for r in dist:
        assert "buckets" in r["tags"]
        assert all("value" in b and "count" in b for b in r["tags"]["buckets"])


def test_generate_null_rate_rows_have_column_tag(all_rows):
    null_rate_rows = [r for r in all_rows if r["metric_name"] == "null_rate"]
    assert all("tags" in r and "column" in r["tags"] for r in null_rate_rows)


def test_generate_all_row_counts_positive(all_rows):
    counts = [r for r in all_rows if r["metric_name"] == "row_count"]
    assert all(r["value"] > 0 for r in counts)


def test_generate_orders_spike_visible(all_rows, gen_start):
    # Sample comfortably inside the seed's 10.5..11.5 spike window so
    # boundary rounding (test's gen_start vs seeder's start) doesn't trip us.
    spike = [
        r for r in all_rows
        if r["table_name"] == "orders"
        and r["metric_name"] == "null_rate"
        and 10.7 < (r["ts"] - gen_start).total_seconds() / 86400 < 11.3
    ]
    assert spike, "No spike rows found in window"
    assert all(r["value"] > 0.15 for r in spike)


def test_generate_users_marketing_step_up(all_rows, gen_start):
    before = [
        r for r in all_rows
        if r["table_name"] == "users" and r["metric_name"] == "row_count"
        and 9.5 < (r["ts"] - gen_start).total_seconds() / 86400 < 10.9
    ]
    after = [
        r for r in all_rows
        if r["table_name"] == "users" and r["metric_name"] == "row_count"
        and 11.1 < (r["ts"] - gen_start).total_seconds() / 86400 < 12.5
    ]
    assert before and after
    avg_before = sum(r["value"] for r in before) / len(before)
    avg_after = sum(r["value"] for r in after) / len(after)
    # Step-up is _MARKETING_STEP_UP; allow ±20% noise margin.
    assert avg_after - avg_before > _MARKETING_STEP_UP * 0.8


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


# --- _compute_base_rows ---

def _make_engine(counts: dict[str, int]):
    """Return a mock SQLAlchemy engine that returns given counts per table."""
    engine = MagicMock()
    conn = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    def scalar_for(table):
        return counts[table]
    conn.execute.side_effect = lambda q: MagicMock(scalar=MagicMock(return_value=scalar_for(
        next(t for t in TABLES if t in str(q))
    )))
    return engine


def test_compute_base_rows_math():
    counts = {"users": 5_000, "products": 500, "orders": 10_000, "events": 20_000}
    engine = _make_engine(counts)
    result = _compute_base_rows(engine)
    # users: 5000 - 15*14 - 1000 = 3790
    assert abs(result["users"] - (5_000 - _GROWTH_PER_DAY["users"] * DAYS - _MARKETING_STEP_UP)) < 1
    # products: 500 - 0 = 500
    assert result["products"] == 500
    # orders: 10000 - 60*14 = 9160
    assert abs(result["orders"] - (10_000 - _GROWTH_PER_DAY["orders"] * DAYS)) < 1
    # events: 20000 - 300*14 = 15800
    assert abs(result["events"] - (20_000 - _GROWTH_PER_DAY["events"] * DAYS)) < 1


def test_compute_base_rows_floor():
    # Even with tiny counts, result stays >= 100
    counts = {"users": 50, "products": 10, "orders": 50, "events": 50}
    engine = _make_engine(counts)
    result = _compute_base_rows(engine)
    for t in TABLES:
        assert result[t] >= 100


def test_compute_base_rows_ending_matches_live():
    """History end-value (day 14) must be within 5% of the live count."""
    counts = {"users": 5_000, "products": 500, "orders": 10_000, "events": 20_000}
    engine = _make_engine(counts)
    base = _compute_base_rows(engine)
    ts = datetime.now(timezone.utc)
    for table in TABLES:
        end_value = _row_count(table, float(DAYS), ts, base)
        assert abs(end_value - counts[table]) / counts[table] < 0.05, (
            f"{table}: history end {end_value:.0f} diverges >5% from live {counts[table]}"
        )


# --- main with live_engine ---

def test_main_uses_live_engine():
    counts = {"users": 5_000, "products": 500, "orders": 10_000, "events": 20_000}
    engine = _make_engine(counts)
    collected_base: list[dict] = []

    original_generate = _generate.__wrapped__ if hasattr(_generate, "__wrapped__") else None

    with patch("scripts.seed_metrics_history._compute_base_rows", return_value={"users": 99, "products": 99, "orders": 99, "events": 99}) as mock_compute, \
         patch("scripts.seed_metrics_history.save_metrics", return_value=0), \
         patch("scripts.seed_metrics_history.save_schema_events"):
        main(live_engine=engine)
        mock_compute.assert_called_once_with(engine)


def test_main_fallback_on_engine_error():
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("DB unavailable")

    with patch("scripts.seed_metrics_history.save_metrics", return_value=0), \
         patch("scripts.seed_metrics_history.save_schema_events"), \
         patch("scripts.seed_metrics_history._generate", return_value=iter([])) as mock_gen:
        main(live_engine=engine)
        # Must fall back to _BASE_ROWS, not crash
        mock_gen.assert_called_once_with(_BASE_ROWS)


def test_main_local_only_uses_base_rows():
    with patch("scripts.seed_metrics_history.save_metrics", return_value=0), \
         patch("scripts.seed_metrics_history.save_schema_events"), \
         patch("scripts.seed_metrics_history._generate", return_value=iter([])) as mock_gen:
        main(live_engine=None)
        mock_gen.assert_called_once_with(_BASE_ROWS)


# --- main ---

def test_main_calls_save_metrics():
    # batch.clear() mutates the list in-place after each save_metrics call,
    # so we capture sizes via side_effect rather than inspecting call_args_list afterwards
    saved_counts: list[int] = []

    def capture(batch):
        saved_counts.append(len(batch))
        return len(batch)

    with patch("scripts.seed_metrics_history.save_metrics", side_effect=capture), \
         patch("scripts.seed_metrics_history.save_schema_events"):
        main()

    assert saved_counts, "save_metrics was never called"
    total_saved = sum(saved_counts)
    ts_count = DAYS * 24 * 60 // INTERVAL_MINUTES
    expected_chart = ts_count * len(TABLES) * 3
    expected_drift = (DAYS + 1) * len(_DRIFT_COLUMNS)
    expected_null_counts = sum(len(cols) for cols in _NULL_FRACTIONS.values())
    expected = expected_chart + expected_drift + expected_null_counts
    assert abs(total_saved - expected) <= len(TABLES) * 4 + len(_DRIFT_COLUMNS)
