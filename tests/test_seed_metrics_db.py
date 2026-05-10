import random
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import text

from scripts.seed_metrics_db import (
    ANOMALY_POINTS,
    BASELINE_NULL_RATE,
    CATEGORICAL_BASELINE_WEIGHTS,
    CATEGORICAL_BUCKETS,
    CATEGORICAL_TARGET_WEIGHTS,
    DEFAULT_PROFILE,
    DRIFT_ONSET_PROGRESS,
    NOISE_AMPLITUDE,
    NUMERIC_BASELINE_MEAN,
    NUMERIC_BUCKETS,
    NUMERIC_TARGET_MEAN,
    PROFILES,
    REGRESSION_DAYS,
    ROW_COUNT_START_FRACTION,
    SCHEMA_EVENT_PROFILES,
    WEEKLY_AMPLITUDE,
    TableProfile,
    TableSnapshot,
    _anomaly_multiplier,
    _build_timestamps,
    _categorical_buckets,
    _drift_factor,
    _generate_distribution_rows,
    _generate_metric_rows,
    _generate_schema_events,
    _null_rate_at,
    _numeric_buckets,
    _profile_for,
    _row_count_at,
    _seasonality_factor,
    main,
)


# Фиксированная среда (вторник 12:00 UTC) для функций, чувствительных
# к дню недели. Берём вторник, потому что cos((2-2)*2π/7) = 1, т.е.
# seasonality_factor ≈ 1+WEEKLY_AMPLITUDE (пик).
MID_WEEK = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)


# --- _build_timestamps ---


def test_build_timestamps_count_and_endpoint():
    end = datetime(2026, 5, 10, 12, 0, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    assert len(ts) == 14 * 24
    assert ts[-1] == end
    assert ts[0] == end - timedelta(hours=14 * 24 - 1)


def test_build_timestamps_15min_step_uniform():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=1, interval_minutes=15)
    assert len(ts) == 24 * 4
    deltas = {(b - a).total_seconds() for a, b in zip(ts, ts[1:])}
    assert deltas == {15 * 60}


def test_build_timestamps_min_one_tick():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=0, interval_minutes=60)
    assert ts == [end]


# --- _seasonality_factor ---


def test_seasonality_factor_peaks_midweek():
    # Понедельник 0..суббота-воскресенье ≈ нижняя половина недели.
    tuesday = datetime(2026, 5, 12, tzinfo=timezone.utc)  # weekday=1
    saturday = datetime(2026, 5, 9, tzinfo=timezone.utc)  # weekday=5
    assert _seasonality_factor(tuesday) > _seasonality_factor(saturday)


def test_seasonality_factor_within_amplitude():
    for day_offset in range(7):
        ts = datetime(2026, 5, 11, tzinfo=timezone.utc) + timedelta(days=day_offset)
        f = _seasonality_factor(ts)
        assert 1 - WEEKLY_AMPLITUDE - 1e-9 <= f <= 1 + WEEKLY_AMPLITUDE + 1e-9


# --- _anomaly_multiplier ---


def test_anomaly_multiplier_at_anchor_progress():
    # При большом n_ticks тик ровно на progress=p из ANOMALY_POINTS должен
    # получить заявленный множитель.
    for p, mult in ANOMALY_POINTS:
        assert _anomaly_multiplier(p, n_ticks=1000) == mult


def test_anomaly_multiplier_far_from_anchor_returns_one():
    assert _anomaly_multiplier(0.10, n_ticks=1000) == 1.0
    assert _anomaly_multiplier(0.40, n_ticks=1000) == 1.0


def test_anomaly_multiplier_one_tick_per_anchor():
    # На реальной длине окна (336 тиков = 14дн × 24) каждый якорь должен
    # затронуть ровно один тик.
    n = 336
    hits = sum(
        1 for i in range(n) if _anomaly_multiplier(i / (n - 1), n) != 1.0
    )
    assert hits == len(ANOMALY_POINTS)


# --- _row_count_at ---


def test_row_count_at_endpoints_within_seasonal_band():
    rng = random.Random(0)
    samples = [_row_count_at(1.0, 1000, MID_WEEK, rng) for _ in range(50)]
    # На progress=1: backfill=0, anomaly=1.0, шум ±0.5%, сезонность ≈ 1+WEEKLY_AMPLITUDE.
    upper = 1000 * (1 + WEEKLY_AMPLITUDE) * (1 + NOISE_AMPLITUDE) + 1
    lower = 1000 * (1 - WEEKLY_AMPLITUDE) * (1 - NOISE_AMPLITUDE) - 1
    assert all(lower <= s <= upper for s in samples)


def test_row_count_at_start_strictly_below_end():
    # Один и тот же ts → одинаковая сезонность; разница только из-за тренда+бэкфила.
    rng = random.Random(0)
    start = [_row_count_at(0.0, 10_000, MID_WEEK, rng) for _ in range(100)]
    end = [_row_count_at(1.0, 10_000, MID_WEEK, rng) for _ in range(100)]
    assert max(start) < min(end)


def test_row_count_at_zero_current_returns_zero():
    assert _row_count_at(0.5, 0, MID_WEEK, random.Random(0)) == 0


def test_row_count_at_anomaly_multiplier_applied():
    rng = random.Random(0)
    # Тик с anomaly_mult=1.18 заметно выше нейтрального тика на той же фазе/ts.
    base = _row_count_at(0.55, 10_000, MID_WEEK, rng, anomaly_mult=1.0)
    rng = random.Random(0)
    spike = _row_count_at(0.55, 10_000, MID_WEEK, rng, anomaly_mult=1.18)
    assert spike > base * 1.10


# --- _null_rate_at ---


def test_null_rate_stable_when_below_threshold():
    for progress in (0.0, 0.3, 0.7, 1.0):
        assert _null_rate_at(progress, 0.05, regression_progress_start=0.5) == 0.05


def test_null_rate_step_baseline_before_onset():
    assert _null_rate_at(0.0, 0.25, regression_progress_start=0.5) == BASELINE_NULL_RATE
    assert _null_rate_at(0.49, 0.25, regression_progress_start=0.5) == BASELINE_NULL_RATE


def test_null_rate_step_jumps_to_current_at_onset():
    # Сразу за порогом — уже current_rate без рампы.
    assert _null_rate_at(0.5, 0.25, regression_progress_start=0.5) == 0.25
    assert _null_rate_at(1.0, 0.25, regression_progress_start=0.5) == 0.25


# --- _drift_factor ---


def test_drift_factor_zero_before_onset():
    assert _drift_factor(0.0) == 0.0
    assert _drift_factor(DRIFT_ONSET_PROGRESS - 0.01) == 0.0


def test_drift_factor_full_at_end():
    assert _drift_factor(1.0) == pytest.approx(1.0)


def test_drift_factor_monotonic_after_onset():
    points = [DRIFT_ONSET_PROGRESS + 0.05 * i for i in range(11)]
    factors = [_drift_factor(p) for p in points]
    assert factors == sorted(factors)


# --- _categorical_buckets / _numeric_buckets ---


def test_categorical_buckets_baseline_when_no_drift():
    out = _categorical_buckets(progress=1.0, drift_amount=0.0)
    weights = [b["count"] for b in out]
    expected_total = sum(int(round(w / sum(CATEGORICAL_BASELINE_WEIGHTS) * 1000))
                         for w in CATEGORICAL_BASELINE_WEIGHTS)
    assert sum(weights) == expected_total
    # Бакеты соответствуют именам.
    assert [b["value"] for b in out] == list(CATEGORICAL_BUCKETS)


def test_categorical_buckets_drifted_at_end():
    base = _categorical_buckets(progress=DRIFT_ONSET_PROGRESS - 0.01, drift_amount=1.0)
    drifted = _categorical_buckets(progress=1.0, drift_amount=1.0)
    # Распределение должно отличаться: первый бакет уменьшается, последний — растёт.
    assert drifted[0]["count"] < base[0]["count"]
    assert drifted[-1]["count"] > base[-1]["count"]


def test_numeric_buckets_count_and_drift():
    base = _numeric_buckets(progress=0.0, drift_amount=1.0)
    drifted = _numeric_buckets(progress=1.0, drift_amount=1.0)
    assert len(base) == NUMERIC_BUCKETS
    # Среднее распределения должно сместиться вправо.
    base_centroid = sum(b["value"] * b["count"] for b in base) / sum(b["count"] for b in base)
    drift_centroid = sum(b["value"] * b["count"] for b in drifted) / sum(b["count"] for b in drifted)
    assert NUMERIC_BASELINE_MEAN - 1.0 <= base_centroid <= NUMERIC_BASELINE_MEAN + 1.0
    assert drift_centroid > base_centroid + 10  # ушло заметно правее


# --- _generate_metric_rows ---


def _snapshot(name="users", row_count=1000, size_bytes=200_000, cols=None):
    if cols is None:
        cols = [
            {"column": "email", "data_type": "text", "null_count": 50, "null_rate": 0.05},
            {"column": "country", "data_type": "text", "null_count": 0, "null_rate": 0.0},
        ]
    return TableSnapshot(name, row_count, size_bytes, cols)


def test_generate_metric_rows_metric_set_per_tick():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=1, interval_minutes=60)
    snap = _snapshot()
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=1)

    # на тик: row_count + size_bytes + last_modified + null_rate + N null_count
    expected_per_tick = 4 + len(snap.columns)
    assert len(rows) == 24 * expected_per_tick

    first_tick = [r for r in rows if r["ts"] == ts[0]]
    assert {r["metric_name"] for r in first_tick} == {
        "row_count", "size_bytes", "last_modified", "null_count", "null_rate",
    }


def test_generate_metric_rows_row_count_anchored_on_current():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    snap = _snapshot(row_count=10_000)
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)

    rcs = [r for r in rows if r["metric_name"] == "row_count"]
    # Старт ниже стартового якоря с учётом бэкфила и сезонной полосы.
    start_band = ROW_COUNT_START_FRACTION * snap.row_count * (1 + WEEKLY_AMPLITUDE)
    assert rcs[0]["value"] < start_band
    # Последний тик — в сезонной полосе вокруг current.
    band = (1 + WEEKLY_AMPLITUDE) * (1 + NOISE_AMPLITUDE)
    assert (1 / band) * snap.row_count <= rcs[-1]["value"] <= band * snap.row_count


def test_generate_metric_rows_size_scales_with_row_count():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    snap = _snapshot(row_count=10_000, size_bytes=1_000_000)  # 100 байт/строку
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)

    by_ts: dict = {}
    for r in rows:
        if r["metric_name"] in ("row_count", "size_bytes"):
            by_ts.setdefault(r["ts"], {})[r["metric_name"]] = r["value"]

    for v in by_ts.values():
        assert v["size_bytes"] == int(v["row_count"] * 100)


def test_generate_metric_rows_step_regression_for_high_null_column():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    cols = [{"column": "ip_address", "data_type": "inet", "null_count": 2500, "null_rate": 0.25}]
    snap = _snapshot("events", row_count=10_000, size_bytes=1_000_000, cols=cols)
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)

    null_counts = [r for r in rows if r["metric_name"] == "null_count"]
    early_rc = next(
        r["value"] for r in rows
        if r["metric_name"] == "row_count" and r["ts"] == null_counts[0]["ts"]
    )
    late_rc = next(
        r["value"] for r in rows
        if r["metric_name"] == "row_count" and r["ts"] == null_counts[-1]["ts"]
    )
    assert null_counts[0]["value"] == pytest.approx(early_rc * BASELINE_NULL_RATE, rel=0.05)
    assert null_counts[-1]["value"] == pytest.approx(late_rc * 0.25, rel=0.05)


def test_generate_metric_rows_regression_window_matches_days():
    """Начало регрессии — ровно за REGRESSION_DAYS до конца окна."""
    from scripts.seed_metrics_db import NULL_SPIKE_PROGRESS

    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    days = 14
    ts = _build_timestamps(end, days=days, interval_minutes=60)
    cols = [{"column": "ip", "data_type": "inet", "null_count": 1, "null_rate": 0.25}]
    # row_count=100_000 — выше суммы growth_steps в events-профиле (30K),
    # чтобы избежать масштабирования и сделать null_count*BASELINE_NULL_RATE
    # достаточно крупным, чтобы целочисленное округление умещалось в 5%.
    snap = _snapshot("events", row_count=100_000, size_bytes=10_000_000, cols=cols)
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=days)

    n = len(ts)
    spike_ts = ts[round(NULL_SPIKE_PROGRESS * (n - 1))] if n > 1 else None

    rates = sorted(
        ((r["ts"], r["value"], next(
            x["value"] for x in rows if x["metric_name"] == "row_count" and x["ts"] == r["ts"]
        )) for r in rows if r["metric_name"] == "null_count"),
        key=lambda x: x[0],
    )
    cutoff = end - timedelta(days=REGRESSION_DAYS)
    for ts_pt, null, rc in rates:
        if ts_pt < cutoff and ts_pt != spike_ts:
            assert null == pytest.approx(rc * BASELINE_NULL_RATE, rel=0.05)


def test_generate_metric_rows_tags_only_on_null_count():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=1, interval_minutes=60)
    snap = _snapshot()
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=1)

    for r in rows:
        if r["metric_name"] == "null_count":
            assert r["tags"]["column"] in {"email", "country"}
        else:
            assert "tags" not in r


def test_generate_metric_rows_no_columns_skips_null_metrics():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=1, interval_minutes=60)
    snap = _snapshot(cols=[])
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=1)

    names = {r["metric_name"] for r in rows}
    assert names == {"row_count", "size_bytes", "last_modified"}


# --- _generate_distribution_rows ---


def test_distribution_rows_one_per_day_per_column():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    snap = _snapshot()
    rows = _generate_distribution_rows(snap, days=14, end=end)
    assert len(rows) == 14 * len(snap.columns)
    assert {r["metric_name"] for r in rows} == {"column_distribution"}


def test_distribution_rows_target_drifts_others_stable():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    cols = [
        {"column": "country", "data_type": "text", "null_count": 0, "null_rate": 0.0},
        {"column": "status", "data_type": "text", "null_count": 0, "null_rate": 0.0},
    ]
    snap = _snapshot(cols=cols)
    rows = _generate_distribution_rows(snap, days=14, end=end)

    by_col: dict = {}
    for r in rows:
        by_col.setdefault(r["tags"]["column"], []).append(r)

    # «country» — первая по алфавиту → дрейфит. «status» — стабильна.
    country_first = by_col["country"][0]["tags"]["buckets"]
    country_last = by_col["country"][-1]["tags"]["buckets"]
    status_first = by_col["status"][0]["tags"]["buckets"]
    status_last = by_col["status"][-1]["tags"]["buckets"]

    # У дрейф-цели первый и последний бакеты заметно различаются.
    assert country_last[0]["count"] != country_first[0]["count"]
    # У стабильной — все совпадают.
    assert status_first == status_last


def test_distribution_rows_numeric_column_uses_numeric_buckets():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    cols = [{"column": "amount", "data_type": "numeric", "null_count": 0, "null_rate": 0.0}]
    snap = _snapshot(cols=cols)
    rows = _generate_distribution_rows(snap, days=14, end=end)

    last_buckets = rows[-1]["tags"]["buckets"]
    assert len(last_buckets) == NUMERIC_BUCKETS
    # Числовые бакеты — float, не строки из CATEGORICAL_BUCKETS.
    assert all(isinstance(b["value"], float) for b in last_buckets)


# --- _generate_schema_events ---


def test_schema_events_profile_covers_all_change_types():
    """Профили должны покрывать все 4 типа дрейфа схемы."""
    types = {p["change_type"] for events in SCHEMA_EVENT_PROFILES.values() for p in events}
    assert types == {"column_added", "column_removed", "type_changed", "nullable_changed"}


def test_schema_events_for_known_table_emits_row():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    snap = _snapshot(name="users")
    events = _generate_schema_events(snap, days=14, end=end)
    assert len(events) == 1
    e = events[0]
    assert e["table_name"] == "users"
    assert e["change_type"] == "column_added"
    assert e["column_name"] == "phone"
    # progress=0.40 в окне 14 дн → ts ≈ end - 14 * (1 - 0.40) = end - 8.4 дн.
    assert end - timedelta(days=9) < e["ts"] < end - timedelta(days=8)


def test_schema_events_unknown_table_returns_empty():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    snap = _snapshot(name="something_unmonitored")
    assert _generate_schema_events(snap, days=14, end=end) == []


def test_schema_events_zero_days_returns_empty():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    snap = _snapshot(name="users")
    assert _generate_schema_events(snap, days=0, end=end) == []


def test_schema_events_events_table_pairs_with_null_regression():
    """events.ip_address nullable_changed должен совпадать с onset null_rate-регрессии."""
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    snap = _snapshot(name="events")
    events = _generate_schema_events(snap, days=14, end=end)
    assert events[0]["change_type"] == "nullable_changed"
    assert events[0]["column_name"] == "ip_address"
    # progress=0.50 → ts ≈ end - 7 дн (≈ REGRESSION_DAYS).
    expected = end - timedelta(days=7)
    assert abs((events[0]["ts"] - expected).total_seconds()) < 86400


def test_distribution_rows_skipped_when_no_columns_or_zero_days():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    assert _generate_distribution_rows(_snapshot(cols=[]), days=14, end=end) == []
    assert _generate_distribution_rows(_snapshot(), days=0, end=end) == []


# --- main(): интеграция со заглушённым target DB + временной monitor.db ---


@pytest.fixture
def monitor_storage(tmp_path, monkeypatch):
    db_path = tmp_path / "metrics.db"
    import app.metrics_storage as storage_mod

    monkeypatch.setattr(storage_mod.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage_mod, "_engine", None)
    monkeypatch.setattr(storage_mod, "_initialized", False)
    return storage_mod


@pytest.fixture
def stub_target(monkeypatch):
    def _stub(tables: dict[str, dict]):
        monkeypatch.setattr(
            "scripts.seed_metrics_db.target_db.list_tables",
            lambda schema=None: [
                {"table_name": name, "schema": "public"} for name in tables
            ],
        )
        monkeypatch.setattr(
            "scripts.seed_metrics_db.target_db.table_stats",
            lambda name, schema=None: (
                {
                    "table_name": name, "schema": "public",
                    "row_count": tables[name]["row_count"],
                    "size_bytes": tables[name]["size_bytes"],
                    "last_analyze": None,
                } if name in tables else None
            ),
        )
        monkeypatch.setattr(
            "scripts.seed_metrics_db.target_db.column_nulls",
            lambda name, schema=None: tables[name].get("columns", []),
        )

    return _stub


def test_main_writes_metrics_and_distributions(monitor_storage, stub_target):
    stub_target({
        "users": {
            "row_count": 1000, "size_bytes": 200_000,
            "columns": [
                {"column": "email", "data_type": "text", "null_count": 50, "null_rate": 0.05},
            ],
        },
    })

    days = 2
    interval_minutes = 60
    n_ticks = days * 24
    n_columns = 1
    per_tick_rows = 4 + n_columns  # row_count, size_bytes, last_modified, null_rate + N null_count
    distribution_rows = days * n_columns

    result = main(days=days, interval_minutes=interval_minutes)

    # users есть в SCHEMA_EVENT_PROFILES → 1 schema-event ожидается.
    # NOTIFICATION_PROFILES — фиксированный набор, не зависит от таблиц в target.
    from scripts.seed_metrics_db import NOTIFICATION_PROFILES
    assert result == {
        "snapshots": 1,
        "rows": n_ticks * per_tick_rows + distribution_rows,
        "deleted": 0,
        "ticks": n_ticks,
        "schema_events": 1,
        "notifications": len(NOTIFICATION_PROFILES),
    }

    with monitor_storage.get_engine().connect() as conn:
        n = conn.execute(text("SELECT COUNT(*) FROM metrics")).scalar()
        kinds = {
            r[0] for r in conn.execute(
                text("SELECT DISTINCT metric_name FROM metrics")
            ).fetchall()
        }
        n_events = conn.execute(
            text("SELECT COUNT(*) FROM schema_events WHERE table_name = 'users'")
        ).scalar()
    assert n == n_ticks * per_tick_rows + distribution_rows
    assert "column_distribution" in kinds
    assert n_events == 1


def test_main_reset_purges_existing_rows(monitor_storage, stub_target):
    monitor_storage.save_metrics([{
        "ts": datetime.now(timezone.utc), "table_name": "stale",
        "metric_name": "row_count", "value": 1,
    }])

    stub_target({
        "users": {"row_count": 100, "size_bytes": 10_000, "columns": []},
    })

    result = main(days=1, interval_minutes=60, reset=True)
    assert result["deleted"] >= 1

    with monitor_storage.get_engine().connect() as conn:
        stale = conn.execute(
            text("SELECT COUNT(*) FROM metrics WHERE table_name = 'stale'")
        ).scalar()
    assert stale == 0


def test_main_reset_purges_derived_tables(monitor_storage, stub_target):
    """anomaly_scores/changepoints/drift_reports — все производные от ts
    из metrics; после reset их нужно тоже почистить, иначе на дашборде
    зависают точки от прошлых прогонов с несовпадающими ts."""
    now = datetime.now(timezone.utc)

    monitor_storage.save_anomaly_scores([
        {"ts": now, "table_name": "stale", "score": -0.5, "is_anomaly": 1},
    ])
    monitor_storage.save_changepoints([{
        "ts": now, "table_name": "stale", "metric_name": "row_count",
        "score": 5.0, "value_before": 100, "value_after": 200,
    }])
    monitor_storage.save_drift_reports("stale", [{
        "column": "x", "data_type": "varchar", "psi": 0.4,
        "ks_pvalue": None, "is_drift": True, "severity": "critical",
    }])
    monitor_storage.save_schema_events([{
        "ts": now, "table_name": "stale", "change_type": "column_added",
        "column_name": "y", "details": {"after": {"name": "y", "type": "text"}},
    }])
    monitor_storage.save_schema_snapshot("stale", [{"name": "x", "type": "text", "nullable": True}])
    monitor_storage.save_notification(
        event_type="anomaly", message="stale alert", status="sent",
        table_name="stale", chat_id="old",
    )

    stub_target({
        "users": {"row_count": 100, "size_bytes": 10_000, "columns": []},
    })

    main(days=1, interval_minutes=60, reset=True)

    with monitor_storage.get_engine().connect() as conn:
        for tbl in (
            "anomaly_scores", "changepoints", "drift_reports",
            "schema_events", "schema_snapshots", "notifications",
        ):
            n = conn.execute(
                text(f"SELECT COUNT(*) FROM {tbl} WHERE table_name = 'stale'")
            ).scalar()
            assert n == 0, f"{tbl} still has stale rows after reset"


def test_main_no_tables_returns_zero(monitor_storage, monkeypatch):
    monkeypatch.setattr(
        "scripts.seed_metrics_db.target_db.list_tables",
        lambda schema=None: [],
    )

    # No snapshots → early return without notifications either (мониторим
    # только то, что реально есть в target DB).
    assert main(days=1, interval_minutes=60) == {
        "snapshots": 0, "rows": 0, "deleted": 0, "ticks": 0,
    }


# --- per-table profiles ---


def test_main_seeds_notifications_with_mixed_statuses(monitor_storage, stub_target):
    """Сид-ноды для /dashboard/notifications: должно быть несколько типов
    событий и хотя бы одна запись со статусом 'failed' (иначе UI выглядит
    нереалистично)."""
    from scripts.seed_metrics_db import NOTIFICATION_PROFILES
    from app.metrics_storage import count_notifications, get_notifications

    stub_target({
        "users": {"row_count": 100, "size_bytes": 10_000, "columns": []},
    })

    result = main(days=14, interval_minutes=60, reset=True)
    assert result["notifications"] == len(NOTIFICATION_PROFILES)
    assert count_notifications() == len(NOTIFICATION_PROFILES)

    items = get_notifications(limit=200)
    event_types = {n["event_type"] for n in items}
    statuses = {n["status"] for n in items}
    # Лента должна показывать разнообразие типов событий и не быть «слишком идеальной».
    assert len(event_types) >= 3
    assert {"sent", "failed"}.issubset(statuses)
    # Bot token не сохраняется ни в одном поле (acceptance #76).
    blob = " ".join(str(v) for n in items for v in n.values() if v is not None)
    assert "TELEGRAM_BOT_TOKEN" not in blob


def test_profile_for_known_tables_returns_specific():
    for name in ("users", "events", "products", "orders"):
        assert _profile_for(name) is PROFILES[name]
        assert _profile_for(name) is not DEFAULT_PROFILE


def test_profile_for_unknown_table_falls_back_to_default():
    assert _profile_for("does_not_exist") is DEFAULT_PROFILE


@pytest.mark.parametrize("table", ["users", "events", "products", "orders"])
def test_profile_is_append_only_with_three_steps_in_last_7_days(table):
    """Каждая из 4 монитор-таблиц: append-only (без сезонности, шума,
    мультипликативных аномалий) + 3 кумулятивные ступеньки по 10–30 тыс.
    строк в правой половине окна (последние 7 дней при days=14).

    Соседние ступеньки разнесены минимум на 72 часа — иначе их схлопнет
    `_dedupe` в `ml/changepoint.py` и на графике вместо трёх «Сдвигов»
    останется один.
    """
    p = PROFILES[table]
    assert p.weekly_amplitude == 0
    assert p.backfill_fraction == 0
    assert p.noise_amplitude == 0
    assert p.anomalies == ()

    assert len(p.growth_steps) == 3
    assert all(isinstance(c, int) and 10_000 <= c <= 30_000 for _, c in p.growth_steps)
    # Все три ступеньки должны попасть в последние 7 дней 14-дневного окна.
    assert all(progress >= 0.5 for progress, _ in p.growth_steps)
    # Ступеньки расположены строго по возрастанию progress (хронология).
    progresses = [progress for progress, _ in p.growth_steps]
    assert progresses == sorted(progresses)
    # Гэпы между соседними ступеньками > DEDUPE_WINDOW_HOURS (72ч) при
    # окне 14 дней → 72 / (14·24) ≈ 0.2143 в единицах прогресса.
    min_gap = 72 / (14 * 24)
    gaps = [b - a for a, b in zip(progresses, progresses[1:])]
    assert all(gap > min_gap for gap in gaps), (
        f"{table}: соседние ступеньки слишком близко: {gaps}"
    )


def test_events_row_count_macro_monotonic_with_steps():
    """events: серия только растёт — ступеньки накопительные, обратных дипов нет."""
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    # current=80K вмещает 30K ступенек без масштабирования.
    snap = _snapshot(name="events", row_count=80_000, size_bytes=8_000_000, cols=[])
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)
    rc = [r["value"] for r in rows if r["metric_name"] == "row_count"]
    max_drop = max(
        (prev - cur for prev, cur in zip(rc, rc[1:]) if cur < prev), default=0
    )
    assert max_drop < 0.005 * snap.row_count


def test_events_row_count_has_three_growth_steps():
    """events: три ступеньки вверх дают три крупных положительных Δ.
    Каждая ступенька — 10–30K (см. профиль), порог 5K отделяет step от ramp."""
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    snap = _snapshot(name="events", row_count=80_000, size_bytes=8_000_000, cols=[])
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)

    rc = [r["value"] for r in rows if r["metric_name"] == "row_count"]
    deltas = [b - a for a, b in zip(rc, rc[1:])]
    big_jumps = [d for d in deltas if d > 5_000]
    expected_steps = [c for _, c in PROFILES["events"].growth_steps]
    assert len(big_jumps) == len(expected_steps)
    # Каждый прыжок ≥ величины ступеньки (плюс маленький ramp-инкремент).
    for jump, step in zip(big_jumps, expected_steps):
        assert step <= jump <= step + 1_000


def test_events_row_count_anchored_on_current_with_steps():
    """events: при growth_steps финальная точка всё равно ≈ current."""
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    snap = _snapshot(name="events", row_count=80_000, size_bytes=8_000_000, cols=[])
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)
    rc = [r["value"] for r in rows if r["metric_name"] == "row_count"]
    assert 0.99 * snap.row_count <= rc[-1] <= 1.01 * snap.row_count


def test_events_row_count_scales_steps_when_current_too_small():
    """Если current не вмещает все ступеньки, они масштабируются и серия
    всё равно остаётся монотонной (не уходит в ноль, не отскакивает обратно)."""
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    # current=20K, ступеньки требуют 30K → scale ≈ 0.53.
    snap = _snapshot(name="events", row_count=20_000, size_bytes=2_000_000, cols=[])
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)
    rc = [r["value"] for r in rows if r["metric_name"] == "row_count"]

    # финал ≈ current
    assert 0.99 * snap.row_count <= rc[-1] <= 1.01 * snap.row_count
    # никаких заметных провалов
    max_drop = max(
        (prev - cur for prev, cur in zip(rc, rc[1:]) if cur < prev), default=0
    )
    assert max_drop < 0.005 * snap.row_count


def test_orders_row_count_macro_monotonic():
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    snap = _snapshot(name="orders", row_count=10_000, size_bytes=1_500_000, cols=[])
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)

    rc = [r["value"] for r in rows if r["metric_name"] == "row_count"]
    max_drop = max(
        (prev - cur for prev, cur in zip(rc, rc[1:]) if cur < prev), default=0
    )
    assert max_drop < 0.005 * snap.row_count


@pytest.mark.parametrize("table,row_count", [
    ("users", 80_000),
    ("products", 80_000),
    ("orders", 80_000),
])
def test_table_row_count_macro_monotonic_with_three_steps(table, row_count):
    """users/products/orders: серия только растёт (без дипов), три крупных
    положительных Δrow_count в правой половине окна — эти три точки
    IsolationForest пометит как аномалии."""
    end = datetime(2026, 5, 10, tzinfo=timezone.utc)
    ts = _build_timestamps(end, days=14, interval_minutes=60)
    snap = _snapshot(name=table, row_count=row_count, size_bytes=row_count * 100, cols=[])
    rows = _generate_metric_rows(snap, ts, random.Random(0), days=14)

    rc = [r["value"] for r in rows if r["metric_name"] == "row_count"]
    # Никаких дипов вниз.
    max_drop = max(
        (prev - cur for prev, cur in zip(rc, rc[1:]) if cur < prev), default=0
    )
    assert max_drop < 0.005 * snap.row_count

    # Ровно три «больших» Δ в правой половине окна (последние 7 дней).
    deltas = [b - a for a, b in zip(rc, rc[1:])]
    half = len(deltas) // 2
    big_jumps = [d for d in deltas[half:] if d > 5_000]
    expected = PROFILES[table].growth_steps
    assert len(big_jumps) == len(expected)
    for jump, (_, step) in zip(big_jumps, expected):
        assert step <= jump <= step + 1_000


def test_main_skips_table_when_stats_missing(monitor_storage, monkeypatch):
    monkeypatch.setattr(
        "scripts.seed_metrics_db.target_db.list_tables",
        lambda schema=None: [
            {"table_name": "real", "schema": "public"},
            {"table_name": "ghost", "schema": "public"},
        ],
    )
    monkeypatch.setattr(
        "scripts.seed_metrics_db.target_db.table_stats",
        lambda name, schema=None: (
            {"table_name": name, "schema": "public", "row_count": 100,
             "size_bytes": 10_000, "last_analyze": None}
            if name == "real" else None
        ),
    )
    monkeypatch.setattr(
        "scripts.seed_metrics_db.target_db.column_nulls",
        lambda name, schema=None: [],
    )

    result = main(days=1, interval_minutes=60)
    assert result["snapshots"] == 1
