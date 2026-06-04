from __future__ import annotations


def test_default_iceberg_dsn_points_to_local_rest_and_minio():
    from scripts.prepare_iceberg_demo import (
        default_app_iceberg_dsn,
        default_iceberg_dsn,
    )

    dsn = default_iceberg_dsn()

    assert dsn.startswith("iceberg+rest://localhost:8181?")
    assert "warehouse=s3%3A%2F%2Ficeberg-smoke%2Fwarehouse" in dsn
    assert "s3.endpoint=http%3A%2F%2Flocalhost%3A9000" in dsn
    assert "s3.path-style-access=true" in dsn

    app_dsn = default_app_iceberg_dsn()
    assert app_dsn.startswith("iceberg+rest://iceberg-rest:8181?")
    assert "s3.endpoint=http%3A%2F%2Fminio%3A9000" in app_dsn


def test_timestamps_build_expected_number_of_ticks():
    from scripts.prepare_iceberg_demo import _timestamps

    ticks = _timestamps(days=14, interval_minutes=60)

    assert len(ticks) == 14 * 24
    assert ticks == sorted(ticks)


def test_events_history_contains_device_id_null_rate_incident():
    from scripts.prepare_iceberg_demo import _metric_rows, _table_specs, _timestamps

    events = next(spec for spec in _table_specs() if spec.name == "events")
    rows = _metric_rows(events, _timestamps(days=14, interval_minutes=60))

    device_nulls = [
        r for r in rows
        if r["table_name"] == "events"
        and r["metric_name"] == "null_count"
        and r.get("tags", {}).get("column") == "device_id"
    ]
    row_counts = [
        r for r in rows
        if r["table_name"] == "events" and r["metric_name"] == "row_count"
    ]

    assert len(device_nulls) == 14 * 24
    assert len(row_counts) == 14 * 24
    early_rate = device_nulls[0]["value"] / row_counts[0]["value"]
    late_rate = device_nulls[-1]["value"] / row_counts[-1]["value"]
    assert early_rate < 0.03
    assert late_rate > 0.15


def test_repair_iceberg_connection_reencrypts_invalid_dsn(tmp_path, monkeypatch):
    from cryptography.fernet import Fernet

    db_path = tmp_path / "iceberg_demo.db"
    import app.metrics_storage as storage

    monkeypatch.setattr(storage.settings, "MONITOR_DB_URL", f"sqlite:///{db_path}")
    monkeypatch.setattr(storage, "_engine", None)
    monkeypatch.setattr(storage, "_initialized", False)

    monkeypatch.setenv("FERNET_KEY", Fernet.generate_key().decode())
    from app import crypto

    crypto.reset_for_tests()

    user = storage.create_user("u1", "lake@example.com", "hash")
    project = storage.create_project("p1", user["id"], "Iceberg Lakehouse", "iceberg")
    stale_ciphertext = Fernet(Fernet.generate_key()).encrypt(b"iceberg+rest://old")
    storage.create_connection(
        connection_id="c1",
        project_id=project["id"],
        name="Local Iceberg REST",
        dsn_encrypted=stale_ciphertext,
        schema_name="old_ns",
    )

    from scripts.prepare_iceberg_demo import (
        _repair_iceberg_connection,
        default_iceberg_dsn,
    )

    _repair_iceberg_connection(
        project["id"],
        "c1",
        default_iceberg_dsn(),
        interval_minutes=1440,
    )

    repaired = storage.get_connection(project["id"], "c1")
    assert crypto.decrypt_dsn(repaired["dsn_encrypted"]) == default_iceberg_dsn()
    assert repaired["schema_name"] == "lakehouse"
    assert repaired["interval_minutes"] == 1440
    assert repaired["is_active"] is True
