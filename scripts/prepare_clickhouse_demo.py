"""Prepare the Demo 3.2 ClickHouse demo path (#177).

Modelled on ``scripts/prepare_iceberg_demo.py`` but simpler: ClickHouse
has a regular SQLAlchemy adapter (#42, #141 probe), so we don't need
catalog/bucket bootstrap — only:

1. Ensure ClickHouse is up + seeded with demo tables (scripts/seed_clickhouse).
2. Resolve the Events-ClickHouse demo project + connection (via
   ``scripts.seed_demo_workspace`` — idempotent, doesn't mutate existing).
3. Repair the connection DSN if Fernet key changed locally.
4. Purge prior metrics for this project so 14-day history is clean.
5. Synthetic 14-day metric backfill matching the 4 CH demo tables
   (users / products / orders / events).
6. One real ``collect_for_connection`` tick to verify end-to-end works.
7. Optional ``warmup_ml`` so dashboard isn't empty.

After this script + `make server`:
  * /dashboard/ shows ClickHouse-tables under "Events ClickHouse" project
  * Forecast / anomaly / changepoint blocks have data
  * table_detail для events / orders отдаёт graph за 14 дней
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from app import crypto
from app.metrics_storage import (
    get_engine as get_monitor_engine,
)
from app.metrics_storage import (
    save_metrics,
    save_schema_events,
)
from collectors.per_project import collect_for_connection
from scripts.seed_demo_workspace import seed_demo_workspace

logger = logging.getLogger(__name__)

# DSN — оставляем дефолт из scripts/seed_clickhouse.py. Если CH в Docker
# из app-контейнера — используется service-host clickhouse:9000, поэтому
# у нас два DSN: один для хоста (localhost:19000, пушится в БД для
# демо-юзера), второй для app внутри compose-сети (clickhouse:9000).
HOST_DSN = "clickhouse+native://default@localhost:19000/demo"
APP_DSN = "clickhouse+native://default@clickhouse:9000/demo"
SCHEMA = "demo"


@dataclass(frozen=True)
class DemoColumn:
    name: str
    nullable: bool
    null_rate: float = 0.0


@dataclass(frozen=True)
class DemoTable:
    name: str
    row_count: int
    size_bytes: int
    columns: tuple[DemoColumn, ...]


def _table_specs() -> list[DemoTable]:
    """Спеки имитируют size + column shape того, что scripts.seed_clickhouse
    реально пишет в demo db. row_count — целевое значение на конец 14-дневного
    окна (растёт от 60% до 100% по тикам)."""
    return [
        DemoTable(
            name="events",
            row_count=10_000,
            size_bytes=2_100_000,
            columns=(
                DemoColumn("event_id", False),
                DemoColumn("user_id", False),
                DemoColumn("event_type", False),
                DemoColumn("server_id", False),
                DemoColumn("device_type", True, 0.04),
                DemoColumn("created_at", False),
            ),
        ),
        DemoTable(
            name="orders",
            row_count=5_000,
            size_bytes=1_350_000,
            columns=(
                DemoColumn("order_id", False),
                DemoColumn("user_id", False),
                DemoColumn("product_id", False),
                DemoColumn("status", False),
                DemoColumn("quantity", False),
                DemoColumn("total_price", True, 0.01),
                DemoColumn("created_at", False),
            ),
        ),
        DemoTable(
            name="users",
            row_count=2_000,
            size_bytes=480_000,
            columns=(
                DemoColumn("user_id", False),
                DemoColumn("email", False),
                DemoColumn("name", True, 0.02),
                DemoColumn("country", False),
                DemoColumn("signup_date", False),
                DemoColumn("signup_source", True, 0.03),
                DemoColumn("is_active", False),
            ),
        ),
        DemoTable(
            name="products",
            row_count=200,
            size_bytes=64_000,
            columns=(
                DemoColumn("product_id", False),
                DemoColumn("name", False),
                DemoColumn("category", True, 0.02),
                DemoColumn("price", False),
                DemoColumn("created_at", False),
            ),
        ),
    ]


def _get_demo_project_and_connection() -> tuple[str, str]:
    """Resolve via seed_demo_workspace (idempotent) → returns (pid, cid)."""
    result = seed_demo_workspace(
        reset_password=True,
        clickhouse_dsn=HOST_DSN,
    )
    project = result["projects"]["events-clickhouse"]
    connection = result["connections"]["events-clickhouse"]
    _repair_clickhouse_connection(project["id"], connection["id"])
    return project["id"], connection["id"]


def _repair_clickhouse_connection(
    project_id: str, connection_id: str, *,
    interval_minutes: int = 15,
) -> None:
    """Re-encrypt the DSN with the current Fernet key if it changed since
    seed_demo_workspace originally wrote it. Mirrors the Iceberg pattern
    in prepare_iceberg_demo._repair_iceberg_connection — needed because
    seed_demo_workspace is intentionally idempotent (doesn't mutate
    existing rows), and a rotated key would otherwise leave us with an
    undecryptable DSN.
    """
    from app.metrics_storage import get_connection

    conn = get_connection(project_id, connection_id)
    if conn is None:
        return
    desired_dsn = APP_DSN
    needs_update = False
    try:
        current = crypto.decrypt_dsn(conn["dsn_encrypted"])
        needs_update = current != desired_dsn or conn["schema_name"] != SCHEMA
    except crypto.InvalidToken:
        needs_update = True
    if not needs_update:
        return
    with get_monitor_engine().begin() as db_conn:
        db_conn.execute(
            text("""
                UPDATE connections
                SET dsn_encrypted = :dsn,
                    schema_name = :schema,
                    interval_minutes = :interval_minutes,
                    is_active = 1
                WHERE project_id = :project_id AND id = :connection_id
            """),
            {
                "dsn": crypto.encrypt_dsn(desired_dsn),
                "schema": SCHEMA,
                "interval_minutes": interval_minutes,
                "project_id": project_id,
                "connection_id": connection_id,
            },
        )
    print("  ClickHouse connection DSN re-saved for current Fernet key")


def _purge_project_history(project_id: str, table_names: list[str]) -> int:
    """Clean slate before backfill so repeated runs of demo prep don't
    layer history on top of history."""
    scoped = ("metrics", "notifications", "anomaly_scores",
              "changepoints", "drift_reports")
    deleted = 0
    with get_monitor_engine().begin() as conn:
        for tbl in scoped:
            result = conn.execute(
                text(f"DELETE FROM {tbl} WHERE project_id = :pid"),
                {"pid": project_id},
            )
            deleted += result.rowcount or 0
        for t in table_names:
            result = conn.execute(
                text("DELETE FROM schema_events WHERE table_name = :name"),
                {"name": t},
            )
            deleted += result.rowcount or 0
    return deleted


def _timestamps(days: int, interval_minutes: int) -> list[datetime]:
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    step = timedelta(minutes=interval_minutes)
    ticks = max(1, (days * 24 * 60) // interval_minutes)
    return [end - step * (ticks - 1 - i) for i in range(ticks)]


def _row_count(spec: DemoTable, progress: float) -> int:
    """Линейный рост от 60% до 100% за окно + три горба для IsolationForest.
    Кривая та же что в prepare_iceberg_demo, чтобы ML-блоки на демо
    выглядели одинаково красиво в обоих проектах."""
    base = spec.row_count * (0.60 + 0.40 * progress)
    bumps = {
        "events":   ((0.55, 600), (0.78, 750), (0.97, 900)),
        "orders":   ((0.54, 300), (0.76, 400), (0.96, 500)),
        "users":    ((0.50, 80),  (0.73, 110), (0.96, 130)),
        "products": ((0.55, 4),   (0.78, 6),   (0.97, 8)),
    }.get(spec.name, ())
    base += sum(v for point, v in bumps if progress >= point)
    return min(spec.row_count, max(0, round(base)))


def _null_rate(spec: DemoTable, col: DemoColumn, progress: float, tick_idx: int) -> float:
    """Steady null-rate с одним инцидент-всплеском в конце окна — даёт PELT
    осмысленный change-point для демо."""
    if spec.name == "events" and col.name == "device_type" and progress < 0.55:
        return 0.01  # «нормальная» фаза до инцидента
    rate = col.null_rate
    if spec.name == "events" and tick_idx % 168 == 96:  # weekly spike
        rate = min(1.0, rate + 0.12)
    return rate


def _distribution_rows(spec: DemoTable, days: int, end: datetime) -> list[dict]:
    """Drift-source: первая нюллабельная категория получает плавный сдвиг
    распределения, остальные стабильны. PSI/KS поднимет один реальный alert."""
    cat_col = next((c for c in spec.columns if c.nullable), None)
    if cat_col is None:
        return []
    rows: list[dict] = []
    for day in range(days):
        ts = end - timedelta(days=days - 1 - day)
        progress = day / (days - 1) if days > 1 else 1.0
        # Фейковые категории — drift от ровного распределения к смещённому.
        labels = ["A", "B", "C", "D", "E"]
        baseline = [0.20] * 5
        target = [0.10, 0.14, 0.20, 0.22, 0.34]
        buckets = []
        for label, b, t in zip(labels, baseline, target, strict=False):
            weight = b + (t - b) * progress
            buckets.append({"value": label, "count": round(weight * 1000)})
        rows.append({
            "ts": ts,
            "table_name": spec.name,
            "metric_name": "column_distribution",
            "value": float(sum(b["count"] for b in buckets)),
            "tags": {
                "column": cat_col.name,
                "data_type": "string",
                "buckets": buckets,
            },
        })
    return rows


def _metric_rows(spec: DemoTable, timestamps: list[datetime]) -> list[dict]:
    rows: list[dict] = []
    avg_row_size = spec.size_bytes / spec.row_count
    for i, ts in enumerate(timestamps):
        progress = i / (len(timestamps) - 1) if len(timestamps) > 1 else 1.0
        rc = _row_count(spec, progress)
        rows.extend([
            {"ts": ts, "table_name": spec.name,
             "metric_name": "row_count", "value": rc},
            {"ts": ts, "table_name": spec.name,
             "metric_name": "size_bytes", "value": int(rc * avg_row_size)},
            {"ts": ts, "table_name": spec.name,
             "metric_name": "last_modified", "value": ts.timestamp()},
        ])
        rates: list[float] = []
        for col in spec.columns:
            if not col.nullable:
                continue
            rate = _null_rate(spec, col, progress, i)
            rates.append(rate)
            rows.append({
                "ts": ts,
                "table_name": spec.name,
                "metric_name": "null_count",
                "value": round(rc * rate),
                "tags": {"column": col.name},
            })
        if rates:
            rows.append({
                "ts": ts,
                "table_name": spec.name,
                "metric_name": "null_rate",
                "value": round(sum(rates) / len(rates), 4),
            })
    return rows


def _schema_events(days: int) -> list[dict]:
    """Пара demo-events чтобы schema-drift панель не была пустой."""
    end = datetime.now(UTC).replace(second=0, microsecond=0)
    return [
        {
            "ts": end - timedelta(days=days * 0.55),
            "table_name": "events",
            "change_type": "nullable_changed",
            "column_name": "device_type",
            "details": {
                "before": {"name": "device_type", "type": "LowCardinality(String)",
                            "nullable": False},
                "after": {"name": "device_type", "type": "LowCardinality(String)",
                            "nullable": True},
            },
        },
        {
            "ts": end - timedelta(days=days * 0.30),
            "table_name": "orders",
            "change_type": "column_added",
            "column_name": "total_price",
            "details": {
                "after": {"name": "total_price", "type": "Decimal(12, 2)",
                            "nullable": True},
            },
        },
    ]


def seed_clickhouse_history(
    project_id: str, *,
    days: int = 14,
    interval_minutes: int = 60,
    reset: bool = True,
) -> dict:
    print("[3/4] ClickHouse demo history")
    specs = _table_specs()
    table_names = [s.name for s in specs]
    deleted = _purge_project_history(project_id, table_names) if reset else 0
    timestamps = _timestamps(days, interval_minutes)
    end = timestamps[-1] if timestamps else datetime.now(UTC)
    rows: list[dict] = []
    for spec in specs:
        rows.extend(_metric_rows(spec, timestamps))
        rows.extend(_distribution_rows(spec, days, end))
    saved = save_metrics(rows, project_id)
    schema_saved = save_schema_events(_schema_events(days))
    print(
        f"  saved {saved} metrics, {schema_saved} schema-events "
        f"for {len(specs)} tables; deleted {deleted} old rows"
    )
    return {
        "rows": saved,
        "schema_events": schema_saved,
        "deleted": deleted,
        "tables": len(specs),
        "ticks": len(timestamps),
    }


def prepare_clickhouse_demo(
    days: int = 14,
    interval_minutes: int = 60,
    *,
    warmup_ml: bool = True,
    skip_live_collect: bool = False,
) -> dict:
    """Top-level orchestrator. Steps printed for operator readability —
    matches the prepare_iceberg_demo UX."""
    print("[1/4] Demo workspace + connection")
    project_id, connection_id = _get_demo_project_and_connection()
    print(f"  project_id={project_id}")
    print(f"  connection_id={connection_id}")

    if not skip_live_collect:
        print("[2/4] Live ClickHouse snapshot via collector")
        try:
            collect_for_connection(project_id, connection_id)
            print("  collected live ClickHouse snapshot")
        except Exception as exc:
            # CH может быть не запущен (нет make clickhouse-up) — мы не
            # фейлим демо-prep на этом шаге, т.к. синтетическая история
            # ниже не зависит от живого CH. Print + move on.
            print(f"  live collect skipped: {exc}")
    else:
        print("[2/4] Live collect skipped (--skip-live-collect)")

    history_result = seed_clickhouse_history(
        project_id,
        days=days,
        interval_minutes=interval_minutes,
        reset=True,
    )

    warmup_result = None
    if warmup_ml:
        print("[4/4] ML warmup")
        from scripts.warmup_ml import main as warmup_main

        warmup_result = warmup_main(project_id=project_id)
    else:
        print("[4/4] ML warmup skipped (--skip-warmup-ml)")

    print("\nClickHouse demo ready.")
    print("  URL: http://localhost:5001/dashboard/")
    print("  Login: demo@dbmonitor.app / demo12345")
    print(f"  PROJECT_ID={project_id}")
    print(f"  CONNECTION_ID={connection_id}")
    return {
        "project_id": project_id,
        "connection_id": connection_id,
        "history": history_result,
        "warmup": warmup_result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--interval-minutes", type=int, default=60)
    parser.add_argument(
        "--skip-warmup-ml", action="store_true",
        help="Only seed CH history; do not run ML warmup",
    )
    parser.add_argument(
        "--skip-live-collect", action="store_true",
        help="Don't attempt live collector tick — useful if CH not running",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    result = prepare_clickhouse_demo(
        days=args.days,
        interval_minutes=args.interval_minutes,
        warmup_ml=not args.skip_warmup_ml,
        skip_live_collect=args.skip_live_collect,
    )
    print("\nResult:")
    print(f"  CH_PROJECT_ID={result['project_id']}")
    print(f"  CH_CONNECTION_ID={result['connection_id']}")


if __name__ == "__main__":
    main()
