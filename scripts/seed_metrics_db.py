"""
Бэкфилл таблицы `metrics` в monitor.db синтетической историей за N дней
(по умолчанию 14). Цель — наполнить хранилище данными такой формы, чтобы
все четыре ML-пайплайна выдавали осмысленные результаты «из коробки»:

* `ml/forecast.py` (Prophet)        — нужен тренд + недельная сезонность.
* `ml/changepoint.py` (PELT/RBF)    — нужны резкие сдвиги среднего, а не
                                      плавные рампы (score ≥ 1.5σ).
* `ml/anomaly_detector.py` (IsolationForest)
                                    — нужно ≥200 совмещённых тиков
                                      (row_count + null_rate) и несколько
                                      точечных выбросов в Δ-фичах.
* `ml/drift.py` (PSI/KS)            — нужны снапшоты `column_distribution`
                                      хотя бы в двух точках за 7 дн.,
                                      где одна колонка реально сдвинулась.

Что генерируется на каждый тик (по умолчанию шаг 60 мин):
  * row_count     — append-only: линейный рост от start_fraction*current
                    до current, плюс ровно три кумулятивные ступеньки
                    (10–30 тыс. строк каждая) в последних 7 днях окна.
                    Серия только растёт; ступеньки дают IsolationForest
                    три выраженные положительные Δrow_count и при
                    contamination=0.01 ≈ 3 точки помечаются как аномалии.
  * size_bytes    — масштабируется от row_count.
  * last_modified — сам timestamp тика (epoch-секунды).
  * null_count    — на колонку. Если текущий null_rate > REGRESSION_THRESHOLD,
                    эмулируется *резкая* регрессия за REGRESSION_DAYS до конца
                    (BASELINE_NULL_RATE → current_rate). Без рампы — это даёт
                    PELT нормальный change-point с большим score.
  * null_rate     — среднее по колонкам.

Дополнительно — раз в день на колонку:
  * column_distribution — синтетические бакеты для drift.py. У первой по
                          алфавиту колонки распределение дрейфует (включается
                          с середины окна и доезжает до целевых весов к концу),
                          у остальных — стабильное.

Использование:
    python -m scripts.seed_metrics_db
    python -m scripts.seed_metrics_db --days 14 --interval-minutes 60
    python -m scripts.seed_metrics_db --reset
"""
from __future__ import annotations

import argparse
import logging
import math
import random
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import text

from app import db as target_db
from app.metrics_storage import (
    get_engine as get_monitor_engine,
)
from app.metrics_storage import (
    save_metrics,
    save_notification,
    save_schema_events,
)

logger = logging.getLogger(__name__)

# Базовый тренд + шум (используется как дефолт TableProfile).
ROW_COUNT_START_FRACTION = 0.70
NOISE_AMPLITUDE = 0.005

# Недельная сезонность — кормит Prophet в forecast.py (дефолт).
WEEKLY_AMPLITUDE = 0.08

# Ступенчатая регрессия null_rate (change-point + страница истории).
REGRESSION_THRESHOLD = 0.10
REGRESSION_DAYS = 7
BASELINE_NULL_RATE = 0.02

# Бэкфил-ступенька на row_count: даёт PELT отдельный change-point на росте.
BACKFILL_PROGRESS = 0.30
BACKFILL_FRACTION = 0.06

# Точечные аномалии для IsolationForest. (progress, multiplier) — на каждую
# точку приходится ровно один тик, ближайший к указанному прогрессу.
ANOMALY_POINTS: tuple[tuple[float, float], ...] = (
    (0.55, 1.18),
    (0.78, 0.80),
    (0.88, 1.15),
)

# Транзиентный выброс null_rate для одного тика. История
# (`/dashboard/history`) считает «Выбросы NULL» как Δnull_rate ≥ 5 п.п. между
# соседними тиками по серии table-level avg null_rate — поэтому пик надо
# дать сразу всем колонкам таблицы, иначе среднее размывается ниже порога.
# Имитирует короткий инцидент качества данных: один час null_rate подскочил
# на 18 п.п. сразу по всем колонкам, потом откатился к норме.
NULL_SPIKE_PROGRESS = 0.20  # ~ 11 дней назад в 14-дневном окне
NULL_SPIKE_DELTA = 0.18     # +18 п.п. к каждой колонке на один тик


@dataclass(frozen=True)
class TableProfile:
    """Параметры синтетической траектории row_count под конкретную таблицу.

    Все четыре монитор-таблицы (users/events/products/orders) идут по
    единому паттерну append-only — монотонный рост с тремя кумулятивными
    ступеньками в последних 7 днях окна. Различаются только величины
    ступенек (10–30 тыс. строк) и start_fraction, чтобы графики не
    выглядели дублями.
    """

    start_fraction: float = ROW_COUNT_START_FRACTION
    weekly_amplitude: float = WEEKLY_AMPLITUDE
    backfill_fraction: float = BACKFILL_FRACTION
    anomalies: tuple[tuple[float, float], ...] = ANOMALY_POINTS
    noise_amplitude: float = NOISE_AMPLITUDE
    # Накопительные ступеньки-«всплески» для append-only таблиц: пара
    # (progress, abs_rows_added). На указанной фазе значение прыгает вверх
    # ровно на abs_rows_added и остаётся повышенным до конца окна — никаких
    # возвратов. Сумма ступенек «съедается» из ramp'а, чтобы последняя
    # точка осталась ≈ current. Если сумма превосходит 80% current, шаги
    # пропорционально масштабируются, чтобы тренд не схлопнулся в ноль.
    growth_steps: tuple[tuple[float, int], ...] = ()


DEFAULT_PROFILE = TableProfile()

# Все четыре таблицы — append-only: монотонный рост + ровно три кумулятивные
# ступеньки-«всплеска» в последних 7 днях 14-дневного окна. Каждая ступенька
# добавляет 10–30 тыс. строк (точное значение масштабируется вниз, если оно
# превысит 80% current — см. _step_scale) и остаётся повышенной до конца
# окна — никаких возвратов вниз. Никакой сезонности, шума и
# мультипликативных аномалий: ровный наклонный тренд, три резких положительных
# Δrow_count в правой половине окна — это ровно три точки, которые
# IsolationForest помечает как аномалии (contamination=0.01 при 336 тиках ≈ 3).
#
# Прогрессы ступенек разнесены по таблицам, чтобы графики users/events/
# products/orders не выглядели зеркальными друг другу — спайки приходятся
# на разные дни.
# Соседние ступеньки разнесены минимум на 72 часа — это окно дедупликации
# в `ml/changepoint.py` (DEDUPE_WINDOW_HOURS): однонаправленные сдвиги
# ближе чем 72ч схлопываются в один changepoint, и тогда дашборд показал
# бы только одну метку «Сдвиг» вместо трёх. С 14-дневным окном это
# означает, что прогрессы соседних ступенек должны различаться не меньше
# чем на 72/(14·24) ≈ 0.215.
PROFILES: dict[str, TableProfile] = {
    "users": TableProfile(
        start_fraction=0.65,
        weekly_amplitude=0.0,
        backfill_fraction=0.0,
        anomalies=(),
        noise_amplitude=0.0,
        growth_steps=((0.50, 12_000), (0.74, 18_000), (0.97, 25_000)),
    ),
    "events": TableProfile(
        start_fraction=0.55,
        weekly_amplitude=0.0,
        backfill_fraction=0.0,
        anomalies=(),
        noise_amplitude=0.0,
        growth_steps=((0.52, 13_000), (0.75, 19_000), (0.98, 26_000)),
    ),
    "products": TableProfile(
        start_fraction=0.55,
        weekly_amplitude=0.0,
        backfill_fraction=0.0,
        anomalies=(),
        noise_amplitude=0.0,
        growth_steps=((0.51, 10_000), (0.74, 14_000), (0.97, 20_000)),
    ),
    "orders": TableProfile(
        start_fraction=0.55,
        weekly_amplitude=0.0,
        backfill_fraction=0.0,
        anomalies=(),
        noise_amplitude=0.0,
        growth_steps=((0.53, 11_000), (0.76, 16_000), (0.98, 22_000)),
    ),
}


def _profile_for(table_name: str) -> TableProfile:
    return PROFILES.get(table_name, DEFAULT_PROFILE)

# Синтетические события `schema_events` на таблицу — по одному на каждый
# тип (column_added / column_removed / type_changed / nullable_changed),
# чтобы дашборд показывал разнообразие. events.ip_address nullable_changed
# совпадает по времени с регрессией null_rate — один связный сюжет
# «вот когда сломалось».
SCHEMA_EVENT_PROFILES: dict[str, list[dict]] = {
    "users": [{
        "progress": 0.40,
        "change_type": "column_added",
        "column_name": "phone",
        "details": {"after": {"name": "phone", "type": "varchar", "nullable": True}},
    }],
    "events": [{
        "progress": 0.50,  # совпадает с regression onset null_rate
        "change_type": "nullable_changed",
        "column_name": "ip_address",
        "details": {
            "before": {"name": "ip_address", "type": "inet", "nullable": False},
            "after":  {"name": "ip_address", "type": "inet", "nullable": True},
        },
    }],
    "products": [{
        "progress": 0.30,
        "change_type": "type_changed",
        "column_name": "stock",
        "details": {
            "before": {"name": "stock", "type": "integer", "nullable": False},
            "after":  {"name": "stock", "type": "bigint", "nullable": False},
        },
    }],
    "orders": [{
        "progress": 0.60,
        "change_type": "column_removed",
        "column_name": "legacy_status",
        "details": {"before": {"name": "legacy_status", "type": "varchar", "nullable": True}},
    }],
}


# Параметры column_distribution → drift.py.
CATEGORICAL_BUCKETS: tuple[str, ...] = ("A", "B", "C", "D", "E")
CATEGORICAL_BASELINE_WEIGHTS: tuple[float, ...] = (0.35, 0.25, 0.20, 0.12, 0.08)
CATEGORICAL_TARGET_WEIGHTS: tuple[float, ...] = (0.10, 0.15, 0.20, 0.25, 0.30)
NUMERIC_BUCKETS = 10
NUMERIC_BASELINE_MEAN = 50.0
NUMERIC_TARGET_MEAN = 75.0
NUMERIC_SIGMA = 15.0
# Дрейф включается за полпути → попадает в окно BASELINE_DAYS (7 дн.) drift.py.
DRIFT_ONSET_PROGRESS = 0.5

_NUMERIC_TYPE_FRAGMENTS = (
    "int", "numeric", "decimal", "real", "double", "float",
    "money", "serial",
)


@dataclass
class TableSnapshot:
    table_name: str
    row_count: int
    size_bytes: int
    columns: list[dict]


def _capture_snapshots(schema: str | None = None) -> list[TableSnapshot]:
    snapshots: list[TableSnapshot] = []
    for t in target_db.list_tables(schema=schema):
        name = t["table_name"]
        stats = target_db.table_stats(name, schema=schema)
        if stats is None:
            continue
        cols = target_db.column_nulls(name, schema=schema)
        snapshots.append(
            TableSnapshot(
                table_name=name,
                row_count=int(stats["row_count"]),
                size_bytes=int(stats["size_bytes"]),
                columns=cols,
            )
        )
    return snapshots


def _build_timestamps(end: datetime, days: int, interval_minutes: int) -> list[datetime]:
    step = timedelta(minutes=interval_minutes)
    n_ticks = max(1, (days * 24 * 60) // interval_minutes)
    return [end - step * (n_ticks - 1 - i) for i in range(n_ticks)]


# ─────── трансформации ряда row_count ───────


def _seasonality_factor(ts: datetime, amplitude: float = WEEKLY_AMPLITUDE) -> float:
    """Множитель недельной сезонности: пик в середине недели, провал на выходных.
    При amplitude=0 функция возвращает строго 1.0 (для append-only таблиц)."""
    if amplitude == 0:
        return 1.0
    dow = ts.weekday()  # Mon=0..Sun=6
    return 1.0 + amplitude * math.cos((dow - 2) * 2 * math.pi / 7)


def _backfill_offset(
    progress: float, current: int, fraction: float = BACKFILL_FRACTION,
) -> int:
    """До ступеньки вычитаем константу, после — ничего не делаем.
    Якорит последнюю точку на `current`, делая ступеньку «накоплением».
    При fraction=0 эффект отключён."""
    if fraction <= 0 or progress >= BACKFILL_PROGRESS:
        return 0
    return -round(current * fraction)


def _anomaly_multiplier(
    progress: float,
    n_ticks: int,
    anomalies: tuple[tuple[float, float], ...] = ANOMALY_POINTS,
) -> float:
    """Возвращает множитель аномалии для тика. 1.0 для нормальных и при
    пустом списке anomalies."""
    if n_ticks <= 1 or not anomalies:
        return 1.0
    half_step = 0.5 / (n_ticks - 1)
    for p, mult in anomalies:
        if abs(progress - p) < half_step:
            return mult
    return 1.0


_STEP_BUDGET_FRACTION = 0.80  # ступеньки могут «съесть» максимум 80% current


def _step_scale(current: int, steps: tuple) -> float:
    """Если сумма ступенек > 80% current, масштабируем все вниз пропорционально.
    Иначе scale=1.0 (используются абсолютные значения как есть)."""
    if not steps or current <= 0:
        return 1.0
    total = sum(c for _, c in steps)
    budget = current * _STEP_BUDGET_FRACTION
    return budget / total if total > budget else 1.0


def _step_contribution(progress: float, steps: tuple, scale: float) -> int:
    if not steps:
        return 0
    return round(scale * sum(c for p, c in steps if progress >= p))


def _row_count_at(
    progress: float,
    current: int,
    ts: datetime,
    rng: random.Random,
    profile: TableProfile = DEFAULT_PROFILE,
    anomaly_mult: float = 1.0,
) -> int:
    scale = _step_scale(current, profile.growth_steps)
    total_steps = round(scale * sum(c for _, c in profile.growth_steps))
    # Ramp заканчивается в (current - total_steps), чтобы вместе со ступеньками
    # дать ≈ current на progress=1.
    ramp_target = max(0, current - total_steps)
    base = ramp_target * (
        profile.start_fraction + (1.0 - profile.start_fraction) * progress
    )
    base += _step_contribution(progress, profile.growth_steps, scale)
    base += _backfill_offset(progress, current, profile.backfill_fraction)
    base *= _seasonality_factor(ts, profile.weekly_amplitude)
    base *= anomaly_mult
    if profile.noise_amplitude > 0:
        base *= 1 + rng.uniform(-profile.noise_amplitude, profile.noise_amplitude)
    return max(0, round(base))


def _null_rate_at(
    progress: float,
    current_rate: float,
    regression_progress_start: float,
) -> float:
    """Резкий шаг: BASELINE до regression_progress_start, иначе current_rate.
    PELT ловит шаг лучше, чем рампу — score не размазывается по соседним тикам."""
    if current_rate <= REGRESSION_THRESHOLD:
        return current_rate
    return BASELINE_NULL_RATE if progress < regression_progress_start else current_rate


def _generate_metric_rows(
    snapshot: TableSnapshot,
    timestamps: list[datetime],
    rng: random.Random,
    days: int,
) -> list[dict]:
    n = len(timestamps)
    if n == 0:
        return []
    profile = _profile_for(snapshot.table_name)
    regression_progress_start = max(0.0, 1.0 - REGRESSION_DAYS / max(days, 1))
    avg_row_size = (
        snapshot.size_bytes / snapshot.row_count if snapshot.row_count else 0.0
    )
    spike_idx = round(NULL_SPIKE_PROGRESS * (n - 1)) if n > 1 else None

    rows: list[dict] = []
    for i, ts in enumerate(timestamps):
        progress = i / (n - 1) if n > 1 else 1.0
        anomaly_mult = _anomaly_multiplier(progress, n, profile.anomalies)
        rc = _row_count_at(progress, snapshot.row_count, ts, rng, profile, anomaly_mult)
        rows.append({
            "ts": ts, "table_name": snapshot.table_name,
            "metric_name": "row_count", "value": rc,
        })
        rows.append({
            "ts": ts, "table_name": snapshot.table_name,
            "metric_name": "size_bytes", "value": int(rc * avg_row_size),
        })
        rows.append({
            "ts": ts, "table_name": snapshot.table_name,
            "metric_name": "last_modified", "value": ts.timestamp(),
        })

        is_spike_tick = i == spike_idx
        col_rates: list[float] = []
        for col in snapshot.columns:
            current_rate = float(col.get("null_rate", 0.0))
            rate = _null_rate_at(progress, current_rate, regression_progress_start)
            if is_spike_tick:
                rate = min(1.0, rate + NULL_SPIKE_DELTA)
            null_count = round(rc * rate)
            rows.append({
                "ts": ts, "table_name": snapshot.table_name,
                "metric_name": "null_count", "value": null_count,
                "tags": {"column": col["column"]},
            })
            col_rates.append(rate)

        if col_rates:
            avg = sum(col_rates) / len(col_rates)
            rows.append({
                "ts": ts, "table_name": snapshot.table_name,
                "metric_name": "null_rate", "value": round(avg, 4),
            })

    return rows


# ─────── column_distribution → drift.py ───────


def _is_numeric_type(data_type: str | None) -> bool:
    if not data_type:
        return False
    t = data_type.lower()
    return any(f in t for f in _NUMERIC_TYPE_FRAGMENTS)


def _drift_factor(progress: float) -> float:
    """0 до DRIFT_ONSET_PROGRESS, потом линейная рампа до 1 в конце окна."""
    if progress < DRIFT_ONSET_PROGRESS:
        return 0.0
    span = 1.0 - DRIFT_ONSET_PROGRESS
    return (progress - DRIFT_ONSET_PROGRESS) / span if span > 0 else 1.0


def _categorical_buckets(progress: float, drift_amount: float) -> list[dict]:
    factor = _drift_factor(progress) * drift_amount
    weights = [
        b + (t - b) * factor
        for b, t in zip(CATEGORICAL_BASELINE_WEIGHTS, CATEGORICAL_TARGET_WEIGHTS, strict=False)
    ]
    total = sum(weights) or 1.0
    return [
        {"value": v, "count": round(w / total * 1000)}
        for v, w in zip(CATEGORICAL_BUCKETS, weights, strict=False)
    ]


def _numeric_buckets(progress: float, drift_amount: float) -> list[dict]:
    factor = _drift_factor(progress) * drift_amount
    mean = NUMERIC_BASELINE_MEAN + (NUMERIC_TARGET_MEAN - NUMERIC_BASELINE_MEAN) * factor
    out: list[dict] = []
    for i in range(NUMERIC_BUCKETS):
        x = float(i) * 10.0
        w = math.exp(-((x - mean) ** 2) / (2 * NUMERIC_SIGMA ** 2))
        out.append({"value": x, "count": round(w * 1000)})
    return out


def _generate_distribution_rows(
    snapshot: TableSnapshot,
    days: int,
    end: datetime,
) -> list[dict]:
    """Один column_distribution-снапшот в день. Дрейфит первая (по алфавиту)
    колонка — drift.py покажет её как critical, остальные — ok."""
    if not snapshot.columns or days <= 0:
        return []
    drift_target = sorted(c["column"] for c in snapshot.columns)[0]
    rows: list[dict] = []
    for day in range(days):
        # day=0 — самый старый снапшот, day=days-1 — текущий конец окна.
        ts = end - timedelta(days=days - 1 - day)
        progress = day / (days - 1) if days > 1 else 1.0
        for col in snapshot.columns:
            drift_amount = 1.0 if col["column"] == drift_target else 0.0
            data_type = col.get("data_type") or "text"
            if _is_numeric_type(data_type):
                buckets = _numeric_buckets(progress, drift_amount)
            else:
                buckets = _categorical_buckets(progress, drift_amount)
            total = sum(b["count"] for b in buckets)
            rows.append({
                "ts": ts,
                "table_name": snapshot.table_name,
                "metric_name": "column_distribution",
                "value": float(total),
                "tags": {
                    "column": col["column"],
                    "data_type": data_type,
                    "buckets": buckets,
                },
            })
    return rows


_PURGE_TABLES = (
    "metrics", "anomaly_scores", "changepoints", "drift_reports",
    "schema_events", "schema_snapshots", "notifications",
)


# Синтетические Telegram-уведомления для страницы /dashboard/notifications.
# Профили подобраны так, чтобы лента истории показывала разные типы событий и
# смесь успешных доставок с одной-двумя ошибками — иначе UI выглядит «слишком
# идеально». Время каждого уведомления привязано к progress в окне сида.
@dataclass(frozen=True)
class NotificationProfile:
    progress: float
    event_type: str
    table_name: str
    metric_name: str | None
    message: str
    status: str = "sent"
    error: str | None = None


NOTIFICATION_PROFILES: tuple[NotificationProfile, ...] = (
    NotificationProfile(
        progress=0.20,
        event_type="anomaly",
        table_name="events",
        metric_name="null_rate",
        message=(
            "🚨 [events] Аномалия (score: -0.2134)\n"
            "Объяснение: резкий выброс null_rate — вероятная причина: сбой ETL "
            "по колонке ip_address."
        ),
    ),
    NotificationProfile(
        progress=0.40,
        event_type="schema_drift",
        table_name="users",
        metric_name=None,
        message=(
            "📋 [users] Дрейф схемы:\n"
            "  • column_added — phone (varchar)"
        ),
    ),
    NotificationProfile(
        progress=0.50,
        event_type="schema_drift",
        table_name="events",
        metric_name=None,
        message=(
            "📋 [events] Дрейф схемы:\n"
            "  • nullable_changed — ip_address (inet)"
        ),
    ),
    NotificationProfile(
        progress=0.55,
        event_type="anomaly",
        table_name="orders",
        metric_name="row_count",
        message=(
            "🚨 [orders] Аномалия (score: -0.1842)\n"
            "Объяснение: row_count подскочил выше прогноза — вероятная "
            "причина: повторный импорт пакета заказов."
        ),
    ),
    NotificationProfile(
        progress=0.62,
        event_type="changepoint",
        table_name="events",
        metric_name="null_rate",
        message="📈 [events] Change-point: null_rate 2.0% → 25.0% (2026-04-30)",
    ),
    NotificationProfile(
        progress=0.70,
        event_type="anomaly",
        table_name="products",
        metric_name="row_count",
        message=(
            "🚨 [products] Аномалия (score: -0.1721)\n"
            "Объяснение: краткосрочный спад каталога — возможен сбой "
            "пайплайна синхронизации."
        ),
        status="failed",
        error="telegram_error: Bad Request: chat not found",
    ),
    NotificationProfile(
        progress=0.78,
        event_type="changepoint",
        table_name="users",
        metric_name="row_count",
        message="📈 [users] Change-point: row_count 53,000 → 71,000 (2026-05-04)",
    ),
    NotificationProfile(
        progress=0.85,
        event_type="root_cause",
        table_name="events",
        metric_name="null_rate",
        message=(
            "🧠 [events] LLM root-cause: рост NULL в ip_address скоррелирован "
            "со сменой nullability колонки 50% назад — рекомендуется проверить "
            "ETL-задачу `etl_events_ingest`."
        ),
    ),
    NotificationProfile(
        progress=0.90,
        event_type="forecast",
        table_name="orders",
        metric_name="row_count",
        message=(
            "📊 [orders] Прогноз: ожидаемый рост ~12% за следующие 7 дней; "
            "верхняя граница 95% CI = 95,300."
        ),
    ),
    NotificationProfile(
        progress=0.95,
        event_type="anomaly",
        table_name="orders",
        metric_name="row_count",
        message=(
            "🚨 [orders] Аномалия (score: -0.2410)\n"
            "Объяснение: финальная ступенька роста — соответствует выкатке "
            "новой версии чекаута."
        ),
        status="failed",
        error="not_configured",
    ),
)


def _generate_notifications(end: datetime, days: int, project_id: str = "legacy") -> int:
    """Запись синтетических Telegram-уведомлений в `notifications`.
    Возвращает количество созданных строк.

    Идемпотентность не отслеживаем — задача сидера прогоняется поверх
    очищенной БД (см. _PURGE_TABLES), поэтому повторный запуск без
    --reset действительно создаст дубликаты, как и для остальных таблиц.
    """
    if days <= 0:
        return 0
    saved = 0
    for p in NOTIFICATION_PROFILES:
        ts = end - timedelta(days=days * (1.0 - p.progress))
        save_notification(
            event_type=p.event_type,
            message=p.message,
            status=p.status,
            table_name=p.table_name,
            metric_name=p.metric_name,
            error=p.error,
            chat_id="seed",
            ts=ts,
            project_id=project_id,
        )
        saved += 1
    return saved


def _generate_schema_events(
    snapshot: TableSnapshot,
    days: int,
    end: datetime,
) -> list[dict]:
    """Синтетические schema_events по таблице из SCHEMA_EVENT_PROFILES.
    Если профиля нет — вернёт пустой список (новые таблицы ничего не получают)."""
    profiles = SCHEMA_EVENT_PROFILES.get(snapshot.table_name, [])
    if not profiles or days <= 0:
        return []
    rows: list[dict] = []
    for p in profiles:
        progress = float(p["progress"])
        ts = end - timedelta(days=days * (1.0 - progress))
        rows.append({
            "ts": ts,
            "table_name": snapshot.table_name,
            "change_type": p["change_type"],
            "column_name": p["column_name"],
            "details": p["details"],
        })
    return rows


def _purge_existing(project_id: str = "legacy") -> int:
    """Очистить metrics и производные таблицы перед ресидом.

    При project_id != "legacy" таблицы с project_id-колонкой чистятся scoped.
    Schema tables пока остаются глобальными: schema_snapshots/schema_events ещё
    не имеют project_id и будут вынесены в отдельную задачу.
    """
    _SCOPED = {
        "metrics", "notifications", "anomaly_scores", "changepoints",
        "drift_reports",
    }
    # Таблицы без project_id — всегда глобальный DELETE.
    _GLOBAL = [t for t in _PURGE_TABLES if t not in _SCOPED]

    deleted = 0
    with get_monitor_engine().begin() as conn:
        if project_id != "legacy":
            for table_name in _SCOPED:
                result = conn.execute(
                    text(f"DELETE FROM {table_name} WHERE project_id = :pid"),
                    {"pid": project_id},
                )
                deleted += result.rowcount or 0
            if _GLOBAL:
                logger.warning(
                    "--reset с project_id=%s: таблицы %s очищены глобально — "
                    "project_id-скоупинг для них будет в отдельной задаче",
                    project_id, ", ".join(_GLOBAL),
                )
            for table_name in _GLOBAL:
                result = conn.execute(text(f"DELETE FROM {table_name}"))
                deleted += result.rowcount or 0
        else:
            for table_name in _PURGE_TABLES:
                result = conn.execute(text(f"DELETE FROM {table_name}"))
                deleted += result.rowcount or 0
    return deleted


def main(
    days: int = 14,
    interval_minutes: int = 60,
    reset: bool = False,
    seed: int = 42,
    schema: str | None = None,
    project_id: str = "legacy",
) -> dict:
    rng = random.Random(seed)
    end = datetime.now(UTC).replace(second=0, microsecond=0)

    snapshots = _capture_snapshots(schema=schema)
    if not snapshots:
        logger.warning("В target DB не найдено таблиц — сидить нечего")
        return {"snapshots": 0, "rows": 0, "deleted": 0, "ticks": 0}

    deleted = _purge_existing(project_id) if reset else 0

    timestamps = _build_timestamps(end, days, interval_minutes)
    all_rows: list[dict] = []
    schema_event_rows: list[dict] = []
    for snap in snapshots:
        all_rows.extend(_generate_metric_rows(snap, timestamps, rng, days))
        all_rows.extend(_generate_distribution_rows(snap, days, end))
        schema_event_rows.extend(_generate_schema_events(snap, days, end))

    # #138: project_id passed via CLI; default is 'legacy' for backwards compat.
    saved = save_metrics(all_rows, project_id)
    schema_events_saved = save_schema_events(schema_event_rows) if schema_event_rows else 0
    notifications_saved = _generate_notifications(end, days, project_id)
    print(
        f"Засеяно {saved} строк метрик по {len(snapshots)} таблицам "
        f"({len(timestamps)} тиков, {days} дн.), "
        f"{schema_events_saved} schema-events, "
        f"{notifications_saved} уведомлений. "
        f"Reset удалил {deleted} строк."
    )
    return {
        "snapshots": len(snapshots),
        "rows": saved,
        "deleted": deleted,
        "ticks": len(timestamps),
        "schema_events": schema_events_saved,
        "notifications": notifications_saved,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Бэкфилл monitor.db синтетической историей метрик."
    )
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--interval-minutes", type=int, default=60)
    parser.add_argument(
        "--reset", action="store_true",
        help=(
            "Удалить существующие данные перед сидом. При --project-id != legacy "
            "metrics/notifications чистятся scoped; остальные таблицы — глобально."
        ),
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--project-id", default="legacy",
        help="project_id для записи метрик и уведомлений (default: legacy)",
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    main(args.days, args.interval_minutes, reset=args.reset, seed=args.seed,
         project_id=args.project_id)
