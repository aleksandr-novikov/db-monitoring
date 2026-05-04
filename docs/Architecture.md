# DB Monitor — Техническая документация Sprint 2

> Документ описывает архитектуру, компонентную модель, схемы интеграции,
> протоколы взаимодействия, форматы данных и технологический стек системы
> мониторинга качества данных **DB Monitor** по состоянию на Sprint 2.

---

## Содержание

1. [Архитектура системы](#1-архитектура-системы)
2. [Компонентная модель](#2-компонентная-модель)
3. [Схемы интеграции](#3-схемы-интеграции)
4. [Протоколы взаимодействия](#4-протоколы-взаимодействия)
5. [Форматы данных](#5-форматы-данных)
6. [Технологический стек](#6-технологический-стек)
7. [Конфигурация](#7-конфигурация)

---

# 1. Архитектура системы

## 1.1 Назначение системы

**DB Monitor** — веб-приложение для автоматического мониторинга качества данных в реляционных базах данных. Система подключается к мониторируемой БД, периодически собирает метрики состояния таблиц, обнаруживает аномалии и изменения с помощью алгоритмов машинного обучения, визуализирует результаты на интерактивном дашборде и сигнализирует о проблемах.

**Целевая аудитория:** команды разработки и аналитики, которые хотят отслеживать деградацию данных в production-БД без ручных запросов и построения пайплайнов мониторинга с нуля.

---

## 1.2 Контекст развёртывания

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Docker Container                            │
│                                                                     │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │                    DB Monitor (Flask)                        │  │
│   │                                                              │  │
│   │  ┌─────────────┐  ┌──────────┐  ┌───────┐  ┌────────────┐  │  │
│   │  │  Collectors │  │ ML Layer │  │  API  │  │  Frontend  │  │  │
│   │  └──────┬──────┘  └────┬─────┘  └───┬───┘  └─────┬──────┘  │  │
│   │         │              │             │             │         │  │
│   │         └──────────────┴─────────────┴─────────────┘         │  │
│   │                              │                                │  │
│   │                    ┌─────────▼─────────┐                     │  │
│   │                    │   monitor.db      │                     │  │
│   │                    │   (SQLite)        │                     │  │
│   │                    └───────────────────┘                     │  │
│   └──────────────────────────────┬───────────────────────────────┘  │
│                                  │ volume mount                     │
│              ┌───────────────────┘                                  │
└──────────────┼──────────────────────────────────────────────────────┘
               │
               │  DATABASE_URL (TCP/TLS)
               ▼
┌──────────────────────────────────┐
│   Мониторируемая БД              │
│   PostgreSQL / MySQL / ClickHouse│
│   (Supabase в demo-режиме)       │
└──────────────────────────────────┘

                    ▲
                    │ HTTP :5001
               Браузер пользователя
```

**Две независимые БД:**
- **Мониторируемая БД** (`DATABASE_URL`) — production-источник, из которого читаются метрики. Запись в неё не производится.
- **Monitor DB** (`MONITOR_DB_URL`) — локальное хранилище метрик, результатов ML и истории схем. По умолчанию SQLite-файл `monitor.db`, монтируется как Docker volume.

---

## 1.3 Слои системы

Система разделена на пять логических слоёв с однонаправленными зависимостями:

| Слой | Компоненты | Ответственность |
|---|---|---|
| **Сбор данных** | `collectors/` | Подключение к мониторируемой БД, извлечение метрик, сохранение в Monitor DB |
| **Хранение** | `app/metrics_storage.py`, `monitor.db` | Персистентность всех метрик, результатов ML, событий схем |
| **ML-аналитика** | `ml/` | Обнаружение аномалий, прогноз, чейндж-поинты, дрейф |
| **API** | `app/api.py` | REST-интерфейс для фронтенда и внешних интеграций |
| **Интерфейс** | `templates/`, `static/` | Визуализация дашбордов, Plotly-графики |

**Правило зависимостей:** каждый слой зависит только от слоя ниже. ML не знает об API. Collectors не знают о ML. API не обращается к мониторируемой БД напрямую — только через хранилище.

---

## 1.4 Режимы работы

Система работает в двух режимах одновременно:

**Режим 1 — Фоновый сбор (push)**
APScheduler запускает периодические задачи без участия пользователя:
```
каждые 15 мин → collect_all_tables()
каждый час    → detect_changepoints()
03:00 UTC     → retrain_forecasts()
04:00 UTC     → retrain_anomaly_detectors()
```

**Режим 2 — Обслуживание запросов (pull)**
Flask-сервер отвечает на HTTP-запросы от браузера. ML-модели при необходимости дообучаются прямо на запросе (on-demand), если персистированная модель устарела.

---

## 1.5 Граница ответственности

| Зона | Что делает система | Что НЕ делает система |
|---|---|---|
| Мониторируемая БД | Читает метаданные через `information_schema`, считает NULL, измеряет размер | Не изменяет данные, не создаёт объекты, не делает полный scan без фильтра |
| Monitor DB | Пишет метрики, результаты ML, события схем | Не хранит пользовательские данные из мониторируемой БД |
| ML | Строит модели на агрегированных метриках | Не работает напрямую с исходными строками таблиц |
| Уведомления | — | На данный момент не реализованы (запланированы в следующем спринте) |

---

## 1.6 Нефункциональные характеристики

| Характеристика | Значение |
|---|---|
| Доступность | Single-instance, без HA. Сбой контейнера = недоступность. |
| Масштабируемость | Вертикальная. Ограничена SQLite (один writer). Post-MVP — Postgres + TimescaleDB. |
| Безопасность | Нет аутентификации (MVP). `SECRET_KEY` для Flask-сессий. TLS на стороне DATABASE_URL. |
| Производительность | Сбор метрик одной таблицы — < 2с. ML-скоринг при запросе — < 500мс при наличии модели. |
| Воспроизводимость | Полностью контейнеризирована. `make server` — единственная точка входа. |

---

# 2. Компонентная модель

## 2.1 Обзор компонентов

Система состоит из 11 компонентов, сгруппированных в 5 пакетов. Каждый компонент имеет строго определённую ответственность и публичный интерфейс.

```
┌─────────────────────────────────────────────────────────────────────────┐
│ db-monitoring                                                           │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ collectors/                                                      │   │
│  │  ┌─────────────────────┐    ┌───────────────────────────────┐   │   │
│  │  │  MetricsCollector   │    │         Scheduler             │   │   │
│  │  │  metrics_collector  │    │       scheduler.py            │   │   │
│  │  └─────────┬───────────┘    └──────────────┬────────────────┘   │   │
│  └────────────┼────────────────────────────────┼───────────────────┘   │
│               │                                │                        │
│  ┌────────────┼────────────────────────────────┼───────────────────┐   │
│  │ app/       │                                │                   │   │
│  │  ┌─────────▼───────────┐    ┌───────────────▼────────────────┐  │   │
│  │  │    DBAdapter        │    │        MetricsStorage          │  │   │
│  │  │      db.py          │    │      metrics_storage.py        │  │   │
│  │  │ Postgres/MySQL/CH   │    │  (monitor.db через SQLAlchemy) │  │   │
│  │  └─────────────────────┘    └───────────────┬────────────────┘  │   │
│  │                                             │                   │   │
│  │  ┌──────────────────────────────────────────┼───────────────┐   │   │
│  │  │                   API Blueprint          │               │   │   │
│  │  │                    api.py                │               │   │   │
│  │  └──────────────────────────────────────────┘               │   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ ml/                                                              │   │
│  │  ┌──────────────────┐  ┌───────────────┐  ┌──────────────────┐  │   │
│  │  │ AnomalyDetector  │  │   Forecast    │  │  Changepoint     │  │   │
│  │  │anomaly_detector  │  │  forecast.py  │  │  changepoint.py  │  │   │
│  │  └──────────────────┘  └───────────────┘  └──────────────────┘  │   │
│  │  ┌──────────────────┐                                            │   │
│  │  │  DriftDetector   │                                            │   │
│  │  │    drift.py      │                                            │   │
│  │  └──────────────────┘                                            │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │ templates/ + static/          Frontend (Jinja2 + Plotly.js)      │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 2.2 Описание компонентов

### 2.2.1 MetricsCollector (`collectors/metrics_collector.py`)

**Ответственность:** извлечение метрик качества одной таблицы из мониторируемой БД за один тик.

**Входы:**
- `table_name: str` — имя таблицы
- `schema: str` — схема (по умолчанию `public`)

**Выходы:**
- `list[dict]` — список строк метрик для записи в Monitor DB

**Собираемые метрики за один тик:**

| Метрика | Тип | Описание |
|---|---|---|
| `row_count` | REAL | Количество строк (из `pg_stat_user_tables.n_live_tup` для PG) |
| `size_bytes` | REAL | Полный размер таблицы включая индексы |
| `last_modified` | REAL | Unix timestamp последнего `ANALYZE` |
| `null_count` | REAL | Количество NULL по каждой колонке (тег: `column`) |
| `null_rate` | REAL | Средняя доля NULL по всем колонкам таблицы |
| `column_distribution` | REAL | Top-20 значений по каждой колонке (тег: `column`, `data_type`, `buckets`) |

**Поведение при ошибке:** возвращает частичный результат (`row_count`/`size_bytes`) если не удалось получить NULL-статистику. Не бросает исключение.

---

### 2.2.2 Scheduler (`collectors/scheduler.py`)

**Ответственность:** управление всеми фоновыми задачами через APScheduler.

**Задачи:**

| ID задачи | Триггер | Функция | Описание |
|---|---|---|---|
| `collect_metrics` | interval, 15 мин | `collect_all_tables()` | Сбор метрик всех таблиц + скоринг аномалий |
| `retrain_forecasts` | cron 03:00 | `retrain_forecasts()` | Переобучение прогнозных моделей |
| `detect_changepoints` | interval, 1 час | `detect_changepoints()` | Обнаружение точек структурных изменений |
| `retrain_anomaly_detectors` | cron 04:00 | `retrain_anomaly_detectors()` | Переобучение моделей аномалий + скоринг 14 дней |

**Побочный эффект `collect_all_tables()`:** после каждого тика сбора вызывает `_score_recent_anomalies()` — скоринг последних 24 часов по уже обученным моделям. Если модель не обучена — тихо пропускает.

**Порядок зависимостей джобов:**
```
collect_metrics (15м)
       ↓ данные в monitor.db
detect_changepoints (1ч)
       ↓ changepoints в monitor.db
retrain_forecasts (03:00) — должен запускаться ПОСЛЕ detect_changepoints
retrain_anomaly_detectors (04:00)
```

---

### 2.2.3 DBAdapter (`app/db.py`)

**Ответственность:** диалект-специфичный доступ к мониторируемой БД. Изолирует разницу между СУБД за единым интерфейсом.

**Реализации:**

| Адаптер | СУБД | Особенности |
|---|---|---|
| `PostgresAdapter` | PostgreSQL | `FILTER (WHERE col IS NULL)` для NULL-подсчёта; `pg_stat_user_tables` для статистики |
| `MySQLAdapter` | MySQL / MariaDB | `information_schema.tables.table_rows` (оценочный, не точный); `update_time` может быть NULL |
| `ClickHouseAdapter` | ClickHouse | `system.tables` / `system.columns` / `system.parts`; `Nullable(T)` определяет nullable-ность |

**Публичный интерфейс:**

```python
list_tables(schema)           → list[dict]   # [{table_name, schema}]
table_stats(table, schema)    → dict | None  # {row_count, size_bytes, last_analyze}
table_schema(table, schema)   → list[dict]   # [{name, type, nullable}]
column_nulls(table, schema)   → list[dict]   # [{column, data_type, null_count, null_rate}]
column_distribution(table, schema, top_n=20) → list[dict]
```

**Ограничение:** `column_distribution` пропускает колонки типов `text`, `json`, `jsonb`, `bytea`, `uuid` — GROUP BY по ним дорог и лишён смысла для drift-анализа.

---

### 2.2.4 MetricsStorage (`app/metrics_storage.py`)

**Ответственность:** единственная точка записи и чтения Monitor DB. Все остальные компоненты работают с хранилищем только через этот модуль.

**Публичный интерфейс:**

```python
# Метрики
save_metrics(rows)                              → int
get_metrics(table, metric, window)              → list[dict]   # [{ts, value, tags}]
get_latest_metric(table, metric)                → dict | None

# Чейндж-поинты
save_changepoints(rows)                         → int
get_changepoints(table, metric, window=14d)     → list[dict]

# Аномалии
save_anomaly_scores(rows)                       → int
get_anomaly_scores(table, window=7d)            → list[dict]   # [{ts, score, is_anomaly}]

# Схема
save_schema_snapshot(table, columns)            → None
get_schema_snapshot(table)                      → list[dict] | None
save_schema_events(events)                      → int
get_schema_events(table, window=30d)            → list[dict]
```

---

### 2.2.5 AnomalyDetector (`ml/anomaly_detector.py`)

**Ответственность:** per-table обнаружение многомерных аномалий через Isolation Forest.

**Алгоритм:**
- Фичи: `[row_count, null_rate, Δrow_count, Δnull_rate]` — 4 измерения
- Дельты делают резкие скачки заметными независимо от абсолютного масштаба
- Предобработка: `StandardScaler` (z-score нормализация)
- Порог: `decision_function(x) < 0` — встроенная конвенция IsolationForest
- `contamination=0.01` — ~1% обучающих данных помечается как аномальные

**Жизненный цикл модели:**
```
train(table)          — обучение на 60 днях истории (MIN_POINTS=200)
      ↓ joblib
models/<table>__anomaly.joblib
      ↓
score_table(table, window_days=14) — скоринг окна по персистированной модели
      ↓ если модель отсутствует → on-demand train()
retrain_all()         — переобучение всех таблиц (вызывается ночным джобом)
```

**Артефакт модели (joblib payload):**
```python
{
    "model": IsolationForest,
    "scaler": StandardScaler,
    "trained_at": "2026-05-01T03:00:00",
    "n_points": 1344
}
```

---

### 2.2.6 Forecast (`ml/forecast.py`)

**Ответственность:** прогноз временного ряда метрики на N дней вперёд.

**Выбор модели по объёму истории:**

| Условие | Модель | Причина |
|---|---|---|
| ≥ 7 дней истории + Prophet установлен | Prophet | Учитывает сезонность, лучше на длинных рядах |
| < 7 дней или Prophet недоступен | LinearModel (OLS) | Быстрый fallback, работает с 2 точками |

**Известное ограничение:** обе модели обучаются на полном 60-дневном окне без учёта структурных переломов. При наличии step-change в истории CI может уходить в отрицательные значения. Исправление запланировано в issue [#63](https://github.com/aleksandr-novikov/db-monitoring/issues/63).

**Артефакт модели (joblib payload):**
```python
{
    "kind": "prophet" | "linear",
    "model": Prophet | LinearModel,
    "last_ts": "2026-05-04T16:00:00",
    "trained_at": "2026-05-04T03:00:00"
}
```

---

### 2.2.7 Changepoint (`ml/changepoint.py`)

**Ответственность:** обнаружение точек структурных изменений в метриках.

**Алгоритмы (каскад):**

| Алгоритм | Библиотека | Применение |
|---|---|---|
| PELT (RBF kernel) | `ruptures` | Основной; работает при наличии `ruptures` |
| CUSUM | pure Python | Fallback без зависимостей |

**Детрендинг:** для кумулятивных метрик (`row_count`, `size_bytes`) применяется детрендинг перед поиском изменений — иначе монотонный рост сам по себе даёт false positive.

**Дедупликация:** события в окне 72 часов в одном направлении сворачиваются в одно. Разнонаправленные (рост + спад) сохраняются как два отдельных события.

---

### 2.2.8 DriftDetector (`ml/drift.py`)

**Ответственность:** обнаружение статистического дрейфа распределений колонок.

**Методы:**

| Тип данных | Метод | Порог |
|---|---|---|
| Категориальные | PSI (Population Stability Index) | PSI > 0.2 → drift |
| Числовые | KS-test (Kolmogorov–Smirnov) | p-value < 0.05 → drift |

**Baseline:** rolling 7-дневное окно. Текущее распределение сравнивается с предыдущей неделей.

---

### 2.2.9 API Blueprint (`app/api.py`)

**Ответственность:** REST-интерфейс. Маршрутизация запросов к хранилищу и ML-компонентам. Валидация параметров.

Все эндпоинты описаны подробно в Разделе 4 (Протоколы взаимодействия).

---

### 2.2.10 SchemaCollector (`collectors/schema_collector.py`)

**Ответственность:** снимок структуры таблиц и обнаружение изменений схемы.

**Типы событий:**

| `change_type` | Описание |
|---|---|
| `column_added` | В таблице появилась новая колонка |
| `column_removed` | Колонка удалена |
| `type_changed` | Изменился тип данных колонки |
| `nullable_changed` | Изменилась nullable-ность колонки |

---

### 2.2.11 Frontend (`templates/`, `static/`)

**Ответственность:** серверный рендеринг страниц (Jinja2) + клиентская интерактивность (Plotly.js).

**Страницы:**

| Шаблон | URL | Содержимое |
|---|---|---|
| `overview.html` | `/` | Таблица всех мониторируемых таблиц с KPI |
| `table_detail.html` | `/table/<name>` | Графики метрик, прогноз, аномалии, схема |
| `schema.html` | `/schema/<name>` | Детальная схема таблицы + история изменений |

---

## 2.3 Матрица зависимостей компонентов

| Компонент | Зависит от |
|---|---|
| `MetricsCollector` | `DBAdapter` |
| `Scheduler` | `MetricsCollector`, `SchemaCollector`, `AnomalyDetector`, `Forecast`, `Changepoint`, `MetricsStorage` |
| `AnomalyDetector` | `MetricsStorage` |
| `Forecast` | `MetricsStorage` |
| `Changepoint` | `MetricsStorage` |
| `DriftDetector` | `MetricsStorage` |
| `API` | `MetricsStorage`, `DBAdapter`, `Forecast`, `DriftDetector` |
| `Frontend` | `API` (через fetch) |
| `MetricsStorage` | `monitor.db` (SQLite) |
| `DBAdapter` | Мониторируемая БД |

---

# 3. Схемы интеграции

## 3.1 Интеграция с мониторируемой БД

### 3.1.1 Подключение

Система подключается к мониторируемой БД через SQLAlchemy connection pool. Параметры подключения задаются переменной окружения `DATABASE_URL`.

```
DB Monitor (Flask)
        │
        │  DATABASE_URL
        │  (postgresql+psycopg2://user:pass@host:5432/db)
        │  (mysql+pymysql://user:pass@host:3306/db)
        │  (clickhouse+native://user:pass@host:9000/db)
        │
        ▼
┌──────────────────────────┐
│  SQLAlchemy Engine       │
│  pool_size=5             │
│  max_overflow=2          │
│  pool_pre_ping=True      │
│  connect_timeout=5s      │
└──────────────────────────┘
        │
        ▼
  Мониторируемая БД
```

**Параметры пула соединений:**

| Параметр | Значение | Назначение |
|---|---|---|
| `pool_size` | 5 | Постоянных соединений в пуле |
| `max_overflow` | 2 | Дополнительных соединений при пике |
| `pool_pre_ping` | `True` | Проверка соединения перед использованием |
| `connect_timeout` | 5с | Таймаут для PostgreSQL и MySQL |

### 3.1.2 Что читается из мониторируемой БД

Система делает **только SELECT-запросы** к системным представлениям. К пользовательским данным не обращается.

| СУБД | Источник | Читаемые данные |
|---|---|---|
| PostgreSQL | `pg_stat_user_tables` | `n_live_tup`, `pg_total_relation_size`, `last_analyze` |
| PostgreSQL | `information_schema.tables` | Список таблиц схемы |
| PostgreSQL | `information_schema.columns` | Список колонок, типы, nullable |
| MySQL | `information_schema.tables` | `table_rows`, `data_length`, `index_length`, `update_time` |
| ClickHouse | `system.tables`, `system.columns`, `system.parts` | Аналоги pg_stat |

**Пользовательские данные** читаются только для подсчёта NULL и distribution (`GROUP BY` top-20) — без передачи самих значений в Monitor DB. В хранилище попадают только агрегаты.

---

## 3.2 Интеграция с Monitor DB (SQLite)

### 3.2.1 Подключение

```
Все компоненты (ML, Collectors, API)
        │
        │  MONITOR_DB_URL
        │  (sqlite:///monitor.db — default)
        │
        ▼
┌────────────────────────────┐
│  MetricsStorage            │
│  app/metrics_storage.py    │
│  (единственная точка       │
│   доступа к monitor.db)    │
└────────────────────────────┘
        │
        ▼
   monitor.db (SQLite файл)
   /app/monitor.db внутри контейнера
   ← volume mount →
   ./monitor.db на хосте
```

### 3.2.2 Volume mount

Monitor DB монтируется как Docker volume, чтобы данные переживали перезапуск контейнера:

```bash
docker run \
  -v $(CURDIR)/monitor.db:/app/monitor.db \
  -v $(CURDIR)/models:/app/models \
  db-monitoring
```

Аналогично монтируется директория `models/` с joblib-артефактами ML-моделей.

---

## 3.3 Интеграция Frontend ↔ API

### 3.3.1 Схема взаимодействия

```
Браузер
   │
   │  GET /  →  Jinja2 рендер overview.html
   │  GET /table/<name>  →  Jinja2 рендер table_detail.html
   │
   │  При загрузке страницы table_detail.html:
   │
   ├─ fetch /api/metrics/<table>?metric=row_count&range=7d
   ├─ fetch /api/metrics/<table>?metric=null_rate&range=7d
   ├─ fetch /api/forecast/<table>?metric=row_count&horizon=7d
   ├─ fetch /api/changepoints/<table>?range=14d
   └─ fetch /api/anomalies/<table>?range=7d
              │
              │  JSON ответы
              ▼
         Plotly.js
    строит интерактивные графики
```

### 3.3.2 Транспорт

- Протокол: HTTP/1.1 (без WebSocket — данные не стримятся)
- Формат: JSON (application/json)
- Аутентификация: отсутствует (MVP)
- CORS: не настроен (single-origin, браузер и сервер на одном хосте)

---

## 3.4 Интеграция ML ↔ Filesystem

ML-модели персистируются на диск через joblib. Это единственная интеграция компонентов с файловой системой помимо monitor.db.

```
ml/anomaly_detector.py          ml/forecast.py
        │                              │
        │ joblib.dump()                │ joblib.dump()
        ▼                              ▼
models/<table>__anomaly.joblib   models/<table>__row_count.joblib
                                 models/<table>__size_bytes.joblib

        │                              │
        │ joblib.load()                │ joblib.load()
        ▼                              ▼
   score_table()                  forecast()
(при каждом тике и запросе)    (при каждом запросе /api/forecast)
```

**Именование файлов:**

| Тип модели | Шаблон имени файла |
|---|---|
| Anomaly | `models/<table>__anomaly.joblib` |
| Forecast row_count | `models/<table>__row_count.joblib` |
| Forecast size_bytes | `models/<table>__size_bytes.joblib` |

Спецсимволы в имени таблицы (`/`, пробел) заменяются на `_`.

---

## 3.5 Интеграция Scheduler ↔ Flask App

Scheduler запускается как фоновый поток внутри Flask-процесса через `start_scheduler(app)`, который вызывается при старте приложения.

```
Flask app startup
       │
       ▼
app/__init__.py → start_scheduler(app)
       │
       ▼
APScheduler BackgroundScheduler
(отдельный поток внутри того же процесса)
       │
       ├─ каждые 15 мин → collect_all_tables()
       ├─ каждый час    → detect_changepoints()
       ├─ cron 03:00    → retrain_forecasts()
       └─ cron 04:00    → retrain_anomaly_detectors()
```

**Важно:** scheduler и Flask-сервер разделяют одну Monitor DB. Конкурентная запись безопасна — SQLite сериализует транзакции через file lock. При масштабировании на несколько воркеров это станет узким местом (post-MVP: переход на Postgres).

---

## 3.6 Схема полного потока данных (end-to-end)

```
Мониторируемая БД (Postgres/MySQL/ClickHouse)
        │
        │  SELECT (каждые 15 мин, APScheduler)
        ▼
MetricsCollector.collect(table)
        │
        │  list[dict] метрик
        ▼
MetricsStorage.save_metrics()
        │
        ▼
monitor.db → таблица metrics
        │
        ├──────────────────────────────────────────────┐
        │  (каждый час)                                │  (03:00)
        ▼                                              ▼
detect_changepoints()                         retrain_forecasts()
        │                                              │
        ▼                                              ▼
monitor.db → changepoints              models/<table>__*.joblib
                                                       │
        │  (после каждого тика collect)               │
        ▼                                             │
_score_recent_anomalies()                            │
        │                                             │
        ▼                                             │
monitor.db → anomaly_scores                          │
        │                                             │
        └──────────────┬──────────────────────────────┘
                       │
                       ▼
                   API (Flask)
                       │
                  GET /api/...
                       │
                       ▼
                   Браузер
                  Plotly.js
```

---

# 4. Протоколы взаимодействия

## 4.1 REST API

### 4.1.1 Общие соглашения

| Параметр | Значение |
|---|---|
| Base URL | `http://host:5001/api` |
| Формат запроса | Query string параметры |
| Формат ответа | `application/json` |
| Кодировка | UTF-8 |
| Аутентификация | Отсутствует (MVP) |
| Успешный ответ | HTTP 200 |
| Ошибка валидации | HTTP 400 `{"error": "..."}` |
| Не найдено | HTTP 404 `{"error": "table not found"}` |
| Недостаточно данных | HTTP 422 `{"error": "insufficient_data", "message": "..."}` |

**Допустимые значения параметра `range`:**
`1h` | `6h` | `24h` | `7d` | `14d` | `30d`

**Допустимые значения параметра `horizon`:**
`1d` | `3d` | `7d` | `14d` | `30d`

---

### 4.1.2 GET /api/tables

Список всех мониторируемых таблиц с последними метриками.

**Запрос:**
```
GET /api/tables
```

**Ответ 200:**
```json
[
  {
    "table_name": "users",
    "row_count": 67099.0,
    "null_rate": 0.0552,
    "last_check": "2026-05-04T14:00:00+00:00"
  },
  {
    "table_name": "orders",
    "row_count": null,
    "null_rate": null,
    "last_check": null
  }
]
```

> `row_count`, `null_rate`, `last_check` — `null` до первого запуска коллектора.

---

### 4.1.3 GET /api/metrics/\<table\>

Временной ряд одной метрики таблицы.

**Запрос:**
```
GET /api/metrics/{table_name}?metric={metric}&range={range}
```

**Параметры:**

| Параметр | Обязателен | По умолчанию | Допустимые значения |
|---|---|---|---|
| `metric` | нет | `row_count` | `row_count`, `null_rate`, `null_count`, `size_bytes`, `last_modified` |
| `range` | нет | `24h` | `1h`, `6h`, `24h`, `7d`, `14d`, `30d` |

**Ответ 200:**
```json
[
  {"ts": "2026-04-28T00:00:00+00:00", "value": 51114.0},
  {"ts": "2026-04-28T00:15:00+00:00", "value": 51115.0}
]
```

> Возвращает `[]` если данных за запрошенный период нет.

**Ответ 400:**
```json
{"error": "metric must be one of ['last_modified', 'null_count', 'null_rate', 'row_count', 'size_bytes']"}
```

---

### 4.1.4 GET /api/forecast/\<table\>

Прогноз метрики на N дней вперёд с доверительным интервалом 95%.

**Запрос:**
```
GET /api/forecast/{table_name}?metric={metric}&horizon={horizon}
```

**Параметры:**

| Параметр | Обязателен | По умолчанию | Допустимые значения |
|---|---|---|---|
| `metric` | нет | `row_count` | `row_count`, `size_bytes` |
| `horizon` | нет | `7d` | `1d`, `3d`, `7d`, `14d`, `30d` |

**Ответ 200:**
```json
[
  {
    "ts": "2026-05-05T00:00:00+00:00",
    "yhat": 67150.0,
    "yhat_lower": 65200.0,
    "yhat_upper": 69100.0
  }
]
```

> Точки с шагом 1 час на весь горизонт. `yhat_lower` ≥ 0 (floor применяется в обоих предсказателях).

**Ответ 422:**
```json
{
  "error": "insufficient_data",
  "message": "need at least 2 points for users/row_count, got 1"
}
```

---

### 4.1.5 GET /api/anomalies/\<table\>

Аномальные точки метрик таблицы за период.

**Запрос:**
```
GET /api/anomalies/{table_name}?range={range}
```

**Параметры:**

| Параметр | Обязателен | По умолчанию | Допустимые значения |
|---|---|---|---|
| `range` | нет | `7d` | `1h`, `6h`, `24h`, `7d`, `14d`, `30d` |

**Ответ 200:**
```json
[
  {
    "ts": "2026-05-01T14:00:00+00:00",
    "score": -0.042239,
    "is_anomaly": 1
  },
  {
    "ts": "2026-05-01T14:15:00+00:00",
    "score": 0.189097,
    "is_anomaly": 0
  }
]
```

> `score` — сырое значение `decision_function` Isolation Forest. Отрицательное значение = аномалия. Возвращает `[]` если модель ещё не обучена.

---

### 4.1.6 GET /api/changepoints/\<table\>

Детектированные точки структурных изменений метрик.

**Запрос:**
```
GET /api/changepoints/{table_name}?metric={metric}&range={range}
```

**Параметры:**

| Параметр | Обязателен | По умолчанию | Допустимые значения |
|---|---|---|---|
| `metric` | нет | все метрики | `row_count`, `null_rate`, `null_count`, `size_bytes`, `last_modified` |
| `range` | нет | `14d` | `1h`, `6h`, `24h`, `7d`, `14d`, `30d` |

**Ответ 200:**
```json
[
  {
    "ts": "2026-05-01T17:53:00+00:00",
    "table_name": "users",
    "metric_name": "row_count",
    "score": 29.558,
    "value_before": 51611.396,
    "value_after": 66866.542
  }
]
```

> `score` — нормализованная величина сдвига (mean shift / pre-std). Чем выше — тем значительнее переход.

---

### 4.1.7 GET /api/drift/\<table\>

Отчёт о дрейфе распределений колонок таблицы.

**Запрос:**
```
GET /api/drift/{table_name}
```

**Ответ 200:**
```json
[
  {
    "column": "signup_source",
    "data_type": "text",
    "psi": 0.312,
    "ks_pvalue": null,
    "is_drift": true,
    "severity": "high"
  },
  {
    "column": "age",
    "data_type": "integer",
    "psi": null,
    "ks_pvalue": 0.003,
    "is_drift": true,
    "severity": "medium"
  }
]
```

> `psi` заполнен для категориальных колонок, `ks_pvalue` — для числовых. `severity`: `ok` | `low` | `medium` | `high`.

---

### 4.1.8 GET /api/schema/\<table\>

Текущая структура таблицы из мониторируемой БД.

**Запрос:**
```
GET /api/schema/{table_name}
```

**Ответ 200:**
```json
[
  {"name": "id",    "type": "uuid",    "nullable": false},
  {"name": "email", "type": "text",    "nullable": true},
  {"name": "age",   "type": "integer", "nullable": true}
]
```

**Ответ 404:**
```json
{"error": "table not found"}
```

---

### 4.1.9 GET /api/schema/\<table\>/changes

История изменений схемы таблицы.

**Запрос:**
```
GET /api/schema/{table_name}/changes?range={range}
```

**Параметры:**

| Параметр | Обязателен | По умолчанию | Допустимые значения |
|---|---|---|---|
| `range` | нет | `30d` | `1h`, `6h`, `24h`, `7d`, `14d`, `30d` |

**Ответ 200:**
```json
[
  {
    "ts": "2026-05-02T00:00:00+00:00",
    "table_name": "users",
    "change_type": "column_added",
    "column_name": "country",
    "details": {
      "before": null,
      "after": {"type": "text", "nullable": false}
    }
  }
]
```

> `change_type`: `column_added` | `column_removed` | `type_changed` | `nullable_changed`

---

## 4.2 Протокол работы Scheduler

### 4.2.1 Последовательность джобов

```
T+0m    collect_all_tables()
         │
         ├─ для каждой таблицы: MetricsCollector.collect()
         │   → save_metrics()
         │
         └─ _score_recent_anomalies()
             для каждой таблицы:
               score_table(window_days=1)
               → save_anomaly_scores()
               пропустить если модель не обучена

T+60m   detect_changepoints()
         │
         └─ для каждой таблицы × метрики:
             detect_changepoints(table, metric, window_days=14)
             → save_changepoints()

T+180m (03:00 UTC)   retrain_forecasts()
         │
         └─ для каждой таблицы × метрики:
             train(table, metric)  ← переобучение на 60 днях истории
             → сохраняет models/<table>__<metric>.joblib

T+240m (04:00 UTC)   retrain_anomaly_detectors()
         │
         ├─ retrain_all()
         │   для каждой таблицы:
         │     train(table)  ← переобучение на 60 днях
         │     → сохраняет models/<table>__anomaly.joblib
         │
         └─ для каждой таблицы:
             score_table(window_days=14)
             → save_anomaly_scores()
```

### 4.2.2 On-demand переобучение

Помимо ночного расписания, модели могут переобучаться прямо на HTTP-запросе:

| Компонент | Условие | Действие |
|---|---|---|
| `forecast()` | Персистированная модель устарела (`last_ts` расходится > 1ч) | `train()` на месте |
| `score_table()` | Файл модели отсутствует | `train()` на месте |

---

## 4.3 Протокол обнаружения аномалий

```
Входные данные:
  metrics(table, row_count, window=60d)
  metrics(table, null_rate, window=60d)
           │
           ▼
Построение фич (inner join по ts):
  X = [row_count, null_rate, Δrow_count, Δnull_rate]
  первая точка отбрасывается (нет дельты)
           │
           ▼
StandardScaler.fit_transform(X)
           │
           ▼
IsolationForest.fit(X_scaled)
  n_estimators=100
  contamination=0.01
  random_state=42
           │
           ▼  joblib.dump()
models/<table>__anomaly.joblib
           │
           ▼  при скоринге:
IsolationForest.decision_function(X_scaled)
           │
           ├─ score < 0  → is_anomaly = 1
           └─ score ≥ 0  → is_anomaly = 0
           │
           ▼
save_anomaly_scores([{ts, table_name, score, is_anomaly}])
```

---

## 4.4 Протокол обнаружения чейндж-поинтов

```
Входные данные:
  metrics(table, metric, window=14d)
           │
           ▼
Детрендинг (только для row_count, size_bytes):
  values = values - линейный тренд
           │
           ▼
Попытка PELT (ruptures):
  model = ruptures.Pelt(model='rbf')
  breakpoints = model.fit_predict(values, pen=penalty)
           │ если ruptures недоступен
           ▼
Fallback CUSUM (pure Python):
  кумулятивная сумма отклонений от среднего
  порог: 3σ
           │
           ▼
Для каждого breakpoint:
  score = |mean_after - mean_before| / std_before
  value_before = mean(LOCAL_WINDOW=48 точек до)
  value_after  = mean(LOCAL_WINDOW=48 точек после)
           │
           ▼
Дедупликация (DEDUPE_WINDOW=72ч):
  однонаправленные события → берётся с максимальным score
  разнонаправленные → сохраняются оба
           │
           ▼
save_changepoints([{ts, table_name, metric_name,
                    score, value_before, value_after}])
```

---

# 5. Форматы данных

## 5.1 Схема monitor.db (SQLite)

Единственное хранилище состояния монитора — файл `monitor.db`. Применяется через `scripts/metrics_schema.sql` при каждом запуске через `_apply_schema()` (идемпотентно, `CREATE TABLE IF NOT EXISTS`).

---

### Таблица `metrics`

Временной ряд собранных числовых показателей. Основная таблица системы.

```sql
CREATE TABLE metrics (
    ts          TEXT NOT NULL,   -- ISO 8601 UTC без суффикса Z: "2026-04-22T07:30:00"
    table_name  TEXT NOT NULL,   -- "users", "orders", ...
    metric_name TEXT NOT NULL,   -- см. справочник метрик ниже
    value       REAL NOT NULL,
    tags        TEXT             -- NULL или JSON-объект (см. §5.3)
);

CREATE INDEX idx_metrics_table_ts  ON metrics (table_name, ts);
CREATE INDEX idx_metrics_metric_ts ON metrics (metric_name, ts);
```

**Справочник metric_name:**

| metric_name           | Единица     | tags                                      | Описание                                          |
|-----------------------|-------------|-------------------------------------------|---------------------------------------------------|
| `row_count`           | строки      | —                                         | Количество строк в таблице                        |
| `size_bytes`          | байты       | —                                         | Физический размер таблицы                         |
| `null_rate`           | доля [0–1]  | —                                         | Среднее отношение NULL ко всем значениям по всем колонкам |
| `null_count`          | строки      | `{"column": "<name>"}`                    | Количество NULL в конкретной колонке              |
| `last_modified`       | Unix epoch  | —                                         | Время последнего `ANALYZE` (из `pg_stat_user_tables`) |
| `column_distribution` | строки      | `{"column", "data_type", "buckets": [...]}` | Распределение значений числовой/категориальной колонки |

---

### Таблица `changepoints`

Обнаруженные точки изменения тренда (PELT/CUSUM). Записывает ежечасовой job.

```sql
CREATE TABLE changepoints (
    ts            TEXT NOT NULL,   -- ISO 8601 UTC момента разрыва
    table_name    TEXT NOT NULL,
    metric_name   TEXT NOT NULL,
    score         REAL NOT NULL,   -- |mean_after − mean_before| / std_before
    value_before  REAL NOT NULL,   -- среднее по LOCAL_WINDOW=48 точек до разрыва
    value_after   REAL NOT NULL,   -- среднее по LOCAL_WINDOW=48 точек после
    detected_at   TEXT NOT NULL,   -- ISO 8601 UTC момента записи в БД
    PRIMARY KEY (ts, table_name, metric_name)
);
```

Составной PK гарантирует идемпотентность: повторный запуск детектора на тех же данных не создаёт дублей.

---

### Таблица `schema_snapshots`

Актуальный список колонок каждой таблицы на момент последнего обхода.

```sql
CREATE TABLE schema_snapshots (
    table_name  TEXT NOT NULL PRIMARY KEY,
    columns     TEXT NOT NULL,    -- JSON: [{"name", "type", "nullable"}, ...]
    captured_at TEXT NOT NULL     -- ISO 8601 UTC
);
```

**Формат `columns` (JSON-массив):**

```json
[
  {"name": "id",    "type": "integer",          "nullable": false},
  {"name": "email", "type": "character varying", "nullable": true},
  {"name": "created_at", "type": "timestamp without time zone", "nullable": false}
]
```

---

### Таблица `schema_events`

История изменений схемы (drift-события). Растёт только при реальных изменениях.

```sql
CREATE TABLE schema_events (
    ts            TEXT NOT NULL,    -- ISO 8601 UTC первого обнаружения
    table_name    TEXT NOT NULL,
    change_type   TEXT NOT NULL,    -- column_added | column_removed |
                                    -- type_changed | nullable_changed
    column_name   TEXT NOT NULL,
    details       TEXT NOT NULL     -- JSON: {"before": {...}, "after": {...}}
);
```

**Возможные значения `change_type` и структура `details`:**

| change_type          | details.before                          | details.after                           |
|----------------------|-----------------------------------------|-----------------------------------------|
| `column_added`       | `null`                                  | `{"name", "type", "nullable"}`          |
| `column_removed`     | `{"name", "type", "nullable"}`          | `null`                                  |
| `type_changed`       | `{"type": "integer"}`                   | `{"type": "bigint"}`                    |
| `nullable_changed`   | `{"nullable": false}`                   | `{"nullable": true}`                    |

---

### Таблица `anomaly_scores`

Оценки аномальности от Isolation Forest. Обновляется после каждого тика коллектора и после ночного переобучения.

```sql
CREATE TABLE anomaly_scores (
    ts          TEXT NOT NULL,
    table_name  TEXT NOT NULL,
    score       REAL NOT NULL,   -- raw decision_function; < 0 → аномалия
    is_anomaly  INTEGER NOT NULL, -- 1 | 0
    PRIMARY KEY (ts, table_name)
);
```

Составной PK обеспечивает upsert-семантику: ночной retrain перезаписывает оценки для уже существующих временных меток без дублирования.

---

## 5.2 Форматы API-ответов

> Полные примеры запросов, параметры и коды ошибок — в [Разделе 4.1](#41-rest-api). Здесь описана только форма JSON-ответов.

Все эндпоинты возвращают `Content-Type: application/json`. Временные метки — ISO 8601 UTC без суффикса Z.

**Общий принцип:** все 9 эндпоинтов возвращают либо плоский JSON-массив, либо плоский объект — без обёрток типа `{"data": [...]}`. Вложенность только там, где она семантически неизбежна (поле `details` у schema changes, поле `buckets` у distribution).

| Эндпоинт | Тип ответа | Ключевые поля |
|---|---|---|
| `GET /api/tables` | массив объектов | `table_name`, `row_count`, `null_rate`, `last_check` |
| `GET /api/metrics/<table>` | массив | `ts`, `value` |
| `GET /api/forecast/<table>` | массив | `ts`, `yhat`, `yhat_lower`, `yhat_upper` |
| `GET /api/anomalies/<table>` | массив | `ts`, `score`, `is_anomaly` |
| `GET /api/changepoints/<table>` | массив | `ts`, `table_name`, `metric_name`, `score`, `value_before`, `value_after` |
| `GET /api/drift/<table>` | массив | `column`, `data_type`, `psi`, `ks_pvalue`, `is_drift`, `severity` |
| `GET /api/schema/<table>` | массив | `name`, `type`, `nullable` |
| `GET /api/schema/<table>/changes` | массив | `ts`, `table_name`, `change_type`, `column_name`, `details` |

---

## 5.3 Формат поля `tags` в таблице `metrics`

Поле `tags` — JSON-объект или `NULL`. Два варианта использования:

**null_count** — одна колонка:
```json
{"column": "email"}
```

**column_distribution** — распределение значений колонки:
```json
{
  "column": "age",
  "data_type": "integer",
  "buckets": [
    {"label": "18–25", "count": 12400},
    {"label": "26–35", "count": 18700},
    {"label": "36–45", "count": 14200},
    {"label": "46+",   "count": 9100}
  ]
}
```

Для числовых колонок `buckets` содержит равномерные диапазоны (histogram). Для категориальных — топ-N значений по частоте. Типы, исключённые из распределения: `text`, `json`, `jsonb`, `bytea`, `blob`, `uuid`.

---

## 5.4 Форматы ML-артефактов (joblib)

Все модели сохраняются в директории `models/` через `joblib.dump()`. Формат — Python-словарь, сериализованный в `.joblib` (pickle-совместимый).

---

### Артефакт прогноза: `models/<table>__<metric>.joblib`

```python
{
    "kind":       str,       # "prophet" | "linear"
    "model":      object,    # Prophet | LinearModel (dataclass)
    "last_ts":    str,       # ISO 8601 UTC последней точки обучения
    "trained_at": str,       # ISO 8601 UTC момента обучения
}
```

`LinearModel` — датакласс с полями: `slope: float`, `intercept: float`, `sigma: float`, `t0: float` (Unix epoch опорной точки).

Кэш считается свежим, если `last_ts ≥ last_ts_in_history − 1h`. При протухании модель автоматически переобучается в рамках запроса `/api/forecast/`.

---

### Артефакт детектора аномалий: `models/<table>__anomaly.joblib`

```python
{
    "model":      IsolationForest,   # sklearn, n_estimators=100, contamination=0.01
    "scaler":     StandardScaler,    # fitted StandardScaler (4 признака)
    "trained_at": str,               # ISO 8601 UTC
    "n_points":   int,               # размер обучающей выборки
}
```

Вектор признаков (порядок фиксирован): `[row_count, null_rate, Δrow_count, Δnull_rate]`.

Артефакты обновляются ежедневно в 04:00 UTC заданием `retrain_anomaly_detectors`. Минимальный порог для обучения: **200 точек** (`MIN_POINTS`), окно обучения: **60 дней** (`TRAIN_WINDOW_DAYS`).

---

# 6. Технологический стек

## 6.1 Сводная таблица зависимостей

| Компонент            | Технология / Библиотека         | Версия       | Роль в системе                                      |
|----------------------|---------------------------------|--------------|-----------------------------------------------------|
| **Язык**             | Python                          | 3.12         | Основной язык бэкенда и ML-слоя                     |
| **Веб-фреймворк**    | Flask                           | 3.1.3        | HTTP-сервер, маршрутизация, SSE                     |
| **ORM / SQL**        | SQLAlchemy                      | 2.0.49       | Унифицированный доступ к PostgreSQL / MySQL / ClickHouse и SQLite |
| **Планировщик**      | APScheduler                     | 3.11.2       | Фоновые задания (collect, retrain, detect)          |
| **ML — аномалии**    | scikit-learn                    | 1.6.1        | IsolationForest + StandardScaler                    |
| **ML — прогноз**     | prophet                         | 1.3.0        | Time-series forecasting с сезонностью               |
| **ML — changepoint** | ruptures                        | 1.1.9        | PELT-алгоритм поиска точек изменения                |
| **Персистенция ML**  | joblib                          | 1.5.3        | Сериализация/десериализация обученных моделей       |
| **Числа / матрицы**  | NumPy (через scikit-learn)      | транзитивно  | Матричные операции в детекторе аномалий             |
| **Валидация конфига** | pydantic / pydantic-settings   | 2.13.2       | Типобезопасные настройки из `.env`                  |
| **Коннектор PG**     | psycopg2-binary                 | 2.9.11       | Подключение к PostgreSQL / Supabase                 |
| **Коннектор MySQL**  | PyMySQL                         | 1.1.1        | Подключение к MySQL                                 |
| **Коннектор CH**     | clickhouse-sqlalchemy           | 0.3.2        | Подключение к ClickHouseDB                          |
| **Тест-данные**      | Faker                           | 40.14.0      | Генерация реалистичных seed-данных                  |
| **Шаблоны**          | Jinja2                          | 3.1.6        | HTML-шаблоны дашборда (через Flask)                 |
| **Окружение**        | python-dotenv                   | 1.2.2        | Загрузка `.env` файлов                              |
| **Инфраструктура**   | Docker                          | —            | Изоляция окружения, воспроизводимость               |
| **Целевая БД**       | Supabase (PostgreSQL 15)        | —            | Мониторируемая база данных (demo)                   |
| **Монитор БД**       | SQLite                          | встроен      | Хранение метрик, changepoints, аномалий             |

---

## 6.2 Обоснование ключевых выборов

### Flask вместо FastAPI

Flask выбран из-за зрелости, простоты и нативной интеграции с Jinja2 — все серверные страницы рендерятся шаблонизатором без дополнительного слоя. Для REST API на этом масштабе (9 эндпоинтов, JSON) Flask не уступает FastAPI по выразительности. FastAPI потребовал бы ASGI-сервера (uvicorn/hypercorn) без очевидного выигрыша: async-I/O здесь не нужен, узкое место — ML-вычисления, а не I/O-ожидание. Валидация входных параметров покрывается вручную через явные проверки в каждом эндпоинте.

### SQLite для monitor.db

Монитор читает и пишет одну БД с одного хоста. SQLite не требует отдельного процесса, хранит всё в одном файле (удобно для Docker volume mount), поддерживает WAL-режим для параллельных читателей. При росте нагрузки возможна миграция на PostgreSQL + TimescaleDB без изменения ORM-слоя — `MONITOR_DB_URL` конфигурируется через `.env`.

### APScheduler (BackgroundScheduler)

In-process планировщик идеален для MVP: не требует отдельного воркера (Celery + Redis), запускается в одной строке внутри Flask app factory, поддерживает `cron` и `interval` триггеры. Ограничение: задания выполняются в том же процессе — при перезапуске Flask прерванный job теряется, что допустимо для аналитических задач мониторинга.

### Prophet + Linear OLS fallback

Prophet обеспечивает надёжный прогноз для таблиц с историей ≥ 7 дней: автоматически захватывает еженедельную сезонность и trend changepoints. Для новых таблиц с коротким рядом используется линейная регрессия (OLS) — детерминированная, без зависимостей, мгновенно обучается на 2+ точках.

### Isolation Forest для детекции аномалий

Unsupervised алгоритм: не требует разметки аномалий. Хорошо работает на 4-мерном пространстве признаков (`row_count, null_rate, Δrow_count, Δnull_rate`). Параметр `contamination=0.01` задаёт ≈1% ложноположительных на обучающих данных — консервативный порог для продуктивной среды. `decision_function` возвращает непрерывную оценку, позволяющую ранжировать аномалии по тяжести.

### ruptures (PELT) + CUSUM fallback

PELT (Pruned Exact Linear Time) — оптимальный алгоритм для точного поиска breakpoints в ряду произвольной длины за O(n log n). Используется с RBF-ядром для захвата нелинейных изменений. CUSUM — чистый Python fallback для окружений, где `ruptures` недоступен.

### scikit-learn + joblib

scikit-learn предоставляет `IsolationForest` и `StandardScaler` с единым API `fit/transform/predict`. joblib — официальный механизм персистенции sklearn-объектов: более эффективен, чем pickle, для массивов NumPy. Оба входят в стандартный ML-стек Python.

### psycopg2-binary / PyMySQL / clickhouse-sqlalchemy

Три коннектора реализуют один интерфейс `DBAdapter (ABC)` в `app/db.py`. Конкретный адаптер выбирается по схеме `DATABASE_URL` при инициализации. Добавление нового адаптера не требует изменений в коллекторах или ML-слое.

---

## 6.3 Требования к версии Python

Минимальная поддерживаемая версия: **Python 3.10** — из-за синтаксиса union-типов (`str | None`) в аннотациях. Docker-образ фиксирует `python:3.12-slim` для воспроизводимости сборки.

Все ML-модули содержат `from __future__ import annotations` для корректной работы форвардных ссылок в аннотациях типов.

---

## 6.4 Тестирование

Тестовые зависимости вынесены в отдельный `requirements-dev.txt` и не входят в production-образ.

| Инструмент | Версия | Назначение |
|------------|--------|------------|
| pytest     | 8.3.4  | Unit + integration тесты |
| coverage   | 7.13.5 | Измерение покрытия кода  |

Тесты расположены в `tests/`. Запуск: `pytest -v`. Все 13 тестов для аномального детектора проходят с нуля (Sprint 2). Интеграционные тесты используют реальную БД (`monitor.db` в памяти через `sqlite:///:memory:`), не моки — по решению команды (опыт Sprint 1: моки пропустили ошибку миграции).

---

# 7. Конфигурация

## 7.1 Переменные окружения

Конфигурация загружается из `.env` файла через `pydantic-settings` (`app/config.py`). Все значения доступны как `settings.<KEY>` во всём приложении.

| Переменная | Обязательная | По умолчанию | Описание |
|---|---|---|---|
| `DATABASE_URL` | **да** | — | DSN мониторируемой БД. Формат: `postgresql+psycopg2://user:pass@host:5432/db` / `mysql+pymysql://...` / `clickhouse+native://...` |
| `MONITOR_DB_URL` | нет | `sqlite:///monitor.db` | DSN хранилища метрик. В production можно заменить на PostgreSQL без изменения кода. |
| `MONITORED_SCHEMA` | нет | `public` | Схема PostgreSQL, в которой ищутся таблицы для мониторинга. |
| `SECRET_KEY` | нет | `dev-secret` | Секрет для Flask-сессий. **Обязательно переопределить в production.** |
| `COLLECT_INTERVAL_MINUTES` | нет | `15` | Интервал сбора метрик в минутах (APScheduler). |
| `LOG_LEVEL` | нет | `INFO` | Уровень логирования: `DEBUG`, `INFO`, `WARNING`, `ERROR`. |
| `FLASK_ENV` | нет | `development` | Режим Flask. В production установить `production`. |

## 7.2 Пример .env

```dotenv
# Обязательно
DATABASE_URL=postgresql+psycopg2://postgres:password@db.supabase.co:5432/postgres

# Опционально (переопределяются при необходимости)
MONITOR_DB_URL=sqlite:///monitor.db
MONITORED_SCHEMA=public
SECRET_KEY=change-me-in-production
COLLECT_INTERVAL_MINUTES=15
LOG_LEVEL=INFO
FLASK_ENV=production
```

## 7.3 Docker-запуск

Переменные передаются через `--env-file`:

```bash
docker run --rm \
  -p 5001:5001 \
  --env-file .env \
  -v $(pwd)/monitor.db:/app/monitor.db \
  -v $(pwd)/models:/app/models \
  db-monitoring
```

Два volume mount обязательны: `monitor.db` сохраняет исторические метрики, `models/` — обученные ML-модели. Без них данные теряются при перезапуске контейнера.
