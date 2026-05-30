---
title: DB Monitoring
emoji: 📊
colorFrom: blue
colorTo: indigo
sdk: docker
app_port: 5001
pinned: false
---

# db-monitoring — система мониторинга данных в БД (Flask)

Веб-приложение на Flask, которое подключается к базе данных, автоматически собирает метрики качества данных (количество записей, пропуски, распределения колонок), визуализирует их на дашбордах и детектирует аномалии. Включает прогноз роста таблиц через Prophet, drift-detection (PSI/KS), change-point detection (PELT/RBF) и schema-drift detection (ALTER TABLE / новые колонки / смена типов).

[![tests](https://github.com/aleksandr-novikov/db-monitoring/actions/workflows/tests.yml/badge.svg)](https://github.com/aleksandr-novikov/db-monitoring/actions/workflows/tests.yml)
[![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/flask-3.1-green)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

---

## Содержание
- [Требования](#требования)
- [Установка](#установка)
- [Локальный Postgres (рекомендуется для разработки)](#локальный-postgres-рекомендуется-для-разработки)
- [Запуск](#запуск)
- [Демо-данные (сидирование)](#демо-данные-сидирование)
- [ML-фичи](#ml-фичи)
- [Schema drift detection](#schema-drift-detection)
- [REST API](#rest-api)
- [Тесты](#тесты)
- [Хранилище метрик](#хранилище-метрик)
- [Поддерживаемые СУБД](#поддерживаемые-субд)
- [Запуск в Docker](#запуск-в-docker)
- [Переменные окружения](#переменные-окружения)
- [Метрики (Prometheus)](#метрики-prometheus)
- [Логи](#логи)
- [Структура проекта](#структура-проекта)
- [Функциональность](#функциональность)
---

## Требования

- Python 3.12+
- DSN мониторируемой БД в `DATABASE_URL` — поддерживаются PostgreSQL / MySQL / ClickHouse (см. [Поддерживаемые СУБД](#поддерживаемые-субд))
- Docker — для команд `make build` / `make server` / `make reset-db`

---

## Установка

```bash
# 1. Клонировать репозиторий
git clone https://github.com/aleksandr-novikov/db-monitoring.git
cd db-monitoring

# 2. Создать виртуальное окружение
python3 -m venv venv

# 3. Активировать окружение
source venv/bin/activate        # macOS / Linux
# venv\Scripts\activate         # Windows

# 4. Обновить pip и установить зависимости
pip install -U pip
pip install -r requirements.txt

# 5. Настроить переменные окружения
cp .env.example .env
# По умолчанию .env.example указывает на локальный Postgres (см. ниже).
# Для подключения к Supabase раскомментируй соответствующую строку и заполни пароль (у тимлида).
```

Основной стек: Flask, SQLAlchemy, APScheduler, Plotly, Prophet, ruptures, joblib.

---

## Локальный Postgres (рекомендуется для разработки)

Чтобы не упираться в сетевую латентность Supabase free tier, для разработки поднимается локальный Postgres 18 через Docker Compose:

```bash
make db-up      # поднимает postgres:18 на localhost:5432 (db=monitor, user=postgres, pass=dev)
make seed       # применяет scripts/schema.sql + сидит дефолтный датасет (~7 сек)
make db-down    # остановить
make db-reset   # стереть том и поднять заново (полная очистка данных)
make db-psql    # интерактивный psql внутри контейнера
make db-logs    # follow логов
```

DSN уже прописан в `.env.example`:

```
DATABASE_URL=postgresql://postgres:dev@localhost:5432/monitor
```

**Производительность сидинга** (350k строк, full demo: `--users 50000 --products 1000 --orders 100000 --events 200000`):

| Окружение                  | Время    |
|----------------------------|----------|
| Supabase pooler (free tier)| ~10 мин  |
| Локальный Postgres 18      | ~70 сек  |

Совместимость по SQL — 1-к-1: используются только стандартные системные вьюхи (`pg_stat_user_tables`, `information_schema`) и `gen_random_uuid()` (встроен в PG 13+). Никаких Supabase-специфичных схем (`auth`/`storage`/`realtime`) или RLS код не использует.

---

## Запуск

### Через Makefile (рекомендуется)

```bash
make server     # docker compose up -d --build app  (поднимает postgres + Flask на :5001)
make reset-db   # TRUNCATE + reseed target Postgres → live collector → change-point sweep
```

Дашборд: [http://localhost:5001](http://localhost:5001)

### Без Docker

```bash
python -m app.app
```

**Проверка:**

```bash
curl http://localhost:5001/healthz
# → {"status": "ok"}
```

**Запуск задач шедулера вручную:**

Шедулер стартует автоматически вместе с приложением (APScheduler). Текущие задачи:
- `collect_all_tables` — каждые `COLLECT_INTERVAL_MINUTES` (по умолчанию 15 мин). В тот же тик после сбора метрик прогоняется schema-drift sweep.
- `retrain_forecasts` — cron `03:00`
- `detect_changepoints` — каждый час

```bash
# разовые прогоны без шедулера
python -c "from collectors.scheduler import collect_all_tables; collect_all_tables()"
python -c "from ml.forecast import retrain_all; retrain_all()"
python -c "from ml.changepoint import detect_all; detect_all()"
python -c "from collectors.schema_collector import collect_all_schemas; collect_all_schemas()"

# принудительный запуск через admin API
curl http://localhost:5001/admin/jobs
curl -X POST http://localhost:5001/admin/jobs/collect_all_tables/run
```

---

## Демо-данные (сидирование)

Самый быстрый путь к рабочему дашборду — `make reset-db`. Он выполняет:

1. **TRUNCATE + reseed** мониторируемой БД (`scripts/seed_target_db.py`)
2. **DROP + reapply** схемы `monitor.db`
3. **Один прогон коллектора** — первый реальный снапшот метрик
4. **Sweep change-point detection** — события записываются в таблицу `changepoints`

```bash
make reset-db
```

После сброса история в `monitor.db` пустая — graphs/drift/anomaly наполнятся, как только шедулер сделает несколько тиков `collect_all_tables` (по умолчанию каждые 15 мин). Для немедленного прогона см. «Запуск задач шедулера вручную» выше.

### Что насеяно в target БД

Дефекты, встроенные в `seed_target_db.py` (на них работают ML-методы по мере накопления реальной истории через коллектор):
- `users.email` — ~5% NULL (стабильный baseline для null_rate).
- `products.price_updated_at` — ~30% NULL.
- `orders` — 5 точных дубликатов.
- `events.ip_address` — ступенчатый NULL-rate: ~2% для старых событий, ~25% за последние 7 дней.

### Сидер вручную

```bash
python -m scripts.seed_target_db --reset                 # очистить и пересидировать
python -m scripts.seed_target_db --users 50000 --products 1000 \
    --orders 100000 --events 200000 --reset              # полный demo-датасет
```

### Live demo (#75)

`scripts/live_demo.py` — стримит синтетические события в `events`, запускает коллектор и (опционально) детектирование change-points на каждый тик. На демо: один скрипт, дашборд обновляется в реальном времени.

```bash
# 20 тиков по 5 сек: ровный трафик
make live-demo ARGS="--ticks 20 --interval 5"

# Инцидент на 8-м тике: 10× объём + 40% NULL по ip_address + смещение server_id
make live-demo ARGS="--ticks 20 --interval 5 --incident-at 8 --changepoints"

# Бесконечно (Ctrl-C для остановки)
python -m scripts.live_demo --interval 10
```

Требования: target Postgres поднят (`make db-up`), приложение запущено (`make server`), в `users` есть хотя бы одна запись (`make seed` или ручной `seed_target_db`).

---

## ML-фичи

### Forecasting роста таблиц (Prophet)

Прогноз `row_count` на 7 дней вперёд. Использует Prophet при наличии ≥7 дней истории, fallback на OLS-линейку. Модели сохраняются в `models/*.joblib`, ночной retrain — cron `03:00`.

- Endpoint: `GET /api/forecast/<table>?metric=row_count&horizon=7d`
- На странице таблицы — toggle «Прогноз 7 дн.» рисует жёлтую пунктирную линию + 95% CI band

### Drift detection (PSI / KS)

Сравнивает свежий снимок `column_distribution` с снимком 7-дневной давности.

- **PSI** — категориальные колонки. Пороги: `> 0.2` warn, `> 0.25` critical.
- **KS-тест** (двухвыборочный) — числовые колонки. `p < 0.05` — drift статистически значим.
- Endpoint: `GET /api/drift/<table>` → `[{column, data_type, psi, ks_pvalue, is_drift, severity}]`
- На странице таблицы — секция «Drift» с цветными бейджами и сворачиваемым списком стабильных колонок.

### Change-point detection (PELT / ruptures)

Детектирует резкие сдвиги в `row_count` / `null_rate` (миграции, сбои ETL).

- **PELT с RBF cost**, для cumulative-метрик (row_count, size_bytes) применяется detrending перед фитом.
- Фильтры: PSI-нормализованный score ≥ 1.5, относительный сдвиг ≥ 15%, dedup в окне 72ч.
- Endpoint: `GET /api/changepoints/<table>?metric=null_rate` → `[{ts, score, value_before, value_after}]`
- На графике — красные пунктирные вертикальные линии с подписью `▼ before → after`.

### Schema drift detection

Ловит изменения схемы — добавление/удаление колонок, смена типа или nullability. Частая причина «молчаливых» поломок витрин.

- Снимок схемы (`information_schema.columns`) на каждом тике коллектора, diff против предыдущего снапшота.
- Первое наблюдение таблицы события не генерирует — нет baseline для сравнения.
- Хранение: `monitor.schema_snapshots` (JSON-список колонок, один на таблицу) + `monitor.schema_events` (журнал событий).
- Endpoint: `GET /api/schema/<table>/changes?range=30d` → `[{ts, change_type, column_name, details}]`
- На странице таблицы:
  - бейдж `⚠ Schema drift (N)` рядом с заголовком (только если есть события за последние 7 дней)
  - секция «Schema drift» с человекочитаемым описанием каждого изменения

Типы событий: `column_added`, `column_removed`, `type_changed`, `nullable_changed`.

---

## REST API

| Метод | Endpoint | Назначение |
|-------|----------|------------|
| `GET` | `/api/tables` | Список мониторируемых таблиц + последние метрики |
| `GET` | `/api/metrics/<table>?metric=&range=` | Time-series значения метрики |
| `GET` | `/api/schema/<table>` | Колонки таблицы из information_schema |
| `GET` | `/api/forecast/<table>?metric=&horizon=` | Прогноз Prophet/линейный |
| `GET` | `/api/drift/<table>` | PSI/KS отчёт по колонкам |
| `GET` | `/api/changepoints/<table>?metric=&range=` | Детектированные change-points |
| `GET` | `/api/schema/<table>/changes?range=` | Schema-drift события |
| `GET` | `/healthz` | Health-check |
| `GET` | `/admin/jobs` | Список APScheduler jobs |
| `POST` | `/admin/jobs/<job_id>/run` | Принудительный запуск job |

---

## Тесты

Юнит-тесты (быстрые, моки/SQLite) — каждый PR:

```bash
pytest                 # все, кроме integration (~5 c)
```

Интеграционные тесты (#44) поднимают реальные Postgres / MySQL / ClickHouse / TimescaleDB через [testcontainers-python](https://testcontainers-python.readthedocs.io/) — нужен запущенный Docker-демон. Покрывают:

- `tests/integration/test_db_postgres.py` / `test_db_mysql.py` / `test_db_clickhouse.py` — диалект-специфичный SQL адаптеров на живых СУБД (3 диалекта по #42).
- `tests/integration/test_metrics_storage_timescale.py` — `app.metrics_storage` на TimescaleDB (acceptance для #40: hypertable, `drop_chunks`, ON CONFLICT, RETURNING id).
- `tests/integration/test_full_cycle.py` — `Postgres → MetricsCollector → SQLite storage → /api/metrics` end-to-end.

Запуск локально:

```bash
make test-integration              # = pytest -m integration -v
# или: pytest -m integration tests/integration/test_db_postgres.py -v
```

В CI отдельный job `integration` запускается **только** на push в `master` (на PR не запускается — медленно, ~3–5 мин).

### E2E дашборда (Playwright)

`tests/e2e/test_dashboard_ui.py` (#45) — headless Chromium ходит по реальному Flask-приложению (запущенному в отдельном потоке через `werkzeug.make_server`). Покрывает:

- Рендер обзора (KPI-карточки, список таблиц).
- Клик по строке таблицы → `/dashboard/schema/<name>` и подгрузку Plotly-графика.
- Переключение `Строки` ↔ `NULL rate` в табах графика.
- `/dashboard/schema` со списком колонок и типов.
- Тогл темы (light/dark): сохранение в `localStorage` без flash на reload.
- Empty-state: «Нет таблиц для мониторинга» / «Нет исторических метрик».

Setup (один раз — выкачивает Chromium ~80 МБ):

```bash
pip install -r requirements-dev.txt
playwright install chromium
# В CI на Ubuntu используем `playwright install --with-deps chromium` —
# подтягивает системные libnss3/libasound2/etc. На macOS dev-машине
# эти библиотеки уже есть.
```

Запуск:

```bash
make test-e2e                      # = pytest -m e2e -v
```

В CI отдельный job `e2e` запускается **только** на push в `master`. Скриншоты упавших тестов прикладываются как артефакт `e2e-screenshots`.

---

## Хранилище метрик

Метрики коллектора (`row_count`, `null_rate`, schema-snapshots, anomaly scores, change-points, …) хранятся в отдельной БД, выбираемой через `MONITOR_DB_URL`:

- **SQLite** (по умолчанию, `sqlite:///monitor.db`) — нулевая настройка для разработки и MVP. Хватает на 14-дневную историю при 4–10 таблицах.
- **PostgreSQL + TimescaleDB** (опционально, #40) — для длинных историй и масштаба. `metrics` становится hypertable, retention идёт через `drop_chunks` вместо row-by-row DELETE.

```bash
# Поднять Timescale-контейнер (порт 5433, профиль `timescale`)
make timescale-up

# Указать новый DSN в .env:
#   MONITOR_DB_URL=postgresql://postgres:dev@localhost:5433/metrics

# Перенести историю SQLite → Timescale
make timescale-migrate                              # = scripts.migrate_metrics_to_timescale
make timescale-migrate ARGS="--reset"               # truncate append-таблиц перед загрузкой

make timescale-down                                 # остановить
```

Тот же `app.metrics_storage` работает на обоих бэкендах через единый интерфейс — переключение управляется только DSN. На plain Postgres (без расширения) тоже работает: `CREATE EXTENSION timescaledb` и `create_hypertable` пропускаются, остаётся обычная таблица с DELETE-retention.

---

## Поддерживаемые СУБД

Мониторируемая БД выбирается через scheme в `DATABASE_URL` — фабрика в `app/db.py` диспатчит вызовы в адаптер для соответствующего диалекта.

| СУБД          | Scheme                                 | Зависимость           | Источник `row_count`/`size`            |
|---------------|----------------------------------------|-----------------------|-----------------------------------------|
| PostgreSQL    | `postgresql://`, `postgresql+psycopg2://` | `psycopg2-binary`   | `pg_stat_user_tables` + `pg_total_relation_size` |
| MySQL/MariaDB | `mysql://`, `mysql+pymysql://`         | `PyMySQL`             | `information_schema.tables` (`table_rows`, `data_length+index_length`) |
| ClickHouse    | `clickhouse://`, `clickhouse+native://` | `clickhouse-sqlalchemy` | `system.tables` + `system.parts.modification_time` |
| Apache Iceberg | `iceberg+rest://`, `iceberg+glue://` | `pyiceberg[pyarrow,glue]`  | snapshot summary metadata (без полного скана) |

Примеры DSN:

```bash
# PostgreSQL / Supabase
DATABASE_URL=postgresql://postgres.<project>:<PASSWORD>@aws-0-<region>.pooler.supabase.com:5432/postgres

# MySQL
DATABASE_URL=mysql+pymysql://user:password@host:3306/dbname

# ClickHouse (native protocol, порт 9000)
DATABASE_URL=clickhouse+native://user:password@host:9000/dbname

# Apache Iceberg — REST-каталог (Polaris, Nessie, Gravitino, Tabular и др.)
DATABASE_URL=iceberg+rest://localhost:8181?warehouse=s3://my-bucket/warehouse
MONITORED_SCHEMA=my_namespace

# Apache Iceberg — AWS Glue (AWS credentials из env: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
DATABASE_URL=iceberg+glue://?warehouse=s3://my-bucket/warehouse
MONITORED_SCHEMA=my_glue_database
```

`MONITORED_SCHEMA` для MySQL/ClickHouse трактуется как имя БД (database). Для ClickHouse значение по умолчанию обычно `default`. Для Iceberg — это namespace каталога.

**Особенности диалектов:**
- **MySQL** — `table_rows` в InnoDB это оценка оптимизатора; для трендовой аналитики достаточно, для точных счётчиков — нет. `update_time` может быть `NULL` на партиционированных таблицах.
- **ClickHouse** — `null_count` для не-`Nullable` колонок всегда 0 (по дизайну). `last_modified` собирается из `system.parts` (`max(modification_time)`).
- **Apache Iceberg** — `row_count` и `null_count` читаются из snapshot/manifest metadata без полного скана данных, что критично для таблиц с миллиардами строк. `column_distribution` не поддерживается (возвращает пустой список). Требует PyIceberg ≥ 0.7. Для S3/MinIO нужны `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` (или IAM-роль). REST-каталог подключается по `http://` — для HTTPS используйте обратный прокси на стороне каталога или отслеживайте issue #124.
- **MS SQL** — пока не поддерживается (требует ODBC-драйвер вне Python).

NULL-статистика во всех SQL-диалектах считается через `COUNT(*) - COUNT(col)` (PostgreSQL дополнительно использует `FILTER (WHERE col IS NULL)` как более идиоматичный вариант). `column_distribution` собирается через `SELECT col, COUNT(*) GROUP BY col ORDER BY 2 DESC LIMIT 20` — пропускает text/json/blob/uuid колонки (top-N по высокой кардинальности — шум, не сигнал).

### Live smoke test для Iceberg

Поднимает локальный REST-каталог + MinIO через Docker Compose и прогоняет адаптер против реального Iceberg-кластера:

```bash
make iceberg-up       # запустить MinIO (9000/9001) + Iceberg REST (8181)
make smoke-iceberg    # создать таблицу, записать данные, проверить адаптер
make iceberg-down     # остановить
```

Скрипт (`scripts/smoke_iceberg.py`) создаёт namespace + таблицу с 5 строками и 2 NULL-полями, затем проверяет все методы адаптера: `list_tables`, `table_schema`, `table_stats` (row_count, size_bytes), `column_nulls` (из manifest metadata, без сканирования), `column_distribution` (→ []).

---

## Запуск в Docker

```bash
make build           # docker build -t db-monitoring .
make server          # build + run на :5001 с volume-маунтами monitor.db и models/
```

Или вручную:

```bash
docker build -t db-monitoring .
docker run --rm -it --init -p 5001:5001 \
    --env-file .env \
    -v "$PWD/monitor.db:/app/monitor.db" \
    -v "$PWD/models:/app/models" \
    db-monitoring
```

Дашборд: [http://localhost:5001](http://localhost:5001).

В контейнере приложение слушает `0.0.0.0:5001`. Все настройки задаются через `.env` или флаги `-e`. Volume-маунты сохраняют `monitor.db` и обученные joblib-модели между перезапусками.

> Образ — `python:3.12-slim`. Внутри тянется Prophet (cmdstanpy + numpy + pandas + matplotlib) и ruptures (scipy) — первая сборка медленная, последующие используют кэш слоёв.

### Подключение к Supabase из Docker

Прямой DSN `db.<project>.supabase.co:5432` Supabase отдаёт **только по IPv6** (политика free-тарифа), а Docker Desktop на Mac/Windows наружу IPv6 не маршрутизирует. Поэтому в `.env` для Docker используй **Connection Pooler** (IPv4):

```
DATABASE_URL=postgresql://postgres.<project>:<PASSWORD>@aws-0-<region>.pooler.supabase.com:5432/postgres
```

Адрес pooler-а: Supabase → Project Settings → Database → Connection Pooling → Session mode.

---

## Переменные окружения

Все параметры задаются через `.env`. Шаблон — в `.env.example`.

| Переменная           | Обязательная | По умолчанию              | Описание                                      |
|----------------------|:------------:|---------------------------|-----------------------------------------------|
| `DATABASE_URL`       | ✅           | —                         | DSN мониторируемой БД (Postgres/MySQL/CH)     |
| `MONITOR_DB_URL`     | —            | `sqlite:///monitor.db`    | DSN хранилища метрик (SQLite или Postgres/Timescale — см. [Хранилище метрик](#хранилище-метрик)) |
| `MONITORED_SCHEMA`   | —            | `public`                  | Схема Postgres / БД для MySQL/CH              |
| `SECRET_KEY`         | ✅           | —                         | Секрет для Flask-сессий/CSRF                  |
| `COLLECT_INTERVAL_MINUTES` | —      | `15`                      | Интервал коллектора метрик                    |
| `LOG_LEVEL`          | —            | `INFO`                    | Уровень логирования                           |
| `LOG_FORMAT`         | —            | `text`                    | `text` (dev) или `json` (Loki/ELK/Datadog) — см. [Логи](#логи) |
| `FLASK_ENV`          | —            | `development`             | Режим Flask                                   |
| `HOST`               | —            | `127.0.0.1`               | Bind-адрес (в Docker — `0.0.0.0`)             |
| `PORT`               | —            | `5001`                    | Порт HTTP-сервера                             |
| `FLASK_DEBUG`        | —            | `1` (Docker — `0`)        | Включает дебаг и автоперезапуск Flask         |

> ⚠️ Файл `.env` содержит секреты — не коммитить в git.

---

## Метрики (Prometheus)

Эндпоинт `GET /metrics` отдаёт payload в стандартном Prometheus
text exposition format. По соглашению — без auth и CSRF; ставится за
network ACL (сидит во внутренней сети кластера, scrape только из
Prometheus / Grafana Agent / VM).

```bash
curl http://localhost:5001/metrics | head -20
```

Что внутри:

| Метрика | Тип | Лейблы | Источник |
|---|---|---|---|
| `http_requests_total` | Counter | `method, endpoint, status` | Flask before/after_request |
| `http_request_duration_seconds` | Histogram | `method, endpoint` | Flask before/after_request |
| `collector_runs_total` | Counter | `result` (`ok`/`error`) | `collectors/per_project.py` |
| `failed_login_attempts_total` | Counter | — | `app/auth.py::login` |

Сам endpoint rate-limited 60/min — защита от misconfigured scraper-а с
1-секундным интервалом, который превратит /metrics в self-DoS.

Пример scrape-конфига для Prometheus:

```yaml
scrape_configs:
  - job_name: db-monitoring
    metrics_path: /metrics
    scrape_interval: 15s
    static_configs:
      - targets: ["app.internal:5001"]
```

---

## Логи

По умолчанию приложение пишет логи свободным текстом в stdout — удобно
для локальной разработки. Для прода поставь `LOG_FORMAT=json` —
каждая строка станет одной JSON-записью, парсимой Loki / ELK / Datadog
без ingest-side регулярок.

```bash
LOG_FORMAT=json python -m app.app | jq .
```

Обязательные поля в JSON-режиме: `timestamp` (ISO 8601 с миллисекундами,
UTC), `level`, `logger`, `message`, `request_id`. Дополнительно:
`exc_info` для перехваченных исключений, любые поля переданные в
`logger.info(..., extra={...})`.

### Корреляция запросов

Каждый HTTP-запрос получает уникальный `request_id` (UUID4 hex). Если
upstream-прокси прислал заголовок `X-Request-Id`, он используется без
изменений — это позволяет проследить запрос через несколько сервисов.

Сервер эхо-возвращает `X-Request-Id` в каждом ответе, так что клиент
может попросить сапорт «найти лог по этому id». Pipe-friendly grep:

```bash
docker compose logs -f app | jq 'select(.request_id == "a1b2…")'
```

DSN-пароли продолжают маскироваться через `DSNFilter` (#56) до того
как formatter увидит запись — `topsecret` в исходном `logger.warning`
превращается в `u:***@host` и в text-, и в JSON-выводе.

---

## Структура проекта

```
db-monitoring/
├── app/
│   ├── app.py              # Фабрика Flask, blueprints, /healthz
│   ├── api.py              # /api/* endpoints
│   ├── admin.py            # /admin/* (jobs)
│   ├── dashboard.py        # /dashboard/* (рендеринг)
│   ├── db.py               # DBAdapter (Postgres/MySQL/ClickHouse)
│   ├── metrics_storage.py  # SQLite + save/get_metrics, save/get_changepoints
│   └── config.py           # pydantic-settings
├── collectors/
│   ├── metrics_collector.py # row_count, null_count, column_distribution
│   └── scheduler.py         # APScheduler — collect / forecast / changepoint jobs
├── ml/
│   ├── forecast.py         # Prophet + linear fallback, joblib-persist
│   ├── drift.py            # PSI + KS, rolling baseline
│   └── changepoint.py      # PELT/RBF + detrend + dedupe
├── scripts/
│   ├── seed_target_db.py   # сидинг мониторируемой БД
│   ├── reset_db.py         # full reset: target reseed → live collector → change-point sweep
│   ├── schema.sql          # схема мониторируемой БД
│   └── metrics_schema.sql  # схема monitor.db (metrics + changepoints)
├── templates/              # Jinja2 (overview, table_detail, schema)
├── tests/                  # pytest, 148+ тестов
├── models/                 # joblib forecast cache (gitignored)
├── monitor.db              # SQLite метрик (gitignored)
├── Dockerfile
├── Makefile
├── requirements.txt
└── .env / .env.example
```

---

## Функциональность

- **Сбор метрик** — `row_count`, `size_bytes`, `null_count` (per column), `null_rate`, `column_distribution`, `last_modified`
- **Drift-детекция** — PSI для категориальных, KS-тест для числовых, rolling baseline 7 дней
- **Forecasting** — Prophet с per-table моделями, ночной retrain через APScheduler, прогноз на 7 дней с CI
- **Change-point detection** — PELT/RBF с detrending для cumulative-метрик, hourly sweep
- **Schema-drift detection** — diff против последнего снапшота, события при добавлении/удалении колонок и смене типа/nullability
- **Дашборд** — Plotly-графики с прогнозом, drift-картой, аннотациями change-points, бейдж schema-drift
- **REST API** — JSON endpoints для интеграции с внешними сервисами
- **Multi-DB** — PostgreSQL / MySQL / ClickHouse через единый `DBAdapter` интерфейс

---
