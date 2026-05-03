# db-monitoring — система мониторинга данных в БД (Flask)

Веб-приложение на Flask, которое подключается к базе данных, автоматически собирает метрики качества данных (количество записей, пропуски, распределения колонок), визуализирует их на дашбордах и детектирует аномалии. Включает прогноз роста таблиц через Prophet, drift-detection (PSI/KS) и change-point detection (PELT/RBF).

[![tests](https://github.com/aleksandr-novikov/db-monitoring/actions/workflows/tests.yml/badge.svg)](https://github.com/aleksandr-novikov/db-monitoring/actions/workflows/tests.yml)
[![Python](https://img.shields.io/badge/python-3.11%20|%203.12%20|%203.13-blue)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/flask-3.1-green)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

---

## Содержание

- [Требования](#требования)
- [Установка](#установка)
- [Запуск](#запуск)
- [Демо-данные (сидирование)](#демо-данные-сидирование)
- [ML-фичи](#ml-фичи)
- [REST API](#rest-api)
- [Поддерживаемые СУБД](#поддерживаемые-субд)
- [Запуск в Docker](#запуск-в-docker)
- [Переменные окружения](#переменные-окружения)
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
# Открой .env и заполни DATABASE_URL (пароль — у тимлида)
```

Основной стек: Flask, SQLAlchemy, APScheduler, Plotly, Prophet, ruptures, joblib.

---

## Запуск

### Через Makefile (рекомендуется)

```bash
make server          # build + запуск Docker-контейнера на :5001
make reset-db        # полный сброс: Supabase + monitor.db + сидинг + детекция (~10 мин)
make reset-metrics-db # быстрый сброс только monitor.db + сидинг (~5 сек)
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
- `collect_all_tables` — каждые `COLLECT_INTERVAL_MINUTES` (по умолчанию 15 мин)
- `retrain_forecasts` — cron `03:00`
- `detect_changepoints` — каждый час

```bash
# разовые прогоны без шедулера
python -c "from collectors.scheduler import collect_all_tables; collect_all_tables()"
python -c "from ml.forecast import retrain_all; retrain_all()"
python -c "from ml.changepoint import detect_all; detect_all()"

# принудительный запуск через admin API
curl http://localhost:5001/admin/jobs
curl -X POST http://localhost:5001/admin/jobs/collect_all_tables/run
```

---

## Демо-данные (сидирование)

Самый быстрый путь к рабочему дашборду — `make reset-db`. Он выполняет:

1. **TRUNCATE + reseed** мониторируемой БД (`scripts/seed_target_db.py`)
2. **DROP + reapply** схемы `monitor.db`
3. **Seed 14 дней** метрик через `scripts/seed_metrics_history.py`
4. **Один прогон коллектора** против Supabase — заполняет `null_count` по колонкам
5. **Sweep change-point detection** — события записываются в таблицу `changepoints`

```bash
make reset-db          # ~10 мин: всё, включая Supabase
make reset-metrics-db  # ~5 сек: только monitor.db (Supabase не трогается)
```

### Что насеяно

**На графиках row_count / null_rate (видны в окне дашборда «7 дней»):**
- `users.row_count` — резкий step-up +15k на 11-й день (маркетинговая кампания)
- `orders.null_rate` — всплеск на дни 10.5–11.5 (сбой загрузки данных)
- `events.null_rate` — резкий рост последние 7 дней (регрессия логирования)
- `products.row_count` — кратковременный провал на 10-й день (случайный DELETE)

**В drift-секции (PSI / KS, baseline 7 дн.):**
- `users.signup_source` — постепенный сдвиг web → mobile
- `orders.shipping_country` — внезапный сдвиг к US в последние ~3 дня
- `orders.amount` — численный сдвиг среднего $400 → $1500 (срабатывает KS)
- `orders.items_count` — рост корзины 2 → 5 (KS)
- `events.server_id` — перекос нагрузки к server-1
- `events.duration_ms` — деградация latency 200ms → 800ms (KS)
- Стабильные контролы: `users.country`, `orders.status`, `events.device_type`, `products.category`

**Change-points** (вертикальные красные линии на графике): три события — users.row_count step, orders.null_rate spike, events.null_rate step-up.

### Сидеры по отдельности

```bash
# Только мониторируемая БД (без monitor.db, без коллектора)
python -m scripts.seed_target_db
python -m scripts.seed_target_db --reset                 # очистить и пересидировать
python -m scripts.seed_target_db --users 50000 --products 1000 \
    --orders 100000 --events 200000 --reset              # полный demo-датасет (~10 мин)

# Только monitor.db (синтетическая 14-дневная история)
python -m scripts.seed_metrics_history
```

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
| `GET` | `/healthz` | Health-check |
| `GET` | `/admin/jobs` | Список APScheduler jobs |
| `POST` | `/admin/jobs/<job_id>/run` | Принудительный запуск job |

---

## Поддерживаемые СУБД

Мониторируемая БД выбирается через scheme в `DATABASE_URL` — фабрика в `app/db.py` диспатчит вызовы в адаптер для соответствующего диалекта.

| СУБД          | Scheme                                 | Зависимость           | Источник `row_count`/`size`            |
|---------------|----------------------------------------|-----------------------|-----------------------------------------|
| PostgreSQL    | `postgresql://`, `postgresql+psycopg2://` | `psycopg2-binary`   | `pg_stat_user_tables` + `pg_total_relation_size` |
| MySQL/MariaDB | `mysql://`, `mysql+pymysql://`         | `PyMySQL`             | `information_schema.tables` (`table_rows`, `data_length+index_length`) |
| ClickHouse    | `clickhouse://`, `clickhouse+native://` | `clickhouse-sqlalchemy` | `system.tables` + `system.parts.modification_time` |

Примеры DSN:

```bash
# PostgreSQL / Supabase
DATABASE_URL=postgresql://postgres.<project>:<PASSWORD>@aws-0-<region>.pooler.supabase.com:5432/postgres

# MySQL
DATABASE_URL=mysql+pymysql://user:password@host:3306/dbname

# ClickHouse (native protocol, порт 9000)
DATABASE_URL=clickhouse+native://user:password@host:9000/dbname
```

`MONITORED_SCHEMA` для MySQL/ClickHouse трактуется как имя БД (database). Для ClickHouse значение по умолчанию обычно `default`.

**Особенности диалектов:**
- **MySQL** — `table_rows` в InnoDB это оценка оптимизатора; для трендовой аналитики достаточно, для точных счётчиков — нет. `update_time` может быть `NULL` на партиционированных таблицах.
- **ClickHouse** — `null_count` для не-`Nullable` колонок всегда 0 (по дизайну). `last_modified` собирается из `system.parts` (`max(modification_time)`).
- **MS SQL** — пока не поддерживается (требует ODBC-драйвер вне Python).

NULL-статистика во всех диалектах считается через `COUNT(*) - COUNT(col)` (PostgreSQL дополнительно использует `FILTER (WHERE col IS NULL)` как более идиоматичный вариант). `column_distribution` собирается через `SELECT col, COUNT(*) GROUP BY col ORDER BY 2 DESC LIMIT 20` — пропускает text/json/blob/uuid колонки (top-N по высокой кардинальности — шум, не сигнал).

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
| `MONITOR_DB_URL`     | —            | `sqlite:///monitor.db`    | DSN хранилища метрик                          |
| `MONITORED_SCHEMA`   | —            | `public`                  | Схема Postgres / БД для MySQL/CH              |
| `SECRET_KEY`         | ✅           | —                         | Секрет для Flask-сессий/CSRF                  |
| `COLLECT_INTERVAL_MINUTES` | —      | `15`                      | Интервал коллектора метрик                    |
| `LOG_LEVEL`          | —            | `INFO`                    | Уровень логирования                           |
| `FLASK_ENV`          | —            | `development`             | Режим Flask                                   |
| `HOST`               | —            | `127.0.0.1`               | Bind-адрес (в Docker — `0.0.0.0`)             |
| `PORT`               | —            | `5001`                    | Порт HTTP-сервера                             |
| `FLASK_DEBUG`        | —            | `1` (Docker — `0`)        | Включает дебаг и автоперезапуск Flask         |

> ⚠️ Файл `.env` содержит секреты — не коммитить в git.

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
│   ├── seed_metrics_history.py # 14 дней синтетической истории + drift + анмалии
│   ├── reset_db.py         # объединённый pipeline reset_db / reset-metrics-db
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
- **Дашборд** — Plotly-графики с прогнозом, drift-картой, аннотациями change-points
- **REST API** — JSON endpoints для интеграции с внешними сервисами
- **Multi-DB** — PostgreSQL / MySQL / ClickHouse через единый `DBAdapter` интерфейс

---
