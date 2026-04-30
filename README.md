# db-monitoring - система мониторинга данных в БД (Flask)

Веб-приложение на Flask, которое подключается к базе данных, автоматически собирает метрики качества данных (количество записей, пропуски, ошибки), визуализирует их на дашбордах и уведомляет об аномалиях. Включает ML-детекцию аномалий и timeseries forecasting (прогноз роста таблиц, сезонность, change-point detection).

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
- [Поддерживаемые СУБД](#поддерживаемые-субд)
- [Переменные окружения](#переменные-окружения)
- [Структура проекта](#структура-проекта)
- [Функциональность](#функциональность)
---

## Требования

- Python 3.12+
- DSN мониторируемой БД в `DATABASE_URL` — поддерживаются PostgreSQL / MySQL / ClickHouse (см. [Поддерживаемые СУБД](#поддерживаемые-субд))

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

Основной стек: Flask, SQLAlchemy, APScheduler, Plotly, scikit-learn, Prophet, statsmodels, ruptures.

---

## Запуск

```bash
python app/app.py
```

Дашборд: [http://localhost:5001](http://localhost:5001)

**Проверка:**

```bash
curl http://localhost:5001/healthz
# → {"status": "ok"}
```

**Запуск сборщика метрик вручную:**

Сборщик стартует автоматически вместе с приложением (APScheduler, интервал — `COLLECT_INTERVAL_MINUTES`, по умолчанию 15 мин). Чтобы прогнать сбор немедленно:

```bash
# разовый прогон без шедулера
python -c "from collectors.scheduler import collect_all_tables; collect_all_tables()"

# или принудительный запуск job через admin API (когда сервер запущен)
curl http://localhost:5001/admin/jobs                    # список job_id
curl -X POST http://localhost:5001/admin/jobs/collect_all_tables/run
```

---

## Демо-данные (сидирование)

Перед запуском дашборда нужно заполнить обе БД тестовыми данными.

**1. Мониторируемая БД** — создаёт таблицы `users`, `products`, `orders`, `events` с контролируемыми дефектами:

```bash
# Быстрый старт (~35k строк, ~1 мин на Supabase free tier):
python -m scripts.seed_target_db

# Повторный запуск / чистая пересидировка — нужен флаг --reset:
python -m scripts.seed_target_db --reset

# Полный demo-датасет (~350k строк, ~10 мин на Supabase free tier):
python -m scripts.seed_target_db --users 50000 --products 1000 --orders 100000 --events 200000 --reset
```

> ⚠️ `--reset` обязателен для очистки таблиц перед повторным сидированием. Без флага данные только добавляются.

**2. История метрик** — генерирует 14 дней метрик (каждые 15 мин) с тремя видимыми аномалиями:

```bash
python -m scripts.seed_metrics_history
```

Аномалии на графиках после сидирования:
- `orders` — всплеск `null_rate` на 6–7-й день
- `events` — постепенный рост `null_rate` последние 5 дней
- `products` — резкое падение `row_count` на 10-й день

> Скрипты идемпотентны: повторный запуск `seed_target_db` очищает данные и сидирует заново.

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
- **ClickHouse** — `null_count` для не-`Nullable` колонок всегда 0 (по дизайну: туда нельзя положить NULL). `last_modified` собирается из `system.parts` (`max(modification_time)`).
- **MS SQL** — пока не поддерживается (отдельный тикет: требует ODBC-драйвер вне Python).

NULL-статистика во всех диалектах считается через `COUNT(*) - COUNT(col)` (PostgreSQL дополнительно использует `FILTER (WHERE col IS NULL)` как более идиоматичный вариант).

---

## Переменные окружения

Все параметры задаются через `.env`. Шаблон — в `.env.example`.

| Переменная           | Обязательная | По умолчанию              | Описание                                      |
|----------------------|:------------:|---------------------------|-----------------------------------------------|
| `DATABASE_URL`       | ✅           | —                         | DSN мониторируемой БД (Postgres/Supabase)     |
| `MONITOR_DB_URL`     | —            | `sqlite:///monitor.db`    | DSN хранилища метрик                          |
| `MONITORED_SCHEMA`   | —            | `public`                  | Схема PostgreSQL для мониторинга              |
| `SECRET_KEY`         | ✅           | —                         | Секрет для Flask-сессий/CSRF                  |
| `LOG_LEVEL`          | —            | `INFO`                    | Уровень логирования                           |
| `FLASK_ENV`          | —            | `development`             | Режим Flask                                   |

> ⚠️ Файл `.env` содержит секреты — не коммитить в git!

---

## Структура проекта

```
db-monitoring/
├── app/
│   ├── __init__.py
│   ├── app.py              # Фабрика Flask-приложения, blueprints, /healthz
│   └── config.py           # Загрузка конфигурации из окружения
├── collectors/             # Сборщики метрик из мониторируемой БД
├── api/                    # REST API blueprints
├── templates/              # Jinja2-шаблоны (дашборды)
├── static/                 # Статика (CSS, JS)
├── tests/                  # Юнит- и интеграционные тесты
├── .env.example            # Шаблон конфигурации
├── .env                    # Локальные секреты (не коммитить!)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Функциональность

- **Сбор метрик** — количество записей, NULL-rate, ошибки типов, schema drift
- **ML-детекция аномалий** — Z-score, Isolation Forest, Prophet residuals
- **Timeseries forecasting** — прогноз роста таблиц, capacity planning, сезонность, change-point detection
- **Дашборды** — интерактивная визуализация на Plotly
- **REST API** — интеграция с внешними сервисами

---

