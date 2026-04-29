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
- [Запуск в Docker](#запуск-в-docker)
- [Переменные окружения](#переменные-окружения)
- [Структура проекта](#структура-проекта)
- [Функциональность](#функциональность)
---

## Требования

- Python 3.12+
- Доступ к Supabase-проекту (`DATABASE_URL` - у тимлида)

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

## Запуск в Docker

```bash
# 1. Собрать образ
docker build -t db-monitor .

# 2. Запустить контейнер с .env
docker run -p 5000:5000 --env-file .env db-monitor
```

Дашборд: [http://localhost:5000](http://localhost:5000)

В контейнере приложение слушает порт `5000` и привязано к `0.0.0.0`. Все настройки задаются через переменные окружения (см. ниже) или `.env`-файл.

```bash
# health-check внутри контейнера
docker exec <container_id> curl -s http://localhost:5000/healthz
# → {"status": "ok"}
```

> Образ — `python:3.12-slim`, без compose: для MVP-демо хватает одного контейнера. Мониторируемая БД (Supabase) подключается по `DATABASE_URL`.

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
| `HOST`               | —            | `127.0.0.1`               | Bind-адрес (в Docker — `0.0.0.0`)             |
| `PORT`               | —            | `5001` (Docker — `5000`)  | Порт HTTP-сервера                             |
| `FLASK_DEBUG`        | —            | `1` (Docker — `0`)        | Включает дебаг и автоперезапуск Flask         |

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

