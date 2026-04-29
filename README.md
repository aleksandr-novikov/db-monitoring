# db-monitoring - система мониторинга данных в БД (Flask)

Веб-приложение на Flask, которое подключается к базе данных, автоматически собирает метрики качества данных (количество записей, пропуски, ошибки), визуализирует их на дашбордах и уведомляет об аномалиях. Включает ML-детекцию аномалий и timeseries forecasting (прогноз роста таблиц, сезонность, change-point detection).

[![Python](https://img.shields.io/badge/python-3.12-blue)](https://www.python.org/)
[![Flask](https://img.shields.io/badge/flask-3.1-green)](https://flask.palletsprojects.com/)
[![License](https://img.shields.io/badge/license-MIT-lightgrey)]()

---

## Содержание

- [Требования](#требования)
- [Установка](#установка)
- [Запуск](#запуск)
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

Основной стек: Flask, SQLAlchemy, APScheduler, Plotly, scikit-learn, Prophet, statsmodels, ruptures, python-telegram-bot.

---

## Запуск

```bash
python app/app.py
```

Дашборд: [http://localhost:5000](http://localhost:5000)

**Проверка:**

```bash
curl http://localhost:5000/healthz
# → {"status": "ok"}
```

**Запуск сборщика метрик вручную:**

Сборщик стартует автоматически вместе с приложением (APScheduler, интервал — `COLLECT_INTERVAL_MINUTES`, по умолчанию 15 мин). Чтобы прогнать сбор немедленно:

```bash
# разовый прогон без шедулера
python -c "from collectors.scheduler import collect_all_tables; collect_all_tables()"

# или принудительный запуск job через admin API (когда сервер запущен)
curl http://localhost:5000/admin/jobs                    # список job_id
curl -X POST http://localhost:5000/admin/jobs/collect_all_tables/run
```

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

