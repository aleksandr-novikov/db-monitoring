# Система мониторинга данных в БД (Flask)

Веб-приложение на Flask, которое подключается к базе данных, автоматически собирает метрики качества данных (количество записей, пропуски, ошибки), визуализирует их на дашбордах и уведомляет об аномалиях. Включает ML-детекцию аномалий и timeseries forecasting (прогноз роста таблиц, сезонность, change-point detection).

## 📦 Установка окружения и зависимостей

Следуйте этим шагам для корректной настройки проекта.

---

## 1. Создать виртуальное окружение

```bash
python3 -m venv .venv
```

---

## 2. Активировать окружение

**macOS / Linux:**
```bash
source .venv/bin/activate
```

**Windows:**
```bash
.\.venv\Scripts\activate
```

---

## 3. Обновить pip и установить зависимости

```bash
pip install -U pip
pip install -r requirements.txt
```

Основной стек: Flask, SQLAlchemy, APScheduler, Plotly, scikit-learn, Prophet, statsmodels, ruptures, python-telegram-bot.

---

## 4. Настроить подключение к БД

Создайте файл `.env` на основе `.env.example`:

```bash
cp .env.example .env
```

Заполните переменные:
- `DATABASE_URL` — строка подключения к мониторируемой БД (Postgres/Supabase)
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` — для алертов (опционально)

---

## 5. Запустить приложение

```bash
python app.py
```

Дашборд: http://localhost:5000

## Функциональность

- Автоматический сбор метрик (количество записей, NULL-rate, ошибки типов, schema drift)
- ML-детекция аномалий (Z-score, Isolation Forest, Prophet residuals)
- Timeseries forecasting (прогноз роста таблиц, capacity planning, сезонность)
- Интерактивные дашборды (Plotly)
- Алерты в Telegram / email / Slack webhook
- REST API для интеграции с внешними сервисами
