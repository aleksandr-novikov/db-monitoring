# Demo 3.2 — сценарий демо и подготовка стенда

## Цель демо

Показать DB Monitor как систему мониторинга качества данных для нескольких
проектов и разных источников данных: Postgres, ClickHouse и Iceberg. Главная
линия показа: пользователь подключает БД, система собирает метрики, показывает
историю и ML-сигналы, а при инциденте оставляет уведомление в Telegram и в
audit trail.

**Ключевые фичи для демо:**
- Per-project Telegram уведомления: каждый проект получает свои алерты (#197)
- Аномалии, changepoint, schema drift — каждый тип уведомлений с названием проекта (#205, #208)
- Поддержка трёх backend: Postgres, ClickHouse, Iceberg
- Shared project access: несколько пользователей видят один проект с разными ролями (#172)
- Anomaly quality gate + LLM объяснения: алерты фильтруются по score/delta, к каждому генерируется LLM-объяснение (#171)
- Prometheus metrics, JSON-логи, Sentry, healthcheck с версией (#101, #102, #103, #105)

---

## Участники и проекты

### Demo user 1

- Email: `demo@dbmonitor.app`
- Password: `demo12345`
- Проекты:
  - `Retail Postgres` (slug: `retail-postgres`)
  - `Events ClickHouse` (slug: `events-clickhouse`)

### Demo user 2

- Email: `lake@dbmonitor.app`
- Password: `demo12345`
- Проект:
  - `Iceberg Lakehouse` (slug: `iceberg-lakehouse`)

### Demo guest (для показа shared access, #172)

- Email: `guest@dbmonitor.app`
- Password: `demo12345`
- Роль: `viewer` на проекте `Retail Postgres`

После `seed_demo_workspace` этот пользователь автоматически добавлен как viewer.
Можно показать: войти под `guest@` — виден `Retail Postgres` от другого пользователя,
но удалить/изменить нельзя.

Добавить участника вручную (если нужно показать CLI):
```bash
python -m scripts.add_project_member \
  --owner demo@dbmonitor.app --slug retail-postgres \
  --email guest@dbmonitor.app --role viewer
```

Создать пользователей и проекты (идемпотентно):

```bash
python -m scripts.seed_demo_workspace --reset-password
```

---

## Быстрая подготовка — одна команда (только Retail Postgres)

Для репетиции и быстрого старта — единая команда `make demo-prepare` (#176)
заменяет шаги 2–3 ниже для проекта `Retail Postgres`:

```bash
docker compose up -d --build app        # Шаг 1 — поднять стек
make demo-prepare                       # seed_workspace + seed_metrics + warmup_ml
make telegram-demo ARGS=configure       # Шаг 6 — Telegram
```

`make demo-prepare` делает за один прогон:
1. `seed_demo_workspace` — создаёт пользователей, проекты, connections
2. `seed_metrics_db` — 14 дней синтетических метрик для retail-postgres
3. `warmup_ml` — обучает Prophet, IsolationForest, PELT, drift

> **Ограничение:** `make demo-prepare` подготавливает только `retail-postgres`.
> Для ClickHouse и Iceberg нужны отдельные шаги 4–5 ниже.

---

## Полная подготовка стенда с нуля

Выполнять по порядку. Каждый шаг обязателен.

### Шаг 1 — Поднять основной стек

```bash
docker compose up -d --build app
```

Проверить healthcheck:

```bash
curl http://localhost:5001/healthz
```

Ожидаемый результат: `"status": "ok"`.

### Шаг 2 — Создать demo users/projects/connections

```bash
docker compose exec app python -m scripts.seed_demo_workspace --reset-password
```

После выполнения в терминале будут напечатаны `PROJECT_ID` и `CONNECTION_ID`
для `Retail Postgres`. Сохрани их — понадобятся на следующих шагах.

### Шаг 3 — Подготовить Retail Postgres

```bash
# Создать таблицы и засеять данные (users, products, orders, events)
docker compose exec app python -m scripts.seed_target_db --reset

# Получить PROJECT_ID для retail-postgres (если не сохранил с шага 2)
make demo-ids

# Засеять 14 дней синтетических метрик
docker compose exec app python -m scripts.seed_metrics_db --reset --project-id <PROJECT_ID>

# Прогреть ML (changepoint, anomaly, forecast, drift)
docker compose exec app python -m scripts.warmup_ml --project-id <PROJECT_ID>
```

Стартовые данные в Postgres:
- `users` — 5 000 строк (~5% email NULL)
- `products` — 500 строк
- `orders` — 10 005 строк
- `events` — 80 000 строк (растущий NULL rate по `ip_address` за последние 7 дней)

### Шаг 4 — Подготовить ClickHouse

```bash
# Поднять ClickHouse контейнер
make clickhouse-up
```

ClickHouse будет доступен:
- HTTP: `http://localhost:8123` (user=default, db=demo, без пароля)
- Native: `localhost:19000`

```bash
# Единый orchestrator: создаёт таблицы, seed-ит 14 дней метрик, прогревает ML
make clickhouse-demo
```

`make clickhouse-demo` делает всё автоматически:
- Создаёт 4 таблицы в ClickHouse: `users`, `products`, `orders`, `events`
- Засеивает синтетические данные с реалистичными паттернами роста
- Seed-ит 14 дней метрик в monitoring DB
- Прогревает ML для проекта `Events ClickHouse`
- Печатает `PROJECT_ID` и `CONNECTION_ID` в конце

После завершения в терминале:
```
ClickHouse demo is ready: http://localhost:5001/dashboard/
PROJECT_ID=...
CONNECTION_ID=...
```

### Шаг 5 — Подготовить Iceberg Lakehouse

```bash
# Поднять Iceberg REST + MinIO
make iceberg-up

# Единый orchestrator: создаёт таблицы, seed-ит данные, прогревает ML
make iceberg-demo
```

`make iceberg-demo` делает всё автоматически — аналогично `clickhouse-demo`.
Iceberg таблицы: `events`, `orders`, `customers`, `sessions`.

### Шаг 6 — Настроить Telegram уведомления

Проверить `.env`:
```
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

Сохранить Telegram конфиг для **всех трёх** demo-проектов:
```bash
make telegram-demo ARGS=configure
```

Вывод должен быть:
```
configured: Retail Postgres (retail-postgres)
configured: Iceberg Lakehouse (iceberg-lakehouse)
configured: Events ClickHouse (events-clickhouse)
```

Проверить что конфиг сохранён:
```bash
make telegram-demo ARGS=test
```

### Шаг 7 — Запустить live incident (опционально)

Для показа динамики в реальном времени:

```bash
docker compose exec app python -m scripts.live_demo \
  --project-id <RETAIL_PROJECT_ID> \
  --connection-id <RETAIL_CONNECTION_ID> \
  --ticks 3 \
  --interval 1 \
  --incident-at 2 \
  --changepoints
```

---

## Telegram demo-команды

Все команды запускаются без изменений в БД — синтетические данные,
throttle обходится автоматически.

### Аномалия

```bash
python -m scripts.telegram_demo alert
```

Отправляет anomaly уведомление для каждого проекта одновременно.

Пример сообщения:
```
🚨 DB Monitor: аномалия
Проект: Retail Postgres (retail-postgres)
Таблица: events
Метрика: null_rate
Время: 2026-06-05 20:23 UTC
Score: -0.4200

Аномалия, обнаруженная в базе данных...
```

### Schema drift (per-project, с паузой)

```bash
python -m scripts.telegram_demo schema_drift --delay 5
```

Отправляет синтетическое schema drift уведомление для каждого проекта
**по очереди** с паузой 5 секунд между проектами. Синтетическое событие:
`column_added — revenue (numeric)`.

Пример сообщения:
```
📋 Дрейф схемы:
Проект: Retail Postgres (retail-postgres)
Таблица: events
  • column_added — revenue (numeric)
```

Через 5 секунд придёт для Iceberg Lakehouse, ещё через 5 — для ClickHouse.

### Changepoint (per-project, с паузой)

```bash
python -m scripts.telegram_demo changepoint --delay 5
```

Отправляет синтетическое changepoint уведомление для каждого проекта
**по очереди** с паузой 5 секунд. Значения реалистичны для каждого проекта:
- Retail Postgres: `null_rate 2.0% → 18.0%`
- Iceberg Lakehouse: `row_count 977,451 → 1,242,816`
- Events ClickHouse: `row_count 75,000 → 95,000`

Пример сообщения:
```
📈 Change-point:
Проект: Iceberg Lakehouse (iceberg-lakehouse)
Таблица: sessions
row_count: 977,451 → 1,242,816 (2026-06-05)
```

### Fallback (если Telegram API недоступен)

```bash
python -m scripts.telegram_demo fallback
```

Записывает `failed` строку в notification history — можно показать
audit trail без реальной доставки.

### Параметры

| Флаг | Описание | Дефолт |
|---|---|---|
| `--delay N` | Пауза в секундах между проектами | 8 |
| `--respect-throttle` | Не обходить throttle для `alert` | выкл |
| `--throttle-minutes N` | Throttle для `configure` | 30 |

---

## Пошаговый сценарий презентации (~11 минут)

### 1. Вход (1 мин)

URL: `http://localhost:5001/auth/login`

Войти как `demo@dbmonitor.app / demo12345`.

Что сказать:
> DB Monitor — многопользовательская система. Каждый пользователь видит
> только свои проекты, подключения и уведомления.

### 2. Проекты (30 сек)

Открыть `/projects`. Показать `Retail Postgres` и `Events ClickHouse`.

Переключиться через project switcher вверху — показать что проекты изолированы.

### 3. Подключения (30 сек)

Открыть Подключения текущего проекта. Показать:
- masked DSN
- статус активно
- кнопка Тест

Что сказать:
> DSN хранится зашифрованным — в UI и логах пароль не раскрывается.

### 4. Обзор — Retail Postgres (1.5 мин)

Перейти на `/dashboard`. Показать:
- 4 таблицы: users, products, orders, events
- total rows ~95 000
- NULL rate
- ML блок: 4 обученные модели

### 5. Детальная страница таблицы events (2 мин)

Открыть `/dashboard/schema/events`. Показать:
- график row count за 14 дней
- включить прогноз (checkbox "Прогноз 7 дн.")
- переключить на NULL rate — виден spike и рост
- anomaly markers (красные точки)
- changepoint labels (вертикальные линии)
- раздел "Причины аномалий" — кликнуть на запись → LLM объяснение

Что сказать:
> Здесь начинается расследование. Мы видим что events.ip_address начал
> чаще приходить NULL — система обнаружила это автоматически.

> LLM объяснения (#171): каждая аномалия анализируется языковой моделью —
> объяснение появляется в карточке прямо на странице. Алерты в Telegram
> также содержат LLM-текст. При этом работает quality gate: мелкие колебания
> (низкий score или малый delta от baseline) фильтруются и не генерируют
> уведомление — только значимые события доходят до Telegram.

### 6. История (1 мин)

Открыть `/dashboard/history`. Показать:
- daily chart
- problems / NULL spikes
- ML anomalies

### 7. Схема и Drift (1 мин)

Открыть `/dashboard/schema`. Показать:
- список таблиц с колонками
- NULL rate по колонкам
- PSI / KS drift статус (Critical / OK)
- история изменений схемы (если есть)

### 8. Per-project Telegram уведомления (2 мин) ⭐

Это главная часть демо для #197.

Открыть терминал рядом с браузером. Запустить по очереди:

```bash
# Schema drift — 3 уведомления по очереди с паузой
python -m scripts.telegram_demo schema_drift --delay 5

# Changepoint — 3 уведомления по очереди с паузой
python -m scripts.telegram_demo changepoint --delay 5
```

Показать Telegram — уведомления приходят одно за другим:
1. Retail Postgres
2. Iceberg Lakehouse (через 5 сек)
3. Events ClickHouse (через 5 сек)

Что сказать:
> Раньше все уведомления шли в один общий Telegram без разбивки.
> Теперь каждый проект получает своё уведомление с именем проекта,
> таблицей и деталями изменения. Три разных источника данных —
> Postgres, Iceberg, ClickHouse — один унифицированный механизм алертов.

Открыть `/dashboard/notifications` — показать audit trail.

### 9. Shared project access — командный доступ (1 мин)

Выйти из `demo@dbmonitor.app`, войти как `guest@dbmonitor.app / demo12345`.

Открыть `/projects`. Показать:
- виден `Retail Postgres` от другого пользователя с ролью `viewer`
- на dashboard данные те же
- нет кнопки удаления / изменения (роль viewer)

Выйти, вернуться под `demo@dbmonitor.app`.

Что сказать:
> Командный доступ (#172): владелец проекта приглашает коллегу с ролью
> viewer, editor или owner. Каждый видит только свои + shared проекты.
> Метрики, уведомления и история привязаны к проекту, а не к пользователю.

### 10. ClickHouse проект (1 мин)

Переключиться на `Events ClickHouse` через project switcher.

Открыть `/dashboard`. Показать те же 4 таблицы — тот же UI, другой backend.

Что сказать:
> Здесь ClickHouse. Для пользователя модель та же: проект, connection,
> dashboard. Разные backend adapters приводятся к единому интерфейсу.

### 11. Iceberg проект (1 мин)

Выйти, войти как `lake@dbmonitor.app / demo12345`.

Открыть `/dashboard`. Показать:
- Iceberg таблицы с большими row counts (sessions ~ 1M строк)
- table detail `events` — NULL rate incident по `device_id`

### 12. Operational signals (30 сек)

Открыть:
- `/healthz` — статус ok, поле `version` показывает git SHA (#105)
- `/metrics` — Prometheus endpoint с HTTP/collector/auth счётчиками (#101)
- `/admin/jobs` — scheduler jobs вида `collect:<project_id>:<connection_id>`
- `/admin/rollback-checklist` — runbook для отката через Docker `:previous` тег (#106/#107)

Что сказать:
> Приложение production-ready: healthcheck с версией, Prometheus метрики,
> структурированные JSON-логи с request_id (#102), Sentry для автоматического
> capture исключений (#103). Откат — смена тега образа без downtime.

---

## Источники данных

### Retail Postgres

Основной demo path. Команды подготовки — см. **Шаг 3** выше.

Быстрый вариант: `make demo-prepare` (см. раздел "Быстрая подготовка").

### Events ClickHouse

```bash
make clickhouse-up
make clickhouse-demo
```

Для остановки: `make clickhouse-down`

### Iceberg Lakehouse

```bash
make iceberg-up
make iceberg-demo
```

Для остановки: `make iceberg-down`

---

## Fallback path

Если live окружение не поднимается:

1. Использовать заранее подготовленный `monitor.db`.
2. Показывать Retail Postgres dashboard как основной сценарий.
3. Для Telegram выполнить `make telegram-demo ARGS=fallback` или использовать заранее подготовленный скрин.
4. ClickHouse/Iceberg показать через smoke output или скринкаст.
5. Не показывать live incident, открыть уже заполненную деталку `events`.

---

## Тайминг

| # | Блок | Время |
|---|---|---|
| 1 | Вход и проекты | 1 мин |
| 2 | Postgres overview | 1.5 мин |
| 3 | Table detail + ML + LLM | 2 мин |
| 4 | History + schema/drift | 1 мин |
| 5 | Per-project Telegram (#197) | 2 мин |
| 6 | Shared project access (#172) | 1 мин |
| 7 | ClickHouse проект | 1 мин |
| 8 | Iceberg проект | 1 мин |
| 9 | Operational signals | 30 сек |
| | **Итого** | **~11 мин** |

---

## Definition of ready для репетиции

> Подробный pre-demo чеклист с таймингами — `docs/CHECKLIST.md` (#109)

### Основное

- [ ] `make server` запущен, `/healthz` → `status: ok`, поле `version` не пустое
- [ ] `demo@dbmonitor.app / demo12345` — вход успешен
- [ ] `lake@dbmonitor.app / demo12345` — вход успешен
- [ ] `guest@dbmonitor.app / demo12345` — вход успешен, виден Retail Postgres

### Retail Postgres

- [ ] 4 таблицы в dashboard (users, products, orders, events)
- [ ] Графики за 14 дней не пустые
- [ ] Прогноз включается на events
- [ ] Раздел "Причины аномалий" не пустой, LLM текст загружается по клику
- [ ] Schema drift history есть (хотя бы 1 событие)

### ClickHouse

- [ ] `make clickhouse-up` + `make clickhouse-demo` выполнены
- [ ] `Events ClickHouse` — таблицы видны в dashboard
- [ ] Графики за 14 дней не пустые

### Iceberg

- [ ] `make iceberg-up` + `make iceberg-demo` выполнены
- [ ] `Iceberg Lakehouse` — таблицы видны, sessions ~ 1M строк

### Telegram

- [ ] `make telegram-demo ARGS=configure` — вывод: 3 проекта configured
- [ ] `make telegram-demo ARGS=test` — 3 тестовых сообщения пришли
- [ ] `python -m scripts.telegram_demo alert` — аномалия с LLM-текстом пришла
- [ ] `python -m scripts.telegram_demo schema_drift --delay 5` — 3 уведомления по очереди с "Проект: ..."
- [ ] `python -m scripts.telegram_demo changepoint --delay 5` — 3 уведомления по очереди с "Проект: ..."
- [ ] `/dashboard/notifications` — audit trail содержит записи

### Shared access

- [ ] `guest@dbmonitor.app` видит `Retail Postgres` в `/projects`
- [ ] Кнопки удаления / изменения недоступны под guest

### Operational

- [ ] `/metrics` отдаёт Prometheus формат
- [ ] `/admin/jobs` показывает `collect:<project_id>:<connection_id>` jobs
- [ ] `/admin/rollback-checklist` открывается без ошибок

## Связанные документы

| Документ | Что |
|---|---|
| [docs/CHECKLIST.md](../CHECKLIST.md) | 10-минутная pre-demo проверка с таймингами (#109) |
| [docs/sprint3_summary.md](../sprint3_summary.md) | Все фичи и PR спринта 3 |
| [docs/runbooks/](../runbooks/) | Backup, rollback, инциденты (#106, #107) |

---

## HF Space handoff

Публичное демо показывается на Hugging Face Space.

### Env vars для Space

```
SECRET_KEY=...
FERNET_KEY=...
DATABASE_URL=...
MONITOR_DB_URL=...       # если не дефолтный sqlite:///monitor.db
TELEGRAM_BOT_TOKEN=...
TELEGRAM_CHAT_ID=...
```

### Подготовка на HF Space

```bash
# 1. Обновить Space до свежего master

# 2. Создать demo workspace
python -m scripts.seed_demo_workspace --reset-password \
  --postgres-dsn "$DEMO_POSTGRES_DSN" \
  --clickhouse-dsn "$DEMO_CLICKHOUSE_DSN" \
  --iceberg-dsn "$DEMO_ICEBERG_DSN"

# 3. Подготовить Retail Postgres (seed + ML warmup)
make demo-prepare

# 4. Подготовить ClickHouse (если доступен в Space)
make clickhouse-demo

# 5. Подготовить Iceberg (если доступен в Space)
make iceberg-demo

# 6. Настроить Telegram
make telegram-demo ARGS=configure

# 7. Проверить
make telegram-demo ARGS=test
curl https://<your-space-url>/healthz
```

Секреты и полные DSN не публиковать в issue/PR/logs.
