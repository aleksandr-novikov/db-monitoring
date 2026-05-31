# Demo 3.2 — сценарий демо и подготовка стенда

## Цель демо

Показать DB Monitor как систему мониторинга качества данных для нескольких
проектов и разных источников данных: Postgres, ClickHouse и Iceberg. Главная
линия показа: пользователь подключает БД, система собирает метрики, показывает
историю и ML-сигналы, а при инциденте оставляет уведомление в Telegram и в
audit trail.

## Участники и проекты

### Demo user 1

- Email: `demo@dbmonitor.app`
- Основные проекты:
  - `Retail Postgres`
  - `Events ClickHouse`

### Demo user 2

- Email: `lake@dbmonitor.app`
- Основной проект:
  - `Iceberg Lakehouse`

### Shared project

Shared project нужен для демонстрации совместного доступа двух пользователей к
одному проекту. На текущий момент в коде проекты привязаны к одному `user_id`,
таблицы `project_members` нет. Поэтому shared project — отдельный gap и задача
`#172`.

Если `#172` не готова к репетиции, shared access показываем как пункт roadmap:
"следующий шаг — командный доступ к одному проекту".

## Источники данных

### Retail Postgres

Основной и самый стабильный demo path.

Таблицы:

- `users`
- `products`
- `orders`
- `events`

Что показываем:

- row count;
- NULL rate;
- schema drift;
- anomaly / changepoint;
- history;
- Telegram alert.

### Events ClickHouse

Показывает, что продукт работает не только с Postgres.

Что показываем:

- отдельный проект;
- ClickHouse connection;
- таблицы ClickHouse на dashboard;
- тот же UI поверх другого backend adapter.

### Iceberg Lakehouse

Показывает lakehouse-сценарий.

Что показываем:

- Iceberg REST + MinIO;
- Iceberg connection;
- таблицу Iceberg;
- schema и null counts из metadata/manifest.

Важно: `make smoke-iceberg` уже проверяет adapter и collector path. Для
полноценного UI demo нужно подготовить отдельный demo project и connection.

## Тайминг

Целевой тайминг: 8-10 минут.

1. Вход и проекты — 1 минута.
2. Postgres overview — 1.5 минуты.
3. Table detail + ML — 2 минуты.
4. History + schema/drift — 1.5 минуты.
5. Telegram + notification history — 1.5 минуты.
6. ClickHouse / Iceberg — 1.5 минуты.
7. Healthcheck / metrics / jobs — 30 секунд.

## Пошаговый сценарий

### 1. Login

Открыть:

```text
/auth/login
```

Действие:

- войти под `demo@dbmonitor.app`;
- перейти на dashboard.

Что сказать:

> Начинаем как обычный пользователь. DB Monitor многопользовательский:
> проекты, подключения, метрики и уведомления привязаны к конкретному
> пользователю.

Ожидаемый результат:

- пользователь залогинен;
- виден header с project switcher;
- доступен dashboard.

### 2. Projects

Открыть:

```text
/projects
```

Показать:

- список проектов пользователя;
- `Retail Postgres`;
- `Events ClickHouse`;
- переход в проект.

Что сказать:

> Проект — это рабочее пространство мониторинга. Внутри проекта лежит
> подключение к источнику данных, расписание сбора и настройки уведомлений.

Ожидаемый результат:

- пользователь видит свои проекты;
- проекты другого пользователя не отображаются.

### 3. Connections

Открыть страницу подключений текущего проекта.

Показать:

- имя подключения;
- masked DSN;
- schema;
- interval;
- active state;
- test connection.

Что сказать:

> DSN хранится зашифрованным, в UI и логах пароль не раскрывается. Активное
> подключение регистрирует per-project scheduler job.

Ожидаемый результат:

- connection активен;
- test connection проходит;
- DSN замаскирован.

### 4. Dashboard overview — Retail Postgres

Открыть:

```text
/dashboard
```

Показать:

- количество мониторируемых таблиц;
- total rows;
- average NULL rate;
- таблицу overview;
- last check;
- ML block.

Что сказать:

> Это обзор качества данных по проекту. Оператор сразу видит масштаб данных,
> свежесть проверки и таблицы, где есть повышенный NULL rate.

Ожидаемый результат:

- видны `users`, `products`, `orders`, `events`;
- метрики не пустые;
- last check заполнен.

### 5. Table detail — events

Открыть:

```text
/dashboard/schema/events
```

Показать:

- row count graph;
- переключатель `NULL rate`;
- forecast toggle;
- anomaly KPI;
- список причин аномалий;
- schema columns;
- drift / schema events.

Что сказать:

> Здесь начинается расследование. Мы видим динамику по конкретной таблице,
> прогноз, аномальные точки и вклад отдельных признаков. Для `events` удобно
> показывать рост NULL по `ip_address`.

Ожидаемый результат:

- график за 14 дней не пустой;
- forecast включается;
- anomaly KPI заполнен на подготовленном incident;
- schema/drift секции содержат данные.

### 6. History

Открыть:

```text
/dashboard/history
```

Показать:

- key insights;
- daily chart;
- последние запуски коллектора;
- problems;
- NULL spikes;
- ML anomalies;
- coverage.

Что сказать:

> История нужна для ретроспективы. Мы видим не только текущее состояние, но и
> как менялось качество данных: где были всплески NULL, аномалии и проблемы
> покрытия мониторингом.

Ожидаемый результат:

- есть последние запуски;
- daily chart построен;
- insights не пустые.

### 7. Schema and drift

Открыть:

```text
/dashboard/schema
```

Показать:

- список таблиц;
- колонки и типы;
- NULL rate по колонкам;
- drift PSI/KS;
- schema drift badge, если есть подготовленные события.

Что сказать:

> Schema drift важен для data pipeline: добавленная колонка, смена типа или
> nullable могут сломать витрины без явной ошибки приложения.

Ожидаемый результат:

- schema page показывает таблицы;
- на деталке таблицы видна история изменений схемы.

### 8. Telegram settings

Открыть настройки уведомлений проекта.

Показать:

- bot token;
- chat id;
- throttle;
- test notification.

Что сказать:

> Telegram настраивается на уровне проекта. У каждой команды может быть свой
> чат и свой throttle, без глобального admin-чата.

Ожидаемый результат:

- settings сохранены;
- test notification отправляется или есть fallback-запись.

### 9. Incident and notification history

Запустить live incident заранее или во время демо:

```bash
make live-demo PROJECT_ID=<project_id> CONNECTION_ID=<connection_id> \
  ARGS="--ticks 20 --interval 5 --incident-at 8 --changepoints"
```

Открыть:

```text
/dashboard/notifications
```

Показать:

- Telegram message;
- notification audit trail;
- status `sent` / `failed`;
- filters.

Что сказать:

> Каждая попытка отправки сохраняется. Даже если Telegram недоступен, это
> видно в истории как failed event. Поэтому можно отличить отсутствие
> инцидента от проблемы доставки.

Ожидаемый результат:

- в Telegram есть alert или подготовленный fallback;
- `/dashboard/notifications` содержит запись.

### 10. ClickHouse project

Переключиться на:

```text
Events ClickHouse
```

Показать:

- тот же dashboard;
- ClickHouse tables;
- table detail.

Что сказать:

> Здесь другой источник данных, ClickHouse, но для пользователя модель та же:
> проект, connection, collector, dashboard. Разные backend adapters приводятся
> к единому интерфейсу мониторинга.

Ожидаемый результат:

- ClickHouse tables видны в overview;
- connection test проходит.

### 11. Iceberg project

Войти под `lake@dbmonitor.app` или переключиться на подготовленный Iceberg
project.

Показать:

- Iceberg connection;
- schema/table detail;
- null counts.

Что сказать:

> Iceberg-путь показывает lakehouse-сценарий. Для таких таблиц мы читаем
> метаданные и manifest-информацию, не делая полный скан данных.

Ожидаемый результат:

- Iceberg table видна в UI;
- row count и null counts заполнены.

### 12. Operational signals

Открыть:

```text
/healthz
/metrics
/admin/jobs
```

Показать:

- health status;
- Prometheus metrics;
- scheduler jobs вида `collect:<project_id>:<connection_id>`;
- логи без plaintext DSN.

Что сказать:

> Это не только UI. У приложения есть healthcheck, Prometheus endpoint,
> scheduler jobs и структурированные логи, поэтому его можно эксплуатировать.

Ожидаемый результат:

- `/healthz` отвечает `ok`;
- `/metrics` отдает Prometheus format;
- jobs видны в admin.

## Fallback path

Если live окружение не поднимается:

1. Использовать заранее подготовленный `monitor.db`.
2. Показывать Postgres dashboard как основной сценарий.
3. Для Telegram использовать заранее созданные notification rows или скрин.
4. ClickHouse/Iceberg показать через smoke output или скринкаст.
5. Не показывать live incident, а открыть уже заполненную деталку `events`.

## Gaps перед демо

- `#173` demo seed: нужно создать пользователей, проекты и connections одной
  командой.
- `#172` shared project: пока нет `project_members`, поэтому общий проект
  двум пользователям не показать честно.
- `#178` Iceberg demo: smoke path есть, нужен устойчивый UI path.
- `#176` history/ML warmup: нужно прогревать данные по каждому demo project.
- `#182` Telegram demo path: нужен стабильный bot/chat или fallback.
- `#180` / `#171` anomaly alert quality: важно убрать ложные и противоречивые
  уведомления перед показом.

## Definition of ready для репетиции

- Demo users существуют и известны пароли.
- Все demo projects видны в UI.
- Connections проходят test.
- Dashboard не пустой.
- History не пустая.
- Table detail `events` показывает график и anomaly.
- Telegram path проверен.
- Iceberg path проверен.
- Есть fallback assets.
