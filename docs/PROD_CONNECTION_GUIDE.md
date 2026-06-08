# Как подключить реальную БД

Этот документ описывает, какие доступы нужны DB Monitor для безопасного подключения к production-базе данных.

**Разделы:** [PostgreSQL](#postgresql) · [Iceberg](#iceberg) · [ClickHouse](#clickhouse)

## Что читает DB Monitor

DB Monitor использует подключение для чтения структуры БД и расчёта метрик:

- список схем, таблиц и колонок;
- типы колонок;
- количество строк;
- долю `NULL` по колонкам;
- размер таблиц, если backend отдаёт эту информацию;
- служебную metadata, например `information_schema`.

## Что DB Monitor не делает

**DB Monitor не выполняет INSERT, UPDATE, DELETE и DDL-операции** в подключаемой базе данных.

Пользователю для мониторинга не нужны права:

- `INSERT`;
- `UPDATE`;
- `DELETE`;
- DDL-права: `CREATE`, `ALTER`, `DROP`.

---

## PostgreSQL

### Минимальные права

Создайте отдельного read-only пользователя:

```sql
CREATE USER monitor_ro WITH PASSWORD '<YOUR_PASSWORD>';

GRANT CONNECT ON DATABASE app TO monitor_ro;
GRANT USAGE ON SCHEMA public TO monitor_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO monitor_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO monitor_ro;
```

Если нужно мониторить несколько схем, повторите `GRANT USAGE` и `GRANT SELECT` для каждой схемы.

### DSN

```text
postgresql://monitor_ro:<YOUR_PASSWORD>@db.example.com:5432/app
```

### Network requirements

- Порт **5432** (по умолчанию) должен быть доступен из контейнера или окружения, где запущен DB Monitor.
- Firewall / security group должен разрешать входящие соединения от DB Monitor.
- DNS-имя БД должно резолвиться внутри runtime DB Monitor.
- TLS-настройки должны соответствовать требованиям вашей БД.

### Smoke-check

```bash
psql "postgresql://monitor_ro:<YOUR_PASSWORD>@db.example.com:5432/app" \
  -c "SELECT current_user, current_database();"
```

Ожидаемый вывод: строка с `monitor_ro` и именем базы. Если `psql` недоступен локально, используйте кнопку «Проверить подключение» в UI после сохранения.

---

## Iceberg

DB Monitor поддерживает Iceberg REST-каталог и AWS Glue.

### DSN

```text
iceberg+rest://catalog.example.com/
```

Для AWS Glue:

```text
iceberg+glue://glue.us-east-1.amazonaws.com/
```

> Параметры `warehouse` и `namespace` вводятся в отдельных полях формы — включать их в DSN не обязательно.

> Подключение к облачным провайдерам (Tabular, Polaris, AWS Glue REST) происходит по HTTPS автоматически. Для dev-стенда без TLS не на localhost добавьте `?ssl=false` в DSN.

### Параметры

| Параметр | Где указывать | Описание |
|---|---|---|
| `warehouse` | Поле «Warehouse» в форме (или `?warehouse=` в DSN) | Путь к корню Iceberg-каталога, например `s3://my-bucket/warehouse` |
| `namespace` | Поле «Namespace» в форме | Namespace (база / схема) для мониторинга, например `prod` |
| `token` | Поле «Auth token» в форме | Bearer-токен для REST-каталога; оставьте пустым, если не требуется |

Значения, введённые в форме, имеют приоритет над параметрами в строке DSN.

### Smoke-check

1. Откройте форму добавления подключения в UI.
2. Введите DSN, Warehouse и Namespace.
3. Нажмите **«Проверить подключение»** — ответ `ok` подтверждает доступность каталога и наличие Namespace.

---

## ClickHouse

DB Monitor поддерживает нативный протокол и HTTP-интерфейс ClickHouse.

### Форматы DSN

| Протокол | Формат | Порт по умолчанию |
|---|---|---|
| Нативный (TCP) | `clickhouse://host:9000/database` | 9000 |
| Нативный явно | `clickhouse+native://host:9000/database` | 9000 |
| HTTP | `clickhouse+http://host:8123/database` | 8123 |

Пример:

```text
clickhouse://monitor_ro:<YOUR_PASSWORD>@ch.example.com:9000/analytics
```

### Минимальные права

Создайте read-only пользователя:

```sql
CREATE USER monitor_ro IDENTIFIED WITH sha256_password BY '<YOUR_PASSWORD>';

-- Права на системные таблицы (нужны для версии и схемы)
GRANT SELECT ON system.tables TO monitor_ro;
GRANT SELECT ON system.columns TO monitor_ro;
GRANT SHOW DATABASES ON *.* TO monitor_ro;

-- Права на вашу базу данных
GRANT SELECT ON analytics.* TO monitor_ro;
```

Для мониторинга нескольких баз повторите последний `GRANT` для каждой.

### Network requirements

- Порт **9000** (нативный протокол) или **8123** (HTTP) должен быть доступен из окружения DB Monitor.
- Firewall должен разрешать входящие соединения от DB Monitor.

### Smoke-check

Если установлен `clickhouse-client`:

```bash
clickhouse-client \
  --host ch.example.com --port 9000 \
  --user monitor_ro --password '<YOUR_PASSWORD>' \
  --query "SELECT version()"
```

Для HTTP-интерфейса:

```bash
curl -u monitor_ro:<YOUR_PASSWORD> \
  "http://ch.example.com:8123/?query=SELECT+version()"
```

Либо используйте кнопку **«Проверить подключение»** в UI после сохранения.

---

## Safety checklist

Перед подключением production БД проверьте:

- используется отдельный пользователь только для чтения;
- у пользователя нет `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP`;
- выбраны только нужные схемы и таблицы;
- интервал сбора подходит для нагрузки на БД;
- тяжёлые таблицы исключены или обрабатываются отдельными load safety настройками.
