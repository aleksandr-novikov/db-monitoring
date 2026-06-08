# Как подключить реальную БД

Этот документ описывает, какие доступы нужны DB Monitor для подключения к production Postgres или другой рабочей БД.

## Какие credentials нужны

Для подключения нужен отдельный пользователь только для чтения. Не используйте admin, owner или superuser-аккаунт.

Минимально нужны:

- host и port БД;
- имя базы данных;
- имя пользователя только для чтения;
- пароль этого пользователя;
- schema или namespace, которые нужно мониторить.

Пример DSN:

```text
postgresql://db_monitor_ro:strong-password@db.example.com:5432/app
```

## Что читает DB Monitor

DB Monitor использует подключение для чтения структуры БД и расчёта метрик:

- список схем, таблиц и колонок;
- типы колонок;
- количество строк;
- долю `NULL` по колонкам;
- размер таблиц, если backend отдаёт эту информацию;
- служебную metadata, например `information_schema`.

## Что DB Monitor не делает

DB Monitor не должен менять данные в подключаемой БД.

Пользователю для мониторинга не нужны права:

- `INSERT`;
- `UPDATE`;
- `DELETE`;
- DDL-права: `CREATE`, `ALTER`, `DROP`.

## Минимальные права для Postgres

Пример создания read-only пользователя:

```sql
CREATE USER db_monitor_ro WITH PASSWORD 'strong-password';

GRANT CONNECT ON DATABASE app TO db_monitor_ro;
GRANT USAGE ON SCHEMA public TO db_monitor_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_monitor_ro;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT ON TABLES TO db_monitor_ro;
```

Если нужно мониторить несколько схем, повторите `GRANT USAGE` и `GRANT SELECT` для каждой схемы.

## Network requirements

Приложение должно иметь сетевой доступ к БД:

- host и port БД доступны из контейнера или окружения, где запущен DB Monitor;
- firewall/security group разрешает входящие соединения от DB Monitor;
- TLS-настройки соответствуют требованиям вашей БД;
- DNS-имя БД резолвится внутри runtime DB Monitor.

## Safety checklist

Перед подключением production БД проверьте:

- используется отдельный пользователь только для чтения;
- у пользователя нет `INSERT`, `UPDATE`, `DELETE`, `CREATE`, `ALTER`, `DROP`;
- выбраны только нужные схемы и таблицы;
- интервал сбора подходит для нагрузки на БД;
- тяжёлые таблицы исключены или обрабатываются отдельными load safety настройками.
