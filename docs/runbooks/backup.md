# Backup runbook (#106)

> **Когда читать:** перед каждым релизом, перед миграциями, при подозрении
> на data corruption. **Ожидаемое время прочтения:** 3 минуты.

## TL;DR

```bash
# Бэкап обоих DB здесь и сейчас:
make backup

# Восстановление из конкретного дампа:
RESTORE_URL=postgresql://postgres:dev@localhost:5432/monitor \
  scripts/restore.sh backups/target-monitor-20260530-031500.sql.gz
```

## Что и куда бэкапится

| Источник | Скрипт пишет | Что внутри |
|---|---|---|
| `DATABASE_URL` (target prod DB) | `backups/target-<dbname>-YYYYMMDD-HHMMSS.sql.gz` | `pg_dump` целиком |
| `MONITOR_DB_URL` (metrics store) | `backups/metrics-<dbname>-{YYYYMMDD-HHMMSS}.{sql,sqlite}.gz` | Postgres → `pg_dump`, SQLite → `sqlite3 .backup` |

Рядом с каждым дампом — `.sha256` sidecar. Восстановление **отказывается**
работать без валидной контрольной суммы (защита от bit-rot).

## Где лежат бэкапы

- **Локально**: `./backups/` в репозитории (gitignored).
- **В Docker**: проброшено host-volume в сервис `backup` (см. ниже).
- **Production**: рекомендуется `./backups/` смонтировать на сетевой
  storage (NFS / s3fs / restic remote) — локальный диск даст потерять
  бэкапы вместе с хостом при cascade-failure.

## Расписание (автомат)

Сервис `backup` в `docker-compose.yml` запускает `scripts/backup.sh`
каждые **сутки в 03:15 UTC**. Профиль `backup` — стартует только когда
просишь:

```bash
docker compose --profile backup up -d backup

# Логи:
docker compose logs -f backup
```

Один бэкап в сутки = RPO 24h. Если нужен меньший — крути cron-schedule
в command-секции, или переходи на streaming-репликацию (выходит за рамки
этого runbook).

## Ротация

- **Daily**: последние 7 дней.
- **Weekly**: каждое воскресенье отдельно маркируется `.weekly` файлом;
  такие дампы живут 4 недели (т.е. последние 4 воскресенья).

```
backups/
├── target-monitor-20260524-031500.sql.gz          (Sunday)
├── target-monitor-20260524-031500.sql.gz.weekly   ← marker
├── target-monitor-20260525-031500.sql.gz          (Monday)
└── target-monitor-20260525-031500.sql.gz.sha256
```

После 7 дней Monday-Saturday backups удаляются. Sunday-backup остаётся
ещё 3 недели.

Кастомизация:

```bash
RETENTION_DAILY=14 RETENTION_WEEKLY=8 scripts/backup.sh
```

## Manual backup (без cron)

```bash
make backup
# или напрямую:
DATABASE_URL=postgresql://... MONITOR_DB_URL=postgresql://... \
  scripts/backup.sh
```

## Восстановление

### Postgres (`target-*.sql.gz`)

1. **Подними чистую базу** на той же major-версии Postgres. НЕ
   восстанавливай поверх живой prod без явного `DROP DATABASE` —
   `psql` сольёт два дампа.
2. Прогон:

   ```bash
   RESTORE_URL=postgresql://postgres:dev@new-host:5432/monitor \
     scripts/restore.sh backups/target-monitor-20260530-031500.sql.gz
   ```

3. Проверка:

   ```bash
   psql "$RESTORE_URL" -c "SELECT count(*) FROM users;"
   ```

### SQLite (`metrics-monitor.sqlite.gz`)

1. **Останови app** (`docker compose stop app`) — иначе новые писатели
   создадут расхождение с восстановленным файлом.
2. Прогон:

   ```bash
   RESTORE_PATH=monitor.db.restored scripts/restore.sh \
     backups/metrics-monitor-20260530-031500.sqlite.gz
   ```

3. Замена + старт:

   ```bash
   mv monitor.db monitor.db.broken
   mv monitor.db.restored monitor.db
   docker compose start app
   curl localhost:5001/healthz | jq .checks.monitor_db
   ```

## SLO

| Метрика | Цель | Как меряем |
|---|---|---|
| **RPO** (Recovery Point Objective — сколько данных теряем) | 24 ч | один автоматический бэкап в сутки |
| **RTO** (Recovery Time Objective — сколько идёт восстановление) | < 5 мин | smoke-тест в CI на demo-датасете |

Если pg-база растёт > 1 ГБ и dump перестанет влезать в 5 минут — переходи на
`pg_dump --format=custom` + `pg_restore -j N` (быстрее) или на физический
бэкап через `pg_basebackup` + WAL-archiving (constant-time RPO).

## Проверки которые иногда нужно делать руками

- Quarterly: проверить что хотя бы один последний weekly-бэкап **реально**
  восстанавливается (не просто валиден по чексумме). Запустить
  `restore.sh` → `psql RESTORE_URL -c '\dt'` → ожидать список таблиц.
- После любого изменения схемы (миграции, ALTER, DROP): сразу прогнать
  ручной бэкап ДО релиза. Автоматический в 03:15 не успеет, если что-то
  пойдёт не так в течение дня.

## Ссылки

- [Rollback runbook](./rollback.md) — что делать когда релиз поломал прод
- `scripts/backup.sh` — реализация
- `scripts/restore.sh` — реализация
