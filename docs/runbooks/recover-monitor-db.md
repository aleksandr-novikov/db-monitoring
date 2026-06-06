# Recovery runbook: corrupted `monitor.db` (#213)

> **Когда читать**: `/dashboard/notifications` отдаёт 500, в логах
> `sqlite3.DatabaseError: database disk image is malformed`.
> Корневая причина устранена в #212 (Docker compose теперь использует
> Timescale), этот документ — как восстановить уже сломанный runtime.

## TL;DR

```bash
make recover-monitor-db                          # автомат: preserve + reseed + verify
# или вручную, если нужен контроль над шагами:
mv monitor.db monitor.db.broken.$(date +%s)     # сохранить файл для разбора
docker compose down                              # выключить app + базы
docker compose up -d --build                     # стартовать с #212 compose (Timescale)
make demo-prepare                                # перенести демо-данные в новый стор
curl -s http://localhost:5001/healthz | jq .checks.monitor_db
# {"status":"ok", "backend":"postgresql", "elapsed_ms": N}
```

## Что произошло

До #212 Docker compose маунтил host-овый `./monitor.db` внутрь app-контейнера и наследовал `MONITOR_DB_URL=sqlite:///monitor.db` из `.env`. Под scheduler write-load файл повредился (`PRAGMA integrity_check` показывает malformed pages / indexes). Любые reads (`/dashboard/notifications`, `/dashboard/history`, `/dashboard/schema`) которые трогают пострадавшие страницы — фейлятся 500.

## Шаги восстановления

### 1. Подтвердить что файл реально битый

```bash
sqlite3 monitor.db "PRAGMA integrity_check;" | head -5
# Если "ok" — файл цел, ищи проблему в другом месте (#100 health-check).
# Если "*** error *** malformed page ..." — corruption подтверждён.
```

### 2. Сохранить broken-копию

```bash
mv monitor.db "monitor.db.broken.$(date +%s)"
```

Не удалять. Может пригодиться для:
- расследования root cause (если #212 не должен был сработать)
- частичного recovery нужных строк через `.recover` команду SQLite

### 3. Switch на Timescale

Если на master с #212 — ничего делать не надо, compose уже использует Timescale:

```bash
docker compose down
docker compose up -d --build
# Дождаться app healthy
until [ "$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-app 2>/dev/null)" = "healthy" ]; do
  sleep 2
done
```

Если работаешь локально (не Docker) и хочешь использовать Timescale:

```bash
make timescale-up
# В .env установить:
#   MONITOR_DB_URL=postgresql://postgres:dev@localhost:5433/metrics
```

### 4. Reseed demo-данные

```bash
make demo-prepare
# Создаст users + projects + connections + 14-дневную историю метрик +
# ML warmup. В конце печатает таблицу со счётчиками — все должны быть > 0.
```

Если демо включает ClickHouse / Iceberg — отдельно:

```bash
make clickhouse-demo  # требует make clickhouse-up заранее
make iceberg-demo     # требует make iceberg-up заранее
```

### 5. Verify

```bash
# Backend на Postgres:
curl -s http://localhost:5001/healthz | jq -r '.checks.monitor_db.backend'
# → "postgresql"

# Strict health:
curl -sf "http://localhost:5001/healthz?strict=true" >/dev/null && echo "OK" || echo "still broken"

# Дашборды:
for path in /dashboard/notifications /dashboard/history /dashboard/schema; do
  printf '%s → %s\n' "$path" "$(curl -s -o /dev/null -w '%{http_code}' \
    -b cookies.txt -c cookies.txt http://localhost:5001$path)"
done
# Все три должны быть 200 (если залогинен) или 302 (на /auth/login).
# 500 — значит recovery не закончилось, ищи в логах.
```

## Что НЕ делать

- ❌ `rm monitor.db` напрямую — теряешь возможность post-mortem
- ❌ возвращать `MONITOR_DB_URL=sqlite:///` в Docker compose — #212/#214 явно
  запрещают, runtime опять упадёт под load
- ❌ восстанавливать broken SQLite через `.recover` команду в production
  runtime — exploring файла OK, но обратно использовать его не надо

## Что выходит за рамки этого runbook

- Перенос всей истории метрик из сломанного SQLite в Timescale —
  возможно частично через `.dump` + `psql`, но `monitor.db.broken`
  может оказаться слишком повреждённым. Принимаем потерю исторических
  rows как acceptable для demo recovery (per #213 spec).

## Ссылки

- [#212 PR — Docker compose использует Timescale по умолчанию](https://github.com/aleksandr-novikov/db-monitoring/pull/217)
- [#214 PR — Startup warning при SQLite metrics-store в Docker](https://github.com/aleksandr-novikov/db-monitoring/pull/218)
- [Backup runbook](./backup.md) — про бэкапы Timescale (на будущее)
- [Rollback runbook](./rollback.md) — если recovery не помогло, откатывайся на `:previous` Docker tag
