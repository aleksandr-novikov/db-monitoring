# Pre-demo checklist (#109)

> **Когда читать:** T−10 минут до выступления.
> **Цель:** уверенно сказать «можно показывать» через 10 минут.
> **Если что-то красное:** см. соответствующий runbook
> ([rollback](./runbooks/rollback.md), [backup](./runbooks/backup.md)).

Каждый пункт — одна команда. Не задумывайся, копируй.

---

## 1 · CI/CD pipeline зелёный

```bash
gh pr list --state open --json number,title,statusCheckRollup \
  --jq '.[] | "#\(.number) \(.title)"'
# Открытые PR не должны быть `red`. Если красный — НЕ мержить и НЕ деплоить.

gh run list --branch master --limit 3
# Последние 3 запуска на master — все ✓.
```

✅ Если последний run на master зелёный.

---

## 2 · .env заполнен, токены не истекли

```bash
test -f .env && grep -vE '^(#|$)' .env | sort | awk -F= '{print $1}'
# Должно содержать как минимум:
#   DATABASE_URL  SECRET_KEY  FERNET_KEY  APP_BASE_URL
# Опционально (если демо включает соответствующие фичи):
#   SENTRY_DSN  SMTP_HOST  TELEGRAM_BOT_TOKEN
```

✅ Все обязательные переменные на месте.

```bash
# Sentry DSN валиден (если используется):
python -c "from app.sentry import init_sentry; print('sentry:', init_sentry())"
# True → DSN формат правильный + SDK поднялся.
```

---

## 3 · Запуск с нуля

> Этот шаг **уничтожает** локальные volumes. Не запускай если в БД что-то нужное.

**Port-conflict precheck** (важно на macOS / любом ноуте с локальным
Postgres): docker compose маппит `5432:5432`. Если на хосте уже слушает
свой postgres (например, `brew install postgresql`), он перебивает Docker
по `localhost:5432` и app получает database "monitor" does not exist.

```bash
lsof -ti :5432 | xargs -r ps -p | head
# Если видишь СВОЙ postgres (не db-monitoring-pg) — останови:
#   brew services stop postgresql@<version>
# Или временно: pg_ctl -D /usr/local/var/postgres stop
```

Запуск:

```bash
docker compose down -v
docker compose up -d --build
# Подожди пока app healthy (15-20 сек):
until [ "$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-app 2>/dev/null)" = "healthy" ]; do
  sleep 2; echo -n "."
done
echo " ✓ app healthy"
```

✅ Контейнер `db-monitoring-app` в state `healthy`. Если получаешь
`database "monitor" does not exist` — проверь port-conflict выше.

---

## 4 · Health-check (strict)

```bash
curl -sf "http://localhost:5001/healthz?strict=true" | jq .
```

✅ HTTP 200 + `status: ok`. Все зависимости должны быть `ok` или `n/a`,
ни одной `down`.

`?strict=true` важен — без него `n/a` (нет подключения к target DB) не
триггерит 503.

---

## 4b · Prometheus `/metrics` endpoint

```bash
curl -sf http://localhost:5001/metrics | grep -E "^# TYPE (http_requests_total|collector_runs_total|failed_login_attempts_total) " | head
```

✅ Выводит три TYPE-строки. Если grep пуст — endpoint не отвечает или
prometheus-client не установлен в образе.

Опционально: `curl -s /metrics | grep "^http_requests_total"` — должен
расти после нескольких curl-ов (smoke что hook реально срабатывает).

---

## 5 · Critical user flow

В новой incognito-вкладке:

1. Открой http://localhost:5001 — должна загрузиться landing-страница
2. → Sign up — заполни email + пароль ≥ 8 символов
3. → автоматически на `/projects/default/connections/new` (онбординг-визард)
4. Вставь DSN (см. шаг 6) → **Сохранить и проверить**
5. ✅ Auto-test показал `status=ok` → редирект на `/dashboard`
6. → меню «Проекты» → видна одна карточка `Default`
7. → меню «Уведомления» — открывается форма

✅ Все 7 шагов прошли без 500 / 404 / спиннеров-в-вечность.

---

## 6 · Тестовые данные

```bash
# Один проход — seed_demo_workspace + 14-дневная история + ML warmup (#176):
make demo-prepare
# Под капотом: создаёт demo@dbmonitor.app + retail-postgres проект,
# заливает 14 дней метрик, тренирует Prophet / IsolationForest / PELT / drift.
# В конце печатает COUNT(*) по metrics / anomaly_scores / changepoints /
# drift_reports / notifications / forecast_models — все должны быть > 0.

# Опционально — target Postgres (если демо включает реальный сбор):
make seed
make seed-clickhouse                # если демо включает CH (после `make clickhouse-up`)

# Проверка что demo-пользователь существует:
make demo-ids
# Должно вывести PROJECT_ID + CONNECTION_ID demo-аккаунта.
```

✅ `demo-ids` отдаёт реальные UUID, не пусто. Финальная таблица
`make demo-prepare` показывает все ненулевые счётчики.

---

## 6b · Telegram alerts работают

Если демо включает «вот пришёл алерт прямо в Telegram»:

1. Открой `/projects/<slug>/settings/notifications`
2. Bot Token + Chat ID должны быть сохранены (видно «Сохранён токен `1234567890:•••`»)
3. Нажми **Тест** — `success` flash → за 2-3 секунды в Telegram пришло
   «Тестовое сообщение из DB Monitor для проекта «...».»
4. Если есть `make telegram-demo` — прогони его, должно прилететь
   несколько реальных alert-ов (anomaly + changepoint + schema_drift)

✅ Тестовое сообщение видно в Telegram, бот не silent. Если ничего —
проверь `chat_id` (для группы должен быть отрицательный с `-100`).

---

## 7 · Логи чистые

```bash
# ERROR/CRITICAL за последние 5 минут — НЕ должно быть.
docker compose logs --since 5m app | grep -iE 'ERROR|CRITICAL' | head
# Пустой вывод = чисто.
```

✅ Никаких ERROR / CRITICAL.

> Если есть WARNING — глянь и реши, не покажет ли это себя на демо.

---

## 8 · Plan B готов

```bash
# Скринкасты под рукой (#108):
ls docs/demo/*.mp4 2>/dev/null && echo "✓ screencasts present"

# demo-stable тэг существует в GHCR:
gh api /user/packages/container/db-monitoring/versions --jq '.[].metadata.container.tags[]' 2>/dev/null \
  | grep -F demo-stable && echo "✓ demo-stable in registry"
```

✅ Если live-демо упадёт — есть скринкаст и стабильный образ для отката.

---

## 9 · Ресурсы

```bash
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
# Ни одно — не 100% CPU и не 100% MEM (с запасом 20%).
```

✅ Все CPU < 80%, MEM с запасом 20%.

> Если близко к лимиту — на демо может выстрелить под нагрузкой.

---

## 10 · Роли распределены

| Роль | Кто | Что делает во время демо |
|---|---|---|
| **Backend** | _____ | Следит за `docker compose logs -f app` |
| **Frontend** | _____ | Кликает по UI, реагирует на freeze |
| **Q&A** | _____ | Отвечает на вопросы из аудитории |
| **Demo-данные** | _____ | Запускает `make live-demo` / `--incident-at` |
| **Plan B** | _____ | Готов запустить скринкаст если live упало |

✅ Каждая роль — отдельный человек. Если меньше людей — Backend
совмещает с Q&A, Frontend с Demo-данные.

---

## Зелёный свет

Все 10 пунктов ✅ → **«Можно показывать.»**

Если хотя бы один красный — НЕ показываем live. Включаем Plan B
(скринкасты или demo-stable образ).

## Ссылки

- [Rollback runbook](./runbooks/rollback.md) — если что-то всё-таки упало
- [Backup runbook](./runbooks/backup.md) — перед демо хорошо иметь свежий бэкап
- [Релизы и Docker tags](../README.md#релизы-и-откат-docker-tags) — что такое `demo-stable`
- `/admin/feature-flags` — что под флагами (требует login; можно быстро выключить если фича ломает демо)
- `/admin/rollback-checklist` — компактная версия rollback runbook на странице (тоже под login)
