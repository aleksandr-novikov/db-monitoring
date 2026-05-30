# Sprint 3 — Multi-tenant эпик #48: справка

## Резюме

За Sprint 3 продукт превратился из single-tenant утилиты («один `.env` — один пользователь — одна таблица метрик») в multi-tenant SaaS с регистрацией, изоляцией данных и шифрованием креденшелов at rest.

**Итог:** 8 из 8 тикетов закрыты, ~10 PR смержено в master, эпик #48 закрыт.

---

## Тикеты

### #49 — Users: модель, регистрация, логин

**Что было:** аутентификации нет. Любой посетитель видит дашборд и метрики.

**Что стало:**
- Таблица `users` (id, email, password_hash, created_at, last_login_at)
- Bcrypt-хеширование (work factor 12)
- Flask-Login для сессий, secure cookie
- Email нормализуется в lowercase (case-insensitive lookup)
- `/auth/register`, `/auth/login`, `/auth/logout`
- `next=` параметр валидируется против денилиста (защита от open redirect)
- `/dashboard` и `/admin/*` требуют login

**Технические детали:** `_NEXT_DENYLIST = ("/auth/logout", "/auth/login", "/auth/register")`, абсолютные URL и protocol-relative (`//host/`) отвергаются — только same-host paths.

PR: #96.

---

### #50 — Project model: CRUD «мои проекты»

**Что было:** не было концепции «проект» — все метрики глобальные.

**Что стало:**
- Таблица `projects` (id, user_id, slug, name, created_at)
- `/projects/` — список своих проектов
- `/projects/new`, `/projects/<slug>` — создание и просмотр
- При регистрации авто-создаётся Default-проект со slug `default`
- Slug-валидация: ASCII, lowercase, dashes (защита от хитрых URL)
- Ownership-проверка: чужой slug → 404 (не 403, чтобы не подтверждать существование)

PR: #98.

---

### #51 — DB connections + Fernet-шифрование DSN

**Что было:** DSN мониторируемой БД в `.env` plaintext, один на всё приложение.

**Что стало:**
- Таблица `connections` (id, project_id, name, dsn_encrypted, schema_name, interval_minutes, is_active)
- DSN шифруется через **Fernet** (AES-128-CBC + HMAC) перед сохранением
- `FERNET_KEY` в env; в dev генерируется и пишется в `.env.local` автоматически
- В UI плейнтекст-DSN никогда не показывается — только `mask_dsn(decrypted)` вида `postgres.proj:***@host`
- `/projects/<slug>/connections/` — CRUD-routes под ownership-чейном
- Двух-уровневый ownership: project принадлежит юзеру И connection принадлежит project

**Технические детали:** `app/crypto.py` — обёртка над `cryptography.fernet`. `InvalidToken` ловится явно в UI на случай ротации ключа.

PR: #112.

---

### #52 — Test-connection endpoint

**Что было:** юзер сохранил DSN → ждал 15 минут до первого тика коллектора → видел в логах ошибку. Никакой обратной связи в момент save.

**Что стало:**
- `POST /projects/<slug>/connections/<id>/test` — синхронный probe
- `probe_connection(dsn)`: пытается подключиться с `connect_timeout=5s`, `NullPool` (не оккупирует pool slot), выполняет `SELECT 1` + `SELECT current_database(), version()`
- Возвращает JSON одной формы: `{status, code, message, latency_ms}` — UI ветвится только на `status`
- Классификация ошибок через `_classify_error()`: `auth_failed`, `timeout`, `network`, `unsupported_dialect`, generic `error`
- Постгрес-only пока (другие диалекты возвращают `unsupported_dialect`)

**Side effect:** заодно укрепился log-scrubbing — `record.args` с exception раньше воскрешали DSN-пароль через `%s` форматирование; теперь `_scrub` рекурсивно ловит `BaseException`.

PR: #113.

---

### #53 — Tenant isolation: project_id во всех метриках

**Что было:** таблица `metrics` без привязки к проекту. Все метрики смешаны.

**Что стало:**
- Колонка `project_id` (NOT NULL) во всех метрических таблицах: `metrics`, `anomaly_scores`, `changepoints`, `drift_reports`, `column_distribution`
- Каждый запрос на чтение метрик идёт с `WHERE project_id = :current_project_id`
- `project_id="legacy"` зарезервирован для бэкап-сценариев и старых данных
- Все публичные функции `metrics_storage` принимают `project_id` как обязательный параметр

**Технические детали:** миграция выполнялась с `DEFAULT 'legacy'` чтобы существующие строки не сломались, потом отдельным шагом снимался DEFAULT и ставился NOT NULL.

PR: #114.

---

### #54 — Per-project APScheduler: job на коннекшн

**Что было:** один глобальный `collect_all_tables` job — собирает метрики по `DATABASE_URL` из `.env`.

**Что стало:**
- Для каждого активного connection регистрируется отдельная APScheduler job
- ID job: `collect:<project_id>:<connection_id>` — admin tooling и логи сразу видят тенант
- Интервал берётся из `connection.interval_minutes` (5..1440)
- Job options: `misfire_grace_time=60s`, `max_instances=1`, `coalesce=True`, `replace_existing=True`
- При создании / включении коннекта job регистрируется немедленно (в #140 добавлен ещё и `next_run_time=now` чтобы первый тик не ждал интервала)
- При удалении / выключении — `remove_job`
- При boot-time — `register_jobs_for_all_active_connections()` перерегистрирует всё

**Ключевая архитектурная находка:** `ContextVar` + контекст-менеджер `using_engine()`:

```python
with using_engine(per_project_engine):
    collect_for_connection(...)  # адаптер автоматически видит правильный engine
```

Это позволило **не трогать адаптерный слой** — адаптеры читают engine из ContextVar, не зная про мульти-тенант. Полный аналог — как `contextvars.ContextVar` в asyncio, thread-local на стероидах.

PR: #115.

---

### #55 — Onboarding flow + публичный landing

**Что было:** аноним попадал на `/auth/login`. Зарегистрированный юзер — на пустой `/dashboard`.

**Что стало:**
- Аноним на `/` видит **публичный landing** — 3 фичи, how-it-works, CTA Sign up / Log in
- После `POST /auth/register` — редирект на `/projects/default/connections/new` (онбординг-визард)
- Визард: левая колонка с подсказками DSN для Supabase / Postgres / MySQL / ClickHouse + советом про read-only пользователя; справа — форма
- После сохранения первого коннекта — авто-probe:
  - `status=ok` → flash success + редирект `/dashboard`
  - иначе → flash error c кодом + редирект `/connections`
- Empty-state на `/dashboard` без коннектов — CTA-карточка «Добавить подключение →» вместо пустой таблицы

**Side effect:** обнаружилась дыра в `dashboard.overview()` — при пустом проекте всё равно вызывался легаси `db.list_tables()` против глобального `DATABASE_URL`. И tenant-isolation leak (новый юзер видел схему admin-овской БД), и крэш при недоступном глобальном DSN. Зафикшено в #119.

PR: #116 (+ fix #119).

---

### #56 — Security hygiene

**Что было:** ни rate-limit, ни CSRF, ни маскирования логов.

**Что стало:**
- **Rate-limit** через Flask-Limiter: 5 попыток логина за 15 минут на IP
- **CSRF** через Flask-WTF на всех POST-формах (auth, connections, projects, settings, logout)
- **DSN-маскирование в логах** через `logging.setLogRecordFactory`:
  - `DSNFilter` рекурсивно обходит `record.args`, ловит pattern `user:password@host` и заменяет на `user:***@host`
  - Срабатывает на raw-strings, dict-args, и (важно!) на `BaseException` в args — раньше exception сквозь `%s` форматирование воскрешал plaintext
- **Werkzeug ProxyFix** включён — корректная работа за reverse-proxy (Hugging Face Spaces, nginx)
- **Secure cookies**: `SESSION_COOKIE_HTTPONLY`, `SESSION_COOKIE_SAMESITE=Lax`, `SESSION_COOKIE_SECURE` в production

PR: #97.

---

## Цифры

| Метрика | Sprint 3 start | Sprint 3 end |
|---|---|---|
| Unit-тесты | ~280 | **460+** |
| Поддерживаемые СУБД (адаптеры в коде) | 3 | 3 (Iceberg добавлен позже) |
| Полноценный end-to-end (UI → probe → collector → dashboard) | 1 (Postgres) | 1 (Postgres) |
| Регистрация и login | ❌ | ✅ |
| Tenant isolation | ❌ | ✅ полностью |
| Шифрование DSN at rest | ❌ | ✅ Fernet |
| Маскирование логов | ❌ | ✅ + защита от args-exception |
| Rate-limit / CSRF | ❌ | ✅ |
| Onboarding flow | ❌ | ✅ landing → wizard → auto-test → dashboard |

---

## Что осталось вне спринта (bonus)

- **#100** Health-check (`/healthz` → JSON со статусом monitor.db + APScheduler + ML)
- **#111 / #126** Iceberg adapter (REST + Glue каталоги, snapshot-based reads)
- **#119** Fix dashboard legacy при пустом проекте
- **#122 / #123** Кнопка «Выйти» в шапке (нашлось когда юзер спросил «а где выйти?»)
- **#127** Изоляция flaky health-теста от реальной БД
- **#137** `/dashboard` при `project=None` → онбординг, не legacy
- **#138** Демо-скрипты под multi-tenant (`--project-id` параметр)
- **#139** Классификатор: `supabase_tenant_not_found` отдельным кодом
- **#140** Первый сбор метрик при добавлении коннекта — сразу, не через 15 мин
- **#144** Запланирован: refactor seed-of-history через target-БД + collector backfill (правильная архитектура)

---

## Архитектурные решения

### ContextVar для tenant-context

Альтернативы которые отвергнуты:
- **DI-фреймворк** (FastAPI Depends-style) — переписывать весь адаптерный слой
- **Engine как параметр** — менять сигнатуру десятков функций в `app/db.py`
- **Thread-local** — не работает с APScheduler thread pool

`ContextVar` оказался компромиссом: коллектор оборачивает свой вызов в `with using_engine(per_project_engine):`, дальше всё работает без знания про тенант. Цена — питоновская магия которую нужно объяснять читателю кода.

### Fernet вместо bcrypt/argon2 для DSN

DSN нужно **расшифровывать обратно** (отдать коллектору), а не верифицировать (как пароль). Поэтому symmetric encryption (Fernet) — единственный правильный выбор. Bcrypt/argon2 — one-way, не подходят.

### `project_id="legacy"` как буфер миграции

Все pre-Sprint-3 данные получили `project_id='legacy'`. Старый глобальный коллектор и старые API-эндпоинты пишут в legacy. Постепенно legacy-данные мигрируют или удаляются — но не одним SQL-statement, чтобы не сломать что-нибудь незаметно.

---

## Что научились / что бы сделали иначе

1. **Tenant — это контекст, а не сущность.** Когда осознали — рефакторинг адаптеров не понадобился.
2. **Авто-test первого коннекта — кардинально лучший UX.** Без него юзер ждёт 15 мин до фидбека.
3. **Маскирование логов через `setLogRecordFactory` ловит больше путей** чем регулярки в форматтерах — но `record.args` с exception были слепой зоной. Нашли только через юнит-тест который специально логировал exception с DSN.
4. **Empty-state важнее чем full-state.** Дашборд без коннектов теперь полезен (CTA «Добавить подключение»), раньше был молчаливым пустым местом.
