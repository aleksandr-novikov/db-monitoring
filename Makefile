IMAGE         ?= db-monitoring
PORT          ?= 5001
PROJECT_ID    ?= legacy
CONNECTION_ID ?=
DEMO_PROJECT_SLUG ?= retail-postgres

.PHONY: build server reset-db reset-metrics warmup-ml db-up db-down db-reset db-logs db-psql seed test test-integration test-e2e lint lint-fix timescale-up timescale-down timescale-migrate live-demo telegram-demo iceberg-up iceberg-down smoke-iceberg iceberg-demo demo-ids clickhouse-up clickhouse-down seed-clickhouse clickhouse-demo backup restore backup-cron-up backup-cron-down demo-prepare

build:
	docker build -t $(IMAGE) .

server:
	PORT=$(PORT) docker compose up --build app

reset-db:
	docker compose run --rm --build app python -m scripts.reset_db

reset-metrics:
	docker compose run --rm --build app python -m scripts.seed_metrics_db \
		--reset --project-id $(PROJECT_ID)
	$(MAKE) warmup-ml

warmup-ml:
	docker compose run --rm --build app python -m scripts.warmup_ml \
		--project-id $(PROJECT_ID)

# ── Локальный Postgres для разработки ────────────────────────────────
db-up:
	docker compose up -d postgres
	@echo "Waiting for Postgres to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-pg 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "Postgres ready on localhost:5432 (db=monitor user=postgres pass=dev)"

db-down:
	docker compose down

db-reset:
	docker compose down -v
	$(MAKE) db-up

db-logs:
	docker compose logs -f postgres

db-psql:
	docker compose exec postgres psql -U postgres -d monitor

seed:
	docker compose run --rm --build app python -m scripts.seed_target_db --reset

test:
	docker compose run --rm --no-deps --build \
		-v $(CURDIR)/tests:/app/tests \
		app pytest $(ARGS)

# Integration tests (#44) — real Postgres / MySQL / ClickHouse / TimescaleDB
# via testcontainers. Requires a running Docker daemon on the host. Run
# locally; CI invokes the same command on push to master only.
test-integration:
	pytest -m integration -v $(ARGS)

# Live demo pipeline (#75/#138) — stream synthetic events into the target Postgres
# and run collector + ML on every tick so the dashboard updates in real time.
# Requires the target Postgres running (`make db-up`) and the app on :5001
# (`make server`).
# For project-scoped demo: make live-demo PROJECT_ID=<id> CONNECTION_ID=<id>
# Get IDs via: make demo-ids
live-demo:
	python -m scripts.live_demo \
		--project-id $(PROJECT_ID) \
		$(if $(CONNECTION_ID),--connection-id $(CONNECTION_ID),) \
		$(ARGS)

telegram-demo: ## Demo 3.2 Telegram path (#182): make telegram-demo ARGS=all
	python -m scripts.telegram_demo $(ARGS)

# Demo 3.2 — backfill 14-day history + ML warmup для demo-проектов (#176).
# Один проход: seed_demo_workspace → seed_metrics_db → warmup_ml → verify.
# По умолчанию готовит retail-postgres; --slug для других.
demo-prepare:
	python -m scripts.demo_prepare $(ARGS)

# Print PROJECT_ID and CONNECTION_ID for the demo account (demo@dbmonitor.app).
demo-ids:
	docker compose run --rm app python -c "\
from app.metrics_storage import get_user_by_email, list_projects_for_user, list_connections_for_project; \
user = get_user_by_email('demo@dbmonitor.app'); \
slug = '$(DEMO_PROJECT_SLUG)'; \
projects = list_projects_for_user(user['id']) if user else []; \
project = next((p for p in projects if p['slug'] == slug), None); \
assert project is not None, 'demo user or project not found: ' + slug; \
conns = list_connections_for_project(project['id']); \
assert conns, 'connection not found for project: ' + slug; \
print(f'PROJECT_ID={project[\"id\"]}'); \
print(f'CONNECTION_ID={conns[0][\"id\"]}') \
"

# E2E dashboard tests (#45) — Playwright + headless Chromium against the live
# Flask app. One-time setup: `playwright install chromium`.
test-e2e:
	pytest -m e2e -v $(ARGS)

# ── TimescaleDB metrics store (#40) ──────────────────────────────────
timescale-up:
	docker compose --profile timescale up -d timescaledb
	@echo "Waiting for TimescaleDB to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-timescale 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "TimescaleDB ready on localhost:5433 (db=metrics user=postgres pass=dev)"

timescale-down:
	docker compose --profile timescale down

timescale-migrate:
	python -m scripts.migrate_metrics_to_timescale \
		--source sqlite:///monitor.db \
		--target postgresql://postgres:dev@localhost:5433/metrics $(ARGS)

# ── Apache Iceberg smoke test (#128) ─────────────────────────────────
iceberg-up:
	docker compose --profile iceberg up -d minio iceberg-rest
	@echo "Waiting for MinIO to become healthy..."
	@i=0; until [ "$$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-minio 2>/dev/null)" = "healthy" ]; do \
		i=$$((i+1)); [ $$i -gt 60 ] && echo "ERROR: MinIO did not become healthy in 60s" && exit 1; sleep 1; done
	@echo "Waiting for Iceberg REST catalog..."
	@i=0; until curl -sf http://localhost:8181/v1/config >/dev/null 2>&1; do \
		i=$$((i+1)); [ $$i -gt 60 ] && echo "ERROR: Iceberg REST did not become ready in 60s" && exit 1; sleep 1; done
	@echo "MinIO:        http://localhost:9000  (user=minioadmin pass=minioadmin)"
	@echo "MinIO UI:     http://localhost:9001"
	@echo "Iceberg REST: http://localhost:8181"

iceberg-down:
	docker compose --profile iceberg stop iceberg-rest minio
	docker compose --profile iceberg rm -f iceberg-rest minio

smoke-iceberg: ## Run live smoke test against local Iceberg REST + MinIO (requires make iceberg-up)
	@curl -sf http://localhost:8181/v1/config >/dev/null 2>&1 || \
		(echo "Iceberg REST не запущен. Сначала выполни: make iceberg-up" && exit 1)
	python -m scripts.smoke_iceberg

iceberg-demo: ## Prepare full Iceberg Lakehouse demo path (#178)
	@curl -sf http://localhost:8181/v1/config >/dev/null 2>&1 || \
		(echo "Iceberg REST не запущен. Сначала выполни: make iceberg-up" && exit 1)
	@set -e; \
		echo "Stopping app scheduler while Iceberg demo history is prepared..."; \
		docker compose stop app >/dev/null; \
		trap 'echo "Starting app with refreshed scheduler..."; docker compose up -d --build app >/dev/null' EXIT; \
		python -m scripts.prepare_iceberg_demo; \
		echo "Iceberg demo is ready: http://localhost:5001/dashboard/"

# ── ClickHouse demo target (#141) ────────────────────────────────────
clickhouse-up:
	docker compose --profile clickhouse up -d clickhouse
	@echo "Waiting for ClickHouse to become healthy..."
	@i=0; until [ "$$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-clickhouse 2>/dev/null)" = "healthy" ]; do \
		i=$$((i+1)); [ $$i -gt 60 ] && echo "ERROR: ClickHouse did not become healthy in 60s" && exit 1; sleep 1; done
	@echo "ClickHouse HTTP:   http://localhost:8123  (user=default db=demo, no password)"
	@echo "ClickHouse native: localhost:19000        (DSN: clickhouse+native://default@localhost:19000/demo)"

clickhouse-down:
	docker compose --profile clickhouse down

seed-clickhouse: ## Заполнить ClickHouse-демо тестовыми данными
	@curl -sf http://localhost:8123/ping >/dev/null 2>&1 || \
		(echo "ClickHouse не запущен. Сначала выполни: make clickhouse-up" && exit 1)
	python -m scripts.seed_clickhouse $(ARGS)

clickhouse-demo: ## Prepare full ClickHouse demo path (#177): workspace + seed + history + warmup
	@curl -sf http://localhost:8123/ping >/dev/null 2>&1 || \
		(echo "ClickHouse не запущен. Сначала выполни: make clickhouse-up" && exit 1)
	@set -e; \
		echo "Seeding ClickHouse live data..."; \
		$(MAKE) seed-clickhouse; \
		echo "Stopping app scheduler while ClickHouse demo history is prepared..."; \
		docker compose stop app 2>/dev/null || true; \
		trap 'echo "Starting app with refreshed scheduler..."; docker compose up -d --build app 2>/dev/null || true' EXIT; \
		python -m scripts.prepare_clickhouse_demo $(ARGS); \
		echo "ClickHouse demo is ready: http://localhost:5001/dashboard/"

# ── Backup / restore (#106) ──────────────────────────────────────────
backup: ## Снять бэкап target + monitor DB в ./backups
	@scripts/backup.sh

restore: ## Восстановить из бэкапа: make restore FILE=backups/...sql.gz [RESTORE_URL=...]
	@test -n "$(FILE)" || (echo "Usage: make restore FILE=<path> [RESTORE_URL=postgresql://...]" && exit 1)
	@scripts/restore.sh $(FILE)

backup-cron-up: ## Запустить фоновый backup-сервис (cron в Docker)
	docker compose --profile backup up -d backup

backup-cron-down: ## Остановить фоновый backup-сервис
	docker compose --profile backup stop backup

# Ruff: linter + import sort + pyupgrade in one tool. Config in pyproject.toml.
# Runs locally via venv (fast, no docker round-trip). Same command runs in CI.
lint:
	ruff check .

lint-fix:
	ruff check . --fix
