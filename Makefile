IMAGE ?= db-monitoring
PORT  ?= 5001

.PHONY: build server reset-db reset-metrics warmup-ml db-up db-down db-reset db-logs db-psql seed test test-integration test-e2e lint lint-fix timescale-up timescale-down timescale-migrate live-demo iceberg-up iceberg-down smoke-iceberg

build:
	docker build -t $(IMAGE) .

server:
	PORT=$(PORT) docker compose up --build app

reset-db:
	docker compose run --rm --build app python -m scripts.reset_db

reset-metrics:
	docker compose run --rm --build app python -m scripts.seed_metrics_db --reset
	$(MAKE) warmup-ml

warmup-ml:
	docker compose run --rm --build app python -m scripts.warmup_ml

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

# Live demo pipeline (#75) — stream synthetic events into the target Postgres
# and run collector + ML on every tick so the dashboard updates in real time.
# Requires the target Postgres running (`make db-up`) and the app on :5001
# (`make server`).
live-demo:
	python -m scripts.live_demo $(ARGS)

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
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-minio 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "Waiting for Iceberg REST catalog..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' db-monitoring-iceberg-rest 2>/dev/null)" = "healthy" ]; do sleep 1; done
	@echo "MinIO:        http://localhost:9000  (user=minioadmin pass=minioadmin)"
	@echo "MinIO UI:     http://localhost:9001"
	@echo "Iceberg REST: http://localhost:8181"

iceberg-down:
	docker compose --profile iceberg down

smoke-iceberg: ## Run live smoke test against local Iceberg REST + MinIO (requires make iceberg-up)
	@curl -sf http://localhost:8181/v1/config >/dev/null 2>&1 || \
		(echo "Iceberg REST не запущен. Сначала выполни: make iceberg-up" && exit 1)
	python -m scripts.smoke_iceberg

# Ruff: linter + import sort + pyupgrade in one tool. Config in pyproject.toml.
# Runs locally via venv (fast, no docker round-trip). Same command runs in CI.
lint:
	ruff check .

lint-fix:
	ruff check . --fix
