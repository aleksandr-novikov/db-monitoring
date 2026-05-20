IMAGE ?= db-monitoring
PORT  ?= 5001

.PHONY: build server reset-db reset-metrics warmup-ml db-up db-down db-reset db-logs db-psql seed test test-integration lint lint-fix timescale-up timescale-down timescale-migrate

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

# Ruff: linter + import sort + pyupgrade in one tool. Config in pyproject.toml.
# Runs locally via venv (fast, no docker round-trip). Same command runs in CI.
lint:
	ruff check .

lint-fix:
	ruff check . --fix
