IMAGE ?= db-monitoring
PORT  ?= 5001

.PHONY: build server server-down server-logs reset-db db-up db-down db-reset db-logs db-psql seed test

build:
	docker build -t $(IMAGE) .

server:
	PORT=$(PORT) docker compose up -d --build app
	@echo "App available at http://localhost:$(PORT)  (logs: make server-logs)"

server-down:
	docker compose stop app

server-logs:
	docker compose logs -f app

reset-db:
	docker compose run --rm --build app python -m scripts.reset_db

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
