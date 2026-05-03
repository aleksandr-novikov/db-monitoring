IMAGE ?= db-monitoring
PORT  ?= 5001

.PHONY: build server reset-db reset-metrics-db

build:
	docker build -t $(IMAGE) .

server: build
	docker run --rm -it --init -p $(PORT):$(PORT) \
		--env-file .env \
		-v $(CURDIR)/monitor.db:/app/monitor.db \
		-v $(CURDIR)/models:/app/models \
		$(IMAGE)

reset-db: build
	docker run --rm -it --init \
		--env-file .env \
		-v $(CURDIR)/monitor.db:/app/monitor.db \
		-v $(CURDIR)/models:/app/models \
		$(IMAGE) python -m scripts.reset_db

reset-metrics-db: build
	docker run --rm -it --init \
		--env-file .env \
		-v $(CURDIR)/monitor.db:/app/monitor.db \
		-v $(CURDIR)/models:/app/models \
		$(IMAGE) python -m scripts.reset_db --local-only
