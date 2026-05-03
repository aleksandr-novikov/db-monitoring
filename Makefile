IMAGE ?= db-monitoring
PORT  ?= 5001

.PHONY: build server

build:
	docker build -t $(IMAGE) .

server: build
	docker run --rm -it --init -p $(PORT):$(PORT) \
		--env-file .env \
		-v $(CURDIR)/monitor.db:/app/monitor.db \
		-v $(CURDIR)/models:/app/models \
		$(IMAGE)
