COMPOSE := docker compose
GRAFANA_URL := http://localhost:3000/d/cart-pipeline/cart-event-pipeline
CONSUMER_WAIT_MAX := 36
CONSUMER_WAIT_SLEEP := 5

.PHONY: help demo up down producer wait-consumer logs ps clean

help:
	@echo "Targets:"
	@echo "  make demo           - start stack, wait for consumer, run 2-min scenario, print Grafana URL"
	@echo "  make up             - build and start all services in background"
	@echo "  make producer       - run scenario producer once (~2 minutes)"
	@echo "  make down           - stop containers (keep volumes)"
	@echo "  make clean          - stop containers and remove volumes"
	@echo "  make logs           - follow consumer logs"
	@echo "  make ps             - show service status"

demo: up wait-consumer
	@echo ""
	@echo "=========================================="
	@echo "  Grafana dashboard (open before/during):"
	@echo "  $(GRAFANA_URL)"
	@echo "  Login: admin / admin"
	@echo "  Time range: Last 15 minutes, refresh 10s"
	@echo "=========================================="
	@echo ""
	@echo "Running scenario producer (~2 minutes)..."
	$(COMPOSE) run --rm producer
	@echo ""
	@echo "Demo finished. Refresh Grafana: $(GRAFANA_URL)"
	@echo ""

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

producer:
	$(COMPOSE) run --rm producer

wait-consumer:
	@echo "Waiting for Faust consumer (metrics + Worker ready)..."
	@i=0; while [ $$i -lt $(CONSUMER_WAIT_MAX) ]; do \
		if curl -sf http://localhost:6066/metrics >/dev/null 2>&1 \
			&& $(COMPOSE) logs consumer 2>&1 | grep -q "Worker ready"; then \
			echo "Consumer is ready."; \
			exit 0; \
		fi; \
		i=$$((i + 1)); \
		printf "."; \
		sleep $(CONSUMER_WAIT_SLEEP); \
	done; \
	echo ""; \
	echo "Timeout: consumer did not become ready in $$(( $(CONSUMER_WAIT_MAX) * $(CONSUMER_WAIT_SLEEP) ))s"; \
	echo "Check: make logs"; \
	exit 1

logs:
	$(COMPOSE) logs -f consumer

ps:
	$(COMPOSE) ps
