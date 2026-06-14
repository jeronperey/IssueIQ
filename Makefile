.PHONY: up down logs logs-ingestion logs-normalizer logs-indexer logs-query reset seed test replay-dlq

# Start all services in detached mode
up:
	docker compose up -d

# Stop all services
down:
	docker compose down

# Tail logs for all services
logs:
	docker compose logs -f

# Tail logs for a specific service
logs-ingestion:
	docker compose logs -f ingestion

logs-normalizer:
	docker compose logs -f normalizer

logs-indexer:
	docker compose logs -f indexer

logs-query:
	docker compose logs -f query

# Full reset: destroy volumes and restart from scratch
reset:
	docker compose down -v
	docker compose up -d

# Seed mock issues via the ingestion HTTP API (requires services to be running)
seed:
	@bash scripts/seed/seed.sh

# Run unit tests (excludes scripts/kafka-learn which has two main() declarations)
test:
	go test ./services/... ./shared/...

# Replay messages from the dead-letter queue
replay-dlq:
	@echo "replay-dlq not yet implemented"
