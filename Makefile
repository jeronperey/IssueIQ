.PHONY: build bench-scaling chaos chaos-quick up down logs logs-ingestion logs-normalizer logs-indexer logs-query reset seed test replay-dlq

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

# Replay dead-lettered messages back to the topic they failed on (see tools/replay)
replay-dlq:
	go run ./tools/replay $(ARGS)

# Build service binaries into ./bin (used by the bench and chaos harnesses)
build:
	@mkdir -p bin && for s in ingestion normalizer indexer; do go build -o bin/$$s ./services/$$s; done

# Saturation throughput vs partitions/consumers (needs: docker compose up -d kafka opensearch)
bench-scaling:
	scripts/bench/scaling.sh

# Fault injection: kills, broker/OpenSearch restarts, malformed events, DLQ replay
chaos: build
	go run ./tools/chaos -runs 5

chaos-quick: build
	go run ./tools/chaos -runs 1 -events 10000
