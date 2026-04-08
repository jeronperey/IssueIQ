.PHONY: up down logs reset seed replay-dlq

# Start all services in detached mode
up:
	docker compose up -d

# Stop all services
down:
	docker compose down

# Tail logs for all services
logs:
	docker compose logs -f

# Full reset: destroy volumes and restart from scratch
reset:
	docker compose down -v
	docker compose up -d

# Seed the database with initial data
seed:
	@echo "seed not yet implemented"

# Replay messages from the dead-letter queue
replay-dlq:
	@echo "replay-dlq not yet implemented"