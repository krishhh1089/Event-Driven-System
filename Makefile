.PHONY: help up down build migrate-all test lint

COMPOSE=docker compose -f infrastructure/docker-compose.yml

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

up: ## Start all services (build images first)
	$(COMPOSE) up --build -d

down: ## Stop and remove containers (keep volumes)
	$(COMPOSE) down

down-v: ## Stop and remove containers + volumes
	$(COMPOSE) down -v

logs: ## Tail all service logs
	$(COMPOSE) logs -f

build: ## Build all service images
	$(COMPOSE) build

# -----------------------------------------------------------------------
# Migrations (run Alembic inside each service container)
# -----------------------------------------------------------------------
migrate-order: ## Run Order Service migrations
	docker exec food-order-service alembic upgrade head

migrate-payment: ## Run Payment Service migrations
	docker exec food-payment-service alembic upgrade head

migrate-restaurant: ## Run Restaurant Service migrations
	docker exec food-restaurant-service alembic upgrade head

migrate-customer: ## Run Customer Service migrations
	docker exec food-customer-service alembic upgrade head

migrate-all: migrate-order migrate-payment migrate-restaurant migrate-customer ## Run all migrations

# -----------------------------------------------------------------------
# Testing
# -----------------------------------------------------------------------
test-unit: ## Run unit tests (no external dependencies)
	cd services/order_service && python -m pytest tests/unit -v
	cd services/payment_service && python -m pytest tests/unit -v
	cd services/restaurant_service && python -m pytest tests/unit -v
	cd services/customer_service && python -m pytest tests/unit -v

test-integration: ## Run integration tests (requires running infrastructure)
	cd services/order_service && python -m pytest tests/integration -v
	cd services/payment_service && python -m pytest tests/integration -v
	cd services/restaurant_service && python -m pytest tests/integration -v
	cd services/customer_service && python -m pytest tests/integration -v

test-e2e: ## Run end-to-end tests
	python -m pytest tests/e2e -v

# -----------------------------------------------------------------------
# Schema registration
# -----------------------------------------------------------------------
register-schemas: ## Register all Avro schemas with Schema Registry
	python scripts/register_schemas.py

# -----------------------------------------------------------------------
# Debezium connector registration
# -----------------------------------------------------------------------
register-debezium: ## Register Debezium outbox connector
	python scripts/register_debezium_connector.py

# -----------------------------------------------------------------------
# Seed data
# -----------------------------------------------------------------------
seed: ## Insert sample restaurant and product data
	python scripts/seed_data.py
