# Project Progress — Advanced Event-Driven Food Ordering Microservices

## 1. Overall Project Status
- **Status:** Core Services Complete & Verified (Order, Payment, Restaurant, Customer).
- **Architecture:** Microservices with FastAPI, SQLAlchemy 2.0 (Async), PostgreSQL (multi-schema), Apache Kafka (KRaft), Confluent Schema Registry (Avro), Transactional Outbox Pattern, Saga Orchestration (Payment & Approval with Compensations), Idempotent Consumers, and CDC readiness.
- **Test Suite Status:** 21/21 Unit Tests passing across all 4 services and common libraries.

---

## 2. Repository Structure
```
event-driven-microservices/
├── common/
│   ├── common_domain/                 # Base aggregates, entities, Money VO, IDs, events
│   ├── common_application/            # Shared exceptions and error DTOs
│   ├── common_dataaccess/             # Shared async SQLAlchemy engine & repository ports
│   └── infrastructure/
│       ├── kafka/                     # Avro wire serializer/deserializer, producer wrapper, schemas (.avsc)
│       ├── outbox/                    # Shared OutboxPublisher interface & exponential backoff
│       └── saga/                      # Saga step and status abstractions
├── infrastructure/
│   ├── docker-compose.yml             # PostgreSQL, Kafka (3 brokers KRaft), Schema Registry, Debezium, Kafka UI, 4 Services
│   └── postgres/
│       └── init.sql                   # Multi-schema initialization (order, payment, restaurant, customer)
├── services/
│   ├── order_service/ (port 8181)     # Saga orchestrator, Order aggregate, outbox publishers, Kafka consumers
│   ├── payment_service/ (port 8182)   # Credit balance ledger, debit/refund handlers, outbox publisher, consumer
│   ├── restaurant_service/ (port 8183)# Catalog validation, approval/rejection handler, outbox publisher, consumer
│   └── customer_service/ (port 8184)  # Customer registration, outbox publisher
├── scripts/
│   ├── register_schemas.py            # Registers Avro schemas with Schema Registry
│   ├── register_debezium_connector.py # Registers Debezium PostgreSQL connector with Kafka Connect
│   └── seed_data.py                   # Seeds initial restaurants, products, and customer credit ledger
├── tests/
│   └── unit/                          # Common infrastructure tests (Money VO, Avro loader, backoff)
├── Makefile                           # Development commands (up, migrate, test, seed, etc.)
└── PROJECT_PROGRESS.md                # Durable progress tracking file
```

---

## 3. Completed Features
1. **Core Shared Libraries (`common/`):**
   - Fixed `build-backend` in all `pyproject.toml` files to PEP-517 compliant `setuptools.build_meta`.
   - Updated `requires-python` to `>=3.10` to support local developer environment and Docker Python 3.12.
   - Built resilient `load_avro_schema` resolving schemas across local filesystem, installed packages, and Docker containers.
   - Added `authlib` for confluent-kafka schema registry client compatibility.
2. **Order Service (Port 8181):**
   - Domain aggregate, state machine transitions (`PENDING -> PAID -> APPROVED` and `PAID -> CANCELLING -> CANCELLED`).
   - Saga orchestration for payment and restaurant approval.
   - Idempotent consumers for `payment-response` and `restaurant-approval-response`.
   - Transactional Outbox for `payment-request` and `restaurant-approval-request`.
   - Alembic migration (`0001_order_initial.py`), Dockerfile, and FastAPI entry point.
3. **Payment Service (Port 8182):**
   - CreditEntry aggregate with `SELECT FOR UPDATE` concurrency protection against double-spending and overdrafts.
   - Append-only CreditHistory ledger with balance consistency validation.
   - Idempotent payment debit and refund/cancellation handlers.
   - Transactional Outbox publishing to `payment-response` topic.
   - Alembic migration (`0001_payment_schema.py`), Dockerfile, and FastAPI entry point.
4. **Restaurant Service (Port 8183):**
   - Full domain model (Restaurant, Product, OrderApproval) and validation service.
   - Catalog check verifying restaurant active status, product existence, availability, and unit price matches.
   - Consumer for `restaurant-approval-request` with manual offset commit only after local DB commit.
   - Transactional Outbox publishing to `restaurant-approval-response` topic.
   - Alembic migration (`0001_restaurant_schema.py`), Dockerfile, REST catalog endpoints, and FastAPI entry point.
5. **Customer Service (Port 8184):**
   - Customer aggregate and creation command handler.
   - Transactional Outbox publishing to `customer` topic.
   - Alembic migration (`0001_customer_schema.py`), Dockerfile, and endpoints (`POST /customers`, `GET /customers/{id}`, `/health/readiness`).
6. **Automation Scripts (`scripts/`):**
   - `register_schemas.py`: Registers 5 Avro schemas with Schema Registry.
   - `register_debezium_connector.py`: Configures Debezium PostgreSQL CDC connector with Kafka Connect.
   - `seed_data.py`: Idempotently seeds sample restaurants, products, and customer test credit.
7. **Automated Testing:**
   - 21 unit tests passing across all services and common modules.

---

## 4. Incomplete Features / Backlog
- End-to-end integration test runner against live Docker Compose containers (`tests/e2e`).
- Kubernetes manifests and Helm chart packaging (optional deployment phase).
- CQRS read model materialized view refreshers (PostgreSQL materialized view trigger / event consumer).

---

## 5. Known Bugs & Resolutions Applied
1. **Invalid build-backend in `pyproject.toml`:**
   - *Problem:* `setuptools.backends.legacy:build` caused pip installation to fail with `BackendUnavailable`.
   - *Resolution:* Fixed to standard `setuptools.build_meta` across all 9 `pyproject.toml` files.
2. **Schema file path fragility in Outbox Publishers:**
   - *Problem:* Hardcoded `parents[7]` failed outside specific deeply nested directory paths and caused crashes inside Docker.
   - *Resolution:* Implemented unified `load_avro_schema` with multi-strategy resolution (`AVRO_SCHEMAS_DIR`, package directory, upward parent crawl, Docker `/app` path).
3. **Import depth in consumer subpackages:**
   - *Problem:* Relative import `..application` from `app.infrastructure.kafka` resolved incorrectly to `app.infrastructure.application`.
   - *Resolution:* Corrected relative imports to `...application`.
4. **Missing dependencies in kafka infrastructure:**
   - *Problem:* Confluent-kafka SchemaRegistryClient required `authlib`.
   - *Resolution:* Added `authlib>=1.3.0` to dependencies.

---

## 6. Commands and Test Results
- **Full Test Suite Execution:**
  ```powershell
  python -m pytest services/order_service/tests services/payment_service/tests services/restaurant_service/tests services/customer_service/tests tests/unit -v
  ```
  *Result:* `21 passed, 1 warning in 0.63s`

- **Service Import & Port Verification:**
  - Order Service: `8181` OK
  - Payment Service: `8182` OK
  - Restaurant Service: `8183` OK
  - Customer Service: `8184` OK

---

## 7. Next Recommended Action
- Create integration / e2e tests (`tests/e2e/test_order_saga_e2e.py`) to simulate the full order flow from creation to approval and compensation refund.
