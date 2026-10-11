# Food Ordering System — Advanced Event-Driven Microservices

A production-grade Python implementation of an advanced event-driven microservices architecture using FastAPI, PostgreSQL, Apache Kafka (KRaft), Confluent Schema Registry (Avro), Transactional Outbox, and Saga orchestration with compensating transactions.

## Architecture Overview

```
                                  +---------------------------+
                                  |     Customer Service      |
                                  |         Port 8184         |
                                  +-------------+-------------+
                                                | customer-created
                                                v
+--------------------------+       +---------------------------+       +-----------------------------+
|      Order Service       |<----->|       Apache Kafka        |<----->|       Payment Service       |
|   (Saga Orchestrator)    |       | (KRaft: 19092/29092/39092)|       |          Port 8182          |
|        Port 8181         |       +-------------+-------------+       +-----------------------------+
+--------------------------+                     ^
             ^                                   |
             |           restaurant-approval     |
             +-----------------------------------+
                                 |
                                 v
                   +---------------------------+
                   |    Restaurant Service     |
                   |         Port 8183         |
                   +---------------------------+
```

### Microservices

| Service | Port | Schema | Description |
| :--- | :--- | :--- | :--- |
| **Order Service** | `8181` | `order` | Saga orchestrator, order lifecycle management, transactional outbox |
| **Payment Service** | `8182` | `payment` | Customer credit ledger, debit and refund processing, overdraft protection |
| **Restaurant Service** | `8183` | `restaurant` | Restaurant & product catalog, order validation and approval |
| **Customer Service** | `8184` | `customer` | Customer registration and event publication |

### Kafka Topics & Avro Schemas

| Topic | Avro Schema | Producer | Consumer |
| :--- | :--- | :--- | :--- |
| `customer` | `customer.avsc` | Customer Service | Order Service |
| `payment-request` | `payment_request.avsc` | Order Service | Payment Service |
| `payment-response` | `payment_response.avsc` | Payment Service | Order Service (Saga) |
| `restaurant-approval-request` | `restaurant_approval_request.avsc` | Order Service | Restaurant Service |
| `restaurant-approval-response` | `restaurant_approval_response.avsc` | Restaurant Service | Order Service (Saga) |

---

## Core Distributed Systems Patterns

1. **Transactional Outbox:**
   - Business data and outbox events commit in the same local database transaction.
   - Outbox records are claimed with `SELECT ... FOR UPDATE SKIP LOCKED` and published to Kafka.
   - Acknowledged by broker before marking `COMPLETED`.
   - Dual-mode support: Polling publisher (default) or Debezium CDC ingestion.

2. **Saga Orchestration & Compensating Transactions:**
   - Forward flow: `Order Created (PENDING)` -> `Payment COMPLETED` -> `Order PAID` -> `Restaurant APPROVED` -> `Order APPROVED`.
   - Compensating flow: If restaurant rejects (`REJECTED`) after payment succeeds, the order transitions to `CANCELLING`, a refund request is published to `payment-request`, the balance is credited, and upon refund acknowledgment the order transitions to `CANCELLED`.

3. **Concurrency & Idempotency:**
   - Account balances protected by `SELECT ... FOR UPDATE` against concurrent double-spending.
   - Duplicate messages handled gracefully via database unique constraints (`order_id` in approvals/payments) and idempotency checks.
   - Kafka offset commit occurs only after successful local database transaction commit.

---

## Getting Started

### Prerequisites
- Python 3.10+
- Docker & Docker Compose

### Running with Docker Compose
```bash
# Start all infrastructure and microservices
docker compose -f infrastructure/docker-compose.yml up --build -d

# Register Avro schemas with Schema Registry
python scripts/register_schemas.py

# Seed initial restaurants, products, and customer test credit
python scripts/seed_data.py
```

### Running Tests Locally
```bash
python -m pytest services/order_service/tests services/payment_service/tests services/restaurant_service/tests services/customer_service/tests tests/unit -v
```
