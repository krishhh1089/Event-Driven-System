"""
Payment Service — Application Entry Point
==========================================
Starts the FastAPI application, registers background tasks, and wires DI.

Background tasks:
1. Payment request consumer (Kafka) — processes PENDING and CANCELLED requests.
2. Payment response outbox publisher — polls DB and publishes to payment-response.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from common_application import ApplicationException
from common_domain import DomainException

from .api.router import router as payment_router
from .api.router import _get_session
from .infrastructure.database.models import Base
from .infrastructure.kafka.payment_request_consumer import PaymentRequestConsumer
from .infrastructure.outbox.publisher import PaymentResponseOutboxPublisher

from kafka_infrastructure import AvroHelper, KafkaProducerWrapper, build_schema_registry_client
from common_dataaccess import build_async_engine, build_session_factory

# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# Configuration (environment variables)
# -------------------------------------------------------------------------
DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql+psycopg://food_user:food_pass@localhost:5432/food_ordering",
)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
PORT = int(os.environ.get("PORT", 8182))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info").lower()
CONSUMER_GROUP = os.environ.get("PAYMENT_CONSUMER_GROUP", "payment-service-group")
OUTBOX_POLL_INTERVAL = float(os.environ.get("OUTBOX_POLL_INTERVAL_SECONDS", "5"))

# -------------------------------------------------------------------------
# Database engine + session factory
# -------------------------------------------------------------------------
engine = build_async_engine(DATABASE_URL)
SessionFactory = build_session_factory(engine)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session


# -------------------------------------------------------------------------
# Kafka clients
# -------------------------------------------------------------------------
schema_registry_client = build_schema_registry_client(SCHEMA_REGISTRY_URL)
avro_helper = AvroHelper(schema_registry_client)
kafka_producer = KafkaProducerWrapper(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",
        "retries": 5,
        "enable.idempotence": True,
    }
)

consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # manual commit after DB transaction
    "max.poll.interval.ms": 300000,
}

# -------------------------------------------------------------------------
# Background components
# -------------------------------------------------------------------------
payment_request_consumer = PaymentRequestConsumer(
    consumer_config=consumer_config,
    schema_registry_url=SCHEMA_REGISTRY_URL,
    session_factory=SessionFactory,
    topic="payment-request",
)

payment_response_publisher = PaymentResponseOutboxPublisher(
    session_factory=SessionFactory,
    producer=kafka_producer,
    avro=avro_helper,
    topic="payment-response",
    poll_interval_s=OUTBOX_POLL_INTERVAL,
)


# -------------------------------------------------------------------------
# Lifespan
# -------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Payment Service starting up...")

    # Auto-create tables in dev (Alembic handles prod migrations)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    tasks = [
        asyncio.create_task(
            payment_request_consumer.start(), name="payment-request-consumer"
        ),
        asyncio.create_task(
            payment_response_publisher.run(), name="payment-response-publisher"
        ),
    ]
    logger.info("Payment Service started. Background tasks: %d", len(tasks))
    yield

    logger.info("Payment Service shutting down...")
    await payment_request_consumer.stop()
    await payment_response_publisher.stop()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    kafka_producer.close()
    await engine.dispose()
    logger.info("Payment Service shutdown complete")


# -------------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------------
app = FastAPI(
    title="Payment Service",
    description="Food ordering system — payment processing and credit management",
    version="1.0.0",
    lifespan=lifespan,
)

# Override DI for session
app.dependency_overrides[_get_session] = get_session

# Routers
app.include_router(payment_router)


# -------------------------------------------------------------------------
# Exception handlers
# -------------------------------------------------------------------------
@app.exception_handler(ApplicationException)
async def application_exception_handler(request: Request, exc: ApplicationException):
    return JSONResponse(
        status_code=400,
        content={"code": "APPLICATION_ERROR", "message": exc.message},
    )


@app.exception_handler(DomainException)
async def domain_exception_handler(request: Request, exc: DomainException):
    return JSONResponse(
        status_code=400, content={"code": "DOMAIN_ERROR", "message": exc.message}
    )


# -------------------------------------------------------------------------
# Health endpoints
# -------------------------------------------------------------------------
@app.get("/health", tags=["health"], summary="Liveness check")
async def health():
    return {"status": "UP", "service": "payment-service"}


@app.get("/health/readiness", tags=["health"], summary="Readiness check")
async def readiness():
    checks = {}
    try:
        async with SessionFactory() as session:
            await session.execute(__import__("sqlalchemy").text("SELECT 1"))
        checks["database"] = "UP"
    except Exception as exc:
        checks["database"] = f"DOWN: {exc}"

    all_up = all(v == "UP" for v in checks.values())
    return JSONResponse(
        status_code=200 if all_up else 503,
        content={"status": "UP" if all_up else "DOWN", "checks": checks},
    )


# -------------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------------
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=PORT,
        log_level=LOG_LEVEL,
        reload=False,
    )
