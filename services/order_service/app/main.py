"""
Order Service — Application Entry Point
=========================================
Starts the FastAPI application, registers background tasks, and wires DI.

Background tasks:
1. Payment outbox publisher (polls order.payment_outbox every N seconds)
2. Restaurant approval outbox publisher
3. Payment response consumer (Kafka)
4. Restaurant approval response consumer (Kafka)
5. Customer event consumer (Kafka)
"""
from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from common_application import (
    ApplicationException,
    CustomerNotFoundException,
    OrderNotFoundException,
)
from common_domain import DomainException

from .config import get_settings
from .api.router import router as order_router
from .api.router import _get_session  # we'll override via DI
from .infrastructure.database.models import Base
from .infrastructure.kafka.payment_response_consumer import PaymentResponseConsumer
from .infrastructure.kafka.approval_response_consumer import RestaurantApprovalResponseConsumer
from .infrastructure.kafka.customer_event_consumer import CustomerEventConsumer
from .infrastructure.outbox.publishers import (
    PaymentOutboxPublisher,
    RestaurantApprovalOutboxPublisher,
)
from .infrastructure.saga_service import SagaApplicationService
from .domain.order_domain_service import OrderDomainService

from kafka_infrastructure import AvroHelper, KafkaProducerWrapper, build_schema_registry_client
from common_dataaccess import build_async_engine, build_session_factory

# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s [%(correlation_id)s] %(message)s"
    if False  # structured JSON logging via structlog recommended in prod
    else "%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

settings = get_settings()

# -------------------------------------------------------------------------
# Database engine + session factory
# -------------------------------------------------------------------------
engine = build_async_engine(settings.database_url)
SessionFactory = build_session_factory(engine)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionFactory() as session:
        yield session


# -------------------------------------------------------------------------
# Kafka clients
# -------------------------------------------------------------------------
schema_registry_client = build_schema_registry_client(settings.schema_registry_url)
avro_helper = AvroHelper(schema_registry_client)
kafka_producer = KafkaProducerWrapper(settings.kafka_producer_config)


def _saga_service_factory(session: AsyncSession) -> SagaApplicationService:
    return SagaApplicationService(session, OrderDomainService())


# -------------------------------------------------------------------------
# Background components
# -------------------------------------------------------------------------

payment_outbox_publisher = PaymentOutboxPublisher(
    session_factory=SessionFactory,
    producer=kafka_producer,
    avro=avro_helper,
    topic=settings.topic_payment_request,
    poll_interval_s=settings.outbox_poll_interval_seconds,
)

restaurant_approval_publisher = RestaurantApprovalOutboxPublisher(
    session_factory=SessionFactory,
    producer=kafka_producer,
    avro=avro_helper,
    topic=settings.topic_restaurant_approval_request,
    poll_interval_s=settings.outbox_poll_interval_seconds,
)

payment_response_consumer = PaymentResponseConsumer(
    consumer_config=settings.kafka_consumer_config,
    schema_registry_url=settings.schema_registry_url,
    saga_service_factory=_saga_service_factory,
    session_factory=SessionFactory,
    topic=settings.topic_payment_response,
)

approval_response_consumer = RestaurantApprovalResponseConsumer(
    consumer_config={
        **settings.kafka_consumer_config,
        "group.id": settings.order_service_consumer_group + "-approval",
    },
    schema_registry_url=settings.schema_registry_url,
    saga_service_factory=_saga_service_factory,
    session_factory=SessionFactory,
    topic=settings.topic_restaurant_approval_response,
)

customer_event_consumer = CustomerEventConsumer(
    consumer_config={
        **settings.kafka_consumer_config,
        "group.id": settings.order_service_consumer_group + "-customer",
    },
    schema_registry_url=settings.schema_registry_url,
    session_factory=SessionFactory,
    topic=settings.topic_customer,
)


# -------------------------------------------------------------------------
# Lifespan
# -------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: migrate DB, start background tasks. Shutdown: stop gracefully."""
    logger.info("Order Service starting up...")

    # Create tables (Alembic handles production migrations; this is for dev convenience)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Launch background tasks
    tasks = [
        asyncio.create_task(payment_outbox_publisher.run(), name="payment-outbox-publisher"),
        asyncio.create_task(
            restaurant_approval_publisher.run(), name="restaurant-approval-publisher"
        ),
        asyncio.create_task(payment_response_consumer.start(), name="payment-response-consumer"),
        asyncio.create_task(
            approval_response_consumer.start(), name="approval-response-consumer"
        ),
        asyncio.create_task(customer_event_consumer.start(), name="customer-event-consumer"),
    ]

    logger.info("Order Service started. Background tasks launched: %d", len(tasks))
    yield

    # Shutdown
    logger.info("Order Service shutting down...")
    await payment_outbox_publisher.stop()
    await restaurant_approval_publisher.stop()
    await payment_response_consumer.stop()
    await approval_response_consumer.stop()
    await customer_event_consumer.stop()
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    kafka_producer.close()
    await engine.dispose()
    logger.info("Order Service shutdown complete")


# -------------------------------------------------------------------------
# FastAPI app
# -------------------------------------------------------------------------

app = FastAPI(
    title="Order Service",
    description="Food ordering system — order lifecycle management and saga orchestration",
    version="1.0.0",
    lifespan=lifespan,
)

# Override DI for session
app.dependency_overrides[_get_session] = get_session

# Routers
app.include_router(order_router)


# -------------------------------------------------------------------------
# Exception handlers
# -------------------------------------------------------------------------

@app.exception_handler(CustomerNotFoundException)
async def customer_not_found_handler(request: Request, exc: CustomerNotFoundException):
    return JSONResponse(status_code=404, content={"code": "CUSTOMER_NOT_FOUND", "message": exc.message})


@app.exception_handler(OrderNotFoundException)
async def order_not_found_handler(request: Request, exc: OrderNotFoundException):
    return JSONResponse(status_code=404, content={"code": "ORDER_NOT_FOUND", "message": exc.message})


@app.exception_handler(DomainException)
async def domain_exception_handler(request: Request, exc: DomainException):
    return JSONResponse(status_code=400, content={"code": "DOMAIN_ERROR", "message": exc.message})


@app.exception_handler(ApplicationException)
async def application_exception_handler(request: Request, exc: ApplicationException):
    return JSONResponse(status_code=400, content={"code": "APPLICATION_ERROR", "message": exc.message})


# -------------------------------------------------------------------------
# Health / Readiness checks
# -------------------------------------------------------------------------

@app.get("/health", tags=["health"], summary="Liveness check")
async def health():
    return {"status": "UP", "service": "order-service"}


@app.get("/health/readiness", tags=["health"], summary="Readiness check")
async def readiness():
    """Check DB and Kafka connectivity."""
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
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
        reload=False,
    )
