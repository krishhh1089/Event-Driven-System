"""Customer Service — FastAPI application entry point."""
from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Depends, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID

from common_application import ApplicationException
from common_domain import DomainException

from .infrastructure.database.models import Base
from .application.create_customer_handler import CreateCustomerCommandHandler
from .infrastructure.outbox.publisher import CustomerOutboxPublisher

from kafka_infrastructure import AvroHelper, KafkaProducerWrapper, build_schema_registry_client
from common_dataaccess import build_async_engine, build_session_factory

logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)

import os
DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg://food_user:food_pass@localhost:5432/food_ordering"
)
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
PORT = int(os.environ.get("PORT", 8184))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info").lower()

engine = build_async_engine(DATABASE_URL)
SessionFactory = build_session_factory(engine)

schema_registry_client = build_schema_registry_client(SCHEMA_REGISTRY_URL)
avro_helper = AvroHelper(schema_registry_client)
kafka_producer = KafkaProducerWrapper({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "acks": "all",
    "retries": 5,
})

customer_outbox_publisher = CustomerOutboxPublisher(
    session_factory=SessionFactory,
    producer=kafka_producer,
    avro=avro_helper,
    topic="customer",
)


async def get_session():
    async with SessionFactory() as session:
        yield session


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Customer Service starting...")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    tasks = [asyncio.create_task(customer_outbox_publisher.run(), name="customer-outbox")]
    logger.info("Customer Service started")
    yield
    logger.info("Customer Service shutting down...")
    await customer_outbox_publisher.stop()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    kafka_producer.close()
    await engine.dispose()


app = FastAPI(
    title="Customer Service",
    description="Customer registration and event publication",
    version="1.0.0",
    lifespan=lifespan,
)


class CreateCustomerRequest(BaseModel):
    customerId: UUID
    username: str = Field(..., min_length=1)
    firstName: str = Field(..., min_length=1)
    lastName: str = Field(..., min_length=1)


class CreateCustomerResponse(BaseModel):
    customerId: UUID
    message: str = "Customer created successfully"


@app.post("/customers", response_model=CreateCustomerResponse, status_code=status.HTTP_201_CREATED)
async def create_customer(
    body: CreateCustomerRequest,
    session: AsyncSession = Depends(get_session),
):
    handler = CreateCustomerCommandHandler(session)
    async with session.begin():
        result = await handler.handle(
            customer_id=body.customerId,
            username=body.username,
            first_name=body.firstName,
            last_name=body.lastName,
        )
    return CreateCustomerResponse(customerId=body.customerId)


@app.get("/health")
async def health():
    return {"status": "UP", "service": "customer-service"}


@app.exception_handler(DomainException)
async def domain_exc(request: Request, exc: DomainException):
    return JSONResponse(status_code=400, content={"code": "DOMAIN_ERROR", "message": exc.message})


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT, log_level=LOG_LEVEL)
