"""
Order Service — Payment Outbox Publisher
==========================================
Polls the payment_outbox table for STARTED records and publishes them
to the 'payment-request' Kafka topic.

Locking strategy:
  SELECT ... FOR UPDATE SKIP LOCKED
  This prevents concurrent publisher instances from processing the same record.

Exactly-once delivery guarantee:
  NOT provided. The outbox pattern provides at-least-once delivery.
  A crash after Kafka ack but before DB commit causes redelivery.
  Consumers MUST be idempotent (enforced by saga status checks + DB constraints).

Mode A (default): Application-managed polling — this file.
Mode B: Debezium CDC — see infrastructure/debezium/ for configuration.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import select, update, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from kafka_infrastructure import AvroHelper, KafkaProducerWrapper, build_schema_registry_client
from ...infrastructure.database.models import (
    PaymentOutboxModel,
    RestaurantApprovalOutboxModel,
    OutboxStatusEnum,
    SagaStatusEnum,
)

logger = logging.getLogger(__name__)

AVRO_SCHEMAS_DIR = "common/infrastructure/kafka/avro"


def _load_schema(name: str) -> str:
    import os, pathlib
    root = pathlib.Path(__file__).parents[7]  # repo root
    schema_path = root / AVRO_SCHEMAS_DIR / name
    with open(schema_path) as f:
        return f.read()


class PaymentOutboxPublisher:
    """
    Polls payment_outbox for STARTED records and publishes to Kafka.
    """

    def __init__(
        self,
        session_factory,
        producer: KafkaProducerWrapper,
        avro: AvroHelper,
        topic: str = "payment-request",
        poll_interval_s: float = 5.0,
        max_retries: int = 5,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._avro = avro
        self._topic = topic
        self._poll_interval_s = poll_interval_s
        self._max_retries = max_retries
        self._schema_str = _load_schema("payment_request.avsc")
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("PaymentOutboxPublisher started, polling every %ss", self._poll_interval_s)
        while self._running:
            try:
                await self.publish_pending()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error in PaymentOutboxPublisher: %s", exc)
            await asyncio.sleep(self._poll_interval_s)

    async def stop(self) -> None:
        self._running = False

    async def publish_pending(self) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                # Claim records with FOR UPDATE SKIP LOCKED
                stmt = (
                    select(PaymentOutboxModel)
                    .where(PaymentOutboxModel.outbox_status == OutboxStatusEnum.STARTED)
                    .with_for_update(skip_locked=True)
                    .limit(50)
                )
                result = await session.execute(stmt)
                records: List[PaymentOutboxModel] = list(result.scalars())

                for record in records:
                    await self._publish_record(session, record)

    async def _publish_record(self, session: AsyncSession, record: PaymentOutboxModel) -> None:
        payload = record.payload
        try:
            avro_data = {
                "id": str(record.id),
                "saga_id": str(record.saga_id),
                "customer_id": payload.get("customer_id", ""),
                "order_id": payload.get("order_id", ""),
                "price": payload.get("price", "0.00"),
                "created_at": record.created_at.isoformat(),
                "payment_order_status": record.payment_status,
            }
            message_bytes = self._avro.serialize(
                self._schema_str,
                avro_data,
                subject="payment-request-value",
            )
            self._producer.produce(
                topic=self._topic,
                value=message_bytes,
                key=payload.get("order_id"),
            )
            self._producer.flush(timeout=10.0)

            # Mark COMPLETED
            await session.execute(
                update(PaymentOutboxModel)
                .where(
                    and_(
                        PaymentOutboxModel.id == record.id,
                        PaymentOutboxModel.version == record.version,
                    )
                )
                .values(
                    outbox_status=OutboxStatusEnum.COMPLETED,
                    processed_at=datetime.now(timezone.utc),
                    version=record.version + 1,
                )
            )
            logger.info(
                "Published payment-request | saga_id=%s order_id=%s",
                record.saga_id,
                payload.get("order_id"),
            )
        except Exception as exc:
            logger.error("Failed to publish payment outbox record %s: %s", record.id, exc)
            # Do not mark COMPLETED — it will be retried next poll cycle


class RestaurantApprovalOutboxPublisher:
    """
    Polls restaurant_approval_outbox for STARTED records and publishes to Kafka.
    """

    def __init__(
        self,
        session_factory,
        producer: KafkaProducerWrapper,
        avro: AvroHelper,
        topic: str = "restaurant-approval-request",
        poll_interval_s: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._avro = avro
        self._topic = topic
        self._poll_interval_s = poll_interval_s
        self._schema_str = _load_schema("restaurant_approval_request.avsc")
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info("RestaurantApprovalOutboxPublisher started")
        while self._running:
            try:
                await self.publish_pending()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error in RestaurantApprovalOutboxPublisher: %s", exc)
            await asyncio.sleep(self._poll_interval_s)

    async def stop(self) -> None:
        self._running = False

    async def publish_pending(self) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                stmt = (
                    select(RestaurantApprovalOutboxModel)
                    .where(RestaurantApprovalOutboxModel.outbox_status == OutboxStatusEnum.STARTED)
                    .with_for_update(skip_locked=True)
                    .limit(50)
                )
                result = await session.execute(stmt)
                records = list(result.scalars())

                for record in records:
                    await self._publish_record(session, record)

    async def _publish_record(
        self, session: AsyncSession, record: RestaurantApprovalOutboxModel
    ) -> None:
        payload = record.payload
        try:
            avro_data = {
                "id": str(record.id),
                "saga_id": str(record.saga_id),
                "restaurant_id": payload.get("restaurant_id", ""),
                "order_id": payload.get("order_id", ""),
                "restaurant_order_status": "PAID",
                "products": payload.get("products", []),
                "price": payload.get("price", "0.00"),
                "created_at": record.created_at.isoformat(),
            }
            message_bytes = self._avro.serialize(
                self._schema_str,
                avro_data,
                subject="restaurant-approval-request-value",
            )
            self._producer.produce(
                topic=self._topic,
                value=message_bytes,
                key=payload.get("order_id"),
            )
            self._producer.flush(timeout=10.0)

            await session.execute(
                update(RestaurantApprovalOutboxModel)
                .where(
                    and_(
                        RestaurantApprovalOutboxModel.id == record.id,
                        RestaurantApprovalOutboxModel.version == record.version,
                    )
                )
                .values(
                    outbox_status=OutboxStatusEnum.COMPLETED,
                    processed_at=datetime.now(timezone.utc),
                    version=record.version + 1,
                )
            )
            logger.info(
                "Published restaurant-approval-request | saga_id=%s order_id=%s",
                record.saga_id,
                payload.get("order_id"),
            )
        except Exception as exc:
            logger.error(
                "Failed to publish restaurant approval outbox record %s: %s", record.id, exc
            )
