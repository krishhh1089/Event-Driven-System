"""
Payment Service — Payment Response Outbox Publisher
=====================================================
Polls the payment_outbox table for STARTED records and publishes them
to the 'payment-response' Kafka topic.

Uses SELECT ... FOR UPDATE SKIP LOCKED to support concurrent publisher
instances (though typically only one runs per service instance).

At-least-once delivery: a crash after Kafka ack but before DB commit
causes redelivery. The Order Service's saga idempotency logic handles duplicates.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update, and_

from kafka_infrastructure import AvroHelper, KafkaProducerWrapper

from ..database.models import (
    OutboxStatusEnum,
    PaymentOutboxModel,
    SagaStatusEnum,
)

logger = logging.getLogger(__name__)


def _load_payment_response_schema() -> str:
    from kafka_infrastructure import load_avro_schema
    return load_avro_schema("payment_response.avsc")


class PaymentResponseOutboxPublisher:
    """Polls payment_outbox for STARTED records and publishes to Kafka."""

    def __init__(
        self,
        session_factory,
        producer: KafkaProducerWrapper,
        avro: AvroHelper,
        topic: str = "payment-response",
        poll_interval_s: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._avro = avro
        self._topic = topic
        self._poll_interval_s = poll_interval_s
        self._schema_str = _load_payment_response_schema()
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info(
            "PaymentResponseOutboxPublisher started, polling every %ss", self._poll_interval_s
        )
        while self._running:
            try:
                await self.publish_pending()
            except Exception as exc:  # noqa: BLE001
                logger.exception("Error in PaymentResponseOutboxPublisher: %s", exc)
            await asyncio.sleep(self._poll_interval_s)

    async def stop(self) -> None:
        self._running = False

    async def publish_pending(self) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                stmt = (
                    select(PaymentOutboxModel)
                    .where(PaymentOutboxModel.outbox_status == OutboxStatusEnum.STARTED)
                    .with_for_update(skip_locked=True)
                    .limit(50)
                )
                result = await session.execute(stmt)
                records = list(result.scalars())

                for record in records:
                    await self._publish_record(session, record)

    async def _publish_record(self, session, record: PaymentOutboxModel) -> None:
        payload = record.payload
        try:
            avro_data = {
                "id": payload["id"],
                "saga_id": payload["saga_id"],
                "payment_id": payload["payment_id"],
                "customer_id": payload["customer_id"],
                "order_id": payload["order_id"],
                "price": payload["price"],
                "created_at": payload.get("created_at", ""),
                "payment_status": payload["payment_status"],
                "failure_messages": payload.get("failure_messages", []),
            }
            msg_bytes = self._avro.serialize(
                self._schema_str, avro_data, subject="payment-response-value"
            )
            self._producer.produce(
                topic=self._topic,
                value=msg_bytes,
                key=payload.get("order_id"),
            )
            self._producer.flush(timeout=10.0)

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
                "Published payment-response | saga_id=%s order_id=%s status=%s",
                payload.get("saga_id"),
                payload.get("order_id"),
                payload.get("payment_status"),
            )
        except Exception as exc:
            logger.error(
                "Failed to publish payment-response outbox record %s: %s", record.id, exc
            )
