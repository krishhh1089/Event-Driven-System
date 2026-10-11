"""
Customer Service — Outbox Publisher
=====================================
Polls customer_outbox for STARTED records and publishes to the 'customer' topic.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select, update, and_

from kafka_infrastructure import AvroHelper, KafkaProducerWrapper
from ..database.models import CustomerOutboxModel, OutboxStatusEnum

logger = logging.getLogger(__name__)


def _load_customer_schema() -> str:
    from kafka_infrastructure import load_avro_schema
    return load_avro_schema("customer.avsc")


class CustomerOutboxPublisher:

    def __init__(
        self,
        session_factory,
        producer: KafkaProducerWrapper,
        avro: AvroHelper,
        topic: str = "customer",
        poll_interval_s: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._avro = avro
        self._topic = topic
        self._poll_interval_s = poll_interval_s
        self._schema_str = _load_customer_schema()
        self._running = False

    async def run(self) -> None:
        self._running = True
        while self._running:
            try:
                await self.publish_pending()
            except Exception as exc:
                logger.exception("CustomerOutboxPublisher error: %s", exc)
            await asyncio.sleep(self._poll_interval_s)

    async def stop(self) -> None:
        self._running = False

    async def publish_pending(self) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                stmt = (
                    select(CustomerOutboxModel)
                    .where(CustomerOutboxModel.outbox_status == OutboxStatusEnum.STARTED)
                    .with_for_update(skip_locked=True)
                    .limit(50)
                )
                result = await session.execute(stmt)
                records = list(result.scalars())

                for record in records:
                    try:
                        payload = record.payload
                        avro_data = {
                            "id": payload["id"],
                            "username": payload["username"],
                            "first_name": payload["first_name"],
                            "last_name": payload["last_name"],
                            "created_at": payload.get("created_at", ""),
                        }
                        msg_bytes = self._avro.serialize(
                            self._schema_str, avro_data, subject="customer-value"
                        )
                        self._producer.produce(
                            topic=self._topic,
                            value=msg_bytes,
                            key=payload["id"],
                        )
                        self._producer.flush(timeout=10.0)

                        await session.execute(
                            update(CustomerOutboxModel)
                            .where(
                                and_(
                                    CustomerOutboxModel.id == record.id,
                                    CustomerOutboxModel.version == record.version,
                                )
                            )
                            .values(
                                outbox_status=OutboxStatusEnum.COMPLETED,
                                processed_at=datetime.now(timezone.utc),
                                version=record.version + 1,
                            )
                        )
                        logger.info("Published customer event for %s", payload["id"])
                    except Exception as exc:
                        logger.error("Failed to publish customer outbox %s: %s", record.id, exc)
