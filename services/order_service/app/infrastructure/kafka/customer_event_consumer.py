"""
Order Service — Customer Event Kafka Consumer
=============================================
Consumes from the 'customer' topic and upserts customer data into
the order service's local customer read model.

Idempotency: ON CONFLICT DO NOTHING — duplicate customer events are ignored.
If the customer fields changed (unlikely in this system), the event is still
safely ignored since the read model only needs existence for order validation.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from uuid import UUID

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from kafka_infrastructure import AvroHelper, build_schema_registry_client
from ...infrastructure.database.models import CustomerReadModel

logger = logging.getLogger(__name__)


class CustomerEventConsumer:

    def __init__(
        self,
        consumer_config: dict,
        schema_registry_url: str,
        session_factory,
        topic: str = "customer",
    ) -> None:
        self._consumer = Consumer(consumer_config)
        self._avro = AvroHelper(build_schema_registry_client(schema_registry_url))
        self._session_factory = session_factory
        self._topic = topic
        self._running = False

    async def start(self) -> None:
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("CustomerEventConsumer subscribed to %s", self._topic)
        while self._running:
            await self._poll_once()

    async def stop(self) -> None:
        self._running = False
        self._consumer.close()

    async def _poll_once(self) -> None:
        loop = asyncio.get_event_loop()
        msg = await loop.run_in_executor(None, lambda: self._consumer.poll(timeout=1.0))
        if msg is None:
            return
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            logger.error("Kafka error in CustomerEventConsumer: %s", msg.error())
            return

        try:
            event = self._avro.deserialize(msg.value())
            await self._upsert_customer(event)
            self._consumer.commit(msg)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to process customer event: %s", exc)

    async def _upsert_customer(self, event: dict) -> None:
        customer_id = UUID(event["id"])
        logger.info("Replicating customer %s into order service read model", customer_id)
        async with self._session_factory() as session:
            async with session.begin():
                stmt = (
                    pg_insert(CustomerReadModel)
                    .values(
                        id=customer_id,
                        username=event["username"],
                        first_name=event["first_name"],
                        last_name=event["last_name"],
                        replicated_at=datetime.now(timezone.utc),
                    )
                    .on_conflict_do_nothing(index_elements=["id"])
                )
                await session.execute(stmt)
        logger.info("Customer %s replicated successfully", customer_id)
