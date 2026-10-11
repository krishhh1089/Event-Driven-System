"""
Restaurant Service — Kafka Approval Request Consumer
======================================================
Consumes from 'restaurant-approval-request' topic.
Deserializes Avro messages and invokes RestaurantApprovalCommandHandler.
Commits Kafka offset ONLY after the local DB transaction commits.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict
from uuid import UUID

from confluent_kafka import Consumer, KafkaError

from kafka_infrastructure import AvroHelper, build_schema_registry_client

from ...application.restaurant_approval_handler import RestaurantApprovalCommandHandler

logger = logging.getLogger(__name__)


class RestaurantApprovalRequestConsumer:
    """Consumer for restaurant approval request events."""

    def __init__(
        self,
        consumer_config: dict,
        schema_registry_url: str,
        session_factory,
        topic: str = "restaurant-approval-request",
    ) -> None:
        self._consumer = Consumer(consumer_config)
        self._avro = AvroHelper(build_schema_registry_client(schema_registry_url))
        self._session_factory = session_factory
        self._topic = topic
        self._running = False

    async def start(self) -> None:
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("RestaurantApprovalRequestConsumer subscribed to %s", self._topic)
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
            logger.error("Kafka error in approval request consumer: %s", msg.error())
            return

        try:
            event_dict: Dict[str, Any] = self._avro.deserialize(msg.value())
            logger.info(
                "Received approval-request | saga_id=%s order_id=%s restaurant_id=%s",
                event_dict.get("saga_id"),
                event_dict.get("order_id"),
                event_dict.get("restaurant_id"),
            )

            saga_id = UUID(event_dict["saga_id"])
            restaurant_id = UUID(event_dict["restaurant_id"])
            order_id = UUID(event_dict["order_id"])
            products = event_dict.get("products", [])
            price = event_dict.get("price", "0.00")

            async with self._session_factory() as session:
                async with session.begin():
                    handler = RestaurantApprovalCommandHandler(session)
                    await handler.handle_approval_request(
                        saga_id=saga_id,
                        restaurant_id=restaurant_id,
                        order_id=order_id,
                        products=products,
                        price=price,
                    )

            # Manual commit: only acknowledged after DB transaction successfully committed
            self._consumer.commit(msg)
            logger.info("Committed offset for order_id=%s", order_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error processing restaurant approval request: %s", exc)
