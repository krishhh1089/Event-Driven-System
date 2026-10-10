"""
Order Service — Restaurant Approval Response Kafka Consumer
============================================================
Consumes from 'restaurant-approval-response' topic.
Handles APPROVED (advance to APPROVED) and REJECTED (initiate refund).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError

from kafka_infrastructure import AvroHelper, build_schema_registry_client

logger = logging.getLogger(__name__)


class ApprovalResponseEvent:
    def __init__(self, event: dict) -> None:
        self.event_id = event["id"]
        self.saga_id = event["saga_id"]
        self.restaurant_id = event["restaurant_id"]
        self.order_id = event["order_id"]
        self.order_approval_status = event["order_approval_status"]
        self.failure_messages = event.get("failure_messages", [])


class RestaurantApprovalResponseConsumer:

    def __init__(
        self,
        consumer_config: dict,
        schema_registry_url: str,
        saga_service_factory,
        session_factory,
        topic: str = "restaurant-approval-response",
    ) -> None:
        self._consumer = Consumer(consumer_config)
        self._avro = AvroHelper(build_schema_registry_client(schema_registry_url))
        self._saga_service_factory = saga_service_factory
        self._session_factory = session_factory
        self._topic = topic
        self._running = False

    async def start(self) -> None:
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("RestaurantApprovalResponseConsumer subscribed to %s", self._topic)
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
            logger.error("Kafka error: %s", msg.error())
            return

        try:
            event_dict = self._avro.deserialize(msg.value())
            response = ApprovalResponseEvent(event_dict)
            logger.info(
                "Processing approval-response | saga_id=%s order_id=%s status=%s",
                response.saga_id,
                response.order_id,
                response.order_approval_status,
            )

            async with self._session_factory() as session:
                async with session.begin():
                    saga_svc = self._saga_service_factory(session)
                    if response.order_approval_status == "APPROVED":
                        await saga_svc.process_approval_response(response)
                    elif response.order_approval_status == "REJECTED":
                        await saga_svc.rollback_approval_response(response)
                    else:
                        logger.warning("Unknown approval status: %s", response.order_approval_status)
                        return

            self._consumer.commit(msg)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Error processing approval-response: %s", exc)
