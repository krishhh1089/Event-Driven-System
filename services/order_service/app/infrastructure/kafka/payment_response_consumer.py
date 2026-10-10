"""
Order Service — Payment Response Kafka Consumer
================================================
Consumes from the 'payment-response' topic.
Handles COMPLETED (advance order to PAID), FAILED (cancel order),
and CANCELLED (complete order cancellation after refund).

Idempotency is enforced in SagaApplicationService via database constraints
and status-based guards (not just a SELECT check — the UPDATE WHERE also
acts as a lock).

Offset commits happen AFTER the database transaction is committed.
This means if the app crashes after DB commit but before offset commit,
the message is redelivered and the idempotency logic ensures no duplicate effect.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException

from kafka_infrastructure import AvroHelper, build_schema_registry_client

logger = logging.getLogger(__name__)


class PaymentResponseConsumer:
    """
    Asynchronous Kafka consumer for payment responses.
    Runs in a background thread/task.
    """

    def __init__(
        self,
        consumer_config: dict,
        schema_registry_url: str,
        saga_service_factory,
        session_factory,
        topic: str = "payment-response",
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
        logger.info("PaymentResponseConsumer subscribed to %s", self._topic)
        while self._running:
            await self._poll_once()

    async def stop(self) -> None:
        self._running = False
        self._consumer.close()
        logger.info("PaymentResponseConsumer stopped")

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
            event = self._avro.deserialize(msg.value())
            await self._handle(event, msg)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Error processing payment-response message | partition=%d offset=%d: %s",
                msg.partition(),
                msg.offset(),
                exc,
            )
            # Do NOT commit offset — message will be redelivered

    async def _handle(self, event: dict, msg: Any) -> None:
        payment_status = event.get("payment_status")
        logger.info(
            "Processing payment-response | saga_id=%s order_id=%s status=%s partition=%d offset=%d",
            event.get("saga_id"),
            event.get("order_id"),
            payment_status,
            msg.partition(),
            msg.offset(),
        )

        async with self._session_factory() as session:
            async with session.begin():
                saga_svc = self._saga_service_factory(session)
                from ...application.sagas import PaymentResponse
                response = PaymentResponse(
                    event_id=event["id"],
                    saga_id=event["saga_id"],
                    payment_id=event["payment_id"],
                    customer_id=event["customer_id"],
                    order_id=event["order_id"],
                    price=event["price"],
                    payment_status=payment_status,
                    failure_messages=event.get("failure_messages", []),
                )
                if payment_status == "COMPLETED":
                    await saga_svc.process_payment_response(response)
                elif payment_status == "FAILED":
                    await saga_svc.handle_payment_failed(response)
                elif payment_status == "CANCELLED":
                    await saga_svc.rollback_payment_response(response)
                else:
                    logger.warning("Unknown payment status: %s", payment_status)
                    return

        # Commit offset only after successful DB transaction
        self._consumer.commit(msg)
        logger.debug("Committed offset %d on partition %d", msg.offset(), msg.partition())
