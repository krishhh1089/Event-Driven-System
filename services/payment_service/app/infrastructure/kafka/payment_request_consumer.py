"""
Payment Service — Payment Request Kafka Consumer
==================================================
Consumes from the 'payment-request' topic.

Message types (determined by the payment_order_status field):
  PENDING   → Process payment (debit customer credit).
  CANCELLED → Refund payment (credit customer balance).

Idempotency:
  Enforced via DB-level unique constraint on (order_id, payment_status).
  If a duplicate arrives the handler returns early and re-emits the outbox
  response so the Order Service can proceed.

Offset commit:
  Offsets are committed manually AFTER the DB transaction commits.
  If the service crashes after DB commit but before offset commit, the
  message is redelivered and the idempotency logic handles it safely.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from uuid import UUID

from confluent_kafka import Consumer, KafkaError

from kafka_infrastructure import AvroHelper, build_schema_registry_client

from ...application.payment_request_handler import PaymentRequestHandler

logger = logging.getLogger(__name__)


class PaymentRequestConsumer:
    """Asynchronous Kafka consumer for payment-request events."""

    def __init__(
        self,
        consumer_config: dict,
        schema_registry_url: str,
        session_factory,
        topic: str = "payment-request",
    ) -> None:
        self._consumer = Consumer(consumer_config)
        self._avro = AvroHelper(build_schema_registry_client(schema_registry_url))
        self._session_factory = session_factory
        self._topic = topic
        self._running = False

    async def start(self) -> None:
        self._consumer.subscribe([self._topic])
        self._running = True
        logger.info("PaymentRequestConsumer subscribed to %s", self._topic)
        while self._running:
            await self._poll_once()

    async def stop(self) -> None:
        self._running = False
        self._consumer.close()
        logger.info("PaymentRequestConsumer stopped")

    async def _poll_once(self) -> None:
        loop = asyncio.get_event_loop()
        msg = await loop.run_in_executor(None, lambda: self._consumer.poll(timeout=1.0))
        if msg is None:
            return
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                return
            logger.error("Kafka error in PaymentRequestConsumer: %s", msg.error())
            return

        try:
            event = self._avro.deserialize(msg.value())
            await self._handle(event, msg)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Error processing payment-request | partition=%d offset=%d: %s",
                msg.partition(),
                msg.offset(),
                exc,
            )
            # Do NOT commit offset — message will be redelivered

    async def _handle(self, event: dict, msg) -> None:
        saga_id = UUID(event["saga_id"])
        order_id = UUID(event["order_id"])
        customer_id = UUID(event["customer_id"])
        price = Decimal(event["price"])
        payment_order_status = event["payment_order_status"]
        event_id = event["id"]

        logger.info(
            "Processing payment-request | saga_id=%s order_id=%s status=%s partition=%d offset=%d",
            saga_id, order_id, payment_order_status, msg.partition(), msg.offset(),
        )

        async with self._session_factory() as session:
            async with session.begin():
                handler = PaymentRequestHandler(session)
                if payment_order_status == "PENDING":
                    await handler.process_payment(
                        saga_id=saga_id,
                        order_id=order_id,
                        customer_id=customer_id,
                        price=price,
                        event_id=event_id,
                    )
                elif payment_order_status == "CANCELLED":
                    await handler.cancel_payment(
                        saga_id=saga_id,
                        order_id=order_id,
                        customer_id=customer_id,
                        price=price,
                        event_id=event_id,
                    )
                else:
                    logger.warning("Unknown payment_order_status: %s", payment_order_status)
                    return

        # Commit offset only after successful DB transaction
        self._consumer.commit(msg)
        logger.debug("Committed offset %d on partition %d", msg.offset(), msg.partition())
