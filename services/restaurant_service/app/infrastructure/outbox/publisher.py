"""
Restaurant Service — Restaurant Approval Outbox Publisher
==========================================================
Polls the restaurant_approval_outbox table for STARTED records and publishes
them to the 'restaurant-approval-response' Kafka topic.

Uses SELECT ... FOR UPDATE SKIP LOCKED for concurrent publisher safety.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import List

from sqlalchemy import and_, select, update

from kafka_infrastructure import AvroHelper, KafkaProducerWrapper, load_avro_schema

from ..database.models import (
    OutboxStatusEnum,
    RestaurantApprovalOutboxModel,
)

logger = logging.getLogger(__name__)


class RestaurantApprovalOutboxPublisher:
    """Polls restaurant_approval_outbox for STARTED records and publishes to Kafka."""

    def __init__(
        self,
        session_factory,
        producer: KafkaProducerWrapper,
        avro: AvroHelper,
        topic: str = "restaurant-approval-response",
        poll_interval_s: float = 5.0,
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer
        self._avro = avro
        self._topic = topic
        self._poll_interval_s = poll_interval_s
        self._schema_str = load_avro_schema("restaurant_approval_response.avsc")
        self._running = False

    async def run(self) -> None:
        self._running = True
        logger.info(
            "RestaurantApprovalOutboxPublisher started, polling every %ss",
            self._poll_interval_s,
        )
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
                records: List[RestaurantApprovalOutboxModel] = list(result.scalars())

                for record in records:
                    await self._publish_record(session, record)

    async def _publish_record(self, session, record: RestaurantApprovalOutboxModel) -> None:
        payload = record.payload
        try:
            avro_data = {
                "id": str(payload.get("id", record.id)),
                "saga_id": str(payload.get("saga_id", record.saga_id)),
                "restaurant_id": str(payload.get("restaurant_id")),
                "order_id": str(payload.get("order_id")),
                "created_at": payload.get("created_at", datetime.now(timezone.utc).isoformat()),
                "order_approval_status": payload.get("order_approval_status", "REJECTED"),
                "failure_messages": payload.get("failure_messages", []),
            }
            msg_bytes = self._avro.serialize(
                self._schema_str,
                avro_data,
                subject="restaurant-approval-response-value",
            )
            self._producer.produce(
                topic=self._topic,
                value=msg_bytes,
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
                "Published restaurant-approval-response | saga_id=%s order_id=%s status=%s",
                payload.get("saga_id"),
                payload.get("order_id"),
                payload.get("order_approval_status"),
            )
        except Exception as exc:
            logger.error(
                "Failed to publish restaurant approval outbox record %s: %s", record.id, exc
            )
