"""
Order Service — Saga Application Service
==========================================
Handles the full saga lifecycle with database persistence.

This is the "saga_service" injected into OrderPaymentSaga and OrderApprovalSaga.
It coordinates:
- Loading orders from the DB
- State transitions via domain methods
- Outbox record creation/updates
- Optimistic concurrency protection
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional, Any
from uuid import UUID, uuid4

from sqlalchemy import select, and_, update
from sqlalchemy.ext.asyncio import AsyncSession

from common_application import OrderNotFoundException, SagaException
from common_domain import Money

from ..domain.entities import OrderStatus
from ..domain.order_domain_service import OrderDomainService
from .database.models import (
    OrderModel,
    PaymentOutboxModel,
    RestaurantApprovalOutboxModel,
    OutboxStatusEnum,
    SagaStatusEnum,
    PaymentOrderStatusEnum,
    RestaurantOrderStatusEnum,
)
from .database.repositories import SqlAlchemyOrderRepository

logger = logging.getLogger(__name__)


class SagaApplicationService:
    """
    Transaction coordinator for Saga state transitions.
    All methods are called within an open database session (transactional).
    """

    def __init__(
        self,
        session: AsyncSession,
        order_domain_service: OrderDomainService,
    ) -> None:
        self._session = session
        self._domain_service = order_domain_service
        self._order_repo = SqlAlchemyOrderRepository(session)

    # ------------------------------------------------------------------
    # Payment Saga — process (payment COMPLETED)
    # ------------------------------------------------------------------

    async def process_payment_response(self, response: Any) -> None:
        """
        Payment COMPLETED: advance order PENDING → PAID, create approval outbox record.
        """
        saga_id = UUID(response.saga_id)
        order_id = UUID(response.order_id)

        # Idempotency: check whether this saga event was already processed
        existing = await self._find_payment_outbox_by_saga_and_status(
            saga_id, [SagaStatusEnum.PROCESSING]
        )
        if not existing:
            logger.warning(
                "No pending payment outbox found for saga_id=%s — duplicate response, skipping",
                saga_id,
            )
            return

        order = await self._order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundException(f"Order {order_id} not found")

        self._domain_service.pay_order(order)

        # Persist updated order status
        await self._session.execute(
            update(OrderModel)
            .where(OrderModel.id == order_id)
            .values(status=OrderStatus.PAID.value, updated_at=datetime.now(timezone.utc))
        )

        # Mark payment outbox COMPLETED
        await self._session.execute(
            update(PaymentOutboxModel)
            .where(
                and_(
                    PaymentOutboxModel.saga_id == saga_id,
                    PaymentOutboxModel.saga_status == SagaStatusEnum.PROCESSING,
                )
            )
            .values(
                outbox_status=OutboxStatusEnum.COMPLETED,
                saga_status=SagaStatusEnum.PROCESSING,
                processed_at=datetime.now(timezone.utc),
                version=PaymentOutboxModel.version + 1,
            )
        )

        # Create restaurant approval outbox record
        await self._create_restaurant_approval_outbox(order, saga_id, response)
        logger.info("Order %s transitioned to PAID, restaurant approval outbox created", order_id)

    # ------------------------------------------------------------------
    # Payment Saga — rollback (payment CANCELLED / refund confirmed)
    # ------------------------------------------------------------------

    async def rollback_payment_response(self, response: Any) -> None:
        """
        Refund CONFIRMED (CANCELLED status): advance order CANCELLING → CANCELLED.
        """
        saga_id = UUID(response.saga_id)
        order_id = UUID(response.order_id)

        # Find the compensating outbox entry
        existing = await self._find_payment_outbox_by_saga_and_status(
            saga_id, [SagaStatusEnum.COMPENSATING]
        )
        if not existing:
            logger.warning(
                "No compensating payment outbox for saga_id=%s — duplicate, skipping", saga_id
            )
            return

        order = await self._order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundException(f"Order {order_id} not found")

        self._domain_service.cancel_order(order, response.failure_messages or [])

        await self._session.execute(
            update(OrderModel)
            .where(OrderModel.id == order_id)
            .values(status=OrderStatus.CANCELLED.value, updated_at=datetime.now(timezone.utc))
        )

        # Mark compensating outbox COMPLETED
        await self._session.execute(
            update(PaymentOutboxModel)
            .where(
                and_(
                    PaymentOutboxModel.saga_id == saga_id,
                    PaymentOutboxModel.saga_status == SagaStatusEnum.COMPENSATING,
                )
            )
            .values(
                outbox_status=OutboxStatusEnum.COMPLETED,
                saga_status=SagaStatusEnum.COMPENSATED,
                processed_at=datetime.now(timezone.utc),
                version=PaymentOutboxModel.version + 1,
            )
        )
        logger.info("Order %s CANCELLED after refund confirmation", order_id)

    # ------------------------------------------------------------------
    # Payment Saga — failed payment
    # ------------------------------------------------------------------

    async def handle_payment_failed(self, response: Any) -> None:
        """Payment FAILED: cancel order immediately (PENDING → CANCELLED)."""
        saga_id = UUID(response.saga_id)
        order_id = UUID(response.order_id)

        existing = await self._find_payment_outbox_by_saga_and_status(
            saga_id, [SagaStatusEnum.PROCESSING]
        )
        if not existing:
            logger.warning("Duplicate failed payment for saga_id=%s, skipping", saga_id)
            return

        order = await self._order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundException(f"Order {order_id} not found")

        self._domain_service.cancel_order(order, response.failure_messages or [])

        await self._session.execute(
            update(OrderModel)
            .where(OrderModel.id == order_id)
            .values(status=OrderStatus.CANCELLED.value, updated_at=datetime.now(timezone.utc))
        )

        await self._session.execute(
            update(PaymentOutboxModel)
            .where(
                and_(
                    PaymentOutboxModel.saga_id == saga_id,
                    PaymentOutboxModel.saga_status == SagaStatusEnum.PROCESSING,
                )
            )
            .values(
                outbox_status=OutboxStatusEnum.COMPLETED,
                saga_status=SagaStatusEnum.FAILED,
                processed_at=datetime.now(timezone.utc),
                version=PaymentOutboxModel.version + 1,
            )
        )
        logger.info("Order %s CANCELLED due to payment failure", order_id)

    # ------------------------------------------------------------------
    # Approval Saga — process (APPROVED)
    # ------------------------------------------------------------------

    async def process_approval_response(self, response: Any) -> None:
        saga_id = UUID(response.saga_id)
        order_id = UUID(response.order_id)

        existing = await self._find_approval_outbox_by_saga_and_status(
            saga_id, [SagaStatusEnum.PROCESSING]
        )
        if not existing:
            logger.warning("Duplicate approval response for saga_id=%s, skipping", saga_id)
            return

        order = await self._order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundException(f"Order {order_id} not found")

        self._domain_service.approve_order(order)

        await self._session.execute(
            update(OrderModel)
            .where(OrderModel.id == order_id)
            .values(status=OrderStatus.APPROVED.value, updated_at=datetime.now(timezone.utc))
        )

        await self._session.execute(
            update(RestaurantApprovalOutboxModel)
            .where(
                and_(
                    RestaurantApprovalOutboxModel.saga_id == saga_id,
                    RestaurantApprovalOutboxModel.saga_status == SagaStatusEnum.PROCESSING,
                )
            )
            .values(
                outbox_status=OutboxStatusEnum.COMPLETED,
                saga_status=SagaStatusEnum.SUCCEEDED,
                processed_at=datetime.now(timezone.utc),
                version=RestaurantApprovalOutboxModel.version + 1,
            )
        )
        logger.info("Order %s APPROVED", order_id)

    # ------------------------------------------------------------------
    # Approval Saga — rollback (REJECTED)
    # ------------------------------------------------------------------

    async def rollback_approval_response(self, response: Any) -> None:
        """
        Restaurant REJECTED: transition order PAID → CANCELLING,
        insert compensating payment outbox record (refund request).
        """
        saga_id = UUID(response.saga_id)
        order_id = UUID(response.order_id)

        existing = await self._find_approval_outbox_by_saga_and_status(
            saga_id, [SagaStatusEnum.PROCESSING]
        )
        if not existing:
            logger.warning("Duplicate rejection for saga_id=%s, skipping", saga_id)
            return

        order = await self._order_repo.find_by_id(order_id)
        if not order:
            raise OrderNotFoundException(f"Order {order_id} not found")

        self._domain_service.cancel_order_payment(order, response.failure_messages or [])

        await self._session.execute(
            update(OrderModel)
            .where(OrderModel.id == order_id)
            .values(status=OrderStatus.CANCELLING.value, updated_at=datetime.now(timezone.utc))
        )

        # Mark approval outbox as compensating
        await self._session.execute(
            update(RestaurantApprovalOutboxModel)
            .where(
                and_(
                    RestaurantApprovalOutboxModel.saga_id == saga_id,
                    RestaurantApprovalOutboxModel.saga_status == SagaStatusEnum.PROCESSING,
                )
            )
            .values(
                saga_status=SagaStatusEnum.COMPENSATING,
                version=RestaurantApprovalOutboxModel.version + 1,
            )
        )

        # Insert compensating payment cancellation outbox record
        await self._create_payment_cancellation_outbox(order, saga_id)
        logger.info("Order %s transitioning to CANCELLING, refund outbox created", order_id)

    # ------------------------------------------------------------------
    # Outbox helpers
    # ------------------------------------------------------------------

    async def create_payment_outbox(
        self, order: Any, saga_id: UUID
    ) -> PaymentOutboxModel:
        """
        Insert initial payment request outbox record.
        Called during order creation (same transaction as order save).
        """
        payload = {
            "order_id": str(order.id.value),
            "customer_id": str(order.customer_id.value),
            "restaurant_id": str(order.restaurant_id.value),
            "price": str(order.price.amount),
            "saga_id": str(saga_id),
        }
        outbox = PaymentOutboxModel(
            id=uuid4(),
            saga_id=saga_id,
            type="PaymentRequestAvroModel",
            payload=payload,
            outbox_status=OutboxStatusEnum.STARTED,
            saga_status=SagaStatusEnum.STARTED,
            payment_status=PaymentOrderStatusEnum.PENDING,
            version=0,
        )
        self._session.add(outbox)
        return outbox

    async def _create_payment_cancellation_outbox(
        self, order: Any, saga_id: UUID
    ) -> None:
        """Insert refund request into payment outbox (compensating action)."""
        payload = {
            "order_id": str(order.id.value),
            "customer_id": str(order.customer_id.value),
            "price": str(order.price.amount),
            "saga_id": str(saga_id),
        }
        outbox = PaymentOutboxModel(
            id=uuid4(),
            saga_id=saga_id,
            type="PaymentRequestAvroModel",
            payload=payload,
            outbox_status=OutboxStatusEnum.STARTED,
            saga_status=SagaStatusEnum.COMPENSATING,
            payment_status=PaymentOrderStatusEnum.CANCELLED,
            version=0,
        )
        self._session.add(outbox)

    async def _create_restaurant_approval_outbox(
        self, order: Any, saga_id: UUID, payment_response: Any
    ) -> None:
        products = [
            {
                "id": str(item.product_id.value),
                "quantity": item.quantity,
                "price": str(item.price.amount),
            }
            for item in order.items
        ]
        payload = {
            "order_id": str(order.id.value),
            "restaurant_id": str(order.restaurant_id.value),
            "price": str(order.price.amount),
            "saga_id": str(saga_id),
            "products": products,
        }
        outbox = RestaurantApprovalOutboxModel(
            id=uuid4(),
            saga_id=saga_id,
            type="RestaurantApprovalRequestAvroModel",
            payload=payload,
            outbox_status=OutboxStatusEnum.STARTED,
            saga_status=SagaStatusEnum.PROCESSING,
            approval_status=RestaurantOrderStatusEnum.PAID,
            version=0,
        )
        self._session.add(outbox)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _find_payment_outbox_by_saga_and_status(
        self, saga_id: UUID, statuses: List[SagaStatusEnum]
    ) -> Optional[PaymentOutboxModel]:
        stmt = select(PaymentOutboxModel).where(
            and_(
                PaymentOutboxModel.saga_id == saga_id,
                PaymentOutboxModel.saga_status.in_(statuses),
            )
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    async def _find_approval_outbox_by_saga_and_status(
        self, saga_id: UUID, statuses: List[SagaStatusEnum]
    ) -> Optional[RestaurantApprovalOutboxModel]:
        stmt = select(RestaurantApprovalOutboxModel).where(
            and_(
                RestaurantApprovalOutboxModel.saga_id == saga_id,
                RestaurantApprovalOutboxModel.saga_status.in_(statuses),
            )
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()
