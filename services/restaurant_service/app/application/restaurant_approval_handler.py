"""
Restaurant Service — Restaurant Approval Command Handler
=========================================================
Handles incoming restaurant approval requests transactionally.

Idempotency:
- If OrderApproval already exists for order_id, the decision is deterministic
  and cannot change. We ensure the outbox response exists and return early.
- Both OrderApprovalModel and RestaurantApprovalOutboxModel are written
  inside the SAME database transaction.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from common_domain import Money, OrderId, ProductId, RestaurantId

from ..domain.entities import (
    OrderApprovalStatus,
    Product,
    Restaurant,
)
from ..domain.restaurant_domain_service import RestaurantDomainService
from ..infrastructure.database.models import (
    OrderApprovalModel,
    OrderApprovalStatusEnum,
    OutboxStatusEnum,
    ProductModel,
    RestaurantModel,
    RestaurantProductModel,
    RestaurantApprovalOutboxModel,
    SagaStatusEnum,
)

logger = logging.getLogger(__name__)


class RestaurantApprovalCommandHandler:
    """Application use case for validating and approving an order."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._domain_service = RestaurantDomainService()

    async def handle_approval_request(
        self,
        saga_id: UUID,
        restaurant_id: UUID,
        order_id: UUID,
        products: List[Dict],
        price: str,
    ) -> None:
        """
        Evaluate order against restaurant catalog and record decision + outbox atomically.
        """
        # 1. Idempotency check: has this order already been decided?
        existing_approval_stmt = select(OrderApprovalModel).where(
            OrderApprovalModel.order_id == order_id
        )
        existing_result = await self._session.execute(existing_approval_stmt)
        existing_approval = existing_result.scalar_one_or_none()

        if existing_approval is not None:
            logger.info(
                "Idempotency: Order %s already evaluated (status=%s), ensuring outbox response",
                order_id,
                existing_approval.approval_status,
            )
            await self._ensure_outbox_response(
                saga_id=saga_id,
                restaurant_id=restaurant_id,
                order_id=order_id,
                approval_status=OrderApprovalStatusEnum(existing_approval.approval_status.value),
                failure_messages=[],
            )
            return

        # 2. Authoritative restaurant lookup
        restaurant_stmt = select(RestaurantModel).where(
            RestaurantModel.id == restaurant_id
        )
        res_result = await self._session.execute(restaurant_stmt)
        restaurant_model = res_result.scalar_one_or_none()

        if restaurant_model is None:
            failure_messages = [f"Restaurant {restaurant_id} not found in catalog."]
            approval_status = OrderApprovalStatusEnum.REJECTED
        else:
            # Query products offered by this restaurant
            products_stmt = (
                select(ProductModel)
                .join(RestaurantProductModel, RestaurantProductModel.product_id == ProductModel.id)
                .where(RestaurantProductModel.restaurant_id == restaurant_id)
            )
            prod_result = await self._session.execute(products_stmt)
            catalog_products = list(prod_result.scalars())

            # Build domain aggregate
            domain_products: Dict[ProductId, Product] = {}
            for pm in catalog_products:
                pid = ProductId(pm.id)
                domain_products[pid] = Product(
                    product_id=pid,
                    name=pm.name,
                    price=Money.of(pm.price),
                    available=pm.available,
                )

            domain_restaurant = Restaurant(
                restaurant_id=RestaurantId(restaurant_id),
                name=restaurant_model.name,
                active=restaurant_model.active,
                products=domain_products,
            )

            # Evaluate domain logic
            domain_approval, failure_messages = self._domain_service.validate_order(
                restaurant=domain_restaurant,
                order_id=OrderId(order_id),
                requested_products=products,
            )
            approval_status = (
                OrderApprovalStatusEnum.APPROVED
                if domain_approval.approval_status == OrderApprovalStatus.APPROVED
                else OrderApprovalStatusEnum.REJECTED
            )

        # 3. Persist OrderApprovalModel
        approval_record = OrderApprovalModel(
            id=uuid4(),
            restaurant_id=restaurant_id,
            order_id=order_id,
            approval_status=approval_status,
        )
        self._session.add(approval_record)

        # 4. Insert RestaurantApprovalOutboxModel in the same transaction
        await self._ensure_outbox_response(
            saga_id=saga_id,
            restaurant_id=restaurant_id,
            order_id=order_id,
            approval_status=approval_status,
            failure_messages=failure_messages,
        )
        await self._session.flush()

    async def _ensure_outbox_response(
        self,
        saga_id: UUID,
        restaurant_id: UUID,
        order_id: UUID,
        approval_status: OrderApprovalStatusEnum,
        failure_messages: List[str],
    ) -> None:
        """Insert outbox record if not already present."""
        stmt = select(RestaurantApprovalOutboxModel).where(
            and_(
                RestaurantApprovalOutboxModel.saga_id == saga_id,
                RestaurantApprovalOutboxModel.saga_status == SagaStatusEnum.PROCESSING,
            )
        )
        res = await self._session.execute(stmt)
        if res.scalar_one_or_none() is not None:
            return

        payload = {
            "id": str(uuid4()),
            "saga_id": str(saga_id),
            "restaurant_id": str(restaurant_id),
            "order_id": str(order_id),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "order_approval_status": approval_status.value,
            "failure_messages": failure_messages,
        }

        outbox_entry = RestaurantApprovalOutboxModel(
            id=uuid4(),
            saga_id=saga_id,
            type="RestaurantApprovalResponseAvroModel",
            payload=payload,
            outbox_status=OutboxStatusEnum.STARTED,
            saga_status=SagaStatusEnum.PROCESSING,
            approval_status=approval_status,
            version=0,
        )
        self._session.add(outbox_entry)
