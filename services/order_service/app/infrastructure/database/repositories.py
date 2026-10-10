"""
Order Service — Repository Implementations
============================================
Concrete SQLAlchemy-backed implementations of the repository ports.
Each method runs within the caller's session (passed via DI).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import select, and_, text
from sqlalchemy.ext.asyncio import AsyncSession

from common_domain import (
    CustomerId,
    Money,
    OrderId,
    ProductId,
    RestaurantId,
    StreetAddress,
    TrackingId,
)
from ...application.ports import CustomerRepository, OrderRepository, RestaurantRepository
from ...domain.entities import (
    Order,
    OrderItem,
    OrderItemId,
    OrderStatus,
    Product,
    Restaurant,
)
from .models import (
    CustomerReadModel,
    OrderAddressModel,
    OrderItemModel,
    OrderModel,
    OrderStatusEnum,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Order Repository
# ---------------------------------------------------------------------------

class SqlAlchemyOrderRepository(OrderRepository):

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, order: Order) -> Order:
        model = self._to_model(order)
        self._session.add(model)
        await self._session.flush()  # get generated ID without commit
        return self._to_entity(model)

    async def find_by_tracking_id(self, tracking_id: UUID) -> Optional[Order]:
        stmt = (
            select(OrderModel)
            .where(OrderModel.tracking_id == tracking_id)
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    async def find_by_id(self, order_id: UUID) -> Optional[Order]:
        stmt = select(OrderModel).where(OrderModel.id == order_id)
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        return self._to_entity(model) if model else None

    def _to_model(self, order: Order) -> OrderModel:
        address_model = OrderAddressModel(
            id=order.delivery_address.id,
            order_id=order.id.value,
            street=order.delivery_address.street,
            postal_code=order.delivery_address.postal_code,
            city=order.delivery_address.city,
        )
        item_models = [
            OrderItemModel(
                order_id=order.id.value,
                product_id=item.product_id.value,
                quantity=item.quantity,
                price=item.price.amount,
                sub_total=item.sub_total.amount,
            )
            for item in order.items
        ]
        failure_str = "|".join(order.failure_messages) if order.failure_messages else None
        return OrderModel(
            id=order.id.value,
            customer_id=order.customer_id.value,
            restaurant_id=order.restaurant_id.value,
            tracking_id=order.tracking_id.value,
            price=order.price.amount,
            status=order.status.value,
            failure_messages=failure_str,
            items=item_models,
            address=address_model,
        )

    def _to_entity(self, model: OrderModel) -> Order:
        failure_messages = (
            model.failure_messages.split("|") if model.failure_messages else []
        )
        items = [
            OrderItem(
                item_id=OrderItemId(UUID(int=m.id)),
                order_id=OrderId(model.id),
                product_id=ProductId(m.product_id),
                quantity=m.quantity,
                price=Money(Decimal(str(m.price))),
                sub_total=Money(Decimal(str(m.sub_total))),
            )
            for m in model.items
        ]
        address = StreetAddress(
            id=model.address.id,
            street=model.address.street,
            postal_code=model.address.postal_code,
            city=model.address.city,
        ) if model.address else None

        order = Order(
            order_id=OrderId(model.id),
            customer_id=CustomerId(model.customer_id),
            restaurant_id=RestaurantId(model.restaurant_id),
            delivery_address=address,
            price=Money(Decimal(str(model.price))),
            items=items,
            tracking_id=TrackingId(model.tracking_id),
            status=OrderStatus(model.status),
            failure_messages=failure_messages,
        )
        return order


# ---------------------------------------------------------------------------
# Customer Read Model Repository
# ---------------------------------------------------------------------------

class SqlAlchemyCustomerRepository(CustomerRepository):

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def exists(self, customer_id: UUID) -> bool:
        stmt = select(CustomerReadModel.id).where(CustomerReadModel.id == customer_id)
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None


# ---------------------------------------------------------------------------
# Restaurant Read Model Repository
# ---------------------------------------------------------------------------

class SqlAlchemyRestaurantRepository(RestaurantRepository):
    """
    Uses the order_restaurant_m_view materialized view to efficiently load
    restaurant and product information for order validation.
    Falls back to direct table query if the view is unavailable.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def find_restaurant_information(
        self, restaurant_id: UUID, product_ids: List[UUID]
    ) -> Optional[Restaurant]:
        """
        Query the restaurant materialized view in the restaurant schema.
        The view is refreshed periodically; reads are eventually consistent.
        """
        product_ids_str = ", ".join(f"'{pid}'" for pid in product_ids)
        stmt = text(
            f"""
            SELECT
                r.id              AS restaurant_id,
                r.active          AS restaurant_active,
                p.id              AS product_id,
                p.name            AS product_name,
                p.price           AS product_price
            FROM restaurant.restaurants r
            JOIN restaurant.restaurant_products rp ON rp.restaurant_id = r.id
            JOIN restaurant.products p             ON p.id = rp.product_id
            WHERE r.id = :restaurant_id
              AND p.id IN ({product_ids_str})
              AND p.available = true
            """
        )
        result = await self._session.execute(stmt, {"restaurant_id": restaurant_id})
        rows = result.fetchall()
        if not rows:
            return None

        first_row = rows[0]
        if not first_row.restaurant_active:
            return None

        products = [
            Product(
                product_id=ProductId(row.product_id),
                name=row.product_name,
                price=Money(Decimal(str(row.product_price))),
            )
            for row in rows
        ]
        return Restaurant(
            restaurant_id=RestaurantId(first_row.restaurant_id),
            products=products,
            active=first_row.restaurant_active,
        )
