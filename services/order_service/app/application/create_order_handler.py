"""
Order Service — Create Order Command Handler
=============================================
Orchestrates:
1. Customer validation (local read model).
2. Restaurant/product validation.
3. Order aggregate creation + validation.
4. Atomic persistence of order + outbox record.
5. Return of tracking ID.
"""
from __future__ import annotations

import logging
import uuid
from decimal import Decimal
from typing import Optional
from uuid import UUID, uuid4

from common_domain import (
    CustomerId,
    DomainException,
    Money,
    OrderId,
    ProductId,
    RestaurantId,
    StreetAddress,
)
from common_application import CustomerNotFoundException, RestaurantNotFoundException

from ..domain.entities import (
    Order,
    OrderItem,
    OrderItemId,
    Product,
    Restaurant,
)
from ..domain.order_domain_service import OrderDomainService
from .commands import CreateOrderCommand
from .ports import CustomerRepository, OrderRepository, RestaurantRepository

logger = logging.getLogger(__name__)


class CreateOrderCommandHandler:
    """
    Use-case handler for CREATE ORDER.

    Design decision: the handler composes the domain aggregate from the
    command, delegates invariant checks to the domain service, and
    delegates persistence to injected repository implementations.

    The outbox insertion happens inside the repository.save() transaction
    so both are committed atomically.
    """

    def __init__(
        self,
        order_domain_service: OrderDomainService,
        order_repository: OrderRepository,
        customer_repository: CustomerRepository,
        restaurant_repository: RestaurantRepository,
    ) -> None:
        self._domain_service = order_domain_service
        self._order_repo = order_repository
        self._customer_repo = customer_repository
        self._restaurant_repo = restaurant_repository

    async def handle(self, command: CreateOrderCommand) -> dict:
        """
        Returns a dict with 'tracking_id' and 'order_status' on success.
        """
        await self._check_customer(command.customer_id)
        restaurant = await self._check_restaurant(command)
        order = self._create_order(command)
        event = self._domain_service.validate_and_initiate_order(order, restaurant)
        saved_order = await self._order_repo.save(order)
        logger.info(
            "Order created | order_id=%s tracking_id=%s",
            saved_order.id,
            saved_order.tracking_id,
        )
        return {
            "tracking_id": saved_order.tracking_id.value,
            "order_status": saved_order.status,
        }

    async def _check_customer(self, customer_id: UUID) -> None:
        exists = await self._customer_repo.exists(customer_id)
        if not exists:
            raise CustomerNotFoundException(
                f"Customer {customer_id} not found in local read model. "
                "The customer may not have been replicated yet. "
                "Retry after a brief delay."
            )

    async def _check_restaurant(self, command: CreateOrderCommand) -> Restaurant:
        product_ids = [item.product_id for item in command.items]
        restaurant = await self._restaurant_repo.find_restaurant_information(
            command.restaurant_id, product_ids
        )
        if restaurant is None:
            raise RestaurantNotFoundException(
                f"Restaurant {command.restaurant_id} not found or not active"
            )
        return restaurant

    def _create_order(self, command: CreateOrderCommand) -> Order:
        address = StreetAddress(
            id=uuid4(),
            street=command.address.street,
            postal_code=command.address.postal_code,
            city=command.address.city,
        )
        items = [
            OrderItem(
                item_id=OrderItemId(uuid4()),
                order_id=None,
                product_id=ProductId(item.product_id),
                quantity=item.quantity,
                price=Money.of(item.price),
                sub_total=Money.of(item.sub_total),
            )
            for item in command.items
        ]
        return Order(
            order_id=None,  # assigned in initialize_order()
            customer_id=CustomerId(command.customer_id),
            restaurant_id=RestaurantId(command.restaurant_id),
            delivery_address=address,
            price=Money.of(command.price),
            items=items,
        )
