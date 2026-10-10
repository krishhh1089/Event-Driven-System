"""
Order Service Domain Service
=============================
Orchestrates multi-entity business rules for order creation and state changes.
Has no knowledge of persistence or messaging infrastructure.
"""
from __future__ import annotations

import logging
from typing import List

from common_domain import DomainException
from .entities import (
    Order,
    OrderCreatedEvent,
    OrderPaidEvent,
    OrderCancelledEvent,
    OrderCancellingEvent,
    Restaurant,
)

logger = logging.getLogger(__name__)


class OrderDomainService:
    """
    Pure domain service.

    Responsibilities:
    - Validate order against the provided restaurant read model.
    - Trigger state transitions and publish domain events.
    """

    def validate_and_initiate_order(
        self, order: Order, restaurant: Restaurant
    ) -> OrderCreatedEvent:
        """
        Validate and initialize a new order.

        Steps:
        1. Assert restaurant is active.
        2. Validate each order item against the restaurant's product catalog.
        3. Update each order item's confirmed price from the catalog.
        4. Run the order's internal price invariants.
        5. Initialize the order (assign ID, tracking ID, set PENDING).
        6. Return an OrderCreatedEvent.
        """
        self._validate_restaurant(order, restaurant)
        self._set_order_product_information(order, restaurant)
        order.validate_order()
        order.initialize_order()
        logger.info("Order %s initialized with tracking ID %s", order.id, order.tracking_id)
        return OrderCreatedEvent(order)

    def pay_order(self, order: Order) -> OrderPaidEvent:
        order.pay()
        logger.info("Order %s marked as PAID", order.id)
        return OrderPaidEvent(order)

    def approve_order(self, order: Order) -> None:
        order.approve()
        logger.info("Order %s APPROVED", order.id)

    def cancel_order_payment(
        self, order: Order, failure_messages: List[str]
    ) -> OrderCancellingEvent:
        order.init_cancel(failure_messages)
        logger.info("Order %s transitioning to CANCELLING: %s", order.id, failure_messages)
        return OrderCancellingEvent(order, failure_messages)

    def cancel_order(self, order: Order, failure_messages: List[str]) -> OrderCancelledEvent:
        order.cancel(failure_messages)
        logger.info("Order %s CANCELLED", order.id)
        return OrderCancelledEvent(order, failure_messages)

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_restaurant(self, order: Order, restaurant: Restaurant) -> None:
        if not restaurant.active:
            raise DomainException(
                f"Restaurant {restaurant.id} is not active, cannot place order"
            )

    def _set_order_product_information(self, order: Order, restaurant: Restaurant) -> None:
        restaurant_product_map = {p.id: p for p in restaurant.products}
        for item in order.items:
            catalog_product = restaurant_product_map.get(item.product_id)
            if catalog_product is None:
                raise DomainException(
                    f"Product {item.product_id} does not belong to restaurant {restaurant.id}"
                )
            item._name = catalog_product.name  # noqa: SLF001
            # Validate client-supplied price against authoritative catalog price.
            if not item.price.equals(catalog_product.price):
                raise DomainException(
                    f"Product {item.product_id} price mismatch: "
                    f"requested {item.price}, catalog {catalog_product.price}"
                )
