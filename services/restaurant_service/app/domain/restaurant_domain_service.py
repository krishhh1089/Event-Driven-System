"""
Restaurant Service — Domain Service
====================================
Pure business logic for validating and approving/rejecting orders.
"""
from __future__ import annotations

import logging
from typing import Dict, List, Tuple
from uuid import uuid4

from common_domain import Money, OrderId, ProductId

from .entities import (
    OrderApproval,
    OrderApprovalId,
    OrderApprovalStatus,
    Product,
    Restaurant,
)

logger = logging.getLogger(__name__)


class RestaurantDomainService:
    """Domain service for evaluating restaurant order approval requests."""

    def validate_order(
        self,
        restaurant: Restaurant,
        order_id: OrderId,
        requested_products: List[Dict],
    ) -> Tuple[OrderApproval, List[str]]:
        """
        Validate an order against the restaurant's catalog.

        Parameters:
        - restaurant: The Restaurant aggregate loaded from authoritative storage.
        - order_id: The order being evaluated.
        - requested_products: List of dicts with {"id": UUID, "quantity": int, "price": Decimal/str}.

        Returns:
        - Tuple of (OrderApproval, failure_messages).
        """
        failure_messages: List[str] = []

        if not restaurant.active:
            failure_messages.append(
                f"Restaurant {restaurant.id.value} is currently inactive and cannot accept orders."
            )
            approval = OrderApproval(
                order_approval_id=OrderApprovalId(uuid4()),
                restaurant_id=restaurant.id,
                order_id=order_id,
                approval_status=OrderApprovalStatus.REJECTED,
            )
            return approval, failure_messages

        for item in requested_products:
            prod_id_raw = item["id"]
            prod_id = ProductId(prod_id_raw)
            catalog_product = restaurant.get_product(prod_id)

            if catalog_product is None:
                failure_messages.append(
                    f"Product {prod_id.value} is not offered by restaurant {restaurant.id.value}."
                )
                continue

            if not catalog_product.available:
                failure_messages.append(
                    f"Product '{catalog_product.name}' ({prod_id.value}) is currently out of stock."
                )
                continue

            requested_price = Money.of(item["price"])
            if not requested_price.equals(catalog_product.price):
                failure_messages.append(
                    f"Product '{catalog_product.name}' price mismatch: "
                    f"catalog is {catalog_product.price}, order specified {requested_price}."
                )

        if failure_messages:
            logger.info(
                "Order %s REJECTED by restaurant %s: %s",
                order_id.value,
                restaurant.id.value,
                failure_messages,
            )
            approval_status = OrderApprovalStatus.REJECTED
        else:
            logger.info(
                "Order %s APPROVED by restaurant %s",
                order_id.value,
                restaurant.id.value,
            )
            approval_status = OrderApprovalStatus.APPROVED

        approval = OrderApproval(
            order_approval_id=OrderApprovalId(uuid4()),
            restaurant_id=restaurant.id,
            order_id=order_id,
            approval_status=approval_status,
        )
        return approval, failure_messages
