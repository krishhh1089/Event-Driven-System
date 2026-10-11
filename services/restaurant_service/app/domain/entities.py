"""
Restaurant Service — Domain Entities
======================================
Pure Python domain models for restaurant catalog and order approval logic.
No infrastructure or database dependencies.
"""
from __future__ import annotations

import enum
from typing import Dict, List, Optional
from uuid import UUID

from common_domain import (
    AggregateRoot,
    BaseEntity,
    BaseId,
    DomainEvent,
    DomainException,
    Money,
    OrderId,
    ProductId,
    RestaurantId,
)


class OrderApprovalStatus(str, enum.Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class OrderApprovalId(BaseId):
    """Typed identifier for an OrderApproval record."""
    pass


class Product(BaseEntity[ProductId]):
    """
    Product entity within a restaurant catalog.
    """

    def __init__(
        self,
        product_id: ProductId,
        name: str,
        price: Money,
        available: bool = True,
    ) -> None:
        super().__init__(product_id)
        self._name = name
        self._price = price
        self._available = available

    @property
    def name(self) -> str:
        return self._name

    @property
    def price(self) -> Money:
        return self._price

    @property
    def available(self) -> bool:
        return self._available

    def update_availability(self, available: bool) -> None:
        self._available = available


class OrderApproval(BaseEntity[OrderApprovalId]):
    """
    Approval decision for an order.
    """

    def __init__(
        self,
        order_approval_id: OrderApprovalId,
        restaurant_id: RestaurantId,
        order_id: OrderId,
        approval_status: OrderApprovalStatus,
    ) -> None:
        super().__init__(order_approval_id)
        self._restaurant_id = restaurant_id
        self._order_id = order_id
        self._approval_status = approval_status

    @property
    def restaurant_id(self) -> RestaurantId:
        return self._restaurant_id

    @property
    def order_id(self) -> OrderId:
        return self._order_id

    @property
    def approval_status(self) -> OrderApprovalStatus:
        return self._approval_status


class OrderApprovalEvent(DomainEvent):
    """Event emitted when an order approval decision is reached."""

    def __init__(
        self,
        order_approval: OrderApproval,
        failure_messages: Optional[List[str]] = None,
    ) -> None:
        super().__init__()
        self.order_approval = order_approval
        self.failure_messages = failure_messages or []


class Restaurant(AggregateRoot[RestaurantId]):
    """
    Restaurant aggregate root.
    Holds restaurant status and product catalog for validation.
    """

    def __init__(
        self,
        restaurant_id: RestaurantId,
        name: str,
        active: bool = True,
        products: Optional[Dict[ProductId, Product]] = None,
    ) -> None:
        super().__init__(restaurant_id)
        self._name = name
        self._active = active
        self._products: Dict[ProductId, Product] = products or {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def active(self) -> bool:
        return self._active

    @property
    def products(self) -> Dict[ProductId, Product]:
        return dict(self._products)

    def add_product(self, product: Product) -> None:
        self._products[product.id] = product

    def get_product(self, product_id: ProductId) -> Optional[Product]:
        return self._products.get(product_id)
