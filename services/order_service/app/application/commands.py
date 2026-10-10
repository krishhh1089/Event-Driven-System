"""
Order Service — Commands and DTOs
===================================
Immutable command objects used by application use-case handlers.
No infrastructure dependencies.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional
from uuid import UUID


@dataclass(frozen=True)
class OrderAddressCommand:
    street: str
    postal_code: str
    city: str


@dataclass(frozen=True)
class OrderItemCommand:
    product_id: UUID
    quantity: int
    price: Decimal
    sub_total: Decimal


@dataclass(frozen=True)
class CreateOrderCommand:
    customer_id: UUID
    restaurant_id: UUID
    address: OrderAddressCommand
    price: Decimal
    items: List[OrderItemCommand]


@dataclass(frozen=True)
class TrackOrderQuery:
    tracking_id: UUID
