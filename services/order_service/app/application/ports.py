"""
Order Service — Repository Ports
==================================
Abstract interfaces for persistence.  Concrete implementations live in the
infrastructure/database layer and are injected through FastAPI's DI system.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional
from uuid import UUID

from ..domain.entities import Order, Restaurant


class OrderRepository(ABC):
    """Port: save and retrieve Order aggregates."""

    @abstractmethod
    async def save(self, order: Order) -> Order:
        ...

    @abstractmethod
    async def find_by_tracking_id(self, tracking_id: UUID) -> Optional[Order]:
        ...

    @abstractmethod
    async def find_by_id(self, order_id: UUID) -> Optional[Order]:
        ...


class CustomerRepository(ABC):
    """Port: check customer existence in the local read model."""

    @abstractmethod
    async def exists(self, customer_id: UUID) -> bool:
        ...


class RestaurantRepository(ABC):
    """Port: load the restaurant read model for order validation."""

    @abstractmethod
    async def find_restaurant_information(
        self, restaurant_id: UUID, product_ids: List[UUID]
    ) -> Optional[Restaurant]:
        ...
