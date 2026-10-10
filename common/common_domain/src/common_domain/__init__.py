"""
Common Domain Layer
===================
Base classes for entities, value objects, aggregate roots, domain events,
and shared value types (Money, identifiers).

No infrastructure dependencies are allowed here.
"""
from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Generic, List, Optional, TypeVar
from uuid import UUID


# ---------------------------------------------------------------------------
# Base identifier
# ---------------------------------------------------------------------------

class BaseId:
    """Typed wrapper around a UUID."""

    def __init__(self, value: UUID) -> None:
        if not isinstance(value, UUID):
            raise TypeError(f"Expected UUID, got {type(value)}")
        self._value = value

    @property
    def value(self) -> UUID:
        return self._value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._value == other._value

    def __hash__(self) -> int:
        return hash(self._value)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._value})"

    def __str__(self) -> str:
        return str(self._value)


ID = TypeVar("ID", bound=BaseId)


# ---------------------------------------------------------------------------
# Concrete identifier types
# ---------------------------------------------------------------------------

class OrderId(BaseId):
    """Identifies an Order aggregate."""


class CustomerId(BaseId):
    """Identifies a Customer aggregate."""


class RestaurantId(BaseId):
    """Identifies a Restaurant aggregate."""


class ProductId(BaseId):
    """Identifies a Product entity."""


class PaymentId(BaseId):
    """Identifies a Payment aggregate."""


class OrderItemId(BaseId):
    """Identifies an OrderItem entity."""


class TrackingId(BaseId):
    """Public-facing tracking identifier for an order."""


# ---------------------------------------------------------------------------
# Money value object
# ---------------------------------------------------------------------------

_TWO_PLACES = Decimal("0.01")


@dataclass(frozen=True)
class Money:
    """
    Immutable monetary amount using Decimal arithmetic.
    All arithmetic preserves two decimal places with ROUND_HALF_UP.
    """
    amount: Decimal

    def __post_init__(self) -> None:
        if not isinstance(self.amount, Decimal):
            object.__setattr__(self, "amount", Decimal(str(self.amount)))
        # Quantize to two decimal places
        object.__setattr__(
            self, "amount", self.amount.quantize(_TWO_PLACES, rounding=ROUND_HALF_UP)
        )

    @classmethod
    def of(cls, value: str | int | float | Decimal) -> "Money":
        try:
            return cls(Decimal(str(value)))
        except InvalidOperation as exc:
            raise ValueError(f"Invalid monetary amount: {value!r}") from exc

    @classmethod
    def zero(cls) -> "Money":
        return cls(Decimal("0.00"))

    def add(self, other: "Money") -> "Money":
        return Money(self.amount + other.amount)

    def subtract(self, other: "Money") -> "Money":
        return Money(self.amount - other.amount)

    def multiply(self, factor: int) -> "Money":
        return Money(self.amount * factor)

    def is_greater_than(self, other: "Money") -> bool:
        return self.amount > other.amount

    def is_greater_than_zero(self) -> bool:
        return self.amount > Decimal("0.00")

    def is_zero(self) -> bool:
        return self.amount == Decimal("0.00")

    def is_negative(self) -> bool:
        return self.amount < Decimal("0.00")

    def equals(self, other: "Money") -> bool:
        return self.amount == other.amount

    def __add__(self, other: "Money") -> "Money":
        return self.add(other)

    def __sub__(self, other: "Money") -> "Money":
        return self.subtract(other)

    def __mul__(self, factor: int) -> "Money":
        return self.multiply(factor)

    def __repr__(self) -> str:
        return f"Money({self.amount})"

    def __str__(self) -> str:
        return str(self.amount)


# ---------------------------------------------------------------------------
# Street address value object
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class StreetAddress:
    """Immutable delivery address."""
    id: UUID
    street: str
    postal_code: str
    city: str

    def __post_init__(self) -> None:
        if not self.street:
            raise ValueError("Street cannot be empty")
        if not self.city:
            raise ValueError("City cannot be empty")


# ---------------------------------------------------------------------------
# Domain events
# ---------------------------------------------------------------------------

class DomainEvent(ABC):
    """Base class for all domain events."""

    def __init__(self) -> None:
        self._created_at: datetime = datetime.now(tz=timezone.utc)

    @property
    def created_at(self) -> datetime:
        return self._created_at


# ---------------------------------------------------------------------------
# Base entity
# ---------------------------------------------------------------------------

class BaseEntity(Generic[ID], ABC):
    """
    Base class for all entities.  Equality is by identity (ID).
    """

    def __init__(self, entity_id: ID) -> None:
        self._id: ID = entity_id

    @property
    def id(self) -> ID:
        return self._id

    @id.setter
    def id(self, value: ID) -> None:
        self._id = value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self._id == other._id

    def __hash__(self) -> int:
        return hash(self._id)


# ---------------------------------------------------------------------------
# Aggregate root
# ---------------------------------------------------------------------------

class AggregateRoot(BaseEntity[ID], ABC):
    """
    Aggregate root; holds a list of pending domain events to be dispatched
    after the transaction commits.
    """

    def __init__(self, entity_id: ID) -> None:
        super().__init__(entity_id)
        self._domain_events: List[DomainEvent] = []

    def add_event(self, event: DomainEvent) -> None:
        self._domain_events.append(event)

    def clear_events(self) -> None:
        self._domain_events.clear()

    @property
    def domain_events(self) -> List[DomainEvent]:
        return list(self._domain_events)


# ---------------------------------------------------------------------------
# Domain exception base
# ---------------------------------------------------------------------------

class DomainException(Exception):
    """Base class for domain-layer exceptions."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
