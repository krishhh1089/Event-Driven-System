"""
Order Service — Domain Entities and Value Objects
=================================================
All business logic lives here; no infrastructure dependencies.
"""
from __future__ import annotations

import enum
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

# We re-use shared value types from common_domain.
# In a strict monorepo with editable installs these are importable.
# For portability the types are also defined inline as aliases.
from common_domain import (
    AggregateRoot,
    BaseEntity,
    BaseId,
    CustomerId,
    DomainEvent,
    DomainException,
    Money,
    OrderId,
    ProductId,
    RestaurantId,
    StreetAddress,
    TrackingId,
)


# ---------------------------------------------------------------------------
# Order status enum
# ---------------------------------------------------------------------------

class OrderStatus(str, enum.Enum):
    PENDING = "PENDING"
    PAID = "PAID"
    APPROVED = "APPROVED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


# ---------------------------------------------------------------------------
# Valid state transitions
# ---------------------------------------------------------------------------

_VALID_TRANSITIONS: dict[OrderStatus, set[OrderStatus]] = {
    OrderStatus.PENDING:    {OrderStatus.PAID, OrderStatus.CANCELLED},
    OrderStatus.PAID:       {OrderStatus.APPROVED, OrderStatus.CANCELLING},
    OrderStatus.APPROVED:   set(),
    OrderStatus.CANCELLING: {OrderStatus.CANCELLED},
    OrderStatus.CANCELLED:  set(),
}


# ---------------------------------------------------------------------------
# OrderItemId
# ---------------------------------------------------------------------------

class OrderItemId(BaseId):
    """Identifies an item within an order."""


# ---------------------------------------------------------------------------
# OrderItem entity
# ---------------------------------------------------------------------------

class OrderItem(BaseEntity[OrderItemId]):
    """A single line item in an order."""

    def __init__(
        self,
        item_id: OrderItemId,
        order_id: Optional[OrderId],
        product_id: ProductId,
        quantity: int,
        price: Money,
        sub_total: Money,
    ) -> None:
        super().__init__(item_id)
        self._order_id = order_id
        self._product_id = product_id
        self._quantity = quantity
        self._price = price
        self._sub_total = sub_total

    @property
    def order_id(self) -> Optional[OrderId]:
        return self._order_id

    @order_id.setter
    def order_id(self, value: OrderId) -> None:
        self._order_id = value

    @property
    def product_id(self) -> ProductId:
        return self._product_id

    @property
    def quantity(self) -> int:
        return self._quantity

    @property
    def price(self) -> Money:
        return self._price

    @property
    def sub_total(self) -> Money:
        return self._sub_total

    def is_price_valid(self) -> bool:
        """Sub-total must equal price × quantity."""
        return self._price.is_greater_than_zero() and self._sub_total.equals(
            self._price.multiply(self._quantity)
        )


# ---------------------------------------------------------------------------
# Order domain events
# ---------------------------------------------------------------------------

class OrderCreatedEvent(DomainEvent):
    def __init__(self, order: "Order") -> None:
        super().__init__()
        self.order = order


class OrderPaidEvent(DomainEvent):
    def __init__(self, order: "Order") -> None:
        super().__init__()
        self.order = order


class OrderCancelledEvent(DomainEvent):
    def __init__(self, order: "Order", failure_messages: List[str]) -> None:
        super().__init__()
        self.order = order
        self.failure_messages = failure_messages


class OrderCancellingEvent(DomainEvent):
    def __init__(self, order: "Order", failure_messages: List[str]) -> None:
        super().__init__()
        self.order = order
        self.failure_messages = failure_messages


# ---------------------------------------------------------------------------
# Order aggregate root
# ---------------------------------------------------------------------------

class Order(AggregateRoot[OrderId]):
    """
    Order aggregate root.

    Enforces all state transitions and price invariants.
    External services communicate with an Order only through its public methods.
    """

    def __init__(
        self,
        order_id: OrderId,
        customer_id: CustomerId,
        restaurant_id: RestaurantId,
        delivery_address: StreetAddress,
        price: Money,
        items: List[OrderItem],
        tracking_id: Optional[TrackingId] = None,
        status: Optional[OrderStatus] = None,
        failure_messages: Optional[List[str]] = None,
    ) -> None:
        super().__init__(order_id)
        self._customer_id = customer_id
        self._restaurant_id = restaurant_id
        self._delivery_address = delivery_address
        self._price = price
        self._items = items
        self._tracking_id = tracking_id
        self._status = status
        self._failure_messages: List[str] = failure_messages or []

    # ------------------------------------------------------------------
    # Lifecycle initialization
    # ------------------------------------------------------------------

    def initialize_order(self) -> None:
        """
        Called once when a new order is created.
        Assigns a UUID, tracking ID, and sets status to PENDING.
        """
        self._id = OrderId(uuid4())
        self._tracking_id = TrackingId(uuid4())
        self._status = OrderStatus.PENDING
        self._initialize_order_items()

    def _initialize_order_items(self) -> None:
        item_id = 1
        for item in self._items:
            item.order_id = self._id
            item.id = OrderItemId(UUID(int=item_id))
            item_id += 1

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def _transition_to(self, new_status: OrderStatus) -> None:
        allowed = _VALID_TRANSITIONS.get(self._status, set())
        if new_status not in allowed:
            raise DomainException(
                f"Order {self._id} cannot transition from {self._status} to {new_status}"
            )
        self._status = new_status

    def pay(self) -> None:
        self._transition_to(OrderStatus.PAID)

    def approve(self) -> None:
        self._transition_to(OrderStatus.APPROVED)

    def init_cancel(self, failure_messages: List[str]) -> None:
        """Begin cancellation workflow (CANCELLING state)."""
        self._transition_to(OrderStatus.CANCELLING)
        self._update_failure_messages(failure_messages)

    def cancel(self, failure_messages: List[str]) -> None:
        """Final cancellation."""
        if self._status not in {OrderStatus.PENDING, OrderStatus.CANCELLING}:
            raise DomainException(
                f"Order {self._id} cannot be cancelled from status {self._status}"
            )
        self._status = OrderStatus.CANCELLED
        self._update_failure_messages(failure_messages)

    def _update_failure_messages(self, messages: List[str]) -> None:
        if messages:
            self._failure_messages.extend(m for m in messages if m)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_order(self) -> None:
        """Run all invariant checks before persisting."""
        self._validate_initial_order()
        self._validate_total_price()
        self._validate_items_price()

    def _validate_initial_order(self) -> None:
        if self._status is not None or self._id is not None:
            raise DomainException("Order is not in a valid state for initialization")

    def _validate_total_price(self) -> None:
        if not self._price.is_greater_than_zero():
            raise DomainException("Total price must be greater than zero")

    def _validate_items_price(self) -> None:
        items_total = Money.zero()
        for item in self._items:
            if not item.is_price_valid():
                raise DomainException(
                    f"OrderItem price {item.price} * quantity {item.quantity} "
                    f"!= sub_total {item.sub_total}"
                )
            items_total = items_total.add(item.sub_total)
        if not items_total.equals(self._price):
            raise DomainException(
                f"Order total {self._price} != sum of item sub-totals {items_total}"
            )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def customer_id(self) -> CustomerId:
        return self._customer_id

    @property
    def restaurant_id(self) -> RestaurantId:
        return self._restaurant_id

    @property
    def delivery_address(self) -> StreetAddress:
        return self._delivery_address

    @property
    def price(self) -> Money:
        return self._price

    @property
    def items(self) -> List[OrderItem]:
        return list(self._items)

    @property
    def tracking_id(self) -> Optional[TrackingId]:
        return self._tracking_id

    @property
    def status(self) -> Optional[OrderStatus]:
        return self._status

    @property
    def failure_messages(self) -> List[str]:
        return list(self._failure_messages)


# ---------------------------------------------------------------------------
# Restaurant / Product read model (used for validation during order creation)
# ---------------------------------------------------------------------------

class Product(BaseEntity[ProductId]):
    """Product entity — used as a read model within Order Service."""

    def __init__(
        self,
        product_id: ProductId,
        name: str,
        price: Money,
    ) -> None:
        super().__init__(product_id)
        self._name = name
        self._price = price

    def update_with_confirmed_name_price(self, name: str, price: Money) -> None:
        self._name = name
        self._price = price

    @property
    def name(self) -> str:
        return self._name

    @property
    def price(self) -> Money:
        return self._price


class Restaurant(AggregateRoot[RestaurantId]):
    """Restaurant aggregate — read model used for order validation."""

    def __init__(
        self,
        restaurant_id: RestaurantId,
        products: List[Product],
        active: bool = True,
    ) -> None:
        super().__init__(restaurant_id)
        self._products = products
        self._active = active

    @property
    def products(self) -> List[Product]:
        return list(self._products)

    @property
    def active(self) -> bool:
        return self._active
