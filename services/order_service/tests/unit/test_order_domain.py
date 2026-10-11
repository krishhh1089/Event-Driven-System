"""
Order Service — Domain Unit Tests
==================================
Tests for Order aggregate root, lifecycle state transitions, and price invariants.
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from common_domain import (
    CustomerId,
    DomainException,
    Money,
    OrderId,
    ProductId,
    RestaurantId,
    StreetAddress,
)
from services.order_service.app.domain.entities import (
    Order,
    OrderItem,
    OrderItemId,
    OrderStatus,
)
from services.order_service.app.domain.order_domain_service import OrderDomainService


def _create_sample_order() -> Order:
    p_id = ProductId(uuid4())
    item = OrderItem(
        item_id=OrderItemId(uuid4()),
        order_id=None,
        product_id=p_id,
        quantity=2,
        price=Money.of("15.00"),
        sub_total=Money.of("30.00"),
    )
    address = StreetAddress(id=uuid4(), street="123 Main St", postal_code="10001", city="NYC")
    order = Order(
        order_id=OrderId(uuid4()),
        customer_id=CustomerId(uuid4()),
        restaurant_id=RestaurantId(uuid4()),
        delivery_address=address,
        price=Money.of("30.00"),
        items=[item],
    )
    order.initialize_order()
    return order


def test_order_initialization():
    order = _create_sample_order()
    assert order.status == OrderStatus.PENDING
    assert order.tracking_id is not None
    assert len(order.items) == 1
    assert order.items[0].order_id == order.id


def test_order_payment_transition():
    order = _create_sample_order()
    svc = OrderDomainService()
    svc.pay_order(order)
    assert order.status == OrderStatus.PAID


def test_order_approval_transition():
    order = _create_sample_order()
    svc = OrderDomainService()
    svc.pay_order(order)
    svc.approve_order(order)
    assert order.status == OrderStatus.APPROVED


def test_order_compensation_saga_flow():
    """
    Saga compensation:
    When restaurant rejects after payment was made:
    PAID -> CANCELLING (refund requested) -> CANCELLED (refund confirmed).
    """
    order = _create_sample_order()
    svc = OrderDomainService()
    svc.pay_order(order)
    assert order.status == OrderStatus.PAID

    svc.cancel_order_payment(order, ["Restaurant out of ingredients"])
    assert order.status == OrderStatus.CANCELLING

    svc.cancel_order(order, ["Refund completed"])
    assert order.status == OrderStatus.CANCELLED
    assert "Restaurant out of ingredients" in order.failure_messages


def test_invalid_state_transition():
    order = _create_sample_order()
    svc = OrderDomainService()
    # Cannot approve directly from PENDING without payment
    with pytest.raises(DomainException, match="cannot transition"):
        svc.approve_order(order)
