"""
Restaurant Service — Domain Unit Tests
=======================================
Tests for Restaurant catalog and RestaurantDomainService order validation.
"""
from decimal import Decimal
from typing import Tuple
from uuid import uuid4

import pytest

from common_domain import Money, OrderId, ProductId, RestaurantId
from services.restaurant_service.app.domain.entities import (
    OrderApprovalStatus,
    Product,
    Restaurant,
)
from services.restaurant_service.app.domain.restaurant_domain_service import (
    RestaurantDomainService,
)


def _build_test_restaurant(active: bool = True) -> Tuple[Restaurant, ProductId, ProductId]:
    rid = RestaurantId(uuid4())
    p1_id = ProductId(uuid4())
    p2_id = ProductId(uuid4())

    p1 = Product(product_id=p1_id, name="Margherita", price=Money.of("12.50"), available=True)
    p2 = Product(product_id=p2_id, name="Tiramisu", price=Money.of("6.00"), available=False)

    restaurant = Restaurant(
        restaurant_id=rid,
        name="Mario's",
        active=active,
        products={p1_id: p1, p2_id: p2},
    )
    return restaurant, p1_id, p2_id


from typing import Tuple


def test_approve_valid_order():
    svc = RestaurantDomainService()
    restaurant, p1_id, _ = _build_test_restaurant(active=True)
    order_id = OrderId(uuid4())

    requested = [{"id": p1_id.value, "quantity": 2, "price": "12.50"}]
    approval, failure_messages = svc.validate_order(restaurant, order_id, requested)

    assert approval.approval_status == OrderApprovalStatus.APPROVED
    assert len(failure_messages) == 0


def test_reject_inactive_restaurant():
    svc = RestaurantDomainService()
    restaurant, p1_id, _ = _build_test_restaurant(active=False)
    order_id = OrderId(uuid4())

    requested = [{"id": p1_id.value, "quantity": 1, "price": "12.50"}]
    approval, failure_messages = svc.validate_order(restaurant, order_id, requested)

    assert approval.approval_status == OrderApprovalStatus.REJECTED
    assert any("inactive" in msg for msg in failure_messages)


def test_reject_unavailable_product():
    svc = RestaurantDomainService()
    restaurant, _, p2_id = _build_test_restaurant(active=True)
    order_id = OrderId(uuid4())

    requested = [{"id": p2_id.value, "quantity": 1, "price": "6.00"}]
    approval, failure_messages = svc.validate_order(restaurant, order_id, requested)

    assert approval.approval_status == OrderApprovalStatus.REJECTED
    assert any("out of stock" in msg for msg in failure_messages)


def test_reject_price_mismatch():
    svc = RestaurantDomainService()
    restaurant, p1_id, _ = _build_test_restaurant(active=True)
    order_id = OrderId(uuid4())

    requested = [{"id": p1_id.value, "quantity": 1, "price": "10.00"}]  # catalog is 12.50
    approval, failure_messages = svc.validate_order(restaurant, order_id, requested)

    assert approval.approval_status == OrderApprovalStatus.REJECTED
    assert any("price mismatch" in msg for msg in failure_messages)


def test_reject_unlisted_product():
    svc = RestaurantDomainService()
    restaurant, _, _ = _build_test_restaurant(active=True)
    order_id = OrderId(uuid4())
    unknown_id = uuid4()

    requested = [{"id": unknown_id, "quantity": 1, "price": "5.00"}]
    approval, failure_messages = svc.validate_order(restaurant, order_id, requested)

    assert approval.approval_status == OrderApprovalStatus.REJECTED
    assert any("not offered" in msg for msg in failure_messages)
