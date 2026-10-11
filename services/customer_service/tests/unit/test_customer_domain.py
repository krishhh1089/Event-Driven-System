"""
Customer Service — Domain Unit Tests
====================================
Tests for Customer aggregate validation and domain service event creation.
"""
from uuid import uuid4

import pytest

from common_domain import CustomerId, DomainException
from services.customer_service.app.domain import (
    Customer,
    CustomerCreatedEvent,
    CustomerDomainService,
)


def test_customer_creation_valid():
    svc = CustomerDomainService()
    cid = CustomerId(uuid4())
    customer = Customer(
        customer_id=cid,
        username="alice",
        first_name="Alice",
        last_name="Smith",
    )
    event = svc.validate_and_create_customer(customer)
    assert isinstance(event, CustomerCreatedEvent)
    assert event.customer.id == cid
    assert event.customer.username == "alice"


def test_customer_creation_empty_username_raises():
    svc = CustomerDomainService()
    cid = CustomerId(uuid4())
    customer = Customer(
        customer_id=cid,
        username="",
        first_name="Alice",
        last_name="Smith",
    )
    with pytest.raises(DomainException, match="Username cannot be empty"):
        svc.validate_and_create_customer(customer)


def test_customer_creation_empty_name_raises():
    svc = CustomerDomainService()
    cid = CustomerId(uuid4())
    customer = Customer(
        customer_id=cid,
        username="alice",
        first_name="",
        last_name="Smith",
    )
    with pytest.raises(DomainException, match="First name cannot be empty"):
        svc.validate_and_create_customer(customer)
