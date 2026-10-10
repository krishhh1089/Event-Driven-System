"""
Customer Service — Domain Layer
================================
Customer aggregate root, domain events, and CustomerDomainService.
"""
from __future__ import annotations

import enum
import logging
from datetime import datetime, timezone
from uuid import UUID

from common_domain import AggregateRoot, BaseId, CustomerId, DomainEvent, DomainException

logger = logging.getLogger(__name__)


class Customer(AggregateRoot[CustomerId]):
    """
    Customer aggregate root.
    Owns: username, first_name, last_name.
    """

    def __init__(
        self,
        customer_id: CustomerId,
        username: str,
        first_name: str,
        last_name: str,
    ) -> None:
        super().__init__(customer_id)
        self._username = username
        self._first_name = first_name
        self._last_name = last_name

    @property
    def username(self) -> str:
        return self._username

    @property
    def first_name(self) -> str:
        return self._first_name

    @property
    def last_name(self) -> str:
        return self._last_name

    def validate(self) -> None:
        if not self._username:
            raise DomainException("Username cannot be empty")
        if not self._first_name:
            raise DomainException("First name cannot be empty")
        if not self._last_name:
            raise DomainException("Last name cannot be empty")


class CustomerCreatedEvent(DomainEvent):
    def __init__(self, customer: Customer) -> None:
        super().__init__()
        self.customer = customer


class CustomerDomainService:
    """Pure domain service for customer operations."""

    def validate_and_create_customer(self, customer: Customer) -> CustomerCreatedEvent:
        customer.validate()
        logger.info("Customer %s validated successfully", customer.id)
        return CustomerCreatedEvent(customer)
