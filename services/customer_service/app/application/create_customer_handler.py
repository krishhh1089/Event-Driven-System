"""
Customer Service — Application Layer
======================================
Command handler for customer creation, including outbox insertion.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from common_application import ApplicationException
from common_domain import CustomerId

from ..domain import Customer, CustomerDomainService
from ..infrastructure.database.models import CustomerModel, CustomerOutboxModel, OutboxStatusEnum

logger = logging.getLogger(__name__)


class CustomerAlreadyExistsException(ApplicationException):
    pass


class CreateCustomerCommandHandler:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._domain_service = CustomerDomainService()

    async def handle(self, customer_id: UUID, username: str, first_name: str, last_name: str) -> dict:
        # Check uniqueness
        from sqlalchemy import select
        existing = await self._session.execute(
            select(CustomerModel.id).where(CustomerModel.id == customer_id)
        )
        if existing.scalar_one_or_none() is not None:
            logger.warning("Customer %s already exists, idempotent response", customer_id)
            return {"customer_id": str(customer_id)}

        # Build domain aggregate
        customer = Customer(
            customer_id=CustomerId(customer_id),
            username=username,
            first_name=first_name,
            last_name=last_name,
        )
        event = self._domain_service.validate_and_create_customer(customer)

        # Persist customer
        customer_model = CustomerModel(
            id=customer_id,
            username=username,
            first_name=first_name,
            last_name=last_name,
        )
        self._session.add(customer_model)

        # Insert outbox record atomically
        outbox = CustomerOutboxModel(
            id=uuid4(),
            customer_id=customer_id,
            type="CustomerCreatedEvent",
            payload={
                "id": str(customer_id),
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "created_at": datetime.now(timezone.utc).isoformat(),
            },
            outbox_status=OutboxStatusEnum.STARTED,
            version=0,
        )
        self._session.add(outbox)

        logger.info("Customer %s created, outbox record inserted", customer_id)
        return {"customer_id": str(customer_id)}
