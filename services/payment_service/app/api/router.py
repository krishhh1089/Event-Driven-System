"""
Payment Service — REST API
============================
Exposes a simple management API for:
  GET  /health           — liveness check
  GET  /health/readiness — readiness check (DB)
  POST /customers/{customer_id}/credits — initialize or top-up customer credit
  GET  /customers/{customer_id}/credits — view current credit balance
"""
from __future__ import annotations

import logging
from decimal import Decimal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..infrastructure.database.models import (
    CreditEntryModel,
    CreditHistoryModel,
    CreditEntryTransactionTypeEnum,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["payment"])


def _get_session():
    raise RuntimeError("Session factory not configured")


class TopUpCreditRequest(BaseModel):
    amount: Decimal = Field(..., gt=0, description="Amount to credit to the customer's account")


class CreditBalanceResponse(BaseModel):
    customer_id: UUID
    total_credit_amount: Decimal


@router.post(
    "/customers/{customer_id}/credits",
    status_code=status.HTTP_200_OK,
    response_model=CreditBalanceResponse,
    summary="Initialize or top-up customer credit",
)
async def top_up_credit(
    customer_id: UUID,
    body: TopUpCreditRequest,
    session: AsyncSession = Depends(_get_session),
) -> CreditBalanceResponse:
    """
    Initialize or increase a customer's credit balance.
    Creates a credit_entries record if none exists.
    Appends a CREDIT history record.
    """
    async with session.begin():
        stmt = select(CreditEntryModel).where(
            CreditEntryModel.customer_id == customer_id
        ).with_for_update()
        result = await session.execute(stmt)
        entry = result.scalar_one_or_none()

        if entry is None:
            entry = CreditEntryModel(
                id=uuid4(),
                customer_id=customer_id,
                total_credit_amount=body.amount,
            )
            session.add(entry)
        else:
            from sqlalchemy import update
            await session.execute(
                update(CreditEntryModel)
                .where(CreditEntryModel.id == entry.id)
                .values(
                    total_credit_amount=CreditEntryModel.total_credit_amount + body.amount,
                    version=CreditEntryModel.version + 1,
                )
            )
            await session.refresh(entry)

        # Append credit history
        history = CreditHistoryModel(
            id=uuid4(),
            customer_id=customer_id,
            amount=body.amount,
            transaction_type=CreditEntryTransactionTypeEnum.CREDIT,
        )
        session.add(history)

    return CreditBalanceResponse(
        customer_id=customer_id,
        total_credit_amount=entry.total_credit_amount,
    )


@router.get(
    "/customers/{customer_id}/credits",
    response_model=CreditBalanceResponse,
    summary="Get current credit balance for a customer",
)
async def get_credit_balance(
    customer_id: UUID,
    session: AsyncSession = Depends(_get_session),
) -> CreditBalanceResponse:
    stmt = select(CreditEntryModel).where(CreditEntryModel.customer_id == customer_id)
    result = await session.execute(stmt)
    entry = result.scalar_one_or_none()
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No credit entry for customer {customer_id}",
        )
    return CreditBalanceResponse(
        customer_id=customer_id,
        total_credit_amount=entry.total_credit_amount,
    )
