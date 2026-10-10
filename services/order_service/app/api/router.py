"""
Order Service — FastAPI Router
================================
Exposes:
  POST /orders        — Create a new order
  GET  /orders/{trackingId} — Track an existing order
  GET  /health        — Health check
"""
from __future__ import annotations

import logging
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from common_application import (
    CustomerNotFoundException,
    OrderNotFoundException,
    RestaurantNotFoundException,
    OrderDomainException,
)
from common_domain import DomainException

from ..application.commands import (
    CreateOrderCommand,
    OrderAddressCommand,
    OrderItemCommand,
    TrackOrderQuery,
)
from ..application.create_order_handler import CreateOrderCommandHandler
from ..domain.order_domain_service import OrderDomainService
from ..infrastructure.database.repositories import (
    SqlAlchemyCustomerRepository,
    SqlAlchemyOrderRepository,
    SqlAlchemyRestaurantRepository,
)
from .schemas import CreateOrderRequest, CreateOrderResponse, TrackOrderResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/orders", tags=["orders"])


def _get_session():
    """Placeholder — actual DI configured in main.py."""
    raise RuntimeError("Session factory not configured")


@router.post(
    "",
    response_model=CreateOrderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new food order",
)
async def create_order(
    body: CreateOrderRequest,
    session: AsyncSession = Depends(_get_session),
) -> CreateOrderResponse:
    """
    Submit a new food order.

    - Validates customer existence in the local read model.
    - Validates restaurant and product catalog.
    - Creates the order aggregate and inserts the payment outbox record atomically.
    - Returns a tracking ID for polling order status.

    **Note:** If the customer was just created, their event may not yet have been
    replicated to the order service read model. In this case, retry after a brief delay.
    """
    command = CreateOrderCommand(
        customer_id=body.customer_id,
        restaurant_id=body.restaurant_id,
        address=OrderAddressCommand(
            street=body.address.street,
            postal_code=body.address.postal_code,
            city=body.address.city,
        ),
        price=Decimal(body.price),
        items=[
            OrderItemCommand(
                product_id=item.product_id,
                quantity=item.quantity,
                price=Decimal(item.price),
                sub_total=Decimal(item.sub_total),
            )
            for item in body.items
        ],
    )

    handler = CreateOrderCommandHandler(
        order_domain_service=OrderDomainService(),
        order_repository=SqlAlchemyOrderRepository(session),
        customer_repository=SqlAlchemyCustomerRepository(session),
        restaurant_repository=SqlAlchemyRestaurantRepository(session),
    )

    try:
        async with session.begin():
            result = await handler.handle(command)
    except CustomerNotFoundException as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=exc.message)
    except RestaurantNotFoundException as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=exc.message)
    except (DomainException, OrderDomainException) as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    return CreateOrderResponse(
        tracking_id=result["tracking_id"],
        order_status=result["order_status"],
    )


@router.get(
    "/{tracking_id}",
    response_model=TrackOrderResponse,
    summary="Track an existing order by tracking ID",
)
async def track_order(
    tracking_id: UUID,
    session: AsyncSession = Depends(_get_session),
) -> TrackOrderResponse:
    """
    Poll the current status of an order.

    Status transitions are driven by Kafka events and are eventually consistent:
    - PENDING:    Payment not yet confirmed
    - PAID:       Payment confirmed, awaiting restaurant approval
    - APPROVED:   Restaurant approved — order is complete
    - CANCELLING: Restaurant rejected — awaiting refund confirmation
    - CANCELLED:  Order is fully cancelled
    """
    repo = SqlAlchemyOrderRepository(session)
    order = await repo.find_by_tracking_id(tracking_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Order with tracking ID {tracking_id} not found",
        )
    return TrackOrderResponse(
        tracking_id=order.tracking_id.value,
        order_status=order.status.value,
        failure_messages=order.failure_messages or None,
    )
