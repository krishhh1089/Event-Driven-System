"""
Restaurant Service — HTTP API Router
======================================
Endpoints for restaurant management, catalog seeding, and approval queries.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..infrastructure.database.models import (
    OrderApprovalModel,
    ProductModel,
    RestaurantModel,
    RestaurantProductModel,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/restaurants", tags=["restaurants"])


# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------

class CreateProductRequest(BaseModel):
    id: Optional[UUID] = None
    name: str = Field(..., min_length=1)
    price: Decimal = Field(..., gt=Decimal("0.00"))
    available: bool = True


class ProductResponse(BaseModel):
    id: UUID
    name: str
    price: str
    available: bool


class CreateRestaurantRequest(BaseModel):
    id: Optional[UUID] = None
    name: str = Field(..., min_length=1)
    active: bool = True
    products: Optional[List[CreateProductRequest]] = None


class RestaurantSummaryResponse(BaseModel):
    id: UUID
    name: str
    active: bool


class RestaurantDetailResponse(BaseModel):
    id: UUID
    name: str
    active: bool
    products: List[ProductResponse]


class OrderApprovalResponse(BaseModel):
    id: UUID
    restaurantId: UUID
    orderId: UUID
    approvalStatus: str
    createdAt: str


# ---------------------------------------------------------------------------
# Dependency Placeholder (overridden at runtime in main.py)
# ---------------------------------------------------------------------------

async def _get_session() -> AsyncSession:  # pragma: no cover
    raise NotImplementedError("Database session dependency not registered")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=RestaurantSummaryResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new restaurant",
)
async def create_restaurant(
    body: CreateRestaurantRequest,
    session: AsyncSession = Depends(_get_session),
):
    restaurant_id = body.id or uuid4()
    restaurant = RestaurantModel(
        id=restaurant_id,
        name=body.name,
        active=body.active,
    )
    session.add(restaurant)

    if body.products:
        for p in body.products:
            prod_id = p.id or uuid4()
            product = ProductModel(
                id=prod_id,
                name=p.name,
                price=p.price,
                available=p.available,
            )
            session.add(product)
            join_entry = RestaurantProductModel(
                restaurant_id=restaurant_id,
                product_id=prod_id,
            )
            session.add(join_entry)

    await session.commit()
    return RestaurantSummaryResponse(
        id=restaurant.id,
        name=restaurant.name,
        active=restaurant.active,
    )


@router.get(
    "",
    response_model=List[RestaurantSummaryResponse],
    summary="List all restaurants",
)
async def list_restaurants(
    session: AsyncSession = Depends(_get_session),
):
    stmt = select(RestaurantModel).order_by(RestaurantModel.name)
    result = await session.execute(stmt)
    records = result.scalars().all()
    return [
        RestaurantSummaryResponse(id=r.id, name=r.name, active=r.active)
        for r in records
    ]


@router.get(
    "/{restaurant_id}",
    response_model=RestaurantDetailResponse,
    summary="Get restaurant details with catalog products",
)
async def get_restaurant(
    restaurant_id: UUID,
    session: AsyncSession = Depends(_get_session),
):
    stmt = select(RestaurantModel).where(RestaurantModel.id == restaurant_id)
    result = await session.execute(stmt)
    restaurant = result.scalar_one_or_none()

    if restaurant is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Restaurant {restaurant_id} not found",
        )

    products_stmt = (
        select(ProductModel)
        .join(RestaurantProductModel, RestaurantProductModel.product_id == ProductModel.id)
        .where(RestaurantProductModel.restaurant_id == restaurant_id)
    )
    prod_result = await session.execute(products_stmt)
    products = prod_result.scalars().all()

    return RestaurantDetailResponse(
        id=restaurant.id,
        name=restaurant.name,
        active=restaurant.active,
        products=[
            ProductResponse(
                id=p.id,
                name=p.name,
                price=str(p.price),
                available=p.available,
            )
            for p in products
        ],
    )


@router.post(
    "/{restaurant_id}/products",
    response_model=ProductResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add a product to restaurant catalog",
)
async def add_product(
    restaurant_id: UUID,
    body: CreateProductRequest,
    session: AsyncSession = Depends(_get_session),
):
    # Verify restaurant exists
    r_stmt = select(RestaurantModel).where(RestaurantModel.id == restaurant_id)
    r_res = await session.execute(r_stmt)
    if r_res.scalar_one_or_none() is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Restaurant {restaurant_id} not found",
        )

    prod_id = body.id or uuid4()
    product = ProductModel(
        id=prod_id,
        name=body.name,
        price=body.price,
        available=body.available,
    )
    session.add(product)
    join_entry = RestaurantProductModel(
        restaurant_id=restaurant_id,
        product_id=prod_id,
    )
    session.add(join_entry)
    await session.commit()

    return ProductResponse(
        id=product.id,
        name=product.name,
        price=str(product.price),
        available=product.available,
    )


@router.get(
    "/{restaurant_id}/approvals",
    response_model=List[OrderApprovalResponse],
    summary="List order approval decisions for restaurant",
)
async def list_approvals(
    restaurant_id: UUID,
    session: AsyncSession = Depends(_get_session),
):
    stmt = (
        select(OrderApprovalModel)
        .where(OrderApprovalModel.restaurant_id == restaurant_id)
        .order_by(OrderApprovalModel.created_at.desc())
    )
    res = await session.execute(stmt)
    approvals = res.scalars().all()
    return [
        OrderApprovalResponse(
            id=a.id,
            restaurantId=a.restaurant_id,
            orderId=a.order_id,
            approvalStatus=a.approval_status.value if hasattr(a.approval_status, "value") else str(a.approval_status),
            createdAt=a.created_at.isoformat(),
        )
        for a in approvals
    ]
