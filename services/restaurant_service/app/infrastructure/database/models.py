"""
Restaurant Service — SQLAlchemy ORM Models
=============================================
Tables in the 'restaurant' PostgreSQL schema.

Tables:
  restaurant.restaurants           — restaurant catalog (active/inactive)
  restaurant.products              — product catalog
  restaurant.restaurant_products   — M2M join (which products a restaurant offers)
  restaurant.order_approvals       — approval records per order (idempotency)
  restaurant.restaurant_approval_outbox — outbox for approval-response events
"""
from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


SCHEMA = "restaurant"


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class OrderApprovalStatusEnum(str, enum.Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class OutboxStatusEnum(str, enum.Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class SagaStatusEnum(str, enum.Enum):
    STARTED = "STARTED"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"


# ---------------------------------------------------------------------------
# ORM Models
# ---------------------------------------------------------------------------

class RestaurantModel(Base):
    __tablename__ = "restaurants"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    restaurant_products = relationship("RestaurantProductModel", back_populates="restaurant")


class ProductModel(Base):
    __tablename__ = "products"
    __table_args__ = (
        CheckConstraint("price > 0", name="ck_product_price_positive"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    price: Mapped[float] = mapped_column(Numeric(19, 2), nullable=False)
    available: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    restaurant_products = relationship("RestaurantProductModel", back_populates="product")


class RestaurantProductModel(Base):
    """Join table: which products each restaurant offers."""
    __tablename__ = "restaurant_products"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    restaurant_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.restaurants.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    product_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.products.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    restaurant = relationship("RestaurantModel", back_populates="restaurant_products")
    product = relationship("ProductModel", back_populates="restaurant_products")


class OrderApprovalModel(Base):
    """
    Records the approval decision for a specific order.
    Idempotency: (order_id) is unique — same order cannot be approved/rejected twice.
    """
    __tablename__ = "order_approvals"
    __table_args__ = (
        UniqueConstraint("order_id", name="uq_order_approval_order_id"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    restaurant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, index=True)
    approval_status: Mapped[str] = mapped_column(
        Enum(OrderApprovalStatusEnum, name="order_approval_status", schema=SCHEMA),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class RestaurantApprovalOutboxModel(Base):
    """
    Outbox for restaurant-approval-response events.
    Published to 'restaurant-approval-response' topic.
    """
    __tablename__ = "restaurant_approval_outbox"
    __table_args__ = (
        UniqueConstraint("saga_id", "saga_status", name="uq_restaurant_approval_outbox_saga_status"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    saga_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    outbox_status: Mapped[str] = mapped_column(
        Enum(OutboxStatusEnum, name="restaurant_approval_outbox_status", schema=SCHEMA),
        nullable=False,
        default=OutboxStatusEnum.STARTED,
    )
    saga_status: Mapped[str] = mapped_column(
        Enum(SagaStatusEnum, name="restaurant_approval_saga_status", schema=SCHEMA),
        nullable=False,
    )
    approval_status: Mapped[str] = mapped_column(
        Enum(OrderApprovalStatusEnum, name="order_approval_status", schema=SCHEMA),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
