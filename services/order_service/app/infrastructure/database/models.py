"""
Order Service — SQLAlchemy ORM Models
========================================
Defines all tables in the 'order' PostgreSQL schema.

Schema note: 'order' is a reserved SQL keyword.
We use schema='order' and quote it consistently.
SQLAlchemy handles the quoting automatically when schema is specified.

Tables:
  order.orders
  order.order_items
  order.order_address
  order.payment_outbox
  order.restaurant_approval_outbox
  order.customers (replicated read model)
"""
from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


SCHEMA = "order"


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Order status enum (mirrors domain)
# ---------------------------------------------------------------------------

class OrderStatusEnum(str, enum.Enum):
    PENDING = "PENDING"
    PAID = "PAID"
    APPROVED = "APPROVED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


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


class PaymentOrderStatusEnum(str, enum.Enum):
    PENDING = "PENDING"
    CANCELLED = "CANCELLED"


class RestaurantOrderStatusEnum(str, enum.Enum):
    PAID = "PAID"


# ---------------------------------------------------------------------------
# ORM Models
# ---------------------------------------------------------------------------

class OrderModel(Base):
    __tablename__ = "orders"
    __table_args__ = (
        CheckConstraint("price > 0", name="ck_order_price_positive"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    customer_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    restaurant_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    tracking_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), nullable=False, unique=True, index=True
    )
    price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    status: Mapped[str] = mapped_column(
        Enum(OrderStatusEnum, name="order_status", schema=SCHEMA), nullable=False
    )
    failure_messages: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    items: Mapped[List["OrderItemModel"]] = relationship(
        "OrderItemModel", back_populates="order", cascade="all, delete-orphan"
    )
    address: Mapped["OrderAddressModel"] = relationship(
        "OrderAddressModel", back_populates="order", cascade="all, delete-orphan", uselist=False
    )


class OrderItemModel(Base):
    __tablename__ = "order_items"
    __table_args__ = (
        CheckConstraint("price > 0", name="ck_order_item_price_positive"),
        CheckConstraint("sub_total > 0", name="ck_order_item_subtotal_positive"),
        {"schema": SCHEMA},
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.orders.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    product_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False)
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    price: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    sub_total: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    product_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    order: Mapped["OrderModel"] = relationship("OrderModel", back_populates="items")


class OrderAddressModel(Base):
    __tablename__ = "order_address"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    order_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        ForeignKey(f"{SCHEMA}.orders.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    )
    street: Mapped[str] = mapped_column(String(255), nullable=False)
    postal_code: Mapped[str] = mapped_column(String(20), nullable=False)
    city: Mapped[str] = mapped_column(String(100), nullable=False)

    order: Mapped["OrderModel"] = relationship("OrderModel", back_populates="address")


class PaymentOutboxModel(Base):
    """
    Payment outbox records published to the payment-request topic.

    Optimistic concurrency via 'version' column.
    Status transitions: STARTED → COMPLETED | FAILED
    """
    __tablename__ = "payment_outbox"
    __table_args__ = (
        UniqueConstraint("saga_id", "saga_status", name="uq_payment_outbox_saga_id_status"),
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
        Enum(OutboxStatusEnum, name="outbox_status_payment", schema=SCHEMA),
        nullable=False,
        default=OutboxStatusEnum.STARTED,
    )
    saga_status: Mapped[str] = mapped_column(
        Enum(SagaStatusEnum, name="saga_status_payment", schema=SCHEMA),
        nullable=False,
    )
    payment_status: Mapped[str] = mapped_column(
        Enum(PaymentOrderStatusEnum, name="payment_order_status", schema=SCHEMA),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class RestaurantApprovalOutboxModel(Base):
    """
    Restaurant approval outbox records published to restaurant-approval-request topic.
    """
    __tablename__ = "restaurant_approval_outbox"
    __table_args__ = (
        UniqueConstraint(
            "saga_id", "saga_status",
            name="uq_restaurant_approval_outbox_saga_id_status"
        ),
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
        Enum(OutboxStatusEnum, name="outbox_status_restaurant", schema=SCHEMA),
        nullable=False,
        default=OutboxStatusEnum.STARTED,
    )
    saga_status: Mapped[str] = mapped_column(
        Enum(SagaStatusEnum, name="saga_status_restaurant", schema=SCHEMA),
        nullable=False,
    )
    approval_status: Mapped[str] = mapped_column(
        Enum(RestaurantOrderStatusEnum, name="restaurant_order_status", schema=SCHEMA),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class CustomerReadModel(Base):
    """
    Local replicated read model of customers consumed from the customer topic.
    This is NOT the authoritative customer database.
    """
    __tablename__ = "customers"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    username: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    first_name: Mapped[str] = mapped_column(String(255), nullable=False)
    last_name: Mapped[str] = mapped_column(String(255), nullable=False)
    replicated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
