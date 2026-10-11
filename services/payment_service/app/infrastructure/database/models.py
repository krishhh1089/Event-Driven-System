"""
Payment Service — SQLAlchemy ORM Models
==========================================
Tables in the 'payment' PostgreSQL schema.

Tables:
  payment.credit_entries       — customer credit balance ledger (current balance)
  payment.credit_history       — append-only ledger of every credit movement
  payment.payments             — payment records (one per saga)
  payment.payment_outbox       — outbox for payment-response events
"""
from __future__ import annotations

import enum
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


SCHEMA = "payment"


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Status enums
# ---------------------------------------------------------------------------

class PaymentStatusEnum(str, enum.Enum):
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


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


class CreditEntryTransactionTypeEnum(str, enum.Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


# ---------------------------------------------------------------------------
# ORM Models
# ---------------------------------------------------------------------------

class CreditEntryModel(Base):
    """
    Current credit balance per customer.
    One row per customer.  Updated with SELECT ... FOR UPDATE to prevent
    concurrent overdrafts.
    """
    __tablename__ = "credit_entries"
    __table_args__ = (
        CheckConstraint("total_credit_amount >= 0", name="ck_credit_entry_non_negative"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    customer_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), nullable=False, unique=True, index=True
    )
    total_credit_amount: Mapped[float] = mapped_column(
        Numeric(19, 2), nullable=False, default=0
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class CreditHistoryModel(Base):
    """
    Append-only ledger of every credit movement.
    Each row records a single debit or credit event.
    """
    __tablename__ = "credit_history"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    customer_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True), nullable=False, index=True
    )
    amount: Mapped[float] = mapped_column(Numeric(19, 2), nullable=False)
    transaction_type: Mapped[str] = mapped_column(
        Enum(
            CreditEntryTransactionTypeEnum,
            name="credit_transaction_type",
            schema=SCHEMA,
        ),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class PaymentModel(Base):
    """
    Payment record — one per saga / order.

    Idempotency: (order_id, payment_status) has a unique constraint so the
    same order cannot be charged or refunded twice.
    """
    __tablename__ = "payments"
    __table_args__ = (
        UniqueConstraint("order_id", "payment_status", name="uq_payment_order_status"),
        CheckConstraint("price > 0", name="ck_payment_price_positive"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    customer_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, index=True)
    order_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, index=True)
    price: Mapped[float] = mapped_column(Numeric(19, 2), nullable=False)
    payment_status: Mapped[str] = mapped_column(
        Enum(PaymentStatusEnum, name="payment_status_enum", schema=SCHEMA),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class PaymentOutboxModel(Base):
    """
    Outbox for payment-response events published to the 'payment-response' topic.

    The publisher polls STARTED records and publishes; marks COMPLETED on ack.
    Idempotency: unique (saga_id, saga_status) prevents duplicate outbox entries.
    """
    __tablename__ = "payment_outbox"
    __table_args__ = (
        UniqueConstraint("saga_id", "saga_status", name="uq_payment_outbox_saga_status"),
        {"schema": SCHEMA},
    )

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    saga_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    processed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    outbox_status: Mapped[str] = mapped_column(
        Enum(OutboxStatusEnum, name="payment_outbox_status", schema=SCHEMA),
        nullable=False,
        default=OutboxStatusEnum.STARTED,
    )
    saga_status: Mapped[str] = mapped_column(
        Enum(SagaStatusEnum, name="payment_saga_status", schema=SCHEMA),
        nullable=False,
    )
    payment_status: Mapped[str] = mapped_column(
        Enum(PaymentStatusEnum, name="payment_outbox_payment_status", schema=SCHEMA),
        nullable=False,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
