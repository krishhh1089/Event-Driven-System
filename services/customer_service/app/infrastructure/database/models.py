"""
Customer Service — SQLAlchemy ORM Models
==========================================
Tables in the 'customer' PostgreSQL schema.
"""
from __future__ import annotations

import enum
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import DateTime, Enum, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB, UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

SCHEMA = "customer"


class Base(DeclarativeBase):
    pass


class OutboxStatusEnum(str, enum.Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CustomerModel(Base):
    __tablename__ = "customers"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    username: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    first_name: Mapped[str] = mapped_column(String(255), nullable=False)
    last_name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )


class CustomerOutboxModel(Base):
    """
    Outbox for customer-created events.
    Published to the 'customer' Kafka topic.
    """
    __tablename__ = "customer_outbox"
    __table_args__ = {"schema": SCHEMA}

    id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True, default=uuid4)
    customer_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    type: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    outbox_status: Mapped[str] = mapped_column(
        Enum(OutboxStatusEnum, name="customer_outbox_status", schema=SCHEMA),
        nullable=False,
        default=OutboxStatusEnum.STARTED,
    )
    version: Mapped[int] = mapped_column(nullable=False, default=0)
