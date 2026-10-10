"""
Common Data-Access Layer
========================
Repository abstractions, database utilities, and shared persistence helpers.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, List, Optional, TypeVar
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase


# ---------------------------------------------------------------------------
# SQLAlchemy declarative base
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    """Shared declarative base for all ORM models."""
    pass


# ---------------------------------------------------------------------------
# Repository protocol / ABC
# ---------------------------------------------------------------------------

T = TypeVar("T")
ID = TypeVar("ID")


class Repository(Generic[T, ID], ABC):
    """
    Generic repository port.
    Concrete implementations live in each service's infrastructure layer.
    """

    @abstractmethod
    async def save(self, entity: T) -> T:
        ...

    @abstractmethod
    async def find_by_id(self, entity_id: ID) -> Optional[T]:
        ...


# ---------------------------------------------------------------------------
# Database session factory helper
# ---------------------------------------------------------------------------

def build_async_engine(database_url: str):
    """Build a SQLAlchemy async engine from a connection string."""
    return create_async_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        echo=False,
    )


def build_session_factory(engine) -> async_sessionmaker[AsyncSession]:
    """Return an async session factory bound to *engine*."""
    return async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
