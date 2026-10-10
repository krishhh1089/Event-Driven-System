"""
Saga Infrastructure
===================
Shared Saga abstractions: step interface, status enum, saga lifecycle.

Design decisions
----------------
- SagaStatus is an enum so it can be stored as a string in PostgreSQL.
- SagaStep is a Protocol; concrete sagas implement it.
- The lifecycle transitions are enforced by the Saga base class so
  individual steps cannot misuse state.
"""
from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar
from uuid import UUID

R = TypeVar("R")   # saga response type


# ---------------------------------------------------------------------------
# Saga status
# ---------------------------------------------------------------------------

class SagaStatus(str, enum.Enum):
    STARTED = "STARTED"
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"


# ---------------------------------------------------------------------------
# Outbox status (shared between all outbox tables)
# ---------------------------------------------------------------------------

class OutboxStatus(str, enum.Enum):
    STARTED = "STARTED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Saga step protocol / ABC
# ---------------------------------------------------------------------------

class SagaStep(ABC, Generic[R]):
    """
    Abstract base for a single step in a saga.

    ``process`` advances the saga forward.
    ``rollback`` compensates (rolls back) the step.
    """

    @abstractmethod
    def process(self, response: R) -> None:
        """Execute the forward action of this step."""
        ...

    @abstractmethod
    def rollback(self, response: R) -> None:
        """Execute the compensating action of this step."""
        ...
