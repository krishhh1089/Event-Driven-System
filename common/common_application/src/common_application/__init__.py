"""
Common Application Layer
========================
Shared exception types, error DTOs, and application-level abstractions.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional
from uuid import UUID


# ---------------------------------------------------------------------------
# Application exception hierarchy
# ---------------------------------------------------------------------------

class ApplicationException(Exception):
    """Base class for application-layer exceptions."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class OrderNotFoundException(ApplicationException):
    """Raised when an order cannot be found."""


class CustomerNotFoundException(ApplicationException):
    """Raised when a customer cannot be found."""


class RestaurantNotFoundException(ApplicationException):
    """Raised when a restaurant cannot be found."""


class PaymentNotFoundException(ApplicationException):
    """Raised when a payment cannot be found."""


class OrderDomainException(ApplicationException):
    """Raised for order domain rule violations."""


class SagaException(ApplicationException):
    """Raised when a Saga is in an unexpected state."""


# ---------------------------------------------------------------------------
# Error DTO
# ---------------------------------------------------------------------------

@dataclass
class ErrorDTO:
    """Structured error response returned by REST APIs."""
    code: str
    message: str
    details: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message, "details": self.details}
