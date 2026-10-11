"""
Payment Service — Domain Layer
================================
Defines the Payment aggregate and CreditEntry / CreditHistory value objects.
No infrastructure dependencies here — pure Python business logic.

Design decisions:
- Payment encodes idempotency expectations (COMPLETED once per order_id).
- CreditEntry is the authoritative current balance per customer.
- CreditHistory is append-only.
- Balance debit is validated in the domain to prevent overdrafts.
"""
from __future__ import annotations

import enum
from decimal import Decimal
from typing import List
from uuid import UUID, uuid4

from common_domain import (
    AggregateRoot,
    BaseEntity,
    BaseId,
    CustomerId,
    DomainException,
    Money,
    OrderId,
    PaymentId,
)


# ---------------------------------------------------------------------------
# Payment status
# ---------------------------------------------------------------------------

class PaymentStatus(str, enum.Enum):
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    FAILED = "FAILED"


# ---------------------------------------------------------------------------
# Credit transaction types
# ---------------------------------------------------------------------------

class TransactionType(str, enum.Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


# ---------------------------------------------------------------------------
# CreditEntry aggregate — current balance
# ---------------------------------------------------------------------------

class CreditEntryId(BaseId):
    """Typed UUID for CreditEntry."""


class CreditEntry(AggregateRoot[CreditEntryId]):
    """
    Current credit balance for a single customer.

    Invariant: total_credit_amount >= 0.
    All mutations are protected by a database-level SELECT … FOR UPDATE.
    """

    def __init__(
        self,
        credit_entry_id: CreditEntryId,
        customer_id: CustomerId,
        total_credit_amount: Money,
    ) -> None:
        super().__init__(credit_entry_id)
        self._customer_id = customer_id
        self._total_credit_amount = total_credit_amount

    # ------------------------------------------------------------------
    # Mutations
    # ------------------------------------------------------------------

    def add_credit_amount(self, amount: Money) -> None:
        """Credit (top-up) the balance."""
        if amount.is_negative() or amount.is_zero():
            raise DomainException("Credit amount must be positive")
        self._total_credit_amount = self._total_credit_amount.add(amount)

    def subtract_credit_amount(self, amount: Money) -> None:
        """Debit the balance; raise if the customer cannot afford it."""
        if amount.is_negative() or amount.is_zero():
            raise DomainException("Debit amount must be positive")
        new_balance = self._total_credit_amount.subtract(amount)
        if new_balance.is_negative():
            raise DomainException(
                f"Customer {self._customer_id} has insufficient credit: "
                f"balance={self._total_credit_amount}, debit={amount}"
            )
        self._total_credit_amount = new_balance

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def customer_id(self) -> CustomerId:
        return self._customer_id

    @property
    def total_credit_amount(self) -> Money:
        return self._total_credit_amount


# ---------------------------------------------------------------------------
# CreditHistory entity — one row per credit movement
# ---------------------------------------------------------------------------

class CreditHistoryId(BaseId):
    pass


class CreditHistory(BaseEntity[CreditHistoryId]):
    """Immutable record of a single credit or debit event."""

    def __init__(
        self,
        credit_history_id: CreditHistoryId,
        customer_id: CustomerId,
        amount: Money,
        transaction_type: TransactionType,
    ) -> None:
        super().__init__(credit_history_id)
        self._customer_id = customer_id
        self._amount = amount
        self._transaction_type = transaction_type

    @property
    def customer_id(self) -> CustomerId:
        return self._customer_id

    @property
    def amount(self) -> Money:
        return self._amount

    @property
    def transaction_type(self) -> TransactionType:
        return self._transaction_type


# ---------------------------------------------------------------------------
# Payment aggregate root
# ---------------------------------------------------------------------------

class Payment(AggregateRoot[PaymentId]):
    """
    Payment aggregate.

    Validates that the payment amount is positive.
    The payment status is set by the application service after
    business rules (balance check) are evaluated.
    """

    def __init__(
        self,
        payment_id: PaymentId,
        customer_id: CustomerId,
        order_id: OrderId,
        price: Money,
        payment_status: PaymentStatus | None = None,
    ) -> None:
        super().__init__(payment_id)
        self._customer_id = customer_id
        self._order_id = order_id
        self._price = price
        self._payment_status = payment_status
        self._failure_messages: List[str] = []

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_payment(self) -> None:
        self._validate_initial_payment()
        self._validate_total_price()

    def _validate_initial_payment(self) -> None:
        pass  # additional future invariants can be added here

    def _validate_total_price(self) -> None:
        if not self._price.is_greater_than_zero():
            raise DomainException("Total price must be greater than zero")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def customer_id(self) -> CustomerId:
        return self._customer_id

    @property
    def order_id(self) -> OrderId:
        return self._order_id

    @property
    def price(self) -> Money:
        return self._price

    @property
    def payment_status(self) -> PaymentStatus | None:
        return self._payment_status

    @payment_status.setter
    def payment_status(self, status: PaymentStatus) -> None:
        self._payment_status = status

    @property
    def failure_messages(self) -> List[str]:
        return list(self._failure_messages)

    def add_failure_message(self, msg: str) -> None:
        self._failure_messages.append(msg)
