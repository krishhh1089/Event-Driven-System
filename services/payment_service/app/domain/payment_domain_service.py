"""
Payment Service — Payment Domain Service
==========================================
Encapsulates multi-entity business rules for payment processing and cancellation.
Has no infrastructure dependencies — pure Python.

Process flow:
  1. CompletePayment:
     - Debit customer credit entry.
     - Append DEBIT credit history record.
     - Set payment status COMPLETED.

  2. CancelPayment (refund):
     - Credit customer credit entry.
     - Append CREDIT credit history record.
     - Set payment status CANCELLED.

If any domain invariant fails, the service raises DomainException.
The caller is responsible for persisting the changes atomically.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import List

from common_domain import (
    CustomerId,
    DomainException,
    Money,
    PaymentId,
    OrderId,
)

from .entities import (
    CreditEntry,
    CreditHistory,
    CreditHistoryId,
    Payment,
    PaymentStatus,
    TransactionType,
)

logger = logging.getLogger(__name__)


class PaymentDomainService:
    """Pure domain service for payment operations."""

    def complete_payment(
        self,
        payment: Payment,
        credit_entry: CreditEntry,
        credit_history_list: List[CreditHistory],
    ) -> None:
        """
        Validate and complete a payment (debit flow).

        Raises DomainException if:
        - Payment price is invalid.
        - Customer has insufficient credit.
        - Credit history records are inconsistent (over-debit detected).
        """
        payment.validate_payment()
        self._validate_credit_history(payment.customer_id, credit_entry, credit_history_list)
        credit_entry.subtract_credit_amount(payment.price)
        payment.payment_status = PaymentStatus.COMPLETED
        self._update_credit_history(payment.price, TransactionType.DEBIT, credit_history_list)
        logger.info(
            "Payment COMPLETED | order_id=%s customer_id=%s amount=%s",
            payment.order_id,
            payment.customer_id,
            payment.price,
        )

    def cancel_payment(
        self,
        payment: Payment,
        credit_entry: CreditEntry,
        credit_history_list: List[CreditHistory],
    ) -> None:
        """
        Cancel / refund a payment (credit flow).
        """
        payment.validate_payment()
        credit_entry.add_credit_amount(payment.price)
        payment.payment_status = PaymentStatus.CANCELLED
        self._update_credit_history(payment.price, TransactionType.CREDIT, credit_history_list)
        logger.info(
            "Payment CANCELLED (refunded) | order_id=%s customer_id=%s amount=%s",
            payment.order_id,
            payment.customer_id,
            payment.price,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _validate_credit_history(
        self,
        customer_id: CustomerId,
        credit_entry: CreditEntry,
        credit_history_list: List[CreditHistory],
    ) -> None:
        """
        Cross-check that the credit entry's current balance matches
        the sum of all history records.  This catches ledger inconsistencies
        that could allow an overdraft to slip through.
        """
        total_credit = Money.zero()
        total_debit = Money.zero()

        for history in credit_history_list:
            if history.transaction_type == TransactionType.CREDIT:
                total_credit = total_credit.add(history.amount)
            elif history.transaction_type == TransactionType.DEBIT:
                total_debit = total_debit.add(history.amount)

        expected_balance = total_credit.subtract(total_debit)
        if not expected_balance.equals(credit_entry.total_credit_amount):
            logger.error(
                "Credit history mismatch for customer %s: "
                "history_balance=%s, entry_balance=%s",
                customer_id,
                expected_balance,
                credit_entry.total_credit_amount,
            )
            raise DomainException(
                f"Credit history inconsistency for customer {customer_id}: "
                f"expected balance {expected_balance}, "
                f"actual balance {credit_entry.total_credit_amount}"
            )

    def _update_credit_history(
        self,
        amount: Money,
        transaction_type: TransactionType,
        credit_history_list: List[CreditHistory],
    ) -> None:
        """Append a new history record to the in-memory list (to be persisted by caller)."""
        from uuid import uuid4
        new_history = CreditHistory(
            credit_history_id=CreditHistoryId(uuid4()),
            customer_id=None,  # will be set when persisting
            amount=amount,
            transaction_type=transaction_type,
        )
        credit_history_list.append(new_history)
