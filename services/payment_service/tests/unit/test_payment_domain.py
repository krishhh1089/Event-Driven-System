"""
Payment Service — Domain Unit Tests
====================================
Tests for Payment aggregate, CreditEntry mutations, and PaymentDomainService.
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from common_domain import CustomerId, DomainException, Money, OrderId, PaymentId
from services.payment_service.app.domain.entities import (
    CreditEntry,
    CreditEntryId,
    CreditHistory,
    CreditHistoryId,
    Payment,
    PaymentStatus,
    TransactionType,
)
from services.payment_service.app.domain.payment_domain_service import PaymentDomainService


def test_credit_entry_subtract_success():
    customer_id = CustomerId(uuid4())
    entry = CreditEntry(
        credit_entry_id=CreditEntryId(uuid4()),
        customer_id=customer_id,
        total_credit_amount=Money(Decimal("100.00")),
    )
    entry.subtract_credit_amount(Money(Decimal("30.00")))
    assert entry.total_credit_amount.amount == Decimal("70.00")


def test_credit_entry_subtract_insufficient_balance():
    customer_id = CustomerId(uuid4())
    entry = CreditEntry(
        credit_entry_id=CreditEntryId(uuid4()),
        customer_id=customer_id,
        total_credit_amount=Money(Decimal("50.00")),
    )
    with pytest.raises(DomainException, match="insufficient credit"):
        entry.subtract_credit_amount(Money(Decimal("60.00")))


def test_payment_domain_service_complete_payment():
    svc = PaymentDomainService()
    cust_id = CustomerId(uuid4())
    order_id = OrderId(uuid4())
    payment_id = PaymentId(uuid4())

    entry = CreditEntry(
        credit_entry_id=CreditEntryId(uuid4()),
        customer_id=cust_id,
        total_credit_amount=Money(Decimal("100.00")),
    )
    history = [
        CreditHistory(
            credit_history_id=CreditHistoryId(uuid4()),
            customer_id=cust_id,
            amount=Money(Decimal("100.00")),
            transaction_type=TransactionType.CREDIT,
        )
    ]
    payment = Payment(
        payment_id=payment_id,
        customer_id=cust_id,
        order_id=order_id,
        price=Money(Decimal("45.00")),
    )

    svc.complete_payment(payment, entry, history)

    assert payment.payment_status == PaymentStatus.COMPLETED
    assert entry.total_credit_amount.amount == Decimal("55.00")
    assert len(history) == 2
    assert history[-1].transaction_type == TransactionType.DEBIT
    assert history[-1].amount.amount == Decimal("45.00")


def test_payment_domain_service_cancel_payment_refund():
    svc = PaymentDomainService()
    cust_id = CustomerId(uuid4())
    order_id = OrderId(uuid4())
    payment_id = PaymentId(uuid4())

    entry = CreditEntry(
        credit_entry_id=CreditEntryId(uuid4()),
        customer_id=cust_id,
        total_credit_amount=Money(Decimal("55.00")),
    )
    history = []
    payment = Payment(
        payment_id=payment_id,
        customer_id=cust_id,
        order_id=order_id,
        price=Money(Decimal("45.00")),
    )

    svc.cancel_payment(payment, entry, history)

    assert payment.payment_status == PaymentStatus.CANCELLED
    assert entry.total_credit_amount.amount == Decimal("100.00")
    assert len(history) == 1
    assert history[-1].transaction_type == TransactionType.CREDIT
    assert history[-1].amount.amount == Decimal("45.00")
