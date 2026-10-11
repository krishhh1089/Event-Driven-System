"""
Common Infrastructure — Unit Tests
===================================
Tests for Money value object, Avro schema loader, and Outbox retry utilities.
"""
import asyncio
from decimal import Decimal

import pytest

from common_domain import Money
from kafka_infrastructure import load_avro_schema
from outbox_infrastructure import OutboxStatus, retry_with_backoff


def test_money_value_object():
    m1 = Money.of("10.50")
    m2 = Money.of("4.25")

    m_sum = m1.add(m2)
    assert m_sum.amount == Decimal("14.75")

    m_diff = m1.subtract(m2)
    assert m_diff.amount == Decimal("6.25")

    m_mult = m2.multiply(3)
    assert m_mult.amount == Decimal("12.75")

    assert m1.is_greater_than(m2)
    assert not m2.is_greater_than(m1)
    assert not m1.is_zero()
    assert Money.zero().is_zero()


def test_avro_schemas_loading():
    schemas = [
        "customer.avsc",
        "payment_request.avsc",
        "payment_response.avsc",
        "restaurant_approval_request.avsc",
        "restaurant_approval_response.avsc",
    ]
    for schema_file in schemas:
        content = load_avro_schema(schema_file)
        assert len(content) > 0
        assert '"name":' in content


@pytest.mark.asyncio
async def test_retry_with_backoff_success_after_failure():
    attempts = 0

    async def flaky_operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("Temporary failure")
        return "success"

    result = await retry_with_backoff(
        flaky_operation,
        max_attempts=4,
        base_delay_s=0.01,
        max_delay_s=0.05,
    )
    assert result == "success"
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_with_backoff_exhausted():
    async def always_fails():
        raise RuntimeError("Permanent error")

    with pytest.raises(RuntimeError, match="Permanent error"):
        await retry_with_backoff(
            always_fails,
            max_attempts=3,
            base_delay_s=0.01,
            max_delay_s=0.05,
        )
