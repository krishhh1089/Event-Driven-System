"""
Order Service — Order Payment Saga
====================================
Processes payment responses (COMPLETED, FAILED, CANCELLED).

Saga ID correlates the payment response to the originating order.

On COMPLETED:
  - Transition order PENDING → PAID
  - Create restaurant approval outbox record
  - Mark payment outbox as COMPLETED

On FAILED:
  - Transition order PENDING → CANCELLED
  - Mark payment outbox as FAILED
  - No restaurant approval sent

On CANCELLED (refund confirmed):
  - Transition order CANCELLING → CANCELLED
  - Mark payment outbox compensated
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List
from uuid import UUID

from saga_infrastructure import SagaStatus, SagaStep

logger = logging.getLogger(__name__)


class PaymentResponse:
    """Deserialized payment response event from Kafka."""

    def __init__(
        self,
        event_id: str,
        saga_id: str,
        payment_id: str,
        customer_id: str,
        order_id: str,
        price: str,
        payment_status: str,
        failure_messages: List[str],
    ) -> None:
        self.event_id = event_id
        self.saga_id = saga_id
        self.payment_id = payment_id
        self.customer_id = customer_id
        self.order_id = order_id
        self.price = price
        self.payment_status = payment_status
        self.failure_messages = failure_messages


class OrderPaymentSaga(SagaStep[PaymentResponse]):
    """
    Saga step that handles the payment phase of the order workflow.

    The actual persistence is delegated to the saga service (injected).
    This class encodes the business logic only.
    """

    def __init__(self, saga_service: Any) -> None:
        # saga_service is the concrete SagaApplicationService injected at runtime
        self._saga_service = saga_service

    def process(self, response: PaymentResponse) -> None:
        """Handle payment COMPLETED — advance order to PAID."""
        logger.info(
            "OrderPaymentSaga.process | saga_id=%s order_id=%s status=%s",
            response.saga_id,
            response.order_id,
            response.payment_status,
        )
        self._saga_service.process_payment_response(response)

    def rollback(self, response: PaymentResponse) -> None:
        """Handle payment CANCELLED (refund confirmed) — complete cancellation."""
        logger.info(
            "OrderPaymentSaga.rollback | saga_id=%s order_id=%s status=%s",
            response.saga_id,
            response.order_id,
            response.payment_status,
        )
        self._saga_service.rollback_payment_response(response)


class OrderApprovalSaga(SagaStep[Any]):
    """
    Saga step that handles the restaurant approval phase of the order workflow.

    On APPROVED: order → APPROVED.
    On REJECTED: order → CANCELLING, emit refund request outbox record.
    """

    def __init__(self, saga_service: Any) -> None:
        self._saga_service = saga_service

    def process(self, response: Any) -> None:
        logger.info(
            "OrderApprovalSaga.process | saga_id=%s order_id=%s status=%s",
            response.saga_id,
            response.order_id,
            response.order_approval_status,
        )
        self._saga_service.process_approval_response(response)

    def rollback(self, response: Any) -> None:
        logger.info(
            "OrderApprovalSaga.rollback | saga_id=%s order_id=%s",
            response.saga_id,
            response.order_id,
        )
        self._saga_service.rollback_approval_response(response)
