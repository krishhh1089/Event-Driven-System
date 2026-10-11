"""
Payment Service — Application Layer
=====================================
Command handlers for payment processing and cancellation.

PaymentRequestHandler:
  Called when the Order Service publishes a payment-request event.
  Executes the full debit or refund flow atomically:
    1. Validate the request (idempotency check via DB constraints).
    2. Load or create the payment record.
    3. Load the credit entry (with SELECT FOR UPDATE).
    4. Load credit history.
    5. Call domain service to process.
    6. Persist payment, credit entry, and credit history records.
    7. Insert a payment-response outbox record in the same transaction.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from uuid import UUID, uuid4

from sqlalchemy import select, update, and_
from sqlalchemy.ext.asyncio import AsyncSession

from common_application import ApplicationException
from common_domain import CustomerId, Money, OrderId, PaymentId

from ..domain.entities import (
    CreditEntry,
    CreditEntryId,
    CreditHistory,
    CreditHistoryId,
    Payment,
    PaymentStatus,
    TransactionType,
)
from ..domain.payment_domain_service import PaymentDomainService
from ..infrastructure.database.models import (
    CreditEntryModel,
    CreditHistoryModel,
    CreditEntryTransactionTypeEnum,
    OutboxStatusEnum,
    PaymentModel,
    PaymentOutboxModel,
    PaymentStatusEnum,
    SagaStatusEnum,
)

logger = logging.getLogger(__name__)


class PaymentNotFoundException(ApplicationException):
    pass


class CustomerCreditEntryNotFoundException(ApplicationException):
    pass


class PaymentRequestHandler:
    """
    Application-layer use case handler for payment-request events.

    All persistence must happen inside the caller's database transaction.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._domain_service = PaymentDomainService()

    # ------------------------------------------------------------------
    # Idempotency guard
    # ------------------------------------------------------------------

    async def _find_existing_payment(
        self, order_id: UUID, payment_status: PaymentStatusEnum
    ) -> Optional[PaymentModel]:
        stmt = select(PaymentModel).where(
            and_(
                PaymentModel.order_id == order_id,
                PaymentModel.payment_status == payment_status,
            )
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none()

    # ------------------------------------------------------------------
    # Credit entry loader (SELECT FOR UPDATE to prevent concurrent overdrafts)
    # ------------------------------------------------------------------

    async def _load_credit_entry(self, customer_id: UUID) -> CreditEntryModel:
        stmt = (
            select(CreditEntryModel)
            .where(CreditEntryModel.customer_id == customer_id)
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        model = result.scalar_one_or_none()
        if model is None:
            raise CustomerCreditEntryNotFoundException(
                f"No credit entry for customer {customer_id}. "
                "Customer must be initialized with a starting balance."
            )
        return model

    async def _load_credit_history(self, customer_id: UUID) -> List[CreditHistoryModel]:
        stmt = select(CreditHistoryModel).where(
            CreditHistoryModel.customer_id == customer_id
        )
        result = await self._session.execute(stmt)
        return list(result.scalars())

    # ------------------------------------------------------------------
    # Domain ↔ ORM adapters
    # ------------------------------------------------------------------

    @staticmethod
    def _model_to_credit_entry(m: CreditEntryModel) -> CreditEntry:
        return CreditEntry(
            credit_entry_id=CreditEntryId(m.id),
            customer_id=CustomerId(m.customer_id),
            total_credit_amount=Money(Decimal(str(m.total_credit_amount))),
        )

    @staticmethod
    def _model_to_credit_history(m: CreditHistoryModel) -> CreditHistory:
        return CreditHistory(
            credit_history_id=CreditHistoryId(m.id),
            customer_id=CustomerId(m.customer_id),
            amount=Money(Decimal(str(m.amount))),
            transaction_type=TransactionType(m.transaction_type),
        )

    # ------------------------------------------------------------------
    # Process payment (debit / charge)
    # ------------------------------------------------------------------

    async def process_payment(
        self,
        saga_id: UUID,
        order_id: UUID,
        customer_id: UUID,
        price: Decimal,
        event_id: str,
    ) -> None:
        """
        Charge the customer for the order (debit).

        Idempotency: if a COMPLETED payment already exists for this order_id,
        we skip processing but still emit the response outbox record so the
        Order Service receives its acknowledgment (in case the previous outbox
        record was lost before delivery).
        """
        existing = await self._find_existing_payment(order_id, PaymentStatusEnum.COMPLETED)
        if existing:
            logger.info(
                "Idempotent payment: order %s already COMPLETED, re-emitting response", order_id
            )
            await self._upsert_outbox_response(
                saga_id=saga_id,
                payment_id=existing.id,
                customer_id=customer_id,
                order_id=order_id,
                price=price,
                payment_status=PaymentStatusEnum.COMPLETED,
                saga_status=SagaStatusEnum.PROCESSING,
                failure_messages=[],
            )
            return

        # Load credit entry with FOR UPDATE
        credit_entry_model = await self._load_credit_entry(customer_id)
        credit_history_models = await self._load_credit_history(customer_id)

        # Build domain objects
        money = Money(price)
        credit_entry = self._model_to_credit_entry(credit_entry_model)
        credit_history_list = [
            self._model_to_credit_history(m) for m in credit_history_models
        ]

        payment = Payment(
            payment_id=PaymentId(uuid4()),
            customer_id=CustomerId(customer_id),
            order_id=OrderId(order_id),
            price=money,
        )

        failure_messages: List[str] = []

        try:
            self._domain_service.complete_payment(payment, credit_entry, credit_history_list)
        except Exception as exc:  # noqa: BLE001
            logger.error("Payment processing failed: %s", exc)
            failure_messages.append(str(exc))
            payment.payment_status = PaymentStatus.FAILED

        # Persist payment record
        payment_model = PaymentModel(
            id=payment.id.value,
            customer_id=customer_id,
            order_id=order_id,
            price=money.amount,
            payment_status=PaymentStatusEnum(payment.payment_status.value),
        )
        self._session.add(payment_model)

        # Persist updated credit entry if successful
        if payment.payment_status == PaymentStatus.COMPLETED:
            await self._session.execute(
                update(CreditEntryModel)
                .where(CreditEntryModel.id == credit_entry_model.id)
                .values(
                    total_credit_amount=credit_entry.total_credit_amount.amount,
                    version=CreditEntryModel.version + 1,
                )
            )
            # Persist the new credit history record
            new_history = credit_history_list[-1]  # appended by domain service
            history_model = CreditHistoryModel(
                id=uuid4(),
                customer_id=customer_id,
                amount=new_history.amount.amount,
                transaction_type=CreditEntryTransactionTypeEnum(new_history.transaction_type.value),
            )
            self._session.add(history_model)

        # Create response outbox record
        response_status = (
            PaymentStatusEnum.COMPLETED
            if payment.payment_status == PaymentStatus.COMPLETED
            else PaymentStatusEnum.FAILED
        )
        await self._session.flush()  # get payment_model.id

        await self._upsert_outbox_response(
            saga_id=saga_id,
            payment_id=payment_model.id,
            customer_id=customer_id,
            order_id=order_id,
            price=price,
            payment_status=response_status,
            saga_status=SagaStatusEnum.PROCESSING,
            failure_messages=failure_messages,
        )

    # ------------------------------------------------------------------
    # Cancel payment (refund / credit)
    # ------------------------------------------------------------------

    async def cancel_payment(
        self,
        saga_id: UUID,
        order_id: UUID,
        customer_id: UUID,
        price: Decimal,
        event_id: str,
    ) -> None:
        """
        Refund the customer for the order (credit).

        Idempotency: if a CANCELLED payment already exists for this order_id,
        re-emit the response outbox so the Order Service can finish its saga.
        """
        existing = await self._find_existing_payment(order_id, PaymentStatusEnum.CANCELLED)
        if existing:
            logger.info(
                "Idempotent cancellation: order %s already CANCELLED, re-emitting response",
                order_id,
            )
            await self._upsert_outbox_response(
                saga_id=saga_id,
                payment_id=existing.id,
                customer_id=customer_id,
                order_id=order_id,
                price=price,
                payment_status=PaymentStatusEnum.CANCELLED,
                saga_status=SagaStatusEnum.COMPENSATING,
                failure_messages=[],
            )
            return

        # Load completed payment to confirm there's something to refund
        completed_payment = await self._find_existing_payment(
            order_id, PaymentStatusEnum.COMPLETED
        )
        if completed_payment is None:
            # Edge case: the original payment never completed (e.g. FAILED).
            # Nothing to refund — emit CANCELLED response to unblock saga.
            logger.warning(
                "Cancel requested for order %s but no COMPLETED payment found. "
                "Emitting CANCELLED response.",
                order_id,
            )
            await self._upsert_outbox_response(
                saga_id=saga_id,
                payment_id=uuid4(),
                customer_id=customer_id,
                order_id=order_id,
                price=price,
                payment_status=PaymentStatusEnum.CANCELLED,
                saga_status=SagaStatusEnum.COMPENSATING,
                failure_messages=[],
            )
            return

        credit_entry_model = await self._load_credit_entry(customer_id)
        credit_history_models = await self._load_credit_history(customer_id)

        money = Money(price)
        credit_entry = self._model_to_credit_entry(credit_entry_model)
        credit_history_list = [
            self._model_to_credit_history(m) for m in credit_history_models
        ]

        payment = Payment(
            payment_id=PaymentId(uuid4()),
            customer_id=CustomerId(customer_id),
            order_id=OrderId(order_id),
            price=money,
        )

        self._domain_service.cancel_payment(payment, credit_entry, credit_history_list)

        # Persist cancellation record
        cancel_model = PaymentModel(
            id=payment.id.value,
            customer_id=customer_id,
            order_id=order_id,
            price=money.amount,
            payment_status=PaymentStatusEnum.CANCELLED,
        )
        self._session.add(cancel_model)

        # Update credit entry (balance restored)
        await self._session.execute(
            update(CreditEntryModel)
            .where(CreditEntryModel.id == credit_entry_model.id)
            .values(
                total_credit_amount=credit_entry.total_credit_amount.amount,
                version=CreditEntryModel.version + 1,
            )
        )

        # Credit history
        new_history = credit_history_list[-1]
        history_model = CreditHistoryModel(
            id=uuid4(),
            customer_id=customer_id,
            amount=new_history.amount.amount,
            transaction_type=CreditEntryTransactionTypeEnum.CREDIT,
        )
        self._session.add(history_model)

        await self._session.flush()

        await self._upsert_outbox_response(
            saga_id=saga_id,
            payment_id=cancel_model.id,
            customer_id=customer_id,
            order_id=order_id,
            price=price,
            payment_status=PaymentStatusEnum.CANCELLED,
            saga_status=SagaStatusEnum.COMPENSATING,
            failure_messages=[],
        )

    # ------------------------------------------------------------------
    # Outbox helper
    # ------------------------------------------------------------------

    async def _upsert_outbox_response(
        self,
        saga_id: UUID,
        payment_id: UUID,
        customer_id: UUID,
        order_id: UUID,
        price: Decimal,
        payment_status: PaymentStatusEnum,
        saga_status: SagaStatusEnum,
        failure_messages: List[str],
    ) -> None:
        """Insert a payment-response outbox record if it does not already exist."""
        # Check for existing outbox record to avoid duplicate emission
        stmt = select(PaymentOutboxModel).where(
            and_(
                PaymentOutboxModel.saga_id == saga_id,
                PaymentOutboxModel.saga_status == saga_status,
            )
        )
        result = await self._session.execute(stmt)
        existing_outbox = result.scalar_one_or_none()
        if existing_outbox:
            logger.info(
                "Outbox record already exists for saga_id=%s saga_status=%s — skipping",
                saga_id,
                saga_status,
            )
            return

        payload = {
            "id": str(uuid4()),
            "saga_id": str(saga_id),
            "payment_id": str(payment_id),
            "customer_id": str(customer_id),
            "order_id": str(order_id),
            "price": str(price),
            "payment_status": payment_status.value,
            "failure_messages": failure_messages,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        outbox = PaymentOutboxModel(
            id=uuid4(),
            saga_id=saga_id,
            type="PaymentResponseAvroModel",
            payload=payload,
            outbox_status=OutboxStatusEnum.STARTED,
            saga_status=saga_status,
            payment_status=payment_status,
            version=0,
        )
        self._session.add(outbox)
