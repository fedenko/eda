"""Domain types and PaymentHandler — System Under Test for hw11."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Protocol


# --- Domain events ---


@dataclass(frozen=True)
class OrderPlacedEvent:
    order_id: uuid.UUID
    customer_id: str
    total_amount: Decimal
    event_id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class PaymentProcessedEvent:
    order_id: uuid.UUID
    transaction_id: str
    amount: Decimal


@dataclass(frozen=True)
class PaymentFailedEvent:
    order_id: uuid.UUID
    reason: str


@dataclass(frozen=True)
class PaymentRecord:
    order_id: uuid.UUID
    transaction_id: str
    amount: Decimal
    paid_at: datetime


@dataclass(frozen=True)
class ChargeResult:
    success: bool
    transaction_id: str | None = None
    error_message: str | None = None


# --- Dependencies (Protocol = duck-typed interface) ---


class PaymentRepository(Protocol):
    async def payment_exists_for_order(self, order_id: uuid.UUID) -> bool: ...
    async def save_payment(self, record: PaymentRecord) -> None: ...


class PaymentGateway(Protocol):
    async def charge(self, customer_id: str, amount: Decimal) -> ChargeResult: ...


class EventPublisher(Protocol):
    async def publish(self, event: object) -> None: ...


# --- The handler we test ---


class PaymentHandler:
    """Idempotency → charge → save+publish or publish(failed)."""

    def __init__(
        self,
        repo: PaymentRepository,
        gateway: PaymentGateway,
        publisher: EventPublisher,
    ) -> None:
        self._repo = repo
        self._gateway = gateway
        self._publisher = publisher

    async def handle(self, event: OrderPlacedEvent) -> None:
        # 1. Idempotency
        if await self._repo.payment_exists_for_order(event.order_id):
            return

        # 2. External gateway call (may raise — propagates upward)
        result = await self._gateway.charge(event.customer_id, event.total_amount)

        # 3. Failure path — publish PaymentFailed, do NOT save
        if not result.success:
            await self._publisher.publish(
                PaymentFailedEvent(
                    order_id=event.order_id,
                    reason=result.error_message or "unknown",
                )
            )
            return

        # 4. Success path — save + publish PaymentProcessed
        await self._repo.save_payment(
            PaymentRecord(
                order_id=event.order_id,
                transaction_id=result.transaction_id,
                amount=event.total_amount,
                paid_at=datetime.now(timezone.utc),
            )
        )
        await self._publisher.publish(
            PaymentProcessedEvent(
                order_id=event.order_id,
                transaction_id=result.transaction_id,
                amount=event.total_amount,
            )
        )
