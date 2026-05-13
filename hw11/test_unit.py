"""Unit tests for PaymentHandler — Частина 1.

Усі залежності — AsyncMock. Жодного Docker / брокера.
NSubstitute → AsyncMock map:
  Received(1)        → assert_awaited_once()
  DidNotReceive()    → assert_not_awaited()
  .Returns(x)        → .return_value = x
  .Throws(exc)       → .side_effect = exc
"""

from __future__ import annotations

from decimal import Decimal

from payment_handler import (
    ChargeResult,
    PaymentFailedEvent,
    PaymentProcessedEvent,
    PaymentRecord,
)


# 1. Successful payment → save_payment + publish(PaymentProcessed).
async def test_successful_payment_saves_and_publishes_processed(
    payment_handler, make_event
) -> None:
    handler, repo, gateway, publisher = payment_handler
    event = make_event()
    repo.payment_exists_for_order.return_value = False
    gateway.charge.return_value = ChargeResult(success=True, transaction_id="tx-1")

    await handler.handle(event)

    repo.save_payment.assert_awaited_once()
    saved: PaymentRecord = repo.save_payment.await_args.args[0]
    assert saved.order_id == event.order_id
    assert saved.transaction_id == "tx-1"
    assert saved.amount == event.total_amount

    publisher.publish.assert_awaited_once()
    published = publisher.publish.await_args.args[0]
    assert isinstance(published, PaymentProcessedEvent)
    assert published.order_id == event.order_id
    assert published.transaction_id == "tx-1"


# 2. Failed payment → publish(PaymentFailed), NOT save_payment.
async def test_failed_payment_publishes_failed_without_save(
    payment_handler, make_event
) -> None:
    handler, repo, gateway, publisher = payment_handler
    event = make_event(amount=Decimal("50"))
    repo.payment_exists_for_order.return_value = False
    gateway.charge.return_value = ChargeResult(success=False, error_message="Declined")

    await handler.handle(event)

    publisher.publish.assert_awaited_once()
    published = publisher.publish.await_args.args[0]
    assert isinstance(published, PaymentFailedEvent)
    assert published.order_id == event.order_id
    assert published.reason == "Declined"

    repo.save_payment.assert_not_awaited()


# 3. Duplicate → NOT charge, NOT publish, NOT save.
async def test_duplicate_skips_gateway_and_publishing(
    payment_handler, make_event
) -> None:
    handler, repo, gateway, publisher = payment_handler
    event = make_event()
    repo.payment_exists_for_order.return_value = True

    await handler.handle(event)

    gateway.charge.assert_not_awaited()
    publisher.publish.assert_not_awaited()
    repo.save_payment.assert_not_awaited()


# 4. Gateway raises → exception propagates, NOT publish, NOT save.
async def test_gateway_error_raises_and_does_not_publish(
    payment_handler, make_event
) -> None:
    handler, repo, gateway, publisher = payment_handler
    event = make_event()
    repo.payment_exists_for_order.return_value = False
    gateway.charge.side_effect = TimeoutError("Gateway timeout")

    raised: TimeoutError | None = None
    try:
        await handler.handle(event)
    except TimeoutError as exc:
        raised = exc
    assert raised is not None, "expected TimeoutError to propagate"
    assert "Gateway timeout" in str(raised)

    publisher.publish.assert_not_awaited()
    repo.save_payment.assert_not_awaited()
