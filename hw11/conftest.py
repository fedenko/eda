"""Shared pytest fixtures and helpers for hw11 tests."""

from __future__ import annotations

import json
import uuid
from collections.abc import Generator
from decimal import Decimal
from unittest.mock import AsyncMock

import aio_pika
import pytest

from payment_handler import (
    EventPublisher,
    OrderPlacedEvent,
    PaymentGateway,
    PaymentHandler,
    PaymentRepository,
)


# ---------- Unit-test fixtures (no Docker) ----------


@pytest.fixture
def payment_handler() -> tuple[PaymentHandler, AsyncMock, AsyncMock, AsyncMock]:
    """PaymentHandler with three AsyncMock dependencies (≡ NSubstitute ctor)."""
    repo = AsyncMock(spec=PaymentRepository)
    gateway = AsyncMock(spec=PaymentGateway)
    publisher = AsyncMock(spec=EventPublisher)
    handler = PaymentHandler(repo, gateway, publisher)
    return handler, repo, gateway, publisher


@pytest.fixture
def make_event():
    """Factory for OrderPlacedEvent with sensible defaults."""

    def _make(amount: Decimal = Decimal("99.99")) -> OrderPlacedEvent:
        return OrderPlacedEvent(
            order_id=uuid.uuid4(),
            customer_id="cust-1",
            total_amount=amount,
        )

    return _make


# ---------- Integration-test fixtures (require Docker) ----------


@pytest.fixture(scope="session")
def rabbit_container() -> Generator[object, None, None]:
    """Start a single RabbitMQ container for the whole test session."""
    from testcontainers.rabbitmq import RabbitMqContainer

    container = RabbitMqContainer("rabbitmq:3.13-management")
    container.start()
    try:
        yield container
    finally:
        container.stop()


@pytest.fixture(scope="session")
def amqp_url(rabbit_container) -> str:
    host = rabbit_container.get_container_host_ip()
    port = rabbit_container.get_exposed_port(5672)
    return f"amqp://guest:guest@{host}:{port}/"


# ---------- Helpers (used by integration + chaos tests) ----------


async def declare_topology(
    channel: aio_pika.abc.AbstractChannel, queue_name: str
) -> tuple[aio_pika.abc.AbstractExchange, aio_pika.abc.AbstractQueue]:
    """Declare topic exchange 'orders' + queue + binding on 'order.placed'."""
    exchange = await channel.declare_exchange(
        "orders", aio_pika.ExchangeType.TOPIC, durable=False
    )
    queue = await channel.declare_queue(queue_name, durable=False, auto_delete=False)
    await queue.bind(exchange, routing_key="order.placed")
    return exchange, queue


def serialize(event: OrderPlacedEvent) -> bytes:
    """Domain → JSON bytes. Decimal → str (so json.dumps doesn't choke)."""
    return json.dumps(
        {
            "event_id": str(event.event_id),
            "order_id": str(event.order_id),
            "customer_id": event.customer_id,
            "total_amount": str(event.total_amount),
        }
    ).encode()


def deserialize(body: bytes) -> OrderPlacedEvent:
    """JSON bytes → Domain. str → Decimal (preserves precision)."""
    d = json.loads(body)
    return OrderPlacedEvent(
        event_id=uuid.UUID(d["event_id"]),
        order_id=uuid.UUID(d["order_id"]),
        customer_id=d["customer_id"],
        total_amount=Decimal(d["total_amount"]),
    )
