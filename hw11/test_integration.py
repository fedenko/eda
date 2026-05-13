"""Integration tests with real RabbitMQ via TestContainers — Частина 2.

Requires Docker (Docker Desktop or Colima on macOS, dockerd on Linux).
Session-scoped `rabbit_container` fixture is defined in conftest.py.
"""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal
from unittest.mock import AsyncMock

import aio_pika

from conftest import declare_topology, deserialize, serialize
from payment_handler import OrderPlacedEvent


# 1. Publish OrderPlaced → consumer deserializes correctly.
async def test_publish_order_placed_consumer_deserializes(amqp_url: str) -> None:
    queue_name = f"test.deserialize.{uuid.uuid4().hex[:8]}"
    received: list[OrderPlacedEvent] = []

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        exchange, queue = await declare_topology(channel, queue_name)

        async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            async with message.process():
                received.append(deserialize(message.body))

        await queue.consume(on_message)

        event = OrderPlacedEvent(
            order_id=uuid.uuid4(),
            customer_id="cust-42",
            total_amount=Decimal("199.99"),
        )
        await exchange.publish(
            aio_pika.Message(serialize(event)),
            routing_key="order.placed",
        )

        # Polling — up to 5s.
        for _ in range(50):
            if received:
                break
            await asyncio.sleep(0.1)

        assert received, "no message received within 5s"
        got = received[0]
        assert got.event_id == event.event_id
        assert got.order_id == event.order_id
        assert got.customer_id == "cust-42"
        assert got.total_amount == Decimal("199.99")
        assert isinstance(got.total_amount, Decimal), "Decimal lost during serialization"


# 2. Re-publish same event → handler invoked once (consumer-level idempotency).
async def test_republish_same_event_no_duplicates(amqp_url: str) -> None:
    queue_name = f"test.idempotency.{uuid.uuid4().hex[:8]}"
    process_order = AsyncMock()
    processed_ids: set[uuid.UUID] = set()

    connection = await aio_pika.connect_robust(amqp_url)
    async with connection:
        channel = await connection.channel()
        exchange, queue = await declare_topology(channel, queue_name)

        async def on_message(message: aio_pika.abc.AbstractIncomingMessage) -> None:
            async with message.process():
                event = deserialize(message.body)
                if event.event_id in processed_ids:
                    return  # infrastructure-level idempotency
                processed_ids.add(event.event_id)
                await process_order(event)

        await queue.consume(on_message)

        event = OrderPlacedEvent(
            order_id=uuid.uuid4(),
            customer_id="cust-7",
            total_amount=Decimal("75.50"),
        )
        body = serialize(event)
        await exchange.publish(aio_pika.Message(body), routing_key="order.placed")
        await exchange.publish(aio_pika.Message(body), routing_key="order.placed")

        # Wait for first delivery; grace period for second.
        for _ in range(50):
            if process_order.await_count >= 1:
                break
            await asyncio.sleep(0.1)
        await asyncio.sleep(0.5)

        assert process_order.await_count == 1, (
            f"expected 1 call, got {process_order.await_count}"
        )
