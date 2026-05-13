"""Chaos test — kill consumer before ACK, verify redelivery (Бонус).

Scenario:
  1. Publisher publishes OrderPlaced.
  2. Consumer A picks it up, sleeps 10s before ack.
  3. Connection A is forcibly closed (emulates crash).
  4. Consumer B on the same queue receives the re-delivered message
     with `message.redelivered == True`.

Requires Docker (same session-scoped fixture as test_integration.py).
"""

from __future__ import annotations

import asyncio
import uuid
from decimal import Decimal

import aio_pika

from conftest import declare_topology, serialize
from payment_handler import OrderPlacedEvent


async def test_consumer_crash_before_ack_triggers_redelivery(amqp_url: str) -> None:
    queue_name = f"test.chaos.{uuid.uuid4().hex[:8]}"

    # 1. Declare topology and publish.
    publisher_conn = await aio_pika.connect_robust(amqp_url)
    async with publisher_conn:
        channel = await publisher_conn.channel()
        exchange, _ = await declare_topology(channel, queue_name)
        event = OrderPlacedEvent(
            order_id=uuid.uuid4(),
            customer_id="cust-chaos",
            total_amount=Decimal("42.00"),
        )
        await exchange.publish(
            aio_pika.Message(serialize(event)),
            routing_key="order.placed",
        )

    # 2. Consumer A receives but "crashes" before ack.
    consumer_a_conn = await aio_pika.connect(amqp_url)  # non-robust: won't reconnect
    channel_a = await consumer_a_conn.channel()
    await channel_a.set_qos(prefetch_count=1)
    queue_a = await channel_a.declare_queue(
        queue_name, durable=False, auto_delete=False
    )

    a_received = asyncio.Event()

    async def consumer_a_callback(message: aio_pika.abc.AbstractIncomingMessage) -> None:
        a_received.set()
        await asyncio.sleep(10)  # we'll kill the connection before this finishes
        await message.ack()       # this ack never runs

    await queue_a.consume(consumer_a_callback, no_ack=False)
    await asyncio.wait_for(a_received.wait(), timeout=5.0)

    # Emulate crash: close connection before callback acks.
    await consumer_a_conn.close()

    # 3. Consumer B picks up the re-delivered message.
    consumer_b_conn = await aio_pika.connect_robust(amqp_url)
    redelivered_flag: dict[str, bool | None] = {"value": None}
    b_received = asyncio.Event()

    async with consumer_b_conn:
        channel_b = await consumer_b_conn.channel()
        queue_b = await channel_b.declare_queue(
            queue_name, durable=False, auto_delete=False
        )

        async def consumer_b_callback(
            message: aio_pika.abc.AbstractIncomingMessage,
        ) -> None:
            redelivered_flag["value"] = message.redelivered
            await message.ack()
            b_received.set()

        await queue_b.consume(consumer_b_callback, no_ack=False)
        await asyncio.wait_for(b_received.wait(), timeout=10.0)

    assert redelivered_flag["value"] is True, (
        f"expected redelivered=True, got {redelivered_flag['value']}"
    )
