import asyncio
import math
import unittest

from amqpstorm_aio import connection
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import message
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory
from amqpstorm_aio.tests.functional import fake_broker


class PublishConsumeTestCase(unittest.IsolatedAsyncioTestCase):
    """Base fixture: one fake broker and one open connection per test."""

    async def asyncSetUp(self):
        self.broker, self.connection = await self.open_connection()
        self.session = self.broker.sessions[0]

    async def open_connection(self, **broker_kwargs):
        broker = fake_broker.FakeBroker(heartbeat=0, **broker_kwargs)
        await broker.start()
        self.addAsyncCleanup(self.shutdown_broker, broker)
        client = connection.Connection(
            "127.0.0.1", "guest", "guest", port=broker.port, heartbeat=0, timeout=5.0
        )
        await client.open()
        self.addAsyncCleanup(self.close_connection, client)
        return broker, client

    @staticmethod
    async def close_connection(client):
        if not client.is_closed:
            await client.close()

    @staticmethod
    async def shutdown_broker(broker):
        for session in broker.sessions:
            session.close_transport()
        async with asyncio.timeout(5):
            await broker.stop()

    async def wait_until(self, predicate, timeout=5.0):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0.001)

    @staticmethod
    def frames_received(session, channel_id, frame_class):
        return [
            frame
            for received_channel_id, frame in session.received
            if received_channel_id == channel_id and isinstance(frame, frame_class)
        ]


class PublishTests(PublishConsumeTestCase):
    """Fire-and-forget and multi-frame publishing."""

    async def test_fire_and_forget_publish_returns_none_and_reaches_broker(self):
        channel = await self.connection.channel()
        result = await channel.basic.publish(b"hello world", "pub.rk", exchange="amq.direct")
        self.assertIsNone(result)
        await self.wait_until(lambda: self.session.published)
        published_channel_id, publish_method, body = self.session.published[0]
        self.assertEqual(channel.channel_id, published_channel_id)
        self.assertEqual("amq.direct", publish_method.exchange)
        self.assertEqual("pub.rk", publish_method.routing_key)
        self.assertEqual(b"hello world", body)

    async def test_large_body_chunks_into_expected_number_of_body_frames(self):
        broker, client = await self.open_connection(frame_max=4096)
        self.assertEqual(4096, client.max_frame_size)
        channel = await client.channel()
        body = bytes(range(256)) * 40
        expected_frames = math.ceil(len(body) / (client.max_frame_size - framing.FRAME_OVERHEAD))
        self.assertGreater(expected_frames, 1, "body must not fit in a single frame")
        await channel.basic.publish(body, "big.rk")
        session = broker.sessions[0]
        await self.wait_until(lambda: session.published)
        self.assertEqual(body, session.published[0][2])
        body_frames = self.frames_received(session, channel.channel_id, bytes)
        self.assertEqual(expected_frames, len(body_frames))

    async def test_str_body_publish_sets_content_encoding_and_reaches_broker_encoded(self):
        channel = await self.connection.channel()
        body = "hallå världen"
        await channel.basic.publish(body, "str.rk")
        await self.wait_until(lambda: self.session.published)
        self.assertEqual(body.encode("utf-8"), self.session.published[0][2])
        content_header = self.frames_received(
            self.session, channel.channel_id, framing.ContentHeader
        )[0]
        self.assertEqual("utf-8", content_header.properties.content_encoding)


class FrameBoundaryTests(PublishConsumeTestCase):
    """Body sizes either side of the frame boundary must split exactly and losslessly."""

    FRAME_MAX = 4096

    async def publish_body(self, body):
        """Publish over a small frame_max; returns the body frames and what arrived."""
        broker, client = await self.open_connection(frame_max=self.FRAME_MAX)
        channel = await client.channel()
        await channel.basic.publish(body, "edge.rk")
        session = broker.sessions[0]
        await self.wait_until(lambda: session.published)
        frames = self.frames_received(session, channel.channel_id, bytes)
        return frames, session.published[0][2]

    @property
    def chunk_size(self):
        return self.FRAME_MAX - framing.FRAME_OVERHEAD

    async def test_a_body_filling_one_frame_exactly_sends_a_single_frame(self):
        body = b"x" * self.chunk_size
        frames, received = await self.publish_body(body)
        self.assertEqual(1, len(frames))
        self.assertEqual(body, received)

    async def test_a_body_one_byte_short_of_a_frame_sends_a_single_frame(self):
        body = b"x" * (self.chunk_size - 1)
        frames, received = await self.publish_body(body)
        self.assertEqual(1, len(frames))
        self.assertEqual(body, received)

    async def test_a_body_one_byte_over_a_frame_spills_into_a_second(self):
        body = b"x" * (self.chunk_size + 1)
        frames, received = await self.publish_body(body)
        self.assertEqual([self.chunk_size, 1], [len(frame) for frame in frames])
        self.assertEqual(body, received)

    async def test_a_body_of_several_whole_frames_sends_no_empty_trailer(self):
        body = b"x" * (self.chunk_size * 3)
        frames, received = await self.publish_body(body)
        self.assertEqual(3, len(frames))
        self.assertEqual(body, received)

    async def test_an_empty_body_sends_no_body_frame_at_all(self):
        frames, received = await self.publish_body(b"")
        self.assertEqual([], frames)
        self.assertEqual(b"", received)

    async def test_a_body_spanning_many_frames_arrives_byte_for_byte(self):
        body = bytes(range(256)) * 200
        frames, received = await self.publish_body(body)
        self.assertGreater(len(frames), 10, "body must span many frames")
        self.assertEqual(body, received)


class BackpressureTests(PublishConsumeTestCase):
    """Publishes flow through a paused transport instead of buffering without bound."""

    async def test_publishes_flow_through_a_tiny_transport_buffer(self):
        channel = await self.connection.channel()
        self.connection._io.transport.set_write_buffer_limits(high=256, low=64)
        body = bytes(512)
        for _ in range(20):
            await channel.basic.publish(body, "bp.rk")
        await self.wait_until(lambda: len(self.session.published) >= 20)
        bodies = [published_body for _, _, published_body in self.session.published]
        self.assertEqual([body] * 20, bodies)


class InboundBackpressureTests(PublishConsumeTestCase):
    """A flood of deliveries pauses reading at the watermark and loses nothing."""

    MESSAGES = 100
    BODY = bytes(512)

    async def test_flood_pauses_reading_then_drains_completely(self):
        from unittest import mock

        from amqpstorm_aio import connection as connection_module
        from amqpstorm_aio.tests import factory

        channel = await self.connection.channel()
        await channel.basic.consume(queue="bp.q")
        with (
            mock.patch.object(connection_module, "INBOUND_BACKLOG_HIGH_WATER", 8 * 1024),
            mock.patch.object(connection_module, "INBOUND_BACKLOG_LOW_WATER", 4 * 1024),
        ):
            for index in range(self.MESSAGES):
                self.session.send_content(
                    channel.channel_id,
                    factory.build(
                        spec.Basic.Deliver,
                        consumer_tag="bp",
                        delivery_tag=index + 1,
                        redelivered=False,
                        exchange="",
                        routing_key="bp.q",
                    ),
                    self.BODY,
                )
            await self.wait_until(lambda: self.connection._read_paused)
            self.assertTrue(self.connection._heartbeat.reads_paused)

            received = 0
            async for message in channel.build_inbound_messages(break_on_empty=True):
                self.assertEqual(self.BODY, message.body)
                received += 1
                if received == self.MESSAGES:
                    break
            self.assertEqual(self.MESSAGES, received, "pausing must not lose messages")
            await self.wait_until(lambda: not self.connection._read_paused)
            self.assertFalse(self.connection._heartbeat.reads_paused)
        self.assertEqual(0, self.connection._inbound_backlog_bytes)


class ConfirmTests(PublishConsumeTestCase):
    """Publisher-confirm outcomes: ack, nack, pipelining, and mandatory returns."""

    async def test_confirmed_publish_acked_returns_true(self):
        channel = await self.connection.channel()
        await channel.confirm_deliveries()
        self.assertTrue(channel.confirming_deliveries)
        result = await channel.basic.publish(b"confirmed", "ok.rk")
        self.assertIs(True, result)

    async def test_confirmed_publish_nacked_returns_false(self):
        def nack_instead_of_ack(session, channel_id, publish, body):
            session.confirming.discard(channel_id)
            session.send_method(channel_id, factory.build(spec.Basic.Nack, delivery_tag=1))

        self.broker.on_publish = nack_instead_of_ack
        channel = await self.connection.channel()
        await channel.confirm_deliveries()
        result = await channel.basic.publish(b"rejected", "nack.rk")
        self.assertIs(False, result)

    async def test_pipelined_confirmed_publishes_all_resolve_true(self):
        channel = await self.connection.channel()
        await channel.confirm_deliveries()
        results = await asyncio.gather(
            channel.basic.publish(b"one", "pipe.rk"),
            channel.basic.publish(b"two", "pipe.rk"),
            channel.basic.publish(b"three", "pipe.rk"),
        )
        self.assertEqual([True, True, True], results)
        self.assertEqual(3, len(self.session.published))

    async def test_mandatory_publish_of_unroutable_message_raises_message_error(self):
        def return_before_ack(session, channel_id, publish, body):
            if publish.mandatory:
                session.send_content(
                    channel_id,
                    factory.build(spec.Basic.Return,
                        reply_code=312,
                        reply_text="NO_ROUTE",
                        exchange=publish.exchange,
                        routing_key=publish.routing_key,
                    ),
                    body,
                )

        self.broker.on_publish = return_before_ack
        channel = await self.connection.channel()
        await channel.confirm_deliveries()
        with self.assertRaises(exception.AMQPMessageError) as context:
            await channel.basic.publish(b"lost", "nowhere.rk", mandatory=True)
        self.assertEqual(312, context.exception.reply_code)
        self.assertStartsWith(str(context.exception), "Message not delivered:")


class ConsumeTests(PublishConsumeTestCase):
    """Consuming, message acknowledgement, and inbound-generator behavior."""

    async def test_consume_delivers_scripted_message_and_ack_reaches_broker(self):
        channel = await self.connection.channel()
        consumer_tag = await channel.basic.consume(queue="consume.q")
        self.assertStartsWith(consumer_tag, "fake-tag-")
        self.assertIn(consumer_tag, channel.consumer_tags)
        self.session.send_content(
            channel.channel_id,
            factory.build(spec.Basic.Deliver,
                consumer_tag=consumer_tag, delivery_tag=7, exchange="e", routing_key="rk"
            ),
            b"payload",
            spec.Basic.Properties(content_type="text/plain"),
        )
        inbound_generator = channel.build_inbound_messages(break_on_empty=True)
        async with asyncio.timeout(5):
            inbound = await anext(inbound_generator)
        await inbound_generator.aclose()
        self.assertEqual(b"payload", inbound.body)
        self.assertEqual(7, inbound.delivery_tag)
        self.assertEqual("text/plain", inbound.properties.content_type)
        self.assertEqual(consumer_tag, inbound.consumer_tag)
        await inbound.ack()
        await self.wait_until(
            lambda: self.frames_received(self.session, channel.channel_id, spec.Basic.Ack)
        )
        ack = self.frames_received(self.session, channel.channel_id, spec.Basic.Ack)[0]
        self.assertEqual(7, ack.delivery_tag)

    async def test_delivered_str_body_decodes_back_via_text(self):
        channel = await self.connection.channel()
        consumer_tag = await channel.basic.consume(queue="echo.q")
        body = "hallå världen"
        self.session.send_content(
            channel.channel_id,
            factory.build(spec.Basic.Deliver, consumer_tag=consumer_tag, delivery_tag=1),
            body.encode("utf-8"),
            spec.Basic.Properties(content_encoding="utf-8"),
        )
        inbound_generator = channel.build_inbound_messages(break_on_empty=True)
        async with asyncio.timeout(5):
            inbound = await anext(inbound_generator)
        await inbound_generator.aclose()
        self.assertEqual(body.encode("utf-8"), inbound.body)
        self.assertEqual(body, inbound.text)
        self.assertEqual("utf-8", inbound.properties.content_encoding)

    async def test_start_consuming_routes_to_callback_and_exits_after_cancel(self):
        channel = await self.connection.channel()
        received = []

        async def on_message(inbound):
            received.append(inbound)
            await channel.basic.cancel(inbound.consumer_tag or "")

        consumer_tag = await channel.basic.consume(on_message, "callback.q")
        self.session.send_content(
            channel.channel_id,
            factory.build(spec.Basic.Deliver, consumer_tag=consumer_tag, delivery_tag=1),
            b"routed",
        )
        async with asyncio.timeout(5):
            await channel.start_consuming()
        self.assertEqual(1, len(received))
        self.assertEqual(b"routed", received[0].body)
        self.assertEqual([], channel.consumer_tags)

    async def test_build_inbound_messages_break_on_empty_without_consumers_exits_at_once(self):
        channel = await self.connection.channel()
        async with asyncio.timeout(1):
            collected = [
                inbound async for inbound in channel.build_inbound_messages(break_on_empty=True)
            ]
        self.assertEqual([], collected)


class GetTests(PublishConsumeTestCase):
    """basic.get outcomes for scripted GetOk, GetEmpty, and consumer conflicts."""

    async def test_get_returns_message_for_scripted_get_ok(self):
        def send_get_ok(session, channel_id, frame):
            session.send_content(
                channel_id,
                factory.build(
                    spec.Basic.GetOk, delivery_tag=5, exchange="e", routing_key="stored.rk"
                ),
                b"stored",
                spec.Basic.Properties(content_type="application/octet-stream"),
            )

        self.broker.responders[spec.Basic.Get.index] = send_get_ok
        channel = await self.connection.channel()
        result = await channel.basic.get("stored.q")
        self.assertIsInstance(result, message.Message)
        self.assertEqual(b"stored", result.body)
        self.assertEqual(5, result.delivery_tag)
        self.assertEqual("stored.rk", result.routing_key)

    async def test_get_returns_none_for_get_empty(self):
        channel = await self.connection.channel()
        self.assertIsNone(await channel.basic.get("empty.q"))

    async def test_get_while_consuming_raises_channel_error(self):
        channel = await self.connection.channel()
        await channel.basic.consume(queue="busy.q")
        with self.assertRaises(exception.AMQPChannelError) as context:
            await channel.basic.get("busy.q")
        self.assertEqual(
            "Cannot call 'get' when channel is set to consume", str(context.exception)
        )


if __name__ == "__main__":
    unittest.main()
