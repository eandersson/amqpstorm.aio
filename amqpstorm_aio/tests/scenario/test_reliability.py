import asyncio
import random
import time
import unittest
import uuid

from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import uri_connection
from amqpstorm_aio.tests import scenario
from amqpstorm_aio.tests.scenario import base

_MAX_CLOSE_SECONDS = 3.0
_LIFECYCLE_ITERATIONS = 1000
_CHANNELS_PER_CONNECTION = 25


class ConnectionLifecycleTests(base.ScenarioTestCase):
    """Repeated open/close cycles must leave no residue on either side."""

    async def assert_closed_without_error(self, connection) -> None:
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            connection.check_for_errors()
        self.assertEqual("connection closed", str(context.exception))

    async def test_open_new_connection_loop_stays_clean(self):
        for _ in range(_LIFECYCLE_ITERATIONS):
            connection = self.make_connection()
            await connection.open()
            channel = await connection.channel()
            self.assertEqual(1, int(channel), "each new connection starts at channel 1")
            await channel.queue.declare(self.queue_name, durable=True)
            self.assertTrue(connection.is_open)
            await channel.close()
            await connection.close()
            await self.assert_closed_without_error(connection)

    async def test_open_close_connection_loop_reuses_the_object(self):
        connection = self.make_connection()
        for _ in range(_LIFECYCLE_ITERATIONS):
            await connection.open()
            channel = await connection.channel()
            self.assertEqual(1, int(channel), "channel ids restart after a reopen")
            await channel.queue.declare(self.queue_name, durable=True)
            await channel.close()
            self.assertTrue(connection.is_open)
            await connection.close()
            await self.assert_closed_without_error(connection)

    async def test_close_completes_quickly(self):
        for _ in range(10):
            connection = self.make_connection()
            await connection.open()
            start = time.monotonic()
            await connection.close()
            self.assertLess(time.monotonic() - start, _MAX_CLOSE_SECONDS)

    async def test_uri_connection_opens(self):
        connection = uri_connection.UriConnection(scenario.URI)
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)
        channel = await connection.channel()
        self.assertTrue(connection.is_open)
        self.assertTrue(channel.is_open)


class ClusterFailoverTests(base.ScenarioTestCase):
    """A hostname list connects to whichever node answers."""

    UNREACHABLE = "no-such-node.invalid"

    async def test_a_dead_first_node_falls_through_to_the_live_one(self):
        connection = self.make_connection(hostname=[self.UNREACHABLE, scenario.HOST])
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)
        self.assertTrue(connection.is_open)
        channel = await connection.channel()
        declared = await channel.queue.declare(self.queue_name, durable=True)
        self.assertEqual(self.queue_name, declared["queue"])

    async def test_rotation_still_finds_the_live_node(self):
        connection = self.make_connection(hostname=[scenario.HOST, self.UNREACHABLE])
        for _ in range(3):
            await connection.open()
            self.assertTrue(connection.is_open)
            await connection.close()

    async def test_all_nodes_unreachable_names_each_one(self):
        connection = self.make_connection(
            hostname=[self.UNREACHABLE, "also-missing.invalid"], timeout=2.0
        )
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        message = str(context.exception)
        self.assertIn(self.UNREACHABLE, message)
        self.assertIn("also-missing.invalid", message)


class ChannelLifecycleTests(base.ScenarioTestCase):
    """Channel churn on a long-lived connection."""

    async def test_open_close_channel_loop_reuses_channel_id(self):
        connection = await self.open_connection()
        for _ in range(_LIFECYCLE_ITERATIONS):
            channel = await connection.channel()
            self.assertTrue(connection.is_open)
            self.assertTrue(channel.is_open)
            self.assertEqual(1, int(channel), "a closed channel's id is reused")
            await channel.close()
            self.assertTrue(connection.is_open)
            self.assertTrue(channel.is_closed)

    async def test_open_multiple_channels_numbers_them_in_order(self):
        connection = self.make_connection()
        for _ in range(5):
            await connection.open()
            channels = []
            for _ in range(_CHANNELS_PER_CONNECTION):
                channel = await connection.channel()
                channels.append(channel)
                self.assertTrue(channel.is_open)
                self.assertEqual(len(channels), int(channel))
            await connection.close()
        await self.assert_connection_closed(connection)

    async def assert_connection_closed(self, connection) -> None:
        self.assertTrue(connection.is_closed)


class ServerForcedCloseTests(base.ScenarioTestCase):
    """The broker closing a channel must not slow down or corrupt teardown."""

    async def test_close_gracefully_after_mandatory_publish_fails(self):
        connection = await self.open_connection()
        for _ in range(3):
            channel = await connection.channel()
            for _ in range(_LIFECYCLE_ITERATIONS):
                try:
                    await channel.basic.publish(
                        b"", self.queue_name, "", mandatory=True
                    )
                except exception.AMQPMessageError:
                    pass
            await self.wait_until(
                lambda bound_channel=channel: self._has_recorded_error(bound_channel)
            )
            await channel.close()
            self.assertTrue(channel.is_closed)
        self.assertTrue(connection.is_open, "returned messages must not kill the connection")

    @staticmethod
    def _has_recorded_error(channel) -> bool:
        try:
            channel.check_for_exceptions()
        except exception.AMQPMessageError:
            return True
        return False

    async def test_close_after_channel_closed_by_server_is_quick(self):
        for _ in range(100):
            connection = await self.open_connection()
            channel = await connection.channel()
            await channel.confirm_deliveries()
            with self.assertRaises(exception.AMQPError):
                await channel.basic.publish(
                    b"message", self.queue_name, exchange="invalid"
                )
            start = time.monotonic()
            await channel.close()
            self.assertLess(time.monotonic() - start, _MAX_CLOSE_SECONDS)

            start = time.monotonic()
            await connection.close()
            self.assertLess(time.monotonic() - start, _MAX_CLOSE_SECONDS)

    async def test_passive_declare_of_missing_queue_closes_only_that_channel(self):
        connection = await self.open_connection()
        for _ in range(5):
            channel = await connection.channel()
            self.assertEqual(1, int(channel))
            with self.assertRaises(exception.AMQPChannelError):
                await channel.queue.declare(self.queue_name, passive=True)
            self.assertTrue(channel.is_closed)
            self.assertTrue(connection.is_open, "a soft error must not close the connection")

        channel = await connection.channel()
        self.assertEqual(1, int(channel), "the closed channel's id is reusable")
        await channel.queue.declare(self.queue_name, durable=True)
        declared = await channel.queue.declare(self.queue_name, passive=True)
        self.assertEqual(self.queue_name, declared["queue"])
        await channel.close()


class ManagementVisibilityTests(base.ScenarioTestCase):
    """Cross-check client state against what the broker reports over the HTTP API."""

    async def test_open_connection_is_reported_then_released(self):
        marker = str(uuid.uuid4())
        connection = self.make_connection(client_properties={"scenario_marker": marker})
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)

        await self.wait_until_broker_sees(marker, expected=True)
        reported = await self.find_marked_connection(marker)
        self.assertEqual("amqpstorm-aio", reported["client_properties"]["product"])
        self.assertTrue(reported["client_properties"]["capabilities"]["publisher_confirms"])

        await connection.close()
        await self.wait_until_broker_sees(marker, expected=False)

    async def test_connection_closed_by_broker_is_observed_by_the_client(self):
        marker = str(uuid.uuid4())
        connection = self.make_connection(client_properties={"scenario_marker": marker})
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)
        channel = await connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)

        await self.wait_until_broker_sees(marker, expected=True)
        reported = await self.find_marked_connection(marker)
        await self.management(lambda: self.api.connection.close(reported["name"]))

        await self.wait_until(lambda: connection.is_closed, timeout=15)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            connection.check_for_errors()
        self.assertIn("closed by remote server", str(context.exception))
        with self.assertRaises(exception.AMQPError):
            await channel.queue.declare(self.queue_name, durable=True)

    async def find_marked_connection(self, marker: str) -> dict:
        for entry in await self.find_own_connections():
            if entry.get("client_properties", {}).get("scenario_marker") == marker:
                return entry
        self.fail(f"broker does not report a connection marked {marker}")

    async def wait_until_broker_sees(self, marker: str, *, expected: bool) -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            entries = await self.find_own_connections()
            present = any(
                entry.get("client_properties", {}).get("scenario_marker") == marker
                for entry in entries
            )
            if present == expected:
                return
            await asyncio.sleep(0.2)
        self.fail(f"broker never reported connection {marker} as present={expected}")


class LargeMessageTests(base.ScenarioTestCase):
    LARGE = 8 * 1024 * 1024
    OVERSIZED = 32 * 1024 * 1024

    @staticmethod
    def patterned_body(size: int) -> bytes:
        return random.Random(size).randbytes(size)

    async def open_channel(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        await channel.confirm_deliveries()
        return connection, channel

    async def assert_round_trip(self, channel, body: bytes) -> None:
        self.assertIs(True, await channel.basic.publish(body, self.queue_name))
        received = await channel.basic.get(self.queue_name)
        self.assertIsNotNone(received)
        self.assertEqual(len(body), len(received.body))
        self.assertEqual(body, received.body)
        await received.ack()

    async def test_a_multi_megabyte_body_round_trips_byte_for_byte(self):
        _connection, channel = await self.open_channel()
        await self.assert_round_trip(channel, self.patterned_body(self.LARGE))

    async def test_bodies_around_the_frame_boundary_round_trip(self):
        connection, channel = await self.open_channel()
        chunk = connection.max_frame_size - framing.FRAME_OVERHEAD
        for size in (0, 1, chunk - 1, chunk, chunk + 1, chunk * 2, chunk * 2 + 1):
            with self.subTest(size=size):
                await self.assert_round_trip(channel, self.patterned_body(size))

    async def test_an_oversized_body_either_arrives_or_is_refused_cleanly(self):
        connection, channel = await self.open_channel()
        body = self.patterned_body(self.OVERSIZED)
        try:
            confirmed = await channel.basic.publish(body, self.queue_name)
        except exception.AMQPChannelError:
            self.assertTrue(connection.is_open, "a refused publish must not kill the connection")
            recovery = await connection.channel()
            declared = await recovery.queue.declare(self.queue_name, passive=True)
            self.assertEqual(self.queue_name, declared["queue"])
            return
        self.assertTrue(confirmed)
        received = await channel.basic.get(self.queue_name)
        self.assertIsNotNone(received)
        self.assertEqual(body, received.body)
        await received.ack()


class ThroughputTests(base.ScenarioTestCase):
    """Bulk publish and consume, the way the original suite stressed the client.

    Heartbeats stay on: a burst must not starve either side's liveness detection,
    because transport backpressure keeps frames flowing both ways throughout.
    """

    MESSAGES = 100_000
    CONSUMERS = 4
    BULK_TIMEOUT_SECONDS = 600

    async def test_publish_and_consume_100k_messages(self):
        connection = await self.open_connection()
        setup_channel = await connection.channel()
        await setup_channel.queue.delete(self.queue_name)
        await setup_channel.queue.declare(self.queue_name, durable=True)

        consumed = asyncio.Event()
        self.messages_consumed = 0
        consumer_tasks = []
        for _ in range(self.CONSUMERS):
            channel = await connection.channel()
            await channel.basic.consume(queue=self.queue_name)
            consumer_tasks.append(
                asyncio.ensure_future(self.consume_until_target(channel, consumed))
            )
        self.addAsyncCleanup(self.cancel_all, consumer_tasks)

        for index in range(self.MESSAGES):
            await setup_channel.basic.publish(f"message {index}".encode(), self.queue_name)

        async with asyncio.timeout(self.BULK_TIMEOUT_SECONDS):
            await consumed.wait()
        self.assertEqual(self.MESSAGES, self.messages_consumed)

    async def consume_until_target(self, channel, consumed: asyncio.Event) -> None:
        async for message in channel.build_inbound_messages(break_on_empty=False):
            await message.ack()
            self.messages_consumed += 1
            if self.messages_consumed >= self.MESSAGES:
                consumed.set()
                return

    @staticmethod
    async def cancel_all(tasks) -> None:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def test_publish_and_consume_until_empty_drains_the_queue(self):
        connection = await self.open_connection()
        publish_channel = await connection.channel(rpc_timeout=self.BULK_TIMEOUT_SECONDS)
        await publish_channel.queue.delete(self.queue_name)
        await publish_channel.queue.declare(self.queue_name)
        await publish_channel.confirm_deliveries()

        confirmations = await asyncio.gather(
            *(
                publish_channel.basic.publish(f"message {index}".encode(), self.queue_name)
                for index in range(self.MESSAGES)
            )
        )
        self.assertTrue(all(confirmations), "every publish must be confirmed")

        consume_channel = await connection.channel()
        await consume_channel.basic.consume(queue=self.queue_name)
        received = 0
        async for message in consume_channel.build_inbound_messages(break_on_empty=True):
            await message.ack()
            received += 1
        self.assertEqual(self.MESSAGES, received, "not all messages were consumed")

        await consume_channel.close()
        check_channel = await connection.channel()
        declared = await check_channel.queue.declare(self.queue_name, passive=True)
        self.assertEqual(0, declared["message_count"])
        await self.wait_until_broker_reports_empty()

    async def wait_until_broker_reports_empty(self) -> None:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if await self.queue_message_count(self.queue_name) == 0:
                return
            await asyncio.sleep(0.2)
        self.fail("broker still reports messages in the queue")


if __name__ == "__main__":
    unittest.main()
