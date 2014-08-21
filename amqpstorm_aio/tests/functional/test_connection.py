import asyncio
import unittest
from collections.abc import Callable

import amqpstorm_aio
from amqpstorm_aio import exception
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory
from amqpstorm_aio.tests.functional import fake_broker


async def _wait_until(predicate: Callable[[], object], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


def _channel0_methods(session: fake_broker.BrokerSession, method_class: type) -> list:
    return [
        frame
        for frame_channel_id, frame in session.received
        if frame_channel_id == 0 and isinstance(frame, method_class)
    ]


class ConnectionFlowTests(unittest.IsolatedAsyncioTestCase):
    """Handshake, lifecycle, and channel management over a real loopback socket."""

    async def _start_broker(self, **broker_options):
        broker = fake_broker.FakeBroker(**broker_options)
        await broker.start()
        self.addAsyncCleanup(self._stop_broker, broker)
        return broker

    async def _stop_broker(self, broker):
        for session in broker.sessions:
            session.close_transport()
        await broker.stop()

    def _make_connection(self, broker, **overrides):
        parameters = {"heartbeat": 0, "timeout": 5.0}
        parameters.update(overrides)
        connection = amqpstorm_aio.Connection(
            "127.0.0.1", "guest", "guest", broker.port, **parameters
        )
        self.addAsyncCleanup(connection.close)
        return connection

    async def test_open_negotiates_server_properties_and_limits(self):
        broker = await self._start_broker(channel_max=1000, frame_max=65_536)
        connection = self._make_connection(broker)
        await connection.open()
        self.assertTrue(connection.is_open)
        self.assertEqual(connection.server_properties.get("product"), "FakeBroker")
        self.assertEqual(
            connection.max_allowed_channels, 1000, "nonzero server channel_max should win"
        )
        self.assertEqual(connection.max_frame_size, 65_536, "nonzero server frame_max should win")

    async def test_open_with_server_zero_limits_falls_back_to_client_maximums(self):
        broker = await self._start_broker(channel_max=0, frame_max=0)
        connection = self._make_connection(broker)
        await connection.open()
        self.assertEqual(
            connection.max_allowed_channels, 65_535, "server 0 means unlimited, not 0 channels"
        )
        self.assertEqual(
            connection.max_frame_size, 131_072, "server 0 means unlimited, not 0-byte frames"
        )

    async def test_tune_ok_carries_the_lower_of_two_nonzero_heartbeats(self):
        broker = await self._start_broker(heartbeat=60)
        connection = self._make_connection(broker, heartbeat=7)
        await connection.open()
        tune_ok_frames = _channel0_methods(broker.sessions[0], spec.Connection.TuneOk)
        self.assertEqual(len(tune_ok_frames), 1)
        self.assertEqual(tune_ok_frames[0].heartbeat, 7)

    async def test_client_zero_heartbeat_yields_to_the_server_value(self):
        # RabbitMQ's negotiation: a zero on either side yields to the other side;
        # heartbeats are disabled only when both sides ask for zero.
        broker = await self._start_broker(heartbeat=60)
        connection = self._make_connection(broker, heartbeat=0)
        await connection.open()
        tune_ok_frames = _channel0_methods(broker.sessions[0], spec.Connection.TuneOk)
        self.assertEqual(tune_ok_frames[0].heartbeat, 60)
        self.assertEqual(60, connection._channel0.negotiated_heartbeat)

    async def test_both_zero_heartbeats_disable_heartbeats(self):
        broker = await self._start_broker(heartbeat=0)
        connection = self._make_connection(broker, heartbeat=0)
        await connection.open()
        tune_ok_frames = _channel0_methods(broker.sessions[0], spec.Connection.TuneOk)
        self.assertEqual(tune_ok_frames[0].heartbeat, 0)
        self.assertIsNone(connection._heartbeat._task, "no heartbeat task when disabled")

    async def test_handshake_answers_a_sasl_challenge_with_the_credentials(self):
        broker = await self._start_broker(heartbeat=0)
        challenged = []

        def challenge_once(session, channel_id, frame):
            challenged.append(frame)
            session.send_method(
                0, factory.build(spec.Connection.Secure, challenge=b"prove-it")
            )

        def answer_with_tune(session, channel_id, frame):
            session.send_method(
                0,
                factory.build(
                    spec.Connection.Tune, channel_max=2047, frame_max=131_072, heartbeat=0
                ),
            )

        broker.responders[spec.Connection.StartOk.index] = challenge_once
        broker.responders[spec.Connection.SecureOk.index] = answer_with_tune
        connection = self._make_connection(broker)
        await connection.open()
        self.assertTrue(connection.is_open)
        secure_ok_frames = _channel0_methods(broker.sessions[0], spec.Connection.SecureOk)
        self.assertEqual(1, len(secure_ok_frames))
        self.assertEqual(b"\x00guest\x00guest", secure_ok_frames[0].response)

    async def test_an_endless_sasl_challenge_is_refused_not_answered_forever(self):
        broker = await self._start_broker(heartbeat=0)

        def always_challenge(session, channel_id, frame):
            session.send_method(
                0, factory.build(spec.Connection.Secure, challenge=b"again")
            )

        broker.responders[spec.Connection.StartOk.index] = always_challenge
        broker.responders[spec.Connection.SecureOk.index] = always_challenge
        connection = self._make_connection(broker, timeout=5.0)
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await connection.open()
        self.assertIn("security challenges", str(caught.exception))
        self.assertTrue(connection.is_closed)
        secure_ok_frames = _channel0_methods(broker.sessions[0], spec.Connection.SecureOk)
        self.assertLessEqual(len(secure_ok_frames), 11, "the loop must be bounded")

    async def test_close_sends_normal_shutdown_and_ends_closed_without_error(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        await connection.open()
        await connection.close()
        self.assertTrue(connection.is_closed)
        close_frames = _channel0_methods(broker.sessions[0], spec.Connection.Close)
        self.assertEqual(len(close_frames), 1)
        self.assertEqual(close_frames[0].reply_code, 200)
        self.assertEqual(close_frames[0].reply_text, "Normal shutdown")
        with self.assertRaises(exception.AMQPConnectionError) as context:
            connection.check_for_errors()
        self.assertEqual(str(context.exception), "connection closed")

    async def test_async_context_manager_opens_and_closes(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        async with connection as entered:
            self.assertIs(entered, connection)
            self.assertTrue(connection.is_open)
        self.assertTrue(connection.is_closed)

    async def test_reopen_after_close_restarts_channel_ids_at_one(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        await connection.open()
        first = await connection.channel()
        second = await connection.channel()
        self.assertEqual(first.channel_id, 1)
        self.assertEqual(second.channel_id, 2)
        await connection.close()
        await connection.open()
        self.assertTrue(connection.is_open)
        reopened = await connection.channel()
        self.assertEqual(reopened.channel_id, 1, "channel ids should restart after reopen")

    async def test_immediate_reopen_after_close_yields_usable_connection(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        await connection.open()
        await connection.close()
        await connection.open()
        channel = await connection.channel()
        self.assertEqual(channel.channel_id, 1)

    async def test_channel_on_closed_connection_raises_connection_error(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        await connection.open()
        await connection.close()
        with self.assertRaises(exception.AMQPConnectionError):
            await connection.channel()

    async def test_channel_id_allocation_reuses_closed_channel_id(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        await connection.open()
        first = await connection.channel()
        second = await connection.channel()
        self.assertEqual(first.channel_id, 1)
        self.assertEqual(second.channel_id, 2)
        await first.close()
        replacement = await connection.channel()
        self.assertEqual(replacement.channel_id, 1, "a closed channel's id should be reused")

    async def test_open_with_unsupported_mechanisms_raises_connection_error(self):
        broker = await self._start_broker(mechanisms=b"KERBEROS")
        connection = self._make_connection(broker)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        self.assertIn("Unsupported Security Mechanism(s)", str(context.exception))
        self.assertTrue(connection.is_closed)

    async def test_server_initiated_close_fails_rpc_and_answers_close_ok(self):
        broker = await self._start_broker()

        def force_close(session, channel_id, frame):
            session.send_method(
                0,
                factory.build(spec.Connection.Close,
                    reply_code=320,
                    reply_text="CONNECTION_FORCED - Closed via management plugin",
                ),
            )

        broker.responders[spec.Queue.Declare.index] = force_close
        connection = self._make_connection(broker)
        await connection.open()
        channel = await connection.channel()
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await channel.queue.declare("functional.forced")
        self.assertIn("CONNECTION_FORCED", str(context.exception))
        self.assertEqual(context.exception.reply_code, 320)
        session = broker.sessions[0]
        await _wait_until(lambda: _channel0_methods(session, spec.Connection.CloseOk))
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPConnectionError) as recorded:
            connection.check_for_errors()
        self.assertIn("CONNECTION_FORCED", str(recorded.exception))
        self.assertEqual(recorded.exception.reply_code, 320)


class ConnectionArgumentValidationTests(unittest.TestCase):
    """Constructor validation rejects bad arguments with exact messages."""

    def test_non_string_hostname_raises_exact_message(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            amqpstorm_aio.Connection(123, "guest", "guest")
        self.assertEqual(
            str(context.exception), "hostname should be a string or a sequence of strings"
        )

    def test_non_string_hostname_in_a_cluster_raises_exact_message(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            amqpstorm_aio.Connection(["localhost", 123], "guest", "guest")
        self.assertEqual(str(context.exception), "hostname should be a string")

    def test_non_integer_port_raises_exact_message(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            amqpstorm_aio.Connection("localhost", "guest", "guest", "5672")
        self.assertEqual(str(context.exception), "port should be an integer")

    def test_boolean_port_raises_exact_message(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            amqpstorm_aio.Connection("localhost", "guest", "guest", True)
        self.assertEqual(str(context.exception), "port should be an integer")

    def test_non_integer_heartbeat_raises_exact_message(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            amqpstorm_aio.Connection("localhost", "guest", "guest", heartbeat="60")
        self.assertEqual(str(context.exception), "heartbeat should be an integer")

    def test_non_numeric_timeout_raises_exact_message(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            amqpstorm_aio.Connection("localhost", "guest", "guest", timeout="10")
        self.assertEqual(str(context.exception), "timeout should be an integer or float")


class PeerAddressTests(unittest.IsolatedAsyncioTestCase):
    """The address actually connected to is reachable without touching internals."""

    async def _start_broker(self):
        broker = fake_broker.FakeBroker(heartbeat=0)
        await broker.start()
        self.addAsyncCleanup(self._stop_broker, broker)
        return broker

    @staticmethod
    async def _stop_broker(broker):
        for session in broker.sessions:
            session.close_transport()
        await broker.stop()

    def _make_connection(self, broker):
        return amqpstorm_aio.Connection(
            "127.0.0.1", "guest", "guest", broker.port, heartbeat=0, timeout=5.0
        )

    async def test_peer_address_is_none_before_opening(self):
        broker = await self._start_broker()
        self.assertIsNone(self._make_connection(broker).peer_address)

    async def test_peer_address_reports_the_connected_host_and_port(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        self.assertEqual(("127.0.0.1", broker.port), connection.peer_address)

    async def test_peer_address_is_none_again_after_closing(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        await connection.open()
        await connection.close()
        self.assertIsNone(connection.peer_address)

    async def test_opening_logs_the_address(self):
        broker = await self._start_broker()
        connection = self._make_connection(broker)
        with self.assertLogs("amqpstorm_aio.connection", level="DEBUG") as captured:
            await connection.open()
        self.addAsyncCleanup(connection.close)
        self.assertTrue(
            any(f"Connection open to 127.0.0.1:{broker.port}" in line for line in captured.output),
            captured.output,
        )

    def test_an_ipv6_peername_is_trimmed_to_host_and_port(self):
        # IPv6 reports (host, port, flowinfo, scopeid); only the first two are wanted.
        # Stubbed rather than opened, since a v6 broker is not guaranteed to exist.
        class StubTransport:
            @staticmethod
            def get_extra_info(name):
                return ("::1", 5672, 0, 0)

        connection = amqpstorm_aio.Connection("::1", "guest", "guest")
        connection._io.transport = StubTransport()
        self.assertEqual(("::1", 5672), connection.peer_address)
