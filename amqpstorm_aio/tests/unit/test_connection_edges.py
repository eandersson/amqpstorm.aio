import asyncio
import unittest

from amqpstorm_aio import base
from amqpstorm_aio import channel as channel_module
from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory

HIGH_WATER = connection_module.INBOUND_BACKLOG_HIGH_WATER
LOW_WATER = connection_module.INBOUND_BACKLOG_LOW_WATER


def make_connection(**overrides):
    """A Connection that is never opened; no socket is ever created."""
    parameters = {"username": "guest", "password": "guest"}
    parameters.update(overrides)
    return connection_module.Connection("node.example", **parameters)


def open_connection(**overrides):
    """A Connection parked in the OPEN state without a transport behind it."""
    connection = make_connection(**overrides)
    connection.set_state(base.Stateful.OPEN)
    return connection


class RecordingTransport:
    """Stand-in transport that records the calls a live socket would receive."""

    def __init__(self, peername=("10.0.0.7", 5672)):
        self.peername = peername
        self.closing = False
        self.aborted = False
        self.reading_paused = False
        self.writes = []

    def get_extra_info(self, name):
        return self.peername if name == "peername" else None

    def is_closing(self):
        return self.closing

    def write(self, data):
        self.writes.append(bytes(data))

    def close(self):
        self.closing = True

    def abort(self):
        self.closing = True
        self.aborted = True

    def pause_reading(self):
        self.reading_paused = True

    def resume_reading(self):
        self.reading_paused = False


async def wait_until(predicate, message, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() >= deadline:
            raise AssertionError(message)
        await asyncio.sleep(0.005)


def method_frame_bytes(channel_id, method):
    buffer = bytearray()
    framing.append_method_frame(buffer, channel_id, method)
    return bytes(buffer)


class ParameterRejectionTests(unittest.TestCase):
    """Bad parameters are refused at construction, before any socket exists."""

    def test_a_port_outside_the_short_range_is_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            make_connection(port=70_000)
        self.assertEqual("port should be between 0 and 65535", str(context.exception))

    def test_a_non_boolean_ssl_flag_is_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            make_connection(ssl="yes")
        self.assertEqual("ssl should be a boolean", str(context.exception))

    def test_a_zero_timeout_is_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            make_connection(timeout=0)
        self.assertEqual("timeout should be greater than 0", str(context.exception))

    def test_client_properties_that_are_not_a_table_are_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            make_connection(client_properties=[("product", "test")])
        self.assertEqual("client_properties should be a dict or None", str(context.exception))

    def test_a_virtual_host_longer_than_the_domain_allows_is_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            make_connection(virtual_host="v" * 300)
        self.assertIn("virtual_host should be at most", str(context.exception))


class ContextManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_entering_an_already_open_connection_does_not_reopen_it(self):
        connection = open_connection()
        self.assertIs(
            connection,
            await connection.__aenter__(),
            "re-entering must skip open(), which would have raised 'already open'",
        )
        self.assertTrue(connection.is_open)

    async def test_an_unhandled_exception_in_the_block_is_logged_and_still_propagates(self):
        connection = open_connection()
        with (
            self.assertLogs("amqpstorm_aio.connection", level="WARNING") as captured,
            self.assertRaises(RuntimeError),
        ):
            async with connection:
                raise RuntimeError("boom")
        self.assertTrue(
            any("unhandled exception: boom" in line for line in captured.output),
            captured.output,
        )
        self.assertTrue(connection.is_closed, "the block must still close the connection")
        self.assertTrue(connection.user_closed)

    async def test_leaving_the_block_cleanly_closes_without_warning_logs(self):
        connection = open_connection()
        with self.assertNoLogs("amqpstorm_aio.connection", level="WARNING"):
            async with connection:
                self.assertTrue(connection.is_open)
        self.assertTrue(connection.is_closed)


class StateReportingTests(unittest.IsolatedAsyncioTestCase):
    def test_a_broker_block_and_unblock_are_visible_on_the_connection(self):
        connection = open_connection()
        self.assertFalse(connection.is_blocked)
        blocked = factory.build(spec.Connection.Blocked, reason="low disk")
        connection._on_data(method_frame_bytes(0, blocked))
        self.assertTrue(connection.is_blocked, "a channel 0 frame must reach channel0")
        connection._on_data(method_frame_bytes(0, factory.build(spec.Connection.Unblocked)))
        self.assertFalse(connection.is_blocked)

    async def test_user_closed_only_becomes_true_after_a_local_close(self):
        connection = open_connection()
        self.assertFalse(connection.user_closed)
        await connection.close()
        self.assertTrue(connection.user_closed)

    async def test_a_broker_initiated_close_leaves_user_closed_false(self):
        connection = open_connection()
        connection.on_remote_close(exception.AMQPConnectionError("connection forced"))
        self.assertTrue(connection.is_closed)
        self.assertFalse(connection.user_closed, "the broker closed us, not the application")

    def test_peer_address_is_none_while_the_transport_reports_no_peer(self):
        connection = make_connection()
        connection._io.transport = RecordingTransport(peername=None)
        self.assertIsNone(connection.peer_address)

    def test_peer_address_is_none_before_a_transport_exists(self):
        self.assertIsNone(make_connection().peer_address)

    def test_peer_address_reports_the_node_actually_connected_to(self):
        connection = make_connection()
        connection._io.transport = RecordingTransport(peername=("10.0.0.7", 5672))
        self.assertEqual(("10.0.0.7", 5672), connection.peer_address)


class InboundBacklogTests(unittest.IsolatedAsyncioTestCase):
    def _connection_with_channel(self):
        connection = open_connection()
        transport = RecordingTransport()
        connection._io.transport = transport
        channel = channel_module.Channel(1, connection, 60)
        connection.channels[1] = channel
        return connection, channel, transport

    def test_buffered_bytes_are_accumulated_and_released_per_report(self):
        connection, channel, _transport = self._connection_with_channel()
        self.assertEqual(0, connection.inbound_backlog_bytes)
        connection.report_inbound_growth(channel, 4_096)
        self.assertEqual(4_096, connection.inbound_backlog_bytes)
        self.assertFalse(connection.is_read_paused, "a small backlog must not pause reads")
        connection.report_inbound_drain(channel, 4_096)
        self.assertEqual(0, connection.inbound_backlog_bytes)

    def test_crossing_the_high_water_pauses_reads_until_the_backlog_drains(self):
        connection, channel, transport = self._connection_with_channel()
        with self.assertLogs("amqpstorm_aio.connection", level="WARNING") as captured:
            connection.report_inbound_growth(channel, HIGH_WATER + 1)
        self.assertTrue(connection.is_read_paused)
        self.assertTrue(transport.reading_paused)
        self.assertTrue(any("pausing reads" in line for line in captured.output), captured.output)
        connection.report_inbound_drain(channel, HIGH_WATER + 1 - LOW_WATER)
        self.assertFalse(connection.is_read_paused)
        self.assertFalse(transport.reading_paused)
        self.assertEqual(LOW_WATER, connection.inbound_backlog_bytes)

    def test_a_report_from_a_channel_that_was_replaced_is_ignored(self):
        connection, channel, _transport = self._connection_with_channel()
        connection.report_inbound_growth(channel, 1_000)
        connection.channels[1] = channel_module.Channel(1, connection, 60)
        connection.report_inbound_drain(channel, 1_000)
        self.assertEqual(
            1_000,
            connection.inbound_backlog_bytes,
            "a stale channel must not decrement another channel's backlog",
        )

    def test_growth_reported_by_a_channel_that_was_replaced_is_ignored(self):
        connection, channel, _transport = self._connection_with_channel()
        connection.channels[1] = channel_module.Channel(1, connection, 60)
        connection.report_inbound_growth(channel, 1_000)
        self.assertEqual(0, connection.inbound_backlog_bytes)


class OpenRefusalTests(unittest.IsolatedAsyncioTestCase):
    async def test_opening_an_already_open_connection_is_refused(self):
        connection = open_connection()
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        self.assertEqual("connection already open", str(context.exception))
        self.assertTrue(connection.is_open, "the refusal must not disturb the open connection")


class ChannelAllocationTests(unittest.IsolatedAsyncioTestCase):
    async def test_requesting_a_channel_while_still_opening_is_refused(self):
        connection = make_connection()
        transport = RecordingTransport()
        connection._io.transport = transport
        connection.set_state(base.Stateful.OPENING)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.channel(rpc_timeout=1)
        self.assertEqual("connection closed", str(context.exception))
        self.assertEqual({}, connection.channels, "no channel may be allocated while opening")
        self.assertEqual(
            [], transport.writes, "Channel.Open must not reach a half-open connection"
        )

    async def test_a_channel_that_cannot_be_opened_releases_its_id(self):
        connection = open_connection()
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.channel()
        self.assertEqual("connection closed", str(context.exception))
        self.assertEqual(
            {}, connection.channels, "a channel that never opened must not hold on to its id"
        )

    async def test_running_out_of_channel_ids_names_the_limit(self):
        connection = open_connection()
        connection._channel0.max_allowed_channels = 2
        for channel_id in (1, 2):
            channel = channel_module.Channel(channel_id, connection, 60)
            channel.set_state(base.Stateful.OPEN)
            connection.channels[channel_id] = channel
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.channel()
        self.assertEqual("reached the maximum number of channels 2", str(context.exception))

    async def test_a_closed_channels_id_is_handed_out_again(self):
        connection = open_connection()
        connection._channel0.max_allowed_channels = 1
        retired = channel_module.Channel(1, connection, 60)
        connection.channels[1] = retired
        self.assertEqual(1, connection._get_next_available_channel_id())

    async def test_an_unused_channel_id_can_be_reclaimed_by_a_reopening_channel(self):
        connection = open_connection()
        channel = channel_module.Channel(7, connection, 60)
        connection.reclaim_channel_id(channel)
        self.assertIs(channel, connection.channels[7])

    async def test_reclaiming_an_id_already_held_by_an_open_channel_is_refused(self):
        connection = open_connection()
        holder = channel_module.Channel(7, connection, 60)
        holder.set_state(base.Stateful.OPEN)
        connection.channels[7] = holder
        with self.assertRaises(exception.AMQPChannelError) as context:
            connection.reclaim_channel_id(channel_module.Channel(7, connection, 60))
        self.assertEqual(
            "channel id 7 has been reissued to another channel", str(context.exception)
        )
        self.assertIs(holder, connection.channels[7])


class InboundFrameTests(unittest.IsolatedAsyncioTestCase):
    def test_a_frame_for_an_unknown_channel_is_logged_and_dropped(self):
        connection = open_connection()
        frame = method_frame_bytes(9, factory.build(spec.Basic.Ack, delivery_tag=1))
        with self.assertLogs("amqpstorm_aio.connection", level="ERROR") as captured:
            connection._on_data(frame)
        self.assertTrue(
            any("unknown channel 9" in line for line in captured.output), captured.output
        )
        self.assertTrue(connection.is_open, "an orphan frame must not kill the connection")

    def test_a_frame_is_delivered_only_to_the_channel_it_is_addressed_to(self):
        connection = open_connection()
        connection._io.transport = RecordingTransport()
        addressed = channel_module.Channel(1, connection, 60)
        bystander = channel_module.Channel(2, connection, 60)
        for channel in (addressed, bystander):
            channel.set_state(base.Stateful.OPEN)
            connection.channels[channel.channel_id] = channel
        close = factory.build(
            spec.Channel.Close, reply_code=404, reply_text="NOT_FOUND - no queue 'x'"
        )
        connection._on_data(method_frame_bytes(1, close))
        with self.assertRaises(exception.AMQPChannelError) as context:
            addressed.check_for_errors()
        self.assertEqual("Channel 1 was closed by remote server: NOT_FOUND - no queue 'x'",
                         str(context.exception))
        self.assertFalse(bystander.is_closed, "only the addressed channel may react")
        self.assertTrue(connection.is_open)

    def test_a_malformed_frame_end_terminates_the_connection_with_the_frame_error(self):
        connection = open_connection()
        frame = bytearray(method_frame_bytes(0, factory.build(spec.Basic.Ack, delivery_tag=1)))
        frame[-1] = 0x00
        connection._on_data(bytes(frame))
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPFrameError) as context:
            connection.check_for_errors()
        self.assertIn("missing frame-end marker", str(context.exception))

    def test_a_decode_failure_that_is_not_an_amqp_error_is_wrapped_as_a_frame_error(self):
        connection = open_connection()
        truncated = bytearray()
        framing.append_frame(truncated, spec.FRAME_METHOD, 1, b"\x00\x3c")
        with self.assertLogs("amqpstorm_aio.connection", level="ERROR"):
            connection._on_data(bytes(truncated))
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPFrameError) as context:
            connection.check_for_errors()
        self.assertIn("failed to decode inbound frame", str(context.exception))
        self.assertIsInstance(
            context.exception.__cause__, Exception, "the original decode error must be kept"
        )
        self.assertNotIsInstance(context.exception.__cause__, exception.AMQPError)


class ConnectionLostTests(unittest.IsolatedAsyncioTestCase):
    def _open_with_transport(self):
        connection = open_connection()
        transport = RecordingTransport()
        connection._io.transport = transport
        return connection, transport

    def test_an_unexplained_transport_loss_records_a_plain_closed_error(self):
        connection, _transport = self._open_with_transport()
        connection._io.transport_lost(None)
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            connection.check_for_errors()
        self.assertEqual("connection closed", str(context.exception))

    def test_a_transport_error_is_named_in_the_recorded_close_reason(self):
        connection, _transport = self._open_with_transport()
        connection._io.transport_lost(ConnectionResetError("reset by peer"))
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            connection.check_for_errors()
        self.assertEqual("connection closed: reset by peer", str(context.exception))

    def test_a_transport_loss_after_a_local_close_records_no_error(self):
        connection, transport = self._open_with_transport()
        connection._user_closed = True
        connection._io.transport_lost(None)
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            connection.check_for_errors()
        self.assertEqual("connection closed", str(context.exception))
        self.assertIsNone(connection._closed_reason, "a local close is not a failure")
        self.assertFalse(transport.aborted)

    def test_a_lost_transport_fails_the_channels_waiters(self):
        connection, _transport = self._open_with_transport()
        channel = channel_module.Channel(1, connection, 60)
        channel.set_state(base.Stateful.OPEN)
        connection.channels[1] = channel
        connection._io.transport_lost(ConnectionResetError("reset by peer"))
        self.assertTrue(channel.is_closed)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            channel.check_for_errors()
        self.assertEqual("connection closed: reset by peer", str(context.exception))


class HeartbeatDeathTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_dead_heartbeat_logs_and_terminates_the_connection(self):
        connection = open_connection(heartbeat=1)
        self.addCleanup(connection._heartbeat.stop)
        with self.assertLogs("amqpstorm_aio.connection", level="ERROR") as captured:
            self.assertTrue(connection._heartbeat.start())
            await wait_until(
                lambda: connection.is_closed, "the dead heartbeat never closed the connection"
            )
        self.assertTrue(
            any("no heartbeat or data received" in line for line in captured.output),
            captured.output,
        )
        with self.assertRaises(exception.AMQPConnectionError) as context:
            connection.check_for_errors()
        self.assertIn("no heartbeat or data received", str(context.exception))

    async def test_inbound_traffic_keeps_the_heartbeat_from_declaring_death(self):
        connection = open_connection(heartbeat=1)
        connection._io.transport = RecordingTransport()
        self.addCleanup(connection._heartbeat.stop)
        self.assertTrue(connection._heartbeat.start())
        deadline = asyncio.get_running_loop().time() + 1.5
        while asyncio.get_running_loop().time() < deadline:
            connection._on_data(framing.HEARTBEAT_FRAME)
            await asyncio.sleep(0.05)
        self.assertTrue(
            connection.is_open, "inbound frames must be registered as signs of life"
        )
        connection.check_for_errors()


if __name__ == "__main__":
    unittest.main()
