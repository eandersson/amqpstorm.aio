import asyncio
import ssl
import unittest
import unittest.mock

from amqpstorm_aio import channel as channel_module
from amqpstorm_aio import channel0 as channel0_module
from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import io as io_module
from amqpstorm_aio import spec
from amqpstorm_aio import uri_connection
from amqpstorm_aio.basic import Basic
from amqpstorm_aio.tests import factory


class FakeConnection:
    """Just enough Connection surface for channel-level tests."""

    def __init__(self):
        self.written = []
        self.user_closed = False
        self.is_closed = False
        self.max_frame_size = 131_072
        self.write_error = None
        self.drain_calls = 0
        self.drain_gate = None
        self.inbound_reports = []
        self.parameters = {"heartbeat": 0}

    def check_for_errors(self):
        pass

    def write_method_frame(self, channel_id, method):
        if self.write_error is not None:
            raise self.write_error
        self.written.append((channel_id, method))

    def write_bytes(self, data):
        if self.write_error is not None:
            raise self.write_error
        self.written.append(("bytes", bytes(data)))

    def reclaim_channel_id(self, channel):
        pass

    def ensure_current_loop(self):
        pass

    def report_inbound_growth(self, channel, size):
        self.inbound_reports.append(size)

    def report_inbound_drain(self, channel, size):
        self.inbound_reports.append(-size)

    async def drain(self):
        self.drain_calls += 1
        if self.drain_gate is not None:
            await self.drain_gate.wait()


def make_channel(rpc_timeout=1):
    return channel_module.Channel(1, FakeConnection(), rpc_timeout)


class InboundWaiterTests(unittest.IsolatedAsyncioTestCase):
    """A timed-out or cancelled consumer must never deregister another's waiter."""

    async def test_two_waiters_both_wake_on_new_message(self):
        channel = make_channel()
        first = asyncio.ensure_future(channel._wait_for_inbound(None))
        second = asyncio.ensure_future(channel._wait_for_inbound(None))
        await asyncio.sleep(0)
        channel._wake_inbound()
        self.assertEqual([True, True], await asyncio.gather(first, second))

    async def test_timeout_in_one_waiter_leaves_the_other_registered(self):
        channel = make_channel()
        short = asyncio.ensure_future(channel._wait_for_inbound(0.01))
        long_lived = asyncio.ensure_future(channel._wait_for_inbound(None))
        self.assertFalse(await short, "short waiter should time out")
        channel._wake_inbound()
        self.assertTrue(
            await asyncio.wait_for(long_lived, 1),
            "surviving waiter must still be woken after the other timed out",
        )

    async def test_cancelled_waiter_leaves_the_other_registered(self):
        channel = make_channel()
        doomed = asyncio.ensure_future(channel._wait_for_inbound(None))
        survivor = asyncio.ensure_future(channel._wait_for_inbound(None))
        await asyncio.sleep(0)
        doomed.cancel()
        await asyncio.sleep(0)
        channel._wake_inbound()
        self.assertTrue(await asyncio.wait_for(survivor, 1))


class DrainWaiterTests(unittest.IsolatedAsyncioTestCase):
    """Each drain() caller owns its future; cancellation must not spread or poison."""

    def _paused_io(self):
        io = io_module.IO(lambda data: None, lambda error: None)
        io.transport = RecordingTransport()
        io.write_paused = True
        return io

    async def test_cancelling_one_drainer_does_not_cancel_the_other(self):
        io = self._paused_io()
        doomed = asyncio.ensure_future(io.drain())
        survivor = asyncio.ensure_future(io.drain())
        await asyncio.sleep(0)
        doomed.cancel()
        await asyncio.sleep(0)
        io.write_paused = False
        for waiter in list(io.drain_waiters):
            waiter.set_result(None)
        io.drain_waiters.clear()
        await asyncio.wait_for(survivor, 1)
        self.assertTrue(doomed.cancelled())

    async def test_drain_after_a_cancelled_drain_is_not_poisoned(self):
        io = self._paused_io()
        doomed = asyncio.ensure_future(io.drain())
        await asyncio.sleep(0)
        doomed.cancel()
        await asyncio.sleep(0)
        retry = asyncio.ensure_future(io.drain())
        await asyncio.sleep(0)
        self.assertFalse(retry.done(), "a fresh drain must wait, not inherit cancellation")
        for waiter in list(io.drain_waiters):
            waiter.set_result(None)
        await asyncio.wait_for(retry, 1)

    async def test_transport_loss_fails_all_drainers(self):
        io = self._paused_io()
        first = asyncio.ensure_future(io.drain())
        second = asyncio.ensure_future(io.drain())
        await asyncio.sleep(0)
        io.transport_lost(ConnectionResetError())
        for drainer in (first, second):
            with self.assertRaises(exception.AMQPConnectionError):
                await drainer


class RecordingTransport:
    """Stand-in transport that keeps every buffer handed to it."""

    def __init__(self):
        self.writes = []
        self.closing = False
        self.reading_paused = False

    def is_closing(self):
        return self.closing

    def write(self, data):
        self.writes.append(bytes(data))

    def close(self):
        self.closing = True

    def abort(self):
        self.closing = True

    def pause_reading(self):
        self.reading_paused = True

    def resume_reading(self):
        self.reading_paused = False


class ChunkedWriteTests(unittest.TestCase):
    """A buffer past the platform's single-write cap is split, in order, without loss."""

    @staticmethod
    def _io_with_transport():
        io = io_module.IO(lambda data: None, lambda error: None)
        transport = RecordingTransport()
        io.transport = transport
        return io, transport

    def test_the_write_limit_stays_under_the_platform_cap(self):
        self.assertLess(
            io_module.MAX_SINGLE_WRITE,
            2**32,
            "WSASend rejects a buffer of ULONG_MAX or more with 'buffer too large'",
        )

    def test_a_write_within_the_limit_is_passed_through_untouched(self):
        io, transport = self._io_with_transport()
        with unittest.mock.patch.object(io_module, "MAX_SINGLE_WRITE", 4):
            io.write(b"abcd")
        self.assertEqual([b"abcd"], transport.writes)

    def test_an_oversized_write_is_split_into_limit_sized_pieces(self):
        io, transport = self._io_with_transport()
        with unittest.mock.patch.object(io_module, "MAX_SINGLE_WRITE", 4):
            io.write(b"abcdefghij")
        self.assertEqual([b"abcd", b"efgh", b"ij"], transport.writes)

    def test_a_split_write_reassembles_to_the_original_bytes(self):
        io, transport = self._io_with_transport()
        payload = bytes(range(256)) * 8
        with unittest.mock.patch.object(io_module, "MAX_SINGLE_WRITE", 7):
            io.write(payload)
        self.assertGreater(len(transport.writes), 1)
        self.assertEqual(payload, b"".join(transport.writes))

    def test_a_bytearray_is_split_without_being_mutated(self):
        io, transport = self._io_with_transport()
        payload = bytearray(b"abcdefghij")
        with unittest.mock.patch.object(io_module, "MAX_SINGLE_WRITE", 4):
            io.write(payload)
        self.assertEqual(bytearray(b"abcdefghij"), payload)
        self.assertEqual(bytes(payload), b"".join(transport.writes))

    def test_a_closing_transport_refuses_an_oversized_write(self):
        io, transport = self._io_with_transport()
        transport.closing = True
        with (
            unittest.mock.patch.object(io_module, "MAX_SINGLE_WRITE", 4),
            self.assertRaises(exception.AMQPConnectionError),
        ):
            io.write(b"abcdefghij")
        self.assertEqual([], transport.writes)


class LocalCloseTests(unittest.IsolatedAsyncioTestCase):
    """A locally closed channel must fail every outstanding waiter immediately."""

    async def test_pending_confirm_future_fails_on_local_close(self):
        channel = make_channel()
        confirm = channel.register_confirm(channel.allocate_delivery_tag())
        channel._close_locally()
        with self.assertRaises(exception.AMQPChannelError):
            await confirm

    async def test_pending_get_waiter_fails_on_local_close(self):
        channel = make_channel()
        get_waiter = channel.register_get_waiter()
        channel._close_locally()
        with self.assertRaises(exception.AMQPChannelError):
            await get_waiter


class StaleRpcResponseTests(unittest.IsolatedAsyncioTestCase):
    """A late reply to a timed-out RPC must not resolve the next RPC's waiter."""

    async def test_late_response_is_discarded_and_next_rpc_gets_its_own(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        with self.assertRaises(exception.AMQPChannelError):
            await channel.rpc_request(
                factory.build(spec.Queue.Declare, queue="first"), timeout=0.01
            )
        self.assertEqual(1, len(channel._stale_rpc_expected))

        second = asyncio.ensure_future(
            channel.rpc_request(factory.build(spec.Queue.Declare, queue="second"))
        )
        await asyncio.sleep(0)
        late_reply = factory.build(
            spec.Queue.DeclareOk, queue="first", message_count=1, consumer_count=1
        )
        channel.on_frame(late_reply)
        self.assertEqual([], channel._stale_rpc_expected)
        self.assertFalse(second.done(), "late reply must not resolve the second rpc")

        real_reply = factory.build(
            spec.Queue.DeclareOk, queue="second", message_count=0, consumer_count=0
        )
        channel.on_frame(real_reply)
        self.assertIs(real_reply, await asyncio.wait_for(second, 1))


class AbandonedRpcTests(unittest.IsolatedAsyncioTestCase):
    """A cancelled RPC whose frame reached the wire must leave a stale record."""

    async def _cancelled_after_write(self, channel):
        request = asyncio.ensure_future(
            channel.rpc_request(factory.build(spec.Queue.Declare, queue="first"))
        )
        await asyncio.sleep(0)
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request

    async def test_cancelled_rpc_records_its_expected_response(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        await self._cancelled_after_write(channel)
        self.assertEqual(1, len(channel._stale_rpc_expected))

    async def test_late_response_after_cancel_does_not_resolve_the_next_rpc(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        await self._cancelled_after_write(channel)

        second = asyncio.ensure_future(
            channel.rpc_request(factory.build(spec.Queue.Declare, queue="second"))
        )
        await asyncio.sleep(0)
        late_reply = factory.build(
            spec.Queue.DeclareOk, queue="first", message_count=42, consumer_count=1
        )
        channel.on_frame(late_reply)
        self.assertFalse(second.done(), "the late reply must be discarded, not delivered")

        real_reply = factory.build(
            spec.Queue.DeclareOk, queue="second", message_count=0, consumer_count=0
        )
        channel.on_frame(real_reply)
        self.assertIs(real_reply, await asyncio.wait_for(second, 1))

    async def test_cancelled_before_the_frame_was_written_records_nothing(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        async with channel.rpc_lock:
            request = asyncio.ensure_future(
                channel.rpc_request(factory.build(spec.Queue.Declare, queue="q"))
            )
            await asyncio.sleep(0)
            request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertEqual([], channel._stale_rpc_expected)
        self.assertEqual([], channel._connection.written, "nothing may have been written")

    async def test_cancel_after_the_response_arrived_records_nothing(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        request = asyncio.ensure_future(
            channel.rpc_request(factory.build(spec.Queue.Declare, queue="q"))
        )
        await asyncio.sleep(0)
        channel.on_frame(
            factory.build(spec.Queue.DeclareOk, queue="q", message_count=0, consumer_count=0)
        )
        request.cancel()
        try:
            await request
        except asyncio.CancelledError:
            pass
        self.assertEqual(
            [], channel._stale_rpc_expected, "a consumed response is not stale"
        )


class StaleGetTests(unittest.IsolatedAsyncioTestCase):
    """A late answer to an abandoned Basic.Get must not become the next get's result."""

    @staticmethod
    def _open_channel():
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        return connection, channel

    @staticmethod
    def deliver_get_ok(channel, body=b"payload", delivery_tag=1):
        channel.on_frame(
            factory.build(
                spec.Basic.GetOk,
                delivery_tag=delivery_tag,
                redelivered=False,
                exchange="",
                routing_key="rk",
                message_count=0,
            )
        )
        channel.on_frame(framing.ContentHeader(len(body), spec.Basic.Properties()))
        channel.on_frame(body)

    async def _abandon_get(self, channel, no_ack=False):
        request = asyncio.ensure_future(Basic(channel).get("q", no_ack=no_ack))
        await asyncio.sleep(0)
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request

    def _written_rejects(self, connection):
        return [frame for _, frame in connection.written if isinstance(frame, spec.Basic.Reject)]

    async def test_late_get_ok_is_discarded_and_requeued(self):
        connection, channel = self._open_channel()
        await self._abandon_get(channel, no_ack=False)
        self.deliver_get_ok(channel, delivery_tag=7)
        rejects = self._written_rejects(connection)
        self.assertEqual(1, len(rejects), "the unwanted message must be requeued")
        self.assertEqual(7, rejects[0].delivery_tag)
        self.assertTrue(rejects[0].requeue)
        self.assertEqual(0, len(channel._stale_gets))

    async def test_late_get_ok_for_a_no_ack_get_is_dropped_without_reject(self):
        connection, channel = self._open_channel()
        await self._abandon_get(channel, no_ack=True)
        self.deliver_get_ok(channel)
        self.assertEqual([], self._written_rejects(connection))
        self.assertEqual(0, len(channel._stale_gets))

    async def test_late_get_empty_is_discarded(self):
        _connection, channel = self._open_channel()
        await self._abandon_get(channel)
        channel.on_frame(factory.build(spec.Basic.GetEmpty))
        self.assertEqual(0, len(channel._stale_gets))

    async def test_next_get_receives_its_own_message_after_a_stale_one(self):
        _connection, channel = self._open_channel()
        await self._abandon_get(channel)
        second = asyncio.ensure_future(Basic(channel).get("q"))
        await asyncio.sleep(0)
        self.deliver_get_ok(channel, body=b"stale", delivery_tag=1)
        self.assertFalse(second.done(), "the stale message must not resolve the new get")
        self.deliver_get_ok(channel, body=b"fresh", delivery_tag=2)
        result = await asyncio.wait_for(second, 1)
        self.assertEqual(b"fresh", result.body)

    async def test_get_timeout_also_records_a_stale_get(self):
        _connection, channel = self._open_channel()
        with self.assertRaises(exception.AMQPChannelError):
            await Basic(channel).get("q")
        self.assertEqual(1, len(channel._stale_gets))


class ConfirmSelectGateTests(unittest.IsolatedAsyncioTestCase):
    """No publish may hit the wire while a Confirm.Select awaits its SelectOk."""

    @staticmethod
    def _open_channel():
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        return connection, channel

    async def test_publish_waits_for_the_select_outcome(self):
        connection, channel = self._open_channel()
        select = asyncio.ensure_future(channel.confirm_deliveries())
        await asyncio.sleep(0)
        publish = asyncio.ensure_future(Basic(channel).publish(b"body", "rk"))
        await asyncio.sleep(0.001)
        self.assertEqual(1, len(connection.written), "only the select may be on the wire")
        self.assertIsInstance(connection.written[0][1], spec.Confirm.Select)
        self.assertFalse(publish.done())

        channel.on_frame(spec.Confirm.SelectOk())
        await asyncio.wait_for(select, 1)
        await asyncio.sleep(0.001)
        self.assertEqual(2, len(connection.written), "the publish follows the SelectOk")
        self.assertEqual(1, channel._next_delivery_tag, "the publish must carry tag 1")
        channel._resolve_confirms(1, False, result=True)
        self.assertTrue(await asyncio.wait_for(publish, 1))

    async def test_abandoned_select_still_enables_confirm_mode(self):
        _connection, channel = self._open_channel()
        select = asyncio.ensure_future(channel.confirm_deliveries())
        await asyncio.sleep(0)
        select.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await select
        self.assertTrue(
            channel.confirming_deliveries, "the broker will confirm; the client must agree"
        )
        self.assertEqual(1, len(channel._stale_rpc_expected))
        channel.on_frame(spec.Confirm.SelectOk())
        self.assertEqual([], channel._stale_rpc_expected, "the late SelectOk is discarded")

    async def test_select_that_never_reached_the_wire_leaves_confirm_mode_off(self):
        connection, channel = self._open_channel()
        connection.write_error = exception.AMQPConnectionError("gone")
        with self.assertRaises(exception.AMQPConnectionError):
            await channel.confirm_deliveries()
        self.assertFalse(channel.confirming_deliveries)
        self.assertIsNone(channel.confirm_select_pending)

    async def test_second_confirm_deliveries_is_a_no_op(self):
        connection, channel = self._open_channel()
        select = asyncio.ensure_future(channel.confirm_deliveries())
        await asyncio.sleep(0)
        channel.on_frame(spec.Confirm.SelectOk())
        await asyncio.wait_for(select, 1)
        await channel.confirm_deliveries()
        selects = [
            frame for _, frame in connection.written if isinstance(frame, spec.Confirm.Select)
        ]
        self.assertEqual(1, len(selects), "an already-confirming channel must not re-select")


class AbandonedChannelOpenTests(unittest.IsolatedAsyncioTestCase):
    """An abandoned Channel.Open keeps its id reserved until the broker's side closes."""

    async def _abandoned_open_channel(self):
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        open_task = asyncio.ensure_future(channel.open())
        await asyncio.sleep(0)
        open_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await open_task
        channel.set_state(channel.CLOSING)
        return connection, channel

    async def test_late_open_ok_is_answered_with_a_close(self):
        connection, channel = await self._abandoned_open_channel()
        self.assertTrue(channel.expects_stale_responses)
        channel.on_frame(factory.build(spec.Channel.OpenOk))
        closes = [frame for _, frame in connection.written if isinstance(frame, spec.Channel.Close)]
        self.assertEqual(1, len(closes), "the broker's half-open channel must be closed")
        self.assertFalse(channel.is_closed, "the id stays reserved until CloseOk lands")

    async def test_late_close_ok_frees_the_channel(self):
        _connection, channel = await self._abandoned_open_channel()
        channel.on_frame(factory.build(spec.Channel.OpenOk))
        channel.on_frame(factory.build(spec.Channel.CloseOk))
        self.assertTrue(channel.is_closed, "after CloseOk the id is reusable")

    async def test_reissued_channel_id_refuses_a_reopen(self):
        connection = FakeConnection()

        def refuse(channel):
            raise exception.AMQPChannelError("channel id 1 has been reissued")

        connection.reclaim_channel_id = refuse
        channel = channel_module.Channel(1, connection, 1)
        with self.assertRaises(exception.AMQPChannelError):
            await channel.open()
        self.assertEqual([], connection.written, "a refused reopen must not reach the wire")


class DrainAfterLossTests(unittest.IsolatedAsyncioTestCase):
    """A lost transport must never leave drain() callers stuck."""

    async def test_drain_after_transport_loss_returns_immediately(self):
        io = io_module.IO(lambda data: None, lambda error: None)
        io.transport = RecordingTransport()
        io.write_paused = True
        io.transport_lost(ConnectionResetError())
        async with asyncio.timeout(1):
            await io.drain()

    async def test_drain_with_no_transport_while_paused_raises(self):
        io = io_module.IO(lambda data: None, lambda error: None)
        io.write_paused = True
        with self.assertRaises(exception.AMQPConnectionError):
            await io.drain()

    async def test_drain_on_a_closing_transport_raises(self):
        io = io_module.IO(lambda data: None, lambda error: None)
        transport = RecordingTransport()
        transport.closing = True
        io.transport = transport
        io.write_paused = True
        with self.assertRaises(exception.AMQPConnectionError):
            await io.drain()

    async def test_close_fails_pending_drainers(self):
        io = io_module.IO(lambda data: None, lambda error: None)
        io.transport = RecordingTransport()
        io.write_paused = True
        drainer = asyncio.ensure_future(io.drain())
        await asyncio.sleep(0)
        io.close()
        with self.assertRaises(exception.AMQPConnectionError):
            await drainer
        self.assertFalse(io.write_paused)


class OutboundFrameLimitTests(unittest.IsolatedAsyncioTestCase):
    """Frames the broker would reject with a connection error are refused locally."""

    async def test_oversized_message_properties_are_refused_before_the_wire(self):
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        with self.assertRaises(exception.AMQPInvalidArgument):
            await Basic(channel).publish(
                b"body", "rk", properties={"headers": {"h": "x" * 200_000}}
            )
        self.assertEqual([], connection.written, "nothing may reach the wire")

    async def test_oversized_method_frame_is_refused_before_the_wire(self):
        connection = connection_module.Connection("127.0.0.1", "guest", "guest")
        huge_declare = factory.build(
            spec.Queue.Declare, queue="q", arguments={"padding": "x" * 200_000}
        )
        with self.assertRaises(exception.AMQPInvalidArgument):
            connection.write_method_frame(1, huge_declare)

    async def test_normal_sized_properties_still_publish(self):
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        await Basic(channel).publish(b"body", "rk", properties={"headers": {"h": "x" * 1000}})
        self.assertEqual(1, len(connection.written))


class InboundBackpressureTests(unittest.IsolatedAsyncioTestCase):
    """Buffered inbound bytes are counted on arrival and released exactly once."""

    @staticmethod
    def _open_channel():
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        return connection, channel

    @staticmethod
    def deliver(channel, body):
        channel.on_frame(
            factory.build(
                spec.Basic.Deliver,
                consumer_tag="t",
                delivery_tag=1,
                redelivered=False,
                exchange="",
                routing_key="rk",
            )
        )
        channel.on_frame(framing.ContentHeader(len(body), spec.Basic.Properties()))
        channel.on_frame(body)

    async def test_delivered_bytes_grow_and_drain_on_pop(self):
        connection, channel = self._open_channel()
        self.deliver(channel, b"x" * 100)
        self.assertEqual([100], connection.inbound_reports)
        self.assertEqual(100, channel._buffered_inbound_bytes)

        generator = channel.build_inbound_messages(break_on_empty=True, empty_timeout=0)
        message = await anext(generator)
        self.assertEqual(100, len(message.body))
        self.assertEqual([100, -100], connection.inbound_reports)
        self.assertEqual(0, channel._buffered_inbound_bytes)
        await generator.aclose()

    async def test_get_ok_bytes_drain_at_delivery(self):
        connection, channel = self._open_channel()
        get_task = asyncio.ensure_future(Basic(channel).get("q"))
        await asyncio.sleep(0)
        channel.on_frame(
            factory.build(
                spec.Basic.GetOk,
                delivery_tag=1,
                redelivered=False,
                exchange="",
                routing_key="rk",
                message_count=0,
            )
        )
        channel.on_frame(framing.ContentHeader(50, spec.Basic.Properties()))
        channel.on_frame(b"y" * 50)
        message = await asyncio.wait_for(get_task, 1)
        self.assertEqual(50, len(message.body))
        self.assertEqual([50, -50], connection.inbound_reports)
        self.assertEqual(0, channel._buffered_inbound_bytes)

    async def test_close_releases_everything_including_partial_content(self):
        connection, channel = self._open_channel()
        self.deliver(channel, b"x" * 100)
        channel.on_frame(
            factory.build(
                spec.Basic.Deliver,
                consumer_tag="t",
                delivery_tag=2,
                redelivered=False,
                exchange="",
                routing_key="rk",
            )
        )
        channel.on_frame(framing.ContentHeader(500, spec.Basic.Properties()))
        channel.on_frame(b"z" * 200)
        self.assertEqual(300, channel._buffered_inbound_bytes)
        channel._close_locally()
        self.assertEqual(0, channel._buffered_inbound_bytes)
        self.assertEqual(0, sum(connection.inbound_reports), "every byte must be released")


class BacklogWatermarkTests(unittest.IsolatedAsyncioTestCase):
    """The connection pauses reading past the high water mark and resumes below low."""

    def _connection_with_channel(self):
        connection = connection_module.Connection("127.0.0.1", "guest", "guest")
        transport = RecordingTransport()
        connection._io.transport = transport
        channel = channel_module.Channel(1, FakeConnection(), 1)
        connection._channels[1] = channel
        return connection, transport, channel

    async def test_growth_past_high_water_pauses_reading(self):
        connection, transport, channel = self._connection_with_channel()
        with unittest.mock.patch.object(
            connection_module, "INBOUND_BACKLOG_HIGH_WATER", 1000
        ), unittest.mock.patch.object(connection_module, "INBOUND_BACKLOG_LOW_WATER", 500):
            connection.report_inbound_growth(channel, 600)
            self.assertFalse(transport.reading_paused)
            connection.report_inbound_growth(channel, 600)
            self.assertTrue(transport.reading_paused)
            self.assertTrue(connection._heartbeat.reads_paused)

    async def test_drain_below_low_water_resumes_reading(self):
        connection, transport, channel = self._connection_with_channel()
        with unittest.mock.patch.object(
            connection_module, "INBOUND_BACKLOG_HIGH_WATER", 1000
        ), unittest.mock.patch.object(connection_module, "INBOUND_BACKLOG_LOW_WATER", 500):
            connection.report_inbound_growth(channel, 1200)
            self.assertTrue(transport.reading_paused)
            connection.report_inbound_drain(channel, 600)
            self.assertTrue(transport.reading_paused, "still above the low water mark")
            connection.report_inbound_drain(channel, 200)
            self.assertFalse(transport.reading_paused)
            self.assertFalse(connection._heartbeat.reads_paused)

    async def test_reports_from_a_superseded_channel_are_ignored(self):
        connection, transport, channel = self._connection_with_channel()
        replacement = channel_module.Channel(1, FakeConnection(), 1)
        connection._channels[1] = replacement
        connection.report_inbound_growth(channel, 10_000_000_000)
        self.assertEqual(0, connection._inbound_backlog_bytes)
        self.assertFalse(transport.reading_paused)


class HeartbeatReadPauseTests(unittest.IsolatedAsyncioTestCase):
    """Self-inflicted read silence must not count toward heartbeat death."""

    async def test_paused_reads_do_not_accumulate_death_threshold(self):
        from amqpstorm_aio import heartbeat as heartbeat_module

        beats = []
        heartbeat = heartbeat_module.Heartbeat(1, lambda: beats.append(1), lambda error: None)
        heartbeat.reads_paused = True
        for _ in range(10):
            self.assertTrue(heartbeat._check_for_life_signs())
        self.assertEqual(0, heartbeat._threshold)
        self.assertEqual(10, len(beats), "heartbeats keep flowing to the broker while paused")


class StaleOpenAttemptTests(unittest.IsolatedAsyncioTestCase):
    """A dying open() attempt may only tear down its own session."""

    @staticmethod
    def _connection():
        return connection_module.Connection("127.0.0.1", "guest", "guest")

    async def test_superseded_attempt_does_not_terminate_the_new_session(self):
        connection = self._connection()
        stale_io = connection._io
        connection._start_new_session()
        connection._abandon_attempt(
            stale_io, exception.AMQPConnectionError("stale failure")
        )
        self.assertIsNone(
            connection._closed_reason, "a stale attempt must not poison the new session"
        )

    async def test_current_attempt_failure_terminates_normally(self):
        connection = self._connection()
        connection.set_state(connection.OPENING)
        connection._abandon_attempt(connection._io, exception.AMQPConnectionError("boom"))
        self.assertTrue(connection.is_closed)
        with self.assertRaises(exception.AMQPConnectionError):
            connection.check_for_errors()

    async def test_attempt_is_stopped_when_user_closed_during_open(self):
        connection = self._connection()
        connection.set_state(connection.OPENING)
        connection._user_closed = True
        with self.assertRaises(exception.AMQPConnectionError):
            connection._ensure_attempt_current(connection._io)

    async def test_attempt_is_stopped_when_the_session_was_replaced(self):
        connection = self._connection()
        stale_io = connection._io
        connection._start_new_session()
        with self.assertRaises(exception.AMQPConnectionError):
            connection._ensure_attempt_current(stale_io)

    async def test_current_attempt_passes_the_guard(self):
        connection = self._connection()
        connection.set_state(connection.OPENING)
        connection._ensure_attempt_current(connection._io)


class ConfirmBookkeepingTests(unittest.IsolatedAsyncioTestCase):
    """Confirm bookkeeping: counter continuity, ordered resolution, write-failure."""

    async def test_second_confirm_deliveries_keeps_tag_counter(self):
        channel = make_channel()
        channel._confirming = True
        self.assertEqual(1, channel.allocate_delivery_tag())
        self.assertEqual(2, channel.allocate_delivery_tag())
        channel._confirming = True
        self.assertEqual(
            3, channel.allocate_delivery_tag(), "tag counter must never reset mid-session"
        )

    async def test_multiple_ack_resolves_in_order_after_out_of_order_single_ack(self):
        channel = make_channel()
        first = channel.register_confirm(channel.allocate_delivery_tag())
        second = channel.register_confirm(channel.allocate_delivery_tag())
        third = channel.register_confirm(channel.allocate_delivery_tag())
        channel._resolve_confirms(2, False, result=True)
        self.assertTrue(await second)
        channel._resolve_confirms(3, True, result=True)
        self.assertTrue(await first)
        self.assertTrue(await third)
        self.assertEqual({}, channel._confirm_futures)

    async def test_publish_write_failure_withdraws_the_confirm(self):
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel._confirming = True
        channel.set_state(channel.OPEN)
        connection.write_error = exception.AMQPConnectionError("gone")
        with self.assertRaises(exception.AMQPConnectionError):
            await Basic(channel).publish(b"body", "rk")
        self.assertEqual({}, channel._confirm_futures, "failed publish must not leak a confirm")


class PublishBackpressureTests(unittest.IsolatedAsyncioTestCase):
    """A publish must not write while the transport is above its high-water mark."""

    @staticmethod
    def _paused_channel():
        connection = FakeConnection()
        connection.drain_gate = asyncio.Event()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        return connection, channel

    async def test_publish_waits_for_drain_before_writing(self):
        connection, channel = self._paused_channel()
        publish = asyncio.ensure_future(Basic(channel).publish(b"body", "rk"))
        await asyncio.sleep(0.001)
        self.assertFalse(publish.done())
        self.assertEqual([], connection.written, "nothing may be written while paused")
        connection.drain_gate.set()
        self.assertIsNone(await asyncio.wait_for(publish, 1))
        self.assertEqual(1, connection.drain_calls)
        self.assertEqual(1, len(connection.written))

    async def test_publish_on_a_channel_that_died_while_draining_does_not_write(self):
        connection, channel = self._paused_channel()
        publish = asyncio.ensure_future(Basic(channel).publish(b"body", "rk"))
        await asyncio.sleep(0.001)
        channel._close_locally()
        connection.drain_gate.set()
        with self.assertRaises(exception.AMQPChannelError):
            await asyncio.wait_for(publish, 1)
        self.assertEqual([], connection.written, "a dead channel must not reach the wire")

    async def test_confirm_publish_allocates_its_tag_only_after_the_drain(self):
        connection, channel = self._paused_channel()
        channel._confirming = True
        publish = asyncio.ensure_future(Basic(channel).publish(b"body", "rk"))
        await asyncio.sleep(0.001)
        self.assertEqual(0, channel._next_delivery_tag, "tag must not be held across the wait")
        connection.drain_gate.set()
        await asyncio.sleep(0.001)
        self.assertEqual(1, channel._next_delivery_tag)
        self.assertEqual(1, len(connection.written))
        channel._resolve_confirms(1, False, result=True)
        self.assertTrue(await asyncio.wait_for(publish, 1))


class RemoteCloseTests(unittest.IsolatedAsyncioTestCase):
    """Server-initiated Channel.Close semantics."""

    def _deliver(self, channel, delivery_tag):
        channel.on_frame(
            factory.build(spec.Basic.Deliver, consumer_tag="t", delivery_tag=delivery_tag)
        )
        channel.on_frame(framing.ContentHeader(1, spec.Basic.Properties()))
        channel.on_frame(b"x")

    async def test_remote_close_drops_buffered_messages(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        self._deliver(channel, 1)
        self.assertEqual(1, len(channel._inbound))
        channel.on_frame(
            factory.build(
                spec.Channel.Close, reply_code=406, reply_text="PRECONDITION_FAILED"
            )
        )
        self.assertEqual(0, len(channel._inbound))
        self.assertTrue(channel.is_closed)

    async def test_remote_close_with_200_records_no_error(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        channel.on_frame(factory.build(spec.Channel.Close, reply_code=200, reply_text=""))
        self.assertTrue(channel.is_closed)
        self.assertEqual(0, len(channel._recorded_errors))

    async def test_remote_close_with_error_code_still_records(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        channel.on_frame(
            factory.build(
                spec.Channel.Close, reply_code=406, reply_text="PRECONDITION_FAILED"
            )
        )
        with self.assertRaises(exception.AMQPChannelError):
            channel.check_for_exceptions()

    async def test_server_cancel_wakes_break_on_empty_consumer(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        channel.add_consumer_tag("tag-1")

        async def consume_until_empty():
            collected = []
            async for inbound in channel.build_inbound_messages(
                break_on_empty=True, empty_timeout=30
            ):
                collected.append(inbound)
            return collected

        consumer = asyncio.ensure_future(consume_until_empty())
        await asyncio.sleep(0)
        channel.on_frame(factory.build(spec.Basic.Cancel, consumer_tag="tag-1"))
        self.assertEqual([], await asyncio.wait_for(consumer, 1))


class AckAllOutstandingTests(unittest.IsolatedAsyncioTestCase):
    """Ack with multiple=1 and delivery-tag 0 acknowledges every outstanding confirm."""

    async def test_delivery_tag_zero_with_multiple_resolves_everything(self):
        channel = make_channel()
        waiters = [channel.register_confirm(channel.allocate_delivery_tag()) for _ in range(3)]
        channel._resolve_confirms(0, True, result=True)
        self.assertEqual([True, True, True], [await waiter for waiter in waiters])
        self.assertEqual({}, channel._confirm_futures)

    async def test_delivery_tag_zero_without_multiple_resolves_nothing(self):
        channel = make_channel()
        waiter = channel.register_confirm(channel.allocate_delivery_tag())
        channel._resolve_confirms(0, False, result=True)
        self.assertFalse(waiter.done())


class ChannelFlowTests(unittest.IsolatedAsyncioTestCase):
    """Channel.Flow(active=0) stops publishes until the broker lifts it."""

    @staticmethod
    def _open_channel():
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        return connection, channel

    async def test_flow_stop_parks_publishes_until_resumed(self):
        connection, channel = self._open_channel()
        channel.on_frame(factory.build(spec.Channel.Flow, active=False))
        flow_oks = [
            frame for _, frame in connection.written if isinstance(frame, spec.Channel.FlowOk)
        ]
        self.assertEqual(1, len(flow_oks))
        self.assertFalse(flow_oks[0].active)

        publish = asyncio.ensure_future(Basic(channel).publish(b"body", "rk"))
        await asyncio.sleep(0.001)
        self.assertFalse(publish.done())
        self.assertEqual(
            1, len(connection.written), "no publish may reach the wire while stopped"
        )

        channel.on_frame(factory.build(spec.Channel.Flow, active=True))
        self.assertIsNone(await asyncio.wait_for(publish, 1))
        self.assertEqual(3, len(connection.written), "FlowOk(active) plus the publish")

    async def test_channel_death_releases_flow_parked_publishers(self):
        _connection, channel = self._open_channel()
        channel.on_frame(factory.build(spec.Channel.Flow, active=False))
        publish = asyncio.ensure_future(Basic(channel).publish(b"body", "rk"))
        await asyncio.sleep(0.001)
        channel._close_locally()
        with self.assertRaises(exception.AMQPChannelError):
            await asyncio.wait_for(publish, 1)


class ContextExitTests(unittest.IsolatedAsyncioTestCase):
    """The body's exception survives a failing close/rollback on context exit."""

    async def test_channel_aexit_prefers_the_body_exception(self):
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        with self.assertRaises(ValueError):
            async with channel:
                connection.write_error = exception.AMQPConnectionError("gone")
                raise ValueError("body failure")

    async def test_tx_aexit_swallows_rollback_failure_on_body_exception(self):
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        transaction = channel.tx
        transaction._tx_active = True
        connection.write_error = exception.AMQPConnectionError("gone")
        await transaction.__aexit__(ValueError, ValueError("body failure"), None)


class ConcurrentConnectionCloseTests(unittest.IsolatedAsyncioTestCase):
    """Two concurrent Connection.close() calls share one Close/CloseOk exchange."""

    async def test_only_one_connection_close_frame_is_sent(self):
        connection = FakeConnection()
        channel0 = channel0_module.Channel0(connection)
        first = asyncio.ensure_future(channel0.close(1))
        await asyncio.sleep(0)
        second = asyncio.ensure_future(channel0.close(1))
        await asyncio.sleep(0)
        channel0.on_frame(factory.build(spec.Connection.CloseOk))
        await asyncio.gather(first, second)
        closes = [
            frame for _, frame in connection.written if isinstance(frame, spec.Connection.Close)
        ]
        self.assertEqual(1, len(closes))


class RpcTimeoutValidationTests(unittest.IsolatedAsyncioTestCase):
    """The per-call rpc timeout is validated like every other timeout."""

    async def test_negative_timeout_raises_invalid_argument(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        with self.assertRaises(exception.AMQPInvalidArgument):
            await channel.rpc_request(factory.build(spec.Queue.Declare, queue="q"), timeout=-1)

    async def test_non_numeric_timeout_raises_invalid_argument(self):
        channel = make_channel()
        channel.set_state(channel.OPEN)
        with self.assertRaises(exception.AMQPInvalidArgument):
            await channel.rpc_request(factory.build(spec.Queue.Declare, queue="q"), timeout="5")


class ApiValidationLowTests(unittest.IsolatedAsyncioTestCase):
    """Validation gaps surface as AMQPInvalidArgument, never raw builtin errors."""

    @staticmethod
    def _open_channel():
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        return connection, channel

    async def test_non_string_header_key_raises_invalid_argument(self):
        _connection, channel = self._open_channel()
        with self.assertRaises(exception.AMQPInvalidArgument):
            await Basic(channel).publish(b"body", "rk", properties={"headers": {1: "x"}})

    async def test_lone_surrogate_routing_key_raises_invalid_argument(self):
        _connection, channel = self._open_channel()
        with self.assertRaises(exception.AMQPInvalidArgument):
            await Basic(channel).publish(b"body", "rk\udc80")

    async def test_non_codec_content_encoding_on_str_body_raises_invalid_argument(self):
        _connection, channel = self._open_channel()
        with self.assertRaises(exception.AMQPInvalidArgument):
            await Basic(channel).publish(
                "text body", "rk", properties={"content_encoding": "gzip"}
            )

    async def test_callable_object_consumer_callback_is_accepted(self):
        _connection, channel = self._open_channel()

        class AsyncHandler:
            async def __call__(self, message):
                pass

        consume_task = asyncio.ensure_future(
            Basic(channel).consume(AsyncHandler(), queue="q")
        )
        await asyncio.sleep(0)
        channel.on_frame(factory.build(spec.Basic.ConsumeOk, consumer_tag="tag-1"))
        self.assertEqual("tag-1", await asyncio.wait_for(consume_task, 1))

    async def test_non_callable_consumer_callback_raises_invalid_argument(self):
        _connection, channel = self._open_channel()
        with self.assertRaises(exception.AMQPInvalidArgument):
            await Basic(channel).consume("not-callable", queue="q")

    async def test_message_text_with_non_codec_content_encoding_raises_message_error(self):
        from amqpstorm_aio import message as message_module

        _connection, channel = self._open_channel()
        message = message_module.Message(
            channel,
            b"\x1f\x8b compressed",
            factory.build(
                spec.Basic.Deliver,
                consumer_tag="t",
                delivery_tag=1,
                redelivered=False,
                exchange="",
                routing_key="rk",
            ),
            spec.Basic.Properties(content_encoding="gzip"),
        )
        with self.assertRaises(exception.AMQPMessageError):
            _ = message.text


class ReturnCorrelationTests(unittest.IsolatedAsyncioTestCase):
    """A mandatory confirmed publish only claims Returns recorded during its flight."""

    @staticmethod
    def _open_confirming_channel():
        connection = FakeConnection()
        channel = channel_module.Channel(1, connection, 1)
        channel.set_state(channel.OPEN)
        channel._confirming = True
        return connection, channel

    @staticmethod
    def _record_return(channel):
        channel._on_basic_return(
            factory.build(
                spec.Basic.Return,
                reply_code=312,
                reply_text="NO_ROUTE",
                exchange="",
                routing_key="rk",
            )
        )

    async def test_stale_return_from_an_earlier_publish_is_not_claimed(self):
        _connection, channel = self._open_confirming_channel()
        self._record_return(channel)
        with self.assertRaises(exception.AMQPMessageError):
            channel.check_for_exceptions()

        publish = asyncio.ensure_future(
            Basic(channel).publish(b"body", "rk", mandatory=True)
        )
        await asyncio.sleep(0.001)
        channel._resolve_confirms(1, False, result=True)
        self.assertIs(
            True,
            await asyncio.wait_for(publish, 1),
            "an old Return must not be pinned on this publish",
        )

    async def test_return_during_the_flight_is_raised(self):
        _connection, channel = self._open_confirming_channel()
        publish = asyncio.ensure_future(
            Basic(channel).publish(b"body", "rk", mandatory=True)
        )
        await asyncio.sleep(0.001)
        self._record_return(channel)
        channel._resolve_confirms(1, False, result=True)
        with self.assertRaises(exception.AMQPMessageError):
            await asyncio.wait_for(publish, 1)


class LoopAffinityTests(unittest.IsolatedAsyncioTestCase):
    """A connection opened on one loop refuses use from another loop."""

    async def test_use_from_a_foreign_loop_raises(self):
        import threading

        connection = connection_module.Connection("127.0.0.1", "guest", "guest")
        connection._loop = asyncio.get_running_loop()
        failure: list[Exception] = []

        def run_in_foreign_loop():
            async def attempt():
                try:
                    connection.ensure_current_loop()
                except exception.AMQPConnectionError as error:
                    failure.append(error)

            asyncio.run(attempt())

        thread = threading.Thread(target=run_in_foreign_loop)
        thread.start()
        await asyncio.to_thread(thread.join)
        self.assertEqual(1, len(failure))
        self.assertIn("another event loop", str(failure[0]))

    async def test_use_from_the_owning_loop_passes(self):
        connection = connection_module.Connection("127.0.0.1", "guest", "guest")
        connection._loop = asyncio.get_running_loop()
        connection.ensure_current_loop()


class AuthenticationSelectionTests(unittest.TestCase):
    """EXTERNAL must not silently discard explicitly configured credentials."""

    def _channel0(self, username, password):
        connection = FakeConnection()
        connection.parameters = {"username": username, "password": password, "heartbeat": 0}
        return channel0_module.Channel0(connection)

    def test_explicit_credentials_prefer_plain_over_external(self):
        mechanism, response = self._channel0("erik", "secret")._select_authentication(
            b"PLAIN EXTERNAL"
        )
        self.assertEqual("PLAIN", mechanism)
        self.assertEqual(b"\x00erik\x00secret", response)

    def test_default_credentials_prefer_external(self):
        mechanism, response = self._channel0("guest", "guest")._select_authentication(
            b"PLAIN EXTERNAL"
        )
        self.assertEqual("EXTERNAL", mechanism)
        self.assertEqual(b"\x00\x00", response)

    def test_external_only_still_used_with_explicit_credentials(self):
        mechanism, _response = self._channel0("erik", "secret")._select_authentication(
            b"EXTERNAL"
        )
        self.assertEqual("EXTERNAL", mechanism)


class SslOptionTests(unittest.TestCase):
    """verify_mode='optional' keeps hostname verification on."""

    def test_optional_verify_mode_keeps_check_hostname(self):
        context = io_module.build_ssl_context({"verify_mode": "optional"})
        self.assertEqual(ssl.CERT_OPTIONAL, context.verify_mode)
        self.assertTrue(context.check_hostname)

    def test_none_verify_mode_disables_check_hostname(self):
        context = io_module.build_ssl_context({"verify_mode": "none"})
        self.assertEqual(ssl.CERT_NONE, context.verify_mode)
        self.assertFalse(context.check_hostname)


class UriErrorTypeTests(unittest.TestCase):
    """URI conversion failures surface as AMQPInvalidArgument (still a ValueError)."""

    def test_garbage_heartbeat_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.parse_uri("amqp://localhost?heartbeat=soon")

    def test_garbage_timeout_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.parse_uri("amqp://localhost?timeout=fast")


if __name__ == "__main__":
    unittest.main()
