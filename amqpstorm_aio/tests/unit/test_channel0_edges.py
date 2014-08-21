import asyncio
import unittest

from amqpstorm_aio import channel0 as channel0_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory


class FakeConnection:
    """Just enough Connection surface for channel-0 tests."""

    def __init__(self):
        self.parameters = {
            "username": "guest",
            "password": "guest",
            "locale": "en_US",
            "heartbeat": 60,
            "virtual_host": "/",
            "client_properties": {},
        }
        self.sent = []
        self.attempted_methods = []
        self.written_bytes = []
        self.attempted_bytes = []
        self.remote_closes = []
        self.method_error = None
        self.bytes_error = None
        self.is_open = True
        self.is_closed = False

    def write_method_frame(self, channel_id, method):
        self.attempted_methods.append((channel_id, method))
        if self.method_error is not None:
            raise self.method_error
        self.sent.append((channel_id, method))

    def write_bytes(self, data):
        self.attempted_bytes.append(bytes(data))
        if self.bytes_error is not None:
            raise self.bytes_error
        self.written_bytes.append(bytes(data))

    def on_remote_close(self, error):
        self.remote_closes.append(error)


def make_channel0():
    connection = FakeConnection()
    return connection, channel0_module.Channel0(connection)


class BlockedNotificationTests(unittest.TestCase):
    """connection.blocked/unblocked flip the flag the publisher path reads."""

    def test_blocked_sets_the_flag_and_logs_the_reason(self):
        _connection, channel0 = make_channel0()
        with self.assertLogs(channel0_module.LOG, level="WARNING") as logs:
            channel0.on_frame(factory.build(spec.Connection.Blocked, reason="low on disk"))
        self.assertTrue(channel0.is_blocked)
        self.assertIn("low on disk", logs.output[0])

    def test_unblocked_clears_the_flag(self):
        _connection, channel0 = make_channel0()
        channel0.on_frame(factory.build(spec.Connection.Blocked, reason="low on disk"))
        with self.assertLogs(channel0_module.LOG, level="INFO") as logs:
            channel0.on_frame(factory.build(spec.Connection.Unblocked))
        self.assertFalse(channel0.is_blocked)
        self.assertIn("no longer blocked", logs.output[0])


class UnexpectedFrameTests(unittest.IsolatedAsyncioTestCase):
    """Frames nobody is waiting for are logged and otherwise ignored."""

    def test_an_unexpected_method_is_logged_as_an_error(self):
        _connection, channel0 = make_channel0()
        frame = factory.build(spec.Basic.Deliver, consumer_tag="ctag")
        with self.assertLogs(channel0_module.LOG, level="ERROR") as logs:
            channel0.on_frame(frame)
        self.assertIn("Unhandled Frame", logs.output[0])

    def test_a_heartbeat_is_dropped_without_logging(self):
        _connection, channel0 = make_channel0()
        with self.assertNoLogs(channel0_module.LOG):
            channel0.on_frame(framing.Heartbeat())

    async def test_an_expected_frame_arriving_after_the_waiter_failed_is_ignored(self):
        _connection, channel0 = make_channel0()
        expecting = asyncio.ensure_future(channel0._expect(spec.Connection.Start))
        await asyncio.sleep(0)
        channel0.fail_waiters(exception.AMQPConnectionError("socket died"))
        channel0.on_frame(factory.build(spec.Connection.Start, mechanisms=b"PLAIN"))
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await expecting
        self.assertEqual("socket died", str(caught.exception))


class UnsolicitedCloseOkTests(unittest.IsolatedAsyncioTestCase):
    """A CloseOk with no close in flight must not disturb later shutdowns."""

    async def test_a_close_ok_with_no_waiter_leaves_close_able_to_run(self):
        connection, channel0 = make_channel0()
        channel0.on_frame(factory.build(spec.Connection.CloseOk))
        closing = asyncio.ensure_future(channel0.close(30))
        await asyncio.sleep(0)
        channel0.on_frame(factory.build(spec.Connection.CloseOk))
        await asyncio.wait_for(closing, 1)
        self.assertEqual(
            1,
            len([f for _, f in connection.sent if isinstance(f, spec.Connection.Close)]),
        )
        self.assertIsNone(channel0._close_ok_waiter)


class CloseWithoutAcknowledgementTests(unittest.IsolatedAsyncioTestCase):
    """close() is best effort: a broker that never answers must not hang the shutdown."""

    async def test_a_close_ok_that_never_arrives_times_out_quietly(self):
        connection, channel0 = make_channel0()
        loop = asyncio.get_running_loop()
        started = loop.time()
        await channel0.close(0.05)
        self.assertGreaterEqual(loop.time() - started, 0.05)
        self.assertIsInstance(connection.sent[0][1], spec.Connection.Close)
        self.assertIsNone(channel0._close_ok_waiter)

    async def test_a_connection_error_while_awaiting_close_ok_is_swallowed(self):
        connection, channel0 = make_channel0()
        closing = asyncio.ensure_future(channel0.close(30))
        await asyncio.sleep(0)
        channel0.fail_waiters(exception.AMQPConnectionError("socket died"))
        await asyncio.wait_for(closing, 1)
        self.assertIsInstance(connection.sent[0][1], spec.Connection.Close)
        self.assertIsNone(channel0._close_ok_waiter)


class PendingCloseTests(unittest.IsolatedAsyncioTestCase):
    """A second close() joins the first exchange and never sends its own frame."""

    async def test_a_joining_close_returns_quietly_when_the_first_times_out(self):
        connection, channel0 = make_channel0()
        loop = asyncio.get_running_loop()
        channel0._close_ok_waiter = loop.create_future()
        started = loop.time()
        await channel0.close(0.05)
        self.assertGreaterEqual(loop.time() - started, 0.05)
        self.assertEqual([], connection.attempted_methods)

    async def test_a_joining_close_returns_quietly_when_the_first_fails(self):
        connection, channel0 = make_channel0()
        pending = asyncio.get_running_loop().create_future()
        channel0._close_ok_waiter = pending
        channel0.fail_waiters(exception.AMQPConnectionError("socket died"))
        await asyncio.wait_for(channel0.close(30), 1)
        self.assertEqual([], connection.attempted_methods)
        self.assertIsInstance(pending.exception(), exception.AMQPConnectionError)


class HeartbeatWriteTests(unittest.TestCase):
    """send_heartbeat never raises out of the heartbeat timer."""

    def test_no_frame_is_written_while_the_connection_is_not_open(self):
        connection, channel0 = make_channel0()
        connection.is_open = False
        channel0.send_heartbeat()
        self.assertEqual([], connection.attempted_bytes)

    def test_a_heartbeat_frame_is_written_while_the_connection_is_open(self):
        connection, channel0 = make_channel0()
        channel0.send_heartbeat()
        self.assertEqual([framing.HEARTBEAT_FRAME], connection.written_bytes)

    def test_a_write_failure_is_attempted_and_then_swallowed(self):
        connection, channel0 = make_channel0()
        connection.bytes_error = exception.AMQPConnectionError("socket died")
        channel0.send_heartbeat()
        self.assertEqual([framing.HEARTBEAT_FRAME], connection.attempted_bytes)
        self.assertEqual([], connection.written_bytes)


class ExpectOnClosedConnectionTests(unittest.IsolatedAsyncioTestCase):
    """The handshake refuses to wait on a socket that is already gone."""

    async def test_negotiate_on_a_closed_connection_raises_immediately(self):
        connection, channel0 = make_channel0()
        connection.is_closed = True
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await channel0.negotiate(5)
        self.assertEqual("connection closed", str(caught.exception))


class NegotiateTimeoutTests(unittest.IsolatedAsyncioTestCase):
    """A silent broker fails the handshake with a timeout message, not a hang."""

    async def test_a_broker_that_never_sends_start_times_out(self):
        _connection, channel0 = make_channel0()
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await channel0.negotiate(0.01)
        self.assertIn("handshake timed out after 0.01s", str(caught.exception))


class ServerCloseTests(unittest.TestCase):
    """Connection.Close from the broker is acknowledged and reported upwards."""

    def test_a_graceful_server_close_reports_no_error(self):
        connection, channel0 = make_channel0()
        channel0.on_frame(
            factory.build(
                spec.Connection.Close,
                reply_code=spec.REPLY_SUCCESS,
                reply_text="OK",
            )
        )
        self.assertEqual([None], connection.remote_closes)
        self.assertIsInstance(connection.sent[0][1], spec.Connection.CloseOk)

    def test_a_faulty_server_close_reports_the_reply_code_and_text(self):
        connection, channel0 = make_channel0()
        channel0.on_frame(
            factory.build(
                spec.Connection.Close,
                reply_code=320,
                reply_text="CONNECTION_FORCED",
            )
        )
        error = connection.remote_closes[0]
        self.assertIsInstance(error, exception.AMQPConnectionError)
        self.assertIn("CONNECTION_FORCED", str(error))
        self.assertEqual(320, error.reply_code)

    def test_a_close_ok_that_cannot_be_written_still_reports_the_close(self):
        connection, channel0 = make_channel0()
        connection.method_error = exception.AMQPConnectionError("socket died")
        channel0.on_frame(
            factory.build(spec.Connection.Close, reply_code=320, reply_text="CONNECTION_FORCED")
        )
        self.assertEqual([], connection.sent)
        self.assertEqual(1, len(connection.attempted_methods))
        self.assertIsInstance(connection.attempted_methods[0][1], spec.Connection.CloseOk)
        self.assertIn("CONNECTION_FORCED", str(connection.remote_closes[0]))


class MechanismSelectionTests(unittest.TestCase):
    """An unusable mechanism list fails loudly rather than sending a bad StartOk."""

    def test_a_mechanism_list_without_plain_or_external_is_rejected(self):
        _connection, channel0 = make_channel0()
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            channel0._select_authentication(b"AMQPLAIN RABBIT-CR-DEMO")
        self.assertIn("Unsupported Security Mechanism(s)", str(caught.exception))
        self.assertIn("AMQPLAIN RABBIT-CR-DEMO", str(caught.exception))

    def test_undecodable_mechanism_bytes_are_still_reported(self):
        _connection, channel0 = make_channel0()
        replacement = "\N{REPLACEMENT CHARACTER}"
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            channel0._select_authentication(b"\xff\xfe")
        self.assertEqual(
            f"Unsupported Security Mechanism(s): {replacement}{replacement}",
            str(caught.exception),
        )


if __name__ == "__main__":
    unittest.main()
