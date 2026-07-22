import asyncio
import unittest

from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio import spec
from amqpstorm_aio.tests.functional import fake_broker


class TeardownTestCase(unittest.IsolatedAsyncioTestCase):
    async def start_broker(self, **broker_kwargs):
        broker = fake_broker.FakeBroker(heartbeat=0, **broker_kwargs)
        await broker.start()
        self.addAsyncCleanup(self.shutdown_broker, broker)
        return broker

    @staticmethod
    async def shutdown_broker(broker):
        for session in broker.sessions:
            session.close_transport()
        async with asyncio.timeout(5):
            await broker.stop()

    @staticmethod
    def make_connection(broker, timeout=5.0):
        return connection_module.Connection(
            "127.0.0.1", "guest", "guest", port=broker.port, heartbeat=0, timeout=timeout
        )


class CloseDuringOpenTests(TeardownTestCase):
    """close() while open() is mid-handshake must fail the open promptly."""

    @staticmethod
    async def _stop_server(server):
        server.close()
        await server.wait_closed()

    async def start_silent_server(self):
        """A listener that accepts and then never speaks AMQP."""

        async def stay_silent(reader, writer):
            try:
                await reader.read()
            finally:
                writer.close()

        server = await asyncio.start_server(stay_silent, "127.0.0.1", 0)
        self.addAsyncCleanup(self._stop_server, server)
        return server.sockets[0].getsockname()[1]

    async def test_close_while_the_broker_stays_silent_fails_the_open_quickly(self):
        port = await self.start_silent_server()
        connection = connection_module.Connection(
            "127.0.0.1", "guest", "guest", port=port, heartbeat=0, timeout=30.0
        )
        open_task = asyncio.ensure_future(connection.open())
        await asyncio.sleep(0.2)
        self.assertFalse(open_task.done(), "the handshake must still be waiting")
        async with asyncio.timeout(5):
            await connection.close()
            with self.assertRaises(exception.AMQPConnectionError):
                await open_task
        self.assertTrue(connection.is_closed)

    async def test_connection_reopens_cleanly_after_a_close_during_open(self):
        port = await self.start_silent_server()
        broker = await self.start_broker()
        connection = connection_module.Connection(
            "127.0.0.1", "guest", "guest", port=port, heartbeat=0, timeout=30.0
        )
        open_task = asyncio.ensure_future(connection.open())
        await asyncio.sleep(0.2)
        await connection.close()
        with self.assertRaises(exception.AMQPConnectionError):
            await open_task

        connection.parameters["port"] = broker.port
        async with asyncio.timeout(5):
            await connection.open()
        self.addAsyncCleanup(connection.close)
        self.assertTrue(connection.is_open)
        channel = await connection.channel()
        self.assertTrue(channel.is_open)


class CloseStallTests(TeardownTestCase):
    """A transport dying mid-close must not stall close() until the rpc timeout."""

    async def test_cancelling_during_channel_close_still_closes_the_connection(self):
        broker = await self.start_broker()

        def withhold_channel_close_ok(session, channel_id, frame):
            return None

        broker.responders[spec.Channel.Close.index] = withhold_channel_close_ok
        connection = self.make_connection(broker, timeout=30.0)
        await connection.open()
        await connection.channel(rpc_timeout=600)

        closing = asyncio.create_task(connection.close())
        try:
            await asyncio.sleep(0.1)
            self.assertFalse(closing.done(), "the channel close must still be awaiting CloseOk")
            closing.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await closing
            self.assertTrue(connection.is_closed)
            self.assertIsNone(connection._io.transport)
        finally:
            connection._terminate(None)

    async def test_socket_death_during_close_returns_promptly(self):
        broker = await self.start_broker()

        def drop_socket_instead_of_close_ok(session, channel_id, frame):
            session.close_transport()

        broker.responders[spec.Connection.Close.index] = drop_socket_instead_of_close_ok
        connection = self.make_connection(broker, timeout=30.0)
        await connection.open()
        async with asyncio.timeout(5):
            await connection.close()
        self.assertTrue(connection.is_closed)

    async def test_socket_death_during_channel_close_returns_promptly(self):
        broker = await self.start_broker()

        def drop_socket_instead_of_channel_close_ok(session, channel_id, frame):
            session.close_transport()

        broker.responders[spec.Channel.Close.index] = drop_socket_instead_of_channel_close_ok
        connection = self.make_connection(broker, timeout=30.0)
        await connection.open()
        await connection.channel(rpc_timeout=600)
        async with asyncio.timeout(5):
            await connection.close()
        self.assertTrue(connection.is_closed)


class OpenAbortTests(TeardownTestCase):
    """Cancelled or crashed open() must tear down and stay retryable."""

    async def test_cancelled_open_leaves_connection_closed_and_retryable(self):
        held_writers = []
        silent_server = await asyncio.start_server(
            lambda reader, writer: held_writers.append(writer), "127.0.0.1", 0
        )
        self.addAsyncCleanup(self.close_server, silent_server, held_writers)
        port = silent_server.sockets[0].getsockname()[1]
        connection = connection_module.Connection(
            "127.0.0.1", "guest", "guest", port=port, heartbeat=0, timeout=30.0
        )
        open_task = asyncio.ensure_future(connection.open())
        await asyncio.sleep(0.1)
        open_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await open_task
        self.assertTrue(connection.is_closed, "cancelled open must not wedge in OPENING")
        connection.parameters["timeout"] = 0.2
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        self.assertNotIn("already open", str(context.exception))

    @staticmethod
    async def close_server(server, held_writers):
        for writer in held_writers:
            writer.close()
        server.close()
        await server.wait_closed()


class ChannelLeakTests(TeardownTestCase):
    """An abandoned channel open reserves its id until the broker's side is resolved.

    Reissuing the id while the broker may still complete the first open would make
    the second Channel.Open a protocol error that kills the whole connection.
    """

    async def test_timed_out_channel_open_reserves_then_frees_the_id(self):
        broker = await self.start_broker()
        silent = {"active": True}
        swallowed = []

        def silence_channel_open(session, channel_id, frame):
            if silent["active"]:
                swallowed.append((session, channel_id))
                return
            session.send_method(channel_id, spec.Channel.OpenOk())

        broker.responders[spec.Channel.Open.index] = silence_channel_open
        connection = self.make_connection(broker)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        with self.assertRaises(exception.AMQPChannelError):
            await connection.channel(rpc_timeout=1)
        self.assertIn(1, connection.channels, "the broker may still open the id; keep it")

        silent["active"] = False
        second = await connection.channel(rpc_timeout=5)
        self.assertEqual(2, second.channel_id, "a reserved id must not be reissued")

        session, channel_id = swallowed[0]
        session.send_method(channel_id, spec.Channel.OpenOk())
        async with asyncio.timeout(5):
            while not connection.channels[1].is_closed:
                await asyncio.sleep(0.01)
        third = await connection.channel(rpc_timeout=5)
        self.assertEqual(1, third.channel_id, "after the late open is closed, the id is free")


class DroppedConnectionReapTests(TeardownTestCase):
    """A dropped, never-closed Connection is reaped instead of living forever."""

    async def test_collected_connection_warns_at_collection(self):
        """The warning must not wait on inbound traffic; a silent broker sends none."""
        import gc
        import weakref

        broker = await self.start_broker()
        connection = connection_module.Connection(
            "127.0.0.1", "guest", "guest", port=broker.port, heartbeat=0, timeout=5.0
        )
        await connection.open()
        finalizer = weakref.ref(connection)

        with self.assertWarns(ResourceWarning):
            del connection
            gc.collect()
        self.assertIsNone(finalizer(), "nothing may pin a dropped connection")

    async def test_a_properly_closed_connection_releases_its_transport(self):
        """Silence at collection follows from this, and is asserted in test_io.

        Checked directly rather than through gc.collect(): a suite-wide collection
        also reaps connections other tests abandoned, whose warnings are ours too and
        indistinguishable from this one's.
        """
        broker = await self.start_broker()
        connection = connection_module.Connection(
            "127.0.0.1", "guest", "guest", port=broker.port, heartbeat=0, timeout=5.0
        )
        await connection.open()
        self.assertIsNotNone(connection._io.transport)
        await connection.close()
        self.assertIsNone(connection._io.transport, "a closed connection holds no transport")


class FrameMaxGuardTests(TeardownTestCase):
    """A frame_max below the spec minimum is rejected during the handshake."""

    async def test_tiny_frame_max_fails_the_handshake(self):
        broker = await self.start_broker(frame_max=1024)
        connection = self.make_connection(broker)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        self.assertIn("frame_max", str(context.exception))
        self.assertTrue(connection.is_closed)


if __name__ == "__main__":
    unittest.main()
