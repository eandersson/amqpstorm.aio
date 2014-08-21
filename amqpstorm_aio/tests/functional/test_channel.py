import asyncio
import unittest

from amqpstorm_aio import connection
from amqpstorm_aio import exception
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory
from amqpstorm_aio.tests.functional import fake_broker


class ChannelBrokerTestCase(unittest.IsolatedAsyncioTestCase):
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

    def frames_received(self, channel_id, frame_class):
        return [
            frame
            for received_channel_id, frame in self.session.received
            if received_channel_id == channel_id and isinstance(frame, frame_class)
        ]


class ChannelLifecycleTests(ChannelBrokerTestCase):
    """Open/close lifecycle, reuse, and close-argument validation."""

    async def test_channel_open_close_transitions_states(self):
        channel = await self.connection.channel()
        self.assertTrue(channel.is_open)
        self.assertEqual(1, channel.channel_id)
        self.assertEqual(1, len(self.frames_received(1, spec.Channel.Open)))
        await channel.close()
        self.assertTrue(channel.is_closed)
        self.assertEqual(1, len(self.frames_received(1, spec.Channel.Close)))

    async def test_channel_reused_after_close_reopens_and_serves_rpcs(self):
        channel = await self.connection.channel()
        await channel.close()
        self.assertTrue(channel.is_closed)
        await channel.open()
        self.assertTrue(channel.is_open)
        declared = await channel.queue.declare("reused.q")
        self.assertEqual("reused.q", declared["queue"])
        await channel.close()
        self.assertTrue(channel.is_closed)

    async def test_close_with_non_integer_reply_code_raises_invalid_argument(self):
        channel = await self.connection.channel()
        for bad_reply_code in ("200", None, True):
            with self.subTest(bad_reply_code=bad_reply_code):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    await channel.close(reply_code=bad_reply_code)
        self.assertTrue(channel.is_open, "failed validation must not close the channel")

    async def test_close_with_non_string_reply_text_raises_invalid_argument(self):
        channel = await self.connection.channel()
        with self.assertRaises(exception.AMQPInvalidArgument):
            await channel.close(reply_text=42)
        self.assertTrue(channel.is_open, "failed validation must not close the channel")


class AbandonedChannelOpenBrokerTests(ChannelBrokerTestCase):
    """An abandoned channel() call must not hand its broker-live id to the next one."""

    def suppress_open_ok(self):
        """Make the broker swallow Channel.Open; returns the captured (session, id)s."""
        captured = []

        def swallow(session, channel_id, frame):
            captured.append((session, channel_id))

        self.broker.responders[spec.Channel.Open.index] = swallow
        return captured

    async def test_abandoned_open_reserves_the_id_for_the_next_channel(self):
        captured = self.suppress_open_ok()
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.connection.channel(), 0.1)
        del self.broker.responders[spec.Channel.Open.index]

        second = await self.connection.channel()
        self.assertEqual(
            2, second.channel_id, "the abandoned id must not be reissued while reserved"
        )
        self.assertEqual([1], [channel_id for _, channel_id in captured])
        declared = await second.queue.declare("tombstone.q")
        self.assertEqual("tombstone.q", declared["queue"])
        self.assertTrue(self.connection.is_open)

    async def test_late_open_ok_is_answered_with_close_and_frees_the_id(self):
        captured = self.suppress_open_ok()
        with self.assertRaises(TimeoutError):
            await asyncio.wait_for(self.connection.channel(), 0.1)
        del self.broker.responders[spec.Channel.Open.index]

        session, channel_id = captured[0]
        session.send_method(channel_id, factory.build(spec.Channel.OpenOk))
        await self.wait_until(
            lambda: any(
                isinstance(frame, spec.Channel.Close)
                for _, frame in self.session.received
            )
        )
        # The broker answers the Close; once CloseOk lands the id is free again.
        await self.wait_until(lambda: self.connection.channels[1].is_closed)
        reused = await self.connection.channel()
        self.assertEqual(1, reused.channel_id)
        self.assertTrue(self.connection.is_open)

    async def test_reopening_a_channel_whose_id_was_reissued_raises(self):
        first = await self.connection.channel()
        await first.close()
        second = await self.connection.channel()
        self.assertEqual(first.channel_id, second.channel_id)
        with self.assertRaises(exception.AMQPChannelError):
            await first.open()
        self.assertTrue(second.is_open, "the current owner must be untouched")
        declared = await second.queue.declare("owner.q")
        self.assertEqual("owner.q", declared["queue"])


class ChannelRpcFailureTests(ChannelBrokerTestCase):
    """RPC timeouts and server-initiated channel errors."""

    async def test_silent_broker_rpc_times_out_with_took_too_long(self):
        self.broker.responders[spec.Queue.Declare.index] = lambda session, channel_id, frame: None
        channel = await self.connection.channel(rpc_timeout=1)
        with self.assertRaisesRegex(exception.AMQPChannelError, "took too long"):
            await channel.queue.declare("silent.q")

    async def test_server_channel_close_fails_rpc_and_leaves_connection_usable(self):
        def close_channel(session, channel_id, frame):
            session.send_method(
                channel_id,
                factory.build(
                    spec.Channel.Close,
                    reply_code=406,
                    reply_text="PRECONDITION_FAILED - no dice",
                ),
            )

        self.broker.responders[spec.Queue.Declare.index] = close_channel
        channel = await self.connection.channel()
        with self.assertRaises(exception.AMQPChannelError) as context:
            await channel.queue.declare("doomed.q")
        self.assertEqual(406, context.exception.reply_code)
        self.assertIn("closed by remote server", str(context.exception))
        self.assertTrue(channel.is_closed)
        await self.wait_until(
            lambda: self.frames_received(channel.channel_id, spec.Channel.CloseOk)
        )
        for _ in range(3):
            with self.assertRaises(exception.AMQPChannelError):
                channel.check_for_errors()
        self.assertTrue(self.connection.is_open)
        del self.broker.responders[spec.Queue.Declare.index]
        replacement = await self.connection.channel()
        declared = await replacement.queue.declare("alive.q")
        self.assertEqual("alive.q", declared["queue"])

    async def test_server_channel_flow_is_echoed_with_flow_ok_active(self):
        def flow_then_declare_ok(session, channel_id, frame):
            session.send_method(channel_id, factory.build(spec.Channel.Flow, active=True))
            session.send_method(channel_id, factory.build(spec.Exchange.DeclareOk))

        self.broker.responders[spec.Exchange.Declare.index] = flow_then_declare_ok
        self.broker.responders[spec.Channel.FlowOk.index] = (
            lambda session, channel_id, frame: None
        )
        channel = await self.connection.channel()
        await channel.exchange.declare("flow.x")
        await self.wait_until(
            lambda: self.frames_received(channel.channel_id, spec.Channel.FlowOk)
        )
        flow_ok = self.frames_received(channel.channel_id, spec.Channel.FlowOk)[0]
        self.assertTrue(flow_ok.active)


class ExchangeAndQueueOperationTests(ChannelBrokerTestCase):
    """Exchange and queue management RPCs return their decoded response fields."""

    async def test_exchange_operations_return_empty_dicts(self):
        channel = await self.connection.channel()
        self.assertEqual({}, await channel.exchange.declare("test.x", "topic"))
        self.assertEqual({}, await channel.exchange.bind("test.x", "amq.direct", "rk"))
        self.assertEqual({}, await channel.exchange.unbind("test.x", "amq.direct", "rk"))
        self.assertEqual({}, await channel.exchange.delete("test.x"))

    async def test_queue_operations_return_names_and_counts(self):
        channel = await self.connection.channel()
        declared = await channel.queue.declare("test.q")
        self.assertEqual(
            {"queue": "test.q", "message_count": 0, "consumer_count": 0}, declared
        )
        self.assertEqual({}, await channel.queue.bind("test.q", "amq.direct", "rk"))
        self.assertEqual({}, await channel.queue.unbind("test.q", "amq.direct", "rk"))
        self.assertEqual({"message_count": 0}, await channel.queue.purge("test.q"))
        self.assertEqual({"message_count": 0}, await channel.queue.delete("test.q"))


class TransactionTests(ChannelBrokerTestCase):
    """Tx select/commit/rollback and the transactional context manager."""

    async def test_tx_select_commit_rollback_return_empty_dicts(self):
        channel = await self.connection.channel()
        self.assertEqual({}, await channel.tx.select())
        self.assertEqual({}, await channel.tx.commit())
        self.assertEqual({}, await channel.tx.rollback())

    async def test_tx_context_manager_rolls_back_when_block_raises(self):
        channel = await self.connection.channel()
        with self.assertRaises(RuntimeError):
            async with channel.tx:
                raise RuntimeError("boom")
        self.assertEqual(
            1,
            len(self.frames_received(channel.channel_id, spec.Tx.Rollback)),
            "an exception inside the block must trigger exactly one Tx.Rollback",
        )
        self.assertEqual([], self.frames_received(channel.channel_id, spec.Tx.Commit))


if __name__ == "__main__":
    unittest.main()
