import asyncio
import unittest

from amqpstorm_aio import base
from amqpstorm_aio import channel as channel_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory

LOGGER_NAME = "amqpstorm_aio.channel"
RPC_TIMEOUT = 0.05

METHOD_CLASSES = {
    value.index: value
    for outer in (
        spec.Connection,
        spec.Channel,
        spec.Exchange,
        spec.Queue,
        spec.Basic,
        spec.Confirm,
        spec.Tx,
    )
    for value in vars(outer).values()
    if isinstance(value, type) and issubclass(value, spec.Method)
}


class FakeConnection:
    """Stands in for a Connection: records writes and can be scripted to misbehave.

    ``auto_respond`` answers every synchronous method with its first valid response,
    ``responses_per_write`` repeats that answer, and ``write_error`` / ``write_errors``
    make the wire refuse one method class or every one of them.
    """

    def __init__(self):
        self.written = []
        self.user_closed = False
        self.is_closed = False
        self.max_frame_size = 131_072
        self.channel = None
        self.auto_respond = False
        self.responses_per_write = 1
        self.write_error = None
        self.write_errors = {}
        self.grown = []
        self.drained = []
        self.reclaimed = []

    def check_for_errors(self):
        pass

    def ensure_current_loop(self):
        pass

    def reclaim_channel_id(self, channel):
        self.reclaimed.append(channel.channel_id)

    def write_method_frame(self, channel_id, method):
        error = self.write_errors.get(type(method), self.write_error)
        if error is not None:
            raise error
        # Marshal for real so a field that slipped past validation still fails here.
        framing.append_method_frame(bytearray(), channel_id, method)
        self.written.append((channel_id, method))
        if not self.auto_respond or self.channel is None or not method.valid_responses:
            return
        response_class = METHOD_CLASSES[method.valid_responses[0]]
        for _ in range(self.responses_per_write):
            self.channel.on_frame(factory.build(response_class))

    def write_bytes(self, data):
        self.written.append(("bytes", bytes(data)))

    async def drain(self):
        return None

    def report_inbound_growth(self, channel, size):
        self.grown.append(size)

    def report_inbound_drain(self, channel, size):
        self.drained.append(size)


def deliver(channel, body, consumer_tag="ctag", routing_key="rk"):
    """Feed one complete Basic.Deliver + header + body, as the connection would."""
    channel.on_frame(
        factory.build(spec.Basic.Deliver, consumer_tag=consumer_tag, routing_key=routing_key)
    )
    channel.on_frame(framing.ContentHeader(len(body), spec.Basic.Properties()))
    channel.on_frame(body)


async def read_one_body(channel):
    """Wait for the next inbound message and return its body."""
    async for message in channel:
        return message.body
    return None


async def buffered_messages(channel):
    """Drain what the channel has already buffered; ends at once with no consumers."""
    return [message async for message in channel.build_inbound_messages(break_on_empty=True)]


def written_methods(connection, method_class):
    return [method for _, method in connection.written if isinstance(method, method_class)]


class ChannelTestCase(unittest.IsolatedAsyncioTestCase):
    def make_channel(self, *, state=base.Stateful.OPEN, rpc_timeout=RPC_TIMEOUT, **knobs):
        connection = FakeConnection()
        for name, value in knobs.items():
            setattr(connection, name, value)
        channel = channel_module.Channel(1, connection, rpc_timeout)
        connection.channel = channel
        channel.set_state(state)
        return channel, connection

    async def spin_until(self, predicate, timeout=5.0):
        async with asyncio.timeout(timeout):
            while not predicate():
                await asyncio.sleep(0)


class ContextManagerTests(ChannelTestCase):
    """``async with channel`` closes on the way out, but only an open channel."""

    async def test_leaving_the_block_cleanly_closes_an_open_channel(self):
        channel, connection = self.make_channel(auto_respond=True)
        async with channel as entered:
            self.assertIs(channel, entered)
        self.assertTrue(channel.is_closed)
        self.assertEqual(1, len(written_methods(connection, spec.Channel.Close)))

    async def test_leaving_the_block_cleanly_writes_nothing_when_already_closed(self):
        channel, connection = self.make_channel(state=base.Stateful.CLOSED, auto_respond=True)
        async with channel:
            pass
        self.assertEqual([], connection.written)

    async def test_an_exception_on_an_already_closed_channel_writes_no_close(self):
        channel, connection = self.make_channel(state=base.Stateful.CLOSED)
        channel.add_consumer_tag("ctag")
        with self.assertRaisesRegex(RuntimeError, "boom"):
            async with channel:
                raise RuntimeError("boom")
        self.assertEqual([], connection.written)
        self.assertEqual(
            ["ctag"],
            channel.consumer_tags,
            "close() must not run at all on a channel that is already closed",
        )

    async def test_a_failing_close_does_not_mask_the_exception_from_the_block(self):
        channel, connection = self.make_channel()
        connection.write_error = exception.AMQPConnectionError("connection closed")
        with self.assertRaisesRegex(RuntimeError, "boom"):
            async with channel:
                raise RuntimeError("boom")
        self.assertTrue(channel.is_closed)

    async def test_iterating_the_channel_yields_delivered_messages(self):
        channel, _ = self.make_channel()
        deliver(channel, b"first")
        received = []
        async for message in channel:
            received.append(message.body)
            break
        self.assertEqual([b"first"], received)

    async def test_int_of_a_channel_is_its_channel_id(self):
        channel, _ = self.make_channel()
        self.assertEqual(1, int(channel))


class CloseTests(ChannelTestCase):
    """Close is best effort: it always ends locally closed, whatever the wire does."""

    async def test_a_close_ok_that_never_arrives_still_closes_the_channel(self):
        channel, connection = self.make_channel()
        await channel.close()
        self.assertTrue(channel.is_closed)
        self.assertEqual(1, len(written_methods(connection, spec.Channel.Close)))

    async def test_closing_a_channel_on_a_dead_connection_writes_nothing(self):
        channel, connection = self.make_channel()
        connection.is_closed = True
        await channel.close()
        self.assertEqual([], connection.written)
        self.assertTrue(channel.is_closed)

    async def test_a_connection_error_while_cancelling_forgets_the_consumers(self):
        channel, connection = self.make_channel(auto_respond=True)
        channel.add_consumer_tag("one")
        channel.add_consumer_tag("two")
        channel.register_consumer_callback("one", None)
        connection.write_errors[spec.Basic.Cancel] = exception.AMQPConnectionError("gone")
        await channel.close()
        self.assertEqual([], channel.consumer_tags)
        self.assertTrue(channel.is_closed)
        self.assertEqual(1, len(written_methods(connection, spec.Channel.Close)))

    async def test_stop_consuming_forgets_every_consumer_whose_cancel_fails(self):
        channel, connection = self.make_channel()
        channel.add_consumer_tag("one")
        channel.add_consumer_tag("two")
        await channel.stop_consuming()
        self.assertEqual(
            ["one", "two"],
            [method.consumer_tag for method in written_methods(connection, spec.Basic.Cancel)],
            "a failed cancel must not abort the cancellation of the remaining consumers",
        )
        self.assertEqual([], channel.consumer_tags)

    async def test_stop_consuming_on_a_closed_channel_sends_nothing(self):
        channel, connection = self.make_channel(state=base.Stateful.CLOSED)
        channel.add_consumer_tag("one")
        await channel.stop_consuming()
        self.assertEqual([], connection.written)
        self.assertEqual([], channel.consumer_tags)

    async def test_stop_consuming_on_a_dead_connection_sends_nothing(self):
        channel, connection = self.make_channel()
        connection.is_closed = True
        channel.add_consumer_tag("one")
        await channel.stop_consuming()
        self.assertEqual([], connection.written, "no cancel may be attempted on a dead connection")
        self.assertEqual([], channel.consumer_tags)

    async def test_closing_a_channel_releases_the_bytes_it_had_buffered(self):
        channel, connection = self.make_channel(auto_respond=True)
        deliver(channel, b"hello")
        self.assertEqual([5], connection.grown)
        await channel.close()
        self.assertEqual([5], connection.drained, "dropped messages must leave the backlog")
        self.assertEqual([], await buffered_messages(channel))


class ConfirmSelectTests(ChannelTestCase):
    """Concurrent confirm_deliveries calls share one Confirm.Select."""

    async def start_leader(self, channel):
        leader = asyncio.create_task(channel.confirm_deliveries())
        await self.spin_until(lambda: channel.confirm_select_pending is not None)
        return leader

    async def test_a_concurrent_call_waits_instead_of_selecting_twice(self):
        channel, connection = self.make_channel()
        leader = await self.start_leader(channel)
        follower = asyncio.create_task(channel.confirm_deliveries())
        await asyncio.sleep(0)
        channel.on_frame(factory.build(spec.Confirm.SelectOk))
        await leader
        await follower
        self.assertTrue(channel.confirming_deliveries)
        self.assertEqual(1, len(written_methods(connection, spec.Confirm.Select)))

    async def test_a_cancelled_follower_does_not_break_the_leaders_select(self):
        channel, _ = self.make_channel()
        leader = await self.start_leader(channel)
        pending = channel.confirm_select_pending
        follower = asyncio.create_task(channel.confirm_deliveries())
        await asyncio.sleep(0)
        follower.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await follower
        self.assertTrue(pending.cancelled(), "cancelling the follower cancels the shared future")
        channel.on_frame(factory.build(spec.Confirm.SelectOk))
        await leader
        self.assertTrue(channel.confirming_deliveries)
        self.assertIsNone(channel.confirm_select_pending)

    async def test_an_abandoned_select_still_leaves_the_channel_in_confirm_mode(self):
        channel, _ = self.make_channel()
        with self.assertRaisesRegex(exception.AMQPChannelError, "took too long"):
            await channel.confirm_deliveries()
        self.assertTrue(
            channel.confirming_deliveries,
            "the broker may still have accepted the select, so publishes must be confirmed",
        )
        self.assertTrue(channel.expects_stale_responses)
        self.assertIsNone(channel.confirm_select_pending)


class ErrorReportingTests(ChannelTestCase):
    async def test_check_for_errors_on_a_closed_channel_raises(self):
        channel, _ = self.make_channel(state=base.Stateful.CLOSED)
        with self.assertRaisesRegex(exception.AMQPChannelError, "channel closed"):
            channel.check_for_errors()

    async def test_check_for_errors_on_an_open_channel_without_errors_is_silent(self):
        channel, _ = self.make_channel()
        self.assertIsNone(channel.check_for_errors())


class InboundGeneratorTests(ChannelTestCase):
    """How build_inbound_messages ends: silently for us, loudly for everyone else."""

    async def test_a_close_we_asked_for_ends_the_generator_silently(self):
        channel, _ = self.make_channel(auto_respond=True)
        await channel.close()
        self.assertEqual([], await buffered_messages(channel))

    async def test_a_close_the_connection_owner_asked_for_ends_the_generator_silently(self):
        channel, connection = self.make_channel()
        connection.user_closed = True
        channel.on_connection_terminated(exception.AMQPConnectionError("connection closed"))
        self.assertEqual([], await buffered_messages(channel))

    async def test_connection_loss_discards_buffered_messages_before_raising(self):
        channel, connection = self.make_channel()
        deliver(channel, b"queued")

        connection_error = exception.AMQPConnectionError("socket closed")

        def raise_connection_error():
            raise connection_error

        connection.check_for_errors = raise_connection_error
        channel.on_connection_terminated(connection_error)

        self.assertEqual(
            [6],
            connection.drained,
            "connection loss must release bytes for deliveries the application cannot ack",
        )
        self.assertEqual(0, channel._buffered_inbound_bytes)
        with self.assertRaisesRegex(exception.AMQPConnectionError, "socket closed"):
            await anext(channel.build_inbound_messages())

    async def test_yielding_a_message_releases_its_bytes_from_the_connection_backlog(self):
        channel, connection = self.make_channel()
        deliver(channel, b"hello")
        self.assertEqual([5], connection.grown)
        self.assertEqual([], connection.drained, "a queued message still occupies the backlog")
        messages = await buffered_messages(channel)
        self.assertEqual([b"hello"], [message.body for message in messages])
        self.assertEqual([5], connection.drained, "a consumed message must free its bytes")

    async def test_a_channel_with_no_consumers_ends_the_generator_without_waiting(self):
        channel, _ = self.make_channel()
        async with asyncio.timeout(1):
            messages = [
                message
                async for message in channel.build_inbound_messages(
                    break_on_empty=True, empty_timeout=30
                )
            ]
        self.assertEqual([], messages)

    async def test_an_idle_consumer_ends_the_generator_once_the_empty_timeout_expires(self):
        channel, _ = self.make_channel()
        channel.add_consumer_tag("ctag")
        messages = [
            message
            async for message in channel.build_inbound_messages(
                break_on_empty=True, empty_timeout=0.01
            )
        ]
        self.assertEqual([], messages)
        self.assertTrue(channel.is_open, "an empty queue must not close the channel")

    async def test_a_cancelled_reader_does_not_stop_the_others_from_waking(self):
        channel, _ = self.make_channel()
        cancelled = asyncio.create_task(read_one_body(channel))
        waiting = asyncio.create_task(read_one_body(channel))
        await asyncio.sleep(0)
        cancelled.cancel()
        deliver(channel, b"payload")
        with self.assertRaises(asyncio.CancelledError):
            await cancelled
        async with asyncio.timeout(5):
            self.assertEqual(b"payload", await waiting)

    async def test_a_broker_close_raises_out_of_the_generator(self):
        channel, _ = self.make_channel()
        channel.on_frame(
            factory.build(spec.Channel.Close, reply_code=404, reply_text="NOT_FOUND - gone")
        )
        with self.assertRaisesRegex(exception.AMQPChannelError, "closed by remote server"):
            await buffered_messages(channel)


class StartConsumingTests(ChannelTestCase):
    async def test_start_consuming_without_a_callback_raises(self):
        channel, _ = self.make_channel()
        with self.assertRaisesRegex(exception.AMQPChannelError, "no consumer callback defined"):
            await channel.start_consuming()

    async def test_a_message_for_an_unknown_consumer_is_dropped_and_logged(self):
        channel, _ = self.make_channel()
        channel.add_consumer_tag("known")
        received = []

        async def callback(message):
            received.append(message.body)
            channel.on_frame(factory.build(spec.Basic.Cancel, consumer_tag="known"))

        channel.register_consumer_callback("known", callback)
        deliver(channel, b"stray", consumer_tag="stranger")
        deliver(channel, b"mine", consumer_tag="known")
        with self.assertLogs(LOGGER_NAME, "WARNING") as logs:
            async with asyncio.timeout(5):
                await channel.start_consuming()
        self.assertEqual([b"mine"], received)
        self.assertTrue(any("'stranger'" in line for line in logs.output), logs.output)
        self.assertEqual([], channel.consumer_tags)

    async def test_start_consuming_keeps_waiting_after_an_idle_period(self):
        channel, _ = self.make_channel()
        channel.add_consumer_tag("ctag")
        received = []

        async def callback(message):
            received.append(message.body)
            channel.on_frame(factory.build(spec.Basic.Cancel, consumer_tag="ctag"))

        channel.register_consumer_callback("ctag", callback)
        consuming = asyncio.create_task(channel.start_consuming())
        await asyncio.sleep(channel_module.DEFAULT_EMPTY_TIMEOUT_SECONDS + 0.1)
        self.assertFalse(
            consuming.done(),
            "an idle period must not end start_consuming while a consumer is registered",
        )
        deliver(channel, b"late")
        async with asyncio.timeout(5):
            await consuming
        self.assertEqual([b"late"], received)


class ConsumerTagTests(ChannelTestCase):
    async def test_adding_the_same_consumer_tag_twice_keeps_one_entry(self):
        channel, _ = self.make_channel()
        channel.add_consumer_tag("ctag")
        channel.add_consumer_tag("ctag")
        self.assertEqual(["ctag"], channel.consumer_tags)

    async def test_removing_an_unknown_consumer_tag_leaves_the_others_alone(self):
        channel, _ = self.make_channel()
        channel.add_consumer_tag("ctag")
        channel.remove_consumer_tag("other")
        self.assertEqual(["ctag"], channel.consumer_tags)


class FrameDispatchTests(ChannelTestCase):
    async def test_a_heartbeat_frame_on_a_channel_is_ignored(self):
        channel, connection = self.make_channel()
        with self.assertNoLogs(LOGGER_NAME):
            channel.on_frame(framing.HEARTBEAT)
        self.assertEqual([], connection.written)
        self.assertTrue(channel.is_open)

    async def test_opening_a_channel_reclaims_its_id_from_the_connection(self):
        channel, connection = self.make_channel(state=base.Stateful.CLOSED, auto_respond=True)
        await channel.open()
        self.assertEqual([1], connection.reclaimed, "a reused id must be released before reopening")
        self.assertTrue(channel.is_open)

    async def test_an_unknown_frame_object_is_logged_and_dropped(self):
        channel, connection = self.make_channel()
        with self.assertLogs(LOGGER_NAME, "ERROR") as logs:
            channel.on_frame(object())
        self.assertTrue(any("Unhandled Frame" in line for line in logs.output), logs.output)
        self.assertEqual([], connection.written)

    async def test_an_unexpected_method_is_logged_and_dropped(self):
        channel, connection = self.make_channel()
        with self.assertLogs(LOGGER_NAME, "ERROR") as logs:
            channel.on_frame(factory.build(spec.Queue.DeclareOk, queue="q"))
        self.assertTrue(any("Unhandled Frame" in line for line in logs.output), logs.output)
        self.assertEqual([], connection.written)

    async def test_a_second_content_method_replaces_the_one_still_awaiting_its_body(self):
        channel, _ = self.make_channel()
        channel.on_frame(factory.build(spec.Basic.Deliver, routing_key="first"))
        with self.assertLogs(LOGGER_NAME, "WARNING") as logs:
            channel.on_frame(factory.build(spec.Basic.Deliver, routing_key="second"))
        self.assertTrue(any("out-of-order" in line for line in logs.output), logs.output)
        channel.on_frame(framing.ContentHeader(4, spec.Basic.Properties()))
        channel.on_frame(b"body")
        messages = await buffered_messages(channel)
        self.assertEqual(["second"], [message.routing_key for message in messages])

    async def test_a_duplicated_rpc_response_is_ignored(self):
        channel, connection = self.make_channel(
            state=base.Stateful.CLOSED, auto_respond=True, responses_per_write=2
        )
        await channel.open()
        self.assertTrue(channel.is_open)
        self.assertEqual(1, len(written_methods(connection, spec.Channel.Open)))


class OutOfOrderContentTests(ChannelTestCase):
    async def test_a_content_header_without_a_method_is_dropped(self):
        channel, _ = self.make_channel()
        with self.assertLogs(LOGGER_NAME, "WARNING") as logs:
            channel.on_frame(framing.ContentHeader(5, spec.Basic.Properties()))
        self.assertTrue(any("out-of-order" in line for line in logs.output), logs.output)
        self.assertEqual([], await buffered_messages(channel))

    async def test_a_body_frame_without_a_header_is_dropped_and_not_accounted(self):
        channel, connection = self.make_channel()
        with self.assertLogs(LOGGER_NAME, "WARNING") as logs:
            channel.on_frame(b"orphan")
        self.assertTrue(any("out-of-order body frame" in line for line in logs.output), logs.output)
        self.assertEqual([], connection.grown, "dropped bytes must not enter the backlog")
        self.assertEqual([], await buffered_messages(channel))

    async def test_a_delivery_with_an_empty_body_needs_no_body_frame(self):
        channel, connection = self.make_channel()
        channel.on_frame(factory.build(spec.Basic.Deliver, routing_key="rk"))
        channel.on_frame(framing.ContentHeader(0, spec.Basic.Properties()))
        messages = await buffered_messages(channel)
        self.assertEqual([b""], [message.body for message in messages])
        self.assertEqual([], connection.grown, "an empty body adds nothing to the backlog")

    async def test_content_for_an_unexpected_method_is_dropped_and_its_bytes_released(self):
        channel, connection = self.make_channel()
        channel.on_frame(factory.build(spec.Basic.Publish, routing_key="rk"))
        channel.on_frame(framing.ContentHeader(5, spec.Basic.Properties()))
        channel.on_frame(b"hello")
        self.assertEqual([], await buffered_messages(channel))
        self.assertEqual([5], connection.grown)
        self.assertEqual([5], connection.drained, "buffered bytes must be released again")

    async def test_completing_content_with_no_method_or_header_is_a_no_op(self):
        # No frame sequence reaches this guard: a header is only accepted after a
        # method, and both are always cleared together. Called directly so the
        # invariant still holds if a future caller breaks that pairing.
        channel, connection = self.make_channel()
        channel._content_body = [b"orphaned"]
        channel._content_received = 8
        channel._complete_content()
        self.assertEqual([], await buffered_messages(channel))
        self.assertEqual([], connection.drained, "nothing was accounted, nothing to release")
        self.assertEqual([], channel._content_body, "partial body must not be left behind")
        self.assertEqual(0, channel._content_received)


class BasicGetFrameTests(ChannelTestCase):
    async def test_a_get_empty_without_a_pending_get_is_ignored(self):
        channel, _ = self.make_channel()
        channel.on_frame(factory.build(spec.Basic.GetEmpty))
        waiter = channel.register_get_waiter()
        channel.on_frame(factory.build(spec.Basic.GetEmpty))
        self.assertIsNone(await waiter)

    async def test_a_stale_get_empty_is_discarded_before_the_next_get_sees_it(self):
        channel, _ = self.make_channel()
        channel.mark_stale_get(True)
        waiter = channel.register_get_waiter()
        with self.assertLogs(LOGGER_NAME, "WARNING") as logs:
            channel.on_frame(factory.build(spec.Basic.GetEmpty))
        self.assertTrue(any("late Basic.GetEmpty" in line for line in logs.output), logs.output)
        self.assertFalse(waiter.done(), "a stale reply must not answer the fresh get")
        channel.on_frame(factory.build(spec.Basic.GetEmpty))
        self.assertIsNone(await waiter)

    async def test_a_stale_get_ok_for_an_acknowledged_get_is_requeued(self):
        channel, connection = self.make_channel()
        channel.mark_stale_get(False)
        with self.assertLogs(LOGGER_NAME, "WARNING"):
            channel.on_frame(factory.build(spec.Basic.GetOk, delivery_tag=7, routing_key="rk"))
            channel.on_frame(framing.ContentHeader(5, spec.Basic.Properties()))
            channel.on_frame(b"hello")
        self.assertEqual(
            [(7, True)],
            [
                (method.delivery_tag, method.requeue)
                for method in written_methods(connection, spec.Basic.Reject)
            ],
        )

    async def test_a_stale_get_ok_for_a_no_ack_get_is_dropped_without_a_reject(self):
        channel, connection = self.make_channel()
        channel.mark_stale_get(True)
        with self.assertLogs(LOGGER_NAME, "WARNING"):
            channel.on_frame(factory.build(spec.Basic.GetOk, delivery_tag=7, routing_key="rk"))
            channel.on_frame(framing.ContentHeader(5, spec.Basic.Properties()))
            channel.on_frame(b"hello")
        self.assertEqual([], connection.written, "a no-ack get leaves nothing to reject")

    async def test_a_get_ok_with_an_empty_body_resolves_the_pending_get(self):
        channel, connection = self.make_channel()
        waiter = channel.register_get_waiter()
        channel.on_frame(factory.build(spec.Basic.GetOk, delivery_tag=7, routing_key="rk"))
        channel.on_frame(framing.ContentHeader(0, spec.Basic.Properties()))
        message = await waiter
        self.assertEqual(b"", message.body)
        self.assertEqual(7, message.delivery_tag)
        self.assertEqual([], connection.drained, "an empty body never entered the backlog")

    async def test_a_get_ok_without_a_pending_get_is_dropped_and_its_bytes_released(self):
        channel, connection = self.make_channel()
        channel.on_frame(factory.build(spec.Basic.GetOk, delivery_tag=7, routing_key="rk"))
        channel.on_frame(framing.ContentHeader(5, spec.Basic.Properties()))
        channel.on_frame(b"hello")
        self.assertEqual([], await buffered_messages(channel))
        self.assertEqual([5], connection.drained)

    async def test_a_stale_get_ok_whose_reject_cannot_be_written_is_still_consumed(self):
        channel, connection = self.make_channel()
        channel.mark_stale_get(False)
        connection.write_error = exception.AMQPConnectionError("connection closed")
        with self.assertLogs(LOGGER_NAME, "WARNING") as logs:
            channel.on_frame(factory.build(spec.Basic.GetOk, delivery_tag=7, routing_key="rk"))
            channel.on_frame(framing.ContentHeader(5, spec.Basic.Properties()))
            channel.on_frame(b"hello")
        self.assertTrue(any("late Basic.GetOk" in line for line in logs.output), logs.output)
        self.assertEqual([], connection.written, "the reject never reached the wire")
        connection.write_error = None
        waiter = channel.register_get_waiter()
        channel.on_frame(factory.build(spec.Basic.GetEmpty))
        self.assertIsNone(await waiter, "the stale slot must have been consumed")


class AbandonedOpenTests(ChannelTestCase):
    async def abandon_open(self, channel):
        with self.assertRaisesRegex(exception.AMQPChannelError, "took too long"):
            await channel.open()
        self.assertTrue(channel.expects_stale_responses)
        # What Connection.channel() does with a channel whose open was abandoned.
        channel.set_state(base.Stateful.CLOSING)

    async def test_a_late_open_ok_whose_close_cannot_be_written_closes_the_channel(self):
        channel, connection = self.make_channel(state=base.Stateful.CLOSED)
        await self.abandon_open(channel)
        connection.write_error = exception.AMQPConnectionError("connection closed")
        channel.on_frame(factory.build(spec.Channel.OpenOk))
        self.assertTrue(channel.is_closed)
        self.assertFalse(
            channel.expects_stale_responses, "no CloseOk can arrive for a close never sent"
        )


class FlowControlTests(ChannelTestCase):
    async def test_a_repeated_flow_stop_keeps_the_gate_that_publishers_already_wait_on(self):
        channel, connection = self.make_channel()
        channel.on_frame(factory.build(spec.Channel.Flow, active=False))
        gate = channel.flow_gate
        self.assertIsNotNone(gate)
        channel.on_frame(factory.build(spec.Channel.Flow, active=False))
        self.assertIs(gate, channel.flow_gate)
        self.assertEqual(
            [False, False],
            [method.active for method in written_methods(connection, spec.Channel.FlowOk)],
        )
        channel.on_frame(factory.build(spec.Channel.Flow, active=True))
        self.assertIsNone(channel.flow_gate)
        self.assertIsNone(await gate)


class RemoteCloseTests(ChannelTestCase):
    async def test_a_remote_close_on_a_dead_connection_writes_no_close_ok(self):
        channel, connection = self.make_channel()
        connection.is_closed = True
        channel.on_frame(
            factory.build(spec.Channel.Close, reply_code=404, reply_text="NOT_FOUND - gone")
        )
        self.assertEqual([], connection.written)
        self.assertTrue(channel.is_closed)
        with self.assertRaisesRegex(exception.AMQPChannelError, "NOT_FOUND - gone"):
            channel.check_for_errors()

    async def test_a_remote_close_whose_close_ok_cannot_be_written_still_closes(self):
        channel, connection = self.make_channel()
        connection.write_error = exception.AMQPConnectionError("connection closed")
        channel.on_frame(
            factory.build(spec.Channel.Close, reply_code=404, reply_text="NOT_FOUND - gone")
        )
        self.assertEqual([], connection.written)
        self.assertTrue(channel.is_closed)
        with self.assertRaisesRegex(exception.AMQPChannelError, "NOT_FOUND - gone"):
            channel.check_for_errors()


class ReturnedMessageTests(ChannelTestCase):
    def record_return(self, channel):
        channel.on_frame(
            factory.build(
                spec.Basic.Return, reply_code=312, reply_text="NO_ROUTE", routing_key="rk"
            )
        )
        channel.on_frame(framing.ContentHeader(2, spec.Basic.Properties()))
        channel.on_frame(b"hi")

    async def test_pop_returned_error_returns_none_when_nothing_was_returned(self):
        channel, _ = self.make_channel()
        self.assertIsNone(channel.pop_returned_error())

    async def test_a_recorded_error_is_raised_once_while_the_channel_is_open(self):
        channel, _ = self.make_channel()
        self.record_return(channel)
        with self.assertRaisesRegex(exception.AMQPMessageError, "NO_ROUTE"):
            channel.check_for_exceptions()
        self.assertIsNone(
            channel.check_for_exceptions(), "an open channel drains one recorded error per call"
        )

    async def test_a_recorded_error_repeats_forever_once_the_channel_is_closed(self):
        channel, _ = self.make_channel()
        self.record_return(channel)
        channel.set_state(base.Stateful.CLOSED)
        with self.assertRaises(exception.AMQPMessageError) as first:
            channel.check_for_exceptions()
        with self.assertRaises(exception.AMQPMessageError) as second:
            channel.check_for_exceptions()
        self.assertIs(first.exception, second.exception)

    async def test_claiming_a_returned_error_first_stops_it_from_being_raised(self):
        channel, _ = self.make_channel()
        self.record_return(channel)
        claimed = channel.pop_returned_error()
        self.assertIsInstance(claimed, exception.AMQPMessageError)
        self.assertIsNone(
            channel.check_for_exceptions(),
            "an error already handed to the publisher must not be raised again",
        )

    async def test_a_returned_error_already_raised_can_still_be_claimed_once(self):
        channel, _ = self.make_channel()
        self.record_return(channel)
        self.assertEqual(1, channel.returns_recorded)
        with self.assertRaises(exception.AMQPMessageError) as context:
            channel.check_for_exceptions()
        self.assertIs(context.exception, channel.pop_returned_error())
        self.assertIsNone(channel.pop_returned_error())


class PublisherConfirmTests(ChannelTestCase):
    async def test_discarding_an_already_confirmed_delivery_tag_is_a_no_op(self):
        channel, _ = self.make_channel()
        waiter = channel.register_confirm(1)
        channel.on_frame(factory.build(spec.Basic.Ack, delivery_tag=1, multiple=False))
        self.assertTrue(await waiter)
        channel.discard_confirm(1)
        self.assertTrue(waiter.result())

    async def test_a_multiple_ack_resolves_the_later_tags_past_an_abandoned_one(self):
        channel, _ = self.make_channel()
        first = channel.register_confirm(1)
        second = channel.register_confirm(2)
        first.cancel()
        channel.on_frame(factory.build(spec.Basic.Ack, delivery_tag=2, multiple=True))
        self.assertTrue(await second)
        self.assertTrue(first.cancelled())

    async def test_a_single_ack_for_an_abandoned_delivery_tag_is_ignored(self):
        channel, _ = self.make_channel()
        abandoned = channel.register_confirm(1)
        later = channel.register_confirm(2)
        abandoned.cancel()
        channel.on_frame(factory.build(spec.Basic.Ack, delivery_tag=1, multiple=False))
        channel.on_frame(factory.build(spec.Basic.Ack, delivery_tag=2, multiple=False))
        self.assertTrue(await later, "an abandoned tag must not derail the confirms after it")
        self.assertTrue(abandoned.cancelled())

    async def test_a_nack_resolves_the_publisher_confirm_as_unconfirmed(self):
        channel, _ = self.make_channel()
        waiter = channel.register_confirm(1)
        channel.on_frame(factory.build(spec.Basic.Nack, delivery_tag=1, multiple=False))
        self.assertFalse(await waiter)

    async def test_a_multiple_ack_leaves_the_tags_above_it_pending(self):
        channel, _ = self.make_channel()
        confirmed = channel.register_confirm(1)
        boundary = channel.register_confirm(2)
        later = channel.register_confirm(3)
        channel.on_frame(factory.build(spec.Basic.Ack, delivery_tag=2, multiple=True))
        self.assertTrue(await confirmed)
        self.assertTrue(await boundary)
        self.assertFalse(later.done(), "a tag above the ack must stay unconfirmed")
        later.cancel()

    async def test_a_dead_connection_fails_the_confirms_past_an_abandoned_one(self):
        channel, _ = self.make_channel()
        first = channel.register_confirm(1)
        second = channel.register_confirm(2)
        first.cancel()
        channel.on_connection_terminated(exception.AMQPConnectionError("socket closed"))
        with self.assertRaisesRegex(exception.AMQPConnectionError, "socket closed"):
            await second
        self.assertTrue(first.cancelled())
        self.assertTrue(channel.is_closed)


if __name__ == "__main__":
    unittest.main()
