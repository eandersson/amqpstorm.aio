import asyncio
import unittest

from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory
from amqpstorm_aio.tests.unit.test_channel_edges import ChannelTestCase


class DeliverySessionTests(ChannelTestCase):
    async def test_reassigning_the_channel_cannot_retarget_a_delivery(self):
        original, _connection = self.make_channel()
        other, connection = self.make_channel()
        message = self.receive(original, spec.Basic.Deliver)
        message.channel = other
        with self.assertRaisesRegex(exception.AMQPChannelError, "session"):
            await message.ack()
        self.assertEqual([], connection.written)

    async def test_old_deliver_and_get_messages_cannot_settle_a_reused_tag(self):
        for method_type in (spec.Basic.Deliver, spec.Basic.GetOk):
            for operation in ("ack", "nack", "reject"):
                with self.subTest(method=method_type, operation=operation):
                    channel, connection = self.make_channel(auto_respond=True)
                    old = self.receive(channel, method_type)
                    await channel.close()
                    await channel.open()
                    fresh = self.receive(channel, method_type)
                    self.assertEqual(old.delivery_tag, fresh.delivery_tag)
                    before = len(connection.written)
                    with self.assertRaisesRegex(exception.AMQPChannelError, "session"):
                        await getattr(old, operation)()
                    self.assertEqual(before, len(connection.written))
                    await getattr(fresh, operation)()
                    self.assertEqual(before + 1, len(connection.written))

    @staticmethod
    def receive(channel, method_type):
        waiter = channel.register_get_waiter() if method_type is spec.Basic.GetOk else None
        channel.on_frame(factory.build(method_type, delivery_tag=1))
        channel.on_frame(framing.ContentHeader(0, spec.Basic.Properties()))
        return waiter.result() if waiter is not None else channel._inbound.popleft()


class ReturnOwnershipTests(ChannelTestCase):
    async def asyncSetUp(self):
        self.channel, self.connection = self.make_channel(auto_respond=True, rpc_timeout=1)
        await self.channel.confirm_deliveries()
        self.connection.written.clear()

    def publish(self, routing_key="good", **kwargs):
        task = asyncio.create_task(
            self.channel.basic.publish(b"identical body", routing_key, **kwargs)
        )
        self.addAsyncCleanup(self.finish_task, task)
        return task

    @staticmethod
    async def finish_task(task):
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    def confirm(self, tag, *, multiple=False, result=True):
        method = spec.Basic.Ack if result else spec.Basic.Nack
        self.channel.on_frame(factory.build(method, delivery_tag=tag, multiple=multiple))

    def returned(self, routing_key="bad"):
        self.channel.on_frame(factory.build(
            spec.Basic.Return, reply_code=312, reply_text="NO_ROUTE", routing_key=routing_key
        ))
        self.channel.on_frame(framing.ContentHeader(0, spec.Basic.Properties()))

    async def test_only_one_return_capable_publish_is_on_the_wire(self):
        first = self.publish(mandatory=True)
        second = self.publish("bad", mandatory=True)
        await self.spin_until(lambda: self.connection.written)
        await asyncio.sleep(0)
        self.assertEqual(1, len(self.connection.written))
        self.confirm(1)
        await self.spin_until(lambda: len(self.connection.written) == 2)
        self.returned()
        self.confirm(2)
        self.assertIs(True, await first)
        with self.assertRaises(exception.AMQPMessageError):
            await second
        self.channel.check_for_errors()

    async def test_ack_then_return_then_ack_before_publishers_resume(self):
        first = self.publish(mandatory=True)
        second = self.publish("bad", mandatory=True)
        await asyncio.sleep(0)
        self.confirm(1)
        # A pipelined implementation has already written both publishes, allowing
        # Ack(1), Return(2), Ack(2) in the same parser callback. With serialization,
        # the broker must first wait for the second publish to reach the wire.
        if len(self.connection.written) < 2:
            await self.spin_until(lambda: len(self.connection.written) == 2)
        self.returned()
        self.confirm(2)
        self.assertIs(True, await first)
        with self.assertRaises(exception.AMQPMessageError):
            await second

    async def test_cancelling_a_queued_publish_does_not_cancel_the_return_gate(self):
        first = self.publish(mandatory=True)
        second = self.publish(mandatory=True)
        third = self.publish(mandatory=True)
        await asyncio.sleep(0)
        second.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await second
        self.assertFalse(self.channel.return_pending.cancelled())
        self.confirm(1)
        self.assertIs(True, await first)
        await self.spin_until(lambda: len(self.connection.written) == 2)
        self.confirm(2)
        self.assertIs(True, await third)

    async def test_immediate_and_mandatory_share_the_return_slot(self):
        immediate = self.publish("bad", immediate=True)
        mandatory = self.publish(mandatory=True)
        await asyncio.sleep(0)
        self.assertEqual(1, len(self.connection.written))
        self.returned()
        self.confirm(1, result=False)
        with self.assertRaises(exception.AMQPMessageError):
            await immediate
        await self.spin_until(lambda: len(self.connection.written) == 2)
        self.confirm(0, multiple=True)
        self.assertIs(True, await mandatory)

    async def test_a_nack_without_a_return_releases_the_next_publish(self):
        first = self.publish(mandatory=True)
        second = self.publish(mandatory=True)
        await asyncio.sleep(0)
        self.confirm(1, result=False)
        self.assertIs(False, await first)
        await self.spin_until(lambda: len(self.connection.written) == 2)
        self.confirm(2)
        self.assertIs(True, await second)

    async def test_out_of_order_confirms_do_not_change_return_ownership(self):
        mandatory = self.publish("bad", mandatory=True)
        ordinary = self.publish()
        await asyncio.sleep(0)
        self.confirm(2)
        self.returned()
        self.confirm(1)
        self.assertIs(True, await ordinary)
        with self.assertRaises(exception.AMQPMessageError):
            await mandatory

    async def test_failed_write_releases_the_return_slot(self):
        from unittest import mock

        with (
            mock.patch.object(self.connection, "write_bytes", side_effect=RuntimeError("write")),
            self.assertRaises(RuntimeError),
        ):
            await self.channel.basic.publish(b"body", "good", mandatory=True)
        self.assertIsNone(self.channel.return_pending)
        self.assertEqual({}, self.channel._confirm_futures)

    async def test_teardown_releases_both_active_and_waiting_publishers(self):
        for remote in (False, True):
            with self.subTest(remote=remote):
                if self.channel.is_closed:
                    await self.channel.open()
                    await self.channel.confirm_deliveries()
                first = self.publish(mandatory=True)
                second = self.publish(mandatory=True)
                await asyncio.sleep(0)
                if remote:
                    self.channel.on_connection_terminated(exception.AMQPConnectionError("lost"))
                else:
                    await self.channel.close()
                results = await asyncio.gather(first, second, return_exceptions=True)
                self.assertTrue(all(isinstance(result, exception.AMQPError) for result in results))
                self.assertIsNone(self.channel.return_pending)

    async def test_queued_publish_cannot_move_to_a_reopened_session(self):
        first = self.publish(mandatory=True)
        queued = self.publish(mandatory=True)
        await asyncio.sleep(0)
        await self.channel.close()
        await self.channel.open()
        await self.channel.confirm_deliveries()
        before = len(self.connection.written)
        results = await asyncio.gather(first, queued, return_exceptions=True)
        self.assertTrue(all(isinstance(result, exception.AMQPChannelError) for result in results))
        self.assertEqual(before, len(self.connection.written))
        fresh = self.publish(mandatory=True)
        await asyncio.sleep(0)
        self.confirm(1)
        self.assertIs(True, await fresh)

    async def test_publish_resuming_after_a_reopen_cannot_withdraw_a_reused_tag(self):
        settled = self.publish()
        await self.spin_until(lambda: self.connection.written)
        # Ack(1) settles the publish, but the channel is reopened before its task runs
        # again and a new publish takes delivery tag 1 in the new session.
        self.confirm(1)
        await self.channel.close()
        await self.channel.open()
        await self.channel.confirm_deliveries()
        fresh = asyncio.create_task(
            self.channel.basic.publish(b"identical body", "good"), eager_start=True
        )
        self.addAsyncCleanup(self.finish_task, fresh)
        self.assertFalse(settled.done(), "the reopen must finish before the publish resumes")
        self.assertIs(True, await settled)
        self.confirm(1)
        self.assertEqual([True], await asyncio.gather(fresh, return_exceptions=True))

    async def test_return_with_batched_confirm_cannot_fail_an_ordinary_publish(self):
        ordinary = self.publish()
        mandatory = self.publish("bad", mandatory=True)
        await self.spin_until(lambda: len(self.connection.written) == 2)
        self.returned()
        self.confirm(2, multiple=True)
        self.assertIs(True, await ordinary)
        with self.assertRaises(exception.AMQPMessageError):
            await mandatory
        self.channel.check_for_errors()

    async def test_an_unrelated_operation_cannot_claim_an_owned_return(self):
        pending = self.publish("bad", mandatory=True)
        await self.spin_until(lambda: self.connection.written)
        self.returned()
        self.channel.check_for_errors()
        self.confirm(1)
        with self.assertRaises(exception.AMQPMessageError):
            await pending

    async def test_cancelled_publish_keeps_ownership_until_its_late_confirm(self):
        first = self.publish("bad", mandatory=True)
        await self.spin_until(lambda: self.connection.written)
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first
        await self.assert_late_return_does_not_poison_next_publish()

    async def test_timed_out_publish_keeps_ownership_until_its_late_confirm(self):
        self.channel._rpc_timeout = 0.01
        with self.assertRaisesRegex(exception.AMQPChannelError, "took too long"):
            await self.channel.basic.publish(b"body", "bad", mandatory=True)
        self.channel._rpc_timeout = 1
        await self.assert_late_return_does_not_poison_next_publish(multiple=True)

    async def assert_late_return_does_not_poison_next_publish(self, *, multiple=False):
        second = self.publish(mandatory=True)
        await asyncio.sleep(0)
        self.assertEqual(1, len(self.connection.written))
        self.returned()
        self.confirm(0 if multiple else 1, multiple=multiple)
        await self.spin_until(lambda: len(self.connection.written) == 2)
        self.confirm(2)
        self.assertIs(True, await second)
        self.channel.check_for_errors()
        self.assertEqual({}, self.channel._confirm_futures)

    async def test_waiting_for_an_abandoned_publish_is_bounded(self):
        self.channel._rpc_timeout = 0.01
        for _ in range(2):
            with self.assertRaises(exception.AMQPChannelError):
                await self.channel.basic.publish(b"body", "bad", mandatory=True)
        self.assertEqual(1, len(self.connection.written))


if __name__ == "__main__":
    unittest.main()
