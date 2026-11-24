import asyncio
import unittest
import uuid

from amqpstorm_aio import exception
from amqpstorm_aio.tests.scenario import base


class DeliverySafetyTests(base.ScenarioTestCase):
    async def test_old_messages_cannot_settle_unrelated_messages_after_reopen(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        old_queue = (await channel.queue.declare(durable=False, exclusive=True))["queue"]
        new_queue = (await channel.queue.declare(durable=False, exclusive=True))["queue"]
        for operation in ("ack", "nack", "reject"):
            with self.subTest(operation=operation):
                await channel.basic.publish(b"old", old_queue)
                old = await channel.basic.get(old_queue)
                await channel.close()
                await channel.open()
                await channel.basic.publish(b"unrelated", new_queue)
                fresh = await channel.basic.get(new_queue)
                self.assertEqual(old.delivery_tag, fresh.delivery_tag)
                kwargs = {} if operation == "ack" else {"requeue": False}
                with self.assertRaisesRegex(exception.AMQPChannelError, "session"):
                    await getattr(old, operation)(**kwargs)
                await channel.close()
                await channel.open()
                redelivery = await channel.basic.get(new_queue)
                self.assertIsNotNone(redelivery, "the unrelated delivery must not be lost")
                self.assertEqual(b"unrelated", redelivery.body)
                await redelivery.ack()
                await channel.queue.purge(old_queue)
                await channel.close()
                await channel.open()

    async def test_concurrent_returns_are_reported_to_the_unroutable_publish_only(self):
        connection = await self.open_connection(heartbeat=0)
        channel = await connection.channel()
        queue = (await channel.queue.declare(durable=False, exclusive=True))["queue"]
        missing = f"missing-{uuid.uuid4().hex}"
        await channel.confirm_deliveries()
        # Hold replies until both callers have started. A pipelined implementation
        # receives Ack(good), Return(bad), Ack(bad) together when reads resume.
        connection._io.pause_reading()
        tasks = [
            asyncio.create_task(channel.basic.publish(b"same", queue, mandatory=True)),
            asyncio.create_task(channel.basic.publish(b"same", missing, mandatory=True)),
        ]
        try:
            await asyncio.sleep(0.05)
        finally:
            connection._io.resume_reading()
        async with asyncio.timeout(10):
            results = await asyncio.gather(*tasks, return_exceptions=True)
        self.assertIs(True, results[0])
        self.assertIsInstance(results[1], exception.AMQPMessageError)
        self.assertIn(missing, str(results[1]))
        channel.check_for_errors()
        received = await channel.basic.get(queue)
        self.assertEqual(b"same", received.body)
        await received.ack()


if __name__ == "__main__":
    unittest.main()
