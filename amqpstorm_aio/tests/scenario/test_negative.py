import unittest
import uuid

from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio.tests import scenario
from amqpstorm_aio.tests.scenario import base


class ChannelErrorTests(base.ScenarioTestCase):
    """Soft errors: the channel dies, the connection carries on."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.connection = await self.open_connection()

    async def assert_channel_error(self, coroutine, reply_code: int, documentation: str):
        """Run an operation expected to be refused, and check the reported reason."""
        with self.assertRaises(exception.AMQPChannelError) as context:
            await coroutine
        error = context.exception
        self.assertEqual(reply_code, error.reply_code)
        self.assertEqual(documentation, error.documentation)
        self.assertTrue(
            self.connection.is_open, "a soft error must not take the connection down"
        )
        return error

    async def test_redeclaring_a_queue_with_different_durability_is_refused(self):
        channel = await self.connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)

        conflicting = await self.connection.channel()
        await self.assert_channel_error(
            conflicting.queue.declare(self.queue_name, durable=False), 406, "PRECONDITION-FAILED"
        )
        self.assertTrue(conflicting.is_closed)

    async def test_binding_to_a_missing_exchange_is_refused(self):
        channel = await self.connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        await self.assert_channel_error(
            channel.queue.bind(self.queue_name, "no-such-exchange", "rk"), 404, "NOT-FOUND"
        )

    async def test_publishing_to_a_missing_exchange_is_refused(self):
        channel = await self.connection.channel()
        await channel.confirm_deliveries()
        await self.assert_channel_error(
            channel.basic.publish(b"body", "rk", exchange="no-such-exchange"), 404, "NOT-FOUND"
        )

    async def test_consuming_from_a_missing_queue_is_refused(self):
        channel = await self.connection.channel()
        await self.assert_channel_error(
            channel.basic.consume(queue=f"missing-{uuid.uuid4().hex}"), 404, "NOT-FOUND"
        )

    async def test_passive_declare_of_a_missing_exchange_is_refused(self):
        channel = await self.connection.channel()
        await self.assert_channel_error(
            channel.exchange.declare("no-such-exchange", passive=True), 404, "NOT-FOUND"
        )

    async def test_declaring_a_reserved_amq_exchange_is_refused(self):
        channel = await self.connection.channel()
        await self.assert_channel_error(
            channel.exchange.declare("amq.reserved-by-broker", durable=True), 403, "ACCESS-REFUSED"
        )

    async def test_declaring_an_exchange_with_an_unknown_type_is_refused(self):
        channel = await self.connection.channel()
        await self.assert_channel_error(
            channel.exchange.declare(self.queue_name, exchange_type="not-a-real-type"),
            406,
            "PRECONDITION-FAILED",
        )

    async def test_deleting_a_non_empty_queue_with_if_empty_is_refused(self):
        channel = await self.connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        await channel.confirm_deliveries()
        await channel.basic.publish(b"body", self.queue_name)
        await self.assert_channel_error(
            channel.queue.delete(self.queue_name, if_empty=True), 406, "PRECONDITION-FAILED"
        )

    async def test_deleting_an_in_use_queue_with_if_unused_is_refused(self):
        channel = await self.connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        await channel.basic.consume(queue=self.queue_name)
        await self.assert_channel_error(
            channel.queue.delete(self.queue_name, if_unused=True), 406, "PRECONDITION-FAILED"
        )

    async def test_acknowledging_an_unknown_delivery_tag_is_refused(self):
        channel = await self.connection.channel()
        await channel.basic.ack(delivery_tag=99_999)
        await self.assert_channel_error(
            channel.queue.declare(self.queue_name, durable=True), 406, "PRECONDITION-FAILED"
        )

    async def test_connection_stays_usable_after_a_channel_error(self):
        channel = await self.connection.channel()
        with self.assertRaises(exception.AMQPChannelError):
            await channel.queue.declare(f"missing-{uuid.uuid4().hex}", passive=True)
        self.assertTrue(channel.is_closed)

        recovered = await self.connection.channel()
        declared = await recovered.queue.declare(self.queue_name, durable=True)
        self.assertEqual(self.queue_name, declared["queue"])


class ExclusiveQueueTests(base.ScenarioTestCase):
    """An exclusive queue belongs to the connection that declared it."""

    async def test_consuming_an_exclusive_queue_from_another_connection_is_refused(self):
        owner = await self.open_connection()
        owner_channel = await owner.channel()
        await owner_channel.queue.declare(self.queue_name, exclusive=True)

        intruder = await self.open_connection()
        intruder_channel = await intruder.channel()
        with self.assertRaises(exception.AMQPChannelError) as context:
            await intruder_channel.basic.consume(queue=self.queue_name)
        self.assertEqual(405, context.exception.reply_code)
        self.assertEqual("RESOURCE-LOCKED", context.exception.documentation)
        self.assertTrue(owner.is_open, "the owning connection is unaffected")


class ConnectionErrorTests(base.ScenarioTestCase):
    """Hard errors: the broker refuses the connection outright."""

    async def test_bad_credentials_are_refused(self):
        connection = connection_module.Connection(
            scenario.HOST, "no-such-user", "wrong-password", port=scenario.PORT, timeout=5.0
        )
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        self.assertEqual(403, context.exception.reply_code)
        self.assertEqual("ACCESS-REFUSED", context.exception.documentation)
        self.assertTrue(connection.is_closed)

    async def test_missing_virtual_host_is_refused(self):
        connection = self.make_connection(virtual_host=f"/missing-{uuid.uuid4().hex}")
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        self.assertEqual(530, context.exception.reply_code)
        self.assertEqual("NOT-ALLOWED", context.exception.documentation)
        self.assertTrue(connection.is_closed)

    async def test_a_refused_connection_can_be_retried_with_good_settings(self):
        connection = self.make_connection(virtual_host=f"/missing-{uuid.uuid4().hex}")
        with self.assertRaises(exception.AMQPConnectionError):
            await connection.open()
        connection.parameters["virtual_host"] = scenario.VIRTUAL_HOST
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)
        self.assertTrue(connection.is_open)


class ClientSideValidationTests(base.ScenarioTestCase):
    """Arguments the broker would reject never leave the client in the first place."""

    async def test_oversized_queue_name_never_reaches_the_broker(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        with self.assertRaises(exception.AMQPInvalidArgument):
            await channel.queue.declare("x" * 128)
        self.assertTrue(channel.is_open, "the channel is untouched by a client-side refusal")
        declared = await channel.queue.declare(self.queue_name, durable=True)
        self.assertEqual(self.queue_name, declared["queue"])

    async def test_queue_name_the_spec_forbids_never_reaches_the_broker(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        with self.assertRaises(exception.AMQPInvalidArgument):
            await channel.queue.declare("queue with spaces")
        self.assertTrue(channel.is_open)

    async def test_oversized_routing_key_never_reaches_the_broker(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        with self.assertRaises(exception.AMQPInvalidArgument):
            await channel.basic.publish(b"body", "x" * 256)
        self.assertTrue(channel.is_open)
        self.assertIsNone(await channel.basic.publish(b"body", self.queue_name))

    async def test_maximum_length_names_are_accepted_by_the_broker(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        longest_name = ("q" + uuid.uuid4().hex).ljust(127, "x")[:127]
        self.addAsyncCleanup(self.delete_queue, longest_name)
        declared = await channel.queue.declare(longest_name, durable=True)
        self.assertEqual(longest_name, declared["queue"])
        self.assertTrue(channel.is_open)

    async def test_broker_generated_queue_name_passes_client_validation(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        generated = (await channel.queue.declare("", exclusive=True))["queue"]
        self.assertTrue(generated)
        consumer_tag = await channel.basic.consume(queue=generated)
        self.assertIn(consumer_tag, channel.consumer_tags)


if __name__ == "__main__":
    unittest.main()
