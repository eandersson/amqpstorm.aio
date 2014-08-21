import unittest

from amqpstorm_aio import exception
from amqpstorm_aio.tests import scenario
from amqpstorm_aio.tests.scenario import base

REQUIRED_CAPABILITIES = (
    "basic.nack",
    "connection.blocked",
    "consumer_cancel_notify",
    "publisher_confirms",
)


class BrokerIdentityTests(base.ScenarioTestCase):
    async def test_the_broker_that_answered_is_the_one_under_test(self):
        if not scenario.BROKER:
            self.skipTest("AMQP_BROKER is unset, so any broker is accepted")
        connection = await self.open_connection()
        self.assertEqual(scenario.BROKER, connection.server_properties.get("product"))

    async def test_the_broker_advertises_every_capability_the_client_uses(self):
        connection = await self.open_connection()
        capabilities = connection.server_properties.get("capabilities", {})
        for capability in REQUIRED_CAPABILITIES:
            with self.subTest(capability=capability):
                self.assertTrue(capabilities.get(capability))

    async def test_the_broker_names_itself_and_its_version(self):
        connection = await self.open_connection()
        properties = connection.server_properties
        self.assertTrue(properties.get("product"))
        self.assertTrue(properties.get("version"))


class PublishRoundTripTests(base.ScenarioTestCase):
    """A confirmed publish survives the round trip with body and properties intact."""

    async def test_a_confirmed_message_comes_back_unchanged(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        await channel.confirm_deliveries()

        body = "hello ✓ world".encode()
        self.assertIs(
            True,
            await channel.basic.publish(
                body,
                self.queue_name,
                properties={
                    "content_type": "text/plain",
                    "delivery_mode": 2,
                    "headers": {"origin": "interop", "attempt": 1},
                },
            ),
        )

        message = await channel.basic.get(self.queue_name)
        self.assertIsNotNone(message)
        self.assertEqual(body, message.body)
        self.assertEqual("hello ✓ world", message.text)
        self.assertEqual("text/plain", message.properties.content_type)
        self.assertEqual(2, message.properties.delivery_mode)
        self.assertEqual({"origin": "interop", "attempt": 1}, message.properties.headers)
        await message.ack()

    async def test_an_unroutable_mandatory_publish_is_returned(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        await channel.confirm_deliveries()
        with self.assertRaises(exception.AMQPMessageError):
            await channel.basic.publish(b"nowhere", "no.such.queue", mandatory=True)

    async def test_a_rejected_message_is_requeued(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        await channel.basic.publish(b"requeue me", self.queue_name)

        first = await channel.basic.get(self.queue_name)
        self.assertIsNotNone(first)
        await first.reject(requeue=True)

        second = await channel.basic.get(self.queue_name)
        self.assertIsNotNone(second)
        self.assertEqual(b"requeue me", second.body)
        await second.ack()


if __name__ == "__main__":
    unittest.main()
