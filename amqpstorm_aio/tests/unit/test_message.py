import unittest

from amqpstorm_aio import message
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory


class FakeBasic:
    """Records acknowledgement calls without touching a real channel."""

    def __init__(self):
        self.calls = []

    async def ack(self, delivery_tag=0, multiple=False):
        self.calls.append(("ack", delivery_tag, multiple))

    async def nack(self, delivery_tag=0, multiple=False, requeue=True):
        self.calls.append(("nack", delivery_tag, multiple, requeue))

    async def reject(self, delivery_tag=0, requeue=True):
        self.calls.append(("reject", delivery_tag, requeue))


class FakeChannel:
    def __init__(self):
        self.basic = FakeBasic()


def make_deliver_message(channel=None, body=b"payload", **properties):
    method = factory.build(spec.Basic.Deliver,
        consumer_tag="ctag-1",
        delivery_tag=42,
        redelivered=True,
        exchange="events",
        routing_key="orders.created",
    )
    return message.Message(channel, body, method, spec.Basic.Properties(**properties))


def make_get_message(body=b"payload"):
    method = factory.build(spec.Basic.GetOk,
        delivery_tag=7,
        redelivered=False,
        exchange="events",
        routing_key="orders.created",
        message_count=3,
    )
    return message.Message(None, body, method, spec.Basic.Properties())


class ConstructionTests(unittest.TestCase):
    """Message stores exactly what it is given; body is always bytes."""

    def test_attributes_are_stored_verbatim(self):
        channel = FakeChannel()
        properties = spec.Basic.Properties(content_type="text/plain")
        method = factory.build(spec.Basic.Deliver, consumer_tag="tag", delivery_tag=1)
        inbound = message.Message(channel, b"body", method, properties)
        self.assertIs(inbound.channel, channel)
        self.assertEqual(inbound.body, b"body")
        self.assertIs(inbound.method, method)
        self.assertIs(inbound.properties, properties)

    def test_len_is_body_length(self):
        self.assertEqual(7, len(make_deliver_message(body=b"payload")))
        self.assertEqual(0, len(make_deliver_message(body=b"")))

    def test_repr_shows_truncated_body_and_method(self):
        rendered = repr(make_deliver_message(body=b"x" * 100))
        self.assertIn("x" * 64, rendered)
        self.assertNotIn("x" * 65, rendered)
        self.assertIn("Basic.Deliver", rendered)


class DecodingTests(unittest.TestCase):
    """text/json decode on demand; body itself never changes type."""

    def test_text_defaults_to_utf8(self):
        inbound = make_deliver_message(body="héllo".encode())
        self.assertEqual("héllo", inbound.text)

    def test_text_honors_declared_content_encoding(self):
        inbound = make_deliver_message(
            body="héllo".encode("latin-1"), content_encoding="latin-1"
        )
        self.assertEqual("héllo", inbound.text)

    def test_text_raises_on_undecodable_body(self):
        inbound = make_deliver_message(body=b"\xff\xfe\xfa")
        self.assertRaises(UnicodeDecodeError, lambda: inbound.text)

    def test_json_parses_body(self):
        inbound = make_deliver_message(body=b'{"count": 3}')
        self.assertEqual({"count": 3}, inbound.json())

    def test_body_stays_bytes_after_text_access(self):
        inbound = make_deliver_message(body=b"payload")
        self.assertEqual("payload", inbound.text)
        self.assertEqual(b"payload", inbound.body)
        self.assertIsInstance(inbound.body, bytes)


class DeliveryContextTests(unittest.TestCase):
    """Context accessors read straight off the Deliver/GetOk method."""

    def test_deliver_context_fields(self):
        inbound = make_deliver_message()
        self.assertEqual(42, inbound.delivery_tag)
        self.assertTrue(inbound.redelivered)
        self.assertEqual("events", inbound.exchange)
        self.assertEqual("orders.created", inbound.routing_key)
        self.assertEqual("ctag-1", inbound.consumer_tag)

    def test_get_ok_context_fields(self):
        inbound = make_get_message()
        self.assertEqual(7, inbound.delivery_tag)
        self.assertFalse(inbound.redelivered)
        self.assertEqual("events", inbound.exchange)
        self.assertEqual("orders.created", inbound.routing_key)

    def test_get_ok_has_no_consumer_tag(self):
        self.assertIsNone(make_get_message().consumer_tag)


class AcknowledgementTests(unittest.IsolatedAsyncioTestCase):
    """ack/nack/reject delegate to channel.basic with this message's delivery tag."""

    async def test_ack_uses_delivery_tag(self):
        channel = FakeChannel()
        await make_deliver_message(channel).ack()
        self.assertEqual([("ack", 42, False)], channel.basic.calls)

    async def test_nack_defaults_to_requeue(self):
        channel = FakeChannel()
        await make_deliver_message(channel).nack()
        self.assertEqual([("nack", 42, False, True)], channel.basic.calls)

    async def test_nack_requeue_false_passes_through(self):
        channel = FakeChannel()
        await make_deliver_message(channel).nack(requeue=False)
        self.assertEqual([("nack", 42, False, False)], channel.basic.calls)

    async def test_reject_defaults_to_requeue(self):
        channel = FakeChannel()
        await make_deliver_message(channel).reject()
        self.assertEqual([("reject", 42, True)], channel.basic.calls)

    async def test_reject_requeue_false_passes_through(self):
        channel = FakeChannel()
        await make_deliver_message(channel).reject(requeue=False)
        self.assertEqual([("reject", 42, False)], channel.basic.calls)


if __name__ == "__main__":
    unittest.main()
