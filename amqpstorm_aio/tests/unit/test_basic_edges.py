import asyncio
import unittest

from amqpstorm_aio import channel as channel_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio.basic import Basic
from amqpstorm_aio.tests import factory


class AnsweringConnection:
    """Records outbound frames and answers the methods a test scripts a reply for."""

    def __init__(self):
        self.written = []
        self.user_closed = False
        self.is_closed = False
        self.max_frame_size = 131_072
        self.write_error = None
        self.answers = {}
        self.channel = None
        self.drain_calls = 0

    def check_for_errors(self):
        pass

    def write_method_frame(self, channel_id, method):
        if self.write_error is not None:
            raise self.write_error
        framing.append_method_frame(bytearray(), channel_id, method)
        self.written.append((channel_id, method))
        answer = self.answers.get(type(method))
        if answer is not None and self.channel is not None:
            self.channel.on_frame(answer)

    def write_bytes(self, data):
        if self.write_error is not None:
            raise self.write_error
        self.written.append(("bytes", bytes(data)))

    def ensure_current_loop(self):
        pass

    def report_inbound_growth(self, channel, size):
        pass

    def report_inbound_drain(self, channel, size):
        pass

    async def drain(self):
        self.drain_calls += 1
        return None

    @property
    def methods_written(self):
        return [method for marker, method in self.written if marker != "bytes"]

    @property
    def bytes_written(self):
        return b"".join(payload for marker, payload in self.written if marker == "bytes")


def make_channel(rpc_timeout=1):
    connection = AnsweringConnection()
    channel = channel_module.Channel(1, connection, rpc_timeout)
    connection.channel = channel
    channel.set_state(channel_module.base.Stateful.OPEN)
    return connection, channel


def decode_frames(data):
    """Decode a published byte buffer back into the frames it carries."""
    frames = []
    parser = framing.FrameParser(lambda channel_id, frame: frames.append(frame), 131_072)
    parser.feed(data)
    return frames


def published_header(data):
    """The content header of the single message in a published byte buffer."""
    return next(frame for frame in decode_frames(data) if isinstance(frame, framing.ContentHeader))


def published_body(data):
    """The body bytes of the single message in a published byte buffer."""
    return b"".join(frame for frame in decode_frames(data) if type(frame) is bytes)


class QosTests(unittest.IsolatedAsyncioTestCase):
    async def test_qos_sends_the_requested_prefetch_limits(self):
        connection, channel = make_channel()
        connection.answers[spec.Basic.Qos] = spec.Basic.QosOk()

        result = await Basic(channel).qos(prefetch_count=10, prefetch_size=4096, global_=True)

        self.assertEqual({}, result)
        self.assertEqual(
            [spec.Basic.Qos(prefetch_size=4096, prefetch_count=10, global_=True)],
            connection.methods_written,
        )

    async def test_qos_rejects_a_prefetch_count_that_is_not_a_short(self):
        connection, channel = make_channel()

        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            await Basic(channel).qos(prefetch_count=65536)

        self.assertEqual(
            "prefetch_count should be between 0 and 65535", str(context.exception)
        )
        self.assertEqual([], connection.written)


class RecoverTests(unittest.IsolatedAsyncioTestCase):
    async def test_recover_asks_the_broker_to_requeue_unacknowledged_messages(self):
        connection, channel = make_channel()
        connection.answers[spec.Basic.Recover] = spec.Basic.RecoverOk()

        result = await Basic(channel).recover(requeue=True)

        self.assertEqual({}, result)
        self.assertEqual([spec.Basic.Recover(requeue=True)], connection.methods_written)

    async def test_recover_rejects_a_non_boolean_requeue(self):
        connection, channel = make_channel()

        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            await Basic(channel).recover(requeue=1)

        self.assertEqual("requeue should be a boolean", str(context.exception))
        self.assertEqual([], connection.written)


class NackTests(unittest.IsolatedAsyncioTestCase):
    async def test_nack_writes_the_tag_and_both_flags(self):
        connection, channel = make_channel()

        await Basic(channel).nack(delivery_tag=7, multiple=True, requeue=False)

        self.assertEqual(
            [spec.Basic.Nack(delivery_tag=7, multiple=True, requeue=False)],
            connection.methods_written,
        )

    async def test_nack_on_a_closed_channel_raises_instead_of_writing(self):
        connection, channel = make_channel()
        channel.set_state(channel_module.base.Stateful.CLOSED)

        with self.assertRaises(exception.AMQPChannelError) as context:
            await Basic(channel).nack(delivery_tag=1)

        self.assertEqual("channel closed", str(context.exception))
        self.assertEqual([], connection.written)

    async def test_nack_rejects_a_non_boolean_requeue_before_writing(self):
        connection, channel = make_channel()

        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            await Basic(channel).nack(delivery_tag=1, requeue="yes")

        self.assertEqual("requeue should be a boolean", str(context.exception))
        self.assertEqual([], connection.written)


class RejectTests(unittest.IsolatedAsyncioTestCase):
    async def test_reject_writes_the_tag_and_the_requeue_flag(self):
        connection, channel = make_channel()

        await Basic(channel).reject(delivery_tag=4, requeue=False)

        self.assertEqual(
            [spec.Basic.Reject(delivery_tag=4, requeue=False)], connection.methods_written
        )

    async def test_reject_on_a_closed_channel_raises_instead_of_writing(self):
        connection, channel = make_channel()
        channel.set_state(channel_module.base.Stateful.CLOSED)

        with self.assertRaises(exception.AMQPChannelError) as context:
            await Basic(channel).reject(delivery_tag=4)

        self.assertEqual("channel closed", str(context.exception))
        self.assertEqual([], connection.written)


class GetWriteFailureTests(unittest.IsolatedAsyncioTestCase):
    """A Basic.Get that never reached the wire has no answer to wait for."""

    async def test_a_failed_write_propagates_and_records_no_stale_get(self):
        connection, channel = make_channel()
        connection.answers[spec.Basic.Get] = spec.Basic.GetEmpty()
        connection.write_error = exception.AMQPConnectionError("socket is gone")

        with self.assertRaises(exception.AMQPConnectionError) as context:
            await Basic(channel).get(queue="fruit")

        self.assertEqual("socket is gone", str(context.exception))

        connection.write_error = None
        self.assertIsNone(await Basic(channel).get(queue="fruit"))


class PublishPreflightTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_publish_on_a_closed_channel_raises_before_touching_the_connection(self):
        connection, channel = make_channel()
        channel.set_state(channel_module.base.Stateful.CLOSED)

        with self.assertRaises(exception.AMQPChannelError) as context:
            await Basic(channel).publish(b"body", "rk")

        self.assertEqual("channel closed", str(context.exception))
        self.assertEqual([], connection.written)
        self.assertEqual(0, connection.drain_calls, "a dead channel must not reach the socket")


class PublishWriteFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_failed_write_withdraws_the_confirm_it_registered(self):
        connection, channel = make_channel()
        channel._confirming = True
        connection.write_error = exception.AMQPConnectionError("socket is gone")

        with self.assertRaises(exception.AMQPConnectionError) as context:
            await Basic(channel).publish(b"body", "rk")

        self.assertEqual("socket is gone", str(context.exception))
        self.assertEqual({}, channel._confirm_futures, "no waiter may outlive the failed write")


class PublishConfirmTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_confirm_that_never_arrives_raises_naming_the_delivery_tag(self):
        connection, channel = make_channel(rpc_timeout=0.01)
        channel._confirming = True

        with self.assertRaises(exception.AMQPChannelError) as context:
            await Basic(channel).publish(b"body", "rk")

        self.assertEqual(
            "publisher confirm for delivery tag 1 took too long", str(context.exception)
        )
        self.assertEqual(1, len(connection.written), "the message was still published")


class MandatoryReturnClaimTests(unittest.IsolatedAsyncioTestCase):
    """Only one publish may claim a Basic.Return; the other keeps its confirm verdict."""

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

    async def test_a_return_already_claimed_leaves_the_second_publish_with_its_ack(self):
        _connection, channel = make_channel()
        channel._confirming = True
        basic = Basic(channel)
        first = asyncio.ensure_future(basic.publish(b"one", "rk", mandatory=True))
        second = asyncio.ensure_future(basic.publish(b"two", "rk", mandatory=True))
        await asyncio.sleep(0.001)

        self._record_return(channel)
        channel._resolve_confirms(2, True, result=True)
        results = await asyncio.wait_for(
            asyncio.gather(first, second, return_exceptions=True), 1
        )

        self.assertIsInstance(results[0], exception.AMQPMessageError)
        self.assertIn("NO_ROUTE", str(results[0]))
        self.assertIs(True, results[1], "the unreturned publish keeps its Basic.Ack verdict")

    async def test_a_return_raised_as_a_channel_error_is_not_reclaimed_by_a_later_publish(self):
        _connection, channel = make_channel()
        channel._confirming = True
        basic = Basic(channel)
        self._record_return(channel)

        with self.assertRaises(exception.AMQPMessageError) as context:
            await basic.publish(b"one", "rk", mandatory=True)
        self.assertIn("NO_ROUTE", str(context.exception))

        later = asyncio.ensure_future(basic.publish(b"two", "rk", mandatory=True))
        await asyncio.sleep(0.001)
        channel._resolve_confirms(1, False, result=True)

        self.assertIs(
            True,
            await asyncio.wait_for(later, 1),
            "only a return recorded during the publish may fail it",
        )


class StrBodyEncodingTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_str_body_declares_utf8_without_mutating_the_given_properties(self):
        connection, channel = make_channel()
        properties = spec.Basic.Properties(content_type="text/plain", priority=3)

        await Basic(channel).publish("héllo", "rk", properties=properties)

        self.assertIsNone(properties.content_encoding, "the caller's object must be untouched")
        header = published_header(connection.bytes_written)
        self.assertEqual("utf-8", header.properties.content_encoding)
        self.assertEqual("text/plain", header.properties.content_type)
        self.assertEqual(3, header.properties.priority)
        self.assertEqual("héllo", published_body(connection.bytes_written).decode("utf-8"))

    async def test_a_declared_content_encoding_is_used_as_given(self):
        connection, channel = make_channel()

        await Basic(channel).publish("héllo", "rk", properties={"content_encoding": "latin-1"})

        header = published_header(connection.bytes_written)
        self.assertEqual("latin-1", header.properties.content_encoding)
        self.assertEqual("héllo", published_body(connection.bytes_written).decode("latin-1"))


if __name__ == "__main__":
    unittest.main()
