import datetime
import random
import struct
import unittest

from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory

_LARGE_FRAME_MAX = 131072


def _make_parser(max_frame_size=_LARGE_FRAME_MAX):
    received = []
    parser = framing.FrameParser(
        lambda channel_id, value: received.append((channel_id, value)), max_frame_size
    )
    return parser, received


def _mixed_frame_stream():
    """One of every frame type, back to back, as a broker would send them."""
    stream = bytearray()
    framing.append_method_frame(
        stream,
        1,
        factory.build(spec.Basic.Deliver, consumer_tag="ctag", delivery_tag=7, routing_key="rk"),
    )
    framing.append_content_header_frame(
        stream, 1, 11, spec.Basic.Properties(content_type="text/plain", delivery_mode=2)
    )
    framing.append_frame(stream, spec.FRAME_BODY, 1, b"hello ")
    framing.append_frame(stream, spec.FRAME_BODY, 1, b"world")
    stream += framing.HEARTBEAT_FRAME
    framing.append_method_frame(
        stream, 2, factory.build(spec.Basic.Qos, prefetch_size=0, prefetch_count=5, global_=False)
    )
    return bytes(stream)


def _comparable(dispatched):
    """Dispatched frames as plain values, so two feeds can be compared for equality."""
    result = []
    for channel_id, value in dispatched:
        if isinstance(value, bytes):
            result.append((channel_id, "body", value))
        elif isinstance(value, framing.Heartbeat):
            result.append((channel_id, "heartbeat"))
        elif isinstance(value, framing.ContentHeader):
            encoded = bytearray()
            value.properties.encode(encoded)
            result.append((channel_id, "header", value.body_size, bytes(encoded)))
        else:
            result.append((channel_id, "method", type(value).__name__, bytes(value.encode())))
    return result


def _feed_in_chunks(stream, chunks):
    parser, received = _make_parser()
    for chunk in chunks:
        parser.feed(chunk)
    return _comparable(received)


class ModuleConstantsTests(unittest.TestCase):
    """The wire-level constants the connection handshake depends on."""

    def test_protocol_header_is_amqp_0_9_1_preamble(self):
        self.assertEqual(framing.PROTOCOL_HEADER, b"AMQP\x00\x00\x09\x01")

    def test_heartbeat_frame_is_exact_eight_byte_frame(self):
        self.assertEqual(framing.HEARTBEAT_FRAME, b"\x08\x00\x00\x00\x00\x00\x00\xce")

    def test_frame_overhead_is_header_plus_end_octet(self):
        self.assertEqual(framing.FRAME_OVERHEAD, 8)


class AppendFrameTests(unittest.TestCase):
    """append_* helpers emit frames the parser (and a real broker) can read back."""

    def test_append_frame_emits_header_payload_and_frame_end(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_BODY, 5, b"abc")
        frame_type, channel_id, payload_size = struct.unpack_from(">BHI", out, 0)
        self.assertEqual(frame_type, spec.FRAME_BODY)
        self.assertEqual(channel_id, 5)
        self.assertEqual(payload_size, 3)
        self.assertEqual(bytes(out[7:10]), b"abc")
        self.assertEqual(out[-1], spec.FRAME_END)
        self.assertEqual(len(out), framing.FRAME_OVERHEAD + 3)

    def test_append_frame_output_parses_back_to_body_bytes(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_BODY, 9, b"payload")
        parser, received = _make_parser()
        parser.feed(bytes(out))
        self.assertEqual(received, [(9, b"payload")])

    def test_append_method_frame_roundtrips_through_parser(self):
        method = factory.build(spec.Basic.Qos, prefetch_size=0, prefetch_count=5, global_=True)
        out = bytearray()
        framing.append_method_frame(out, 3, method)
        parser, received = _make_parser()
        parser.feed(bytes(out))
        self.assertEqual(len(received), 1)
        channel_id, decoded = received[0]
        self.assertEqual(channel_id, 3)
        self.assertIsInstance(decoded, spec.Basic.Qos)
        self.assertEqual(decoded, method)

    def test_append_content_header_frame_emits_basic_class_with_zero_weight(self):
        out = bytearray()
        framing.append_content_header_frame(out, 1, 7, spec.Basic.Properties())
        payload = bytes(out[7:-1])
        self.assertEqual(payload[:2], b"\x00\x3c")
        self.assertEqual(payload[2:4], b"\x00\x00", "weight must be zero per AMQP 0-9-1")
        self.assertEqual(struct.unpack_from(">Q", payload, 4)[0], 7)

    def test_append_content_header_frame_roundtrips_properties(self):
        properties = spec.Basic.Properties(
            content_type="text/plain",
            delivery_mode=2,
            headers={"x-key": "value"},
            correlation_id="corr-1",
            timestamp=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        )
        out = bytearray()
        framing.append_content_header_frame(out, 4, 1024, properties)
        parser, received = _make_parser()
        parser.feed(bytes(out))
        self.assertEqual(len(received), 1)
        channel_id, decoded = received[0]
        self.assertEqual(channel_id, 4)
        self.assertIsInstance(decoded, framing.ContentHeader)
        self.assertEqual(decoded.body_size, 1024)
        self.assertEqual(decoded.properties, properties)


class FrameParserDispatchTests(unittest.TestCase):
    """Happy-path dispatch behaviour of FrameParser.feed."""

    def test_multiple_frames_in_one_feed_dispatch_in_order(self):
        method = factory.build(spec.Basic.Qos, prefetch_count=7)
        out = bytearray()
        framing.append_method_frame(out, 1, method)
        framing.append_frame(out, spec.FRAME_BODY, 1, b"hello")
        out += framing.HEARTBEAT_FRAME
        parser, received = _make_parser()
        parser.feed(bytes(out))
        self.assertEqual(len(received), 3)
        self.assertEqual(received[0], (1, method))
        self.assertEqual(received[1], (1, b"hello"))
        self.assertEqual(received[2][0], 0)
        self.assertIs(received[2][1], framing.HEARTBEAT)

    def test_frame_fed_byte_by_byte_dispatches_exactly_once(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_BODY, 2, b"drip")
        parser, received = _make_parser()
        for index in range(len(out)):
            parser.feed(bytes(out[index : index + 1]))
            if index < len(out) - 1:
                self.assertEqual(received, [], f"dispatched early after byte {index}")
        self.assertEqual(received, [(2, b"drip")])

    def test_every_split_point_dispatches_the_same_frames(self):
        """A lossy link delivers the same bytes in arbitrary pieces.

        TCP repairs loss and reordering, so the parser always sees a correct byte
        stream -- but segmented wherever retransmission happened to land. Where the
        boundary falls must never change what comes out.
        """
        stream = _mixed_frame_stream()
        expected = _feed_in_chunks(stream, [stream])
        self.assertEqual(6, len(expected), "fixture should cover every frame type")
        for split in range(len(stream) + 1):
            with self.subTest(split=split):
                self.assertEqual(
                    expected, _feed_in_chunks(stream, [stream[:split], stream[split:]])
                )

    def test_a_byte_at_a_time_dispatches_the_same_frames(self):
        stream = _mixed_frame_stream()
        expected = _feed_in_chunks(stream, [stream])
        drip = [stream[index : index + 1] for index in range(len(stream))]
        self.assertEqual(expected, _feed_in_chunks(stream, drip))

    def test_random_segmentation_dispatches_the_same_frames(self):
        stream = _mixed_frame_stream()
        expected = _feed_in_chunks(stream, [stream])
        for seed in range(50):
            rng = random.Random(seed)
            chunks = []
            offset = 0
            while offset < len(stream):
                size = rng.randint(1, 17)
                chunks.append(stream[offset : offset + size])
                offset += size
            with self.subTest(seed=seed):
                self.assertEqual(expected, _feed_in_chunks(stream, chunks))

    def test_an_empty_feed_between_fragments_changes_nothing(self):
        stream = _mixed_frame_stream()
        expected = _feed_in_chunks(stream, [stream])
        midpoint = len(stream) // 2
        self.assertEqual(
            expected,
            _feed_in_chunks(stream, [b"", stream[:midpoint], b"", stream[midpoint:], b""]),
        )

    def test_partial_trailing_frame_stays_buffered_until_completed(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_BODY, 1, b"first")
        second = bytearray()
        framing.append_frame(second, spec.FRAME_BODY, 1, b"second")
        parser, received = _make_parser()
        parser.feed(bytes(out) + bytes(second[:6]))
        self.assertEqual(received, [(1, b"first")])
        parser.feed(bytes(second[6:]))
        self.assertEqual(received, [(1, b"first"), (1, b"second")])

    def test_a_frame_completed_from_the_buffer_is_not_dispatched_again(self):
        first = bytearray()
        framing.append_frame(first, spec.FRAME_BODY, 1, b"one")
        second = bytearray()
        framing.append_frame(second, spec.FRAME_BODY, 2, b"two")
        parser, received = _make_parser()
        parser.feed(bytes(first[:4]))
        parser.feed(bytes(first[4:]))
        self.assertEqual(received, [(1, b"one")])
        parser.feed(bytes(second))
        self.assertEqual(received, [(1, b"one"), (2, b"two")])

    def test_body_frame_dispatches_bytes_with_channel_id(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_BODY, 7, b"\x00\x01binary\xff")
        parser, received = _make_parser()
        parser.feed(bytes(out))
        channel_id, body = received[0]
        self.assertEqual(channel_id, 7)
        self.assertIsInstance(body, bytes)
        self.assertEqual(body, b"\x00\x01binary\xff")

    def test_empty_body_frame_dispatches_empty_bytes(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_BODY, 1, b"")
        parser, received = _make_parser()
        parser.feed(bytes(out))
        self.assertEqual(received, [(1, b"")])

    def test_heartbeat_frame_dispatches_heartbeat_sentinel(self):
        parser, received = _make_parser()
        parser.feed(framing.HEARTBEAT_FRAME)
        self.assertEqual(len(received), 1)
        channel_id, value = received[0]
        self.assertEqual(channel_id, 0)
        self.assertIs(value, framing.HEARTBEAT)


class FrameParserErrorTests(unittest.TestCase):
    """Malformed input must raise; the parser is documented non-resynchronizable."""

    def test_wrong_frame_end_octet_raises_frame_error_naming_byte(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_BODY, 1, b"abc")
        out[-1] = 0xAB
        parser, _received = _make_parser()
        with self.assertRaisesRegex(exception.AMQPFrameError, "0xAB"):
            parser.feed(bytes(out))

    def test_unknown_frame_type_raises_frame_error(self):
        out = bytearray()
        framing.append_frame(out, 9, 1, b"abc")
        parser, _received = _make_parser()
        with self.assertRaisesRegex(exception.AMQPFrameError, "unknown frame type 9"):
            parser.feed(bytes(out))

    def test_payload_size_over_max_frame_size_raises_frame_error(self):
        parser, _received = _make_parser(max_frame_size=16)
        header = struct.pack(">BHI", spec.FRAME_BODY, 1, 17) + b"\x00"
        with self.assertRaisesRegex(exception.AMQPFrameError, "exceeds"):
            parser.feed(header)

    def test_server_amqp_preamble_raises_connection_error_with_version(self):
        parser, _received = _make_parser()
        with self.assertRaisesRegex(exception.AMQPConnectionError, r"0\.9-1"):
            parser.feed(b"AMQP\x00\x00\x09\x01")

    def test_protocol_header_split_across_feeds_is_buffered_then_rejected(self):
        parser, received = _make_parser()
        parser.feed(b"AMQP\x00")
        self.assertEqual(received, [])
        with self.assertRaisesRegex(exception.AMQPConnectionError, r"0\.9-1"):
            parser.feed(b"\x00\x09\x01")

    def test_leading_a_that_is_not_the_amqp_name_is_parsed_as_a_frame(self):
        parser, _received = _make_parser()
        with self.assertRaisesRegex(exception.AMQPFrameError, "unknown frame type 65"):
            parser.feed(b"AMQ\x00\x00\x00\x00\xce")

    def test_unknown_method_index_raises_frame_error(self):
        out = bytearray()
        framing.append_frame(out, spec.FRAME_METHOD, 1, b"\x00\x3c\x03\xe7")
        parser, _received = _make_parser()
        with self.assertRaisesRegex(exception.AMQPFrameError, "unknown method"):
            parser.feed(bytes(out))

    def test_content_header_for_non_basic_class_raises_frame_error(self):
        payload = b"\x00\x32\x00\x00" + struct.pack(">Q", 10) + b"\x00\x00"
        out = bytearray()
        framing.append_frame(out, spec.FRAME_HEADER, 1, payload)
        parser, _received = _make_parser()
        with self.assertRaisesRegex(exception.AMQPFrameError, "class 50"):
            parser.feed(bytes(out))


if __name__ == "__main__":
    unittest.main()
