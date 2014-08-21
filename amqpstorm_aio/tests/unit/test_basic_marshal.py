import unittest

from amqpstorm_aio import basic as basic_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec

FRAME_MAX = 131_072
CHUNK = FRAME_MAX - framing.FRAME_OVERHEAD


def marshal(body, properties=None, max_frame_size=FRAME_MAX, channel_id=1,
            exchange="", routing_key="rk", mandatory=False, immediate=False):
    return basic_module.Basic._marshal_publish(
        channel_id=channel_id,
        exchange=exchange,
        routing_key=routing_key,
        mandatory=mandatory,
        immediate=immediate,
        body=body,
        properties=basic_module._EMPTY_PROPERTIES if properties is None else properties,
        max_frame_size=max_frame_size,
    )


def split_frames(buffer):
    """Walk a marshalled buffer into (frame_type, channel_id, payload) triples."""
    frames = []
    offset = 0
    data = bytes(buffer)
    while offset < len(data):
        frame_type, channel_id, size = framing.PACK_FRAME_HEADER.unpack_from(data, offset)
        payload_start = offset + 7
        payload = data[payload_start : payload_start + size]
        end = data[payload_start + size]
        if end != spec.FRAME_END:
            raise AssertionError(f"frame at {offset} is not terminated (got {end:#x})")
        frames.append((frame_type, channel_id, payload))
        offset = payload_start + size + 1
    return frames


class MarshalTestCase(unittest.TestCase):
    """The publish frame cache is module state; no test may inherit another's."""

    def setUp(self):
        basic_module._PUBLISH_FRAME_CACHE.clear()
        self.addCleanup(basic_module._PUBLISH_FRAME_CACHE.clear)


class PublishFrameCacheTests(MarshalTestCase):
    """A cached method frame must be indistinguishable from a freshly built one."""

    @staticmethod
    def uncached_method_frame(**overrides):
        arguments = {
            "exchange": "", "routing_key": "rk", "mandatory": False, "immediate": False
        }
        arguments.update(overrides)
        buffer = bytearray()
        framing.append_method_frame(buffer, 1, spec.Basic.Publish(**arguments))
        return bytes(buffer)

    def test_a_miss_and_a_hit_produce_the_same_bytes(self):
        first = bytes(marshal(b"x" * 16))
        self.assertEqual(1, len(basic_module._PUBLISH_FRAME_CACHE))
        second = bytes(marshal(b"x" * 16))
        self.assertEqual(first, second)

    def test_the_cached_frame_matches_the_generic_encoder(self):
        method_frame, _header, _body = split_frames(
            marshal(b"x", exchange="amq.topic", routing_key="a.b", mandatory=True)
        )
        expected = self.uncached_method_frame(
            exchange="amq.topic", routing_key="a.b", mandatory=True
        )
        self.assertEqual(expected[7:-1], method_frame[2])

    def test_a_reused_entry_is_not_mutated_by_differing_body_sizes(self):
        small = split_frames(marshal(b"x" * 5))[0][2]
        big = split_frames(marshal(b"y" * 4000))[0][2]
        again = split_frames(marshal(b"z" * 5))[0][2]
        self.assertEqual(small, big, "the method frame does not depend on the body")
        self.assertEqual(small, again, "a cache entry must survive intact")
        self.assertEqual(1, len(basic_module._PUBLISH_FRAME_CACHE))

    def test_each_distinct_argument_gets_its_own_entry(self):
        marshal(b"x", routing_key="one")
        marshal(b"x", routing_key="two")
        marshal(b"x", routing_key="two", mandatory=True)
        marshal(b"x", routing_key="two", channel_id=9)
        self.assertEqual(4, len(basic_module._PUBLISH_FRAME_CACHE))

    def test_the_cache_stops_growing_at_its_bound(self):
        for index in range(basic_module.PUBLISH_FRAME_CACHE_MAX + 50):
            marshal(b"x", routing_key=f"key.{index}")
        self.assertEqual(
            basic_module.PUBLISH_FRAME_CACHE_MAX, len(basic_module._PUBLISH_FRAME_CACHE)
        )

    def test_frames_stay_correct_once_the_cache_is_full(self):
        for index in range(basic_module.PUBLISH_FRAME_CACHE_MAX):
            marshal(b"x", routing_key=f"key.{index}")
        # This key never gets stored; it must still marshal correctly every time.
        for _ in range(3):
            method_frame, _header, _body = split_frames(marshal(b"x", routing_key="overflow"))
            self.assertEqual(
                self.uncached_method_frame(routing_key="overflow")[7:-1], method_frame[2]
            )


class FrameLayoutTests(MarshalTestCase):
    """The marshalled bytes are the wire format, checked directly rather than round-tripped."""

    def test_a_normal_body_emits_method_header_and_one_body_frame(self):
        frames = split_frames(marshal(b"x" * 64))
        self.assertEqual(
            [spec.FRAME_METHOD, spec.FRAME_HEADER, spec.FRAME_BODY],
            [frame_type for frame_type, _channel, _payload in frames],
        )
        self.assertEqual(b"x" * 64, frames[2][2])

    def test_an_empty_body_emits_no_body_frame(self):
        # A zero-length body frame after a body_size=0 header is a protocol error the
        # local receiver would silently drop, so a round-trip test cannot catch it.
        frames = split_frames(marshal(b""))
        self.assertEqual(
            [spec.FRAME_METHOD, spec.FRAME_HEADER],
            [frame_type for frame_type, _channel, _payload in frames],
        )

    def test_the_content_header_carries_class_weight_and_body_size(self):
        _method, header, _body = split_frames(marshal(b"x" * 7))
        class_id, weight, body_size = framing._UNPACK_CONTENT_HEADER(header[2], 0)
        self.assertEqual(60, class_id)
        self.assertEqual(0, weight)
        self.assertEqual(7, body_size)

    def test_every_frame_carries_the_channel_id(self):
        frames = split_frames(marshal(b"x" * 64, channel_id=7))
        self.assertEqual([7, 7, 7], [channel_id for _type, channel_id, _payload in frames])

    def test_bodies_around_the_chunk_boundary_split_exactly(self):
        for length, expected in ((CHUNK - 1, 1), (CHUNK, 1), (CHUNK + 1, 2), (CHUNK * 2, 2)):
            with self.subTest(length=length):
                frames = split_frames(marshal(b"y" * length))
                bodies = [p for frame_type, _c, p in frames if frame_type == spec.FRAME_BODY]
                self.assertEqual(expected, len(bodies))
                self.assertEqual(b"y" * length, b"".join(bodies))

    def test_the_decoded_method_frame_matches_the_arguments(self):
        method_frame, _header, _body = split_frames(
            marshal(b"z", exchange="amq.direct", routing_key="a.b.c", mandatory=True)
        )
        published = spec.METHOD_DECODERS[spec.Basic.Publish.index](method_frame[2])
        self.assertEqual("amq.direct", published.exchange)
        self.assertEqual("a.b.c", published.routing_key)
        self.assertTrue(published.mandatory)
        self.assertFalse(published.immediate)


class PropertiesLayoutTests(MarshalTestCase):
    """The empty-properties fast path must produce exactly what the generic path does."""

    def test_empty_properties_match_the_generic_encoder_byte_for_byte(self):
        fast = bytes(marshal(b"x" * 32))
        generic = bytearray()
        framing.append_method_frame(
            generic, 1,
            spec.Basic.Publish(exchange="", routing_key="rk", mandatory=False, immediate=False),
        )
        framing.append_content_header_frame(generic, 1, 32, spec.Basic.Properties())
        generic += framing.PACK_FRAME_HEADER.pack(spec.FRAME_BODY, 1, 32)
        generic += b"x" * 32
        generic.append(spec.FRAME_END)
        self.assertEqual(bytes(generic), fast)

    def test_set_properties_round_trip_through_the_header(self):
        properties = spec.Basic.Properties(content_type="text/plain", delivery_mode=2)
        _method, header, _body = split_frames(marshal(b"x", properties=properties))
        decoded, offset = spec.Basic.Properties.decode(header[2], 12)
        self.assertEqual("text/plain", decoded.content_type)
        self.assertEqual(2, decoded.delivery_mode)
        self.assertEqual(len(header[2]), offset)

    def test_oversized_properties_are_refused(self):
        properties = spec.Basic.Properties(headers={"pad": "x" * 200_000})
        with self.assertRaises(exception.AMQPInvalidArgument):
            marshal(b"x", properties=properties)


if __name__ == "__main__":
    unittest.main()
