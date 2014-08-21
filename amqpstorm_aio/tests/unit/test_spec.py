import datetime
import unittest

from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory

_OUTER_CLASSES = (
    spec.Connection,
    spec.Channel,
    spec.Exchange,
    spec.Queue,
    spec.Basic,
    spec.Tx,
    spec.Confirm,
)

_ASYNCHRONOUS_METHODS = frozenset(
    {
        "Connection.Blocked",
        "Connection.Unblocked",
        "Channel.FlowOk",
        "Basic.Publish",
        "Basic.Return",
        "Basic.Deliver",
        "Basic.Ack",
        "Basic.Reject",
        "Basic.RecoverAsync",
        "Basic.Nack",
    }
)

_SAMPLE_KWARGS = {
    "Connection.Start": {
        "version_major": 8,
        "version_minor": 1,
        "server_properties": {"product": "RabbitMQ", "version": "4.0"},
        "mechanisms": b"PLAIN AMQPLAIN",
        "locales": b"en_US",
    },
    "Connection.StartOk": {
        "client_properties": {"platform": "Python", "capabilities": {"basic.nack": True}},
        "mechanism": "PLAIN",
        "response": b"\x00guest\x00guest",
        "locale": "en_US",
    },
    "Connection.Secure": {"challenge": b"challenge-bytes"},
    "Connection.SecureOk": {"response": b"response-bytes"},
    "Connection.Tune": {"channel_max": 2047, "frame_max": 131072, "heartbeat": 60},
    "Connection.TuneOk": {"channel_max": 1023, "frame_max": 65536, "heartbeat": 30},
    "Connection.Open": {"virtual_host": "/production"},
    "Connection.OpenOk": {},
    "Connection.Close": {
        "reply_code": 320,
        "reply_text": "CONNECTION_FORCED",
        "class_id": 60,
        "method_id": 40,
    },
    "Connection.CloseOk": {},
    "Connection.Blocked": {"reason": "low on memory"},
    "Connection.Unblocked": {},
    "Connection.UpdateSecret": {"new_secret": b"new-token", "reason": "token rotation"},
    "Connection.UpdateSecretOk": {},
    "Channel.Open": {},
    "Channel.OpenOk": {},
    "Channel.Flow": {"active": True},
    "Channel.FlowOk": {"active": True},
    "Channel.Close": {
        "reply_code": 406,
        "reply_text": "PRECONDITION_FAILED",
        "class_id": 50,
        "method_id": 10,
    },
    "Channel.CloseOk": {},
    "Exchange.Declare": {
        "exchange": "events",
        "type": "topic",
        "passive": True,
        "durable": True,
        "auto_delete": True,
        "internal": True,
        "no_wait": True,
        "arguments": {"alternate-exchange": "fallback"},
    },
    "Exchange.DeclareOk": {},
    "Exchange.Delete": {"exchange": "events", "if_unused": True, "no_wait": True},
    "Exchange.DeleteOk": {},
    "Exchange.Bind": {
        "destination": "dst-exchange",
        "source": "src-exchange",
        "routing_key": "events.#",
        "no_wait": True,
        "arguments": {"x-match": "all"},
    },
    "Exchange.BindOk": {},
    "Exchange.Unbind": {
        "destination": "dst-exchange",
        "source": "src-exchange",
        "routing_key": "events.#",
        "no_wait": True,
        "arguments": {"x-match": "any"},
    },
    "Exchange.UnbindOk": {},
    "Queue.Declare": {
        "queue": "work-queue",
        "passive": True,
        "durable": True,
        "exclusive": True,
        "auto_delete": True,
        "no_wait": True,
        "arguments": {"x-max-length": 1000},
    },
    "Queue.DeclareOk": {"queue": "work-queue", "message_count": 5, "consumer_count": 2},
    "Queue.Bind": {
        "queue": "work-queue",
        "exchange": "events",
        "routing_key": "events.created",
        "no_wait": True,
        "arguments": {"binding-note": "unit"},
    },
    "Queue.BindOk": {},
    "Queue.Unbind": {
        "queue": "work-queue",
        "exchange": "events",
        "routing_key": "events.created",
        "arguments": {"binding-note": "unit"},
    },
    "Queue.UnbindOk": {},
    "Queue.Purge": {"queue": "work-queue", "no_wait": True},
    "Queue.PurgeOk": {"message_count": 7},
    "Queue.Delete": {"queue": "work-queue", "if_unused": True, "if_empty": True, "no_wait": True},
    "Queue.DeleteOk": {"message_count": 3},
    "Basic.Qos": {"prefetch_size": 1024, "prefetch_count": 10, "global_": True},
    "Basic.QosOk": {},
    "Basic.Consume": {
        "queue": "work-queue",
        "consumer_tag": "ctag-1",
        "no_local": True,
        "no_ack": True,
        "exclusive": True,
        "no_wait": True,
        "arguments": {"x-priority": 5},
    },
    "Basic.ConsumeOk": {"consumer_tag": "ctag-1"},
    "Basic.Cancel": {"consumer_tag": "ctag-1", "no_wait": True},
    "Basic.CancelOk": {"consumer_tag": "ctag-1"},
    "Basic.Publish": {
        "exchange": "events",
        "routing_key": "events.created",
        "mandatory": True,
        "immediate": True,
    },
    "Basic.Return": {
        "reply_code": 312,
        "reply_text": "NO_ROUTE",
        "exchange": "events",
        "routing_key": "events.created",
    },
    "Basic.Deliver": {
        "consumer_tag": "ctag-1",
        "delivery_tag": 99,
        "redelivered": True,
        "exchange": "events",
        "routing_key": "events.created",
    },
    "Basic.Get": {"queue": "work-queue", "no_ack": True},
    "Basic.GetOk": {
        "delivery_tag": 2**40 + 5,
        "redelivered": True,
        "exchange": "events",
        "routing_key": "events.created",
        "message_count": 8,
    },
    "Basic.GetEmpty": {},
    "Basic.Ack": {"delivery_tag": 42, "multiple": True},
    "Basic.Reject": {"delivery_tag": 43, "requeue": True},
    "Basic.RecoverAsync": {"requeue": True},
    "Basic.Recover": {"requeue": True},
    "Basic.RecoverOk": {},
    "Basic.Nack": {"delivery_tag": 44, "multiple": True, "requeue": True},
    "Tx.Select": {},
    "Tx.SelectOk": {},
    "Tx.Commit": {},
    "Tx.CommitOk": {},
    "Tx.Rollback": {},
    "Tx.RollbackOk": {},
    "Confirm.Select": {"nowait": True},
    "Confirm.SelectOk": {},
}

_PROPERTY_FLAG_BITS = (
    ("content_type", "text/plain", 15),
    ("content_encoding", "utf-8", 14),
    ("headers", {}, 13),
    ("delivery_mode", 1, 12),
    ("priority", 3, 11),
    ("correlation_id", "corr-1", 10),
    ("reply_to", "reply.queue", 9),
    ("expiration", "60000", 8),
    ("message_id", "msg-1", 7),
    ("timestamp", datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC), 6),
    ("type", "unit.test", 5),
    ("user_id", "guest", 4),
    ("app_id", "spec-tests", 3),
)


def _method_classes():
    for outer in _OUTER_CLASSES:
        for value in vars(outer).values():
            if isinstance(value, type) and issubclass(value, spec.Method):
                yield value


class MethodIndexTests(unittest.TestCase):
    """Combined class/method indexes and decoder-table coverage."""

    def test_key_method_indexes_match_rabbitmq_spec(self):
        self.assertEqual(0x003C0028, spec.Basic.Publish.index)
        self.assertEqual(0x0032000B, spec.Queue.DeclareOk.index)
        self.assertEqual(0x0055000A, spec.Confirm.Select.index)

    def test_exchange_unbind_errata_pairs_method_40_with_ok_51(self):
        self.assertEqual(0x00280028, spec.Exchange.Unbind.index)
        self.assertEqual(0x00280033, spec.Exchange.UnbindOk.index)
        self.assertEqual((0x00280033,), spec.Exchange.Unbind.valid_responses)

    def test_method_decoder_table_covers_every_generated_method(self):
        indexes = {cls.index for cls in _method_classes()}
        self.assertEqual(indexes, set(spec.METHOD_DECODERS))
        self.assertEqual(64, len(spec.METHOD_DECODERS))


class ValidResponsesTests(unittest.TestCase):
    """valid_responses wiring for the synchronous request methods."""

    def test_queue_declare_expects_declare_ok(self):
        self.assertEqual((spec.Queue.DeclareOk.index,), spec.Queue.Declare.valid_responses)

    def test_basic_get_expects_get_ok_or_get_empty(self):
        self.assertEqual(2, len(spec.Basic.Get.valid_responses))
        self.assertEqual(
            {spec.Basic.GetOk.index, spec.Basic.GetEmpty.index},
            set(spec.Basic.Get.valid_responses),
        )

    def test_channel_close_expects_close_ok(self):
        self.assertEqual((spec.Channel.CloseOk.index,), spec.Channel.Close.valid_responses)


class HasContentTests(unittest.TestCase):
    """has_content marks exactly the four content-bearing basic methods."""

    def test_has_content_true_only_for_basic_content_methods(self):
        content_methods = {cls.name for cls in _method_classes() if cls.has_content}
        self.assertEqual(
            {"Basic.Publish", "Basic.Return", "Basic.Deliver", "Basic.GetOk"},
            content_methods,
        )


class SynchronousFlagTests(unittest.TestCase):
    """synchronous classifies request/reply methods versus fire-and-forget ones."""

    def test_synchronous_flags_match_generated_spec(self):
        for cls in _method_classes():
            with self.subTest(method=cls.name):
                self.assertEqual(cls.name not in _ASYNCHRONOUS_METHODS, cls.synchronous)

    def test_basic_recover_is_synchronous_with_recover_ok_response(self):
        self.assertTrue(spec.Basic.Recover.synchronous)
        self.assertEqual((spec.Basic.RecoverOk.index,), spec.Basic.Recover.valid_responses)


class MethodRoundtripTests(unittest.TestCase):
    """encode() output feeds back through METHOD_DECODERS for every method class."""

    def test_sample_matrix_covers_every_method_class(self):
        discovered = {cls.name for cls in _method_classes()}
        self.assertEqual(discovered, set(_SAMPLE_KWARGS))
        self.assertEqual(64, len(discovered))

    def test_every_method_roundtrips_nondefault_values(self):
        for cls in _method_classes():
            with self.subTest(method=cls.name):
                method = cls(**_SAMPLE_KWARGS[cls.name])
                payload = bytes(method.encode())
                self.assertEqual(cls.index, int.from_bytes(payload[:4], "big"))
                decoded = spec.METHOD_DECODERS[cls.index](payload)
                self.assertIsInstance(decoded, cls)
                self.assertEqual(method, decoded)

    def test_table_default_methods_reencode_to_identical_bytes(self):
        for cls in _method_classes():
            kwargs = _SAMPLE_KWARGS[cls.name]
            table_fields = [key for key, value in kwargs.items() if isinstance(value, dict)]
            if not table_fields:
                continue
            with self.subTest(method=cls.name):
                # An unset table is None on the class and an empty table on the wire.
                without_tables = {**kwargs, **dict.fromkeys(table_fields)}
                method = cls(**without_tables)
                payload = bytes(method.encode())
                decoded = spec.METHOD_DECODERS[cls.index](payload)
                for field in table_fields:
                    self.assertEqual({}, getattr(decoded, field))
                self.assertEqual(payload, bytes(decoded.encode()))


class BitPackingTests(unittest.TestCase):
    """Consecutive bit fields share one octet at the position the grammar dictates."""

    def test_queue_declare_packs_five_bits_into_one_octet(self):
        method = factory.build(spec.Queue.Declare,
            queue="q",
            passive=True,
            durable=True,
            exclusive=True,
            auto_delete=True,
            no_wait=True,
        )
        payload = bytes(method.encode())
        bit_offset = 4 + 2 + 1 + len(b"q")
        self.assertEqual(0b11111, payload[bit_offset])
        self.assertEqual(b"\x00\x00\x00\x00", payload[bit_offset + 1:], "empty argument table")

    def test_queue_delete_packs_three_bits_into_one_octet(self):
        method = factory.build(
            spec.Queue.Delete, queue="q", if_unused=True, if_empty=True, no_wait=True
        )
        payload = bytes(method.encode())
        self.assertEqual(0b111, payload[4 + 2 + 1 + len(b"q")])

    def test_basic_nack_packs_multiple_and_requeue_bits(self):
        bit_offset = 4 + 8
        both = bytes(
            factory.build(
                spec.Basic.Nack, delivery_tag=1, multiple=True, requeue=True
            ).encode()
        )
        self.assertEqual(0b11, both[bit_offset])
        multiple_only = bytes(
            factory.build(spec.Basic.Nack, delivery_tag=1, multiple=True).encode()
        )
        self.assertEqual(0b01, multiple_only[bit_offset])
        requeue_only = bytes(factory.build(spec.Basic.Nack, delivery_tag=1, requeue=True).encode())
        self.assertEqual(0b10, requeue_only[bit_offset])


class ReservedFieldTests(unittest.TestCase):
    """Reserved spec fields are emitted as zeros and skipped transparently on decode."""

    def test_connection_open_encodes_reserved_shortstr_and_bit_as_zeros(self):
        payload = bytes(factory.build(spec.Connection.Open, virtual_host="/").encode())
        self.assertEqual(b"\x00\x0a\x00\x28\x01/\x00\x00", payload)

    def test_connection_open_roundtrips_virtual_host(self):
        method = factory.build(spec.Connection.Open, virtual_host="/production")
        decoded = spec.METHOD_DECODERS[spec.Connection.Open.index](bytes(method.encode()))
        self.assertEqual("/production", decoded.virtual_host)
        self.assertEqual(method, decoded)

    def test_basic_get_empty_encodes_reserved_shortstr_and_roundtrips(self):
        payload = bytes(factory.build(spec.Basic.GetEmpty).encode())
        self.assertEqual(b"\x00\x3c\x00\x48\x00", payload)
        decoded = spec.METHOD_DECODERS[spec.Basic.GetEmpty.index](payload)
        self.assertEqual(factory.build(spec.Basic.GetEmpty), decoded)


class SingletonDecoderTests(unittest.TestCase):
    """No-field methods decode to one shared instance instead of fresh allocations."""

    def test_no_field_methods_decode_to_shared_singleton(self):
        no_field_classes = [cls for cls in _method_classes() if cls.__slots__ == ()]
        self.assertEqual(23, len(no_field_classes))
        for cls in no_field_classes:
            with self.subTest(method=cls.name):
                payload = bytes(cls().encode())
                decoder = spec.METHOD_DECODERS[cls.index]
                self.assertIs(decoder(payload), decoder(payload))

    def test_singleton_decode_equals_fresh_instance(self):
        payload = bytes(factory.build(spec.Tx.Select).encode())
        self.assertEqual(
            factory.build(spec.Tx.Select),
            spec.METHOD_DECODERS[spec.Tx.Select.index](payload),
        )


class BasicPropertiesTests(unittest.TestCase):
    """Content-header property flags, field encoding, and decode symmetry."""

    def test_all_fields_roundtrip_preserves_values(self):
        properties = spec.Basic.Properties(
            content_type="application/json",
            content_encoding="utf-8",
            headers={"x-key": "value", "x-count": 3},
            delivery_mode=2,
            priority=5,
            correlation_id="corr-1",
            reply_to="reply.queue",
            expiration="60000",
            message_id="msg-42",
            timestamp=datetime.datetime(2026, 7, 19, 12, 30, 45, tzinfo=datetime.UTC),
            type="unit.test",
            user_id="guest",
            app_id="spec-tests",
        )
        out = bytearray()
        properties.encode(out)
        decoded, offset = spec.Basic.Properties.decode(bytes(out), 0)
        self.assertEqual(len(out), offset)
        self.assertEqual(properties, decoded)

    def test_empty_properties_encode_flags_zero(self):
        out = bytearray()
        spec.Basic.Properties().encode(out)
        self.assertEqual(b"\x00\x00", bytes(out))
        decoded, offset = spec.Basic.Properties.decode(bytes(out), 0)
        self.assertEqual(2, offset)
        self.assertEqual(spec.Basic.Properties(), decoded)

    def test_each_flag_occupies_documented_bit_position(self):
        self.assertEqual(len(spec.Basic.Properties.__slots__), len(_PROPERTY_FLAG_BITS))
        for field, value, bit in _PROPERTY_FLAG_BITS:
            with self.subTest(field=field):
                out = bytearray()
                spec.Basic.Properties(**{field: value}).encode(out)
                self.assertEqual(1 << bit, int.from_bytes(out[:2], "big"))

    def test_delivery_mode_encodes_as_single_octet(self):
        out = bytearray()
        spec.Basic.Properties(delivery_mode=2).encode(out)
        self.assertEqual(b"\x10\x00\x02", bytes(out))

    def test_timestamp_roundtrips_as_utc_datetime(self):
        timestamp = datetime.datetime(2026, 7, 19, 8, 15, 30, tzinfo=datetime.UTC)
        out = bytearray()
        spec.Basic.Properties(timestamp=timestamp).encode(out)
        decoded, offset = spec.Basic.Properties.decode(bytes(out), 0)
        self.assertEqual(len(out), offset)
        self.assertEqual(timestamp, decoded.timestamp)
        self.assertIs(datetime.UTC, decoded.timestamp.tzinfo)

    def test_reserved_cluster_id_flag_skips_shortstr_on_decode(self):
        payload = b"\x00\x04\x0bold-cluster"
        decoded, offset = spec.Basic.Properties.decode(payload, 0)
        self.assertEqual(len(payload), offset)
        self.assertEqual(spec.Basic.Properties(), decoded)


class MethodDunderTests(unittest.TestCase):
    """__repr__ and __eq__ behavior of generated methods and Properties."""

    def test_repr_includes_name_and_fields(self):
        method = factory.build(spec.Basic.Publish, exchange="ex", routing_key="rk", mandatory=True)
        self.assertEqual(
            "Basic.Publish(exchange='ex', routing_key='rk', mandatory=True, immediate=False)",
            repr(method),
        )

    def test_repr_of_no_field_method_is_bare_name(self):
        self.assertEqual("Tx.Select()", repr(factory.build(spec.Tx.Select)))

    def test_properties_repr_lists_only_set_fields(self):
        properties = spec.Basic.Properties(delivery_mode=2, app_id="worker")
        self.assertEqual("Properties(delivery_mode=2, app_id='worker')", repr(properties))

    def test_equal_field_values_compare_equal(self):
        self.assertEqual(
            factory.build(spec.Basic.Ack, delivery_tag=5, multiple=True),
            factory.build(spec.Basic.Ack, delivery_tag=5, multiple=True),
        )

    def test_different_field_values_compare_unequal(self):
        self.assertNotEqual(
            factory.build(spec.Basic.Ack, delivery_tag=5, multiple=True),
            factory.build(spec.Basic.Ack, delivery_tag=5, multiple=False),
        )

    def test_different_method_types_compare_unequal(self):
        self.assertNotEqual(factory.build(spec.Tx.Select), factory.build(spec.Tx.SelectOk))

    def test_comparison_with_non_method_returns_false(self):
        self.assertFalse(factory.build(spec.Tx.Select) == "Tx.Select")
        self.assertFalse(spec.Basic.Properties() == "Properties()")

    def test_properties_equality_matches_field_values(self):
        self.assertEqual(spec.Basic.Properties(), spec.Basic.Properties())
        self.assertNotEqual(
            spec.Basic.Properties(delivery_mode=1),
            spec.Basic.Properties(delivery_mode=2),
        )


class PropertiesTimestampTests(unittest.TestCase):
    """Timestamps outside the representable range never surface raw struct errors."""

    def test_pre_epoch_timestamp_raises_invalid_argument(self):
        from amqpstorm_aio import exception

        properties = spec.Basic.Properties(
            timestamp=datetime.datetime(1960, 1, 1, tzinfo=datetime.UTC)
        )
        with self.assertRaises(exception.AMQPInvalidArgument):
            properties.encode(bytearray())

    def test_out_of_range_inbound_timestamp_decodes_to_none(self):
        from amqpstorm_aio import encoding

        out = bytearray()
        spec.Basic.Properties(
            timestamp=datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
        ).encode(out)
        out[-8:] = encoding.PACK_LONG_LONG.pack(2**63)
        decoded, offset = spec.Basic.Properties.decode(bytes(out), 0)
        self.assertIsNone(decoded.timestamp, "a poison timestamp must not raise")
        self.assertEqual(len(out), offset)

    def test_out_of_range_table_timestamp_decodes_to_none(self):
        from amqpstorm_aio import encoding

        wire = b"T" + encoding.PACK_LONG_LONG.pack(2**63)
        decoded, offset = encoding.decode_table_value(wire, 0)
        self.assertIsNone(decoded)
        self.assertEqual(len(wire), offset)


class PropertiesFlagContinuationTests(unittest.TestCase):
    """A set continuation bit consumes the extra flags word instead of shifting fields."""

    def test_decode_skips_continuation_flag_words(self):
        from amqpstorm_aio import encoding

        reference = bytearray()
        spec.Basic.Properties(delivery_mode=2, content_type="text/plain").encode(reference)
        (flags,) = encoding.PACK_SHORT.unpack_from(reference, 0)

        crafted = bytearray()
        crafted += encoding.PACK_SHORT.pack(flags | 0x0001)
        crafted += encoding.PACK_SHORT.pack(0x0000)
        crafted += bytes(reference[2:])
        decoded, offset = spec.Basic.Properties.decode(bytes(crafted), 0)
        self.assertEqual(2, decoded.delivery_mode)
        self.assertEqual("text/plain", decoded.content_type)
        self.assertEqual(len(crafted), offset)


class FieldAssertReferentTests(unittest.TestCase):
    """Relational asserts keep the method.field they compare against."""

    def test_le_assert_names_its_bound(self):
        self.assertIn(
            ("channel-max", "le", "tune.channel-max"),
            spec.FIELD_ASSERTS["Connection.TuneOk"],
        )


if __name__ == "__main__":
    unittest.main()
