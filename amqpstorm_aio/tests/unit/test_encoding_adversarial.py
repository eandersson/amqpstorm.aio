import datetime
import struct
import unittest
from decimal import Decimal

from amqpstorm_aio import encoding
from amqpstorm_aio import exception

_DECODE_ERRORS = (struct.error, IndexError, UnicodeDecodeError, exception.AMQPFrameError)


class UnknownTagTests(unittest.TestCase):
    """Unrecognized field-table value tags must fail loudly as frame errors."""

    def test_unknown_value_tag_raises_frame_error(self):
        with self.assertRaises(exception.AMQPFrameError):
            encoding.decode_table_value(b"Z\x01\x02\x03", 0)

    def test_unknown_tag_inside_table_raises_frame_error(self):
        payload = b"\x00\x00\x00\x03\x01kZ"
        with self.assertRaises(exception.AMQPFrameError):
            encoding.decode_table(payload, 0)


class TruncatedBufferTests(unittest.TestCase):
    """Truncated or corrupt buffers must raise rather than return garbage.

    This layer trusts the frame reader for buffer boundaries, so the exact exception type
    (struct.error / IndexError / UnicodeDecodeError) is not part of the contract; the tests
    only pin down that decoding fails instead of fabricating values.
    """

    def test_empty_payload_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"", 0)

    def test_truncated_shortstr_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_shortstr(b"", 0)

    def test_truncated_table_length_prefix_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table(b"\x00\x00", 0)

    def test_table_length_beyond_buffer_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table(b"\x00\x00\x00\x0a", 0)

    def test_truncated_int32_payload_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"I\xff\xff", 0)

    def test_truncated_int64_payload_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"l\x00\x00\x00", 0)

    def test_truncated_double_payload_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"d\x00", 0)

    def test_truncated_decimal_payload_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"D\x02\x00", 0)

    def test_truncated_timestamp_payload_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"T\x00\x00", 0)

    def test_truncated_bool_payload_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"t", 0)

    def test_truncated_array_length_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"A\x00\x00", 0)

    def test_truncated_array_element_raises(self):
        with self.assertRaises(_DECODE_ERRORS):
            encoding.decode_table_value(b"A\x00\x00\x00\x05I\x00", 0)

class CorruptBufferLeniencyTests(unittest.TestCase):
    """Where corrupt input is tolerated by design, and where it must fail loudly."""

    def test_invalid_utf8_longstr_value_decodes_with_surrogates(self):
        decoded, offset = encoding.decode_table_value(b"S\x00\x00\x00\x02\xff\xfe", 0)
        self.assertEqual(decoded, "\udcff\udcfe")
        self.assertEqual(offset, 7)

    def test_invalid_utf8_table_key_decodes_with_surrogates(self):
        # The spec allows any octets in a shortstr; one odd key must not be able to
        # take the whole connection down.
        payload = b"\x00\x00\x00\x04\x02\xff\xfeV"
        decoded, offset = encoding.decode_table(payload, 0)
        self.assertEqual({"\udcff\udcfe": None}, decoded)
        self.assertEqual(offset, len(payload))

    def test_longstr_length_beyond_buffer_raises(self):
        with self.assertRaises(exception.AMQPFrameError):
            encoding.decode_longstr(b"\x00\x00\x00\x64abc", 0)

    def test_shortstr_length_beyond_buffer_raises(self):
        with self.assertRaises(exception.AMQPFrameError):
            encoding.decode_shortstr(b"\x64abc", 0)


class UnsupportedTypeTests(unittest.TestCase):
    """Values with no field-table representation are rejected before touching the wire."""

    def test_object_value_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), object())

    def test_set_value_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), {1, 2, 3})

    def test_complex_value_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), complex(1, 2))


class DeepNestingTests(unittest.TestCase):
    """Recursive encode/decode survives pathological nesting depth."""

    NESTING_DEPTH = 50

    def test_fifty_level_nested_dict_roundtrips(self):
        table = {"leaf": 1}
        for _ in range(self.NESTING_DEPTH):
            table = {"child": table}
        out = bytearray()
        encoding.encode_table(out, table)
        decoded, offset = encoding.decode_table(bytes(out), 0)
        self.assertEqual(offset, len(out))
        self.assertEqual(decoded, table)

    def test_fifty_level_nested_list_roundtrips(self):
        value = [1]
        for _ in range(self.NESTING_DEPTH):
            value = [value]
        out = bytearray()
        encoding.encode_table_value(out, value)
        decoded, offset = encoding.decode_table_value(bytes(out), 0)
        self.assertEqual(offset, len(out))
        self.assertEqual(decoded, value)


class NestingDepthBoundTests(unittest.TestCase):
    """Nesting past MAX_TABLE_DEPTH is refused instead of recursing to death."""

    @staticmethod
    def nested_dict(levels):
        table = {"leaf": 1}
        for _ in range(levels):
            table = {"child": table}
        return table

    def test_nesting_at_the_bound_encodes(self):
        out = bytearray()
        encoding.encode_table(out, self.nested_dict(encoding.MAX_TABLE_DEPTH))
        decoded, _offset = encoding.decode_table(bytes(out), 0)
        self.assertEqual(self.nested_dict(encoding.MAX_TABLE_DEPTH), decoded)

    def test_dict_nesting_past_the_bound_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table(bytearray(), self.nested_dict(encoding.MAX_TABLE_DEPTH + 1))

    def test_list_nesting_past_the_bound_raises_invalid_argument(self):
        value = [1]
        for _ in range(encoding.MAX_TABLE_DEPTH + 1):
            value = [value]
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), value)

    def test_inbound_deep_nested_tables_raise_frame_error(self):
        # Hand-built wire bytes, since the encoder refuses to produce this depth.
        table = b"\x00\x00\x00\x00"
        for _ in range(encoding.MAX_TABLE_DEPTH + 10):
            entry = b"\x01k" + b"F" + table
            table = encoding.PACK_LONG.pack(len(entry)) + entry
        with self.assertRaises(exception.AMQPFrameError):
            encoding.decode_table(table, 0)

    def test_inbound_deep_nested_arrays_raise_frame_error(self):
        value = encoding.PACK_LONG.pack(0)
        for _ in range(encoding.MAX_TABLE_DEPTH + 10):
            item = b"A" + value
            value = encoding.PACK_LONG.pack(len(item)) + item
        with self.assertRaises(exception.AMQPFrameError):
            encoding.decode_table_value(b"A" + value, 0)


class NonUtf8RoundTripTests(unittest.TestCase):
    """Arbitrary octets received off the wire re-encode to the same octets."""

    def test_shortstr_with_raw_high_octets_decodes_and_reencodes(self):
        wire = b"\x02\xff\xfe"
        value, offset = encoding.decode_shortstr(wire, 0)
        self.assertEqual(3, offset)
        out = bytearray()
        encoding.encode_shortstr(out, value)
        self.assertEqual(wire, bytes(out))

    def test_table_string_value_with_raw_high_octets_round_trips(self):
        wire = bytearray()
        encoding.encode_shortstr(wire, "key")
        wire += b"S"
        encoding.encode_longstr(wire, b"\xffbinary\xfe")
        table = encoding.PACK_LONG.pack(len(wire)) + bytes(wire)
        decoded, _offset = encoding.decode_table(table, 0)
        out = bytearray()
        encoding.encode_table(out, decoded)
        self.assertEqual(table, bytes(out), "decoded tables must republish byte-for-byte")


class OversizedKeyTests(unittest.TestCase):
    """Field-table keys are shortstrs and share the 255-byte cap."""

    def test_key_over_255_bytes_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table(bytearray(), {"k" * 256: 1})

    def test_multibyte_key_over_255_bytes_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table(bytearray(), {"é" * 128: 1})


class NonStringKeyTests(unittest.TestCase):
    """Field-table keys that are not strings fail loudly and typed."""

    def test_integer_key_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table(bytearray(), {1: "value"})

    def test_bytes_key_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table(bytearray(), {b"key": "value"})


class PositiveExponentDecimalTests(unittest.TestCase):
    """Whole-number decimals with positive exponents are representable at scale 0."""

    def test_positive_exponent_decimal_round_trips(self):
        out = bytearray()
        encoding.encode_table_value(out, Decimal("1E+2"))
        decoded, _offset = encoding.decode_table_value(bytes(out), 0)
        self.assertEqual(Decimal(100), decoded)

    def test_normalized_hundred_equals_plain_hundred_on_the_wire(self):
        plain = bytearray()
        encoding.encode_table_value(plain, Decimal(100))
        normalized = bytearray()
        encoding.encode_table_value(normalized, Decimal(100).normalize())
        self.assertEqual(bytes(plain), bytes(normalized))

    def test_positive_exponent_beyond_int32_still_raises(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), Decimal("1E+30"))


class EncoderRangeGapTests(unittest.TestCase):
    """Values AMQP cannot represent raise AMQPInvalidArgument, never a raw struct.error."""

    def test_decimal_scale_over_255_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), Decimal("1E-300"))

    def test_decimal_unscaled_beyond_32_bit_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), Decimal(9999999999))

    def test_pre_epoch_datetime_raises_invalid_argument(self):
        value = datetime.datetime(1960, 1, 1, tzinfo=datetime.UTC)
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), value)
