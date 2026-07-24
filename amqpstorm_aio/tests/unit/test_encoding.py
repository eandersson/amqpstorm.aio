import datetime
import enum
import unittest
from decimal import Decimal
from typing import ClassVar

from amqpstorm_aio import encoding
from amqpstorm_aio import exception

_UNSET = object()


class EncodingTestCase(unittest.TestCase):
    """Shared roundtrip helpers for field-table value tests."""

    def encode_value(self, value):
        out = bytearray()
        encoding.encode_table_value(out, value)
        return bytes(out)

    def assert_value_roundtrips(self, value, expected=_UNSET):
        payload = self.encode_value(value)
        decoded, offset = encoding.decode_table_value(payload, 0)
        self.assertEqual(offset, len(payload), "decoder must consume the entire encoded value")
        self.assertEqual(decoded, value if expected is _UNSET else expected)
        return decoded


class ShortStrTests(unittest.TestCase):
    """encode_shortstr / decode_shortstr length-prefixed UTF-8 strings."""

    def roundtrip(self, value):
        out = bytearray()
        encoding.encode_shortstr(out, value)
        decoded, offset = encoding.decode_shortstr(bytes(out), 0)
        self.assertEqual(offset, len(out), "decoder must consume the entire encoded string")
        return decoded

    def test_ascii_value_roundtrips(self):
        self.assertEqual(self.roundtrip("amq.direct"), "amq.direct")

    def test_unicode_value_roundtrips(self):
        self.assertEqual(self.roundtrip("kö åäö ✓ ☃"), "kö åäö ✓ ☃")

    def test_empty_value_roundtrips(self):
        out = bytearray()
        encoding.encode_shortstr(out, "")
        self.assertEqual(bytes(out), b"\x00")
        self.assertEqual(encoding.decode_shortstr(bytes(out), 0), ("", 1))

    def test_255_byte_value_roundtrips(self):
        value = "a" * 255
        self.assertEqual(self.roundtrip(value), value)

    def test_256_byte_value_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_shortstr(bytearray(), "a" * 256)

    def test_multibyte_value_over_255_bytes_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_shortstr(bytearray(), "é" * 128)

    def test_decode_starts_at_given_offset(self):
        payload = b"\xff\x03abc"
        self.assertEqual(encoding.decode_shortstr(payload, 1), ("abc", 5))


class LongStrTests(unittest.TestCase):
    """encode_longstr / decode_longstr 32-bit length-prefixed byte strings."""

    def test_bytes_value_roundtrips(self):
        out = bytearray()
        encoding.encode_longstr(out, b"\x00\x01binary\xff")
        self.assertEqual(encoding.decode_longstr(bytes(out), 0), (b"\x00\x01binary\xff", len(out)))

    def test_empty_value_roundtrips(self):
        out = bytearray()
        encoding.encode_longstr(out, b"")
        self.assertEqual(bytes(out), b"\x00\x00\x00\x00")
        self.assertEqual(encoding.decode_longstr(bytes(out), 0), (b"", 4))

    def test_length_prefix_is_big_endian_uint32(self):
        out = bytearray()
        encoding.encode_longstr(out, b"abc")
        self.assertEqual(bytes(out), b"\x00\x00\x00\x03abc")


class IntTagSelectionTests(EncodingTestCase):
    """Integers pick the smallest-fitting signed tag and roundtrip at every boundary."""

    BOUNDARY_TAG_CASES: ClassVar[list[tuple[int, bytes]]] = [
        (-128, b"b"),
        (127, b"b"),
        (-129, b"s"),
        (128, b"s"),
        (-32768, b"s"),
        (32767, b"s"),
        (-32769, b"I"),
        (32768, b"I"),
        (-2147483648, b"I"),
        (2147483647, b"I"),
        (-2147483649, b"l"),
        (2147483648, b"l"),
        (-9223372036854775808, b"l"),
        (9223372036854775807, b"l"),
    ]

    def test_boundary_values_use_smallest_fitting_tag(self):
        for value, tag in self.BOUNDARY_TAG_CASES:
            with self.subTest(value=value, tag=tag):
                payload = self.encode_value(value)
                self.assertEqual(payload[0:1], tag)
                self.assert_value_roundtrips(value)

    def test_zero_uses_octet_tag(self):
        self.assertEqual(self.encode_value(0), b"b\x00")

    def test_value_above_64_bit_range_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), 2**63)

    def test_value_below_64_bit_range_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), -(2**63) - 1)


class ScalarValueRoundtripTests(EncodingTestCase):
    """Every scalar field-table type the encoder supports roundtrips."""

    def test_bool_values_roundtrip(self):
        self.assertEqual(self.encode_value(True), b"t\x01")
        self.assertEqual(self.encode_value(False), b"t\x00")
        self.assertIs(self.assert_value_roundtrips(True), True)
        self.assertIs(self.assert_value_roundtrips(False), False)

    def test_float_roundtrips_as_double(self):
        self.assertEqual(self.encode_value(1.25)[0:1], b"d")
        self.assert_value_roundtrips(1.25)
        self.assert_value_roundtrips(0.1)
        self.assert_value_roundtrips(-2.5)

    def test_decimal_roundtrips(self):
        self.assert_value_roundtrips(Decimal("3.14"))

    def test_negative_decimal_roundtrips(self):
        self.assert_value_roundtrips(Decimal("-1.23"))

    def test_integral_decimal_roundtrips(self):
        self.assert_value_roundtrips(Decimal(5))

    def test_decimal_with_positive_exponent_roundtrips_as_its_integral_value(self):
        out = bytearray()
        encoding.encode_table_value(out, Decimal("1.5E+3"))
        decoded, _offset = encoding.decode_table_value(bytes(out), 0)
        self.assertEqual(Decimal(1500), decoded)

    def test_decimal_nan_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), Decimal("NaN"))

    def test_decimal_infinity_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            encoding.encode_table_value(bytearray(), Decimal("Infinity"))

    def test_str_value_roundtrips(self):
        self.assertEqual(self.encode_value("hej")[0:1], b"S")
        self.assert_value_roundtrips("héllo wörld ✓")
        self.assert_value_roundtrips("")

    def test_bytes_value_roundtrips(self):
        self.assertEqual(self.encode_value(b"ab")[0:1], b"x")
        self.assert_value_roundtrips(b"\x00\x01\xfe\xff")

    def test_bytearray_value_decodes_as_bytes(self):
        decoded = self.assert_value_roundtrips(bytearray(b"raw"), expected=b"raw")
        self.assertIsInstance(decoded, bytes)

    def test_aware_datetime_roundtrips_as_utc(self):
        value = datetime.datetime(2024, 6, 1, 12, 30, 45, tzinfo=datetime.UTC)
        decoded = self.assert_value_roundtrips(value)
        self.assertEqual(decoded.tzinfo, datetime.UTC)

    def test_datetime_truncates_to_whole_seconds(self):
        value = datetime.datetime(2024, 6, 1, 12, 30, 45, 999999, tzinfo=datetime.UTC)
        self.assert_value_roundtrips(value, expected=value.replace(microsecond=0))

    def test_none_roundtrips(self):
        self.assertEqual(self.encode_value(None), b"V")
        self.assertIsNone(self.assert_value_roundtrips(None))


class ContainerValueRoundtripTests(EncodingTestCase):
    """Nested dict, list, and tuple field-table values roundtrip."""

    def test_nested_dict_roundtrips(self):
        self.assert_value_roundtrips({"outer": {"inner": "value", "n": 1}})

    def test_list_roundtrips(self):
        self.assert_value_roundtrips([1, "two", True, None, b"x"])

    def test_tuple_decodes_as_list(self):
        self.assert_value_roundtrips((1, "two"), expected=[1, "two"])

    def test_empty_list_roundtrips(self):
        self.assertEqual(self.encode_value([]), b"A\x00\x00\x00\x00")
        self.assert_value_roundtrips([])


class DecodeTagTests(unittest.TestCase):
    """Every wire tag decodes, including the unsigned/float32 tags the encoder never emits."""

    KNOWN_TAG_CASES: ClassVar[list[tuple[bytes, object]]] = [
        (b"t\x01", True),
        (b"b\xfb", -5),
        (b"B\xfb", 251),
        (b"s\xff\x00", -256),
        (b"u\xff\x00", 65280),
        (b"I\xff\xff\xff\xff", -1),
        (b"i\xff\xff\xff\xff", 4294967295),
        (b"l\xff\xff\xff\xff\xff\xff\xff\xff", -1),
        (b"f\x3f\x80\x00\x00", 1.0),
        (b"d\x3f\xf0\x00\x00\x00\x00\x00\x00", 1.0),
        (b"D\x02\x00\x00\x01\x3a", Decimal("3.14")),
        (b"S\x00\x00\x00\x02hi", "hi"),
        (b"x\x00\x00\x00\x02hi", b"hi"),
        (b"T\x00\x00\x00\x00\x00\x00\x00\x00", datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)),
        (b"V", None),
        (b"F\x00\x00\x00\x00", {}),
        (b"A\x00\x00\x00\x00", []),
    ]

    def test_uint8_tag_decodes(self):
        self.assertEqual(encoding.decode_table_value(b"B\xff", 0), (255, 2))

    def test_uint16_tag_decodes(self):
        self.assertEqual(encoding.decode_table_value(b"u\xff\xff", 0), (65535, 3))

    def test_uint32_tag_decodes(self):
        self.assertEqual(encoding.decode_table_value(b"i\xff\xff\xff\xff", 0), (4294967295, 5))

    def test_float32_tag_decodes(self):
        self.assertEqual(encoding.decode_table_value(b"f\x40\x20\x00\x00", 0), (2.5, 5))

    def test_every_known_tag_decodes(self):
        self.assertEqual(len(self.KNOWN_TAG_CASES), 17, "one case per supported wire tag")
        for payload, expected in self.KNOWN_TAG_CASES:
            with self.subTest(tag=chr(payload[0])):
                decoded, offset = encoding.decode_table_value(payload, 0)
                self.assertEqual(decoded, expected)
                self.assertEqual(offset, len(payload))


class TableTests(unittest.TestCase):
    """encode_table / decode_table framing and full-table roundtrips."""

    def test_empty_table_encodes_as_four_zero_bytes(self):
        out = bytearray()
        encoding.encode_table(out, {})
        self.assertEqual(bytes(out), b"\x00\x00\x00\x00")

    def test_empty_table_roundtrips_empty_dict(self):
        self.assertEqual(encoding.decode_table(b"\x00\x00\x00\x00", 0), ({}, 4))

    def test_table_with_every_supported_type_roundtrips(self):
        table = {
            "bool": True,
            "int8": 5,
            "int64": 2**40,
            "float": 1.25,
            "decimal": Decimal("3.14"),
            "str": "héllo",
            "bytes": b"\x00\xff",
            "time": datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.UTC),
            "none": None,
            "nested": {"inner": [1, "two", None]},
            "tuple": (1, 2),
        }
        out = bytearray()
        encoding.encode_table(out, table)
        decoded, offset = encoding.decode_table(bytes(out), 0)
        self.assertEqual(offset, len(out))
        expected = dict(table, tuple=[1, 2])
        self.assertEqual(decoded, expected)

    def test_table_decode_returns_offset_past_table(self):
        out = bytearray()
        encoding.encode_table(out, {"a": 1})
        encoding.encode_table(out, {"b": 2})
        first, offset = encoding.decode_table(bytes(out), 0)
        second, end = encoding.decode_table(bytes(out), offset)
        self.assertEqual(first, {"a": 1})
        self.assertEqual(second, {"b": 2})
        self.assertEqual(end, len(out))

    def test_table_decodes_at_nonzero_offset(self):
        out = bytearray(b"\xde\xad")
        encoding.encode_table(out, {"key": "value"})
        self.assertEqual(encoding.decode_table(bytes(out), 2), ({"key": "value"}, len(out)))


class Priority(enum.IntEnum):
    LOW = 1
    HIGH = 9


class Label(enum.StrEnum):
    ORDERS = "orders"


class Tag(str):
    pass


class Stamp(datetime.datetime):
    pass


class SubclassValueTests(EncodingTestCase):
    """Values of subclasses fall back to the encoder of the nearest supported base type."""

    def test_int_subclass_encodes_as_its_base_int(self):
        payload = self.encode_value(Priority.HIGH)
        self.assertEqual(payload, b"b\x09")
        self.assertEqual(encoding.decode_table_value(payload, 0), (9, 2))

    def test_str_subclass_encodes_as_its_base_str(self):
        payload = self.encode_value(Tag("amq.direct"))
        self.assertEqual(payload[0:1], b"S")
        self.assertEqual(encoding.decode_table_value(payload, 0), ("amq.direct", len(payload)))

    def test_str_enum_member_encodes_its_string_value(self):
        payload = self.encode_value(Label.ORDERS)
        self.assertEqual(payload, b"S\x00\x00\x00\x06orders")

    def test_datetime_subclass_encodes_as_a_timestamp(self):
        value = Stamp(2024, 6, 1, 12, 30, 45, tzinfo=datetime.UTC)
        payload = self.encode_value(value)
        self.assertEqual(payload[0:1], b"T")
        decoded, offset = encoding.decode_table_value(payload, 0)
        self.assertEqual(offset, len(payload))
        self.assertEqual(decoded, datetime.datetime(2024, 6, 1, 12, 30, 45, tzinfo=datetime.UTC))

    def test_int_subclass_valued_one_encodes_as_an_int_and_not_as_a_bool(self):
        self.assertEqual(self.encode_value(Priority.LOW), b"b\x01")
        decoded = self.assert_value_roundtrips(Priority.LOW, expected=1)
        self.assertNotIsInstance(decoded, bool)
        self.assertEqual(self.encode_value(True), b"t\x01")
        self.assertIs(self.assert_value_roundtrips(True), True)

    def test_subclass_of_an_unsupported_type_raises_invalid_argument(self):
        class Basket(set):
            pass

        with self.assertRaises(exception.AMQPInvalidArgument) as ctx:
            encoding.encode_table_value(bytearray(), Basket())
        self.assertIn("unsupported field-table value type Basket", str(ctx.exception))
