import datetime
import logging
import struct
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from amqpstorm_aio import exception

LOG = logging.getLogger(__name__)

PACK_OCTET = struct.Struct(">B")
PACK_SHORT = struct.Struct(">H")
PACK_SHORT_SIGNED = struct.Struct(">h")
PACK_LONG = struct.Struct(">I")
PACK_LONG_SIGNED = struct.Struct(">i")
PACK_LONG_LONG = struct.Struct(">Q")
PACK_LONG_LONG_SIGNED = struct.Struct(">q")
PACK_FLOAT = struct.Struct(">f")
PACK_DOUBLE = struct.Struct(">d")
PACK_SIGNED_BYTE = struct.Struct(">b")
PACK_DECIMAL = struct.Struct(">Bi")

_SHORTSTR_MAX_LEN = 255

MAX_TABLE_DEPTH = 100

_INT8_MIN, _INT8_MAX = -(2**7), 2**7 - 1
_INT16_MIN, _INT16_MAX = -(2**15), 2**15 - 1
_INT32_MIN, _INT32_MAX = -(2**31), 2**31 - 1
_INT64_MIN, _INT64_MAX = -(2**63), 2**63 - 1


def encode_shortstr(out: bytearray, value: str) -> None:
    """Append a short string: one length octet + at most 255 bytes of UTF-8.

    Encoded with surrogateescape so any octet sequence received off the wire
    round-trips back out unchanged; user input is validated before it gets here.
    """
    encoded = value.encode("utf-8", "surrogateescape")
    if len(encoded) > _SHORTSTR_MAX_LEN:
        raise exception.AMQPInvalidArgument(
            f"short string exceeds {_SHORTSTR_MAX_LEN} bytes: {value[:50]!r}..."
        )
    out.append(len(encoded))
    out += encoded


def encode_longstr(out: bytearray, value: bytes) -> None:
    """Append a long string: 32-bit length + raw bytes."""
    out += PACK_LONG.pack(len(value))
    out += value


def _encode_table_int(out: bytearray, value: int) -> None:
    if _INT8_MIN <= value <= _INT8_MAX:
        out += b"b"
        out += PACK_SIGNED_BYTE.pack(value)
    elif _INT16_MIN <= value <= _INT16_MAX:
        out += b"s"
        out += PACK_SHORT_SIGNED.pack(value)
    elif _INT32_MIN <= value <= _INT32_MAX:
        out += b"I"
        out += PACK_LONG_SIGNED.pack(value)
    elif _INT64_MIN <= value <= _INT64_MAX:
        out += b"l"
        out += PACK_LONG_LONG_SIGNED.pack(value)
    else:
        raise exception.AMQPInvalidArgument(f"integer out of 64-bit range: {value}")


def _encode_table_bool(out: bytearray, value: bool) -> None:
    out += b"t\x01" if value else b"t\x00"


def _encode_table_float(out: bytearray, value: float) -> None:
    out += b"d"
    out += PACK_DOUBLE.pack(value)


def _encode_table_str(out: bytearray, value: str) -> None:
    out += b"S"
    encode_longstr(out, value.encode("utf-8", "surrogateescape"))


def _encode_table_bytes(out: bytearray, value: bytes) -> None:
    out += b"x"
    encode_longstr(out, value)


def _encode_table_decimal(out: bytearray, value: Decimal) -> None:
    _, _, exponent = value.as_tuple()
    if not isinstance(exponent, int):
        raise exception.AMQPInvalidArgument(f"decimal not representable in AMQP: {value}")
    if exponent > 0:
        scale = 0
        unscaled = int(value)
    else:
        scale = -exponent
        unscaled = int(value.scaleb(scale))
    if scale > 255 or not _INT32_MIN <= unscaled <= _INT32_MAX:
        raise exception.AMQPInvalidArgument(f"decimal not representable in AMQP: {value}")
    out += b"D"
    out += PACK_DECIMAL.pack(scale, unscaled)


def encode_timestamp(out: bytearray, value: datetime.datetime) -> None:
    """Append a 64-bit POSIX timestamp."""
    posix = int(value.timestamp())
    if posix < 0:
        raise exception.AMQPInvalidArgument(f"pre-epoch datetime not representable: {value}")
    out += PACK_LONG_LONG.pack(posix)


def decode_timestamp(payload: bytes, offset: int) -> tuple[datetime.datetime | None, int]:
    """Read a 64-bit POSIX timestamp.

    The wire allows values datetime cannot hold; those decode as None rather than
    letting one poison message tear the connection down.
    """
    (posix,) = PACK_LONG_LONG.unpack_from(payload, offset)
    try:
        return datetime.datetime.fromtimestamp(posix, tz=datetime.UTC), offset + 8
    except OverflowError, OSError, ValueError:
        LOG.debug(f"discarding out-of-range timestamp {posix} from inbound message")
        return None, offset + 8


def _encode_table_datetime(out: bytearray, value: datetime.datetime) -> None:
    out += b"T"
    encode_timestamp(out, value)


def _encode_table_none(out: bytearray, value: None) -> None:
    out += b"V"


def _require_encode_depth(depth: int) -> None:
    if depth >= MAX_TABLE_DEPTH:
        raise exception.AMQPInvalidArgument(
            f"field table nested deeper than {MAX_TABLE_DEPTH} levels"
        )


_TABLE_ENCODERS_BY_TYPE: dict[type, Callable[[bytearray, Any], None]] = {
    bool: _encode_table_bool,
    int: _encode_table_int,
    float: _encode_table_float,
    str: _encode_table_str,
    bytes: _encode_table_bytes,
    bytearray: _encode_table_bytes,
    Decimal: _encode_table_decimal,
    datetime.datetime: _encode_table_datetime,
    type(None): _encode_table_none,
}


def encode_table_value(out: bytearray, value: Any, depth: int = 0) -> None:
    """Append one tagged field-table value."""
    if isinstance(value, dict):
        _require_encode_depth(depth)
        out += b"F"
        encode_table(out, value, depth + 1)
        return
    if isinstance(value, (list, tuple)):
        _require_encode_depth(depth)
        out += b"A"
        payload = bytearray()
        for item in value:
            encode_table_value(payload, item, depth + 1)
        encode_longstr(out, bytes(payload))
        return
    encoder = _TABLE_ENCODERS_BY_TYPE.get(type(value))
    if encoder is None:
        for base, candidate in _TABLE_ENCODERS_BY_TYPE.items():
            if isinstance(value, base):
                encoder = candidate
                break
        else:
            raise exception.AMQPInvalidArgument(
                f"unsupported field-table value type {type(value).__name__}: {value!r}"
            )
    encoder(out, value)


def encode_table(out: bytearray, table: dict[str, Any], depth: int = 0) -> None:
    """Append a field table: 32-bit byte-length prefix + shortstr/value pairs."""
    size_offset = len(out)
    out += b"\x00\x00\x00\x00"
    for key, value in table.items():
        if not isinstance(key, str):
            raise exception.AMQPInvalidArgument(
                f"field-table keys should be strings, got {type(key).__name__}: {key!r}"
            )
        encode_shortstr(out, key)
        encode_table_value(out, value, depth)
    PACK_LONG.pack_into(out, size_offset, len(out) - size_offset - 4)


def decode_shortstr(payload: bytes, offset: int) -> tuple[str, int]:
    """Read a short string; returns (value, next offset).

    Decoded with surrogateescape: the spec allows any octets here, and one binary
    routing key must not be able to take down the whole connection.
    """
    length = payload[offset]
    offset += 1
    end = offset + length
    if end > len(payload):
        raise exception.AMQPFrameError("short string extends past the end of the frame")
    return payload[offset:end].decode("utf-8", "surrogateescape"), end


def decode_longstr(payload: bytes, offset: int) -> tuple[bytes, int]:
    """Read a long string as raw bytes; returns (value, next offset)."""
    (length,) = PACK_LONG.unpack_from(payload, offset)
    offset += 4
    end = offset + length
    if end > len(payload):
        raise exception.AMQPFrameError("long string extends past the end of the frame")
    return payload[offset:end], end


def _decode_bool(payload: bytes, offset: int) -> tuple[bool, int]:
    return payload[offset] != 0, offset + 1


def _decode_int8(payload: bytes, offset: int) -> tuple[int, int]:
    return PACK_SIGNED_BYTE.unpack_from(payload, offset)[0], offset + 1


def _decode_uint8(payload: bytes, offset: int) -> tuple[int, int]:
    return payload[offset], offset + 1


def _decode_int16(payload: bytes, offset: int) -> tuple[int, int]:
    return PACK_SHORT_SIGNED.unpack_from(payload, offset)[0], offset + 2


def _decode_uint16(payload: bytes, offset: int) -> tuple[int, int]:
    return PACK_SHORT.unpack_from(payload, offset)[0], offset + 2


def _decode_int32(payload: bytes, offset: int) -> tuple[int, int]:
    return PACK_LONG_SIGNED.unpack_from(payload, offset)[0], offset + 4


def _decode_uint32(payload: bytes, offset: int) -> tuple[int, int]:
    return PACK_LONG.unpack_from(payload, offset)[0], offset + 4


def _decode_int64(payload: bytes, offset: int) -> tuple[int, int]:
    return PACK_LONG_LONG_SIGNED.unpack_from(payload, offset)[0], offset + 8


def _decode_float(payload: bytes, offset: int) -> tuple[float, int]:
    return PACK_FLOAT.unpack_from(payload, offset)[0], offset + 4


def _decode_double(payload: bytes, offset: int) -> tuple[float, int]:
    return PACK_DOUBLE.unpack_from(payload, offset)[0], offset + 8


def _decode_decimal(payload: bytes, offset: int) -> tuple[Decimal, int]:
    scale, unscaled = PACK_DECIMAL.unpack_from(payload, offset)
    return Decimal(unscaled).scaleb(-scale), offset + 5


def _decode_longstr_value(payload: bytes, offset: int) -> tuple[str, int]:
    raw, offset = decode_longstr(payload, offset)
    return raw.decode("utf-8", errors="surrogateescape"), offset


def _decode_bytes_value(payload: bytes, offset: int) -> tuple[bytes, int]:
    return decode_longstr(payload, offset)


def _decode_timestamp(payload: bytes, offset: int) -> tuple[datetime.datetime | None, int]:
    return decode_timestamp(payload, offset)


def _decode_void(payload: bytes, offset: int) -> tuple[None, int]:
    return None, offset


def _require_decode_depth(depth: int) -> None:
    if depth >= MAX_TABLE_DEPTH:
        raise exception.AMQPFrameError(
            f"field table nested deeper than {MAX_TABLE_DEPTH} levels"
        )


def _decode_array(payload: bytes, offset: int, depth: int) -> tuple[list[Any], int]:
    (length,) = PACK_LONG.unpack_from(payload, offset)
    offset += 4
    end = offset + length
    values: list[Any] = []
    while offset < end:
        value, offset = decode_table_value(payload, offset, depth)
        values.append(value)
    return values, end


_TAG_NESTED_TABLE = ord("F")
_TAG_ARRAY = ord("A")

_TABLE_DECODERS_BY_TAG: dict[int, Callable[[bytes, int], tuple[Any, int]]] = {
    ord("t"): _decode_bool,
    ord("b"): _decode_int8,
    ord("B"): _decode_uint8,
    ord("s"): _decode_int16,
    ord("u"): _decode_uint16,
    ord("I"): _decode_int32,
    ord("i"): _decode_uint32,
    ord("l"): _decode_int64,
    ord("f"): _decode_float,
    ord("d"): _decode_double,
    ord("D"): _decode_decimal,
    ord("S"): _decode_longstr_value,
    ord("x"): _decode_bytes_value,
    ord("T"): _decode_timestamp,
    ord("V"): _decode_void,
}


def decode_table_value(payload: bytes, offset: int, depth: int = 0) -> tuple[Any, int]:
    """Read one tagged field-table value; returns (value, next offset)."""
    tag = payload[offset]
    if tag == _TAG_NESTED_TABLE:
        _require_decode_depth(depth)
        return decode_table(payload, offset + 1, depth + 1)
    if tag == _TAG_ARRAY:
        _require_decode_depth(depth)
        return _decode_array(payload, offset + 1, depth + 1)
    decoder = _TABLE_DECODERS_BY_TAG.get(tag)
    if decoder is None:
        raise exception.AMQPFrameError(f"unknown field-table value tag {chr(tag)!r}")
    return decoder(payload, offset + 1)


def decode_table(payload: bytes, offset: int, depth: int = 0) -> tuple[dict[str, Any], int]:
    """Read a field table; returns (dict, next offset)."""
    (length,) = PACK_LONG.unpack_from(payload, offset)
    offset += 4
    end = offset + length
    table: dict[str, Any] = {}
    while offset < end:
        key, offset = decode_shortstr(payload, offset)
        table[key], offset = decode_table_value(payload, offset, depth)
    return table, end
