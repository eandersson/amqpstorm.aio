import struct
from collections.abc import Callable

from amqpstorm_aio import encoding
from amqpstorm_aio import exception
from amqpstorm_aio import spec

PACK_FRAME_HEADER = struct.Struct(">BHI")
_UNPACK_FRAME_HEADER = PACK_FRAME_HEADER.unpack_from
_UNPACK_METHOD_INDEX = encoding.PACK_LONG.unpack_from
_UNPACK_CONTENT_HEADER = struct.Struct(">HHQ").unpack_from

_FRAME_HEADER_SIZE = 7
FRAME_OVERHEAD = _FRAME_HEADER_SIZE + 1

# frame header + class + weight + body size, then the properties the caller appends.
_PACK_CONTENT_HEADER_PREFIX = struct.Struct(">BHIHHQ").pack
# The same, plus empty property flags and the frame end: a complete 22-byte frame.
_PACK_EMPTY_CONTENT_HEADER = struct.Struct(">BHIHHQHB").pack
_EMPTY_HEADER_PAYLOAD = 14
_PACK_LONG_INTO = encoding.PACK_LONG.pack_into

PROTOCOL_HEADER = b"AMQP\x00\x00\x09\x01"
_PROTOCOL_NAME = PROTOCOL_HEADER[:4]
_CONTENT_HEADER_PREFIX = b"\x00\x3c\x00\x00"
_BASIC_CLASS_ID = 60
_CONTENT_HEADER_SIZE = 12

_FRAME_END = spec.FRAME_END
_FRAME_METHOD = spec.FRAME_METHOD
_FRAME_HEADER = spec.FRAME_HEADER
_FRAME_BODY = spec.FRAME_BODY
_FRAME_HEARTBEAT = spec.FRAME_HEARTBEAT
_METHOD_DECODERS = spec.METHOD_DECODERS


class ContentHeader:
    """Decoded content-header frame: body size + Basic properties."""

    __slots__ = ("body_size", "properties")

    def __init__(self, body_size: int, properties: spec.Basic.Properties) -> None:
        self.body_size = body_size
        self.properties = properties


class Heartbeat:
    """Marker type for a received heartbeat frame."""

    __slots__ = ()


HEARTBEAT = Heartbeat()
HEARTBEAT_FRAME = PACK_FRAME_HEADER.pack(_FRAME_HEARTBEAT, 0, 0) + bytes((_FRAME_END,))

type FrameValue = spec.Method | ContentHeader | bytes | Heartbeat
type DispatchCallable = Callable[[int, FrameValue], None]


def append_frame(
    out: bytearray, frame_type: int, channel_id: int, payload: bytes | bytearray | memoryview[int]
) -> None:
    out += PACK_FRAME_HEADER.pack(frame_type, channel_id, len(payload))
    out += payload
    out.append(_FRAME_END)


def append_method_frame(out: bytearray, channel_id: int, method: spec.Method) -> None:
    append_frame(out, _FRAME_METHOD, channel_id, method.encode())


def append_content_header_frame(
    out: bytearray, channel_id: int, body_size: int, properties: spec.Basic.Properties
) -> None:
    """Append a content-header frame, writing its length once the payload is known."""
    start = len(out)
    out += _PACK_CONTENT_HEADER_PREFIX(_FRAME_HEADER, channel_id, 0, _BASIC_CLASS_ID, 0, body_size)
    properties.encode(out)
    _PACK_LONG_INTO(out, start + 3, len(out) - start - _FRAME_HEADER_SIZE)
    out.append(_FRAME_END)


def append_empty_content_header_frame(out: bytearray, channel_id: int, body_size: int) -> None:
    """The whole frame for property-less content, in one pack.

    Fixed at 22 bytes, so it cannot exceed a negotiated frame_max, which the handshake
    floors at spec.FRAME_MIN_SIZE.
    """
    out += _PACK_EMPTY_CONTENT_HEADER(
        _FRAME_HEADER, channel_id, _EMPTY_HEADER_PAYLOAD, _BASIC_CLASS_ID, 0, body_size, 0,
        _FRAME_END,
    )


def _reject_on_protocol_mismatch(source: bytes | bytearray, buffered: int) -> None:
    if buffered < len(PROTOCOL_HEADER) or source[0] != _PROTOCOL_NAME[0]:
        return
    if source[: len(_PROTOCOL_NAME)] != _PROTOCOL_NAME:
        return
    raise exception.AMQPConnectionError(
        "broker rejected the protocol version: server speaks "
        f"AMQP {source[5]}.{source[6]}-{source[7]}"
    )


def _decode_method(payload: bytes) -> spec.Method:
    (index,) = _UNPACK_METHOD_INDEX(payload, 0)
    decoder = _METHOD_DECODERS.get(index)
    if decoder is None:
        raise exception.AMQPFrameError(
            f"unknown method (class {index >> 16}, method {index & 0xFFFF})"
        )
    return decoder(payload)


def _decode_content_header(payload: bytes) -> ContentHeader:
    class_id, _weight, body_size = _UNPACK_CONTENT_HEADER(payload, 0)
    if class_id != _BASIC_CLASS_ID:
        raise exception.AMQPFrameError(f"content header for unexpected class {class_id}")
    properties, _end = spec.Basic.Properties.decode(payload, _CONTENT_HEADER_SIZE)
    return ContentHeader(body_size, properties)


class FrameParser:
    __slots__ = ("_buffer", "_dispatch", "max_frame_size")

    def __init__(self, dispatch: DispatchCallable, max_frame_size: int) -> None:
        self._buffer = bytearray()
        self._dispatch = dispatch
        self.max_frame_size = max_frame_size

    def feed(self, data: bytes) -> None:
        buffer = self._buffer
        was_buffered = bool(buffer)
        if was_buffered:
            buffer += data
            source: bytes = bytes(buffer)
        else:
            source = data
        buffered = len(source)
        _reject_on_protocol_mismatch(source, buffered)
        offset = 0
        dispatch = self._dispatch
        try:
            while buffered - offset >= FRAME_OVERHEAD:
                frame_type, channel_id, payload_size = _UNPACK_FRAME_HEADER(source, offset)
                if payload_size > self.max_frame_size:
                    raise exception.AMQPFrameError(
                        f"frame of {payload_size} bytes exceeds negotiated maximum "
                        f"{self.max_frame_size}"
                    )
                frame_end = offset + _FRAME_HEADER_SIZE + payload_size
                if buffered < frame_end + 1:
                    break
                if source[frame_end] != _FRAME_END:
                    raise exception.AMQPFrameError(
                        f"missing frame-end marker (got 0x{source[frame_end]:02X})"
                    )
                payload = source[offset + _FRAME_HEADER_SIZE : frame_end]
                if frame_type == _FRAME_METHOD:
                    dispatch(channel_id, _decode_method(payload))
                elif frame_type == _FRAME_BODY:
                    dispatch(channel_id, payload)
                elif frame_type == _FRAME_HEADER:
                    dispatch(channel_id, _decode_content_header(payload))
                elif frame_type == _FRAME_HEARTBEAT:
                    dispatch(channel_id, HEARTBEAT)
                else:
                    raise exception.AMQPFrameError(f"unknown frame type {frame_type}")
                offset = frame_end + 1
        finally:
            self._keep_unparsed_tail(was_buffered, data, offset, buffered)

    def _keep_unparsed_tail(
        self, was_buffered: bool, data: bytes, offset: int, buffered: int
    ) -> None:
        """Leave exactly the unparsed bytes in the buffer.

        When buffering, the buffer already holds everything the parse saw, so only the
        consumed prefix goes; otherwise the buffer is empty and takes the leftover tail.
        """
        buffer = self._buffer
        if was_buffered:
            del buffer[:offset]
        elif offset < buffered:
            buffer += data[offset:]
