"""AMQP 0-9-1 protocol definitions (RabbitMQ extended dialect).

GENERATED FILE -- DO NOT EDIT BY HAND.
Source: codegen/amqp0-9-1.extended.xml;
regenerate with: python codegen/generate.py
"""

import datetime
from collections.abc import Callable
from typing import Any
from typing import ClassVar
from typing import TypedDict

from amqpstorm_aio import encoding

_PACK_SHORT = encoding.PACK_SHORT.pack
_PACK_LONG = encoding.PACK_LONG.pack
_PACK_LONG_LONG = encoding.PACK_LONG_LONG.pack
_PACK_SHORT_INTO = encoding.PACK_SHORT.pack_into
_UNPACK_SHORT = encoding.PACK_SHORT.unpack_from
_UNPACK_LONG = encoding.PACK_LONG.unpack_from
_UNPACK_LONG_LONG = encoding.PACK_LONG_LONG.unpack_from
_ENCODE_SHORTSTR = encoding.encode_shortstr
_ENCODE_LONGSTR = encoding.encode_longstr
_ENCODE_TABLE = encoding.encode_table
_ENCODE_TIMESTAMP = encoding.encode_timestamp
_DECODE_SHORTSTR = encoding.decode_shortstr
_DECODE_LONGSTR = encoding.decode_longstr
_DECODE_TABLE = encoding.decode_table
_DECODE_TIMESTAMP = encoding.decode_timestamp

FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_HEARTBEAT = 8
FRAME_MIN_SIZE = 4096
FRAME_END = 206
REPLY_SUCCESS = 200
CONTENT_TOO_LARGE = 311
NO_ROUTE = 312
NO_CONSUMERS = 313
CONNECTION_FORCED = 320
INVALID_PATH = 402
ACCESS_REFUSED = 403
NOT_FOUND = 404
RESOURCE_LOCKED = 405
PRECONDITION_FAILED = 406
FRAME_ERROR = 501
SYNTAX_ERROR = 502
COMMAND_INVALID = 503
CHANNEL_ERROR = 504
UNEXPECTED_FRAME = 505
RESOURCE_ERROR = 506
NOT_ALLOWED = 530
NOT_IMPLEMENTED = 540
INTERNAL_ERROR = 541

# Soft errors close the channel; hard errors close the whole connection.
SOFT_ERROR_CODES = frozenset([311, 312, 313, 403, 404, 405, 406])
HARD_ERROR_CODES = frozenset([320, 402, 501, 502, 503, 504, 505, 506, 530, 540, 541])

# Constraints declared by <assert> in the spec XML, verbatim.
DOMAIN_ASSERTS: dict[str, tuple[tuple[str, str | None], ...]] = {
    'exchange-name': (('length', '127'), ('regexp', '^[a-zA-Z0-9-_.:]*$')),
    'path': (('notnull', None), ('length', '127')),
    'queue-name': (('length', '127'), ('regexp', '^[a-zA-Z0-9-_.:]*$')),
    'reply-code': (('notnull', None),),
    'reply-text': (('notnull', None),),
}

# Per-method field asserts, keyed by the generated method name.
FIELD_ASSERTS: dict[str, tuple[tuple[str, str, str | None], ...]] = {
    'Connection.Start': (('mechanisms', 'notnull', None), ('locales', 'notnull', None)),
    'Connection.StartOk': (('mechanism', 'notnull', None), ('response', 'notnull', None), ('locale', 'notnull', None)),
    'Connection.SecureOk': (('response', 'notnull', None),),
    'Connection.TuneOk': (('channel-max', 'notnull', None), ('channel-max', 'le', 'tune.channel-max')),
    'Exchange.Declare': (('exchange', 'notnull', None),),
    'Exchange.Delete': (('exchange', 'notnull', None),),
    'Queue.DeclareOk': (('queue', 'notnull', None),),
}

# Which constrained domain each method field draws from.
FIELD_DOMAINS: dict[str, tuple[tuple[str, str], ...]] = {
    'Connection.Open': (('virtual-host', 'path'),),
    'Connection.Close': (('reply-code', 'reply-code'), ('reply-text', 'reply-text')),
    'Channel.Close': (('reply-code', 'reply-code'), ('reply-text', 'reply-text')),
    'Exchange.Declare': (('exchange', 'exchange-name'),),
    'Exchange.Delete': (('exchange', 'exchange-name'),),
    'Exchange.Bind': (('destination', 'exchange-name'), ('source', 'exchange-name')),
    'Exchange.Unbind': (('destination', 'exchange-name'), ('source', 'exchange-name')),
    'Queue.Declare': (('queue', 'queue-name'),),
    'Queue.DeclareOk': (('queue', 'queue-name'),),
    'Queue.Bind': (('queue', 'queue-name'), ('exchange', 'exchange-name')),
    'Queue.Unbind': (('queue', 'queue-name'), ('exchange', 'exchange-name')),
    'Queue.Purge': (('queue', 'queue-name'),),
    'Queue.Delete': (('queue', 'queue-name'),),
    'Basic.Consume': (('queue', 'queue-name'),),
    'Basic.Publish': (('exchange', 'exchange-name'),),
    'Basic.Return': (('reply-code', 'reply-code'), ('reply-text', 'reply-text'), ('exchange', 'exchange-name')),
    'Basic.Deliver': (('exchange', 'exchange-name'),),
    'Basic.Get': (('queue', 'queue-name'),),
    'Basic.GetOk': (('exchange', 'exchange-name'),),
}


class PropertiesDict(TypedDict, total=False):
    """Basic content properties as a mapping; every key is optional."""

    content_type: str
    content_encoding: str
    headers: dict[str, Any]
    delivery_mode: int
    priority: int
    correlation_id: str
    reply_to: str
    expiration: str
    message_id: str
    timestamp: datetime.datetime
    type: str
    user_id: str
    app_id: str


class Method:
    """Base of every generated method class."""

    __slots__: ClassVar[tuple[str, ...]] = ()

    index: ClassVar[int] = 0
    name: ClassVar[str] = ""
    synchronous: ClassVar[bool] = False
    has_content: ClassVar[bool] = False
    valid_responses: ClassVar[tuple[int, ...]] = ()

    def encode(self) -> bytearray:
        """Return the method-frame payload (class + method + arguments)."""
        raise NotImplementedError

    def __repr__(self) -> str:
        fields = ', '.join(f'{n}={getattr(self, n)!r}' for n in self.__slots__)
        return f'{self.name}({fields})'

    def __eq__(self, other: object) -> bool:
        if type(other) is not type(self):
            return NotImplemented
        return all(getattr(self, n) == getattr(other, n) for n in self.__slots__)


class Connection:
    """Methods of the connection class (index 10)."""

    class Start(Method):
        """connection.start"""

        __slots__ = ("version_major", "version_minor", "server_properties", "mechanisms", "locales")

        index = 0x000A000A  # class 10, method 10
        name = "Connection.Start"
        synchronous = True
        has_content = False
        valid_responses = (0x000A000B,)

        def __init__(
            self,
            *,
            version_major: int,
            version_minor: int,
            server_properties: dict[str, Any] | None,
            mechanisms: bytes,
            locales: bytes,
        ) -> None:
            self.version_major = version_major
            self.version_minor = version_minor
            self.server_properties = server_properties
            self.mechanisms = mechanisms
            self.locales = locales

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00\n')
            out.append(self.version_major)
            out.append(self.version_minor)
            if self.server_properties:
                _ENCODE_TABLE(out, self.server_properties)
            else:
                out += b"\x00\x00\x00\x00"
            _ENCODE_LONGSTR(out, self.mechanisms)
            _ENCODE_LONGSTR(out, self.locales)
            return out

    class StartOk(Method):
        """connection.start-ok"""

        __slots__ = ("client_properties", "mechanism", "response", "locale")

        index = 0x000A000B  # class 10, method 11
        name = "Connection.StartOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            client_properties: dict[str, Any] | None,
            mechanism: str,
            response: bytes,
            locale: str,
        ) -> None:
            self.client_properties = client_properties
            self.mechanism = mechanism
            self.response = response
            self.locale = locale

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00\x0b')
            if self.client_properties:
                _ENCODE_TABLE(out, self.client_properties)
            else:
                out += b"\x00\x00\x00\x00"
            _ENCODE_SHORTSTR(out, self.mechanism)
            _ENCODE_LONGSTR(out, self.response)
            _ENCODE_SHORTSTR(out, self.locale)
            return out

    class Secure(Method):
        """connection.secure"""

        __slots__ = ("challenge",)

        index = 0x000A0014  # class 10, method 20
        name = "Connection.Secure"
        synchronous = True
        has_content = False
        valid_responses = (0x000A0015,)

        def __init__(
            self,
            *,
            challenge: bytes,
        ) -> None:
            self.challenge = challenge

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00\x14')
            _ENCODE_LONGSTR(out, self.challenge)
            return out

    class SecureOk(Method):
        """connection.secure-ok"""

        __slots__ = ("response",)

        index = 0x000A0015  # class 10, method 21
        name = "Connection.SecureOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            response: bytes,
        ) -> None:
            self.response = response

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00\x15')
            _ENCODE_LONGSTR(out, self.response)
            return out

    class Tune(Method):
        """connection.tune"""

        __slots__ = ("channel_max", "frame_max", "heartbeat")

        index = 0x000A001E  # class 10, method 30
        name = "Connection.Tune"
        synchronous = True
        has_content = False
        valid_responses = (0x000A001F,)

        def __init__(
            self,
            *,
            channel_max: int,
            frame_max: int,
            heartbeat: int,
        ) -> None:
            self.channel_max = channel_max
            self.frame_max = frame_max
            self.heartbeat = heartbeat

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00\x1e')
            out += _PACK_SHORT(self.channel_max)
            out += _PACK_LONG(self.frame_max)
            out += _PACK_SHORT(self.heartbeat)
            return out

    class TuneOk(Method):
        """connection.tune-ok"""

        __slots__ = ("channel_max", "frame_max", "heartbeat")

        index = 0x000A001F  # class 10, method 31
        name = "Connection.TuneOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            channel_max: int,
            frame_max: int,
            heartbeat: int,
        ) -> None:
            self.channel_max = channel_max
            self.frame_max = frame_max
            self.heartbeat = heartbeat

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00\x1f')
            out += _PACK_SHORT(self.channel_max)
            out += _PACK_LONG(self.frame_max)
            out += _PACK_SHORT(self.heartbeat)
            return out

    class Open(Method):
        """connection.open"""

        __slots__ = ("virtual_host",)

        index = 0x000A0028  # class 10, method 40
        name = "Connection.Open"
        synchronous = True
        has_content = False
        valid_responses = (0x000A0029,)

        def __init__(
            self,
            *,
            virtual_host: str,
        ) -> None:
            self.virtual_host = virtual_host

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00(')
            _ENCODE_SHORTSTR(out, self.virtual_host)
            out += b'\x00'
            out += b"\x00"
            return out

    class OpenOk(Method):
        """connection.open-ok"""

        __slots__ = ()

        index = 0x000A0029  # class 10, method 41
        name = "Connection.OpenOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00)\x00')
            return out

    class Close(Method):
        """connection.close"""

        __slots__ = ("reply_code", "reply_text", "class_id", "method_id")

        index = 0x000A0032  # class 10, method 50
        name = "Connection.Close"
        synchronous = True
        has_content = False
        valid_responses = (0x000A0033,)

        def __init__(
            self,
            *,
            reply_code: int,
            reply_text: str,
            class_id: int,
            method_id: int,
        ) -> None:
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x002')
            out += _PACK_SHORT(self.reply_code)
            _ENCODE_SHORTSTR(out, self.reply_text)
            out += _PACK_SHORT(self.class_id)
            out += _PACK_SHORT(self.method_id)
            return out

    class CloseOk(Method):
        """connection.close-ok"""

        __slots__ = ()

        index = 0x000A0033  # class 10, method 51
        name = "Connection.CloseOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x003')
            return out

    class Blocked(Method):
        """connection.blocked"""

        __slots__ = ("reason",)

        index = 0x000A003C  # class 10, method 60
        name = "Connection.Blocked"
        synchronous = False
        has_content = False

        def __init__(
            self,
            *,
            reason: str,
        ) -> None:
            self.reason = reason

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00<')
            _ENCODE_SHORTSTR(out, self.reason)
            return out

    class Unblocked(Method):
        """connection.unblocked"""

        __slots__ = ()

        index = 0x000A003D  # class 10, method 61
        name = "Connection.Unblocked"
        synchronous = False
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00=')
            return out

    class UpdateSecret(Method):
        """connection.update-secret"""

        __slots__ = ("new_secret", "reason")

        index = 0x000A0046  # class 10, method 70
        name = "Connection.UpdateSecret"
        synchronous = True
        has_content = False
        valid_responses = (0x000A0047,)

        def __init__(
            self,
            *,
            new_secret: bytes,
            reason: str,
        ) -> None:
            self.new_secret = new_secret
            self.reason = reason

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00F')
            _ENCODE_LONGSTR(out, self.new_secret)
            _ENCODE_SHORTSTR(out, self.reason)
            return out

    class UpdateSecretOk(Method):
        """connection.update-secret-ok"""

        __slots__ = ()

        index = 0x000A0047  # class 10, method 71
        name = "Connection.UpdateSecretOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\n\x00G')
            return out


class Channel:
    """Methods of the channel class (index 20)."""

    class Open(Method):
        """channel.open"""

        __slots__ = ()

        index = 0x0014000A  # class 20, method 10
        name = "Channel.Open"
        synchronous = True
        has_content = False
        valid_responses = (0x0014000B,)

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\x14\x00\n\x00')
            return out

    class OpenOk(Method):
        """channel.open-ok"""

        __slots__ = ()

        index = 0x0014000B  # class 20, method 11
        name = "Channel.OpenOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\x14\x00\x0b')
            out += b'\x00\x00\x00\x00'
            return out

    class Flow(Method):
        """channel.flow"""

        __slots__ = ("active",)

        index = 0x00140014  # class 20, method 20
        name = "Channel.Flow"
        synchronous = True
        has_content = False
        valid_responses = (0x00140015,)

        def __init__(
            self,
            *,
            active: bool,
        ) -> None:
            self.active = active

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\x14\x00\x14')
            bits = 0
            if self.active:
                bits |= 1
            out.append(bits)
            return out

    class FlowOk(Method):
        """channel.flow-ok"""

        __slots__ = ("active",)

        index = 0x00140015  # class 20, method 21
        name = "Channel.FlowOk"
        synchronous = False
        has_content = False

        def __init__(
            self,
            *,
            active: bool,
        ) -> None:
            self.active = active

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\x14\x00\x15')
            bits = 0
            if self.active:
                bits |= 1
            out.append(bits)
            return out

    class Close(Method):
        """channel.close"""

        __slots__ = ("reply_code", "reply_text", "class_id", "method_id")

        index = 0x00140028  # class 20, method 40
        name = "Channel.Close"
        synchronous = True
        has_content = False
        valid_responses = (0x00140029,)

        def __init__(
            self,
            *,
            reply_code: int,
            reply_text: str,
            class_id: int,
            method_id: int,
        ) -> None:
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.class_id = class_id
            self.method_id = method_id

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\x14\x00(')
            out += _PACK_SHORT(self.reply_code)
            _ENCODE_SHORTSTR(out, self.reply_text)
            out += _PACK_SHORT(self.class_id)
            out += _PACK_SHORT(self.method_id)
            return out

    class CloseOk(Method):
        """channel.close-ok"""

        __slots__ = ()

        index = 0x00140029  # class 20, method 41
        name = "Channel.CloseOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00\x14\x00)')
            return out


class Exchange:
    """Methods of the exchange class (index 40)."""

    class Declare(Method):
        """exchange.declare"""

        __slots__ = ("exchange", "type", "passive", "durable", "auto_delete", "internal", "no_wait", "arguments")

        index = 0x0028000A  # class 40, method 10
        name = "Exchange.Declare"
        synchronous = True
        has_content = False
        valid_responses = (0x0028000B,)

        def __init__(
            self,
            *,
            exchange: str,
            type: str,
            passive: bool,
            durable: bool,
            auto_delete: bool,
            internal: bool,
            no_wait: bool,
            arguments: dict[str, Any] | None,
        ) -> None:
            self.exchange = exchange
            self.type = type
            self.passive = passive
            self.durable = durable
            self.auto_delete = auto_delete
            self.internal = internal
            self.no_wait = no_wait
            self.arguments = arguments

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x00\n\x00\x00')
            _ENCODE_SHORTSTR(out, self.exchange)
            _ENCODE_SHORTSTR(out, self.type)
            bits = 0
            if self.passive:
                bits |= 1
            if self.durable:
                bits |= 2
            if self.auto_delete:
                bits |= 4
            if self.internal:
                bits |= 8
            if self.no_wait:
                bits |= 16
            out.append(bits)
            if self.arguments:
                _ENCODE_TABLE(out, self.arguments)
            else:
                out += b"\x00\x00\x00\x00"
            return out

    class DeclareOk(Method):
        """exchange.declare-ok"""

        __slots__ = ()

        index = 0x0028000B  # class 40, method 11
        name = "Exchange.DeclareOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x00\x0b')
            return out

    class Delete(Method):
        """exchange.delete"""

        __slots__ = ("exchange", "if_unused", "no_wait")

        index = 0x00280014  # class 40, method 20
        name = "Exchange.Delete"
        synchronous = True
        has_content = False
        valid_responses = (0x00280015,)

        def __init__(
            self,
            *,
            exchange: str,
            if_unused: bool,
            no_wait: bool,
        ) -> None:
            self.exchange = exchange
            self.if_unused = if_unused
            self.no_wait = no_wait

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x00\x14\x00\x00')
            _ENCODE_SHORTSTR(out, self.exchange)
            bits = 0
            if self.if_unused:
                bits |= 1
            if self.no_wait:
                bits |= 2
            out.append(bits)
            return out

    class DeleteOk(Method):
        """exchange.delete-ok"""

        __slots__ = ()

        index = 0x00280015  # class 40, method 21
        name = "Exchange.DeleteOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x00\x15')
            return out

    class Bind(Method):
        """exchange.bind"""

        __slots__ = ("destination", "source", "routing_key", "no_wait", "arguments")

        index = 0x0028001E  # class 40, method 30
        name = "Exchange.Bind"
        synchronous = True
        has_content = False
        valid_responses = (0x0028001F,)

        def __init__(
            self,
            *,
            destination: str,
            source: str,
            routing_key: str,
            no_wait: bool,
            arguments: dict[str, Any] | None,
        ) -> None:
            self.destination = destination
            self.source = source
            self.routing_key = routing_key
            self.no_wait = no_wait
            self.arguments = arguments

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x00\x1e\x00\x00')
            _ENCODE_SHORTSTR(out, self.destination)
            _ENCODE_SHORTSTR(out, self.source)
            _ENCODE_SHORTSTR(out, self.routing_key)
            bits = 0
            if self.no_wait:
                bits |= 1
            out.append(bits)
            if self.arguments:
                _ENCODE_TABLE(out, self.arguments)
            else:
                out += b"\x00\x00\x00\x00"
            return out

    class BindOk(Method):
        """exchange.bind-ok"""

        __slots__ = ()

        index = 0x0028001F  # class 40, method 31
        name = "Exchange.BindOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x00\x1f')
            return out

    class Unbind(Method):
        """exchange.unbind"""

        __slots__ = ("destination", "source", "routing_key", "no_wait", "arguments")

        index = 0x00280028  # class 40, method 40
        name = "Exchange.Unbind"
        synchronous = True
        has_content = False
        valid_responses = (0x00280033,)

        def __init__(
            self,
            *,
            destination: str,
            source: str,
            routing_key: str,
            no_wait: bool,
            arguments: dict[str, Any] | None,
        ) -> None:
            self.destination = destination
            self.source = source
            self.routing_key = routing_key
            self.no_wait = no_wait
            self.arguments = arguments

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x00(\x00\x00')
            _ENCODE_SHORTSTR(out, self.destination)
            _ENCODE_SHORTSTR(out, self.source)
            _ENCODE_SHORTSTR(out, self.routing_key)
            bits = 0
            if self.no_wait:
                bits |= 1
            out.append(bits)
            if self.arguments:
                _ENCODE_TABLE(out, self.arguments)
            else:
                out += b"\x00\x00\x00\x00"
            return out

    class UnbindOk(Method):
        """exchange.unbind-ok"""

        __slots__ = ()

        index = 0x00280033  # class 40, method 51
        name = "Exchange.UnbindOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00(\x003')
            return out


class Queue:
    """Methods of the queue class (index 50)."""

    class Declare(Method):
        """queue.declare"""

        __slots__ = ("queue", "passive", "durable", "exclusive", "auto_delete", "no_wait", "arguments")

        index = 0x0032000A  # class 50, method 10
        name = "Queue.Declare"
        synchronous = True
        has_content = False
        valid_responses = (0x0032000B,)

        def __init__(
            self,
            *,
            queue: str,
            passive: bool,
            durable: bool,
            exclusive: bool,
            auto_delete: bool,
            no_wait: bool,
            arguments: dict[str, Any] | None,
        ) -> None:
            self.queue = queue
            self.passive = passive
            self.durable = durable
            self.exclusive = exclusive
            self.auto_delete = auto_delete
            self.no_wait = no_wait
            self.arguments = arguments

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00\n\x00\x00')
            _ENCODE_SHORTSTR(out, self.queue)
            bits = 0
            if self.passive:
                bits |= 1
            if self.durable:
                bits |= 2
            if self.exclusive:
                bits |= 4
            if self.auto_delete:
                bits |= 8
            if self.no_wait:
                bits |= 16
            out.append(bits)
            if self.arguments:
                _ENCODE_TABLE(out, self.arguments)
            else:
                out += b"\x00\x00\x00\x00"
            return out

    class DeclareOk(Method):
        """queue.declare-ok"""

        __slots__ = ("queue", "message_count", "consumer_count")

        index = 0x0032000B  # class 50, method 11
        name = "Queue.DeclareOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            queue: str,
            message_count: int,
            consumer_count: int,
        ) -> None:
            self.queue = queue
            self.message_count = message_count
            self.consumer_count = consumer_count

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00\x0b')
            _ENCODE_SHORTSTR(out, self.queue)
            out += _PACK_LONG(self.message_count)
            out += _PACK_LONG(self.consumer_count)
            return out

    class Bind(Method):
        """queue.bind"""

        __slots__ = ("queue", "exchange", "routing_key", "no_wait", "arguments")

        index = 0x00320014  # class 50, method 20
        name = "Queue.Bind"
        synchronous = True
        has_content = False
        valid_responses = (0x00320015,)

        def __init__(
            self,
            *,
            queue: str,
            exchange: str,
            routing_key: str,
            no_wait: bool,
            arguments: dict[str, Any] | None,
        ) -> None:
            self.queue = queue
            self.exchange = exchange
            self.routing_key = routing_key
            self.no_wait = no_wait
            self.arguments = arguments

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00\x14\x00\x00')
            _ENCODE_SHORTSTR(out, self.queue)
            _ENCODE_SHORTSTR(out, self.exchange)
            _ENCODE_SHORTSTR(out, self.routing_key)
            bits = 0
            if self.no_wait:
                bits |= 1
            out.append(bits)
            if self.arguments:
                _ENCODE_TABLE(out, self.arguments)
            else:
                out += b"\x00\x00\x00\x00"
            return out

    class BindOk(Method):
        """queue.bind-ok"""

        __slots__ = ()

        index = 0x00320015  # class 50, method 21
        name = "Queue.BindOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00\x15')
            return out

    class Unbind(Method):
        """queue.unbind"""

        __slots__ = ("queue", "exchange", "routing_key", "arguments")

        index = 0x00320032  # class 50, method 50
        name = "Queue.Unbind"
        synchronous = True
        has_content = False
        valid_responses = (0x00320033,)

        def __init__(
            self,
            *,
            queue: str,
            exchange: str,
            routing_key: str,
            arguments: dict[str, Any] | None,
        ) -> None:
            self.queue = queue
            self.exchange = exchange
            self.routing_key = routing_key
            self.arguments = arguments

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x002\x00\x00')
            _ENCODE_SHORTSTR(out, self.queue)
            _ENCODE_SHORTSTR(out, self.exchange)
            _ENCODE_SHORTSTR(out, self.routing_key)
            if self.arguments:
                _ENCODE_TABLE(out, self.arguments)
            else:
                out += b"\x00\x00\x00\x00"
            return out

    class UnbindOk(Method):
        """queue.unbind-ok"""

        __slots__ = ()

        index = 0x00320033  # class 50, method 51
        name = "Queue.UnbindOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x003')
            return out

    class Purge(Method):
        """queue.purge"""

        __slots__ = ("queue", "no_wait")

        index = 0x0032001E  # class 50, method 30
        name = "Queue.Purge"
        synchronous = True
        has_content = False
        valid_responses = (0x0032001F,)

        def __init__(
            self,
            *,
            queue: str,
            no_wait: bool,
        ) -> None:
            self.queue = queue
            self.no_wait = no_wait

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00\x1e\x00\x00')
            _ENCODE_SHORTSTR(out, self.queue)
            bits = 0
            if self.no_wait:
                bits |= 1
            out.append(bits)
            return out

    class PurgeOk(Method):
        """queue.purge-ok"""

        __slots__ = ("message_count",)

        index = 0x0032001F  # class 50, method 31
        name = "Queue.PurgeOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            message_count: int,
        ) -> None:
            self.message_count = message_count

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00\x1f')
            out += _PACK_LONG(self.message_count)
            return out

    class Delete(Method):
        """queue.delete"""

        __slots__ = ("queue", "if_unused", "if_empty", "no_wait")

        index = 0x00320028  # class 50, method 40
        name = "Queue.Delete"
        synchronous = True
        has_content = False
        valid_responses = (0x00320029,)

        def __init__(
            self,
            *,
            queue: str,
            if_unused: bool,
            if_empty: bool,
            no_wait: bool,
        ) -> None:
            self.queue = queue
            self.if_unused = if_unused
            self.if_empty = if_empty
            self.no_wait = no_wait

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00(\x00\x00')
            _ENCODE_SHORTSTR(out, self.queue)
            bits = 0
            if self.if_unused:
                bits |= 1
            if self.if_empty:
                bits |= 2
            if self.no_wait:
                bits |= 4
            out.append(bits)
            return out

    class DeleteOk(Method):
        """queue.delete-ok"""

        __slots__ = ("message_count",)

        index = 0x00320029  # class 50, method 41
        name = "Queue.DeleteOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            message_count: int,
        ) -> None:
            self.message_count = message_count

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x002\x00)')
            out += _PACK_LONG(self.message_count)
            return out


class Basic:
    """Methods of the basic class (index 60)."""

    class Properties:
        """Basic content-header properties; unset fields stay ``None``."""

        __slots__ = ("content_type", "content_encoding", "headers", "delivery_mode", "priority", "correlation_id", "reply_to", "expiration", "message_id", "timestamp", "type", "user_id", "app_id")

        def __init__(
            self,
            *,
            content_type: str | None = None,
            content_encoding: str | None = None,
            headers: dict[str, Any] | None = None,
            delivery_mode: int | None = None,
            priority: int | None = None,
            correlation_id: str | None = None,
            reply_to: str | None = None,
            expiration: str | None = None,
            message_id: str | None = None,
            timestamp: datetime.datetime | None = None,
            type: str | None = None,
            user_id: str | None = None,
            app_id: str | None = None,
        ) -> None:
            self.content_type = content_type
            self.content_encoding = content_encoding
            self.headers = headers
            self.delivery_mode = delivery_mode
            self.priority = priority
            self.correlation_id = correlation_id
            self.reply_to = reply_to
            self.expiration = expiration
            self.message_id = message_id
            self.timestamp = timestamp
            self.type = type
            self.user_id = user_id
            self.app_id = app_id

        def __repr__(self) -> str:
            fields = ', '.join(
                f'{name}={getattr(self, name)!r}'
                for name in self.__slots__
                if getattr(self, name) is not None
            )
            return f'Properties({fields})'

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, Basic.Properties):
                return NotImplemented
            return all(getattr(self, n) == getattr(other, n) for n in self.__slots__)

        def encode(self, out: bytearray) -> None:
            """Append property flags + the set fields onto ``out``."""
            flags = 0
            flags_offset = len(out)
            out += b"\x00\x00"
            if self.content_type is not None:
                flags |= 0x8000
                _ENCODE_SHORTSTR(out, self.content_type)
            if self.content_encoding is not None:
                flags |= 0x4000
                _ENCODE_SHORTSTR(out, self.content_encoding)
            if self.headers is not None:
                flags |= 0x2000
                _ENCODE_TABLE(out, self.headers)
            if self.delivery_mode is not None:
                flags |= 0x1000
                out.append(self.delivery_mode)
            if self.priority is not None:
                flags |= 0x0800
                out.append(self.priority)
            if self.correlation_id is not None:
                flags |= 0x0400
                _ENCODE_SHORTSTR(out, self.correlation_id)
            if self.reply_to is not None:
                flags |= 0x0200
                _ENCODE_SHORTSTR(out, self.reply_to)
            if self.expiration is not None:
                flags |= 0x0100
                _ENCODE_SHORTSTR(out, self.expiration)
            if self.message_id is not None:
                flags |= 0x0080
                _ENCODE_SHORTSTR(out, self.message_id)
            if self.timestamp is not None:
                flags |= 0x0040
                _ENCODE_TIMESTAMP(out, self.timestamp)
            if self.type is not None:
                flags |= 0x0020
                _ENCODE_SHORTSTR(out, self.type)
            if self.user_id is not None:
                flags |= 0x0010
                _ENCODE_SHORTSTR(out, self.user_id)
            if self.app_id is not None:
                flags |= 0x0008
                _ENCODE_SHORTSTR(out, self.app_id)
            _PACK_SHORT_INTO(out, flags_offset, flags)

        @staticmethod
        def decode(payload: bytes, offset: int) -> tuple["Basic.Properties", int]:
            """Read flags + announced fields; returns (properties, next offset)."""
            (flags,) = _UNPACK_SHORT(payload, offset)
            offset += 2
            # Bit 0 announces another (legal but unused) flags word.
            more_flags = flags
            while more_flags & 0x0001:
                (more_flags,) = _UNPACK_SHORT(payload, offset)
                offset += 2
            properties = Basic.Properties.__new__(Basic.Properties)
            properties.content_type = None
            properties.content_encoding = None
            properties.headers = None
            properties.delivery_mode = None
            properties.priority = None
            properties.correlation_id = None
            properties.reply_to = None
            properties.expiration = None
            properties.message_id = None
            properties.timestamp = None
            properties.type = None
            properties.user_id = None
            properties.app_id = None
            if flags & 0x8000:
                properties.content_type, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x4000:
                properties.content_encoding, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x2000:
                properties.headers, offset = _DECODE_TABLE(payload, offset)
            if flags & 0x1000:
                properties.delivery_mode = payload[offset]
                offset += 1
            if flags & 0x0800:
                properties.priority = payload[offset]
                offset += 1
            if flags & 0x0400:
                properties.correlation_id, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x0200:
                properties.reply_to, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x0100:
                properties.expiration, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x0080:
                properties.message_id, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x0040:
                properties.timestamp, offset = _DECODE_TIMESTAMP(payload, offset)
            if flags & 0x0020:
                properties.type, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x0010:
                properties.user_id, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x0008:
                properties.app_id, offset = _DECODE_SHORTSTR(payload, offset)
            if flags & 0x0004:
                offset += 1 + payload[offset]
            return properties, offset

    class Qos(Method):
        """basic.qos"""

        __slots__ = ("prefetch_size", "prefetch_count", "global_")

        index = 0x003C000A  # class 60, method 10
        name = "Basic.Qos"
        synchronous = True
        has_content = False
        valid_responses = (0x003C000B,)

        def __init__(
            self,
            *,
            prefetch_size: int,
            prefetch_count: int,
            global_: bool,
        ) -> None:
            self.prefetch_size = prefetch_size
            self.prefetch_count = prefetch_count
            self.global_ = global_

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00\n')
            out += _PACK_LONG(self.prefetch_size)
            out += _PACK_SHORT(self.prefetch_count)
            bits = 0
            if self.global_:
                bits |= 1
            out.append(bits)
            return out

    class QosOk(Method):
        """basic.qos-ok"""

        __slots__ = ()

        index = 0x003C000B  # class 60, method 11
        name = "Basic.QosOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00\x0b')
            return out

    class Consume(Method):
        """basic.consume"""

        __slots__ = ("queue", "consumer_tag", "no_local", "no_ack", "exclusive", "no_wait", "arguments")

        index = 0x003C0014  # class 60, method 20
        name = "Basic.Consume"
        synchronous = True
        has_content = False
        valid_responses = (0x003C0015,)

        def __init__(
            self,
            *,
            queue: str,
            consumer_tag: str,
            no_local: bool,
            no_ack: bool,
            exclusive: bool,
            no_wait: bool,
            arguments: dict[str, Any] | None,
        ) -> None:
            self.queue = queue
            self.consumer_tag = consumer_tag
            self.no_local = no_local
            self.no_ack = no_ack
            self.exclusive = exclusive
            self.no_wait = no_wait
            self.arguments = arguments

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00\x14\x00\x00')
            _ENCODE_SHORTSTR(out, self.queue)
            _ENCODE_SHORTSTR(out, self.consumer_tag)
            bits = 0
            if self.no_local:
                bits |= 1
            if self.no_ack:
                bits |= 2
            if self.exclusive:
                bits |= 4
            if self.no_wait:
                bits |= 8
            out.append(bits)
            if self.arguments:
                _ENCODE_TABLE(out, self.arguments)
            else:
                out += b"\x00\x00\x00\x00"
            return out

    class ConsumeOk(Method):
        """basic.consume-ok"""

        __slots__ = ("consumer_tag",)

        index = 0x003C0015  # class 60, method 21
        name = "Basic.ConsumeOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            consumer_tag: str,
        ) -> None:
            self.consumer_tag = consumer_tag

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00\x15')
            _ENCODE_SHORTSTR(out, self.consumer_tag)
            return out

    class Cancel(Method):
        """basic.cancel"""

        __slots__ = ("consumer_tag", "no_wait")

        index = 0x003C001E  # class 60, method 30
        name = "Basic.Cancel"
        synchronous = True
        has_content = False
        valid_responses = (0x003C001F,)

        def __init__(
            self,
            *,
            consumer_tag: str,
            no_wait: bool,
        ) -> None:
            self.consumer_tag = consumer_tag
            self.no_wait = no_wait

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00\x1e')
            _ENCODE_SHORTSTR(out, self.consumer_tag)
            bits = 0
            if self.no_wait:
                bits |= 1
            out.append(bits)
            return out

    class CancelOk(Method):
        """basic.cancel-ok"""

        __slots__ = ("consumer_tag",)

        index = 0x003C001F  # class 60, method 31
        name = "Basic.CancelOk"
        synchronous = True
        has_content = False

        def __init__(
            self,
            *,
            consumer_tag: str,
        ) -> None:
            self.consumer_tag = consumer_tag

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00\x1f')
            _ENCODE_SHORTSTR(out, self.consumer_tag)
            return out

    class Publish(Method):
        """basic.publish"""

        __slots__ = ("exchange", "routing_key", "mandatory", "immediate")

        index = 0x003C0028  # class 60, method 40
        name = "Basic.Publish"
        synchronous = False
        has_content = True

        def __init__(
            self,
            *,
            exchange: str,
            routing_key: str,
            mandatory: bool,
            immediate: bool,
        ) -> None:
            self.exchange = exchange
            self.routing_key = routing_key
            self.mandatory = mandatory
            self.immediate = immediate

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00(\x00\x00')
            _ENCODE_SHORTSTR(out, self.exchange)
            _ENCODE_SHORTSTR(out, self.routing_key)
            bits = 0
            if self.mandatory:
                bits |= 1
            if self.immediate:
                bits |= 2
            out.append(bits)
            return out

    class Return(Method):
        """basic.return"""

        __slots__ = ("reply_code", "reply_text", "exchange", "routing_key")

        index = 0x003C0032  # class 60, method 50
        name = "Basic.Return"
        synchronous = False
        has_content = True

        def __init__(
            self,
            *,
            reply_code: int,
            reply_text: str,
            exchange: str,
            routing_key: str,
        ) -> None:
            self.reply_code = reply_code
            self.reply_text = reply_text
            self.exchange = exchange
            self.routing_key = routing_key

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x002')
            out += _PACK_SHORT(self.reply_code)
            _ENCODE_SHORTSTR(out, self.reply_text)
            _ENCODE_SHORTSTR(out, self.exchange)
            _ENCODE_SHORTSTR(out, self.routing_key)
            return out

    class Deliver(Method):
        """basic.deliver"""

        __slots__ = ("consumer_tag", "delivery_tag", "redelivered", "exchange", "routing_key")

        index = 0x003C003C  # class 60, method 60
        name = "Basic.Deliver"
        synchronous = False
        has_content = True

        def __init__(
            self,
            *,
            consumer_tag: str,
            delivery_tag: int,
            redelivered: bool,
            exchange: str,
            routing_key: str,
        ) -> None:
            self.consumer_tag = consumer_tag
            self.delivery_tag = delivery_tag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routing_key = routing_key

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00<')
            _ENCODE_SHORTSTR(out, self.consumer_tag)
            out += _PACK_LONG_LONG(self.delivery_tag)
            bits = 0
            if self.redelivered:
                bits |= 1
            out.append(bits)
            _ENCODE_SHORTSTR(out, self.exchange)
            _ENCODE_SHORTSTR(out, self.routing_key)
            return out

    class Get(Method):
        """basic.get"""

        __slots__ = ("queue", "no_ack")

        index = 0x003C0046  # class 60, method 70
        name = "Basic.Get"
        synchronous = True
        has_content = False
        valid_responses = (0x003C0047, 0x003C0048)

        def __init__(
            self,
            *,
            queue: str,
            no_ack: bool,
        ) -> None:
            self.queue = queue
            self.no_ack = no_ack

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00F\x00\x00')
            _ENCODE_SHORTSTR(out, self.queue)
            bits = 0
            if self.no_ack:
                bits |= 1
            out.append(bits)
            return out

    class GetOk(Method):
        """basic.get-ok"""

        __slots__ = ("delivery_tag", "redelivered", "exchange", "routing_key", "message_count")

        index = 0x003C0047  # class 60, method 71
        name = "Basic.GetOk"
        synchronous = True
        has_content = True

        def __init__(
            self,
            *,
            delivery_tag: int,
            redelivered: bool,
            exchange: str,
            routing_key: str,
            message_count: int,
        ) -> None:
            self.delivery_tag = delivery_tag
            self.redelivered = redelivered
            self.exchange = exchange
            self.routing_key = routing_key
            self.message_count = message_count

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00G')
            out += _PACK_LONG_LONG(self.delivery_tag)
            bits = 0
            if self.redelivered:
                bits |= 1
            out.append(bits)
            _ENCODE_SHORTSTR(out, self.exchange)
            _ENCODE_SHORTSTR(out, self.routing_key)
            out += _PACK_LONG(self.message_count)
            return out

    class GetEmpty(Method):
        """basic.get-empty"""

        __slots__ = ()

        index = 0x003C0048  # class 60, method 72
        name = "Basic.GetEmpty"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00H\x00')
            return out

    class Ack(Method):
        """basic.ack"""

        __slots__ = ("delivery_tag", "multiple")

        index = 0x003C0050  # class 60, method 80
        name = "Basic.Ack"
        synchronous = False
        has_content = False

        def __init__(
            self,
            *,
            delivery_tag: int,
            multiple: bool,
        ) -> None:
            self.delivery_tag = delivery_tag
            self.multiple = multiple

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00P')
            out += _PACK_LONG_LONG(self.delivery_tag)
            bits = 0
            if self.multiple:
                bits |= 1
            out.append(bits)
            return out

    class Reject(Method):
        """basic.reject"""

        __slots__ = ("delivery_tag", "requeue")

        index = 0x003C005A  # class 60, method 90
        name = "Basic.Reject"
        synchronous = False
        has_content = False

        def __init__(
            self,
            *,
            delivery_tag: int,
            requeue: bool,
        ) -> None:
            self.delivery_tag = delivery_tag
            self.requeue = requeue

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00Z')
            out += _PACK_LONG_LONG(self.delivery_tag)
            bits = 0
            if self.requeue:
                bits |= 1
            out.append(bits)
            return out

    class RecoverAsync(Method):
        """basic.recover-async"""

        __slots__ = ("requeue",)

        index = 0x003C0064  # class 60, method 100
        name = "Basic.RecoverAsync"
        synchronous = False
        has_content = False

        def __init__(
            self,
            *,
            requeue: bool,
        ) -> None:
            self.requeue = requeue

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00d')
            bits = 0
            if self.requeue:
                bits |= 1
            out.append(bits)
            return out

    class Recover(Method):
        """basic.recover"""

        __slots__ = ("requeue",)

        index = 0x003C006E  # class 60, method 110
        name = "Basic.Recover"
        synchronous = True
        has_content = False
        valid_responses = (0x003C006F,)

        def __init__(
            self,
            *,
            requeue: bool,
        ) -> None:
            self.requeue = requeue

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00n')
            bits = 0
            if self.requeue:
                bits |= 1
            out.append(bits)
            return out

    class RecoverOk(Method):
        """basic.recover-ok"""

        __slots__ = ()

        index = 0x003C006F  # class 60, method 111
        name = "Basic.RecoverOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00o')
            return out

    class Nack(Method):
        """basic.nack"""

        __slots__ = ("delivery_tag", "multiple", "requeue")

        index = 0x003C0078  # class 60, method 120
        name = "Basic.Nack"
        synchronous = False
        has_content = False

        def __init__(
            self,
            *,
            delivery_tag: int,
            multiple: bool,
            requeue: bool,
        ) -> None:
            self.delivery_tag = delivery_tag
            self.multiple = multiple
            self.requeue = requeue

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00<\x00x')
            out += _PACK_LONG_LONG(self.delivery_tag)
            bits = 0
            if self.multiple:
                bits |= 1
            if self.requeue:
                bits |= 2
            out.append(bits)
            return out


class Tx:
    """Methods of the tx class (index 90)."""

    class Select(Method):
        """tx.select"""

        __slots__ = ()

        index = 0x005A000A  # class 90, method 10
        name = "Tx.Select"
        synchronous = True
        has_content = False
        valid_responses = (0x005A000B,)

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00Z\x00\n')
            return out

    class SelectOk(Method):
        """tx.select-ok"""

        __slots__ = ()

        index = 0x005A000B  # class 90, method 11
        name = "Tx.SelectOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00Z\x00\x0b')
            return out

    class Commit(Method):
        """tx.commit"""

        __slots__ = ()

        index = 0x005A0014  # class 90, method 20
        name = "Tx.Commit"
        synchronous = True
        has_content = False
        valid_responses = (0x005A0015,)

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00Z\x00\x14')
            return out

    class CommitOk(Method):
        """tx.commit-ok"""

        __slots__ = ()

        index = 0x005A0015  # class 90, method 21
        name = "Tx.CommitOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00Z\x00\x15')
            return out

    class Rollback(Method):
        """tx.rollback"""

        __slots__ = ()

        index = 0x005A001E  # class 90, method 30
        name = "Tx.Rollback"
        synchronous = True
        has_content = False
        valid_responses = (0x005A001F,)

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00Z\x00\x1e')
            return out

    class RollbackOk(Method):
        """tx.rollback-ok"""

        __slots__ = ()

        index = 0x005A001F  # class 90, method 31
        name = "Tx.RollbackOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00Z\x00\x1f')
            return out


class Confirm:
    """Methods of the confirm class (index 85)."""

    class Select(Method):
        """confirm.select"""

        __slots__ = ("nowait",)

        index = 0x0055000A  # class 85, method 10
        name = "Confirm.Select"
        synchronous = True
        has_content = False
        valid_responses = (0x0055000B,)

        def __init__(
            self,
            *,
            nowait: bool,
        ) -> None:
            self.nowait = nowait

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00U\x00\n')
            bits = 0
            if self.nowait:
                bits |= 1
            out.append(bits)
            return out

    class SelectOk(Method):
        """confirm.select-ok"""

        __slots__ = ()

        index = 0x0055000B  # class 85, method 11
        name = "Confirm.SelectOk"
        synchronous = True
        has_content = False

        def encode(self) -> bytearray:
            """Return the method-frame payload (class + method + arguments)."""
            out = bytearray(b'\x00U\x00\x0b')
            return out


_CONNECTION_OPEN_OK_INSTANCE = Connection.OpenOk()
_CONNECTION_CLOSE_OK_INSTANCE = Connection.CloseOk()
_CONNECTION_UNBLOCKED_INSTANCE = Connection.Unblocked()
_CONNECTION_UPDATE_SECRET_OK_INSTANCE = Connection.UpdateSecretOk()
_CHANNEL_OPEN_INSTANCE = Channel.Open()
_CHANNEL_OPEN_OK_INSTANCE = Channel.OpenOk()
_CHANNEL_CLOSE_OK_INSTANCE = Channel.CloseOk()
_EXCHANGE_DECLARE_OK_INSTANCE = Exchange.DeclareOk()
_EXCHANGE_DELETE_OK_INSTANCE = Exchange.DeleteOk()
_EXCHANGE_BIND_OK_INSTANCE = Exchange.BindOk()
_EXCHANGE_UNBIND_OK_INSTANCE = Exchange.UnbindOk()
_QUEUE_BIND_OK_INSTANCE = Queue.BindOk()
_QUEUE_UNBIND_OK_INSTANCE = Queue.UnbindOk()
_BASIC_QOS_OK_INSTANCE = Basic.QosOk()
_BASIC_GET_EMPTY_INSTANCE = Basic.GetEmpty()
_BASIC_RECOVER_OK_INSTANCE = Basic.RecoverOk()
_TX_SELECT_INSTANCE = Tx.Select()
_TX_SELECT_OK_INSTANCE = Tx.SelectOk()
_TX_COMMIT_INSTANCE = Tx.Commit()
_TX_COMMIT_OK_INSTANCE = Tx.CommitOk()
_TX_ROLLBACK_INSTANCE = Tx.Rollback()
_TX_ROLLBACK_OK_INSTANCE = Tx.RollbackOk()
_CONFIRM_SELECT_OK_INSTANCE = Confirm.SelectOk()


def _decode_connection_start(payload: bytes) -> "Connection.Start":
    method = Connection.Start.__new__(Connection.Start)
    offset = 4
    method.version_major = payload[offset]
    offset += 1
    method.version_minor = payload[offset]
    offset += 1
    method.server_properties, offset = _DECODE_TABLE(payload, offset)
    method.mechanisms, offset = _DECODE_LONGSTR(payload, offset)
    method.locales, offset = _DECODE_LONGSTR(payload, offset)
    return method


def _decode_connection_start_ok(payload: bytes) -> "Connection.StartOk":
    method = Connection.StartOk.__new__(Connection.StartOk)
    offset = 4
    method.client_properties, offset = _DECODE_TABLE(payload, offset)
    method.mechanism, offset = _DECODE_SHORTSTR(payload, offset)
    method.response, offset = _DECODE_LONGSTR(payload, offset)
    method.locale, offset = _DECODE_SHORTSTR(payload, offset)
    return method


def _decode_connection_secure(payload: bytes) -> "Connection.Secure":
    method = Connection.Secure.__new__(Connection.Secure)
    offset = 4
    method.challenge, offset = _DECODE_LONGSTR(payload, offset)
    return method


def _decode_connection_secure_ok(payload: bytes) -> "Connection.SecureOk":
    method = Connection.SecureOk.__new__(Connection.SecureOk)
    offset = 4
    method.response, offset = _DECODE_LONGSTR(payload, offset)
    return method


def _decode_connection_tune(payload: bytes) -> "Connection.Tune":
    method = Connection.Tune.__new__(Connection.Tune)
    offset = 4
    (method.channel_max,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    (method.frame_max,) = _UNPACK_LONG(payload, offset)
    offset += 4
    (method.heartbeat,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    return method


def _decode_connection_tune_ok(payload: bytes) -> "Connection.TuneOk":
    method = Connection.TuneOk.__new__(Connection.TuneOk)
    offset = 4
    (method.channel_max,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    (method.frame_max,) = _UNPACK_LONG(payload, offset)
    offset += 4
    (method.heartbeat,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    return method


def _decode_connection_open(payload: bytes) -> "Connection.Open":
    method = Connection.Open.__new__(Connection.Open)
    offset = 4
    method.virtual_host, offset = _DECODE_SHORTSTR(payload, offset)
    offset += 1 + payload[offset]
    bits = payload[offset]
    offset += 1
    return method


def _decode_connection_open_ok(payload: bytes) -> "Connection.OpenOk":
    return _CONNECTION_OPEN_OK_INSTANCE


def _decode_connection_close(payload: bytes) -> "Connection.Close":
    method = Connection.Close.__new__(Connection.Close)
    offset = 4
    (method.reply_code,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    method.reply_text, offset = _DECODE_SHORTSTR(payload, offset)
    (method.class_id,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    (method.method_id,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    return method


def _decode_connection_close_ok(payload: bytes) -> "Connection.CloseOk":
    return _CONNECTION_CLOSE_OK_INSTANCE


def _decode_connection_blocked(payload: bytes) -> "Connection.Blocked":
    method = Connection.Blocked.__new__(Connection.Blocked)
    offset = 4
    method.reason, offset = _DECODE_SHORTSTR(payload, offset)
    return method


def _decode_connection_unblocked(payload: bytes) -> "Connection.Unblocked":
    return _CONNECTION_UNBLOCKED_INSTANCE


def _decode_connection_update_secret(payload: bytes) -> "Connection.UpdateSecret":
    method = Connection.UpdateSecret.__new__(Connection.UpdateSecret)
    offset = 4
    method.new_secret, offset = _DECODE_LONGSTR(payload, offset)
    method.reason, offset = _DECODE_SHORTSTR(payload, offset)
    return method


def _decode_connection_update_secret_ok(payload: bytes) -> "Connection.UpdateSecretOk":
    return _CONNECTION_UPDATE_SECRET_OK_INSTANCE


def _decode_channel_open(payload: bytes) -> "Channel.Open":
    return _CHANNEL_OPEN_INSTANCE


def _decode_channel_open_ok(payload: bytes) -> "Channel.OpenOk":
    return _CHANNEL_OPEN_OK_INSTANCE


def _decode_channel_flow(payload: bytes) -> "Channel.Flow":
    method = Channel.Flow.__new__(Channel.Flow)
    offset = 4
    bits = payload[offset]
    offset += 1
    method.active = (bits & 1) != 0
    return method


def _decode_channel_flow_ok(payload: bytes) -> "Channel.FlowOk":
    method = Channel.FlowOk.__new__(Channel.FlowOk)
    offset = 4
    bits = payload[offset]
    offset += 1
    method.active = (bits & 1) != 0
    return method


def _decode_channel_close(payload: bytes) -> "Channel.Close":
    method = Channel.Close.__new__(Channel.Close)
    offset = 4
    (method.reply_code,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    method.reply_text, offset = _DECODE_SHORTSTR(payload, offset)
    (method.class_id,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    (method.method_id,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    return method


def _decode_channel_close_ok(payload: bytes) -> "Channel.CloseOk":
    return _CHANNEL_CLOSE_OK_INSTANCE


def _decode_exchange_declare(payload: bytes) -> "Exchange.Declare":
    method = Exchange.Declare.__new__(Exchange.Declare)
    offset = 4
    offset += 2
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    method.type, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.passive = (bits & 1) != 0
    method.durable = (bits & 2) != 0
    method.auto_delete = (bits & 4) != 0
    method.internal = (bits & 8) != 0
    method.no_wait = (bits & 16) != 0
    method.arguments, offset = _DECODE_TABLE(payload, offset)
    return method


def _decode_exchange_declare_ok(payload: bytes) -> "Exchange.DeclareOk":
    return _EXCHANGE_DECLARE_OK_INSTANCE


def _decode_exchange_delete(payload: bytes) -> "Exchange.Delete":
    method = Exchange.Delete.__new__(Exchange.Delete)
    offset = 4
    offset += 2
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.if_unused = (bits & 1) != 0
    method.no_wait = (bits & 2) != 0
    return method


def _decode_exchange_delete_ok(payload: bytes) -> "Exchange.DeleteOk":
    return _EXCHANGE_DELETE_OK_INSTANCE


def _decode_exchange_bind(payload: bytes) -> "Exchange.Bind":
    method = Exchange.Bind.__new__(Exchange.Bind)
    offset = 4
    offset += 2
    method.destination, offset = _DECODE_SHORTSTR(payload, offset)
    method.source, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.no_wait = (bits & 1) != 0
    method.arguments, offset = _DECODE_TABLE(payload, offset)
    return method


def _decode_exchange_bind_ok(payload: bytes) -> "Exchange.BindOk":
    return _EXCHANGE_BIND_OK_INSTANCE


def _decode_exchange_unbind(payload: bytes) -> "Exchange.Unbind":
    method = Exchange.Unbind.__new__(Exchange.Unbind)
    offset = 4
    offset += 2
    method.destination, offset = _DECODE_SHORTSTR(payload, offset)
    method.source, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.no_wait = (bits & 1) != 0
    method.arguments, offset = _DECODE_TABLE(payload, offset)
    return method


def _decode_exchange_unbind_ok(payload: bytes) -> "Exchange.UnbindOk":
    return _EXCHANGE_UNBIND_OK_INSTANCE


def _decode_queue_declare(payload: bytes) -> "Queue.Declare":
    method = Queue.Declare.__new__(Queue.Declare)
    offset = 4
    offset += 2
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.passive = (bits & 1) != 0
    method.durable = (bits & 2) != 0
    method.exclusive = (bits & 4) != 0
    method.auto_delete = (bits & 8) != 0
    method.no_wait = (bits & 16) != 0
    method.arguments, offset = _DECODE_TABLE(payload, offset)
    return method


def _decode_queue_declare_ok(payload: bytes) -> "Queue.DeclareOk":
    method = Queue.DeclareOk.__new__(Queue.DeclareOk)
    offset = 4
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    (method.message_count,) = _UNPACK_LONG(payload, offset)
    offset += 4
    (method.consumer_count,) = _UNPACK_LONG(payload, offset)
    offset += 4
    return method


def _decode_queue_bind(payload: bytes) -> "Queue.Bind":
    method = Queue.Bind.__new__(Queue.Bind)
    offset = 4
    offset += 2
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.no_wait = (bits & 1) != 0
    method.arguments, offset = _DECODE_TABLE(payload, offset)
    return method


def _decode_queue_bind_ok(payload: bytes) -> "Queue.BindOk":
    return _QUEUE_BIND_OK_INSTANCE


def _decode_queue_unbind(payload: bytes) -> "Queue.Unbind":
    method = Queue.Unbind.__new__(Queue.Unbind)
    offset = 4
    offset += 2
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    method.arguments, offset = _DECODE_TABLE(payload, offset)
    return method


def _decode_queue_unbind_ok(payload: bytes) -> "Queue.UnbindOk":
    return _QUEUE_UNBIND_OK_INSTANCE


def _decode_queue_purge(payload: bytes) -> "Queue.Purge":
    method = Queue.Purge.__new__(Queue.Purge)
    offset = 4
    offset += 2
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.no_wait = (bits & 1) != 0
    return method


def _decode_queue_purge_ok(payload: bytes) -> "Queue.PurgeOk":
    method = Queue.PurgeOk.__new__(Queue.PurgeOk)
    offset = 4
    (method.message_count,) = _UNPACK_LONG(payload, offset)
    offset += 4
    return method


def _decode_queue_delete(payload: bytes) -> "Queue.Delete":
    method = Queue.Delete.__new__(Queue.Delete)
    offset = 4
    offset += 2
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.if_unused = (bits & 1) != 0
    method.if_empty = (bits & 2) != 0
    method.no_wait = (bits & 4) != 0
    return method


def _decode_queue_delete_ok(payload: bytes) -> "Queue.DeleteOk":
    method = Queue.DeleteOk.__new__(Queue.DeleteOk)
    offset = 4
    (method.message_count,) = _UNPACK_LONG(payload, offset)
    offset += 4
    return method


def _decode_basic_qos(payload: bytes) -> "Basic.Qos":
    method = Basic.Qos.__new__(Basic.Qos)
    offset = 4
    (method.prefetch_size,) = _UNPACK_LONG(payload, offset)
    offset += 4
    (method.prefetch_count,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    bits = payload[offset]
    offset += 1
    method.global_ = (bits & 1) != 0
    return method


def _decode_basic_qos_ok(payload: bytes) -> "Basic.QosOk":
    return _BASIC_QOS_OK_INSTANCE


def _decode_basic_consume(payload: bytes) -> "Basic.Consume":
    method = Basic.Consume.__new__(Basic.Consume)
    offset = 4
    offset += 2
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    method.consumer_tag, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.no_local = (bits & 1) != 0
    method.no_ack = (bits & 2) != 0
    method.exclusive = (bits & 4) != 0
    method.no_wait = (bits & 8) != 0
    method.arguments, offset = _DECODE_TABLE(payload, offset)
    return method


def _decode_basic_consume_ok(payload: bytes) -> "Basic.ConsumeOk":
    method = Basic.ConsumeOk.__new__(Basic.ConsumeOk)
    offset = 4
    method.consumer_tag, offset = _DECODE_SHORTSTR(payload, offset)
    return method


def _decode_basic_cancel(payload: bytes) -> "Basic.Cancel":
    method = Basic.Cancel.__new__(Basic.Cancel)
    offset = 4
    method.consumer_tag, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.no_wait = (bits & 1) != 0
    return method


def _decode_basic_cancel_ok(payload: bytes) -> "Basic.CancelOk":
    method = Basic.CancelOk.__new__(Basic.CancelOk)
    offset = 4
    method.consumer_tag, offset = _DECODE_SHORTSTR(payload, offset)
    return method


def _decode_basic_publish(payload: bytes) -> "Basic.Publish":
    method = Basic.Publish.__new__(Basic.Publish)
    offset = 4
    offset += 2
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.mandatory = (bits & 1) != 0
    method.immediate = (bits & 2) != 0
    return method


def _decode_basic_return(payload: bytes) -> "Basic.Return":
    method = Basic.Return.__new__(Basic.Return)
    offset = 4
    (method.reply_code,) = _UNPACK_SHORT(payload, offset)
    offset += 2
    method.reply_text, offset = _DECODE_SHORTSTR(payload, offset)
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    return method


def _decode_basic_deliver(payload: bytes) -> "Basic.Deliver":
    method = Basic.Deliver.__new__(Basic.Deliver)
    offset = 4
    method.consumer_tag, offset = _DECODE_SHORTSTR(payload, offset)
    (method.delivery_tag,) = _UNPACK_LONG_LONG(payload, offset)
    offset += 8
    bits = payload[offset]
    offset += 1
    method.redelivered = (bits & 1) != 0
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    return method


def _decode_basic_get(payload: bytes) -> "Basic.Get":
    method = Basic.Get.__new__(Basic.Get)
    offset = 4
    offset += 2
    method.queue, offset = _DECODE_SHORTSTR(payload, offset)
    bits = payload[offset]
    offset += 1
    method.no_ack = (bits & 1) != 0
    return method


def _decode_basic_get_ok(payload: bytes) -> "Basic.GetOk":
    method = Basic.GetOk.__new__(Basic.GetOk)
    offset = 4
    (method.delivery_tag,) = _UNPACK_LONG_LONG(payload, offset)
    offset += 8
    bits = payload[offset]
    offset += 1
    method.redelivered = (bits & 1) != 0
    method.exchange, offset = _DECODE_SHORTSTR(payload, offset)
    method.routing_key, offset = _DECODE_SHORTSTR(payload, offset)
    (method.message_count,) = _UNPACK_LONG(payload, offset)
    offset += 4
    return method


def _decode_basic_get_empty(payload: bytes) -> "Basic.GetEmpty":
    return _BASIC_GET_EMPTY_INSTANCE


def _decode_basic_ack(payload: bytes) -> "Basic.Ack":
    method = Basic.Ack.__new__(Basic.Ack)
    offset = 4
    (method.delivery_tag,) = _UNPACK_LONG_LONG(payload, offset)
    offset += 8
    bits = payload[offset]
    offset += 1
    method.multiple = (bits & 1) != 0
    return method


def _decode_basic_reject(payload: bytes) -> "Basic.Reject":
    method = Basic.Reject.__new__(Basic.Reject)
    offset = 4
    (method.delivery_tag,) = _UNPACK_LONG_LONG(payload, offset)
    offset += 8
    bits = payload[offset]
    offset += 1
    method.requeue = (bits & 1) != 0
    return method


def _decode_basic_recover_async(payload: bytes) -> "Basic.RecoverAsync":
    method = Basic.RecoverAsync.__new__(Basic.RecoverAsync)
    offset = 4
    bits = payload[offset]
    offset += 1
    method.requeue = (bits & 1) != 0
    return method


def _decode_basic_recover(payload: bytes) -> "Basic.Recover":
    method = Basic.Recover.__new__(Basic.Recover)
    offset = 4
    bits = payload[offset]
    offset += 1
    method.requeue = (bits & 1) != 0
    return method


def _decode_basic_recover_ok(payload: bytes) -> "Basic.RecoverOk":
    return _BASIC_RECOVER_OK_INSTANCE


def _decode_basic_nack(payload: bytes) -> "Basic.Nack":
    method = Basic.Nack.__new__(Basic.Nack)
    offset = 4
    (method.delivery_tag,) = _UNPACK_LONG_LONG(payload, offset)
    offset += 8
    bits = payload[offset]
    offset += 1
    method.multiple = (bits & 1) != 0
    method.requeue = (bits & 2) != 0
    return method


def _decode_tx_select(payload: bytes) -> "Tx.Select":
    return _TX_SELECT_INSTANCE


def _decode_tx_select_ok(payload: bytes) -> "Tx.SelectOk":
    return _TX_SELECT_OK_INSTANCE


def _decode_tx_commit(payload: bytes) -> "Tx.Commit":
    return _TX_COMMIT_INSTANCE


def _decode_tx_commit_ok(payload: bytes) -> "Tx.CommitOk":
    return _TX_COMMIT_OK_INSTANCE


def _decode_tx_rollback(payload: bytes) -> "Tx.Rollback":
    return _TX_ROLLBACK_INSTANCE


def _decode_tx_rollback_ok(payload: bytes) -> "Tx.RollbackOk":
    return _TX_ROLLBACK_OK_INSTANCE


def _decode_confirm_select(payload: bytes) -> "Confirm.Select":
    method = Confirm.Select.__new__(Confirm.Select)
    offset = 4
    bits = payload[offset]
    offset += 1
    method.nowait = (bits & 1) != 0
    return method


def _decode_confirm_select_ok(payload: bytes) -> "Confirm.SelectOk":
    return _CONFIRM_SELECT_OK_INSTANCE


METHOD_DECODERS: dict[int, Callable[[bytes], Method]] = {
    0x000A000A: _decode_connection_start,
    0x000A000B: _decode_connection_start_ok,
    0x000A0014: _decode_connection_secure,
    0x000A0015: _decode_connection_secure_ok,
    0x000A001E: _decode_connection_tune,
    0x000A001F: _decode_connection_tune_ok,
    0x000A0028: _decode_connection_open,
    0x000A0029: _decode_connection_open_ok,
    0x000A0032: _decode_connection_close,
    0x000A0033: _decode_connection_close_ok,
    0x000A003C: _decode_connection_blocked,
    0x000A003D: _decode_connection_unblocked,
    0x000A0046: _decode_connection_update_secret,
    0x000A0047: _decode_connection_update_secret_ok,
    0x0014000A: _decode_channel_open,
    0x0014000B: _decode_channel_open_ok,
    0x00140014: _decode_channel_flow,
    0x00140015: _decode_channel_flow_ok,
    0x00140028: _decode_channel_close,
    0x00140029: _decode_channel_close_ok,
    0x0028000A: _decode_exchange_declare,
    0x0028000B: _decode_exchange_declare_ok,
    0x00280014: _decode_exchange_delete,
    0x00280015: _decode_exchange_delete_ok,
    0x0028001E: _decode_exchange_bind,
    0x0028001F: _decode_exchange_bind_ok,
    0x00280028: _decode_exchange_unbind,
    0x00280033: _decode_exchange_unbind_ok,
    0x0032000A: _decode_queue_declare,
    0x0032000B: _decode_queue_declare_ok,
    0x00320014: _decode_queue_bind,
    0x00320015: _decode_queue_bind_ok,
    0x00320032: _decode_queue_unbind,
    0x00320033: _decode_queue_unbind_ok,
    0x0032001E: _decode_queue_purge,
    0x0032001F: _decode_queue_purge_ok,
    0x00320028: _decode_queue_delete,
    0x00320029: _decode_queue_delete_ok,
    0x003C000A: _decode_basic_qos,
    0x003C000B: _decode_basic_qos_ok,
    0x003C0014: _decode_basic_consume,
    0x003C0015: _decode_basic_consume_ok,
    0x003C001E: _decode_basic_cancel,
    0x003C001F: _decode_basic_cancel_ok,
    0x003C0028: _decode_basic_publish,
    0x003C0032: _decode_basic_return,
    0x003C003C: _decode_basic_deliver,
    0x003C0046: _decode_basic_get,
    0x003C0047: _decode_basic_get_ok,
    0x003C0048: _decode_basic_get_empty,
    0x003C0050: _decode_basic_ack,
    0x003C005A: _decode_basic_reject,
    0x003C0064: _decode_basic_recover_async,
    0x003C006E: _decode_basic_recover,
    0x003C006F: _decode_basic_recover_ok,
    0x003C0078: _decode_basic_nack,
    0x005A000A: _decode_tx_select,
    0x005A000B: _decode_tx_select_ok,
    0x005A0014: _decode_tx_commit,
    0x005A0015: _decode_tx_commit_ok,
    0x005A001E: _decode_tx_rollback,
    0x005A001F: _decode_tx_rollback_ok,
    0x0055000A: _decode_confirm_select,
    0x0055000B: _decode_confirm_select_ok,
}
