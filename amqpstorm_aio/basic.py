import asyncio
import logging
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from typing import Any
from typing import cast

from amqpstorm_aio import base
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import message as message_module
from amqpstorm_aio import spec
from amqpstorm_aio import validation

LOG = logging.getLogger(__name__)

type ConsumerCallback = Callable[[message_module.Message], Awaitable[None]]

_EMPTY_PROPERTIES = spec.Basic.Properties()
_FRAME_BODY = spec.FRAME_BODY
_FRAME_END = spec.FRAME_END
_SHORTSTR_PROPERTIES = (
    "content_type",
    "content_encoding",
    "correlation_id",
    "reply_to",
    "expiration",
    "message_id",
    "type",
    "user_id",
    "app_id",
)
_DELIVERY_MODES = (1, 2)

PROPERTY_NAMES = frozenset(spec.Basic.Properties.__slots__)

_PUBLISH_FRAME_CACHE: dict[tuple[int, str, str, bool, bool], bytes] = {}
PUBLISH_FRAME_CACHE_MAX = 256


def _publish_method_frame(
    channel_id: int, exchange: str, routing_key: str, mandatory: bool, immediate: bool
) -> bytes:
    """The encoded Basic.Publish frame for these arguments; cached where it pays."""
    key = (channel_id, exchange, routing_key, mandatory, immediate)
    frame = _PUBLISH_FRAME_CACHE.get(key)
    if frame is None:
        buffer = bytearray()
        framing.append_method_frame(
            buffer,
            channel_id,
            spec.Basic.Publish(
                exchange=exchange,
                routing_key=routing_key,
                mandatory=mandatory,
                immediate=immediate,
            ),
        )
        frame = bytes(buffer)
        if len(_PUBLISH_FRAME_CACHE) < PUBLISH_FRAME_CACHE_MAX:
            _PUBLISH_FRAME_CACHE[key] = frame
    return frame


def _fail_confirm_on_timeout(waiter: asyncio.Future[bool], delivery_tag: int) -> None:
    """Fail the waiter itself rather than cancelling whoever awaits it.

    Cancelling the awaiting task would surface as CancelledError and skip the cleanup
    that returns the delivery tag.
    """
    if not waiter.done():
        waiter.set_exception(
            exception.AMQPChannelError(
                f"publisher confirm for delivery tag {delivery_tag} took too long"
            )
        )


def properties_from_mapping(mapping: Mapping[str, Any]) -> spec.Basic.Properties:
    """Build Properties from a mapping of field names, rejecting unknown keys."""
    if not isinstance(mapping, Mapping):
        raise exception.AMQPInvalidArgument(
            "properties should be a Basic.Properties, a mapping, or None"
        )
    unknown = set(mapping) - PROPERTY_NAMES
    if unknown:
        raise exception.AMQPInvalidArgument(
            f"unknown message properties {sorted(unknown)}; "
            f"valid names are {sorted(PROPERTY_NAMES)}"
        )
    return spec.Basic.Properties(**mapping)


def _validate_properties(properties: spec.Basic.Properties) -> None:
    """Reject property values the wire format or the spec cannot carry.

    Properties are a plain mutable record, so they are validated at publish time
    rather than on assignment.
    """
    for name in _SHORTSTR_PROPERTIES:
        value = getattr(properties, name)
        if value is not None:
            validation.require_shortstr(f"properties.{name}", value)
    if properties.delivery_mode is not None and properties.delivery_mode not in _DELIVERY_MODES:
        raise exception.AMQPInvalidArgument(
            "properties.delivery_mode should be 1 (transient) or 2 (persistent)"
        )
    if properties.priority is not None:
        validation.require_octet("properties.priority", properties.priority)
    if properties.headers is not None:
        validation.require_table("properties.headers", properties.headers)


class Basic(base.Handler):
    """Basic-class operations for one channel."""

    __slots__ = ()

    async def qos(
        self, prefetch_count: int = 0, prefetch_size: int = 0, global_: bool = False
    ) -> dict[str, Any]:
        """Set the channel's prefetch limits."""
        validation.require_short("prefetch_count", prefetch_count)
        validation.require_long("prefetch_size", prefetch_size)
        validation.require_boolean("global_", global_)
        result = await self._channel.rpc_request(
            spec.Basic.Qos(
                prefetch_size=prefetch_size, prefetch_count=prefetch_count, global_=global_
            )
        )
        return base.method_to_dict(result)

    async def consume(
        self,
        callback: ConsumerCallback | None = None,
        queue: str = "",
        consumer_tag: str = "",
        exclusive: bool = False,
        no_ack: bool = False,
        no_local: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> str:
        """Start a consumer; returns the (server-assigned) consumer tag."""
        validation.require_domain("queue-name", "queue", queue)
        validation.require_shortstr("consumer_tag", consumer_tag)
        validation.require_boolean("exclusive", exclusive)
        validation.require_boolean("no_ack", no_ack)
        validation.require_boolean("no_local", no_local)
        validation.require_table("arguments", arguments)
        if callback is not None and not callable(callback):
            raise exception.AMQPInvalidArgument("callback should be an async callable or None")
        result = cast(
            spec.Basic.ConsumeOk,
            await self._channel.rpc_request(
                spec.Basic.Consume(
                    queue=queue,
                    consumer_tag=consumer_tag,
                    no_local=no_local,
                    no_ack=no_ack,
                    exclusive=exclusive,
                    no_wait=False,
                    arguments=arguments,
                )
            ),
        )
        self._channel.add_consumer_tag(result.consumer_tag)
        if callback is not None:
            self._channel.register_consumer_callback(result.consumer_tag, callback)
        return result.consumer_tag

    async def cancel(self, consumer_tag: str = "") -> dict[str, Any]:
        """Cancel a consumer."""
        validation.require_shortstr("consumer_tag", consumer_tag)
        result = await self._channel.rpc_request(
            spec.Basic.Cancel(consumer_tag=consumer_tag, no_wait=False)
        )
        self._channel.remove_consumer_tag(consumer_tag)
        return base.method_to_dict(result)

    async def get(self, queue: str = "", no_ack: bool = False) -> message_module.Message | None:
        """Fetch one message; None when the queue is empty."""
        validation.require_domain("queue-name", "queue", queue)
        validation.require_boolean("no_ack", no_ack)
        channel = self._channel
        if channel.consumer_tags:
            raise exception.AMQPChannelError(
                "Cannot call 'get' when channel is set to consume"
            )
        async with channel.rpc_lock:
            channel.check_for_errors()
            waiter = channel.register_get_waiter()
            written = False
            try:
                channel.connection.write_method_frame(
                    channel.channel_id, spec.Basic.Get(queue=queue, no_ack=no_ack)
                )
                written = True
                async with asyncio.timeout(channel.rpc_timeout):
                    return await waiter
            except TimeoutError:
                channel.mark_stale_get(no_ack)
                raise exception.AMQPChannelError("rpc request Basic.Get took too long") from None
            except BaseException:
                if written and (not waiter.done() or waiter.cancelled()):
                    channel.mark_stale_get(no_ack)
                raise
            finally:
                channel.clear_get_waiter()

    async def recover(self, requeue: bool = False) -> dict[str, Any]:
        """Redeliver all unacknowledged messages on this channel."""
        validation.require_boolean("requeue", requeue)
        result = await self._channel.rpc_request(spec.Basic.Recover(requeue=requeue))
        return base.method_to_dict(result)

    async def ack(self, delivery_tag: int = 0, multiple: bool = False) -> None:
        """Acknowledge one delivery (or everything up to it with ``multiple``)."""
        validation.require_long_long("delivery_tag", delivery_tag)
        validation.require_boolean("multiple", multiple)
        self._channel.check_for_errors()
        self._channel.connection.write_method_frame(
            self._channel.channel_id,
            spec.Basic.Ack(delivery_tag=delivery_tag, multiple=multiple),
        )

    async def nack(
        self, delivery_tag: int = 0, multiple: bool = False, requeue: bool = True
    ) -> None:
        """Negatively acknowledge one delivery (or everything up to it)."""
        validation.require_long_long("delivery_tag", delivery_tag)
        validation.require_boolean("multiple", multiple)
        validation.require_boolean("requeue", requeue)
        self._channel.check_for_errors()
        self._channel.connection.write_method_frame(
            self._channel.channel_id,
            spec.Basic.Nack(delivery_tag=delivery_tag, multiple=multiple, requeue=requeue),
        )

    async def reject(self, delivery_tag: int = 0, requeue: bool = True) -> None:
        """Reject one delivery."""
        validation.require_long_long("delivery_tag", delivery_tag)
        validation.require_boolean("requeue", requeue)
        self._channel.check_for_errors()
        self._channel.connection.write_method_frame(
            self._channel.channel_id,
            spec.Basic.Reject(delivery_tag=delivery_tag, requeue=requeue),
        )

    async def publish(
        self,
        body: bytes | str,
        routing_key: str,
        exchange: str = "",
        properties: spec.Basic.Properties | spec.PropertiesDict | None = None,
        mandatory: bool = False,
        immediate: bool = False,
    ) -> bool | None:
        """Publish a message.

        ``properties`` accepts either a ``spec.Basic.Properties`` or a plain mapping of
        the same field names, e.g. ``{"expiration": "6000"}``.

        Returns None in fire-and-forget mode. In confirm mode
        (:meth:`Channel.confirm_deliveries`) returns True on Basic.Ack and False on
        Basic.Nack; with ``mandatory`` an unroutable message raises
        :class:`~amqpstorm_aio.exception.AMQPMessageError` carrying the broker's reason.

        A ``mandatory`` publish without confirm mode cannot await the broker's verdict:
        its Basic.Return is recorded on the channel and raised by the next call that
        checks channel errors (or :meth:`Channel.check_for_exceptions`).
        """
        if not isinstance(body, (bytes, str)):
            raise exception.AMQPInvalidArgument("body should be a string or bytes")
        validation.require_shortstr("routing_key", routing_key)
        validation.require_domain("exchange-name", "exchange", exchange)
        validation.require_boolean("mandatory", mandatory)
        validation.require_boolean("immediate", immediate)
        body, properties = self._prepare_content(body, properties)
        channel = self._channel
        session_token = channel.session_token
        connection = channel.connection
        connection.ensure_current_loop()
        channel.check_for_errors()
        frames = self._marshal_publish(
            channel_id=channel.channel_id,
            exchange=exchange,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            body=body,
            properties=properties,
            max_frame_size=connection.max_frame_size,
        )
        may_return = mandatory or immediate
        if may_return:
            async with channel.return_lock:
                return await self._publish_frames(frames, session_token, may_return=True)
        return await self._publish_frames(frames, session_token, may_return=False)

    async def _publish_frames(
        self, frames: bytearray, session_token: object, *, may_return: bool
    ) -> bool | None:
        channel = self._channel
        connection = channel.connection
        await connection.drain()
        while True:
            while (select_in_flight := channel.confirm_select_pending) is not None:
                await select_in_flight
            while (flow_stopped := channel.flow_gate) is not None:
                await flow_stopped
            pending_return = channel.return_pending if may_return else None
            if pending_return is None:
                break
            try:
                async with asyncio.timeout(channel.rpc_timeout):
                    await asyncio.shield(pending_return)
            except TimeoutError:
                raise exception.AMQPChannelError(
                    "previous return-capable publish confirm took too long"
                ) from None
        if session_token is not channel.session_token:
            raise exception.AMQPChannelError("channel session changed while publish was waiting")
        channel.check_for_errors()
        if not channel.confirming_deliveries:
            connection.write_bytes(frames)
            return None
        delivery_tag = channel.allocate_delivery_tag()
        confirm_waiter = channel.register_confirm(delivery_tag, may_return=may_return)
        try:
            connection.write_bytes(frames)
        except BaseException:
            channel.discard_confirm(delivery_tag)
            raise
        loop = asyncio.get_running_loop()
        deadline = loop.call_later(
            channel.rpc_timeout, _fail_confirm_on_timeout, confirm_waiter, delivery_tag
        )
        try:
            confirmed = await confirm_waiter
        finally:
            deadline.cancel()
            if session_token is channel.session_token:
                channel.discard_confirm(delivery_tag, written=True)
        return confirmed

    @staticmethod
    def _prepare_content(
        body: bytes | str,
        properties: spec.Basic.Properties | spec.PropertiesDict | None,
    ) -> tuple[bytes, spec.Basic.Properties]:
        """Validate the properties and encode the body they describe."""
        if properties is not None and not isinstance(properties, spec.Basic.Properties):
            properties = properties_from_mapping(properties)
        if properties is not None:
            _validate_properties(properties)
        if isinstance(body, str):
            return Basic._encode_str_body(body, properties)
        return body, properties if properties is not None else _EMPTY_PROPERTIES

    @staticmethod
    def _encode_str_body(
        body: str, properties: spec.Basic.Properties | None
    ) -> tuple[bytes, spec.Basic.Properties]:
        """Encode a str body per its declared content encoding, without mutating input."""
        if properties is None:
            return body.encode("utf-8"), spec.Basic.Properties(content_encoding="utf-8")
        if properties.content_encoding is None:
            copied = spec.Basic.Properties(
                **{name: getattr(properties, name) for name in properties.__slots__}
            )
            copied.content_encoding = "utf-8"
            return body.encode("utf-8"), copied
        try:
            return body.encode(properties.content_encoding), properties
        except LookupError:
            raise exception.AMQPInvalidArgument(
                f"content_encoding {properties.content_encoding!r} is not a text codec; "
                f"encode the body to bytes yourself"
            ) from None

    @staticmethod
    def _marshal_publish(
        *,
        channel_id: int,
        exchange: str,
        routing_key: str,
        mandatory: bool,
        immediate: bool,
        body: bytes,
        properties: spec.Basic.Properties,
        max_frame_size: int,
    ) -> bytearray:
        """Build the method, content header and body frames as one buffer.

        The cached method frame is copied, never handed on: asyncio's proactor
        transport retains the exact buffer it is given and would extend a shared entry.
        """
        frames = bytearray(
            _publish_method_frame(channel_id, exchange, routing_key, mandatory, immediate)
        )
        body_length = len(body)
        if properties is _EMPTY_PROPERTIES:
            framing.append_empty_content_header_frame(frames, channel_id, body_length)
        else:
            header_start = len(frames)
            framing.append_content_header_frame(frames, channel_id, body_length, properties)
            if len(frames) - header_start > max_frame_size:
                raise exception.AMQPInvalidArgument(
                    f"content header of {len(frames) - header_start} bytes exceeds the "
                    f"negotiated frame maximum {max_frame_size}; properties are too large"
                )
        chunk_size = max_frame_size - framing.FRAME_OVERHEAD
        if body_length <= chunk_size:
            # An empty body carries no body frame at all, which the chunking loop got
            # for free from range(0, 0, n) and this branch must preserve.
            if body_length:
                frames += framing.PACK_FRAME_HEADER.pack(_FRAME_BODY, channel_id, body_length)
                frames += body
                frames.append(_FRAME_END)
            return frames
        body_view = memoryview(body)
        for offset in range(0, body_length, chunk_size):
            framing.append_frame(
                frames, _FRAME_BODY, channel_id, body_view[offset : offset + chunk_size]
            )
        return frames
