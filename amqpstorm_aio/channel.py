import asyncio
import logging
from collections import deque
from collections.abc import AsyncGenerator
from types import TracebackType
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import cast

from amqpstorm_aio import base
from amqpstorm_aio import basic as basic_module
from amqpstorm_aio import exception
from amqpstorm_aio import exchange as exchange_module
from amqpstorm_aio import framing
from amqpstorm_aio import message as message_module
from amqpstorm_aio import queue as queue_module
from amqpstorm_aio import spec
from amqpstorm_aio import tx as tx_module
from amqpstorm_aio import validation

if TYPE_CHECKING:
    from amqpstorm_aio.connection import Connection

LOG = logging.getLogger(__name__)

_BASIC_ACK = spec.Basic.Ack.index
_BASIC_NACK = spec.Basic.Nack.index
_BASIC_CANCEL = spec.Basic.Cancel.index
_BASIC_GET_EMPTY = spec.Basic.GetEmpty.index
_CHANNEL_CLOSE = spec.Channel.Close.index
_CHANNEL_FLOW = spec.Channel.Flow.index

DEFAULT_EMPTY_TIMEOUT_SECONDS = 1.0
_MAX_RECORDED_ERRORS = 128


class Channel(base.Stateful):
    """One AMQP channel; created via ``Connection.channel()``."""

    __slots__ = (
        "_basic",
        "_buffered_inbound_bytes",
        "_channel_id",
        "_confirm_futures",
        "_confirm_select_pending",
        "_confirming",
        "_connection",
        "_consumer_callbacks",
        "_consumer_tags",
        "_content_body",
        "_content_header",
        "_content_method",
        "_content_received",
        "_exchange",
        "_flow_gate",
        "_get_waiter",
        "_inbound",
        "_inbound_waiters",
        "_next_delivery_tag",
        "_queue",
        "_recorded_errors",
        "_return_confirm_tag",
        "_return_error",
        "_return_lock",
        "_return_pending",
        "_returned_errors",
        "_returns_recorded",
        "_rpc_expected",
        "_rpc_lock",
        "_rpc_timeout",
        "_rpc_waiter",
        "_session_token",
        "_stale_gets",
        "_stale_rpc_expected",
        "_tx",
        "_user_closed",
    )

    def __init__(self, channel_id: int, connection: Connection, rpc_timeout: int) -> None:
        super().__init__()
        self._channel_id = channel_id
        self._session_token = object()
        self._connection = connection
        self._rpc_timeout = rpc_timeout
        self._rpc_lock = asyncio.Lock()
        self._rpc_expected: tuple[int, ...] = ()
        self._rpc_waiter: asyncio.Future[spec.Method] | None = None
        self._stale_rpc_expected: list[tuple[int, ...]] = []
        self._stale_gets: deque[bool] = deque()
        self._get_waiter: asyncio.Future[message_module.Message | None] | None = None
        self._inbound: deque[message_module.Message] = deque()
        self._buffered_inbound_bytes = 0
        self._inbound_waiters: set[asyncio.Future[None]] = set()
        self._consumer_callbacks: dict[str, Any] = {}
        self._consumer_tags: list[str] = []
        self._confirming = False
        self._confirm_select_pending: asyncio.Future[None] | None = None
        self._flow_gate: asyncio.Future[None] | None = None
        self._next_delivery_tag = 0
        self._confirm_futures: dict[int, asyncio.Future[bool]] = {}
        self._return_confirm_tag: int | None = None
        self._return_error: exception.AMQPMessageError | None = None
        self._return_lock = asyncio.Lock()
        self._return_pending: asyncio.Future[None] | None = None
        self._returned_errors: deque[exception.AMQPMessageError] = deque(
            maxlen=_MAX_RECORDED_ERRORS
        )
        self._returns_recorded = 0
        self._recorded_errors: deque[exception.AMQPChannelError] = deque(
            maxlen=_MAX_RECORDED_ERRORS
        )
        self._content_method: spec.Method | None = None
        self._content_header: framing.ContentHeader | None = None
        self._content_body: list[bytes] = []
        self._content_received = 0
        self._user_closed = False
        self._basic = basic_module.Basic(self)
        self._queue = queue_module.Queue(self)
        self._exchange = exchange_module.Exchange(self)
        self._tx = tx_module.Tx(self)

    def __int__(self) -> int:
        return self._channel_id

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if exception_value is not None:
            LOG.warning(f"Closing channel due to an unhandled exception: {exception_value}")
            if self.is_open:
                try:
                    await self.close()
                except exception.AMQPError:
                    pass
            return
        if self.is_open:
            await self.close()

    def __aiter__(self) -> AsyncGenerator[message_module.Message]:
        return self.build_inbound_messages()

    @property
    def basic(self) -> basic_module.Basic:
        return self._basic

    @property
    def queue(self) -> queue_module.Queue:
        return self._queue

    @property
    def exchange(self) -> exchange_module.Exchange:
        return self._exchange

    @property
    def tx(self) -> tx_module.Tx:
        return self._tx

    @property
    def channel_id(self) -> int:
        return self._channel_id

    @property
    def session_token(self) -> object:
        """Identity of this channel's current delivery-tag namespace."""
        return self._session_token

    @property
    def rpc_lock(self) -> asyncio.Lock:
        return self._rpc_lock

    @property
    def rpc_timeout(self) -> int:
        return self._rpc_timeout

    @property
    def connection(self) -> Connection:
        return self._connection

    @property
    def confirming_deliveries(self) -> bool:
        return self._confirming

    @property
    def consumer_tags(self) -> list[str]:
        return self._consumer_tags

    async def open(self) -> None:
        """Send Channel.Open and wait for OpenOk; reusable while its id is unclaimed."""
        self._connection.reclaim_channel_id(self)
        self._session_token = object()
        self._release_return_slot()
        self._drop_inbound_buffer()
        self._recorded_errors.clear()
        self._returned_errors.clear()
        self._confirm_futures.clear()
        self._stale_rpc_expected.clear()
        self._stale_gets.clear()
        self._confirming = False
        self._confirm_select_pending = None
        self._flow_gate = None
        self._next_delivery_tag = 0
        self._user_closed = False
        self._tx.reset()
        self.set_state(base.Stateful.OPENING)
        await self.rpc_request(spec.Channel.Open())
        self.set_state(base.Stateful.OPEN)

    async def close(
        self, reply_code: int = spec.REPLY_SUCCESS, reply_text: str = "Normal shutdown"
    ) -> None:
        """Cancel consumers, send Channel.Close, and wait for CloseOk."""
        validation.require_reply_code("reply_code", reply_code)
        validation.require_domain("reply-text", "reply_text", reply_text)
        LOG.debug(f"Channel #{self._channel_id} closing")
        self._user_closed = True
        if self._connection.is_closed or not self.is_open:
            self._close_locally()
            return
        self.set_state(base.Stateful.CLOSING)
        try:
            await self.stop_consuming()
        except exception.AMQPError:
            self._consumer_tags.clear()
            self._consumer_callbacks.clear()
        try:
            await self.rpc_request(
                spec.Channel.Close(
                    reply_code=reply_code,
                    reply_text=reply_text,
                    class_id=0,
                    method_id=0,
                ),
                check_channel_errors=False,
            )
        except exception.AMQPChannelError:
            pass
        finally:
            self._close_locally()
        LOG.debug(f"Channel #{self._channel_id} closed")

    async def stop_consuming(self) -> None:
        """Cancel every active consumer (best effort on a dying channel)."""
        if not self._consumer_tags:
            return
        if not self.is_closed and not self._connection.is_closed:
            for consumer_tag in list(self._consumer_tags):
                try:
                    await self._basic.cancel(consumer_tag)
                except exception.AMQPChannelError:
                    self.remove_consumer_tag(consumer_tag)
        self._consumer_tags.clear()
        self._consumer_callbacks.clear()

    async def confirm_deliveries(self) -> None:
        """Put the channel in publisher-confirm mode (Confirm.Select).

        Publishes issued while the select is in flight wait for its outcome, so the
        client's delivery-tag sequence cannot drift from the broker's.
        """
        while self._confirm_select_pending is not None:
            await self._confirm_select_pending
        if self._confirming:
            return
        pending: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._confirm_select_pending = pending
        try:
            await self.rpc_request(spec.Confirm.Select(nowait=False))
            self._confirming = True
        finally:
            self._confirm_select_pending = None
            if not pending.done():
                pending.set_result(None)

    @property
    def confirm_select_pending(self) -> asyncio.Future[None] | None:
        """The in-flight Confirm.Select, while one is awaiting its SelectOk."""
        return self._confirm_select_pending

    @property
    def expects_stale_responses(self) -> bool:
        """True while an abandoned request's reply has not yet arrived."""
        return bool(self._stale_rpc_expected)

    @property
    def flow_gate(self) -> asyncio.Future[None] | None:
        """Resolves when the broker lifts an active Channel.Flow stop."""
        return self._flow_gate

    def check_for_errors(self) -> None:
        """Raise pending connection errors, then channel errors, then closed state."""
        try:
            self._connection.check_for_errors()
        except exception.AMQPConnectionError:
            self.set_state(base.Stateful.CLOSED)
            raise
        self.check_for_exceptions()
        if self.is_closed:
            raise exception.AMQPChannelError("channel closed")

    def check_for_exceptions(self) -> None:
        """Raise the oldest recorded channel error.

        Recoverable errors (e.g. a Basic.Return) drain one per call while the channel
        is open; on a closed channel the same error re-raises forever.
        """
        if not self._recorded_errors:
            return
        if self.is_open:
            raise self._recorded_errors.popleft()
        raise self._recorded_errors[0]

    async def rpc_request(
        self,
        method: spec.Method,
        *,
        timeout: float | None = None,
        check_channel_errors: bool = True,
    ) -> spec.Method:
        """Write a synchronous method and await its matching response frame."""
        if timeout is not None:
            validation.require_positive_number("timeout", timeout)
        self._connection.ensure_current_loop()
        async with self._rpc_lock:
            if check_channel_errors:
                self.check_for_errors()
            else:
                self._connection.check_for_errors()
            waiter: asyncio.Future[spec.Method] = asyncio.get_running_loop().create_future()
            self._rpc_expected = method.valid_responses
            self._rpc_waiter = waiter
            written = False
            try:
                self._connection.write_method_frame(self._channel_id, method)
                written = True
                async with asyncio.timeout(timeout or self._rpc_timeout):
                    return await waiter
            except TimeoutError:
                self._mark_abandoned_rpc(method)
                raise exception.AMQPChannelError(
                    f"rpc request {method.name} took too long"
                ) from None
            except BaseException:
                if written and (not waiter.done() or waiter.cancelled()):
                    self._mark_abandoned_rpc(method)
                raise
            finally:
                self._rpc_waiter = None
                self._rpc_expected = ()

    def _mark_abandoned_rpc(self, method: spec.Method) -> None:
        """A written request was abandoned; the broker's answer is still on its way."""
        self._stale_rpc_expected.append(method.valid_responses)
        if isinstance(method, spec.Confirm.Select):
            self._confirming = True

    async def build_inbound_messages(
        self,
        break_on_empty: bool = False,
        empty_timeout: float | None = DEFAULT_EMPTY_TIMEOUT_SECONDS,
    ) -> AsyncGenerator[message_module.Message]:
        """Yield inbound messages as they arrive.

        With ``break_on_empty`` the generator ends once the queue stays empty: at once
        when the channel has no active consumers, otherwise after the queue has been
        continuously empty for ``empty_timeout`` seconds (a falsy timeout ends at
        once). A user-initiated close ends the generator silently; a broker- or
        network-driven close raises.
        """
        while True:
            if self._inbound:
                inbound_message = self._inbound.popleft()
                self._buffered_inbound_bytes -= len(inbound_message.body)
                self._connection.report_inbound_drain(self, len(inbound_message.body))
                yield inbound_message
                continue
            try:
                self.check_for_errors()
            except exception.AMQPError:
                if self._user_initiated_close():
                    return
                raise
            if break_on_empty:
                if not self._consumer_tags or not empty_timeout:
                    return
                if not await self._wait_for_inbound(empty_timeout):
                    return
            else:
                await self._wait_for_inbound(None)

    async def start_consuming(self) -> None:
        """Route inbound messages to their registered consumer callbacks.

        Returns when the channel closes or the last consumer is cancelled.
        """
        if not self._consumer_callbacks:
            raise exception.AMQPChannelError("no consumer callback defined")
        while not self.is_closed and self._consumer_tags:
            async for inbound_message in self.build_inbound_messages(break_on_empty=True):
                consumer_tag = inbound_message.consumer_tag or ""
                callback = self._consumer_callbacks.get(consumer_tag)
                if callback is None:
                    LOG.warning(
                        f"message for unknown consumer {consumer_tag!r} dropped "
                        f"on channel {self._channel_id}"
                    )
                    continue
                await callback(inbound_message)

    def add_consumer_tag(self, consumer_tag: str) -> None:
        if not isinstance(consumer_tag, str):
            raise exception.AMQPChannelError("consumer tag needs to be a string")
        if consumer_tag not in self._consumer_tags:
            self._consumer_tags.append(consumer_tag)

    def remove_consumer_tag(self, consumer_tag: str | None = None) -> None:
        """Forget one consumer tag, or every tag when ``None``."""
        if consumer_tag is None:
            self._consumer_tags.clear()
        elif consumer_tag in self._consumer_tags:
            self._consumer_tags.remove(consumer_tag)

    def on_frame(self, frame: framing.FrameValue) -> None:
        """Dispatch one inbound frame for this channel (wire order, single-threaded)."""
        frame_type = type(frame)
        if frame_type is bytes:
            self._on_body_chunk(frame)  # type: ignore[arg-type]
            return
        elif frame_type is framing.ContentHeader:
            self._on_content_header(frame)  # type: ignore[arg-type]
            return
        elif isinstance(frame, spec.Method):
            self._on_method(frame)
            return
        elif frame_type is framing.Heartbeat:
            return
        LOG.error(f"[Channel{self._channel_id}] Unhandled Frame: {frame!r}")

    def on_connection_terminated(self, error: exception.AMQPConnectionError) -> None:
        """Fail every pending waiter after the connection died underneath us."""
        self._close_locally(error)

    def _on_method(self, method: spec.Method) -> None:
        if method.has_content:
            if self._content_method is not None:
                LOG.warning(f"Received an out-of-order frame: {method}")
            self._content_method = method
            self._content_header = None
            self._content_body = []
            self._content_received = 0
            return
        index = method.index
        if index == _BASIC_ACK:
            self._resolve_publisher_confirm(method, result=True)
        elif index == _BASIC_NACK:
            self._resolve_publisher_confirm(method, result=False)
        elif self._is_stale_rpc_response(index):
            self._discard_stale_rpc_response(method)
        elif self._is_awaited_rpc_response(index):
            self._deliver_rpc_response(method)
        elif index == _BASIC_GET_EMPTY:
            self._deliver_empty_get()
        elif index == _BASIC_CANCEL:
            self._on_server_cancel(cast(spec.Basic.Cancel, method))
        elif index == _CHANNEL_CLOSE:
            self._on_remote_close(cast(spec.Channel.Close, method))
        elif index == _CHANNEL_FLOW:
            self._echo_flow(cast(spec.Channel.Flow, method))
        else:
            LOG.error(f"[Channel{self._channel_id}] Unhandled Frame: {method}")

    def _resolve_publisher_confirm(self, method: spec.Method, *, result: bool) -> None:
        confirmation = cast(spec.Basic.Ack | spec.Basic.Nack, method)
        self._resolve_confirms(confirmation.delivery_tag, confirmation.multiple, result=result)

    def _is_stale_rpc_response(self, index: int) -> bool:
        return bool(self._stale_rpc_expected) and index in self._stale_rpc_expected[0]

    def _discard_stale_rpc_response(self, method: spec.Method) -> None:
        self._stale_rpc_expected.pop(0)
        LOG.warning(f"discarding late rpc response {method} on channel {self._channel_id}")
        if isinstance(method, spec.Channel.OpenOk) and self.is_closing:
            self._close_abandoned_open()
        elif isinstance(method, spec.Channel.CloseOk) and self.is_closing:
            self.set_state(base.Stateful.CLOSED)

    def _close_abandoned_open(self) -> None:
        """The broker completed an open this side gave up on; shut its half down.

        Until the CloseOk lands, the channel id stays reserved -- reusing it while the
        broker still considers it open would kill the whole connection.
        """
        try:
            self._connection.write_method_frame(
                self._channel_id,
                spec.Channel.Close(
                    reply_code=spec.REPLY_SUCCESS,
                    reply_text="abandoned open",
                    class_id=0,
                    method_id=0,
                ),
            )
        except exception.AMQPError:
            self.set_state(base.Stateful.CLOSED)
            return
        self._stale_rpc_expected.append((spec.Channel.CloseOk.index,))

    def _is_awaited_rpc_response(self, index: int) -> bool:
        return self._rpc_waiter is not None and index in self._rpc_expected

    def _deliver_rpc_response(self, method: spec.Method) -> None:
        if self._rpc_waiter is not None and not self._rpc_waiter.done():
            self._rpc_waiter.set_result(method)
        self._stale_rpc_expected.clear()

    def _deliver_empty_get(self) -> None:
        if self._stale_gets:
            self._stale_gets.popleft()
            LOG.warning(f"discarding late Basic.GetEmpty on channel {self._channel_id}")
            return
        if self._get_waiter is not None and not self._get_waiter.done():
            self._get_waiter.set_result(None)

    def _on_server_cancel(self, cancel: spec.Basic.Cancel) -> None:
        LOG.warning(f"Received Basic.Cancel on consumer_tag: {cancel.consumer_tag}")
        self.remove_consumer_tag(cancel.consumer_tag)
        self._wake_inbound()

    def _echo_flow(self, flow: spec.Channel.Flow) -> None:
        if flow.active:
            self._release_flow_gate()
        elif self._flow_gate is None:
            self._flow_gate = asyncio.get_running_loop().create_future()
        self._connection.write_method_frame(
            self._channel_id, spec.Channel.FlowOk(active=flow.active)
        )

    def _release_flow_gate(self) -> None:
        gate = self._flow_gate
        self._flow_gate = None
        if gate is not None and not gate.done():
            gate.set_result(None)

    def _on_content_header(self, header: framing.ContentHeader) -> None:
        if self._content_method is None:
            LOG.warning(f"Received an out-of-order frame: {header}")
            return
        self._content_header = header
        if header.body_size == 0:
            self._complete_content()

    def _on_body_chunk(self, chunk: bytes) -> None:
        header = self._content_header
        if header is None:
            LOG.warning(f"Received an out-of-order body frame ({len(chunk)} bytes)")
            return
        self._content_body.append(chunk)
        self._content_received += len(chunk)
        self._buffered_inbound_bytes += len(chunk)
        self._connection.report_inbound_growth(self, len(chunk))
        if self._content_received >= header.body_size:
            self._complete_content()

    def _complete_content(self) -> None:
        method = self._content_method
        header = self._content_header
        self._content_method = None
        self._content_header = None
        body_parts = self._content_body
        self._content_body = []
        self._content_received = 0
        if method is None or header is None:
            return
        body = body_parts[0] if len(body_parts) == 1 else b"".join(body_parts)
        if isinstance(method, spec.Basic.Deliver):
            self._inbound.append(message_module.Message(self, body, method, header.properties))
            self._wake_inbound()
            return
        if body:
            self._buffered_inbound_bytes -= len(body)
            self._connection.report_inbound_drain(self, len(body))
        if isinstance(method, spec.Basic.GetOk):
            if self._stale_gets:
                self._discard_stale_get(method)
            elif self._get_waiter is not None and not self._get_waiter.done():
                self._get_waiter.set_result(
                    message_module.Message(self, body, method, header.properties)
                )
        elif isinstance(method, spec.Basic.Return):
            self._on_basic_return(method)

    def _discard_stale_get(self, method: spec.Basic.GetOk) -> None:
        no_ack = self._stale_gets.popleft()
        LOG.warning(f"discarding late Basic.GetOk on channel {self._channel_id}")
        if no_ack:
            return
        try:
            self._connection.write_method_frame(
                self._channel_id,
                spec.Basic.Reject(delivery_tag=method.delivery_tag, requeue=True),
            )
        except exception.AMQPError:
            pass

    def _on_basic_return(self, returned: spec.Basic.Return) -> None:
        returned_error = exception.AMQPMessageError(
            f"Message not delivered: {returned.reply_text} ({returned.reply_code}) "
            f"to queue '{returned.routing_key}' from exchange '{returned.exchange}'",
            reply_code=returned.reply_code,
        )
        LOG.warning(str(returned_error))
        self._returns_recorded += 1
        if self._return_confirm_tag is not None:
            # Basic.Return has no delivery tag. Only one return-capable confirmed
            # publish is in flight, so the error belongs exclusively to its waiter.
            self._return_error = returned_error
            return
        self._returned_errors.append(returned_error)
        self._recorded_errors.append(returned_error)

    def _on_remote_close(self, close: spec.Channel.Close) -> None:
        self.set_state(base.Stateful.CLOSING)
        if not self._connection.is_closed:
            try:
                self._connection.write_method_frame(self._channel_id, spec.Channel.CloseOk())
            except exception.AMQPError:
                pass
        self.remove_consumer_tag()
        self._drop_inbound_buffer()
        self._content_method = None
        self._content_header = None
        self._content_body = []
        self._content_received = 0
        self._stale_rpc_expected.clear()
        self._stale_gets.clear()
        if close.reply_code == spec.REPLY_SUCCESS:
            error = exception.AMQPChannelError("channel closed")
        else:
            error = exception.AMQPChannelError(
                f"Channel {self._channel_id} was closed by remote server: {close.reply_text}",
                reply_code=close.reply_code,
            )
            self._recorded_errors.append(error)
        self.set_state(base.Stateful.CLOSED)
        self._fail_waiters(error)
        self._wake_inbound()

    def allocate_delivery_tag(self) -> int:
        """Claim the next delivery tag for a confirmed publish.

        Each call consumes a tag, so it must be paired with the publish that carries
        it -- skipping one desynchronises the client from the broker's sequence.
        """
        self._next_delivery_tag += 1
        return self._next_delivery_tag

    def register_confirm(
        self, delivery_tag: int, *, may_return: bool = False
    ) -> asyncio.Future[bool]:
        waiter: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._confirm_futures[delivery_tag] = waiter
        if may_return:
            self._return_confirm_tag = delivery_tag
            self._return_error = None
            self._return_pending = asyncio.get_running_loop().create_future()
        return waiter

    @property
    def return_lock(self) -> asyncio.Lock:
        """Serialize return-capable callers without waking the entire publish queue."""
        return self._return_lock

    @property
    def return_pending(self) -> asyncio.Future[None] | None:
        """Wire-level completion of the outstanding return-capable publish.

        This survives caller cancellation/timeout: a late Return must never be
        assigned to a later publish. Only a broker confirm or teardown releases it.
        """
        return self._return_pending

    def _release_return_slot(self) -> None:
        pending = self._return_pending
        self._return_pending = None
        self._return_confirm_tag = None
        self._return_error = None
        if pending is not None and not pending.done():
            pending.set_result(None)

    @property
    def returns_recorded(self) -> int:
        """Monotonic count of Basic.Returns seen, including publish-owned returns."""
        return self._returns_recorded

    def pop_returned_error(self) -> exception.AMQPMessageError | None:
        """Claim the oldest Basic.Return not owned by a confirmed publish."""
        if not self._returned_errors:
            return None
        returned_error = self._returned_errors.popleft()
        if returned_error in self._recorded_errors:
            self._recorded_errors.remove(returned_error)
        return returned_error

    def register_consumer_callback(self, consumer_tag: str, callback: Any) -> None:
        self._consumer_callbacks[consumer_tag] = callback

    def register_get_waiter(self) -> asyncio.Future[message_module.Message | None]:
        waiter: asyncio.Future[message_module.Message | None] = (
            asyncio.get_running_loop().create_future()
        )
        self._get_waiter = waiter
        return waiter

    def clear_get_waiter(self) -> None:
        self._get_waiter = None

    def mark_stale_get(self, no_ack: bool) -> None:
        """Record an abandoned Basic.Get whose answer is still on its way."""
        self._stale_gets.append(no_ack)

    def discard_confirm(self, delivery_tag: int, *, written: bool = False) -> None:
        """Withdraw a waiter, retaining return ownership if its publish was written."""
        waiter = self._confirm_futures.pop(delivery_tag, None)
        if waiter is not None and not waiter.done():
            waiter.cancel()
        if not written and delivery_tag == self._return_confirm_tag:
            self._release_return_slot()

    def _resolve_confirm(self, delivery_tag: int, *, result: bool) -> None:
        returned_error = None
        if delivery_tag == self._return_confirm_tag:
            returned_error = self._return_error
            self._release_return_slot()
        waiter = self._confirm_futures.pop(delivery_tag, None)
        if waiter is not None and not waiter.done():
            if returned_error is not None:
                waiter.set_exception(returned_error)
            else:
                waiter.set_result(result)

    def _resolve_confirms(self, delivery_tag: int, multiple: bool, *, result: bool) -> None:
        if multiple:
            if delivery_tag == 0:
                resolved = list(self._confirm_futures)
            else:
                resolved = []
                for tag in self._confirm_futures:
                    if tag > delivery_tag:
                        break
                    resolved.append(tag)
            for tag in resolved:
                self._resolve_confirm(tag, result=result)
            # An abandoned caller has no waiter, but still owns its late Return.
            return_tag = self._return_confirm_tag
            if return_tag is not None and (delivery_tag == 0 or return_tag <= delivery_tag):
                self._resolve_confirm(return_tag, result=result)
            return
        self._resolve_confirm(delivery_tag, result=result)

    def _fail_waiters(self, error: exception.AMQPError) -> None:
        for waiter in (self._rpc_waiter, self._get_waiter):
            if waiter is not None and not waiter.done():
                waiter.set_exception(error)
        for confirm_waiter in self._confirm_futures.values():
            if not confirm_waiter.done():
                confirm_waiter.set_exception(error)
        self._confirm_futures.clear()
        self._release_return_slot()
        self._release_flow_gate()

    async def _wait_for_inbound(self, timeout: float | None) -> bool:
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._inbound_waiters.add(waiter)
        try:
            if timeout is None:
                await waiter
                return True
            async with asyncio.timeout(timeout):
                await waiter
            return True
        except TimeoutError:
            return False
        finally:
            self._inbound_waiters.discard(waiter)

    def _drop_inbound_buffer(self) -> None:
        """Discard buffered and half-assembled inbound bytes, releasing their backlog."""
        self._inbound.clear()
        self._content_method = None
        self._content_header = None
        self._content_body.clear()
        self._content_received = 0
        released = self._buffered_inbound_bytes
        self._buffered_inbound_bytes = 0
        if released:
            self._connection.report_inbound_drain(self, released)

    def _wake_inbound(self) -> None:
        for waiter in self._inbound_waiters:
            if not waiter.done():
                waiter.set_result(None)

    def _user_initiated_close(self) -> bool:
        return self._user_closed or self._connection.user_closed

    def _close_locally(self, error: exception.AMQPError | None = None) -> None:
        self._consumer_tags.clear()
        self._consumer_callbacks.clear()
        self._drop_inbound_buffer()
        self._stale_rpc_expected.clear()
        self._stale_gets.clear()
        self.set_state(base.Stateful.CLOSED)
        self._fail_waiters(error or exception.AMQPChannelError("channel closed"))
        self._wake_inbound()
