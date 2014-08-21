import asyncio
import logging
import random
from collections.abc import Sequence
from types import TracebackType
from typing import Any

from amqpstorm_aio import base
from amqpstorm_aio import channel as channel_module
from amqpstorm_aio import channel0 as channel0_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import heartbeat as heartbeat_module
from amqpstorm_aio import io as io_module
from amqpstorm_aio import spec
from amqpstorm_aio import validation

LOG = logging.getLogger(__name__)

DEFAULT_PORT = 5672
DEFAULT_SSL_PORT = 5671
DEFAULT_VIRTUAL_HOST = "/"
DEFAULT_HEARTBEAT_TIMEOUT = 60
DEFAULT_SOCKET_TIMEOUT = 10.0
DEFAULT_LOCALE = "en_US"
DEFAULT_RPC_TIMEOUT = 60

# Buffered inbound message bytes at which reading pauses, and resumes again. TCP
# backpressure then reaches the broker instead of this process buffering unboundedly.
INBOUND_BACKLOG_HIGH_WATER = 512 * 1024 * 1024
INBOUND_BACKLOG_LOW_WATER = INBOUND_BACKLOG_HIGH_WATER // 2


def _normalize_hostnames(hostname: str | Sequence[str]) -> tuple[str, ...]:
    """Accept one hostname or a sequence of them, always returning a tuple."""
    if isinstance(hostname, str):
        validation.require_string("hostname", hostname)
        return (hostname,)
    if isinstance(hostname, (bytes, bytearray)) or not isinstance(hostname, Sequence):
        raise exception.AMQPInvalidArgument(
            "hostname should be a string or a sequence of strings"
        )
    hostnames = tuple(hostname)
    if not hostnames:
        raise exception.AMQPInvalidArgument("hostname should not be an empty sequence")
    for entry in hostnames:
        validation.require_string("hostname", entry)
    return hostnames


def _shuffled_hostnames(hostname: str | Sequence[str]) -> tuple[str, ...]:
    """Normalize the hostname argument into the order this connection will try.

    Cluster nodes are peers, so every client starting at the same one would send the
    whole fleet to a single node. Each connection draws its own order.
    """
    hostnames = _normalize_hostnames(hostname)
    if len(hostnames) == 1:
        return hostnames
    return tuple(random.sample(hostnames, len(hostnames)))


def _validate_parameters(
    *,
    username: str,
    password: str,
    virtual_host: str,
    port: int,
    heartbeat: int,
    timeout: float,
    ssl: bool,
    ssl_options: dict[str, Any] | None,
    client_properties: dict[str, Any] | None,
    locale: str,
) -> None:
    validation.require_shortstr("username", username)
    validation.require_shortstr("password", password)
    validation.require_domain("path", "virtual_host", virtual_host)
    validation.require_short("port", port)
    validation.require_short("heartbeat", heartbeat)
    validation.require_positive_number("timeout", timeout)
    validation.require_boolean("ssl", ssl)
    validation.require_table("ssl_options", ssl_options)
    validation.require_table("client_properties", client_properties)
    validation.require_shortstr("locale", locale)


class Connection(base.Stateful):
    """One AMQP connection over asyncio."""

    __slots__ = (
        "__weakref__",
        "_channel0",
        "_channels",
        "_closed_reason",
        "_connect_attempt",
        "_heartbeat",
        "_inbound_backlog_bytes",
        "_io",
        "_last_channel_id",
        "_loop",
        "_parser",
        "_read_paused",
        "_user_closed",
        "parameters",
    )

    def __init__(
        self,
        hostname: str | Sequence[str],
        username: str,
        password: str,
        port: int = DEFAULT_PORT,
        *,
        virtual_host: str = DEFAULT_VIRTUAL_HOST,
        heartbeat: int = DEFAULT_HEARTBEAT_TIMEOUT,
        timeout: float = DEFAULT_SOCKET_TIMEOUT,
        ssl: bool = False,
        ssl_options: dict[str, Any] | None = None,
        client_properties: dict[str, Any] | None = None,
        locale: str = DEFAULT_LOCALE,
    ) -> None:
        super().__init__()
        hostnames = _shuffled_hostnames(hostname)
        _validate_parameters(
            username=username,
            password=password,
            virtual_host=virtual_host,
            port=port,
            heartbeat=heartbeat,
            timeout=timeout,
            ssl=ssl,
            ssl_options=ssl_options,
            client_properties=client_properties,
            locale=locale,
        )
        self.parameters: dict[str, Any] = {
            "hostname": hostnames[0],
            "hostnames": hostnames,
            "username": username,
            "password": password,
            "port": port,
            "virtual_host": virtual_host,
            "heartbeat": heartbeat,
            "timeout": timeout,
            "ssl": ssl,
            "ssl_options": ssl_options or {},
            "client_properties": client_properties or {},
            "locale": locale,
        }
        self._channel0 = channel0_module.Channel0(self)
        self._channels: dict[int, channel_module.Channel] = {}
        self._last_channel_id = 0
        self._connect_attempt = 0
        self._closed_reason: exception.AMQPConnectionError | None = None
        self._user_closed = False
        self._inbound_backlog_bytes = 0
        self._read_paused = False
        self._loop: asyncio.AbstractEventLoop | None = None
        self._io = io_module.IO(self._on_data, self._on_connection_lost)
        self._parser = framing.FrameParser(self._dispatch, channel0_module.MAX_FRAME_SIZE)
        self._heartbeat = heartbeat_module.Heartbeat(
            heartbeat, self._channel0.send_heartbeat, self._on_heartbeat_dead
        )

    async def __aenter__(self) -> Connection:
        if self.is_closed:
            await self.open()
        return self

    async def __aexit__(
        self,
        exception_type: type[Exception] | None,
        exception_value: Exception | None,
        traceback: TracebackType | None,
    ) -> None:
        if exception_value is not None:
            LOG.warning(f"Closing connection due to an unhandled exception: {exception_value}")
        await self.close()

    @property
    def is_blocked(self) -> bool:
        """True while the broker has this connection flow-blocked."""
        return self._channel0.is_blocked

    @property
    def user_closed(self) -> bool:
        """True when the local application initiated the close."""
        return self._user_closed

    @property
    def peer_address(self) -> tuple[str, int] | None:
        """The address actually connected to, or None while closed.

        Worth logging when the hostname does not identify the node: a list of hosts or
        a name with several records both leave the chosen one implicit.
        """
        transport = self._io.transport
        if transport is None:
            return None
        peername = transport.get_extra_info("peername")
        if not peername:
            return None
        return peername[0], peername[1]

    @property
    def server_properties(self) -> dict[str, Any]:
        """Properties announced by the broker in Connection.Start."""
        return self._channel0.server_properties

    @property
    def max_allowed_channels(self) -> int:
        return self._channel0.max_allowed_channels

    @property
    def max_frame_size(self) -> int:
        return self._channel0.max_frame_size

    @property
    def channels(self) -> dict[int, channel_module.Channel]:
        return self._channels

    @property
    def inbound_backlog_bytes(self) -> int:
        """Inbound message bytes buffered across this connection's channels."""
        return self._inbound_backlog_bytes

    @property
    def is_read_paused(self) -> bool:
        """True while reads are paused because the inbound backlog hit its high water."""
        return self._read_paused

    async def open(self) -> None:
        """Connect, negotiate, and start heartbeats; reusable after ``close``."""
        if not self.is_closed:
            raise exception.AMQPConnectionError("connection already open")
        LOG.debug("Connection opening")
        self.set_state(base.Stateful.OPENING)
        self._loop = asyncio.get_running_loop()
        self._start_new_session()
        attempt_io = self._io
        attempt_channel0 = self._channel0
        parameters = self.parameters
        try:
            await attempt_io.open(
                self._connect_order(),
                parameters["port"],
                parameters["timeout"],
                parameters["ssl"],
                parameters["ssl_options"],
            )
            self._ensure_attempt_current(attempt_io)
            attempt_io.write(framing.PROTOCOL_HEADER)
            await attempt_channel0.negotiate(parameters["timeout"])
            self._ensure_attempt_current(attempt_io)
        except exception.AMQPError as error:
            connection_error = (
                error
                if isinstance(error, exception.AMQPConnectionError)
                else exception.AMQPConnectionError(str(error))
            )
            self._abandon_attempt(attempt_io, connection_error)
            raise connection_error from error
        except BaseException:
            self._abandon_attempt(
                attempt_io, exception.AMQPConnectionError("connection open aborted")
            )
            raise
        self._parser.max_frame_size = self._channel0.max_frame_size
        self.set_state(base.Stateful.OPEN)
        self._heartbeat.apply_interval(self._channel0.negotiated_heartbeat)
        self._heartbeat.start()
        peer = self.peer_address
        where = f"{peer[0]}:{peer[1]}" if peer else "an unknown address"
        LOG.debug(f"Connection open to {where}")

    def _ensure_attempt_current(self, attempt_io: io_module.IO) -> None:
        """Stop this open() attempt if the connection was closed or reopened meanwhile."""
        if self._user_closed or self._io is not attempt_io:
            raise exception.AMQPConnectionError("connection was closed during open")

    def _abandon_attempt(
        self, attempt_io: io_module.IO, error: exception.AMQPConnectionError
    ) -> None:
        """Fail a dying open() attempt without touching a newer session's state."""
        if self._io is attempt_io:
            self._terminate(error)
        attempt_io.abort()

    async def close(self) -> None:
        """Cleanly close every channel and then the connection itself."""
        if self.is_closed:
            return
        LOG.debug("Connection closing")
        self._user_closed = True
        if self.is_opening:
            self._terminate(exception.AMQPConnectionError("connection was closed during open"))
            LOG.debug("Connection closed")
            return
        self.set_state(base.Stateful.CLOSING)
        self._heartbeat.stop()
        for open_channel in list(self._channels.values()):
            if not open_channel.is_closed:
                try:
                    await open_channel.close()
                except exception.AMQPError:
                    pass
        try:
            await self._channel0.close(self.parameters["timeout"])
        finally:
            self._io.close()
            self.set_state(base.Stateful.CLOSED)
        LOG.debug("Connection closed")

    async def channel(self, rpc_timeout: int = DEFAULT_RPC_TIMEOUT) -> channel_module.Channel:
        """Open a new channel on this connection."""
        validation.require_positive_integer("rpc_timeout", rpc_timeout)
        self.ensure_current_loop()
        self.check_for_errors()
        if not self.is_open:
            raise exception.AMQPConnectionError("connection closed")
        channel_id = self._get_next_available_channel_id()
        new_channel = channel_module.Channel(channel_id, self, rpc_timeout)
        self._channels[channel_id] = new_channel
        try:
            await new_channel.open()
        except BaseException:
            if new_channel.expects_stale_responses:
                new_channel.set_state(base.Stateful.CLOSING)
            else:
                new_channel.set_state(base.Stateful.CLOSED)
                self._channels.pop(channel_id, None)
            raise
        return new_channel

    def reclaim_channel_id(self, channel: channel_module.Channel) -> None:
        """Re-register a reopening channel; its id must not have been reissued."""
        channel_id = channel.channel_id
        current = self._channels.get(channel_id)
        if current is channel:
            return
        if current is not None and not current.is_closed:
            raise exception.AMQPChannelError(
                f"channel id {channel_id} has been reissued to another channel"
            )
        self._channels[channel_id] = channel

    def _start_new_session(self) -> None:
        """Replace the per-connection state so a closed Connection can be reopened."""
        self._closed_reason = None
        self._user_closed = False
        self._channels = {}
        self._last_channel_id = 0
        self._inbound_backlog_bytes = 0
        self._read_paused = False
        self._channel0 = channel0_module.Channel0(self)
        self._io = io_module.IO(self._on_data, self._on_connection_lost)
        self._parser = framing.FrameParser(self._dispatch, channel0_module.MAX_FRAME_SIZE)
        self._heartbeat = heartbeat_module.Heartbeat(
            self.parameters["heartbeat"], self._channel0.send_heartbeat, self._on_heartbeat_dead
        )

    def _connect_order(self) -> tuple[str, ...]:
        hostnames: tuple[str, ...] = self.parameters["hostnames"]
        start = self._connect_attempt % len(hostnames)
        self._connect_attempt += 1
        return hostnames[start:] + hostnames[:start]

    def check_for_errors(self) -> None:
        """Raise the recorded connection error, or 'connection closed' when closed."""
        if self._closed_reason is not None:
            raise self._closed_reason
        if self.is_closed:
            raise exception.AMQPConnectionError("connection closed")

    def ensure_current_loop(self) -> None:
        """Refuse use from a different event loop than the one that opened us.

        Cross-loop futures never resolve and cross-loop writes race the reader
        thread-unsafely, so failing loudly beats the silent stall.
        """
        if self._loop is not None and asyncio.get_running_loop() is not self._loop:
            raise exception.AMQPConnectionError(
                "this connection belongs to another event loop"
            )

    def report_inbound_growth(self, channel: channel_module.Channel, size: int) -> None:
        """A channel buffered ``size`` more inbound body bytes; pause reads past the mark."""
        if self._channels.get(channel.channel_id) is not channel:
            return
        self._inbound_backlog_bytes += size
        if not self._read_paused and self._inbound_backlog_bytes > INBOUND_BACKLOG_HIGH_WATER:
            self._read_paused = True
            self._heartbeat.reads_paused = True
            self._io.pause_reading()
            LOG.warning(
                f"inbound backlog reached {self._inbound_backlog_bytes} bytes; "
                f"pausing reads until consumers catch up"
            )

    def report_inbound_drain(self, channel: channel_module.Channel, size: int) -> None:
        """A channel released ``size`` buffered inbound body bytes; resume when drained."""
        if self._channels.get(channel.channel_id) is not channel:
            return
        self._inbound_backlog_bytes -= size
        if self._read_paused and self._inbound_backlog_bytes <= INBOUND_BACKLOG_LOW_WATER:
            self._read_paused = False
            self._heartbeat.reads_paused = False
            self._io.resume_reading()
            LOG.debug("inbound backlog drained; resuming reads")

    def write_method_frame(self, channel_id: int, method: spec.Method) -> None:
        """Marshal and write one method frame."""
        buffer = bytearray()
        framing.append_method_frame(buffer, channel_id, method)
        if len(buffer) > self.max_frame_size:
            raise exception.AMQPInvalidArgument(
                f"{method.name} frame of {len(buffer)} bytes exceeds "
                f"the negotiated frame maximum {self.max_frame_size}"
            )
        self.write_bytes(buffer)

    def write_bytes(self, data: bytes | bytearray) -> None:
        """Write pre-marshalled frame bytes; counts as one heartbeat write."""
        self._heartbeat.register_write()
        self._io.write(data)

    async def drain(self) -> None:
        """Backpressure hook: wait until the transport write buffer drains."""
        await self._io.drain()

    def on_remote_close(self, error: exception.AMQPConnectionError | None) -> None:
        """Called by channel0 when the broker closes the connection."""
        self._terminate(error)

    def _on_heartbeat_dead(self, error: exception.AMQPConnectionError) -> None:
        LOG.error(str(error))
        self._terminate(error)

    def _dispatch(self, channel_id: int, frame: framing.FrameValue) -> None:
        if channel_id == 0:
            self._channel0.on_frame(frame)
            return
        target = self._channels.get(channel_id)
        if target is None:
            LOG.error(f"received frame for unknown channel {channel_id}: {frame!r}")
            return
        target.on_frame(frame)

    def _on_data(self, data: bytes) -> None:
        self._heartbeat.register_read()
        try:
            self._parser.feed(data)
        except exception.AMQPConnectionError as error:
            self._terminate(error)
        except Exception as error:  # noqa
            LOG.exception("inbound frame processing failed")
            frame_error = exception.AMQPFrameError(f"failed to decode inbound frame: {error!r}")
            frame_error.__cause__ = error
            self._terminate(frame_error)

    def _on_connection_lost(self, transport_error: Exception | None) -> None:
        if self._user_closed or self.is_closed:
            self._terminate(None)
            return
        message = "connection closed"
        if transport_error is not None:
            message = f"connection closed: {transport_error}"
        self._terminate(exception.AMQPConnectionError(message))

    def _terminate(self, error: exception.AMQPConnectionError | None) -> None:
        """Single teardown path: record the reason, fail every waiter, drop the socket."""
        if self.is_closed and self._closed_reason is not None:
            return
        if error is not None and self._closed_reason is None:
            self._closed_reason = error
        self.set_state(base.Stateful.CLOSED)
        self._heartbeat.stop()
        failure = error or exception.AMQPConnectionError("connection closed")
        self._channel0.fail_waiters(failure)
        for open_channel in self._channels.values():
            open_channel.on_connection_terminated(failure)
        if error is None:
            self._io.close()
        else:
            self._io.abort()

    def _get_next_available_channel_id(self) -> int:
        limit = self._channel0.max_allowed_channels
        for channel_id in range(1, limit + 1):
            existing = self._channels.get(channel_id)
            if existing is None or existing.is_closed:
                self._last_channel_id = channel_id
                return channel_id
        raise exception.AMQPConnectionError(
            f"reached the maximum number of channels {limit}"
        )
