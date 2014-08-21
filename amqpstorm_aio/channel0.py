import asyncio
import logging
import platform
from typing import TYPE_CHECKING
from typing import Any

import amqpstorm_aio
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec

if TYPE_CHECKING:
    from amqpstorm_aio.connection import Connection

LOG = logging.getLogger(__name__)

MAX_CHANNELS = 65_535
MAX_FRAME_SIZE = 131_072
_MAX_SECURE_ROUNDS = 10


def _negotiate(server_value: int, client_value: int) -> int:
    return min(server_value, client_value) or client_value


def _negotiate_heartbeat(server_value: int, client_value: int) -> int:
    """RabbitMQ's rule: lower of two non-zero values; zero yields to the other side."""
    if server_value and client_value:
        return min(server_value, client_value)
    return max(server_value, client_value)


class Channel0:
    """Handles all traffic on channel 0 for one connection."""

    __slots__ = (
        "__weakref__",
        "_auth_response",
        "_close_ok_waiter",
        "_connection",
        "_expected_indexes",
        "_frame_waiter",
        "is_blocked",
        "max_allowed_channels",
        "max_frame_size",
        "negotiated_heartbeat",
        "server_properties",
    )

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._expected_indexes: tuple[int, ...] = ()
        self._frame_waiter: asyncio.Future[spec.Method] | None = None
        self._close_ok_waiter: asyncio.Future[None] | None = None
        self._auth_response = b""
        self.is_blocked = False
        self.max_allowed_channels = MAX_CHANNELS
        self.max_frame_size = MAX_FRAME_SIZE
        self.negotiated_heartbeat: int = connection.parameters["heartbeat"]
        self.server_properties: dict[str, Any] = {}

    def on_frame(self, frame: framing.FrameValue) -> None:
        """Dispatch one channel-0 frame."""
        if type(frame) is framing.Heartbeat:
            return
        if isinstance(frame, spec.Connection.Close):
            self._on_server_close(frame)
        elif isinstance(frame, spec.Connection.CloseOk):
            if self._close_ok_waiter is not None and not self._close_ok_waiter.done():
                self._close_ok_waiter.set_result(None)
        elif isinstance(frame, spec.Connection.Blocked):
            self.is_blocked = True
            LOG.warning(f"Connection is blocked by remote server: {frame.reason}")
        elif isinstance(frame, spec.Connection.Unblocked):
            self.is_blocked = False
            LOG.info("Connection is no longer blocked by remote server")
        elif (
            isinstance(frame, spec.Method)
            and frame.index in self._expected_indexes
            and self._frame_waiter is not None
        ):
            if not self._frame_waiter.done():
                self._frame_waiter.set_result(frame)
        else:
            LOG.error(f"[Channel0] Unhandled Frame: {frame!r}")

    async def negotiate(self, timeout: float) -> None:
        """Run Start/Tune/Open; on return the connection is protocol-level OPEN."""
        try:
            async with asyncio.timeout(timeout):
                await self._authenticate()
                await self._agree_limits()
                await self._open_virtual_host()
        except TimeoutError:
            raise exception.AMQPConnectionError(
                f"connection handshake timed out after {timeout}s"
            ) from None
        finally:
            self._frame_waiter = None
            self._expected_indexes = ()

    async def _authenticate(self) -> None:
        connection = self._connection
        start = await self._expect(spec.Connection.Start)
        self.server_properties = start.server_properties or {}
        mechanism, response = self._select_authentication(start.mechanisms)
        self._auth_response = response
        connection.write_method_frame(
            0,
            spec.Connection.StartOk(
                client_properties=self._client_properties(),
                mechanism=mechanism,
                response=response,
                locale=connection.parameters["locale"],
            ),
        )

    async def _agree_limits(self) -> None:
        connection = self._connection
        tune = await self._expect(spec.Connection.Tune, spec.Connection.Secure)
        secure_rounds = 0
        while isinstance(tune, spec.Connection.Secure):
            secure_rounds += 1
            if secure_rounds > _MAX_SECURE_ROUNDS:
                raise exception.AMQPConnectionError(
                    f"broker demanded more than {_MAX_SECURE_ROUNDS} security challenges"
                )
            connection.write_method_frame(
                0, spec.Connection.SecureOk(response=self._auth_response)
            )
            tune = await self._expect(spec.Connection.Tune, spec.Connection.Secure)
        self.max_allowed_channels = _negotiate(tune.channel_max, MAX_CHANNELS)
        self.max_frame_size = _negotiate(tune.frame_max, MAX_FRAME_SIZE)
        if self.max_frame_size < spec.FRAME_MIN_SIZE:
            raise exception.AMQPConnectionError(
                f"broker proposed frame_max {tune.frame_max} below the "
                f"spec minimum {spec.FRAME_MIN_SIZE}"
            )
        self.negotiated_heartbeat = _negotiate_heartbeat(
            tune.heartbeat, connection.parameters["heartbeat"]
        )
        connection.write_method_frame(
            0,
            spec.Connection.TuneOk(
                channel_max=self.max_allowed_channels,
                frame_max=self.max_frame_size,
                heartbeat=self.negotiated_heartbeat,
            ),
        )

    async def _open_virtual_host(self) -> None:
        connection = self._connection
        connection.write_method_frame(
            0, spec.Connection.Open(virtual_host=connection.parameters["virtual_host"])
        )
        await self._expect(spec.Connection.OpenOk)

    async def close(self, timeout: float) -> None:
        """Send Connection.Close(200) and wait for CloseOk (best effort on timeout)."""
        pending = self._close_ok_waiter
        if pending is not None:
            try:
                async with asyncio.timeout(timeout):
                    await pending
            except TimeoutError, exception.AMQPConnectionError:
                pass
            return
        self._close_ok_waiter = asyncio.get_running_loop().create_future()
        try:
            self._connection.write_method_frame(
                0,
                spec.Connection.Close(
                    reply_code=spec.REPLY_SUCCESS,
                    reply_text="Normal shutdown",
                    class_id=0,
                    method_id=0,
                ),
            )
            async with asyncio.timeout(timeout):
                await self._close_ok_waiter
        except TimeoutError, exception.AMQPConnectionError:
            pass
        finally:
            self._close_ok_waiter = None

    def send_heartbeat(self) -> None:
        """Write one heartbeat frame; a no-op unless the connection is open."""
        if not self._connection.is_open:
            return
        try:
            self._connection.write_bytes(framing.HEARTBEAT_FRAME)
        except exception.AMQPConnectionError:
            pass

    def fail_waiters(self, error: exception.AMQPConnectionError) -> None:
        """Fail any pending negotiate/close waiters after a connection error."""
        for waiter in (self._frame_waiter, self._close_ok_waiter):
            if waiter is not None and not waiter.done():
                waiter.set_exception(error)

    async def _expect(self, *method_classes: type[spec.Method]) -> Any:
        if self._connection.is_closed:
            raise exception.AMQPConnectionError("connection closed")
        self._expected_indexes = tuple(method_class.index for method_class in method_classes)
        loop = asyncio.get_running_loop()
        self._frame_waiter = loop.create_future()
        try:
            return await self._frame_waiter
        finally:
            self._frame_waiter = None
            self._expected_indexes = ()

    def _on_server_close(self, frame: spec.Connection.Close) -> None:
        try:
            self._connection.write_method_frame(0, spec.Connection.CloseOk())
        except exception.AMQPError:
            pass
        if frame.reply_code == spec.REPLY_SUCCESS:
            self._connection.on_remote_close(None)
            return
        self._connection.on_remote_close(
            exception.AMQPConnectionError(
                f"Connection was closed by remote server: {frame.reply_text}",
                reply_code=frame.reply_code,
            )
        )

    def _select_authentication(self, mechanisms: bytes) -> tuple[str, bytes]:
        offered = mechanisms.split()
        parameters = self._connection.parameters
        username: str = parameters["username"]
        password: str = parameters["password"]
        plain_offered = b"PLAIN" in offered
        explicit_credentials = (username, password) != ("guest", "guest")
        if b"EXTERNAL" in offered and not (plain_offered and explicit_credentials):
            return "EXTERNAL", b"\x00\x00"
        if plain_offered:
            return "PLAIN", f"\x00{username}\x00{password}".encode()
        raise exception.AMQPConnectionError(
            f"Unsupported Security Mechanism(s): {mechanisms.decode(errors='replace')}"
        )

    def _client_properties(self) -> dict[str, Any]:
        properties: dict[str, Any] = {
            "product": "amqpstorm-aio",
            "platform": (
                f"Python {platform.python_version()} ({platform.python_implementation()})"
            ),
            "capabilities": {
                "basic.nack": True,
                "connection.blocked": True,
                "publisher_confirms": True,
                "consumer_cancel_notify": True,
                "authentication_failure_close": True,
            },
            "information": "See https://github.com/eandersson/amqpstorm-aio",
            "version": amqpstorm_aio.__version__,
        }
        properties.update(self._connection.parameters["client_properties"])
        return properties
