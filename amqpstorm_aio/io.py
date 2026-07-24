import asyncio
import logging
import random
import socket
import ssl as ssl_module
import warnings
import weakref
from collections.abc import Callable
from collections.abc import Sequence
from typing import Any
from typing import cast

from amqpstorm_aio import exception

LOG = logging.getLogger(__name__)

MAX_SINGLE_WRITE = 8 * 1024 * 1024

_VERIFY_MODES = {
    "none": ssl_module.CERT_NONE,
    "optional": ssl_module.CERT_OPTIONAL,
    "required": ssl_module.CERT_REQUIRED,
}
_VERIFY_STRICTNESS = {
    ssl_module.CERT_NONE: 0,
    ssl_module.CERT_OPTIONAL: 1,
    ssl_module.CERT_REQUIRED: 2,
}


def _resolve_verify_mode(verify_mode: Any) -> Any:
    if not isinstance(verify_mode, str):
        return verify_mode
    resolved = _VERIFY_MODES.get(verify_mode.strip().lower().removeprefix("cert_"))
    if resolved is None:
        LOG.warning(f"unknown verify_mode {verify_mode!r}, keeping CERT_REQUIRED")
        return ssl_module.CERT_REQUIRED
    return resolved


def _requested_verify_mode(ssl_options: dict[str, Any]) -> Any:
    """The verify mode, taking the stricter when both spellings are present.

    ``verify_mode`` and ``cert_reqs`` are aliases; picking whichever appears would let
    a weaker one win by name alone, so the more restrictive is kept.
    """
    resolved = [
        _resolve_verify_mode(ssl_options[key])
        for key in ("verify_mode", "cert_reqs")
        if ssl_options.get(key) is not None
    ]
    if not resolved:
        return None
    return max(resolved, key=lambda mode: _VERIFY_STRICTNESS.get(mode, 2))


def build_ssl_context(ssl_options: dict[str, Any]) -> ssl_module.SSLContext:
    """Build an SSLContext from AMQPStorm-style ``ssl_options``.

    A caller-supplied ``context`` wins outright. Otherwise the context starts from
    ``ssl.create_default_context`` (secure defaults: TLS client, CERT_REQUIRED,
    hostname checking on) and is loosened only where explicitly asked.
    """
    context = ssl_options.get("context")
    if context is not None:
        if not isinstance(context, ssl_module.SSLContext):
            raise exception.AMQPInvalidArgument("ssl_options['context'] should be an SSLContext")
        return context
    context = ssl_module.create_default_context(cafile=ssl_options.get("ca_certs"))
    verify_mode = _requested_verify_mode(ssl_options)
    if verify_mode is not None:
        if verify_mode == ssl_module.CERT_NONE:
            context.check_hostname = False
        context.verify_mode = verify_mode
    if "check_hostname" in ssl_options:
        context.check_hostname = bool(ssl_options["check_hostname"])
    certfile = ssl_options.get("certfile")
    if certfile:
        context.load_cert_chain(certfile, ssl_options.get("keyfile"))
    return context


class _AMQPProtocol(asyncio.Protocol):
    """Thin adapter forwarding transport events to the owning IO object.

    The IO is held weakly: an unclosed Connection that gets garbage collected would
    otherwise be pinned forever by its own transport. IO.__del__ does the warning;
    this side reaps the socket on the next transport event, which is the first moment
    the loop can safely abort it.
    """

    __slots__ = ("_io_ref", "_transport")

    def __init__(self, io: IO) -> None:
        self._io_ref = weakref.ref(io)
        self._transport: asyncio.Transport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self._transport = cast(asyncio.Transport, transport)

    def _owner(self) -> IO | None:
        io = self._io_ref()
        if io is None and self._transport is not None:
            transport, self._transport = self._transport, None
            transport.abort()
        return io

    def data_received(self, data: bytes) -> None:
        io = self._owner()
        if io is not None:
            io.on_data(data)

    def connection_lost(self, exc: Exception | None) -> None:
        io = self._io_ref()
        if io is not None:
            io.transport_lost(exc)

    def pause_writing(self) -> None:
        io = self._owner()
        if io is not None:
            io.write_paused = True

    def resume_writing(self) -> None:
        io = self._owner()
        if io is None:
            return
        io.write_paused = False
        for waiter in io.drain_waiters:
            if not waiter.done():
                waiter.set_result(None)
        io.drain_waiters.clear()


class IO:
    """Owns the asyncio transport for one connection."""

    __slots__ = (
        "__weakref__",
        "drain_waiters",
        "on_connection_lost",
        "on_data",
        "transport",
        "write_paused",
    )

    def __init__(
        self,
        on_data: Callable[[bytes], None],
        on_connection_lost: Callable[[Exception | None], None],
    ) -> None:
        self.on_data = on_data
        self.on_connection_lost = on_connection_lost
        self.transport: asyncio.Transport | None = None
        self.write_paused = False
        self.drain_waiters: list[asyncio.Future[None]] = []

    def __del__(self, _warn: Callable[..., None] = warnings.warn) -> None:
        if self.transport is not None:
            _warn(
                "unclosed amqpstorm-aio Connection was garbage collected; "
                "await connection.close() or use it as an async context manager",
                ResourceWarning,
                source=self,
            )

    async def open(
        self,
        hostnames: Sequence[str],
        port: int,
        timeout: float,
        use_ssl: bool,
        ssl_options: dict[str, Any],
    ) -> None:
        """Connect to the first reachable address, optionally over TLS.

        Each hostname is resolved and its addresses shuffled, so clients sharing one
        round-robin name spread across the nodes behind it instead of all taking
        whichever address the resolver happens to list first. Every address gets its
        own ``timeout``, and the error names each one that was tried.

        TLS verifies the certificate against the hostname, never against the address it
        resolved to, so shuffling here cannot weaken it.
        """
        ssl_context = self._build_context(use_ssl, ssl_options)
        loop = asyncio.get_running_loop()
        failures: list[str] = []
        for hostname in hostnames:
            server_hostname = None
            if use_ssl:
                server_hostname = ssl_options.get("server_hostname") or hostname
                if not isinstance(server_hostname, str) or not server_hostname.strip():
                    raise exception.AMQPConnectionError(
                        "TLS requires a non-empty hostname or "
                        "ssl_options['server_hostname']; an empty name would disable "
                        "certificate hostname verification"
                    )
            for family, address in await self._resolve(loop, hostname, port, failures):
                transport = await self._connect(
                    loop, hostname, family, address, timeout, ssl_context, server_hostname, failures
                )
                if transport is not None:
                    self.transport = transport
                    return
        raise exception.AMQPConnectionError("could not connect to " + "; ".join(failures))

    @staticmethod
    def _build_context(
        use_ssl: bool, ssl_options: dict[str, Any]
    ) -> ssl_module.SSLContext | None:
        if not use_ssl:
            return None
        try:
            return build_ssl_context(ssl_options)
        except (OSError, ssl_module.SSLError, ValueError) as error:
            raise exception.AMQPConnectionError(f"invalid TLS configuration: {error}") from error

    async def _connect(
        self,
        loop: asyncio.AbstractEventLoop,
        hostname: str,
        family: int,
        address: Any,
        timeout: float,
        ssl_context: ssl_module.SSLContext | None,
        server_hostname: str | None,
        failures: list[str],
    ) -> asyncio.Transport | None:
        """Connect to one resolved address; returns None and records why on failure."""
        where = f"{hostname} [{address[0]}]:{address[1]}"
        LOG.debug(f"connecting to {where}...")
        try:
            async with asyncio.timeout(timeout):
                transport, _protocol = await loop.create_connection(
                    lambda: _AMQPProtocol(self),
                    host=address[0],
                    port=address[1],
                    family=family,
                    ssl=ssl_context,
                    server_hostname=server_hostname,
                )
        except TimeoutError:
            failures.append(f"{where} (timed out after {timeout}s)")
            return None
        except (OSError, ssl_module.SSLError) as error:
            failures.append(f"{where} ({error})")
            return None
        return transport

    @staticmethod
    async def _resolve(
        loop: asyncio.AbstractEventLoop, hostname: str, port: int, failures: list[str]
    ) -> list[tuple[int, Any]]:
        """Resolve a hostname to a shuffled list of (family, address) candidates.

        A name that will not resolve is recorded as a failure and skipped, so one bad
        entry in a cluster list does not stop the rest from being tried.
        """
        try:
            entries = await loop.getaddrinfo(hostname, port, type=socket.SOCK_STREAM)
        except OSError as error:
            failures.append(f"{hostname}:{port} ({error})")
            return []
        candidates: list[tuple[int, Any]] = list(
            dict.fromkeys((entry[0], entry[4]) for entry in entries)
        )
        random.shuffle(candidates)
        return candidates

    def write(self, data: bytes | bytearray) -> None:
        transport = self.transport
        if transport is None or transport.is_closing():
            raise exception.AMQPConnectionError("connection closed")
        if len(data) <= MAX_SINGLE_WRITE:
            transport.write(data)
            return
        view = memoryview(data)
        try:
            for offset in range(0, len(view), MAX_SINGLE_WRITE):
                transport.write(view[offset : offset + MAX_SINGLE_WRITE])
        finally:
            view.release()

    async def drain(self) -> None:
        """Wait for the transport's write buffer to fall below the high-water mark."""
        if not self.write_paused:
            return
        transport = self.transport
        if transport is None or transport.is_closing():
            raise exception.AMQPConnectionError("connection closed")
        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self.drain_waiters.append(waiter)
        try:
            await waiter
        finally:
            if waiter in self.drain_waiters:
                self.drain_waiters.remove(waiter)

    def transport_lost(self, transport_error: Exception | None) -> None:
        """Transport-level connection_lost: fail drainers, forward unless detached."""
        self.write_paused = False
        self._fail_drain_waiters()
        if self.transport is None:
            return
        self.transport = None
        self.on_connection_lost(transport_error)

    def _fail_drain_waiters(self) -> None:
        for waiter in self.drain_waiters:
            if not waiter.done():
                waiter.set_exception(exception.AMQPConnectionError("connection closed"))
        self.drain_waiters.clear()

    def pause_reading(self) -> None:
        transport = self.transport
        if transport is not None and not transport.is_closing():
            transport.pause_reading()

    def resume_reading(self) -> None:
        transport = self.transport
        if transport is not None and not transport.is_closing():
            transport.resume_reading()

    def close(self) -> None:
        self.write_paused = False
        self._fail_drain_waiters()
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    def abort(self) -> None:
        self.write_paused = False
        self._fail_drain_waiters()
        if self.transport is not None:
            self.transport.abort()
            self.transport = None
