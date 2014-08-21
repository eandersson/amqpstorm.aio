import asyncio
import gc
import logging
import socket
import ssl as ssl_module
import unittest
import warnings
from unittest import mock

from amqpstorm_aio import exception
from amqpstorm_aio import io


async def _close_client(reader, writer):
    writer.close()


def _entry(family, host, port):
    """One getaddrinfo 5-tuple: (family, socktype, proto, canonname, sockaddr)."""
    return (family, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (host, port))


class RecordingTransport(asyncio.Transport):
    """Minimal transport recording the calls IO makes on it."""

    def __init__(self, closing=False):
        super().__init__()
        self.closing = closing
        self.aborted = False
        self.closed = False
        self.reading_paused = False
        self.reading_resumed = False
        self.writes = []

    def is_closing(self):
        return self.closing

    def abort(self):
        self.aborted = True
        self.closing = True

    def close(self):
        self.closed = True
        self.closing = True

    def pause_reading(self):
        self.reading_paused = True

    def resume_reading(self):
        self.reading_resumed = True

    def write(self, data):
        self.writes.append(bytes(data))


class ExplodingTransport(RecordingTransport):
    """A transport whose writes fail, to exercise IO's cleanup paths."""

    def write(self, data):
        raise OSError("transport gone")


class RecordingContext:
    """Stands in for an SSLContext, recording how build_ssl_context configures it."""

    def __init__(self):
        self.check_hostname = True
        self.verify_mode = ssl_module.CERT_REQUIRED
        self.cert_chains = []

    def load_cert_chain(self, certfile, keyfile=None):
        self.cert_chains.append((certfile, keyfile))


class ResolvingLoop:
    """Stands in for the event loop, answering getaddrinfo from a scripted list."""

    def __init__(self, entries=(), error=None):
        self.entries = list(entries)
        self.error = error

    async def getaddrinfo(self, host, port, *, type=None):
        if self.error is not None:
            raise self.error
        return self.entries


class ConnectingLoop:
    """Stands in for the event loop, scripting one create_connection outcome."""

    def __init__(self, transport=None, error=None, hang=False):
        self.transport = transport
        self.error = error
        self.hang = hang
        self.kwargs = None
        self.protocol = None

    async def create_connection(self, protocol_factory, **kwargs):
        self.kwargs = kwargs
        if self.hang:
            await asyncio.Event().wait()
        if self.error is not None:
            raise self.error
        self.protocol = protocol_factory()
        return self.transport, self.protocol


class RecordingIO(io.IO):
    """An IO that records what ``open`` asks ``_connect`` to do, without connecting."""

    def __init__(self):
        super().__init__(lambda data: None, lambda error: None)
        self.attempts = []

    async def _connect(
        self,
        loop,
        hostname,
        family,
        address,
        timeout,
        ssl_context,
        server_hostname,
        failures,
    ):
        self.attempts.append((hostname, address, ssl_context, server_hostname))
        return RecordingTransport()


class ResolveVerifyModeTests(unittest.TestCase):
    def test_a_non_string_verify_mode_is_returned_untouched(self):
        self.assertIs(
            io._resolve_verify_mode(ssl_module.CERT_OPTIONAL), ssl_module.CERT_OPTIONAL
        )

    def test_a_named_verify_mode_maps_to_the_ssl_constant(self):
        for name, expected in [
            ("none", ssl_module.CERT_NONE),
            ("CERT_OPTIONAL", ssl_module.CERT_OPTIONAL),
            ("required", ssl_module.CERT_REQUIRED),
            ("  None  ", ssl_module.CERT_NONE),
            ("\tcert_Optional\n", ssl_module.CERT_OPTIONAL),
        ]:
            with self.subTest(name=name):
                self.assertIs(io._resolve_verify_mode(name), expected)

    def test_an_unknown_verify_mode_falls_back_to_cert_required_with_a_warning(self):
        with self.assertLogs(io.LOG, level=logging.WARNING) as logs:
            resolved = io._resolve_verify_mode("kinda-optional")

        self.assertIs(resolved, ssl_module.CERT_REQUIRED)
        self.assertIn("unknown verify_mode 'kinda-optional'", logs.output[0])


class BuildSSLContextTests(unittest.TestCase):
    def test_a_caller_supplied_context_is_returned_unchanged(self):
        supplied = ssl_module.create_default_context()

        self.assertIs(io.build_ssl_context({"context": supplied, "verify_mode": "none"}), supplied)
        self.assertTrue(supplied.check_hostname, "a supplied context must not be loosened")

    def test_a_context_that_is_not_an_ssl_context_is_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as caught:
            io.build_ssl_context({"context": "please-trust-me"})

        self.assertEqual(
            "ssl_options['context'] should be an SSLContext", str(caught.exception)
        )

    def test_default_options_produce_a_verifying_context(self):
        context = io.build_ssl_context({})

        self.assertIs(context.verify_mode, ssl_module.CERT_REQUIRED)
        self.assertTrue(context.check_hostname)

    def test_verify_mode_none_also_disables_hostname_checking(self):
        context = io.build_ssl_context({"verify_mode": "none"})

        self.assertIs(context.verify_mode, ssl_module.CERT_NONE)
        self.assertFalse(context.check_hostname)

    def test_cert_reqs_is_accepted_as_an_alias_for_verify_mode(self):
        context = io.build_ssl_context({"cert_reqs": ssl_module.CERT_NONE})

        self.assertIs(context.verify_mode, ssl_module.CERT_NONE)
        self.assertFalse(context.check_hostname)

    def test_an_explicit_check_hostname_overrides_the_verify_mode_default(self):
        context = io.build_ssl_context({"verify_mode": "optional", "check_hostname": False})

        self.assertIs(context.verify_mode, ssl_module.CERT_OPTIONAL)
        self.assertFalse(context.check_hostname)

    def test_the_stricter_of_two_verify_mode_spellings_wins(self):
        strict_via_cert_reqs = io.build_ssl_context(
            {"cert_reqs": ssl_module.CERT_REQUIRED, "verify_mode": "none"}
        )
        self.assertIs(strict_via_cert_reqs.verify_mode, ssl_module.CERT_REQUIRED)
        strict_via_verify_mode = io.build_ssl_context(
            {"verify_mode": "required", "cert_reqs": ssl_module.CERT_NONE}
        )
        self.assertIs(strict_via_verify_mode.verify_mode, ssl_module.CERT_REQUIRED)

    def test_both_spellings_set_to_none_still_disables(self):
        context = io.build_ssl_context(
            {"cert_reqs": ssl_module.CERT_NONE, "verify_mode": "none"}
        )
        self.assertIs(context.verify_mode, ssl_module.CERT_NONE)

    def test_a_missing_client_certificate_surfaces_as_a_file_not_found_error(self):
        with self.assertRaises(FileNotFoundError):
            io.build_ssl_context({"certfile": "/nope/no-such-client-cert.pem"})

    def test_check_hostname_wins_over_a_verify_mode_that_disabled_it(self):
        context = io.build_ssl_context({"verify_mode": "none", "check_hostname": 1})

        self.assertTrue(context.check_hostname)
        self.assertIs(
            context.verify_mode,
            ssl_module.CERT_REQUIRED,
            "hostname checking is meaningless without certificate verification",
        )

    def test_the_client_key_is_loaded_alongside_the_certificate(self):
        context = RecordingContext()

        with mock.patch.object(io.ssl_module, "create_default_context", return_value=context):
            built = io.build_ssl_context({"certfile": "client.pem", "keyfile": "client.key"})

        self.assertIs(context, built)
        self.assertEqual([("client.pem", "client.key")], context.cert_chains)

    def test_a_certificate_without_a_key_is_loaded_on_its_own(self):
        context = RecordingContext()

        with mock.patch.object(io.ssl_module, "create_default_context", return_value=context):
            io.build_ssl_context({"certfile": "bundle.pem"})

        self.assertEqual([("bundle.pem", None)], context.cert_chains)

    def test_an_empty_certfile_loads_no_client_certificate(self):
        context = RecordingContext()

        with mock.patch.object(io.ssl_module, "create_default_context", return_value=context):
            io.build_ssl_context({"certfile": "", "keyfile": "client.key"})

        self.assertEqual([], context.cert_chains)


class BuildContextForOpenTests(unittest.TestCase):
    def test_no_context_is_built_when_ssl_is_disabled(self):
        self.assertIsNone(io.IO._build_context(False, {"ca_certs": "/nope/missing.pem"}))

    def test_an_unreadable_ca_bundle_is_reported_as_a_connection_error(self):
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            io.IO._build_context(True, {"ca_certs": "/nope/this-file-does-not-exist.pem"})

        self.assertIn("invalid TLS configuration", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, OSError)

    def test_a_missing_client_certificate_is_reported_as_a_connection_error(self):
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            io.IO._build_context(True, {"certfile": "/nope/no-such-client-cert.pem"})

        self.assertIn("invalid TLS configuration", str(caught.exception))
        self.assertIsInstance(caught.exception.__cause__, FileNotFoundError)


class UnclosedIoWarningTests(unittest.TestCase):
    """IO reports itself at collection only while it still holds a transport."""

    def test_collecting_an_io_that_still_holds_a_transport_warns(self):
        owner = io.IO(lambda data: None, lambda error: None)
        owner.transport = RecordingTransport()
        with self.assertWarns(ResourceWarning):
            del owner
            gc.collect()

    def test_collecting_a_closed_io_is_silent(self):
        owner = io.IO(lambda data: None, lambda error: None)
        owner.transport = RecordingTransport()
        owner.close()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            del owner
            gc.collect()
        ours = [entry for entry in caught if "amqpstorm-aio Connection" in str(entry.message)]
        self.assertEqual([], ours, "a closed connection must not warn")


class ProtocolWithCollectedOwnerTests(unittest.TestCase):
    """Transport events after the owning IO is gone reap the socket instead of crashing."""

    def _orphaned_protocol(self):
        """Collect the owner, leaving the protocol holding a live transport.

        The ResourceWarning belongs to collection, not to the later transport event:
        a silent broker never sends one, so waiting for an event would let an orphan
        go unreported. Reaping the socket still happens on the next event, which is
        the first moment the loop can safely abort it.
        """
        owner = io.IO(lambda data: None, lambda error: None)
        protocol = io._AMQPProtocol(owner)
        transport = RecordingTransport()
        protocol.connection_made(transport)
        owner.transport = transport
        with self.assertWarns(ResourceWarning):
            del owner
            gc.collect()
        return protocol, transport

    def test_pause_writing_after_the_owner_is_collected_aborts_the_transport(self):
        protocol, transport = self._orphaned_protocol()

        protocol.pause_writing()

        self.assertTrue(transport.aborted)

    def test_resume_writing_after_the_owner_is_collected_aborts_the_transport(self):
        protocol, transport = self._orphaned_protocol()

        protocol.resume_writing()

        self.assertTrue(transport.aborted)

    def test_data_received_after_the_owner_is_collected_aborts_the_transport(self):
        protocol, transport = self._orphaned_protocol()

        protocol.data_received(b"\x01")

        self.assertTrue(transport.aborted)

    def test_the_transport_is_only_aborted_once(self):
        protocol, transport = self._orphaned_protocol()

        protocol.pause_writing()
        transport.aborted = False
        protocol.resume_writing()

        self.assertFalse(transport.aborted, "a reaped transport must not be aborted again")

    def test_connection_lost_after_the_owner_is_collected_is_silent(self):
        protocol, transport = self._orphaned_protocol()

        protocol.connection_lost(None)

        self.assertFalse(transport.aborted, "connection_lost already released the socket")


class ProtocolFlowControlTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.lost = []
        self.data = []
        self.io = io.IO(self.data.append, self.lost.append)
        self.protocol = io._AMQPProtocol(self.io)
        self.transport = RecordingTransport()
        self.protocol.connection_made(self.transport)
        self.io.transport = self.transport

    async def test_data_received_is_forwarded_to_the_owner(self):
        self.protocol.data_received(b"AMQP")

        self.assertEqual([b"AMQP"], self.data)

    async def test_pause_writing_marks_the_owner_paused(self):
        self.protocol.pause_writing()

        self.assertTrue(self.io.write_paused)

    async def test_resume_writing_wakes_every_pending_drainer(self):
        self.protocol.pause_writing()
        first = asyncio.ensure_future(self.io.drain())
        second = asyncio.ensure_future(self.io.drain())
        await asyncio.sleep(0)

        self.protocol.resume_writing()
        await asyncio.wait_for(asyncio.gather(first, second), 1)

        self.assertFalse(self.io.write_paused)
        self.assertEqual([], self.io.drain_waiters)

    async def test_resume_writing_skips_a_waiter_that_is_already_done(self):
        done = asyncio.get_running_loop().create_future()
        done.set_result(None)
        self.io.write_paused = True
        self.io.drain_waiters.append(done)

        self.protocol.resume_writing()

        self.assertFalse(self.io.write_paused)
        self.assertEqual([], self.io.drain_waiters)

    async def test_connection_lost_is_forwarded_to_the_owner(self):
        error = OSError("reset by peer")

        self.protocol.connection_lost(error)

        self.assertEqual([error], self.lost)
        self.assertIsNone(self.io.transport)


class IOWriteTests(unittest.TestCase):
    def setUp(self):
        self.io = io.IO(lambda data: None, lambda error: None)
        self.transport = RecordingTransport()
        self.io.transport = self.transport

    def test_writing_without_a_transport_raises_connection_closed(self):
        self.io.transport = None

        with self.assertRaises(exception.AMQPConnectionError) as caught:
            self.io.write(b"data")

        self.assertEqual("connection closed", str(caught.exception))

    def test_writing_to_a_closing_transport_raises_connection_closed(self):
        self.transport.closing = True

        with self.assertRaises(exception.AMQPConnectionError):
            self.io.write(b"data")

    def test_a_small_payload_is_written_in_one_call(self):
        self.io.write(b"AMQP\x00\x00\x09\x01")

        self.assertEqual([b"AMQP\x00\x00\x09\x01"], self.transport.writes)

    def test_an_oversized_payload_is_chunked(self):
        payload = bytes(io.MAX_SINGLE_WRITE + 17)

        self.io.write(payload)

        self.assertEqual(2, len(self.transport.writes))
        self.assertEqual(io.MAX_SINGLE_WRITE, len(self.transport.writes[0]))
        self.assertEqual(17, len(self.transport.writes[1]))
        self.assertEqual(payload, b"".join(self.transport.writes))

    def test_a_failing_chunked_write_raises_and_leaves_the_payload_usable(self):
        self.io.transport = ExplodingTransport()
        payload = bytearray(io.MAX_SINGLE_WRITE + 17)

        with self.assertRaises(OSError) as caught:
            self.io.write(payload)

        self.assertEqual("transport gone", str(caught.exception))
        payload.clear()
        self.assertEqual(0, len(payload), "the caller's buffer must not stay exported")


class IODrainTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.io = io.IO(lambda data: None, lambda error: None)
        self.transport = RecordingTransport()
        self.io.transport = self.transport

    async def test_drain_returns_immediately_when_writing_is_not_paused(self):
        await asyncio.wait_for(self.io.drain(), 1)

        self.assertEqual([], self.io.drain_waiters)

    async def test_drain_on_a_closing_transport_raises_connection_closed(self):
        self.io.write_paused = True
        self.transport.closing = True

        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await self.io.drain()

        self.assertEqual("connection closed", str(caught.exception))

    async def test_a_cancelled_drainer_removes_itself_from_the_waiters(self):
        self.io.write_paused = True
        waiting = asyncio.ensure_future(self.io.drain())
        await asyncio.sleep(0)
        self.assertEqual(1, len(self.io.drain_waiters))

        waiting.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await waiting

        self.assertEqual([], self.io.drain_waiters)

    async def test_a_lost_transport_fails_every_pending_drainer(self):
        self.io.write_paused = True
        waiting = asyncio.ensure_future(self.io.drain())
        await asyncio.sleep(0)

        self.io.transport_lost(OSError("boom"))

        with self.assertRaises(exception.AMQPConnectionError):
            await waiting
        self.assertFalse(self.io.write_paused)

    async def test_a_drain_waiter_that_is_already_done_is_left_alone_on_loss(self):
        done = asyncio.get_running_loop().create_future()
        done.set_result(None)
        self.io.drain_waiters.append(done)

        self.io.transport_lost(None)

        self.assertIsNone(done.result())
        self.assertEqual([], self.io.drain_waiters)


class IOTransportLostTests(unittest.TestCase):
    def setUp(self):
        self.lost = []
        self.io = io.IO(lambda data: None, self.lost.append)

    def test_transport_loss_forwards_the_error_once(self):
        self.io.transport = RecordingTransport()
        error = OSError("peer went away")

        self.io.transport_lost(error)
        self.io.transport_lost(error)

        self.assertEqual([error], self.lost)
        self.assertIsNone(self.io.transport)

    def test_a_detached_io_does_not_report_the_loss(self):
        self.io.transport_lost(OSError("late"))

        self.assertEqual([], self.lost)


class IOReadControlTests(unittest.TestCase):
    def setUp(self):
        self.io = io.IO(lambda data: None, lambda error: None)
        self.transport = RecordingTransport()

    def test_read_control_is_forwarded_to_a_live_transport(self):
        self.io.transport = self.transport

        self.io.pause_reading()
        self.io.resume_reading()

        self.assertTrue(self.transport.reading_paused)
        self.assertTrue(self.transport.reading_resumed)

    def test_read_control_without_a_transport_is_a_no_op(self):
        self.io.pause_reading()
        self.io.resume_reading()

        self.assertFalse(self.transport.reading_paused)
        self.assertFalse(self.transport.reading_resumed)

    def test_read_control_on_a_closing_transport_is_a_no_op(self):
        self.transport.closing = True
        self.io.transport = self.transport

        self.io.pause_reading()
        self.io.resume_reading()

        self.assertFalse(self.transport.reading_paused)
        self.assertFalse(self.transport.reading_resumed)


class IOShutdownTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.io = io.IO(lambda data: None, lambda error: None)
        self.transport = RecordingTransport()
        self.io.transport = self.transport

    async def test_close_closes_the_transport_and_detaches_it(self):
        self.io.write_paused = True

        self.io.close()

        self.assertTrue(self.transport.closed)
        self.assertIsNone(self.io.transport)
        self.assertFalse(self.io.write_paused)

    async def test_close_without_a_transport_is_a_no_op(self):
        self.io.transport = None

        self.io.close()

        self.assertFalse(self.transport.closed)

    async def test_abort_aborts_the_transport_and_detaches_it(self):
        self.io.write_paused = True

        self.io.abort()

        self.assertTrue(self.transport.aborted)
        self.assertFalse(self.transport.closed, "abort must not go through a graceful close")
        self.assertIsNone(self.io.transport)
        self.assertFalse(self.io.write_paused)

    async def test_abort_without_a_transport_is_a_no_op(self):
        self.io.transport = None

        self.io.abort()

        self.assertFalse(self.transport.aborted)

    async def test_close_fails_pending_drainers(self):
        self.io.write_paused = True
        waiting = asyncio.ensure_future(self.io.drain())
        await asyncio.sleep(0)

        self.io.close()

        with self.assertRaises(exception.AMQPConnectionError):
            await waiting

    async def test_abort_fails_pending_drainers(self):
        self.io.write_paused = True
        waiting = asyncio.ensure_future(self.io.drain())
        await asyncio.sleep(0)

        self.io.abort()

        with self.assertRaises(exception.AMQPConnectionError):
            await waiting


class IOOpenTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.received = []
        self.lost = []
        self.io = io.IO(self.received.append, self.lost.append)
        self.addCleanup(self.io.abort)

    async def _listener(self):
        server = await asyncio.start_server(_close_client, "127.0.0.1", 0)
        self.addCleanup(server.close)
        return server.sockets[0].getsockname()[1]

    async def test_open_connects_to_the_first_reachable_address(self):
        port = await self._listener()

        await self.io.open(["127.0.0.1"], port, 5.0, False, {})

        self.assertIsNotNone(self.io.transport)

    async def test_open_falls_through_to_a_later_hostname(self):
        port = await self._listener()

        await self.io.open(["240.0.0.0", "127.0.0.1"], port, 0.2, False, {})

        self.assertIsNotNone(self.io.transport)

    async def test_open_reports_every_address_it_tried(self):
        closed_port = await self._listener_free_port()

        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await self.io.open(["127.0.0.1"], closed_port, 2.0, False, {})

        message = str(caught.exception)
        self.assertIn("could not connect to", message)
        self.assertIn(f"127.0.0.1 [127.0.0.1]:{closed_port}", message)
        self.assertIsNone(self.io.transport)

    async def _listener_free_port(self):
        server = await asyncio.start_server(_close_client, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        server.close()
        await server.wait_closed()
        return port

    async def test_a_hostname_that_does_not_resolve_is_recorded_and_skipped(self):
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await self.io.open(["no-such-host.invalid"], 5672, 1.0, False, {})

        self.assertIn("no-such-host.invalid:5672", str(caught.exception))

    async def test_a_tls_handshake_against_a_plain_listener_is_recorded(self):
        port = await self._listener()

        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await self.io.open(["127.0.0.1"], port, 1.0, True, {"verify_mode": "none"})

        self.assertIn(f"127.0.0.1 [127.0.0.1]:{port}", str(caught.exception))
        self.assertIsNone(self.io.transport)


class ResolveTests(unittest.IsolatedAsyncioTestCase):
    async def test_an_address_the_resolver_repeats_is_only_tried_once(self):
        loop = ResolvingLoop([_entry(socket.AF_INET, "10.0.0.1", 5672)] * 3)
        failures = []

        candidates = await io.IO._resolve(loop, "broker.invalid", 5672, failures)

        self.assertEqual([(socket.AF_INET, ("10.0.0.1", 5672))], candidates)
        self.assertEqual([], failures)

    async def test_the_resolved_addresses_are_shuffled_between_calls(self):
        expected = {(f"10.0.0.{octet}", 5672) for octet in range(1, 9)}
        loop = ResolvingLoop(_entry(socket.AF_INET, host, port) for host, port in expected)

        orders = set()
        for _attempt in range(20):
            candidates = await io.IO._resolve(loop, "round-robin.invalid", 5672, [])
            orders.add(tuple(address for _family, address in candidates))

        self.assertGreater(len(orders), 1, "clients must spread across a round-robin name")
        for order in orders:
            self.assertEqual(expected, set(order), "shuffling must not drop an address")

    async def test_a_name_that_will_not_resolve_is_recorded_with_its_reason(self):
        loop = ResolvingLoop(error=socket.gaierror("Name or service not known"))
        failures = []

        candidates = await io.IO._resolve(loop, "broker.invalid", 5672, failures)

        self.assertEqual([], candidates)
        self.assertEqual(["broker.invalid:5672 (Name or service not known)"], failures)


class IOConnectTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.received = []
        self.io = io.IO(self.received.append, lambda error: None)
        self.failures = []

    async def _connect(self, loop, timeout=5.0, ssl_context=None, server_hostname=None):
        return await self.io._connect(
            loop,
            "broker.example.com",
            socket.AF_INET,
            ("10.0.0.1", 5672),
            timeout,
            ssl_context,
            server_hostname,
            self.failures,
        )

    async def test_a_successful_connect_returns_the_transport_and_records_no_failure(self):
        transport = RecordingTransport()

        result = await self._connect(ConnectingLoop(transport=transport))

        self.assertIs(transport, result)
        self.assertEqual([], self.failures)

    async def test_the_resolved_address_and_family_are_what_gets_dialled(self):
        context = ssl_module.create_default_context()
        loop = ConnectingLoop(transport=RecordingTransport())

        await self._connect(loop, ssl_context=context, server_hostname="broker.example.com")

        self.assertEqual(
            {
                "host": "10.0.0.1",
                "port": 5672,
                "family": socket.AF_INET,
                "ssl": context,
                "server_hostname": "broker.example.com",
            },
            loop.kwargs,
        )

    async def test_the_new_transport_feeds_its_bytes_back_to_this_io(self):
        loop = ConnectingLoop(transport=RecordingTransport())

        await self._connect(loop)
        loop.protocol.data_received(b"AMQP\x00\x00\x09\x01")

        self.assertEqual([b"AMQP\x00\x00\x09\x01"], self.received)

    async def test_a_refused_address_is_recorded_with_the_reason(self):
        loop = ConnectingLoop(error=ConnectionRefusedError("connection refused"))

        self.assertIsNone(await self._connect(loop))

        self.assertEqual(
            ["broker.example.com [10.0.0.1]:5672 (connection refused)"], self.failures
        )

    async def test_a_failed_tls_handshake_is_recorded_with_the_reason(self):
        loop = ConnectingLoop(error=ssl_module.SSLError(1, "wrong version number"))

        self.assertIsNone(await self._connect(loop))

        self.assertEqual(1, len(self.failures))
        self.assertIn("broker.example.com [10.0.0.1]:5672", self.failures[0])
        self.assertIn("wrong version number", self.failures[0])

    async def test_an_address_that_never_answers_is_recorded_as_a_timeout(self):
        loop = ConnectingLoop(hang=True)

        self.assertIsNone(await self._connect(loop, timeout=0.01))

        self.assertEqual(
            ["broker.example.com [10.0.0.1]:5672 (timed out after 0.01s)"], self.failures
        )


class OpenServerHostnameTests(unittest.IsolatedAsyncioTestCase):
    """TLS must be verified against the hostname, never the address it resolved to."""

    async def test_tls_is_verified_against_the_hostname_not_the_resolved_address(self):
        recorder = RecordingIO()

        await recorder.open(["localhost"], 5672, 1.0, True, {})

        (hostname, address, ssl_context, server_hostname) = recorder.attempts[0]
        self.assertEqual("localhost", hostname)
        self.assertNotEqual("localhost", address[0], "the address must be a resolved literal")
        self.assertEqual("localhost", server_hostname)
        self.assertIsInstance(ssl_context, ssl_module.SSLContext)

    async def test_an_explicit_server_hostname_overrides_the_connect_hostname(self):
        recorder = RecordingIO()

        await recorder.open(
            ["localhost"], 5672, 1.0, True, {"server_hostname": "broker.example.com"}
        )

        self.assertEqual("broker.example.com", recorder.attempts[0][3])

    async def test_no_server_hostname_or_context_is_used_without_tls(self):
        recorder = RecordingIO()

        await recorder.open(["localhost"], 5672, 1.0, False, {})

        self.assertIsNone(recorder.attempts[0][2])
        self.assertIsNone(recorder.attempts[0][3])

    async def test_an_empty_hostname_over_tls_is_refused_not_connected(self):
        recorder = RecordingIO()
        with self.assertRaises(exception.AMQPConnectionError) as caught:
            await recorder.open([""], 5672, 1.0, True, {})
        self.assertIn("hostname", str(caught.exception))
        self.assertEqual([], recorder.attempts, "nothing may reach the transport")

    async def test_a_blank_server_hostname_over_tls_is_refused(self):
        recorder = RecordingIO()
        with self.assertRaises(exception.AMQPConnectionError):
            await recorder.open(["broker"], 5672, 1.0, True, {"server_hostname": "  "})
        self.assertEqual([], recorder.attempts)

    async def test_an_empty_hostname_without_tls_is_left_alone(self):
        recorder = RecordingIO()
        await recorder.open(["localhost"], 5672, 1.0, False, {})
        self.assertEqual(1, len(recorder.attempts))
