"""Cluster failover: an unreachable node is skipped for the next one.

Two layers are shuffled and walked: the hostname list, and the addresses each of those
names resolves to. TLS still verifies against the hostname, never the address.
"""

import asyncio
import contextlib
import os
import socket
import unittest
from unittest import mock

from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio.tests.functional import fake_broker


@contextlib.contextmanager
def fixed_connect_order():
    """Stop the per-connection shuffle so a test controls which node is tried first.

    Failover only runs when the dead node comes first, which a random order would
    only reach half the time.
    """
    with mock.patch.object(
        connection_module.random, "sample", lambda population, k: list(population)
    ):
        yield


def closed_port() -> int:
    """A port nothing is listening on, so connecting to it fails immediately."""
    probe = socket.socket()
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    return port


CLUSTER_HOST = os.environ.get("AMQP_CLUSTER_HOST", "rmq-cluster.eandersson.net")


def resolved_addresses(hostname: str) -> list[str]:
    """Addresses for ``hostname`` in the order the resolver offers them."""
    try:
        entries = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except OSError:
        return []
    return list(dict.fromkeys(entry[4][0] for entry in entries))


class RoundRobinDnsTests(unittest.IsolatedAsyncioTestCase):
    """One name, several addresses: the event loop tries them until one answers."""

    def setUp(self):
        self.addresses = resolved_addresses(CLUSTER_HOST)
        if len(self.addresses) < 2:
            self.skipTest(f"{CLUSTER_HOST} does not resolve to several addresses")

    @staticmethod
    async def shutdown_broker(broker: fake_broker.FakeBroker) -> None:
        for session in broker.sessions:
            session.close_transport()
        async with asyncio.timeout(5):
            await broker.stop()

    async def start_broker_on(self, host: str) -> fake_broker.FakeBroker:
        broker = fake_broker.FakeBroker(heartbeat=0)
        await broker.start(host=host)
        self.addAsyncCleanup(self.shutdown_broker, broker)
        return broker

    def connect_to_cluster_name(self, port: int, **overrides):
        parameters = {"port": port, "heartbeat": 0, "timeout": 30.0}
        parameters.update(overrides)
        return connection_module.Connection(CLUSTER_HOST, "guest", "guest", **parameters)

    async def test_every_resolved_address_is_tried_until_one_answers(self):
        broker = await self.start_broker_on(self.addresses[-1])
        connection = self.connect_to_cluster_name(broker.port)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        self.assertTrue(connection.is_open)

    async def test_connections_spread_across_the_resolved_addresses(self):
        first = fake_broker.FakeBroker(heartbeat=0)
        await first.start(host=self.addresses[0])
        self.addAsyncCleanup(self.shutdown_broker, first)
        port = first.port
        for address in self.addresses[1:]:
            await self.start_broker_on_port(address, port)

        landed = set()
        for _ in range(30):
            connection = self.connect_to_cluster_name(port)
            await connection.open()
            landed.add(connection.peer_address[0])
            await connection.close()
        self.assertGreater(
            len(landed), 1, f"every connection landed on the same address: {landed}"
        )

    async def start_broker_on_port(self, host: str, port: int) -> fake_broker.FakeBroker:
        broker = fake_broker.FakeBroker(heartbeat=0)
        broker._server = await asyncio.start_server(broker._handle_client, host, port)
        broker.port = port
        self.addAsyncCleanup(self.shutdown_broker, broker)
        return broker

    async def test_a_live_first_address_still_connects(self):
        broker = await self.start_broker_on(self.addresses[0])
        connection = self.connect_to_cluster_name(broker.port)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        self.assertTrue(connection.is_open)

    async def test_a_resolved_cluster_carries_normal_traffic(self):
        broker = await self.start_broker_on(self.addresses[-1])
        connection = self.connect_to_cluster_name(broker.port)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        channel = await connection.channel()
        declared = await channel.queue.declare("dns.cluster.q", durable=True)
        self.assertEqual("dns.cluster.q", declared["queue"])

    async def test_no_address_answering_reports_the_hostname(self):
        connection = self.connect_to_cluster_name(closed_port(), timeout=2.0)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        self.assertIn(CLUSTER_HOST, str(context.exception))
        self.assertTrue(connection.is_closed)

    def test_a_single_name_is_not_expanded_by_the_client(self):
        connection = connection_module.Connection(CLUSTER_HOST, "guest", "guest")
        self.assertEqual((CLUSTER_HOST,), connection.parameters["hostnames"])


class FailoverTests(unittest.IsolatedAsyncioTestCase):
    async def start_broker(self) -> fake_broker.FakeBroker:
        broker = fake_broker.FakeBroker(heartbeat=0)
        await broker.start()
        self.addAsyncCleanup(self.shutdown_broker, broker)
        return broker

    @staticmethod
    async def shutdown_broker(broker: fake_broker.FakeBroker) -> None:
        for session in broker.sessions:
            session.close_transport()
        async with asyncio.timeout(5):
            await broker.stop()

    def make_connection(self, hostname, port, **overrides):
        parameters = {"port": port, "heartbeat": 0, "timeout": 2.0}
        parameters.update(overrides)
        with fixed_connect_order():
            return connection_module.Connection(hostname, "guest", "guest", **parameters)

    async def test_unreachable_first_host_falls_through_to_a_live_one(self):
        broker = await self.start_broker()
        connection = self.make_connection(["127.0.0.2", "127.0.0.1"], broker.port)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        self.assertTrue(connection.is_open)

    async def test_a_live_first_host_is_used_directly(self):
        broker = await self.start_broker()
        connection = self.make_connection(["127.0.0.1", "127.0.0.2"], broker.port)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        self.assertTrue(connection.is_open)
        self.assertEqual(1, len(broker.sessions))

    async def test_every_host_unreachable_reports_them_all(self):
        port = closed_port()
        connection = self.make_connection(["127.0.0.1", "127.0.0.2"], port)
        with self.assertRaises(exception.AMQPConnectionError) as context:
            await connection.open()
        message = str(context.exception)
        self.assertIn("127.0.0.1", message)
        self.assertIn("127.0.0.2", message)
        self.assertTrue(connection.is_closed)

    async def test_hosts_are_shuffled_across_connections(self):
        broker = await self.start_broker()
        first_hosts = {
            connection_module.Connection(
                ["127.0.0.1", "127.0.0.2", "127.0.0.3"], "guest", "guest",
                port=broker.port, heartbeat=0, timeout=2.0,
            ).parameters["hostname"]
            for _ in range(60)
        }
        self.assertGreater(len(first_hosts), 1)

    async def test_reopening_rotates_which_host_is_tried_first(self):
        broker = await self.start_broker()
        connection = self.make_connection(["127.0.0.1", "127.0.0.1"], broker.port)
        for _ in range(3):
            await connection.open()
            self.assertTrue(connection.is_open)
            await connection.close()
        self.assertEqual(3, connection._connect_attempt)

    async def test_a_cluster_connection_carries_normal_traffic(self):
        broker = await self.start_broker()
        connection = self.make_connection(["127.0.0.2", "127.0.0.1"], broker.port)
        await connection.open()
        self.addAsyncCleanup(connection.close)
        channel = await connection.channel()
        declared = await channel.queue.declare("cluster.q", durable=True)
        self.assertEqual("cluster.q", declared["queue"])


if __name__ == "__main__":
    unittest.main()
