import os.path
import ssl
import time
import unittest

from amqpstorm_aio import uri_connection
from amqpstorm_aio.tests import scenario
from amqpstorm_aio.tests.scenario import base

_MAX_CLOSE_SECONDS = 3.0
_CHANNEL_ITERATIONS = 100

requires_certificates = unittest.skipUnless(
    os.path.exists(scenario.CAFILE), "TLS certificates not present in resources/ssl"
)


def build_client_context() -> ssl.SSLContext:
    """Client context trusting the test CA and presenting the client certificate."""
    context = ssl.create_default_context(cafile=scenario.CAFILE)
    context.load_cert_chain(certfile=scenario.CERTFILE, keyfile=scenario.KEYFILE)
    return context


@requires_certificates
class SslConnectionTests(base.ScenarioTestCase):
    """A TLS connection behaves exactly like a plaintext one."""

    def make_connection(self, **overrides):
        parameters = {
            "port": scenario.SSL_PORT,
            "ssl": True,
            "ssl_options": {
                "context": build_client_context(),
                "server_hostname": scenario.HOST,
            },
        }
        parameters.update(overrides)
        return super().make_connection(**parameters)

    async def test_ssl_connection_opens_and_closes(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        self.assertTrue(connection.is_open)
        await channel.queue.declare(self.queue_name, durable=True)
        await channel.close()
        await connection.close()
        self.assertTrue(connection.is_closed)

    async def test_ssl_open_new_connection_loop(self):
        for _ in range(5):
            connection = self.make_connection()
            await connection.open()
            channel = await connection.channel()
            self.assertEqual(1, int(channel))
            await channel.queue.declare(self.queue_name, durable=True)
            self.assertTrue(connection.is_open)
            await channel.close()
            await connection.close()
            self.assertTrue(connection.is_closed)

    async def test_ssl_open_close_connection_loop_reuses_the_object(self):
        connection = self.make_connection()
        for _ in range(5):
            await connection.open()
            channel = await connection.channel()
            self.assertEqual(1, int(channel))
            await channel.queue.declare(self.queue_name, durable=True)
            await channel.close()
            self.assertTrue(connection.is_open)
            await connection.close()
            self.assertTrue(connection.is_closed)

    async def test_ssl_open_close_channel_loop_reuses_channel_id(self):
        connection = await self.open_connection()
        for _ in range(_CHANNEL_ITERATIONS):
            channel = await connection.channel()
            self.assertTrue(connection.is_open)
            self.assertTrue(channel.is_open)
            self.assertEqual(1, int(channel))
            await channel.close()
            self.assertTrue(channel.is_closed)

    async def test_ssl_open_multiple_channels_numbers_them_in_order(self):
        connection = self.make_connection()
        for _ in range(5):
            await connection.open()
            channels = []
            for _ in range(3):
                channel = await connection.channel()
                channels.append(channel)
                self.assertTrue(channel.is_open)
                self.assertEqual(len(channels), int(channel))
            await connection.close()

    async def test_ssl_close_completes_quickly(self):
        for _ in range(10):
            connection = self.make_connection(timeout=60.0)
            await connection.open()
            start = time.monotonic()
            await connection.close()
            self.assertLess(time.monotonic() - start, _MAX_CLOSE_SECONDS)

    async def test_ssl_publish_and_get_roundtrip(self):
        connection = await self.open_connection()
        channel = await connection.channel()
        await channel.queue.declare(self.queue_name, durable=True)
        await channel.confirm_deliveries()
        self.assertIs(True, await channel.basic.publish(b"over tls", self.queue_name))
        message = await channel.basic.get(self.queue_name)
        self.assertIsNotNone(message)
        self.assertEqual(b"over tls", message.body)
        await message.ack()


@requires_certificates
class SslUriConnectionTests(base.ScenarioTestCase):
    """amqps:// URIs carry the TLS options through to the context."""

    async def test_ssl_uri_connection_opens(self):
        connection = uri_connection.UriConnection(
            scenario.SSL_URI,
            ssl_options={
                "context": build_client_context(),
                "server_hostname": scenario.HOST,
            },
        )
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)
        channel = await connection.channel()
        self.assertTrue(connection.is_open)
        self.assertTrue(channel.is_open)

    async def test_ssl_uri_with_certificate_paths_opens(self):
        uri = (
            f"{scenario.SSL_URI}?ca_certs={scenario.CAFILE}"
            f"&certfile={scenario.CERTFILE}&keyfile={scenario.KEYFILE}"
            f"&server_hostname={scenario.HOST}"
        )
        connection = uri_connection.UriConnection(uri)
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)
        self.assertTrue(connection.is_open)


if __name__ == "__main__":
    unittest.main()
