import asyncio
import socket
import unittest
from collections.abc import Callable
from typing import Any

from amqpstorm.management import ManagementApi

from amqpstorm_aio import connection as connection_module
from amqpstorm_aio.tests import scenario

_BROKER_PROBE_TIMEOUT_SECONDS = 0.5


def broker_is_reachable() -> bool:
    probe = socket.socket()
    probe.settimeout(_BROKER_PROBE_TIMEOUT_SECONDS)
    try:
        probe.connect((scenario.HOST, scenario.PORT))
        return True
    except OSError:
        return False
    finally:
        probe.close()


BROKER_AVAILABLE = broker_is_reachable()
requires_broker = unittest.skipUnless(
    BROKER_AVAILABLE, f"no broker listening on {scenario.HOST}:{scenario.PORT}"
)


@requires_broker
class ScenarioTestCase(unittest.IsolatedAsyncioTestCase):
    """Base for end-to-end tests: named queues, management access, guaranteed cleanup."""

    async def asyncSetUp(self) -> None:
        self.api = ManagementApi(
            scenario.HTTP_URL, scenario.USERNAME, scenario.PASSWORD, timeout=5
        )
        self.queue_name = self.id().rsplit(".", 1)[-1]
        self.addAsyncCleanup(self.delete_queue, self.queue_name)

    def make_connection(self, **overrides: Any) -> connection_module.Connection:
        """Build a Connection against the test broker (not yet opened)."""
        parameters: dict[str, Any] = {
            "port": scenario.PORT,
            "virtual_host": scenario.VIRTUAL_HOST,
            "timeout": 5.0,
        }
        parameters.update(overrides)
        hostname = parameters.pop("hostname", scenario.HOST)
        return connection_module.Connection(
            hostname, scenario.USERNAME, scenario.PASSWORD, **parameters
        )

    async def open_connection(self, **overrides: Any) -> connection_module.Connection:
        """Open a Connection and close it during cleanup."""
        connection = self.make_connection(**overrides)
        await connection.open()
        self.addAsyncCleanup(self.close_connection, connection)
        return connection

    @staticmethod
    async def close_connection(connection: connection_module.Connection) -> None:
        if not connection.is_closed:
            await connection.close()

    async def management(self, call: Callable[[], Any]) -> Any:
        """Run a blocking management API call without stalling the event loop."""
        return await asyncio.to_thread(call)

    async def delete_queue(self, queue_name: str) -> None:
        try:
            await self.management(lambda: self.api.queue.delete(queue_name))
        except Exception:  # noqa
            pass

    async def queue_message_count(self, queue_name: str) -> int:
        """Message count straight from the broker's HTTP API.

        RabbitMQ omits the counter entirely while its stats database holds nothing for
        the queue, which is itself the empty case. A passive declare over AMQP is the
        authoritative check; this one corroborates it from the broker's own view.
        """
        details = await self.management(lambda: self.api.queue.get(queue_name))
        return int(details.get("messages", 0))

    async def wait_until(
        self, predicate: Callable[[], bool], timeout: float = 10.0, interval: float = 0.05
    ) -> None:
        """Poll ``predicate`` until true, failing the test on timeout."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return
            await asyncio.sleep(interval)
        self.fail(f"condition not met within {timeout}s")

    async def find_own_connections(self) -> list[dict[str, Any]]:
        """Every broker-side connection opened by this library."""
        connections = await self.management(lambda: self.api.connection.list())
        return [
            entry
            for entry in connections
            if entry.get("client_properties", {}).get("product") == "amqpstorm-aio"
        ]
