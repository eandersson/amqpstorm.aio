import logging
from types import TracebackType
from typing import Any

from amqpstorm_aio import base
from amqpstorm_aio import exception
from amqpstorm_aio import spec

LOG = logging.getLogger(__name__)


class Tx(base.Handler):
    """Transaction operations for one channel."""

    __slots__ = ("_tx_active",)

    def __init__(self, channel: Any) -> None:
        super().__init__(channel)
        self._tx_active = False

    async def __aenter__(self) -> Tx:
        await self.select()
        return self

    async def __aexit__(
        self,
        exception_type: type[Exception] | None,
        exception_value: Exception | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._tx_active:
            return
        if exception_value is not None:
            LOG.warning(f"Leaving Transaction on exception: {exception_value}")
            try:
                await self.rollback()
            except exception.AMQPError:
                pass
            return
        await self.commit()

    async def select(self) -> dict[str, Any]:
        """Enable transactions on the channel.

        Marked active before the request is awaited, not after: an abandoned select
        may still have reached the broker, and a transaction the client has forgotten
        swallows every later publish silently.
        """
        self._tx_active = True
        result = await self._channel.rpc_request(spec.Tx.Select())
        return base.method_to_dict(result)

    async def commit(self) -> dict[str, Any]:
        """Commit the current transaction.

        The transaction is finished either way, so it is cleared before the request:
        a commit that failed halfway must not be retried by ``__aexit__``.
        """
        self._tx_active = False
        result = await self._channel.rpc_request(spec.Tx.Commit())
        return base.method_to_dict(result)

    async def rollback(self) -> dict[str, Any]:
        """Abandon the current transaction."""
        self._tx_active = False
        result = await self._channel.rpc_request(spec.Tx.Rollback())
        return base.method_to_dict(result)

    def reset(self) -> None:
        """Forget transaction mode; a reopened channel starts outside one."""
        self._tx_active = False
