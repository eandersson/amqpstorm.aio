from typing import Any

from amqpstorm_aio import base
from amqpstorm_aio import spec
from amqpstorm_aio import validation


class Queue(base.Handler):
    """Queue-class operations for one channel."""

    __slots__ = ()

    async def declare(
        self,
        queue: str = "",
        passive: bool = False,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Declare a queue; returns queue name, message count, and consumer count."""
        validation.require_domain("queue-name", "queue", queue)
        validation.require_boolean("passive", passive)
        validation.require_boolean("durable", durable)
        validation.require_boolean("exclusive", exclusive)
        validation.require_boolean("auto_delete", auto_delete)
        validation.require_table("arguments", arguments)
        result = await self._channel.rpc_request(
            spec.Queue.Declare(
                queue=queue,
                passive=passive,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                no_wait=False,
                arguments=arguments,
            )
        )
        return base.method_to_dict(result)

    async def delete(
        self, queue: str = "", if_unused: bool = False, if_empty: bool = False
    ) -> dict[str, Any]:
        """Delete a queue; the returned dict carries the ``message_count`` deleted."""
        validation.require_domain("queue-name", "queue", queue)
        validation.require_boolean("if_unused", if_unused)
        validation.require_boolean("if_empty", if_empty)
        result = await self._channel.rpc_request(
            spec.Queue.Delete(
                queue=queue, if_unused=if_unused, if_empty=if_empty, no_wait=False
            )
        )
        return base.method_to_dict(result)

    async def purge(self, queue: str) -> dict[str, Any]:
        """Discard all messages in a queue; the returned dict carries the purged count."""
        validation.require_domain("queue-name", "queue", queue)
        result = await self._channel.rpc_request(spec.Queue.Purge(queue=queue, no_wait=False))
        return base.method_to_dict(result)

    async def bind(
        self,
        queue: str = "",
        exchange: str = "",
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Bind a queue to an exchange."""
        validation.require_domain("queue-name", "queue", queue)
        validation.require_domain("exchange-name", "exchange", exchange)
        validation.require_shortstr("routing_key", routing_key)
        validation.require_table("arguments", arguments)
        result = await self._channel.rpc_request(
            spec.Queue.Bind(
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                no_wait=False,
                arguments=arguments,
            )
        )
        return base.method_to_dict(result)

    async def unbind(
        self,
        queue: str = "",
        exchange: str = "",
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Unbind a queue from an exchange."""
        validation.require_domain("queue-name", "queue", queue)
        validation.require_domain("exchange-name", "exchange", exchange)
        validation.require_shortstr("routing_key", routing_key)
        validation.require_table("arguments", arguments)
        result = await self._channel.rpc_request(
            spec.Queue.Unbind(
                queue=queue, exchange=exchange, routing_key=routing_key, arguments=arguments
            )
        )
        return base.method_to_dict(result)
