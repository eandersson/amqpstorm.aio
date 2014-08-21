from typing import Any

from amqpstorm_aio import base
from amqpstorm_aio import spec
from amqpstorm_aio import validation


class Exchange(base.Handler):
    """Exchange-class operations for one channel."""

    __slots__ = ()

    async def declare(
        self,
        exchange: str = "",
        exchange_type: str = "direct",
        passive: bool = False,
        durable: bool = True,
        auto_delete: bool = False,
        internal: bool = False,
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Declare an exchange."""
        validation.require_domain("exchange-name", "exchange", exchange, not_null=True)
        validation.require_shortstr("exchange_type", exchange_type)
        validation.require_boolean("passive", passive)
        validation.require_boolean("durable", durable)
        validation.require_boolean("auto_delete", auto_delete)
        validation.require_boolean("internal", internal)
        validation.require_table("arguments", arguments)
        result = await self._channel.rpc_request(
            spec.Exchange.Declare(
                exchange=exchange,
                type=exchange_type,
                passive=passive,
                durable=durable,
                auto_delete=auto_delete,
                internal=internal,
                no_wait=False,
                arguments=arguments,
            )
        )
        return base.method_to_dict(result)

    async def delete(self, exchange: str = "", if_unused: bool = False) -> dict[str, Any]:
        """Delete an exchange."""
        validation.require_domain("exchange-name", "exchange", exchange, not_null=True)
        validation.require_boolean("if_unused", if_unused)
        result = await self._channel.rpc_request(
            spec.Exchange.Delete(exchange=exchange, if_unused=if_unused, no_wait=False)
        )
        return base.method_to_dict(result)

    async def bind(
        self,
        destination: str = "",
        source: str = "",
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Bind an exchange to an exchange."""
        validation.require_domain("exchange-name", "destination", destination)
        validation.require_domain("exchange-name", "source", source)
        validation.require_shortstr("routing_key", routing_key)
        validation.require_table("arguments", arguments)
        result = await self._channel.rpc_request(
            spec.Exchange.Bind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                no_wait=False,
                arguments=arguments,
            )
        )
        return base.method_to_dict(result)

    async def unbind(
        self,
        destination: str = "",
        source: str = "",
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Unbind an exchange from an exchange."""
        validation.require_domain("exchange-name", "destination", destination)
        validation.require_domain("exchange-name", "source", source)
        validation.require_shortstr("routing_key", routing_key)
        validation.require_table("arguments", arguments)
        result = await self._channel.rpc_request(
            spec.Exchange.Unbind(
                destination=destination,
                source=source,
                routing_key=routing_key,
                no_wait=False,
                arguments=arguments,
            )
        )
        return base.method_to_dict(result)
