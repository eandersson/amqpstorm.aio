import json
from typing import TYPE_CHECKING
from typing import Any

from amqpstorm_aio import exception
from amqpstorm_aio import spec

if TYPE_CHECKING:
    from amqpstorm_aio.channel import Channel

type DeliveryMethod = spec.Basic.Deliver | spec.Basic.GetOk


class Message:
    """One delivered AMQP message, bound to the channel session that received it.

    Acknowledgement methods reject messages from an earlier session after the
    channel is reopened, even when the broker has reused their delivery tags.
    """

    __slots__ = ("_session_token", "body", "channel", "method", "properties")

    def __init__(
        self,
        channel: Channel,
        body: bytes,
        method: DeliveryMethod,
        properties: spec.Basic.Properties,
    ) -> None:
        self.channel = channel
        self._session_token = channel.session_token
        self.body = body
        self.method = method
        self.properties = properties

    def __len__(self) -> int:
        return len(self.body)

    def __repr__(self) -> str:
        return f"Message(body={self.body[:64]!r}, method={self.method!r})"

    @property
    def text(self) -> str:
        """Body decoded per its declared content encoding (UTF-8 when undeclared)."""
        encoding_name = self.properties.content_encoding or "utf-8"
        try:
            return self.body.decode(encoding_name)
        except LookupError:
            raise exception.AMQPMessageError(
                f"cannot decode body: content_encoding {encoding_name!r} is not a text codec"
            ) from None

    def json(self) -> Any:
        """Body deserialized as JSON."""
        return json.loads(self.body)

    @property
    def delivery_tag(self) -> int:
        return self.method.delivery_tag

    @property
    def redelivered(self) -> bool:
        return self.method.redelivered

    @property
    def exchange(self) -> str:
        return self.method.exchange

    @property
    def routing_key(self) -> str:
        return self.method.routing_key

    @property
    def consumer_tag(self) -> str | None:
        """The consuming subscription's tag (None for Basic.Get results)."""
        method = self.method
        if isinstance(method, spec.Basic.Deliver):
            return method.consumer_tag
        return None

    async def ack(self) -> None:
        """Acknowledge this delivery."""
        self._check_session()
        await self.channel.basic.ack(delivery_tag=self.method.delivery_tag)

    async def nack(self, requeue: bool = True) -> None:
        """Negatively acknowledge this delivery."""
        self._check_session()
        await self.channel.basic.nack(delivery_tag=self.method.delivery_tag, requeue=requeue)

    async def reject(self, requeue: bool = True) -> None:
        """Reject this delivery."""
        self._check_session()
        await self.channel.basic.reject(delivery_tag=self.method.delivery_tag, requeue=requeue)

    def _check_session(self) -> None:
        if self._session_token is not self.channel.session_token:
            raise exception.AMQPChannelError("message belongs to a different channel session")
