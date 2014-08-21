from typing import TYPE_CHECKING
from typing import Any

from amqpstorm_aio import spec

if TYPE_CHECKING:
    from amqpstorm_aio.channel import Channel


def method_to_dict(method: spec.Method) -> dict[str, Any]:
    """Expose a decoded method's fields as a dict (AMQPStorm returned dicts here)."""
    return {name: getattr(method, name) for name in method.__slots__}


class Stateful:
    """Four-state lifecycle: CLOSED -> OPENING -> OPEN -> CLOSING -> CLOSED."""

    CLOSED = 0
    CLOSING = 1
    OPENING = 2
    OPEN = 3

    __slots__ = ("_state",)

    def __init__(self) -> None:
        self._state = Stateful.CLOSED

    def set_state(self, state: int) -> None:
        self._state = state

    @property
    def current_state(self) -> int:
        return self._state

    @property
    def is_closed(self) -> bool:
        return self._state == Stateful.CLOSED

    @property
    def is_closing(self) -> bool:
        return self._state == Stateful.CLOSING

    @property
    def is_opening(self) -> bool:
        return self._state == Stateful.OPENING

    @property
    def is_open(self) -> bool:
        return self._state == Stateful.OPEN


class Handler:
    """Base for the per-channel operation groups (basic, queue, exchange, tx)."""

    __slots__ = ("_channel",)

    def __init__(self, channel: Channel) -> None:
        self._channel = channel
