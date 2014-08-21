import asyncio
import logging
import weakref
from collections.abc import Callable

from amqpstorm_aio import exception

LOG = logging.getLogger(__name__)

_DEATH_THRESHOLD_INTERVALS = 2
_STARVED_CHECK_GRACE = 1.5


def _weakly[T](callback: Callable[..., T]) -> Callable[[], Callable[..., T] | None]:
    """Hold a bound method weakly so the heartbeat task cannot pin its owner.

    Plain functions (tests, module-level callbacks) have no owner to pin and are
    held strongly.
    """
    try:
        return weakref.WeakMethod(callback)
    except TypeError:
        return lambda: callback


class Heartbeat:
    """Sends heartbeats when idle and fails the connection when the peer goes silent."""

    __slots__ = (
        "_interval_seconds",
        "_last_check_seconds",
        "_on_dead",
        "_reads_since_check",
        "_send_heartbeat",
        "_task",
        "_threshold",
        "_timeout_seconds",
        "_writes_since_check",
        "reads_paused",
    )

    def __init__(
        self,
        timeout_seconds: float,
        send_heartbeat: Callable[[], None],
        on_dead: Callable[[exception.AMQPConnectionError], None],
    ) -> None:
        self._timeout_seconds = timeout_seconds
        self._interval_seconds = timeout_seconds / 2 if timeout_seconds else 0.0
        self._send_heartbeat = _weakly(send_heartbeat)
        self._on_dead = _weakly(on_dead)
        self._reads_since_check = 0
        self._writes_since_check = 0
        self._threshold = 0
        self._last_check_seconds = 0.0
        self._task: asyncio.Task[None] | None = None
        self.reads_paused = False

    def apply_interval(self, timeout_seconds: float) -> None:
        """Adopt the handshake-negotiated heartbeat timeout before starting."""
        self._timeout_seconds = timeout_seconds
        self._interval_seconds = timeout_seconds / 2 if timeout_seconds else 0.0

    def register_read(self) -> None:
        self._reads_since_check += 1

    def register_write(self) -> None:
        self._writes_since_check += 1

    def start(self) -> bool:
        """Start the checker; returns False (disabled) when the timeout is falsy."""
        self.stop()
        if not self._interval_seconds or self._interval_seconds <= 0:
            return False
        loop = asyncio.get_running_loop()
        self._reads_since_check = 0
        self._writes_since_check = 0
        self._threshold = 0
        self._last_check_seconds = loop.time()
        self._task = loop.create_task(self._run(), name="amqpstorm-aio-heartbeat")
        return True

    def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._interval_seconds)
            if not self._check_for_life_signs():
                self._task = None
                return

    def _check_for_life_signs(self) -> bool:
        send_heartbeat = self._send_heartbeat()
        on_dead = self._on_dead()
        if send_heartbeat is None or on_dead is None:
            return False
        now = asyncio.get_running_loop().time()
        elapsed = now - self._last_check_seconds
        self._last_check_seconds = now
        if not self._writes_since_check:
            send_heartbeat()
        if self._reads_since_check:
            self._threshold = 0
        elif self.reads_paused:
            self._threshold = 0
        elif elapsed > self._interval_seconds * _STARVED_CHECK_GRACE:
            LOG.debug(
                f"heartbeat check delayed by a busy event loop "
                f"({elapsed:.1f}s since the previous check); not counting the silence"
            )
        else:
            self._threshold += 1
            if self._threshold >= _DEATH_THRESHOLD_INTERVALS:
                self._reads_since_check = 0
                self._writes_since_check = 0
                on_dead(
                    exception.AMQPConnectionError(
                        "Connection dead, no heartbeat or data received in >= "
                        f"{int(self._interval_seconds * 2)}s"
                    )
                )
                return False
        self._reads_since_check = 0
        self._writes_since_check = 0
        return True
