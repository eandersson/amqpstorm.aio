AMQP_ERROR_NAME_BY_CODE = {
    311: "CONTENT-TOO-LARGE",
    312: "NO-ROUTE",
    313: "NO-CONSUMERS",
    320: "CONNECTION-FORCED",
    402: "INVALID-PATH",
    403: "ACCESS-REFUSED",
    404: "NOT-FOUND",
    405: "RESOURCE-LOCKED",
    406: "PRECONDITION-FAILED",
    501: "FRAME-ERROR",
    502: "SYNTAX-ERROR",
    503: "COMMAND-INVALID",
    504: "CHANNEL-ERROR",
    505: "UNEXPECTED-FRAME",
    506: "RESOURCE-ERROR",
    530: "NOT-ALLOWED",
    540: "NOT-IMPLEMENTED",
    541: "INTERNAL-ERROR",
}


class AMQPError(Exception):
    """Base error for everything raised by this library."""

    __slots__ = ("reply_code",)

    def __init__(self, message: str, reply_code: int | None = None) -> None:
        super().__init__(message)
        self.reply_code = reply_code

    @property
    def documentation(self) -> str:
        """The spec constant name for :attr:`reply_code` (empty when unknown)."""
        if self.reply_code is None:
            return ""
        return AMQP_ERROR_NAME_BY_CODE.get(self.reply_code, "")


class AMQPConnectionError(AMQPError):
    """The connection failed, was closed by the broker, or violated the protocol."""

    __slots__ = ()


class AMQPFrameError(AMQPConnectionError):
    """A frame could not be decoded; the connection is no longer trustworthy."""

    __slots__ = ()


class AMQPChannelError(AMQPError):
    """The channel was closed by the broker or an operation on it failed."""

    __slots__ = ()


class AMQPMessageError(AMQPChannelError):
    """A published message was returned, nacked, or could not be confirmed."""

    __slots__ = ()


class AMQPInvalidArgument(AMQPError, ValueError):
    """A caller-supplied argument failed validation before touching the wire."""

    __slots__ = ()
