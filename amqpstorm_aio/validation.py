import re
from typing import Any

from amqpstorm_aio import exception
from amqpstorm_aio import spec

SHORTSTR_MAX_BYTES = 255

OCTET_MAX = 0xFF
SHORT_MAX = 0xFFFF
LONG_MAX = 0xFFFF_FFFF
LONG_LONG_MAX = 0xFFFF_FFFF_FFFF_FFFF


class DomainRule:
    """The <assert> constraints the spec declares for one domain."""

    __slots__ = ("max_bytes", "not_null", "pattern")

    def __init__(
        self, max_bytes: int | None, pattern: re.Pattern[str] | None, not_null: bool
    ) -> None:
        self.max_bytes = max_bytes
        self.pattern = pattern
        self.not_null = not_null


def _compile_domain_rules() -> dict[str, DomainRule]:
    rules = {}
    for domain, asserts in spec.DOMAIN_ASSERTS.items():
        max_bytes = None
        pattern = None
        not_null = False
        for check, value in asserts:
            if check == "length":
                max_bytes = int(value or 0)
            elif check == "regexp":
                pattern = re.compile(value or "")
            elif check == "notnull":
                not_null = True
            else:
                raise ValueError(f"unhandled spec assert {check!r} on domain {domain!r}")
        rules[domain] = DomainRule(max_bytes, pattern, not_null)
    return rules


DOMAIN_RULES = _compile_domain_rules()


def require_positive_number(name: str, value: Any) -> None:
    """Reject anything but an int or float greater than zero."""
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise exception.AMQPInvalidArgument(f"{name} should be an integer or float")
    if value <= 0:
        raise exception.AMQPInvalidArgument(f"{name} should be greater than 0")


def require_positive_integer(name: str, value: Any) -> None:
    """Reject anything but an int greater than zero."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise exception.AMQPInvalidArgument(f"{name} should be an integer")
    if value <= 0:
        raise exception.AMQPInvalidArgument(f"{name} should be greater than 0")


def require_string(name: str, value: Any) -> None:
    if not isinstance(value, str):
        raise exception.AMQPInvalidArgument(f"{name} should be a string")


def _encoded_length(name: str, value: str) -> int:
    """UTF-8 byte length; pure-ASCII strings skip the encode entirely."""
    if value.isascii():
        return len(value)
    try:
        return len(value.encode("utf-8"))
    except UnicodeEncodeError:
        raise exception.AMQPInvalidArgument(
            f"{name} contains characters that cannot be encoded"
        ) from None


def require_shortstr(name: str, value: Any) -> None:
    require_string(name, value)
    encoded_length = _encoded_length(name, value)
    if encoded_length > SHORTSTR_MAX_BYTES:
        raise exception.AMQPInvalidArgument(
            f"{name} should be at most {SHORTSTR_MAX_BYTES} bytes, got {encoded_length}"
        )


def require_domain(domain: str, name: str, value: Any, *, not_null: bool = False) -> None:
    require_string(name, value)
    rule = DOMAIN_RULES[domain]
    if (not_null or rule.not_null) and not value:
        raise exception.AMQPInvalidArgument(f"{name} should not be empty")
    max_bytes = rule.max_bytes if rule.max_bytes is not None else SHORTSTR_MAX_BYTES
    encoded_length = _encoded_length(name, value)
    if encoded_length > max_bytes:
        raise exception.AMQPInvalidArgument(
            f"{name} should be at most {max_bytes} bytes, got {encoded_length}"
        )
    if rule.pattern is not None and not rule.pattern.fullmatch(value):
        raise exception.AMQPInvalidArgument(
            f"{name} should match {rule.pattern.pattern} as required for {domain}"
        )


def require_boolean(name: str, value: Any) -> None:
    if not isinstance(value, bool):
        raise exception.AMQPInvalidArgument(f"{name} should be a boolean")


def require_table(name: str, value: Any) -> None:
    if value is not None and not isinstance(value, dict):
        raise exception.AMQPInvalidArgument(f"{name} should be a dict or None")


def require_integer(name: str, value: Any, maximum: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise exception.AMQPInvalidArgument(f"{name} should be an integer")
    if not 0 <= value <= maximum:
        raise exception.AMQPInvalidArgument(f"{name} should be between 0 and {maximum}")


def require_octet(name: str, value: Any) -> None:
    require_integer(name, value, OCTET_MAX)


def require_short(name: str, value: Any) -> None:
    require_integer(name, value, SHORT_MAX)


def require_long(name: str, value: Any) -> None:
    require_integer(name, value, LONG_MAX)


def require_long_long(name: str, value: Any) -> None:
    require_integer(name, value, LONG_LONG_MAX)


def require_reply_code(name: str, value: Any) -> None:
    """A reply code is a short the spec marks notnull, so zero is not a reply code."""
    require_short(name, value)
    if not value:
        raise exception.AMQPInvalidArgument(f"{name} should not be zero")
