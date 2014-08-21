"""Build spec methods in tests without restating every field.

The generated method classes declare no defaults, so no call site can quietly pick
protocol semantics -- but a test usually cares about one or two fields. This fills the
rest with the zero value for their wire type, which is the "unspecified" value in every
case: an unset bit, an empty name, a count of zero, an absent table.
"""

import inspect
from typing import Any

from amqpstorm_aio import spec

_ZERO_BY_TYPE: dict[Any, Any] = {bool: False, int: 0, str: "", bytes: b""}


def build(method_class: type[spec.Method], **fields: Any) -> Any:
    """Instantiate ``method_class``, defaulting the fields not named here."""
    if "__init__" not in vars(method_class):
        return method_class()
    annotations = inspect.get_annotations(method_class.__init__)
    values = {
        name: _ZERO_BY_TYPE.get(annotation)
        for name, annotation in annotations.items()
        if name != "return"
    }
    values.update(fields)
    return method_class(**values)
