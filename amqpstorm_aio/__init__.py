__version__ = "0.1.0"
__author__ = "eandersson"

import logging

logging.getLogger("amqpstorm_aio").addHandler(logging.NullHandler())

from amqpstorm_aio.channel import Channel  # noqa
from amqpstorm_aio.connection import Connection  # noqa
from amqpstorm_aio.exception import AMQPChannelError  # noqa
from amqpstorm_aio.exception import AMQPConnectionError  # noqa
from amqpstorm_aio.exception import AMQPError  # noqa
from amqpstorm_aio.exception import AMQPFrameError  # noqa
from amqpstorm_aio.exception import AMQPInvalidArgument  # noqa
from amqpstorm_aio.exception import AMQPMessageError  # noqa
from amqpstorm_aio.message import Message  # noqa
from amqpstorm_aio import spec # noqa
from amqpstorm_aio.uri_connection import UriConnection  # noqa

Properties = spec.Basic.Properties

__all__ = [
    "AMQPChannelError",
    "AMQPConnectionError",
    "AMQPError",
    "AMQPFrameError",
    "AMQPInvalidArgument",
    "AMQPMessageError",
    "Channel",
    "Connection",
    "Message",
    "Properties",
    "UriConnection",
    "__version__",
]
