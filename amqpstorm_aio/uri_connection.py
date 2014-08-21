import logging
import urllib.parse
from typing import Any

from amqpstorm_aio import connection
from amqpstorm_aio import exception

LOG = logging.getLogger(__name__)

_SSL_OPTION_KEYS = frozenset(
    {"keyfile", "certfile", "ca_certs", "cert_reqs", "verify_mode", "server_hostname"}
)
_FALSY_STRINGS = frozenset({"false", "0", "no", "off"})


def _split_host_port(segment: str) -> tuple[str, str]:
    """Split one authority segment into (host, port), respecting IPv6 brackets."""
    if segment.startswith("["):
        host, separator, port = segment.rpartition("]:")
        if separator:
            return host + "]", port
        return segment, ""
    if segment.count(":") == 1:
        host, _, port = segment.partition(":")
        return host, port
    return segment, ""


def split_hosts(uri: str) -> tuple[str, list[str]]:
    scheme, separator, rest = uri.partition("://")
    if not separator or "," not in rest:
        return uri, []
    authority_end = min(
        (index for index in (rest.find(mark) for mark in "/?#") if index != -1),
        default=len(rest),
    )
    authority, tail = rest[:authority_end], rest[authority_end:]
    if "," not in authority:
        return uri, []
    credentials, at_sign, hosts_part = authority.rpartition("@")
    hosts = []
    ports = set()
    for segment in hosts_part.split(","):
        host, port = _split_host_port(segment.strip())
        host = host.strip()
        if port:
            ports.add(port)
        if host:
            hosts.append(host)
    if len(hosts) < 2:
        return uri, []
    if len(ports) > 1:
        raise exception.AMQPInvalidArgument(
            f"cluster uri hosts disagree on the port: {sorted(ports)}"
        )
    port_suffix = f":{ports.pop()}" if ports else ""
    single_host_uri = f"{scheme}://{credentials}{at_sign}{hosts[0]}{port_suffix}{tail}"
    hostnames = [host.removeprefix("[").removesuffix("]") for host in hosts]
    return single_host_uri, hostnames


def parse_uri(uri: str) -> dict[str, Any]:
    """Parse an AMQP URI into Connection keyword arguments."""
    if not isinstance(uri, str):
        raise exception.AMQPInvalidArgument("uri should be a string")
    single_host_uri, hostnames = split_hosts(uri)
    parts = urllib.parse.urlsplit(single_host_uri)
    if parts.scheme not in ("amqp", "amqps"):
        raise exception.AMQPInvalidArgument(
            f"uri scheme should be amqp or amqps, got {parts.scheme!r}"
        )
    use_ssl = parts.scheme == "amqps"
    try:
        uri_port = parts.port
    except ValueError:
        raise exception.AMQPInvalidArgument("uri port should be an integer") from None
    if uri_port == 0:
        raise exception.AMQPInvalidArgument("uri port should be between 1 and 65535")
    parameters: dict[str, Any] = {
        "hostname": hostnames or parts.hostname or "localhost",
        "port": uri_port or (connection.DEFAULT_SSL_PORT if use_ssl else connection.DEFAULT_PORT),
        "username": urllib.parse.unquote(parts.username) if parts.username else "guest",
        "password": urllib.parse.unquote(parts.password) if parts.password else "guest",
        "virtual_host": urllib.parse.unquote(parts.path[1:]) or "/",
        "ssl": use_ssl,
    }
    ssl_options: dict[str, Any] = {}
    for key, values in urllib.parse.parse_qs(parts.query).items():
        _apply_query_option(key, values[0], use_ssl, parameters, ssl_options)
    if ssl_options:
        parameters["ssl_options"] = ssl_options
    return parameters


def _apply_query_option(
    key: str,
    value: str,
    use_ssl: bool,
    parameters: dict[str, Any],
    ssl_options: dict[str, Any],
) -> None:
    if key == "heartbeat":
        parameters["heartbeat"] = _as_integer(key, value)
    elif key == "timeout":
        parameters["timeout"] = _as_number(key, value)
    elif key == "locale":
        parameters["locale"] = value
    elif use_ssl and key in _SSL_OPTION_KEYS:
        ssl_options[key] = value
    elif use_ssl and key == "check_hostname":
        ssl_options["check_hostname"] = value.strip().lower() not in _FALSY_STRINGS
    else:
        LOG.warning(f"ignoring unknown uri option: {key!r}")


def _as_integer(key: str, value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise exception.AMQPInvalidArgument(
            f"{key} should be an integer, got {value!r}"
        ) from None


def _as_number(key: str, value: str) -> float:
    try:
        return float(value)
    except ValueError:
        raise exception.AMQPInvalidArgument(f"{key} should be a number, got {value!r}") from None


class UriConnection(connection.Connection):
    """A Connection whose parameters come from an AMQP URI."""

    __slots__ = ()

    def __init__(
        self,
        uri: str,
        *,
        ssl_options: dict[str, Any] | None = None,
        client_properties: dict[str, Any] | None = None,
    ) -> None:
        parameters = parse_uri(uri)
        uri_ssl_options = parameters.pop("ssl_options", {})
        caller_ssl_options = dict(ssl_options or {})
        overridden = sorted(set(uri_ssl_options) & set(caller_ssl_options))
        if overridden:
            LOG.warning(
                f"ignoring uri TLS options already set in code: {overridden}"
            )
        merged_ssl_options = dict(uri_ssl_options)
        merged_ssl_options.update(caller_ssl_options)
        if merged_ssl_options:
            parameters["ssl_options"] = merged_ssl_options
        hostname = parameters.pop("hostname")
        username = parameters.pop("username")
        password = parameters.pop("password")
        port = parameters.pop("port")
        super().__init__(
            hostname,
            username,
            password,
            port,
            client_properties=client_properties,
            **parameters,
        )
