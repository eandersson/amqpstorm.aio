import os
import pathlib

RESOURCES = pathlib.Path(__file__).parent / "resources"

HOST = os.environ.get("AMQP_HOST", "rmq.eandersson.net")
PORT = int(os.environ.get("AMQP_PORT", "5672"))
SSL_PORT = int(os.environ.get("AMQP_SSL_PORT", "5671"))
HTTP_PORT = int(os.environ.get("AMQP_HTTP_PORT", "15672"))
USERNAME = os.environ.get("AMQP_USERNAME", "amqpstorm")
PASSWORD = os.environ.get("AMQP_PASSWORD", "2a55f70a841f18b")
VIRTUAL_HOST = os.environ.get("AMQP_VIRTUAL_HOST", "/")
BROKER = os.environ.get("AMQP_BROKER", "")

URI = os.environ.get("AMQP_URI", f"amqp://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/%2F")
SSL_URI = os.environ.get("AMQP_SSL_URI", f"amqps://{USERNAME}:{PASSWORD}@{HOST}:{SSL_PORT}/%2F")
HTTP_URL = os.environ.get("AMQP_HTTP_URL", f"http://{HOST}:{HTTP_PORT}")

CAFILE = os.environ.get("AMQP_CAFILE", str(RESOURCES / "ssl" / "ca_certificate.pem"))
CERTFILE = os.environ.get(
    "AMQP_CERTFILE", str(RESOURCES / "ssl" / "client" / "client_certificate.pem")
)
KEYFILE = os.environ.get("AMQP_KEYFILE", str(RESOURCES / "ssl" / "client" / "private_key.pem"))
