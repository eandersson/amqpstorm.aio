"""Conformance with the <assert> constraints declared in the spec XML.

The spec is the source of truth here, even where brokers are more permissive:
RabbitMQ accepts 200-character queue names full of spaces and unicode, but the spec
asserts 127 characters matching ^[a-zA-Z0-9-_.:]*$ and that is what this client sends.

Every constraint in the generated tables must be classified below. A constraint added
by a future spec revision fails the coverage tests until it is triaged, so enforcement
cannot silently fall behind the XML.
"""

import asyncio
import unittest

from amqpstorm_aio import channel as channel_module
from amqpstorm_aio import channel0 as channel0_module
from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio import validation
from amqpstorm_aio.tests import factory

ENFORCED = "enforced"
INBOUND = "inbound"

# How each domain-level assert is handled. Domains are applied to outbound arguments
# by validation.require_domain, which reads the same generated table.
DOMAIN_POLICY = {
    ("exchange-name", "length"): ENFORCED,
    ("exchange-name", "regexp"): ENFORCED,
    ("queue-name", "length"): ENFORCED,
    ("queue-name", "regexp"): ENFORCED,
    ("path", "notnull"): ENFORCED,
    ("path", "length"): ENFORCED,
    ("reply-code", "notnull"): ENFORCED,
    ("reply-text", "notnull"): ENFORCED,
}

# Field-level asserts. INBOUND marks fields only ever received from the broker, which
# the client reports rather than validates.
FIELD_POLICY = {
    ("Connection.Start", "mechanisms", "notnull"): INBOUND,
    ("Connection.Start", "locales", "notnull"): INBOUND,
    ("Connection.StartOk", "mechanism", "notnull"): ENFORCED,
    ("Connection.StartOk", "response", "notnull"): ENFORCED,
    ("Connection.StartOk", "locale", "notnull"): ENFORCED,
    ("Connection.SecureOk", "response", "notnull"): INBOUND,
    ("Connection.TuneOk", "channel-max", "notnull"): ENFORCED,
    ("Connection.TuneOk", "channel-max", "le"): ENFORCED,
    ("Exchange.Declare", "exchange", "notnull"): ENFORCED,
    ("Exchange.Delete", "exchange", "notnull"): ENFORCED,
    ("Queue.DeclareOk", "queue", "notnull"): INBOUND,
}

# Names the spec's regexp forbids, even though RabbitMQ accepts every one of them.
NAMES_THE_SPEC_FORBIDS = ("has space", "café", "a#b", "a/b", "a+b", "a=b", "a,b", "a\tb")
NAMES_THE_SPEC_ALLOWS = ("", "simple", "my.queue", "a-b_c.d:e", "amq.gen-JRJJQa1v-Lx2GrJThMwb_g")


METHOD_CLASSES = {
    value.index: value
    for outer in (spec.Connection, spec.Channel, spec.Exchange, spec.Queue, spec.Basic, spec.Tx)
    for value in vars(outer).values()
    if isinstance(value, type) and issubclass(value, spec.Method)
}


class RecordingConnection:
    """Records outbound frames and answers synchronous methods immediately.

    Auto-answering keeps the positive cases (a name the spec allows) from waiting out
    the rpc timeout, so a test can assert the argument was accepted and marshalled.
    """

    def __init__(self):
        self.written = []
        self.user_closed = False
        self.is_closed = False
        self.max_frame_size = 131_072
        self.channel = None

    def check_for_errors(self):
        pass

    def write_method_frame(self, channel_id, method):
        framing.append_method_frame(bytearray(), channel_id, method)
        self.written.append((channel_id, method))
        if self.channel is not None and method.valid_responses:
            self.channel.on_frame(factory.build(METHOD_CLASSES[method.valid_responses[0]]))

    def write_bytes(self, data):
        self.written.append(("bytes", bytes(data)))

    async def drain(self):
        return None

    def ensure_current_loop(self):
        pass


def make_channel():
    connection = RecordingConnection()
    channel = channel_module.Channel(1, connection, 1)
    connection.channel = channel
    channel.set_state(channel_module.base.Stateful.OPEN)
    return channel


class AssertCoverageTests(unittest.TestCase):
    """Every constraint the generator extracted has a decision recorded against it."""

    def test_every_domain_assert_is_classified(self):
        declared = {
            (domain, check)
            for domain, asserts in spec.DOMAIN_ASSERTS.items()
            for check, _value in asserts
        }
        self.assertEqual(
            declared,
            set(DOMAIN_POLICY),
            "a spec domain assert is unclassified; triage it in DOMAIN_POLICY",
        )

    def test_every_field_assert_is_classified(self):
        declared = {
            (method, field, check)
            for method, asserts in spec.FIELD_ASSERTS.items()
            for field, check, _value in asserts
        }
        self.assertEqual(
            declared,
            set(FIELD_POLICY),
            "a spec field assert is unclassified; triage it in FIELD_POLICY",
        )

    def test_every_enforced_domain_has_a_compiled_rule(self):
        for domain, check in DOMAIN_POLICY:
            with self.subTest(domain=domain, check=check):
                rule = validation.DOMAIN_RULES[domain]
                if check == "length":
                    self.assertIsNotNone(rule.max_bytes)
                elif check == "regexp":
                    self.assertIsNotNone(rule.pattern)
                elif check == "notnull":
                    self.assertTrue(rule.not_null)


class DomainRuleTests(unittest.TestCase):
    """The compiled rules match the values written in the XML."""

    def test_name_domains_cap_at_127_bytes(self):
        for domain in ("queue-name", "exchange-name", "path"):
            with self.subTest(domain=domain):
                self.assertEqual(127, validation.DOMAIN_RULES[domain].max_bytes)

    def test_name_domains_are_stricter_than_the_shortstr_limit(self):
        self.assertLess(
            validation.DOMAIN_RULES["queue-name"].max_bytes, validation.SHORTSTR_MAX_BYTES
        )

    def test_names_at_the_limit_are_accepted(self):
        validation.require_domain("queue-name", "queue", "q" * 127)

    def test_names_over_the_limit_are_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_domain("queue-name", "queue", "q" * 128)
        self.assertEqual("queue should be at most 127 bytes, got 128", str(context.exception))

    def test_length_is_measured_in_bytes(self):
        # 64 two-byte characters are 128 bytes, one past the limit.
        with self.assertRaises(exception.AMQPInvalidArgument):
            validation.require_domain("path", "virtual_host", "é" * 64)

    def test_spec_allowed_names_pass(self):
        for name in NAMES_THE_SPEC_ALLOWS:
            with self.subTest(name=name):
                validation.require_domain("queue-name", "queue", name)
                validation.require_domain("exchange-name", "exchange", name)

    def test_spec_forbidden_names_are_rejected(self):
        for name in NAMES_THE_SPEC_FORBIDS:
            with self.subTest(name=name):
                with self.assertRaises(exception.AMQPInvalidArgument) as context:
                    validation.require_domain("queue-name", "queue", name)
                self.assertIn("should match", str(context.exception))

    def test_path_has_no_character_restriction(self):
        validation.require_domain("path", "virtual_host", "/")
        validation.require_domain("path", "virtual_host", "/production")

    def test_path_rejects_empty(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_domain("path", "virtual_host", "")
        self.assertEqual("virtual_host should not be empty", str(context.exception))


class QueueNameEnforcementTests(unittest.IsolatedAsyncioTestCase):
    """Every operation taking a queue-name applies the domain rules."""

    def setUp(self):
        self.channel = make_channel()

    async def assert_rejected(self, coroutine):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await coroutine
        self.assertEqual([], self.channel.connection.written)

    async def test_operations_reject_names_the_spec_forbids(self):
        bad = "bad name"
        await self.assert_rejected(self.channel.queue.declare(bad))
        await self.assert_rejected(self.channel.queue.delete(bad))
        await self.assert_rejected(self.channel.queue.purge(bad))
        await self.assert_rejected(self.channel.queue.bind(bad))
        await self.assert_rejected(self.channel.queue.unbind(bad))
        await self.assert_rejected(self.channel.basic.consume(queue=bad))
        await self.assert_rejected(self.channel.basic.get(queue=bad))

    async def test_operations_reject_names_over_127_bytes(self):
        too_long = "q" * 128
        await self.assert_rejected(self.channel.queue.declare(too_long))
        await self.assert_rejected(self.channel.basic.consume(queue=too_long))
        await self.assert_rejected(self.channel.basic.get(queue=too_long))

    async def test_broker_generated_names_are_accepted(self):
        # A name of this shape comes back from declaring an anonymous queue, so it
        # must pass validation when handed to consume.
        await self.channel.queue.declare("amq.gen-JRJJQa1v-Lx2GrJThMwb_g")


class ExchangeNameEnforcementTests(unittest.IsolatedAsyncioTestCase):
    """Every operation taking an exchange-name applies the domain rules."""

    def setUp(self):
        self.channel = make_channel()

    async def assert_rejected(self, coroutine):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await coroutine
        self.assertEqual([], self.channel.connection.written)

    async def test_operations_reject_names_the_spec_forbids(self):
        bad = "bad exchange"
        await self.assert_rejected(self.channel.exchange.declare(bad))
        await self.assert_rejected(self.channel.exchange.delete(bad))
        await self.assert_rejected(self.channel.exchange.bind(destination=bad))
        await self.assert_rejected(self.channel.exchange.unbind(source=bad))
        await self.assert_rejected(self.channel.queue.bind("q", bad))
        await self.assert_rejected(self.channel.basic.publish(b"body", "rk", exchange=bad))

    async def test_declare_and_delete_reject_the_empty_default_exchange(self):
        # Field-level notnull: "" is the default exchange and cannot be declared.
        await self.assert_rejected(self.channel.exchange.declare(""))
        await self.assert_rejected(self.channel.exchange.delete(""))

    async def test_publishing_to_the_default_exchange_is_allowed(self):
        # Only declare/delete carry notnull; publishing to "" is how the default
        # exchange is addressed.
        await self.channel.basic.publish(b"body", "rk", exchange="")
        self.assertTrue(self.channel.connection.written)


class RoutingKeyTests(unittest.IsolatedAsyncioTestCase):
    """Routing keys draw no constrained domain, so topic patterns stay legal."""

    def setUp(self):
        self.channel = make_channel()

    def test_routing_key_fields_have_no_domain_constraint(self):
        for method in ("Basic.Publish", "Queue.Bind", "Exchange.Bind"):
            with self.subTest(method=method):
                constrained = dict(spec.FIELD_DOMAINS.get(method, ()))
                self.assertNotIn("routing-key", constrained)

    async def test_topic_wildcards_are_accepted(self):
        for routing_key in ("events.#", "events.*.created", "a b", "*"):
            with self.subTest(routing_key=routing_key):
                await self.channel.queue.bind("q", "ex", routing_key)


class ReplyFieldTests(unittest.IsolatedAsyncioTestCase):
    """reply-code and reply-text are both notnull."""

    def setUp(self):
        self.channel = make_channel()

    async def test_close_rejects_zero_reply_code(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            await self.channel.close(reply_code=0)
        self.assertEqual("reply_code should not be zero", str(context.exception))

    async def test_close_rejects_empty_reply_text(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            await self.channel.close(reply_text="")
        self.assertEqual("reply_text should not be empty", str(context.exception))

    def test_default_close_arguments_satisfy_the_spec(self):
        signature = channel_module.Channel.close.__defaults__
        self.assertEqual((spec.REPLY_SUCCESS, "Normal shutdown"), signature)


class VirtualHostTests(unittest.TestCase):
    """connection.open's virtual-host uses the path domain."""

    def build(self, **overrides):
        parameters = {"hostname": "localhost", "username": "guest", "password": "guest"}
        parameters.update(overrides)
        return connection_module.Connection(**parameters)

    def test_rejects_empty_virtual_host(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            self.build(virtual_host="")

    def test_rejects_virtual_host_over_127_bytes(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            self.build(virtual_host="/" + "v" * 127)

    def test_accepts_conventional_virtual_hosts(self):
        for virtual_host in ("/", "/production", "vhost-1"):
            with self.subTest(virtual_host=virtual_host):
                self.build(virtual_host=virtual_host)


class HandshakeAssertTests(unittest.IsolatedAsyncioTestCase):
    """The frames the client sends during negotiation satisfy their notnull asserts."""

    class HandshakeConnection:
        def __init__(self):
            self.parameters = {
                "username": "guest",
                "password": "guest",
                "locale": "en_US",
                "heartbeat": 60,
                "virtual_host": "/",
                "client_properties": {},
            }
            self.sent = []

        def write_method_frame(self, channel_id, method):
            framing.append_method_frame(bytearray(), channel_id, method)
            self.sent.append(method)

        def on_remote_close(self, error):
            pass

        @property
        def is_open(self):
            return True

        @property
        def is_closed(self):
            return False

    async def run_handshake(self, server_channel_max=2047):
        connection = self.HandshakeConnection()
        channel0 = channel0_module.Channel0(connection)
        negotiation = asyncio.ensure_future(channel0.negotiate(5))
        await asyncio.sleep(0)
        channel0.on_frame(
            factory.build(spec.Connection.Start,
                version_major=0,
                version_minor=9,
                server_properties={"product": "RabbitMQ"},
                mechanisms=b"PLAIN AMQPLAIN",
                locales=b"en_US",
            )
        )
        await asyncio.sleep(0)
        channel0.on_frame(
            factory.build(spec.Connection.Tune,
                channel_max=server_channel_max, frame_max=131_072, heartbeat=60
            )
        )
        await asyncio.sleep(0)
        channel0.on_frame(factory.build(spec.Connection.OpenOk))
        await negotiation
        return connection.sent

    async def test_start_ok_fields_are_not_null(self):
        sent = await self.run_handshake()
        start_ok = next(f for f in sent if isinstance(f, spec.Connection.StartOk))
        self.assertTrue(start_ok.mechanism, "mechanism is notnull")
        self.assertTrue(start_ok.response, "response is notnull")
        self.assertTrue(start_ok.locale, "locale is notnull")

    async def test_tune_ok_channel_max_is_not_null_and_within_the_server_value(self):
        server_channel_max = 2047
        sent = await self.run_handshake(server_channel_max=server_channel_max)
        tune_ok = next(f for f in sent if isinstance(f, spec.Connection.TuneOk))
        self.assertTrue(tune_ok.channel_max, "channel-max is notnull")
        self.assertLessEqual(
            tune_ok.channel_max, server_channel_max, "channel-max asserts le the server value"
        )

    async def test_tune_ok_channel_max_is_not_null_when_the_server_offers_no_limit(self):
        sent = await self.run_handshake(server_channel_max=0)
        tune_ok = next(f for f in sent if isinstance(f, spec.Connection.TuneOk))
        self.assertTrue(tune_ok.channel_max, "a server value of 0 must not propagate as 0")

    async def test_open_carries_a_non_empty_virtual_host(self):
        sent = await self.run_handshake()
        opened = next(f for f in sent if isinstance(f, spec.Connection.Open))
        self.assertTrue(opened.virtual_host, "virtual-host uses the notnull path domain")


if __name__ == "__main__":
    unittest.main()
