"""Negative tests: bad arguments are rejected before anything reaches the wire."""

import types
import unittest
import unittest.mock

from amqpstorm_aio import channel as channel_module
from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio import framing
from amqpstorm_aio import spec
from amqpstorm_aio import validation

# Values that are not strings at all. bytes is included deliberately: AMQPStorm
# accepted it for names, this client does not.
NOT_STRINGS = (123, b"bytes", None, ["list"], {"a": 1}, 1.5, object())
NOT_BOOLEANS = (0, 1, "true", "", None, [], 1.0)
NOT_TABLES = (["list"], "string", 42, b"bytes", object())
NOT_INTEGERS = ("1", None, 1.5, [], b"1", object())

OVERSIZED_NAME = "x" * (validation.SHORTSTR_MAX_BYTES + 1)
# 128 two-byte characters are only 128 characters but 256 bytes.
OVERSIZED_UNICODE_NAME = "é" * 128
MAX_LENGTH_NAME = "x" * validation.SHORTSTR_MAX_BYTES


class RecordingConnection:
    """Stands in for a Connection and records everything written to the wire."""

    def __init__(self):
        self.written = []
        self.user_closed = False
        self.is_closed = False
        self.max_frame_size = 131_072

    def check_for_errors(self):
        pass

    def write_method_frame(self, channel_id, method):
        # Marshal for real so an argument that slipped past validation still fails here.
        framing.append_method_frame(bytearray(), channel_id, method)
        self.written.append((channel_id, method))

    def write_bytes(self, data):
        self.written.append(("bytes", bytes(data)))

    async def drain(self):
        return None

    def ensure_current_loop(self):
        pass


def make_channel():
    channel = channel_module.Channel(1, RecordingConnection(), 1)
    channel.set_state(channel_module.base.Stateful.OPEN)
    return channel


class RequireStringTests(unittest.TestCase):
    def test_non_strings_are_rejected(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value), self.assertRaises(exception.AMQPInvalidArgument):
                validation.require_string("queue", value)

    def test_message_names_the_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_string("routing_key", 1)
        self.assertEqual("routing_key should be a string", str(context.exception))

    def test_empty_string_is_allowed(self):
        validation.require_string("queue", "")


class RequireShortstrTests(unittest.TestCase):
    """Names travel as shortstr: text, at most 255 bytes of UTF-8."""

    def test_exactly_255_bytes_is_allowed(self):
        validation.require_shortstr("queue", MAX_LENGTH_NAME)

    def test_256_bytes_is_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_shortstr("queue", OVERSIZED_NAME)
        self.assertEqual("queue should be at most 255 bytes, got 256", str(context.exception))

    def test_length_is_counted_in_bytes_not_characters(self):
        self.assertEqual(128, len(OVERSIZED_UNICODE_NAME))
        with self.assertRaises(exception.AMQPInvalidArgument):
            validation.require_shortstr("queue", OVERSIZED_UNICODE_NAME)

    def test_multibyte_name_within_the_byte_limit_is_allowed(self):
        validation.require_shortstr("queue", "é" * 127)

    def test_non_strings_are_rejected_before_the_length_check(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_shortstr("queue", b"x" * 300)
        self.assertEqual("queue should be a string", str(context.exception))


class RequireBooleanTests(unittest.TestCase):
    def test_non_booleans_are_rejected(self):
        for value in NOT_BOOLEANS:
            with self.subTest(value=value), self.assertRaises(exception.AMQPInvalidArgument):
                validation.require_boolean("durable", value)

    def test_message_names_the_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_boolean("durable", 1)
        self.assertEqual("durable should be a boolean", str(context.exception))


class RequireTableTests(unittest.TestCase):
    def test_non_tables_are_rejected(self):
        for value in NOT_TABLES:
            with self.subTest(value=value), self.assertRaises(exception.AMQPInvalidArgument):
                validation.require_table("arguments", value)

    def test_none_and_dict_are_allowed(self):
        validation.require_table("arguments", None)
        validation.require_table("arguments", {"x-max-length": 10})


class RequireIntegerTests(unittest.TestCase):
    """Unsigned integers must fit the wire field they are marshalled into."""

    def test_non_integers_are_rejected(self):
        for value in NOT_INTEGERS:
            with self.subTest(value=value), self.assertRaises(exception.AMQPInvalidArgument):
                validation.require_short("port", value)

    def test_booleans_are_rejected_despite_being_integers(self):
        for value in (True, False):
            with self.subTest(value=value), self.assertRaises(exception.AMQPInvalidArgument):
                validation.require_long_long("delivery_tag", value)

    def test_negative_values_are_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_short("prefetch_count", -1)
        self.assertEqual("prefetch_count should be between 0 and 65535", str(context.exception))

    def test_field_boundaries_are_inclusive(self):
        for require, maximum in (
            (validation.require_octet, validation.OCTET_MAX),
            (validation.require_short, validation.SHORT_MAX),
            (validation.require_long, validation.LONG_MAX),
            (validation.require_long_long, validation.LONG_LONG_MAX),
        ):
            with self.subTest(maximum=maximum):
                require("value", 0)
                require("value", maximum)
                with self.assertRaises(exception.AMQPInvalidArgument):
                    require("value", maximum + 1)


class QueueArgumentTests(unittest.IsolatedAsyncioTestCase):
    """queue.* rejects bad names, flags and argument tables."""

    def setUp(self):
        self.channel = make_channel()

    async def assert_rejected(self, coroutine):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await coroutine
        self.assertEqual([], self.channel.connection.written, "nothing may reach the wire")

    async def test_declare_rejects_non_string_names(self):
        for value in NOT_STRINGS:
            with self.subTest(value=value):
                await self.assert_rejected(self.channel.queue.declare(value))

    async def test_declare_rejects_oversized_names(self):
        for value in (OVERSIZED_NAME, OVERSIZED_UNICODE_NAME):
            with self.subTest(value=value[:10]):
                await self.assert_rejected(self.channel.queue.declare(value))

    async def test_declare_rejects_non_boolean_flags(self):
        for flag in ("passive", "durable", "exclusive", "auto_delete"):
            with self.subTest(flag=flag):
                await self.assert_rejected(self.channel.queue.declare("q", **{flag: "yes"}))

    async def test_declare_rejects_non_table_arguments(self):
        await self.assert_rejected(self.channel.queue.declare("q", arguments=["not-a-dict"]))

    async def test_delete_rejects_bad_arguments(self):
        await self.assert_rejected(self.channel.queue.delete(123))
        await self.assert_rejected(self.channel.queue.delete("q", if_unused="yes"))
        await self.assert_rejected(self.channel.queue.delete("q", if_empty=1))

    async def test_purge_rejects_bad_queue(self):
        await self.assert_rejected(self.channel.queue.purge(None))
        await self.assert_rejected(self.channel.queue.purge(OVERSIZED_NAME))

    async def test_bind_and_unbind_reject_every_name_argument(self):
        for name in ("queue", "exchange", "routing_key"):
            with self.subTest(name=name):
                await self.assert_rejected(self.channel.queue.bind(**{name: 123}))
                await self.assert_rejected(self.channel.queue.unbind(**{name: OVERSIZED_NAME}))


class ExchangeArgumentTests(unittest.IsolatedAsyncioTestCase):
    """exchange.* rejects bad names, types, flags and argument tables."""

    def setUp(self):
        self.channel = make_channel()

    async def assert_rejected(self, coroutine):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await coroutine
        self.assertEqual([], self.channel.connection.written, "nothing may reach the wire")

    async def test_declare_rejects_bad_name_and_type(self):
        await self.assert_rejected(self.channel.exchange.declare(123))
        await self.assert_rejected(self.channel.exchange.declare(OVERSIZED_NAME))
        await self.assert_rejected(self.channel.exchange.declare("ex", exchange_type=None))
        await self.assert_rejected(
            self.channel.exchange.declare("ex", exchange_type=OVERSIZED_NAME)
        )

    async def test_declare_rejects_non_boolean_flags(self):
        for flag in ("passive", "durable", "auto_delete", "internal"):
            with self.subTest(flag=flag):
                await self.assert_rejected(self.channel.exchange.declare("ex", **{flag: 1}))

    async def test_delete_rejects_bad_arguments(self):
        await self.assert_rejected(self.channel.exchange.delete(b"ex"))
        await self.assert_rejected(self.channel.exchange.delete("ex", if_unused="yes"))

    async def test_bind_and_unbind_reject_every_name_argument(self):
        for name in ("destination", "source", "routing_key"):
            with self.subTest(name=name):
                await self.assert_rejected(self.channel.exchange.bind(**{name: 1.5}))
                await self.assert_rejected(self.channel.exchange.unbind(**{name: OVERSIZED_NAME}))


class BasicArgumentTests(unittest.IsolatedAsyncioTestCase):
    """basic.* rejects bad names, tags, counters and delivery tags."""

    def setUp(self):
        self.channel = make_channel()

    async def assert_rejected(self, coroutine):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await coroutine
        self.assertEqual([], self.channel.connection.written, "nothing may reach the wire")

    async def test_qos_rejects_out_of_range_counters(self):
        await self.assert_rejected(self.channel.basic.qos(prefetch_count=-1))
        await self.assert_rejected(self.channel.basic.qos(prefetch_count=validation.SHORT_MAX + 1))
        await self.assert_rejected(self.channel.basic.qos(prefetch_size=-1))
        await self.assert_rejected(self.channel.basic.qos(prefetch_size=validation.LONG_MAX + 1))

    async def test_qos_rejects_non_integer_counters(self):
        await self.assert_rejected(self.channel.basic.qos(prefetch_count="100"))
        await self.assert_rejected(self.channel.basic.qos(prefetch_count=True))
        await self.assert_rejected(self.channel.basic.qos(global_="yes"))

    async def test_consume_rejects_bad_queue_and_tag(self):
        await self.assert_rejected(self.channel.basic.consume(queue=123))
        await self.assert_rejected(self.channel.basic.consume(queue=OVERSIZED_NAME))
        await self.assert_rejected(self.channel.basic.consume(consumer_tag=OVERSIZED_NAME))
        await self.assert_rejected(self.channel.basic.consume(arguments="not-a-dict"))

    async def test_consume_rejects_non_boolean_flags(self):
        for flag in ("exclusive", "no_ack", "no_local"):
            with self.subTest(flag=flag):
                await self.assert_rejected(self.channel.basic.consume(**{flag: 1}))

    async def test_consume_rejects_a_non_callable_callback(self):
        # The contract is Callable[..., Awaitable]; anything callable is allowed
        # through (an async __call__ object is valid), but a plain value is not.
        await self.assert_rejected(self.channel.basic.consume("not-callable", "q"))
        await self.assert_rejected(self.channel.basic.consume(42, "q"))

    async def test_cancel_rejects_bad_consumer_tag(self):
        await self.assert_rejected(self.channel.basic.cancel(None))
        await self.assert_rejected(self.channel.basic.cancel(OVERSIZED_NAME))

    async def test_get_rejects_bad_queue(self):
        await self.assert_rejected(self.channel.basic.get(queue=123))
        await self.assert_rejected(self.channel.basic.get(queue=OVERSIZED_NAME))
        await self.assert_rejected(self.channel.basic.get(no_ack="yes"))

    async def test_acknowledgements_reject_out_of_range_delivery_tags(self):
        too_large = validation.LONG_LONG_MAX + 1
        for operation in (
            self.channel.basic.ack,
            self.channel.basic.nack,
            self.channel.basic.reject,
        ):
            with self.subTest(operation=operation.__name__):
                await self.assert_rejected(operation(delivery_tag=-1))
                await self.assert_rejected(operation(delivery_tag=too_large))
                await self.assert_rejected(operation(delivery_tag=True))
                await self.assert_rejected(operation(delivery_tag="1"))

    async def test_acknowledgements_reject_non_boolean_flags(self):
        await self.assert_rejected(self.channel.basic.ack(1, multiple=1))
        await self.assert_rejected(self.channel.basic.nack(1, requeue="yes"))
        await self.assert_rejected(self.channel.basic.reject(1, requeue=None))

    async def test_recover_rejects_non_boolean_requeue(self):
        await self.assert_rejected(self.channel.basic.recover(requeue=1))


class PublishArgumentTests(unittest.IsolatedAsyncioTestCase):
    """publish rejects bad bodies, names, flags and properties."""

    def setUp(self):
        self.channel = make_channel()

    async def assert_rejected(self, coroutine):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await coroutine
        self.assertEqual([], self.channel.connection.written, "nothing may reach the wire")

    async def test_rejects_non_string_body(self):
        for body in (123, None, ["list"], {"a": 1}):
            with self.subTest(body=body):
                await self.assert_rejected(self.channel.basic.publish(body, "rk"))

    async def test_rejects_bad_routing_key_and_exchange(self):
        await self.assert_rejected(self.channel.basic.publish(b"body", 123))
        await self.assert_rejected(self.channel.basic.publish(b"body", OVERSIZED_NAME))
        await self.assert_rejected(self.channel.basic.publish(b"body", "rk", exchange=None))
        await self.assert_rejected(
            self.channel.basic.publish(b"body", "rk", exchange=OVERSIZED_NAME)
        )

    async def test_rejects_non_boolean_flags(self):
        await self.assert_rejected(self.channel.basic.publish(b"body", "rk", mandatory=1))
        await self.assert_rejected(self.channel.basic.publish(b"body", "rk", immediate="yes"))

    async def test_rejects_properties_of_the_wrong_type(self):
        for properties in ("text/plain", 42, ["content_type"]):
            with self.subTest(properties=properties):
                await self.assert_rejected(
                    self.channel.basic.publish(b"body", "rk", properties=properties)
                )

    async def test_rejects_oversized_property_strings(self):
        for name in ("content_type", "correlation_id", "reply_to", "message_id", "app_id"):
            with self.subTest(name=name):
                properties = spec.Basic.Properties(**{name: OVERSIZED_NAME})
                await self.assert_rejected(
                    self.channel.basic.publish(b"body", "rk", properties=properties)
                )

    async def test_rejects_out_of_range_priority(self):
        for priority in (-1, 256, validation.OCTET_MAX + 100):
            with self.subTest(priority=priority):
                properties = spec.Basic.Properties(priority=priority)
                await self.assert_rejected(
                    self.channel.basic.publish(b"body", "rk", properties=properties)
                )

    async def test_rejects_delivery_mode_outside_the_spec(self):
        for delivery_mode in (0, 3, 99, -1):
            with self.subTest(delivery_mode=delivery_mode):
                properties = spec.Basic.Properties(delivery_mode=delivery_mode)
                await self.assert_rejected(
                    self.channel.basic.publish(b"body", "rk", properties=properties)
                )

    async def test_accepts_both_documented_delivery_modes(self):
        for delivery_mode in (1, 2):
            with self.subTest(delivery_mode=delivery_mode):
                properties = spec.Basic.Properties(delivery_mode=delivery_mode)
                await self.channel.basic.publish(b"body", "rk", properties=properties)

    async def test_rejects_unencodable_header_values(self):
        properties = spec.Basic.Properties(headers={"key": object()})
        await self.assert_rejected(
            self.channel.basic.publish(b"body", "rk", properties=properties)
        )


class PublishPropertiesMappingTests(unittest.IsolatedAsyncioTestCase):
    """publish accepts properties as a mapping as well as a Properties object."""

    def setUp(self):
        self.channel = make_channel()

    async def assert_rejected(self, coroutine):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await coroutine
        self.assertEqual([], self.channel.connection.written, "nothing may reach the wire")

    async def test_mapping_and_object_produce_identical_frames(self):
        fields = {"content_type": "text/plain", "delivery_mode": 2, "expiration": "6000"}
        await self.channel.basic.publish(b"body", "rk", properties=fields)
        from_mapping = self.channel.connection.written[:]

        self.channel = make_channel()
        await self.channel.basic.publish(
            b"body", "rk", properties=spec.Basic.Properties(**fields)
        )
        self.assertEqual(from_mapping, self.channel.connection.written)

    async def test_a_single_property_publishes(self):
        await self.channel.basic.publish(b"body", "rk", properties={"expiration": "6000"})
        self.assertTrue(self.channel.connection.written)

    async def test_empty_mapping_publishes(self):
        await self.channel.basic.publish(b"body", "rk", properties={})
        self.assertTrue(self.channel.connection.written)

    async def test_mapping_that_is_not_a_dict_is_accepted(self):
        properties = types.MappingProxyType({"content_type": "text/plain"})
        await self.channel.basic.publish(b"body", "rk", properties=properties)
        self.assertTrue(self.channel.connection.written)

    async def test_unknown_key_is_rejected_and_names_itself(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            await self.channel.basic.publish(b"body", "rk", properties={"expiry": "6000"})
        message = str(context.exception)
        self.assertIn("expiry", message)
        self.assertIn("expiration", message, "the valid names should be listed")
        self.assertEqual([], self.channel.connection.written)

    async def test_the_amqpstorm_message_type_key_is_rejected(self):
        # AMQPStorm exposed the AMQP "type" property as message_type; the error lists
        # the valid names so a migrating caller can find the replacement.
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            await self.channel.basic.publish(b"body", "rk", properties={"message_type": "x"})
        message = str(context.exception)
        self.assertIn("message_type", message)
        self.assertIn("'type'", message)

    async def test_non_mapping_properties_are_rejected(self):
        for properties in (["content_type"], "text/plain", 42, object()):
            with self.subTest(properties=properties):
                await self.assert_rejected(
                    self.channel.basic.publish(b"body", "rk", properties=properties)
                )

    async def test_mapping_values_go_through_the_same_validation(self):
        for fields in (
            {"delivery_mode": 99},
            {"priority": 300},
            {"content_type": OVERSIZED_NAME},
            {"headers": {"key": object()}},
        ):
            with self.subTest(fields=fields):
                await self.assert_rejected(
                    self.channel.basic.publish(b"body", "rk", properties=fields)
                )

    def test_typed_dict_declares_exactly_the_properties_fields(self):
        self.assertEqual(
            set(spec.Basic.Properties.__slots__),
            set(spec.PropertiesDict.__annotations__),
            "the generated class and mapping forms must stay in step",
        )

    def test_every_typed_dict_key_is_optional(self):
        self.assertEqual(set(), spec.PropertiesDict.__required_keys__)


class ChannelArgumentTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.channel = make_channel()

    async def test_close_rejects_out_of_range_reply_code(self):
        for reply_code in (-1, validation.SHORT_MAX + 1):
            with self.subTest(reply_code=reply_code):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    await self.channel.close(reply_code=reply_code)

    async def test_close_rejects_bad_reply_text(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await self.channel.close(reply_text=123)
        with self.assertRaises(exception.AMQPInvalidArgument):
            await self.channel.close(reply_text=OVERSIZED_NAME)

    async def test_close_rejects_boolean_reply_code(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            await self.channel.close(reply_code=True)

    def test_add_consumer_tag_rejects_non_strings(self):
        for value in (123, None, b"tag"):
            with self.subTest(value=value), self.assertRaises(exception.AMQPChannelError):
                self.channel.add_consumer_tag(value)


class ConnectionArgumentTests(unittest.TestCase):
    """Connection parameters are validated at construction, before any socket work."""

    def build(self, **overrides):
        parameters = {"hostname": "localhost", "username": "guest", "password": "guest"}
        parameters.update(overrides)
        return connection_module.Connection(**parameters)

    def test_rejects_non_string_credentials_and_host(self):
        for name in ("hostname", "username", "password", "virtual_host", "locale"):
            with self.subTest(name=name), self.assertRaises(exception.AMQPInvalidArgument):
                self.build(**{name: 123})

    def test_rejects_oversized_credentials(self):
        for name in ("username", "password", "virtual_host", "locale"):
            with self.subTest(name=name), self.assertRaises(exception.AMQPInvalidArgument):
                self.build(**{name: OVERSIZED_NAME})

    def test_rejects_out_of_range_port(self):
        for port in (-1, 0x1_0000, "5672", True):
            with self.subTest(port=port), self.assertRaises(exception.AMQPInvalidArgument):
                self.build(port=port)

    def test_rejects_out_of_range_heartbeat(self):
        for heartbeat in (-1, validation.SHORT_MAX + 1, "60", 1.5):
            with self.subTest(heartbeat=heartbeat):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    self.build(heartbeat=heartbeat)

    def test_rejects_non_positive_timeout(self):
        for timeout in (0, -1, -0.5):
            with self.subTest(timeout=timeout):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    self.build(timeout=timeout)

    def test_rejects_non_numeric_timeout(self):
        for timeout in ("10", None, True):
            with self.subTest(timeout=timeout):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    self.build(timeout=timeout)

    def test_rejects_non_dict_option_tables(self):
        for name in ("ssl_options", "client_properties"):
            with self.subTest(name=name), self.assertRaises(exception.AMQPInvalidArgument):
                self.build(**{name: ["not-a-dict"]})

    def test_rejects_non_boolean_ssl(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            self.build(ssl="yes")

    def test_accepts_boundary_values(self):
        self.build(port=validation.SHORT_MAX, heartbeat=0, timeout=0.001)


class ConnectionChannelArgumentTests(unittest.IsolatedAsyncioTestCase):
    async def test_channel_rejects_bad_rpc_timeout(self):
        connection = connection_module.Connection("localhost", "guest", "guest")
        for rpc_timeout in (None, "60", 1.5, True, 0, -1):
            with self.subTest(rpc_timeout=rpc_timeout):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    await connection.channel(rpc_timeout=rpc_timeout)


class DomainRuleCompilationTests(unittest.TestCase):
    """The domain rules are compiled from the generated spec at import time."""

    def compile_with(self, asserts):
        with unittest.mock.patch.object(spec, "DOMAIN_ASSERTS", asserts):
            return validation._compile_domain_rules()

    def test_an_unknown_spec_assert_is_refused_at_compile_time(self):
        # A future spec revision that grows a new <assert> kind must not be silently
        # ignored, because ignoring it would let invalid values onto the wire.
        with self.assertRaises(ValueError) as context:
            self.compile_with({"queue-name": (("maxlen", "27"),)})
        self.assertEqual(
            "unhandled spec assert 'maxlen' on domain 'queue-name'", str(context.exception)
        )

    def test_each_known_assert_becomes_the_matching_rule_field(self):
        rules = self.compile_with(
            {"thing": (("length", "12"), ("regexp", "^a+$"), ("notnull", None))}
        )
        rule = rules["thing"]
        self.assertEqual(12, rule.max_bytes)
        self.assertEqual("^a+$", rule.pattern.pattern)
        self.assertTrue(rule.not_null)

    def test_a_domain_without_asserts_constrains_nothing(self):
        rule = self.compile_with({"thing": ()})["thing"]
        self.assertIsNone(rule.max_bytes)
        self.assertIsNone(rule.pattern)
        self.assertFalse(rule.not_null)

    def test_a_valueless_length_and_regexp_degrade_to_empty_constraints(self):
        rule = self.compile_with({"thing": (("length", None), ("regexp", None))})["thing"]
        self.assertEqual(0, rule.max_bytes)
        self.assertEqual("", rule.pattern.pattern)


class RequireDomainTests(unittest.TestCase):
    """require_domain enforces the length, regexp and notnull the spec declares."""

    def test_a_queue_name_over_127_bytes_is_rejected_with_the_domain_limit(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_domain("queue-name", "queue", "x" * 128)
        self.assertEqual("queue should be at most 127 bytes, got 128", str(context.exception))

    def test_a_queue_name_outside_the_spec_pattern_is_rejected(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_domain("queue-name", "queue", "no spaces")
        self.assertEqual(
            "queue should match ^[a-zA-Z0-9-_.:]*$ as required for queue-name",
            str(context.exception),
        )

    def test_a_notnull_domain_rejects_the_empty_string(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_domain("reply-text", "reply_text", "")
        self.assertEqual("reply_text should not be empty", str(context.exception))

    def test_not_null_can_be_demanded_for_a_domain_that_allows_empty(self):
        validation.require_domain("queue-name", "queue", "")
        with self.assertRaises(exception.AMQPInvalidArgument):
            validation.require_domain("queue-name", "queue", "", not_null=True)

    def test_a_domain_without_a_max_falls_back_to_the_shortstr_limit(self):
        validation.require_domain("reply-text", "reply_text", MAX_LENGTH_NAME)
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_domain("reply-text", "reply_text", OVERSIZED_NAME)
        self.assertEqual("reply_text should be at most 255 bytes, got 256", str(context.exception))

    def test_a_non_string_is_rejected_before_the_domain_rules_are_consulted(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_domain("queue-name", "queue", 123)
        self.assertEqual("queue should be a string", str(context.exception))

    def test_a_valid_name_passes_every_rule(self):
        validation.require_domain("exchange-name", "exchange", "amq.direct")


class EncodedLengthTests(unittest.TestCase):
    def test_a_string_that_cannot_be_encoded_is_reported_by_name(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_shortstr("queue", "\ud800")
        self.assertEqual(
            "queue contains characters that cannot be encoded", str(context.exception)
        )


class RequirePositiveNumberTests(unittest.TestCase):
    def test_zero_and_negatives_are_rejected(self):
        for value in (0, -1, -0.5):
            with self.subTest(value=value), self.assertRaises(exception.AMQPInvalidArgument):
                validation.require_positive_number("timeout", value)

    def test_ints_and_floats_above_zero_are_allowed(self):
        validation.require_positive_number("timeout", 1)
        validation.require_positive_number("timeout", 0.001)

    def test_booleans_and_strings_are_rejected_as_non_numbers(self):
        for value in (True, "1", None):
            with self.subTest(value=value):
                with self.assertRaises(exception.AMQPInvalidArgument) as context:
                    validation.require_positive_number("timeout", value)
                self.assertEqual("timeout should be an integer or float", str(context.exception))


class RequirePositiveIntegerTests(unittest.TestCase):
    def test_a_float_is_rejected_even_when_positive(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_positive_integer("rpc_timeout", 1.5)
        self.assertEqual("rpc_timeout should be an integer", str(context.exception))

    def test_zero_is_rejected_and_one_is_allowed(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_positive_integer("rpc_timeout", 0)
        self.assertEqual("rpc_timeout should be greater than 0", str(context.exception))
        validation.require_positive_integer("rpc_timeout", 1)


class RequireReplyCodeTests(unittest.TestCase):
    def test_zero_is_not_a_reply_code(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            validation.require_reply_code("reply_code", 0)
        self.assertEqual("reply_code should not be zero", str(context.exception))

    def test_a_real_reply_code_is_allowed(self):
        validation.require_reply_code("reply_code", 200)


if __name__ == "__main__":
    unittest.main()
