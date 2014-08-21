import unittest

from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory


def _roundtrip(method):
    """Encode ``method``, decode the payload back, and return (payload, decoded)."""
    payload = bytes(method.encode())
    return payload, spec.METHOD_DECODERS[method.index](payload)


class BitFieldTests(unittest.TestCase):
    """A bit field sets its own position when true and leaves it clear when false."""

    def test_channel_flow_round_trips_active_in_both_directions(self):
        stopped, from_stopped = _roundtrip(factory.build(spec.Channel.Flow, active=False))
        started, from_started = _roundtrip(factory.build(spec.Channel.Flow, active=True))
        self.assertEqual(b"\x00\x14\x00\x14\x00", stopped)
        self.assertEqual(b"\x00\x14\x00\x14\x01", started)
        self.assertIs(False, from_stopped.active)
        self.assertIs(True, from_started.active)

    def test_channel_flow_ok_round_trips_active_in_both_directions(self):
        stopped, from_stopped = _roundtrip(factory.build(spec.Channel.FlowOk, active=False))
        started, from_started = _roundtrip(factory.build(spec.Channel.FlowOk, active=True))
        self.assertEqual(b"\x00\x14\x00\x15\x00", stopped)
        self.assertEqual(b"\x00\x14\x00\x15\x01", started)
        self.assertIs(False, from_stopped.active)
        self.assertIs(True, from_started.active)

    def test_basic_reject_round_trips_requeue_in_the_octet_after_the_delivery_tag(self):
        dropped, from_dropped = _roundtrip(
            factory.build(spec.Basic.Reject, delivery_tag=9, requeue=False)
        )
        requeued, from_requeued = _roundtrip(
            factory.build(spec.Basic.Reject, delivery_tag=77, requeue=True)
        )
        self.assertEqual(b"\x00\x00\x00\x00\x00\x00\x00\x09", dropped[4:12])
        self.assertEqual(b"\x00\x00\x00\x00\x00\x00\x00\x4d", requeued[4:12])
        self.assertEqual(0b0, dropped[12])
        self.assertEqual(0b1, requeued[12])
        self.assertEqual((9, False), (from_dropped.delivery_tag, from_dropped.requeue))
        self.assertEqual((77, True), (from_requeued.delivery_tag, from_requeued.requeue))

    def test_basic_recover_async_round_trips_requeue_in_both_directions(self):
        dropped, from_dropped = _roundtrip(factory.build(spec.Basic.RecoverAsync, requeue=False))
        requeued, from_requeued = _roundtrip(factory.build(spec.Basic.RecoverAsync, requeue=True))
        self.assertEqual(b"\x00\x3c\x00\x64\x00", dropped)
        self.assertEqual(b"\x00\x3c\x00\x64\x01", requeued)
        self.assertIs(False, from_dropped.requeue)
        self.assertIs(True, from_requeued.requeue)

    def test_basic_recover_round_trips_requeue_in_both_directions(self):
        dropped, from_dropped = _roundtrip(factory.build(spec.Basic.Recover, requeue=False))
        requeued, from_requeued = _roundtrip(factory.build(spec.Basic.Recover, requeue=True))
        self.assertEqual(b"\x00\x3c\x00\x6e\x00", dropped)
        self.assertEqual(b"\x00\x3c\x00\x6e\x01", requeued)
        self.assertIs(False, from_dropped.requeue)
        self.assertIs(True, from_requeued.requeue)

    def test_exchange_declare_with_durable_false_clears_only_the_durable_bit(self):
        method = factory.build(
            spec.Exchange.Declare,
            exchange="ex",
            type="direct",
            passive=True,
            durable=False,
            auto_delete=True,
            internal=True,
            no_wait=True,
            arguments={},
        )
        payload, decoded = _roundtrip(method)
        bit_offset = 4 + 2 + 1 + len(b"ex") + 1 + len(b"direct")
        self.assertEqual(0b11101, payload[bit_offset])
        self.assertIs(False, decoded.durable)
        self.assertEqual(method, decoded)

    def test_exchange_declare_round_trips_durable_as_bit_two_of_the_flag_octet(self):
        bit_offset = 4 + 2 + 1 + len(b"ex") + 1 + len(b"direct")
        transient, from_transient = _roundtrip(
            factory.build(spec.Exchange.Declare, exchange="ex", type="direct", durable=False)
        )
        durable, from_durable = _roundtrip(
            factory.build(spec.Exchange.Declare, exchange="ex", type="direct", durable=True)
        )
        self.assertEqual(0b0, transient[bit_offset])
        self.assertEqual(0b10, durable[bit_offset])
        self.assertIs(False, from_transient.durable)
        self.assertIs(True, from_durable.durable)
        self.assertIs(False, from_durable.passive)
        self.assertIs(False, from_durable.auto_delete)


if __name__ == "__main__":
    unittest.main()
