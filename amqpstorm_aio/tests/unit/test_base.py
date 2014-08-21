import unittest

from amqpstorm_aio import base
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory


class StatefulTests(unittest.TestCase):
    """State starts CLOSED, moves only via set_state, and each is_* checks one exact state."""

    def setUp(self):
        self.stateful = base.Stateful()

    def test_initial_state_is_closed(self):
        self.assertEqual(self.stateful.current_state, base.Stateful.CLOSED)
        self.assertTrue(self.stateful.is_closed)

    def test_set_state_opening_updates_current_state(self):
        self.stateful.set_state(base.Stateful.OPENING)
        self.assertEqual(self.stateful.current_state, base.Stateful.OPENING)

    def test_full_lifecycle_transitions_in_order(self):
        lifecycle = [
            base.Stateful.OPENING,
            base.Stateful.OPEN,
            base.Stateful.CLOSING,
            base.Stateful.CLOSED,
        ]
        for state in lifecycle:
            self.stateful.set_state(state)
            self.assertEqual(self.stateful.current_state, state)

    def test_each_state_sets_exactly_one_is_flag(self):
        expectations = [
            (base.Stateful.CLOSED, "is_closed"),
            (base.Stateful.CLOSING, "is_closing"),
            (base.Stateful.OPENING, "is_opening"),
            (base.Stateful.OPEN, "is_open"),
        ]
        flags = ["is_closed", "is_closing", "is_opening", "is_open"]
        for state, expected_flag in expectations:
            with self.subTest(state=state):
                self.stateful.set_state(state)
                for flag in flags:
                    self.assertEqual(
                        getattr(self.stateful, flag),
                        flag == expected_flag,
                        f"{flag} should be {flag == expected_flag} in state {state}",
                    )

    def test_closing_state_is_neither_open_nor_closed(self):
        self.stateful.set_state(base.Stateful.CLOSING)
        self.assertFalse(self.stateful.is_open)
        self.assertFalse(self.stateful.is_closed)
        self.assertTrue(self.stateful.is_closing)

    def test_state_constants_are_distinct(self):
        states = {
            base.Stateful.CLOSED,
            base.Stateful.CLOSING,
            base.Stateful.OPENING,
            base.Stateful.OPEN,
        }
        self.assertEqual(len(states), 4)


class MethodToDictTests(unittest.TestCase):
    def test_queue_declare_returns_all_slot_fields(self):
        method = factory.build(
            spec.Queue.Declare, queue="fruit", durable=True, arguments={"x-max-length": 10}
        )
        self.assertEqual(
            base.method_to_dict(method),
            {
                "queue": "fruit",
                "passive": False,
                "durable": True,
                "exclusive": False,
                "auto_delete": False,
                "no_wait": False,
                "arguments": {"x-max-length": 10},
            },
        )

    def test_queue_declare_ok_returns_all_slot_fields(self):
        method = factory.build(
            spec.Queue.DeclareOk, queue="fruit", message_count=5, consumer_count=2
        )
        self.assertEqual(
            base.method_to_dict(method),
            {"queue": "fruit", "message_count": 5, "consumer_count": 2},
        )

    def test_result_keys_match_method_slots(self):
        method = factory.build(spec.Basic.Get, queue="fruit")
        self.assertEqual(set(base.method_to_dict(method)), set(method.__slots__))

    def test_excludes_class_level_metadata(self):
        result = base.method_to_dict(factory.build(spec.Queue.DeclareOk))
        self.assertNotIn("index", result)
        self.assertNotIn("name", result)
        self.assertNotIn("synchronous", result)


class HandlerTests(unittest.TestCase):
    def test_init_stores_channel(self):
        channel = object()
        handler = base.Handler(channel)
        self.assertIs(handler._channel, channel)


if __name__ == "__main__":
    unittest.main()
