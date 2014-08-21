import asyncio
import gc
import time
import unittest

from amqpstorm_aio import exception
from amqpstorm_aio import heartbeat

TIMEOUT = 0.2
INTERVAL = TIMEOUT / 2
WAIT_DEADLINE = 2.0
TASK_NAME = "amqpstorm-aio-heartbeat"
_DEATH_THRESHOLD_INTERVALS = heartbeat._DEATH_THRESHOLD_INTERVALS


def _live_heartbeat_tasks():
    return [task for task in asyncio.all_tasks() if task.get_name() == TASK_NAME]


class CollectableOwner:
    """Owner of the bound callbacks the heartbeat is only allowed to hold weakly."""

    def __init__(self, sends, deaths):
        self.sends = sends
        self.deaths = deaths

    def send_heartbeat(self):
        self.sends.append(None)

    def on_dead(self, error):
        self.deaths.append(error)


def _make_send_recorder(sends, owner_box):
    """Build a plain function the caller keeps no reference to of its own."""

    def record_send():
        sends.append(None)
        owner_box[0].register_read()

    return record_send


class HeartbeatTests(unittest.IsolatedAsyncioTestCase):
    async def _wait_until(self, condition, message):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + WAIT_DEADLINE
        while not condition():
            if loop.time() >= deadline:
                self.fail(message)
            await asyncio.sleep(0.005)

    async def test_start_with_zero_timeout_returns_false_and_stays_inert(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)

        checker = heartbeat.Heartbeat(0, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertFalse(checker.start())
        self.assertEqual([], _live_heartbeat_tasks())
        await asyncio.sleep(INTERVAL * 3)
        self.assertEqual([], sends, "disabled checker must never send heartbeats")
        self.assertEqual([], deaths, "disabled checker must never declare the connection dead")

    async def test_idle_interval_fires_send_callback(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)
            checker.register_read()

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        await self._wait_until(lambda: len(sends) >= 1, "no heartbeat sent on an idle interval")
        self.assertEqual([], deaths)

    async def test_own_heartbeat_send_does_not_suppress_next_interval(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)
            checker.register_write()
            checker.register_read()

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        await self._wait_until(
            lambda: len(sends) >= 2,
            "two idle intervals should produce two heartbeat sends",
        )
        self.assertEqual([], deaths)

    async def test_register_write_before_interval_suppresses_that_intervals_send(self):
        sends = []
        deaths = []
        loop = asyncio.get_running_loop()

        def record_send():
            sends.append(loop.time())
            checker.register_read()

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        started_at = loop.time()
        self.assertTrue(checker.start())
        checker.register_write()
        await self._wait_until(lambda: len(sends) >= 1, "heartbeat never sent after suppression")
        self.assertGreater(
            sends[0] - started_at,
            INTERVAL * 1.5,
            "the registered write should have suppressed the first interval's send",
        )
        self.assertEqual([], deaths)

    async def test_two_read_silent_intervals_invoke_on_dead_with_exact_message(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        await self._wait_until(lambda: len(deaths) >= 1, "connection never declared dead")
        self.assertEqual(1, len(deaths))
        error = deaths[0]
        self.assertIsInstance(error, exception.AMQPConnectionError)
        self.assertEqual(
            f"Connection dead, no heartbeat or data received in >= {int(INTERVAL * 2)}s",
            str(error),
        )

    async def test_register_read_between_intervals_resets_death_threshold(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)
            if len(sends) <= 2:
                checker.register_read()

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        await self._wait_until(lambda: len(deaths) >= 1, "connection never declared dead")
        self.assertEqual(
            4,
            len(sends),
            "reads during the first two intervals should defer death by two more intervals",
        )
        self.assertIsInstance(deaths[0], exception.AMQPConnectionError)

    async def test_checks_delayed_by_a_blocked_loop_do_not_add_up_to_death(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        for _ in range(_DEATH_THRESHOLD_INTERVALS + 1):
            await asyncio.sleep(0.001)
            time.sleep(INTERVAL * 2.5)
            await asyncio.sleep(0.001)
        self.assertEqual([], deaths, "a starved interval is not evidence of a dead peer")

    async def test_death_stops_task_and_further_sends(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        await self._wait_until(lambda: len(deaths) >= 1, "connection never declared dead")
        sends_at_death = len(sends)
        await asyncio.sleep(INTERVAL * 4)
        self.assertEqual(sends_at_death, len(sends), "send callback fired after death")
        self.assertEqual(1, len(deaths), "on_dead fired again after the task should have stopped")
        self.assertEqual([], _live_heartbeat_tasks())

    async def test_stop_cancels_running_task(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        tasks = _live_heartbeat_tasks()
        self.assertEqual(1, len(tasks))
        checker.stop()
        await asyncio.sleep(INTERVAL * 3)
        self.assertTrue(tasks[0].cancelled())
        self.assertEqual([], sends, "no heartbeat may be sent after stop()")
        self.assertEqual([], deaths, "no death may be declared after stop()")
        self.assertEqual([], _live_heartbeat_tasks())

    async def test_start_twice_replaces_task_without_leaking(self):
        sends = []
        deaths = []

        def record_send():
            sends.append(None)
            checker.register_read()

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        first = _live_heartbeat_tasks()
        self.assertEqual(1, len(first))
        self.assertTrue(checker.start())
        await asyncio.sleep(0.01)
        self.assertTrue(first[0].cancelled(), "second start should cancel the first task")
        live = _live_heartbeat_tasks()
        self.assertEqual(1, len(live), "restarting must not leave two heartbeat tasks running")
        self.assertNotIn(first[0], live)
        await self._wait_until(
            lambda: len(sends) >= 1,
            "the replacement task should still send heartbeats",
        )

    async def test_bound_callbacks_are_held_weakly_and_do_not_pin_their_owner(self):
        sends = []
        deaths = []
        owner = CollectableOwner(sends, deaths)
        checker = heartbeat.Heartbeat(TIMEOUT, owner.send_heartbeat, owner.on_dead)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        await self._wait_until(lambda: len(sends) >= 1, "no heartbeat sent on an idle interval")

        del owner
        gc.collect()
        await self._wait_until(
            lambda: not _live_heartbeat_tasks(),
            "the checker kept running after its callback owner was collected",
        )
        sends_at_collection = len(sends)
        await asyncio.sleep(INTERVAL * 3)
        self.assertEqual(sends_at_collection, len(sends), "send callback fired after collection")
        self.assertEqual([], deaths, "a collected owner must not be told the connection died")

    async def test_a_collected_on_dead_owner_stops_the_checker_without_declaring_death(self):
        sends = []
        deaths = []
        owner = CollectableOwner(sends, deaths)

        def record_send():
            sends.append(None)

        checker = heartbeat.Heartbeat(TIMEOUT, record_send, owner.on_dead)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        del owner
        gc.collect()
        await self._wait_until(
            lambda: not _live_heartbeat_tasks(),
            "the checker kept running after its on_dead owner was collected",
        )
        self.assertEqual([], sends, "no heartbeat may be sent once on_dead has been collected")
        self.assertEqual([], deaths)

    async def test_a_collected_send_heartbeat_owner_stops_the_checker(self):
        sends = []
        deaths = []
        owner = CollectableOwner(sends, deaths)

        checker = heartbeat.Heartbeat(TIMEOUT, owner.send_heartbeat, deaths.append)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        tasks = _live_heartbeat_tasks()
        self.assertEqual(1, len(tasks))
        del owner
        gc.collect()
        await self._wait_until(
            lambda: not _live_heartbeat_tasks(),
            "the checker kept running after its send_heartbeat owner was collected",
        )
        self.assertIsNone(
            tasks[0].exception(),
            "the checker must retire quietly, not crash on the collected callback",
        )
        self.assertEqual([], sends, "a collected owner must not receive heartbeat sends")
        self.assertEqual(
            [],
            deaths,
            "a collected send_heartbeat owner is not evidence of a dead connection",
        )

    async def test_a_plain_function_callback_is_held_strongly_across_collection(self):
        sends = []
        deaths = []
        owner_box = []
        checker = heartbeat.Heartbeat(TIMEOUT, _make_send_recorder(sends, owner_box), deaths.append)
        owner_box.append(checker)
        self.addCleanup(checker.stop)

        self.assertTrue(checker.start())
        gc.collect()
        await self._wait_until(
            lambda: len(sends) >= 2,
            "a plain function callback must survive garbage collection",
        )
        self.assertEqual(1, len(_live_heartbeat_tasks()))
        self.assertEqual([], deaths)


if __name__ == "__main__":
    unittest.main()
