import unittest

from amqpstorm_aio import exception
from amqpstorm_aio import spec
from amqpstorm_aio.tests import factory
from amqpstorm_aio.tx import Tx


class ReplyWithFields:
    """A reply that carries fields; the real Tx *-Ok methods carry none."""

    __slots__ = ("reply_code",)

    def __init__(self, reply_code):
        self.reply_code = reply_code


class RecordingChannel:
    """Records every rpc_request and answers with the matching *-Ok method."""

    RESPONSES = {
        spec.Tx.Select: spec.Tx.SelectOk,
        spec.Tx.Commit: spec.Tx.CommitOk,
        spec.Tx.Rollback: spec.Tx.RollbackOk,
    }

    def __init__(self):
        self.requests = []
        self.errors = {}
        self.responses = {}

    async def rpc_request(self, method):
        self.requests.append(method)
        error = self.errors.get(type(method))
        if error is not None:
            raise error
        response = self.responses.get(type(method))
        if response is not None:
            return response
        return factory.build(self.RESPONSES[type(method)])

    @property
    def sent(self):
        return [type(method).name for method in self.requests]


class TxRequestTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.channel = RecordingChannel()
        self.tx = Tx(self.channel)

    async def test_select_sends_tx_select_and_returns_the_fieldless_ok_as_an_empty_dict(self):
        self.assertEqual({}, await self.tx.select())
        self.assertEqual(["Tx.Select"], self.channel.sent)

    async def test_commit_sends_tx_commit_and_returns_the_fieldless_ok_as_an_empty_dict(self):
        self.assertEqual({}, await self.tx.commit())
        self.assertEqual(["Tx.Commit"], self.channel.sent)

    async def test_rollback_sends_tx_rollback_and_returns_the_fieldless_ok_as_an_empty_dict(self):
        self.assertEqual({}, await self.tx.rollback())
        self.assertEqual(["Tx.Rollback"], self.channel.sent)

    async def test_every_call_returns_the_fields_of_the_reply_it_received(self):
        calls = {
            spec.Tx.Select: self.tx.select,
            spec.Tx.Commit: self.tx.commit,
            spec.Tx.Rollback: self.tx.rollback,
        }
        for request, call in calls.items():
            with self.subTest(request=request.name):
                self.channel.responses[request] = ReplyWithFields(reply_code=311)
                self.assertEqual({"reply_code": 311}, await call())

    async def test_select_marks_the_transaction_active(self):
        await self.tx.select()
        self.assertTrue(self.tx._tx_active)

    async def test_a_select_that_fails_still_counts_as_active(self):
        self.channel.errors[spec.Tx.Select] = exception.AMQPChannelError("gone")
        with self.assertRaises(exception.AMQPChannelError):
            await self.tx.select()
        self.assertTrue(self.tx._tx_active)

    async def test_reset_clears_the_active_transaction(self):
        await self.tx.select()
        self.tx.reset()
        self.assertFalse(self.tx._tx_active)

    async def test_commit_clears_the_active_transaction(self):
        await self.tx.select()
        await self.tx.commit()
        self.assertFalse(self.tx._tx_active)

    async def test_rollback_clears_the_active_transaction(self):
        await self.tx.select()
        await self.tx.rollback()
        self.assertFalse(self.tx._tx_active)

    async def test_a_failing_commit_still_clears_the_active_transaction(self):
        await self.tx.select()
        self.channel.errors[spec.Tx.Commit] = exception.AMQPChannelError("no transaction")
        with self.assertRaises(exception.AMQPChannelError):
            await self.tx.commit()
        self.assertFalse(self.tx._tx_active)

    async def test_a_failing_rollback_still_clears_the_active_transaction(self):
        await self.tx.select()
        self.channel.errors[spec.Tx.Rollback] = exception.AMQPChannelError("no transaction")
        with self.assertRaises(exception.AMQPChannelError):
            await self.tx.rollback()
        self.assertFalse(self.tx._tx_active)


class TxContextManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.channel = RecordingChannel()
        self.tx = Tx(self.channel)

    async def test_entering_the_context_selects_and_yields_the_same_tx(self):
        async with self.tx as entered:
            self.assertIs(self.tx, entered)
            self.assertEqual(["Tx.Select"], self.channel.sent)

    async def test_a_clean_exit_commits(self):
        async with self.tx:
            pass
        self.assertEqual(["Tx.Select", "Tx.Commit"], self.channel.sent)

    async def test_an_exception_in_the_body_rolls_back_and_propagates(self):
        with self.assertRaises(ValueError) as context:
            async with self.tx:
                raise ValueError("body blew up")
        self.assertEqual("body blew up", str(context.exception))
        self.assertEqual(["Tx.Select", "Tx.Rollback"], self.channel.sent)

    async def test_a_rollback_that_itself_fails_does_not_replace_the_body_exception(self):
        self.channel.errors[spec.Tx.Rollback] = exception.AMQPChannelError("rollback refused")
        with self.assertRaises(ValueError) as context:
            async with self.tx:
                raise ValueError("body blew up")
        self.assertEqual("body blew up", str(context.exception))
        self.assertEqual(["Tx.Select", "Tx.Rollback"], self.channel.sent)

    async def test_the_body_exception_is_logged_as_a_warning(self):
        with self.assertLogs("amqpstorm_aio.tx", level="WARNING") as logs:
            with self.assertRaises(ValueError):
                async with self.tx:
                    raise ValueError("body blew up")
        self.assertEqual(
            ["WARNING:amqpstorm_aio.tx:Leaving Transaction on exception: body blew up"],
            logs.output,
        )

    async def test_leaving_the_context_after_a_manual_commit_does_not_commit_again(self):
        async with self.tx:
            await self.tx.commit()
        self.assertEqual(["Tx.Select", "Tx.Commit"], self.channel.sent)

    async def test_leaving_on_an_exception_after_a_manual_rollback_does_not_roll_back_again(self):
        with self.assertRaises(ValueError):
            async with self.tx:
                await self.tx.rollback()
                raise ValueError("body blew up")
        self.assertEqual(["Tx.Select", "Tx.Rollback"], self.channel.sent)

    async def test_exiting_without_a_transaction_sends_nothing(self):
        await self.tx.__aexit__(None, None, None)
        self.assertEqual([], self.channel.sent)


if __name__ == "__main__":
    unittest.main()
