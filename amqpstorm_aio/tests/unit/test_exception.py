import unittest

from amqpstorm_aio import exception


class ExceptionHierarchyTests(unittest.TestCase):
    """The class tree must let callers catch broad or narrow, AMQPStorm-style."""

    def test_connection_error_subclasses_amqp_error(self):
        self.assertIsSubclass(exception.AMQPConnectionError, exception.AMQPError)

    def test_frame_error_subclasses_connection_error(self):
        self.assertIsSubclass(exception.AMQPFrameError, exception.AMQPConnectionError)

    def test_frame_error_subclasses_amqp_error(self):
        self.assertIsSubclass(exception.AMQPFrameError, exception.AMQPError)

    def test_channel_error_subclasses_amqp_error(self):
        self.assertIsSubclass(exception.AMQPChannelError, exception.AMQPError)

    def test_message_error_subclasses_channel_error(self):
        self.assertIsSubclass(exception.AMQPMessageError, exception.AMQPChannelError)

    def test_invalid_argument_subclasses_amqp_error_and_value_error(self):
        self.assertIsSubclass(exception.AMQPInvalidArgument, exception.AMQPError)
        self.assertIsSubclass(exception.AMQPInvalidArgument, ValueError)

    def test_channel_error_is_not_connection_error(self):
        self.assertNotIsSubclass(exception.AMQPChannelError, exception.AMQPConnectionError)

    def test_catching_amqp_error_catches_every_subclass_instance(self):
        subclasses = [
            exception.AMQPConnectionError,
            exception.AMQPFrameError,
            exception.AMQPChannelError,
            exception.AMQPMessageError,
            exception.AMQPInvalidArgument,
        ]
        for cls in subclasses:
            with self.subTest(cls=cls.__name__):
                with self.assertRaises(exception.AMQPError):
                    raise cls("boom")

    def test_invalid_argument_instance_caught_as_value_error(self):
        with self.assertRaises(ValueError):
            raise exception.AMQPInvalidArgument("bad argument")


class ReplyCodeTests(unittest.TestCase):
    """reply_code is stored verbatim and documentation maps it to the spec constant name."""

    def test_reply_code_stored_on_instance(self):
        error = exception.AMQPChannelError("no queue", reply_code=404)
        self.assertEqual(error.reply_code, 404)

    def test_reply_code_defaults_to_none(self):
        error = exception.AMQPError("plain failure")
        self.assertIsNone(error.reply_code)

    def test_documentation_404_maps_to_not_found(self):
        error = exception.AMQPChannelError("no queue", reply_code=404)
        self.assertEqual(error.documentation, "NOT-FOUND")

    def test_documentation_312_maps_to_no_route(self):
        error = exception.AMQPMessageError("returned", reply_code=312)
        self.assertEqual(error.documentation, "NO-ROUTE")

    def test_documentation_unknown_code_returns_empty_string(self):
        error = exception.AMQPError("mystery", reply_code=999)
        self.assertEqual(error.documentation, "")

    def test_documentation_without_code_returns_empty_string(self):
        error = exception.AMQPError("no code attached")
        self.assertEqual(error.documentation, "")

    def test_documentation_known_codes_match_spec_names(self):
        cases = [
            (311, "CONTENT-TOO-LARGE"),
            (320, "CONNECTION-FORCED"),
            (403, "ACCESS-REFUSED"),
            (406, "PRECONDITION-FAILED"),
            (501, "FRAME-ERROR"),
            (541, "INTERNAL-ERROR"),
        ]
        for reply_code, expected in cases:
            with self.subTest(reply_code=reply_code):
                error = exception.AMQPError("failure", reply_code=reply_code)
                self.assertEqual(error.documentation, expected)


class ExceptionMessageTests(unittest.TestCase):
    """str() of an exception must be exactly the message passed in."""

    def test_str_returns_message(self):
        self.assertEqual(str(exception.AMQPError("something went wrong")), "something went wrong")

    def test_str_returns_message_when_reply_code_present(self):
        error = exception.AMQPConnectionError("connection forced", reply_code=320)
        self.assertEqual(str(error), "connection forced")

    def test_str_returns_message_for_every_subclass(self):
        subclasses = [
            exception.AMQPConnectionError,
            exception.AMQPFrameError,
            exception.AMQPChannelError,
            exception.AMQPMessageError,
            exception.AMQPInvalidArgument,
        ]
        for cls in subclasses:
            with self.subTest(cls=cls.__name__):
                self.assertEqual(str(cls("kaboom")), "kaboom")


if __name__ == "__main__":
    unittest.main()
