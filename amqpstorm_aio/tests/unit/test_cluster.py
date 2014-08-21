import unittest

from amqpstorm_aio import connection as connection_module
from amqpstorm_aio import exception
from amqpstorm_aio import uri_connection

HOSTS = ["node-a.example", "node-b.example", "node-c.example"]


def build(hostname, **overrides):
    parameters = {"username": "guest", "password": "guest"}
    parameters.update(overrides)
    return connection_module.Connection(hostname, **parameters)


class HostnameNormalizationTests(unittest.TestCase):
    """A single hostname and a sequence of them end up in the same shape."""

    def test_single_hostname_becomes_a_one_entry_tuple(self):
        connection = build("only.example")
        self.assertEqual(("only.example",), connection.parameters["hostnames"])
        self.assertEqual("only.example", connection.parameters["hostname"])

    def test_list_keeps_every_host(self):
        connection = build(HOSTS)
        self.assertEqual(set(HOSTS), set(connection.parameters["hostnames"]))
        self.assertEqual(len(HOSTS), len(connection.parameters["hostnames"]))

    def test_hostname_stays_a_single_string_for_a_cluster(self):
        hostname = build(HOSTS).parameters["hostname"]
        self.assertIsInstance(hostname, str)
        self.assertIn(hostname, HOSTS)

    def test_tuples_are_accepted(self):
        self.assertEqual(set(HOSTS), set(build(tuple(HOSTS)).parameters["hostnames"]))

    def test_empty_sequence_is_rejected(self):
        for empty in ([], ()):
            with self.subTest(empty=empty):
                with self.assertRaises(exception.AMQPInvalidArgument) as context:
                    build(empty)
                self.assertIn("empty", str(context.exception))

    def test_non_string_entries_are_rejected(self):
        for hostname in ([123], ["ok", None], [b"bytes"]):
            with self.subTest(hostname=hostname):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    build(hostname)

    def test_non_sequence_hostnames_are_rejected(self):
        for hostname in (123, None, object(), {"host": "a"}):
            with self.subTest(hostname=hostname):
                with self.assertRaises(exception.AMQPInvalidArgument):
                    build(hostname)

    def test_bytes_are_rejected_rather_than_iterated(self):
        with self.assertRaises(exception.AMQPInvalidArgument) as context:
            build(b"node-a.example")
        self.assertIn("sequence of strings", str(context.exception))


class HostnameShuffleTests(unittest.TestCase):
    """Each connection draws its own order so a fleet spreads across the cluster."""

    SAMPLE_SIZE = 60

    def test_shuffling_keeps_every_host(self):
        for _ in range(10):
            connection = build(HOSTS)
            self.assertEqual(set(HOSTS), set(connection.parameters["hostnames"]))
            self.assertEqual(len(HOSTS), len(connection.parameters["hostnames"]))

    def test_clients_do_not_all_start_at_the_same_host(self):
        first_hosts = {
            build(HOSTS).parameters["hostname"] for _ in range(self.SAMPLE_SIZE)
        }
        self.assertGreater(len(first_hosts), 1)

    def test_a_single_hostname_is_unaffected(self):
        connection = build("only.example")
        self.assertEqual(("only.example",), connection.parameters["hostnames"])

    def test_hostname_matches_the_first_entry_after_shuffling(self):
        for _ in range(10):
            connection = build(HOSTS)
            self.assertEqual(
                connection.parameters["hostnames"][0], connection.parameters["hostname"]
            )

    def test_rotation_still_covers_every_host_after_shuffling(self):
        connection = build(HOSTS)
        orders = [connection._connect_order() for _ in range(len(HOSTS))]
        self.assertEqual({tuple(sorted(HOSTS))}, {tuple(sorted(order)) for order in orders})
        self.assertEqual(len(HOSTS), len({order[0] for order in orders}))


class ConnectOrderTests(unittest.TestCase):
    """Successive attempts start at a different host so reconnects spread out."""

    def test_first_attempt_uses_the_stored_order(self):
        connection = build(HOSTS)
        self.assertEqual(connection.parameters["hostnames"], connection._connect_order())

    def test_each_attempt_rotates_by_one(self):
        connection = build(HOSTS)
        hostnames = connection.parameters["hostnames"]
        self.assertEqual(
            [hostnames[index:] + hostnames[:index] for index in range(3)],
            [connection._connect_order() for _ in range(3)],
        )

    def test_rotation_wraps_around(self):
        connection = build(HOSTS)
        first = connection._connect_order()
        for _ in range(len(HOSTS) - 1):
            connection._connect_order()
        self.assertEqual(first, connection._connect_order())

    def test_every_attempt_offers_all_hosts(self):
        connection = build(HOSTS)
        for _ in range(6):
            self.assertEqual(set(HOSTS), set(connection._connect_order()))

    def test_single_host_always_yields_that_host(self):
        connection = build("only.example")
        for _ in range(3):
            self.assertEqual(("only.example",), connection._connect_order())


class ClusterUriTests(unittest.TestCase):
    """A comma-separated authority names every node in the cluster."""

    def test_multiple_hosts_share_the_trailing_port(self):
        parameters = uri_connection.parse_uri("amqp://guest:guest@a,b,c:5673/%2F")
        self.assertEqual(["a", "b", "c"], parameters["hostname"])
        self.assertEqual(5673, parameters["port"])

    def test_multiple_hosts_without_a_port_use_the_scheme_default(self):
        self.assertEqual(5672, uri_connection.parse_uri("amqp://a,b,c/%2F")["port"])
        self.assertEqual(5671, uri_connection.parse_uri("amqps://a,b,c/%2F")["port"])

    def test_credentials_survive_the_split(self):
        parameters = uri_connection.parse_uri("amqp://user:secret@a,b:5672/%2F")
        self.assertEqual("user", parameters["username"])
        self.assertEqual("secret", parameters["password"])

    def test_virtual_host_and_query_survive_the_split(self):
        parameters = uri_connection.parse_uri("amqp://a,b:5672/my-vhost?heartbeat=30")
        self.assertEqual("my-vhost", parameters["virtual_host"])
        self.assertEqual(30, parameters["heartbeat"])

    def test_whitespace_around_hosts_is_stripped(self):
        parameters = uri_connection.parse_uri("amqp://guest:guest@ a , b , c :5672/%2F")
        self.assertEqual(["a", "b", "c"], parameters["hostname"])

    def test_single_host_uri_is_untouched(self):
        parameters = uri_connection.parse_uri("amqp://guest:guest@localhost:5672/%2F")
        self.assertEqual("localhost", parameters["hostname"])

    def test_split_hosts_leaves_a_single_host_uri_alone(self):
        uri = "amqp://guest:guest@localhost:5672/%2F"
        self.assertEqual((uri, []), uri_connection.split_hosts(uri))

    def test_split_hosts_returns_a_parseable_single_host_uri(self):
        single, hostnames = uri_connection.split_hosts("amqp://guest:guest@a,b,c:5672/%2F")
        self.assertEqual("amqp://guest:guest@a:5672/%2F", single)
        self.assertEqual(["a", "b", "c"], hostnames)

    def test_a_trailing_comma_does_not_create_an_empty_host(self):
        self.assertEqual(["a", "b"], uri_connection.parse_uri("amqp://a,b,:5672/%2F")["hostname"])

    def test_cluster_uri_builds_a_connection(self):
        connection = uri_connection.UriConnection("amqp://guest:guest@a,b,c:5672/%2F")
        self.assertEqual({"a", "b", "c"}, set(connection.parameters["hostnames"]))
        self.assertEqual(5672, connection.parameters["port"])

    def test_cluster_uri_hosts_are_shuffled(self):
        first_hosts = {
            uri_connection.UriConnection("amqp://a,b,c:5672/%2F").parameters["hostname"]
            for _ in range(60)
        }
        self.assertGreater(len(first_hosts), 1)

    def test_cluster_uri_keeps_every_host(self):
        connection = uri_connection.UriConnection(
            "amqp://guest:guest@a,b,c:5672/%2F"
        )
        self.assertEqual({"a", "b", "c"}, set(connection.parameters["hostnames"]))


if __name__ == "__main__":
    unittest.main()
