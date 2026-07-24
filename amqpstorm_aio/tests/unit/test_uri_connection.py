import unittest

from amqpstorm_aio import connection
from amqpstorm_aio import exception
from amqpstorm_aio import uri_connection


class ParseUriTests(unittest.TestCase):
    """parse_uri turns amqp:// and amqps:// URIs into Connection keyword arguments."""

    def test_full_uri_parses_all_components(self):
        parameters = uri_connection.parse_uri("amqp://alice:secret@rabbit.example.com:5674/prod")
        self.assertEqual(parameters["hostname"], "rabbit.example.com")
        self.assertEqual(parameters["port"], 5674)
        self.assertEqual(parameters["username"], "alice")
        self.assertEqual(parameters["password"], "secret")
        self.assertEqual(parameters["virtual_host"], "prod")
        self.assertFalse(parameters["ssl"])

    def test_bare_amqp_host_applies_guest_defaults(self):
        parameters = uri_connection.parse_uri("amqp://localhost")
        self.assertEqual(parameters["hostname"], "localhost")
        self.assertEqual(parameters["port"], connection.DEFAULT_PORT)
        self.assertEqual(parameters["username"], "guest")
        self.assertEqual(parameters["password"], "guest")
        self.assertEqual(parameters["virtual_host"], "/")
        self.assertFalse(parameters["ssl"])

    def test_amqps_scheme_enables_ssl_with_ssl_port(self):
        parameters = uri_connection.parse_uri("amqps://localhost")
        self.assertTrue(parameters["ssl"])
        self.assertEqual(parameters["port"], connection.DEFAULT_SSL_PORT)

    def test_explicit_port_overrides_amqps_default(self):
        parameters = uri_connection.parse_uri("amqps://localhost:5674")
        self.assertTrue(parameters["ssl"])
        self.assertEqual(parameters["port"], 5674)

    def test_encoded_vhost_decodes_to_slash(self):
        parameters = uri_connection.parse_uri("amqp://localhost/%2F")
        self.assertEqual(parameters["virtual_host"], "/")

    def test_trailing_slash_only_uses_default_vhost(self):
        parameters = uri_connection.parse_uri("amqp://localhost/")
        self.assertEqual(parameters["virtual_host"], "/")

    def test_encoded_credentials_decode(self):
        parameters = uri_connection.parse_uri("amqp://user%20name:p%40ss%2Fword@localhost")
        self.assertEqual(parameters["username"], "user name")
        self.assertEqual(parameters["password"], "p@ss/word")

    def test_cluster_hosts_with_matching_ports_parse(self):
        parameters = uri_connection.parse_uri("amqp://alice:secret@h1:5674,h2:5674/prod")
        self.assertEqual(parameters["hostname"], ["h1", "h2"])
        self.assertEqual(parameters["port"], 5674)
        self.assertEqual(parameters["username"], "alice")
        self.assertEqual(parameters["password"], "secret")
        self.assertEqual(parameters["virtual_host"], "prod")

    def test_cluster_hosts_with_one_trailing_port_share_it(self):
        parameters = uri_connection.parse_uri("amqp://h1,h2,h3:5674/%2F")
        self.assertEqual(parameters["hostname"], ["h1", "h2", "h3"])
        self.assertEqual(parameters["port"], 5674)

    def test_cluster_hosts_with_disagreeing_ports_raise(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.parse_uri("amqp://h1:5672,h2:5673/%2F")

    def test_cluster_hosts_without_ports_use_the_scheme_default(self):
        parameters = uri_connection.parse_uri("amqp://h1,h2/%2F")
        self.assertEqual(parameters["hostname"], ["h1", "h2"])
        self.assertEqual(parameters["port"], connection.DEFAULT_PORT)

    def test_bracketed_ipv6_cluster_hosts_parse(self):
        parameters = uri_connection.parse_uri("amqp://u:p@[::1]:5674,[fe80::2]:5674/%2F")
        self.assertEqual(parameters["hostname"], ["::1", "fe80::2"])
        self.assertEqual(parameters["port"], 5674)

    def test_bracketed_ipv6_cluster_hosts_without_ports_use_the_scheme_default(self):
        parameters = uri_connection.parse_uri("amqp://u:p@[::1],[fe80::2]/%2F")
        self.assertEqual(parameters["hostname"], ["::1", "fe80::2"])
        self.assertEqual(parameters["port"], connection.DEFAULT_PORT)

    def test_a_port_on_one_bracketed_ipv6_host_is_shared_by_the_unported_ones(self):
        parameters = uri_connection.parse_uri("amqps://[::1],[fe80::2]:5671/%2F")
        self.assertEqual(parameters["hostname"], ["::1", "fe80::2"])
        self.assertEqual(parameters["port"], 5671)

    def test_non_numeric_port_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.parse_uri("amqp://localhost:notaport/%2F")

    def test_port_zero_raises_instead_of_silently_using_the_default(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.parse_uri("amqp://localhost:0/%2F")

    def test_query_options_convert_to_native_types(self):
        parameters = uri_connection.parse_uri(
            "amqp://localhost?heartbeat=120&timeout=7.5&locale=en_GB"
        )
        self.assertEqual(parameters["heartbeat"], 120)
        self.assertIsInstance(parameters["heartbeat"], int)
        self.assertEqual(parameters["timeout"], 7.5)
        self.assertIsInstance(parameters["timeout"], float)
        self.assertEqual(parameters["locale"], "en_GB")

    def test_amqps_query_collects_ssl_options(self):
        parameters = uri_connection.parse_uri(
            "amqps://localhost"
            "?certfile=/tmp/cert.pem&keyfile=/tmp/key.pem&ca_certs=/tmp/ca.pem"
            "&cert_reqs=required&verify_mode=none&server_hostname=rabbit.internal"
        )
        self.assertEqual(
            parameters["ssl_options"],
            {
                "certfile": "/tmp/cert.pem",
                "keyfile": "/tmp/key.pem",
                "ca_certs": "/tmp/ca.pem",
                "cert_reqs": "required",
                "verify_mode": "none",
                "server_hostname": "rabbit.internal",
            },
        )

    def test_ssl_option_on_plain_amqp_warns_and_is_dropped(self):
        with self.assertLogs(uri_connection.LOG, level="WARNING") as captured:
            parameters = uri_connection.parse_uri("amqp://localhost?certfile=/tmp/cert.pem")
        self.assertNotIn("ssl_options", parameters)
        self.assertTrue(
            any("unknown uri option" in line and "certfile" in line for line in captured.output),
            f"expected an 'unknown uri option' warning for certfile, got {captured.output}",
        )

    def test_check_hostname_falsy_strings_parse_false(self):
        for raw in ("False", "false", "0", "no", "off", "OFF", " false "):
            with self.subTest(raw=raw):
                parameters = uri_connection.parse_uri(f"amqps://localhost?check_hostname={raw}")
                self.assertIs(parameters["ssl_options"]["check_hostname"], False)

    def test_check_hostname_truthy_strings_parse_true(self):
        for raw in ("true", "True", "1", "yes", "on"):
            with self.subTest(raw=raw):
                parameters = uri_connection.parse_uri(f"amqps://localhost?check_hostname={raw}")
                self.assertIs(parameters["ssl_options"]["check_hostname"], True)

    def test_check_hostname_on_plain_amqp_warns_and_is_dropped(self):
        with self.assertLogs(uri_connection.LOG, level="WARNING") as captured:
            parameters = uri_connection.parse_uri("amqp://localhost?check_hostname=false")
        self.assertNotIn("ssl_options", parameters)
        self.assertTrue(
            any("unknown uri option" in line for line in captured.output),
            f"expected an 'unknown uri option' warning, got {captured.output}",
        )

    def test_unknown_query_key_warns(self):
        with self.assertLogs(uri_connection.LOG, level="WARNING") as captured:
            parameters = uri_connection.parse_uri("amqp://localhost?bogus_key=1")
        self.assertNotIn("bogus_key", parameters)
        self.assertTrue(
            any("unknown uri option" in line and "bogus_key" in line for line in captured.output),
            f"expected an 'unknown uri option' warning for bogus_key, got {captured.output}",
        )

    def test_no_query_leaves_optional_parameters_unset(self):
        parameters = uri_connection.parse_uri("amqp://localhost")
        self.assertNotIn("heartbeat", parameters)
        self.assertNotIn("timeout", parameters)
        self.assertNotIn("locale", parameters)
        self.assertNotIn("ssl_options", parameters)

    def test_bad_scheme_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.parse_uri("http://localhost")

    def test_missing_scheme_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.parse_uri("localhost:5672")

    def test_non_string_uri_raises_invalid_argument(self):
        for bad_uri in (None, 5672, b"amqp://localhost", ["amqp://localhost"]):
            with (
                self.subTest(bad_uri=bad_uri),
                self.assertRaises(exception.AMQPInvalidArgument),
            ):
                uri_connection.parse_uri(bad_uri)

    def test_garbage_heartbeat_raises_value_error(self):
        with self.assertRaises(ValueError):
            uri_connection.parse_uri("amqp://localhost?heartbeat=sixty")

    def test_garbage_timeout_raises_value_error(self):
        with self.assertRaises(ValueError):
            uri_connection.parse_uri("amqp://localhost?timeout=soon")


class SplitHostsTests(unittest.TestCase):
    """split_hosts only treats commas inside the authority as a cluster host list."""

    def test_a_comma_in_the_path_is_not_a_cluster_separator(self):
        uri = "amqp://localhost/a,b"
        self.assertEqual(uri_connection.split_hosts(uri), (uri, []))
        self.assertEqual(uri_connection.parse_uri(uri)["virtual_host"], "a,b")

    def test_a_comma_in_the_query_is_not_a_cluster_separator(self):
        uri = "amqp://localhost?locale=en_GB,en_US"
        self.assertEqual(uri_connection.split_hosts(uri), (uri, []))
        self.assertEqual(uri_connection.parse_uri(uri)["locale"], "en_GB,en_US")

    def test_a_comma_in_the_fragment_is_not_a_cluster_separator(self):
        uri = "amqp://localhost#a,b"
        self.assertEqual(uri_connection.split_hosts(uri), (uri, []))

    def test_an_authority_with_a_trailing_comma_is_left_alone(self):
        uri = "amqp://h1,/%2F"
        self.assertEqual(uri_connection.split_hosts(uri), (uri, []))

    def test_an_authority_of_only_commas_is_left_alone(self):
        uri = "amqp://,,/%2F"
        self.assertEqual(uri_connection.split_hosts(uri), (uri, []))

    def test_a_single_host_authority_does_not_raise_on_disagreeing_ports(self):
        uri = "amqp://h1:5672,:5673/%2F"
        self.assertEqual(uri_connection.split_hosts(uri), (uri, []))

    def test_whitespace_around_cluster_hosts_is_trimmed(self):
        single_host_uri, hostnames = uri_connection.split_hosts(
            "amqp://alice:secret@h1 :5674, h2 :5674/prod"
        )
        self.assertEqual(single_host_uri, "amqp://alice:secret@h1:5674/prod")
        self.assertEqual(hostnames, ["h1", "h2"])

    def test_the_rewritten_single_host_uri_keeps_the_credentials(self):
        single_host_uri, hostnames = uri_connection.split_hosts("amqp://alice:secret@h1,h2/prod")
        self.assertEqual(single_host_uri, "amqp://alice:secret@h1/prod")
        self.assertEqual(hostnames, ["h1", "h2"])

    def test_a_uri_without_a_scheme_separator_is_left_alone(self):
        uri = "h1,h2:5672"
        self.assertEqual(uri_connection.split_hosts(uri), (uri, []))


class UriConnectionTests(unittest.TestCase):
    """UriConnection feeds parsed URI values into the Connection constructor."""

    def test_full_uri_populates_connection_parameters(self):
        connection_instance = uri_connection.UriConnection(
            "amqps://alice:secret@rabbit.example.com:5671/prod"
            "?heartbeat=30&timeout=2.5&locale=en_GB"
        )
        parameters = connection_instance.parameters
        self.assertEqual(parameters["hostname"], "rabbit.example.com")
        self.assertEqual(parameters["port"], 5671)
        self.assertEqual(parameters["username"], "alice")
        self.assertEqual(parameters["password"], "secret")
        self.assertEqual(parameters["virtual_host"], "prod")
        self.assertEqual(parameters["heartbeat"], 30)
        self.assertEqual(parameters["timeout"], 2.5)
        self.assertEqual(parameters["locale"], "en_GB")
        self.assertTrue(parameters["ssl"])

    def test_bare_uri_applies_connection_defaults(self):
        connection_instance = uri_connection.UriConnection("amqp://localhost")
        parameters = connection_instance.parameters
        self.assertEqual(parameters["hostname"], "localhost")
        self.assertEqual(parameters["port"], connection.DEFAULT_PORT)
        self.assertEqual(parameters["username"], "guest")
        self.assertEqual(parameters["password"], "guest")
        self.assertEqual(parameters["virtual_host"], "/")
        self.assertEqual(parameters["heartbeat"], connection.DEFAULT_HEARTBEAT_TIMEOUT)
        self.assertEqual(parameters["timeout"], connection.DEFAULT_SOCKET_TIMEOUT)
        self.assertEqual(parameters["locale"], connection.DEFAULT_LOCALE)
        self.assertFalse(parameters["ssl"])
        self.assertEqual(parameters["ssl_options"], {})
        self.assertEqual(parameters["client_properties"], {})

    def test_code_supplied_ssl_options_win_over_the_uri(self):
        connection_instance = uri_connection.UriConnection(
            "amqps://localhost?ca_certs=/uri/ca.pem&check_hostname=false",
            ssl_options={"ca_certs": "/ctor/ca.pem", "keyfile": "/ctor/key.pem"},
        )
        self.assertEqual(
            connection_instance.parameters["ssl_options"],
            {
                "ca_certs": "/ctor/ca.pem",
                "keyfile": "/ctor/key.pem",
                "check_hostname": False,
            },
        )

    def test_a_uri_overriding_a_code_ssl_option_warns(self):
        with self.assertLogs("amqpstorm_aio.uri_connection", level="WARNING") as logs:
            uri_connection.UriConnection(
                "amqps://localhost?ca_certs=/uri/ca.pem",
                ssl_options={"ca_certs": "/ctor/ca.pem"},
            )
        self.assertIn("ca_certs", logs.output[0])

    def test_a_uri_may_still_add_ssl_options_the_caller_did_not_set(self):
        connection_instance = uri_connection.UriConnection(
            "amqps://localhost?check_hostname=false",
            ssl_options={"ca_certs": "/ctor/ca.pem"},
        )
        self.assertEqual(
            connection_instance.parameters["ssl_options"],
            {"ca_certs": "/ctor/ca.pem", "check_hostname": False},
        )

    def test_ctor_ssl_options_alone_survive_plain_uri(self):
        connection_instance = uri_connection.UriConnection(
            "amqp://localhost", ssl_options={"server_hostname": "rabbit.internal"}
        )
        self.assertEqual(
            connection_instance.parameters["ssl_options"],
            {"server_hostname": "rabbit.internal"},
        )

    def test_client_properties_pass_through(self):
        client_properties = {"platform": "test-suite", "information": "unit test"}
        connection_instance = uri_connection.UriConnection(
            "amqp://localhost", client_properties=client_properties
        )
        self.assertEqual(connection_instance.parameters["client_properties"], client_properties)

    def test_non_string_uri_raises_invalid_argument(self):
        with self.assertRaises(exception.AMQPInvalidArgument):
            uri_connection.UriConnection(None)


if __name__ == "__main__":
    unittest.main()
