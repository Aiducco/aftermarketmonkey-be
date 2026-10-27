"""
Tests for ``src.integrations.clients.ntw.api_client`` and the pure helpers in
``src.integrations.orders.ntw``.

The interesting cases are the two API behaviours that fail *silently* when they are not
contained, both verified live against TBC's QA gateway on 2026-10-26:

* ``criteria[]`` honours only the first TWO entries, answering HTTP 200 with no warning. A
  10-size request looks completely successful and returns two sizes.
* Errors arrive as HTTP 200 with an ``errorCd`` body and no ``product`` key, so status-code-only
  branching turns a failure into "nothing in stock".

No network and no database: ``_request`` is replaced, and the client takes its configuration as
constructor arguments so nothing reads settings.
"""
import datetime
import typing

from django.test import SimpleTestCase

from src.integrations.clients.ntw import api_client as ntw_api
from src.integrations.clients.ntw import exceptions as ntw_exceptions
from src.integrations.orders import ntw as ntw_orders


def _client(**overrides: typing.Any) -> ntw_api.NtwApiClient:
    kwargs = {
        "client_id": "cid",
        "client_secret": "secret",
        "base_url": "https://example.test/api",
        "ca_bundle": "/tmp/bundle.pem",
        "signing_secret": "signing",
    }
    kwargs.update(overrides)
    return ntw_api.NtwApiClient(**kwargs)


def _product(part_no: str, qty: int = 4, location: str = "3289") -> typing.Dict:
    return {
        "partNo": part_no,
        "brandName": "Michelin",
        "description": "Michelin Premier AS",
        "productCost": {"amount": "97.84", "currencyCode": "USD"},
        "estimatedDelivery": "Next Day",
        "estimatedDeliveryDate": "10/27/2026",
        "freightPerTire": 0.0,
        "availableSource": [{"locationId": location, "quantityAvailable": qty}],
    }


class CriteriaChunkingTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.client = _client()
        self.calls: typing.List[typing.Dict] = []

        def fake_request(method, path, body=None, params=None):
            self.calls.append({"method": method, "path": path, "body": body, "params": params})
            # Echo one product per criteria entry actually sent, so a dropped entry is visible.
            products = []
            for i, criterion in enumerate(body["criteria"]):
                products.append(_product("PN{}".format(criterion["tireSize"]["width"])))
            return {"product": products}

        self.client._request = fake_request

    @staticmethod
    def _criteria(n: int) -> typing.List[typing.Dict]:
        return [
            {"tireSize": {"width": str(200 + i), "ratio": "55", "diameter": "16"}, "requestedQty": 1}
            for i in range(n)
        ]

    def test_two_criteria_go_in_one_call(self):
        self.client.get_tire_inventory("10062387", self._criteria(2))
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(len(self.calls[0]["body"]["criteria"]), 2)

    def test_five_criteria_are_split_across_three_calls(self):
        """The whole point: TBC would silently answer only the first two of five."""
        products = self.client.get_tire_inventory("10062387", self._criteria(5))
        self.assertEqual(len(self.calls), 3)
        self.assertEqual([len(c["body"]["criteria"]) for c in self.calls], [2, 2, 1])
        # Every size asked for comes back, which is what a single call would not deliver.
        self.assertEqual(len(products), 5)

    def test_no_call_ever_sends_more_than_two_criteria(self):
        self.client.get_tire_inventory("10062387", self._criteria(11))
        self.assertTrue(all(len(c["body"]["criteria"]) <= 2 for c in self.calls))
        self.assertEqual(len(self.calls), 6)

    def test_empty_criteria_makes_no_call(self):
        self.assertEqual(self.client.get_tire_inventory("10062387", []), [])
        self.assertEqual(self.calls, [])

    def test_body_carries_the_fixed_search_shape(self):
        self.client.get_tire_inventory("10062387", self._criteria(1), store_number="99", dealer_code="D1")
        body = self.calls[0]["body"]
        self.assertEqual(body["customerId"], "10062387")
        self.assertEqual(body["searchType"], ntw_api.SEARCH_BY_TIRE_SIZE)
        self.assertEqual(body["customerType"], ntw_api.CUSTOMER_TYPE_EXTERNAL)
        self.assertEqual(body["storeNumber"], "99")
        self.assertEqual(self.calls[0]["params"], {"dealerCode": "D1"})


class MissingCustomerIdTests(SimpleTestCase):
    databases = []

    def test_a_missing_customer_id_is_refused_before_any_call(self):
        client = _client()
        called = []
        client._request = lambda *a, **k: called.append(1)
        with self.assertRaises(ntw_exceptions.NtwApiNotConfiguredError):
            client.get_tire_inventory("", [{"tireSize": {}, "requestedQty": 1}])
        self.assertEqual(called, [])


class TwoHundredWithErrorBodyTests(SimpleTestCase):
    databases = []

    def _client_returning(self, payload):
        client = _client()
        client._request = lambda method, path, body=None, params=None: payload
        return client

    def test_error_code_1006_is_an_empty_answer_not_a_failure(self):
        # "Could not get any inventory from backend" is what a size nobody stocks looks like.
        client = self._client_returning(
            {"errorCd": 1006, "errorDesc": "NOT_FOUND", "errorMsg": "Could not get any inventory from backend"}
        )
        self.assertEqual(
            client.get_tire_inventory("10062387", [{"tireSize": {"width": "225"}, "requestedQty": 1}]),
            [],
        )

    def test_any_other_error_body_raises_despite_the_200(self):
        client = self._client_returning(
            {"errorCd": 1005, "errorDesc": "BAD_REQUEST", "errorMsg": "Invalid request"}
        )
        with self.assertRaises(ntw_exceptions.NtwApiBusinessError) as ctx:
            client.get_tire_inventory("10062387", [{"tireSize": {"width": "225"}, "requestedQty": 1}])
        self.assertIn("Invalid request", str(ctx.exception))

    def test_a_single_product_object_is_accepted_as_well_as_a_list(self):
        client = self._client_returning({"product": _product("87266")})
        rows = client.get_tire_inventory("10062387", [{"tireSize": {"width": "225"}, "requestedQty": 1}])
        self.assertEqual(len(rows), 1)


class FindByPartNumberTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.client = _client()
        self.sent: typing.List[typing.Dict] = []

        def fake_request(method, path, body=None, params=None):
            self.sent.append(body)
            return {"product": [_product("87266"), _product("50004"), _product("5540843")]}

        self.client._request = fake_request

    def test_it_filters_the_size_response_down_to_the_part(self):
        row = self.client.find_tire_by_part_number(
            "10062387", "50004", {"width": "225", "ratio": "55", "diameter": "16"}
        )
        self.assertIsNotNone(row)
        self.assertEqual(row["partNo"], "50004")

    def test_part_matching_ignores_case_and_padding(self):
        row = self.client.find_tire_by_part_number(
            "10062387", "  87266 ", {"width": "225", "ratio": "55", "diameter": "16"}
        )
        self.assertEqual(row["partNo"], "87266")

    def test_a_part_the_size_query_did_not_return_is_none_not_an_error(self):
        # Out of stock, which is an answer -- the caller records a zero.
        row = self.client.find_tire_by_part_number(
            "10062387", "NOPE", {"width": "225", "ratio": "55", "diameter": "16"}
        )
        self.assertIsNone(row)

    def test_a_stock_read_always_asks_for_quantity_one(self):
        """requestedQty FILTERS on TBC's side: asking for 4 hides every tire with 3 in stock."""
        self.client.find_tire_by_part_number(
            "10062387", "87266", {"width": "225", "ratio": "55", "diameter": "16"}
        )
        self.assertEqual(self.sent[0]["criteria"][0]["requestedQty"], 1)
        self.assertEqual(ntw_api.STOCK_READ_QTY, 1)


class ConfigurationGuardTests(SimpleTestCase):
    databases = []

    def test_missing_gateway_credentials_refuse_construction(self):
        with self.assertRaises(ntw_exceptions.NtwApiNotConfiguredError):
            _client(client_id="")
        with self.assertRaises(ntw_exceptions.NtwApiNotConfiguredError):
            _client(client_secret="")

    def test_missing_base_url_refuses_construction(self):
        with self.assertRaises(ntw_exceptions.NtwApiNotConfiguredError):
            _client(base_url="")

    def test_a_missing_ca_bundle_raises_instead_of_skipping_verification(self):
        """
        TBC serves an incomplete chain, so a bundle is required. The failure mode this guards
        against is a well-meant verify=False, which would post a dealer's cost and orders into
        an unauthenticated connection.
        """
        client = _client(ca_bundle="")
        with self.assertRaises(ntw_exceptions.NtwApiNotConfiguredError) as ctx:
            client._verify()
        self.assertIn("NTW_API_CA_BUNDLE", str(ctx.exception))

    def test_signing_without_a_secret_refuses_rather_than_sending_unsigned(self):
        client = _client(signing_secret="")
        with self.assertRaises(ntw_exceptions.NtwApiNotConfiguredError):
            client._signature({"a": 1}, "1700000000")

    def test_signature_is_deterministic_and_covers_body_and_timestamp(self):
        client = _client()
        first = client._signature({"b": 2, "a": 1}, "1700000000")
        # Key order in the dict must not change the signature.
        self.assertEqual(first, client._signature({"a": 1, "b": 2}, "1700000000"))
        self.assertNotEqual(first, client._signature({"a": 1, "b": 3}, "1700000000"))
        self.assertNotEqual(first, client._signature({"b": 2, "a": 1}, "1700000001"))
        self.assertEqual(len(first), 64)


class OrderHelperTests(SimpleTestCase):
    databases = []

    def test_tbc_date_formats(self):
        self.assertEqual(ntw_orders._date("11/06/2026"), datetime.date(2026, 11, 6))
        self.assertEqual(ntw_orders._date("2026-11-06"), datetime.date(2026, 11, 6))
        self.assertIsNone(ntw_orders._date(""))
        self.assertIsNone(ntw_orders._date(None))
        self.assertIsNone(ntw_orders._date("sometime next week"))

    def test_delivery_status_is_none_unless_tbc_actually_says_so(self):
        # Their status vocabulary is undocumented and we have never seen a real value, so
        # anything unrecognised must stay unknown rather than be mapped optimistically.
        self.assertIsNone(ntw_orders._delivery_status("PROCESSING"))
        self.assertIsNone(ntw_orders._delivery_status(""))
        self.assertIsNone(ntw_orders._delivery_status(None))
        self.assertEqual(ntw_orders._delivery_status("In Transit"), "in_transit")
        self.assertEqual(ntw_orders._delivery_status("DELIVERED"), "delivered")
        self.assertEqual(ntw_orders._delivery_status("Cancelled"), "cancelled")

    def test_criteria_sizes_lets_two_lines_of_one_size_share_a_slot(self):
        size = {"width": "225", "ratio": "55", "diameter": "16"}
        criteria = [{"tireSize": size, "requestedQty": 1}]
        self.assertIn(size, ntw_orders.criteria_sizes(criteria))
        self.assertNotIn({"width": "235", "ratio": "65", "diameter": "17"},
                         ntw_orders.criteria_sizes(criteria))

    def test_decimal_parsing_tolerates_tbc_string_amounts(self):
        self.assertEqual(ntw_orders._decimal("97.84"), __import__("decimal").Decimal("97.84"))
        self.assertIsNone(ntw_orders._decimal(""))
        self.assertIsNone(ntw_orders._decimal(None))
        self.assertIsNone(ntw_orders._decimal("n/a"))
