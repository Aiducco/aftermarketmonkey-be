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


class ByProductIdLookupTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.client = _client()
        self.sent: typing.List[typing.Dict] = []

        def fake_request(method, path, body=None, params=None):
            self.sent.append(body)
            wanted = {c["partNo"] for c in body.get("criteria", [])}
            return {"product": [_product(pn) for pn in ("87266", "50004", "5540843") if pn in wanted]}

        self.client._request = fake_request

    def test_it_returns_the_requested_part(self):
        row = self.client.find_tire_by_part_number("10062387", "50004", "BFGoodrich")
        self.assertIsNotNone(row)
        self.assertEqual(row["partNo"], "50004")

    def test_part_matching_ignores_case_and_padding(self):
        row = self.client.find_tire_by_part_number("10062387", "  87266 ", "Michelin")
        self.assertEqual(row["partNo"], "87266")

    def test_a_part_tbc_does_not_list_is_none_not_an_error(self):
        row = self.client.find_tire_by_part_number("10062387", "NOPE", "Michelin")
        self.assertIsNone(row)

    def test_a_blank_part_number_is_never_sent(self):
        """
        A blank partNo is a silent BRAND WILDCARD on this API -- {"brandName": "Cooper",
        "partNo": ""} returned Cooper's whole 131-product catalog, HTTP 200. One such row would
        both inflate the response and break line matching, so it must never leave the client.
        """
        self.sent.clear()
        found = self.client.get_inventory_by_parts(
            "10062387", [("Cooper", ""), ("Michelin", "87266"), ("", "123")]
        )
        self.assertEqual(len(self.sent), 1)
        sent_parts = [c["partNo"] for c in self.sent[0]["criteria"]]
        self.assertEqual(sent_parts, ["87266"])
        self.assertIn(("MICHELIN", "87266"), found)

    def test_batches_are_split_but_not_capped_at_two(self):
        """The two-entry cap is a ByTireSize behaviour; ByProductId took 286 in one call."""
        self.sent.clear()
        pairs = [("Michelin", str(90000 + i)) for i in range(250)]
        self.client.get_inventory_by_parts("10062387", pairs, batch_size=200)
        self.assertEqual([len(c["criteria"]) for c in self.sent], [200, 50])

    def test_a_stock_read_asks_for_quantity_zero(self):
        """
        requestedQty FILTERS rather than requests. At 1 the gateway returns only what is in
        stock, so an out-of-stock part looks unknown instead of empty; measured on 25 parts,
        qty=1 returned 7 and qty=0 returned 11, the extra four being zero-stock rows.
        """
        self.client.find_tire_by_part_number("10062387", "87266", "Michelin")
        self.assertEqual(self.sent[0]["criteria"][0]["requestedQty"], 0)
        self.assertEqual(ntw_api.STOCK_READ_QTY, 0)


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

    def test_the_order_endpoints_send_no_query_parameters(self):
        """
        TBC's portal advertises signature/timestamp query params on /v1/order; the Order Service
        v1.0 spec says the API has no query parameters at all. This asserts we follow the spec,
        because the earlier reading of the portal is what blocked ordering entirely.
        """
        client = _client()
        seen = {}

        def fake(method, path, body=None, params=None):
            seen.update({"method": method, "path": path, "params": params})
            return [{"id": "SAP_1", "items": []}]

        client._request = fake
        client.create_order({"customerId": "1", "items": []})
        self.assertIsNone(seen["params"])
        self.assertEqual(seen["path"], "/v1/order")

        client.get_order("SAP_1")
        self.assertIsNone(seen["params"])
        self.assertEqual(seen["path"], "/v1/order/SAP_1")


class OrderHelperTests(SimpleTestCase):
    databases = []

    def test_both_tbc_date_formats(self):
        """
        The two APIs disagree: inventory sends M/D/YYYY, the order service sends RFC 3339 with a
        trailing Z and no seconds. Only knowing the first silently dropped every order ETA.
        """
        self.assertEqual(ntw_orders._date("11/06/2026"), datetime.date(2026, 11, 6))
        self.assertEqual(ntw_orders._date("2023-11-01T13:50Z"), datetime.date(2023, 11, 1))
        self.assertEqual(ntw_orders._date("2017-06-01T19:30Z"), datetime.date(2017, 6, 1))
        self.assertEqual(ntw_orders._date("2026-10-27T08:20:33+00:00"), datetime.date(2026, 10, 27))
        self.assertEqual(ntw_orders._date("2026-11-06"), datetime.date(2026, 11, 6))
        self.assertIsNone(ntw_orders._date(""))
        self.assertIsNone(ntw_orders._date(None))
        self.assertIsNone(ntw_orders._date("sometime next week"))

    def test_delivery_status_is_a_closed_vocabulary(self):
        """
        TBC documents exactly three statuses. Anything else yields no signal rather than a
        guess, so a value they add later shows as unknown instead of being mis-asserted.
        """
        self.assertEqual(ntw_orders._delivery_status("Rejected"), "cancelled")
        self.assertEqual(ntw_orders._delivery_status("  delivered "), "delivered")
        self.assertIsNone(ntw_orders._delivery_status("Processing"))
        for unknown in ("Shipped", "In Transit", "Invoiced", "", None):
            self.assertIsNone(ntw_orders._delivery_status(unknown), unknown)

    def test_delivered_is_tbcs_billing_signal_not_proof_of_arrival(self):
        """
        TBC's "Delivered" means invoiced OR shipped OR delivered -- they collapse all three. It
        is the nearest thing to an invoice feed this integration has, since there is no invoice
        endpoint, and it must not be read as "the customer has the tires".
        """
        self.assertEqual(ntw_orders._delivery_status("Delivered"), "delivered")
        self.assertNotIn("INVOICED", ntw_orders._TBC_DELIVERY_STATUS)

    def test_carrier_catalogue_matches_the_published_enumeration(self):
        codes = dict((c, s) for s, c in ntw_api.CARRIER_SERVICE_CODES)
        self.assertEqual(len(ntw_api.CARRIER_SERVICE_CODES), 20)
        self.assertEqual(codes["FEDEX_GROUND"], "FEDX")
        self.assertEqual(codes["UPS_GROUND"], "UPSN")
        self.assertEqual(codes["WILL CALL"], "NTWL")
        # Only sent on a drop-ship order -- TBC ignores them otherwise.
        self.assertIn(("NTWL", "NTW_GROUND"), ntw_api.CARRIER_SERVICE_CODES)
        self.assertNotIn({"width": "235", "ratio": "65", "diameter": "17"},
                         ntw_orders.criteria_sizes(criteria))

    def test_decimal_parsing_tolerates_tbc_string_amounts(self):
        self.assertEqual(ntw_orders._decimal("97.84"), __import__("decimal").Decimal("97.84"))
        self.assertIsNone(ntw_orders._decimal(""))
        self.assertIsNone(ntw_orders._decimal(None))
        self.assertIsNone(ntw_orders._decimal("n/a"))


class _FakeDistributorOrder:
    def __init__(self, number):
        self.distributor_order_number = number


class _FakeRelated:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return self._rows


class _FakePurchaseOrder:
    def __init__(self, numbers):
        self.distributor_orders = _FakeRelated([_FakeDistributorOrder(n) for n in numbers])


def _adapter_with(responses):
    """
    An NtwOrderAdapter without its __init__: construction needs a CompanyProviders row and real
    credentials, and none of the invoice path touches either.
    """
    adapter = ntw_orders.NtwOrderAdapter.__new__(ntw_orders.NtwOrderAdapter)
    adapter._client = type("C", (), {"get_order": staticmethod(lambda oid: responses[oid])})()
    return adapter


def _status_payload(status, **extra):
    payload = {
        "status": status,
        "estDelivery": "2023-11-01T13:50Z",
        "location": {"id": "3021", "name": "RIVIERA BEACH FL2", "city": "RIVIERA BEACH", "state": "FL"},
        "pickUpPhone": "(800) 474-9980",
        "items": [{"brandName": "Sailun", "partNo": "9630377", "qty": 2}],
    }
    payload.update(extra)
    return payload


class InvoiceFromOrderStatusTests(SimpleTestCase):
    """
    NTW has no invoice endpoint. TBC's status "Delivered" means invoiced OR shipped OR
    delivered, so it is the only billing signal available and get_invoices is built on it.
    """
    databases = []

    def test_a_delivered_order_produces_one_invoice(self):
        adapter = _adapter_with({"2244850569": _status_payload("Delivered", trackingNumber="1Z999")})
        invoices = adapter.get_invoices(_FakePurchaseOrder(["2244850569"]))
        self.assertEqual(len(invoices), 1)
        inv = invoices[0]
        self.assertEqual(inv.invoice_number, "2244850569")
        self.assertEqual(inv.distributor_order_number, "2244850569")
        self.assertEqual([t.tracking_number for t in inv.tracking], ["1Z999"])
        self.assertEqual(len(inv.line_items), 1)
        self.assertEqual(inv.line_items[0].part_number, "9630377")
        self.assertEqual(inv.line_items[0].quantity, 2)
        self.assertEqual(inv.line_items[0].warehouse_code, "3021")

    def test_amounts_are_never_invented_from_our_own_quote(self):
        """An invoice must report what TBC billed. We hold a quote, so these stay blank."""
        adapter = _adapter_with({"1": _status_payload("Delivered")})
        inv = adapter.get_invoices(_FakePurchaseOrder(["1"]))[0]
        self.assertIsNone(inv.total_price)
        self.assertIsNone(inv.freight)
        self.assertIsNone(inv.amount_due)
        self.assertIsNone(inv.paid_amount)
        self.assertIsNone(inv.invoice_date)
        self.assertIsNone(inv.line_items[0].unit_price)
        self.assertIsNone(inv.line_items[0].total_price)
        self.assertIn("not a real invoice document", inv.comments)

    def test_an_unbilled_or_cancelled_order_yields_no_invoice(self):
        adapter = _adapter_with({
            "a": _status_payload("Processing"),
            "b": _status_payload("Rejected"),
        })
        self.assertEqual(adapter.get_invoices(_FakePurchaseOrder(["a", "b"])), [])

    def test_one_po_can_accumulate_several_invoices(self):
        adapter = _adapter_with({
            "o1": _status_payload("Delivered", trackingNumber="T1"),
            "o2": _status_payload("Processing"),
            "o3": _status_payload("Delivered", trackingNumber="T3"),
        })
        invoices = adapter.get_invoices(_FakePurchaseOrder(["o1", "o2", "o3"]))
        self.assertEqual([i.invoice_number for i in invoices], ["o1", "o3"])

    def test_no_distributor_orders_means_no_call_and_no_invoices(self):
        adapter = ntw_orders.NtwOrderAdapter.__new__(ntw_orders.NtwOrderAdapter)
        adapter._client = type("C", (), {"get_order": staticmethod(
            lambda oid: (_ for _ in ()).throw(AssertionError("should not be called")))})()
        self.assertEqual(adapter.get_invoices(_FakePurchaseOrder([])), [])

    def test_the_adapter_advertises_invoices(self):
        adapter = ntw_orders.NtwOrderAdapter.__new__(ntw_orders.NtwOrderAdapter)
        self.assertTrue(adapter.supports_invoices())
        self.assertFalse(adapter.supports_cancel())
        self.assertTrue(adapter.supports_shipping_method_selection())
