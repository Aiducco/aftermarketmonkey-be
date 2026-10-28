"""
Tests for Motor State's ordering client and adapter.

Payloads are real responses from api.motorstate.com, captured 2026-10-28 with a live dealer
key. Two of them encode things the Swagger spec does not say and that fail silently if you get
them wrong -- a mandatory PurchaseOrderNumber on calls that create nothing, and rate entries
with no Rate key at all.

No network.
"""
import datetime
from decimal import Decimal

from django.test import SimpleTestCase

from src.integrations.clients.motorstate import exceptions as ms_exceptions
from src.integrations.clients.motorstate import order_client as oc
from src.integrations.orders import base as order_base
from src.integrations.orders import exceptions as order_exceptions
from src.integrations.orders import motorstate as ms_orders

CREDS = {"api_key": "81b0c2d0-dead-beef-0000-000000000000"}

# GET /api/OrderOptions, real shape. Trident has a card; terms accounts report ON ACCOUNT.
ORDER_OPTIONS = {
    "PaymentMethods": [
        {"Code": "CHECK", "Description": "Bank Check/Money Order (C.O.D.)"},
        {"Code": "CREDITCARD", "Description": "Adams Lowe Trident Automotive", "CreditCardLast4": "2517"},
    ],
    "ShippingMethods": [
        {"Code": "PFXG", "Description": "FedEx Ground"},
        {"Code": "pupg", "Description": "UPS Ground"},
        {"Code": "tkpp", "Description": "Truck Freight"},
        {"Code": "P-UP", "Description": "Pickup at Motor State"},
    ],
}

# POST /api/OrderInquiry. Note PPSP carries NO Rate key -- Motor State cannot price it here.
INQUIRY = {
    "CustomerNumber": 853809,
    "PurchaseOrderNumber": "AMS-QUOTE-ABC123",
    "InvoiceAmount": 38.5099879,
    "ShippingRate": 16.42,
    "SalesTaxAmount": 0.0,
    "Lines": [{
        "LineNumber": 1, "PartNumber": "MAH4065MS-112-1", "QuantityShip": 1,
        "QuantityBackOrder": 0, "QuantityAvailable": 4,
        "Price": 22.0899879, "LineTotal": 22.0899879,
    }],
    "AvailableShippingMethodRates": [
        {"ShippingMethodCode": "PFXG", "Description": "FedEx Ground", "Rate": 16.42},
        {"ShippingMethodCode": "PFO2", "Description": "FedEx OneRate 2 Day", "Rate": 9.99},
        {"ShippingMethodCode": "PPSP", "Description": "USPS - Priority Mail"},
    ],
    "HeaderErrors": [],
    "Warnings": [],
}


class ClientContractTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.client = oc.MotorStateOrderClient(CREDS)

    def test_a_missing_api_key_raises_valueerror_so_registries_report_no_adapter(self):
        for creds in ({}, {"api_key": ""}, {"api_key": "   "}):
            with self.subTest(creds=creds):
                with self.assertRaises(ValueError):
                    oc.MotorStateOrderClient(creds)

    def test_a_priced_request_without_a_payment_method_is_refused_locally(self):
        """
        Motor State's own failure is unhelpful: an inquiry returns HasErrors with an EMPTY rate
        list, and ShippingEstimate returns a bare HTTP 500. Neither names the missing field.
        """
        with self.assertRaises(ValueError) as ctx:
            self.client._purchase_order(
                [{"part_number": "X", "quantity": 1}],
                payment_method_code="",
                purchase_order_number="AMS-QUOTE-1",
            )
        self.assertIn("PaymentMethodCode", str(ctx.exception))

    def test_a_priced_request_without_a_po_number_is_refused_locally(self):
        """
        The surprise: OrderInquiry and ShippingEstimate demand a PurchaseOrderNumber even
        though neither creates an order. Measured -- "Purchase Order Number has not been
        entered", again with no rates and nothing naming the field.
        """
        for po in ("", "   ", None):
            with self.subTest(po=po):
                with self.assertRaises(ValueError) as ctx:
                    self.client._purchase_order(
                        [{"part_number": "X", "quantity": 1}],
                        payment_method_code="CREDITCARD",
                        purchase_order_number=po,
                    )
                self.assertIn("PurchaseOrderNumber", str(ctx.exception))

    def test_both_mandatory_fields_are_always_in_the_body(self):
        body = self.client._purchase_order(
            [{"part_number": "A", "quantity": 2}, {"part_number": "B", "quantity": 1}],
            payment_method_code="CREDITCARD", purchase_order_number="PO-1",
        )
        self.assertEqual(body["PaymentMethodCode"], "CREDITCARD")
        self.assertEqual(body["PurchaseOrderNumber"], "PO-1")
        self.assertEqual([l["LineNumber"] for l in body["Lines"]], [1, 2])
        self.assertEqual([l["PartNumber"] for l in body["Lines"]], ["A", "B"])

    def test_optional_fields_are_omitted_rather_than_sent_empty(self):
        body = self.client._purchase_order(
            [{"part_number": "A", "quantity": 1}],
            payment_method_code="CREDITCARD", purchase_order_number="PO-1",
        )
        for absent in ("ShippingMethodCode", "DropShipAddress", "CreditCardLast4",
                       "SpecialInstructions", "SignatureRequired", "FreightOptions"):
            self.assertNotIn(absent, body)

    def test_a_quote_reference_is_unique_and_obviously_not_a_real_po(self):
        a, b = oc.quote_reference(), oc.quote_reference()
        self.assertNotEqual(a, b)
        self.assertTrue(a.startswith("AMS-QUOTE-"))
        self.assertTrue(oc.quote_reference("AMS-STOCK").startswith("AMS-STOCK-"))


class PaymentMethodTests(SimpleTestCase):
    databases = []

    def _client(self, payment_methods):
        client = oc.MotorStateOrderClient(CREDS)
        client._order_options = {"PaymentMethods": payment_methods, "ShippingMethods": []}
        return client

    def test_a_card_is_preferred_over_cod(self):
        """
        COD is last on purpose: Motor State refuses it for drop shipments (code 17) and FedEx
        retired every COD service in July 2023 (code 20), so defaulting to it fails most carts.
        """
        code, last4 = self._client(ORDER_OPTIONS["PaymentMethods"]).default_payment_method()
        self.assertEqual(code, "CREDITCARD")
        self.assertEqual(last4, "2517")

    def test_terms_accounts_win_over_a_card(self):
        code, _ = self._client([
            {"Code": "CREDITCARD", "Description": "card", "CreditCardLast4": "1111"},
            {"Code": "ON ACCOUNT", "Description": "terms"},
        ]).default_payment_method()
        self.assertEqual(code, "ON ACCOUNT")

    def test_cod_is_used_only_when_it_is_the_only_option(self):
        code, _ = self._client([{"Code": "CHECK", "Description": "COD"}]).default_payment_method()
        self.assertEqual(code, "CHECK")

    def test_no_payment_methods_at_all_returns_none(self):
        self.assertEqual(self._client([]).default_payment_method(), (None, None))


class _ProviderPart:
    def __init__(self, external_id, pk=1):
        self.provider_external_id = external_id
        self.id = pk


class _CompanyProvider:
    id = 132
    company_id = 16


def _line(line_item_id, part_number, quantity):
    return order_base.OrderLineItemRequest(
        line_item_id=line_item_id,
        provider_part=_ProviderPart(part_number),
        quantity=quantity,
    )


SHIP_TO = order_base.ShipToAddress(
    name="Shop", address1="1 Main", city="Tampa", state="FL",
    postal_code="33601", country="US", is_shop_address=True,
)
DROP_SHIP_TO = order_base.ShipToAddress(
    name="Jane", address1="2 Oak", city="Tampa", state="FL",
    postal_code="33601", country="US", is_shop_address=False, phone="555", email="j@x.com",
)


class _AdapterHarness(SimpleTestCase):
    databases = []

    def adapter(self, client, order_credentials=None):
        a = ms_orders.MotorStateOrderAdapter.__new__(ms_orders.MotorStateOrderAdapter)
        a.company_provider = _CompanyProvider()
        a.order_account = None
        a._order_credentials = order_credentials or {}
        a._client = client
        a._payment = None
        return a


class _Client:
    def __init__(self, inquiry=None, order=None, options=None, orders=None, raises=None):
        self._inquiry = inquiry or INQUIRY
        self._order = order
        self._options = options or ORDER_OPTIONS
        self._orders = orders or []
        self._raises = raises
        self.last_kwargs = None

    def default_payment_method(self):
        methods = self._options.get("PaymentMethods") or []
        for m in methods:
            if m.get("Code") == "CREDITCARD":
                return "CREDITCARD", m.get("CreditCardLast4")
        return (methods[0].get("Code"), None) if methods else (None, None)

    def shipping_methods(self):
        return self._options.get("ShippingMethods") or []

    def order_inquiry(self, lines, **kwargs):
        if self._raises:
            raise self._raises
        self.last_kwargs = kwargs
        return self._inquiry

    def place_order(self, lines, **kwargs):
        if self._raises:
            raise self._raises
        self.last_kwargs = kwargs
        return self._order

    def get_orders(self, po_numbers=None, **kwargs):
        return self._orders


class QuoteTests(_AdapterHarness):
    def setUp(self):
        self.client = _Client()
        self.result = self.adapter(self.client).get_shipping_quote(
            [_line(11, "MAH4065MS-112-1", 1)], SHIP_TO)

    def test_a_quote_sends_a_throwaway_po_number(self):
        """Required by Motor State even though an inquiry creates nothing."""
        self.assertTrue(self.client.last_kwargs["purchase_order_number"].startswith("AMS-QUOTE-"))

    def test_line_pricing_and_availability(self):
        line = self.result.lines[0]
        self.assertEqual(line.line_item_id, 11)
        self.assertEqual(line.quantity_available, 1)
        self.assertEqual(line.quantity_backordered, 0)
        self.assertEqual(line.distributor_unit_price, Decimal("22.0899879"))

    def test_freight_comes_back_as_real_per_method_rates(self):
        """The first distributor here that can price shipping before an order exists."""
        codes = {o.service_level_code: o.cost for o in self.result.lines[0].ship_options}
        self.assertEqual(codes["PFXG"], Decimal("16.42"))
        self.assertEqual(codes["PFO2"], Decimal("9.99"))

    def test_a_method_with_no_rate_is_dropped_not_offered_free(self):
        """
        PPSP comes back with no Rate key when Motor State cannot price it for this cart.
        Reading that as 0.00 would offer free shipping on a method that cannot carry the order.
        """
        self.assertNotIn("PPSP", {o.service_level_code for o in self.result.lines[0].ship_options})
        self.assertEqual(len(self.result.lines[0].ship_options), 2)

    def test_the_total_is_motor_states_own_all_in_figure(self):
        self.assertEqual(self.result.distributor_total, Decimal("38.5099879"))

    def test_zero_valued_fees_are_not_listed(self):
        self.assertEqual([f.fee_type for f in self.result.fees], [])

    def test_a_business_rejection_surfaces_motor_states_own_words(self):
        rejected = dict(INQUIRY, HasErrors=True, HeaderErrors=[
            {"Message": "COD payment is not available for drop shipment orders.", "Code": 17},
            {"Message": "FedEx has retired All COD services as of July 31st, 2023.", "Code": 20},
        ])
        with self.assertRaises(order_exceptions.OrderValidationError) as ctx:
            self.adapter(_Client(inquiry=rejected)).get_shipping_quote(
                [_line(11, "MAH4065MS-112-1", 1)], SHIP_TO)
        self.assertIn("COD payment is not available", str(ctx.exception))

    def test_a_rejected_api_key_says_it_is_not_the_feed_login(self):
        err = ms_exceptions.MotorStatePermissionError(message="nope", code=403)
        with self.assertRaises(order_exceptions.OrderValidationError) as ctx:
            self.adapter(_Client(raises=err)).get_shipping_quote(
                [_line(11, "X", 1)], SHIP_TO)
        self.assertIn("separately from the FTP feed login", str(ctx.exception))

    def test_a_transport_failure_is_not_a_validation_error(self):
        err = ms_exceptions.MotorStateAPIConnectionError("timed out")
        with self.assertRaises(order_exceptions.OrderAdapterError) as ctx:
            self.adapter(_Client(raises=err)).get_shipping_quote([_line(11, "X", 1)], SHIP_TO)
        self.assertNotIsInstance(ctx.exception, order_exceptions.OrderValidationError)


class DropShipTests(_AdapterHarness):
    def test_shipping_to_the_shop_sends_no_drop_ship_address(self):
        """
        A DropShipAddress turns a stock order into a drop shipment -- priced differently, and
        refused outright for COD.
        """
        self.assertIsNone(ms_orders.MotorStateOrderAdapter._drop_ship_address(SHIP_TO))

    def test_an_end_customer_address_is_sent(self):
        d = ms_orders.MotorStateOrderAdapter._drop_ship_address(DROP_SHIP_TO)
        self.assertEqual(d["City"], "Tampa")
        self.assertEqual(d["Country"], "US")
        self.assertEqual(d["Phone"], "555")


class PinnedPaymentMethodTests(_AdapterHarness):
    def test_an_account_can_pin_a_payment_method(self):
        a = self.adapter(_Client(), order_credentials={
            "api_key": "k", "payment_method_code": "ON ACCOUNT"})
        self.assertEqual(a._payment_method(), ("ON ACCOUNT", None))

    def test_otherwise_the_accounts_own_options_decide(self):
        self.assertEqual(self.adapter(_Client())._payment_method(), ("CREDITCARD", "2517"))

    def test_no_payment_method_anywhere_is_a_clear_error(self):
        client = _Client(options={"PaymentMethods": [], "ShippingMethods": []})
        with self.assertRaises(order_exceptions.OrderValidationError) as ctx:
            self.adapter(client)._payment_method()
        self.assertIn("no payment methods", str(ctx.exception))


ORDER_RESULT = {
    "OrderNumber": "4455667", "PurchaseOrderNumber": "PO-77", "Held": False,
    "InvoiceAmount": 38.51, "ShippingRate": 16.42, "ShippingMethod": "FedEx Ground",
    "TrackingNumbers": "1Z999AA1,1Z999AA2", "StatusDescription": "Shipped",
    "ShipDate": "2026-10-29T00:00:00", "InvoiceDate": "2026-10-30T00:00:00",
    "Lines": [{"PartNumber": "MAH4065MS-112-1", "QuantityShip": 1, "QuantityBackOrder": 0,
               "Price": 22.09, "LineTotal": 22.09}],
    "HeaderErrors": [], "Warnings": [],
}


class _PO:
    id = 1
    po_number = "PO-77"
    po_name = None
    ship_method = "PFXG"


class SubmitTests(_AdapterHarness):
    def test_a_placed_order_records_the_order_number_and_quantities(self):
        result = self.adapter(_Client(order=ORDER_RESULT)).submit_order(
            _PO(), [_line(11, "MAH4065MS-112-1", 1)], SHIP_TO)
        self.assertEqual(result.distributor_order_numbers, ["4455667"])
        self.assertEqual(result.line_item_placements[0].quantity_confirmed, 1)
        self.assertTrue(result.distributor_confirmed)

    def test_a_held_order_is_not_reported_as_confirmed(self):
        """Held means Motor State parked it for review -- accepted, but not yet an order."""
        result = self.adapter(_Client(order=dict(ORDER_RESULT, Held=True))).submit_order(
            _PO(), [_line(11, "MAH4065MS-112-1", 1)], SHIP_TO)
        self.assertFalse(result.distributor_confirmed)

    def test_no_order_number_is_an_error_not_a_silent_success(self):
        with self.assertRaises(order_exceptions.OrderValidationError):
            self.adapter(_Client(order=dict(ORDER_RESULT, OrderNumber=""))).submit_order(
                _PO(), [_line(11, "MAH4065MS-112-1", 1)], SHIP_TO)

    def test_a_rejection_on_submit_surfaces_the_reason(self):
        bad = dict(ORDER_RESULT, HasErrors=True, OrderNumber="",
                   HeaderErrors=[{"Message": "Credit hold", "Code": 9}])
        with self.assertRaises(order_exceptions.OrderValidationError) as ctx:
            self.adapter(_Client(order=bad)).submit_order(
                _PO(), [_line(11, "MAH4065MS-112-1", 1)], SHIP_TO)
        self.assertIn("Credit hold", str(ctx.exception))


class StatusAndInvoiceTests(_AdapterHarness):
    def _adapter(self):
        return self.adapter(_Client(orders=[{"Found": True, "Order": ORDER_RESULT}]))

    def test_tracking_numbers_are_split_from_one_comma_joined_string(self):
        """Motor State returns TrackingNumbers as a single string, not a list."""
        status = self._adapter().get_order_status(_PO()).orders[0]
        self.assertEqual(status.tracking_numbers, ["1Z999AA1", "1Z999AA2"])
        self.assertEqual(status.ship_date, datetime.date(2026, 10, 29))
        self.assertEqual(status.status_code, "Shipped")

    def test_an_order_not_found_yields_no_status_rather_than_an_error(self):
        a = self.adapter(_Client(orders=[{"OrderNumber": "x", "Found": False, "Order": None}]))
        self.assertEqual(a.get_order_status(_PO()).orders, [])

    def test_invoices_are_synthesised_from_the_order(self):
        """Motor State exposes no invoice endpoint -- only InvoiceDate/InvoiceAmount."""
        inv = self._adapter().get_invoices(_PO())[0]
        self.assertEqual(inv.invoice_number, "4455667")
        self.assertEqual(inv.invoice_date, datetime.date(2026, 10, 30))
        self.assertEqual(inv.total_price, Decimal("38.51"))
        self.assertEqual(inv.freight, Decimal("16.42"))

    def test_an_uninvoiced_order_produces_no_invoice(self):
        a = self.adapter(_Client(orders=[{"Found": True, "Order": dict(
            ORDER_RESULT, InvoiceDate=None, InvoiceAmount=None)}]))
        self.assertEqual(a.get_invoices(_PO()), [])


class CapabilityTests(_AdapterHarness):
    def test_capabilities_match_the_api(self):
        a = ms_orders.MotorStateOrderAdapter
        # No cancel operation exists in their API at all.
        self.assertFalse(a.supports_cancel(a))
        self.assertTrue(a.supports_invoices(a))
        # OrderOptions publishes real codes, so the picker is genuine.
        self.assertTrue(a.supports_shipping_method_selection(a))

    def test_shipping_methods_come_from_the_account_not_a_hardcoded_list(self):
        methods = self.adapter(_Client()).list_shipping_methods()
        self.assertEqual([m.code for m in methods], ["PFXG", "pupg", "tkpp", "P-UP"])
        self.assertEqual(methods[0].name, "FedEx Ground")

    def test_cancel_refuses_clearly(self):
        with self.assertRaises(order_exceptions.OrderNotSupportedError):
            self.adapter(_Client()).cancel_order(_PO())
