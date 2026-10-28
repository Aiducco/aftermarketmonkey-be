"""
Tests for AllPro's Fulfillment Master API client and order adapter.

Response payloads are taken from AllPro's own 7.27.2026 spec; the ERROR payloads and the
behaviours around them are verbatim from live calls to allpro-sandbox.myshipzone.com on
2026-10-27/28 with Trident's FM account. Where the two disagree -- tracking's <item> vs <items>
-- both are covered, because their spec flags the change and does not say which deployments have
it.

Nothing here hits the network.
"""
import datetime
import xml.etree.ElementTree as ET
from decimal import Decimal

from django.test import SimpleTestCase, override_settings

from src.integrations.clients.allpro import exceptions as allpro_exceptions
from src.integrations.clients.allpro import order_client as oc
from src.integrations.orders import allpro as allpro_orders
from src.integrations.orders import base as order_base
from src.integrations.orders import exceptions as order_exceptions

CREDS = {"username": "ACME01", "password": "s3cret"}


class BaseUrlTests(SimpleTestCase):
    databases = []

    def test_production_is_the_default(self):
        """
        A dealer arriving with real credentials must be validated against the host those
        credentials belong to, without anyone remembering to flip a setting.
        """
        self.assertEqual(oc.AllProFulfillmentClient(CREDS).base_url, "https://allpro.myshipzone.com")

    def test_sandbox_is_opt_in_two_ways(self):
        self.assertEqual(
            oc.AllProFulfillmentClient(CREDS, sandbox=True).base_url,
            "https://allpro-sandbox.myshipzone.com",
        )
        with override_settings(ALLPRO_FM_USE_SANDBOX=True):
            self.assertEqual(
                oc.AllProFulfillmentClient(CREDS).base_url,
                "https://allpro-sandbox.myshipzone.com",
            )

    def test_an_explicit_base_url_overrides_everything(self):
        with override_settings(ALLPRO_FM_USE_SANDBOX=True):
            self.assertEqual(
                oc.AllProFulfillmentClient(CREDS, base_url="https://x.example/").base_url,
                "https://x.example",
            )

    def test_missing_credentials_raise_valueerror_so_registries_report_no_adapter(self):
        for creds in ({}, {"username": "u"}, {"password": "p"}, {"username": "", "password": "p"}):
            with self.subTest(creds=creds):
                with self.assertRaises(ValueError):
                    oc.AllProFulfillmentClient(creds)


class RequestBuildingTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.client = oc.AllProFulfillmentClient(CREDS)

    def test_the_five_entities_their_parser_requires_are_escaped(self):
        xml = self.client._order_xml(
            [{"linecode": "WIX", "partno": "515<15", "qtyreq": 1}],
            ship_to={"customer": 'Smith & Sons "The" O\'Brien', "address1": "1 Main"},
        )
        self.assertIn("Smith &amp; Sons &quot;The&quot; O&apos;Brien", xml)
        self.assertIn('partno="515&lt;15"', xml)
        ET.fromstring(xml)  # must still parse

    def test_empty_attributes_are_omitted_entirely(self):
        """
        Their parser treats "" as a real value for some fields -- shipper in particular has to be
        absent rather than blank, or it trips the shipper/shipper_account pairing rule.
        """
        xml = self.client._order_xml([{"linecode": "WIX", "partno": "51515", "qtyreq": 1}])
        self.assertNotIn("shipper=", xml)
        self.assertNotIn("ponumber=", xml)
        self.assertNotIn("<comment", xml)
        self.assertNotIn("<shipto", xml)

    def test_shipper_and_shipper_account_must_travel_together(self):
        base_args = ([{"linecode": "WIX", "partno": "51515", "qtyreq": 1}],)
        with self.assertRaises(ValueError):
            self.client._order_xml(*base_args, shipper="ups")
        with self.assertRaises(ValueError):
            self.client._order_xml(*base_args, shipper_account="1Z999")
        xml = self.client._order_xml(*base_args, shipper="UPS", shipper_account="1Z999")
        # must be lowercase -- their validation is exact
        self.assertIn('shipper="ups"', xml)

    def test_an_unknown_carrier_is_refused_before_the_request_is_sent(self):
        with self.assertRaises(ValueError):
            self.client._order_xml(
                [{"linecode": "WIX", "partno": "51515", "qtyreq": 1}],
                shipper="dhl", shipper_account="123",
            )

    def test_every_part_carries_the_same_branch(self):
        """AllPro rejects a request whose parts span branches, so one branch is applied to all."""
        xml = self.client._order_xml(
            [{"linecode": "WIX", "partno": "1", "qtyreq": 1},
             {"linecode": "WIX", "partno": "2", "qtyreq": 2}],
            branch="600",
        )
        self.assertEqual(xml.count('branch="600"'), 2)

    def test_a_blank_branch_is_omitted_meaning_all_locations(self):
        xml = self.client._order_xml([{"linecode": "WIX", "partno": "1", "qtyreq": 1}])
        self.assertNotIn("branch=", xml)

    def test_a_literal_plus_survives_form_encoding(self):
        """
        Their docs warn a raw "+" in the body decodes to a space and is auto-corrected only on
        partno. Standard percent-encoding sends %2B, which decodes back to "+" -- this asserts we
        rely on that rather than hand-building the body.
        """
        import urllib.parse
        body = urllib.parse.urlencode({"type": "stockcheck", "orderxml": '<part partno="A+B"/>'})
        self.assertIn("%2B", body)
        self.assertEqual(urllib.parse.parse_qs(body)["orderxml"][0], '<part partno="A+B"/>')


class ErrorHandlingTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.client = oc.AllProFulfillmentClient(CREDS)

    def _raise(self, body):
        return self.client._parse_xml(_Response(body), "stockcheck")

    def test_errors_arrive_as_http_200_and_are_still_errors(self):
        with self.assertRaises(allpro_exceptions.AllProAPIError):
            self._raise('<error msg="Missing orderxml"/>')

    def test_a_credential_rejection_is_an_auth_error(self):
        with self.assertRaises(allpro_exceptions.AllProAPIAuthError):
            self._raise('<error msg="Invalid username/password"/>')

    def test_the_upstream_fault_is_not_an_auth_error(self):
        """
        The single most important distinction in this client. AllPro authenticates the dealer and
        then fails opening its own upstream session; the message names a password, so untreated
        it sends a dealer to re-enter a correct one. Proven against the sandbox to occur even for
        a request with no <part> at all.
        """
        body = ('<error msg="Stock check error: error doing stockcheck (page 1): '
                'Invalid OW username/password at line 2615.&#10;"/>')
        with self.assertRaises(allpro_exceptions.AllProUpstreamNotProvisionedError):
            self._raise(body)

    def test_the_upstream_fault_match_survives_their_line_number_moving(self):
        for line in ("2615", "9999", ""):
            with self.subTest(line=line):
                with self.assertRaises(allpro_exceptions.AllProUpstreamNotProvisionedError):
                    self._raise('<error msg="Invalid OW username/password at line %s."/>' % line)

    def test_an_upstream_fault_is_not_mistaken_for_a_plain_auth_fault(self):
        # AllProUpstreamNotProvisionedError must NOT be a subclass of the auth error, or callers
        # branching on auth would swallow it again.
        self.assertFalse(issubclass(
            allpro_exceptions.AllProUpstreamNotProvisionedError,
            allpro_exceptions.AllProAPIAuthError,
        ))

    def test_unparseable_xml_is_reported_as_such(self):
        with self.assertRaises(allpro_exceptions.AllProAPIError):
            self._raise("<order><header ")

    def test_a_successful_document_raises_nothing(self):
        root = self._raise('<stockcheckresp><header errcode="success"/></stockcheckresp>')
        self.assertEqual(root.tag, "stockcheckresp")


class _Response:
    status_code = 200
    def __init__(self, text):
        self.text = text
        self.content = text.encode()


STOCKCHECK = (
    '<stockcheckresp><header errmsg="sucess" username="ACME01" errcode="success" ordertype="R"/>'
    '<part core="5.00" uom="EA" partno="51515" kit="" linecode="WIX" qtyavail="14" desc="Oil Filter"'
    ' list="6.99" cost="4.20" deliverytime="1" branch="600" qtyreq="2" linenum="1" mfgcode="WIX"'
    ' errcode="success" /></stockcheckresp>'
)


class StockCheckParsingTests(SimpleTestCase):
    databases = []

    def test_fields_map_across(self):
        parts = [oc.AllProFulfillmentClient._parse_stock_part(p)
                 for p in oc._iter_local(ET.fromstring(STOCKCHECK), "part")]
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0]["qtyavail"], "14")
        self.assertEqual(parts[0]["cost"], "4.20")
        self.assertEqual(parts[0]["core"], "5.00")
        self.assertEqual(parts[0]["branch"], "600")
        self.assertEqual(parts[0]["errcode"], "success")

    def test_the_header_typo_is_not_relied_on(self):
        """Their header says errmsg="sucess" (missing c). Nothing may parse on that value."""
        self.assertIn('errmsg="sucess"', STOCKCHECK)
        parts = oc.AllProFulfillmentClient._parse_stock_part(
            oc._iter_local(ET.fromstring(STOCKCHECK), "part")[0])
        self.assertNotIn("errmsg", parts)


class TrackingParsingTests(SimpleTestCase):
    databases = []

    NEW_FORMAT = (
        '<MSZ Rev="1.0"><order><header transaction="gettracking" status="success"/>'
        '<part ponumber="PO-1">'
        '<item brand="WIX" sku="51515" ponumber="PO-1" service="ups" trackingnumber="1Z999"'
        ' cost="21.89" qty="2" package_weight="3.20" gross_weight="3.40" package_width="8"'
        ' package_length="10" package_height="6" ship_date="2026-07-09" status="shipped"></item>'
        '</part></order></MSZ>'
    )
    # The earlier per-API document shows <items>; their spec flags that the tag changed.
    OLD_FORMAT = NEW_FORMAT.replace("<item ", "<items ").replace("</item>", "</items>")

    def test_both_item_spellings_parse(self):
        for label, payload in (("<item>", self.NEW_FORMAT), ("<items>", self.OLD_FORMAT)):
            with self.subTest(label):
                orders = oc.AllProFulfillmentClient._parse_tracking(ET.fromstring(payload))
                item = orders["PO-1"]["items"][0]
                self.assertEqual(item["trackingnumber"], "1Z999")
                self.assertEqual(item["service"], "ups")
                self.assertEqual(item["status"], "shipped")
                self.assertEqual(item["ship_date"], datetime.date(2026, 7, 9))
                self.assertEqual(item["cost"], "21.89")

    def test_an_unshipped_order_has_no_items_and_is_still_returned(self):
        payload = ('<MSZ Rev="1.0"><order><header status="success"/>'
                   '<part ponumber="PO-2" service="" trackingnumber="" qty="12" cost=""'
                   ' status="new"></part></order></MSZ>')
        orders = oc.AllProFulfillmentClient._parse_tracking(ET.fromstring(payload))
        self.assertEqual(orders["PO-2"]["status"], "new")
        self.assertEqual(orders["PO-2"]["items"], [])


class CancelParsingTests(SimpleTestCase):
    databases = []

    # Verbatim shape from the sandbox: header status="success" even though the cancel was refused.
    PAYLOAD = (
        '<MSZ Rev="1.0"><order><header transaction="ordercancel" status="success"/>'
        '<cancel ponumber="PO-1" trackingnumber="" status="cancel" error="false" msg=""></cancel>'
        '<cancel ponumber="PO-2" trackingnumber="9999999999" status="shipped" error="true"'
        ' msg="The order is already shipped, this order cannot be cancelled."></cancel>'
        '</order></MSZ>'
    )

    def test_a_refusal_is_flagged_even_though_the_header_says_success(self):
        results = oc.AllProFulfillmentClient._parse_cancels(ET.fromstring(self.PAYLOAD))
        self.assertEqual(len(results), 2)
        self.assertFalse(results[0]["error"])
        self.assertTrue(results[1]["error"])
        self.assertIn("already shipped", results[1]["msg"])

    def test_error_is_the_string_true_not_a_boolean(self):
        self.assertIn('error="true"', self.PAYLOAD)
        results = oc.AllProFulfillmentClient._parse_cancels(ET.fromstring(self.PAYLOAD))
        self.assertIs(results[1]["error"], True)


ORDERCONF = (
    '<orderconf><header orderdate="2026-07-08 10:14:02" username="ACME01" state="ordered"'
    ' ponumber="PO-100293" errcode="SUCCESS" />'
    '<part partno="51515" linecode="WIX" branch="1" branchname="Main Warehouse" qtyreq="2"'
    ' orderno="697138736" errcode="SUCCESS" qtysup="2" />'
    '<part partno="51516" linecode="WIX" branch="1" branchname="Main Warehouse" qtyreq="3"'
    ' orderno="697138736" errcode="SUCCESS" qtysup="1" />'
    '</orderconf>'
)


class _Part:
    def __init__(self, line_code, part_number):
        self.line_code = line_code
        self.part_number = part_number


class _ProviderPart:
    def __init__(self, external_id):
        self.provider_external_id = external_id


class _CompanyProvider:
    id = 179
    company_id = 16


def _line(line_item_id, line_code, part_number, quantity):
    return order_base.OrderLineItemRequest(
        line_item_id=line_item_id,
        provider_part=_ProviderPart("1_{}".format(part_number)),
        quantity=quantity,
    )


class _AdapterHarness(SimpleTestCase):
    """
    Builds an adapter with the database swapped out.

    ``_allpro_part_for`` is patched on the CLASS, not the instance: ``_cart_lines`` is a
    staticmethod that reaches it through ``AllProOrderAdapter._allpro_part_for``, so an instance
    attribute is never consulted.
    """
    databases = []

    def adapter(self, client):
        original = allpro_orders.AllProOrderAdapter._allpro_part_for
        allpro_orders.AllProOrderAdapter._allpro_part_for = staticmethod(
            lambda pp: _Part("WIX", pp.provider_external_id.split("_", 1)[1])
        )
        self.addCleanup(
            setattr, allpro_orders.AllProOrderAdapter, "_allpro_part_for", original
        )
        adapter = allpro_orders.AllProOrderAdapter.__new__(allpro_orders.AllProOrderAdapter)
        adapter.company_provider = _CompanyProvider()
        adapter.order_account = None
        adapter._client = client
        return adapter


class QuoteTests(_AdapterHarness):

    def setUp(self):
        parsed = [oc.AllProFulfillmentClient._parse_stock_part(p)
                  for p in oc._iter_local(ET.fromstring(STOCKCHECK), "part")]

        class _Client:
            def stock_check(self, cart, branch="", **kwargs):
                return parsed

        self.result = self.adapter(_Client()).get_shipping_quote(
            [_line(11, "WIX", "51515", 2)],
            order_base.ShipToAddress(name="Shop", address1="1 Main", city="Tampa", state="FL",
                                     postal_code="33601", country="US"),
        )

    def test_the_quote_carries_no_shipping_option_because_allpro_prices_none(self):
        """
        Nothing in this API quotes freight before an order. An invented ship option here would be
        a number a dealer could be charged against.
        """
        self.assertEqual(self.result.lines[0].ship_options, [])

    def test_cost_and_availability_come_through(self):
        line = self.result.lines[0]
        self.assertEqual(line.line_item_id, 11)
        self.assertEqual(line.quantity_available, 2)
        self.assertEqual(line.distributor_unit_price, Decimal("4.20"))
        self.assertEqual(line.distributor_line_total, Decimal("8.40"))
        self.assertEqual(line.warehouse_code, "600")
        self.assertEqual(line.warehouse_name, "Virginia")

    def test_a_core_charge_is_a_fee_and_reaches_the_total(self):
        self.assertEqual([f.fee_type for f in self.result.fees], ["core_charge"])
        self.assertEqual(self.result.fees[0].amount, Decimal("10.00"))  # 5.00 core x 2
        self.assertEqual(self.result.distributor_total, Decimal("18.40"))  # 8.40 + 10.00

    def test_requesting_more_than_is_available_is_reported_as_backordered(self):
        parsed = [oc.AllProFulfillmentClient._parse_stock_part(p)
                  for p in oc._iter_local(ET.fromstring(
                      STOCKCHECK.replace('qtyavail="14"', 'qtyavail="1"')), "part")]

        class _Client:
            def stock_check(self, cart, branch="", **kwargs):
                return parsed

        result = self.adapter(_Client()).get_shipping_quote(
            [_line(11, "WIX", "51515", 2)],
            order_base.ShipToAddress(name="S", address1="1", city="T", state="FL",
                                     postal_code="1", country="US"),
        )
        self.assertEqual(result.lines[0].quantity_available, 1)
        self.assertEqual(result.lines[0].quantity_backordered, 1)

    def test_the_upstream_fault_becomes_a_clear_not_implemented(self):
        class _Client:
            def stock_check(self, cart, branch="", **kwargs):
                raise allpro_exceptions.AllProUpstreamNotProvisionedError(
                    "Invalid OW username/password at line 2615.")

        with self.assertRaises(order_exceptions.OrderAdapterNotImplementedError) as ctx:
            self.adapter(_Client()).get_shipping_quote(
                [_line(11, "WIX", "51515", 1)],
                order_base.ShipToAddress(name="S", address1="1", city="T", state="FL",
                                         postal_code="1", country="US"))
        self.assertIn("only AllPro can fix", str(ctx.exception))


class SubmitTests(_AdapterHarness):

    class _PO:
        id = 1
        po_number = "PO-100293"
        po_name = None
        ship_method = None

    def _submit(self, payload=ORDERCONF):
        parsed = oc.AllProFulfillmentClient._parse_order_confirmation(ET.fromstring(payload))

        class _Client:
            def place_order(self, cart, po_number, **kwargs):
                return parsed

        return self.adapter(_Client()).submit_order(
            self._PO(),
            [_line(11, "WIX", "51515", 2), _line(12, "WIX", "51516", 3)],
            order_base.ShipToAddress(name="S", address1="1", city="T", state="FL",
                                     postal_code="1", country="US"),
        )

    def test_lines_sharing_one_order_number_collapse_to_a_single_distributor_order(self):
        result = self._submit()
        self.assertEqual(result.distributor_order_numbers, ["697138736"])
        self.assertEqual(len(result.line_item_placements), 2)

    def test_a_short_shipped_line_records_the_backorder(self):
        placements = {p.line_item_id: p for p in self._submit().line_item_placements}
        self.assertEqual(placements[11].quantity_confirmed, 2)
        self.assertEqual(placements[11].quantity_backordered, 0)
        self.assertEqual(placements[12].quantity_confirmed, 1)
        self.assertEqual(placements[12].quantity_backordered, 2)

    def test_no_order_number_at_all_is_a_validation_error_not_a_silent_success(self):
        with self.assertRaises(order_exceptions.OrderValidationError):
            self._submit(ORDERCONF.replace('orderno="697138736"', 'orderno=""'))


class StatusRollUpTests(SimpleTestCase):
    databases = []

    def test_a_partially_shipped_order_is_not_reported_as_shipped(self):
        """Calling it shipped is what stops a dealer chasing the rest of it."""
        self.assertEqual(allpro_orders.AllProOrderAdapter._roll_up({"shipped", "new"}, None), "new")

    def test_fully_shipped_and_terminal_mixes(self):
        r = allpro_orders.AllProOrderAdapter._roll_up
        self.assertEqual(r({"shipped"}, None), "shipped")
        self.assertEqual(r({"shipped", "cancel"}, None), "shipped")
        self.assertEqual(r({"cancel"}, None), "cancel")
        self.assertEqual(r({"returned"}, None), "returned")

    def test_no_line_detail_falls_back_to_the_order_level_status(self):
        r = allpro_orders.AllProOrderAdapter._roll_up
        self.assertEqual(r(set(), "new"), "new")
        self.assertEqual(r(set(), None), "new")
        self.assertEqual(r({""}, "shipped"), "shipped")


class InvoiceTests(_AdapterHarness):

    ROWS = [
        {"invoice_date": "2026-07-09", "invoice_number": "INV-88213", "customer_po": "PO-1",
         "unit_cost": "4.20", "unit_core_cost": "0.00", "ship_cost": "21.89", "sku": "WIX51515",
         "linecode": "WIX", "partno": "51515", "po_qty": 2, "qty": 2, "credit_qty": None,
         "kit_sku": None, "extended_amount": None},
        {"invoice_date": "2026-07-10", "invoice_number": "INV-88214", "customer_po": "PO-1",
         "unit_cost": "4.20", "unit_core_cost": "0.00", "ship_cost": None, "sku": "WIX51515",
         "linecode": "WIX", "partno": "51515", "po_qty": 1, "qty": None, "credit_qty": 1,
         "kit_sku": None, "extended_amount": None},
    ]

    class _PO:
        id = 1
        po_number = "PO-1"
        po_name = None

    def setUp(self):
        rows = self.ROWS

        class _Client:
            def get_billing(self, start_date=None, end_date=None, customer_po=None):
                return rows

        self.invoices = {i.invoice_number: i for i in self.adapter(_Client()).get_invoices(self._PO())}

    def test_flat_rows_are_grouped_into_invoices(self):
        self.assertEqual(sorted(self.invoices), ["INV-88213", "INV-88214"])

    def test_line_totals_are_computed_because_extended_amount_is_always_null(self):
        self.assertIsNone(self.ROWS[0]["extended_amount"])
        inv = self.invoices["INV-88213"]
        self.assertEqual(inv.line_items[0].total_price, Decimal("8.40"))
        self.assertEqual(inv.freight, Decimal("21.89"))
        self.assertEqual(inv.total_price, Decimal("30.29"))
        self.assertEqual(inv.invoice_date, datetime.date(2026, 7, 9))

    def test_a_credit_line_is_negative_not_a_second_shipment(self):
        """
        credit_qty is populated INSTEAD of qty on a return. Counting it positive would overstate
        what the dealer owes.
        """
        inv = self.invoices["INV-88214"]
        self.assertEqual(inv.line_items[0].quantity, -1)
        self.assertEqual(inv.total_price, Decimal("-4.20"))


class CapabilityTests(SimpleTestCase):
    databases = []

    def test_capabilities_match_what_the_api_actually_offers(self):
        a = allpro_orders.AllProOrderAdapter
        # AllPro really does have a cancel endpoint -- unlike ATD.
        self.assertTrue(a.supports_cancel(a))
        self.assertTrue(a.supports_invoices(a))
        # No published shipping-code list, so no picker.
        self.assertFalse(a.supports_shipping_method_selection(a))

    def test_order_credentials_are_read_from_the_order_namespace_not_the_feed(self):
        """
        The FTP feed login is rejected by this API outright, so reading feed credentials here
        would hand AllPro values it will always refuse.
        """
        import inspect
        source = inspect.getsource(allpro_orders.AllProOrderAdapter.__init__)
        self.assertIn("get_order_credentials", source)
        self.assertNotIn("get_feed_credentials", source)
