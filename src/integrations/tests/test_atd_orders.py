"""
Tests for ``src.integrations.clients.atd.order_client`` -- ATD's SOAP order service.

Everything here is XML in, dicts out. No network: the envelopes below are built to the live
WSDL at ws.atdconnect.com/ws/3_6/orders.wsdl, read on 2026-10-27, and the fault is the verbatim
one a non-entitled account receives.

The parsing deliberately ignores namespaces and matches on local names, because ATD's responses
are inconsistently prefixed; these tests use a prefixed envelope so a namespace-sensitive
implementation would fail them.
"""
import datetime
import xml.etree.ElementTree as ET

from django.test import SimpleTestCase

from src.integrations.clients.atd import exceptions as atd_exceptions
from src.integrations.clients.atd import order_client as oc
from src.integrations.orders import atd as atd_orders
from src.integrations.orders import base as order_base

CREDS = {"username": "u", "password": "p", "client_id": "cid", "location": "3036915"}


def _envelope(inner: str) -> ET.Element:
    return ET.fromstring(
        '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"'
        ' xmlns:ns2="http://api.atdconnect.com/atd/3_6/orders">'
        "<SOAP-ENV:Body>{}</SOAP-ENV:Body></SOAP-ENV:Envelope>".format(inner)
    )


PREVIEW = _envelope(
    "<ns2:previeworderresponse><ns2:previeworder>"
    "<ns2:customerponumber>AMS-1</ns2:customerponumber>"
    "<ns2:ordertotal>512.40</ns2:ordertotal>"
    "<ns2:orderline>"
    "  <ns2:cartlinenumber>1</ns2:cartlinenumber>"
    "  <ns2:atdproductnumber>203920</ns2:atdproductnumber>"
    "  <ns2:description>275/55R20XL 117H NIT NT420V</ns2:description>"
    "  <ns2:quantity>3</ns2:quantity>"
    "  <ns2:cost>162.70</ns2:cost>"
    "  <ns2:fet>0.00</ns2:fet>"
    "  <ns2:linetotal>488.10</ns2:linetotal>"
    "  <ns2:discontinued>false</ns2:discontinued>"
    "  <ns2:fulfillment>"
    "    <ns2:status>Available</ns2:status><ns2:quantity>2</ns2:quantity>"
    "    <ns2:freight>12.15</ns2:freight><ns2:shipmethod>GROUND</ns2:shipmethod>"
    "    <ns2:estimateddelivery>2026-10-30</ns2:estimateddelivery>"
    "    <ns2:sourcedc>DC12</ns2:sourcedc><ns2:sourcedcname>Orlando DC</ns2:sourcedcname>"
    "  </ns2:fulfillment>"
    "  <ns2:fulfillment>"
    "    <ns2:status>Backordered</ns2:status><ns2:quantity>1</ns2:quantity>"
    "    <ns2:freight>0.00</ns2:freight><ns2:shipmethod>GROUND</ns2:shipmethod>"
    "    <ns2:sourcedc>DC99</ns2:sourcedc>"
    "  </ns2:fulfillment>"
    "</ns2:orderline>"
    "</ns2:previeworder></ns2:previeworderresponse>"
)

DETAIL = _envelope(
    "<ns2:getorderdetailresponse><ns2:orderdetail>"
    "<ns2:confirmationnumber>CONF-77</ns2:confirmationnumber>"
    "<ns2:customerponumber>AMS-1</ns2:customerponumber>"
    "<ns2:subtotal>488.10</ns2:subtotal><ns2:freight>12.15</ns2:freight>"
    "<ns2:ordertotal>500.25</ns2:ordertotal>"
    "<ns2:orderline><ns2:cartlinenumber>1</ns2:cartlinenumber>"
    "  <ns2:product><ns2:atdproductnumber>203920</ns2:atdproductnumber>"
    "    <ns2:productname>NIT NT420V</ns2:productname><ns2:quantity>3</ns2:quantity>"
    "    <ns2:cost>162.70</ns2:cost></ns2:product>"
    "  <ns2:fulfillment><ns2:status>Shipped</ns2:status>"
    "    <ns2:quantityshipped>2</ns2:quantityshipped><ns2:linetotal>325.40</ns2:linetotal>"
    "    <ns2:shipmethod>GROUND</ns2:shipmethod><ns2:carriername>FedEx</ns2:carriername>"
    "    <ns2:shippeddate>2026-10-28T09:12:00</ns2:shippeddate>"
    "    <ns2:estimateddeliverydate>2026-10-30T00:00:00</ns2:estimateddeliverydate>"
    "    <ns2:trackingnumber>1Z999</ns2:trackingnumber>"
    "    <ns2:invoicenumber>INV-5001</ns2:invoicenumber></ns2:fulfillment>"
    "</ns2:orderline>"
    "</ns2:orderdetail></ns2:getorderdetailresponse>"
)


class PreviewParsingTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.parsed = oc.ATDOrderClient._parse_preview(PREVIEW)

    def test_order_level_fields(self):
        self.assertEqual(self.parsed["order_total"], "512.40")
        self.assertEqual(self.parsed["customer_po_number"], "AMS-1")
        self.assertEqual(len(self.parsed["lines"]), 1)

    def test_line_pricing(self):
        line = self.parsed["lines"][0]
        self.assertEqual(line["atd_product_number"], "203920")
        self.assertEqual(line["cost"], "162.70")
        self.assertEqual(line["line_total"], "488.10")

    def test_a_line_split_across_dcs_keeps_both_fulfillments(self):
        """ATD routes one line to several DCs; collapsing them would lose the backorder."""
        fulfillments = self.parsed["lines"][0]["fulfillments"]
        self.assertEqual(len(fulfillments), 2)
        self.assertEqual(fulfillments[0]["source_dc"], "DC12")
        self.assertEqual(fulfillments[0]["freight"], "12.15")
        self.assertEqual(fulfillments[1]["status"], "Backordered")


class OrderDetailParsingTests(SimpleTestCase):
    databases = []

    def test_tracking_and_invoice_come_off_the_fulfillment(self):
        parsed = oc.ATDOrderClient._parse_order_detail(DETAIL)
        self.assertEqual(parsed["confirmation_number"], "CONF-77")
        self.assertEqual(parsed["freight"], "12.15")
        f = parsed["lines"][0]["fulfillments"][0]
        self.assertEqual(f["tracking_number"], "1Z999")
        self.assertEqual(f["invoice_number"], "INV-5001")
        self.assertEqual(f["carrier_name"], "FedEx")
        self.assertEqual(parsed["lines"][0]["product_name"], "NIT NT420V")


class FaultHandlingTests(SimpleTestCase):
    databases = []

    NOT_ENTITLED = (
        '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">'
        "<SOAP-ENV:Body><SOAP-ENV:Fault><faultcode>SOAP-ENV:Server</faultcode>"
        '<faultstring xml:lang="en">ATDConnectException</faultstring>'
        "<detail><message>Unable to authenticate User's Credentials. Please contact Online "
        "Customer Service if you feel that this User Account should have access to ATDConnect"
        "</message></detail></SOAP-ENV:Fault></SOAP-ENV:Body></SOAP-ENV:Envelope>"
    )

    def _client_returning(self, status, body):
        client = oc.ATDOrderClient(CREDS, sandbox=False)

        class _R:
            status_code = status
            content = body.encode() if isinstance(body, str) else body
            text = body if isinstance(body, str) else ""

        import src.integrations.clients.atd.order_client as mod
        original = mod.requests.post
        mod.requests.post = lambda *a, **k: _R()
        self.addCleanup(setattr, mod.requests, "post", original)
        return client

    def test_the_entitlement_fault_is_its_own_error(self):
        """
        The exact fault SunCoast's working feed credentials get from the order service. It is
        NOT a wrong password -- treating it as one sends a dealer to re-enter a correct one.
        """
        client = self._client_returning(500, self.NOT_ENTITLED)
        with self.assertRaises(atd_exceptions.ATDOrderNotEntitledError):
            client.preview_order([{"atd_product_number": "203920", "quantity": 1}])

    def test_an_empty_404_is_reported_as_a_malformed_request(self):
        """
        ATD answers an unroutable SOAP body with a bare 404 and no content -- which looks like a
        dead endpoint but means the namespace or element name is wrong. Getting this message
        right is what turned a day of "the endpoint is down" into a one-line namespace fix.
        """
        client = self._client_returning(404, "")
        with self.assertRaises(atd_exceptions.ATDAPIException) as ctx:
            client.preview_order([{"atd_product_number": "203920", "quantity": 1}])
        self.assertIn("not routed", str(ctx.exception))


class CartBuildingTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.client = oc.ATDOrderClient(CREDS, sandbox=False)

    def test_lines_are_numbered_and_namespaced(self):
        xml = self.client._cart_xml([
            {"atd_product_number": "203920", "quantity": 2},
            {"atd_product_number": "200570", "quantity": 1},
        ], customer_po_number="AMS-9")
        self.assertIn("<ord:customerponumber>AMS-9</ord:customerponumber>", xml)
        self.assertIn("<ord:cartlinenumber>1</ord:cartlinenumber>", xml)
        self.assertIn("<ord:cartlinenumber>2</ord:cartlinenumber>", xml)
        self.assertIn("<ord:atdproductnumber>200570</ord:atdproductnumber>", xml)

    def test_empty_values_are_omitted_not_sent_blank(self):
        """The schema is an xs:sequence, so an empty element is not the same as an absent one."""
        xml = self.client._cart_xml([{"atd_product_number": "1", "quantity": 1}])
        self.assertNotIn("<ord:customerponumber>", xml)
        self.assertNotIn("<ord:customercomment>", xml)

    def test_a_consumer_block_is_only_built_for_a_drop_ship(self):
        """
        ATD knows the shop's address from the location number. Describing the dealer as an end
        consumer is how a shop-pickup order turns into a residential delivery.
        """
        address = dict(
            name="Shop", address1="1 Main", city="Tampa", state="FL",
            postal_code="33601", country="US",
        )
        self.assertIsNone(
            atd_orders.ATDOrderAdapter._ship_to_dict(
                order_base.ShipToAddress(is_shop_address=True, **address)))

        consumer = atd_orders.ATDOrderAdapter._ship_to_dict(
            order_base.ShipToAddress(is_shop_address=False, **address))
        self.assertEqual(consumer["city"], "Tampa")
        xml = self.client._cart_xml([{"atd_product_number": "1", "quantity": 1}], ship_to=consumer)
        self.assertIn("<ord:consumerdata>", xml)
        self.assertIn("<c:zipcode>33601</c:zipcode>", xml)

    def test_xml_special_characters_are_escaped(self):
        xml = self.client._cart_xml(
            [{"atd_product_number": "1", "quantity": 1}], customer_po_number='A&B "x" <y>')
        self.assertIn("A&amp;B &quot;x&quot; &lt;y&gt;", xml)
        self.assertNotIn("<y>", xml)


class AdapterHelperTests(SimpleTestCase):
    databases = []

    def test_dates_parse_from_both_shapes_atd_uses(self):
        # preview sends xs:date, order detail sends xs:dateTime
        self.assertEqual(atd_orders._date("2026-10-30"), datetime.date(2026, 10, 30))
        self.assertEqual(atd_orders._date("2026-10-28T09:12:00"), datetime.date(2026, 10, 28))
        self.assertIsNone(atd_orders._date(""))
        self.assertIsNone(atd_orders._date(None))

    def test_unknown_fulfillment_status_counts_as_shipping(self):
        """
        Their status vocabulary is undocumented. Treating an unknown value as a backorder would
        silently move a real quantity out of the shippable bucket, which is the worse error.
        """
        self.assertTrue(atd_orders._ships_now("Available"))
        self.assertTrue(atd_orders._ships_now("Something New"))
        self.assertFalse(atd_orders._ships_now("Backordered"))
        self.assertFalse(atd_orders._ships_now("  BACKORDER "))

    def test_adapter_capabilities(self):
        a = atd_orders.ATDOrderAdapter
        self.assertFalse(a.supports_cancel(a))
        self.assertFalse(a.supports_shipping_method_selection(a))
        self.assertTrue(a.supports_invoices(a))


class CredentialResolutionTests(SimpleTestCase):
    """
    ATD issues one credential set per dealer covering catalog AND ordering, so the adapter reads
    the feed namespace -- there is nothing for a dealer to re-enter. The only order-account field
    is an optional location override.
    """
    databases = []

    FEED = {"username": "sc", "password": "pw", "client_id": "cid", "location": "3036915"}

    def _adapter(self, feed, order):
        atd_orders.credentials_helper.get_feed_credentials = lambda cp: feed
        atd_orders.credentials_helper.get_order_credentials = lambda cp, acct=None: order
        self.addCleanup(self._restore)
        return atd_orders.ATDOrderAdapter(object(), None)

    def _restore(self):
        from src.integrations import credentials as real
        import importlib
        importlib.reload(real)
        atd_orders.credentials_helper = real

    def test_feed_credentials_are_used_with_no_order_account_configured(self):
        client = self._adapter(self.FEED, {})._client
        self.assertEqual(client.username, "sc")
        self.assertEqual(client.client_id, "cid")
        self.assertEqual(client.location, "3036915")

    def test_an_order_account_can_override_only_the_location(self):
        """A dealer may pull catalog from one ATD DC and buy from another."""
        client = self._adapter(self.FEED, {"location": "3036999"})._client
        self.assertEqual(client.location, "3036999")
        self.assertEqual(client.username, "sc", "auth must still be the dealer's feed credentials")

    def test_a_blank_override_does_not_wipe_the_feed_location(self):
        for blank in ("", "   ", None):
            with self.subTest(blank=blank):
                self.assertEqual(self._adapter(self.FEED, {"location": blank})._client.location,
                                 "3036915")

    def test_missing_feed_credentials_raise_valueerror_so_the_registry_reports_no_adapter(self):
        """
        orders/registry.get_adapter() turns ValueError into None, which is how the FE learns to
        disable ordering. Any other exception would crash the orders page instead.
        """
        with self.assertRaises(ValueError):
            self._adapter({"username": "sc"}, {})

    def test_the_catalog_declares_no_required_api_order_fields(self):
        """
        Guards the pairing between the catalog and this adapter: if someone adds a required API
        order field, dealers get asked for credentials the adapter never reads.
        """
        from src import constants as src_constants
        from src import enums as src_enums
        entry = src_constants.catalog_entry_for_kind(src_enums.BrandProviderKind.ATD.value)
        self.assertFalse(entry.get("order_connection_required_fields"))
        # ...but NOT both lists empty -- create_order_account refuses such a provider outright,
        # which would make the API channel unreachable from the UI.
        self.assertEqual(entry.get("order_connection_optional_fields"), ["location"])
