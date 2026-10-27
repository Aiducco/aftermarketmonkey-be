"""
Tests for the Rough Country EDI order adapter and the service that applies their inbound
documents to PurchaseOrder rows.

Inbound payloads are generated with ``x12.build_interchange`` from Rough Country's side of the
envelope rather than hand-written, so every fixture is a structurally valid interchange with a
correct SE01 and a real fixed-width ISA — the same reader path production uses.

The cases worth reading: a replayed document must not apply twice (FTP delivery is
at-least-once), a document naming an unknown PO must be stored rather than dropped, and the
duplicate-SKU case, where one part number appears on two PO lines and nothing inbound carries a
line number to tell them apart.
"""
import datetime
import decimal
from unittest import mock

from django.test import SimpleTestCase, TestCase

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.rough_country import ack_997
from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.clients.rough_country import po_850
from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import x12
from src.integrations.orders import base as order_base
from src.integrations.orders import exceptions as order_exceptions
from src.integrations.orders import registry as order_registry
from src.integrations.orders import rough_country as rough_country_orders
from src.integrations.services import rough_country_edi as apply_service

_PO_NUMBER = "TEST-C16-20261016"
_PART = "922"


def _inbound(functional_id: str, transaction_set_code: str, body, control_number="430772341"):
    """One interchange as Rough Country would send it."""
    return x12.build_interchange(
        sender_id="64ROUGHCT",
        receiver_id="TRIDENT",
        interchange_control_number=control_number,
        group_control_number=control_number,
        functional_id_code=functional_id,
        transactions=[(transaction_set_code, "163802490", body)],
        usage_indicator="T",
    )


def _855(po_number=_PO_NUMBER, part=_PART, acked=1, ack_code="IA", control_number="430772341"):
    return _inbound(
        "PR",
        "855",
        [
            x12.segment("BAK", "00", "AD", po_number, "20261016"),
            x12.segment("PO1", "1", "1", "EA", "33.22", "", "VC", part, "SK", "RC-922"),
            x12.segment("ACK", ack_code, str(acked), "EA"),
            x12.segment("CTT", "1"),
        ],
        control_number,
    )


def _856(po_number=_PO_NUMBER, part=_PART, shipped=1, control_number="431684201"):
    return _inbound(
        "SH",
        "856",
        [
            x12.segment("BSN", "00", "12013357", "20261016", "125500", "0001"),
            x12.segment("HL", "1", "", "S"),
            x12.segment("TD5", "", "", "", "ZZ", "99"),
            x12.segment("REF", "CN", "546558395139"),
            x12.segment("DTM", "011", "20261016"),
            x12.segment("N1", "SE", "Rough Country", "92", "435"),
            x12.segment("HL", "2", "1", "O"),
            x12.segment("PRF", po_number, "", "", "20261016"),
            x12.segment("HL", "3", "2", "P"),
            x12.segment("MAN", "CP", "546558395139"),
            x12.segment("HL", "4", "3", "I"),
            x12.segment("LIN", "24154877", "VC", part, "SK", "RC-922"),
            x12.segment("SN1", "", str(shipped), "EA"),
            x12.segment("CTT", "1"),
        ],
        control_number,
    )


def _810(po_number=_PO_NUMBER, part=_PART, qty=1, unit="33.2168", control_number="431684200"):
    return _inbound(
        "IN",
        "810",
        [
            x12.segment("BIG", "20261016", "12484183", "20261016", po_number),
            x12.segment("ITD", "", "", "", "", "", "", "0"),
            x12.segment("IT1", "", str(qty), "EA", unit, "", "VC", part, "SK", "RC-922"),
            x12.segment("PID", "F", "", "", "", "2 Inch Leveling Kit"),
            x12.segment("TDS", "3322"),
            x12.segment("CAD", "ZZ", "", "", "99", "Shopground"),
            x12.segment("CTT", "1"),
        ],
        control_number,
    )


class Ack997Tests(SimpleTestCase):
    def test_acknowledges_the_group_and_each_transaction_set(self):
        payload = ack_997.build_997(
            "PR",
            "430772341",
            [("855", "163802490")],
            "TRIDENT",
            "64ROUGHCT",
            "000000002",
            "2",
            "0001",
        )
        transaction = x12.parse_interchange(payload).transactions()[0]
        self.assertEqual(transaction.transaction_set_code, "997")
        self.assertEqual(transaction.find("AK1").elements, ["PR", "430772341"])
        self.assertEqual(transaction.find("AK2").elements, ["855", "163802490"])
        self.assertEqual(transaction.find("AK5").get(1), "A")

    def test_ak9_counts_match_the_ak2s_present(self):
        payload = ack_997.build_997(
            "PR",
            "430772341",
            [("855", "1"), ("855", "2")],
            "TRIDENT",
            "64ROUGHCT",
            "000000002",
            "2",
            "0001",
        )
        transaction = x12.parse_interchange(payload).transactions()[0]
        self.assertEqual(transaction.find("AK9").elements, ["A", "2", "2", "2"])
        self.assertEqual(len(transaction.find_all("AK2")), 2)

    def test_group_functional_id_is_fa_regardless_of_what_is_acknowledged(self):
        payload = ack_997.build_997("IN", "1", [("810", "1")], "TRIDENT", "64ROUGHCT", "000000002", "2", "0001")
        self.assertEqual(x12.parse_interchange(payload).groups[0].functional_id_code, "FA")

    def test_refuses_to_acknowledge_nothing(self):
        with self.assertRaises(edi_exceptions.EdiBuildError):
            ack_997.build_997_segments("PR", "430772341", [])


class RoughCountryEdiFixture(TestCase):
    """A company with a configured Rough Country EDI order account and a one-line PO."""

    def setUp(self):
        self.company = src_models.Company.objects.create(
            name="Trident Motorsports", slug="trident-motorsports-test", status=1, status_name="ACTIVE"
        )
        self.provider = src_models.Providers.objects.create(
            name="Rough Country",
            status=1,
            status_name="ACTIVE",
            type=1,
            type_name="DISTRIBUTOR",
            kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
            kind_name="ROUGH_COUNTRY",
        )
        self.company_provider = src_models.CompanyProviders.objects.create(
            company=self.company, provider=self.provider, credentials={}
        )
        self.account = src_models.CompanyProviderOrderAccount.objects.create(
            company_provider=self.company_provider,
            label="Default",
            is_default=True,
            active=True,
            credentials={
                "ftp_user": "TridentRC",
                "ftp_password": "secret",
                "edi_sender_id": "TRIDENT",
            },
        )
        brand = src_models.Brands.objects.create(name="Rough Country", status=1, status_name="ACTIVE")
        self.master_part = src_models.MasterPart.objects.create(brand=brand, part_number="RC-922")
        self.provider_part = src_models.ProviderPart.objects.create(
            master_part=self.master_part, provider=self.provider, provider_external_id=_PART
        )
        self.po = src_models.PurchaseOrder.objects.create(
            company=self.company,
            company_provider=self.company_provider,
            po_number=_PO_NUMBER,
            status=src_enums.PurchaseOrderStatus.SUBMITTED.value,
            status_name=src_enums.PurchaseOrderStatus.SUBMITTED.name,
            source=src_enums.PurchaseOrderSource.STAFF_MANUAL.value,
            source_name=src_enums.PurchaseOrderSource.STAFF_MANUAL.name,
        )
        self.line_item = src_models.PurchaseOrderLineItem.objects.create(
            purchase_order=self.po,
            provider_part=self.provider_part,
            quantity=1,
            unit_cost=decimal.Decimal("33.22"),
            status=src_enums.PurchaseOrderLineItemStatus.PENDING.value,
            status_name=src_enums.PurchaseOrderLineItemStatus.PENDING.name,
        )

    def adapter(self):
        return rough_country_orders.RoughCountryEdiOrderAdapter(self.company_provider, self.account)

    def set_environment(self, value):
        self.account.credentials = dict(self.account.credentials, edi_environment=value)
        self.account.save(update_fields=["credentials"])


class AdapterCapabilityTests(RoughCountryEdiFixture):
    def test_registered_against_rough_countrys_provider_kind(self):
        adapter = order_registry.get_adapter(self.company_provider, self.account)
        self.assertIsInstance(adapter, rough_country_orders.RoughCountryEdiOrderAdapter)

    def test_reports_the_edi_fulfillment_channel(self):
        self.assertEqual(self.adapter().fulfillment_channel(), "edi")
        self.assertEqual(order_registry.fulfillment_channel_for_kind(self.provider.kind), "edi")

    def test_unconfigured_account_yields_no_adapter_rather_than_raising(self):
        self.account.credentials = {}
        self.account.save(update_fields=["credentials"])
        self.assertIsNone(order_registry.get_adapter(self.company_provider, self.account))

    def test_ftp_login_alone_is_saved_but_not_yet_order_ready(self):
        """
        A dealer enters only the FTP username and password -- the EDI trading-partner id is an
        ISA value Alluvia issues alongside it and support fills in. Until it is present the
        connection must report "not configured" rather than half-work, because an 850 cannot be
        addressed without a sender id.
        """
        self.account.credentials = {"ftp_user": "TridentRC", "ftp_password": "secret"}
        self.account.save(update_fields=["credentials"])
        self.assertIsNone(order_registry.get_adapter(self.company_provider, self.account))
        self.assertIn(
            "aren't configured",
            order_registry.get_adapter_unavailable_reason(self.company_provider, self.account),
        )

        self.account.credentials = dict(self.account.credentials, edi_sender_id="TRIDENT")
        self.account.save(update_fields=["credentials"])
        adapter = order_registry.get_adapter(self.company_provider, self.account)
        self.assertIsInstance(adapter, rough_country_orders.RoughCountryEdiOrderAdapter)
        # No environment set, so the connection is live -- that is the default for a new dealer.
        self.assertTrue(adapter.config.is_production)

    def test_shipping_methods_are_rough_countrys_own_codes(self):
        methods = self.adapter().list_shipping_methods()
        self.assertEqual([m.code for m in methods], ["55", "58", "78", "79"])
        self.assertTrue(self.adapter().supports_shipping_method_selection())

    def test_cancel_is_not_supported(self):
        adapter = self.adapter()
        self.assertFalse(adapter.supports_cancel())
        with self.assertRaises(order_exceptions.OrderNotSupportedError):
            adapter.cancel_order(self.po)

    def test_quote_returns_no_lines_because_nothing_can_be_asked(self):
        """Synthesizing a line would show a shop a quantity_available nobody checked."""
        result = self.adapter().get_shipping_quote(
            [order_base.OrderLineItemRequest(self.line_item.id, self.provider_part, 1)],
            order_base.ShipToAddress("A", "1 Main", "Dallas", "TX", "75201", "US"),
            ship_method="55",
        )
        self.assertEqual(result.lines, [])
        self.assertEqual(result.raw_response["channel"], "edi")
        self.assertEqual(result.raw_response["ship_method_name"], "FedEx Ground")


class AdapterSubmitTests(RoughCountryEdiFixture):
    def _submit(self):
        adapter = self.adapter()
        ship_to = order_base.ShipToAddress(
            name="Trident Motorsports Test",
            address1="1111 Mockingbird Lane",
            city="Dallas",
            state="TX",
            postal_code="75201",
            country="US",
        )
        items = [order_base.OrderLineItemRequest(self.line_item.id, self.provider_part, 1)]
        with mock.patch.object(
            rough_country_edi.RoughCountryEdiClient, "send_document", return_value="/Inbound/850/x.x12"
        ) as send:
            result = adapter.submit_order(self.po, items, ship_to)
        return result, send

    def test_submits_a_valid_850_and_records_it(self):
        result, send = self._submit()
        self.assertEqual(send.call_count, 1)
        document_type, filename, payload = send.call_args[0]
        self.assertEqual(document_type, "850")
        transaction = x12.parse_interchange(payload).transactions()[0]
        self.assertEqual(transaction.transaction_set_code, "850")
        self.assertEqual(transaction.find("BEG").get(3), _PO_NUMBER)
        self.assertEqual(transaction.find("PO1").get(7), _PART)

        document = src_models.EdiDocument.objects.get(doc_type="850")
        self.assertEqual(document.status, src_enums.EdiDocumentStatus.SENT.value)
        self.assertEqual(document.direction, src_enums.EdiDirection.OUTBOUND.value)
        self.assertEqual(document.company_provider_id, self.company_provider.id)
        self.assertEqual(document.purchase_order_id, self.po.id)

    def test_delivery_is_not_confirmation(self):
        """The 850 reaching the mailbox says nothing about acceptance; the 855 does, later."""
        result, _ = self._submit()
        self.assertFalse(result.distributor_confirmed)
        self.assertEqual(result.distributor_order_numbers, [_PO_NUMBER])
        self.assertEqual(result.line_item_placements, [])

    def test_test_environment_addresses_the_test_partner(self):
        self.set_environment("test")
        _, send = self._submit()
        interchange = x12.parse_interchange(send.call_args[0][2])
        self.assertEqual(interchange.receiver_id, "64ROUGHCT")
        self.assertEqual(interchange.usage_indicator, "T")

    def test_an_account_with_no_environment_addresses_the_production_partner(self):
        """The default. A new dealer places real orders as soon as their mailbox exists."""
        self.assertIsNone(self.account.credentials.get("edi_environment"))
        _, send = self._submit()
        interchange = x12.parse_interchange(send.call_args[0][2])
        self.assertEqual(interchange.receiver_id, "64ROUGHC")
        self.assertEqual(interchange.usage_indicator, "P")

    def test_production_environment_addresses_the_production_partner(self):
        self.set_environment("production")
        _, send = self._submit()
        interchange = x12.parse_interchange(send.call_args[0][2])
        self.assertEqual(interchange.receiver_id, "64ROUGHC")
        self.assertEqual(interchange.usage_indicator, "P")

    def test_control_numbers_advance_between_submissions(self):
        first, _ = self._submit()
        self.po.po_number = "TEST-C16-SECOND"
        self.po.save(update_fields=["po_number"])
        second, _ = self._submit()
        self.assertNotEqual(
            first.raw_response["interchange_control_number"],
            second.raw_response["interchange_control_number"],
        )
        self.assertEqual(src_models.EdiDocument.objects.filter(doc_type="850").count(), 2)

    def test_an_undeliverable_850_marks_the_document_and_raises(self):
        adapter = self.adapter()
        ship_to = order_base.ShipToAddress("A", "1 Main", "Dallas", "TX", "75201", "US")
        items = [order_base.OrderLineItemRequest(self.line_item.id, self.provider_part, 1)]
        with mock.patch.object(
            rough_country_edi.RoughCountryEdiClient,
            "send_document",
            side_effect=edi_exceptions.EdiTransportError("timed out"),
        ):
            with self.assertRaises(order_exceptions.OrderAdapterError):
                adapter.submit_order(self.po, items, ship_to)
        document = src_models.EdiDocument.objects.get(doc_type="850")
        self.assertEqual(document.status, src_enums.EdiDocumentStatus.ERROR.value)
        self.assertIn("timed out", document.error_message)

    def test_an_unknown_ship_method_falls_back_rather_than_going_out_verbatim(self):
        self.po.ship_method = "NOT-A-CODE"
        self.po.save(update_fields=["ship_method"])
        _, send = self._submit()
        transaction = x12.parse_interchange(send.call_args[0][2]).transactions()[0]
        self.assertEqual(transaction.find("TD5").get(5), po_850.DEFAULT_SHIP_METHOD_CODE)

    def test_an_unencodable_po_number_is_a_validation_error(self):
        self.po.po_number = "X" * 30
        self.po.save(update_fields=["po_number"])
        adapter = self.adapter()
        ship_to = order_base.ShipToAddress("A", "1 Main", "Dallas", "TX", "75201", "US")
        items = [order_base.OrderLineItemRequest(self.line_item.id, self.provider_part, 1)]
        with mock.patch.object(rough_country_edi.RoughCountryEdiClient, "send_document") as send:
            with self.assertRaises(order_exceptions.OrderValidationError):
                adapter.submit_order(self.po, items, ship_to)
        send.assert_not_called()


class ApplyInboundTests(RoughCountryEdiFixture):
    def _apply(self, payload, doc_type):
        document, is_new = apply_service.record_inbound(self.company_provider, doc_type, payload, "f.x12")
        applied = apply_service.apply_document(document) if is_new else True
        document.refresh_from_db()
        return document, is_new, applied

    def test_855_confirms_the_order_and_its_lines(self):
        document, _, applied = self._apply(_855(), "855")
        self.assertTrue(applied)
        self.assertEqual(document.status, src_enums.EdiDocumentStatus.APPLIED.value)
        self.po.refresh_from_db()
        self.line_item.refresh_from_db()
        self.assertEqual(self.po.status, src_enums.PurchaseOrderStatus.CONFIRMED.value)
        self.assertEqual(self.line_item.quantity_confirmed, 1)
        self.assertEqual(self.line_item.quantity_backordered, 0)
        self.assertEqual(self.line_item.status, src_enums.PurchaseOrderLineItemStatus.CONFIRMED.value)
        self.assertEqual(document.purchase_order_id, self.po.id)

    def test_a_replayed_855_is_recognised_and_not_applied_twice(self):
        """FTP delivery is at-least-once: the same file reappears whenever archiving failed."""
        self._apply(_855(), "855")
        _, is_new, _ = self._apply(_855(), "855")
        self.assertFalse(is_new)
        self.assertEqual(
            src_models.EdiDocument.objects.filter(
                direction=src_enums.EdiDirection.INBOUND.value, doc_type="855"
            ).count(),
            1,
        )

    def test_a_short_acknowledgment_backorders_the_remainder(self):
        self.line_item.quantity = 3
        self.line_item.save(update_fields=["quantity"])
        self._apply(_855(acked=1), "855")
        self.line_item.refresh_from_db()
        self.assertEqual(self.line_item.quantity_confirmed, 1)
        self.assertEqual(self.line_item.quantity_backordered, 2)
        self.assertEqual(self.line_item.status, src_enums.PurchaseOrderLineItemStatus.BACKORDERED.value)

    def test_a_rejected_line_fails_the_order_rather_than_confirming_it(self):
        self._apply(_855(ack_code="IR", acked=0), "855")
        self.po.refresh_from_db()
        self.line_item.refresh_from_db()
        self.assertEqual(self.line_item.status, src_enums.PurchaseOrderLineItemStatus.REJECTED.value)
        self.assertEqual(self.po.status, src_enums.PurchaseOrderStatus.FAILED.value)

    def test_856_writes_tracking_and_ships_the_lines(self):
        self._apply(_855(), "855")
        document, _, applied = self._apply(_856(), "856")
        self.assertTrue(applied)
        self.po.refresh_from_db()
        self.line_item.refresh_from_db()
        order_row = self.po.distributor_orders.get()
        self.assertEqual(order_row.tracking_numbers, ["546558395139"])
        self.assertEqual(order_row.carrier, "99")
        self.assertEqual(order_row.warehouse_code, "435")
        self.assertEqual(order_row.ship_date, datetime.date(2026, 10, 16))
        self.assertEqual(order_row.distributor_internal_order_number, "12013357")
        self.assertEqual(self.line_item.status, src_enums.PurchaseOrderLineItemStatus.SHIPPED.value)
        self.assertEqual(self.po.status, src_enums.PurchaseOrderStatus.FULFILLED.value)

    def test_a_second_asn_adds_tracking_rather_than_replacing_it(self):
        self._apply(_856(), "856")
        second = _856(control_number="431684299").replace("546558395139", "999888777666")
        self._apply(second, "856")
        order_row = self.po.distributor_orders.get()
        self.assertEqual(set(order_row.tracking_numbers), {"546558395139", "999888777666"})

    def test_810_creates_an_invoice_keeping_four_decimal_precision(self):
        document, _, applied = self._apply(_810(qty=100), "810")
        self.assertTrue(applied)
        invoice = self.po.invoices.get()
        self.assertEqual(invoice.invoice_number, "12484183")
        self.assertEqual(invoice.total_price, decimal.Decimal("33.22"))
        # The 2dp columns cannot hold Rough Country's own unit price, so it survives in the JSON.
        self.assertEqual(invoice.line_items[0]["unit_price"], "33.2168")
        self.assertEqual(invoice.line_items[0]["total_price"], "3321.6800")
        # Deliberately not guessed: whether TDS includes freight is still unconfirmed.
        self.assertIsNone(invoice.amount_due)

    def test_a_replayed_810_updates_the_same_invoice(self):
        self._apply(_810(), "810")
        self._apply(_810(control_number="431684299"), "810")
        self.assertEqual(self.po.invoices.count(), 1)

    def test_a_document_naming_an_unknown_po_is_stored_not_dropped(self):
        document, _, applied = self._apply(_855(po_number="NOT-OUR-PO"), "855")
        self.assertEqual(document.status, src_enums.EdiDocumentStatus.UNMATCHED.value)
        self.assertIsNone(document.purchase_order_id)
        self.assertIn("NOT-OUR-PO", document.error_message)
        # Archived anyway: re-collecting forever would re-alert forever, and it is stored.
        self.assertTrue(applied)
        self.po.refresh_from_db()
        self.assertEqual(self.po.status, src_enums.PurchaseOrderStatus.SUBMITTED.value)

    def test_another_companys_po_number_does_not_match(self):
        other_company = src_models.Company.objects.create(
            name="Sun Coast Offroad", slug="sun-coast", status=1, status_name="ACTIVE"
        )
        other_cp = src_models.CompanyProviders.objects.create(
            company=other_company, provider=self.provider, credentials={}
        )
        self.assertIsNone(
            apply_service.match_purchase_order(other_cp, _PO_NUMBER),
            "a PO number must never resolve across company boundaries",
        )
        # And the same 855 arriving in the other shop's mailbox is stored UNMATCHED, not applied
        # to Trident's order.
        document, is_new = apply_service.record_inbound(other_cp, "855", _855(), "f.x12")
        self.assertTrue(is_new)
        apply_service.apply_document(document)
        document.refresh_from_db()
        self.assertEqual(document.status, src_enums.EdiDocumentStatus.UNMATCHED.value)
        self.po.refresh_from_db()
        self.assertEqual(self.po.status, src_enums.PurchaseOrderStatus.SUBMITTED.value)

    def test_an_unparseable_payload_is_stored_with_an_error(self):
        document, is_new = apply_service.record_inbound(self.company_provider, "855", "this is not X12", "junk.x12")
        self.assertTrue(is_new)
        self.assertEqual(document.status, src_enums.EdiDocumentStatus.ERROR.value)
        self.assertFalse(apply_service.apply_document(document))

    def test_a_duplicated_sku_distributes_rather_than_double_counting(self):
        """
        Two PO lines can carry the SAME Rough Country part number, and nothing inbound carries a
        line number to tell them apart. The route is not kit expansion — PurchaseOrderLineItem
        is unique on (purchase_order, provider_part), so one ProviderPart appears at most once.
        It is duplicate MasterParts: ProviderPart is unique on (master_part, provider), NOT on
        (provider, provider_external_id), so two master parts that are really the same product
        each get their own ProviderPart pointing at Rough Country SKU 922.

        The only safe reading of "3 of part 922" across lines of 2 and 2 is to fill the first,
        then the second.
        """
        self.line_item.quantity = 2
        self.line_item.save(update_fields=["quantity"])
        duplicate_master_part = src_models.MasterPart.objects.create(
            brand=self.master_part.brand, part_number="RC-922-DUPLICATE"
        )
        duplicate_provider_part = src_models.ProviderPart.objects.create(
            master_part=duplicate_master_part,
            provider=self.provider,
            provider_external_id=_PART,
        )
        second_line = src_models.PurchaseOrderLineItem.objects.create(
            purchase_order=self.po,
            provider_part=duplicate_provider_part,
            quantity=2,
            status=src_enums.PurchaseOrderLineItemStatus.PENDING.value,
            status_name=src_enums.PurchaseOrderLineItemStatus.PENDING.name,
        )
        self._apply(_855(acked=3), "855")
        self.line_item.refresh_from_db()
        second_line.refresh_from_db()
        self.assertEqual(self.line_item.quantity_confirmed, 2)
        self.assertEqual(second_line.quantity_confirmed, 1)
        self.assertEqual(second_line.quantity_backordered, 1)

    def test_a_cancelled_po_is_not_resurrected_by_a_late_document(self):
        self.po.status = src_enums.PurchaseOrderStatus.CANCELLED.value
        self.po.status_name = src_enums.PurchaseOrderStatus.CANCELLED.name
        self.po.save(update_fields=["status", "status_name"])
        self._apply(_855(), "855")
        self.po.refresh_from_db()
        self.assertEqual(self.po.status, src_enums.PurchaseOrderStatus.CANCELLED.value)
