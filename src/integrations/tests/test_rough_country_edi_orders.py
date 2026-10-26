"""
Tests for the Rough Country EDI order channel: the 850 we build, the three documents we parse,
the per-company credential resolver, and the control-number counters.

The inbound fixtures are a mix on purpose. ``*_live_20261019.x12`` came from Rough Country's
production generator during the 16-19 Oct 2026 round trip and is what real traffic looks like;
``*_sample.x12`` are Alluvia's hand-edited August samples, which are wrong in ways worth
pinning down (the 855's ACK02 disagrees with its own PO1-02, the 810's second line repeats the
first line's vendor part). Both must parse.

The multi-carton 856 here is synthetic because every real ASN we have is a single package, and
a single package cannot detect the bug that matters: attaching every tracking number in the
shipment to every item instead of walking the HL tree.
"""
import datetime
import decimal
import os

from django.test import SimpleTestCase, TestCase

from src import models as src_models
from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.clients.rough_country import edi_config, inbound, po_850
from src.integrations.edi import control_numbers
from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import x12

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "rough_country_edi")

_CREDS = {"ftp_user": "TridentRC", "ftp_password": "secret", "edi_sender_id": "TRIDENT"}


def _fixture_transaction(name: str) -> x12.Transaction:
    with open(os.path.join(_FIXTURE_DIR, name), "r", encoding="ascii", errors="replace") as handle:
        return x12.parse_interchange(handle.read()).transactions()[0]


def _request(**overrides) -> po_850.Po850Request:
    defaults = dict(
        po_number="TEST-C16-20261016",
        lines=[
            po_850.Po850Line(
                line_number=1,
                quantity=1,
                rough_country_part_number="922",
                our_part_number="RC-922",
                unit_price=decimal.Decimal("33.22"),
                description="2 Inch Leveling Kit | Chevy/GMC Canyon (2015-2022)",
            )
        ],
        ship_to=po_850.Po850ShipTo(
            name="Trident Motorsports Test",
            address1="1111 Mockingbird Lane",
            city="Dallas",
            state="TX",
            postal_code="75201",
        ),
        contact=po_850.Po850Contact(name="EDI Test", phone="9999999999", email="edi@test.com"),
        order_date=datetime.date(2026, 10, 16),
        ship_method_code="55",
    )
    defaults.update(overrides)
    return po_850.Po850Request(**defaults)


class Build850Tests(SimpleTestCase):
    def test_round_trips_through_our_own_reader(self):
        payload = po_850.build_850(
            _request(),
            "TRIDENT",
            "64ROUGHCT",
            "000000001",
            "1",
            "0001",
            now=datetime.datetime(2026, 10, 16, 13, 19),
        )
        interchange = x12.parse_interchange(payload)
        self.assertEqual(interchange.sender_id, "TRIDENT")
        self.assertEqual(interchange.receiver_id, "64ROUGHCT")
        self.assertEqual(interchange.usage_indicator, "T")
        transaction = interchange.transactions()[0]
        self.assertEqual(transaction.transaction_set_code, "850")
        self.assertEqual(transaction.find("BEG").get(3), "TEST-C16-20261016")
        # TD5-04 is the "mutually defined" method qualifier; TD5-05 carries their code.
        self.assertEqual(transaction.find("TD5").get(4), "ZZ")
        self.assertEqual(transaction.find("TD5").get(5), "55")

    def test_se01_is_computed_not_declared(self):
        payload = po_850.build_850(_request(), "TRIDENT", "64ROUGHCT", "000000001", "1", "0001")
        transaction = x12.parse_interchange(payload).transactions()[0]
        se = [s for s in transaction.segments if s.tag == "SE"][0]
        self.assertEqual(se.get(1), str(len(transaction.segments)))

    def test_ctt_counts_lines_unlike_alluvias_own_sample(self):
        """Their sample 850 says CTT*1 for two PO1 lines. Ours cannot."""
        two_lines = [
            po_850.Po850Line(1, 1, "922", "RC-922", decimal.Decimal("33.22")),
            po_850.Po850Line(2, 3, "70920BDA", "RC-70920BDA", decimal.Decimal("160.97")),
        ]
        segments = po_850.build_850_segments(_request(lines=two_lines))
        ctt = [s for s in segments if s.tag == "CTT"][0]
        self.assertEqual(ctt.get(1), "2")

    def test_amt_total_is_quantity_times_price(self):
        two_lines = [
            po_850.Po850Line(1, 1, "922", "RC-922", decimal.Decimal("33.22")),
            po_850.Po850Line(2, 3, "70920BDA", "RC-70920BDA", decimal.Decimal("160.97")),
        ]
        segments = po_850.build_850_segments(_request(lines=two_lines))
        amt = [s for s in segments if s.tag == "AMT"][0]
        self.assertEqual(amt.get(2), "516.13")  # 33.22 + 3 x 160.97

    def test_amt_omitted_when_any_line_has_no_price(self):
        """A total that silently excludes unpriced lines is worse than no total; AMT is optional."""
        lines = [
            po_850.Po850Line(1, 1, "922", "RC-922", decimal.Decimal("33.22")),
            po_850.Po850Line(2, 1, "70920BDA", "RC-70920BDA", None),
        ]
        segments = po_850.build_850_segments(_request(lines=lines))
        self.assertFalse([s for s in segments if s.tag == "AMT"])

    def test_over_long_po_number_is_rejected_rather_than_truncated(self):
        """Every inbound document echoes BEG03 back as the only join key, so a shortened PO
        number means an order whose 855/856/810 can never be matched to it."""
        with self.assertRaises(edi_exceptions.EdiBuildError):
            po_850.build_850_segments(_request(po_number="X" * 23))

    def test_po_number_containing_a_delimiter_is_rejected(self):
        with self.assertRaises(edi_exceptions.EdiBuildError):
            po_850.build_850_segments(_request(po_number="PO*1"))

    def test_non_positive_quantity_is_rejected(self):
        bad = [po_850.Po850Line(1, 0, "922", "RC-922", decimal.Decimal("1"))]
        with self.assertRaises(edi_exceptions.EdiBuildError):
            po_850.build_850_segments(_request(lines=bad))

    def test_delimiters_in_free_text_are_scrubbed(self):
        ship_to = po_850.Po850ShipTo(
            name="A*B ~ C", address1="1 Main>St", city="Dallas", state="TX", postal_code="75201"
        )
        segments = po_850.build_850_segments(_request(ship_to=ship_to))
        n1 = [s for s in segments if s.tag == "N1"][0]
        self.assertNotIn("*", n1.get(2))
        self.assertNotIn("~", n1.get(2))

    def test_price_never_renders_in_scientific_notation(self):
        lines = [po_850.Po850Line(1, 1, "922", "RC-922", decimal.Decimal("1E+2"))]
        segments = po_850.build_850_segments(_request(lines=lines))
        po1 = [s for s in segments if s.tag == "PO1"][0]
        self.assertEqual(po1.get(4), "100.00")


class Parse855Tests(SimpleTestCase):
    def test_ack02_wins_over_po1_02_where_the_sample_contradicts_itself(self):
        ack = inbound.parse_855(_fixture_transaction("855_sample.x12"))
        self.assertEqual(ack.po_number, "PONUMBER1")
        second = ack.lines[1]
        self.assertEqual(second.quantity_ordered, 1)  # what PO1-02 claims
        self.assertEqual(second.quantity_acknowledged, 3)  # what ACK02 says, and what we use
        self.assertTrue(second.is_accepted)

    def test_po1_with_no_ack_is_kept_with_no_acknowledged_quantity(self):
        """Dropping it would silently confirm a line the partner never acknowledged."""
        payload = "ST*855*0001~BAK*00*AD*PO1*20261016~" "PO1*1*2*EA*10**VC*922*SK*RC-922~" "CTT*1~SE*4*0001~"
        segments, _ = x12.parse_segments(
            "ISA*00*          *00*          *ZZ*64ROUGHCT      *ZZ*TRIDENT        "
            "*261016*1725*|*00501*430772341*0*T*>~GS*PR*64ROUGHCT*TRIDENT*20261016*172541"
            "*430772341*X*005010~" + payload + "GE*1*430772341~IEA*1*430772341~"
        )
        transaction = x12.Transaction("855", "0001", segments)
        ack = inbound.parse_855(transaction)
        self.assertEqual(len(ack.lines), 1)
        self.assertIsNone(ack.lines[0].quantity_acknowledged)
        self.assertFalse(ack.lines[0].is_accepted)

    def test_rejection_and_backorder_codes_are_classified(self):
        line = inbound.Ack855Line(1, "922", "RC-922", 1, 0, "IR", None)
        self.assertTrue(line.is_rejected)
        self.assertFalse(line.is_accepted)
        line = inbound.Ack855Line(1, "922", "RC-922", 1, 0, "IB", None)
        self.assertTrue(line.is_backordered)


class Parse856Tests(SimpleTestCase):
    def test_live_asn(self):
        asn = inbound.parse_856(_fixture_transaction("856_live_20261019.x12"))
        self.assertEqual(asn.shipment_id, "12013357")
        self.assertEqual(asn.po_number, "TEST-C16-20261016")
        self.assertEqual(asn.ship_date, datetime.date(2026, 10, 16))
        self.assertEqual(asn.warehouse_code, "435")
        self.assertEqual(asn.tracking_numbers, ("546558395139",))
        self.assertEqual(len(asn.items), 1)
        self.assertEqual(asn.items[0].rough_country_part_number, "922")
        self.assertEqual(asn.items[0].quantity_shipped, 1)

    def test_carrier_code_outside_the_published_list_is_preserved_verbatim(self):
        """Their published TD5-05 list is 55/58/78/79; this real ASN says 99. Inbound codes are
        opaque strings, never validated against a closed set."""
        asn = inbound.parse_856(_fixture_transaction("856_live_20261019.x12"))
        self.assertEqual(asn.carrier_code, "99")

    def test_multi_carton_tracking_follows_the_hl_tree(self):
        """Two packs, one item in each. A flat scan would give both items both tracking
        numbers, which is the whole reason the parser walks HL parents."""
        body = (
            "ST*856*0001~BSN*00*SHIP1*20261016*125500*0001~"
            "HL*1**S~TD5****ZZ*55~DTM*011*20261016~"
            "N1*SE*Rough Country*92*435~"
            "HL*2*1*O~PRF*PO-MULTI***20261016~"
            "HL*3*2*P~MAN*CP*TRACK-A~"
            "HL*4*3*I~LIN*1*VC*922*SK*RC-922~SN1**1*EA~"
            "HL*5*2*P~MAN*CP*TRACK-B~"
            "HL*6*5*I~LIN*2*VC*70920BDA*SK*RC-70920BDA~SN1**2*EA~"
            "CTT*2~SE*17*0001~"
        )
        segments, _ = x12.parse_segments(
            "ISA*00*          *00*          *ZZ*64ROUGHCT      *ZZ*TRIDENT        "
            "*261016*1725*|*00501*431684201*0*T*>~GS*SH*64ROUGHCT*TRIDENT*20261016*172541"
            "*431684201*X*005010~" + body + "GE*1*431684201~IEA*1*431684201~"
        )
        asn = inbound.parse_856(x12.Transaction("856", "0001", segments))
        self.assertEqual(asn.po_number, "PO-MULTI")
        self.assertEqual(len(asn.items), 2)
        by_part = {i.rough_country_part_number: i for i in asn.items}
        self.assertEqual(by_part["922"].tracking_numbers, ("TRACK-A",))
        self.assertEqual(by_part["70920BDA"].tracking_numbers, ("TRACK-B",))
        self.assertEqual(by_part["70920BDA"].quantity_shipped, 2)
        self.assertEqual(set(asn.tracking_numbers), {"TRACK-A", "TRACK-B"})


class Parse810Tests(SimpleTestCase):
    def test_tds_and_sac_are_implied_two_decimal_integers(self):
        invoice = inbound.parse_810(_fixture_transaction("810_sample.x12"))
        self.assertEqual(invoice.invoice_number, "88812345")
        self.assertEqual(invoice.total_amount, decimal.Decimal("1849.97"))  # TDS*184997
        self.assertEqual(invoice.freight, decimal.Decimal("8.95"))  # SAC...895
        self.assertEqual(invoice.net_days, 30)

    def test_tds_excluding_freight_is_visible_rather_than_reconciled_away(self):
        invoice = inbound.parse_810(_fixture_transaction("810_sample.x12"))
        self.assertEqual(invoice.total_amount, invoice.line_subtotal)
        self.assertIsNotNone(invoice.freight)

    def test_unit_price_keeps_four_decimal_places(self):
        """Rough Country's price is authoritative and arrives as 33.2168. Rounding it to 2dp
        here would drift against their invoice as quantity grows."""
        body = (
            "ST*810*0001~BIG*20261016*12484183*20261016*TEST-C16-20261016~ITD*******0~"
            "IT1**100*EA*33.2168**VC*922*SK*RC-922~TDS*332168~CTT*1~SE*7*0001~"
        )
        segments, _ = x12.parse_segments(
            "ISA*00*          *00*          *ZZ*64ROUGHCT      *ZZ*TRIDENT        "
            "*261016*1725*^*00501*431684200*0*T*>~GS*IN*64ROUGHCT*TRIDENT*20261016*172541"
            "*431684200*X*005010~" + body + "GE*1*431684200~IEA*1*431684200~"
        )
        invoice = inbound.parse_810(x12.Transaction("810", "0001", segments))
        self.assertEqual(invoice.lines[0].unit_price, decimal.Decimal("33.2168"))
        self.assertEqual(invoice.line_subtotal, decimal.Decimal("3321.68"))


class EdiConfigTests(SimpleTestCase):
    def test_defaults_to_the_test_environment(self):
        config = edi_config.from_credentials(_CREDS)
        self.assertEqual(config.receiver_id, "64ROUGHCT")
        self.assertEqual(config.usage_indicator, "T")
        self.assertFalse(config.is_production)

    def test_production_pairs_the_production_id_with_isa15_p(self):
        config = edi_config.from_credentials(dict(_CREDS, edi_environment="production"))
        self.assertEqual(config.receiver_id, "64ROUGHC")
        self.assertEqual(config.usage_indicator, "P")
        self.assertTrue(config.is_production)

    def test_an_unrecognised_environment_fails_safe_to_test(self):
        """'prod' must not be read as production: ISA15=P against the production id is how a
        test file becomes a real shipment."""
        for value in ("prod", "live", "PRODUCTIONN", ""):
            config = edi_config.from_credentials(dict(_CREDS, edi_environment=value))
            self.assertEqual(config.usage_indicator, "T", value)
            self.assertEqual(config.receiver_id, "64ROUGHCT", value)

    def test_environment_is_case_and_whitespace_insensitive(self):
        config = edi_config.from_credentials(dict(_CREDS, edi_environment=" Production "))
        self.assertTrue(config.is_production)

    def test_missing_credentials_raise_value_error_for_the_registry_to_catch(self):
        with self.assertRaises(ValueError):
            edi_config.from_credentials({"ftp_user": "only"})

    def test_sender_id_longer_than_isa06_is_rejected(self):
        with self.assertRaises(ValueError):
            edi_config.from_credentials(dict(_CREDS, edi_sender_id="A" * 16))

    def test_describe_never_leaks_the_password(self):
        config = edi_config.from_credentials(_CREDS)
        self.assertNotIn("secret", config.describe())


class ClientTests(SimpleTestCase):
    def test_local_directory_is_namespaced_per_trading_partner(self):
        client = rough_country_edi.RoughCountryEdiClient(edi_config.from_credentials(_CREDS))
        self.assertTrue(client.local_dir.endswith("TRIDENT"))

    def test_refuses_to_send_a_document_type_we_only_receive(self):
        client = rough_country_edi.RoughCountryEdiClient(edi_config.from_credentials(_CREDS))
        with self.assertRaises(ValueError):
            client.send_document("855", "x.x12", "payload")


class ControlNumberTests(TestCase):
    def test_numbers_are_monotonic_and_never_repeat(self):
        issued = [control_numbers.next_interchange_control_number("TRIDENT") for _ in range(50)]
        self.assertEqual(len(set(issued)), 50)
        self.assertEqual(issued, sorted(issued))

    def test_interchange_number_is_padded_to_isa13s_fixed_width(self):
        self.assertEqual(control_numbers.next_interchange_control_number("TRIDENT"), "000000001")

    def test_transaction_number_meets_st02s_four_character_minimum(self):
        self.assertEqual(control_numbers.next_transaction_control_number("TRIDENT"), "0001")

    def test_each_partner_keeps_its_own_sequence(self):
        """Two shops both start at 1 — which is exactly why EdiDocument's dedup constraint is
        scoped per company rather than per interchange number."""
        self.assertEqual(control_numbers.next_interchange_control_number("TRIDENT"), "000000001")
        self.assertEqual(control_numbers.next_interchange_control_number("SUNCOAST"), "000000001")
        self.assertEqual(control_numbers.next_interchange_control_number("TRIDENT"), "000000002")

    def test_the_three_sequences_are_independent(self):
        control_numbers.next_interchange_control_number("TRIDENT")
        control_numbers.next_interchange_control_number("TRIDENT")
        self.assertEqual(control_numbers.next_group_control_number("TRIDENT"), "1")
        self.assertEqual(
            control_numbers.peek("TRIDENT"),
            {src_models.EdiControlNumber.KIND_INTERCHANGE: 2, src_models.EdiControlNumber.KIND_GROUP: 1},
        )

    def test_blank_partner_id_is_refused(self):
        with self.assertRaises(ValueError):
            control_numbers.next_interchange_control_number("  ")
