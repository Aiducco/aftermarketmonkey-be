"""
Tests for the X12 reader against Rough Country's own sample documents.

The fixtures are Alluvia's four samples byte-for-byte, quirks included, because the quirks are
the point. Their generator does not produce consistent files: the repetition separator is ``^``
on the 810, ``|`` on the 855/856 and ``U`` on the 850 (the 4010 usage indicator, left in a
005010 document), the 856's GS03 carries a trailing space, and the 850 is *mixed* CRLF and bare
CR with segment breaks appearing both mid-line and at line ends. Any reader that assumes
delimiters, or that treats the files as lines, parses at least one of these wrongly.

The truncation test is the one that protects real data: we archive a file out of Alluvia's
Outbound folder once we have downloaded it, so failing to notice a half-written file would
lose that document permanently.
"""
import os

from django.test import SimpleTestCase

from src.integrations.clients.rough_country import edi as rough_country_edi
from src.integrations.edi import exceptions as edi_exceptions
from src.integrations.edi import x12

_FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "rough_country_edi")


def _fixture(name: str) -> str:
    with open(os.path.join(_FIXTURE_DIR, name), "r", encoding="ascii", errors="replace") as handle:
        return handle.read()


class RoughCountrySampleParsingTests(SimpleTestCase):
    def test_delimiters_are_read_from_each_isa_not_assumed(self):
        # Same trading partner, same version, three different repetition separators.
        self.assertEqual(x12.parse_interchange(_fixture("810_sample.x12")).delimiters.repetition, "^")
        self.assertEqual(x12.parse_interchange(_fixture("855_sample.x12")).delimiters.repetition, "|")
        self.assertEqual(x12.parse_interchange(_fixture("856_sample.x12")).delimiters.repetition, "|")
        self.assertEqual(x12.parse_interchange(_fixture("850_sample.x12")).delimiters.repetition, "U")

        for name in ("810_sample.x12", "850_sample.x12", "855_sample.x12", "856_sample.x12"):
            delimiters = x12.parse_interchange(_fixture(name)).delimiters
            self.assertEqual(delimiters.element, "*", name)
            self.assertEqual(delimiters.component, ">", name)
            self.assertEqual(delimiters.segment, "~", name)

    def test_mixed_cr_and_crlf_850_parses(self):
        """The 850 sample wraps some segments onto their own lines with bare CR. Treating the
        file as lines, or leaving the CRs in, shifts the ISA's fixed offsets and corrupts every
        delimiter read out of it."""
        interchange = x12.parse_interchange(_fixture("850_sample.x12"))
        self.assertEqual(interchange.sender_id, "TRIDENT")
        self.assertEqual(interchange.receiver_id, "64ROUGHCT")
        transaction = interchange.transactions()[0]
        self.assertEqual(transaction.transaction_set_code, "850")
        self.assertEqual(len(transaction.find_all("PO1")), 2)
        self.assertEqual(
            [s.get(7) for s in transaction.find_all("PO1")], ["922", "70920BDA"]
        )

    def test_partner_ids_are_whitespace_stripped(self):
        """The 856's GS03 is "TRIDENT " — a trailing space their generator pads in. Matching
        partner ids without stripping would fail to recognise our own mailbox."""
        interchange = x12.parse_interchange(_fixture("856_sample.x12"))
        self.assertEqual(interchange.receiver_id, "TRIDENT")
        self.assertEqual(interchange.groups[0].receiver_id, "TRIDENT")

    def test_transaction_envelopes_and_segment_counts(self):
        expected = {
            "810_sample.x12": ("IN", "810", 19),
            "850_sample.x12": ("PO", "850", 16),
            "855_sample.x12": ("PR", "855", 11),
            "856_sample.x12": ("SH", "856", 24),
        }
        for name, (functional_id, transaction_set, segment_count) in expected.items():
            interchange = x12.parse_interchange(_fixture(name))
            group = interchange.groups[0]
            transaction = group.transactions[0]
            self.assertEqual(group.functional_id_code, functional_id, name)
            self.assertEqual(group.version, "005010", name)
            self.assertEqual(transaction.transaction_set_code, transaction_set, name)
            # SE01 is the sender's own declared ST..SE count; all four samples get it right.
            self.assertEqual(transaction.segments[-1].get(1), str(segment_count), name)
            self.assertEqual(len(transaction.segments), segment_count, name)

    def test_850_sample_ctt_undercounts_its_own_lines(self):
        """Pins the defect rather than working around it: the 850 sample says CTT*1 with two
        PO1 lines. It is the template we were given for the document *we* generate, so this
        test exists to stop anyone copying it verbatim."""
        transaction = x12.parse_interchange(_fixture("850_sample.x12")).transactions()[0]
        self.assertEqual(transaction.find("CTT").get(1), "1")
        self.assertEqual(len(transaction.find_all("PO1")), 2)

    def test_truncated_interchange_is_rejected(self):
        payload = _fixture("855_sample.x12")
        truncated = payload[: len(payload) // 2]
        with self.assertRaises(edi_exceptions.EdiParseError):
            x12.parse_interchange(truncated)

    def test_payload_without_iea_is_rejected(self):
        """A file caught mid-write parses cleanly right up to the cut. The missing IEA trailer
        is the only reliable signal, and it gates the archive move."""
        payload = _fixture("810_sample.x12")
        no_trailer = payload[: payload.rindex("IEA")]
        with self.assertRaises(edi_exceptions.EdiParseError):
            x12.parse_interchange(no_trailer)

    def test_non_x12_payload_is_rejected(self):
        with self.assertRaises(edi_exceptions.EdiParseError):
            x12.parse_interchange("<html>404 Not Found</html>")


class RoughCountryDocumentSummaryTests(SimpleTestCase):
    def test_po_reference_is_extracted_from_each_inbound_document_type(self):
        """All three inbound documents carry our own PO number, each in a different segment —
        BAK03 on the 855, PRF01 on the 856, BIG04 on the 810. That is the only join key back to
        a PurchaseOrder, since Rough Country's 855 carries no order number of their own."""
        for name in ("855_sample.x12", "856_sample.x12", "810_sample.x12"):
            summary = rough_country_edi.describe(_fixture(name))
            self.assertNotIn("error", summary, name)
            self.assertEqual(summary["po_references"], ["PONUMBER1"], name)
            self.assertEqual(summary["sender"], "64ROUGHCT", name)

    def test_describe_reports_transaction_sets(self):
        summary = rough_country_edi.describe(_fixture("856_sample.x12"))
        self.assertEqual(summary["transaction_sets"], ["856"])
        self.assertEqual(summary["interchange_control_number"], "415360312")

    def test_describe_never_raises_on_junk(self):
        self.assertIn("error", rough_country_edi.describe("not an interchange"))


class X12ScrubTests(SimpleTestCase):
    def test_delimiters_are_stripped_from_free_text(self):
        """X12 has no escape mechanism at all: one ``*`` in a customer's name silently shifts
        every following element by one."""
        self.assertEqual(x12.scrub("A*B~C>D^E"), "A B C D E")

    def test_newlines_and_control_characters_collapse_to_single_spaces(self):
        self.assertEqual(x12.scrub("123 Main St\r\nApt 4"), "123 Main St Apt 4")

    def test_max_length_truncates(self):
        self.assertEqual(x12.scrub("Rough Country 20 inch LED Light Bar", max_length=13), "Rough Country")
