"""
Tests for ``src.integrations.services.ntw``'s feed parser and for the product-type vocabulary.

The fixture is 19 real rows from the first live file (DELTI_0037746990_20261026230053.csv,
2026-10-27), chosen to cover every shape the parser has to survive rather than the common case:
metric sizes with and without an aspect-ratio column, LT-prefixed sizes, numeric notation where
column 7 is an overall diameter instead of a width, a farm size, a product id that does not
follow the ``<PREFIX>-<part number>`` convention, and a three-digit quantity.

The parser is positional against a HEADERLESS file, so the thing worth testing is that a column
never silently shifts. Everything here is a pure function -- no database.
"""
import pathlib

from django.test import SimpleTestCase

from src.integrations.services import ntw
from src.integrations.utils import product_type as pt

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "ntw" / "DELTI_sample.csv"


def _fixture_bytes() -> bytes:
    return FIXTURE.read_bytes()


class ParseFeedContentTests(SimpleTestCase):
    databases = []

    def setUp(self):
        self.rows = ntw.parse_feed_content(_fixture_bytes())

    def test_every_fixture_row_parses(self):
        self.assertEqual(len(self.rows), 19)

    def test_columns_land_in_the_right_fields(self):
        row = next(r for r in self.rows if r["part_number"] == "5530548")
        self.assertEqual(row["brand_name"], "Sumitomo")
        self.assertEqual(row["product_id"], "SUMI-5530548")
        self.assertEqual(row["size_description"], "10R22.5 TL 14 141/139L")
        self.assertEqual(row["category"], "MEDIUM RADIAL TRUCK")
        self.assertEqual(row["location_id"], "3289")
        self.assertEqual(row["account_number"], "0037746990")
        self.assertEqual(row["model_name"], "Sumitomo ST727")
        self.assertEqual(str(row["cost"]), "322.32")
        self.assertEqual(row["quantity"], 1)

    def test_decimal_formatted_quantities_become_ints(self):
        # Quantities arrive as "4.000"; an int is what ProviderPartInventory stores.
        quantities = {r["part_number"]: r["quantity"] for r in self.rows}
        self.assertEqual(quantities["5110101"], 4)
        self.assertEqual(quantities["62538"], 101)
        for qty in quantities.values():
            self.assertIsInstance(qty, int)

    def test_blank_aspect_ratio_column_is_none_not_zero(self):
        # "10R22.5" has no aspect ratio, so column 8 is empty. Coercing it to 0 would invent a
        # 0-series tire; the field must stay unknown.
        row = next(r for r in self.rows if r["part_number"] == "5530548")
        self.assertIsNone(row["size_component_2"])
        self.assertEqual(row["size_component_1"], "10")
        self.assertEqual(row["size_component_3"], "22.5")

    def test_numeric_notation_keeps_components_verbatim(self):
        # "11X4.00-5": column 7 is the OVERALL DIAMETER here, not a section width. The parser
        # must not reinterpret it -- that is src.domain.tire_size's job, from the size string.
        row = next(r for r in self.rows if r["part_number"] == "5110101")
        self.assertEqual(row["size_description"], "11X4.00-5 TL 2")
        self.assertEqual(row["size_component_1"], "11")
        self.assertEqual(row["size_component_2"], "400")
        self.assertEqual(row["size_component_3"], "5")

    def test_product_id_can_carry_a_different_identifier_than_part_number(self):
        """
        58 of 3,533 live rows break the ``<PREFIX>-<part number>`` convention, and the two Cooper
        rows in this fixture show how: the product id's numeric half is a TBC-internal item number
        (90000002527) that is not the part number (160018024) at all.

        This is the reason ProviderPart identity is keyed on (brand, part_number) rather than on
        the product id -- deriving a part number by trimming the prefix would silently produce a
        different part for these rows.
        """
        cooper = {r["part_number"]: r["product_id"] for r in self.rows if r["brand_name"] == "Cooper"}
        self.assertEqual(cooper["160018024"], "COOP-90000002527")
        self.assertEqual(cooper["170196003"], "COOP-90000008304")
        for part_number, product_id in cooper.items():
            self.assertNotEqual(product_id.split("-", 1)[1], part_number)

    def test_raw_row_is_preserved_for_reparse(self):
        row = next(r for r in self.rows if r["part_number"] == "08404")
        self.assertEqual(row["brand_name"], "Michelin")
        self.assertEqual(row["size_description"], "LT225/75R16 115/112Q E")

    def test_rows_with_the_wrong_column_count_are_dropped(self):
        # A layout change must lose rows loudly rather than map fields by position into the
        # wrong columns.
        content = b"only,three,columns\n" + _fixture_bytes()
        rows = ntw.parse_feed_content(content)
        self.assertEqual(len(rows), 19)

    def test_rows_without_brand_or_part_number_are_dropped(self):
        blank_brand = b"0037746990,3289,desc,205/55R16,1.000,PASSENGER,205,55,16,99.99,model,,12345,X-12345\n"
        rows = ntw.parse_feed_content(blank_brand + _fixture_bytes())
        self.assertEqual(len(rows), 19)

    def test_empty_file_yields_no_rows(self):
        self.assertEqual(ntw.parse_feed_content(b""), [])
        self.assertEqual(ntw.parse_feed_content(b"\n\n"), [])

    def test_latin1_bytes_do_not_raise(self):
        # The feed has been clean ASCII so far; a stray byte must not abort a whole ingest.
        content = b"0037746990,3289,Pirelli P Zero\xe2,205/55R16,1.000,PASSENGER,205,55,16,99.99,P Zero,Pirelli,99999,PIRE-99999\n"
        rows = ntw.parse_feed_content(content)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["brand_name"], "Pirelli")


class ProductTypeTests(SimpleTestCase):
    databases = []

    def test_every_category_in_the_fixture_classifies_as_a_tire(self):
        for row in ntw.parse_feed_content(_fixture_bytes()):
            verdict = pt.classify_ntw(row["category"])
            self.assertIsNotNone(verdict, row["category"])
            self.assertEqual(verdict.product_type, pt.TIRE)
            self.assertEqual(verdict.source, "ntw:category")

    def test_categories_that_do_not_read_like_tires_still_are(self):
        for category in ("SPECIALTY", "FARM", "INDUSTRIAL", "OFF THE ROAD TIRE"):
            self.assertEqual(pt.classify_ntw(category).product_type, pt.TIRE)

    def test_an_unknown_category_yields_no_verdict(self):
        # The closed vocabulary is the point: the day NTW ships wheels through this feed we want
        # silence, not a tire verdict.
        self.assertIsNone(pt.classify_ntw("WHEELS"))
        self.assertIsNone(pt.classify_ntw("TPMS SENSOR"))
        self.assertIsNone(pt.classify_ntw(""))
        self.assertIsNone(pt.classify_ntw(None))


class ProviderExternalIdTests(SimpleTestCase):
    databases = []

    def test_key_is_brand_scoped(self):
        from src.integrations.services import master_parts

        self.assertEqual(master_parts._ntw_provider_external_id(7, "08404"), "7_08404")
        # Same part number under two feed brands must not collide.
        self.assertNotEqual(
            master_parts._ntw_provider_external_id(7, "08404"),
            master_parts._ntw_provider_external_id(8, "08404"),
        )

    def test_part_number_is_stripped_but_not_normalized(self):
        from src.integrations.services import master_parts

        # Leading zeros are significant in NTW part numbers ("08404"), so nothing may strip them.
        self.assertEqual(master_parts._ntw_provider_external_id(1, " 08404 "), "1_08404")
