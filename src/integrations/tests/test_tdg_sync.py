"""
Tests for the TDG merge.

TDG is the *second* catalog, so most of what matters here is deference: it must not overwrite a
value SimpleTire supplied, and it must not read the two of its own columns that are wrong. The
load-range reader gets its own class because reusing SimpleTire's silently dropped 24,000 rows --
an unmapped value is a skip, not an error, so the bug was invisible until the counts were read.
"""
from django.test import SimpleTestCase

from src.integrations.services import tdg_sync, tire_catalog, tire_reparse
from src.models import TireSpec


class LoadRangeTests(SimpleTestCase):
    databases = []

    def test_bare_designations(self):
        self.assertEqual(tdg_sync.parse_load_range("XL").code, "XL")
        self.assertEqual(tdg_sync.parse_load_range("SL").code, "SL")
        self.assertEqual(tdg_sync.parse_load_range("LL").code, "LL")

    def test_lr_prefix_is_just_load_range_spelled_out(self):
        self.assertEqual(tdg_sync.parse_load_range("LRE").code, "E")
        self.assertEqual(tdg_sync.parse_load_range("LRC").code, "C")
        self.assertEqual(tdg_sync.parse_load_range("E").code, "E")

    def test_a_ply_count_with_no_letter_is_still_usable(self):
        self.assertEqual(tdg_sync.parse_load_range("24 PLY").ply_rating, 24)
        self.assertEqual(tdg_sync.parse_load_range("2PLY").ply_rating, 2)
        self.assertIsNone(tdg_sync.parse_load_range("24 PLY").code)

    def test_mojibake_survives(self):
        """A handful of rows carry a stray high byte after the designation."""
        self.assertEqual(tdg_sync.parse_load_range("XL\xc2").code, "XL")

    def test_values_we_have_no_code_for_are_reported_not_guessed(self):
        for value in ("3*", "2*", "HL", "NCS KS", "0", "14"):
            reading = tdg_sync.parse_load_range(value)
            self.assertIsNone(reading.code, value)
            self.assertEqual(reading.unmapped, value.upper())

    def test_empty(self):
        self.assertEqual(tdg_sync.parse_load_range(None), (None, None, None))


def _row(**kwargs):
    row = {
        "id": 1,
        "brand_name": "Michelin",
        "part_number": "12345",
        "item_number": "IT12345",
        "tire_size_display": "285/70R17",
        "product_line_name": "Defender LTX M/S",
        "tread_depth_32nds": None,
        "utqg_treadwear": None,
        "utqg_traction": None,
        "utqg_temperature": None,
        "rim_width_min_in": None,
        "rim_width_max_in": None,
        "load_range": None,
        "load_index": None,
        "speed_rating": None,
        "sidewall": None,
        "season": None,
        "tire_type": None,
        "is_run_flat": None,
        "is_3pmsf": None,
        "winter_studding": None,
        "oe_marking": None,
        "product_image_url": None,
        "gtin": None,
    }
    row.update(kwargs)
    return row


class PrecedenceTests(SimpleTestCase):
    databases = []

    def test_tdg_does_not_overwrite_a_simpletire_value(self):
        """SimpleTire has better coverage and finer precision on every shared field, so on a row
        it owns, TDG may only fill gaps."""
        spec = TireSpec(spec_source=TireSpec.SPEC_SOURCE_SIMPLETIRE, utqg_treadwear=500)
        updates = tdg_sync.build_updates(spec, _row(utqg_treadwear=460))
        self.assertNotIn("utqg_treadwear", updates)

    def test_tdg_fills_a_gap_a_simpletire_row_left(self):
        spec = TireSpec(spec_source=TireSpec.SPEC_SOURCE_SIMPLETIRE, utqg_treadwear=None)
        updates = tdg_sync.build_updates(spec, _row(utqg_treadwear=460))
        self.assertEqual(updates["utqg_treadwear"], 460)

    def test_tdg_takes_a_parser_row_outright(self):
        """No better catalog has it, so TDG becomes the source rather than a gap-filler."""
        spec = TireSpec(spec_source=TireSpec.SPEC_SOURCE_PARSER, utqg_treadwear=500)
        updates = tdg_sync.build_updates(spec, _row(utqg_treadwear=460))
        self.assertEqual(updates["utqg_treadwear"], 460)

    def test_tdg_only_fields_are_written_even_on_a_simpletire_row(self):
        """SimpleTire has no column for these at all, so there is nothing to defer to."""
        spec = TireSpec(spec_source=TireSpec.SPEC_SOURCE_SIMPLETIRE)
        updates = tdg_sync.build_updates(spec, _row(oe_marking="N0 - Porsche"))
        self.assertEqual(updates["oe_marking"], "N0 - Porsche")


class FlagTests(SimpleTestCase):
    databases = []

    def test_run_flat_fills_but_does_not_overrule(self):
        """It fills 20,427 of our nulls, which is the win. Where we already have an answer the two
        disagree on 15% of rows and ours came from an explicit RF in the distributor's title."""
        spec = TireSpec(is_run_flat=None)
        self.assertEqual(tdg_sync.build_updates(spec, _row(is_run_flat=True))["is_run_flat"], True)
        spec = TireSpec(is_run_flat=True)
        self.assertNotIn("is_run_flat", tdg_sync.build_updates(spec, _row(is_run_flat=False)))

    def test_run_flat_false_is_a_real_answer_here(self):
        """Unlike SimpleTire's column, which is False on all 58,124 rows and carries no signal."""
        spec = TireSpec(is_run_flat=None)
        self.assertEqual(tdg_sync.build_updates(spec, _row(is_run_flat=False))["is_run_flat"], False)

    def test_3pmsf_is_positive_only(self):
        spec = TireSpec(is_3pmsf=None)
        self.assertEqual(tdg_sync.build_updates(spec, _row(is_3pmsf=True))["is_3pmsf"], True)
        self.assertNotIn("is_3pmsf", tdg_sync.build_updates(spec, _row(is_3pmsf=None)))

    def test_studdable_reads_the_winter_studding_text(self):
        spec = TireSpec(is_studdable=None)
        self.assertEqual(tdg_sync.build_updates(spec, _row(winter_studding="Studdable"))["is_studdable"], True)
        self.assertEqual(tdg_sync.build_updates(spec, _row(winter_studding="Studded"))["is_studdable"], True)
        self.assertNotIn("is_studdable", tdg_sync.build_updates(spec, _row(winter_studding=None)))


class VocabularyTests(SimpleTestCase):
    databases = []

    def test_sidewall_is_normalised_to_one_house_style(self):
        """TDG says 'Black Sidewall' where SimpleTire says 'Blackwall'. One column must not end up
        holding both vocabularies."""
        spec = TireSpec()
        self.assertEqual(tdg_sync.build_updates(spec, _row(sidewall="Black Sidewall"))["sidewall_style"], "Blackwall")
        self.assertEqual(
            tdg_sync.build_updates(spec, _row(sidewall="Outlined White Letters"))["sidewall_style"],
            "Outlined White Lettering",
        )

    def test_season_populates_the_second_axis(self):
        spec = TireSpec()
        self.assertEqual(tdg_sync.build_updates(spec, _row(season="Winter"))["season_category_id"], "WINTER")

    def test_off_road_weight_classes_are_not_a_tread_category(self):
        """tire_type mixes seasons with weight bands like '10 - Off Road Pneumatic 1<15kg', which
        say nothing about tread pattern."""
        spec = TireSpec(tread_category_id=None)
        self.assertNotIn(
            "tread_category_id", tdg_sync.build_updates(spec, _row(tire_type="10 - Off Road Pneumatic 1&lt;15kg"))
        )


class OwnershipTests(SimpleTestCase):
    databases = []

    def test_the_merge_cannot_write_a_field_it_must_not_own(self):
        self.assertEqual(set(tdg_sync.WRITE_FIELDS) & tdg_sync._NEVER_WRITE, set())

    def test_the_kilometre_warranty_is_never_read(self):
        """TDG's warranty_mileage_miles is kilometres: its modal values are 80,000 / 105,000 /
        120,000 against SimpleTire's 50,000 / 60,000, and 80,000 km is 49,710 miles."""
        self.assertIn("mileage_warranty_miles", tdg_sync._NEVER_WRITE)
        self.assertNotIn("mileage_warranty_miles", tdg_sync.WRITE_FIELDS)

    def test_the_empty_max_load_column_is_never_read(self):
        self.assertIn("max_load_lb", tdg_sync._NEVER_WRITE)

    def test_shared_exclusions_are_inherited_not_restated(self):
        self.assertTrue(tire_catalog.NEVER_WRITE <= tdg_sync._NEVER_WRITE)

    def test_reparse_protects_tdg_owned_rows_too(self):
        """Without this a parser fix would revert TDG's load range exactly as it would SimpleTire's."""
        self.assertIn(TireSpec.SPEC_SOURCE_TDG, tire_reparse._CATALOG_SOURCES)
        self.assertIn(TireSpec.SPEC_SOURCE_SIMPLETIRE, tire_reparse._CATALOG_SOURCES)


class MatchTests(SimpleTestCase):
    databases = []

    def _index(self, *rows):
        return tire_catalog.CatalogIndex(
            list(rows),
            brand_field="brand_name",
            part_field="part_number",
            size_field="tire_size_display",
            model_field="product_line_name",
            extra_part_fields=("item_number",),
        )

    def test_the_item_number_is_a_second_valid_identifier(self):
        """A distributor may have filed either identifier as the part number."""
        index = self._index(_row())
        found = index.match(
            brand="MICHELIN",
            part_number="IT12345",
            size_display="285/70R17",
            model_name=None,
            aliases=tdg_sync.BRAND_ALIASES,
        )
        self.assertEqual(found.tier, 1)

    def test_brand_alias_is_applied(self):
        index = self._index(_row(brand_name="Continental"))
        found = index.match(
            brand="CONTINENTAL TIRE",
            part_number="12345",
            size_display="285/70R17",
            model_name=None,
            aliases=tdg_sync.BRAND_ALIASES,
        )
        self.assertEqual(found.tier, 1)

    def test_size_disagreement_rejects_the_match(self):
        index = self._index(_row())
        self.assertIsNone(
            index.match(
                brand="MICHELIN",
                part_number="12345",
                size_display="225/65R16",
                model_name=None,
                aliases=tdg_sync.BRAND_ALIASES,
            )
        )
