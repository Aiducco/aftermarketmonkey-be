"""Tests for ``src.domain.tire_query``. Pure module -- brands and categories are injected."""
from django.test import SimpleTestCase

from src.domain import tire_query

BRANDS = frozenset(["NITTO", "BFGOODRICH", "MICKEY THOMPSON", "TOYO"])
CATEGORIES = frozenset(["MT", "AT", "RT", "HT", "XT", "WINTER", "ALL_SEASON", "ALL_WEATHER", "UHP", "TRACK", "SAND"])


def parse(text):
    return tire_query.parse_query(text, brand_names=BRANDS, valid_categories=CATEGORIES)


class SizeExtractionTests(SimpleTestCase):
    databases = []

    def test_plain_metric_size(self):
        result = parse("275/70R18")
        self.assertEqual(result.filters, {"section_width_mm": 275, "aspect_ratio": 70, "rim_diameter_in": 18.0})
        self.assertEqual(result.residue, "")

    def test_service_type_is_not_filtered_on(self):
        # LT275/70R18 and 275/70R18 are different tires, but distributors disagree about the
        # prefix constantly, so filtering on it hides inventory the shopper wants to see.
        self.assertNotIn("service_type", parse("LT275/70R18").filters)

    def test_flotation_filters_on_inch_fields(self):
        result = parse("35x12.50R20")
        self.assertEqual(
            result.filters,
            {"section_width_in": 12.5, "overall_diameter_in": 35.0, "rim_diameter_in": 20.0},
        )

    def test_loose_size_inside_a_longer_query(self):
        # Three separate tokens, with words after them.
        result = parse("275 70 18 mud terrain")
        self.assertEqual(result.filters["section_width_mm"], 275)
        self.assertEqual(result.filters["tread_category"], "MT")
        self.assertEqual(result.residue, "")

    def test_separator_free_size_beside_a_brand(self):
        result = parse("2757018 nitto")
        self.assertEqual(result.filters["rim_diameter_in"], 18.0)
        self.assertEqual(result.filters["brand_name"], "NITTO")
        self.assertEqual(result.residue, "")

    def test_assumed_aspect_ratio_is_not_filtered_on(self):
        # "275R18" assumes 82-series; filtering on the guess would exclude the 45- and 70-series
        # tires the shopper most likely wants.
        result = parse("275R18")
        self.assertIn("section_width_mm", result.filters)
        self.assertNotIn("aspect_ratio", result.filters)


class SignalExtractionTests(SimpleTestCase):
    databases = []

    def test_tread_category_synonyms(self):
        self.assertEqual(parse("mud terrain").filters["tread_category"], "MT")
        self.assertEqual(parse("a/t tires").filters["tread_category"], "AT")

    def test_longest_phrase_wins(self):
        # "all terrain" must not be shadowed by a shorter overlapping synonym.
        self.assertEqual(parse("all terrain").filters["tread_category"], "AT")

    def test_category_outside_the_supplied_vocabulary_is_ignored(self):
        result = tire_query.parse_query("paddle", brand_names=BRANDS, valid_categories=frozenset(["MT"]))
        self.assertNotIn("tread_category", result.filters)

    def test_severe_snow_intent(self):
        self.assertTrue(parse("275/70R18 3pmsf").filters["is_3pmsf"])
        self.assertTrue(parse("severe snow tires").filters["is_3pmsf"])

    def test_load_range_text_is_consumed_not_left_in_the_residue(self):
        # Otherwise "load range E" gets text-searched against model names and matches nothing.
        result = parse("LT265/70R17 load range E")
        self.assertEqual(result.filters["load_range"], "E")
        self.assertEqual(result.residue, "")

    def test_ply_rating(self):
        self.assertEqual(parse("10 ply").filters["ply_rating"], 10)

    def test_brand_and_leftover_model_text(self):
        result = parse("nitto ridge grappler 275/70R18")
        self.assertEqual(result.filters["brand_name"], "NITTO")
        self.assertEqual(result.residue, "ridge grappler")

    def test_longest_brand_wins(self):
        self.assertEqual(parse("mickey thompson baja").filters["brand_name"], "MICKEY THOMPSON")

    def test_unrecognised_text_stays_in_the_residue(self):
        result = parse("ridge grappler")
        self.assertEqual(result.filters, {})
        self.assertEqual(result.residue, "ridge grappler")
        self.assertFalse(result.parsed_anything)

    def test_empty_query(self):
        self.assertEqual(parse("").filters, {})
        self.assertEqual(tire_query.parse_query(None).filters, {})


class RelaxationTests(SimpleTestCase):
    databases = []

    def test_drops_the_least_important_filter_first(self):
        filters = {"section_width_mm": 275, "tread_category": "MT", "brand_name": "NITTO"}
        dropped, reduced = tire_query.relax(filters)
        self.assertEqual(dropped, "brand_name")
        self.assertNotIn("brand_name", reduced)
        self.assertIn("tread_category", reduced)

    def test_never_relaxes_a_dimension(self):
        # Returning a 265/65R17 to someone who asked for a 275/70R18 is worse than returning
        # nothing: it looks like a match and fits nothing.
        for field in tire_query.NEVER_RELAX:
            self.assertNotIn(field, tire_query.RELAXATION_ORDER, field)

    def test_nothing_left_to_relax(self):
        self.assertIsNone(tire_query.relax({"section_width_mm": 275, "rim_diameter_in": 18.0}))

    def test_original_filters_are_not_mutated(self):
        filters = {"section_width_mm": 275, "brand_name": "NITTO"}
        tire_query.relax(filters)
        self.assertIn("brand_name", filters)
