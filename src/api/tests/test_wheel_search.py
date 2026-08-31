"""
Tests for wheel search filter compilation and response shaping.

Pure: no Meilisearch, no database. The compiler is where a wrong type or a wrong OR/AND turns into
results the customer did not ask for, which for wheels means parts that do not fit the car.
"""
from django.test import SimpleTestCase

from src.api.services import wheel_search


class CompileFilterTests(SimpleTestCase):
    databases = []

    def test_numbers_are_unquoted(self):
        """A quoted "20" matches nothing against a numeric field."""
        self.assertEqual(wheel_search.compile_filters({"diameter_in": 20}), ["diameter_in = 20"])

    def test_strings_are_quoted(self):
        self.assertEqual(wheel_search.compile_filters({"finish_family": "black"}), ['finish_family = "black"'])

    def test_a_list_is_an_or_within_the_field(self):
        self.assertEqual(
            wheel_search.compile_filters({"finish_family": ["black", "bronze"]}),
            ['(finish_family = "black" OR finish_family = "bronze")'],
        )

    def test_bolt_pattern_filters_the_array_field(self):
        """Filtering the scalar column would hide multi-fit wheels from hubs they really fit."""
        self.assertEqual(wheel_search.compile_filters({"bolt_pattern": "6x5.5"}), ['bolt_patterns = "6x5.5"'])
        self.assertEqual(wheel_search.compile_filters({"bolt_circle_mm": 139.7}), ["bolt_circles_mm = 139.7"])

    def test_ranges(self):
        self.assertEqual(
            wheel_search.compile_filters({"offset_mm": {"gte": -12, "lte": 35}}),
            ["offset_mm >= -12", "offset_mm <= 35"],
        )

    def test_a_range_on_a_string_field_is_refused(self):
        with self.assertRaises(wheel_search.SearchError):
            wheel_search.compile_filters({"finish_family": {"gte": 1}})

    def test_booleans(self):
        self.assertEqual(wheel_search.compile_filters({"in_stock": True}), ["in_stock = true"])
        self.assertEqual(wheel_search.compile_filters({"in_stock": "false"}), ["in_stock = false"])

    def test_an_unknown_field_is_an_error_not_a_silent_drop(self):
        """Ignoring it shows more wheels than the customer asked for with no way to tell."""
        with self.assertRaises(wheel_search.SearchError):
            wheel_search.compile_filters({"tread_category": "MT"})

    def test_empty_values_are_skipped(self):
        self.assertEqual(wheel_search.compile_filters({"finish_family": "", "brand_id": None}), [])

    def test_a_quote_in_a_value_cannot_break_out(self):
        self.assertEqual(wheel_search.compile_filters({"finish_family": 'bl"ack'}), ['finish_family = "bl\\"ack"'])


class ShapingTests(SimpleTestCase):
    databases = []

    def test_spec_line(self):
        line = wheel_search._spec_line(
            {"size_display": "20x9", "bolt_patterns": ["6x135", "6x5.5"], "offset_mm": 18, "center_bore_mm": 106.1}
        )
        self.assertEqual(line, "20x9  6x135 / 6x5.5  +18mm  106.1mm bore")

    def test_spec_line_says_undrilled(self):
        line = wheel_search._spec_line(
            {"size_display": "20x9", "bolt_patterns": [], "is_blank_drilled": True, "offset_mm": 0}
        )
        self.assertIn("undrilled", line)

    def test_zero_offset_is_shown_not_dropped(self):
        self.assertIn("+0mm", wheel_search._spec_line({"size_display": "17x8.5", "offset_mm": 0}))

    def test_facet_values_come_back_as_the_indexed_type(self):
        """Meilisearch returns every facet key as a string. Sent back as one, the client posts a
        quoted value the numeric index will not match."""
        shaped = wheel_search._shape_facets({"diameter_in": {"20": 5, "18": 3}, "finish_family": {"black": 9}})
        by_field = {facet["field"]: facet for facet in shaped}
        self.assertEqual(by_field["diameter_in"]["values"][0]["value"], 20)
        self.assertIsInstance(by_field["diameter_in"]["values"][0]["value"], int)
        self.assertEqual(by_field["finish_family"]["values"][0]["value"], "black")

    def test_every_configured_facet_is_a_known_filter(self):
        for facet in wheel_search.FACETS_CONFIG:
            self.assertIn(facet["field"], wheel_search.FILTER_FIELDS, facet["field"])
