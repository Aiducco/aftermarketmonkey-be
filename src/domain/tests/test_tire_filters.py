"""Tests for ``src.domain.tire_filters``. Pure module, no database."""
from django.test import SimpleTestCase

from src.domain import tire_filters

ALLOWED = frozenset(["rim_diameter_in", "load_range", "is_3pmsf", "in_stock", "brand_name", "overall_diameter_in"])


class BuildFilterTests(SimpleTestCase):
    databases = []

    def test_number_and_string(self):
        self.assertEqual(tire_filters.build_filter({"rim_diameter_in": 18}, ALLOWED), "rim_diameter_in = 18")
        self.assertEqual(tire_filters.build_filter({"load_range": "E"}, ALLOWED), 'load_range = "E"')

    def test_list_is_an_or_within_the_field(self):
        # Picking two load ranges means "either" -- an AND would match nothing.
        self.assertEqual(
            tire_filters.build_filter({"load_range": ["D", "E"]}, ALLOWED),
            '(load_range = "D" OR load_range = "E")',
        )

    def test_single_item_list_needs_no_parentheses(self):
        self.assertEqual(tire_filters.build_filter({"load_range": ["E"]}, ALLOWED), 'load_range = "E"')

    def test_range(self):
        self.assertEqual(
            tire_filters.build_filter({"overall_diameter_in": {"min": 32, "max": 34}}, ALLOWED),
            "overall_diameter_in >= 32 AND overall_diameter_in <= 34",
        )

    def test_open_ended_range(self):
        self.assertEqual(
            tire_filters.build_filter({"overall_diameter_in": {"min": 32}}, ALLOWED), "overall_diameter_in >= 32"
        )

    def test_true_filters_and_false_omits(self):
        self.assertEqual(tire_filters.build_filter({"is_3pmsf": True}, ALLOWED), "is_3pmsf = true")
        # The crucial one: the index omits is_3pmsf when unknown, so "= false" would match almost
        # nothing. An off toggle means "do not filter".
        self.assertEqual(tire_filters.build_filter({"is_3pmsf": False}, ALLOWED), "")

    def test_fields_are_anded_together(self):
        self.assertEqual(
            tire_filters.build_filter({"rim_diameter_in": 18, "load_range": "E"}, ALLOWED),
            'load_range = "E" AND rim_diameter_in = 18',
        )

    def test_unknown_key_raises_rather_than_being_dropped(self):
        # A silently ignored filter shows more results than the user asked for, invisibly.
        with self.assertRaises(tire_filters.UnknownFilterField):
            tire_filters.build_filter({"section_width_mm": 275}, ALLOWED)

    def test_string_values_are_escaped(self):
        # Filter expressions are a query language; an unescaped quote is injection into it.
        self.assertEqual(
            tire_filters.build_filter({"brand_name": 'Ni"tto OR id = 1'}, ALLOWED),
            'brand_name = "Ni\\"tto OR id = 1"',
        )

    def test_backslash_is_escaped_before_the_quote(self):
        self.assertEqual(tire_filters.build_filter({"brand_name": "a\\b"}, ALLOWED), 'brand_name = "a\\\\b"')

    def test_empty_inputs(self):
        self.assertEqual(tire_filters.build_filter(None, ALLOWED), "")
        self.assertEqual(tire_filters.build_filter({}, ALLOWED), "")
        self.assertEqual(tire_filters.build_filter({"load_range": []}, ALLOWED), "")
        self.assertEqual(tire_filters.build_filter({"load_range": None}, ALLOWED), "")

    def test_bad_object_shape_raises(self):
        with self.assertRaises(tire_filters.InvalidFilterValue):
            tire_filters.build_filter({"overall_diameter_in": {"between": [1, 2]}}, ALLOWED)
