"""Tests for ``src.domain.spec_line``."""
from django.test import SimpleTestCase

from src.domain import spec_line


class SpecLineTests(SimpleTestCase):
    databases = []

    def test_full_line(self):
        self.assertEqual(
            spec_line.build_spec_line(
                {
                    "size_display": "LT275/70R18",
                    "load_index": 116,
                    "speed_rating": "T",
                    "max_load_lb": 2756,
                    "max_speed_mph": 118,
                    "overall_diameter_in": 33.2,
                    "load_range": "E",
                    "ply_rating": 10,
                    "notation": "metric",
                }
            ),
            'LT275/70R18 · 116T (2,756 lb, 118 mph) · 33.2" OD · Load E (10 ply)',
        )

    def test_missing_segments_are_dropped_not_rendered_empty(self):
        self.assertEqual(
            spec_line.build_spec_line({"size_display": "225/45R19", "overall_diameter_in": 27.0, "notation": "metric"}),
            '225/45R19 · 27.0" OD',
        )

    def test_nominal_diameter_is_suppressed_for_numeric_notation(self):
        # A numeric size states no aspect ratio; its diameter is a convention, not a measurement,
        # and showing it beside exact figures would imply a precision it does not have.
        self.assertEqual(
            spec_line.build_spec_line({"size_display": "7.50-16", "overall_diameter_in": 31.0, "notation": "numeric"}),
            "7.50-16",
        )

    def test_code_without_resolved_values(self):
        self.assertEqual(
            spec_line.build_spec_line({"size_display": "35X12.50R17LT", "load_index": 121, "speed_rating": "Q"}),
            "35X12.50R17LT · 121Q",
        )

    def test_load_range_without_ply(self):
        self.assertEqual(
            spec_line.build_spec_line({"size_display": "LT245/75R16", "load_range": "E"}),
            "LT245/75R16 · Load E",
        )

    def test_empty_document(self):
        self.assertEqual(spec_line.build_spec_line({}), "")
