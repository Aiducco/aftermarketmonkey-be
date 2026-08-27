"""Tests for ``src.domain.tire_spec_display``."""
import datetime
import decimal

from django.test import SimpleTestCase

from src import models as src_models
from src.domain import tire_spec_display


def _row(**overrides):
    """A fully enriched metric passenger tire -- 245/40ZR18 97Y XL, the worked example."""
    row = {
        "notation": "metric",
        "size_display": "245/40ZR18",
        "model_name": "Pilot Sport Cup 2",
        "sub_model": "",
        "service_type": "P",
        "section_width_mm": 245,
        "aspect_ratio": 40,
        "section_width_in": None,
        "construction": "ZR",
        "overall_diameter_in": decimal.Decimal("25.7"),
        "rim_diameter_in": decimal.Decimal("18.0"),
        "load_index": 97,
        "load_index_dual": None,
        "max_load_lb": 1609,
        "speed_rating": "Y",
        "max_speed_mph": 186,
        "load_range": "XL",
        "ply_rating": None,
        "tread_category": "track",
        "tread_category_label": "Track / Competition",
        "vehicle_class": "passenger",
        "rim_width_min_in": decimal.Decimal("8.0"),
        "rim_width_max_in": decimal.Decimal("9.5"),
        "is_3pmsf": False,
        "use_case_tags": ["track"],
        "enriched_at": datetime.datetime(2026, 8, 1, 12, 30),
    }
    row.update(overrides)
    return row


class TireSpecDisplayTests(SimpleTestCase):
    databases = []

    def test_codes_ship_with_their_resolved_meaning(self):
        payload = tire_spec_display.build_tire_specs(_row())

        self.assertEqual(payload["service_type"], "P")
        self.assertEqual(payload["service_type_label"], "P-metric")
        self.assertEqual(payload["construction_label"], "ZR radial")
        self.assertEqual(payload["load_range_label"], "Extra load")
        self.assertEqual(payload["vehicle_class_label"], "Passenger")
        self.assertEqual(payload["tread_category_label"], "Track / Competition")
        self.assertEqual(payload["max_load_lb"], 1609)
        self.assertEqual(payload["max_speed_mph"], 186)

    def test_set_of_four_multiplies_the_per_tire_rating(self):
        self.assertEqual(tire_spec_display.build_tire_specs(_row())["set_of_four_max_load_lb"], 6436)

    def test_set_of_four_is_null_when_the_load_index_never_resolved(self):
        payload = tire_spec_display.build_tire_specs(_row(load_index=None, max_load_lb=None))
        self.assertIsNone(payload["set_of_four_max_load_lb"])

    def test_set_of_four_is_withheld_for_a_motorcycle_tire(self):
        """A bike runs two. Four times the per-tire rating is a wrong answer, not a rounded one."""
        payload = tire_spec_display.build_tire_specs(_row(vehicle_class="motorcycle"))
        self.assertIsNone(payload["set_of_four_max_load_lb"])
        self.assertEqual(payload["max_load_lb"], 1609)

    def test_set_of_four_still_applies_to_an_atv(self):
        payload = tire_spec_display.build_tire_specs(_row(vehicle_class="atv_utv"))
        self.assertEqual(payload["set_of_four_max_load_lb"], 6436)

    def test_metric_width_carries_its_inch_equivalent(self):
        payload = tire_spec_display.build_tire_specs(_row(section_width_in=decimal.Decimal("9.65")))
        self.assertEqual(payload["section_width_mm"], 245)
        self.assertEqual(payload["section_width_in"], 9.65)

    def test_inch_width_falls_back_to_the_same_conversion_the_parser_uses(self):
        """A row enriched before the column existed still renders 245 mm (9.65")."""
        payload = tire_spec_display.build_tire_specs(_row(section_width_in=None))
        self.assertEqual(payload["section_width_in"], 9.65)

    def test_flotation_keeps_the_stated_inch_width(self):
        payload = tire_spec_display.build_tire_specs(
            _row(
                notation="flotation",
                size_display="33x12.50R18",
                section_width_mm=None,
                aspect_ratio=None,
                section_width_in=decimal.Decimal("12.50"),
                construction="R",
            )
        )
        self.assertEqual(payload["section_width_in"], 12.5)
        self.assertFalse(payload["overall_diameter_is_nominal"])

    def test_numeric_notation_flags_its_diameter_as_nominal(self):
        payload = tire_spec_display.build_tire_specs(_row(notation="numeric", size_display="7.50-16"))
        self.assertTrue(payload["overall_diameter_is_nominal"])

    def test_known_false_flag_is_returned_as_false(self):
        payload = tire_spec_display.build_tire_specs(_row())
        self.assertIs(payload["is_3pmsf"], False)

    def test_unknown_flags_are_omitted_never_coerced_to_false(self):
        payload = tire_spec_display.build_tire_specs(_row(is_3pmsf=None, is_ms=None))
        # Absent means unknown. A false here would claim the tire was checked and failed.
        self.assertNotIn("is_3pmsf", payload)
        self.assertNotIn("is_ms", payload)
        self.assertNotIn("is_run_flat", payload)

    def test_lt_load_range_letters_have_no_expansion(self):
        payload = tire_spec_display.build_tire_specs(_row(load_range="E", ply_rating=10))
        self.assertEqual(payload["load_range_label"], "Load range E")
        self.assertEqual(payload["ply_rating"], 10)

    def test_sparse_row_still_returns_every_key(self):
        """A barely enriched tire renders a blank card, not a KeyError."""
        payload = tire_spec_display.build_tire_specs(
            {"notation": "metric", "size_display": "225/45R19", "rim_diameter_in": decimal.Decimal("19.0")}
        )
        self.assertEqual(payload["size_display"], "225/45R19")
        self.assertIsNone(payload["max_load_lb"])
        self.assertIsNone(payload["service_type_label"])
        self.assertIsNone(payload["section_width_in"])
        self.assertEqual(payload["use_case_tags"], [])
        self.assertFalse(payload["size_disputed"])
        self.assertIsNone(payload["enriched_at"])

    def test_decimals_and_timestamps_are_json_ready(self):
        payload = tire_spec_display.build_tire_specs(_row())
        self.assertEqual(payload["overall_diameter_in"], 25.7)
        self.assertEqual(payload["rim_width_min_in"], 8.0)
        self.assertEqual(payload["enriched_at"], "2026-08-01T12:30:00")
        self.assertNotIsInstance(payload["rim_diameter_in"], decimal.Decimal)


class VocabularyParityTests(SimpleTestCase):
    """The labels held here mirror the model; a divergence would render a raw code to a customer."""

    databases = []

    def test_vehicle_class_labels_match_the_model_choices(self):
        self.assertEqual(
            tire_spec_display.VEHICLE_CLASS_LABELS,
            dict(src_models.TireSpec.VEHICLE_CLASS_CHOICES),
        )
