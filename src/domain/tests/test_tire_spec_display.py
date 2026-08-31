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
        "sub_model": None,
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
        "sidewall_style": "Blackwall",
        "tread_design": "Directional",
        "mileage_warranty_miles": 30000,
        "tire_weight_lb": decimal.Decimal("26.40"),
        "oe_marking": "N0 - Porsche",
        "season_category": "SUMMER",
        "season_category_label": "Summer",
        "spec_source": "simpletire",
        "simpletire_match_tier": 1,
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

    def test_unknown_flags_are_null_never_coerced_to_false(self):
        payload = tire_spec_display.build_tire_specs(_row(is_3pmsf=None, is_ms=None))
        # Present but null means unknown. A false here would claim the tire was checked and failed.
        for flag in tire_spec_display.TRISTATE_FLAGS:
            self.assertIn(flag, payload)
        self.assertIsNone(payload["is_3pmsf"])
        self.assertIsNone(payload["is_ms"])
        self.assertIsNone(payload["is_run_flat"])
        self.assertIsNone(payload["is_tubeless"])
        self.assertIsNone(payload["has_reinforced_sidewall"])

    def test_unknown_text_is_null_never_an_empty_string(self):
        """One token for two states is a bug: NULL is "we never learned it"."""
        payload = tire_spec_display.build_tire_specs(
            _row(service_type=None, load_range=None, tier=None, model_name=None, oe_marking="")
        )
        self.assertIsNone(payload["service_type"])
        self.assertIsNone(payload["load_range"])
        self.assertIsNone(payload["tier"])
        self.assertIsNone(payload["model_name"])
        self.assertIsNone(payload["sub_model"])
        self.assertIsNone(payload["oe_marking"])

    def test_the_catalog_block_ships(self):
        """Fields only a manufacturer-grade catalog supplies -- the ones a buyer filters on."""
        payload = tire_spec_display.build_tire_specs(_row())
        self.assertEqual(payload["sidewall_style"], "Blackwall")
        self.assertEqual(payload["tread_design"], "Directional")
        self.assertEqual(payload["mileage_warranty_miles"], 30000)
        self.assertEqual(payload["tire_weight_lb"], 26.4)
        self.assertEqual(payload["oe_marking"], "N0 - Porsche")
        self.assertIsNone(payload["commercial_position"])

    def test_season_is_a_second_axis_with_its_own_label(self):
        payload = tire_spec_display.build_tire_specs(_row())
        self.assertEqual(payload["season_category"], "SUMMER")
        self.assertEqual(payload["season_category_label"], "Summer")
        # ...and does not displace the terrain/performance answer.
        self.assertEqual(payload["tread_category"], "track")

    def test_provenance_says_who_supplied_the_specs(self):
        payload = tire_spec_display.build_tire_specs(_row())
        self.assertEqual(payload["spec_source"], "simpletire")
        self.assertEqual(payload["spec_source_label"], "SimpleTire catalog")
        self.assertEqual(payload["simpletire_match_tier"], 1)
        self.assertIsNone(payload["tdg_match_tier"])

    def test_revolutions_per_mile_comes_off_the_diameter(self):
        payload = tire_spec_display.build_tire_specs(_row(overall_diameter_in=decimal.Decimal("27.9")))
        self.assertEqual(payload["revolutions_per_mile"], 722.9)

    def test_revolutions_per_mile_ratio_is_the_speedometer_error(self):
        """The number exists to answer "how far off will my speedo read" -- that is a ratio."""
        stock = tire_spec_display.build_tire_specs(_row(overall_diameter_in=decimal.Decimal("30.0")))
        bigger = tire_spec_display.build_tire_specs(_row(overall_diameter_in=decimal.Decimal("33.0")))
        self.assertAlmostEqual(
            stock["revolutions_per_mile"] / bigger["revolutions_per_mile"], 33.0 / 30.0, places=3
        )

    def test_equivalent_sizes_count_is_the_callers_to_supply(self):
        self.assertIsNone(tire_spec_display.build_tire_specs(_row())["equivalent_sizes_count"])
        payload = tire_spec_display.build_tire_specs(_row(), equivalent_sizes_count=14)
        self.assertEqual(payload["equivalent_sizes_count"], 14)

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
        self.assertIsNone(payload["sidewall_style"])
        self.assertIsNone(payload["season_category_label"])
        self.assertIsNone(payload["spec_source_label"])
        self.assertIsNone(payload["is_3pmsf"])
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

    def test_spec_source_labels_cover_every_source_the_model_allows(self):
        """The wording differs (the card says where a spec came from, in a buyer's words); the
        set of codes must not."""
        self.assertEqual(
            set(tire_spec_display.SPEC_SOURCE_LABELS),
            {code for code, _ in src_models.TireSpec.SPEC_SOURCE_CHOICES},
        )

    def test_every_tristate_flag_is_a_nullable_boolean_column(self):
        for flag in tire_spec_display.TRISTATE_FLAGS:
            field = src_models.TireSpec._meta.get_field(flag)
            self.assertTrue(field.null, "{} must stay nullable to mean unknown".format(flag))
