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
        values = [v["value"] for v in by_field["diameter_in"]["values"]]
        self.assertEqual(values, [18, 20])
        self.assertIsInstance(values[0], int)
        self.assertEqual(by_field["finish_family"]["values"][0]["value"], "black")

    def test_a_measurement_is_ordered_numerically_and_a_vocabulary_by_count(self):
        """18 before 20 regardless of which is commoner -- a size list that jumps around by
        popularity is unreadable. A finish list is the opposite: commonest first."""
        shaped = wheel_search._shape_facets(
            {"diameter_in": {"22": 99, "17": 1, "20": 50}, "finish_family": {"bronze": 2, "black": 90}}
        )
        by_field = {facet["field"]: facet for facet in shaped}
        self.assertEqual([v["value"] for v in by_field["diameter_in"]["values"]], [17, 20, 22])
        self.assertEqual([v["value"] for v in by_field["finish_family"]["values"]], ["black", "bronze"])

    def test_every_configured_facet_is_a_known_filter(self):
        for facet in wheel_search.FACETS_CONFIG:
            self.assertIn(facet["field"], wheel_search.FILTER_FIELDS, facet["field"])


class FacetContractTests(SimpleTestCase):
    """The shape the client renders from. It must match the tire rail key for key, because one
    component draws both."""

    databases = []

    REQUIRED_CONFIG_KEYS = {
        "field",
        "label",
        "widget",
        "unit",
        "collapse_after",
        "value_labels",
        "value_order",
        "value_sequence",
        "min_distinct_values",
        "requires_filter_on",
        "requires_true_value",
    }

    def test_config_matches_the_tire_rail_key_for_key(self):
        """REQUIRED_CONFIG_KEYS is the tire rail's key set, copied here rather than read from it:
        tire_search.facets_config() loads from the facet_config table and this suite runs without
        a database. If the tire rail grows a key, this test is the thing that should fail."""
        for facet in wheel_search.facets_config():
            self.assertEqual(set(facet), self.REQUIRED_CONFIG_KEYS, facet["field"])

    def test_every_facet_declares_a_widget(self):
        """Without it the client cannot tell a slider from a checkbox list -- the gap that blocked
        the UI."""
        for facet in wheel_search.facets_config():
            self.assertIn(facet["widget"], ("multiselect", "range", "toggle"), facet["field"])

    def test_measurements_carry_a_unit(self):
        units = {f["field"]: f["unit"] for f in wheel_search.facets_config()}
        self.assertEqual(units["diameter_in"], "in")
        self.assertEqual(units["offset_mm"], "mm")
        self.assertEqual(units["load_rating_lb"], "lb")

    def test_every_numeric_facet_carries_stats_whatever_its_widget(self):
        """A slider needs min/max; a multiselect does not. Sending it either way means switching
        a widget is a config change with no backend work."""
        shaped = wheel_search._shape_facets(
            {"diameter_in": {"20": 5, "18": 3}, "offset_mm": {"-12": 4, "35": 9}},
            stats={"offset_mm": {"min": -76, "max": 60}},
        )
        by_field = {facet["field"]: facet for facet in shaped}
        self.assertEqual(by_field["offset_mm"]["stats"], {"min": -76.0, "max": 60.0})
        # No engine stats for diameter: derived from the distribution rather than omitted.
        self.assertEqual(by_field["diameter_in"]["stats"], {"min": 18.0, "max": 20.0})

    def test_a_string_facet_has_no_stats(self):
        shaped = wheel_search._shape_facets({"finish_family": {"black": 9}})
        self.assertNotIn("stats", shaped[0])

    def test_a_facet_that_cannot_narrow_anything_is_omitted(self):
        """One value is a dead control: clicking it changes nothing. Construction holds only
        'forged' today because no feed publishes it yet."""
        shaped = wheel_search._shape_facets({"construction": {"forged": 459}})
        self.assertEqual(shaped, [])

    def test_coded_values_are_labelled(self):
        shaped = wheel_search._shape_facets({"finish_family": {"gunmetal": 5}})
        self.assertEqual(shaped[0]["values"][0]["label"], "Gunmetal")
