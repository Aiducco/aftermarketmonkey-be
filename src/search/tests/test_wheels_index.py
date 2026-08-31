"""
Tests for the wheels index projection.

The assertions that matter are about *types* and *arrays*. A dimension that reaches Meilisearch as
a string or a Decimal makes its facet unclickable -- it renders, the user clicks it, and nothing
matches -- and a bolt pattern held as a scalar hides multi-fit wheels from the very customers they
fit.
"""
import datetime
import decimal

from django.test import SimpleTestCase

from src.search import wheels_index


def _row(**overrides):
    row = {
        "id": 1,
        "brand_id": 7,
        "brand_name": "KMC",
        "part_number": "KM10029050318",
        "sku": "KM10029050318",
        "gtin": "0123456789012",
        "image_url": "https://example.test/a.png",
        "model_name": "KM100 SYNC",
        "sub_model": None,
        "style_number": "KM100",
        "size_display": "20x9",
        "diameter_in": decimal.Decimal("20.00"),
        "width_in": decimal.Decimal("9.00"),
        "bolt_lug_count": 6,
        "bolt_circle_mm": decimal.Decimal("135.00"),
        "bolt_pattern_display": "6x135",
        "bolt_lug_count_2": 6,
        "bolt_circle_mm_2": decimal.Decimal("139.70"),
        "bolt_pattern_2_display": "6x5.5",
        "is_blank_drilled": False,
        "offset_mm": 18,
        "backspacing_in": None,
        "center_bore_mm": decimal.Decimal("106.10"),
        "load_rating_lb": 2500,
        "weight_lb": None,
        "finish": "SATIN BLACK",
        "finish_family": "black",
        "construction": None,
        "material": None,
        "vehicle_class": None,
        "is_beadlock": None,
        "is_dually": None,
        "tpms_compatible": None,
        "lug_seat": None,
        "search_aliases": [],
        "size_disputed": False,
        "in_stock": True,
        "available_qty": 4,
        "distributor_ids": [6],
        "distributor_names": ["Wheel Pros"],
        "updated_at": datetime.datetime(2026, 8, 31, 12, 0, 0),
    }
    row.update(overrides)
    return row


class ProjectionTypeTests(SimpleTestCase):
    databases = []

    def test_dimensions_are_floats(self):
        """Meilisearch will not coerce: ``diameter_in = 20`` matches nothing against "20.00"."""
        doc = wheels_index.project_wheel(_row())
        for field in ("diameter_in", "width_in", "center_bore_mm"):
            self.assertIsInstance(doc[field], float, field)

    def test_every_numeric_filterable_really_is_numeric(self):
        """Pins NUMERIC_FILTERABLE against the projection so the list cannot rot."""
        doc = wheels_index.project_wheel(_row())
        for field in wheels_index.NUMERIC_FILTERABLE:
            if field not in doc or doc[field] is None:
                continue
            values = doc[field] if isinstance(doc[field], list) else [doc[field]]
            for value in values:
                self.assertIsInstance(value, (int, float), "{} -> {!r}".format(field, value))
                self.assertNotIsInstance(value, bool, field)

    def test_booleans_are_booleans(self):
        doc = wheels_index.project_wheel(_row(is_beadlock=True))
        for field in wheels_index.BOOLEAN_FILTERABLE:
            if field in doc:
                self.assertIsInstance(doc[field], bool, field)


class BoltPatternArrayTests(SimpleTestCase):
    databases = []

    def test_both_drillings_are_indexed(self):
        """A multi-fit wheel fits either hub. Held as a scalar, a filter on the second pattern
        would hide a wheel that physically bolts to the customer's car."""
        doc = wheels_index.project_wheel(_row())
        self.assertEqual(doc["bolt_patterns"], ["6x135", "6x5.5"])
        self.assertEqual(doc["bolt_circles_mm"], [135.0, 139.7])
        self.assertEqual(doc["bolt_lug_counts"], [6, 6])

    def test_a_single_pattern_is_still_an_array(self):
        doc = wheels_index.project_wheel(
            _row(bolt_lug_count_2=None, bolt_circle_mm_2=None, bolt_pattern_2_display=None)
        )
        self.assertEqual(doc["bolt_circles_mm"], [135.0])

    def test_an_undrilled_wheel_has_no_pattern_but_says_so(self):
        doc = wheels_index.project_wheel(
            _row(
                bolt_lug_count=None,
                bolt_circle_mm=None,
                bolt_pattern_display=None,
                bolt_lug_count_2=None,
                bolt_circle_mm_2=None,
                bolt_pattern_2_display=None,
                is_blank_drilled=True,
            )
        )
        self.assertEqual(doc["bolt_circles_mm"], [])
        self.assertIs(doc["is_blank_drilled"], True)


class TriStateTests(SimpleTestCase):
    databases = []

    def test_unknown_flags_are_omitted_not_false(self):
        """A false would make the beadlock facet silently exclude every wheel whose feed never
        mentioned it -- which reads as missing inventory, not as a data gap."""
        doc = wheels_index.project_wheel(_row())
        for flag in wheels_index._TRISTATE_FLAGS:
            self.assertNotIn(flag, doc)

    def test_a_known_flag_is_present(self):
        doc = wheels_index.project_wheel(_row(is_beadlock=True, is_dually=False))
        self.assertIs(doc["is_beadlock"], True)
        self.assertIs(doc["is_dually"], False)

    def test_blank_drilled_is_not_tri_state(self):
        """It is non-null with a real default: an undrilled wheel must always be excludable."""
        self.assertNotIn("is_blank_drilled", wheels_index._TRISTATE_FLAGS)
        self.assertIn("is_blank_drilled", wheels_index.project_wheel(_row()))


class SafetyTests(SimpleTestCase):
    databases = []

    def test_refuses_to_target_another_index(self):
        from src.search import meilisearch_client as parts_index

        for name in (parts_index.INDEX_NAME, parts_index.INDEX_NAME_VEHICLES, "tires_v1"):
            with self.assertRaises(RuntimeError, msg=name):
                wheels_index._assert_not_a_shared_index(name)

    def test_its_own_name_is_allowed(self):
        wheels_index._assert_not_a_shared_index(wheels_index.INDEX_NAME_WHEELS)

    def test_placeholder_image_urls_are_dropped(self):
        """536,041 master parts carry a literal "NA" in image_url, which is truthy in Python."""
        self.assertEqual(wheels_index.project_wheel(_row(image_url="NA"))["image_url"], "")


class FitmentTextTests(SimpleTestCase):
    """
    The safety net under the query parser.

    A size or a pattern normally becomes a filter and never reaches the text engine. When the
    parser does not recognise a shape the raw string is still sent as ``q``, and this is what gives
    it something to hit instead of returning an empty page.
    """

    databases = []

    def test_it_carries_the_size_and_every_pattern(self):
        text = wheels_index.project_wheel(_row())["fitment_text"]
        self.assertIn("20x9", text)
        self.assertIn("6x135", text)
        self.assertIn("6x5.5", text)

    def test_both_spellings_of_a_pattern_are_present(self):
        """The feed published one of "6x5.5" and "6x139.7"; a customer may type either."""
        text = wheels_index.project_wheel(_row())["fitment_text"]
        self.assertIn("6x5.5", text)
        self.assertIn("6x139.7", text)

    def test_no_duplicates(self):
        text = wheels_index.project_wheel(
            _row(
                bolt_pattern_display="6x135", bolt_lug_count_2=None, bolt_circle_mm_2=None, bolt_pattern_2_display=None
            )
        )["fitment_text"]
        self.assertEqual(text.count("6x135"), 1)

    def test_typo_tolerance_is_off_for_it(self):
        """ "6x135" is five characters, which Meilisearch matches to "6x139" with one typo allowed.
        Those are different bolt patterns on a part that bolts to a car."""
        self.assertIn("fitment_text", wheels_index.SPEC.typo_disabled)

    def test_it_is_searchable(self):
        self.assertIn("fitment_text", wheels_index.SEARCHABLE_ATTRIBUTES)
