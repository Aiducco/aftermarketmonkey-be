"""
Tests for ``src.search.tires_index``.

Only the pure parts: ``project_tire`` and the guard that keeps this module away from the parts
index. The projection SQL and the Meilisearch calls are exercised by running the command against
a real instance, not here.
"""
import datetime
import decimal

from django.test import SimpleTestCase

from src.search import meilisearch_client as parts_index
from src.search import tires_index


def _row(**overrides):
    """A complete projection row, so a test can override just the field it cares about."""
    row = {
        "id": 1,
        "brand_id": 42,
        "brand_name": "NITTO",
        "part_number": "N205-730",
        "sku": "N205-730",
        "gtin": "840269932199",
        "image_url": "https://example.test/a.jpg",
        "model_name": "Trail Grappler M/T",
        "sub_model": "",
        "size_display": "35X12.50R17LT",
        "notation": "flotation",
        "service_type": "LT",
        "section_width_mm": None,
        "aspect_ratio": None,
        "section_width_in": decimal.Decimal("12.50"),
        "overall_diameter_in": decimal.Decimal("35.0"),
        "rim_diameter_in": decimal.Decimal("17.0"),
        "construction": "R",
        "load_index": 121,
        "load_index_dual": None,
        "max_load_lb": 3197,
        "speed_rating": "Q",
        "max_speed_mph": 99,
        "speed_sort": 20,
        "load_range": "E",
        "ply_rating": 10,
        "tread_category": "MT",
        "tread_category_label": "Mud Terrain",
        "vehicle_class": "light_truck",
        "use_case_tags": ["off-road", "mud"],
        "search_aliases": ["Trail Grappler", "TRAIL GRAP"],
        "tier": "premium",
        "noise_level": "loud",
        "oe_marking": None,
        "is_3pmsf": None,
        "is_ms": True,
        "is_run_flat": None,
        "is_studdable": None,
        "tread_depth_32nds": None,
        "max_psi": None,
        "rim_width_min_in": None,
        "rim_width_max_in": None,
        "utqg_treadwear": None,
        "utqg_traction": None,
        "utqg_temperature": None,
        "in_stock": True,
        "available_qty": 254,
        "distributor_ids": [5, 6, 29],
        "distributor_names": ["Rough Country", "Wheel Pros", "APG Wholesale (Premier)"],
        "updated_at": datetime.datetime(2026, 8, 25, 19, 23, 1),
    }
    row.update(overrides)
    return row


class ProjectTireTests(SimpleTestCase):
    databases = []

    def test_unknown_tristate_flags_are_omitted_not_false(self):
        # The whole reason the severe-snow facet works: a tire of unknown certification must not
        # be indexed as "not certified", or the filter silently hides real inventory.
        document = tires_index.project_tire(_row())
        self.assertNotIn("is_3pmsf", document)
        self.assertNotIn("is_run_flat", document)
        self.assertNotIn("is_studdable", document)
        self.assertIs(document["is_ms"], True)

    def test_known_false_is_kept(self):
        document = tires_index.project_tire(_row(is_3pmsf=False))
        self.assertIs(document["is_3pmsf"], False)

    def test_dimensions_are_floats_not_decimals(self):
        # A Decimal serialises as a string, and "rim_diameter_in = 18" then matches nothing.
        document = tires_index.project_tire(_row())
        for field in ("section_width_in", "overall_diameter_in", "rim_diameter_in"):
            self.assertIsInstance(document[field], float, field)

    def test_float_mangled_gtin_is_repaired(self):
        # 2,674 master parts hold "840269932199.0" from an upstream float round-trip. GTIN is an
        # exact-match lane, so the suffix prevents the match rather than degrading it.
        self.assertEqual(tires_index.project_tire(_row(gtin="840269932199.0"))["gtin"], "840269932199")

    def test_a_real_gtin_ending_in_zero_is_untouched(self):
        self.assertEqual(tires_index.project_tire(_row(gtin="8402699321990"))["gtin"], "8402699321990")

    def test_placeholder_image_url_becomes_empty(self):
        # 536,041 master parts carry the literal string "NA", which is truthy in Python.
        self.assertEqual(tires_index.project_tire(_row(image_url="NA"))["image_url"], "")
        self.assertEqual(tires_index.project_tire(_row(image_url="  "))["image_url"], "")

    def test_search_text_carries_brand_model_and_aliases(self):
        text = tires_index.project_tire(_row())["search_text"]
        self.assertIn("nitto", text)
        self.assertIn("trail grappler", text)
        self.assertIn("trail grap", text)
        self.assertEqual(text, text.lower())

    def test_distributor_count_is_derived_and_nulls_are_dropped(self):
        document = tires_index.project_tire(_row(distributor_ids=[5, None, 6], distributor_names=["A", None]))
        self.assertEqual(document["distributor_ids"], [5, 6])
        self.assertEqual(document["distributor_count"], 2)
        self.assertEqual(document["distributor_names"], ["A"])

    def test_no_price_field_is_ever_indexed(self):
        # Cost is negotiated per company; a single global price would be wrong for every viewer
        # and invisible when it was.
        document = tires_index.project_tire(_row())
        for key in document:
            self.assertNotIn("price", key.lower(), key)
            self.assertNotIn("cost", key.lower(), key)

    def test_oe_markings_are_split_so_each_one_is_selectable(self):
        """281 tires carry two. As one string they would face as a bucket nobody searches for."""
        document = tires_index.project_tire(_row(oe_marking="* - MINI, MO - Mercedes-Benz"))
        self.assertEqual(document["oe_marking"], ["* - MINI", "MO - Mercedes-Benz"])

    def test_a_tire_with_no_oe_marking_carries_an_empty_list(self):
        # Which Meilisearch has no value for -- so the facet counts only homologated tires, which
        # is what makes "hide the facet unless some row has one" work.
        self.assertEqual(tires_index.project_tire(_row(oe_marking=None))["oe_marking"], [])
        self.assertEqual(tires_index.project_tire(_row(oe_marking="  "))["oe_marking"], [])

    def test_every_filterable_attribute_exists_on_the_document(self):
        # A filterable attribute the projection never emits is a facet that silently returns
        # nothing. Tri-state flags are the documented exception.
        document = tires_index.project_tire(_row())
        missing = [
            field
            for field in tires_index.FILTERABLE_ATTRIBUTES
            if field not in document and field not in tires_index._TRISTATE_FLAGS
        ]
        self.assertEqual(missing, [])

    def test_the_declared_numeric_and_boolean_attributes_really_are(self):
        """
        A facet value goes back to the client as the type the index holds, so this list decides
        whether clicking a facet filters anything. It cannot be allowed to drift.
        """
        document = tires_index.project_tire(_row(is_3pmsf=True, is_ms=True, is_run_flat=True, is_studdable=True))
        for field in sorted(tires_index.NUMERIC_FILTERABLE):
            value = document[field]
            sample = value[0] if isinstance(value, list) else value
            if sample is None:
                continue
            self.assertIsInstance(sample, (int, float), field)
            self.assertNotIsInstance(sample, bool, field)
        for field in sorted(tires_index.BOOLEAN_FILTERABLE):
            self.assertIsInstance(document[field], bool, field)

    def test_no_declared_type_is_missing_from_the_filterable_list(self):
        declared = tires_index.NUMERIC_FILTERABLE | tires_index.BOOLEAN_FILTERABLE
        self.assertEqual(sorted(declared - set(tires_index.FILTERABLE_ATTRIBUTES)), [])

    def test_every_sortable_attribute_exists_on_the_document(self):
        document = tires_index.project_tire(_row())
        self.assertEqual([f for f in tires_index.SORTABLE_ATTRIBUTES if f not in document], [])


class SharedIndexGuardTests(SimpleTestCase):
    databases = []

    def test_refuses_to_target_the_parts_index(self):
        # swap_indexes and delete_index are aimed by a string. A bad MEILISEARCH_INDEX_TIRES is
        # the one configuration mistake that could destroy the parts index.
        for name in (
            parts_index.INDEX_NAME,
            parts_index.INDEX_NAME_VEHICLES,
            "{}_staging".format(parts_index.INDEX_NAME),
        ):
            with self.assertRaises(RuntimeError, msg=name):
                tires_index._assert_not_a_shared_index(name)

    def test_allows_its_own_index(self):
        tires_index._assert_not_a_shared_index("tires_v1")
        tires_index._assert_not_a_shared_index("tires_v1_staging")
