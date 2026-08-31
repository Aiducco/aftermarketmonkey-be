"""
Tests for ``src.integrations.brand_data``.

Pure logic -- no network, no database. Three things carry the risk in this department and are what
is pinned down here:

* **the readers**, because every value a manufacturer publishes arrives as a string written for a
  human, and the difference between "not published" and "we could not read it" is the entire
  contract of ``raw_tire_specs``. A parser that returns False for an unreadable 3PMSF column is
  the failure this table exists to prevent;
* **the field map**, because it is the thing a non-author edits, per brand, in production data --
  loose header matching, fallback lists, and an explicitly mapped column outranking a fan-out are
  all things a brand's next spreadsheet will depend on;
* **identity**, because ``derive_external_key`` decides whether re-running a file updates rows or
  doubles them, and ``content_hash`` decides whether an unchanged re-send writes 40,000 rows.
"""
import decimal
import pathlib
import tempfile

from django.test import SimpleTestCase

from src import models as src_models
from src.integrations.brand_data import base, ingest, mapping, normalize
from src.integrations.brand_data import registry as brand_registry
from src.integrations.brand_data.loaders import csv_file


class NormalizeTests(SimpleTestCase):
    def test_blank_shapes_are_absence_not_values(self):
        for blank in ["", "  ", "N/A", "-", "—", "#N/A", "TBD"]:
            self.assertIsNone(normalize.clean(blank), blank)

    def test_boolean_is_tri_state_and_reports_what_it_cannot_read(self):
        self.assertEqual(normalize.boolean("Yes"), (True, None))
        self.assertEqual(normalize.boolean("N"), (False, None))
        self.assertEqual(normalize.boolean(""), (None, None))
        value, warning = normalize.boolean("Severe Snow")
        self.assertIsNone(value)
        self.assertIn("not yes/no", warning)

    def test_tread_depth_keeps_32nds_and_refuses_other_denominators(self):
        self.assertEqual(normalize.tread_depth('11.5/32"')[0], decimal.Decimal("11.5"))
        self.assertEqual(normalize.tread_depth("12/32nds")[0], decimal.Decimal("12"))
        self.assertEqual(normalize.tread_depth("10.4")[0], decimal.Decimal("10.4"))
        value, warning = normalize.tread_depth("11/16")
        self.assertIsNone(value)
        self.assertIn("unexpected units", warning)

    def test_utqg_splits_the_printed_grade(self):
        printed, grades, warning = normalize.utqg("500 A A")
        self.assertEqual(printed, "500 A A")
        self.assertEqual(grades["utqg_treadwear"], 500)
        self.assertEqual(grades["utqg_traction"], "A")
        self.assertEqual(grades["utqg_temperature"], "A")
        self.assertIsNone(warning)
        self.assertEqual(normalize.utqg("620AA")[1]["utqg_treadwear"], 620)

    def test_utqg_keeps_the_string_when_it_cannot_grade_it(self):
        printed, grades, warning = normalize.utqg("N/A per FMVSS")
        self.assertEqual(printed, "N/A per FMVSS")
        self.assertEqual(grades, {})
        self.assertIn("UTQG", warning)

    def test_service_description_reads_the_dual_index(self):
        self.assertEqual(normalize.service_description("116T")[0], {"load_index": 116, "speed_rating": "T"})
        self.assertEqual(
            normalize.service_description("121/118Q")[0],
            {"load_index": 121, "load_index_dual": 118, "speed_rating": "Q"},
        )

    def test_load_range_carries_a_ply_rating_when_the_brand_prints_one(self):
        self.assertEqual(normalize.load_range("E (10 Ply)")[0], {"load_range": "E", "ply_rating": 10})
        self.assertEqual(normalize.load_range("Standard Load")[0], {"load_range": "SL"})
        self.assertEqual(normalize.load_range("XL")[0], {"load_range": "XL"})

    def test_rim_width_range_handles_a_single_width(self):
        self.assertEqual(
            normalize.rim_width_range('6.0" - 8.0"')[0],
            {"rim_width_min_in": decimal.Decimal("6.0"), "rim_width_max_in": decimal.Decimal("8.0")},
        )
        self.assertEqual(
            normalize.rim_width_range('7.5"')[0],
            {"rim_width_min_in": decimal.Decimal("7.5"), "rim_width_max_in": decimal.Decimal("7.5")},
        )

    def test_miles_reads_the_shorthand(self):
        self.assertEqual(normalize.miles("65k")[0], 65000)
        self.assertEqual(normalize.miles("65,000 miles")[0], 65000)
        self.assertIsNone(normalize.miles("N/A")[0])


FIELD_MAP = {
    "brand_name": {"const": "Michelin"},
    "part_number": "Article No.",
    "model_name": ["Pattern", "Tread Pattern"],
    "size_raw": "Size",
    "service_description": "Load/Speed",
    "load_range": "Load Range",
    "utqg": "UTQG",
    "tread_depth_32nds": "Tread Depth",
    "rim_width_range": "Approved Rim Width",
    "measured_rim_width_in": "Measuring Rim",
    "is_3pmsf": "3PMSF",
}

ROW = {
    "Article No.": "12345",
    "Tread Pattern": "Defender LTX M/S",
    "Size": "LT265/70R17 121/118R",
    "Load/Speed": "121/118R",
    "Load Range": "E (10 Ply)",
    "UTQG": "600 A B",
    "Tread Depth": '12/32"',
    "Approved Rim Width": '7.0" - 8.5"',
    "Measuring Rim": '7.5"',
    "3PMSF": "Yes",
    "Marketing Blurb": "Built for the long haul",
}


class MappingTests(SimpleTestCase):
    def test_a_brand_row_maps_to_columns(self):
        row = mapping.map_record(ROW, FIELD_MAP)
        self.assertEqual(row.values["brand_name"], "Michelin")
        self.assertEqual(row.values["part_number"], "12345")
        # The second name in the fallback list is the one this sheet uses.
        self.assertEqual(row.values["model_name"], "Defender LTX M/S")
        self.assertEqual(row.values["load_index"], 121)
        self.assertEqual(row.values["load_index_dual"], 118)
        self.assertEqual(row.values["speed_rating"], "R")
        self.assertEqual(row.values["load_range"], "E")
        self.assertEqual(row.values["ply_rating"], 10)
        self.assertEqual(row.values["utqg_treadwear"], 600)
        self.assertEqual(row.values["tread_depth_32nds"], decimal.Decimal("12"))
        self.assertEqual(row.values["rim_width_min_in"], decimal.Decimal("7.0"))
        self.assertEqual(row.values["rim_width_max_in"], decimal.Decimal("8.5"))
        self.assertEqual(row.values["measured_rim_width_in"], decimal.Decimal("7.5"))
        self.assertIs(row.values["is_3pmsf"], True)
        self.assertEqual(row.warnings, [])

    def test_the_size_is_isolated_and_decoded_without_touching_published_columns(self):
        row = mapping.map_record(ROW, FIELD_MAP)
        self.assertEqual(row.values["size_raw"], "LT265/70R17 121/118R")
        self.assertEqual(row.values["parsed_size"]["rim_diameter_in"], "17")
        self.assertEqual(row.values["parsed_size"]["section_width_mm"], 265)
        self.assertEqual(row.values["parsed_size"]["service_type"], "LT")
        self.assertIn("265/70R17", row.values["size_display"])

    def test_a_size_that_will_not_parse_is_reported_not_dropped(self):
        row = mapping.map_record({**ROW, "Size": "see catalogue"}, FIELD_MAP)
        self.assertIsNone(row.values["parsed_size"])
        self.assertEqual(row.values["size_display"], "see catalogue")
        self.assertTrue(any("size did not parse" in warning for warning in row.warnings))

    def test_headers_match_loosely(self):
        renamed = {"ARTICLE_NO": "12345", "tread pattern": "Pilot Sport 4S", "size": "245/40R18"}
        row = mapping.map_record(
            renamed, {"part_number": "Article No.", "model_name": "Tread Pattern", "size_raw": "Size"}
        )
        self.assertEqual(row.values["part_number"], "12345")
        self.assertEqual(row.values["model_name"], "Pilot Sport 4S")

    def test_an_explicit_column_outranks_a_composite_cell(self):
        field_map = {**FIELD_MAP, "load_index": "Load Index"}
        row = mapping.map_record({**ROW, "Load Index": "126"}, field_map)
        self.assertEqual(row.values["load_index"], 126)
        # ...and the fan-out still supplies what the sheet has no column for.
        self.assertEqual(row.values["load_index_dual"], 118)

    def test_unreadable_values_warn_rather_than_writing_a_default(self):
        row = mapping.map_record({**ROW, "3PMSF": "Severe Snow Rated"}, FIELD_MAP)
        self.assertNotIn("is_3pmsf", row.values)
        self.assertTrue(any(warning.startswith("is_3pmsf") for warning in row.warnings))

    def test_everything_published_is_kept_even_unmapped(self):
        row = mapping.map_record(ROW, FIELD_MAP)
        self.assertEqual(row.attributes["Marketing Blurb"], "Built for the long haul")
        self.assertNotIn("Marketing Blurb", row.used_keys)

    def test_nested_json_flattens_and_leaf_keys_still_resolve(self):
        payload = {"sku": "A1", "specs": {"treadDepth": "10/32", "utqg": "500 A A"}, "sizes": ["A", "B"]}
        row = mapping.map_record(
            payload, {"external_key": "sku", "tread_depth_32nds": "specs.treadDepth", "utqg": "utqg"}
        )
        self.assertEqual(row.values["tread_depth_32nds"], decimal.Decimal("10"))
        self.assertEqual(row.values["utqg"], "500 A A")
        self.assertEqual(row.attributes["sizes"], "A, B")

    def test_a_field_map_naming_a_column_that_does_not_exist_is_refused(self):
        with self.assertRaises(mapping.FieldMapError):
            mapping.validate_field_map({"tread_dept_32nds": "Tread Depth"})
        with self.assertRaisesMessage(mapping.FieldMapError, "derived by ingest"):
            mapping.validate_field_map({"brand_key": "Brand"})


class IdentityTests(SimpleTestCase):
    def test_the_brands_own_number_is_the_key_when_there_is_one(self):
        self.assertEqual(ingest.derive_external_key("SKU-9", {"part_number_key": "12345"}), "SKU-9")
        self.assertEqual(ingest.derive_external_key(None, {"external_key": "SKU-9"}), "SKU-9")

    def test_a_derived_key_is_deterministic_and_marked_as_ours(self):
        values = {"brand_key": "MICHELIN", "part_number_key": "12345", "size_display": "LT265/70R17"}
        first = ingest.derive_external_key(None, values)
        self.assertTrue(first.startswith("d:"))
        self.assertEqual(first, ingest.derive_external_key(None, dict(values)))
        self.assertNotEqual(first, ingest.derive_external_key(None, {**values, "size_display": "LT285/70R17"}))

    def test_a_record_identifying_nothing_has_no_key(self):
        self.assertIsNone(ingest.derive_external_key(None, {}))

    def test_content_hash_moves_only_when_something_did(self):
        values = {column: None for column in ingest.VALUE_COLUMNS}
        values.update({"part_number": "12345", "tread_depth_32nds": decimal.Decimal("12")})
        raw = {"Article No.": "12345"}
        first = ingest.content_hash(values, raw, [])
        self.assertEqual(first, ingest.content_hash(dict(values), dict(raw), []))
        self.assertNotEqual(first, ingest.content_hash({**values, "max_psi": 80}, raw, []))
        # A correction to a field no column maps still has to be recorded.
        self.assertNotEqual(first, ingest.content_hash(values, {**raw, "Notes": "revised"}, []))

    def test_a_row_needs_a_size_or_a_part_number_to_be_worth_keeping(self):
        self.assertFalse(mapping.map_record({"Marketing Blurb": "x"}, FIELD_MAP).identifies_a_tire)
        self.assertTrue(mapping.map_record({"Article No.": "12345"}, FIELD_MAP).identifies_a_tire)


class RegistryTests(SimpleTestCase):
    def test_known_handlers_resolve(self):
        self.assertIn("csv", brand_registry.loader_names())
        self.assertIn("http_json", brand_registry.loader_names())

    def test_a_planned_source_says_so_rather_than_looking_like_a_typo(self):
        source = src_models.TireBrandSource(slug="acme", brand_name="Acme", method="manual", handler="")
        with self.assertRaisesMessage(base.SourceConfigError, "declaration, not a pull"):
            brand_registry.resolve(source)

    def test_an_unknown_handler_lists_the_ones_that_exist(self):
        source = src_models.TireBrandSource(slug="acme", brand_name="Acme", method="csv", handler="xlsx")
        with self.assertRaisesMessage(base.SourceConfigError, "known handlers"):
            brand_registry.resolve(source)


class CsvLoaderTests(SimpleTestCase):
    def _context(self, path, **config):
        source = src_models.TireBrandSource(slug="acme", brand_name="Acme", method="csv", handler="csv")
        return base.LoaderContext(source=source, config={"path": str(path), **config})

    def test_it_reads_rows_and_fingerprints_the_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "acme.csv"
            path.write_text("Article No.,Size,Type\n12345,265/70R17,Tire\n,,\n999,18x9,Wheel\n")
            ctx = self._context(path)
            records = list(csv_file.load(ctx))
            self.assertEqual([record.payload["Article No."] for record in records], ["12345", "999"])
            self.assertEqual(ctx.fingerprint, csv_file.fingerprint(self._context(path)))

    def test_row_filter_keeps_the_sheets_tires_and_drops_the_rest(self):
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "acme.csv"
            path.write_text("Article No.,Size,Type\n12345,265/70R17,Tire\n999,18x9,Wheel\n777,,\n")
            ctx = self._context(path, row_filter={"type": "tire"})
            self.assertEqual([record.payload["Article No."] for record in csv_file.load(ctx)], ["12345"])

    def test_a_missing_file_says_where_files_live(self):
        ctx = self._context("/nonexistent/acme-2026.csv")
        with self.assertRaisesMessage(base.SourceFetchError, "resources/brand_data/"):
            list(csv_file.load(ctx))
