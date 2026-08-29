"""
Tests for ``src.integrations.services.tdg``.

Pure logic only -- no network, no database. Two things are worth pinning down:

* the scalar parsers, because TDG sends every specification as a display string and each typed
  column on ``tdg_products`` is a guess at that string's shape; and
* the per-type dispatch, because six product types share one table and one ``specifications``
  key (``type``) means two different things depending on the row -- putting a wheel's finish on a
  tire, or reading a lug kit's Nut/Bolt as a wheel's Alloy/Steel, is the failure mode this table
  is exposed to.
"""
import decimal

from django.test import SimpleTestCase

from src.integrations.clients.tdg import client as tdg_client
from src.integrations.clients.tdg import exceptions as tdg_exceptions
from src.integrations.services import tdg
from src.models import TdgProduct


def _tire(**specs):
    """A tire product trimmed to what the mapper reads, with TDG's own field spellings."""
    base = {
        "size": "2355517, P235/55R17 XL",
        "sidewall": "Black Sidewall",
        "loadIndex": "103",
        "speedRating": "W",
        "serviceDescription": "103W",
        "loadRange": "XL",
        "warrantyMileage": "70000",
        "serviceType": "Passenger",
        "wheelDiameter": "17",
        "outsideDiameter": "27.18",
        "tireSizeType": "Metric",
        "season": "All Season",
        "runFlat": "No",
        "eVOptimized": "No",
        "uTQG": "400 A A",
        "tireType": "AS - All Season Tire",
        "treadDepth32nds": "11",
        "wheelWidthMin": "6.5",
        "wheelWidthMax": "8.5",
    }
    base.update(specs)
    return {
        "brandId": 583,
        "brandName": "BF Goodrich",
        "productId": 182326,
        "productName": "G Force Comp2 AS Plus",
        "type": "Tire",
        "id": 1475034,
        "gtin": "086699174888",
        "itemNumber": "BF-17488",
        "partNumber": "17488",
        "isInactive": False,
        "isAvailabilityRestricted": False,
        "specifications": base,
        "productImageUrl": "https://api.ridestyler.net/Resource/Download/public_abc.png?width=800",
    }


class ScalarParsingTests(SimpleTestCase):
    def test_numbers_arrive_as_strings(self):
        self.assertEqual(tdg._as_int("103"), 103)
        self.assertEqual(tdg._as_int("-12"), -12)
        self.assertEqual(tdg._as_int("2,500"), 2500)
        self.assertEqual(tdg._as_decimal("8.5"), decimal.Decimal("8.5"))

    def test_blank_and_unparseable_are_none_not_zero(self):
        for value in (None, "", "   ", "N/A", "--"):
            self.assertIsNone(tdg._as_int(value), value)
            self.assertIsNone(tdg._as_decimal(value), value)

    def test_blank_text_is_none_but_a_non_numeric_string_survives(self):
        for value in (None, "", "   "):
            self.assertIsNone(tdg._as_text(value), value)
        # _as_text is not a number parser: 'N/A' is a value TDG published and columns like
        # spec_mileage_warranty are supposed to keep it verbatim.
        self.assertEqual(tdg._as_text("N/A"), "N/A")

    def test_yes_no_maps_to_bool_and_anything_else_to_none(self):
        self.assertIs(tdg._as_bool("Yes"), True)
        self.assertIs(tdg._as_bool("No"), False)
        # Unknown must stay distinguishable from "no": every boolean here is a claim.
        self.assertIsNone(tdg._as_bool("Unspecified"))
        self.assertIsNone(tdg._as_bool(None))

    def test_text_is_truncated_to_the_column_width(self):
        self.assertEqual(tdg._as_text("x" * 100, max_length=8), "x" * 8)


class SizeSplittingTests(SimpleTestCase):
    def test_composite_size_splits_into_code_and_printed_form(self):
        raw, code, display = tdg._split_size("2355517, P235/55R17 XL")
        self.assertEqual(raw, "2355517, P235/55R17 XL")
        self.assertEqual(code, "2355517")
        self.assertEqual(display, "P235/55R17 XL")

    def test_printed_size_alone_is_kept_as_the_display_half(self):
        _, code, display = tdg._split_size("LT285/75R16")
        self.assertIsNone(code)
        self.assertEqual(display, "LT285/75R16")

    def test_code_alone_does_not_become_a_display_size(self):
        # A digits-only string is a size code, never something to hand to a size parser.
        _, code, display = tdg._split_size("2355517")
        self.assertEqual(code, "2355517")
        self.assertIsNone(display)

    def test_flotation_size_survives_the_split(self):
        _, _, display = tdg._split_size("33X12.50R20LT")
        self.assertEqual(display, "33X12.50R20LT")

    def test_missing_size_is_all_none(self):
        self.assertEqual(tdg._split_size(None), (None, None, None))


class UtqgTests(SimpleTestCase):
    def test_spaced_grade_splits_into_three_components(self):
        raw, treadwear, traction, temperature = tdg._split_utqg("400 A A")
        self.assertEqual(raw, "400 A A")
        self.assertEqual(treadwear, 400)
        self.assertEqual(traction, "A")
        self.assertEqual(temperature, "A")

    def test_unspaced_grade_and_two_letter_traction(self):
        _, treadwear, traction, temperature = tdg._split_utqg("460AA")
        self.assertEqual((treadwear, traction, temperature), (460, "AA", None))

    def test_hyphenated_grade_is_the_same_grade(self):
        # TDG spells one grade three ways; '340-AA-A' cost ~320 rows their treadwear before this.
        self.assertEqual(tdg._split_utqg("340-AA-A")[1:], (340, "AA", "A"))
        self.assertEqual(tdg._split_utqg("500-A-A")[1:], (500, "A", "A"))

    def test_zero_treadwear_is_ungraded_not_a_grade_of_zero(self):
        for value in ("0", "0 0 0"):
            raw, treadwear, traction, temperature = tdg._split_utqg(value)
            self.assertEqual(raw, value)
            self.assertEqual((treadwear, traction, temperature), (None, None, None), value)

    def test_unrecognised_grade_keeps_the_raw_string_and_nulls_the_rest(self):
        raw, treadwear, traction, temperature = tdg._split_utqg("N/A")
        self.assertEqual(raw, "N/A")
        self.assertEqual((treadwear, traction, temperature), (None, None, None))


class MeasurementTests(SimpleTestCase):
    def test_zero_measurement_is_not_published(self):
        # A 0/32" tread depth or a 0" minimum rim describes nothing that is a tire.
        self.assertIsNone(tdg._as_measurement("0"))
        self.assertIsNone(tdg._as_measurement("0.00"))

    def test_real_measurements_survive(self):
        self.assertEqual(tdg._as_measurement("11"), decimal.Decimal("11"))
        self.assertEqual(tdg._as_measurement("6.5"), decimal.Decimal("6.5"))

    def test_offset_keeps_its_zero(self):
        # _as_decimal, not _as_measurement: ET0 is a real and common wheel offset.
        self.assertEqual(tdg._as_decimal("0"), decimal.Decimal("0"))


class MileageTests(SimpleTestCase):
    def test_plain_and_k_suffixed_mileages(self):
        self.assertEqual(tdg._mileage_to_miles("70000"), 70000)
        self.assertEqual(tdg._mileage_to_miles("65k"), 65000)

    def test_zero_and_missing_are_none(self):
        self.assertIsNone(tdg._mileage_to_miles("0"))
        self.assertIsNone(tdg._mileage_to_miles(None))


class TireMappingTests(SimpleTestCase):
    def test_identity_and_specs_land_on_the_row(self):
        row = tdg.map_product(_tire())
        self.assertEqual(row.tdg_id, 1475034)
        self.assertEqual(row.item_number, "BF-17488")
        self.assertEqual(row.part_number, "17488")
        self.assertEqual(row.product_type, TdgProduct.TYPE_TIRE)
        # productId is the model line, id is the SKU. Transposing them is the easy mistake.
        self.assertEqual(row.product_line_id, 182326)
        self.assertEqual(row.product_line_name, "G Force Comp2 AS Plus")
        self.assertEqual(row.tire_size_display, "P235/55R17 XL")
        self.assertEqual(row.load_index, 103)
        self.assertEqual(row.speed_rating, "W")
        self.assertEqual(row.rim_diameter_in, decimal.Decimal("17"))
        self.assertEqual(row.tread_depth_32nds, decimal.Decimal("11"))
        self.assertEqual(row.utqg_treadwear, 400)
        self.assertEqual(row.warranty_mileage_miles, 70000)
        self.assertIs(row.is_run_flat, False)

    def test_gtin_keeps_its_leading_zero(self):
        # Stored as sent: a GTIN is a string, and stripping the zero breaks the match into
        # master_parts that this table exists to seed.
        self.assertEqual(tdg.map_product(_tire()).gtin, "086699174888")

    def test_3pmsf_is_true_or_unknown_never_false(self):
        self.assertIs(tdg.map_product(_tire(otherAttributeS="3PMS")).is_3pmsf, True)
        # A certification TDG did not assert is unknown, not "uncertified".
        self.assertIsNone(tdg.map_product(_tire()).is_3pmsf)

    def test_zero_tread_depth_is_dropped_rather_than_recorded(self):
        self.assertIsNone(tdg.map_product(_tire(treadDepth32nds="0")).tread_depth_32nds)
        self.assertIsNone(tdg.map_product(_tire(wheelWidthMin="0")).rim_width_min_in)

    def test_wheel_columns_stay_empty_on_a_tire(self):
        row = tdg.map_product(_tire())
        self.assertIsNone(row.bolt_pattern)
        self.assertIsNone(row.offset_mm)
        self.assertIsNone(row.finish)

    def test_raw_payload_is_kept_whole(self):
        product = _tire()
        row = tdg.map_product(product)
        self.assertEqual(row.raw, product)
        # Including keys with no column of their own -- the backfill path.
        self.assertIn("otherAttributeS", tdg.map_product(_tire(otherAttributeS="3PMS")).raw["specifications"])


class OtherProductTypeTests(SimpleTestCase):
    def test_wheel_specs_map_to_the_wheel_block(self):
        row = tdg.map_product(
            {
                "id": 22,
                "type": "Wheel",
                "itemNumber": "MK-1234",
                "brandName": "Mak",
                "specifications": {
                    "diameter": "18",
                    "width": "8.5",
                    "boltPattern": "5x114.3",
                    "offset": "-12",
                    "centerbore": "66.6",
                    "maxLoad": "1763",
                    "type": "Alloy",
                    "finish": "Gloss Black",
                    "material": "Aluminum",
                    "construction": "Cast",
                    "lugSeat": "Conical",
                    "winterApproved": "Yes",
                },
            }
        )
        self.assertEqual(row.product_type, TdgProduct.TYPE_WHEEL)
        self.assertEqual(row.wheel_diameter_in, decimal.Decimal("18"))
        self.assertEqual(row.bolt_pattern, "5x114.3")
        self.assertEqual(row.offset_mm, -12)
        self.assertEqual(row.wheel_type, "Alloy")
        self.assertIs(row.is_winter_approved, True)
        # The tire block must stay untouched -- same table, different type.
        self.assertIsNone(row.tire_size_display)
        self.assertIsNone(row.load_index)

    def test_dual_drilled_bolt_pattern_is_kept_whole(self):
        row = tdg.map_product(
            {"id": 23, "type": "Wheel", "itemNumber": "W-2", "specifications": {"boltPattern": "6x135, 6x139.7", "offset": "0"}}
        )
        self.assertEqual(row.bolt_pattern, "6x135, 6x139.7")
        # ET0 is a real offset and must not be nulled the way a 0 measurement is.
        self.assertEqual(row.offset_mm, 0)

    def test_lug_kit_type_is_nut_or_bolt_not_a_wheel_type(self):
        row = tdg.map_product(
            {
                "id": 33,
                "type": "Lug Kit",
                "itemNumber": "LK-1",
                "specifications": {
                    "thread": '9/16" x 18',
                    "seat": "Conical",
                    "type": "Nut",
                    "style": "Acorn",
                    "endType": "Closed",
                    "finish": "Chrome",
                    "material": "Steel",
                },
            }
        )
        self.assertEqual(row.thread, '9/16" x 18')
        self.assertEqual(row.wheel_type, "Nut")
        self.assertIsNone(row.wheel_diameter_in)

    def test_hub_ring_diameters(self):
        row = tdg.map_product(
            {
                "id": 44,
                "type": "Hub Ring",
                "itemNumber": "HR-1",
                "specifications": {"insideDiameter": "57.1", "overallDiameter": "72.6", "material": "Plastic"},
            }
        )
        self.assertEqual(row.inside_diameter_mm, decimal.Decimal("57.1"))
        self.assertEqual(row.overall_diameter_mm, decimal.Decimal("72.6"))

    def test_generic_product_with_no_specs_still_maps(self):
        row = tdg.map_product({"id": 55, "type": "Generic Product", "itemNumber": "G-1", "specifications": {}})
        self.assertEqual(row.tdg_id, 55)
        self.assertEqual(row.product_type, TdgProduct.TYPE_GENERIC)

    def test_unknown_type_is_written_rather_than_dropped(self):
        # A new TDG category must not vanish from a pull that claims to be complete.
        row = tdg.map_product({"id": 66, "type": "Brake Kit", "itemNumber": "B-1", "specifications": {"foo": "bar"}})
        self.assertIsNotNone(row)
        self.assertEqual(row.product_type, "Brake Kit")
        self.assertEqual(row.raw["specifications"], {"foo": "bar"})

    def test_product_without_an_id_is_skipped(self):
        self.assertIsNone(tdg.map_product({"type": "Tire", "itemNumber": "X-1"}))

    def test_missing_specifications_key_does_not_raise(self):
        row = tdg.map_product({"id": 77, "type": "Tire", "itemNumber": "T-1"})
        self.assertEqual(row.tdg_id, 77)
        self.assertIsNone(row.tire_size_raw)


class ResponseShapeTests(SimpleTestCase):
    def test_bare_array_is_the_documented_shape(self):
        self.assertEqual(tdg_client._as_product_list([{"id": 1}], "u"), [{"id": 1}])

    def test_envelope_is_unwrapped(self):
        self.assertEqual(tdg_client._as_product_list({"products": [{"id": 1}]}, "u"), [{"id": 1}])

    def test_non_dict_entries_are_dropped(self):
        self.assertEqual(tdg_client._as_product_list([{"id": 1}, "junk", None], "u"), [{"id": 1}])

    def test_a_lone_object_raises_rather_than_iterating_its_keys(self):
        with self.assertRaises(tdg_exceptions.TdgRequestError):
            tdg_client._as_product_list({"id": 1, "type": "Tire"}, "u")

    def test_a_string_body_raises(self):
        with self.assertRaises(tdg_exceptions.TdgRequestError):
            tdg_client._as_product_list("not json", "u")


class UpdateFieldsTests(SimpleTestCase):
    def test_every_mapped_column_is_refreshed_on_conflict(self):
        """
        A column missing from UPDATE_FIELDS is written once and then frozen forever -- the upsert
        would silently keep the first pull's value. Guard the list against the model.
        """
        concrete = {
            field.name
            for field in TdgProduct._meta.concrete_fields
            if field.name not in ("id", "tdg_id", "created_at", "updated_at")
        }
        self.assertEqual(concrete - set(tdg.UPDATE_FIELDS), set())
        self.assertEqual(set(tdg.UPDATE_FIELDS) - concrete, set())
