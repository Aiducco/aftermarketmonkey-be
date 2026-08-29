"""
Tests for ``src.integrations.services.simpletire``.

Pure logic only -- no network, no database. Two things are worth pinning down:

* the scalar parsers, because every typed column on ``simpletire_skus`` is a guess at the shape of
  a display string SimpleTire wrote for humans, and those shapes drift; and
* the size selection, because ``/api/product-detail`` answers 200 with the *default* size when it
  dislikes the selector, so "wrong row, no error" is the failure mode this table is exposed to.
"""
import decimal

from django.test import SimpleTestCase
from django.utils import timezone

from src.integrations.services import simpletire


def _payload(*, item_id=155650, part_number="ANT8207Z", size="225/65R16", specs=None, sizes=None):
    """A product-detail response trimmed to the blocks the mapper actually reads."""
    return {
        "siteProductLine": {
            "name": "Sierra S6",
            "productLineId": 13168,
            "brandTier": 3,
            "startingPriceInCents": "8999",
            "overview": "A tire.",
            "brand": {"label": "Antares", "image": {"src": "https://images.example/antares.svg"}},
            "assetList": [{"image": {"src": "https://images.example/sierra-s6.jpg"}}],
            "heroBrandColor": "#FE5F10",
        },
        "siteProductLineSizeDetail": {
            "id": item_id,
            "partNumber": part_number,
            "size": size,
            "ProductTypeId": 2,
            "productSubType": "Passenger",
            "productStatus": "ProductStatusAvailable",
            "loadSpeedRating": "100S",
            "quantity": 84,
            "deliveryDays": 3,
            "isRunFlat": False,
            "oversized": False,
            "simpleScore": "7.30",
            "handlDuraScore": "7.60",
            "longevityScore": "7.20",
            "tractionScore": "7.20",
            "priceLabel": "36% off",
            "oversizeFee": 0,
            "fetFee": 0,
            "price": {
                "estimatedRetailPriceInCents": "13999",
                "salePriceInCents": "8999",
                "webPriceInCents": "8999",
            },
            "roadHazard": {"durationLabel": "3 years", "pricePerTireInCents": "2070"},
        },
        "siteProductSpecs": specs
        if specs is not None
        else [
            {"name": "Category", "values": ["All Season"]},
            {"name": "Vehicle", "values": ["Passenger"]},
            {"name": "Load Index", "values": ["1764 lbs (100)"]},
            {"name": "Max Speed", "values": ["112 MPH (S)"]},
            {"name": "Load Range", "values": ["Standard (SL)"]},
            {"name": "UTQG", "values": ["460AA"]},
            {"name": "Tire Weight", "values": ["42.51 lbs"]},
            {"name": "Overall Diameter", "values": ['28.5"']},
            {"name": "Mileage Warranty", "values": ["N/A"]},
            {"name": "Three-Peak Mountain Snowflake (3PMS)", "values": ["No"]},
            {"name": "Low Rolling Resistance", "values": ["Present"]},
        ],
        "siteProductLineAvailableSizeList": sizes
        if sizes is not None
        else [
            {
                "size": "225/65R16",
                "loadRange": "SL",
                "rim": 16,
                "partNumber": "ANT8207Z",
                "siteQueryParams": {"itemId": 155650, "mpn": "ANT8207Z", "tireSize": "225-65rr16"},
            },
            {
                "size": "265/70R18",
                "loadRange": "SL",
                "rim": 18,
                "partNumber": "599",
                "siteQueryParams": {"itemId": 234653, "mpn": "599", "tireSize": "265-70rr18"},
            },
        ],
    }


class ScalarParserTests(SimpleTestCase):
    def test_parse_int_ignores_units_and_separators(self):
        self.assertEqual(simpletire.parse_int("51 PSI"), 51)
        self.assertEqual(simpletire.parse_int("8999"), 8999)
        self.assertEqual(simpletire.parse_int("1,764 lbs"), 1764)
        self.assertEqual(simpletire.parse_int(0), 0)

    def test_parse_int_treats_the_site_s_several_spellings_of_missing_as_none(self):
        for absent in (None, "", "NA", "N/A", "none", "--"):
            self.assertIsNone(simpletire.parse_int(absent), absent)

    def test_parse_decimal_keeps_precision(self):
        self.assertEqual(simpletire.parse_decimal('32.6"'), decimal.Decimal("32.6"))
        self.assertEqual(simpletire.parse_decimal("42.51 lbs"), decimal.Decimal("42.51"))
        self.assertIsNone(simpletire.parse_decimal("NA"))

    def test_parse_bool_returns_none_rather_than_inventing_false(self):
        self.assertIs(simpletire.parse_bool("Yes"), True)
        self.assertIs(simpletire.parse_bool("No"), False)
        # A certification column must not read False just because the string was unfamiliar.
        self.assertIsNone(simpletire.parse_bool("Sometimes"))
        self.assertIsNone(simpletire.parse_bool("N/A"))

    def test_parse_load_index_single_and_dual(self):
        self.assertEqual(simpletire.parse_load_index("2756 lbs (116)"), (116, None, 2756, None))
        self.assertEqual(
            simpletire.parse_load_index("6173 lbs/5842 lbs (144/142)"),
            (144, 142, 6173, 5842),
        )
        self.assertEqual(simpletire.parse_load_index("N/A"), (None, None, None, None))

    def test_parse_max_speed_keeps_multi_character_symbols(self):
        self.assertEqual(simpletire.parse_max_speed("112 MPH (S)"), (112, "S"))
        # ATV/industrial ratings are two characters; truncating to one would corrupt them.
        self.assertEqual(simpletire.parse_max_speed("25 MPH (A8)"), (25, "A8"))

    def test_parse_ply_rating_only_where_stated(self):
        self.assertEqual(simpletire.parse_ply_rating("E (10 Ply)"), 10)
        self.assertIsNone(simpletire.parse_ply_rating("Standard (SL)"))
        self.assertIsNone(simpletire.parse_ply_rating("Extra (XL)"))

    def test_parse_utqg_splits_the_letter_run_greedily(self):
        self.assertEqual(simpletire.parse_utqg("460AA"), ("460AA", 460, "A", "A"))
        # Three letters can only be traction AA + temperature A.
        self.assertEqual(simpletire.parse_utqg("220AAA"), ("220AAA", 220, "AA", "A"))
        self.assertEqual(simpletire.parse_utqg("500AB"), ("500AB", 500, "A", "B"))

    def test_parse_utqg_keeps_the_raw_value_when_the_shape_is_unfamiliar(self):
        raw, treadwear, traction, temperature = simpletire.parse_utqg("300 A B")
        self.assertEqual(raw, "300 A B")
        self.assertEqual(treadwear, 300)
        self.assertIsNone(traction)
        self.assertIsNone(temperature)

    def test_parse_rim_range_handles_a_single_width(self):
        self.assertEqual(
            simpletire.parse_rim_range('7.50-8.25"'),
            (decimal.Decimal("7.50"), decimal.Decimal("8.25")),
        )
        self.assertEqual(simpletire.parse_rim_range('8.25"'), (decimal.Decimal("8.25"), decimal.Decimal("8.25")))
        self.assertEqual(simpletire.parse_rim_range("NA"), (None, None))

    def test_parse_mileage_warranty(self):
        self.assertEqual(simpletire.parse_mileage_warranty("65k"), 65000)
        self.assertEqual(simpletire.parse_mileage_warranty("60,000 miles"), 60000)
        self.assertIsNone(simpletire.parse_mileage_warranty("N/A"))

    def test_parse_studdable_accepts_a_stud_spec_as_yes(self):
        self.assertIs(simpletire.parse_studdable("TSMI #11"), True)
        self.assertIs(simpletire.parse_studdable("No"), False)
        self.assertIsNone(simpletire.parse_studdable("N/A"))


class DirtySourceDataTests(SimpleTestCase):
    """
    SimpleTire's spec sheet is not always the quantity its label claims. These pin the three cases
    the first full crawl actually hit -- all found the hard way, none hypothetical.
    """

    def test_rim_diameter_normalises_tenths_encoded_commercial_sizes(self):
        # 11R24.5 arrives as rim=245. Stored raw, the column would read 245 inches.
        self.assertEqual(simpletire.parse_rim_diameter(245), decimal.Decimal("24.5"))
        self.assertEqual(simpletire.parse_rim_diameter(225), decimal.Decimal("22.5"))
        self.assertEqual(simpletire.parse_rim_diameter(153), decimal.Decimal("15.3"))

    def test_rim_diameter_rescales_industrial_sizes_that_need_more_than_one_divide(self):
        # The encoding's scale is not fixed. Each expectation below is confirmed by the size string
        # SimpleTire ships alongside it (e.g. '18x8.00-12.125' -> 12.125).
        self.assertEqual(simpletire.parse_rim_diameter(12125), decimal.Decimal("12.125"))
        self.assertEqual(simpletire.parse_rim_diameter(1125), decimal.Decimal("11.25"))
        self.assertEqual(simpletire.parse_rim_diameter(915), decimal.Decimal("9.15"))
        self.assertEqual(simpletire.parse_rim_diameter(85), decimal.Decimal("8.5"))

    def test_rim_diameter_leaves_whole_inch_rims_alone(self):
        # Real whole-inch rims top out at 54, so there is a clean gap below the tenths encoding.
        for whole in (15, 16, 22, 54):
            self.assertEqual(simpletire.parse_rim_diameter(whole), decimal.Decimal(whole))

    def test_rim_range_rejects_a_part_number_masquerading_as_a_rim_width(self):
        # Goodyear Competition Eliminator Super Comp publishes 'Rim Range': ['2533"'] -- its own
        # part number. NULL is correct; widening the column would store 2533 as a rim width.
        self.assertEqual(simpletire.parse_rim_range('2533"'), (None, None))

    def test_bounded_inches_nulls_the_implausible_and_keeps_the_real(self):
        self.assertEqual(simpletire.bounded_inches('33.7"', maximum=200, field="diameter"), decimal.Decimal("33.7"))
        self.assertIsNone(simpletire.bounded_inches('2533"', maximum=200, field="diameter"))
        self.assertIsNone(simpletire.bounded_inches("0", maximum=200, field="diameter"))

    def test_a_rejected_spec_still_survives_verbatim_in_specs_map(self):
        # Nulling a column must never lose the evidence.
        payload = _payload(specs=[{"name": "Rim Range", "values": ['2533"']}])
        sku = simpletire.build_sku(
            simpletire.ProductLineRef("goodyear-tires", "competition-eliminator-super-comp"),
            payload=payload,
            size=None,
            scraped_at=timezone.now(),
        )
        self.assertIsNone(sku.spec_rim_width_min_in)
        self.assertEqual(sku.specs_map["Rim Range"], '2533"')


class ProductLineRefTests(SimpleTestCase):
    def test_brand_param_drops_the_url_only_tires_suffix(self):
        self.assertEqual(simpletire.ProductLineRef("antares-tires", "sierra-s6").brand_param, "antares")
        self.assertEqual(simpletire.ProductLineRef("mickey-thompson-tires", "baja-boss").brand_param, "mickey-thompson")

    def test_brand_param_leaves_a_slug_without_the_suffix_alone(self):
        self.assertEqual(simpletire.ProductLineRef("nankang", "ea603").brand_param, "nankang")

    def test_page_url(self):
        self.assertEqual(
            simpletire.ProductLineRef("antares-tires", "sierra-s6").page_url,
            "https://simpletire.com/brands/antares-tires/sierra-s6",
        )


class SizeRefTests(SimpleTestCase):
    def test_sizes_without_an_item_id_are_dropped(self):
        # itemId is the only parameter that actually selects a size; without it a refetch returns
        # the default SKU, which would file one tire's specs under another's part number.
        payload = _payload(sizes=[{"size": "225/65R16", "siteQueryParams": {"mpn": "ANT8207Z"}}])
        self.assertEqual(simpletire._size_refs(payload), [])

    def test_size_refs_carry_what_a_refetch_needs(self):
        refs = simpletire._size_refs(_payload())
        self.assertEqual([ref.item_id for ref in refs], [155650, 234653])
        self.assertEqual(refs[1].part_number, "599")
        self.assertEqual(refs[1].tire_size_slug, "265-70rr18")


class BuildSkuTests(SimpleTestCase):
    def _build(self, **kwargs):
        payload = _payload(**kwargs)
        ref = simpletire.ProductLineRef("antares-tires", "sierra-s6")
        size = simpletire._size_refs(payload)[0] if payload["siteProductLineAvailableSizeList"] else None
        return simpletire.build_sku(ref, payload=payload, size=size, scraped_at=timezone.now())

    def test_identity_and_provenance(self):
        sku = self._build()
        self.assertEqual(sku.item_id, 155650)
        self.assertEqual(sku.part_number, "ANT8207Z")
        self.assertEqual(sku.brand_slug, "antares-tires")
        self.assertEqual(sku.product_line_slug, "sierra-s6")
        self.assertEqual(sku.page_url, "https://simpletire.com/brands/antares-tires/sierra-s6")
        self.assertEqual(sku.tire_size_slug, "225-65rr16")

    def test_prices_stay_integer_cents(self):
        sku = self._build()
        self.assertEqual(sku.estimated_retail_price_cents, 13999)
        self.assertEqual(sku.sale_price_cents, 8999)
        self.assertEqual(sku.road_hazard_price_cents, 2070)
        self.assertEqual(sku.price_label, "36% off")

    def test_specs_are_mapped_onto_typed_columns(self):
        sku = self._build()
        self.assertEqual(sku.spec_category, "All Season")
        self.assertEqual(sku.spec_load_index, 100)
        self.assertEqual(sku.spec_max_load_lb, 1764)
        self.assertEqual(sku.spec_speed_rating, "S")
        self.assertEqual(sku.spec_max_speed_mph, 112)
        self.assertEqual(sku.spec_utqg_treadwear, 460)
        self.assertEqual(sku.spec_tire_weight_lb, decimal.Decimal("42.51"))
        self.assertEqual(sku.spec_overall_diameter_in, decimal.Decimal("28.5"))
        self.assertIs(sku.spec_is_3pmsf, False)
        self.assertIsNone(sku.spec_mileage_warranty_miles)

    def test_specs_without_a_column_stay_queryable_in_specs_map(self):
        sku = self._build()
        self.assertEqual(sku.specs_map["Low Rolling Resistance"], "Present")

    def test_raw_blobs_are_kept_so_a_parser_fix_needs_no_recrawl(self):
        sku = self._build()
        self.assertEqual(sku.raw_size_detail["id"], 155650)
        self.assertEqual(sku.raw_size["siteQueryParams"]["tireSize"], "225-65rr16")
        self.assertTrue(any(spec["name"] == "UTQG" for spec in sku.raw_specs))
        # Hero art is dropped: it is identical on thousands of rows and nothing reads it.
        self.assertNotIn("heroBrandColor", sku.raw_product_line)
        self.assertEqual(sku.raw_product_line["productLineId"], 13168)

    def test_a_payload_without_a_size_detail_id_is_skipped_rather_than_written(self):
        payload = _payload()
        payload["siteProductLineSizeDetail"].pop("id")
        sku = simpletire.build_sku(
            simpletire.ProductLineRef("antares-tires", "sierra-s6"),
            payload=payload,
            size=None,
            scraped_at=timezone.now(),
        )
        self.assertIsNone(sku)

    def test_a_line_with_no_purchasable_sizes_still_yields_its_default_sku(self):
        # Discontinued lines return an empty size list and one out-of-stock detail block. The spec
        # sheet is real data even when the SKU cannot be bought.
        sku = self._build(sizes=[])
        self.assertIsNotNone(sku)
        self.assertEqual(sku.item_id, 155650)
        self.assertIsNone(sku.raw_size)
        # loadRange comes off the size-list entry; with none, fall back to the printed spec.
        self.assertEqual(sku.load_range, "Standard (SL)")


class TokenBucketTests(SimpleTestCase):
    def test_rate_must_be_positive(self):
        with self.assertRaises(ValueError):
            simpletire._TokenBucket(0)
