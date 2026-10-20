"""
Tests for the S&B workbook parsers.

Every value below is a real one, copied out of S&B's two shared dealer workbooks on 2026-10-16 --
the quirks are the point, because they are what a parser written from the column names alone gets
wrong:

  * Sheets exports entirely-numeric cells as floats, so the filter-wrap SKU ``101010`` arrives as
    ``101010.0``, the UPC ``841372040375`` as ``841372040375.0`` and emissions code ``1`` as
    ``1.0``. Store any of those verbatim and they match nothing.
  * the Year column is five different shapes in one column, including ``1990 - 1991, 1993 - 1996``
    where the gap year is real and must not be flattened away.
  * the same fitment is spelled three ways across the two workbooks, and the legacy miva dump
    enumerates one row per model year where the curated tab gives a range -- which is why
    precedence is per vehicle rather than per year range.
  * Shopify's export is positional: a product's extra image rows carry nothing but a URL, and one
    product can have several variant SKUs that all share its description, imagery and fitment.

All SimpleTestCase with ``databases = []`` -- these are pure functions, so the suite runs without
a database (see the repo's test notes).
"""
from django.test import SimpleTestCase

from src import constants as src_constants
from src.integrations.clients.sb import feed_spec
from src.integrations.services import master_part_enrichment as enrichment
from src.integrations.services import sb as sb_services


class YearRangeParsingTests(SimpleTestCase):
    databases = []

    def test_single_year_arriving_as_a_float(self):
        # Sheets exports 1980 as 1980.0; parsed as a one-year range, not dropped.
        self.assertEqual(sb_services.parse_year_ranges(1980.0), [(1980, 1980)])
        self.assertEqual(sb_services.parse_year_ranges("1980.0"), [(1980, 1980)])

    def test_spaced_and_unspaced_ranges(self):
        # The Fitment tab writes "2011 - 2016"; Shopify's metafield writes "2015-2026".
        self.assertEqual(sb_services.parse_year_ranges("2011 - 2016"), [(2011, 2016)])
        self.assertEqual(sb_services.parse_year_ranges("2015-2026"), [(2015, 2026)])

    def test_gaps_are_preserved_not_flattened(self):
        # The whole reason comma segments stay separate: 1992 is genuinely not covered here, and
        # collapsing this to (1990, 1996) would invent fitment S&B never claimed.
        self.assertEqual(
            sb_services.parse_year_ranges("1990 - 1991, 1993 - 1996"),
            [(1990, 1991), (1993, 1996)],
        )
        self.assertEqual(sb_services.parse_year_ranges("2004, 2006"), [(2004, 2004), (2006, 2006)])

    def test_reversed_range_is_ordered(self):
        self.assertEqual(sb_services.parse_year_ranges("2016 - 2011"), [(2011, 2016)])

    def test_unparseable_and_out_of_range_values_yield_nothing(self):
        # 261 rows of the Fitment tab are blank filler; "TBD" and a stray 3-digit number are the
        # kinds of thing a hand-maintained sheet grows. None of them may become a fitment row.
        for value in (None, "", "-", "TBD", "see notes", "20111", "123", "0000"):
            self.assertEqual(sb_services.parse_year_ranges(value), [], msg=repr(value))


class ShopifyFitmentParsingTests(SimpleTestCase):
    databases = []

    def test_newline_separated_form(self):
        raw = "2018|Textron|Wildcat XX|UTV\n2015-2017|Can-Am|Maverick Turbo|UTV"
        self.assertEqual(
            sb_services.parse_pipe_fitment(raw),
            [
                sb_services.SbFitmentSpec(2018, 2018, "Textron", "Wildcat XX", "UTV"),
                sb_services.SbFitmentSpec(2015, 2017, "Can-Am", "Maverick Turbo", "UTV"),
            ],
        )

    def test_json_array_form_from_the_older_tab(self):
        raw = (
            '["2015-2026|Ford|F-150|2.7L Ecoboost, Gas",'
            '"2015-2026|Ford|F-150 Raptor|3.5L Ecoboost, Gas"]'
        )
        self.assertEqual(
            sb_services.parse_pipe_fitment(raw),
            [
                sb_services.SbFitmentSpec(2015, 2026, "Ford", "F-150", "2.7L Ecoboost, Gas"),
                sb_services.SbFitmentSpec(
                    2015, 2026, "Ford", "F-150 Raptor", "3.5L Ecoboost, Gas"
                ),
            ],
        )

    def test_missing_engine_field_is_allowed(self):
        # "2021|Ford|Bronco Sport|2.0L" has an engine; some rows stop at the model.
        self.assertEqual(
            sb_services.parse_pipe_fitment("2021|Ford|Bronco Sport"),
            [sb_services.SbFitmentSpec(2021, 2021, "Ford", "Bronco Sport", "")],
        )

    def test_entries_without_a_model_are_dropped(self):
        self.assertEqual(sb_services.parse_pipe_fitment("2021|Ford"), [])
        self.assertEqual(sb_services.parse_pipe_fitment("2021||F-150|3.5L"), [])


class MivaFitmentParsingTests(SimpleTestCase):
    databases = []

    def test_pipe_joined_angle_delimited_vehicles(self):
        raw = (
            "2022>Ford>Bronco>2.3L Ecoboost, Gas|2022>Ford>Bronco>2.7L Ecoboost, Gas"
            "|2021>Ford>Bronco>2.3L Ecoboost, Gas"
        )
        self.assertEqual(
            sb_services.parse_miva_fitment(raw),
            [
                sb_services.SbFitmentSpec(2022, 2022, "Ford", "Bronco", "2.3L Ecoboost, Gas"),
                sb_services.SbFitmentSpec(2022, 2022, "Ford", "Bronco", "2.7L Ecoboost, Gas"),
                sb_services.SbFitmentSpec(2021, 2021, "Ford", "Bronco", "2.3L Ecoboost, Gas"),
            ],
        )

    def test_blank_cell(self):
        self.assertEqual(sb_services.parse_miva_fitment(None), [])
        self.assertEqual(sb_services.parse_miva_fitment(""), [])


class YearCollapsingTests(SimpleTestCase):
    databases = []

    def test_contiguous_years_collapse_and_gaps_survive(self):
        self.assertEqual(
            sb_services.collapse_years_to_ranges([2011, 2012, 2013, 2016]),
            [(2011, 2013), (2016, 2016)],
        )
        self.assertEqual(sb_services.collapse_years_to_ranges([]), [])

    def test_per_year_rows_for_one_vehicle_become_one_range(self):
        specs = [
            sb_services.SbFitmentSpec(year, year, "Ford", "F-250", "6.7L, Diesel")
            for year in range(2011, 2017)
        ]
        self.assertEqual(
            sb_services._collapse_specs(specs),
            [sb_services.SbFitmentSpec(2011, 2016, "Ford", "F-250", "6.7L, Diesel")],
        )

    def test_different_engines_stay_separate(self):
        specs = [
            sb_services.SbFitmentSpec(2021, 2021, "Ford", "Bronco", "2.3L Ecoboost, Gas"),
            sb_services.SbFitmentSpec(2022, 2022, "Ford", "Bronco", "2.3L Ecoboost, Gas"),
            sb_services.SbFitmentSpec(2021, 2021, "Ford", "Bronco", "2.7L Ecoboost, Gas"),
        ]
        collapsed = sorted(sb_services._collapse_specs(specs))
        self.assertEqual(
            collapsed,
            [
                sb_services.SbFitmentSpec(2021, 2021, "Ford", "Bronco", "2.7L Ecoboost, Gas"),
                sb_services.SbFitmentSpec(2021, 2022, "Ford", "Bronco", "2.3L Ecoboost, Gas"),
            ],
        )


class FitmentSourcePrecedenceTests(SimpleTestCase):
    databases = []

    def test_miva_does_not_restate_a_vehicle_the_curated_tab_already_covers(self):
        """
        The real duplication this rule exists for: the Fitment tab gives one 2011-2016 row for a
        Ford F-250 and the miva dump enumerates the same truck year by year. Measured against the
        live workbooks, keeping both more than doubled the fitment table without adding a single
        new vehicle.
        """
        data = {
            "fitment": [
                {
                    "sku": "10-1000",
                    "year": "2011 - 2016",
                    "make": "Ford",
                    "model": "F-250",
                    "engine": "6.7L, Diesel",
                }
            ],
            "miva": [
                {
                    "sku": "10-1000",
                    "fitment": "|".join(
                        "{}>Ford>F-250>6.7L, Diesel".format(year) for year in range(2011, 2017)
                    ),
                }
            ],
        }
        specs = sb_services._fitment_specs_by_sku(data, {})
        self.assertEqual(
            specs,
            {
                "10-1000": {
                    sb_services.SbFitmentSpec(2011, 2016, "Ford", "F-250", "6.7L, Diesel"):
                        sb_services.FITMENT_SOURCE_FITMENT_TAB
                }
            },
        )

    def test_a_vehicle_only_miva_knows_about_is_kept(self):
        data = {
            "fitment": [
                {
                    "sku": "10-1000",
                    "year": "2011 - 2016",
                    "make": "Ford",
                    "model": "F-250",
                    "engine": "6.7L, Diesel",
                }
            ],
            "miva": [
                {
                    "sku": "10-1000",
                    "fitment": "2011>Ford>F-350>6.7L, Diesel|2012>Ford>F-350>6.7L, Diesel",
                }
            ],
        }
        specs = sb_services._fitment_specs_by_sku(data, {})
        self.assertEqual(
            specs["10-1000"],
            {
                sb_services.SbFitmentSpec(2011, 2016, "Ford", "F-250", "6.7L, Diesel"):
                    sb_services.FITMENT_SOURCE_FITMENT_TAB,
                sb_services.SbFitmentSpec(2011, 2012, "Ford", "F-350", "6.7L, Diesel"):
                    sb_services.FITMENT_SOURCE_MIVA,
            },
        )

    def test_miva_rows_are_matched_by_product_code_when_the_sku_cell_is_blank(self):
        data = {
            "miva": [
                {
                    "sku": None,
                    "product_code": "75-5158M",
                    "fitment": "2020>Chevrolet>Silverado 2500>6.6L, Gas",
                }
            ]
        }
        specs = sb_services._fitment_specs_by_sku(data, {})
        self.assertIn("75-5158M", specs)

    def test_numeric_skus_are_normalized_before_matching(self):
        # The Fitment tab's filter-wrap rows arrive as floats; without the trim these would be
        # keyed "101010.0" and never find their catalog part.
        data = {
            "fitment": [
                {
                    "sku": 101010.0,
                    "year": 2015.0,
                    "make": "Ford",
                    "model": "F-250",
                    "engine": "6.7L, Diesel",
                }
            ]
        }
        self.assertEqual(list(sb_services._fitment_specs_by_sku(data, {})), ["101010"])


class ShopifyGroupingTests(SimpleTestCase):
    databases = []

    @staticmethod
    def _row(tab, product_id, **fields):
        row = {
            "tab": tab,
            "product_id": product_id,
            "sku": None,
            "title": None,
            "body_html": None,
            "product_type": None,
            "vendor": None,
            "image_src": None,
            "image_type": None,
            "fitment": None,
        }
        row.update(fields)
        return row

    def test_image_only_continuation_rows_attach_to_the_product(self):
        rows = [
            self._row(
                "Shopify Export Fitment w Images",
                "10373759959333",
                sku="KF-1121",
                title="S&B Intake Replacement Filter",
                body_html="<p>Replacement filter</p>",
                product_type="Air Filter",
                vendor="S&B",
                image_src="https://cdn.shopify.com/a.png",
                image_type="Image",
                fitment="2024-2026|Ford|Mustang|5.0L, Gas",
            ),
            self._row(
                "Shopify Export Fitment w Images",
                "10373759959333",
                image_src="https://cdn.shopify.com/b.png",
                image_type="Image",
            ),
        ]
        products = sb_services.group_shopify_products(rows)
        self.assertEqual(list(products), ["KF-1121"])
        product = products["KF-1121"]
        self.assertEqual(
            product.images,
            ["https://cdn.shopify.com/a.png", "https://cdn.shopify.com/b.png"],
        )
        self.assertEqual(product.body_html, "<p>Replacement filter</p>")
        self.assertEqual(
            product.fitment,
            [sb_services.SbFitmentSpec(2024, 2026, "Ford", "Mustang", "5.0L, Gas")],
        )

    def test_every_variant_of_a_product_gets_its_description_and_imagery(self):
        # 220 products in the current tab have more than one variant row, and the description,
        # imagery and fitment sit on the product rather than the variant.
        rows = [
            self._row(
                "Shopify Export Fitment w Images",
                "123",
                sku="75-5061-1",
                body_html="<p>Cold air intake</p>",
                image_src="https://cdn.shopify.com/a.png",
                image_type="Image",
            ),
            self._row("Shopify Export Fitment w Images", "123", sku="75-5061-1D"),
        ]
        products = sb_services.group_shopify_products(rows)
        self.assertEqual(sorted(products), ["75-5061-1", "75-5061-1D"])
        self.assertEqual(products["75-5061-1D"].body_html, "<p>Cold air intake</p>")
        self.assertEqual(products["75-5061-1D"].images, ["https://cdn.shopify.com/a.png"])

    def test_video_rows_are_not_treated_as_images(self):
        rows = [
            self._row(
                "Sheet3",
                "456",
                sku="CAI3-FMG05",
                image_src="https://cdn.shopify.com/clip.mp4",
                image_type="VIDEO",
            ),
            self._row(
                "Sheet3",
                "456",
                image_src="https://cdn.shopify.com/still.png",
                image_type="IMAGE",
            ),
        ]
        product = sb_services.group_shopify_products(rows)["CAI3-FMG05"]
        self.assertEqual(product.images, ["https://cdn.shopify.com/still.png"])
        self.assertEqual(product.video, "https://cdn.shopify.com/clip.mp4")

    def test_the_current_tab_wins_over_the_older_leftover_tab(self):
        # Both tabs are the same export at different dates and share product ids; the rows arrive
        # in SHOPIFY_TAB_PRIORITY order and the first record for a SKU must win.
        rows = [
            self._row(
                "Shopify Export Fitment w Images",
                "789",
                sku="75-5099D",
                body_html="<p>current</p>",
            ),
            self._row("Sheet3", "789", sku="75-5099D", body_html="<p>older</p>"),
        ]
        self.assertEqual(
            sb_services.group_shopify_products(rows)["75-5099D"].body_html, "<p>current</p>"
        )


class CatalogCoercionTests(SimpleTestCase):
    databases = []

    def test_float_formatted_identifiers_are_trimmed(self):
        self.assertEqual(sb_services._safe_sku(101010.0), "101010")
        self.assertEqual(sb_services._safe_upc(841372040375.0), "841372040375")
        self.assertEqual(sb_services._safe_code(1.0, 16), "1")
        # A genuinely non-integer value keeps its decimals.
        self.assertEqual(sb_services._safe_code("1.5", 16), "1.5")

    def test_not_applicable_sentinels_become_none(self):
        # S&B write "-" in 358 of 828 filter_type cells; storing it would read as a filter media.
        for value in ("-", "--", "N/A", "none", ""):
            self.assertIsNone(sb_services._safe_str(value), msg=repr(value))

    def test_last_catalog_row_wins_per_sku(self):
        rows = [
            {"sku": "75-5016", "us_retail_price": 379.0},
            {"sku": "75-5016", "us_retail_price": 399.0},
        ]
        self.assertEqual(
            sb_services._catalog_rows_by_sku(rows)["75-5016"]["us_retail_price"], 399.0
        )


class PricingLevelTests(SimpleTestCase):
    databases = []

    def test_level_two_is_read_in_the_spellings_support_might_type(self):
        for raw in (2, "2", "Level 2", "level2", " 2 "):
            self.assertEqual(
                sb_services.sb_pricing_level_for_credentials(
                    {src_constants.SB_CREDENTIALS_PRICING_LEVEL: raw}
                ),
                src_constants.SB_PRICING_LEVEL_2,
                msg=repr(raw),
            )

    def test_anything_unset_or_unrecognized_falls_back_to_level_one(self):
        """
        Level 1 is the higher of the two dealer prices, so this default understates margin rather
        than overstating it -- the safe direction for a misconfigured connection.
        """
        for creds in ({}, {src_constants.SB_CREDENTIALS_PRICING_LEVEL: ""},
                      {src_constants.SB_CREDENTIALS_PRICING_LEVEL: "gold"},
                      {src_constants.SB_CREDENTIALS_PRICING_LEVEL: 7}):
            self.assertEqual(
                sb_services.sb_pricing_level_for_credentials(creds),
                src_constants.SB_PRICING_LEVEL_1,
                msg=repr(creds),
            )

    def test_price_region(self):
        self.assertEqual(
            sb_services.sb_price_region_for_credentials(
                {src_constants.SB_CREDENTIALS_PRICE_REGION: "ca"}
            ),
            src_constants.SB_PRICE_REGION_CA,
        )
        self.assertEqual(
            sb_services.sb_price_region_for_credentials(
                {src_constants.SB_CREDENTIALS_PRICE_REGION: "Canada"}
            ),
            src_constants.SB_PRICE_REGION_CA,
        )
        self.assertEqual(
            sb_services.sb_price_region_for_credentials({}),
            src_constants.SB_PRICE_REGION_US,
        )


class ShopifyHtmlDescriptionTests(SimpleTestCase):
    """
    ``master_part_enrichment`` helpers that exist for S&B -- the only feed in that module whose
    description arrives as rendered HTML rather than as typed pieces.
    """

    databases = []

    def test_markup_is_reduced_to_text_and_never_passed_through(self):
        # A real 10-1000 body, trimmed: Shopify wraps everything in styled spans, and a stray
        # <meta> tag turns up inside the body too.
        body = (
            '<p><meta charset="utf-8"><span><strong>Important Note: </strong></span>'
            '<span style="color: #000000; font-family: -apple-system;">The nozzle at high-flow '
            'fuel stations will not fill automatically.</span></p>'
        )
        paragraphs, bullets = enrichment._html_to_prose_and_bullets(body)
        self.assertEqual(
            paragraphs,
            ["Important Note: The nozzle at high-flow fuel stations will not fill automatically."],
        )
        self.assertEqual(bullets, [])

    def test_list_items_become_bullets_and_paragraphs_stay_paragraphs(self):
        """
        The distinction matters: flattening everything into bullets turns a product's own opening
        sentence into a stray bullet point, and flattening everything into prose loses S&B's
        feature lists.
        """
        body = (
            "<p>Important Note about the Fuel Gauge Operation:</p>"
            "<ul><li>The distance to empty can't be reprogrammed.</li>"
            "<li>High-flow nozzles will not fill automatically.</li></ul>"
        )
        paragraphs, bullets = enrichment._html_to_prose_and_bullets(body)
        self.assertEqual(paragraphs, ["Important Note about the Fuel Gauge Operation:"])
        self.assertEqual(
            bullets,
            [
                "The distance to empty can't be reprogrammed.",
                "High-flow nozzles will not fill automatically.",
            ],
        )

    def test_entities_are_decoded_and_hand_rolled_bullet_glyphs_dropped(self):
        body = "<ul><li>&bull; Fits F&amp;I trims</li><li>1) Tune required</li></ul>"
        _, bullets = enrichment._html_to_prose_and_bullets(body)
        self.assertEqual(bullets, ["Fits F&I trims", "Tune required"])

    def test_br_separated_copy_splits_into_paragraphs(self):
        body = "Line one.<br>Line two.<br /><br>Line three."
        paragraphs, bullets = enrichment._html_to_prose_and_bullets(body)
        self.assertEqual(paragraphs, ["Line one.", "Line two.", "Line three."])
        self.assertEqual(bullets, [])

    def test_blank_and_placeholder_bodies(self):
        for value in (None, "", "   ", "N/A"):
            self.assertEqual(
                enrichment._html_to_prose_and_bullets(value), ([], []), msg=repr(value)
            )

    def test_json_image_column_is_decoded_from_the_raw_sql_string(self):
        """
        The enrichment fetch path uses hand-written SQL, so a JSONField comes back as the raw
        string psycopg2 hands over. Without decoding it, a part's full image list silently falls
        back to the first six numbered columns -- 75-5222 has 16.
        """
        self.assertEqual(enrichment._json_list('["a", "b"]'), ["a", "b"])
        self.assertEqual(enrichment._json_list(["a"]), ["a"])
        for value in (None, "", "not json", "{}", '{"a": 1}', "[oops"):
            self.assertIsNone(enrichment._json_list(value), msg=repr(value))


class FeedSpecTests(SimpleTestCase):
    databases = []

    def test_tabs_are_matched_loosely_so_a_renamed_pricing_tab_still_ingests(self):
        # The first tab is named for the date of the last price change and does move.
        sheet_names = [
            "SKUs Affected by Price Change a",
            "All Product Info",
            "Fitment",
            "Notes",
            "miva export",
        ]
        self.assertEqual(
            feed_spec.find_tab(sheet_names, feed_spec.CATALOG_TAB_KEYS), "All Product Info"
        )
        self.assertEqual(feed_spec.find_tab(sheet_names, feed_spec.MIVA_TAB_KEYS), "miva export")
        self.assertIsNone(feed_spec.find_tab(sheet_names, ("discontinued",)))

    def test_shopify_tabs_come_back_in_priority_order(self):
        self.assertEqual(
            feed_spec.find_tabs(
                ["Sheet3", "Shopify Export Fitment w Images"], feed_spec.SHOPIFY_TAB_PRIORITY
            ),
            ["Shopify Export Fitment w Images", "Sheet3"],
        )

    def test_catalog_columns_tolerate_s_and_b_own_typos(self):
        header = [
            "sku", "upc", "brand", "category", "filter_type", "vehicle_type",
            "tilte_description", "US Retail Price", "Canadian Retail Price", "Level 1",
            "Level 2", "carb_exampt_id",
        ]
        columns = feed_spec.map_columns(header, feed_spec.CATALOG_COLUMNS)
        self.assertEqual(columns["title"], 6)
        self.assertEqual(columns["carb_exempt_id"], 11)
        self.assertEqual(columns["level_2_price"], 10)
        # A column the sheet doesn't carry is simply absent, not an error.
        self.assertNotIn("weight", columns)

    def test_inventory_columns_are_found_by_substring(self):
        """
        S&B's live-inventory format is unconfirmed, so the column match has to be forgiving --
        this is what lets a header like "Available On Hand Qty" resolve.
        """
        header = ["Part Number", "Description", "Available On Hand Qty"]
        self.assertEqual(feed_spec.find_column(header, feed_spec.INVENTORY_SKU_COLUMNS), 0)
        self.assertEqual(feed_spec.find_column(header, feed_spec.INVENTORY_QTY_COLUMNS), 2)
        self.assertIsNone(
            feed_spec.find_column(["Colour", "Notes"], feed_spec.INVENTORY_QTY_COLUMNS)
        )
