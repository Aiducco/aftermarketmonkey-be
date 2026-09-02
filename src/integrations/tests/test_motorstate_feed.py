"""
Tests for the Motor State FTP feed parser.

The thing worth pinning here is that the column set is account-dependent. Motor State
provisions each dealer's file with the columns that dealer is entitled to: the enriched account
gets ``Image URL`` / ``Category Level 1-3`` / ``Long Description(150)``, the plain one gets
``CanadaRestricted`` / ``AcquiredDate`` / ``EmissionsWarning`` / ``Oversized`` instead, and 22
columns are common. Parse positionally and a plain-account run silently writes the wrong field
into every enriched column; treat a missing column as False rather than unknown and shipping
decisions get made on data the account never sent.
"""
import os
import tempfile

from django.test import SimpleTestCase

from src.integrations.clients.motorstate import feed_spec, ftp_client
from src.integrations.services import motorstate_feed

_ENRICHED_HEADER = (
    "PartNumber,Description,Brand,SuggestedRetail,Cost,Length,Width,Height,Weight,QtyAvail,UPC,"
    "Jobber,AAIACode,MapPrice,VendorMSRP,AirRestricted,StateRestricted,TruckFrtOnly,"
    "ManufacturerPart,ShipAlone,Status,MotorStateNotes,Image URL,Category Level 1,"
    "Category Level 2,Category Level 3,Long Description(150)"
)
_ENRICHED_ROW = (
    "AMRAR105M7761B,Wheel Torq Thrust M,AMERICAN RACING WHEELS,412.99,330.40,20.0,20.0,10.0,"
    "24.5,3,886126110116,371.69,WHEL,412.99,,NO,,NO,AR105M7761B,YES,S,,"
    "https://msimg.blob.core.windows.net/product/AMRAR105M7761B.jpg,Wheels and Tires,Wheels,"
    "Wheels,Wheel - Torq Thrust M - 17 x 7 in - 4.000 in Backspace - 5 x 4.75 in Bolt Pattern"
)
_PLAIN_HEADER = (
    "PartNumber,Description,Brand,SuggestedRetail,Cost,Length,Width,Height,Weight,QtyAvail,UPC,"
    "Jobber,AAIACode,MapPrice,VendorMSRP,AirRestricted,StateRestricted,TruckFrtOnly,"
    "ManufacturerPart,ShipAlone,Status,MotorStateNotes,CanadaRestricted,AcquiredDate,"
    "EmissionsWarning,Oversized"
)
_PLAIN_ROW = (
    "AAA00004,Hose End #4 Straight ,A-1 PRODUCTS,13.79,9.49,4.65,2.90,0.65,0.04,1,,11.03,BHXN,"
    "13.79,,NO,,NO,A1P00004,NO,S,,NO,2019-04-01,NO,NO"
)


def _write_feed(lines, encoding="utf-8-sig", newline="\r\n"):
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w", encoding=encoding, newline="") as fh:
        fh.write(newline.join(lines) + newline)
    return path


class HeaderMappingTests(SimpleTestCase):
    databases = []

    def test_both_account_column_sets_map_completely(self):
        """Neither live file has a column the spec cannot name -- an unmapped column would be
        data silently dropped on ingest."""
        for header in (_ENRICHED_HEADER, _PLAIN_HEADER):
            columns = header.split(",")
            mapped = feed_spec.map_header_row(columns)
            self.assertEqual(len(mapped), len(columns), header[:40])

    def test_header_matching_survives_spacing_and_punctuation_drift(self):
        for label in ("Category Level 1", "categorylevel1", "CATEGORY_LEVEL_1", " Category Level 1 "):
            self.assertEqual(feed_spec.canonical_column(label), "category_level_1")
        for label in ("Long Description(150)", "Long Description", "longdescription150"):
            self.assertEqual(feed_spec.canonical_column(label), "long_description")

    def test_a_duplicated_header_label_does_not_overwrite_the_first_column(self):
        mapped = feed_spec.map_header_row(["PartNumber", "Cost", "Cost"])
        self.assertEqual(mapped, {0: "part_number", 1: "cost"})

    def test_unrecognized_columns_are_skipped_not_guessed(self):
        mapped = feed_spec.map_header_row(["PartNumber", "SomeNewColumn", "Cost"])
        self.assertEqual(mapped, {0: "part_number", 2: "cost"})


class FeedReadingTests(SimpleTestCase):
    databases = []

    def test_enriched_rows_carry_the_content_columns(self):
        path = _write_feed([_ENRICHED_HEADER, _ENRICHED_ROW])
        try:
            rows = list(motorstate_feed.iter_feed_rows(path))
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["part_number"], "AMRAR105M7761B")
            self.assertEqual(row["category_level_1"], "Wheels and Tires")
            self.assertEqual(row["category_level_3"], "Wheels")
            self.assertTrue(row["image_url"].endswith("AMRAR105M7761B.jpg"))
            self.assertIn("Backspace", row["long_description"])
            self.assertTrue(motorstate_feed.feed_has_catalog_columns(
                motorstate_feed.read_feed_header(path)
            ))
        finally:
            os.remove(path)

    def test_plain_account_rows_simply_lack_the_enriched_keys(self):
        """Absent, not empty-string: the catalog writer keys off presence to decide whether it
        is allowed to touch those columns at all."""
        path = _write_feed([_PLAIN_HEADER, _PLAIN_ROW])
        try:
            row = next(iter(motorstate_feed.iter_feed_rows(path)))
            for column in feed_spec.ENRICHED_COLUMNS:
                self.assertNotIn(column, row)
            self.assertEqual(row["canada_restricted"], "NO")
            self.assertFalse(motorstate_feed.feed_has_catalog_columns(
                motorstate_feed.read_feed_header(path)
            ))
        finally:
            os.remove(path)

    def test_a_file_without_a_part_number_column_yields_nothing(self):
        path = _write_feed(["Description,Cost", "Hose End,9.49"])
        try:
            self.assertEqual(list(motorstate_feed.iter_feed_rows(path)), [])
        finally:
            os.remove(path)

    def test_a_cp1252_feed_still_parses(self):
        """The samples are UTF-8 with a BOM, but a vendor-side encoding change must degrade to
        a decoded row rather than an exception mid-ingest."""
        # U+2013 encodes to the single byte 0x96 in cp1252, which is not valid UTF-8 -- so
        # this file can only be read by falling past the two UTF-8 attempts.
        path = _write_feed([_PLAIN_HEADER, _PLAIN_ROW.replace("Hose End", "Hose\u2013End")],
                           encoding="cp1252")
        try:
            row = next(iter(motorstate_feed.iter_feed_rows(path)))
            self.assertEqual(row["part_number"], "AAA00004")
        finally:
            os.remove(path)


class CoercionTests(SimpleTestCase):
    databases = []

    def test_brand_code_is_the_part_number_prefix(self):
        self.assertEqual(motorstate_feed.brand_code_from_part_number("AAA00004"), "AAA")
        self.assertEqual(motorstate_feed.brand_code_from_part_number(" amrar105 "), "AMR")
        self.assertIsNone(motorstate_feed.brand_code_from_part_number("AB"))
        self.assertIsNone(motorstate_feed.brand_code_from_part_number(""))

    def test_missing_flags_stay_unknown_rather_than_false(self):
        """A blank or absent column means the account was not told, which must not read as
        'not restricted' -- these flags gate air/freight shipping."""
        self.assertIsNone(motorstate_feed._to_bool(""))
        self.assertIsNone(motorstate_feed._to_bool(None))
        self.assertIsNone(motorstate_feed._to_bool("MAYBE"))
        self.assertTrue(motorstate_feed._to_bool("YES"))
        self.assertFalse(motorstate_feed._to_bool("no"))

    def test_upc_is_reduced_to_digits(self):
        self.assertEqual(motorstate_feed._normalized_upc(" 886126110116 "), "886126110116")
        self.assertEqual(motorstate_feed._normalized_upc("8-861-261"), "8861261")
        self.assertIsNone(motorstate_feed._normalized_upc(""))
        self.assertIsNone(motorstate_feed._normalized_upc("N/A"))

    def test_prices_tolerate_currency_formatting(self):
        self.assertEqual(str(motorstate_feed._to_decimal("$1,234.56")), "1234.56")
        self.assertIsNone(motorstate_feed._to_decimal(""))
        self.assertIsNone(motorstate_feed._to_decimal("call for price"))

    def test_a_row_with_no_prices_at_all_is_not_written(self):
        """Writing it would blank out a good price from a previous run."""
        self.assertIsNone(motorstate_feed._pricing_from_feed_row({"cost": "", "jobber": None}))
        priced = motorstate_feed._pricing_from_feed_row({"cost": "9.49", "map_price": ""})
        self.assertEqual(str(priced["customer_price"]), "9.49")
        self.assertFalse(priced["is_map_restricted"])

    def test_acquired_date_accepts_the_formats_seen_in_the_feed(self):
        self.assertEqual(str(motorstate_feed._to_date("2019-04-01")), "2019-04-01")
        self.assertEqual(str(motorstate_feed._to_date("04/01/2019")), "2019-04-01")
        self.assertIsNone(motorstate_feed._to_date("not a date"))


class FilenameDerivationTests(SimpleTestCase):
    databases = []

    def test_the_feed_filename_comes_from_the_account_number_in_the_login(self):
        """Motor State names each dealer's file after the account number that is also the local
        part of the FTP login, so onboarding a company needs only user + password."""
        self.assertEqual(
            ftp_client.default_remote_filename_for_user("853809@motorstateftp.com"), "853809.csv"
        )
        self.assertEqual(ftp_client.default_remote_filename_for_user("838674"), "838674.csv")
        self.assertEqual(ftp_client.default_remote_filename_for_user(""), "")


class DimensionBoundsTests(SimpleTestCase):
    databases = []

    def test_an_out_of_range_dimension_is_dropped_not_clamped(self):
        """The live feed carries a 788,120,000,000 lb weight on TCI242900. numeric(12,5) cannot
        hold it and the whole batch aborts on overflow. Clamping would feed a wrong figure into
        freight quoting, so it becomes null instead."""
        self.assertIsNone(motorstate_feed._to_dimension("788120000000.00000", "TCI242900", "weight"))
        self.assertEqual(str(motorstate_feed._to_dimension("24.50000")), "24.50000")
        self.assertEqual(str(motorstate_feed._to_dimension("9999999.99999")), "9999999.99999")
        self.assertIsNone(motorstate_feed._to_dimension(""))
