"""
Tests for ``src.integrations.services.tdg_images``.

Pure logic only -- no network, no database. The normalisers are the whole job here: the two
catalogs spell the same brand, barcode and part number three different ways, and every one of
those differences is what stands between a match and a missing image. The lookup precedence is
pinned too, because GTIN and brand+MPN can disagree and the wrong winner puts one
manufacturer's photo on another's product.
"""
from django.test import SimpleTestCase

from src.integrations.services import tdg_images


class GtinNormalisationTests(SimpleTestCase):
    def test_leading_zeros_do_not_make_two_barcodes(self):
        self.assertEqual(tdg_images.normalize_gtin("086699174888"), tdg_images.normalize_gtin("86699174888"))

    def test_punctuation_is_stripped(self):
        self.assertEqual(tdg_images.normalize_gtin("0-866-991 74888"), "86699174888")

    def test_empty_and_zero_are_none_not_a_key(self):
        # '0' must not become a key every un-barcoded part collides on.
        for value in (None, "", "0", "0000", "abc"):
            self.assertIsNone(tdg_images.normalize_gtin(value), value)


class BrandNormalisationTests(SimpleTestCase):
    def test_our_suffixed_names_collapse_onto_tdgs_bare_ones(self):
        for ours, theirs in [
            ("YOKOHAMA TIRE", "Yokohama"),
            ("CONTINENTAL TIRE", "Continental"),
            ("COOPER TIRES", "Cooper"),
            ("NEXEN TIRE", "Nexen"),
            ("CARLISLE TIRE AND WHEEL COMPANY", "Carlisle"),
        ]:
            self.assertEqual(tdg_images.normalize_brand(ours), tdg_images.normalize_brand(theirs), ours)

    def test_hand_checked_aliases(self):
        self.assertEqual(tdg_images.normalize_brand("PirelliB"), tdg_images.normalize_brand("Pirelli"))
        self.assertEqual(tdg_images.normalize_brand("Goodrich"), tdg_images.normalize_brand("BF Goodrich"))

    def test_distinct_brands_stay_distinct(self):
        # The normaliser must not over-collapse: these are different manufacturers.
        self.assertNotEqual(tdg_images.normalize_brand("General Tire"), tdg_images.normalize_brand("Generic"))
        self.assertNotEqual(tdg_images.normalize_brand("Toyo"), tdg_images.normalize_brand("Tokyo"))

    def test_a_name_that_is_only_noise_yields_nothing_matchable(self):
        # 'TIRE' alone must not become a key that matches every brand.
        self.assertEqual(tdg_images.normalize_brand("TIRE"), "")
        self.assertEqual(tdg_images.normalize_brand(None), "")


class PartNumberNormalisationTests(SimpleTestCase):
    def test_punctuation_and_case_are_ignored(self):
        self.assertEqual(tdg_images.normalize_part_number("ah-4126"), "AH4126")
        self.assertEqual(tdg_images.normalize_part_number(" AH 4126 "), "AH4126")

    def test_missing_part_number_is_empty(self):
        self.assertEqual(tdg_images.normalize_part_number(None), "")


class LookupPrecedenceTests(SimpleTestCase):
    def setUp(self):
        self.by_gtin = {"86699174888": "https://img/by-gtin.png"}
        self.by_brand_part = {("bfgoodrich", "AH4126"): "https://img/by-part.png"}

    def _row(self, **kw):
        row = {"gtin": None, "brand__name": None, "part_number": None}
        row.update(kw)
        return row

    def test_gtin_wins_over_brand_and_part_number(self):
        row = self._row(gtin="086699174888", brand__name="BF Goodrich", part_number="AH-4126")
        self.assertEqual(tdg_images._lookup(row, self.by_gtin, self.by_brand_part), ("https://img/by-gtin.png", "gtin"))

    def test_brand_and_part_number_is_the_fallback(self):
        row = self._row(brand__name="BF Goodrich", part_number="AH-4126")
        url, how = tdg_images._lookup(row, self.by_gtin, self.by_brand_part)
        self.assertEqual((url, how), ("https://img/by-part.png", "brand+part_number"))

    def test_an_unknown_gtin_falls_through_rather_than_failing(self):
        row = self._row(gtin="99999999999", brand__name="BF Goodrich", part_number="AH4126")
        self.assertEqual(tdg_images._lookup(row, self.by_gtin, self.by_brand_part)[1], "brand+part_number")

    def test_no_match_returns_none(self):
        row = self._row(brand__name="Nokian", part_number="XYZ")
        self.assertEqual(tdg_images._lookup(row, self.by_gtin, self.by_brand_part), (None, ""))

    def test_a_part_with_no_brand_cannot_match_on_part_number_alone(self):
        # ('', 'AH4126') must never be a usable key -- it would match across every brand.
        row = self._row(part_number="AH-4126")
        self.assertEqual(tdg_images._lookup(row, self.by_gtin, {("", "AH4126"): "x"}), (None, ""))
