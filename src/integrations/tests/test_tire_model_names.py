"""
Tests for model-name consolidation.

The interesting assertions are the negative ones. Merging two spellings of one tire is a small
win; merging two different tires is a catalog bug that is hard to see and hard to undo, so the
cases below pin the distinctions the key has to keep -- generation markers, and the '+' that an
earlier version of this key stripped.
"""
from django.test import SimpleTestCase

from src.integrations.services.tire_model_names import normalise


class NormaliseTests(SimpleTestCase):
    databases = []

    def _same(self, a, b, brand):
        self.assertEqual(normalise(a, brand), normalise(b, brand), "{!r} and {!r} should merge".format(a, b))

    def _different(self, a, b, brand):
        self.assertNotEqual(normalise(a, brand), normalise(b, brand), "{!r} and {!r} must NOT merge".format(a, b))

    def test_punctuation_and_spacing_are_noise(self):
        self._same("Geolandar A/T G015", "Geolandar AT G015", "YOKOHAMA TIRE")
        self._same("Eagle LS-2", "Eagle LS2", "GOODYEAR")
        self._same("P Zero (PZ4)", "P Zero PZ4", "PIRELLI")
        self._same("WinterContact TS 860 S", "WinterContact TS860 S", "CONTINENTAL TIRE")

    def test_case_is_noise(self):
        self._same("Blizzak IcePeak", "Blizzak Icepeak", "BRIDGESTONE")
        self._same("iceGUARD G075", "iceGuard G075", "YOKOHAMA TIRE")

    def test_roman_and_arabic_generations_are_the_same(self):
        self._same("Crosstek II", "Crosstek 2", "FALKEN TIRE")

    def test_roman_numerals_are_matched_longest_first(self):
        """VIII must not be read as V followed by III."""
        self.assertEqual(normalise("Model VIII", "ACME"), "MODEL8")
        self.assertEqual(normalise("Model VII", "ACME"), "MODEL7")

    def test_the_brand_glued_to_the_front_is_noise(self):
        self._same("Kanati Mud Hog M/T", "Mud Hog M/T", "GREENBALL CORPORATION/ KANATI")
        self._same("Cooper Instinct RS", "Instinct RS", "COOPER TIRES")

    def test_a_generic_word_inside_a_brand_name_is_not_stripped(self):
        """'GROUP' or 'COMPANY' appearing in a brand must not be removed from a model name that
        legitimately contains it."""
        self.assertIn("GROUP", normalise("Trail Group 5", "THE WHEEL GROUP (TWG)"))

    def test_a_generation_marker_is_never_noise(self):
        self._different("Terra Grappler", "Terra Grappler G2", "NITTO")
        self._different("Dynapro AT2", "Dynapro AT", "HANKOOK")

    def test_plus_distinguishes_real_products(self):
        """Goodyear sells both. Stripping '+' merged them in the first version of this key."""
        self._different("Ultra Grip Performance", "UltraGrip Performance+", "GOODYEAR")
        self._different("Proxes Sport A/S", "Proxes Sport A/S+", "TOYO")

    def test_empty_input(self):
        self.assertEqual(normalise(None, "NITTO"), "")
        self.assertEqual(normalise("", None), "")
