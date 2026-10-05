"""
Tests for the Premier "Wheel Pros" brand repair.

Two pieces are worth pinning: which marque a bucket row belongs to, and which of two competing
tire specs survives a merge. Both are decisions that are hard to see once made -- a wrong marque
puts a wheel under someone else's name, and a wrong spec choice silently discards catalog-verified
figures in favour of parser-derived ones.
"""
from django.test import SimpleTestCase

from src.integrations.services import premier, wheelpros_brand_repair


class MarqueTests(SimpleTestCase):
    databases = []

    def test_the_distributor_prefix_is_stripped_before_reading_the_marque(self):
        """63% of the bucket's descriptions lead with "Wheel Pros". Without stripping it the
        leading phrase is the bucket brand itself, which resolves as a no-op -- the resolver
        matched 2 rows out of 6,754 for exactly this reason."""
        self.assertEqual(premier._leading_brand_phrase("Wheel Pros Niche 1PC 17X8 TURIN"), "Niche")
        self.assertEqual(premier._leading_brand_phrase("Wheel Pros Fuel Tires 40X15.50R22LT"), "Fuel Tires")

    def test_style_codes_name_their_maker(self):
        cases = [
            ("Wheel Pros VN215 TT-II 1PC 15X4", "AMERICAN RACING VINTAGE"),
            ("VNCL205 20X8 5X4.75 TT-GRY POL", "AMERICAN RACING VINTAGE"),
            ("MO992 20X10 8X6.5 G-BLK MILL", "MOTO METAL"),
            ("PR104C 18X9.5 5X4.75 70.3 CHROME", "PERFORMANCE REPLICAS"),
            ("R159 18X8.5 5X100/112 MT-BLK 45MM", "ROTIFORM"),
            ("Wheel Pros Niche 1PC 17X8 TURIN", "NICHE 1PC"),
            ("Wheel Pros DUB 1PC 22X9 BALLER", "DUB 1PC"),
        ]
        for description, expected in cases:
            self.assertEqual(premier.marque_from_bucket_description(description), expected, description)

    def test_tire_model_codes_resolve_to_falken(self):
        for description in (
            "LT31X10.5R15 AT3W 109S 30.5",
            "P225/60R16 SN-211 97T 26.6",
            "255/35ZR18 FK-452 91Y",
            "Falken WDPEAK AT4W 265/70R17",
            "P305/45R22 S/TZ04 118H XL",
        ):
            self.assertEqual(premier.marque_from_bucket_description(description), "FALKEN TIRE", description)

    def test_an_unknown_style_name_resolves_to_nothing(self):
        """Guessing is what created this bucket. A style name we cannot attribute stays put."""
        for description in ("Wheel Pros BALLER 24X10 5X5.5", "Wheel Pros VOSSO 20X9", "Wheel Pros DFS 20X9 5X120"):
            self.assertIsNone(premier.marque_from_bucket_description(description), description)

    def test_empty_input(self):
        self.assertIsNone(premier.marque_from_bucket_description(None))
        self.assertIsNone(premier.marque_from_bucket_description(""))


class _Spec:
    """A stand-in carrying only what _better_spec reads."""

    class _Meta:
        concrete_fields = ()

    def __init__(self, spec_source, populated=0):
        self.spec_source = spec_source
        self._populated = populated
        self._meta = self._Meta()


class SpecPrecedenceTests(SimpleTestCase):
    """
    Which tire spec survives when a duplicate master part is merged away.

    Both sides were enriched independently, so both have a real row. Keeping the survivor's
    unconditionally would sometimes discard a catalog-matched row in favour of a parser-derived
    one -- the opposite of what the catalog merge was for.
    """

    databases = []

    def setUp(self):
        self._orig = wheelpros_brand_repair._populated_field_count
        wheelpros_brand_repair._populated_field_count = lambda spec: spec._populated

    def tearDown(self):
        wheelpros_brand_repair._populated_field_count = self._orig

    def test_a_catalog_row_beats_a_parsed_one(self):
        survivor, loser = _Spec("parser", 40), _Spec("simpletire", 5)
        self.assertIs(wheelpros_brand_repair._better_spec(survivor, loser), loser)

    def test_simpletire_beats_tdg(self):
        a, b = _Spec("simpletire", 1), _Spec("tdg", 30)
        self.assertIs(wheelpros_brand_repair._better_spec(a, b), a)

    def test_on_the_same_source_the_fuller_row_wins(self):
        thin, full = _Spec("simpletire", 12), _Spec("simpletire", 31)
        self.assertIs(wheelpros_brand_repair._better_spec(thin, full), full)

    def test_a_tie_keeps_the_first_argument(self):
        """Called as (survivor, loser), so a genuine tie leaves the surviving part's row in place
        and avoids a pointless write."""
        survivor, loser = _Spec("tdg", 20), _Spec("tdg", 20)
        self.assertIs(wheelpros_brand_repair._better_spec(survivor, loser), survivor)

    def test_an_unknown_source_ranks_below_every_known_one(self):
        known, unknown = _Spec("parser", 1), _Spec("", 99)
        self.assertIs(wheelpros_brand_repair._better_spec(known, unknown), known)

    def test_the_ranking_matches_the_catalog_merge(self):
        rank = wheelpros_brand_repair.SPEC_SOURCE_RANK
        self.assertGreater(rank["simpletire"], rank["tdg"])
        self.assertGreater(rank["tdg"], rank["parser"])
