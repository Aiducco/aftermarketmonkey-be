"""
Tests for the wheel sizing parser.

Every fixture is a real string from ``wheelpros_parts``, ``thewheelgroup_parts``,
``elitewheels_part_wheels`` or a distributor title. The negative cases matter as much as the
positive ones: this parser decides what is a wheel, and a false positive puts a shock absorber or
a tire into wheel fitment results.
"""
import decimal

from django.test import SimpleTestCase

from src.domain import wheel_size


class BoltPatternUnitTests(SimpleTestCase):
    """The inch/millimetre distinction. Getting this wrong is a fitment error on a customer's car."""

    databases = []

    def test_inch_circles_convert_to_millimetres(self):
        for text, mm in (
            ("6X5.5", "139.7"),
            ("5X4.5", "114.3"),
            ("5X5.0", "127"),
            ("8X6.5", "165.1"),
            ("5X4.75", "120.65"),
        ):
            parsed = wheel_size.parse_bolt_pattern(text)
            self.assertIsNotNone(parsed, text)
            self.assertEqual(parsed.circle_mm, decimal.Decimal(mm), text)
            self.assertTrue(parsed.stated_in_inches, text)

    def test_millimetre_circles_are_left_alone(self):
        for text, mm in (("6X135", "135"), ("8X170", "170"), ("5X112", "112"), ("4X110", "110")):
            parsed = wheel_size.parse_bolt_pattern(text)
            self.assertEqual(parsed.circle_mm, decimal.Decimal(mm), text)
            self.assertFalse(parsed.stated_in_inches, text)

    def test_the_two_units_never_overlap(self):
        """Every inch circle is under 20 and every metric one over 90, so the threshold is not a
        judgement call. This pins the gap so a future feed cannot quietly narrow it."""
        for standard in wheel_size.CANONICAL_BOLT_CIRCLES_MM:
            self.assertGreater(standard, wheel_size._INCH_IF_BELOW)

    def test_separator_style_does_not_matter(self):
        a = wheel_size.parse_bolt_pattern("5-114.3")
        b = wheel_size.parse_bolt_pattern("5x114.3")
        c = wheel_size.parse_bolt_pattern("5X4.5")
        self.assertEqual(a.circle_mm, b.circle_mm)
        self.assertEqual(b.circle_mm, c.circle_mm)

    def test_display_keeps_the_source_spelling(self):
        """A Jeep owner searches '6x5.5', not '6x139.7'."""
        self.assertEqual(wheel_size.parse_bolt_pattern("6X5.5").display, "6x5.5")
        self.assertEqual(wheel_size.parse_bolt_pattern("6X135").display, "6x135")

    def test_nonsense_is_rejected(self):
        for text in ("", "5.5", "ABC", "2X50", "12X300", None):
            self.assertIsNone(wheel_size.parse_bolt_pattern(text), text)


class CanonicalCircleTests(SimpleTestCase):
    databases = []

    def test_truncations_snap_to_the_pattern_they_spell(self):
        """Wheel Pros publishes 6X5.5 in its attribute column and 6X139 in the same row's title."""
        for truncated, real in (("6X139", "139.7"), ("5X114", "114.3"), ("8X165", "165.1")):
            self.assertEqual(wheel_size.parse_bolt_pattern(truncated).circle_mm, decimal.Decimal(real))

    def test_the_two_spellings_of_one_pattern_compare_equal(self):
        for inch, metric in (("6X5.5", "6X139"), ("5X4.5", "5X114"), ("5X4.25", "5X108")):
            a, b = wheel_size.parse_bolt_pattern(inch), wheel_size.parse_bolt_pattern(metric)
            self.assertEqual((a.lug_count, a.circle_mm), (b.lug_count, b.circle_mm))

    def test_distinct_patterns_stay_distinct(self):
        """4x136 and 4x137 are 1.0 mm apart and both real, so neither may absorb the other."""
        self.assertNotEqual(
            wheel_size.parse_bolt_pattern("4X136").circle_mm,
            wheel_size.parse_bolt_pattern("4X137").circle_mm,
        )
        self.assertNotEqual(
            wheel_size.parse_bolt_pattern("5X120").circle_mm,
            wheel_size.parse_bolt_pattern("5X4.75").circle_mm,
        )

    def test_a_value_equidistant_from_two_standards_is_not_snapped(self):
        midpoint = decimal.Decimal("127.5")  # 127 and 128 are both standard
        self.assertEqual(wheel_size.canonical_circle_mm(midpoint), midpoint)

    def test_a_value_far_from_everything_is_left_alone(self):
        self.assertEqual(wheel_size.canonical_circle_mm(decimal.Decimal("123")), decimal.Decimal("123"))


class BlankTests(SimpleTestCase):
    databases = []

    def test_blank_is_a_product_state_not_a_missing_value(self):
        """674 Wheel Pros rows are undrilled wheels, drilled to order. Reading that as 'unknown'
        would put them in fitment results for every vehicle."""
        self.assertTrue(wheel_size.is_blank("BLANK"))
        self.assertTrue(wheel_size.is_blank("Blank 5x/6x"))
        self.assertFalse(wheel_size.is_blank("5X114.3"))
        self.assertIsNone(wheel_size.parse_bolt_pattern("BLANK"))


class MultiPatternTests(SimpleTestCase):
    databases = []

    def test_a_shared_lug_count_is_carried_across_the_slash(self):
        """``6X135/5.5`` is 6x135 and 6x5.5, not 6x135 and something with no lug count. A wheel
        cannot have six lugs on one circle and five on another."""
        found = wheel_size.parse_bolt_patterns("6X135/5.5")
        self.assertEqual([p.display for p in found], ["6x135", "6x5.5"])
        self.assertEqual(found[1].circle_mm, decimal.Decimal("139.7"))

    def test_two_columns_are_merged(self):
        found = wheel_size.parse_bolt_patterns("5-112", "5-120")
        self.assertEqual(len(found), 2)

    def test_duplicates_collapse(self):
        self.assertEqual(len(wheel_size.parse_bolt_patterns("5X4.5", "5X114.3", "5X114")), 1)


class SizeTests(SimpleTestCase):
    databases = []

    def test_diameter_and_width(self):
        self.assertEqual(wheel_size.parse_size("20X9"), (decimal.Decimal("20"), decimal.Decimal("9")))
        self.assertEqual(wheel_size.parse_size("17X8.5"), (decimal.Decimal("17"), decimal.Decimal("8.5")))
        self.assertEqual(wheel_size.parse_size("20x8.25"), (decimal.Decimal("20"), decimal.Decimal("8.25")))

    def test_a_wheel_is_never_wider_than_it_is_tall(self):
        self.assertIsNone(wheel_size.parse_size("9X20"))

    def test_out_of_range_pairs_are_rejected(self):
        for text in ("2X1", "40X30", "100X50"):
            self.assertIsNone(wheel_size.parse_size(text), text)


class OffsetTests(SimpleTestCase):
    databases = []

    def test_signed_offsets(self):
        self.assertEqual(wheel_size.parse_offset_mm("+36"), 36)
        self.assertEqual(wheel_size.parse_offset_mm("-40"), -40)
        self.assertEqual(wheel_size.parse_offset_mm("42MM"), 42)

    def test_zero_is_a_real_offset(self):
        """5,493 Wheel Pros rows are +0. It must survive every falsy check on the way to the row."""
        self.assertEqual(wheel_size.parse_offset_mm("0"), 0)
        self.assertEqual(wheel_size.parse_offset_mm("+0"), 0)
        self.assertIsNotNone(wheel_size.parse_offset_mm("0"))

    def test_backspacing_needs_the_width_to_become_an_offset(self):
        self.assertEqual(wheel_size.parse_backspacing_in("4+3 Offset"), decimal.Decimal("4.3"))
        # 4.3" back-spacing on a 7" wheel: centreline is 4.0", so +0.3" = +8 mm
        self.assertEqual(wheel_size.backspacing_to_offset_mm(decimal.Decimal("4.3"), decimal.Decimal("7")), 8)


class WholeTitleTests(SimpleTestCase):
    databases = []

    def test_real_distributor_titles(self):
        cases = [
            ("WEBB BL UTV 14X7 4X110 +36 80 M-BLK", "14x7", "4x110", 36),
            ("TIARA 22X9 5X110 65 +15 SBLK MCH", "22x9", "5x110", 15),
            ("BR BATONA 18X8 5X112 +30 66 BTL GRAY", "18x8", "5x112", 30),
            ("NOMAD DELUXE 17X8.5 5X5 71 +0 MTL-BLK", "17x8.5", "5x5", 0),
        ]
        for text, size, pattern, offset in cases:
            parsed = wheel_size.parse(text)
            self.assertIsNotNone(parsed, text)
            self.assertEqual(parsed.size_display, size, text)
            self.assertEqual(parsed.bolt_pattern.display, pattern, text)
            self.assertEqual(parsed.offset_mm, offset, text)

    def test_the_size_never_contributes_digits_to_a_bolt_pattern(self):
        """``12x7 / 4/137``: reading across the slash gives "7 / 4", a 7-lug 4-inch pattern that
        does not exist. Skipping the match is not enough -- the scan must not see those digits at
        all, or the real 4/137 that follows becomes unreachable."""
        parsed = wheel_size.parse("ITP Delta Steel 12x7 / 4/137 12mm BP / 4+3 Offset Black Wheel")
        self.assertEqual(parsed.size_display, "12x7")
        self.assertEqual(parsed.bolt_pattern.display, "4x137")

    def test_a_multi_fit_title_yields_both_patterns(self):
        parsed = wheel_size.parse("TOUREN TR60 3260 GLOSS BLACK MACHINED 17X7.5 5-112/5-120 42MM 72.62MM")
        self.assertEqual(parsed.bolt_pattern.display, "5x112")
        self.assertEqual(parsed.bolt_pattern_2.display, "5x120")
        self.assertTrue(parsed.is_dually)

    def test_things_that_are_not_wheels(self):
        for text in (
            'JK2 FALCON 2.1 SP2 SHOCK KIT 3-3.5"',
            "LT285/70R17 121/118Q BAJA BOSS A/T",
            "285/70R17 Ridge Grappler",
            "",
        ):
            self.assertIsNone(wheel_size.parse(text), text)

    def test_a_bolt_pattern_alone_is_not_a_wheel(self):
        """Hubs, adapters and spacers all publish a bolt pattern and there are tens of thousands
        of them. Without a diameter and width there is no wheel."""
        self.assertIsNone(wheel_size.parse("WHEEL ADAPTER 5X114.3 TO 5X120"))


class CenterBoreTests(SimpleTestCase):
    databases = []

    def test_bore(self):
        self.assertEqual(wheel_size.parse_center_bore_mm("72.62"), decimal.Decimal("72.62"))
        self.assertEqual(wheel_size.parse_center_bore_mm("106.1"), decimal.Decimal("106.1"))

    def test_implausible_bores_are_rejected(self):
        for text in ("0", "5", "500"):
            self.assertIsNone(wheel_size.parse_center_bore_mm(text), text)


class QueryParsingTests(SimpleTestCase):
    """
    Turning a search box into filters.

    This is what "20x9 6x4.5" needs to become. Passed to Meilisearch as text it matches nothing at
    all -- the searchable attributes are brand, model and style number, none of which contain a
    size -- so a router that detects a wheel query and then forwards the raw string returns an
    empty page for a query the catalog can answer 130 times over.
    """

    databases = []

    def test_size_and_pattern_become_filters(self):
        parsed = wheel_size.parse_query("20x9 6x4.5")
        self.assertEqual(
            parsed.filters,
            {"diameter_in": 20.0, "width_in": 9.0, "bolt_circle_mm": 114.3, "bolt_lug_count": 6},
        )
        self.assertEqual(parsed.residue, "")

    def test_the_bolt_pattern_filters_on_the_canonical_circle_not_the_spelling(self):
        """A customer types 6x4.5 and the feed published 6x114.3. One circle, two spellings, and
        the index stores whichever its source used -- only the millimetre value finds both."""
        typed_inches = wheel_size.parse_query("20x9 6x4.5").filters
        typed_metric = wheel_size.parse_query("20x9 6x114.3").filters
        self.assertEqual(typed_inches, typed_metric)

    def test_words_the_parser_did_not_claim_stay_as_text(self):
        parsed = wheel_size.parse_query("fuel 20x9 6x135 -12")
        self.assertEqual(parsed.residue, "fuel")
        self.assertEqual(parsed.filters["offset_mm"], -12)
        self.assertEqual(parsed.filters["bolt_circle_mm"], 135.0)

    def test_a_size_alone_is_enough(self):
        parsed = wheel_size.parse_query("20x9")
        self.assertEqual(parsed.filters, {"diameter_in": 20.0, "width_in": 9.0})

    def test_free_text_parses_to_nothing_and_stays_text(self):
        parsed = wheel_size.parse_query("nitto ridge grappler")
        self.assertEqual(parsed.filters, {})
        self.assertEqual(parsed.residue, "nitto ridge grappler")
        self.assertFalse(parsed.parsed_anything)

    def test_empty_input(self):
        self.assertEqual(wheel_size.parse_query("").filters, {})
        self.assertEqual(wheel_size.parse_query(None).residue, "")


class BoltPatternQueryTests(SimpleTestCase):
    """
    A bolt pattern typed on its own.

    ``parse`` refuses it, correctly: a pattern with no size describes a hub or an adapter, not a
    wheel, and that gate is what keeps tens of thousands of them out of the catalog. But a customer
    with a Silverado starts by typing "6x135", so the search box has to ask a question that
    identification never does.
    """

    databases = []

    def test_a_bare_pattern_becomes_a_filter(self):
        parsed = wheel_size.parse_query("6x135")
        self.assertTrue(parsed.parsed_anything)
        self.assertEqual(parsed.filters, {"bolt_circle_mm": 135.0, "bolt_lug_count": 6})
        self.assertEqual(parsed.matched, {"bolt_pattern": "6x135"})

    def test_a_truncated_pattern_finds_the_real_one(self):
        """6x139 is how people type 6x139.7, and the feeds publish that drilling as 6x5.5 as often
        as not. All three have to reach the same wheels."""
        for text in ("6x139", "6x139.7", "6x5.5"):
            self.assertEqual(wheel_size.parse_query(text).filters["bolt_circle_mm"], 139.7, text)

    def test_a_size_still_wins_where_both_could_claim_it(self):
        """ "10x8" is a real 10-inch wheel far more often than a ten-lug pattern on an 8-inch
        circle, and "18x9" is not a pattern at all -- 18 lugs do not exist."""
        self.assertEqual(wheel_size.parse_query("18x9").filters, {"diameter_in": 18.0, "width_in": 9.0})
        self.assertEqual(wheel_size.parse_query("10x8").filters, {"diameter_in": 10.0, "width_in": 8.0})

    def test_a_brand_beside_a_pattern_survives_as_text(self):
        parsed = wheel_size.parse_query("fuel 6x135")
        self.assertEqual(parsed.filters["bolt_circle_mm"], 135.0)
        self.assertEqual(parsed.residue, "fuel")

    def test_something_that_is_not_a_pattern_stays_text(self):
        parsed = wheel_size.parse_query("nitto grappler")
        self.assertFalse(parsed.parsed_anything)
        self.assertEqual(parsed.residue, "nitto grappler")

    def test_an_implausible_pattern_is_not_one(self):
        for text in ("2x50", "12x300"):
            self.assertFalse(wheel_size.parse_query(text).parsed_anything, text)
