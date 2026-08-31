"""
Tests for the wheel spec card.

The derivations are the interesting part -- backspacing and the set-of-four load are computed, not
stored, so they are the two places the card can be silently wrong. The rest of the suite pins the
contract the client depends on: every key present, unknown flags null rather than false.
"""
import decimal

from django.test import SimpleTestCase

from src.domain import wheel_spec_display


def _row(**overrides):
    row = {
        "size_display": "20x9",
        "model_name": "KM100 SYNC",
        "sub_model": None,
        "style_number": "100",
        "diameter_in": decimal.Decimal("20.00"),
        "width_in": decimal.Decimal("9.00"),
        "bolt_lug_count": 6,
        "bolt_circle_mm": decimal.Decimal("135.00"),
        "bolt_pattern_display": "6x135",
        "bolt_lug_count_2": None,
        "bolt_circle_mm_2": None,
        "bolt_pattern_2_display": None,
        "is_blank_drilled": False,
        "offset_mm": 18,
        "backspacing_in": None,
        "center_bore_mm": decimal.Decimal("106.10"),
        "load_rating_lb": 2500,
        "max_psi": None,
        "weight_lb": None,
        "vehicle_class": None,
        "tier": None,
        "style_tags": [],
        "material": None,
        "construction": None,
        "piece_count": None,
        "finish": "SATIN BLACK",
        "finish_family": "black",
        "structural_warranty": None,
        "finish_warranty": None,
        "lug_seat": None,
        "lug_thread_size": None,
        "hub_rings": None,
        "spec_source": "feed",
        "source_feed": "wheelpros",
        "size_disputed": False,
        "enriched_at": None,
        "is_beadlock": None,
        "is_simulated_beadlock": None,
        "is_hub_centric": None,
        "is_directional": None,
        "is_dually": None,
        "tpms_compatible": None,
        "caps_included": None,
        "lugs_included": None,
    }
    row.update(overrides)
    return row


class BackspacingTests(SimpleTestCase):
    """Offset and backspacing are the same fact in two vocabularies, and the card ships both.
    Truck buyers think in backspacing; OE fitment data is published as ET."""

    databases = []

    def test_derived_from_offset_and_width(self):
        # 9/2 + 0.5 + 18/25.4 = 5.71
        self.assertEqual(wheel_spec_display.build_wheel_specs(_row())["backspacing_in"], 5.71)

    def test_negative_offset(self):
        self.assertEqual(wheel_spec_display.build_wheel_specs(_row(offset_mm=-12))["backspacing_in"], 4.53)

    def test_zero_offset_is_not_treated_as_missing(self):
        """+0 is a real and common offset. A falsy check anywhere on this path drops it."""
        card = wheel_spec_display.build_wheel_specs(_row(offset_mm=0, width_in=decimal.Decimal("8.5")))
        self.assertEqual(card["offset_mm"], 0)
        self.assertEqual(card["backspacing_in"], 4.75)

    def test_the_same_offset_is_different_backspacing_on_a_wider_wheel(self):
        """Which is why neither converts without the width, and why a feed publishing one never
        publishes the other."""
        narrow = wheel_spec_display.build_wheel_specs(_row(width_in=decimal.Decimal("8")))["backspacing_in"]
        wide = wheel_spec_display.build_wheel_specs(_row(width_in=decimal.Decimal("10")))["backspacing_in"]
        self.assertNotEqual(narrow, wide)

    def test_a_published_backspacing_wins_over_the_derivation(self):
        """A feed that states it is more authoritative than our arithmetic."""
        card = wheel_spec_display.build_wheel_specs(_row(backspacing_in=decimal.Decimal("6.25")))
        self.assertEqual(card["backspacing_in"], 6.25)

    def test_no_offset_means_no_backspacing(self):
        self.assertIsNone(wheel_spec_display.build_wheel_specs(_row(offset_mm=None))["backspacing_in"])


class RatingTests(SimpleTestCase):
    databases = []

    def test_set_of_four(self):
        self.assertEqual(wheel_spec_display.build_wheel_specs(_row())["set_of_four_load_lb"], 10000)

    def test_no_rating_means_no_set_figure(self):
        card = wheel_spec_display.build_wheel_specs(_row(load_rating_lb=None))
        self.assertIsNone(card["set_of_four_load_lb"])

    def test_max_psi_only_shows_on_a_beadlock(self):
        """A beadlock ring has a pressure limit. On a normal wheel the number means nothing, so
        the row must not render at all."""
        self.assertIsNone(wheel_spec_display.build_wheel_specs(_row(max_psi=35))["max_psi"])
        self.assertEqual(wheel_spec_display.build_wheel_specs(_row(max_psi=35, is_beadlock=True))["max_psi"], 35)


class BoltPatternTests(SimpleTestCase):
    databases = []

    def test_both_vocabularies(self):
        """6x139.7 is what the spec sheet says; 6x5.5 is what a Jeep owner searches for."""
        pattern = wheel_spec_display.build_wheel_specs(
            _row(bolt_circle_mm=decimal.Decimal("139.70"), bolt_pattern_display="6x5.5")
        )["bolt_pattern"]
        self.assertEqual(pattern["circle_mm"], 139.7)
        self.assertEqual(pattern["circle_in"], 5.5)
        self.assertEqual(pattern["display"], "6x5.5")

    def test_the_second_pattern_is_absent_on_a_single_drilled_wheel(self):
        self.assertIsNone(wheel_spec_display.build_wheel_specs(_row())["bolt_pattern_2"])

    def test_a_dual_drilled_wheel_shows_both(self):
        card = wheel_spec_display.build_wheel_specs(
            _row(bolt_lug_count_2=6, bolt_circle_mm_2=decimal.Decimal("139.70"), bolt_pattern_2_display="6x5.5")
        )
        self.assertEqual(card["bolt_pattern_2"]["display"], "6x5.5")

    def test_an_undrilled_wheel_says_so_rather_than_showing_nothing(self):
        card = wheel_spec_display.build_wheel_specs(
            _row(bolt_lug_count=None, bolt_circle_mm=None, bolt_pattern_display=None, is_blank_drilled=True)
        )
        self.assertIsNone(card["bolt_pattern"])
        self.assertIs(card["is_blank_drilled"], True)


class PillTests(SimpleTestCase):
    databases = []

    def test_only_true_flags_become_pills(self):
        card = wheel_spec_display.build_wheel_specs(_row(is_beadlock=True, is_directional=True, is_dually=False))
        self.assertEqual(card["pills"], ["Beadlock", "Directional"])

    def test_nothing_known_means_no_pills(self):
        self.assertEqual(wheel_spec_display.build_wheel_specs(_row())["pills"], [])

    def test_beadlock_and_simulated_beadlock_are_separate(self):
        """A simulated beadlock has the bolts and the look and does not clamp the bead. Someone
        airing down for a trail needs to know which they bought."""
        card = wheel_spec_display.build_wheel_specs(_row(is_simulated_beadlock=True))
        self.assertEqual(card["pills"], ["Simulated beadlock"])
        self.assertIsNone(card["is_beadlock"])


class ContractTests(SimpleTestCase):
    databases = []

    def test_unknown_flags_are_null_never_false(self):
        """A false would tell the customer the wheel is not hub centric when we simply do not
        know, which is worse than saying nothing."""
        card = wheel_spec_display.build_wheel_specs(_row())
        for flag in wheel_spec_display.TRISTATE_FLAGS:
            self.assertIsNone(card[flag], flag)

    def test_every_key_is_present_even_when_empty(self):
        """The client renders a fixed template and blanks nulls; it must never probe for keys."""
        full = wheel_spec_display.build_wheel_specs(_row())
        sparse = wheel_spec_display.build_wheel_specs({"size_display": "17x8"})
        self.assertEqual(set(full), set(sparse))

    def test_labels_accompany_every_code(self):
        card = wheel_spec_display.build_wheel_specs(
            _row(vehicle_class="light_truck", tier="premium", construction="flow_formed", hub_rings="required")
        )
        self.assertEqual(card["vehicle_class_label"], "Light truck")
        self.assertEqual(card["tier_label"], "Premium")
        self.assertEqual(card["construction_label"], "Flow formed")
        self.assertEqual(card["hub_rings_label"], "Required")

    def test_both_finish_strings_ship(self):
        """The card shows the manufacturer's wording; the facet rail groups by the family."""
        card = wheel_spec_display.build_wheel_specs(_row(finish="Matte Black w/ Milled Accents", finish_family="black"))
        self.assertEqual(card["finish"], "Matte Black w/ Milled Accents")
        self.assertEqual(card["finish_family"], "black")

    def test_a_disputed_size_is_surfaced(self):
        """623 wheels have a feed that contradicts its own title. A buyer should not be the one
        who discovers it."""
        self.assertIs(wheel_spec_display.build_wheel_specs(_row(size_disputed=True))["size_disputed"], True)


class BlankContradictionTests(SimpleTestCase):
    """
    A card showed "5x114.3" and a crossed-out "Blank (undrilled)" pill together, which reads as a
    contradiction. The data is fine -- a check constraint forbids a blank from carrying a circle
    and zero rows violate it -- so the fix is to say nothing rather than to say "not blank".
    """

    databases = []

    def test_a_normal_wheel_says_nothing_about_being_blank(self):
        self.assertIsNone(wheel_spec_display.build_wheel_specs(_row())["is_blank_drilled"])

    def test_an_undrilled_wheel_still_says_so(self):
        card = wheel_spec_display.build_wheel_specs(
            _row(is_blank_drilled=True, bolt_lug_count=None, bolt_circle_mm=None)
        )
        self.assertIs(card["is_blank_drilled"], True)
        self.assertIsNone(card["bolt_pattern"])


class DisplayCasingTests(SimpleTestCase):
    """The feeds shout. Raw casing on a product page reads as unprocessed data."""

    databases = []

    def test_shouted_values_get_a_display_form(self):
        card = wheel_spec_display.build_wheel_specs(
            _row(finish="GLOSS BLACK MILLED", lug_seat="CONICAL", finish_family="black")
        )
        self.assertEqual(card["finish"], "GLOSS BLACK MILLED")
        self.assertEqual(card["finish_display"], "Gloss Black Milled")
        self.assertEqual(card["lug_seat_display"], "Conical")
        self.assertEqual(card["finish_family_label"], "Black")

    def test_mixed_case_is_left_as_the_source_wrote_it(self):
        card = wheel_spec_display.build_wheel_specs(_row(finish="Matte Black w/ Milled Accents"))
        self.assertEqual(card["finish_display"], "Matte Black w/ Milled Accents")

    def test_abbreviations_are_not_capitalised_into_words(self):
        self.assertEqual(wheel_spec_display._title_case("OE TPMS UTV"), "OE TPMS UTV")
