"""
Tests for building wheel specs from the four distributor feeds.

Two things are worth pinning here. The **size gate**, because a feed declaring a row a wheel is not
enough on its own -- Vossen ships centre caps in the same table with diameter 0. And **one spec per
master part**, because two feeds describing the same wheel must resolve by a stated rule rather
than by whichever ran last.
"""
import decimal

from django.test import SimpleTestCase

from src.integrations.services import wheel_enrichment


def _row(**overrides):
    row = {
        "master_part_id": 1,
        "master_description": None,
        "part_number": "KM10029050318",
        "title": "KM100 SYNC 20X9 6X135 +18 106.1 SATIN BLACK",
        "size_raw": "20X9",
        "diameter_raw": None,
        "width_raw": None,
        "bolt_pattern_1": "6X135",
        "bolt_pattern_2": None,
        "offset_raw": "18",
        "center_bore_raw": "106.10",
        "load_rating_raw": "2500",
        "backspace_raw": None,
        "weight_raw": None,
        "finish_raw": "SATIN BLACK",
        "model_raw": "KM100 SYNC",
        "style_number_raw": "100",
        "lug_seat_raw": None,
        "lug_thread_raw": None,
        "structural_warranty_raw": None,
        "finish_warranty_raw": None,
        "tpms_raw": None,
        "dually_raw": None,
        "image_url": None,
    }
    row.update(overrides)
    return row


class SizeGateTests(SimpleTestCase):
    databases = []

    def test_a_wheel_is_built(self):
        spec = wheel_enrichment.build_spec(_row(), feed="wheelpros")
        self.assertEqual(spec.size_display, "20x9")
        self.assertEqual(spec.bolt_pattern_display, "6x135")
        self.assertEqual(spec.offset_mm, 18)

    def test_a_centre_cap_is_rejected(self):
        """119 of Vossen's 3,263 rows are caps and hardware, published with diameter 0 in the same
        table as the wheels. The feed saying 'wheel' does not make them one."""
        spec = wheel_enrichment.build_spec(
            _row(
                size_raw=None,
                diameter_raw="0",
                width_raw="0.00",
                bolt_pattern_1=None,
                title="BILLET SPORT CAP 2.0 - SMALL - HYBRID FORGED",
            ),
            feed="vossen",
        )
        self.assertIsNone(spec)

    def test_separate_diameter_and_width_columns(self):
        """Vossen and The Wheel Group publish the two as their own columns rather than one string."""
        spec = wheel_enrichment.build_spec(
            _row(size_raw=None, diameter_raw="19", width_raw="8.50", title=""), feed="vossen"
        )
        self.assertEqual(spec.size_display, "19x8.5")

    def test_zero_offset_survives(self):
        spec = wheel_enrichment.build_spec(_row(offset_raw="0", title=""), feed="wheelpros")
        self.assertEqual(spec.offset_mm, 0)


class FeedFieldTests(SimpleTestCase):
    databases = []

    def test_the_wheel_group_extras_land(self):
        spec = wheel_enrichment.build_spec(
            _row(
                lug_seat_raw="CONICAL",
                lug_thread_raw="BOLT-M8-20BLK",
                tpms_raw="YES",
                weight_raw="27",
                backspace_raw="5.9",
                dually_raw="NO",
                structural_warranty_raw="LIFETIME WARRANTY",
                finish_warranty_raw="LIMITED 1 YEAR WARRANTY",
            ),
            feed="thewheelgroup",
        )
        self.assertEqual(spec.lug_seat, "CONICAL")
        self.assertIs(spec.tpms_compatible, True)
        self.assertEqual(spec.weight_lb, decimal.Decimal("27"))
        self.assertEqual(spec.backspacing_in, decimal.Decimal("5.9"))

    def test_warranty_casing_is_unified(self):
        """The same warranty ships in two casings -- 1,545 rows shout it and 322 do not."""
        shouted = wheel_enrichment.build_spec(_row(finish_warranty_raw="LIMITED 1 YEAR WARRANTY"), feed="thewheelgroup")
        quiet = wheel_enrichment.build_spec(_row(finish_warranty_raw="Limited 1 Year Warranty"), feed="thewheelgroup")
        self.assertEqual(shouted.finish_warranty, quiet.finish_warranty)
        self.assertEqual(shouted.finish_warranty, "Limited 1 Year Warranty")

    def test_a_boolean_column_reads_yes_and_no(self):
        self.assertIs(wheel_enrichment._boolean("YES"), True)
        self.assertIs(wheel_enrichment._boolean("NO"), False)
        self.assertIsNone(wheel_enrichment._boolean(None))
        self.assertIsNone(wheel_enrichment._boolean("MAYBE"))

    def test_a_second_bolt_pattern_column_is_read(self):
        spec = wheel_enrichment.build_spec(
            _row(bolt_pattern_1="5-112", bolt_pattern_2="5-120", title=""), feed="thewheelgroup"
        )
        self.assertEqual(spec.bolt_pattern_display, "5x112")
        self.assertEqual(spec.bolt_pattern_2_display, "5x120")

    def test_nothing_is_invented_for_a_thin_feed(self):
        """Vossen publishes dimensions and nothing else. Those fields must stay NULL rather than
        being guessed from the title."""
        spec = wheel_enrichment.build_spec(
            _row(load_rating_raw=None, finish_raw=None, lug_seat_raw=None, title=""), feed="vossen"
        )
        for field in ("load_rating_lb", "finish", "finish_family", "lug_seat", "structural_warranty"):
            self.assertIsNone(getattr(spec, field), field)


class PrecedenceTests(SimpleTestCase):
    databases = []

    def test_every_feed_has_a_rank(self):
        """A feed missing from FEED_ORDER would make collision resolution depend on dict order."""
        self.assertEqual(set(wheel_enrichment.FEED_ORDER), set(wheel_enrichment.FEEDS))

    def test_the_richest_feed_outranks_the_others(self):
        """The Wheel Group is the only source publishing lug seat, both warranties, TPMS and a
        real weight, so where two describe one wheel it should win."""
        order = list(wheel_enrichment.FEED_ORDER)
        self.assertEqual(order[0], wheel_enrichment.FEED_THEWHEELGROUP)
        self.assertLess(order.index("thewheelgroup"), order.index("wheelpros"))
        self.assertLess(order.index("wheelpros"), order.index("vossen"))

    def test_the_write_set_never_includes_a_column_the_merge_does_not_own(self):
        """master_part_id is the conflict target, not an updatable column, and the LLM-facing
        fields are not this module's to write."""
        for field in ("master_part_id", "id", "created_at", "llm_confidence", "llm_reason"):
            self.assertNotIn(field, wheel_enrichment.WRITE_FIELDS, field)


class FeedSqlTests(SimpleTestCase):
    databases = []

    def test_every_feed_selects_the_same_vocabulary(self):
        """One shared column list is what keeps build_spec free of a branch per source."""
        for name, feed in wheel_enrichment.FEEDS.items():
            for column in ("master_part_id", "size_raw", "bolt_pattern_1", "offset_raw", "part_number"):
                self.assertIn(column, feed.sql, "{} is missing {}".format(name, column))

    def test_every_feed_deduplicates_by_master_part(self):
        """Without DISTINCT ON, a batch can hold one master_part_id twice and Postgres rejects the
        whole write: 'ON CONFLICT DO UPDATE command cannot affect row a second time'."""
        for name, feed in wheel_enrichment.FEEDS.items():
            self.assertIn("DISTINCT ON (mp.id)", feed.sql, name)
            self.assertIn("ORDER BY mp.id", feed.sql, name)

    def test_offset_is_quoted_where_it_is_a_column(self):
        """``offset`` is a reserved word in Postgres; selected unquoted it is a syntax error, and
        every one of these four feeds happens to have a column by that name."""
        quoted = 'f."offset"'
        for name, feed in wheel_enrichment.FEEDS.items():
            self.assertIn(quoted, feed.sql, "{} selects offset without quoting it".format(name))


class PlaceholderTests(SimpleTestCase):
    databases = []

    def test_a_distributor_placeholder_is_not_a_value(self):
        """The Wheel Group ships a literal "NONE" in its screw column on 820 rows, which rendered
        on the detail card as "Lug thread size: NONE"."""
        spec = wheel_enrichment.build_spec(_row(lug_thread_raw="NONE"), feed="thewheelgroup")
        self.assertIsNone(spec.lug_thread_size)

    def test_the_usual_suspects(self):
        for text in ("NONE", "N/A", "NA", "NULL", "-", "", "  ", "TBD", "unknown"):
            self.assertIsNone(wheel_enrichment._clean(text), repr(text))

    def test_a_real_value_survives(self):
        self.assertEqual(wheel_enrichment._clean("M14 x 1.5"), "M14 x 1.5")


class VehicleClassTests(SimpleTestCase):
    databases = []

    def _class(self, pattern, title=""):
        return wheel_enrichment.build_spec(
            _row(bolt_pattern_1=pattern, title=title, size_raw="20X8.25"), feed="wheelpros"
        ).vehicle_class

    def test_patterns_only_one_kind_of_vehicle_uses(self):
        self.assertEqual(self._class("8X200"), "commercial")
        self.assertEqual(self._class("10X225"), "commercial")
        self.assertEqual(self._class("4X137"), "atv_utv")

    def test_a_trailing_zero_does_not_break_the_lookup(self):
        """format(Decimal("200"), "f").rstrip("0") is "2". That silently left 8x200 and 8x210
        unlabelled while 10x225 worked, because 225 ends in a non-zero digit."""
        for pattern in ("8X200", "8X210", "10X225"):
            self.assertIsNotNone(self._class(pattern), pattern)

    def test_an_ambiguous_pattern_is_left_alone(self):
        """5x114.3 and 6x139.7 span cars, crossovers and trucks. Mapping them would have labelled
        11,236 wheels on a guess."""
        self.assertIsNone(self._class("5X114.3"))
        self.assertIsNone(self._class("6X139.7"))
        self.assertIsNone(self._class("6X135"))

    def test_the_title_still_wins_for_utv(self):
        self.assertEqual(self._class("5X114.3", title="SHREDDER SS UTV 22X7"), "atv_utv")
