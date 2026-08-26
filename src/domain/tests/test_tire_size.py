"""
Unit tests for ``src.domain.tire_size``.

``SimpleTestCase`` with ``databases = []`` on purpose: the parser is pure, and a test that
needs a database is a test nobody runs before a rule change. Every fixture below is a real
string taken from ``master_parts.description`` or ``provider_parts.product_details`` in
production, not an invented one -- the whole point of this parser is surviving what
distributors actually write.

Run with:  ./venv/bin/python manage.py test src.domain
"""
import decimal

from django.test import SimpleTestCase

from src.domain import tire_size


class ParseMetricTests(SimpleTestCase):
    databases = []

    def test_plain_passenger_size(self):
        parsed = tire_size.parse("MOTIVO 365 225/45R19 96W XL 26.97")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.notation, tire_size.NOTATION_METRIC)
        self.assertEqual(parsed.size_display, "225/45R19")
        self.assertIsNone(parsed.service_type)
        self.assertEqual(parsed.section_width_mm, 225)
        self.assertEqual(parsed.aspect_ratio, 45)
        self.assertEqual(parsed.rim_diameter_in, decimal.Decimal("19"))
        self.assertEqual(parsed.load_index, 96)
        self.assertEqual(parsed.speed_rating, "W")
        self.assertEqual(parsed.load_range, "XL")

    def test_overall_diameter_is_computed_not_read(self):
        # Wheel Pros appends its own diameter (28.98); ours is derived from the size alone and
        # must land on it independently, which is what makes the appended figure a free check.
        parsed = tire_size.parse("TER GRAP G3 235/70R16 109/T XL 28.98")
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("29.0"))
        self.assertEqual(parsed.load_index, 109)
        self.assertEqual(parsed.speed_rating, "T")

    def test_light_truck_prefix(self):
        parsed = tire_size.parse("TER GRAP G3 LT225/75R16 115/Q E 29.53")
        self.assertEqual(parsed.service_type, "LT")
        self.assertEqual(parsed.size_display, "LT225/75R16")
        self.assertEqual(parsed.load_index, 115)
        self.assertEqual(parsed.speed_rating, "Q")
        # E is a load range here, not the 70 km/h speed symbol -- position decides.
        self.assertEqual(parsed.load_range, "E")

    def test_slash_between_load_index_and_speed_is_not_a_dual_load(self):
        parsed = tire_size.parse("NOMAD GRAP 215/60R17 100/H XL 27.17")
        self.assertEqual(parsed.load_index, 100)
        self.assertIsNone(parsed.load_index_dual)
        self.assertEqual(parsed.speed_rating, "H")

    def test_dual_load_index(self):
        parsed = tire_size.parse("LT245/75R16 120/116S E")
        self.assertEqual(parsed.load_index, 120)
        self.assertEqual(parsed.load_index_dual, 116)
        self.assertEqual(parsed.speed_rating, "S")

    def test_zr_is_construction_not_speed_rating(self):
        parsed = tire_size.parse("NT555 G2 275/40ZR20 106W XL")
        self.assertEqual(parsed.construction, tire_size.CONSTRUCTION_ZR)
        self.assertEqual(parsed.speed_rating, "W")
        self.assertEqual(parsed.size_display, "275/40ZR20")

    def test_hyphen_is_bias_construction(self):
        parsed = tire_size.parse("NT-SN1 205/55-16 91H 24.9")
        self.assertEqual(parsed.construction, tire_size.CONSTRUCTION_BIAS)
        # Rendered with the hyphen the feed wrote; construction still records the D.
        self.assertEqual(parsed.size_display, "205/55-16")
        self.assertEqual(parsed.load_index, 91)

    def test_space_separator_from_premier(self):
        parsed = tire_size.parse("Nitto NT90W 275 45R19 103T 28.74 N213-120")
        self.assertEqual(parsed.size_display, "275/45R19")
        self.assertEqual(parsed.load_index, 103)
        self.assertEqual(parsed.speed_rating, "T")

    def test_half_inch_commercial_rim(self):
        parsed = tire_size.parse("245/70R19.5 136/134M G")
        self.assertEqual(parsed.rim_diameter_in, decimal.Decimal("19.5"))
        self.assertEqual(parsed.size_display, "245/70R19.5")

    def test_motorcycle_two_digit_width(self):
        # The car pattern cannot express a 2-digit width; a separate, tightly-bounded pattern can.
        parsed = tire_size.parse("90/90-21 54H")
        self.assertEqual(parsed.size_display, "90/90-21")
        self.assertEqual(parsed.section_width_mm, 90)
        self.assertEqual(parsed.aspect_ratio, 90)
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("27.4"))

    def test_motorcycle_aspect_of_one_hundred(self):
        parsed = tire_size.parse("Kenda K772 Parker DT Rear Tire - 120/100-18 6PR 68M TT")
        self.assertEqual(parsed.size_display, "120/100-18")
        self.assertEqual(parsed.aspect_ratio, 100)

    def test_two_digit_width_traps_are_rejected(self):
        # This is why the motorcycle pattern has a 60mm width floor and a 60% aspect floor.
        for text in ("10/30 SYNTHETIC OIL 5QT", "50/50 SPLIT BENCH SEAT", "SHOCK 60/40 VALVING"):
            self.assertIsNone(tire_size.parse(text), text)

    def test_glued_load_range_marker(self):
        # "R19XL" -- the XL is stamped flush against the rim, with no separator.
        parsed = tire_size.parse("MOTIVO 365 275/35R19XL 100W 26.57")
        self.assertEqual(parsed.size_display, "275/35R19")
        self.assertEqual(parsed.load_range, "XL")
        self.assertEqual(parsed.load_index, 100)

    def test_bare_z_is_the_same_marker_as_zr(self):
        parsed = tire_size.parse("MS932 XP+ 255/35Z-18 94W 25")
        self.assertEqual(parsed.construction, tire_size.CONSTRUCTION_ZR)
        self.assertEqual(parsed.speed_rating, "W")

    def test_tirerack_leading_size_with_noise(self):
        parsed = tire_size.parse("285/70R17~~ NI RIDGE GRAPPLER")
        self.assertEqual(parsed.size_display, "285/70R17")
        self.assertIsNone(parsed.load_index)


class ParseFlotationTests(SimpleTestCase):
    databases = []

    def test_flotation_with_trailing_service_type(self):
        parsed = tire_size.parse("RECON GRAP 33x11.50R16LT 124R 32.5")
        self.assertEqual(parsed.notation, tire_size.NOTATION_FLOTATION)
        self.assertEqual(parsed.size_display, "33X11.50R16LT")
        self.assertEqual(parsed.service_type, "LT")
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("33.0"))
        self.assertEqual(parsed.section_width_in, decimal.Decimal("11.50"))
        self.assertEqual(parsed.rim_diameter_in, decimal.Decimal("16"))
        self.assertEqual(parsed.load_index, 124)
        self.assertEqual(parsed.speed_rating, "R")

    def test_overall_diameter_is_taken_from_the_string(self):
        parsed = tire_size.parse("TER GRAP G3 35x12.50R17 128/R F 34.53")
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("35.0"))
        self.assertEqual(parsed.load_range, "F")

    def test_leading_service_type(self):
        # Mickey Thompson writes the service type in front; Nitto writes it behind. Same field.
        parsed = tire_size.parse("BAJ BOSS AT LT33X12.50-20 114Q 32.8")
        self.assertEqual(parsed.service_type, "LT")
        self.assertEqual(parsed.size_display, "33X12.50-20LT")
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("33.0"))

    def test_bias_flotation_renders_with_a_hyphen_not_a_d(self):
        # Inch notation writes bias as "-"; only metric uses the letter D. construction stays "D".
        parsed = tire_size.parse("AT 33X8-18 6PR  BKT AT171 TL 33818")
        self.assertEqual(parsed.construction, tire_size.CONSTRUCTION_BIAS)
        self.assertEqual(parsed.size_display, "33X8-18")

    def test_drag_racing_slash_flotation(self):
        parsed = tire_size.parse("ET FRONT 27.5/4.0-17 27.8")
        self.assertEqual(parsed.notation, tire_size.NOTATION_FLOTATION)
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("27.5"))
        self.assertEqual(parsed.section_width_in, decimal.Decimal("4.00"))
        self.assertEqual(parsed.rim_diameter_in, decimal.Decimal("17"))

    def test_single_digit_rim(self):
        parsed = tire_size.parse("PRO-RIDER 18X8.50-8 18")
        self.assertEqual(parsed.rim_diameter_in, decimal.Decimal("8"))

    def test_reversed_drag_size_is_rejected_not_transposed(self):
        # 14.5/32.0-15 states width first. Rather than guess at the intent, a 32" section width
        # fails the bounds check and the row goes unparsed.
        self.assertIsNone(tire_size.parse("ET DRAG 14.5/32.0-15 33"))

    def test_unicode_multiplication_sign(self):
        parsed = tire_size.parse("TRAIL GRAP 37×12.50R18LT 128Q E 36.7")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.size_display, "37X12.50R18LT")
        self.assertEqual(parsed.load_range, "E")


class ParseNumericTests(SimpleTestCase):
    databases = []

    def test_conventional_numeric(self):
        parsed = tire_size.parse("FIRESTONE DELUXE CHAMPION 7.50-16")
        self.assertEqual(parsed.notation, tire_size.NOTATION_NUMERIC)
        self.assertEqual(parsed.size_display, "7.50-16")
        self.assertEqual(parsed.section_width_in, decimal.Decimal("7.50"))
        # Nominal, from section height == section width. See the module docstring.
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("31.0"))

    def test_numeric_requires_a_decimal_point(self):
        # "750-16" is a part number as often as it is a size, so it is left unparsed.
        self.assertIsNone(tire_size.parse("SOME PART 750-16"))


class RejectionTests(SimpleTestCase):
    databases = []

    def test_wheel_size_with_bolt_pattern(self):
        self.assertIsNone(tire_size.parse("ARROW 20X10.5 5X112 66.5 RBL +40 BMW CAP"))

    def test_wheel_size_with_negative_offset(self):
        self.assertIsNone(tire_size.parse("FLUX 15X6 6X5.5 +0 G-BLK-DDT"))

    def test_wheel_offset_is_not_a_rim_diameter(self):
        # 20X12-44 is a 20x12 wheel at -44 offset. Read as flotation it would be a 20" tire on
        # a 44" rim, which the tire-taller-than-rim and rim-bounds guards both reject.
        self.assertIsNone(tire_size.parse("HOSTAGE 20X12-44 8X170"))

    def test_inch_bolt_pattern_rejects_the_whole_row(self):
        # 4.79-18 is a plausible numeric size on its own; the 5X4.75 bolt pattern earlier in the
        # line is what says this row is a wheel.
        self.assertIsNone(tire_size.parse("20X10 NITROUS X SE 5X4.75 P 4.79-18 72"))

    def test_offset_is_not_a_bias_construction(self):
        # "20X9 -12MM" is a wheel insert at -12mm offset. The space in front of the hyphen and
        # the MM behind the number both disqualify it.
        self.assertIsNone(tire_size.parse("XD811 FINS 20X9 -12MM RED (5PK)"))

    def test_a_spring_rate_is_not_a_tire_size(self):
        # Real production row. 4.5-10.5 is a progressive spring rate; a 10.5" rim does not exist
        # in numeric notation, which is what the half-inch rule encodes.
        self.assertIsNone(
            tire_size.parse("Ohlins 06-09 Kawasaki VN 900 Classic Fork Springs - Prog. 4.5-10.5 N/mm")
        )

    def test_commercial_half_inch_rims_still_parse(self):
        self.assertEqual(tire_size.parse("245/70R19.5 136/134M G").rim_diameter_in, decimal.Decimal("19.5"))
        self.assertEqual(tire_size.parse("225/70R16.5 LRE").rim_diameter_in, decimal.Decimal("16.5"))

    def test_empty_and_none(self):
        self.assertIsNone(tire_size.parse(None))
        self.assertIsNone(tire_size.parse(""))
        self.assertIsNone(tire_size.parse("NI TRAIL GRAPPLER DISP ON WEB"))

    def test_size_glued_to_a_stray_operator_is_a_fragment(self):
        self.assertIsNone(tire_size.parse("NITTO TRAIL GRAPPLER M/T 37+12.5R18LT"))

    def test_lug_nut_description_is_not_a_size(self):
        self.assertIsNone(tire_size.parse("LUG NUT 14X1.5 BULGE ACORN 60 DEG"))


class ParseBestTests(SimpleTestCase):
    databases = []

    def test_richest_title_wins(self):
        best = tire_size.parse_best(
            [
                "285/70R17~~ NI RIDGE GRAPPLER",
                "NITTO RIDGE GRAPPLER 285/70R17",
                "RIDGE GRAP 285/70R17 116Q SL 32.7",
            ]
        )
        self.assertEqual(best.size_display, "285/70R17")
        self.assertEqual(best.load_index, 116)
        self.assertEqual(best.speed_rating, "Q")
        self.assertEqual(best.load_range, "SL")

    def test_unparseable_titles_are_skipped(self):
        best = tire_size.parse_best(["NI TRAIL GRAPPLER DISP ON WEB", None, "LT285/70R17 121Q E"])
        self.assertEqual(best.size_display, "LT285/70R17")

    def test_a_weak_notation_never_outranks_a_strong_one(self):
        # Real row: the distributor typed "+" where an "x" belonged. The numeric pattern can find
        # "12.5R18LT" inside that (a 12.5" tire on an 18" rim -> 43" tall) and it carries a
        # service type, so on field count alone it would beat the correct flotation reading of
        # the sibling title. Notation trust has to decide this, not field count.
        best = tire_size.parse_best(
            [
                "37X12.50R18 NI TRL GRAPLER MT",
                "NITTO TRAIL GRAPPLER M/T 37+12.5R18LT",
            ]
        )
        self.assertEqual(best.notation, tire_size.NOTATION_FLOTATION)
        self.assertEqual(best.size_display, "37X12.50R18")
        self.assertEqual(best.overall_diameter_in, decimal.Decimal("37.0"))

    def test_no_parseable_title(self):
        self.assertIsNone(tire_size.parse_best(["WIDGET", None]))

    def test_service_type_alone_is_not_a_disagreement(self):
        # Rough Country omits the LT that Wheel Pros writes. Same tire, two spellings.
        self.assertEqual(
            len(
                tire_size.disagreements(
                    [
                        "35x12.50R17 Nitto Trail Grappler M/T",
                        "TRAIL GRAP 35x12.50R17LT 121Q E 34.8",
                    ]
                )
            ),
            1,
        )

    def test_disagreement_is_reported_not_resolved(self):
        self.assertEqual(
            tire_size.disagreements(["275/70R18 116T", "285/70R17 116Q"]),
            ["275/70R18", "285/70R17"],
        )
        self.assertEqual(
            tire_size.disagreements(["275/70R18 116T", "275/70R18~~ NI TERRA GRAP G3"]),
            ["275/70R18"],
        )


class ParseQueryTests(SimpleTestCase):
    """
    ``parse_query`` is what a search box uses. It accepts everything ``parse`` does plus the
    separator-free forms a person types. The separation is the point: the same three numbers
    that mean a size in a search box are routinely a part number in a distributor title.
    """

    databases = []

    def test_loose_forms_all_agree_with_the_strict_one(self):
        expected = tire_size.parse("275/70R18")
        for text in ("275 70 18", "2757018", "275/70/18"):
            parsed = tire_size.parse_query(text)
            self.assertIsNotNone(parsed, text)
            self.assertEqual(parsed.size_display, expected.size_display, text)
            self.assertEqual(parsed.overall_diameter_in, expected.overall_diameter_in, text)

    def test_loose_flotation(self):
        parsed = tire_size.parse_query("35 12.50 20")
        self.assertEqual(parsed.notation, tire_size.NOTATION_FLOTATION)
        self.assertEqual(parsed.overall_diameter_in, decimal.Decimal("35.0"))

    def test_lr_prefixed_load_range(self):
        parsed = tire_size.parse_query("LT265/70R17 121/118S LRE")
        self.assertEqual(parsed.load_index, 121)
        self.assertEqual(parsed.load_index_dual, 118)
        self.assertEqual(parsed.speed_rating, "S")
        self.assertEqual(parsed.load_range, "E")

    def test_bare_width_assumes_82_series_and_says_so(self):
        parsed = tire_size.parse_query("275R18")
        self.assertEqual(parsed.aspect_ratio, 82)
        self.assertTrue(parsed.aspect_assumed)
        # The display must not show the guess as if it were stamped on the sidewall.
        self.assertEqual(parsed.size_display, "275R18")

    def test_bare_width_stays_out_of_the_catalog_parser(self):
        # Accepting this in parse() would put part numbers in the tire index.
        self.assertIsNone(tire_size.parse("275R18"))

    def test_the_three_rejects(self):
        for text in ("275718", "6x139.7", "17x9", "4981910571360"):
            self.assertIsNone(tire_size.parse_query(text), text)

    def test_residue_is_cut_by_span(self):
        text = "nitto ridge grappler 275/70R18"
        self.assertEqual(tire_size.residue(text, tire_size.parse_query(text)), "nitto ridge grappler")

    def test_residue_of_an_unparseable_query_is_the_whole_query(self):
        self.assertEqual(tire_size.residue("ridge  grappler", None), "ridge grappler")
