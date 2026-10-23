"""
Tests for the Summit Racing applications parser and SKU resolution.

The two HTML fixtures are real pages, trimmed to their first few rows: ``engine_rich`` is a BDS
coil spring (BDX-BDS034451), whose rows carry engine, drivetrain and transmission because coil
spring fitment varies by all three; ``no_engine`` is a bumper (SHI-900-800), whose rows carry
none of them because bumper fitment does not. That difference is the point -- Summit declares a
part's fitment dimensions per part type, so a parser that expects a fixed column set silently
drops engine data on the parts that have it.

``classify_response`` is tested against the shapes Imperva actually returns, all of which arrive
as HTTP 200: treating either as content would record "this part has no fitment" for a part that
was simply blocked.
"""
import pathlib

from django.test import SimpleTestCase

from src.integrations.clients.summit_racing import applications as summit_applications
from src.integrations.clients.summit_racing import client as summit_client
from src.integrations.clients.summit_racing import exceptions as summit_exceptions
from src.integrations.clients.summit_racing import sku_index as summit_sku_index

_FIXTURE_DIR = pathlib.Path(__file__).parent / "fixtures" / "summit_racing"


def _fixture(name: str) -> str:
    return (_FIXTURE_DIR / name).read_text(encoding="utf-8")


class ParseApplicationsTests(SimpleTestCase):
    def test_engine_rich_rows_keep_engine_and_drivetrain(self):
        rows = summit_applications.parse_applications(_fixture("applications_engine_rich.html"))

        self.assertTrue(rows)
        first = rows[0]
        self.assertEqual(first.make, "JEEP")
        self.assertEqual(first.model, "WRANGLER")
        self.assertEqual(first.year_start, 2006)
        self.assertEqual(first.year_end, 2006)
        self.assertEqual(first.submodel, "65TH ANNIVERSARY EDITION")
        # Engine Size is preferred over Engine Type: "4.0L/242" discriminates between
        # applications, "L6" does not.
        self.assertEqual(first.engine, "4.0L/242")
        self.assertEqual(first.drive_type, "4WD")

    def test_no_engine_part_parses_without_engine_fields(self):
        rows = summit_applications.parse_applications(_fixture("applications_no_engine.html"))

        self.assertTrue(rows)
        first = rows[0]
        self.assertEqual(first.make, "CHEVROLET")
        self.assertEqual(first.model, "C1500")
        self.assertEqual(first.engine, "")
        self.assertEqual(first.drive_type, "")
        self.assertTrue(first.is_usable)

    def test_results_found_is_total_across_pages_not_rows_on_page(self):
        html = _fixture("applications_engine_rich.html")

        # The fixture holds 3 rows but the part has 40 applications; the sweep needs the total
        # to know how many pages to request.
        self.assertEqual(summit_applications.parse_results_found(html), 40)
        self.assertLess(len(summit_applications.parse_applications(html)), 40)

    def test_missing_applications_tab_is_none_not_zero(self):
        # None ("not an applications page") and 0 ("no applications") drive different counters:
        # one is a SKU we guessed wrong, the other is a universal-fit part.
        self.assertIsNone(summit_applications.parse_results_found("<html><body>nope</body></html>"))

    def test_universal_rows_are_dropped(self):
        html = (
            '<div class="attribute-container application-item">'
            '<div class="small-12 medium-9 large-6"><strong>Universal:</strong></div>'
            '<div class="small-12 medium-15 large-18">Yes</div>'
            '<div class="small-12 medium-9 large-6"><strong>Make:</strong></div>'
            '<div class="small-12 medium-15 large-18">CHEVROLET</div>'
            '<div class="small-12 medium-9 large-6"><strong>Model:</strong></div>'
            '<div class="small-12 medium-15 large-18">C1500</div>'
            '<div class="small-12 medium-9 large-6"><strong>Beginning Year:</strong></div>'
            '<div class="small-12 medium-15 large-18">1990</div>'
            '<div class="small-12 medium-9 large-6"><strong>Ending Year:</strong></div>'
            '<div class="small-12 medium-15 large-18">1993</div>'
            "</div>"
        )

        # "Universal: Yes" is the absence of a vehicle application, not an application to every
        # vehicle -- storing it would assert fitment against the whole catalog.
        self.assertEqual(summit_applications.parse_applications(html), [])

    def test_row_without_year_is_dropped(self):
        html = (
            '<div class="attribute-container application-item">'
            '<div class="small-12 medium-9 large-6"><strong>Make:</strong></div>'
            '<div class="small-12 medium-15 large-18">JEEP</div>'
            '<div class="small-12 medium-9 large-6"><strong>Model:</strong></div>'
            '<div class="small-12 medium-15 large-18">WRANGLER</div>'
            "</div>"
        )

        self.assertEqual(summit_applications.parse_applications(html), [])

    def test_entities_are_unescaped_and_whitespace_collapsed(self):
        html = (
            '<div class="attribute-container application-item">'
            '<div class="small-12 medium-9 large-6"><strong>Make:</strong></div>'
            '<div class="small-12 medium-15 large-18">\n    MERCEDES-BENZ\n   </div>'
            '<div class="small-12 medium-9 large-6"><strong>Model:</strong></div>'
            '<div class="small-12 medium-15 large-18">SPRINTER 2500</div>'
            '<div class="small-12 medium-9 large-6"><strong>Submodel:</strong></div>'
            '<div class="small-12 medium-15 large-18">BASE &amp; CREW</div>'
            '<div class="small-12 medium-9 large-6"><strong>Beginning Year:</strong></div>'
            '<div class="small-12 medium-15 large-18">2019</div>'
            '<div class="small-12 medium-9 large-6"><strong>Ending Year:</strong></div>'
            '<div class="small-12 medium-15 large-18">2021</div>'
            "</div>"
        )

        row = summit_applications.parse_applications(html)[0]
        self.assertEqual(row.make, "MERCEDES-BENZ")
        self.assertEqual(row.submodel, "BASE & CREW")


class ClassifyResponseTests(SimpleTestCase):
    def test_challenge_body_raises_challenged(self):
        # Imperva serves this with HTTP 200, so status is no signal at all.
        with self.assertRaises(summit_exceptions.SummitRacingChallenged):
            summit_client.classify_response(200, "<html><title>Pardon Our Interruption</title></html>")

    def test_short_body_raises_throttled(self):
        with self.assertRaises(summit_exceptions.SummitRacingThrottled):
            summit_client.classify_response(200, "<html>" + ("x" * 200) + "</html>")

    def test_real_page_passes(self):
        summit_client.classify_response(200, _fixture("applications_engine_rich.html") + "y" * 10_000)

    def test_page_referencing_the_incapsula_sensor_is_not_a_challenge(self):
        # Regression: ordinary Summit pages load Incapsula's sensor from /_Incapsula_Resource.
        # Treating that as a challenge marker stopped the first full sweep after 13 requests on
        # a 188KB page that carried 25 application rows.
        healthy = (
            '<script src="/_Incapsula_Resource?SWJIYLWA=abc123&ns=7"></script>'
            + _fixture("applications_engine_rich.html")
            + "y" * 10_000
        )

        summit_client.classify_response(200, healthy)


class SkuLookupTests(SimpleTestCase):
    def test_resolves_both_spellings_of_a_part_number(self):
        lookup = summit_sku_index.build_lookup(["bdx-bds034451"])

        # Feeds disagree about whether the brand letters belong in the part number; both must
        # land on the same Summit SKU.
        for part_number in ("034451", "BDS034451", "bds-034451"):
            keys = summit_sku_index.candidate_keys_for_part_number(part_number)
            self.assertEqual(
                next((lookup[k] for k in keys if k in lookup), None),
                "bdx-bds034451",
                msg="failed to resolve {!r}".format(part_number),
            )

    def test_unknown_part_number_resolves_to_nothing(self):
        lookup = summit_sku_index.build_lookup(["bdx-bds034451"])
        keys = summit_sku_index.candidate_keys_for_part_number("not-a-real-part")

        self.assertIsNone(next((lookup[k] for k in keys if k in lookup), None))
