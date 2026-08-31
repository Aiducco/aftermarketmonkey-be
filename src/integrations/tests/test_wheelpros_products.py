"""
Tests for ``src.integrations.services.wheelpros_products``.

Pure logic -- no network, no database. The things worth pinning down are the ones where being
wrong is *silent*:

* the partition plan, because the API caps results at 10,000 and reports that cap as if it were
  the total, so an under-partitioned crawl looks like a complete one;
* :func:`fetch_slice`'s short-read check, which is the last line of defence against that; and
* :func:`apply_enrichment`, because it writes over rows the CSV sync owns and must not blank a
  field the API simply does not carry.
"""
import decimal

from django.test import SimpleTestCase
from django.utils import timezone

from src.integrations.services import wheelpros_products as service


class _FakeClient:
    """Stands in for the API: canned facet buckets and pages, and a record of what was asked."""

    requests_made = 0

    def __init__(self, *, total, facets=None, pages=None):
        self._total = total
        self._facets = facets or {}
        self._pages = pages or {}
        self.searches = []

    def true_count(self, kind, **filters):
        return self._total

    def facet_buckets(self, kind, facet_name, **filters):
        return self._facets.get((facet_name, tuple(sorted(filters.items()))), [])

    def search(self, kind, *, page=1, page_size=100, **filters):
        self.searches.append((page, page_size, tuple(sorted(filters.items()))))
        return self._pages.get((page, tuple(sorted(filters.items()))), [])


def _row(sku="AR1", sku_type="WHEEL", **overrides):
    row = {
        "sku": sku,
        "upc": "194933114801",
        "skuType": sku_type,
        "title": "AR172 BAJA 16X8 5X4.75 POL 0MM",
        "brand": {"code": "AR", "description": "American Racing", "parent": "American Racing"},
        "inventory": {"type": "SO", "localStock": 3, "globalStock": 17},
        "properties": {"model": "AR172", "offset": "0.0", "boltPattern": "5X120.65",
                       "finish": "POLISHED", "width": "8.00", "diameter": "16.0",
                       "centerbore": "83.06"},
        "prices": {"msrp": [{"currencyAmount": "302.00", "currencyCode": "USD"}],
                   "map": [{"currencyAmount": None, "currencyCode": "USD"}],
                   "nip": [{"currencyAmount": "211.40", "currencyCode": "USD"}]},
        "images": [
            {"aspect": "Face", "imageUrlLarge": "https://img/face-large.png"},
            {"aspect": "Standard", "imageUrlLarge": "https://img/std-large.png"},
        ],
    }
    row.update(overrides)
    return row


class PlanSlicesTests(SimpleTestCase):
    def test_small_catalogue_is_one_unsliced_query(self):
        client = _FakeClient(total=5934)
        slices = service.plan_slices(client, "tire")
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0].filters, ())
        self.assertEqual(slices[0].expected, 5934)

    def test_large_catalogue_is_split_until_every_slice_fits(self):
        client = _FakeClient(
            total=30000,
            facets={("wheel_diameter", ()): [("20.0", 20000), ("22.0", 10000)],
                    ("width", (("diameter", "20.0"),)): [("9.00", 8000), ("10.00", 8000), ("12.00", 4000)],
                    ("width", (("diameter", "22.0"),)): [("9.00", 6000), ("10.00", 4000)]},
        )
        slices = service.plan_slices(client, "wheel")
        self.assertTrue(all(s.expected <= service.SLICE_TARGET for s in slices), [s.expected for s in slices])
        self.assertEqual(sum(s.expected for s in slices), 30000)

    def test_a_facet_that_misses_rows_does_not_silently_drop_them(self):
        # 30000 rows but the facet only accounts for 12000. Slicing on it would make 18000
        # unreachable, so the level must be left unsliced instead.
        client = _FakeClient(
            total=30000,
            facets={("wheel_diameter", ()): [("20.0", 7000), ("22.0", 5000)]},
        )
        slices = service.plan_slices(client, "wheel", axes=[("diameter", "wheel_diameter")])
        self.assertEqual(len(slices), 1)
        self.assertEqual(slices[0].expected, 30000)

    def test_an_empty_facet_value_is_kept_as_its_own_slice(self):
        # "" cannot be expressed as a filter, so those rows would vanish from the plan.
        client = _FakeClient(
            total=10500,
            facets={("wheel_diameter", ()): [("20.0", 9000), ("", 1500)]},
        )
        slices = service.plan_slices(client, "wheel", axes=[("diameter", "wheel_diameter")])
        self.assertEqual(sum(s.expected for s in slices), 10500)


class FetchSliceTests(SimpleTestCase):
    def test_pages_until_exhausted(self):
        rows = [_row(sku="S{}".format(i)) for i in range(150)]
        client = _FakeClient(total=150, pages={(1, ()): rows[:100], (2, ()): rows[100:]})
        got = service.fetch_slice(client, service.Slice("wheel", (), 150), page_size=100)
        self.assertEqual(len(got), 150)

    def test_short_read_is_an_error_not_a_silent_truncation(self):
        client = _FakeClient(total=500, pages={(1, ()): [_row(sku="S1")]})
        with self.assertRaises(service.WheelProsProductError) as caught:
            service.fetch_slice(client, service.Slice("wheel", (), 500), page_size=100)
        self.assertIn("truncated", str(caught.exception))

    def test_duplicate_skus_across_pages_are_collapsed(self):
        client = _FakeClient(total=1, pages={(1, ()): [_row(sku="S1"), _row(sku="S1")]})
        self.assertEqual(len(service.fetch_slice(client, service.Slice("wheel", (), 1), page_size=100)), 1)

    def test_never_pages_past_the_result_window(self):
        client = _FakeClient(total=1, pages={(p, ()): [_row(sku="S{}".format(p))] for p in range(1, 40)})
        service.fetch_slice(client, service.Slice("wheel", (), 0), page_size=1000)
        self.assertTrue(all(page * size <= service.RESULT_WINDOW for page, size, _ in client.searches))


class BuildPartTests(SimpleTestCase):
    def setUp(self):
        from src import models as src_models
        # Unsaved but real: WheelProsPart.brand is a FK and rejects a stand-in object.
        self.brand = src_models.WheelProsBrand(id=1, external_id="AMERICAN RACING", name="AMERICAN RACING")

    def test_inventory_comes_from_the_api_and_cost_does_not(self):
        part = service.build_part(_row(), brand=self.brand, synced_at=timezone.now())
        self.assertEqual(part.total_qoh, 17)                       # globalStock
        self.assertEqual(part.msrp_usd, decimal.Decimal("302.00"))  # list price is safe to carry
        self.assertIsNone(part.map_usd)                             # null in the payload
        # nip is on api_data for reference, but never becomes a purchase price here.
        self.assertEqual(part.api_data["prices"]["nip"][0]["currencyAmount"], "211.40")

    def test_raw_data_stays_null_so_api_only_rows_are_identifiable(self):
        part = service.build_part(_row(), brand=self.brand, synced_at=timezone.now())
        self.assertIsNone(part.raw_data)
        self.assertIsNotNone(part.api_synced_at)

    def test_sku_type_becomes_the_feed_type_vocabulary_already_in_use(self):
        for sku_type, expected in [("WHEEL", "wheel"), ("TIRE", "tire"), ("ACC", "accessories")]:
            part = service.build_part(_row(sku_type=sku_type), brand=self.brand, synced_at=timezone.now())
            self.assertEqual(part.feed_type, expected)

    def test_unknown_sku_type_is_none_not_a_guess(self):
        part = service.build_part(_row(sku_type="MYSTERY"), brand=self.brand, synced_at=timezone.now())
        self.assertIsNone(part.feed_type)


class PrimaryImageTests(SimpleTestCase):
    def test_prefers_the_standard_aspect(self):
        self.assertEqual(service.primary_image_url(_row()), "https://img/std-large.png")

    def test_falls_back_to_the_first_image(self):
        row = _row(images=[{"aspect": "Face", "imageUrlLarge": "https://img/face.png"}])
        self.assertEqual(service.primary_image_url(row), "https://img/face.png")

    def test_no_images_is_none(self):
        self.assertIsNone(service.primary_image_url(_row(images=[])))


class ApplyEnrichmentTests(SimpleTestCase):
    def _part(self, **kwargs):
        from src import models as src_models
        defaults = dict(part_number="AR1", feed_type="wheel", total_qoh=5,
                        image_url="https://csv/image.png", part_description="CSV description")
        defaults.update(kwargs)
        return src_models.WheelProsPart(**defaults)

    def test_records_the_payload_and_refreshes_stock(self):
        part = self._part()
        changed = service.apply_enrichment(part, _row(), synced_at=timezone.now())
        self.assertIn("api_data", changed)
        self.assertIn("total_qoh", changed)
        self.assertEqual(part.total_qoh, 17)

    def test_backfills_a_missing_feed_type(self):
        part = self._part(feed_type=None)
        changed = service.apply_enrichment(part, _row(sku_type="ACC"), synced_at=timezone.now())
        self.assertIn("feed_type", changed)
        self.assertEqual(part.feed_type, "accessories")

    def test_never_overwrites_a_feed_type_the_csv_already_set(self):
        part = self._part(feed_type="wheel")
        service.apply_enrichment(part, _row(sku_type="ACC"), synced_at=timezone.now())
        self.assertEqual(part.feed_type, "wheel")

    def test_never_blanks_a_csv_value_the_api_lacks(self):
        part = self._part()
        service.apply_enrichment(part, _row(images=[], inventory={}), synced_at=timezone.now())
        self.assertEqual(part.image_url, "https://csv/image.png")
        self.assertEqual(part.total_qoh, 5)
        self.assertEqual(part.part_description, "CSV description")

    def test_fills_an_image_gap_without_replacing_an_existing_one(self):
        part = self._part(image_url="   ")
        changed = service.apply_enrichment(part, _row(), synced_at=timezone.now())
        self.assertIn("image_url", changed)
        self.assertEqual(part.image_url, "https://img/std-large.png")


class TotalQohTests(SimpleTestCase):
    def test_prefers_global_then_local(self):
        self.assertEqual(service.total_qoh(_row()), 17)
        self.assertEqual(service.total_qoh(_row(inventory={"localStock": 4})), 4)

    def test_zero_is_a_real_value(self):
        self.assertEqual(service.total_qoh(_row(inventory={"globalStock": 0})), 0)

    def test_missing_inventory_is_none(self):
        self.assertIsNone(service.total_qoh(_row(inventory={})))
