"""
Tests for ``src.integrations.services.tire_enrichment``.

Pure logic only -- validation, and the submission loop's back-pressure. The LLM and the database
are stubbed; what matters here is that a bad response is rejected rather than repaired, and that a
long run writes as it goes instead of at the end.
"""
import decimal
import unittest.mock as mock

from django.test import SimpleTestCase

from src.domain import tire_size
from src.integrations.services import tire_enrichment

CATEGORIES = frozenset(["MT", "AT", "UHP", "MC_OFFROAD"])


def _candidate(titles=("TRAIL GRAP 35x12.50R17LT 121Q E 34.8",), master_part_id=1):
    parsed = tire_size.parse_best(titles)
    return tire_enrichment.TireCandidate(
        master_part_id=master_part_id,
        brand_id=1,
        brand_name="NITTO",
        product_type=None,
        product_type_source=None,
        titles=list(titles),
        part_numbers=[],
        categories=[],
        structured={},
        parsed=parsed,
        size_variants=[],
    )


class ValidationTests(SimpleTestCase):
    databases = []

    def _ok(self, **overrides):
        payload = {
            "is_tire": True,
            "model_name": "Trail Grappler M/T",
            "tread_category": "MT",
            "confidence": 0.95,
            "reason": "known model",
        }
        payload.update(overrides)
        return payload

    def test_a_size_field_rejects_the_whole_response(self):
        # The canary: a model that has started inventing dimensions cannot be trusted on the
        # fields we did ask for either, so the finding drops everything, not just the bad key.
        validated, reason = tire_enrichment.validate(self._ok(rim_diameter_in=17), _candidate(), CATEGORIES)
        self.assertIsNone(validated)
        self.assertTrue(reason.startswith("size-leak:"))

    def test_a_null_size_field_is_not_a_leak(self):
        validated, reason = tire_enrichment.validate(self._ok(load_index=None), _candidate(), CATEGORIES)
        self.assertIsNotNone(validated, reason)

    def test_an_invented_tread_category_is_rejected_not_repaired(self):
        validated, reason = tire_enrichment.validate(self._ok(tread_category="ALL_TERRAIN"), _candidate(), CATEGORIES)
        self.assertIsNone(validated)
        self.assertIn("unknown-tread-category", reason)

    def test_low_confidence_3pmsf_is_downgraded_to_unknown(self):
        # A certification with legal weight: a maybe is worth less than an unknown.
        validated, _ = tire_enrichment.validate(self._ok(is_3pmsf=True, confidence=0.6), _candidate(), CATEGORIES)
        self.assertIsNone(validated.is_3pmsf)

    def test_confident_3pmsf_is_kept(self):
        validated, _ = tire_enrichment.validate(self._ok(is_3pmsf=True, confidence=0.95), _candidate(), CATEGORIES)
        self.assertIs(validated.is_3pmsf, True)

    def test_a_model_name_echoing_a_title_is_dropped(self):
        title = "TRAIL GRAP 35x12.50R17LT 121Q E 34.8"
        validated, _ = tire_enrichment.validate(self._ok(model_name=title), _candidate((title,)), CATEGORIES)
        self.assertIsNone(validated.model_name)

    def test_out_of_vocabulary_tier_drops_the_field_not_the_response(self):
        # Measured: 8 of 1,581 responses answered tier="performance". Rejecting outright threw
        # away 8 correct identifications over a field nothing depends on.
        validated, reason = tire_enrichment.validate(self._ok(tier="performance"), _candidate(), CATEGORIES)
        self.assertIsNotNone(validated, reason)
        self.assertIsNone(validated.tier)
        self.assertEqual(validated.model_name, "Trail Grappler M/T")

    def test_is_tire_false_short_circuits(self):
        validated, _ = tire_enrichment.validate({"is_tire": False}, _candidate(), CATEGORIES)
        self.assertFalse(validated.is_tire)

    def test_missing_is_tire_is_rejected(self):
        validated, reason = tire_enrichment.validate({"model_name": "x"}, _candidate(), CATEGORIES)
        self.assertIsNone(validated)
        self.assertEqual(reason, "missing-is_tire")

    def test_confidence_is_clamped(self):
        validated, _ = tire_enrichment.validate(self._ok(confidence=1.7), _candidate(), CATEGORIES)
        self.assertEqual(validated.confidence, decimal.Decimal("1.00"))


class SubmissionBackPressureTests(SimpleTestCase):
    databases = []

    def test_the_candidate_stream_is_not_drained_before_the_first_write(self):
        """
        Regression: ``run`` used ``ThreadPoolExecutor.map``, which drains its whole iterable up
        front. On a full-catalog run that meant no row was written until the entire 3.17M-row scan
        had finished -- 12 minutes in, zero rows -- with every future held in memory and a crash
        losing all of it. Submission must be bounded so writes stay incremental.
        """
        pulled = []
        write_points = []

        def candidates():
            for i in range(500):
                pulled.append(i)
                yield _candidate(master_part_id=i + 1)

        def fake_write(specs, not_a_tire_ids, stats):
            # Record how much of the stream had been consumed at each write.
            write_points.append(len(pulled))
            stats.written += len(specs)

        response = {
            "is_tire": True,
            "model_name": "Trail Grappler M/T",
            "tread_category": "MT",
            "confidence": 0.95,
            "reason": "ok",
        }

        with mock.patch.object(tire_enrichment, "iter_candidates", lambda **kw: candidates()), mock.patch.object(
            tire_enrichment, "write_batch", fake_write
        ), mock.patch.object(tire_enrichment, "LookupTables") as lookups, mock.patch.object(
            tire_enrichment.azure_llm, "client", return_value=object()
        ), mock.patch.object(
            tire_enrichment.azure_llm, "deployment", return_value="test"
        ), mock.patch.object(
            tire_enrichment.azure_llm, "complete_json", return_value=(response, None)
        ), mock.patch.object(
            tire_enrichment, "build_system_prompt", return_value="sys"
        ):
            lookups.return_value.tread_categories = CATEGORIES
            lookups.return_value.resolve.return_value = {"max_load_lb": None, "max_speed_mph": None, "ply_rating": None}
            stats = tire_enrichment.run(apply_changes=True, max_workers=4, write_batch_size=50)

        self.assertEqual(stats.written, 500)
        self.assertTrue(write_points, "nothing was ever written")
        # The first write must land long before the generator is exhausted.
        self.assertLess(
            write_points[0],
            500,
            "the whole candidate stream was consumed before the first write -- back-pressure is gone",
        )
