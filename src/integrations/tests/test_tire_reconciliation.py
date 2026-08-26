"""
Tests for ``src.integrations.services.tire_reconciliation``.

The interesting cases are all about what must NOT be merged. Reconciliation rewrites every size
of a model, so an over-eager rule silently relabels hundreds of SKUs at once.
"""
from django.test import SimpleTestCase

from src.integrations.services import tire_reconciliation as tr


class ModelKeyTests(SimpleTestCase):
    databases = []

    def test_case_and_punctuation_collapse(self):
        self.assertEqual(tr.model_key("Recon Grappler A/T"), tr.model_key("recon grappler at"))
        self.assertEqual(tr.model_key("Invo"), tr.model_key("INVO"))

    def test_generations_stay_apart(self):
        # The whole reason the key is exact-modulo-punctuation rather than fuzzy.
        self.assertNotEqual(tr.model_key("Terra Grappler G2"), tr.model_key("Terra Grappler G3"))
        self.assertNotEqual(tr.model_key("Motivo"), tr.model_key("Motivo 365"))
        self.assertNotEqual(tr.model_key("NT05"), tr.model_key("NT05R"))
        self.assertNotEqual(tr.model_key("P Zero"), tr.model_key("P Zero Winter"))


class BrandlessKeyTests(SimpleTestCase):
    databases = []

    def test_a_redundant_brand_prefix_is_stripped(self):
        self.assertEqual(tr.brandless_key("Nokian One", "NOKIAN"), tr.brandless_key("One", "NOKIAN"))
        self.assertEqual(tr.brandless_key("Firestone FT140", "FIRESTONE"), tr.brandless_key("FT140", "FIRESTONE"))
        self.assertEqual(
            tr.brandless_key("Milestar Patagonia A/T", "MILESTAR"), tr.brandless_key("Patagonia A/T", "MILESTAR")
        )

    def test_a_model_named_after_its_brand_keeps_its_name(self):
        # Stripping here would reduce the key to nothing and merge it with everything.
        self.assertEqual(tr.brandless_key("Nokian", "NOKIAN"), tr.model_key("Nokian"))

    def test_it_does_not_merge_different_products(self):
        # A brand prefix is the ONLY thing it removes; it is not a general prefix rule.
        self.assertNotEqual(tr.brandless_key("P Zero", "PIRELLI"), tr.brandless_key("P Zero Winter", "PIRELLI"))
        self.assertNotEqual(
            tr.brandless_key("Pilot Sport 4", "MICHELIN"), tr.brandless_key("Pilot Sport 4 S", "MICHELIN")
        )

    def test_a_brand_appearing_mid_name_is_not_stripped(self):
        self.assertEqual(tr.brandless_key("Super Nokian Thing", "NOKIAN"), tr.model_key("Super Nokian Thing"))


class CategoryVoteTests(SimpleTestCase):
    databases = []

    def _vote(self, counts, conf=None):
        return tr.CategoryVote(brand_id=1, model_name="X", counts=counts, mean_confidence=conf or {})

    def test_winner_and_share(self):
        vote = self._vote({"AT": 19, "RT": 27})
        self.assertEqual(vote.winner, "RT")
        self.assertAlmostEqual(vote.winner_share, 27 / 46)
        self.assertTrue(vote.is_split)

    def test_a_tie_is_broken_deterministically(self):
        # Deterministic so a rerun does not flip-flop -- but it is still a tie, and the command's
        # --min-agreement guard is what stops it being written.
        vote = self._vote({"TRACK": 6, "UHP": 6}, {"TRACK": 0.9, "UHP": 0.95})
        self.assertEqual(vote.winner, "UHP")
        self.assertEqual(vote.winner_share, 0.5)

    def test_unanimous_is_not_split(self):
        self.assertFalse(self._vote({"MT": 40}).is_split)
