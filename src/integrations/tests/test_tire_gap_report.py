"""
Tests for the catalog gap classifier.

The classifier's whole job is to tell four different problems apart, because they need four
different responses. The test that matters most is the co-occurrence one: an earlier version
checked size and model separately against the brand's whole inventory and reported 4,825 tires as
matchable when the real figure was 20.
"""
from django.test import SimpleTestCase

from src.integrations.services import tire_gap_report as gaps
from src.integrations.services.tire_catalog import brand_key, canonical_size, model_key


def _catalog(*skus):
    """(brand, size, model) triples -> the three indexes ``classify`` reads."""
    sizes, models, triples = {}, {}, {}
    for brand, size, model in skus:
        key, canon = brand_key(brand), canonical_size(size)
        sizes.setdefault(key, set()).add(canon)
        models.setdefault(key, set()).add(model_key(model))
        triples[(key, model_key(model), canon)] = triples.get((key, model_key(model), canon), 0) + 1
    return {"sizes": sizes, "models": models, "triples": triples}


class ClassifyTests(SimpleTestCase):
    databases = []

    def test_brand_nobody_carries(self):
        result = gaps.classify(
            brand="VREDESTEIN",
            size_display="285/70R17",
            model_name="Wintrac",
            **_catalog(("Nitto", "285/70R17", "Ridge Grappler")),
        )
        self.assertEqual(result, gaps.REASON_NO_BRAND)

    def test_size_we_cannot_read(self):
        result = gaps.classify(
            brand="Nitto",
            size_display="not a size",
            model_name="Ridge Grappler",
            **_catalog(("Nitto", "285/70R17", "Ridge Grappler")),
        )
        self.assertEqual(result, gaps.REASON_BAD_SIZE)

    def test_brand_carried_but_not_the_size(self):
        result = gaps.classify(
            brand="Nitto",
            size_display="225/65R16",
            model_name="Ridge Grappler",
            **_catalog(("Nitto", "285/70R17", "Ridge Grappler")),
        )
        self.assertEqual(result, gaps.REASON_NO_SIZE)

    def test_size_carried_but_not_the_model(self):
        result = gaps.classify(
            brand="Nitto",
            size_display="285/70R17",
            model_name="Terra Grappler",
            **_catalog(("Nitto", "285/70R17", "Ridge Grappler")),
        )
        self.assertEqual(result, gaps.REASON_NO_MODEL)

    def test_size_and_model_both_carried_but_never_together(self):
        """The correction that matters. The brand has the size, and has the model, but no single
        product has both -- so nothing is matchable and reporting otherwise sends you looking for
        a matcher bug that does not exist."""
        catalog = _catalog(
            ("Nitto", "285/70R17", "Ridge Grappler"),
            ("Nitto", "225/65R16", "Terra Grappler"),
        )
        result = gaps.classify(brand="Nitto", size_display="285/70R17", model_name="Terra Grappler", **catalog)
        self.assertEqual(result, gaps.REASON_NO_MODEL)

    def test_several_skus_share_the_key(self):
        catalog = _catalog(
            ("Nitto", "285/70R17", "Ridge Grappler"),
            ("Nitto", "285/70R17", "Ridge Grappler"),
        )
        result = gaps.classify(brand="Nitto", size_display="285/70R17", model_name="Ridge Grappler", **catalog)
        self.assertEqual(result, gaps.REASON_AMBIGUOUS)

    def test_exactly_one_sku_means_the_matcher_should_have_found_it(self):
        result = gaps.classify(
            brand="Nitto",
            size_display="285/70R17",
            model_name="Ridge Grappler",
            **_catalog(("Nitto", "285/70R17", "Ridge Grappler")),
        )
        self.assertEqual(result, gaps.REASON_MATCHABLE)

    def test_a_brand_alias_is_resolved_before_anything_else(self):
        """We say YOKOHAMA TIRE, both catalogs say Yokohama. Without the alias every one of those
        tires would be reported as a brand nobody carries."""
        result = gaps.classify(
            brand="YOKOHAMA TIRE",
            size_display="285/70R17",
            model_name="Geolandar",
            **_catalog(("Yokohama", "285/70R17", "Geolandar")),
        )
        self.assertEqual(result, gaps.REASON_MATCHABLE)
