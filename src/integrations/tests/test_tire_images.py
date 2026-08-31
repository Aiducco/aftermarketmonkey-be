"""
Tests for the tire image backfill.

This is the only part of the tire pipeline that writes to ``master_parts``, so the tests are about
what it refuses: placeholders, and any column other than ``image_url``.
"""
from django.test import SimpleTestCase

from src.integrations.services import tire_images


class PlaceholderTests(SimpleTestCase):
    databases = []

    def test_the_generic_sidewall_is_refused(self):
        """SimpleTire serves one stand-in image for tires nobody photographed, and it is attached
        to 4,792 SKUs across 339 unrelated brands. Copying it would put the same picture on parts
        that have nothing to do with each other."""
        self.assertTrue(
            tire_images.is_placeholder(
                "https://images.simpletire.com/images/line-images/_generic/sidewall/sample-tire.png"
            )
        )

    def test_common_placeholder_spellings_are_refused(self):
        for url in (
            "https://cdn.example.com/no-image.png",
            "https://cdn.example.com/NoImage.JPG",
            "https://cdn.example.com/assets/placeholder.webp",
        ):
            self.assertTrue(tire_images.is_placeholder(url), url)

    def test_a_real_product_image_is_accepted(self):
        for url in (
            "https://images.simpletire.com/images/q_auto/line-images/3413/3413-sidetread/nitto-ridge-grappler.png",
            "https://api.ridestyler.net/Resource/Download/public_c15f1d6b51664111a6e20e57.png?width=800",
        ):
            self.assertFalse(tire_images.is_placeholder(url), url)

    def test_missing_is_treated_as_unusable(self):
        self.assertTrue(tire_images.is_placeholder(None))
        self.assertTrue(tire_images.is_placeholder(""))

    def test_many_sizes_sharing_one_image_is_normal(self):
        """A tire photograph is a property of the model, not the size -- roughly 24 SKUs to a
        picture. Only the generic asset is filtered, never a repeated real one."""
        url = "https://images.simpletire.com/images/q_auto/line-images/3413/3413-sidetread/x.png"
        self.assertFalse(tire_images.is_placeholder(url))
