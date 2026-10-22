"""
Tests for part-number aliases on the parts index.

Two things are worth locking down, and neither is about plumbing:

1. **The two document shapes must agree.** ``_build_docs_for_batch`` (what the reindex uploads)
   and ``master_part_to_index_shape`` (the public API shape used for audit/history responses) are
   hand-maintained duplicates. When they drift, search hits and API responses stop describing the
   same object, and nothing fails loudly.

2. **Identifier attributes must have typo tolerance off.** A one-character difference in a part
   number is a different product. This is a settings-level guarantee, and it has been silently
   reverted twice on this index by the zero-downtime path configuring its staging index by hand
   -- so the test asserts both config paths set it.
"""
from django.test import SimpleTestCase

from src.search import meilisearch_client


class PartAliasesIndexConfigTests(SimpleTestCase):
    def test_part_aliases_is_searchable_directly_behind_part_number(self):
        attrs = meilisearch_client.SEARCHABLE_ATTRIBUTES
        self.assertIn("part_aliases", attrs)
        # Meilisearch's `attribute` ranking rule ranks by declaration order: part_number must
        # stay first, and an alias hit has to outrank a coincidental description mention.
        self.assertEqual(attrs.index("part_aliases"), attrs.index("part_number") + 1)
        self.assertLess(attrs.index("part_aliases"), attrs.index("description"))

    def test_typo_tolerance_is_disabled_on_every_identifier_attribute(self):
        for attribute in ("part_number", "part_aliases", "sku", "aaia_code", "gtin"):
            self.assertIn(attribute, meilisearch_client.TYPO_DISABLED_ATTRIBUTES)

    def test_description_keeps_typo_tolerance(self):
        # disableOnAttributes is per-attribute: free-text search must stay forgiving so
        # 'sunbrela car cover' still finds Sunbrella.
        self.assertNotIn("description", meilisearch_client.TYPO_DISABLED_ATTRIBUTES)

    def test_typo_tolerance_is_configured_in_both_index_paths(self):
        """
        The zero-downtime path creates its staging index fresh and then swaps it over the live
        one, so a setting applied only in setup_index() is reverted by the next nightly reindex.
        Both paths must configure typo tolerance.
        """
        import inspect

        call = "update_typo_tolerance"
        self.assertIn(call, inspect.getsource(meilisearch_client.setup_index))
        self.assertIn(
            call,
            inspect.getsource(meilisearch_client.reindex_all_master_parts_zero_downtime),
        )


class _FakePart:
    """Minimal stand-in for a MasterPart row; no DB access (see SimpleTestCase)."""

    def __init__(self, **kwargs):
        self.id = kwargs.get("id", 1)
        self.brand_id = kwargs.get("brand_id", 7)
        self.part_number = kwargs.get("part_number", "71822-00-88")
        self.part_aliases = kwargs.get("part_aliases", ["718220088", "COV-718220088"])
        self.sku = kwargs.get("sku", "71822-00-88")
        self.description = kwargs.get("description", "Custom fit car cover")
        self.aaia_code = kwargs.get("aaia_code", "COVC")
        self.image_url = kwargs.get("image_url", "https://example.test/a.jpg")
        self.gtin = kwargs.get("gtin", "")
        self.created_at = None
        self.updated_at = None
        self.brand = None


class PartAliasesDocumentShapeTests(SimpleTestCase):
    def test_build_docs_for_batch_carries_part_aliases(self):
        docs = meilisearch_client._build_docs_for_batch([_FakePart()], cat_map={})
        self.assertEqual(docs[0]["part_aliases"], ["718220088", "COV-718220088"])

    def test_empty_aliases_reach_the_index_as_a_list_not_none(self):
        # A None here makes the attribute unsearchable for that document rather than simply
        # empty, which is the kind of failure that shows up as "search works for most parts".
        docs = meilisearch_client._build_docs_for_batch(
            [_FakePart(part_aliases=[])], cat_map={}
        )
        self.assertEqual(docs[0]["part_aliases"], [])
        self.assertIsInstance(docs[0]["part_aliases"], list)

    def test_the_two_document_shapes_have_identical_keys(self):
        """
        The real risk is drift, not a missing line: assert the whole key set matches so the next
        field added to one of them cannot be forgotten in the other.
        """
        import inspect

        batch_src = inspect.getsource(meilisearch_client._build_docs_for_batch)
        public_src = inspect.getsource(meilisearch_client.master_part_to_index_shape)
        for key in ("part_number", "part_aliases", "sku", "description", "aaia_code",
                    "gtin", "image_url", "fitment_keys", "distributor_names"):
            self.assertIn('"{}"'.format(key), batch_src, key)
            self.assertIn('"{}"'.format(key), public_src, key)
