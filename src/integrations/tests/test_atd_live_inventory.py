"""
Tests for ``src.integrations.live_inventory.atd``.

The payloads below are verbatim from ATD's ``/product/product-availability``, called with
SunCoast's own credentials from the production host on 2026-10-27 -- note what they do NOT
contain: no ``availability`` wrapper and no ``sthinventory`` key, both of which the catalog
sweep's ``productcatalog`` response does have.

The ProviderPartInventory merge itself (this company's key updated, every other company's left
alone, warehouse_total_qty recomputed as the max nationwide) was verified live against production
with a second company's entry seeded in, rather than reproduced here against a fixture -- see the
commit message. What is covered here is everything that can fail silently without a DB.
"""
from django.test import SimpleTestCase

from src.integrations.clients.atd import exceptions as atd_exceptions
from src.integrations.live_inventory import atd as atd_live
from src.integrations.live_inventory import exceptions as live_exceptions

# Real response row, product 001081 (Bridgestone), location 3036915.
LIVE_PRODUCT = {
    "local": 0,
    "localplus": 11,
    "nationwide": 9,
    "onhand": 0,
    "mfgproductnumber": "001081",
    "atdproductnumber": "001081",
    "brand": "Bridgestone",
}

# How the CATALOG endpoint shapes the same information. Passing one of these to the live provider
# is the mistake these tests exist to catch.
CATALOG_SHAPED_PRODUCT = {
    "atdproductnumber": "001081",
    "availability": {"local": 0, "localplus": 11, "nationwide": 9, "onhand": 0, "sthinventory": 20},
}


class BucketReadingTests(SimpleTestCase):
    databases = []

    def test_buckets_are_read_flat_off_the_product(self):
        self.assertEqual(
            atd_live._buckets_from(LIVE_PRODUCT),
            {"local": 0, "localplus": 11, "nationwide": 9, "onhand": 0},
        )

    def test_sthinventory_is_not_among_the_buckets_this_endpoint_reports(self):
        """
        It is absent from every response -- confirmed against parts whose stored values were 15,
        20, 145 and 250. Treating it as a reported bucket would write a zero over a real number
        that the product page renders as "Ship-to-Home Eligible".
        """
        self.assertNotIn("sthinventory", atd_live._buckets_from(LIVE_PRODUCT))
        self.assertNotIn("sthinventory", dict(atd_live._BUCKETS))
        self.assertEqual(
            [api_key for api_key, _ in atd_live._BUCKETS],
            ["local", "localplus", "nationwide", "onhand"],
        )

    def test_a_catalog_shaped_payload_yields_zeros_rather_than_silently_right_numbers(self):
        """
        Documents the trap rather than endorsing it: if this ever starts returning 11 for
        localplus, the two endpoints have converged and the module docstring is stale.
        """
        self.assertEqual(
            atd_live._buckets_from(CATALOG_SHAPED_PRODUCT),
            {"local": 0, "localplus": 0, "nationwide": 0, "onhand": 0},
        )

    def test_missing_and_junk_values_become_zero_not_none(self):
        # The columns are IntegerField; a None would be stored as NULL and read as "unknown".
        self.assertEqual(
            atd_live._buckets_from({"local": None, "localplus": "11", "nationwide": "x"}),
            {"local": 0, "localplus": 11, "nationwide": 0, "onhand": 0},
        )


class _Part:
    def __init__(self, number="001081"):
        self.atdproductnumber = number


class _ProviderPart:
    id = 1
    def __init__(self, external_id="001081"):
        self.provider_external_id = external_id


class _CompanyProvider:
    id = 180
    company_id = 67


class ErrorMappingTests(SimpleTestCase):
    """
    The product page branches on these two types: NotFound shows "no stock on file" for this one
    part, Transport shows a broken-connection state for the whole integration. Conflating them
    tells a dealer their ATD connection is down because one tire is not carried.
    """
    databases = []

    def _provider(self, raising):
        provider = atd_live.ATDLiveInventoryProvider.__new__(atd_live.ATDLiveInventoryProvider)
        provider.company_provider = _CompanyProvider()

        class _Client:
            def get_product_availability(self, numbers):
                raise raising

        provider._client = _Client()
        provider._atd_part_for = lambda pp: _Part()
        return provider

    def test_product_access_denied_is_a_not_found_not_a_credential_failure(self):
        """
        ATD answers HTTP 460 {"errorMessage": "product-access-denied"} when it will not report on
        any requested product, and the client turns every errorMessage into ATDAuthError. The
        credentials are fine -- ATD just does not carry that item for this location.
        """
        provider = self._provider(atd_exceptions.ATDAuthError("product-access-denied", 460))
        with self.assertRaises(live_exceptions.LiveInventoryNotFoundError):
            provider.refresh(_ProviderPart())

    def test_the_match_is_case_and_whitespace_tolerant(self):
        provider = self._provider(atd_exceptions.ATDAuthError("  Product-Access-Denied  ", 460))
        with self.assertRaises(live_exceptions.LiveInventoryNotFoundError):
            provider.refresh(_ProviderPart())

    def test_a_real_credential_failure_stays_a_transport_error(self):
        """
        The location-related errorMessages arrive by the same ATDAuthError route and must NOT be
        softened into "this part has no stock" -- the connection genuinely needs attention.
        """
        for message in (
            "does not have access to this location",
            "Please enter a valid location",
            "ATD rejected the provided credentials.",
        ):
            with self.subTest(message=message):
                provider = self._provider(atd_exceptions.ATDAuthError(message, 401))
                with self.assertRaises(live_exceptions.LiveInventoryTransportError):
                    provider.refresh(_ProviderPart())

    def test_transport_failures_are_transport_errors(self):
        for error in (
            atd_exceptions.ATDConnectionError("timed out"),
            atd_exceptions.ATDAPIBadResponseCodeError("boom", 500),
            atd_exceptions.ATDAPIException("unparseable"),
        ):
            with self.subTest(error=type(error).__name__):
                provider = self._provider(error)
                with self.assertRaises(live_exceptions.LiveInventoryTransportError):
                    provider.refresh(_ProviderPart())


class ResultMatchingTests(SimpleTestCase):
    databases = []

    def _provider(self, returning):
        provider = atd_live.ATDLiveInventoryProvider.__new__(atd_live.ATDLiveInventoryProvider)
        provider.company_provider = _CompanyProvider()

        class _Client:
            def get_product_availability(self, numbers):
                return returning

        provider._client = _Client()
        provider._atd_part_for = lambda pp: _Part()
        provider._write = lambda pp, part, product: product  # bypass the ORM
        return provider

    def test_the_matching_product_is_selected_by_number(self):
        other = dict(LIVE_PRODUCT, atdproductnumber="999999", nationwide=4242)
        product = self._provider([other, LIVE_PRODUCT]).refresh(_ProviderPart())
        self.assertEqual(product["atdproductnumber"], "001081")
        self.assertEqual(product["nationwide"], 9)

    def test_an_omitted_product_is_a_not_found(self):
        """
        A mixed request returns 200 and silently drops the inaccessible product rather than
        erroring, so an empty/other-only result is a real not-found, not a transport problem.
        """
        for returning in ([], None, [dict(LIVE_PRODUCT, atdproductnumber="999999")]):
            with self.subTest(returning=returning):
                with self.assertRaises(live_exceptions.LiveInventoryNotFoundError):
                    self._provider(returning).refresh(_ProviderPart())

    def test_a_provider_part_with_no_external_id_is_a_not_found(self):
        provider = atd_live.ATDLiveInventoryProvider.__new__(atd_live.ATDLiveInventoryProvider)
        provider.company_provider = _CompanyProvider()
        with self.assertRaises(live_exceptions.LiveInventoryNotFoundError):
            provider.refresh(_ProviderPart(external_id="  "))
