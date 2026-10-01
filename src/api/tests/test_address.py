"""
Tests for ``src.api.services.address`` -- the frontend-facing side: opaque session-scoped
suggestion ids, the cache that makes /resolve free, the rate limiter, and the rule that
nothing here may ever surface a provider failure to the user.

Uses a locmem cache rather than the project's Redis so the suite needs no running Redis.
"""
import typing
import unittest.mock as mock

from django.test import SimpleTestCase, override_settings

from common import exceptions as common_exceptions
from common import utils as common_utils
from src import enums as src_enums
from src.api.schemas import address as address_schemas
from src.api.services import address as address_services
from src.integrations.address import base
from src.integrations.address import exceptions as address_exceptions

_LOCMEM_CACHE = {
    "default": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
        "LOCATION": "address-tests",
    }
}

_SESSION = "11111111-1111-1111-1111-111111111111"
_OTHER_SESSION = "22222222-2222-2222-2222-222222222222"

_US_ADDRESS = base.Address(
    line1="1600 Amphitheatre Parkway",
    city="Mountain View",
    state="CA",
    postal_code="94043",
    country="US",
)


class _StubProvider:
    name = "stub"

    def __init__(
        self,
        suggestions: typing.Optional[typing.List[base.Suggestion]] = None,
        resolved: typing.Optional[base.Address] = None,
        validation: typing.Optional[base.ValidationResult] = None,
        raises: typing.Optional[Exception] = None,
    ) -> None:
        self._suggestions = suggestions or []
        self._resolved = resolved
        self._validation = validation
        self._raises = raises
        self.suggest_calls: typing.List[dict] = []
        self.resolve_calls: typing.List[dict] = []

    def suggest(self, q, country, session):
        self.suggest_calls.append({"q": q, "country": country, "session": session})
        if self._raises:
            raise self._raises
        return self._suggestions

    def resolve(self, provider_ref, session):
        self.resolve_calls.append({"provider_ref": provider_ref, "session": session})
        if self._raises:
            raise self._raises
        return self._resolved

    def validate(self, address):
        if self._raises:
            raise self._raises
        return self._validation


def _patch_provider(provider) -> typing.Any:
    return mock.patch.object(address_services.registry, "get_provider", return_value=provider)


def _clear_cache() -> None:
    from django.core.cache import cache

    cache.clear()


@override_settings(CACHES=_LOCMEM_CACHE, ADDRESS_SUGGEST_LIMIT=5, ADDRESS_SUGGEST_RATE_LIMIT_PER_MINUTE=0)
class SuggestAndResolveTest(SimpleTestCase):
    def setUp(self):
        _clear_cache()

    def test_ids_are_opaque_and_resolve_from_cache_without_a_second_provider_call(self):
        provider = _StubProvider(
            suggestions=[
                base.Suggestion(label="1600 Amphitheatre Parkway, …", provider_ref="place-abc", address=_US_ADDRESS)
            ]
        )

        with _patch_provider(provider):
            suggestions = address_services.suggest_addresses(q="1600 Amph", country="US", session=_SESSION)

            self.assertEqual(len(suggestions), 1)
            suggestion_id = suggestions[0]["id"]
            # The provider's own id must never reach the browser.
            self.assertNotEqual(suggestion_id, "place-abc")
            self.assertNotIn("place", suggestion_id)
            self.assertEqual(suggestions[0]["label"], "1600 Amphitheatre Parkway, …")

            resolved = address_services.resolve_address(suggestion_id=suggestion_id, session=_SESSION)

        self.assertEqual(resolved, _US_ADDRESS.as_dict())
        # Resolve was served entirely from cache.
        self.assertEqual(provider.resolve_calls, [])

    def test_each_hit_gets_its_own_id(self):
        provider = _StubProvider(
            suggestions=[
                base.Suggestion(label="a", provider_ref="p1", address=_US_ADDRESS),
                base.Suggestion(label="b", provider_ref="p2", address=_US_ADDRESS),
            ]
        )

        with _patch_provider(provider):
            suggestions = address_services.suggest_addresses(q="1600 Amph", country="US", session=_SESSION)

        self.assertEqual(len({s["id"] for s in suggestions}), 2)

    @override_settings(ADDRESS_SUGGEST_LIMIT=2)
    def test_caps_the_number_of_suggestions(self):
        provider = _StubProvider(
            suggestions=[
                base.Suggestion(label=str(i), provider_ref="p{}".format(i), address=_US_ADDRESS)
                for i in range(10)
            ]
        )

        with _patch_provider(provider):
            suggestions = address_services.suggest_addresses(q="1600 Amph", country="US", session=_SESSION)

        self.assertEqual(len(suggestions), 2)

    def test_expired_or_unknown_id_resolves_to_none(self):
        # Nothing was ever cached under this id -- the same state the cache is in once the
        # 10-minute TTL has passed.
        with _patch_provider(_StubProvider()):
            self.assertIsNone(
                address_services.resolve_address(suggestion_id="deadbeef", session=_SESSION)
            )

    def test_an_id_from_another_session_does_not_resolve(self):
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="a", provider_ref="p1", address=_US_ADDRESS)]
        )

        with _patch_provider(provider):
            suggestion_id = address_services.suggest_addresses(
                q="1600 Amph", country="US", session=_SESSION
            )[0]["id"]

            self.assertIsNone(
                address_services.resolve_address(suggestion_id=suggestion_id, session=_OTHER_SESSION)
            )

    def test_label_only_provider_falls_back_to_a_details_call(self):
        # The shape a future Google adapter has: autocomplete gives a label, the address needs
        # a second call.
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="1600 Amphitheatre Pkwy", provider_ref="place-abc")],
            resolved=_US_ADDRESS,
        )

        with _patch_provider(provider):
            suggestion_id = address_services.suggest_addresses(
                q="1600 Amph", country="US", session=_SESSION
            )[0]["id"]
            resolved = address_services.resolve_address(suggestion_id=suggestion_id, session=_SESSION)

        self.assertEqual(resolved, _US_ADDRESS.as_dict())
        self.assertEqual(provider.resolve_calls, [{"provider_ref": "place-abc", "session": _SESSION}])

    def test_cache_write_failure_yields_an_empty_list_and_never_escapes(self):
        """
        The exact production failure this guards: an unreachable Redis made
        /api/address/suggest/ answer 200 {"suggestions": []} for every query while
        /api/address/validate/ kept working, because validate touches no cache. The empty list
        is still the right response -- an id nobody can resolve would give the user a dropdown
        that does nothing -- but the error must not propagate, and it must be logged.
        """
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="a", provider_ref="p1", address=_US_ADDRESS)]
        )

        with _patch_provider(provider):
            with mock.patch.object(
                address_services.cache, "set", side_effect=RuntimeError("redis down")
            ):
                with self.assertLogs(address_services.logger, level="ERROR") as captured:
                    result = address_services.suggest_addresses(
                        q="1600 Amph", country="US", session=_SESSION
                    )

        self.assertEqual(result, [])
        self.assertIn("CACHE WRITE FAILED", "\n".join(captured.output))

    def test_cache_write_failure_does_not_log_the_address_text(self):
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="1600 Amphitheatre Parkway", provider_ref="p1", address=_US_ADDRESS)]
        )

        with _patch_provider(provider):
            with mock.patch.object(
                address_services.cache, "set", side_effect=RuntimeError("redis down")
            ):
                with self.assertLogs(address_services.logger, level="ERROR") as captured:
                    address_services.suggest_addresses(q="1600 Amph", country="US", session=_SESSION)

        logged = "\n".join(captured.output)
        self.assertNotIn("Amphitheatre", logged)
        self.assertNotIn("94043", logged)

    def test_provider_timeout_yields_an_empty_list_not_an_error(self):
        provider = _StubProvider(raises=address_exceptions.AddressProviderTimeout("too slow"))

        with _patch_provider(provider):
            self.assertEqual(
                address_services.suggest_addresses(q="1600 Amph", country="US", session=_SESSION), []
            )

    def test_no_provider_configured_yields_an_empty_list(self):
        with _patch_provider(None):
            self.assertEqual(
                address_services.suggest_addresses(q="1600 Amph", country="US", session=_SESSION), []
            )

    @override_settings(ADDRESS_SUGGEST_DEFAULT_COUNTRY="US")
    def test_omitted_country_falls_back_to_the_configured_default(self):
        """
        An unfiltered Geoapify lookup is up to 13x slower and can exceed the request timeout
        outright, surfacing as an empty dropdown. The form always has a COUNTRY value, so a
        request without one gets the configured default rather than a planet-wide search.
        """
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="a", provider_ref="p1", address=_US_ADDRESS)]
        )

        with _patch_provider(provider):
            address_services.suggest_addresses(q="1600 Amph", country=None, session=_SESSION)

        self.assertEqual(provider.suggest_calls[0]["country"], "US")

    @override_settings(ADDRESS_SUGGEST_DEFAULT_COUNTRY="US")
    def test_an_explicit_country_always_wins_over_the_default(self):
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="a", provider_ref="p1", address=_US_ADDRESS)]
        )

        with _patch_provider(provider):
            address_services.suggest_addresses(q="Jovana", country="me", session=_SESSION)

        self.assertEqual(provider.suggest_calls[0]["country"], "ME")

    @override_settings(ADDRESS_SUGGEST_DEFAULT_COUNTRY="")
    def test_blank_default_means_no_country_filter(self):
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="a", provider_ref="p1", address=_US_ADDRESS)]
        )

        with _patch_provider(provider):
            address_services.suggest_addresses(q="1600 Amph", country=None, session=_SESSION)

        self.assertIsNone(provider.suggest_calls[0]["country"])

    @override_settings(ADDRESS_ALLOWED_COUNTRIES=["US", "CA"], ADDRESS_SUGGEST_DEFAULT_COUNTRY="US")
    def test_country_we_do_not_ship_to_is_not_looked_up_at_all(self):
        provider = _StubProvider(
            suggestions=[base.Suggestion(label="a", provider_ref="p1", address=_US_ADDRESS)]
        )

        with _patch_provider(provider):
            self.assertEqual(
                address_services.suggest_addresses(q="Jovana", country="ME", session=_SESSION), []
            )
            self.assertEqual(
                len(address_services.suggest_addresses(q="1600 Amph", country="us", session=_SESSION)), 1
            )

        self.assertEqual(len(provider.suggest_calls), 1)


@override_settings(CACHES=_LOCMEM_CACHE)
class ValidateTest(SimpleTestCase):
    def setUp(self):
        _clear_cache()

    def test_valid_result_has_no_suggested_key(self):
        provider = _StubProvider(
            validation=base.ValidationResult(status=src_enums.AddressValidationStatus.VALID)
        )

        with _patch_provider(provider):
            result = address_services.validate_address(_US_ADDRESS.as_dict())

        self.assertEqual(result, {"status": "valid", "messages": []})

    def test_corrected_result_carries_the_suggested_address(self):
        suggested = base.Address(
            line1="1600 Amphitheatre Parkway",
            city="Mountain View",
            state="CA",
            postal_code="94043",
            country="US",
        )
        provider = _StubProvider(
            validation=base.ValidationResult(
                status=src_enums.AddressValidationStatus.CORRECTED,
                suggested=suggested,
                messages=("Unit number may be missing",),
            )
        )

        with _patch_provider(provider):
            result = address_services.validate_address(dict(_US_ADDRESS.as_dict(), postal_code="94044"))

        self.assertEqual(result["status"], "corrected")
        self.assertEqual(result["suggested"], suggested.as_dict())
        self.assertEqual(result["messages"], ["Unit number may be missing"])

    def test_provider_error_degrades_to_unverified(self):
        provider = _StubProvider(raises=address_exceptions.AddressProviderError("boom"))

        with _patch_provider(provider):
            result = address_services.validate_address(_US_ADDRESS.as_dict())

        self.assertEqual(result["status"], "unverified")
        self.assertTrue(result["messages"])

    def test_no_provider_configured_degrades_to_unverified(self):
        with _patch_provider(None):
            result = address_services.validate_address(_US_ADDRESS.as_dict())

        self.assertEqual(result["status"], "unverified")

    def test_address_text_is_never_logged(self):
        provider = _StubProvider(raises=address_exceptions.AddressProviderError("boom"))

        with _patch_provider(provider):
            with self.assertLogs(address_services.logger, level="WARNING") as captured:
                address_services.validate_address(_US_ADDRESS.as_dict())

        logged = "\n".join(captured.output)
        self.assertNotIn("Amphitheatre", logged)
        self.assertNotIn("94043", logged)


@override_settings(CACHES=_LOCMEM_CACHE)
class SuggestRateLimitTest(SimpleTestCase):
    def setUp(self):
        _clear_cache()

    @override_settings(ADDRESS_SUGGEST_RATE_LIMIT_PER_MINUTE=3)
    def test_allows_up_to_the_limit_then_refuses(self):
        self.assertTrue(address_services.consume_suggest_quota("user:1"))
        self.assertTrue(address_services.consume_suggest_quota("user:1"))
        self.assertTrue(address_services.consume_suggest_quota("user:1"))
        self.assertFalse(address_services.consume_suggest_quota("user:1"))

    @override_settings(ADDRESS_SUGGEST_RATE_LIMIT_PER_MINUTE=1)
    def test_counts_per_identity(self):
        self.assertTrue(address_services.consume_suggest_quota("user:1"))
        self.assertFalse(address_services.consume_suggest_quota("user:1"))
        self.assertTrue(address_services.consume_suggest_quota("user:2"))

    @override_settings(ADDRESS_SUGGEST_RATE_LIMIT_PER_MINUTE=0)
    def test_zero_disables_the_limit(self):
        for _ in range(50):
            self.assertTrue(address_services.consume_suggest_quota("user:1"))

    @override_settings(ADDRESS_SUGGEST_RATE_LIMIT_PER_MINUTE=1)
    def test_fails_open_when_the_cache_is_down(self):
        # Losing the quota guard must not cost the user their autocomplete.
        with mock.patch.object(address_services.cache, "add", side_effect=RuntimeError("redis down")):
            self.assertTrue(address_services.consume_suggest_quota("user:1"))


class StatusWireMappingTest(SimpleTestCase):
    def test_round_trips(self):
        for status in src_enums.AddressValidationStatus:
            self.assertEqual(
                address_services.status_from_api(address_services.status_to_api(status)), status
            )

    def test_unknown_or_missing_value_is_none(self):
        self.assertIsNone(address_services.status_from_api(None))
        self.assertIsNone(address_services.status_from_api(""))
        self.assertIsNone(address_services.status_from_api("probably"))

    def test_stored_value_maps_to_the_wire_string(self):
        self.assertEqual(
            address_services.status_to_api_from_value(
                src_enums.AddressValidationStatus.CORRECTED.value
            ),
            "corrected",
        )

    def test_stored_null_or_unknown_value_is_none(self):
        self.assertIsNone(address_services.status_to_api_from_value(None))
        self.assertIsNone(address_services.status_to_api_from_value(0))
        self.assertIsNone(address_services.status_to_api_from_value(99))

    def test_is_case_insensitive(self):
        self.assertEqual(
            address_services.status_from_api(" Corrected "),
            src_enums.AddressValidationStatus.CORRECTED,
        )


class SchemaTest(SimpleTestCase):
    def _load(self, data, schema):
        return common_utils.validate_data_schema(data=data, schema=schema)

    def test_suggest_rejects_fewer_than_three_characters(self):
        with self.assertRaises(common_exceptions.ValidationSchemaException):
            self._load({"q": "16", "session": _SESSION}, address_schemas.SuggestAddressSchema())

    def test_suggest_rejects_an_overlong_query(self):
        with self.assertRaises(common_exceptions.ValidationSchemaException):
            self._load({"q": "x" * 201, "session": _SESSION}, address_schemas.SuggestAddressSchema())

    def test_suggest_rejects_a_non_iso2_country(self):
        with self.assertRaises(common_exceptions.ValidationSchemaException):
            self._load(
                {"q": "1600 Amph", "country": "USA", "session": _SESSION},
                address_schemas.SuggestAddressSchema(),
            )

    def test_suggest_rejects_a_non_uuid_session(self):
        with self.assertRaises(common_exceptions.ValidationSchemaException):
            self._load({"q": "1600 Amph", "session": "not-a-uuid"}, address_schemas.SuggestAddressSchema())

    def test_suggest_country_is_optional(self):
        validated = self._load({"q": "1600 Amph", "session": _SESSION}, address_schemas.SuggestAddressSchema())

        self.assertIsNone(validated["country"])

    def test_address_accepts_an_empty_state(self):
        validated = self._load(
            {"line1": "Jovana Tomaševića 12", "city": "Bar", "postal_code": "85000", "country": "ME"},
            address_schemas.AddressSchema(),
        )

        self.assertEqual(validated["state"], "")
        self.assertEqual(validated["line2"], "")

    def test_address_requires_line1_and_country(self):
        with self.assertRaises(common_exceptions.ValidationSchemaException):
            self._load({"city": "Bar", "country": "ME"}, address_schemas.AddressSchema())
        with self.assertRaises(common_exceptions.ValidationSchemaException):
            self._load({"line1": "Jovana Tomaševića 12"}, address_schemas.AddressSchema())

    def test_address_rejects_a_line1_longer_than_the_column(self):
        with self.assertRaises(common_exceptions.ValidationSchemaException):
            self._load({"line1": "x" * 256, "country": "US"}, address_schemas.AddressSchema())
