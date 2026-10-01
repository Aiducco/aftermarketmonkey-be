"""
Tests for the Geoapify address adapter: the field mapping, and how a provider answer is
graded into valid / corrected / unverified.

The transport client is replaced by a stub throughout -- what's under test is the mapping and
the grading rules, not HTTP. Fixtures are trimmed copies of real Geoapify
``feature.properties`` objects (only the keys the mapping reads, plus the rank/result_type
keys the grading reads).
"""
import typing
import unittest.mock as mock

from django.test import SimpleTestCase, override_settings

from src import enums as src_enums
from src.integrations.address import base, geoapify
from src.integrations.clients.geoapify import exceptions as geoapify_exceptions

# All three fixtures are trimmed copies of REAL responses captured from the live Geoapify
# API, down to the quirks: note that the US one reports result_type "amenity" (Geoapify names
# the building that sits on the delivery point) even though it is a perfect house-number match,
# which is why the grading reads rank.confidence_building_level instead.
US_PROPERTIES = {
    "housenumber": "1600",
    "street": "Amphitheatre Parkway",
    "city": "Mountain View",
    "county": "Santa Clara County",
    "state": "California",
    "state_code": "CA",
    "postcode": "94043",
    "country": "United States",
    "country_code": "us",
    "formatted": "1600 Amphitheatre Parkway, Mountain View, CA 94043, United States",
    # Geoapify puts the POI name here and demotes the street to address_line2 -- the exact
    # case _line1's containment check exists for.
    "address_line1": "Google Building 41",
    "address_line2": "1600 Amphitheatre Parkway, Mountain View, CA 94043",
    "place_id": "51a6f4bd0d2e8f5ec0591ccb1b0c0a5d",
    "result_type": "amenity",
    "rank": {
        "confidence": 1,
        "confidence_city_level": 1,
        "confidence_street_level": 1,
        "confidence_building_level": 1,
        "match_type": "inner_part",
    },
}

# Montenegro: Geoapify reports a municipality in ``state`` but no ``state_code``, because the
# country has no state codes. Our ``state`` must come out empty rather than borrowing the
# municipality name -- the frontend's State dropdown has nothing to match it against.
MONTENEGRO_PROPERTIES = {
    "housenumber": "12",
    "street": "Jovana Tomaševića",
    "city": "Bar",
    # The live API does return a state_code for Montenegro ("BA" = Bar) -- the no-state case
    # is covered by NO_STATE_PROPERTIES below instead.
    "state": "Bar",
    "state_code": "BA",
    "postcode": "85000",
    "country": "Montenegro",
    "country_code": "me",
    "formatted": "Jovana Tomaševića 12, 85000 Bar, Montenegro",
    # Geoapify puts the house number where the country actually puts it.
    "address_line1": "Jovana Tomaševića 12",
    "address_line2": "85000 Bar, Montenegro",
    "place_id": "51d0a4e8a3b1c0a5c059",
    "result_type": "building",
    "rank": {
        "confidence": 1,
        "confidence_street_level": 1,
        "confidence_building_level": 1,
        "match_type": "full_match",
    },
}

# No state at all, and the city arrives under ``town`` rather than ``city`` -- the common
# shape for small places.
NO_STATE_PROPERTIES = {
    "housenumber": "7",
    "street": "Rue du Marché",
    "town": "Echternach",
    "postcode": "6460",
    "country": "Luxembourg",
    "country_code": "lu",
    "formatted": "7 Rue du Marché, 6460 Echternach, Luxembourg",
    "address_line1": "7 Rue du Marché",
    "address_line2": "6460 Echternach, Luxembourg",
    "place_id": "51aa00bb11cc22dd33",
    "result_type": "building",
    "rank": {
        "confidence": 0.95,
        "confidence_street_level": 1,
        "confidence_building_level": 0.95,
        "match_type": "full_match",
    },
}


class _StubClient:
    """Stands in for GeoapifyApiClient. ``raises`` short-circuits every call."""

    def __init__(
        self,
        autocomplete_result: typing.Optional[typing.List[dict]] = None,
        search_result: typing.Optional[typing.List[dict]] = None,
        raises: typing.Optional[Exception] = None,
    ) -> None:
        self.autocomplete_result = autocomplete_result or []
        self.search_result = search_result or []
        self.raises = raises
        self.calls: typing.List[dict] = []

    def autocomplete(self, text, country_codes=None, limit=5):
        self.calls.append(
            {"endpoint": "autocomplete", "text": text, "country_codes": country_codes, "limit": limit}
        )
        if self.raises:
            raise self.raises
        return self.autocomplete_result

    def search(self, text, country_code=None, limit=1, timeout_seconds=None):
        self.calls.append(
            {
                "endpoint": "search",
                "text": text,
                "country_code": country_code,
                "limit": limit,
                "timeout_seconds": timeout_seconds,
            }
        )
        if self.raises:
            raise self.raises
        return self.search_result


def _provider(**kwargs) -> geoapify.GeoapifyAddressProvider:
    return geoapify.GeoapifyAddressProvider(client=_StubClient(**kwargs))


class AddressFromPropertiesTest(SimpleTestCase):
    def test_us_address(self):
        address = geoapify.address_from_properties(US_PROPERTIES)

        self.assertEqual(
            address.as_dict(),
            {
                "line1": "1600 Amphitheatre Parkway",
                "line2": "",
                "city": "Mountain View",
                "state": "CA",
                "postal_code": "94043",
                "country": "US",
            },
        )

    def test_montenegro_address_puts_the_house_number_last(self):
        address = geoapify.address_from_properties(MONTENEGRO_PROPERTIES)

        # Composing "housenumber + street" would give "12 Jovana Tomaševića" -- an ordering no
        # local would recognize. Geoapify's own address_line1 has it right.
        self.assertEqual(address.line1, "Jovana Tomaševića 12")
        self.assertEqual(address.city, "Bar")
        self.assertEqual(address.postal_code, "85000")
        self.assertEqual(address.country, "ME")
        # Montenegro DOES have state codes in Geoapify's data ("BA" = Bar), contrary to the
        # spec's assumption. It is passed through as-is; the frontend's own rule (drop a state
        # code the State dropdown doesn't contain) is what keeps the form consistent.
        self.assertEqual(address.state, "BA")

    def test_state_is_empty_when_the_provider_reports_no_state_code(self):
        properties = dict(MONTENEGRO_PROPERTIES)
        properties.pop("state_code")

        # `state` ("Bar") is still there, but a name the State dropdown can't match is worse
        # than nothing.
        self.assertEqual(geoapify.address_from_properties(properties).state, "")

    def test_no_state_and_city_from_town(self):
        address = geoapify.address_from_properties(NO_STATE_PROPERTIES)

        self.assertEqual(address.city, "Echternach")
        self.assertEqual(address.state, "")
        self.assertEqual(address.country, "LU")

    def test_amenity_name_in_address_line1_does_not_replace_the_street(self):
        # Geoapify puts a POI's NAME in address_line1 and demotes the street to address_line2.
        # Filling the ADDRESS field with "Googleplex" would lose the street entirely.
        address = geoapify.address_from_properties(
            {
                "name": "Googleplex",
                "housenumber": "1600",
                "street": "Amphitheatre Parkway",
                "address_line1": "Googleplex",
                "address_line2": "1600 Amphitheatre Parkway, Mountain View, CA 94043",
                "city": "Mountain View",
                "state_code": "CA",
                "postcode": "94043",
                "country_code": "us",
                "result_type": "amenity",
            }
        )

        self.assertEqual(address.line1, "1600 Amphitheatre Parkway")

    def test_falls_back_to_address_line1_when_no_street_parsed(self):
        address = geoapify.address_from_properties(
            {
                "name": "Googleplex",
                "address_line1": "Googleplex",
                "city": "Mountain View",
                "state_code": "CA",
                "postcode": "94043",
                "country_code": "us",
            }
        )

        self.assertEqual(address.line1, "Googleplex")

    def test_missing_everything_yields_empty_strings_not_none(self):
        address = geoapify.address_from_properties({})

        self.assertEqual(
            address.as_dict(),
            {"line1": "", "line2": "", "city": "", "state": "", "postal_code": "", "country": ""},
        )


class SuggestTest(SimpleTestCase):
    @override_settings(ADDRESS_SUGGEST_LIMIT=5)
    def test_maps_each_hit_and_keeps_the_provider_label(self):
        provider = _provider(autocomplete_result=[US_PROPERTIES, MONTENEGRO_PROPERTIES])

        suggestions = provider.suggest(q="1600 Amph", country="US", session="s-1")

        self.assertEqual(len(suggestions), 2)
        self.assertEqual(suggestions[0].label, US_PROPERTIES["formatted"])
        self.assertEqual(suggestions[0].provider_ref, US_PROPERTIES["place_id"])
        self.assertEqual(suggestions[0].address.state, "CA")

    @override_settings(ADDRESS_SUGGEST_LIMIT=5)
    def test_passes_country_filter_through(self):
        client = _StubClient(autocomplete_result=[])
        provider = geoapify.GeoapifyAddressProvider(client=client)

        provider.suggest(q="Jovana", country="ME", session="s-1")

        self.assertEqual(client.calls[0]["country_codes"], ["ME"])

    @override_settings(ADDRESS_SUGGEST_LIMIT=5)
    def test_sends_no_filter_when_the_caller_resolved_no_country(self):
        # The service layer is what guarantees a country is present (see
        # _effective_suggest_country); the provider just forwards what it is given.
        client = _StubClient(autocomplete_result=[])

        geoapify.GeoapifyAddressProvider(client=client).suggest(q="Jovana", country=None, session="s-1")

        self.assertIsNone(client.calls[0]["country_codes"])

    @override_settings(ADDRESS_SUGGEST_LIMIT=5)
    def test_skips_hits_with_nothing_to_put_in_the_address_field(self):
        # A city-level hit: no street, no address_line1, nothing to fill ADDRESS with.
        provider = _provider(
            autocomplete_result=[{"city": "Bar", "country_code": "me", "formatted": "Bar, Montenegro"}]
        )

        self.assertEqual(provider.suggest(q="Bar", country="ME", session="s-1"), [])

    def test_timeout_becomes_an_address_provider_timeout(self):
        provider = _provider(raises=geoapify_exceptions.GeoapifyTimeout("too slow"))

        with self.assertRaises(geoapify.exceptions.AddressProviderTimeout):
            provider.suggest(q="1600 Amph", country="US", session="s-1")

    def test_api_error_becomes_an_address_provider_error(self):
        provider = _provider(raises=geoapify_exceptions.GeoapifyAuthError("bad key"))

        with self.assertRaises(geoapify.exceptions.AddressProviderError):
            provider.suggest(q="1600 Amph", country="US", session="s-1")


class ResolveTest(SimpleTestCase):
    def test_returns_none_because_geoapify_has_no_details_call(self):
        # Not a gap: the API layer serves /resolve from its own cache (the autocomplete
        # response already carried the structured address) and only reaches the provider once
        # that entry has expired, at which point 404 is the right answer.
        self.assertIsNone(_provider().resolve(provider_ref="whatever", session="s-1"))


@override_settings(
    ADDRESS_VALIDATION_CONFIDENCE_THRESHOLD=0.9,
    ADDRESS_VALIDATION_CORRECTION_FLOOR=0.5,
    ADDRESS_VALIDATE_TIMEOUT_SECONDS=5.0,
)
class ValidateTest(SimpleTestCase):
    """
    Every scenario here was first observed against the live Geoapify API; the ``rank`` values
    in each test are the ones it actually returned for that input.
    """

    def _entered_us(self, **overrides) -> base.Address:
        data = {
            "line1": "1600 Amphitheatre Parkway",
            "city": "Mountain View",
            "state": "CA",
            "postal_code": "94043",
            "country": "US",
        }
        data.update(overrides)
        return base.Address(**data)

    def test_building_level_match_is_valid_even_when_result_type_is_amenity(self):
        # The regression this whole grading rewrite exists for: keying off
        # result_type == "building" marked Google HQ unverified.
        result = _provider(search_result=[US_PROPERTIES]).validate(self._entered_us())

        self.assertEqual(US_PROPERTIES["result_type"], "amenity")
        self.assertEqual(result.status, src_enums.AddressValidationStatus.VALID)
        self.assertIsNone(result.suggested)
        self.assertEqual(result.messages, ())

    def test_wrong_postcode_is_corrected_even_though_the_provider_scores_it_perfect(self):
        # Geoapify does not penalize a wrong postcode at all -- it silently substitutes the
        # right one and still reports confidence 1. Comparing fields is the only way to catch
        # it.
        provider = _provider(search_result=[US_PROPERTIES])

        result = provider.validate(self._entered_us(postal_code="94044", line2="Suite 7"))

        self.assertEqual(result.status, src_enums.AddressValidationStatus.CORRECTED)
        self.assertEqual(result.suggested.postal_code, "94043")
        # The provider never has line2, so the user's unit must survive the suggestion.
        self.assertEqual(result.suggested.line2, "Suite 7")

    def test_street_typo_is_corrected_from_below_the_valid_threshold(self):
        # "Amphitheater Pkwy" -> 0.75 at both levels: too low to call valid, far too high to
        # claim we don't recognize it.
        properties = dict(US_PROPERTIES)
        properties["address_line1"] = "1600 Amphitheatre Parkway"
        properties["rank"] = {
            "confidence": 0.75,
            "confidence_street_level": 0.75,
            "confidence_building_level": 0.75,
            "match_type": "full_match",
        }
        provider = _provider(search_result=[properties])

        result = provider.validate(self._entered_us(line1="1600 Amphitheater Pkwy"))

        self.assertEqual(result.status, src_enums.AddressValidationStatus.CORRECTED)
        self.assertEqual(result.suggested.line1, "1600 Amphitheatre Parkway")

    def test_real_street_with_a_house_number_the_provider_lacks_is_unverified(self):
        # "99281 Amphitheatre Parkway": result_type "building" and match_type "full_match",
        # but confidence_building_level 0. Nothing differs, so there is nothing to suggest.
        properties = dict(US_PROPERTIES)
        properties["housenumber"] = "99281"
        properties["address_line1"] = "99281 Amphitheatre Parkway"
        properties["result_type"] = "building"
        properties["rank"] = {
            "confidence": 0.5,
            "confidence_city_level": 1,
            "confidence_street_level": 1,
            "confidence_building_level": 0,
            "match_type": "full_match",
        }
        provider = _provider(search_result=[properties])

        result = provider.validate(self._entered_us(line1="99281 Amphitheatre Parkway"))

        self.assertEqual(result.status, src_enums.AddressValidationStatus.UNVERIFIED)
        self.assertIn("house number", result.messages[0])

    def test_street_confirmed_but_unlisted_house_still_offers_a_postcode_correction(self):
        # "123 Main Street, Springfield IL 62701" -> the street is certain, the house is not in
        # the data, and the real postcode is 62702. Worth suggesting; the street carries it.
        properties = {
            "housenumber": "123",
            "street": "Main Street",
            "address_line1": "123 Main Street",
            "city": "Springfield",
            "state_code": "IL",
            "postcode": "62702",
            "country_code": "us",
            "result_type": "building",
            "rank": {
                "confidence": 0.5,
                "confidence_city_level": 1,
                "confidence_street_level": 1,
                "confidence_building_level": 0,
                "match_type": "full_match",
            },
        }
        provider = _provider(search_result=[properties])

        result = provider.validate(
            base.Address(
                line1="123 Main Street",
                city="Springfield",
                state="IL",
                postal_code="62701",
                country="US",
            )
        )

        self.assertEqual(result.status, src_enums.AddressValidationStatus.CORRECTED)
        self.assertEqual(result.suggested.postal_code, "62702")

    def test_made_up_address_falls_back_to_a_district_match_and_is_unverified(self):
        # The live answer for a nonsense street: a suburb centroid at confidence 0.25, with no
        # street or building grade at all. Must not be offered as a correction.
        properties = {
            "address_line1": "Mountain View South",
            "city": "Mountain View",
            "state_code": "CA",
            "postcode": "94043",
            "country_code": "us",
            "result_type": "suburb",
            "rank": {
                "confidence": 0.25,
                "confidence_city_level": 1,
                "match_type": "match_by_city_or_disrict",
            },
        }
        provider = _provider(search_result=[properties])

        result = provider.validate(self._entered_us(line1="99281 Nonexistent Fakestreet Blvd"))

        self.assertEqual(result.status, src_enums.AddressValidationStatus.UNVERIFIED)
        self.assertIsNone(result.suggested)

    def test_no_match_at_all_is_unverified(self):
        result = _provider(search_result=[]).validate(self._entered_us())

        self.assertEqual(result.status, src_enums.AddressValidationStatus.UNVERIFIED)
        self.assertTrue(result.messages)

    def test_blank_entered_field_is_filled_in_silently_not_reported_as_a_correction(self):
        result = _provider(search_result=[US_PROPERTIES]).validate(self._entered_us(postal_code=""))

        self.assertEqual(result.status, src_enums.AddressValidationStatus.VALID)

    def test_multi_unit_building_with_no_line2_adds_a_message(self):
        properties = dict(US_PROPERTIES)
        properties["datasource"] = {"raw": {"building": "apartments"}}
        provider = _provider(search_result=[properties])

        result = provider.validate(self._entered_us())

        self.assertEqual(result.status, src_enums.AddressValidationStatus.VALID)
        self.assertIn(geoapify._MULTI_UNIT_MESSAGE, result.messages)

    def test_multi_unit_building_with_line2_filled_stays_quiet(self):
        properties = dict(US_PROPERTIES)
        properties["datasource"] = {"raw": {"building": "apartments"}}
        provider = _provider(search_result=[properties])

        result = provider.validate(self._entered_us(line2="Apt 4"))

        self.assertEqual(result.messages, ())

    def test_single_family_building_tag_is_not_treated_as_multi_unit(self):
        properties = dict(US_PROPERTIES)
        properties["datasource"] = {"raw": {"building": "house"}}
        provider = _provider(search_result=[properties])

        self.assertEqual(provider.validate(self._entered_us()).messages, ())

    def test_timeout_propagates_for_the_service_layer_to_degrade(self):
        provider = _provider(raises=geoapify_exceptions.GeoapifyTimeout("too slow"))

        with self.assertRaises(geoapify.exceptions.AddressProviderTimeout):
            provider.validate(self._entered_us())

    def test_uses_the_longer_validate_timeout_not_the_keystroke_one(self):
        client = _StubClient(search_result=[US_PROPERTIES])
        geoapify.GeoapifyAddressProvider(client=client).validate(self._entered_us())

        call = client.calls[0]
        self.assertEqual(call["endpoint"], "search")
        self.assertEqual(call["timeout_seconds"], 5.0)
        self.assertEqual(call["country_code"], "US")
        self.assertEqual(call["text"], "1600 Amphitheatre Parkway, Mountain View CA 94043, US")


class ProviderConstructionTest(SimpleTestCase):
    @override_settings(GEOAPIFY_API_KEY="")
    def test_missing_api_key_raises_not_configured(self):
        with self.assertRaises(geoapify.exceptions.AddressProviderNotConfigured):
            geoapify.GeoapifyAddressProvider()

    @override_settings(GEOAPIFY_API_KEY="k", ADDRESS_PROVIDER_TIMEOUT_SECONDS=2.0)
    def test_builds_its_own_client_from_settings(self):
        with mock.patch.object(geoapify.geoapify_client, "GeoapifyApiClient") as client_cls:
            geoapify.GeoapifyAddressProvider()

        client_cls.assert_called_once_with()
