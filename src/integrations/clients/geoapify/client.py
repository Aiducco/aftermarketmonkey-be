"""
Transport client for Geoapify's Geocoding API -- the two endpoints the ship-to address feature
needs:

* ``/v1/geocode/autocomplete``  type-ahead. Unusually for an autocomplete API, the response
  already carries the full structured address parts of every hit, which is why /resolve can be
  answered from our own cache instead of a second billed call.
* ``/v1/geocode/search``        one-shot geocode of a finished address, used for validation.
  ``properties.rank`` (confidence / match_type) and ``properties.result_type`` are what we
  grade the match on.

Both return GeoJSON; this client unwraps ``features[].properties`` and returns those dicts,
since nothing here cares about the geometry. Mapping those dicts onto our own Address shape is
deliberately NOT done here -- that belongs to the vendor-agnostic layer
(``src/integrations/address/geoapify.py``).

The apiKey is a query parameter, not a header -- that is Geoapify's own scheme. It therefore
appears in the outbound URL, so nothing here may ever log a full request URL.
"""
import logging
import typing

import requests
from django.conf import settings

from common import utils as common_utils
from src.integrations.clients.geoapify import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[GEOAPIFY-CLIENT]"

# Geoapify's own hard cap on the autocomplete `limit` parameter.
_MAX_LIMIT = 20


class GeoapifyApiClient(object):
    """One instance per request is fine -- construction only reads settings."""

    def __init__(
        self,
        api_key: typing.Optional[str] = None,
        timeout_seconds: typing.Optional[float] = None,
        base_url: typing.Optional[str] = None,
    ) -> None:
        self.api_key = api_key if api_key is not None else settings.GEOAPIFY_API_KEY
        if not self.api_key:
            raise ValueError("Missing Geoapify api_key.")
        self.timeout_seconds = (
            timeout_seconds if timeout_seconds is not None else settings.ADDRESS_PROVIDER_TIMEOUT_SECONDS
        )
        self.base_url = (base_url if base_url is not None else settings.GEOAPIFY_BASE_URL).rstrip("/")

    def _get(
        self, endpoint: str, params: dict, timeout_seconds: typing.Optional[float] = None
    ) -> typing.List[dict]:
        """Returns the ``properties`` dict of every returned feature, in Geoapify's order."""
        timeout = timeout_seconds if timeout_seconds is not None else self.timeout_seconds
        url = "{}/{}".format(self.base_url, endpoint)
        query = dict(params)
        query["apiKey"] = self.api_key
        query["format"] = "geojson"

        try:
            response = requests.get(url=url, params=query, timeout=timeout)
        except requests.exceptions.Timeout as e:
            raise exceptions.GeoapifyTimeout(
                "Timed out after {}s calling {}. Error: {}".format(
                    timeout, endpoint, common_utils.get_exception_message(exception=e)
                )
            )
        except requests.RequestException as e:
            raise exceptions.GeoapifyAPIException(
                "Request exception calling {}. Error: {}".format(
                    endpoint, common_utils.get_exception_message(exception=e)
                )
            )

        if response.status_code in (401, 403):
            raise exceptions.GeoapifyAuthError(
                "Geoapify rejected the apiKey (status_code={}).".format(response.status_code)
            )
        if not (200 <= response.status_code < 300):
            raise exceptions.GeoapifyAPIException(
                "Geoapify error (endpoint={} status_code={}).".format(endpoint, response.status_code)
            )

        try:
            payload = response.json()
        except ValueError as e:
            raise exceptions.GeoapifyAPIException(
                "Geoapify returned an unparseable body (endpoint={}). Error: {}".format(
                    endpoint, common_utils.get_exception_message(exception=e)
                )
            )

        features = payload.get("features") or []
        return [feature.get("properties") or {} for feature in features if isinstance(feature, dict)]

    def autocomplete(
        self,
        text: str,
        country_code: typing.Optional[str] = None,
        limit: int = 5,
    ) -> typing.List[dict]:
        """
        ``country_code`` is ISO 3166-1 alpha-2; Geoapify wants it lowercased inside its
        ``filter=countrycode:`` expression.

        No ``type=`` filter is sent on purpose. Restricting to ``type=street`` drops exactly
        the hits users most often want -- a building with a house number comes back as
        ``type=building``, and named places as ``type=amenity`` -- so filtering is left to the
        mapping layer, which simply skips hits with no street.
        """
        params: typing.Dict[str, typing.Any] = {
            "text": text,
            "limit": max(1, min(int(limit), _MAX_LIMIT)),
        }
        if country_code:
            params["filter"] = "countrycode:{}".format(country_code.strip().lower())
        return self._get(endpoint="autocomplete", params=params)

    def search(
        self,
        text: str,
        country_code: typing.Optional[str] = None,
        limit: int = 1,
        timeout_seconds: typing.Optional[float] = None,
    ) -> typing.List[dict]:
        """
        Geocode a finished address. Free-form ``text`` rather than Geoapify's structured
        ``housenumber``/``street`` parameters: our form has one combined ADDRESS line, and
        splitting a house number back out of it is guesswork that fails on everything from
        "Apt 4, 12B Foo St" to house numbers that trail the street name. Geoapify's own
        parser does that job better than a regex of ours would.

        ``timeout_seconds`` overrides the instance default, because /search is measurably
        slower than /autocomplete -- 3.9s observed for "350 5th Ave, New York" against 0.4-0.9s
        for most lookups -- and it runs on a button click rather than a keystroke.
        """
        params: typing.Dict[str, typing.Any] = {
            "text": text,
            "limit": max(1, min(int(limit), _MAX_LIMIT)),
        }
        if country_code:
            params["filter"] = "countrycode:{}".format(country_code.strip().lower())
        return self._get(endpoint="search", params=params, timeout_seconds=timeout_seconds)
