"""
Client for Thoroughbred Diesel's public catalog (HawkSearch) and vehicle-decode (main site ajax)
endpoints. Neither needs any auth, cookies, or login -- see docs/THOROUGHBRED_DIESEL_INGEST_SPEC.md
for the full live-measured ingest spec this module implements.

Two hosts, two completely different UA postures -- do not mix them up:

- ``thoroughbreddiesel.searchapi-na.hawksearch.com`` (HawkSearchClient) is NOT behind Cloudflare
  and is UA-irrelevant -- no special header needed.
- ``www.thoroughbreddiesel.com`` (MainSiteAjaxClient) IS behind Cloudflare with a *backwards*
  rule: no User-Agent header or a bare ``curl/8.x`` string gets 200; a desktop-browser UA or the
  requests/urllib library default both get 403. Every request on this host must send an explicit
  curl-style UA (never the library default, never a browser string).
"""
import logging
import typing

import requests

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[THOROUGHBRED-CLIENT]"

HAWKSEARCH_URL = "https://thoroughbreddiesel.searchapi-na.hawksearch.com/api/v2/search"
HAWKSEARCH_CLIENT_GUID = "362f7bd3bfc14e378f68a0f5745aa87d"
HAWKSEARCH_INDEX_NAME = "thoroughbreddiesel"
# HawkSearch's MaxPerPage only accepts 24/48/72 -- any other value silently falls back to 24.
HAWKSEARCH_MAX_PER_PAGE = 72
HAWKSEARCH_REQUEST_TIMEOUT_SECONDS = 30

MAIN_SITE_BASE_URL = "https://www.thoroughbreddiesel.com"
MAIN_SITE_AJAX_PATH = "/ajax/store/ajax.aspx"
# Backwards-Cloudflare-rule-compliant UA: a bare curl string, never a browser string, never the
# requests library default (both of those get 403 on this host).
MAIN_SITE_USER_AGENT = "curl/8.7.1"
MAIN_SITE_REQUEST_TIMEOUT_SECONDS = 20

# Hardcoded per the live-measured spec -- there is no "list all makes" endpoint, so these are not
# crawled.
THOROUGHBRED_MAKES = {
    "Chevy": 204,
    "Dodge": 205,
    "Ford": 206,
    "GMC": 203,
    "Jeep": 207,
    "Nissan": 208,
    "Toyota": 210,
    "Volkswagen": 209,
}

# One doc in the index uses this as an "unlimited / drop-ship" inventory sentinel -- never a real
# quantity, must be clamped (to None) before it reaches storage, or a single row injects 100M
# phantom units into any sum.
INVENTORY_SENTINEL = 99999999


def g(doc: dict, key: str) -> typing.Optional[typing.Any]:
    """
    Tolerant accessor for a HawkSearch document: every field is returned as a 1-element
    list-of-strings except the three ``_hawk*boost`` fields, which are raw floats.
    """
    v = doc.get(key)
    if isinstance(v, list):
        return v[0] if v else None
    return v


class ThoroughbredApiError(Exception):
    pass


class HawkSearchClient:
    """Public, unauthenticated HawkSearch catalog+fitment search. No UA/cookie handling needed --
    this host is not behind Cloudflare at all."""

    def __init__(self, session: typing.Optional[requests.Session] = None):
        self._session = session or requests.Session()

    def search_page(
        self,
        page_no: int,
        max_per_page: int = HAWKSEARCH_MAX_PER_PAGE,
        query: typing.Optional[str] = None,
    ) -> dict:
        body = {
            "ClientGuid": HAWKSEARCH_CLIENT_GUID,
            "IndexName": HAWKSEARCH_INDEX_NAME,
            "MaxPerPage": max_per_page,
            "PageNo": page_no,
        }
        # Omitting "Query" entirely returns the full catalog -- do not send Query=None/"" instead,
        # HawkSearch treats an empty string differently than an absent key.
        if query:
            body["Query"] = query
        resp = self._session.post(
            HAWKSEARCH_URL, json=body, timeout=HAWKSEARCH_REQUEST_TIMEOUT_SECONDS
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("Success", True):
            raise ThoroughbredApiError(
                "{} HawkSearch returned Success=false on page {}: {}".format(
                    _LOG_PREFIX, page_no, data
                )
            )
        return data

    def iter_all_docs(
        self, max_per_page: int = HAWKSEARCH_MAX_PER_PAGE
    ) -> typing.Iterator[dict]:
        """Yields every raw ``Document`` dict in the catalog, paging until a page comes back empty."""
        page_no = 1
        while True:
            data = self.search_page(page_no, max_per_page=max_per_page)
            results = data.get("Results") or []
            if not results:
                return
            for result in results:
                doc = result.get("Document")
                if doc:
                    yield doc
            pagination = data.get("Pagination") or {}
            total_pages = pagination.get("NofPages")
            if total_pages and page_no >= total_pages:
                return
            page_no += 1


class MainSiteAjaxClient:
    """Vehicle-id decode walk against the Cloudflare-fronted main site. A decode miss
    (``Success: false``, or a non-200 response) is a soft miss -- callers get ``None``/``[]``,
    never an exception, since the walk must survive e.g. the one known orphan vehicle id (1678)
    without aborting the whole sweep."""

    def __init__(self, session: typing.Optional[requests.Session] = None):
        self._session = session or requests.Session()
        self._session.headers["User-Agent"] = MAIN_SITE_USER_AGENT

    def _get(self, params: dict) -> typing.Optional[dict]:
        try:
            resp = self._session.get(
                MAIN_SITE_BASE_URL + MAIN_SITE_AJAX_PATH,
                params=params,
                timeout=MAIN_SITE_REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            logger.warning("{} ajax request failed params={}: {}".format(_LOG_PREFIX, params, e))
            return None
        if resp.status_code != 200:
            logger.warning(
                "{} ajax request got status={} params={}".format(_LOG_PREFIX, resp.status_code, params)
            )
            return None
        try:
            data = resp.json()
        except ValueError:
            logger.warning("{} ajax response was not JSON, params={}".format(_LOG_PREFIX, params))
            return None
        if not data.get("Success"):
            return None
        return data

    def get_years(self, make_id: int) -> typing.List[int]:
        data = self._get({"f": "GetYears", "makeId": make_id})
        if not data:
            return []
        return data.get("Years") or []

    def get_models(self, make_id: int, year: int) -> typing.List[dict]:
        data = self._get({"f": "GetModels", "makeId": make_id, "year": year})
        if not data:
            return []
        return data.get("Models") or []

    def get_vehicle(self, make_id: int, year: int, model_id: int) -> typing.Optional[dict]:
        data = self._get({"f": "GetVehicle", "makeId": make_id, "year": year, "modelId": model_id})
        if not data:
            return None
        return data.get("Vehicle")

    def walk_all_vehicles(
        self, makes: typing.Optional[typing.Dict[str, int]] = None
    ) -> typing.Iterator[typing.Tuple[int, int, str, str]]:
        """Yields (vehicle_id, year, make_name, model_name) across every make/year/model
        combination -- ~1,545 requests total for the 8 hardcoded makes. A missing/unreachable
        combination at any step (years, models, or the final vehicle lookup) is simply skipped,
        never raised -- see the orphan vehicle id 1678 note on ``ThoroughbredVehicle``."""
        makes = makes or THOROUGHBRED_MAKES
        for make_name, make_id in makes.items():
            years = self.get_years(make_id)
            for year in years:
                models = self.get_models(make_id, year)
                for model in models:
                    model_id = model.get("VehicleModelId")
                    model_name = model.get("Name")
                    if model_id is None:
                        continue
                    vehicle = self.get_vehicle(make_id, year, model_id)
                    if not vehicle:
                        continue
                    vehicle_id = vehicle.get("VehicleId")
                    if vehicle_id is None:
                        continue
                    yield vehicle_id, year, make_name, model_name
