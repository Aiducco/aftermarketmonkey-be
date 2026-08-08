"""
Client for the Western Power Sports Data Depot API (https://api.wps-inc.com).

Auth is a per-dealer bearer token issued through the Data Depot signup. The API is JSON:API
flavoured and, unlike Motor State, supports everything needed for a cheap bulk ingest:

  * cursor pagination -- ``page[size]`` + ``page[cursor]``, loop while ``meta.cursor.next``
    is non-null. Confirmed working at ``page[size]=10000``.
  * sparse fieldsets -- ``fields[items]=id,sku,...`` to cut payload.
  * filters -- e.g. ``filter[updated_at][gt]=2026-08-01`` for incremental polling,
    ``filter[brand_id]=406``.
  * includes -- ``include=inventory,images,taxonomyterms``. Needed for images and taxonomy,
    which have no item foreign key on their own collection endpoints.

Inventory figures are capped at 25 per warehouse by WPS on purpose ("so as to avoid disclosing
our actual inventory levels"), so totals are a floor, not a true count.
"""
import decimal
import logging
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.wps import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WPS-API-CLIENT]"

# Confirmed working; WPS documents no maximum. Kept high because the catalog is ~127k items
# and each extra round trip costs a full cursor hop.
DEFAULT_PAGE_SIZE = 10000

# Lower default when ?include= is in play -- a page of items with inventory+images+taxonomy is
# roughly 2KB/item, so 10k rows would be a ~20MB response.
DEFAULT_INCLUDE_PAGE_SIZE = 500

REQUEST_TIMEOUT_SECONDS = 120

# Stop runaway cursor loops if the API ever returns a self-referencing next cursor.
_MAX_PAGES = 5000


class WpsApiClient(object):
    API_BASE_URL = settings.WPS_BASE_URL
    VALID_STATUS_CODES = [200]

    def __init__(self, credentials: typing.Dict):
        # Stripped because these tokens are pasted; a stray newline would otherwise be sent
        # verbatim in the Authorization header and rejected by requests as an invalid header.
        self.api_key = ((credentials or {}).get("api_key") or "").strip()
        if not self.api_key:
            raise ValueError("Invalid credentials parameter: missing api_key.")
        self._session = requests.Session()
        self._session.headers.update(
            {"Accept": "application/json", "Authorization": "Bearer {}".format(self.api_key)}
        )

    def test_connection(self) -> None:
        """Validate the token against the cheapest real endpoint (a single item page)."""
        try:
            self._request("items", params={"page[size]": 1})
        except exceptions.WpsAPIBadResponseCodeError as e:
            if e.code in (401, 403):
                raise exceptions.WpsPermissionError(
                    message=(
                        "Connected to Western Power Sports, but this API token was rejected "
                        "(HTTP {}). Confirm the token is active in the Data Depot.".format(e.code)
                    ),
                    code=e.code,
                )
            raise

    # -- collections -------------------------------------------------------------------
    def iter_collection(
        self,
        endpoint: str,
        fields: typing.Optional[str] = None,
        include: typing.Optional[str] = None,
        filters: typing.Optional[typing.Dict[str, typing.Any]] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> typing.Iterator[typing.List[typing.Dict]]:
        """
        Yield successive pages of a WPS collection, following ``meta.cursor.next`` until it is
        null. Yields the raw ``data`` list per page so callers can upsert incrementally instead
        of holding the whole catalog in memory.
        """
        params: typing.Dict[str, typing.Any] = {"page[size]": page_size}
        if fields:
            params["fields[{}]".format(endpoint)] = fields
        if include:
            params["include"] = include
        for key, value in (filters or {}).items():
            params[key] = value

        cursor: typing.Optional[str] = None
        pages = 0
        while True:
            if cursor:
                params["page[cursor]"] = cursor
            payload = self._get_json(endpoint, params)
            data = payload.get("data") or []
            yield data

            pages += 1
            next_cursor = ((payload.get("meta") or {}).get("cursor") or {}).get("next")
            if not next_cursor or not data or next_cursor == cursor or pages >= _MAX_PAGES:
                if pages >= _MAX_PAGES:
                    logger.warning(
                        "{} Stopped {} after {} pages (cursor guard).".format(
                            _LOG_PREFIX, endpoint, pages
                        )
                    )
                break
            cursor = next_cursor

    def get_brands(self) -> typing.List[typing.Dict]:
        out: typing.List[typing.Dict] = []
        for page in self.iter_collection("brands"):
            out.extend(page)
        return out

    def get_warehouses(self) -> typing.List[typing.Dict]:
        out: typing.List[typing.Dict] = []
        for page in self.iter_collection("warehouses"):
            out.extend(page)
        return out

    def iter_products(self, page_size: int = DEFAULT_PAGE_SIZE):
        return self.iter_collection("products", page_size=page_size)

    def iter_items(
        self,
        include: typing.Optional[str] = None,
        fields: typing.Optional[str] = None,
        updated_after: typing.Optional[str] = None,
        page_size: typing.Optional[int] = None,
    ):
        """
        Items, optionally scoped to rows changed since ``updated_after`` (ISO date/datetime) for
        incremental polling.
        """
        filters = {"filter[updated_at][gt]": updated_after} if updated_after else None
        if page_size is None:
            page_size = DEFAULT_INCLUDE_PAGE_SIZE if include else DEFAULT_PAGE_SIZE
        return self.iter_collection(
            "items", fields=fields, include=include, filters=filters, page_size=page_size
        )

    def iter_inventory(self, page_size: int = DEFAULT_PAGE_SIZE):
        return self.iter_collection("inventory", page_size=page_size)

    # -- plumbing ----------------------------------------------------------------------
    def _get_json(self, endpoint: str, params: typing.Dict[str, typing.Any]) -> typing.Dict:
        response = self._request(endpoint, params=params)
        return simplejson.loads(response.content, parse_float=decimal.Decimal)

    def _request(
        self, endpoint: str, params: typing.Optional[dict] = None
    ) -> requests.Response:
        url = "{}/{}".format(self.API_BASE_URL, endpoint)
        try:
            response = self._session.request(
                url=url,
                method=common_enums.HttpMethod.GET.value,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code not in self.VALID_STATUS_CODES:
                msg = "Invalid API client response (status_code={}, data={}).".format(
                    response.status_code, response.content.decode("utf-8", errors="replace")[:500]
                )
                logger.error("{} {}".format(_LOG_PREFIX, msg))
                raise exceptions.WpsAPIBadResponseCodeError(message=msg, code=response.status_code)
        except requests.exceptions.ConnectTimeout as e:
            msg = "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WpsAPIException(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.WpsAPIException(msg)
        return response
