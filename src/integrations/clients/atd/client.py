"""
REST client for ATD Connect (American Tire Distributors Ship-to-Home API).

Per-company credentials, same shape as Turn 14's client_id/client_secret: every dealer is issued
its own username/password/clientId/locationnumber, and `locationnumber` is load-bearing on nearly
every call -- it determines which brands/products are visible, cost (pricing is per-location), and
availability (local/localplus are relative to that location's DC). There is no "global catalog"
call; see ``iter_product_catalog``.

Auth is plain HTTP Basic (username:password) plus a ``clientId`` header -- no token exchange, no
refresh, unlike Turn 14's OAuth client-credentials flow.

Confirmed live 2026-10-16 against a real production account (Suncoast Off-Road, locationnumber
3036915): ``/location/location-by-criteria`` with an empty body bootstraps the account's own
location, and ``/product/productcatalog`` paginates the full ~34K-SKU catalog with cost + stock +
specs + images in one response per product -- see master_parts.py's ATD section for how that
feeds MasterPart/ProviderPart/ProviderPartCompanyPricing.

The API's own error responses are inconsistent -- sometimes ``{"error": "...", "message": "..."}``,
sometimes ``{"errorMessage": "..."}`` -- ``_raise_for_response`` normalizes both.
"""
import logging
import typing

import requests
from django.conf import settings

from src.integrations.clients.atd import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ATD-API-CLIENT]"

DEFAULT_BASE_URL = "https://ws.atdconnect.com/rs/3_6"

# requests had no timeout before on every other client here until someone hit a stalled host from
# inside a synchronous test_connection() call -- set explicitly up front this time.
REQUEST_TIMEOUT_SECONDS = 30

DEFAULT_PRODUCT_CATALOG_PAGE_SIZE = 1000  # ATD's own documented ceiling; do not exceed.


class ATDApiClient(object):
    LOG_PREFIX = _LOG_PREFIX

    def __init__(self, credentials: typing.Dict):
        creds = credentials or {}
        self.username = str(creds.get("username") or "").strip()
        self.password = str(creds.get("password") or "")
        self.client_id = str(creds.get("client_id") or "").strip()
        self.location = str(creds.get("location") or "").strip()

        missing = [
            k for k, v in (
                ("username", self.username), ("password", self.password),
                ("client_id", self.client_id), ("location", self.location),
            ) if not v
        ]
        if missing:
            raise ValueError(
                "Invalid ATD credentials -- missing: {}.".format(", ".join(missing))
            )

        self.base_url = str(getattr(settings, "ATD_BASE_URL", None) or DEFAULT_BASE_URL).rstrip("/")

    def _headers(self) -> typing.Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-Language": "en-US",
            "clientId": self.client_id,
        }

    def _raise_for_response(self, response: requests.Response, resource: str) -> typing.Dict:
        """Parse the response body and raise ATDAuthError/ATDConnectionError for a bad status or
        for the error shapes ATD returns with a 200 -- see the module docstring."""
        try:
            data = response.json()
        except ValueError:
            data = None

        if response.status_code == 401 or (isinstance(data, dict) and data.get("error") == "401"):
            msg = (data or {}).get("message") or "ATD rejected the provided credentials."
            raise exceptions.ATDAuthError(msg, response.status_code)

        if isinstance(data, dict) and data.get("errorMessage"):
            msg = data["errorMessage"]
            # "does not have access to this location" / "enter a valid location" are credential
            # problems in practice (bad locationnumber), not transient connectivity failures --
            # surfaced the same way as a 401 so the connect-flow validator reports them as such.
            raise exceptions.ATDAuthError(msg, response.status_code)

        if response.status_code != 200:
            msg = "Unexpected ATD response fetching {} (status_code={}, body={}).".format(
                resource, response.status_code, (response.text or "")[:500]
            )
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.ATDAPIBadResponseCodeError(msg, response.status_code)

        if data is None:
            raise exceptions.ATDConnectionError(
                "Non-JSON response from ATD fetching {}.".format(resource)
            )
        return data

    def _post(self, path: str, body: typing.Dict, resource: str) -> typing.Dict:
        url = "{}{}".format(self.base_url, path)
        try:
            response = requests.post(
                url,
                json=body,
                headers=self._headers(),
                auth=(self.username, self.password),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.Timeout as e:
            raise exceptions.ATDConnectionError("Timed out calling ATD {}: {}".format(resource, e))
        except requests.RequestException as e:
            raise exceptions.ATDConnectionError("Request to ATD {} failed: {}".format(resource, e))
        return self._raise_for_response(response, resource)

    def get_locations(self, criteria: typing.Optional[typing.Dict] = None) -> typing.List[typing.Dict]:
        """``POST /location/location-by-criteria``. An empty/omitted ``criteria`` lists every
        location visible to these credentials -- the documented bootstrap call."""
        body = {"criteria": criteria} if criteria else {}
        data = self._post("/location/location-by-criteria", body, "location-by-criteria")
        return data.get("locations") or []

    def test_connection(self) -> typing.Dict:
        """Validate credentials by confirming this account's own locationnumber resolves.
        Raises ATDAuthError if the location doesn't resolve or credentials are rejected."""
        locations = self.get_locations({"locationnumber": self.location})
        if not locations:
            raise exceptions.ATDAuthError(
                "Credentials were accepted, but location {} was not returned by ATD.".format(
                    self.location
                ),
                200,
            )
        return locations[0]

    def fetch_product_catalog_page(
        self,
        page_number: int,
        page_size: int = DEFAULT_PRODUCT_CATALOG_PAGE_SIZE,
        brand: typing.Optional[typing.List[str]] = None,
        productgroup: typing.Optional[typing.List[str]] = None,
    ) -> typing.Tuple[typing.List[typing.Dict], int]:
        """One page of ``POST /product/productcatalog``. Returns (products, totalpages)."""
        body: typing.Dict[str, typing.Any] = {
            "locationnumber": self.location,
            "pagenumber": page_number,
            "pagesize": page_size,
        }
        if brand:
            body["brand"] = brand
        if productgroup:
            body["productgroup"] = productgroup
        data = self._post("/product/productcatalog", body, "productcatalog")
        products = data.get("products") or []
        totalpages = data.get("totalpages") or 1
        try:
            totalpages = int(totalpages)
        except (TypeError, ValueError):
            totalpages = 1
        return products, max(totalpages, 1)

    def iter_product_catalog(
        self,
        page_size: int = DEFAULT_PRODUCT_CATALOG_PAGE_SIZE,
        brand: typing.Optional[typing.List[str]] = None,
        productgroup: typing.Optional[typing.List[str]] = None,
        max_pages: typing.Optional[int] = None,
    ) -> typing.Iterator[typing.Dict]:
        """
        Stream every product in this account's catalog, paging until ``page == totalpages``.
        There is no "global catalog" call (see the module docstring) -- this always runs against
        ``self.location``, so a platform-wide catalog sweep means calling this once per connected
        company's own credentials, not once for the whole distributor.
        """
        page = 1
        totalpages = 1
        while page <= totalpages:
            products, totalpages = self.fetch_product_catalog_page(
                page, page_size=page_size, brand=brand, productgroup=productgroup
            )
            for product in products:
                yield product
            if max_pages is not None and page >= max_pages:
                break
            page += 1

    def get_product_availability(self, atd_product_numbers: typing.List[str]) -> typing.List[typing.Dict]:
        """``POST /product/product-availability`` -- quantities only, no product detail. Lighter
        candidate for a frequent stock-only refresh; not used by the initial catalog sync, which
        gets availability for free from ``productcatalog``."""
        if not atd_product_numbers:
            return []
        body = {
            "locationnumber": self.location,
            "criteria": {"atdproductnumber": atd_product_numbers},
        }
        data = self._post("/product/product-availability", body, "product-availability")
        return data.get("products") or []
