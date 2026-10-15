"""
Client for Tri-State Enterprises' real, documented distributor API -- PACE's "Customer API",
reached at ``https://etristate.com/api/{endpoint}`` for Tri-State specifically (PACE is a
catalog/ordering platform other AAM member distributors also run on their own hostnames, but
this client is deliberately Tri-State-only for now -- see BrandProviderKind.TRI_STATE).

Confirmed live 2026-10-15 against the ``TridentMotorsports`` App-Id:
  - ``test``                     -- works, used as the connection health check.
  - ``get_warehouses``           -- works.
  - ``get_inventory_and_pricing``-- works; the one endpoint this integration actually needs.
  - ``get_brands``               -- 403, ACL-restricted for this App-Id (pending wider scope --
    see the email thread with Tri-State's rep). This is why TSEBrand is seeded from a bundled
    file instead of fetched live -- see src.management.commands.seed_tse_brands.
  - ``get_purchase_orders`` / ``submit_purchase_order`` -- also 403; irrelevant anyway since
    this integration deliberately does not implement ordering yet.

Auth is two static headers, not a token exchange (no login/refresh step, unlike Turn14's OAuth
client-credentials flow): ``Api-Key`` and ``App-Id``, both issued directly by Tri-State's rep.
``Response-Object: 1`` is also required on every call -- without it the API serves XML instead
of JSON (observed, not just documented).

Every request is a plain GET with query-string params (confirmed via ``-G --data-urlencode``),
even though the API otherwise looks RESTful -- there is no POST path in anything confirmed
working here.

Response shape on success (HTTP 200, JSON, confirmed real example against ``test``):
    {
        "api_version": "v1", "api_endpoint": "test", "request_received": "...",
        "request_method": "GET", "requesting_ip": "...", "requesting_app": "TridentMotorsports",
        "http_response_code": 200, "success": 1, "message": "...",
        "data": {...} or [...]
    }

Response shape on an ACL/permission failure (HTTP 403, confirmed real, plain text -- NOT JSON
despite ``Response-Object: 1``, so this has to be detected before attempting to parse JSON):
    ERROR: APP TridentMotorsports NOT ALLOWED TO ACCESS ENDPOINT GET_BRANDS
"""
import logging
import typing

import requests
from django.conf import settings

from src.integrations.clients.tristate import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TRISTATE-API-CLIENT]"

DEFAULT_BASE_URL = "https://etristate.com/api"
REQUEST_TIMEOUT_SECONDS = 20

# PACE's own documented best practice for get_inventory_and_pricing is to batch 50-100 "items"
# per call rather than one call per part; 75 sits in the middle of that documented range.
DEFAULT_ITEMS_BATCH_SIZE = 75


class TriStateApiClient:
    """
    Credentials: ``app_id`` + ``api_key``, both issued directly by Tri-State -- same namespace
    shape as every other API-credential provider's feed credentials (see
    src.constants.PROVIDER_CATALOG's TRI_STATE entry).
    """

    LOG_PREFIX = _LOG_PREFIX

    def __init__(self, credentials: typing.Dict, base_url: typing.Optional[str] = None):
        creds = credentials or {}
        self.app_id = (creds.get("app_id") or "").strip()
        self.api_key = (creds.get("api_key") or "").strip()
        if not self.app_id or not self.api_key:
            raise ValueError("Invalid credentials. Missing app_id or api_key.")
        self.base_url = (
            base_url or getattr(settings, "TRISTATE_API_BASE_URL", None) or DEFAULT_BASE_URL
        ).rstrip("/")

    def _headers(self) -> typing.Dict[str, str]:
        return {
            "Api-Key": self.api_key,
            "App-Id": self.app_id,
            "Response-Object": "1",
        }

    def _call(self, endpoint: str, params: typing.Optional[typing.Dict] = None) -> typing.Any:
        """GET {base_url}/{endpoint}, return the parsed ``data`` field of a successful response.

        Raises TriStatePermissionError for the documented plain-text ACL-denial shape,
        TriStateAuthError for a 401, and TriStateTransportError for anything else unexpected
        (network failure, timeout, or a response matching neither documented shape).
        """
        url = "{}/{}".format(self.base_url, endpoint)
        try:
            response = requests.get(
                url, headers=self._headers(), params=params or {}, timeout=REQUEST_TIMEOUT_SECONDS
            )
        except requests.RequestException as e:
            msg = "Request to {} failed: {}".format(endpoint, e)
            logger.exception("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.TriStateTransportError(msg) from e

        text = response.text or ""
        stripped = text.strip()

        if stripped.upper().startswith("ERROR:"):
            if response.status_code == 401 or "NOT ALLOWED" not in stripped.upper():
                raise exceptions.TriStateAuthError(stripped)
            raise exceptions.TriStatePermissionError(stripped)

        try:
            body = response.json()
        except ValueError as e:
            msg = "Non-JSON, non-ERROR response from {} (status={}): {!r}".format(
                endpoint, response.status_code, stripped[:500]
            )
            logger.error("{} {}".format(_LOG_PREFIX, msg))
            raise exceptions.TriStateTransportError(msg) from e

        if not body.get("success"):
            msg = body.get("message") or "Unknown error (status={})".format(response.status_code)
            raise exceptions.TriStateTransportError("{}: {}".format(endpoint, msg))

        return body.get("data")

    def test_connection(self) -> None:
        """Hits the ``test`` endpoint -- the cheapest real call, confirmed always allowed for a
        valid Api-Key/App-Id pair regardless of which real endpoints are ACL-restricted."""
        self._call("test", params={"hello": "world"})

    def get_warehouses(self) -> typing.List[typing.Dict]:
        data = self._call("get_warehouses")
        return data if isinstance(data, list) else []

    def get_inventory_and_pricing(
        self, items: typing.Sequence[typing.Tuple[str, str]], aliasing: bool = True
    ) -> typing.List[typing.Dict]:
        """
        ``items``: a sequence of (brand_code, part_number) pairs, e.g.
        ``[("AIR", "25980EZ"), ("WIX", "51515")]``. ``brand_code`` can be either Tri-State's own
        internal warehouse brand code (TSEBrand.warehouse_code, e.g. "AIR" for Air Lift) OR the
        generic AAIA code (e.g. "BBCG") when ``aliasing=True`` -- confirmed live: with
        aliasing, PACE matches the supplied code against both namespaces and against normalized
        part numbers, and echoes back the resolved pair as ``brand_code``/``aaia_brand_code`` on
        each result row. A result with no ``aaia_brand_code`` key means aliasing found no match
        at all for that item (not merely zero stock) -- see
        src.integrations.services.tristate._resolve_response_identity.

        ``aliasing`` defaults True: confirmed live it resolves already-correct warehouse codes
        just as well as unresolved AAIA codes (both come back with the same enriched shape), so
        there's no real downside to always leaving it on other than PACE's documented extra
        per-item DB lookup cost -- and it's what lets TSEBrand.warehouse_code get filled in over
        time without a separate manual-scraping step.

        Returns the raw ``data`` list verbatim (one dict per item, each carrying brand_code,
        part_number, description, orderable, account_number, account_price, list_price,
        inventory, inventory_locations, and -- when aliasing resolved it -- aaia_brand_code) --
        confirmed real shape, see this module's docstring. Callers needing more than
        DEFAULT_ITEMS_BATCH_SIZE items must batch themselves (see
        src.integrations.services.tristate.fetch_tristate_inventory_and_pricing_for_company).
        """
        items_param = ",".join("{}|{}".format(brand_code, part_number) for brand_code, part_number in items)
        if not items_param:
            return []
        params = {"items": items_param}
        if aliasing:
            params["aliasing"] = "true"
        data = self._call("get_inventory_and_pricing", params=params)
        return data if isinstance(data, list) else []
