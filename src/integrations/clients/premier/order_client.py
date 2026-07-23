"""
Transport client for Premier (APG Wholesale)'s v5 REST Order API — inventory availability,
order placement, and shipment tracking. Separate from ``client.py`` (the existing FTP catalog/
pricing client) since this targets a different capability (write operations) over a different
transport and credential shape (a single ``api_key``, vs. ftp_user/ftp_password).

Auth: a single ``api_key`` (issued by a Premier rep) is exchanged for a session token via
GET /authenticate?apiKey=..., used as ``Authorization: Bearer <token>`` on every subsequent
call. The token is a JWT; Premier's docs don't state its validity duration, so the client
decodes the token's own ``exp`` claim (no signature verification needed — we're only reading
when to refresh, not trusting the claim for authorization) to know when to re-authenticate,
falling back to a conservative cache window if that claim is missing or the token can't be
decoded.

Re-confirmed against a full read of https://developer.premierwd.com/ (0.5.0, current as of this
pass) — the docs are actually complete for every endpoint this client uses (inventory, pricing,
tracking, sales-orders); the "thin docs" note from an earlier pass was wrong. Two real gaps
remain, both because they're genuinely undocumented rather than missed: no documented
error-response body shape, and no documented success HTTP status code for POST /sales-orders/
(handled as "any 2xx", see _request). Most importantly, the documented POST /sales-orders/
response example does NOT include Premier's own salesOrderNumber — confirmed directly from the
docs' own example response, not inferred. Every choice below that depends on one of these gaps
is called out at the point it's made; treat this client as best-effort until confirmed against
Premier's test environment.

SAFETY: ``create_sales_order`` places a REAL order against Premier — their docs state orders
are committed immediately on POST, with no dry-run/preview mode. Must only ever be invoked from
an explicit, user-approved submission — see the Purchase Orders job-queue path this runs behind.
There is no cancel endpoint at all (see ``src/integrations/orders/premier.py``).
"""
import base64
import binascii
import decimal
import time
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.premier import exceptions

REQUEST_TIMEOUT_SECONDS = 30
TOKEN_EXPIRATION_BUFFER_SECONDS = 60
# Used only when the session token's own "exp" JWT claim can't be read — Premier's docs don't
# state a validity duration, so this is a deliberately short, conservative fallback.
DEFAULT_TOKEN_CACHE_SECONDS = 10 * 60
# Inventory/pricing endpoints accept up to 50 item numbers per call, per Premier's docs.
MAX_ITEMS_PER_BULK_REQUEST = 50

_ENVIRONMENT_BASE_URLS = {
    "testing": settings.PREMIER_ORDER_TEST_BASE_URL,
    "production": settings.PREMIER_ORDER_PRODUCTION_BASE_URL,
}


def _decode_jwt_exp(token: str) -> typing.Optional[float]:
    """Best-effort read of a JWT's "exp" claim, without verifying its signature — used only to
    decide when to refresh our own cached copy, never to authorize anything."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        payload_b64 = parts[1]
        padded = payload_b64 + "=" * (-len(payload_b64) % 4)
        payload = simplejson.loads(base64.urlsafe_b64decode(padded))
        exp = payload.get("exp")
        return float(exp) if exp is not None else None
    except (ValueError, TypeError, simplejson.JSONDecodeError, binascii.Error):
        return None


class PremierOrderApiClient(object):
    """One instance per (credentials, environment) pair. Defaults to "production" (unlike
    Turn14/Meyer's "testing" class default) since PremierOrderAdapter always passes
    settings.PREMIER_ORDER_ENVIRONMENT explicitly anyway, and that setting has always
    defaulted to "production" for Premier."""

    def __init__(self, credentials: typing.Dict, environment: str = "production") -> None:
        self.api_key = credentials.get("api_key", "")
        if not self.api_key:
            raise ValueError("Invalid credentials parameter.")

        if environment not in _ENVIRONMENT_BASE_URLS:
            raise ValueError("Invalid environment: {}. Must be 'testing' or 'production'.".format(environment))
        self.environment = environment
        self.api_base_url = _ENVIRONMENT_BASE_URLS[environment]

        self._cached_session_token: typing.Optional[str] = None
        self._token_expires_at: typing.Optional[float] = None

    # -- Auth -----------------------------------------------------------------------------

    def _is_token_valid(self) -> bool:
        if self._cached_session_token is None or self._token_expires_at is None:
            return False
        return time.time() < (self._token_expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS)

    def _get_valid_session_token(self) -> str:
        if self._is_token_valid():
            return self._cached_session_token

        try:
            response = requests.get(
                "{}/authenticate".format(self.api_base_url),
                params={"apiKey": self.api_key},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            raise exceptions.PremierOrderAPIException(
                "Authenticate request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            )

        if response.status_code == 401 or response.status_code == 403:
            raise exceptions.PremierOrderAuthError("Invalid Premier api_key.")
        if response.status_code != 200:
            raise exceptions.PremierOrderAPIException(
                "Invalid authenticate response (status_code={}, data={})".format(
                    response.status_code, response.content.decode("utf-8", errors="replace")
                )
            )

        try:
            data = simplejson.loads(response.content, parse_float=decimal.Decimal)
        except simplejson.JSONDecodeError as e:
            raise exceptions.PremierOrderAPIException("Non-JSON authenticate response: {}".format(e))

        token = data.get("sessionToken")
        if not token:
            raise exceptions.PremierOrderAPIException("No sessionToken in authenticate response.")

        self._cached_session_token = token
        exp = _decode_jwt_exp(token)
        self._token_expires_at = exp if exp is not None else (time.time() + DEFAULT_TOKEN_CACHE_SECONDS)
        return token

    def _clear_token_cache(self) -> None:
        self._cached_session_token = None
        self._token_expires_at = None

    # -- Request/response plumbing ---------------------------------------------------------

    def _request(
        self,
        endpoint: str,
        method: common_enums.HttpMethod,
        params: typing.Optional[dict] = None,
        json_body: typing.Optional[dict] = None,
        retry_on_401: bool = True,
    ) -> typing.Union[typing.Dict, typing.List]:
        url = "{}/{}".format(self.api_base_url, endpoint)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self._get_valid_session_token()),
        }
        try:
            response = requests.request(
                url=url,
                method=method.value,
                params=params,
                json=json_body,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.ConnectTimeout as e:
            raise exceptions.PremierOrderAPIException(
                "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            )
        except requests.RequestException as e:
            raise exceptions.PremierOrderAPIException(
                "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            )

        if response.status_code in (401, 403) and retry_on_401:
            self._clear_token_cache()
            return self._request(endpoint, method, params=params, json_body=json_body, retry_on_401=False)
        if response.status_code in (401, 403):
            raise exceptions.PremierOrderAuthError(
                "Premier API rejected the session token (status_code={} after refresh).".format(
                    response.status_code
                )
            )

        # Premier's docs don't document an error body shape or a success status code for
        # POST /sales-orders/, so success is treated as "2xx", and the body (JSON if parseable,
        # else raw text) is surfaced verbatim in the exception message on any other status —
        # there's no documented {errorCode, errorMessage}-style field to extract cleanly.
        if not (200 <= response.status_code < 300):
            raise exceptions.PremierOrderValidationError(
                message="Premier API error (status_code={}): {}".format(
                    response.status_code, response.content.decode("utf-8", errors="replace")[:2000]
                ),
                code=str(response.status_code),
            )

        if not response.content:
            return {}
        try:
            return simplejson.loads(response.content, parse_float=decimal.Decimal)
        except simplejson.JSONDecodeError as e:
            raise exceptions.PremierOrderAPIException(
                "Non-JSON response from Premier {} {} ({}). Raw: {}".format(
                    method.value, endpoint, e, response.content.decode("utf-8", errors="replace")[:2000]
                )
            )

    # -- Inventory (no shipping-quote endpoint exists — see module docstring) -------------

    def get_inventory(self, item_numbers: typing.List[str]) -> typing.List[typing.Dict]:
        """GET /inventory. Up to 50 item numbers per call — batches internally if given more."""
        results: typing.List[typing.Dict] = []
        for i in range(0, len(item_numbers), MAX_ITEMS_PER_BULK_REQUEST):
            batch = item_numbers[i : i + MAX_ITEMS_PER_BULK_REQUEST]
            data = self._request(
                endpoint="inventory",
                method=common_enums.HttpMethod.GET,
                params={"itemNumbers": ",".join(batch)},
            )
            results.extend(data if isinstance(data, list) else [data])
        return results

    # -- Pricing --------------------------------------------------------------------------

    def get_pricing(self, item_numbers: typing.List[str]) -> typing.List[typing.Dict]:
        """GET /pricing. Up to 50 item numbers per call — batches internally if given more.
        Returns cost/jobber/map/retail per item, once per currency (USD and, where the item has
        Canadian pricing, CAD) — see PremierOrderAdapter._get_prices for how the right currency
        row is picked per order."""
        results: typing.List[typing.Dict] = []
        for i in range(0, len(item_numbers), MAX_ITEMS_PER_BULK_REQUEST):
            batch = item_numbers[i : i + MAX_ITEMS_PER_BULK_REQUEST]
            data = self._request(
                endpoint="pricing",
                method=common_enums.HttpMethod.GET,
                params={"itemNumbers": ",".join(batch)},
            )
            results.extend(data if isinstance(data, list) else [data])
        return results

    # -- Order (SUBMIT — real order placement, see module docstring) --------------------

    def create_sales_order(self, data: typing.Dict) -> typing.Dict:
        """POST /sales-orders/. Places a real order — Premier's docs state there is no
        dry-run/preview mode. Do not call outside an explicit, user-approved submission flow."""
        result = self._request(endpoint="sales-orders/", method=common_enums.HttpMethod.POST, json_body=data)
        return result if isinstance(result, dict) else {}

    # -- Status / tracking ----------------------------------------------------------------

    def get_tracking_by_purchase_order_number(self, purchase_order_number: str) -> typing.List[typing.Dict]:
        """GET /tracking?purchaseOrderNumber=... . Used in place of
        GET /sales-orders/{salesOrderNumber} for status polling, since Premier's order-creation
        response never hands back its own salesOrderNumber (see module docstring) — this is the
        only documented lookup keyed by something we actually have (our own PO number)."""
        data = self._request(
            endpoint="tracking",
            method=common_enums.HttpMethod.GET,
            params={"purchaseOrderNumber": purchase_order_number},
        )
        return data if isinstance(data, list) else [data]

    def test_connection(self) -> None:
        """Cheap connectivity/auth probe — Premier has no dedicated ping endpoint, but forcing
        an authenticate call (bypassing any cache) proves the api_key works without touching
        any order/customer data."""
        self._clear_token_cache()
        self._get_valid_session_token()
