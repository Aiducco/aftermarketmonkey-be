"""
Transport client for Wheel Pros' Orders API (REST/JSON) — order placement and tracking, plus
the Inventory Search API used for availability lookups. Separate from ``client.py`` (the
existing SFTP feed client used by the nightly catalog/pricing sync) since this targets a
different capability (write operations) over a different transport (REST over HTTPS vs. SFTP)
and credential shape (username/password, vs. sftp_user/sftp_password).

Auth: username/password (Product Data Portal account) are exchanged for a short-lived (1hr)
Bearer JWT via POST /auth/v1/authorize. Every subsequent call carries the token in a standard
``Authorization: Bearer <token>`` header — see https://developer.wheelpros.com.

Host: a single host serves every API area, split by top-level path prefix (``/auth``,
``/orders``, ``/inventory``, ...) — not a distinct host per capability like Turn14/Meyer/Premier.
"testing" points at Wheel Pros' dev subdomain, same environment-split pattern as the other
distributor order clients.

SAFETY: ``create_sales_order_edi`` places a REAL order against Wheel Pros. It must only ever be
invoked from an explicit, user-approved submission — never from exploratory/dev code, automated
tests, or ad-hoc scripts.
"""
import decimal
import time
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.wheelpros import exceptions

TOKEN_EXPIRATION_BUFFER_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 30

_ENVIRONMENT_BASE_URLS = {
    "testing": settings.WHEELPROS_ORDER_TEST_BASE_URL,
    "production": settings.WHEELPROS_ORDER_PRODUCTION_BASE_URL,
}

# Wheel Pros' Orders API has no dedicated no-op/ping endpoint — used as a syntactically valid
# but essentially-guaranteed-not-to-exist lookup for test_connection() (see its docstring).
_CONNECTION_TEST_PO_NUMBER = "AMS-CONNECTION-TEST"


class WheelProsOrderApiClient(object):
    """One instance per (credentials, environment) pair. ``environment`` must be "testing" or
    "production" and must match the host implied by it — see _ENVIRONMENT_BASE_URLS."""

    VALID_STATUS_CODES = [200, 201]

    def __init__(self, credentials: typing.Dict, environment: str = "production") -> None:
        self.username = credentials.get("username", "")
        self.password = credentials.get("password", "")
        if not self.username or not self.password:
            raise ValueError("Invalid credentials parameter.")

        if environment not in _ENVIRONMENT_BASE_URLS:
            raise ValueError("Invalid environment: {}. Must be 'testing' or 'production'.".format(environment))
        self.environment = environment
        self.api_base_url = _ENVIRONMENT_BASE_URLS[environment]

        self._cached_token: typing.Optional[str] = None
        self._token_expires_at: typing.Optional[float] = None

    # -- Auth -----------------------------------------------------------------------------

    def _is_token_valid(self) -> bool:
        if self._cached_token is None or self._token_expires_at is None:
            return False
        return time.time() < (self._token_expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS)

    def _get_valid_token(self) -> str:
        if self._is_token_valid():
            return self._cached_token

        try:
            response = requests.request(
                url="{}/auth/v1/authorize".format(self.api_base_url),
                method=common_enums.HttpMethod.POST.value,
                headers={"Content-Type": "application/json"},
                json={"userName": self.username, "password": self.password},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.ConnectTimeout as e:
            raise exceptions.WheelProsOrderAPIException(
                "Connect timeout during authentication. Error: {}".format(
                    common_utils.get_exception_message(exception=e)
                )
            )
        except requests.RequestException as e:
            raise exceptions.WheelProsOrderAPIException(
                "Authentication request exception. Error: {}".format(
                    common_utils.get_exception_message(exception=e)
                )
            )

        if response.status_code == 401:
            raise exceptions.WheelProsOrderAuthError("Invalid Wheel Pros username/password.")
        if response.status_code not in [200]:
            raise exceptions.WheelProsOrderAPIException(
                "Invalid authorize response (status_code={}, data={})".format(
                    response.status_code, response.content.decode("utf-8")
                )
            )

        data = simplejson.loads(response.content, parse_float=decimal.Decimal)
        access_token = data.get("accessToken")
        if not access_token:
            raise exceptions.WheelProsOrderAPIException("No accessToken in authorize response.")

        self._cached_token = access_token
        expires_in = data.get("expiresIn")
        if isinstance(expires_in, decimal.Decimal):
            expires_in = int(expires_in)
        self._token_expires_at = time.time() + (expires_in or 3600)
        return self._cached_token

    def _clear_token_cache(self) -> None:
        self._cached_token = None
        self._token_expires_at = None

    # -- Request/response plumbing ---------------------------------------------------------

    @staticmethod
    def _extract_message(response: requests.Response) -> typing.Optional[str]:
        try:
            data = simplejson.loads(response.content, parse_float=decimal.Decimal)
        except (ValueError, simplejson.JSONDecodeError):
            return None
        if isinstance(data, dict):
            return data.get("message") or data.get("error") or data.get("errorMessage")
        return None

    def _request(
        self,
        path: str,
        method: common_enums.HttpMethod,
        json_body: typing.Optional[dict] = None,
        params: typing.Optional[dict] = None,
        retry_on_401: bool = True,
    ) -> typing.Dict:
        url = "{}/{}".format(self.api_base_url, path)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self._get_valid_token()),
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
            raise exceptions.WheelProsOrderAPIException(
                "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            )
        except requests.RequestException as e:
            raise exceptions.WheelProsOrderAPIException(
                "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            )

        if response.status_code == 401 and retry_on_401:
            self._clear_token_cache()
            return self._request(path, method, json_body=json_body, params=params, retry_on_401=False)
        if response.status_code == 401:
            raise exceptions.WheelProsOrderAuthError("Wheel Pros rejected the access token (401 after refresh).")
        if response.status_code == 403:
            raise exceptions.WheelProsOrderPermissionError(
                self._extract_message(response)
                or "Wheel Pros denied access to this API for this account (403)."
            )
        if response.status_code == 400:
            raise exceptions.WheelProsOrderValidationError(
                message=self._extract_message(response) or "Wheel Pros rejected the request (400).",
                code="400",
            )
        if response.status_code not in self.VALID_STATUS_CODES:
            raise exceptions.WheelProsOrderAPIException(
                "Invalid API client response (status_code={}, data={})".format(
                    response.status_code, response.content.decode("utf-8")
                )
            )
        return simplejson.loads(response.content, parse_float=decimal.Decimal)

    # -- Order (SUBMIT — real order placement, see module docstring) --------------------

    def create_sales_order_edi(self, data: typing.Dict, customer: typing.Optional[str] = None) -> typing.Dict:
        """POST /orders/v1/create?orderType=edi. Places a real order. Do not call outside an
        explicit, user-approved submission flow. ``orderType=edi`` is the dealer-facing path —
        Wheel Pros' docs mark the ECOM variant "Wheel Pros use only"."""
        params = {"orderType": "edi"}
        if customer:
            params["customer"] = customer
        return self._request(
            "orders/v1/create", common_enums.HttpMethod.POST, json_body=data, params=params
        )

    # -- Status / tracking ----------------------------------------------------------------

    def get_order_tracking(self, **params: typing.Any) -> typing.Dict:
        """GET /orders/v1/track. Accepts any of salesOrderNumber/poNumber/trackingNumber/
        addressCode/realtimeShipmentStatus/customer as kwargs; None values are dropped."""
        query = {k: v for k, v in params.items() if v is not None}
        return self._request("orders/v1/track", common_enums.HttpMethod.GET, params=query)

    # -- Inventory (availability only — see WheelProsOrderAdapter.get_shipping_quote) ----

    def search_inventory(
        self,
        skus: typing.Iterable[str],
        country_codes: typing.Iterable[str] = ("US",),
        warehouse_ids: typing.Optional[typing.Iterable[str]] = None,
        request_id: typing.Optional[str] = None,
    ) -> typing.Dict:
        """POST /inventory/v1/search. Marked "Internal Use Only" in Wheel Pros' own docs — some
        dealer accounts may not be granted access to this specific API even with valid Orders
        API access; callers should handle WheelProsOrderPermissionError distinctly from other
        failures (see get_shipping_quote)."""
        data: typing.Dict[str, typing.Any] = {
            "skus": list(skus),
            "countryCode": [c.upper() for c in country_codes],
        }
        if warehouse_ids:
            data["warehouseId"] = list(warehouse_ids)
        if request_id:
            data["requestId"] = request_id
        return self._request("inventory/v1/search", common_enums.HttpMethod.POST, json_body=data)

    # -- Connection test ------------------------------------------------------------------

    def test_connection(self) -> None:
        """
        Validates username/password via /auth/v1/authorize (raises WheelProsOrderAuthError on
        bad credentials), then confirms Orders API access specifically with a cheap real call:
        GET /orders/v1/track for a purchase-order number that will not exist. A 403 here means
        the account authenticated but was never granted Orders API access (a separate grant per
        Wheel Pros' "Request Wheel Pros for APIs you want to access" onboarding step) — that
        propagates as WheelProsOrderPermissionError. A validation rejection (no matching order)
        is the expected, successful outcome, since Wheel Pros' Orders API has no dedicated
        no-op/ping endpoint — same real-call-as-probe pattern as Keystone/Turn14's test_connection.
        """
        self._get_valid_token()
        try:
            self.get_order_tracking(poNumber=_CONNECTION_TEST_PO_NUMBER)
        except exceptions.WheelProsOrderValidationError:
            pass
