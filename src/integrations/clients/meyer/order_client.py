"""
Transport client for Meyer Distributing's Order API (v2 REST/JSON) — shipping rate quotes,
order placement, cancellation, and order-status/tracking lookups. Separate from ``client.py``
(the existing SFTP relay client used by the nightly catalog/pricing sync) since this targets a
different capability (write operations) over a different transport (REST over HTTPS vs. SFTP)
and credential shape (username/password + customer_number, vs. sftp_user/sftp_password).

Auth: username/password are exchanged for an API key via POST /Authentication, valid 30 days
(creating a new key does not invalidate old ones). Every subsequent call carries the key in an
``Authorization: Espresso <apikey>:1`` header — a non-standard scheme specific to Meyer's API
gateway (Espresso Logic), not a bearer token despite the similar shape.

SAFETY: ``create_order`` places a REAL order against Meyer (even against the "testing"
environment — Meyer's docs warn the test API "can contain outdated information" but do not
describe it as a sandbox that skips real order creation). ``cancel_order`` also has real,
possibly irreversible effect (Meyer's docs: an order already being processed cannot be canceled
via the API). Callers must never invoke either except through an explicit, user-approved
submission — see the Purchase Orders job-queue path this is meant to run behind.
"""
import datetime
import decimal
import time
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.meyer import exceptions

TOKEN_EXPIRATION_BUFFER_SECONDS = 60 * 60  # 1 hour headroom against the 30-day apikey expiry
REQUEST_TIMEOUT_SECONDS = 30

_ENVIRONMENT_BASE_URLS = {
    "testing": settings.MEYER_ORDER_TEST_BASE_URL,
    "production": settings.MEYER_ORDER_PRODUCTION_BASE_URL,
}


class MeyerOrderApiClient(object):
    """One instance per (credentials, environment) pair. ``environment`` must be "testing" or
    "production" and must match the host implied by it — see _ENVIRONMENT_BASE_URLS."""

    VALID_STATUS_CODES = [200, 201]

    def __init__(self, credentials: typing.Dict, environment: str = "testing") -> None:
        self.username = credentials.get("username", "")
        self.password = credentials.get("password", "")
        self.customer_number = credentials.get("customer_number", "")
        if not self.username or not self.password or not self.customer_number:
            raise ValueError("Invalid credentials parameter.")

        if environment not in _ENVIRONMENT_BASE_URLS:
            raise ValueError("Invalid environment: {}. Must be 'testing' or 'production'.".format(environment))
        self.environment = environment
        self.api_base_url = _ENVIRONMENT_BASE_URLS[environment]

        self._cached_api_key: typing.Optional[str] = None
        self._api_key_expires_at: typing.Optional[float] = None

    # -- Auth -----------------------------------------------------------------------------

    def _is_api_key_valid(self) -> bool:
        if self._cached_api_key is None or self._api_key_expires_at is None:
            return False
        return time.time() < (self._api_key_expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS)

    def _get_valid_api_key(self) -> str:
        if self._is_api_key_valid():
            return self._cached_api_key

        try:
            response = requests.request(
                url="{}/Authentication".format(self.api_base_url),
                method=common_enums.HttpMethod.POST.value,
                headers={"Content-Type": "application/json"},
                json={"username": self.username, "password": self.password},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as e:
            raise exceptions.MeyerOrderAPIException(
                "Authentication request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            )

        if response.status_code == 401:
            raise exceptions.MeyerOrderAuthError("Invalid Meyer username/password.")
        if response.status_code not in self.VALID_STATUS_CODES:
            raise exceptions.MeyerOrderAPIException(
                "Invalid Authentication response (status_code={}, data={})".format(
                    response.status_code, response.content.decode("utf-8")
                )
            )

        data = simplejson.loads(response.content, parse_float=decimal.Decimal)
        api_key = data.get("apikey")
        if not api_key:
            raise exceptions.MeyerOrderAPIException("No apikey in Authentication response.")

        self._cached_api_key = api_key
        # "expiration" is an absolute ISO timestamp, not a relative seconds-from-now like
        # Turn14's expires_in — parse it defensively; on any failure, fall back to a
        # conservative 24h cache rather than treating auth as permanently valid.
        expiration_raw = data.get("expiration")
        try:
            expires_dt = datetime.datetime.fromisoformat(str(expiration_raw).replace("Z", "+00:00"))
            self._api_key_expires_at = expires_dt.timestamp()
        except (TypeError, ValueError):
            self._api_key_expires_at = time.time() + 24 * 60 * 60
        return self._cached_api_key

    def _clear_api_key_cache(self) -> None:
        self._cached_api_key = None
        self._api_key_expires_at = None

    # -- Request/response plumbing ---------------------------------------------------------

    def _raise_for_business_error(self, response: requests.Response) -> None:
        try:
            data = simplejson.loads(response.content, parse_float=decimal.Decimal)
        except (ValueError, simplejson.JSONDecodeError):
            return
        if not isinstance(data, dict):
            return
        error_code = data.get("errorCode")
        error_message = data.get("errorMessage")
        if error_code is not None or error_message is not None:
            raise exceptions.MeyerOrderValidationError(
                message=error_message or "Meyer API rejected the request.",
                code=str(error_code) if error_code is not None else None,
            )

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
            "Authorization": "Espresso {}:1".format(self._get_valid_api_key()),
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
            raise exceptions.MeyerOrderAPIException(
                "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            )
        except requests.RequestException as e:
            raise exceptions.MeyerOrderAPIException(
                "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            )

        if response.status_code == 401 and retry_on_401:
            self._clear_api_key_cache()
            return self._request(endpoint, method, params=params, json_body=json_body, retry_on_401=False)
        if response.status_code == 401:
            raise exceptions.MeyerOrderAuthError("Meyer API rejected the apikey (401 after refresh).")

        # Business-rule rejections come back with a distinct {"errorCode", "errorMessage"} body
        # — check this before the raw status-code gate below, since Meyer doesn't consistently
        # use HTTP status codes to signal these (some come back 200/201 with an error body).
        self._raise_for_business_error(response)

        if response.status_code not in self.VALID_STATUS_CODES:
            raise exceptions.MeyerOrderAPIException(
                "Invalid API client response (status_code={}, data={})".format(
                    response.status_code, response.content.decode("utf-8")
                )
            )

        return simplejson.loads(response.content, parse_float=decimal.Decimal)

    # -- Shipping quote -----------------------------------------------------------------

    def get_shipping_rate_mass_quote(self, data: typing.Dict) -> typing.List[typing.Dict]:
        """POST /ShippingRateMassQuote. Non-binding, estimates only — safe to call freely."""
        result = self._request(
            endpoint="ShippingRateMassQuote", method=common_enums.HttpMethod.POST, json_body=data
        )
        return result if isinstance(result, list) else [result]

    # -- Order (SUBMIT/CANCEL — real effect, see module docstring) ------------------------

    def create_order(self, customer_number: str, data: typing.Dict, consolidate: bool = True) -> typing.Dict:
        """POST /CreateOrder. Places a real order. Do not call outside an explicit,
        user-approved submission flow."""
        return self._request(
            endpoint="CreateOrder",
            method=common_enums.HttpMethod.POST,
            params={"CustomerNumber": customer_number, "Consolidate": "1" if consolidate else "0"},
            json_body=data,
        )

    def cancel_order(self, order_number: str, customer_number: str) -> typing.Dict:
        """DELETE /CancelOrder. Real, possibly-irreversible effect (an order already being
        processed cannot be canceled). Do not call outside an explicit, user-approved flow."""
        return self._request(
            endpoint="CancelOrder",
            method=common_enums.HttpMethod.DELETE,
            params={"OrderNumber": order_number, "CustomerNumber": customer_number},
        )

    # -- Status / tracking ----------------------------------------------------------------

    def get_sales_order_detail(self, order_number: str, customer_number: str) -> typing.List[typing.Dict]:
        """GET /SalesOrderDetail. Accepts either a real Meyer OrderNumber (returns a single
        object) or a CustPO (returns an array, since one PO can map to several Meyer orders) —
        always normalized to a list here."""
        result = self._request(
            endpoint="SalesOrderDetail",
            method=common_enums.HttpMethod.GET,
            params={"OrderNumber": order_number, "CustomerNumber": customer_number},
        )
        return result if isinstance(result, list) else [result]

    def get_ship_methods(self) -> typing.List[typing.Dict]:
        """GET /ShipMethods. All ship methods valid for CreateOrder's ShipMethod field."""
        result = self._request(endpoint="ShipMethods", method=common_enums.HttpMethod.GET)
        return result if isinstance(result, list) else [result]

    def test_connection(self) -> None:
        """Cheap connectivity/auth probe — ShipMethods takes no parameters and doesn't need
        customer_number, so a successful call proves the username/password (and therefore the
        apikey exchange) work, without touching any order/customer data."""
        self.get_ship_methods()
