"""
Transport client for Turn 14's Electronic Order API (quote / order / order status). Separate
from ``client.py`` (the existing read-only catalog/pricing/inventory client used by the
nightly sync pipeline) since this targets a different capability (write operations) and, per
Turn 14's docs, a different host depending on "testing" vs "production" environment.

Reuses the same OAuth2 client-credentials flow as ``client.py`` rather than importing it
directly, so the read-only catalog sync path is never affected by anything here.

SAFETY: ``create_order`` and ``promote_quote_to_order`` submit a real order to Turn 14 (even
against the testing environment, this creates a real test order in their system). Callers
must never invoke these except through an explicit, user-approved submission — see the
Purchase Orders plan for the job-queue path this is meant to run behind.
"""
import decimal
import time
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.turn_14 import exceptions

TOKEN_EXPIRATION_BUFFER_SECONDS = 60
REQUEST_TIMEOUT_SECONDS = 30

_ENVIRONMENT_BASE_URLS = {
    "testing": settings.TURN14_ORDER_TEST_BASE_URL,
    "production": settings.TURN14_ORDER_PRODUCTION_BASE_URL,
}


class Turn14OrderApiClient(object):
    """
    One instance per (credentials, environment) pair. ``environment`` must be "testing" or
    "production" and must match the host implied by it — see _ENVIRONMENT_BASE_URLS — since
    Turn 14 requires both the host and the "environment" field in every request body to agree.
    """

    VALID_STATUS_CODES = [200, 201]
    LOG_PREFIX = "[TURN14-ORDER-API-CLIENT]"

    def __init__(self, credentials: typing.Dict, environment: str = "testing") -> None:
        self.client_id = credentials.get("client_id", "")
        self.client_secret = credentials.get("client_secret", "")
        if not self.client_id or not self.client_secret:
            raise ValueError("Invalid credentials parameter.")

        if environment not in _ENVIRONMENT_BASE_URLS:
            raise ValueError("Invalid environment: {}. Must be 'testing' or 'production'.".format(environment))
        self.environment = environment
        self.api_base_url = _ENVIRONMENT_BASE_URLS[environment]

        self._cached_token: typing.Optional[str] = None
        self._token_expires_at: typing.Optional[float] = None

    # -- Auth (same client-credentials flow as the read-only client) --------------------

    def _is_token_valid(self) -> bool:
        if self._cached_token is None or self._token_expires_at is None:
            return False
        return time.time() < (self._token_expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS)

    def _get_valid_token(self) -> str:
        if self._is_token_valid():
            return self._cached_token

        response = requests.request(
            url="{}/token".format(self.api_base_url),
            method=common_enums.HttpMethod.POST.value,
            headers={"Content-Type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code not in [200]:
            msg = "Invalid token response (status_code={}, data={})".format(
                response.status_code, response.content.decode("utf-8")
            )
            raise exceptions.Turn14APIBadResponseCodeError(message=msg, code=response.status_code)

        auth_data = simplejson.loads(response.content, parse_float=decimal.Decimal)
        access_token = auth_data.get("access_token")
        if not access_token:
            raise exceptions.Turn14APIException("No access_token in authorization response.")

        self._cached_token = access_token
        expires_in = auth_data.get("expires_in")
        if isinstance(expires_in, decimal.Decimal):
            expires_in = int(expires_in)
        self._token_expires_at = time.time() + (expires_in or 3600)
        return self._cached_token

    def _clear_token_cache(self) -> None:
        self._cached_token = None
        self._token_expires_at = None

    def _request(
        self,
        endpoint: str,
        method: common_enums.HttpMethod,
        payload: typing.Optional[dict] = None,
        params: typing.Optional[dict] = None,
        retry_on_401: bool = True,
    ) -> typing.Dict:
        url = "{}/{}".format(self.api_base_url, endpoint)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer {}".format(self._get_valid_token()),
        }
        try:
            response = requests.request(
                url=url,
                method=method.value,
                params=params,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.exceptions.ConnectTimeout as e:
            msg = "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            raise exceptions.Turn14APIException(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            raise exceptions.Turn14APIException(msg)

        if response.status_code == 401 and retry_on_401:
            self._clear_token_cache()
            return self._request(endpoint, method, payload=payload, params=params, retry_on_401=False)

        if response.status_code not in self.VALID_STATUS_CODES:
            msg = "Invalid API client response (status_code={}, data={})".format(
                response.status_code, response.content.decode("utf-8")
            )
            raise exceptions.Turn14APIBadResponseCodeError(message=msg, code=response.status_code)

        return simplejson.loads(response.content, parse_float=decimal.Decimal)

    # -- Quote --------------------------------------------------------------------------

    def create_quote(self, data: typing.Dict) -> typing.Dict:
        """POST /v1/quote. Non-binding — safe to call freely. ``data`` is the inner "data"
        object per Turn 14's schema (environment, po_number, locations, recipient, ...)."""
        return self._request(
            endpoint="quote",
            method=common_enums.HttpMethod.POST,
            payload={"data": data},
        )

    # -- Order (SUBMIT — real order placement, see module docstring) --------------------

    def create_order(self, data: typing.Dict) -> typing.Dict:
        """POST /v1/order. Places a real order. Do not call outside an explicit,
        user-approved submission flow."""
        return self._request(
            endpoint="order",
            method=common_enums.HttpMethod.POST,
            payload={"data": data},
        )

    def promote_quote_to_order(self, data: typing.Dict) -> typing.Dict:
        """POST /v1/order/from_quote. Places a real order from a prior create_quote() result.
        Do not call outside an explicit, user-approved submission flow."""
        return self._request(
            endpoint="order/from_quote",
            method=common_enums.HttpMethod.POST,
            payload={"data": data},
        )

    # -- Status / tracking ----------------------------------------------------------------

    def get_order(self, order_id: str) -> typing.Dict:
        """GET /v1/orders/{order_id}."""
        return self._request(
            endpoint="orders/{}".format(order_id),
            method=common_enums.HttpMethod.GET,
        )

    def get_orders_by_po_number(self, po_number: str) -> typing.Dict:
        """GET /v1/orders/po/{purchase_order_number}."""
        return self._request(
            endpoint="orders/po/{}".format(po_number),
            method=common_enums.HttpMethod.GET,
        )

    def get_shipping_options(self) -> typing.Dict:
        """GET /v1/shipping. All shipping service levels available to the account."""
        return self._request(
            endpoint="shipping",
            method=common_enums.HttpMethod.GET,
        )
