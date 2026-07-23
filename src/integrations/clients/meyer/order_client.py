"""
Transport client for Meyer Distributing's Order API (v2 REST/JSON) — shipping rate quotes,
order placement, cancellation, and order-status/tracking lookups. Separate from ``client.py``
(the existing SFTP relay client used by the nightly catalog/pricing sync) since this targets a
different capability (write operations) over a different transport (REST over HTTPS vs. SFTP)
and credential shape (api_key + customer_number, vs. sftp_user/sftp_password).

Auth: a static API key issued directly by a Meyer rep, used as-is — per Meyer's own docs, "If
you receive a static key from us, you don't need to make a call to [the Authentication]
endpoint," so this client never does. Every call carries the key in an
``Authorization: Espresso <apikey>:1`` header — a non-standard scheme specific to Meyer's API
gateway (Espresso Logic), not a bearer token despite the similar shape. There is no rotation
logic here: a static key isn't ours to refresh — if Meyer ever invalidates one, a new key has
to come from the Meyer rep and be re-entered, the same as any other distributor credential
update.

Transport: every request is routed through a SOCKS5 proxy (``settings.MEYER_ORDER_PROXY_URL``)
whose IP is allowlisted with Meyer — direct connections to either Meyer host fail the TLS
handshake entirely from non-allowlisted IPs (TCP connects, ``ClientHello`` sent, no
``ServerHello`` ever comes back). Requires ``PySocks`` for the ``socks5h://`` scheme.

SAFETY: ``create_order`` places a REAL order against Meyer (even against the "testing"
environment — Meyer's docs warn the test API "can contain outdated information" but do not
describe it as a sandbox that skips real order creation). ``cancel_order`` also has real,
possibly irreversible effect (Meyer's docs: an order already being processed cannot be canceled
via the API). Callers must never invoke either except through an explicit, user-approved
submission — see the Purchase Orders job-queue path this is meant to run behind.
"""
import decimal
import logging
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations.clients.meyer import exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[MEYER-ORDER-CLIENT]"

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
        self.api_key = credentials.get("api_key", "")
        self.customer_number = credentials.get("customer_number", "")
        if not self.api_key or not self.customer_number:
            raise ValueError("Invalid credentials parameter.")

        if environment not in _ENVIRONMENT_BASE_URLS:
            raise ValueError("Invalid environment: {}. Must be 'testing' or 'production'.".format(environment))
        self.environment = environment
        self.api_base_url = _ENVIRONMENT_BASE_URLS[environment]
        self.proxy_url = settings.MEYER_ORDER_PROXY_URL or None

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
    ) -> typing.Union[typing.Dict, typing.List]:
        url = "{}/{}".format(self.api_base_url, endpoint)
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Espresso {}:1".format(self.api_key),
        }
        logger.info(
            "{} -> {} {} (proxy={}, params={}, json_body={})".format(
                _LOG_PREFIX, method.value, url, self.proxy_url, params, json_body
            )
        )
        try:
            response = requests.request(
                url=url,
                method=method.value,
                params=params,
                json=json_body,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
                proxies={"http": self.proxy_url, "https": self.proxy_url} if self.proxy_url else None,
            )
        except requests.exceptions.ConnectTimeout as e:
            logger.error("{} Connect timeout calling {} {}.".format(_LOG_PREFIX, method.value, url))
            raise exceptions.MeyerOrderAPIException(
                "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            )
        except requests.RequestException as e:
            logger.error("{} Request exception calling {} {}: {}.".format(_LOG_PREFIX, method.value, url, e))
            raise exceptions.MeyerOrderAPIException(
                "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            )

        logger.info(
            "{} <- {} {} status={} body={}".format(
                _LOG_PREFIX, method.value, url, response.status_code, response.content[:2000]
            )
        )

        # A static key isn't ours to refresh (see module docstring) — a 401 here means the
        # stored key itself is invalid/revoked, not a transient/expired-token condition to
        # retry past.
        if response.status_code == 401:
            raise exceptions.MeyerOrderAuthError("Meyer API rejected the apikey.")

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

    # -- Pricing ----------------------------------------------------------------------------

    def get_item_information(self, item_numbers: typing.List[str], customer_number: str) -> typing.List[typing.Dict]:
        """GET /ItemInformation. Returns part description, dimensions, inventory, and pricing
        (CustomerPrice) for up to 100 items per call — Meyer's docs cap it there and don't
        otherwise rate-limit this call (no "once per hour" warning like Keystone's
        CheckPriceBulk), so callers don't need a caching layer in front of it; see
        MeyerOrderAdapter._get_prices, the only caller, for chunking. Non-binding — safe to
        call freely."""
        result = self._request(
            endpoint="ItemInformation",
            method=common_enums.HttpMethod.GET,
            params={"CustomerNumber": customer_number, "ItemNumber": ",".join(item_numbers)},
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

    def get_warehouses(self) -> typing.List[typing.Dict]:
        """GET /Warehouses. All active Meyer warehouses — {LocationCode, City, State, Country}
        — used to decode a shipping quote's bare warehouse code into a human-readable location
        (see fetch_and_save_meyer_locations / MeyerLocation)."""
        result = self._request(endpoint="Warehouses", method=common_enums.HttpMethod.GET)
        return result if isinstance(result, list) else [result]

    def test_connection(self) -> None:
        """Cheap connectivity/auth probe — ShipMethods takes no parameters and doesn't need
        customer_number, so a successful call proves the api_key alone is valid, without
        touching any order/customer data."""
        self.get_ship_methods()
