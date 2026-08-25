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
import typing

import requests
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations import rate_limit as rate_limit_base
from src.integrations.clients.turn_14 import USER_AGENT, exceptions
from src.integrations.clients.turn_14 import rate_limit as turn14_rate_limit

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

        # Identity for the shared token cache and the shared rate-limit buckets. On production
        # this is the bare client_id, so this client and the catalog client (client.py) draw on
        # the *same* token and the *same* 5 000/hour allowance -- which is what Turn 14 actually
        # meters, since both authenticate as the same credential set against the same host.
        # The testing host is a separate system with its own tokens and counters, so it gets a
        # separate identity rather than spending production's budget.
        self._identity = (
            self.client_id if environment == "production" else "{}@testing".format(self.client_id)
        )

    # -- Auth (same client-credentials flow as the read-only client) --------------------

    def _get_valid_token(self) -> str:
        cached = turn14_rate_limit.get_cached_token(self._identity)
        if cached is not None:
            return cached

        # Token issuance is metered per IP (10/minute) across every credential set and both
        # Turn 14 clients, so this bucket is deliberately not scoped to a client_id.
        rate_limit_base.acquire(turn14_rate_limit.token_buckets(), meter_key="t14:token")

        response = requests.request(
            url="{}/token".format(self.api_base_url),
            method=common_enums.HttpMethod.POST.value,
            headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
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

        expires_in = auth_data.get("expires_in")
        if isinstance(expires_in, decimal.Decimal):
            expires_in = int(expires_in)
        turn14_rate_limit.store_token(self._identity, access_token, expires_in)
        return access_token

    def _clear_token_cache(self) -> None:
        turn14_rate_limit.clear_token(self._identity)

    def _request(
        self,
        endpoint: str,
        method: common_enums.HttpMethod,
        payload: typing.Optional[dict] = None,
        params: typing.Optional[dict] = None,
        retry_on_401: bool = True,
        buckets: typing.Optional[typing.List[rate_limit_base.Bucket]] = None,
    ) -> typing.Dict:
        # This client had no rate limiting at all, while spending the same per-credential
        # allowance as the catalog client. ``buckets`` lets create_quote substitute Turn 14's
        # tighter 2/second quote limit for the ordinary 5/second one.
        rate_limit_base.acquire(
            buckets or turn14_rate_limit.get_buckets(self._identity), meter_key="t14:get"
        )

        url = "{}/{}".format(self.api_base_url, endpoint)
        headers = {
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
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
            return self._request(
                endpoint, method, payload=payload, params=params, retry_on_401=False, buckets=buckets
            )

        # See client.py's _request for why a 429 shuts the local bucket and defers.
        if response.status_code == 429:
            rate_limit_base.mark_exhausted(turn14_rate_limit.hourly_bucket(self._identity))
            raise rate_limit_base.RateBudgetExhausted(
                scope="t14:get:hour (upstream 429)",
                limit=turn14_rate_limit.GET_PER_HOUR,
                period_seconds=3600,
                retry_after_seconds=300.0,
            )

        if response.status_code not in self.VALID_STATUS_CODES:
            msg = "Invalid API client response (status_code={}, data={})".format(
                response.status_code, response.content.decode("utf-8")
            )
            raise exceptions.Turn14APIBadResponseCodeError(message=msg, code=response.status_code)

        return simplejson.loads(response.content, parse_float=decimal.Decimal)

    # -- Connection test ------------------------------------------------------------------

    def test_connection(self) -> None:
        """
        Validate order-API credentials by requesting a token, then confirming the account can
        read shipping options — the cheapest real, read-only call on the Order API. A Turn 14
        account can have valid catalog-API access (see clients/turn_14/client.py) without a
        separate grant for order placement, or vice versa; this catches that at connect time
        instead of surfacing later as a failed submit_order().
        """
        self._get_valid_token()
        try:
            self.get_shipping_options()
        except exceptions.Turn14APIBadResponseCodeError as e:
            if e.code in (401, 403):
                raise exceptions.Turn14PermissionError(
                    message=(
                        "Connected to Turn 14, but this account does not have permission to "
                        "place orders via the API ({} environment). Contact Turn 14 support to "
                        "enable order API access for this account.".format(self.environment)
                    ),
                    code=e.code,
                )
            raise

    # -- Quote --------------------------------------------------------------------------

    def create_quote(self, data: typing.Dict) -> typing.Dict:
        """POST /v1/quote. Non-binding — safe to call freely. ``data`` is the inner "data"
        object per Turn 14's schema (environment, po_number, locations, recipient, ...)."""
        return self._request(
            endpoint="quote",
            method=common_enums.HttpMethod.POST,
            payload={"data": data},
            buckets=turn14_rate_limit.quote_buckets(self._identity),
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

    def get_invoices_by_po_number(self, po_number: str) -> typing.Dict:
        """GET /v1/invoices/po/{purchase_order_number}. Invoices are only created once items
        actually ship, so this can legitimately return an empty ``data`` list for a while
        after an order is placed."""
        return self._request(
            endpoint="invoices/po/{}".format(po_number),
            method=common_enums.HttpMethod.GET,
        )

    # -- Bulk date-range sweeps ------------------------------------------------------------
    #
    # The hourly tier of Turn 14's proposed model. Per-PO polling costs one request per open
    # order per cycle; these cost one request per company per cycle regardless of how many
    # orders are open.

    def get_tracking(
        self,
        start_date: typing.Optional[str] = None,
        end_date: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        GET /v1/tracking. With no dates Turn 14 returns everything shipped today, which is
        exactly what an hourly sweep wants.

        Turn 14 rejects ranges wider than three days with a 400, so callers backfilling history
        must chunk -- see ``tracking_date_chunks``.
        """
        params = {}
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        return self._request(
            endpoint="tracking",
            method=common_enums.HttpMethod.GET,
            params=params or None,
        )

    def get_package_details(
        self,
        start_date: typing.Optional[str] = None,
        end_date: typing.Optional[str] = None,
        tracking_number: typing.Optional[str] = None,
    ) -> typing.Dict:
        """
        GET /v1/tracking/package_details -- package-level weights, dimensions and status behind
        a tracking number. Query by ``tracking_number`` for one shipment, or by date range
        (again, at most three days apart).
        """
        params = {}
        if tracking_number:
            params["tracking_number"] = tracking_number
        else:
            if start_date:
                params["start_date"] = start_date
            if end_date:
                params["end_date"] = end_date
        return self._request(
            endpoint="tracking/package_details",
            method=common_enums.HttpMethod.GET,
            params=params or None,
        )

    def get_invoices(self, start_date: str, end_date: str) -> typing.Dict:
        """
        GET /v1/invoices?start_date=&end_date=. Invoices only exist once items ship, so an
        hourly sweep over today..tomorrow is how an order stops being "uninvoiced".
        """
        return self._request(
            endpoint="invoices",
            method=common_enums.HttpMethod.GET,
            params={"start_date": start_date, "end_date": end_date},
        )

    def get_orders(self, start_date: str, end_date: str, page: int = 1) -> typing.Dict:
        """
        GET /v1/orders?start_date=&end_date=&page= -- every order placed in the range, paginated
        (confirmed live: same JSON:API {data, meta: {total_pages}, links} shape as the catalog
        client's endpoints), newest first. One bulk call (or a handful, paginated) here replaces
        what used to be one GET /v1/orders/po/{ref} call per confirmed PO.
        """
        return self._request(
            endpoint="orders",
            method=common_enums.HttpMethod.GET,
            params={"start_date": start_date, "end_date": end_date, "page": page},
        )

    def get_shipping_options(self) -> typing.Dict:
        """GET /v1/shipping. All shipping service levels available to the account."""
        return self._request(
            endpoint="shipping",
            method=common_enums.HttpMethod.GET,
        )
