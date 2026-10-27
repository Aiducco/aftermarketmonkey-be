"""
Client for TBC Corporation's IBM API Connect gateway -- the two APIs behind NTW that are not the
file feed: on-demand tire inventory (v2) and ordering (v1).

Auth is two static headers, ``X-IBM-Client-Id`` and ``X-IBM-Client-Secret``, with no token
exchange. They are **account-level, not per-dealer**: one application registered to us covers
every dealer, and the dealer is identified by ``customerId`` in each request body. That is why
the credentials live in settings while ``customer_id`` lives on the connection -- the only
provider in this codebase split that way, so expect it to look wrong at a glance.

Note the version split: inventory is ``/v2/products/tires`` and ordering is ``/v1/order``. They
are two APIs that happen to share a gateway, not one versioned surface.

Four behaviours verified live 2026-10-26/27 that this client exists to contain:

1. **``criteria[]`` honours only the first TWO entries.** Five tire sizes returned rows for two,
   HTTP 200, no warning; reordering proved it keeps the first two. So ``get_tire_inventory``
   chunks its criteria in pairs and merges, rather than trusting one call.
2. **``requestedQty`` is an availability filter, not a quantity.** One size returned 15 rows at
   qty 1, 13 at 2, 5 at 10, 1 at 100. Reading stock therefore always sends 1; the quantity a
   buyer actually wants is compared against the response afterwards, never pushed into the
   filter.
3. **Errors arrive as HTTP 200** with ``{errorCd, errorDesc, errorMsg}`` and no ``product`` key.
   Branching on the status code alone silently turns a failure into "no stock".
4. **The TLS chain is incomplete.** ``tbcservicesqa.tbccorp.com`` serves only its leaf and omits
   the ``Go Daddy Secure Certificate Authority - G2`` intermediate, so ``requests`` fails with
   "unable to get local issuer certificate" while a browser (which fetches the intermediate via
   the certificate's AIA extension) looks fine. The fix is a CA bundle containing that
   intermediate, pointed at by ``settings.NTW_API_CA_BUNDLE``. Verification is never disabled:
   these calls carry a dealer's cost and place real orders.

``searchType: "ByPartNumber"`` is not used anywhere here because it does not work -- errorCd 1006
for every identifier shape tried, including TBC's own documented sample and the TBC-internal item
number that some feed rows carry in ``product_id``. A single-SKU lookup therefore goes by that
SKU's tire size and filters the response, which is what ``find_tire_by_part_number`` does.
"""
import hashlib
import hmac
import json
import logging
import time
import typing

import requests
from django.conf import settings

from src.integrations.clients.ntw import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[NTW-API-CLIENT]"

# The gateway honours only the first two criteria entries; see the module docstring.
MAX_CRITERIA_PER_CALL = 2
# Always 1 when reading stock, because requestedQty filters rather than requests.
STOCK_READ_QTY = 1

DEFAULT_TIMEOUT_SECONDS = 45

SEARCH_BY_TIRE_SIZE = "ByTireSize"
CUSTOMER_TYPE_EXTERNAL = "external"


class NtwApiClient:
    def __init__(
        self,
        client_id: typing.Optional[str] = None,
        client_secret: typing.Optional[str] = None,
        base_url: typing.Optional[str] = None,
        ca_bundle: typing.Optional[str] = None,
        signing_secret: typing.Optional[str] = None,
    ) -> None:
        self.client_id = client_id or getattr(settings, "NTW_API_CLIENT_ID", "") or ""
        self.client_secret = client_secret or getattr(settings, "NTW_API_CLIENT_SECRET", "") or ""
        self.base_url = (base_url or getattr(settings, "NTW_API_BASE_URL", "") or "").rstrip("/")
        self.ca_bundle = ca_bundle or getattr(settings, "NTW_API_CA_BUNDLE", "") or ""
        self.signing_secret = signing_secret or getattr(settings, "NTW_API_SIGNING_SECRET", "") or ""

        if not self.client_id or not self.client_secret:
            raise exceptions.NtwApiNotConfiguredError(
                "NTW_API_CLIENT_ID / NTW_API_CLIENT_SECRET are not set. These are account-level "
                "gateway credentials, not per-dealer, so they belong in the environment."
            )
        if not self.base_url:
            raise exceptions.NtwApiNotConfiguredError("NTW_API_BASE_URL is not set.")

    # -- transport -----------------------------------------------------------

    def _verify(self) -> typing.Any:
        """
        The CA bundle that completes TBC's chain. Missing configuration raises rather than
        falling back to ``verify=False``: a silent downgrade here would post a dealer's
        credentials and orders into an unauthenticated connection.
        """
        if not self.ca_bundle:
            raise exceptions.NtwApiNotConfiguredError(
                "NTW_API_CA_BUNDLE is not set. TBC's gateway serves only its leaf certificate, so "
                "a bundle containing the 'Go Daddy Secure Certificate Authority - G2' intermediate "
                "is required (the certificate's AIA extension names it: "
                "http://certificates.godaddy.com/repository/gdig2.crt). Verification is not "
                "optional for this API."
            )
        return self.ca_bundle

    def _headers(self) -> typing.Dict[str, str]:
        return {
            "X-IBM-Client-Id": self.client_id,
            "X-IBM-Client-Secret": self.client_secret,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        path: str,
        body: typing.Optional[typing.Dict] = None,
        params: typing.Optional[typing.Dict] = None,
    ) -> typing.Dict:
        url = "{}{}".format(self.base_url, path)
        try:
            response = requests.request(
                method,
                url,
                headers=self._headers(),
                json=body,
                params=params,
                timeout=DEFAULT_TIMEOUT_SECONDS,
                verify=self._verify(),
            )
        except exceptions.NtwException:
            raise
        except requests.exceptions.SSLError as e:
            raise exceptions.NtwApiError(
                "TLS failure talking to TBC ({}). Their gateway omits the GoDaddy G2 "
                "intermediate; check NTW_API_CA_BUNDLE. Error: {}".format(url, str(e))
            )
        except requests.exceptions.RequestException as e:
            raise exceptions.NtwApiError("Request to {} failed: {}".format(url, str(e)))

        if response.status_code == 401:
            raise exceptions.NtwApiError(
                "TBC rejected the gateway credentials (401). Body: {}".format(response.text[:300]),
                status=401,
            )
        if response.status_code == 403:
            # The gateway's own wording is "Not registered to plan." -- the application exists
            # and authenticates, but is not subscribed to this API's plan. Only TBC can fix it,
            # so say so rather than letting it read as a credential problem.
            raise exceptions.NtwApiError(
                "TBC authenticated the application but refused the call (403): {}. If this says "
                "'Not registered to plan', the app is not subscribed to this API product and TBC "
                "has to subscribe it.".format(response.text[:300]),
                status=403,
            )

        try:
            payload = response.json()
        except ValueError:
            raise exceptions.NtwApiError(
                "TBC returned non-JSON ({}): {}".format(response.status_code, response.text[:300]),
                status=response.status_code,
            )

        if response.status_code >= 400:
            raise exceptions.NtwApiBusinessError(
                self._error_message(payload) or "TBC returned HTTP {}.".format(response.status_code),
                status=response.status_code,
                payload=payload,
            )
        return payload

    @staticmethod
    def _error_message(payload: typing.Any) -> typing.Optional[str]:
        if not isinstance(payload, dict):
            return None
        for key in ("errorMsg", "moreInformation", "httpMessage", "message"):
            value = payload.get(key)
            if value:
                return str(value)
        return None

    # -- inventory (v2) ------------------------------------------------------

    def get_tire_inventory(
        self,
        customer_id: str,
        criteria: typing.List[typing.Dict],
        store_number: typing.Optional[str] = None,
        is_drop_ship: bool = False,
        dealer_code: typing.Optional[str] = None,
    ) -> typing.List[typing.Dict]:
        """
        Every product row TBC returns for ``criteria``, which are ``{"tireSize": {...},
        "requestedQty": n}`` entries.

        Chunked in pairs because the gateway drops everything past the second entry **without
        erroring** -- the single most dangerous behaviour of this API, since a 10-size request
        looks like a success and silently answers two of them.
        """
        if not customer_id:
            raise exceptions.NtwApiNotConfiguredError(
                "This connection has no NTW customer_id, which the inventory API requires. It is "
                "not the account number in the feed filename -- TBC issues it separately."
            )
        if not criteria:
            return []

        products: typing.List[typing.Dict] = []
        for i in range(0, len(criteria), MAX_CRITERIA_PER_CALL):
            chunk = criteria[i : i + MAX_CRITERIA_PER_CALL]
            body = {
                "customerId": str(customer_id),
                "isDropShip": bool(is_drop_ship),
                "customerType": CUSTOMER_TYPE_EXTERNAL,
                "searchType": SEARCH_BY_TIRE_SIZE,
                "criteria": chunk,
            }
            if store_number:
                body["storeNumber"] = str(store_number)
            params = {"dealerCode": dealer_code} if dealer_code else None

            payload = self._request("POST", "/v2/products/tires", body=body, params=params)

            # HTTP 200 with an errorCd body and no product key. "Could not get any inventory from
            # backend" (1006) is a legitimate empty answer for a size nobody stocks, so it is not
            # raised -- anything else is.
            if "product" not in payload:
                code = str(payload.get("errorCd") or "")
                message = self._error_message(payload) or "TBC returned no product list."
                if code == "1006":
                    logger.info("{} No inventory for criteria chunk: {}".format(_LOG_PREFIX, message))
                    continue
                raise exceptions.NtwApiBusinessError(message, status=200, payload=payload)

            rows = payload.get("product") or []
            if isinstance(rows, dict):
                rows = [rows]
            products.extend(rows)

        return products

    def find_tire_by_part_number(
        self,
        customer_id: str,
        part_number: str,
        tire_size: typing.Dict,
        **kwargs: typing.Any,
    ) -> typing.Optional[typing.Dict]:
        """
        One SKU's live row, looked up by its size and then filtered down by part number -- the
        detour ``searchType: "ByPartNumber"`` would make unnecessary if it worked. Returns None
        when TBC lists the size but not this part, which is the normal "out of stock" answer.
        """
        rows = self.get_tire_inventory(
            customer_id,
            [{"tireSize": tire_size, "requestedQty": STOCK_READ_QTY}],
            **kwargs
        )
        wanted = (part_number or "").strip().upper()
        for row in rows:
            if str(row.get("partNo") or "").strip().upper() == wanted:
                return row
        return None

    # -- ordering (v1) -------------------------------------------------------

    def _signature(self, body: typing.Optional[typing.Dict], timestamp: str) -> str:
        """
        HMAC SHA-256 for the order endpoints' ``signature`` query parameter.

        **TBC has not documented what string is signed.** Their docs show ``timestamp`` and
        ``signature`` as opaque query parameters with example values that are not derivable
        (``"demiwiru"``). What is implemented here is the conventional construction --
        ``timestamp`` followed by the exact request body, keyed with the shared secret -- and it
        is isolated in this one method precisely so the canonicalisation can be corrected without
        touching the adapter.

        It fails loudly when no secret is configured rather than sending an unsigned order,
        because the failure mode on the other side of a wrong signature is unknown: an order
        that is silently accepted-but-unsigned, or rejected hours later with no 997-equivalent to
        tell us, are both worse than not sending it.
        """
        if not self.signing_secret:
            raise exceptions.NtwApiNotConfiguredError(
                "NTW_API_SIGNING_SECRET is not set, and TBC's order endpoints require an HMAC "
                "SHA-256 signature. Ask TBC for the signing key and the exact string it is "
                "computed over before enabling NTW ordering."
            )
        message = timestamp + (json.dumps(body, separators=(",", ":"), sort_keys=True) if body else "")
        return hmac.new(
            self.signing_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def create_order(self, body: typing.Dict) -> typing.Any:
        """
        POST /v1/order. Returns TBC's response verbatim -- **an array** of
        ``{id, items}`` objects, because one submit can yield several TBC order numbers.
        """
        timestamp = str(int(time.time()))
        params = {"signature": self._signature(body, timestamp), "timestamp": timestamp}
        return self._request("POST", "/v1/order", body=body, params=params)

    def get_order(self, order_id: str) -> typing.Dict:
        """GET /v1/order/{orderId} -- status, estDelivery, location, pickUpPhone, trackingNumber."""
        timestamp = str(int(time.time()))
        params = {"signature": self._signature(None, timestamp), "timestamp": timestamp}
        return self._request("GET", "/v1/order/{}".format(order_id), params=params)
