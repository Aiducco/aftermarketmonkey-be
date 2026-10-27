"""
Transport client for Wheel Pros' Pricing API — real, per-account dealer cost (as opposed to the
SFTP feed's MSRP/MAP, which are manufacturer-published and the same for every account).
Spec: https://developer.wheelpros.com/assets/specs/pricing-api/openapi/api.html

Shares ``/auth/v1/authorize`` with the Order/Vehicle/Product clients; separate from all three
because it is its own, separately-entitled API area (confirmed live 2026-10-27: an account with
working Product/Order access can still 403 here) — same ``wp-api-core-pricing``-style Cognito
gating documented on ``vehicle_client.py``.

Confirmed live against real data (not just the published spec, which omits several of these):

* ``filters.company`` is undocumented but required — a bare request 400s with
  ``".body.filters must have required property company"``. Use the same sales-org codes as
  ``product_client.py`` ("1000" is CAD|USD, "1500" USD).
* No documented or discovered hard cap on ``filters.sku`` length, but the gateway 504s somewhere
  around 4,000-5,000 SKUs in one call (confirmed live: 4,000 succeeded in ~26s, 5,000 and 6,000
  both 504'd). :meth:`search_pricing` batches at a safe 2,500.
* A requested SKU that doesn't resolve in the given ``company`` is simply absent from the
  response — not an error, not a null placeholder. A SKU that *does* resolve but has no price of
  a given type comes back with ``currencyAmount: null`` for that type — also normal, not an error.
* ``nip`` (Net Invoice Price) is the real, per-account negotiated cost — confirmed live: median
  discount off ``msrp`` across 2,506 real SKUs matched a known dealer's manually-configured markup
  (30%) exactly, while the real per-SKU discount actually ranged 10%-50%, i.e. this is genuine
  per-SKU pricing, not another flat list tier.
"""
import logging
import threading
import time
import typing
from decimal import Decimal, InvalidOperation

import requests
from django.conf import settings

from src.integrations.clients.wheelpros import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-PRICING-CLIENT]"

TOKEN_EXPIRATION_BUFFER_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 90
MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 2.0

# No documented or discovered hard cap; found empirically that the gateway 504s around 4,000-5,000
# SKUs in one call. 2,500 leaves solid margin while still being far cheaper than one-SKU-at-a-time.
BATCH_SIZE = 2500

PRICE_TYPES = ("msrp", "map", "nip")

# A syntactically valid but essentially-guaranteed-not-to-match SKU, used only to prove the
# account is entitled to call this API at all (see test_connection) — mirrors the same
# real-call-as-probe pattern order_client.py uses for Orders API entitlement.
_CONNECTION_TEST_SKU = "AMS-CONNECTION-TEST-SKU"


class WheelProsPricingApiClient(object):
    """One instance per credentials set. Safe to share across threads."""

    def __init__(self, credentials: typing.Dict, company: str = "1000", currency: str = "USD") -> None:
        self.username = credentials.get("username", "")
        self.password = credentials.get("password", "")
        if not self.username or not self.password:
            raise ValueError("Invalid credentials parameter: username and password are required.")

        self.api_base_url = str(
            getattr(settings, "WHEELPROS_PRICING_BASE_URL", "") or "https://api.wheelpros.com"
        ).rstrip("/")
        self.company = company
        self.currency = currency

        self._cached_token: typing.Optional[str] = None
        self._token_expires_at: typing.Optional[float] = None
        self._token_lock = threading.Lock()
        self._thread_state = threading.local()

    # -- auth ------------------------------------------------------------------------------------

    def _is_token_valid(self) -> bool:
        if self._cached_token is None or self._token_expires_at is None:
            return False
        return time.time() < (self._token_expires_at - TOKEN_EXPIRATION_BUFFER_SECONDS)

    def _get_valid_token(self) -> str:
        if self._is_token_valid():
            return self._cached_token
        with self._token_lock:
            if self._is_token_valid():
                return self._cached_token
            try:
                response = requests.post(
                    "{}/auth/v1/authorize".format(self.api_base_url),
                    headers={"Content-Type": "application/json"},
                    json={"userName": self.username, "password": self.password},
                    timeout=60,
                )
            except requests.RequestException as e:
                raise exceptions.WheelProsPricingAPIException("Authentication failed: {}".format(e))
            if response.status_code == 401:
                raise exceptions.WheelProsOrderAuthError("Invalid Wheel Pros username/password.")
            if response.status_code != 200:
                raise exceptions.WheelProsPricingAPIException(
                    "Invalid authorize response ({}): {}".format(response.status_code, response.text[:300])
                )
            data = response.json()
            self._cached_token = data.get("accessToken")
            if not self._cached_token:
                raise exceptions.WheelProsPricingAPIException("No accessToken in authorize response.")
            self._token_expires_at = time.time() + int(data.get("expiresIn") or 3600)
            return self._cached_token

    # -- transport -------------------------------------------------------------------------------

    def _session(self) -> requests.Session:
        session = getattr(self._thread_state, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_state.session = session
        return session

    def _discard_session(self) -> None:
        session = getattr(self._thread_state, "session", None)
        self._thread_state.session = None
        if session is not None:
            try:
                session.close()
            except Exception:
                logger.debug("%s ignoring error closing a broken session", _LOG_PREFIX, exc_info=True)

    def _post(self, path: str, body: dict) -> typing.Any:
        """POST with retries. 5xx (including the gateway timeouts a too-large batch can trigger)
        is retried rather than raised immediately."""
        url = "{}{}".format(self.api_base_url, path)
        last_error: typing.Optional[Exception] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            token = self._get_valid_token()
            try:
                response = self._session().post(
                    url,
                    headers={
                        "Authorization": "Bearer {}".format(token),
                        "Content-Type": "application/json",
                        "Accept": "application/json",
                    },
                    json=body,
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
            except requests.RequestException as e:
                last_error = e
                self._discard_session()
                logger.warning("%s transport error on %s (%s/%s): %s", _LOG_PREFIX, url, attempt, MAX_ATTEMPTS, e)
            else:
                if response.status_code == 200:
                    return response.json()
                if response.status_code == 403:
                    raise exceptions.WheelProsPricingPermissionError(
                        "403 from {}. Account {} is not entitled to the Pricing API.".format(url, self.username)
                    )
                if response.status_code == 401:
                    self._cached_token = None
                    last_error = exceptions.WheelProsPricingAPIException("401 from {}".format(url))
                elif response.status_code == 400:
                    # A genuine validation error (e.g. missing filters.company) will not improve
                    # with a retry -- fail immediately with the distributor's own message.
                    raise exceptions.WheelProsPricingAPIException(
                        "400 from {}: {}".format(url, response.text[:300])
                    )
                elif response.status_code == 429 or response.status_code >= 500:
                    last_error = exceptions.WheelProsPricingAPIException(
                        "{} from {}: {}".format(response.status_code, url, response.text[:200])
                    )
                    logger.warning(
                        "%s HTTP %s on %s (%s/%s)", _LOG_PREFIX, response.status_code, url, attempt, MAX_ATTEMPTS
                    )
                else:
                    raise exceptions.WheelProsPricingAPIException(
                        "{} from {}: {}".format(response.status_code, url, response.text[:300])
                    )
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
        raise exceptions.WheelProsPricingAPIException(
            "Giving up on {} after {} attempts: {}".format(url, MAX_ATTEMPTS, last_error)
        )

    # -- pricing -----------------------------------------------------------------------------------

    @staticmethod
    def _to_decimal(raw: typing.Any) -> typing.Optional[Decimal]:
        if raw is None:
            return None
        try:
            return Decimal(str(raw))
        except (InvalidOperation, ValueError, TypeError):
            return None

    def _search_one_batch(self, skus: typing.List[str], price_types: typing.Sequence[str]) -> dict:
        body = {
            "filters": {"sku": skus, "company": self.company, "currency": self.currency},
            "priceType": list(price_types),
        }
        data = self._post("/pricings/v1/search", body)
        out = {}
        for row in data or []:
            sku = row.get("sku")
            if not sku:
                continue
            prices = row.get("prices") or {}
            parsed = {}
            for price_type in price_types:
                entries = prices.get(price_type) or []
                parsed[price_type] = self._to_decimal(entries[0].get("currencyAmount")) if entries else None
            out[sku] = parsed
        return out

    def search_pricing(
        self, skus: typing.Iterable[str], price_types: typing.Sequence[str] = PRICE_TYPES
    ) -> typing.Dict[str, typing.Dict[str, typing.Optional[Decimal]]]:
        """
        ``{sku: {"msrp": Decimal|None, "map": Decimal|None, "nip": Decimal|None}}`` for every
        requested SKU that resolves in ``self.company`` — a SKU wholly absent from WheelPros'
        response (doesn't exist, or doesn't exist in this sales org) is simply absent from the
        returned dict, not an error and not a ``None`` placeholder entry.
        """
        sku_list = [s for s in dict.fromkeys(skus) if s]  # de-dupe, preserve order, drop blanks
        if not sku_list:
            return {}
        out: typing.Dict[str, typing.Dict[str, typing.Optional[Decimal]]] = {}
        for i in range(0, len(sku_list), BATCH_SIZE):
            batch = sku_list[i : i + BATCH_SIZE]
            out.update(self._search_one_batch(batch, price_types))
        return out

    def test_connection(self) -> None:
        """Authenticates, then confirms Pricing API access specifically with a cheap real call
        (a SKU guaranteed not to exist still returns 200 with an empty/absent result -- only a
        403 distinguishes "not entitled to this API" from "bad credentials", which
        _get_valid_token already catches separately)."""
        self.search_pricing([_CONNECTION_TEST_SKU], price_types=("msrp",))
