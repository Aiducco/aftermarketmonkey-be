import decimal
import typing
import requests
import logging
import simplejson
from django.conf import settings

from common import enums as common_enums
from common import utils as common_utils
from src.integrations import rate_limit as rate_limit_base
from src.integrations.clients.turn_14 import USER_AGENT, exceptions
from src.integrations.clients.turn_14 import rate_limit as turn14_rate_limit

logger = logging.getLogger(__name__)


def _retry_after_seconds(response: requests.Response, default: float = 300.0) -> float:
    """``Retry-After`` in seconds when the response carries a usable one, else ``default``."""
    raw = (response.headers or {}).get("Retry-After")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default

# requests.request() had no timeout at all before, so a stalled Turn 14 response could hang
# the calling thread indefinitely — this matters now that test_connection() runs synchronously
# inside the connect/update-connection HTTP request.
REQUEST_TIMEOUT_SECONDS = 30


class Turn14ApiClient(object):
    API_BASE_URL = settings.TURN14_BASE_URL
    VALID_STATUS_CODES = [200]

    LOG_PREFIX = "[TURN14-API-CLIENT]"

    def __init__(self, credentials: typing.Dict):
        self.client_id = credentials.get("client_id", "")
        self.client_secret = credentials.get("client_secret", "")

        if not self.client_id or not self.client_secret:
            raise ValueError("Invalid credentials parameter.")

    def _get_valid_token(self) -> str:
        """
        A live access token, minted only when the cache has none.

        The cache is process-wide and keyed by client_id (see turn_14.rate_limit), not
        per-instance: several services construct a fresh client inside a per-brand loop, which
        under the old per-instance cache meant one token request per brand -- 464 per sweep
        against Turn 14's 10-per-minute-per-IP token limit.
        """
        cached = turn14_rate_limit.get_cached_token(self.client_id)
        if cached is not None:
            logger.debug("{} Using cached authorization token.".format(self.LOG_PREFIX))
            return cached

        logger.debug("{} Creating new authorization token.".format(self.LOG_PREFIX))
        auth_data = self._create_authorization_token()

        access_token = auth_data.get('access_token')
        if not access_token:
            raise exceptions.Turn14APIException("No access_token in authorization response.")

        expires_in = auth_data.get('expires_in')
        if isinstance(expires_in, decimal.Decimal):
            expires_in = int(expires_in)
        if not expires_in:
            logger.warning(
                "{} No expires_in in token response. Assuming 1 hour expiration.".format(self.LOG_PREFIX)
            )

        turn14_rate_limit.store_token(self.client_id, access_token, expires_in)
        return access_token

    def _permission_or_reraise(
        self, e: exceptions.Turn14APIBadResponseCodeError, resource: str
    ) -> exceptions.Turn14APIBadResponseCodeError:
        """
        A 401/403 here means the token itself was already accepted (test_connection got this
        far), so it's the account lacking a permission grant, not bad credentials — convert to
        Turn14PermissionError with a resource-specific message. Any other status is left as-is.
        """
        if e.code in (401, 403):
            return exceptions.Turn14PermissionError(
                message=(
                    "Connected to Turn 14, but this account does not have permission to "
                    "access {} data. Contact Turn 14 support to enable API access for "
                    "your client_id.".format(resource)
                ),
                code=e.code,
            )
        return e

    def test_connection(self) -> None:
        """
        Validate credentials by requesting a token, then confirm the account can actually
        read Brands data. Some Turn 14 accounts have valid API credentials but lack API
        permission (a separate grant from Turn 14 support), which otherwise only surfaces
        later as a failed catalog sync — catch it here instead. Brands is used because it's
        the cheapest real endpoint to check against — a single, unscoped, unpaginated call.
        """
        self._get_valid_token()

        try:
            self.get_brands()
        except exceptions.Turn14APIBadResponseCodeError as e:
            raise self._permission_or_reraise(e, "Brands")

    def _create_authorization_token(self) -> typing.Dict:
        """
        Mint a new token. Turn 14 meters token issuance at 10/minute **per IP** (not per
        credential), so the bucket this waits on is shared by every credential set on this
        host and by the Order API client.
        """
        rate_limit_base.acquire(turn14_rate_limit.token_buckets(), meter_key="t14:token")
        try:
            response = requests.request(
                url=f"{self.API_BASE_URL}/token",
                method=common_enums.HttpMethod.POST.value,
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": USER_AGENT,
                },
                json={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code not in self.VALID_STATUS_CODES:
                msg = "Invalid API client response (status_code={}, data={})".format(
                    response.status_code,
                    response.content.decode(encoding="utf-8"),
                )
                logger.error("{} {}.".format(self.LOG_PREFIX, msg))
                raise exceptions.Turn14APIBadResponseCodeError(message=msg, code=response.status_code)

        except requests.exceptions.ConnectTimeout as e:
            msg = "Connect timeout. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.Turn14APIException(msg)
        except requests.RequestException as e:
            msg = "Request exception. Error: {}".format(common_utils.get_exception_message(exception=e))
            logger.exception("{} {}.".format(self.LOG_PREFIX, msg))
            raise exceptions.Turn14APIException(msg)

        return simplejson.loads(response.content, parse_float=decimal.Decimal)

    def _paginated(
        self,
        endpoint: str,
        page: int,
        extra_params: typing.Optional[typing.Dict] = None,
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """
        One page of a JSON:API collection, plus the next page number (None on the last page).

        Every paginated Turn 14 endpoint answers the same shape -- ``{"meta": {"total_pages": N},
        "data": [...]}`` -- and this was open-coded identically in a dozen methods.

        Note ``total_pages`` defaults to 1, not 0: a response that omits meta entirely is a
        single page, and treating it as zero would make ``page == total_pages`` false and page
        forever.
        """
        params = {"page": page}
        if extra_params:
            params.update(extra_params)

        response = simplejson.loads(
            self._request(
                endpoint=endpoint,
                method=common_enums.HttpMethod.GET,
                params=params,
            ).content
        )

        data = response.get("data", [])
        total_pages = response.get("meta", {}).get("total_pages", 1)
        next_page = None if page >= total_pages else page + 1

        return data, next_page

    def get_pricelists(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        return self._paginated("pricing/brand/{}".format(brand_id), page)

    def get_items_for_brand(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        return self._paginated("items/brand/{}".format(brand_id), page)

    def get_inventory_items_for_brand(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        return self._paginated("inventory/brand/{}".format(brand_id), page)

    def get_inventory_item(self, item_id: str) -> typing.Optional[typing.Dict]:
        """
        GET /v1/inventory/{item_id} - live inventory for a single item (per Turn14's docs this
        also accepts a comma-separated list of up to 250 ids, but we only ever pass one), for
        on-demand refresh distinct from get_inventory_items_for_brand's paginated bulk pull
        used by the scheduled catalog sync. Takes the same numeric item id used everywhere
        else (ProviderPart.provider_external_id / Turn14Items.external_id) - no separate
        lookup needed. Returns the raw JSON:API "data" object, or None if Turn14 has no
        inventory record for this item id (404, "No item exists for that item_id").
        """
        try:
            response = simplejson.loads(
                self._request(
                    endpoint="inventory/{}".format(item_id),
                    method=common_enums.HttpMethod.GET,
                ).content
            )
        except exceptions.Turn14APIBadResponseCodeError as e:
            if e.code == 404:
                return None
            raise

        data = response.get("data")
        if isinstance(data, list):
            return data[0] if data else None
        return data if isinstance(data, dict) else None

    def get_inventory_items_updates(
            self, page: int = 1, minutes: int = 60
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        return self._paginated("inventory/updates", page, extra_params={"minutes": str(minutes)})

    def get_items_updates(
            self, page: int = 1, days: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        return self._paginated("items/updates", page, extra_params={"days": str(days)})

    def get_pricing_changes(
        self,
        start_date: str,
        end_date: str,
        page: int = 1,
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """
        GET /v1/pricing/changes. Each item has id, type 'PricingChange', and attributes.itemcode.

        Not in Turn 14's published documentation -- it works and the delta pricing path depends
        on it, but treat it as unsupported until they confirm otherwise (open question for Dan).
        """
        return self._paginated(
            "pricing/changes", page, extra_params={"start_date": start_date, "end_date": end_date}
        )

    def get_item_fitment_for_brand(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        return self._paginated("items/fitment/brand/{}".format(brand_id), page)

    def get_brand_media(
            self, brand_id: str, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        return self._paginated("items/data/brand/{}".format(brand_id), page)

    # -- Catalog-wide sweeps ---------------------------------------------------------------
    #
    # The unscoped collections, as opposed to the /brand/{id} variants above. Measured page
    # sizes: these return 1 000 rows/page (items/data 450, fitment 200) where the brand-scoped
    # endpoints return 200 -- so sweeping the catalog flat costs ~776 requests against ~4 200
    # for the same data brand by brand. Only `items` carries attributes.brand_id; the rest
    # identify rows by item id alone, so callers resolve the brand through Turn14Items (which
    # is why the items sweep has to run first).

    def get_items(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """GET /v1/items - every item Turn 14 carries."""
        return self._paginated("items", page)

    def get_items_data(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """GET /v1/items/data - media and descriptions for every item."""
        return self._paginated("items/data", page)

    def get_inventory(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """GET /v1/inventory - warehouse availability for every item."""
        return self._paginated("inventory", page)

    def get_items_fitment(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """GET /v1/items/fitment - ACES vehicle ids per item, for every item."""
        return self._paginated("items/fitment", page)

    def get_pricing(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """GET /v1/pricing - this account's pricing for every item. Customer-specific."""
        return self._paginated("pricing", page)

    def get_dropship_controller(self, dropship_id: int) -> typing.Optional[typing.Dict]:
        """
        GET /v1/dropship/{id} - the ruleset and fee schedule governing whether a brand can
        dropship, keyed by Turn14Items.dropship_controller_id. Returns None when Turn 14 has no
        such controller (404), which is not an error worth failing a sweep over.
        """
        try:
            response = simplejson.loads(
                self._request(
                    endpoint="dropship/{}".format(dropship_id),
                    method=common_enums.HttpMethod.GET,
                ).content
            )
        except exceptions.Turn14APIBadResponseCodeError as e:
            if e.code == 404:
                return None
            raise
        return response.get("data")

    def get_item_shipping_estimates_for_brand(
            self, brand_id: int, page: int = 1
    ) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """GET /v1/shipping/item_estimation/brand/{id} - min/average/max ground rates per item."""
        return self._paginated("shipping/item_estimation/brand/{}".format(brand_id), page)

    def get_item_shipping_estimates(self, page: int = 1) -> typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]:
        """
        GET /v1/shipping/item_estimation - flat, unscoped. 1000 rows/page (measured live), same
        as the per-brand variant -- confirmed empirically 2026-08-25, contradicting the earlier
        assumption both were 200/page. Summed over every brand's own ceil(items/1000), the
        per-brand walk costs 1081 requests against this endpoint's 795 for the same catalog
        (measured live against production: 457 brands, 794581 items) -- 26.5% fewer requests,
        the same efficiency gain items/items-data/inventory already get from going flat.
        """
        return self._paginated("shipping/item_estimation", page)

    def get_shipping_options(self) -> typing.List[typing.Dict]:
        """GET /v1/shipping - the service levels available to this account."""
        return simplejson.loads(
            self._request(
                endpoint="shipping",
                method=common_enums.HttpMethod.GET,
            ).content
        ).get("data", [])

    def get_brands(self) -> typing.List[typing.Dict]:
        return simplejson.loads(
            self._request(
                endpoint="brands",
                method=common_enums.HttpMethod.GET,
            ).content
        ).get("data", [])

    def get_locations(self) -> typing.List[typing.Dict]:
        """Fetch Turn14 warehouse locations from GET /v1/locations."""
        return simplejson.loads(
            self._request(
                endpoint="locations",
                method=common_enums.HttpMethod.GET,
            ).content
        ).get("data", [])

    def _clear_token_cache(self) -> None:
        """Drop this credential's cached token so the next call mints a fresh one."""
        turn14_rate_limit.clear_token(self.client_id)
        logger.debug("{} Cleared cached authorization token.".format(self.LOG_PREFIX))

    def _request(
            self,
            endpoint: str,
            method: common_enums.HttpMethod,
            params: typing.Optional[dict] = None,
            payload: typing.Optional[dict] = None,
            include_auth: bool = True,
            retry_on_401: bool = True,
    ) -> requests.Response:
        # Buckets are keyed on client_id and shared with the Order API client, because Turn 14
        # meters per credential set, not per client class or per process. Raises
        # RateBudgetExhausted (NOT a Turn14APIException) when the hour/day budget is spent, so
        # the per-brand `except Turn14APIException: continue` handlers upstream do not swallow
        # it and march on through the rest of the catalog.
        rate_limit_base.acquire(turn14_rate_limit.get_buckets(self.client_id), meter_key="t14:get")

        url = f"{self.API_BASE_URL}/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        }

        if include_auth:
            access_token = self._get_valid_token()
            headers["Authorization"] = f"Bearer {access_token}"

        try:
            response = requests.request(
                url=url,
                method=method.value,
                params=params,
                json=payload,
                headers=headers,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            # Handle 401 Unauthorized - token might be invalid
            if response.status_code == 401 and include_auth and retry_on_401:
                logger.warning(
                    "{} Received 401 Unauthorized. Clearing token cache and retrying once (endpoint={}).".format(
                        self.LOG_PREFIX, endpoint
                    )
                )
                self._clear_token_cache()
                # Retry once with a fresh token
                return self._request(
                    endpoint=endpoint,
                    method=method,
                    params=params,
                    payload=payload,
                    include_auth=include_auth,
                    retry_on_401=False,  # Don't retry again to avoid infinite loop
                )

            # A 429 despite our own accounting means something else is spending these
            # credentials (Turn 14 customers may share a client_id with third-party
            # integrators) or their rolling window disagrees with our fixed one. Either way
            # our local count is an undercount -- shut the hour bucket so concurrent workers
            # stop immediately instead of each discovering the 429 for themselves, and raise
            # RateBudgetExhausted so the caller defers rather than skipping this brand and
            # marching into the next one.
            if response.status_code == 429:
                retry_after = _retry_after_seconds(response)
                rate_limit_base.mark_exhausted(turn14_rate_limit.hourly_bucket(self.client_id))
                logger.warning(
                    "{} Upstream 429 (endpoint={}). Deferring for {:.0f}s.".format(
                        self.LOG_PREFIX, endpoint, retry_after
                    )
                )
                raise rate_limit_base.RateBudgetExhausted(
                    scope="t14:get:hour (upstream 429)",
                    limit=turn14_rate_limit.GET_PER_HOUR,
                    period_seconds=3600,
                    retry_after_seconds=retry_after,
                )

            if response.status_code not in self.VALID_STATUS_CODES:
                msg = f"Invalid API client response (status_code={response.status_code}, data={response.content.decode('utf-8')})"
                logger.error(f"{self.LOG_PREFIX} {msg}.")
                raise exceptions.Turn14APIBadResponseCodeError(message=msg, code=response.status_code)

            logger.debug(
                f"{self.LOG_PREFIX} Successful response (endpoint={endpoint}, status_code={response.status_code}, payload={payload}, params={params}, raw_response={response.content.decode('utf-8')})."
            )
        except requests.exceptions.ConnectTimeout as e:
            msg = f"Connect timeout. Error: {common_utils.get_exception_message(exception=e)}"
            logger.exception(f"{self.LOG_PREFIX} {msg}.")
            raise exceptions.Turn14APIException(msg)
        except requests.RequestException as e:
            msg = f"Request exception. Error: {common_utils.get_exception_message(exception=e)}"
            logger.exception(f"{self.LOG_PREFIX} {msg}.")
            raise exceptions.Turn14APIException(msg)

        return response

    @staticmethod
    def _get_response_data(response: requests.Response) -> typing.Dict:
        return simplejson.loads(
            response.content,
            parse_float=decimal.Decimal,
        )