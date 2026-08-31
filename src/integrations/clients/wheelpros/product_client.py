"""
Transport client for Wheel Pros' Product API — the wheel/tire/accessory catalogue.
Spec: https://developer.wheelpros.com/assets/specs/product-api/openapi/api.html

Shares ``/auth/v1/authorize`` with the order and vehicle clients; separate from both because it
is a distinct, separately-entitled API area with its own quirks, several of which contradict the
published spec and were established by probing the live API:

* ``pageSize`` is documented as max 100. It actually accepts **1000** (2000 returns 400), which
  makes a full catalogue pull 10x cheaper.
* Results are capped at an **offset of 10,000** — ``page * pageSize > 10000`` returns 500 — and
  ``totalCount`` *saturates* at exactly ``10000`` instead of reporting the real size. Facet
  counts are not capped, so :meth:`true_count` reads the real number from a facet and
  :meth:`facet_buckets` drives the partitioning that keeps every query inside the window. See
  ``src.integrations.services.wheelpros_products``.
* ``facetCount`` is documented as max 1000. Wheel search intermittently 500s above ~30
  ("Cannot read properties of undefined"), so requests are retried and the value kept modest.
* Filter values must match the catalogue's own formatting exactly, and a wrong format returns
  **0 results rather than an error** — ``diameter`` wants ``"24.0"`` not ``24``, ``width`` wants
  ``"12.00"``, and ``brand`` wants the display name ("Asanti Black"), not the brand code ("AB").
* ``brand`` is a **prefix** match — ``brand=American Force`` also returns "American Force Cast"
  — so brand is not a safe partition axis.
* ``GET /v1/details/{sku}`` answers **500 "Product Not Found"** for a SKU that does not resolve,
  including SKUs that search itself just returned. Callers must treat that as a skip.

Prices and inventory are omitted unless asked for: ``fields=inventory,price`` plus ``company``
are required before ``prices`` and ``inventory`` are populated at all, and ``priceType`` must
name ``map``/``nip`` explicitly or only ``msrp`` comes back.
"""
import logging
import threading
import time
import typing

import requests
from django.conf import settings

from src.integrations.clients.wheelpros import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-PRODUCT-CLIENT]"

TOKEN_EXPIRATION_BUFFER_SECONDS = 120
REQUEST_TIMEOUT_SECONDS = 180
MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 2.0

MAX_PAGE_SIZE = 1000
RESULT_WINDOW = 10000
DEFAULT_FACET_COUNT = 30
# Escalation ladder for facet requests. Wheel search 500s intermittently on larger values,
# which _get retries; the last rung is well past the widest facet observed (73 brands).
FACET_COUNT_LADDER = (30, 80, 200, 500)

SEARCH_KINDS = ("wheel", "tire", "accessory")

# The facet that carries a brand bucket differs by endpoint: wheel search returns Elasticsearch
# field names, tire and accessory return camelCase aliases. Both are queried by name, so the
# caller has to know which; these are the observed ones.
BRAND_FACETS = {"wheel": "brand_desc", "tire": "brandDescription", "accessory": "brandDescription"}
# A second single-valued axis, used to cross-check the brand total. Accessories have none --
# they carry only brand facets -- so their count rests on the brand axis alone.
SIZE_FACETS = {"wheel": "wheel_diameter", "tire": "diameter", "accessory": None}


class WheelProsProductApiClient(object):
    """One instance per credentials set. Safe to share across threads."""

    def __init__(self, credentials: typing.Dict, company: str = "1000", currency: str = "USD") -> None:
        self.username = credentials.get("username", "")
        self.password = credentials.get("password", "")
        if not self.username or not self.password:
            raise ValueError("Invalid credentials parameter: username and password are required.")

        self.api_base_url = str(
            getattr(settings, "WHEELPROS_PRODUCT_BASE_URL", "") or "https://api.wheelpros.com"
        ).rstrip("/")
        # Sales org. Governs which price list `nip`/`msrp` come from — 1000 is CAD|USD, 1500 USD.
        self.company = company
        self.currency = currency

        self._cached_token: typing.Optional[str] = None
        self._token_expires_at: typing.Optional[float] = None
        self._token_lock = threading.Lock()
        self._thread_state = threading.local()

        self.requests_made = 0
        self._counter_lock = threading.Lock()

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
                raise exceptions.WheelProsProductAPIException("Authentication failed: {}".format(e))
            if response.status_code == 401:
                raise exceptions.WheelProsOrderAuthError("Invalid Wheel Pros username/password.")
            if response.status_code != 200:
                raise exceptions.WheelProsProductAPIException(
                    "Invalid authorize response ({}): {}".format(response.status_code, response.text[:300])
                )
            data = response.json()
            self._cached_token = data.get("accessToken")
            if not self._cached_token:
                raise exceptions.WheelProsProductAPIException("No accessToken in authorize response.")
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

    def _get(self, path: str, params: dict) -> typing.Any:
        """
        GET with retries. 5xx is retried rather than raised: wheel search 500s intermittently on
        larger ``facetCount`` values and usually succeeds on a second attempt.
        """
        url = "{}{}".format(self.api_base_url, path)
        last_error: typing.Optional[Exception] = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            token = self._get_valid_token()
            try:
                with self._counter_lock:
                    self.requests_made += 1
                response = self._session().get(
                    url,
                    headers={"Authorization": "Bearer {}".format(token), "Accept": "application/json"},
                    params=params,
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
                    raise exceptions.WheelProsProductPermissionError(
                        "403 from {}. Account {} is not entitled to the Product API.".format(url, self.username)
                    )
                if response.status_code == 401:
                    self._cached_token = None
                    last_error = exceptions.WheelProsProductAPIException("401 from {}".format(url))
                elif response.status_code == 429 or response.status_code >= 500:
                    last_error = exceptions.WheelProsProductAPIException(
                        "{} from {}: {}".format(response.status_code, url, response.text[:200])
                    )
                    logger.warning("%s HTTP %s on %s (%s/%s)", _LOG_PREFIX, response.status_code,
                                   url, attempt, MAX_ATTEMPTS)
                else:
                    raise exceptions.WheelProsProductAPIException(
                        "{} from {}: {}".format(response.status_code, url, response.text[:300])
                    )
            if attempt < MAX_ATTEMPTS:
                time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
        raise exceptions.WheelProsProductAPIException(
            "Giving up on {} after {} attempts: {}".format(url, MAX_ATTEMPTS, last_error)
        )

    # -- search ----------------------------------------------------------------------------------

    @staticmethod
    def _check_kind(kind: str) -> None:
        if kind not in SEARCH_KINDS:
            raise ValueError("Invalid kind: {}. Must be one of {}.".format(kind, SEARCH_KINDS))

    def _search_params(self, *, page: int, page_size: int, facet_count: int, **filters) -> dict:
        if page_size > MAX_PAGE_SIZE:
            raise ValueError("page_size must be <= {}".format(MAX_PAGE_SIZE))
        if page * page_size > RESULT_WINDOW:
            raise ValueError(
                "page {} x pageSize {} exceeds the API's {} result window".format(page, page_size, RESULT_WINDOW)
            )
        params = {
            "page": page,
            "pageSize": page_size,
            "facetCount": facet_count,
            # Without these, `prices` and `inventory` come back empty or absent entirely.
            "fields": "inventory,price",
            "priceType": "msrp,map,nip",
            "company": self.company,
            "currencyCode": self.currency,
        }
        params.update({k: v for k, v in filters.items() if v not in (None, "")})
        return params

    def search(self, kind: str, *, page: int = 1, page_size: int = 100, **filters) -> list:
        """One page of results. ``filters`` are passed through verbatim — values must already be
        in the catalogue's formatting (see the module docstring)."""
        self._check_kind(kind)
        data = self._get(
            "/products/v1/search/{}".format(kind),
            self._search_params(page=page, page_size=page_size, facet_count=1, **filters),
        )
        return (data or {}).get("results") or []

    def facets(self, kind: str, *, facet_count: int = DEFAULT_FACET_COUNT, **filters) -> dict:
        self._check_kind(kind)
        data = self._get(
            "/products/v1/search/{}".format(kind),
            self._search_params(page=1, page_size=1, facet_count=facet_count, **filters),
        )
        return (data or {}).get("facets") or {}

    def _facet_buckets(
        self, kind: str, facet_name: str, *, facet_count: int = DEFAULT_FACET_COUNT, **filters
    ) -> tuple:
        """
        ``(buckets, complete)`` for one facet, biggest count first.

        ``facetCount`` caps how many buckets come back; it is not a page size, and there is no
        way to ask for "the next 30". Requesting too few silently returns the top N, which would
        both under-count the catalogue and leave whole slices out of a partition plan --
        accessories have 73 brands, so a facetCount of 30 drops 43 brands from the crawl with no
        error anywhere. So the request escalates until the response proves itself complete:
        **fewer buckets returned than were asked for** is the only reliable exhaustion signal.

        Wheel search returns a hard 500 ("Cannot read properties of undefined") above roughly
        facetCount 30 -- consistently, not intermittently. That is treated as "this endpoint
        will not go wider" and the last good result is kept, flagged incomplete, rather than
        failing the run: an incomplete axis is usable as long as the caller knows not to trust
        its total or partition on it alone.
        """
        buckets: list = []
        complete = False
        for attempt_count in FACET_COUNT_LADDER:
            if attempt_count < facet_count:
                continue
            try:
                facet = self.facets(kind, facet_count=attempt_count, **filters).get(facet_name) or {}
            except exceptions.WheelProsProductAPIException as exc:
                if not buckets:
                    raise
                logger.info(
                    "%s %s will not serve facetCount=%s (%s); keeping %s buckets from the "
                    "previous rung", _LOG_PREFIX, kind, attempt_count, str(exc)[:80], len(buckets),
                )
                break
            raw = facet.get("buckets") or []
            buckets = [(b.get("value"), int(b.get("count") or 0)) for b in raw]
            if len(raw) < attempt_count:
                complete = True
                break
        return sorted(buckets, key=lambda b: -b[1]), complete

    def facet_buckets(
        self, kind: str, facet_name: str, *, facet_count: int = DEFAULT_FACET_COUNT, **filters
    ) -> list:
        """Every ``(value, count)`` of one facet, biggest first. See :meth:`_facet_buckets`."""
        buckets, complete = self._facet_buckets(kind, facet_name, facet_count=facet_count, **filters)
        if not complete and buckets:
            logger.warning(
                "%s facet %s on %s%s may be truncated at %s buckets; the partition could be "
                "incomplete", _LOG_PREFIX, facet_name, kind,
                " {}".format(filters) if filters else "", len(buckets),
            )
        return buckets

    def true_count(self, kind: str, *, facet_count: int = DEFAULT_FACET_COUNT, **filters) -> int:
        """
        The real number of matching SKUs. ``totalCount`` cannot be used: it saturates at exactly
        10,000 rather than reporting the true size.

        Counted off a facet whose values are **single-valued per SKU**, so the buckets partition
        the catalogue and their counts sum to it. ``wheel_diameter``/``diameter`` is that axis and
        is tried first. Brand is only a fallback, and a poor one: the brand facet counts parent
        and sub-brand separately ("American Force" *and* "American Force Cast"), so it over-counts
        -- 97,361 against the diameter axis's 85,863 for wheels. Accessories expose no size facet
        at all, so brand is all they have.

        An axis is only trusted when its bucket list came back complete; an incomplete axis can
        only under-report, so it is used as a floor when nothing better is available.
        """
        self._check_kind(kind)
        floor = 0
        for axis in (SIZE_FACETS.get(kind), BRAND_FACETS.get(kind)):
            if not axis:
                continue
            buckets, complete = self._facet_buckets(kind, axis, facet_count=facet_count, **filters)
            if not buckets:
                continue
            total = sum(count for _, count in buckets)
            if complete:
                return total
            floor = max(floor, total)
        if floor:
            logger.warning(
                "%s %s%s: no facet axis came back complete; using %s as a floor",
                _LOG_PREFIX, kind, " {}".format(filters) if filters else "", floor,
            )
            return floor
        data = self._get(
            "/products/v1/search/{}".format(kind),
            self._search_params(page=1, page_size=1, facet_count=1, **filters),
        )
        return int((data or {}).get("totalCount") or 0)

    def get_brands(self) -> list:
        return (self._get("/products/v1/brands", {}) or {}).get("results") or []

    def get_details(self, sku: str) -> typing.Optional[dict]:
        """``GET /v1/details/{sku}``. Returns ``None`` when Wheel Pros reports the SKU as not
        found — which it does with a **500**, not a 404, so a plain status check would abort a
        run on perfectly ordinary missing data."""
        try:
            return self._get("/products/v1/details/{}".format(requests.utils.quote(sku, safe="")), {})
        except exceptions.WheelProsProductAPIException as exc:
            if "Product Not Found" in str(exc):
                return None
            raise

    def test_connection(self) -> None:
        self.get_brands()
