"""
RANDYS Worldwide public catalogue client.

Two public sources, no credentials anywhere in this module:

* **Convermax** (``randys-worldwide.myconvermax.com``) is the catalogue and fitment of record.
  Its item count matches Shopify's ``/products.json`` exactly (17,314 measured 2026-10-28), so it
  is the whole catalogue rather than a search subset, and unlike the Shopify endpoint it also
  carries category, differential codes, wheel specs and -- via ``ymm/fitments.json`` -- real
  Year/Make/Model fitment rows.
* **Shopify** fills the few gaps Convermax has: ``body_html``, the full image list, tags and
  per-variant grams.

Three Convermax behaviours below are load-bearing, because each one fails by returning a
perfectly plausible HTTP 200 rather than an error:

1. ``page`` is **0-based**. Starting at 1 drops the first page -- 5,000 of 17,314 products, with
   nothing in the response to say so. ``iter_catalog_pages`` therefore starts at 0, and
   ``fetch_catalog`` asserts the de-duplicated id count against the server's own ``TotalHits``.
2. The text parameter is ``query``, **not** ``q``. ``q`` is silently ignored and the full
   catalogue comes back, which downstream reads as "this search matched everything".
3. ``pagesize`` caps at 10,000 *and* the offset has to stay inside the result set --
   ``pagesize=10000&page=1`` is a 400, not a short final page.

``query`` additionally wraps matches in ``<b>`` tags inside ``sku`` and ``title``. The catalogue
sweep is paging-only and never sends ``query``, so nothing here has to strip highlighting; if you
add a search-backed path later, it does.
"""
import logging
import random
import time
import typing

import requests
from django.conf import settings

from src import constants as src_constants
from src.integrations.clients.randys import exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[RANDYS-CLIENT]"

DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 5
RETRY_BASE_DELAY_SECONDS = 2.0
MAX_RETRY_DELAY_SECONDS = 30.0

# Convermax 403s a bare urllib UA but is happy with a normal browser string. Shopify does not
# care either way, so both use the same one.
_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _base_url(setting_name: str, constant_value: str) -> str:
    configured = str(getattr(settings, setting_name, "") or "").strip()
    return (configured or constant_value).rstrip("/")


class RandysClient:
    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        self.convermax_base_url = _base_url(
            "RANDYS_CONVERMAX_BASE_URL", src_constants.RANDYS_CONVERMAX_BASE_URL
        )
        self.shopify_base_url = _base_url(
            "RANDYS_SHOPIFY_BASE_URL", src_constants.RANDYS_SHOPIFY_BASE_URL
        )
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _USER_AGENT, "Accept": "application/json"})

    def _get_json(self, url: str, params: typing.Optional[typing.Dict] = None) -> typing.Any:
        """
        GET with exponential backoff and jitter.

        The backoff is not decoration. Convermax rate-limits the fitment endpoint on *sustained*
        volume -- short bursts measure clean, which is exactly what makes it easy to get wrong --
        and an earlier version of this method retried with no delay at all, so a 429 burned all
        three attempts inside a few milliseconds and the part was recorded as failed. That run
        lost 622 of its first 1,500 parts. 429 (and 5xx) now wait, which turns a rate-limit into
        a pause instead of a hole in the data.

        ``Retry-After`` is honoured when present; Convermax does not currently send one, hence
        the computed fallback.
        """
        last_error: typing.Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                if response.status_code == 429 or response.status_code >= 500:
                    retry_after = response.headers.get("Retry-After")
                    if attempt < self.max_retries:
                        delay = self._retry_delay(attempt, retry_after)
                        logger.warning(
                            "{} GET {} returned {}; backing off {:.1f}s (attempt {}/{}).".format(
                                _LOG_PREFIX, url, response.status_code, delay, attempt,
                                self.max_retries,
                            )
                        )
                        time.sleep(delay)
                        continue
                response.raise_for_status()
                return response.json()
            except Exception as e:
                last_error = e
                if attempt < self.max_retries:
                    delay = self._retry_delay(attempt, None)
                    logger.warning(
                        "{} GET {} attempt {}/{} failed: {}. Retrying in {:.1f}s.".format(
                            _LOG_PREFIX, url, attempt, self.max_retries, str(e), delay
                        )
                    )
                    time.sleep(delay)
                else:
                    logger.warning(
                        "{} GET {} attempt {}/{} failed: {}.".format(
                            _LOG_PREFIX, url, attempt, self.max_retries, str(e)
                        )
                    )
        raise exceptions.RandysAPIError(
            "RANDYS request failed after {} attempts: {} ({})".format(
                self.max_retries, url, str(last_error)
            )
        )

    def _retry_delay(self, attempt: int, retry_after: typing.Optional[str]) -> float:
        if retry_after:
            try:
                return min(float(retry_after), MAX_RETRY_DELAY_SECONDS)
            except (TypeError, ValueError):
                pass
        # Jittered exponential backoff. The jitter matters more than the curve here: the fitment
        # sweep runs several workers, and without it they all wake together and re-trip the limit.
        base = min(RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)), MAX_RETRY_DELAY_SECONDS)
        return base * (0.5 + random.random())

    # ------------------------------------------------------------------ catalogue

    def _search_url(self) -> str:
        return "{}/{}".format(self.convermax_base_url, src_constants.RANDYS_CONVERMAX_SEARCH_PATH)

    def fetch_catalog_page(self, page: int, page_size: int) -> typing.Tuple[int, typing.List[typing.Dict]]:
        """
        One page of the index. ``page`` is 0-based -- see the module docstring; passing 1 for the
        first page is the single easiest way to lose 5,000 products here.

        Returns ``(total_hits, items)``.
        """
        if page < 0:
            raise ValueError("RANDYS catalogue page is 0-based; got {}.".format(page))
        if page_size > src_constants.RANDYS_CONVERMAX_MAX_PAGE_SIZE:
            raise ValueError(
                "RANDYS pagesize caps at {}; got {}.".format(
                    src_constants.RANDYS_CONVERMAX_MAX_PAGE_SIZE, page_size
                )
            )
        payload = self._get_json(self._search_url(), params={"pagesize": page_size, "page": page})
        if not isinstance(payload, dict) or "Items" not in payload:
            raise exceptions.RandysDataValidationError(
                "RANDYS search.json returned no Items for page={} pagesize={}.".format(page, page_size)
            )
        return int(payload.get("TotalHits") or 0), list(payload.get("Items") or [])

    def iter_catalog_pages(
        self, page_size: typing.Optional[int] = None
    ) -> typing.Iterator[typing.Tuple[int, typing.List[typing.Dict]]]:
        """
        Yields ``(total_hits, items)`` per page, starting at page 0. Stops once the pages seen
        cover ``TotalHits`` -- going one further is a 400, not an empty page.
        """
        size = int(page_size or src_constants.RANDYS_CONVERMAX_PAGE_SIZE)
        page = 0
        seen = 0
        total: typing.Optional[int] = None
        while True:
            total_hits, items = self.fetch_catalog_page(page, size)
            if total is None:
                total = total_hits
                logger.info("{} Catalogue reports {} products.".format(_LOG_PREFIX, total))
            if not items:
                break
            yield total_hits, items
            seen += len(items)
            if not total or seen >= total:
                break
            page += 1

    def fetch_catalog(self, page_size: typing.Optional[int] = None) -> typing.List[typing.Dict]:
        """
        The whole catalogue, de-duplicated by Shopify product id.

        Deliberately strict about completeness: the 0-based ``page`` trap produces a short
        catalogue that looks entirely healthy, so a sweep that does not reach ``TotalHits`` raises
        here rather than quietly upserting a partial catalogue over a good one.
        """
        size = int(page_size or src_constants.RANDYS_CONVERMAX_PAGE_SIZE)
        by_code: typing.Dict[str, typing.Dict] = {}
        total_hits = 0
        for page_total, page_items in self.iter_catalog_pages(page_size=size):
            total_hits = page_total or total_hits
            for item in page_items:
                code = str(item.get("id") or "").strip()
                if code:
                    by_code[code] = item

        if total_hits and len(by_code) < total_hits:
            raise exceptions.RandysDataValidationError(
                "RANDYS catalogue sweep returned {} unique products but the index reports {} -- "
                "refusing to ingest a partial catalogue (pagesize={}).".format(
                    len(by_code), total_hits, size
                )
            )
        logger.info(
            "{} Catalogue sweep collected {} unique products (index reports {}).".format(
                _LOG_PREFIX, len(by_code), total_hits
            )
        )
        return list(by_code.values())

    # ------------------------------------------------------------------ fitment

    def fetch_fitments(self, product_id: str) -> typing.Tuple[typing.List[typing.Dict], bool]:
        """
        Vehicle fitment rows for one product, as real tuples -- each row is a vehicle, so nothing
        here needs cross-producting flat make/model lists (the classic way to fabricate fitment).

        ``Year`` inside each row is a range-and-list string and must be expanded, never int()'d.

        Returns ``(rows, truncated)``. ``truncated`` is True when the endpoint returned exactly
        its 1000-row cap: it supports no offset parameter of any kind, so rows beyond that are
        simply unreachable and the caller records the loss instead of pretending it did not happen.
        """
        url = "{}/{}".format(
            self.convermax_base_url, src_constants.RANDYS_CONVERMAX_FITMENTS_PATH
        )
        payload = self._get_json(url, params={"productId": str(product_id)})
        if not isinstance(payload, dict):
            return [], False
        rows = [r for r in (payload.get("Fitments") or []) if isinstance(r, dict)]
        return rows, len(rows) >= src_constants.RANDYS_CONVERMAX_FITMENT_ROW_CAP

    # ------------------------------------------------------------------ shopify enrichment

    def iter_shopify_products(self) -> typing.Iterator[typing.List[typing.Dict]]:
        """
        Shopify ``/products.json``, paged. Used only for what Convermax lacks: body_html, the full
        image list, tags and grams. 1-based paging here, unlike Convermax above.
        """
        url = "{}/{}".format(self.shopify_base_url, src_constants.RANDYS_SHOPIFY_PRODUCTS_PATH)
        page = 1
        while True:
            payload = self._get_json(
                url, params={"limit": src_constants.RANDYS_SHOPIFY_PAGE_SIZE, "page": page}
            )
            products = (payload or {}).get("products") or []
            if not products:
                break
            yield products
            if len(products) < src_constants.RANDYS_SHOPIFY_PAGE_SIZE:
                break
            page += 1
