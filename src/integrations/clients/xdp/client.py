"""
Client for XDP (Xtreme Diesel Performance, xtremediesel.com) — a Miva storefront fronted by a
SearchSpring search index, plus a per-dealer CSV feed covering XDP's own house brand.

Cost comes from two different places and it matters which:

- XDP's **own brand** ("XDP", 556 indexed rows) has a real per-dealer CSV feed carrying actual
  ``COST``, ``MAP_PRICE`` and a numeric ``IN-STOCK`` — see ``XdpDealerFeedClient``. Each dealer
  gets their own feed URL, stored per company in ``CompanyProviders.credentials["feed_url"]``
  (same shape as Rough Country's jobber feed).
- Every **other** brand XDP resells (241 of them, ~60k products) has no wholesale number exposed
  anywhere reachable, so cost there is derived from the company's own markup off XDP's public
  retail price — see services/xdp.py:dealer_cost_from_retail.

FOUR hosts/paths, and the Cloudflare posture differs *per path* on the same host — do not assume
one host means one policy:

- ``www.xtremediesel.com/feeds/*.csv`` (``XdpDealerFeedClient``) is NOT Cloudflare-gated; a bare
  curl with no User-Agent gets the CSV.

- ``k72wrs.a.searchspring.io`` (``XdpSearchSpringClient``) is not behind Cloudflare at all.
  It is the ONLY source of brand (``manufacturer_name``), clean MPN, and vehicle fitment.
- ``www.xtremediesel.com/mm5/json.mvc`` (``XdpJsonMvcClient``) is NOT Cloudflare-gated — a bare
  curl with no User-Agent works. This is the full-catalog spine and the variant resolver.
- ``www.xtremediesel.com`` HTML pages and ``/ajax`` ARE Cloudflare-gated (JS challenge, 403 for
  every UA tried including Googlebot). Nothing in this module touches them; anything that needs
  them needs a real browser and does not belong in the nightly ingest.

Two traps this module encodes, both measured live (2026-10-28):

- ``Session_Type=runtime`` must be in the json.mvc *query string*. Omit it and the endpoint
  answers HTTP 200 with a zero-byte body, which reads like a dead endpoint rather than a bad
  request. Every wrong parameter guess fails that same silent way.
- SearchSpring's ``resultsPerPage`` caps at 100 and ``page`` caps at 100 — a hard 10,000-result
  window ("Bad request" past page 100). The catalog is 60k+, so it MUST be swept per brand;
  no brand exceeds the cap (largest is Dorman at ~4,500).
"""
import csv
import io
import logging
import typing

import requests

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[XDP-CLIENT]"

# ---------------------------------------------------------------------------
# Per-dealer house-brand CSV feed (not Cloudflare-gated)
# ---------------------------------------------------------------------------
# Each dealer is given their own file under /feeds/. The URL is the company's credential, not a
# constant -- this one is only the shape reference and the default used when a connection has
# none. Pinning the prefix keeps a credential from pointing the ingest at an arbitrary host
# (same guard Rough Country's feed client uses).
DEALER_FEED_URL_PREFIX = "https://www.xtremediesel.com/feeds/"
DEFAULT_DEALER_FEED_URL = DEALER_FEED_URL_PREFIX + "xdp.csv"
DEALER_FEED_TIMEOUT_SECONDS = 60

# Header of the dealer feed, verbatim. There is a trailing empty column ("...,IN-STOCK,"), so
# csv.DictReader yields an extra "" key -- harmless, but do not treat a missing "" as malformed.
DEALER_FEED_PART_NUMBER = "PART NUMBER"
DEALER_FEED_REQUIRED_COLUMNS = (
    DEALER_FEED_PART_NUMBER,
    "PRODUCT_NAME",
    "MAP_PRICE",
    "COST",
    "IN-STOCK",
)

# ---------------------------------------------------------------------------
# Miva json.mvc (not Cloudflare-gated)
# ---------------------------------------------------------------------------
JSON_MVC_URL = "https://www.xtremediesel.com/mm5/json.mvc"
STORE_CODE = "XDP"
# Required in the QUERY STRING on every call. See the module docstring: without it the server
# returns 200 with an empty body instead of an error.
SESSION_TYPE = "runtime"
JSON_MVC_TIMEOUT_SECONDS = 90

# Runtime_ProductList_Load_Query honours Count=5000 and arbitrary Offset (no 10k-style window),
# so the whole catalog is ~13 requests. Larger values were not probed; 5000 is already plenty.
CATALOG_PAGE_SIZE = 5000

IMAGE_SIZES = "448x448"

# ---------------------------------------------------------------------------
# SearchSpring (not Cloudflare-gated)
# ---------------------------------------------------------------------------
SEARCHSPRING_URL = "https://k72wrs.a.searchspring.io/api/search/search.json"
SEARCHSPRING_SITE_ID = "k72wrs"
# Hard server-side caps -- see module docstring.
SEARCHSPRING_MAX_PER_PAGE = 100
SEARCHSPRING_MAX_PAGE = 100
SEARCHSPRING_TIMEOUT_SECONDS = 45
SEARCHSPRING_RETRIES = 3

# SearchSpring rejects the library default UA on some edges; a plain desktop Chrome string is
# what the storefront itself sends and is known-good.
SEARCHSPRING_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"
)

BRAND_FACET_FIELD = "manufacturer_name"
FITMENT_FACET_FIELD = "parts_finder_data"
FITMENT_HIERARCHY_DELIMITER = ">"
# Every `All` node in the fitment hierarchy returns a count EQUAL TO ITS PARENT -- `2017>All` ==
# `2017`, `2017>Ford>All` == `2017>Ford`. They are UI wildcards, not data rows. Walking them as if
# they were real re-counts the entire catalog at every level of the tree.
FITMENT_WILDCARD_NODE = "All"


class XdpApiError(Exception):
    pass


def _first(value: typing.Any) -> typing.Optional[typing.Any]:
    """SearchSpring returns some fields (notably ``mpn``) as a list. Take the first entry."""
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _opt_decimal_str(raw: typing.Any) -> typing.Optional[str]:
    """Blank-tolerant passthrough for a numeric CSV cell. Returns the trimmed string (so the
    service layer owns Decimal conversion) or None when the cell is empty."""
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _opt_int(raw: typing.Any) -> typing.Optional[int]:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


class XdpDealerFeedClient:
    """
    One dealer's house-brand CSV feed. This is the only place XDP hands over a real wholesale
    ``COST``, and the only place with a real numeric quantity for non-variant products.

    Covers XDP's own brand only -- 449 parts against the 556 rows SearchSpring indexes under
    brand "XDP" (the other ~95 are catalogued but not in the dealer feed). Every feed part number
    matched SearchSpring's ``sku`` AND ``mpn`` exactly when measured (2026-10-28), so the join
    needs no normalisation.

    ``IN-STOCK`` goes NEGATIVE (48 of 449 rows, as low as -133) for oversold/backordered lines --
    the same convention Miva's own ``inv_available`` uses. ``quantity`` clamps that to 0 so a
    negative can never subtract from a stock total, while ``quantity_raw`` keeps what the file
    actually said.
    """

    def __init__(
        self,
        feed_url: typing.Optional[str] = None,
        session: typing.Optional[requests.Session] = None,
    ):
        self.feed_url = (feed_url or "").strip() or DEFAULT_DEALER_FEED_URL
        if not self.feed_url.startswith(DEALER_FEED_URL_PREFIX):
            raise ValueError(
                "Invalid XDP dealer feed URL — must start with '{}'. Got: '{}'.".format(
                    DEALER_FEED_URL_PREFIX, self.feed_url
                )
            )
        self._session = session or requests.Session()

    def _download(self) -> str:
        try:
            resp = self._session.get(self.feed_url, timeout=DEALER_FEED_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            raise XdpApiError(
                "{} dealer feed download failed ({}): {}".format(_LOG_PREFIX, self.feed_url, e)
            )
        if resp.status_code == 404:
            raise XdpApiError(
                "{} dealer feed not found at {} — XDP publishes one file per dealer, so check the "
                "URL the rep supplied.".format(_LOG_PREFIX, self.feed_url)
            )
        if resp.status_code != 200:
            raise XdpApiError(
                "{} dealer feed returned status {} for {}".format(
                    _LOG_PREFIX, resp.status_code, self.feed_url
                )
            )
        text = resp.text
        if "Just a moment" in text[:2000]:
            # /feeds/ is not gated today, but the same host's HTML pages are -- if that ever
            # changes this is the symptom, and it is not a parse problem.
            raise XdpApiError(
                "{} dealer feed came back as a Cloudflare challenge, not CSV ({}).".format(
                    _LOG_PREFIX, self.feed_url
                )
            )
        return text

    def fetch_rows(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """Downloads and normalises the whole feed. Rows without a part number are dropped."""
        text = self._download()
        # Strip a UTF-8 BOM if one is present: left in place it corrupts the first header name
        # and makes "PART NUMBER" unfindable, which surfaces as a bogus "missing column" error.
        reader = csv.DictReader(io.StringIO(text.lstrip("﻿")))
        fieldnames = [(f or "").strip() for f in (reader.fieldnames or [])]
        missing = [c for c in DEALER_FEED_REQUIRED_COLUMNS if c not in fieldnames]
        if missing:
            raise XdpApiError(
                "{} dealer feed {} is missing expected column(s) {} — got {}".format(
                    _LOG_PREFIX, self.feed_url, missing, fieldnames
                )
            )

        out = []
        skipped = 0
        for raw in reader:
            part_number = (raw.get(DEALER_FEED_PART_NUMBER) or "").strip()
            if not part_number:
                skipped += 1
                continue
            quantity_raw = _opt_int(raw.get("IN-STOCK"))
            out.append({
                "part_number": part_number,
                "name": (raw.get("PRODUCT_NAME") or "").strip() or None,
                "application": (raw.get("APPLICATION") or "").strip() or None,
                "map_price": _opt_decimal_str(raw.get("MAP_PRICE")),
                "cost": _opt_decimal_str(raw.get("COST")),
                "url": (raw.get("PRODUCT_URL") or "").strip() or None,
                "weight": _opt_decimal_str(raw.get("WEIGHT")),
                "width": _opt_decimal_str(raw.get("WIDTH")),
                "length": _opt_decimal_str(raw.get("LENGTH")),
                "height": _opt_decimal_str(raw.get("HEIGHT")),
                "quantity_raw": quantity_raw,
                "quantity": max(quantity_raw, 0) if quantity_raw is not None else None,
            })
        if skipped:
            logger.warning(
                "{} dealer feed {}: skipped {} row(s) with no part number.".format(
                    _LOG_PREFIX, self.feed_url, skipped
                )
            )
        logger.info(
            "{} dealer feed {}: parsed {} row(s).".format(_LOG_PREFIX, self.feed_url, len(out))
        )
        return out

    def test_connection(self) -> int:
        """Used by the connect-flow validator. Returns the row count; raises on anything that
        would stop a real ingest."""
        rows = self.fetch_rows()
        if not rows:
            raise XdpApiError(
                "{} dealer feed {} parsed as CSV but contained no priced parts.".format(
                    _LOG_PREFIX, self.feed_url
                )
            )
        return len(rows)


class XdpJsonMvcClient:
    """
    Miva storefront JSON API. No auth of any kind; works from a bare request with no User-Agent.

    Only four Runtime_* functions actually respond — every other name probed returned an empty
    body. ``ondemandcolumns``/``Filter``/``On_Demand_Columns`` are all ignored, so the field set
    is fixed and XDP's own ``cost`` is NOT obtainable here (by design, as far as the ingest is
    concerned).
    """

    def __init__(self, session: typing.Optional[requests.Session] = None):
        self._session = session or requests.Session()

    def _call(
        self,
        function: str,
        params: typing.Optional[typing.Dict[str, typing.Any]] = None,
        data: typing.Optional[typing.Dict[str, typing.Any]] = None,
        method: str = "GET",
    ) -> typing.Optional[typing.Dict]:
        query = {
            "Store_Code": STORE_CODE,
            "Session_Type": SESSION_TYPE,
            "Function": function,
        }
        query.update(params or {})
        try:
            if method == "POST":
                resp = self._session.post(
                    JSON_MVC_URL, params=query, data=data or {},
                    timeout=JSON_MVC_TIMEOUT_SECONDS,
                )
            else:
                resp = self._session.get(
                    JSON_MVC_URL, params=query, timeout=JSON_MVC_TIMEOUT_SECONDS
                )
        except requests.RequestException as e:
            raise XdpApiError("{} json.mvc {} request failed: {}".format(_LOG_PREFIX, function, e))
        if resp.status_code != 200:
            raise XdpApiError(
                "{} json.mvc {} returned status {}".format(_LOG_PREFIX, function, resp.status_code)
            )
        if not resp.content:
            # The documented silent failure: 200 + zero bytes means the parameters were wrong
            # (most often a missing Session_Type), not that the catalog is empty.
            logger.warning(
                "{} json.mvc {} returned an empty body -- check Session_Type and required "
                "params.".format(_LOG_PREFIX, function)
            )
            return None
        try:
            payload = resp.json()
        except ValueError:
            raise XdpApiError("{} json.mvc {} response was not JSON".format(_LOG_PREFIX, function))
        if not payload.get("success"):
            logger.warning(
                "{} json.mvc {} returned success=0: {}".format(
                    _LOG_PREFIX, function, str(payload)[:300]
                )
            )
            return None
        return payload

    def catalog_page(self, offset: int, count: int = CATALOG_PAGE_SIZE) -> typing.Dict:
        """One page of Runtime_ProductList_Load_Query. Returns the inner ``data`` object
        (``total_count``, ``start_offset``, ``data``)."""
        payload = self._call(
            "Runtime_ProductList_Load_Query", params={"Count": count, "Offset": offset}
        )
        if not payload:
            raise XdpApiError(
                "{} catalog page offset={} returned no usable payload".format(_LOG_PREFIX, offset)
            )
        return payload.get("data") or {}

    def iter_catalog(self, count: int = CATALOG_PAGE_SIZE) -> typing.Iterator[typing.Dict]:
        """Yields every product row in the Miva catalog. ~13 requests at the default page size."""
        offset = 0
        total = None
        while True:
            data = self.catalog_page(offset, count=count)
            rows = data.get("data") or []
            if total is None:
                total = data.get("total_count")
                logger.info("{} catalog total_count={}".format(_LOG_PREFIX, total))
            if not rows:
                return
            for row in rows:
                yield row
            offset += len(rows)
            if total is not None and offset >= total:
                return

    def attributes_for_product(
        self, product_code: str, product_id: typing.Any
    ) -> typing.List[typing.Dict]:
        """
        Attribute + option structure for one product, with the numeric ids the variant resolver
        needs. BOTH ``Product_Code`` and ``Product_ID`` are required — passing Product_ID alone
        comes back with no ``data`` key at all.
        """
        payload = self._call(
            "Runtime_AttributeAndOptionList_Load_Product",
            params={},
            data={"Product_Code": product_code, "Product_ID": product_id},
            method="POST",
        )
        if not payload:
            return []
        return payload.get("data") or []

    def resolve_variant(
        self,
        product_code: str,
        selected_attribute_ids: typing.Sequence[typing.Any],
        selected_attmpat_ids: typing.Sequence[typing.Any],
        selected_option_ids: typing.Sequence[typing.Any],
        selected_attribute_types: typing.Sequence[str],
    ) -> typing.Optional[typing.Dict]:
        """
        Resolve one option combination to a concrete variant. This is the ONLY place XDP exposes a
        real numeric quantity (``variant.inv_available`` / ``inv_instock``); a negative value
        means dropship/backorder, not a count.

        The ids are comma-separated numeric lists from ``attributes_for_product`` — attribute and
        option *codes* do not work here.
        """
        join = lambda seq: ",".join(str(x) for x in seq)  # noqa: E731
        last_attr = selected_attribute_ids[-1] if selected_attribute_ids else 0
        last_attmpat = selected_attmpat_ids[-1] if selected_attmpat_ids else 0
        last_option = selected_option_ids[-1] if selected_option_ids else 0
        payload = self._call(
            "Runtime_AttributeList_Load_ProductVariant_Possible",
            params={},
            data={
                "Product_Code": product_code,
                "Dependency_Resolution": 1,
                "Last_Selected_Attribute_ID": last_attr,
                "Last_Selected_AttributeTemplateAttribute_ID": last_attmpat,
                "Last_Selected_Option_ID": last_option,
                "Selected_Attribute_IDs": join(selected_attribute_ids),
                "Selected_AttributeTemplateAttribute_IDs": join(selected_attmpat_ids),
                "Selected_Option_IDs": join(selected_option_ids),
                "Selected_Attribute_Types": join(selected_attribute_types),
                "Unselected_Attribute_IDs": "",
                "Unselected_AttributeTemplateAttribute_IDs": "",
            },
            method="POST",
        )
        if not payload:
            return None
        return payload.get("data") or None

    def image_list(
        self, product_code: str, variant_id: typing.Any = 0, sizes: str = IMAGE_SIZES
    ) -> typing.List[typing.Dict]:
        """Full ordered image set for a product/variant at an arbitrary size."""
        payload = self._call(
            "Runtime_ProductImageList_Load_Product_Variant",
            params={
                "Product_Code": product_code,
                "Variant_ID": variant_id,
                "Image_Sizes": sizes,
            },
        )
        if not payload:
            return []
        return payload.get("data") or []


class XdpSearchSpringClient:
    """
    SearchSpring index behind XDP's storefront search. Unauthenticated and off Cloudflare.

    This is the only source for three things the Miva API does not carry at all: the brand name,
    a clean manufacturer part number (``mpn``), and vehicle fitment.
    """

    def __init__(self, session: typing.Optional[requests.Session] = None):
        self._session = session or requests.Session()
        self._session.headers["User-Agent"] = SEARCHSPRING_USER_AGENT

    def _search(self, params: typing.Dict[str, typing.Any]) -> typing.Dict:
        query = {
            "siteId": SEARCHSPRING_SITE_ID,
            "resultsFormat": "native",
        }
        query.update(params)
        last_error: typing.Optional[Exception] = None
        for attempt in range(SEARCHSPRING_RETRIES):
            try:
                resp = self._session.get(
                    SEARCHSPRING_URL, params=query, timeout=SEARCHSPRING_TIMEOUT_SECONDS
                )
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                last_error = e
                logger.warning(
                    "{} SearchSpring attempt {}/{} failed (params={}): {}".format(
                        _LOG_PREFIX, attempt + 1, SEARCHSPRING_RETRIES, query, e
                    )
                )
        raise XdpApiError(
            "{} SearchSpring request failed after {} attempts: {}".format(
                _LOG_PREFIX, SEARCHSPRING_RETRIES, last_error
            )
        )

    @staticmethod
    def _facet_values(payload: typing.Dict, field: str) -> typing.List[typing.Dict]:
        for facet in payload.get("facets") or []:
            if facet.get("field") == field:
                return facet.get("values") or []
        return []

    def brands(self) -> typing.List[typing.Tuple[str, int]]:
        """``[(brand_name, product_count)]`` from the catalog-level brand facet. 242 brands
        covering ~99.97% of the index as of 2026-10-28."""
        payload = self._search({"resultsPerPage": 1})
        out = []
        for value in self._facet_values(payload, BRAND_FACET_FIELD):
            name = (value.get("label") or value.get("value") or "").strip()
            if name:
                out.append((name, value.get("count") or 0))
        return out

    def iter_brand_products(self, brand: str) -> typing.Iterator[typing.Dict]:
        """
        Every indexed product for one brand. Brand is the partition key precisely because the
        index has a hard 10,000-result window and no single brand comes close to it; a sweep that
        pages the unfiltered index instead silently stops at 10,000 of 60,000+.
        """
        page = 1
        while page <= SEARCHSPRING_MAX_PAGE:
            payload = self._search({
                "resultsPerPage": SEARCHSPRING_MAX_PER_PAGE,
                "page": page,
                "bgfilter.{}".format(BRAND_FACET_FIELD): brand,
            })
            results = payload.get("results") or []
            if not results:
                return
            for row in results:
                yield row
            pagination = payload.get("pagination") or {}
            total_pages = pagination.get("totalPages")
            if total_pages and page >= min(total_pages, SEARCHSPRING_MAX_PAGE):
                if total_pages > SEARCHSPRING_MAX_PAGE:
                    logger.warning(
                        "{} brand {!r} reports {} pages, past the {}-page window -- rows beyond "
                        "it are unreachable and need a narrower partition.".format(
                            _LOG_PREFIX, brand, total_pages, SEARCHSPRING_MAX_PAGE
                        )
                    )
                return
            page += 1

    # -- fitment -----------------------------------------------------------
    def fitment_children(self, path: typing.Optional[str] = None) -> typing.List[str]:
        """
        Child nodes one level below ``path`` in the ``parts_finder_data`` hierarchy (years when
        path is None, then makes, models, engines).

        ``All`` wildcard nodes are filtered out here, once, so no caller has to remember to:
        they return their parent's own count and would duplicate the entire catalog at every
        level of a walk.
        """
        params: typing.Dict[str, typing.Any] = {"resultsPerPage": 1}
        if path:
            params["bgfilter.{}".format(FITMENT_FACET_FIELD)] = path
        payload = self._search(params)
        out = []
        for value in self._facet_values(payload, FITMENT_FACET_FIELD):
            raw = value.get("value") or ""
            if not raw:
                continue
            if raw.split(FITMENT_HIERARCHY_DELIMITER)[-1] == FITMENT_WILDCARD_NODE:
                continue
            out.append(raw)
        return out

    def iter_fitment_node_products(self, path: str) -> typing.Iterator[typing.Dict]:
        """Every product under one fitment node (used at the engine-leaf level)."""
        page = 1
        while page <= SEARCHSPRING_MAX_PAGE:
            payload = self._search({
                "resultsPerPage": SEARCHSPRING_MAX_PER_PAGE,
                "page": page,
                "bgfilter.{}".format(FITMENT_FACET_FIELD): path,
            })
            results = payload.get("results") or []
            if not results:
                return
            for row in results:
                yield row
            pagination = payload.get("pagination") or {}
            total_pages = pagination.get("totalPages")
            if total_pages and page >= min(total_pages, SEARCHSPRING_MAX_PAGE):
                if total_pages > SEARCHSPRING_MAX_PAGE:
                    logger.warning(
                        "{} fitment node {!r} reports {} pages, past the {}-page window.".format(
                            _LOG_PREFIX, path, total_pages, SEARCHSPRING_MAX_PAGE
                        )
                    )
                return
            page += 1

    def iter_engine_leaves(self) -> typing.Iterator[str]:
        """
        Walks year -> make -> model -> engine and yields every real (non-wildcard) engine-leaf
        path, e.g. ``2017>Ford>F-250 Super Duty>6.7 V8 DIESEL``.

        ~74 + ~549 + ~2,657 discovery requests before any product paging. Deliberately a
        generator so the caller can page each leaf and release it.
        """
        for year in self.fitment_children(None):
            for make in self.fitment_children(year):
                for model in self.fitment_children(make):
                    engines = self.fitment_children(model)
                    if not engines:
                        # Model with no engine breakdown of its own -- still a usable vehicle.
                        yield model
                        continue
                    for engine in engines:
                        yield engine
