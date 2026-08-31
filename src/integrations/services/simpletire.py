"""
Crawl simpletire.com's public tire catalog into :class:`src.models.SimpleTireSku`.

What this talks to
------------------
SimpleTire is a Next.js app. Its product pages render server-side, so the SKU data is embedded in
the page's React flight stream -- but the *same* payload is served as plain JSON by the endpoint
the page itself calls when you switch size on a PDP::

    GET /api/product-detail?brand={brand}&productLine={line}[&itemId=&mpn=&tireSize=]

So we call that instead of parsing markup: same data, ~17 KB of JSON against ~430 KB of HTML, and
no dependency on their component tree. The HTML path is not implemented at all on purpose -- there
is nothing in the page that is not in the JSON.

**itemId is what selects a size.** ``mpn`` and ``tireSize`` alone are silently ignored: ask for
``mpn=MB5023&tireSize=235-65rr18`` without ``itemId`` and you get the line's *default* size back
with a 200 and no warning. That is the single easiest way to fill this table with 60,000 copies of
the wrong spec sheet, which is why :func:`_fetch_product_detail` requires them as a set and
:func:`_crawl_product_line` verifies the id it got back is the id it asked for.

Why one request per size
------------------------
``siteProductSpecs`` describes the *selected* SKU, not the model: load index, weight and overall
diameter all differ between 225/65R16 and 265/70R18 of the same tire. A per-model crawl would be
one request instead of sixty and would be wrong sixty times over.

Enumeration
-----------
``/sitemap/product-line.xml`` is the authoritative list of model pages (~11k, all of the exact
shape ``/brands/{brand-slug}/{line-slug}``) and is far more complete than the brand pages, which
show a curated subset. Stale entries exist; a 404 on one is expected and skipped, not fatal.

Only *purchasable* sizes appear in ``siteProductLineAvailableSizeList``. A discontinued line comes
back with an empty list and a single out-of-stock ``siteProductLineSizeDetail``, which we still
record -- one row, ``product_status='ProductStatusOutOfStock'`` -- because the spec sheet is real
data even when the SKU cannot be bought.

Politeness
----------
No credentials, no auth, no per-account quota, and no observed throttling -- so the shared
Postgres buckets in :mod:`src.integrations.rate_limit` (built for Turn 14's per-client_id limits)
are the wrong tool. A plain in-process token bucket is enough because this is one operator running
one command. Note that simpletire.com's robots.txt disallows ``/api/*``: this crawler is a
deliberate, rate-limited, one-off competitive-data pull, not a search-engine crawl. Keep
``--rate`` conservative.

Resuming
--------
A full pass is ~70k requests. The run appends every finished product line to a JSONL checkpoint
and skips those on restart, so a crash costs one line rather than the crawl. The checkpoint is the
resume mechanism rather than a query over the table because a line that legitimately yields zero
rows would otherwise be retried on every run, forever.
"""
import dataclasses
import decimal
import json
import logging
import os
import pathlib
import re
import sys
import threading
import time
import typing
import urllib.parse
import xml.etree.ElementTree as ElementTree
from concurrent import futures

from django.db import transaction
from django.db import utils as db_utils
from django.utils import timezone

from src.models import SimpleTireSku

logger = logging.getLogger(__name__)

BASE_URL = "https://simpletire.com"
PRODUCT_LINE_SITEMAP_URL = f"{BASE_URL}/sitemap/product-line.xml"
BRAND_SITEMAP_URL = f"{BASE_URL}/sitemap/brand.xml"
PRODUCT_DETAIL_PATH = "/api/product-detail"

# Chrome's TLS/JA3 fingerprint via curl_cffi. A stock `requests` handshake is fingerprintable and
# is what gets a scrape blocked long before request volume does.
IMPERSONATE_PROFILE = "chrome"
REQUEST_TIMEOUT_SECONDS = 45
MAX_ATTEMPTS = 4
BACKOFF_BASE_SECONDS = 1.5

# Largest rim in the catalogue is an OTR wheel at ~63"; used to undo SimpleTire's variable-scale
# integer encoding of rim diameters. See :func:`parse_rim_diameter`.
_MAX_RIM_DIAMETER_IN = decimal.Decimal(63)
_MAX_RIM_RESCALES = 4

# Stall detection. A single product line is at most a few hundred requests, so several minutes
# without one completing means something is wedged, not merely slow.
STALL_CHECK_SECONDS = 15
STALL_WARN_SECONDS = 120
STALL_ABORT_SECONDS = 420

DEFAULT_CONCURRENCY = 6
DEFAULT_RATE_PER_SECOND = 5.0
DEFAULT_BATCH_SIZE = 500

# Brand slugs in the URL carry a "-tires" suffix the API does not want: /brands/antares-tires ->
# brand=antares. Bare slugs (e.g. "universal-tires" -> "universal") go through the same rule.
_BRAND_SLUG_SUFFIX = "-tires"

_SITEMAP_LOC = ".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc"

# Every field the upsert refreshes -- i.e. everything except the identity column and created_at.
UPDATE_FIELDS: typing.Final[list[str]] = [
    "part_number",
    "product_line_id",
    "brand_slug",
    "product_line_slug",
    "page_url",
    "brand_name",
    "brand_tier",
    "brand_logo_url",
    "product_line_name",
    "product_line_overview",
    "product_line_image_url",
    "starting_price_cents",
    "size_display",
    "tire_size_slug",
    "load_speed_rating",
    "load_range",
    "rim_diameter_in",
    "product_type_id",
    "product_sub_type",
    "product_status",
    "quantity",
    "delivery_days",
    "estimated_retail_price_cents",
    "sale_price_cents",
    "web_price_cents",
    "price_label",
    "road_hazard_price_cents",
    "road_hazard_duration_label",
    "oversize_fee_cents",
    "fet_fee_cents",
    "is_run_flat",
    "is_electric_optimized",
    "is_oversized",
    "is_installable",
    "simple_score",
    "handling_durability_score",
    "longevity_score",
    "traction_score",
    "spec_category",
    "spec_vehicle",
    "spec_sidewall",
    "spec_tread_design",
    "spec_load_range",
    "spec_ply_rating",
    "spec_load_index",
    "spec_load_index_dual",
    "spec_max_load_lb",
    "spec_max_load_dual_lb",
    "spec_speed_rating",
    "spec_max_speed_mph",
    "spec_tread_depth_32nds",
    "spec_overall_diameter_in",
    "spec_section_width_in",
    "spec_max_psi",
    "spec_rim_width_min_in",
    "spec_rim_width_max_in",
    "spec_tire_weight_lb",
    "spec_utqg",
    "spec_utqg_treadwear",
    "spec_utqg_traction",
    "spec_utqg_temperature",
    "spec_wet_traction",
    "spec_mileage_warranty",
    "spec_mileage_warranty_miles",
    "spec_is_3pmsf",
    "spec_is_studdable",
    "spec_commercial_position",
    "spec_commercial_application",
    "spec_smartway_verified",
    "raw_specs",
    "specs_map",
    "raw_size",
    "raw_size_detail",
    "raw_product_line",
    "scraped_at",
]

# Reported verbatim when the database rejects a row, so the offending value is in the log rather
# than something to go re-derive by hand.
_NUMERIC_FIELD_NAMES: typing.Final[tuple[str, ...]] = (
    "rim_diameter_in",
    "spec_rim_width_min_in",
    "spec_rim_width_max_in",
    "spec_tread_depth_32nds",
    "spec_overall_diameter_in",
    "spec_section_width_in",
    "spec_tire_weight_lb",
    "simple_score",
    "handling_durability_score",
    "longevity_score",
    "traction_score",
    "spec_max_psi",
    "spec_max_load_lb",
)

# CMS/hero art repeated identically on thousands of rows. Dropped before raw_product_line is
# stored; everything else in siteProductLine is kept verbatim.
_PRODUCT_LINE_NOISE_KEYS = frozenset(
    {
        "heroBrandColor",
        "heroBrandImage",
        "heroCategoryImageM",
        "heroCategoryImageS",
        "heroCategoryImageXL",
        "siteProductBrand",
    }
)


class SimpleTireError(Exception):
    """A request to simpletire.com failed in a way retrying did not fix."""


class ProductLineNotFound(SimpleTireError):
    """The sitemap lists a product line the site no longer serves (404). Expected; skipped."""


@dataclasses.dataclass(frozen=True)
class ProductLineRef:
    """A model page to crawl, as named by the sitemap."""

    brand_slug: str
    line_slug: str

    @property
    def brand_param(self) -> str:
        """The ``brand`` query value: the URL slug minus its ``-tires`` suffix."""
        if self.brand_slug.endswith(_BRAND_SLUG_SUFFIX):
            return self.brand_slug[: -len(_BRAND_SLUG_SUFFIX)]
        return self.brand_slug

    @property
    def page_url(self) -> str:
        return f"{BASE_URL}/brands/{self.brand_slug}/{self.line_slug}"

    @property
    def key(self) -> str:
        return f"{self.brand_slug}/{self.line_slug}"


@dataclasses.dataclass
class CrawlStats:
    """Counters for one run. Printed by the command; also what a cron would alert on."""

    lines_total: int = 0
    lines_skipped: int = 0
    lines_done: int = 0
    lines_missing: int = 0
    lines_failed: int = 0
    lines_empty: int = 0
    skus_written: int = 0
    requests_made: int = 0

    # Only requests_made is touched off the main thread (every worker increments it), and `+=` on
    # an int is a read-modify-write that can drop increments under threads. Every other counter is
    # written by the loop that consumes results, so this is the only one that needs the lock.
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock, repr=False)

    def count_request(self) -> None:
        with self._lock:
            self.requests_made += 1


# ---------------------------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------------------------


class _TokenBucket:
    """Process-wide request throttle shared by the worker threads."""

    def __init__(self, rate_per_second: float) -> None:
        if rate_per_second <= 0:
            raise ValueError("rate_per_second must be positive")
        self._interval = 1.0 / rate_per_second
        self._lock = threading.Lock()
        self._next_at = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = max(0.0, self._next_at - now)
            # Anchor the next slot on the one we just handed out, so a burst of threads spaces
            # out instead of all sleeping to the same instant.
            self._next_at = max(now, self._next_at) + self._interval
        if wait:
            time.sleep(wait)


def _requests_module():  # pragma: no cover - import shape, exercised by every real call
    """
    Import curl_cffi lazily so importing this module (and therefore Django's app registry) does
    not hard-require a scraping dependency that only the crawl command needs.
    """
    try:
        from curl_cffi import requests as cffi_requests
    except ImportError as exc:  # pragma: no cover
        raise SimpleTireError(
            "curl_cffi is required to crawl simpletire.com (pip install curl_cffi). Plain requests "
            "is fingerprintable and gets blocked."
        ) from exc
    return cffi_requests


_thread_state = threading.local()


def _session():
    """
    One curl_cffi Session per worker thread, reused across requests.

    The first cut called the module-level ``requests.get()``, which builds and discards a Session
    -- and therefore a TLS connection -- per request. That was wrong twice: it made every request
    pay a fresh handshake (measured ~4.4s effective latency against ~0.33s once a connection is
    warm), and the discarded connections did not all close, so a run at concurrency 8 accumulated
    31+ ESTABLISHED sockets and eventually wedged with every worker stuck and no output.

    Thread-local rather than one shared Session because a curl handle is not safe to drive from
    several threads at once.
    """
    session = getattr(_thread_state, "session", None)
    if session is None:
        session = _requests_module().Session(
            impersonate=IMPERSONATE_PROFILE,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        _thread_state.session = session
    return session


def _discard_session() -> None:
    """Drop this thread's Session after a transport error, so a wedged handle is never retried on."""
    session = getattr(_thread_state, "session", None)
    _thread_state.session = None
    if session is not None:
        try:
            session.close()
        except Exception:  # closing a already-broken handle must not mask the original error
            logger.debug("simpletire: ignoring error while closing a broken session", exc_info=True)


def _get(url: str, *, referer: str, bucket: _TokenBucket | None, stats: CrawlStats) -> str:
    """
    GET ``url`` with a Chrome fingerprint, retrying transport errors and 5xx.

    Raises :class:`ProductLineNotFound` on 404 -- callers treat that as "skip this line", not as a
    failure worth aborting the crawl for. Any other 4xx is a bug in how we built the URL and is
    raised immediately rather than retried.
    """
    headers = {
        "accept": "application/json, text/plain, */*",
        "accept-language": "en-US,en;q=0.9",
        "referer": referer,
    }

    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if bucket is not None:
            bucket.acquire()
        try:
            stats.count_request()
            response = _session().get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
        except Exception as exc:  # curl_cffi raises its own transport errors; all are retryable
            last_error = exc
            # The handle may be half-open; a retry on it would hang rather than fail.
            _discard_session()
            logger.warning("simpletire: transport error on %s (attempt %s/%s): %s", url, attempt, MAX_ATTEMPTS, exc)
        else:
            if response.status_code == 200:
                return response.text
            if response.status_code == 404:
                raise ProductLineNotFound(url)
            if response.status_code < 500 and response.status_code != 429:
                raise SimpleTireError(f"{response.status_code} from {url}")
            last_error = SimpleTireError(f"{response.status_code} from {url}")
            logger.warning(
                "simpletire: HTTP %s on %s (attempt %s/%s)", response.status_code, url, attempt, MAX_ATTEMPTS
            )

        if attempt < MAX_ATTEMPTS:
            time.sleep(BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))

    raise SimpleTireError(f"giving up on {url} after {MAX_ATTEMPTS} attempts: {last_error}")


def _fetch_product_detail(
    ref: ProductLineRef,
    *,
    size: "SizeRef | None",
    bucket: _TokenBucket | None,
    stats: CrawlStats,
) -> dict:
    """
    Fetch one product-detail payload. ``size=None`` asks for the line's default SKU, which is also
    the only call that returns the full size list we then iterate.
    """
    params: list[tuple[str, str]] = []
    if size is not None:
        # All three, always. itemId is what actually selects; mpn/tireSize are sent because the
        # site sends them and an endpoint that sees a familiar shape is one that keeps working.
        params += [
            ("mpn", size.part_number or ""),
            ("tireSize", size.tire_size_slug or ""),
            ("itemId", str(size.item_id)),
        ]
    params += [("brand", ref.brand_param), ("productLine", ref.line_slug)]
    # urlencode, not an f-string: part numbers are free text and a '#' or '&' in one would
    # truncate the query and drop itemId -- i.e. quietly ask for a different tire.
    url = f"{BASE_URL}{PRODUCT_DETAIL_PATH}?{urllib.parse.urlencode(params)}"

    body = _get(url, referer=ref.page_url, bucket=bucket, stats=stats)
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SimpleTireError(f"non-JSON product-detail response for {ref.key}") from exc
    if not isinstance(payload, dict):
        raise SimpleTireError(f"unexpected product-detail shape for {ref.key}: {type(payload).__name__}")
    return payload


# ---------------------------------------------------------------------------------------------
# Enumeration
# ---------------------------------------------------------------------------------------------


def _parse_sitemap_locations(xml_text: str) -> list[str]:
    root = ElementTree.fromstring(xml_text)
    return [element.text.strip() for element in root.iterfind(_SITEMAP_LOC) if element.text]


def fetch_product_line_refs(*, stats: CrawlStats | None = None) -> list[ProductLineRef]:
    """
    Every ``/brands/{brand}/{line}`` page in the sitemap, de-duplicated and stably ordered.

    URLs of any other shape are dropped rather than guessed at -- the sitemap has been uniform in
    practice, and a silent reinterpretation here would produce rows attributed to the wrong brand.
    """
    stats = stats or CrawlStats()
    xml_text = _get(PRODUCT_LINE_SITEMAP_URL, referer=BASE_URL, bucket=None, stats=stats)

    refs: dict[str, ProductLineRef] = {}
    for location in _parse_sitemap_locations(xml_text):
        parts = location.split("/")
        if len(parts) != 6 or parts[3] != "brands":
            logger.debug("simpletire: ignoring non-product-line sitemap entry %s", location)
            continue
        ref = ProductLineRef(brand_slug=parts[4], line_slug=parts[5])
        refs.setdefault(ref.key, ref)
    return sorted(refs.values(), key=lambda ref: ref.key)


def fetch_brand_slugs(*, stats: CrawlStats | None = None) -> list[str]:
    """Brand slugs from ``/sitemap/brand.xml``. Handy for ``--brand`` validation and for counting."""
    stats = stats or CrawlStats()
    xml_text = _get(BRAND_SITEMAP_URL, referer=BASE_URL, bucket=None, stats=stats)
    slugs = {location.rsplit("/", 1)[-1] for location in _parse_sitemap_locations(xml_text)}
    return sorted(slug for slug in slugs if slug)


# ---------------------------------------------------------------------------------------------
# Scalar parsing
#
# Every function below turns a display string SimpleTire chose for humans into a number. They are
# pure and individually tested; the source string always survives in raw_specs, so a bug here is
# fixed by re-deriving columns, never by re-crawling.
# ---------------------------------------------------------------------------------------------

_NOT_AVAILABLE = frozenset({"", "na", "n/a", "none", "null", "-", "--"})


def _clean(value: object) -> str | None:
    """Trim a scalar to a non-empty string, mapping SimpleTire's several spellings of NA to None."""
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _NOT_AVAILABLE:
        return None
    return text


def parse_int(value: object) -> int | None:
    """First integer in the value, sign-aware. ``'51 PSI'`` -> 51, ``'8999'`` -> 8999."""
    text = _clean(value)
    if text is None:
        return None
    match = re.search(r"-?\d+", text.replace(",", ""))
    return int(match.group()) if match else None


def parse_decimal(value: object) -> decimal.Decimal | None:
    """First decimal in the value. ``'32.6\"'`` -> Decimal('32.6')."""
    text = _clean(value)
    if text is None:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return decimal.Decimal(match.group())
    except decimal.InvalidOperation:  # pragma: no cover - the regex cannot produce this
        return None


def parse_bool(value: object) -> bool | None:
    """
    Yes/No to a bool. Anything unrecognised is None, because on this table False is a claim
    ("not 3PMSF certified") and must not be invented from an unparsed string.
    """
    text = _clean(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in {"yes", "true", "y", "present", "available"}:
        return True
    if lowered in {"no", "false", "n", "not available"}:
        return False
    return None


def parse_load_index(value: object) -> tuple[int | None, int | None, int | None, int | None]:
    """
    ``'2756 lbs (116)'``            -> (116, None, 2756, None)
    ``'6173 lbs/5842 lbs (144/142)'`` -> (144, 142, 6173, 5842)   [single, dual, lb, dual lb]
    """
    text = _clean(value)
    if text is None:
        return None, None, None, None

    pounds = [int(match.replace(",", "")) for match in re.findall(r"([\d,]+)\s*lbs", text)]
    indexes: list[int] = []
    bracket = re.search(r"\(([^)]*)\)", text)
    if bracket:
        indexes = [int(match) for match in re.findall(r"\d+", bracket.group(1))]

    return (
        indexes[0] if indexes else None,
        indexes[1] if len(indexes) > 1 else None,
        pounds[0] if pounds else None,
        pounds[1] if len(pounds) > 1 else None,
    )


def parse_max_speed(value: object) -> tuple[int | None, str | None]:
    r"""
    ``'112 MPH (S)'`` -> (112, 'S'); ``'25 MPH (A8)'`` -> (25, 'A8').

    The bracket is matched greedily and anchored to the end of the string, because the symbol can
    itself be parenthesised: a tire rated above Y is published as ``'186 MPH ((Y))'``, and a
    non-greedy ``\(([^)]*)\)`` stops at the inner bracket and yields the unbalanced ``'(Y'``. That
    reached 603 SKUs and 733 tire specs before it was noticed, because ``'(Y'`` is not a code any
    lookup table has, so it failed silently rather than loudly.
    """
    text = _clean(value)
    if text is None:
        return None, None
    mph = parse_int(text)
    bracket = re.search(r"\((.*)\)\s*$", text)
    symbol = _clean(bracket.group(1)) if bracket else None
    return mph, symbol


def parse_ply_rating(value: object) -> int | None:
    """Ply count out of ``'E (10 Ply)'``. None for 'Standard (SL)' / 'Extra (XL)', which state none."""
    text = _clean(value)
    if text is None:
        return None
    match = re.search(r"(\d+)\s*ply", text, re.IGNORECASE)
    return int(match.group(1)) if match else None


def parse_utqg(value: object) -> tuple[str | None, int | None, str | None, str | None]:
    """
    ``'460AA'`` -> ('460AA', 460, 'A', 'A'); ``'220AAA'`` -> ('220AAA', 220, 'AA', 'A').

    Temperature is graded A/B/C -- always exactly one letter -- while traction is AA/A/B/C. So the
    letter run splits from the *right*: the last character is temperature and whatever precedes it
    is traction. Splitting from the left instead reads '460AA' as traction AA with no temperature,
    which is wrong for the most common grade string on the site.
    """
    text = _clean(value)
    if text is None:
        return None, None, None, None
    match = re.fullmatch(r"\s*(\d+)\s*([A-Za-z]*)\s*", text)
    if not match:
        return text, parse_int(text), None, None

    treadwear = int(match.group(1))
    letters = match.group(2).upper()
    if not letters:
        return text, treadwear, None, None
    if len(letters) == 1:
        # A lone letter is the traction grade; temperature simply was not published.
        return text, treadwear, letters, None
    return text, treadwear, letters[:-1], letters[-1]


def bounded_inches(value: object, *, maximum: decimal.Decimal | int, field: str) -> decimal.Decimal | None:
    """
    Parse a dimension, returning None when the result is not physically possible.

    SimpleTire's spec sheet is not clean. The Goodyear Competition Eliminator Super Comp publishes
    ``'Rim Range': ['2533"']`` -- its own part number, in the rim field. Widening the column to fit
    that would be the wrong repair: it would store 2533 as a rim width and let it reach a query one
    day. NULL is the honest answer, and the raw string is still in ``specs_map`` for anyone who
    wants to see what SimpleTire actually said.
    """
    parsed = parse_decimal(value)
    if parsed is None:
        return None
    if not decimal.Decimal(0) < parsed <= decimal.Decimal(maximum):
        logger.warning("simpletire: implausible %s %r; storing NULL (raw value kept in specs_map)", field, value)
        return None
    return parsed


def parse_rim_diameter(value: object) -> decimal.Decimal | None:
    """
    The size list's ``rim`` field, normalised to inches.

    SimpleTire drops the decimal point and sends the digits: 22.5" as ``225``, 24.5" as ``245``,
    and -- on industrial and lawn sizes -- 8.5" as ``85``, 11.25" as ``1125``, 12.125" as ``12125``.
    The scale is not fixed, so a single divide-by-ten is not enough (it turns ``915`` on a
    ``28-9.15`` into a 91.5" rim, and leaves ``85`` reading as 85").

    What *is* fixed is the physical ceiling: the largest rim in the catalogue is an OTR wheel at
    about 63". So divide by ten until the value is possible. Every correction this makes is
    checkable against ``size_display``, which spells the same number out (``18x8.00-12.125`` ->
    12.125). Values that never become plausible are NULL, not a guess.
    """
    parsed = parse_decimal(value)
    if parsed is None:
        return None
    for _ in range(_MAX_RIM_RESCALES):
        if parsed <= _MAX_RIM_DIAMETER_IN:
            break
        parsed = parsed / decimal.Decimal(10)
    if not decimal.Decimal(0) < parsed <= _MAX_RIM_DIAMETER_IN:
        logger.warning("simpletire: implausible rim diameter %r; storing NULL", value)
        return None
    return parsed


def parse_rim_range(value: object) -> tuple[decimal.Decimal | None, decimal.Decimal | None]:
    """``'7.50-8.25\"'`` -> (7.50, 8.25); a single width ``'8.25\"'`` -> (8.25, 8.25)."""
    text = _clean(value)
    if text is None:
        return None, None
    numbers = [decimal.Decimal(match) for match in re.findall(r"\d+(?:\.\d+)?", text)]
    # A rim wider than 60" does not exist; a value that large means SimpleTire put something other
    # than a rim width in the field. See :func:`bounded_inches`.
    numbers = [number for number in numbers if decimal.Decimal(0) < number <= decimal.Decimal(60)]
    if not numbers:
        if text:
            logger.warning("simpletire: no plausible rim width in %r; storing NULL", text)
        return None, None
    if len(numbers) == 1:
        return numbers[0], numbers[0]
    return min(numbers), max(numbers)


def parse_mileage_warranty(value: object) -> int | None:
    """``'65k'`` -> 65000, ``'60,000 miles'`` -> 60000, ``'N/A'`` -> None."""
    text = _clean(value)
    if text is None:
        return None
    match = re.search(r"([\d,.]+)\s*k\b", text, re.IGNORECASE)
    if match:
        return int(float(match.group(1).replace(",", "")) * 1000)
    return parse_int(text)


def parse_studdable(value: object) -> bool | None:
    """
    Studdability is published either as Yes/No or as a stud spec ('TSMI #11'). A stud pin number
    is an affirmative answer, so anything that is not an explicit no counts as True.
    """
    text = _clean(value)
    if text is None:
        return None
    explicit = parse_bool(text)
    if explicit is not None:
        return explicit
    return True


# ---------------------------------------------------------------------------------------------
# Payload -> row
# ---------------------------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SizeRef:
    """One entry of ``siteProductLineAvailableSizeList``, reduced to what refetching it needs."""

    item_id: int
    part_number: str | None
    tire_size_slug: str | None
    raw: dict


def _size_refs(payload: dict) -> list[SizeRef]:
    """
    Every purchasable size of the line. Entries without an itemId are dropped: without it the
    refetch silently returns the default size, which would write one SKU's specs under another's
    part number.
    """
    refs: list[SizeRef] = []
    for entry in payload.get("siteProductLineAvailableSizeList") or []:
        if not isinstance(entry, dict):
            continue
        query_params = entry.get("siteQueryParams") or {}
        item_id = parse_int(query_params.get("itemId"))
        if item_id is None:
            logger.warning("simpletire: size entry without itemId (%s); skipped", entry.get("size"))
            continue
        refs.append(
            SizeRef(
                item_id=item_id,
                part_number=_clean(entry.get("partNumber")) or _clean(query_params.get("mpn")),
                tire_size_slug=_clean(query_params.get("tireSize")),
                raw=entry,
            )
        )
    return refs


def _specs_map(raw_specs: list) -> dict[str, str]:
    """
    ``[{name, values: [...]}, ...]`` flattened to ``{name: 'a | b'}``.

    This is the escape hatch: SimpleTire publishes a long tail of specs (Low Rolling Resistance,
    Smartway, per-category oddities) that do not deserve columns, and this keeps them queryable
    without a schema change.
    """
    flattened: dict[str, str] = {}
    for spec in raw_specs:
        if not isinstance(spec, dict):
            continue
        name = _clean(spec.get("name"))
        if name is None:
            continue
        values = spec.get("values")
        if isinstance(values, list):
            joined = " | ".join(str(value).strip() for value in values if value is not None)
        else:
            joined = "" if values is None else str(values).strip()
        flattened[name] = joined
    return flattened


def _spec_columns(specs: dict[str, str]) -> dict[str, object]:
    """Map the spec sheet onto typed columns. Unknown spec names stay in ``specs_map`` only."""
    load_index, load_index_dual, max_load_lb, max_load_dual_lb = parse_load_index(specs.get("Load Index"))
    max_speed_mph, speed_rating = parse_max_speed(specs.get("Max Speed"))
    utqg, utqg_treadwear, utqg_traction, utqg_temperature = parse_utqg(specs.get("UTQG"))
    rim_min, rim_max = parse_rim_range(specs.get("Rim Range"))
    mileage_warranty = _clean(specs.get("Mileage Warranty"))

    return {
        "spec_category": _clean(specs.get("Category")),
        "spec_vehicle": _clean(specs.get("Vehicle")),
        "spec_sidewall": _clean(specs.get("Sidewall")),
        "spec_tread_design": _clean(specs.get("Tread Design")),
        "spec_load_range": _clean(specs.get("Load Range")),
        "spec_ply_rating": parse_ply_rating(specs.get("Load Range")),
        "spec_load_index": load_index,
        "spec_load_index_dual": load_index_dual,
        "spec_max_load_lb": max_load_lb,
        "spec_max_load_dual_lb": max_load_dual_lb,
        "spec_speed_rating": speed_rating,
        "spec_max_speed_mph": max_speed_mph,
        # Bounded rather than raw: these land in narrow numeric columns, and SimpleTire's spec
        # sheet occasionally carries a value that is not the quantity its label claims.
        "spec_tread_depth_32nds": bounded_inches(specs.get("Tread Depth"), maximum=200, field="tread depth"),
        "spec_overall_diameter_in": bounded_inches(
            specs.get("Overall Diameter"), maximum=200, field="overall diameter"
        ),
        "spec_section_width_in": bounded_inches(specs.get("Section Width"), maximum=200, field="section width"),
        "spec_max_psi": parse_int(specs.get("Inflation Pressure")),
        "spec_rim_width_min_in": rim_min,
        "spec_rim_width_max_in": rim_max,
        "spec_tire_weight_lb": bounded_inches(specs.get("Tire Weight"), maximum=20000, field="tire weight"),
        "spec_utqg": utqg,
        "spec_utqg_treadwear": utqg_treadwear,
        "spec_utqg_traction": utqg_traction,
        "spec_utqg_temperature": utqg_temperature,
        "spec_wet_traction": _clean(specs.get("Wet Traction")),
        "spec_mileage_warranty": mileage_warranty,
        "spec_mileage_warranty_miles": parse_mileage_warranty(mileage_warranty),
        "spec_is_3pmsf": parse_bool(specs.get("Three-Peak Mountain Snowflake (3PMS)")),
        "spec_is_studdable": parse_studdable(specs.get("Studdable")),
        "spec_commercial_position": _clean(specs.get("Commercial Position")),
        "spec_commercial_application": _clean(specs.get("Commercial Application")),
        "spec_smartway_verified": _clean(specs.get("Smartway Verified")),
    }


def _product_line_columns(product_line: dict) -> dict[str, object]:
    brand = product_line.get("brand") or {}
    brand_image = brand.get("image") or {}
    assets = product_line.get("assetList") or []
    image_url = None
    for asset in assets:
        image = (asset or {}).get("image") or {}
        if image.get("src"):
            image_url = image["src"]
            break

    return {
        "product_line_id": parse_int(product_line.get("productLineId")),
        "brand_name": _clean(brand.get("label")),
        "brand_tier": parse_int(product_line.get("brandTier")),
        "brand_logo_url": _clean(brand_image.get("src")),
        "product_line_name": _clean(product_line.get("name")),
        "product_line_overview": _clean(product_line.get("overview")),
        "product_line_image_url": image_url,
        "starting_price_cents": parse_int(product_line.get("startingPriceInCents")),
    }


def build_sku(
    ref: ProductLineRef,
    *,
    payload: dict,
    size: SizeRef | None,
    scraped_at,
) -> SimpleTireSku | None:
    """
    Turn one product-detail payload into an unsaved :class:`SimpleTireSku`.

    ``size`` is the size-list entry this payload was fetched for, or None for a line's default
    SKU. Returns None when the payload carries no ``siteProductLineSizeDetail.id`` -- without the
    natural key there is nothing to upsert on.
    """
    size_detail = payload.get("siteProductLineSizeDetail") or {}
    item_id = parse_int(size_detail.get("id"))
    if item_id is None:
        logger.warning("simpletire: %s returned a payload with no size-detail id; skipped", ref.key)
        return None

    price = size_detail.get("price") or {}
    road_hazard = size_detail.get("roadHazard") or {}
    product_line = payload.get("siteProductLine") or {}
    raw_specs = [spec for spec in (payload.get("siteProductSpecs") or []) if isinstance(spec, dict)]
    specs = _specs_map(raw_specs)

    raw_size_entry = size.raw if size is not None else None
    tire_size_slug = size.tire_size_slug if size is not None else None

    return SimpleTireSku(
        item_id=item_id,
        part_number=_clean(size_detail.get("partNumber")),
        brand_slug=ref.brand_slug,
        product_line_slug=ref.line_slug,
        page_url=ref.page_url,
        size_display=_clean(size_detail.get("size")),
        tire_size_slug=tire_size_slug,
        load_speed_rating=_clean(size_detail.get("loadSpeedRating")),
        # Load range is on the size-list entry, not the detail block; fall back to the spec sheet's
        # printed form ('Standard (SL)') when this SKU is a line's default and has no list entry.
        load_range=_clean((raw_size_entry or {}).get("loadRange")) or _clean(specs.get("Load Range")),
        rim_diameter_in=parse_rim_diameter((raw_size_entry or {}).get("rim")),
        product_type_id=parse_int(size_detail.get("ProductTypeId")),
        product_sub_type=_clean(size_detail.get("productSubType")),
        product_status=_clean(size_detail.get("productStatus")),
        quantity=parse_int(size_detail.get("quantity")),
        delivery_days=parse_int(size_detail.get("deliveryDays")),
        estimated_retail_price_cents=parse_int(price.get("estimatedRetailPriceInCents")),
        sale_price_cents=parse_int(price.get("salePriceInCents")),
        web_price_cents=parse_int(price.get("webPriceInCents")),
        price_label=_clean(size_detail.get("priceLabel")),
        road_hazard_price_cents=parse_int(road_hazard.get("pricePerTireInCents")),
        road_hazard_duration_label=_clean(road_hazard.get("durationLabel")),
        oversize_fee_cents=parse_int(size_detail.get("oversizeFee")),
        fet_fee_cents=parse_int(size_detail.get("fetFee")),
        is_run_flat=size_detail.get("isRunFlat") if isinstance(size_detail.get("isRunFlat"), bool) else None,
        is_electric_optimized=(
            size_detail.get("isElectricOptimized") if isinstance(size_detail.get("isElectricOptimized"), bool) else None
        ),
        is_oversized=size_detail.get("oversized") if isinstance(size_detail.get("oversized"), bool) else None,
        is_installable=(
            size_detail.get("isInstallable") if isinstance(size_detail.get("isInstallable"), bool) else None
        ),
        simple_score=parse_decimal(size_detail.get("simpleScore")),
        handling_durability_score=parse_decimal(size_detail.get("handlDuraScore")),
        longevity_score=parse_decimal(size_detail.get("longevityScore")),
        traction_score=parse_decimal(size_detail.get("tractionScore")),
        raw_specs=raw_specs,
        specs_map=specs,
        raw_size=raw_size_entry,
        raw_size_detail=size_detail,
        raw_product_line={key: value for key, value in product_line.items() if key not in _PRODUCT_LINE_NOISE_KEYS},
        scraped_at=scraped_at,
        **_product_line_columns(product_line),
        **_spec_columns(specs),
    )


# ---------------------------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------------------------


def _crawl_product_line(
    ref: ProductLineRef,
    *,
    bucket: _TokenBucket | None,
    stats: CrawlStats,
    max_sizes: int | None = None,
) -> list[SimpleTireSku]:
    """
    All SKUs of one product line: one request for the line, then one per remaining size.

    The default payload already contains the full spec sheet for whichever size SimpleTire chose
    to preselect, so that SKU is built from it rather than refetched.
    """
    scraped_at = timezone.now()
    default_payload = _fetch_product_detail(ref, size=None, bucket=bucket, stats=stats)

    sizes = _size_refs(default_payload)
    if max_sizes is not None:
        sizes = sizes[:max_sizes]

    default_id = parse_int((default_payload.get("siteProductLineSizeDetail") or {}).get("id"))
    by_item_id = {size.item_id: size for size in sizes}

    rows: list[SimpleTireSku] = []
    default_row = build_sku(ref, payload=default_payload, size=by_item_id.get(default_id), scraped_at=scraped_at)
    if default_row is not None:
        rows.append(default_row)

    for size in sizes:
        if size.item_id == default_id:
            continue  # already built from the default payload
        payload = _fetch_product_detail(ref, size=size, bucket=bucket, stats=stats)
        returned_id = parse_int((payload.get("siteProductLineSizeDetail") or {}).get("id"))
        if returned_id != size.item_id:
            # The endpoint answers 200 with the default size when it does not like the selector.
            # Writing that row would attribute one SKU's specs to another's part number.
            logger.error(
                "simpletire: asked %s for itemId=%s and got %s back; dropping the row",
                ref.key,
                size.item_id,
                returned_id,
            )
            continue
        row = build_sku(ref, payload=payload, size=size, scraped_at=scraped_at)
        if row is not None:
            rows.append(row)

    return rows


def _persist(rows: list[SimpleTireSku], *, batch_size: int) -> int:
    """
    Upsert on ``item_id``. Called from the main thread only -- workers do HTTP, never DB.

    Django's own ON CONFLICT DO UPDATE rather than pgbulk (which most of this package uses):
    pgbulk 3.2.4 reaches for a psycopg3-only escaping API and raises on this repo's psycopg2
    driver. Same reason as ``tire_enrichment._write_specs``.

    Rows are de-duplicated on ``item_id`` first because Postgres refuses a statement that tries to
    touch the same row twice ("cannot affect row a second time"), and one bad size list would
    otherwise take down a batch of 500 good ones.
    """
    unique_rows = {row.item_id: row for row in rows}
    if len(unique_rows) != len(rows):
        logger.warning("simpletire: %s duplicate item_id(s) collapsed before write", len(rows) - len(unique_rows))

    ordered = list(unique_rows.values())
    written = 0
    for start in range(0, len(ordered), batch_size):
        batch = ordered[start : start + batch_size]
        try:
            _upsert(batch)
            written += len(batch)
        except db_utils.DatabaseError as exc:
            # One unrepresentable value must not cost a multi-hour crawl. Retry the batch row by
            # row so the good rows land, and log the bad one loudly enough to fix the column.
            logger.warning("simpletire: batch upsert failed (%s); retrying %s rows singly", exc, len(batch))
            written += _upsert_individually(batch)
    return written


def _upsert(rows: list[SimpleTireSku]) -> None:
    SimpleTireSku.objects.bulk_create(
        rows,
        update_conflicts=True,
        unique_fields=["item_id"],
        update_fields=UPDATE_FIELDS,
    )


def _upsert_individually(rows: list[SimpleTireSku]) -> int:
    """
    Write ``rows`` one at a time, skipping any the database rejects.

    A rejected row is a schema bug, not bad luck -- SimpleTire published a value wider than the
    column we guessed for it -- so the log line carries the value and the URL needed to reproduce
    it. The crawl carries on: the row is re-fetched on the next run, once the column is widened.
    """
    written = 0
    for row in rows:
        try:
            with transaction.atomic():
                _upsert([row])
        except db_utils.DatabaseError as exc:
            logger.error(
                "simpletire: REJECTED item_id=%s (%s %s) from %s -- %s | numeric fields: %s",
                row.item_id,
                row.part_number,
                row.size_display,
                row.page_url,
                str(exc).strip().replace("\n", " "),
                {name: getattr(row, name) for name in _NUMERIC_FIELD_NAMES if getattr(row, name, None) is not None},
            )
        else:
            written += 1
    return written


class _StallWatchdog:
    """
    Turns a hung crawl into a loud, resumable exit instead of a process that sits there forever.

    A worker can wedge on a half-open socket -- that is exactly how the first full run died, with
    every worker stuck, ``as_completed`` blocked, and no output at all for ten minutes. Retries and
    per-request timeouts reduce the odds but cannot promise never; silence is the part that is
    unacceptable, because a stalled crawl looks identical to a slow one.

    So: a daemon thread watches the time since the last completed product line. It warns first, and
    if nothing lands for :data:`STALL_ABORT_SECONDS` it kills the process.

    ``os._exit`` is deliberate. There is no way to interrupt a thread blocked in libcurl, and a
    normal exit would join the pool and hang with it. It is safe here precisely because of how the
    rest of the module is arranged: every batch is committed by its own upsert before the line is
    checkpointed, so a hard exit loses nothing that was recorded, and re-running resumes.
    """

    def __init__(self, *, emit: typing.Callable[[str], None]) -> None:
        self._emit = emit
        self._last_beat = time.monotonic()
        self._lock = threading.Lock()
        self._stopping = threading.Event()
        self._warned = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="simpletire-watchdog", daemon=True)
        self._thread.start()

    def beat(self) -> None:
        with self._lock:
            self._last_beat = time.monotonic()
            self._warned = False

    def stop(self) -> None:
        self._stopping.set()

    def _run(self) -> None:
        while not self._stopping.wait(STALL_CHECK_SECONDS):
            with self._lock:
                idle = time.monotonic() - self._last_beat
                warned = self._warned
                if idle >= STALL_WARN_SECONDS:
                    self._warned = True

            if idle >= STALL_ABORT_SECONDS:
                message = (
                    f"simpletire: no product line has completed in {int(idle)}s -- the crawl is "
                    "wedged. Exiting so it is visible; re-run the same command to resume from the "
                    "checkpoint."
                )
                logger.error(message)
                self._emit(f"  STALLED: {message}")
                sys.stdout.flush()
                sys.stderr.flush()
                os._exit(75)  # EX_TEMPFAIL: retryable, and the checkpoint makes retrying cheap.

            if idle >= STALL_WARN_SECONDS and not warned:
                self._emit(f"  WARNING: no product line completed in the last {int(idle)}s")


class _Checkpoint:
    """
    Append-only record of finished product lines, one JSON object per line.

    Written from the main thread as results land, so a kill -9 loses at most the lines still in
    flight. Reading it back is what ``--resume`` skips on.
    """

    def __init__(self, path: pathlib.Path | None) -> None:
        self.path = path

    def completed_keys(self) -> set[str]:
        if self.path is None or not self.path.exists():
            return set()
        keys: set[str] = set()
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("simpletire: unreadable checkpoint line; ignoring it")
                    continue
                key = record.get("key")
                if key:
                    keys.add(key)
        return keys

    def record(self, ref: ProductLineRef, *, status: str, skus: int) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"key": ref.key, "status": status, "skus": skus}) + "\n")
            handle.flush()


def run_crawl(
    *,
    refs: list[ProductLineRef],
    concurrency: int = DEFAULT_CONCURRENCY,
    rate_per_second: float = DEFAULT_RATE_PER_SECOND,
    batch_size: int = DEFAULT_BATCH_SIZE,
    checkpoint_path: pathlib.Path | None = None,
    resume: bool = True,
    dry_run: bool = False,
    max_sizes_per_line: int | None = None,
    progress: typing.Callable[[str], None] | None = None,
) -> CrawlStats:
    """
    Crawl ``refs`` and upsert every SKU found.

    Product lines are fetched concurrently; each worker owns one line end-to-end (its own sizes
    included) so a failure is contained to that line. Writes happen on the main thread as results
    arrive, which keeps the DB single-writer and means the checkpoint can only ever record a line
    whose rows are already committed.
    """
    stats = CrawlStats(lines_total=len(refs))
    checkpoint = _Checkpoint(checkpoint_path)
    emit = progress or (lambda message: None)

    pending = refs
    if resume:
        done = checkpoint.completed_keys()
        if done:
            pending = [ref for ref in refs if ref.key not in done]
            stats.lines_skipped = len(refs) - len(pending)
            emit(f"resume: skipping {stats.lines_skipped} product lines already in the checkpoint")

    if not pending:
        return stats

    bucket = _TokenBucket(rate_per_second)
    watchdog = _StallWatchdog(emit=emit)
    watchdog.start()

    def work(ref: ProductLineRef) -> tuple[ProductLineRef, list[SimpleTireSku] | None, Exception | None]:
        try:
            return ref, _crawl_product_line(ref, bucket=bucket, stats=stats, max_sizes=max_sizes_per_line), None
        except ProductLineNotFound as exc:
            return ref, None, exc
        except Exception as exc:  # one bad line must not take the crawl down
            return ref, None, exc

    with futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        submitted = {pool.submit(work, ref): ref for ref in pending}
        for index, future in enumerate(futures.as_completed(submitted), start=1):
            ref, rows, error = future.result()
            watchdog.beat()

            if isinstance(error, ProductLineNotFound):
                stats.lines_missing += 1
                checkpoint.record(ref, status="missing", skus=0)
            elif error is not None:
                stats.lines_failed += 1
                logger.exception("simpletire: %s failed", ref.key, exc_info=error)
                emit(f"  FAILED {ref.key}: {error}")
                # Deliberately NOT checkpointed: a failure should be retried by the next run.
            else:
                rows = rows or []
                if not rows:
                    stats.lines_empty += 1
                if not dry_run and rows:
                    stats.skus_written += _persist(rows, batch_size=batch_size)
                elif dry_run:
                    stats.skus_written += len(rows)
                stats.lines_done += 1
                if not dry_run:
                    checkpoint.record(ref, status="ok", skus=len(rows))

            if index % 25 == 0 or index == len(pending):
                emit(
                    f"  {index}/{len(pending)} lines | {stats.skus_written} skus | "
                    f"{stats.requests_made} requests | {stats.lines_missing} missing | "
                    f"{stats.lines_failed} failed"
                )

    watchdog.stop()
    return stats
