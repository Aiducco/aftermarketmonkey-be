"""
Shared plumbing for importing AAM/PACE's Meilisearch backend (``search.theaamgroup.com``), which
every PACE member distributor's storefront queries client-side.

Underscore-prefixed so Django's command discovery skips it -- a helper module, not a command.
Used by import_pace_taxonomy, import_pace_parts and import_pace_parts_by_id, each of which
takes ``--member``.

One index set per member, named ``PACE_<member>_<thing>``, all with the same document shape, so
the member code is the only thing that varies. Members are registered in ``MEMBERS``; adding a
distributor means four models, a migration and one entry here -- no new command code.

TWO TRAPS, both confirmed the hard way
--------------------------------------
1. The host is unreachable from the office IP -- a direct request times out with no response at
   all. Every call MUST go through a StormProxies gateway, and the calling IP has to be
   whitelisted in the StormProxies panel first.
2. ``STORM_PROXY_ACCOUNT_NAME`` is an email address, so a ``@`` interpolated into a proxy URL
   makes it parse as host ``gmail.com`` and fail with a DNS error that reads like a dead proxy.
   Credentials are percent-encoded in ``proxy_url``.

Also: nginx in front of Meilisearch 403s anything it does not take for the real storefront, so
the browser's full header set is sent verbatim. The host intermittently drops into sustained
403 waves lasting minutes, and ~1 in 10 requests dies as a connection timeout -- hence gateway
rotation plus request spacing.

THE 1,000-RESULT CAP
--------------------
``maxTotalHits`` is 1,000 on this instance, and ``maxValuesPerFacet`` likewise. Confirmed, not
inferred: a query reporting ``totalHits`` 1549 / ``totalPages`` 16 returns zero hits on page 11.
No query can ever yield more than 1,000 documents, so every import strategy is really a way of
carving the catalog into sub-1,000 slices. See the two parts commands for the two strategies and
the measurements that decide between them.
"""
import dataclasses
import decimal
import logging
import threading
import time
import urllib.parse

import pgbulk
import requests
from django.conf import settings
from django.core.management.base import CommandError

from src import models as src_models

logger = logging.getLogger(__name__)

BASE_URL = "https://search.theaamgroup.com"

# The storefront's own public search key -- read-only, exposed client-side to every site
# visitor's browser. Not a distributor API credential.
SEARCH_API_KEY = "e1aa20da300595b7e34652a96ae9eefd4b98a5e73e514582da38f0c148a79ecf"

BROWSER_HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"
    ),
    "origin": "https://etristate.com",
    "referer": "https://etristate.com/",
    "content-type": "application/json",
    "accept": "*/*",
    "sec-fetch-site": "cross-site",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
}

DEFAULT_GATEWAYS = (
    "37.48.118.90:13081",
    "83.149.70.159:13081",
    "108.59.14.203:13081",
    "108.59.14.208:13081",
)

MAX_TOTAL_HITS = 1000
REQUEST_TIMEOUT_SECONDS = 45
MAX_ATTEMPTS = 6
UPSERT_BATCH = 1000
DEFAULT_REQUEST_SPACING_SECONDS = 1.0
DEFAULT_WORKERS = 4


@dataclasses.dataclass(frozen=True)
class Member:
    """One PACE member distributor: its index prefix and the four tables it lands in."""

    code: str
    name: str
    category_model: type
    brand_model: type
    series_model: type
    part_model: type

    def index(self, thing: str) -> str:
        return "PACE_{}_{}".format(self.code, thing)


MEMBERS: dict[str, Member] = {
    m.code: m
    for m in (
        Member("TSE", "Tri-State Enterprises", src_models.PaceTseCategory,
               src_models.PaceTseBrand, src_models.PaceTseSeries, src_models.PaceTsePart),
        Member("DPN", "DIX Performance", src_models.PaceDpnCategory,
               src_models.PaceDpnBrand, src_models.PaceDpnSeries, src_models.PaceDpnPart),
        Member("LOR", "Lordco Auto Parts", src_models.PaceLorCategory,
               src_models.PaceLorBrand, src_models.PaceLorSeries, src_models.PaceLorPart),
        Member("TFT", "Toys for Trucks", src_models.PaceTftCategory,
               src_models.PaceTftBrand, src_models.PaceTftSeries, src_models.PaceTftPart),
        Member("A1D", "A1 Distributing", src_models.PaceA1dCategory,
               src_models.PaceA1dBrand, src_models.PaceA1dSeries, src_models.PaceA1dPart),
        Member("APD", "AllPro Distributing", src_models.PaceApdCategory,
               src_models.PaceApdBrand, src_models.PaceApdSeries, src_models.PaceApdPart),
        Member("RPT", "RPT Distributing", src_models.PaceRptCategory,
               src_models.PaceRptBrand, src_models.PaceRptSeries, src_models.PaceRptPart),
        Member("AAS", "Alamo Auto Supply", src_models.PaceAasCategory,
               src_models.PaceAasBrand, src_models.PaceAasSeries, src_models.PaceAasPart),
        Member("MWW", "Midwest Wheel", src_models.PaceMwwCategory,
               src_models.PaceMwwBrand, src_models.PaceMwwSeries, src_models.PaceMwwPart),
    )
}


def add_member_argument(parser) -> None:
    parser.add_argument(
        "--member",
        action="append",
        metavar="CODE",
        help="PACE member code (repeatable): {}. Omit for all.".format(", ".join(MEMBERS)),
    )


def resolve_members(codes: list[str] | None) -> list[Member]:
    """No --member means every registered member, so a full sweep needs no arguments."""
    if not codes:
        return list(MEMBERS.values())
    resolved, unknown = [], []
    for raw in codes:
        key = (raw or "").strip().upper()
        (resolved.append(MEMBERS[key]) if key in MEMBERS else unknown.append(raw))
    if unknown:
        raise CommandError(
            "unknown member(s) {} -- known: {}".format(
                ", ".join(repr(u) for u in unknown), ", ".join(sorted(MEMBERS))
            )
        )
    # dict.fromkeys keeps order while dropping repeats
    return list(dict.fromkeys(resolved))


def gateways() -> list[str]:
    raw = (
        getattr(settings, "PACE_PROXY_GATEWAYS", "")
        or getattr(settings, "PACE_TSE_PROXY_GATEWAYS", "")
        or ""
    )
    configured = [g.strip() for g in raw.split(",") if g.strip()]
    return configured or list(DEFAULT_GATEWAYS)


def proxy_url(gateway: str) -> str:
    """Percent-encode the credentials -- see trap 2 in this module's docstring."""
    username = getattr(settings, "STORM_PROXY_ACCOUNT_NAME", "") or ""
    password = getattr(settings, "STORM_PROXY_PASSWORD", "") or ""
    host = gateway.replace("http://", "").replace("https://", "")
    if not (username and password):
        return "http://{}".format(host)
    return "http://{}:{}@{}".format(
        urllib.parse.quote(username, safe=""), urllib.parse.quote(password, safe=""), host
    )


class Searcher:
    """Posts searches against one index, rotating gateways on failure and pacing requests.

    A failure is usually a dead proxy exit IP, which the next gateway's fresh exit IP fixes
    immediately -- so this rotates rather than backing off hard against one endpoint.
    """

    def __init__(self, index: str, spacing_seconds: float = DEFAULT_REQUEST_SPACING_SECONDS):
        self.index = index
        self.url = "{}/indexes/{}/search".format(BASE_URL, index)
        self.session = requests.Session()
        self.gateways = gateways()
        self.spacing_seconds = spacing_seconds
        self._cursor = 0
        self._last_request_at = 0.0
        self.request_count = 0

    def _wait_turn(self) -> None:
        """Measured from the previous request rather than slept blindly, so time already spent
        waiting on a slow response counts toward the gap."""
        if self.spacing_seconds <= 0:
            return
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < self.spacing_seconds:
            time.sleep(self.spacing_seconds - elapsed)

    def search(self, body: dict) -> dict:
        last_error: Exception | None = None
        for attempt in range(MAX_ATTEMPTS):
            self._wait_turn()
            self._last_request_at = time.monotonic()
            gateway = self.gateways[(self._cursor + attempt) % len(self.gateways)]
            proxy = proxy_url(gateway)
            try:
                response = self.session.post(
                    self.url,
                    headers={"Authorization": "Bearer {}".format(SEARCH_API_KEY), **BROWSER_HEADERS},
                    json=body,
                    proxies={"http": proxy, "https": proxy},
                    timeout=REQUEST_TIMEOUT_SECONDS,
                )
                response.raise_for_status()
                self._cursor += 1
                self.request_count += 1
                return response.json()
            except requests.RequestException as e:
                last_error = e
                logger.warning(
                    "[PACE-SEARCH] %s attempt %s/%s via %s failed: %s",
                    self.index, attempt + 1, MAX_ATTEMPTS, gateway, e,
                )
                time.sleep(1.0 + attempt)
        raise CommandError(
            "{}: search failed after {} attempts: {}".format(self.index, MAX_ATTEMPTS, last_error)
        )

    def facet(self, filter_str: str, facet: str) -> dict[str, int]:
        """Exact per-value counts. Capped at 1,000 distinct values (maxValuesPerFacet), so a
        facet reporting exactly 1,000 buckets is truncated and its counts cannot be trusted as
        a complete partition."""
        body = {"q": "", "facets": [facet], "hitsPerPage": 1, "page": 1}
        if filter_str:
            body["filter"] = filter_str
        payload = self.search(body)
        return (payload.get("facetDistribution") or {}).get(facet) or {}

    def slice_hits(self, filter_str: str, page_size: int = MAX_TOTAL_HITS) -> tuple[list[dict], int]:
        """(hits, totalHits) for one slice. totalHits above the hits returned means the response
        was capped and this slice needs splitting -- the only reliable truncation signal."""
        body = {"q": "", "hitsPerPage": page_size, "page": 1}
        if filter_str:
            body["filter"] = filter_str
        payload = self.search(body)
        return payload.get("hits") or [], payload.get("totalHits") or 0

    def walk(self, filter_str: str, page_size: int = 100) -> list[dict]:
        """Page an index straight through. Only safe where the result set is under the 1,000
        cap -- used for the taxonomy indexes, never for parts."""
        hits: list[dict] = []
        page, total_pages = 1, None
        while page <= 1000:
            body = {"q": "", "hitsPerPage": page_size, "page": page}
            if filter_str:
                body["filter"] = filter_str
            payload = self.search(body)
            batch = payload.get("hits") or []
            if not batch:
                break
            hits.extend(batch)
            if total_pages is None:
                total_pages = payload.get("totalPages")
            if total_pages is not None and page >= total_pages:
                break
            page += 1
        return hits


# One Searcher per worker thread per index: requests.Session is not safe to share across
# threads, and each searcher keeping its own spacing is what makes the pacing per worker.
_thread_state = threading.local()


def thread_searcher(index: str, spacing_seconds: float) -> Searcher:
    cache = getattr(_thread_state, "searchers", None)
    if cache is None:
        cache = _thread_state.searchers = {}
    if index not in cache:
        cache[index] = Searcher(index, spacing_seconds=spacing_seconds)
    return cache[index]


def thread_request_count() -> int:
    cache = getattr(_thread_state, "searchers", None) or {}
    return sum(s.request_count for s in cache.values())


# --- document -> model -------------------------------------------------------------------

def _text(hit: dict, key: str, limit: int | None = None) -> str:
    """Documents omit keys entirely rather than sending null, and the models use empty-string
    defaults rather than nullable text."""
    value = hit.get(key)
    text = "" if value is None else str(value)
    return text[:limit] if limit else text


def _list(hit: dict, key: str) -> list:
    value = hit.get(key)
    return value if isinstance(value, list) else []


def _int(hit: dict, key: str) -> int:
    value = hit.get(key)
    return int(value) if isinstance(value, (int, float)) else 0


# Part fields with their own column; everything else in a parts document lands in `fitment`.
_PART_SCALARS = {
    "objectID", "part", "aaia_brand_code", "catalog_object_id", "stock_number", "brand", "name",
    "ext", "slug", "image", "has_image", "category", "subcategory", "series", "price",
    "inventory", "Sales", "viewed", "aces", "part_search", "makes", "models", "years",
    "site_id", "member_code", "group_name",
}


def category_instance(member: Member, hit: dict):
    object_id = _text(hit, "objectID", 64)
    if not object_id:
        return None
    return member.category_model(
        object_id=object_id,
        name=_text(hit, "name", 255),
        slug=_text(hit, "slug", 255),
        parent_name=_text(hit, "parent_name", 255),
        parent_slug=_text(hit, "parent_slug", 255),
        parts=_list(hit, "parts"),
        series=_list(hit, "series"),
        brands=_list(hit, "brands"),
        site_id=_text(hit, "site_id", 16),
        member_code=_text(hit, "member_code", 16) or member.code,
        group_name=_text(hit, "group_name", 32),
    )


def brand_instance(member: Member, hit: dict):
    object_id = _text(hit, "objectID", 64)
    if not object_id:
        return None
    return member.brand_model(
        object_id=object_id,
        # objectID on this index IS the AAIA code; fall back to it since aaia_brand_code is
        # what anything else joins on.
        aaia_brand_code=(_text(hit, "aaia_brand_code") or object_id)[:8],
        name=_text(hit, "name", 255),
        slug=_text(hit, "slug", 255),
        image=_text(hit, "image"),
        parts=_list(hit, "parts"),
        categories=_list(hit, "categories"),
        subcategories=_list(hit, "subcategories"),
        series=_list(hit, "series"),
        site_id=_text(hit, "site_id", 16),
        member_code=_text(hit, "member_code", 16) or member.code,
        group_name=_text(hit, "group_name", 32),
    )


def series_instance(member: Member, hit: dict):
    object_id = _text(hit, "objectID", 64)
    if not object_id:
        return None
    return member.series_model(
        object_id=object_id,
        name=_text(hit, "name", 255),
        slug=_text(hit, "slug", 255),
        brand=_text(hit, "brand", 255),
        aaia_brand_code=_text(hit, "aaia_brand_code", 8),
        brand_slug=_text(hit, "brand_slug", 255),
        parts=_list(hit, "parts"),
        categories=_list(hit, "categories"),
        subcategories=_list(hit, "subcategories"),
        site_id=_text(hit, "site_id", 16),
        member_code=_text(hit, "member_code", 16) or member.code,
        group_name=_text(hit, "group_name", 32),
    )


def part_instance(member: Member, hit: dict):
    object_id = _text(hit, "objectID", 64)
    if not object_id:
        return None

    price = hit.get("price")
    try:
        price_value = decimal.Decimal(str(price)) if price is not None else None
    except (decimal.InvalidOperation, ValueError):
        price_value = None

    return member.part_model(
        object_id=object_id,
        part=_text(hit, "part", 128),
        aaia_brand_code=_text(hit, "aaia_brand_code", 8),
        catalog_object_id=_text(hit, "catalog_object_id", 255),
        stock_number=_text(hit, "stock_number", 128),
        brand=_text(hit, "brand", 255),
        name=_text(hit, "name"),
        ext=_text(hit, "ext"),
        slug=_text(hit, "slug"),
        image=_text(hit, "image"),
        has_image=bool(hit.get("has_image")),
        category=_text(hit, "category", 255),
        subcategory=_text(hit, "subcategory", 255),
        series=_text(hit, "series", 255),
        price=price_value,
        inventory=_int(hit, "inventory"),
        sales=_int(hit, "Sales"),
        viewed=_int(hit, "viewed"),
        aces=_int(hit, "aces"),
        part_search=_list(hit, "part_search"),
        makes=_list(hit, "makes"),
        models=_list(hit, "models"),
        years=_list(hit, "years"),
        fitment={k: v for k, v in hit.items() if k not in _PART_SCALARS},
        site_id=_text(hit, "site_id", 16),
        member_code=_text(hit, "member_code", 16) or member.code,
        group_name=_text(hit, "group_name", 32),
    )


CATEGORY_UPDATE_FIELDS = [
    "name", "slug", "parent_name", "parent_slug", "parts", "series", "brands",
    "site_id", "member_code", "group_name", "updated_at",
]
BRAND_UPDATE_FIELDS = [
    "aaia_brand_code", "name", "slug", "image", "parts", "categories", "subcategories",
    "series", "site_id", "member_code", "group_name", "updated_at",
]
SERIES_UPDATE_FIELDS = [
    "name", "slug", "brand", "aaia_brand_code", "brand_slug", "parts", "categories",
    "subcategories", "site_id", "member_code", "group_name", "updated_at",
]
PART_UPDATE_FIELDS = [
    "part", "aaia_brand_code", "catalog_object_id", "stock_number", "brand", "name", "ext",
    "slug", "image", "has_image", "category", "subcategory", "series", "price", "inventory",
    "sales", "viewed", "aces", "part_search", "makes", "models", "years", "fitment",
    "site_id", "member_code", "group_name", "updated_at",
]


def upsert(model, instances: list, update_fields: list[str]) -> int:
    """Bulk upsert on object_id, so re-running any of these imports is idempotent."""
    written = 0
    for start in range(0, len(instances), UPSERT_BATCH):
        batch = instances[start:start + UPSERT_BATCH]
        pgbulk.upsert(
            model, batch, unique_fields=["object_id"], update_fields=update_fields,
            returning=False,
        )
        written += len(batch)
    return written
