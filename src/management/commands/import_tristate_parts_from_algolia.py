"""
One-off local import of Tri-State Enterprises' own catalog slice from AAM/PACE's shared Algolia
"parts" search index (the same public search-only key every PACE storefront exposes client-side
-- not a distributor API credential, not Tri-State's PACE Api-Key/App-Id). See
src/models.py::TriStatePart for what this stores and why it's separate from pace_catalog.

Only ``TSE`` (Tri-State's own member code) is ever requested from the per-distributor sub-objects
the index carries -- via an explicit ``attributesToRetrieve`` allowlist, same safeguard as
pace_catalog's import, so no OTHER distributor's inventory is ever received.

Algolia's search endpoint hard-caps at 1000 total results per query for this key (confirmed
live: requesting page 1 of an already-1000-hit page 0 returns an explicit
"you can only fetch the 1000 hits for this query ... use the browse method" error --
``/browse`` itself is blocked for this key with "missing ACL browse", and the limit itself
(``paginationLimitedTo``) is an index-level setting, not a per-query parameter). To get a
brand's FULL catalog regardless of size, each brand is fetched via ``TSE.inventory`` bucketing:
starting from the full range (0..infinity), any bucket that comes back at exactly 1000 is
recursively split in half (by inventory value) until every leaf bucket is confirmed under the
cap -- at which point its count is exact, not an estimate. In practice Tri-State's largest brand
seen so far (Air Lift, 734 parts) never needed splitting at all; this is a correctness
guarantee for any brand that might.

Not a scheduled/cron job -- run by hand, once, from a developer machine.

    python manage.py import_tristate_parts_from_algolia
    python manage.py import_tristate_parts_from_algolia --brands BBCG,DHTP --dry-run
"""
import json
import logging
import os
import time
import typing

import pgbulk
import requests
from django.core.management.base import BaseCommand
from concurrent.futures import ThreadPoolExecutor, as_completed

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[IMPORT-TRISTATE-PARTS]"

# Public Algolia search credentials -- see import_pace_catalog_from_algolia.py's own docstring
# for why this is safe (the same key any site visitor's browser already uses).
_ALGOLIA_APP_ID = "KQYP22CAB6"
_ALGOLIA_SEARCH_API_KEY = "3c45e1ee9dac3ee161e58118412b600c"
_ALGOLIA_INDEX = "parts"
_ALGOLIA_QUERY_URL = "https://{app_id}-dsn.algolia.net/1/indexes/{index}/query".format(
    app_id=_ALGOLIA_APP_ID, index=_ALGOLIA_INDEX
)

_HITS_PER_PAGE = 1000
_INVENTORY_CEILING = 1_000_000  # effective "infinity" for the unbounded-above split -- no real
                                 # distributor stocks anywhere near this many units of one part.
_PRICE_CEILING = 1_000_000.0  # same idea, for the price-based secondary split fallback below.
_MAX_SPLIT_DEPTH = 30  # 2**30 >> either ceiling; purely a runaway-recursion guard.
_REQUEST_TIMEOUT_SECONDS = 20
_MAX_RETRIES = 3
_UPSERT_BATCH = 2000

# Shared catalog fields (never any OTHER distributor's sub-object) plus "TSE" itself -- Tri-State's
# own inventory/category/product_id/slug for this part.
_ATTRIBUTES_TO_RETRIEVE = [
    "objectID", "aaia_brand_code", "part", "name", "ext", "price", "part_search", "pace", "aw",
    "TSE",
    "BaseVehicleID", "SubModelID", "EngineBaseID", "EngineMfrID", "EngineVINID",
    "EngineDesignationID", "EngineVersionID", "EngineSizeID", "EngineFamilyID", "FuelTypeID",
    "FuelDeliveryTypeID", "FuelDeliverySubTypeID", "FuelSystemControlTypeID", "FuelSystemDesignID",
    "DriveTypeID", "BodyTypeID", "BodyNumDoorsID", "BedTypeID", "BedLengthID", "WheelBaseID",
    "VehicleTypeID", "RegionID", "TransmissionTypeID", "TransmissionMfrID",
    "TransmissionMfrCodeID", "TransmissionControlTypeID", "TransmissionNumSpeedsID",
    "TransElecControlledID", "SteeringTypeID", "SteeringSystemID", "FrontBrakeTypeID",
    "RearBrakeTypeID", "BrakeSystemID", "BrakeABSID", "FrontSpringTypeID", "RearSpringTypeID",
    "CylinderHeadTypeID", "AspirationID", "IgnitionSystemTypeID", "PowerOutputID",
    "ValvesPerEngineID", "MfrBodyCodeID",
]
_FITMENT_ATTRS = {
    a for a in _ATTRIBUTES_TO_RETRIEVE
    if a != "objectID" and (a.endswith("ID") or a in ("BaseVehicleID", "SubModelID"))
}


def _brand_codes_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "tristate_brand_codes.json")


def _load_brand_codes() -> typing.List[typing.Dict[str, str]]:
    with open(_brand_codes_path()) as f:
        return json.load(f)


def _inventory_filter(lo: int, hi: typing.Optional[int]) -> str:
    if hi is None:
        return "TSE.inventory >= {}".format(lo)
    return "TSE.inventory >= {} AND TSE.inventory <= {}".format(lo, hi)


def _price_filter(exact_inventory: int, plo: float, phi: typing.Optional[float]) -> str:
    base = "TSE.inventory = {}".format(exact_inventory)
    if phi is None:
        return "{} AND price >= {}".format(base, plo)
    return "{} AND price >= {} AND price <= {}".format(base, plo, phi)


def _query_algolia(aaia_brand_code: str, inventory_filter: str, page: int) -> typing.Dict:
    body = {
        "query": "",
        "hitsPerPage": _HITS_PER_PAGE,
        "page": page,
        "facetFilters": ["aaia_brand_code:{}".format(aaia_brand_code)],
        "filters": inventory_filter,
        "attributesToRetrieve": _ATTRIBUTES_TO_RETRIEVE,
    }
    last_error = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = requests.post(
                _ALGOLIA_QUERY_URL,
                headers={
                    "X-Algolia-API-Key": _ALGOLIA_SEARCH_API_KEY,
                    "X-Algolia-Application-Id": _ALGOLIA_APP_ID,
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            last_error = e
            if attempt < _MAX_RETRIES:
                time.sleep(1.5 * attempt)
    raise last_error


def _fetch_single_filter(aaia_brand_code: str, filter_str: str) -> typing.Optional[typing.List[typing.Dict]]:
    """Page through one filter with no further splitting. Returns None (instead of a possibly-
    truncated list) if a page came back exactly full -- Algolia's 1000-result cap makes the next
    page unreliable (it errors regardless of whether the true total is exactly 1000 or more), so
    the caller must split and retry rather than trust this count."""
    hits: typing.List[typing.Dict] = []
    page = 0
    while True:
        data = _query_algolia(aaia_brand_code, filter_str, page)
        page_hits = data.get("hits", [])
        hits.extend(page_hits)
        if len(page_hits) < _HITS_PER_PAGE:
            return hits
        return None


def _fetch_price_bucket(
    aaia_brand_code: str, exact_inventory: int, plo: float, phi: typing.Optional[float], depth: int = 0
) -> typing.List[typing.Dict]:
    """Secondary split dimension, used only when a single exact TSE.inventory value alone has
    >=1000 matching parts (so the primary inventory-based split has bottomed out). Splits that
    slice by price instead. Known gap: parts with no price field in Algolia at all are invisible
    to any price filter (same as a missing inventory value would be) -- there's no documented
    way to query "attribute absent" via this public search API, so a brand would need BOTH the
    exact same inventory value AND no price on >=1000 parts for this to lose data -- logged
    loudly if max split depth is ever reached, since this fallback can't detect that case itself."""
    filter_str = _price_filter(exact_inventory, plo, phi)
    result = _fetch_single_filter(aaia_brand_code, filter_str)
    if result is not None:
        return result

    if depth >= _MAX_SPLIT_DEPTH:
        logger.error(
            "{} {}: TSE.inventory={} price range [{}, {}] still capped at max split depth {} -- "
            "giving up, results for this slice are likely incomplete.".format(
                _LOG_PREFIX, aaia_brand_code, exact_inventory, plo, phi, _MAX_SPLIT_DEPTH
            )
        )
        return _fetch_single_filter(aaia_brand_code, filter_str) or []

    ceiling = phi if phi is not None else _PRICE_CEILING
    mid = plo + (ceiling - plo) / 2
    if mid <= plo + 1e-6:
        logger.error(
            "{} {}: TSE.inventory={} price range [{}, {}] cannot be split further (degenerate) "
            "-- results for this slice are likely incomplete.".format(
                _LOG_PREFIX, aaia_brand_code, exact_inventory, plo, phi
            )
        )
        return _fetch_single_filter(aaia_brand_code, filter_str) or []

    left = _fetch_price_bucket(aaia_brand_code, exact_inventory, plo, mid, depth + 1)
    right = _fetch_price_bucket(aaia_brand_code, exact_inventory, mid, phi, depth + 1)
    return left + right


def _fetch_bucket(
    aaia_brand_code: str, lo: int, hi: typing.Optional[int], depth: int = 0
) -> typing.List[typing.Dict]:
    result = _fetch_single_filter(aaia_brand_code, _inventory_filter(lo, hi))
    if result is not None:
        return result

    # Capped at exactly 1000 for this range -- split it and recurse. Each half that comes back
    # under the cap is an exact count for that half, not an estimate.
    if lo == hi:
        # Bottomed out: a single exact inventory value alone has >=1000 matches. Switch to the
        # price-based secondary split instead of giving up.
        logger.info(
            "{} {}: TSE.inventory={} alone has >=1000 parts -- switching to a price-based "
            "secondary split for this slice.".format(_LOG_PREFIX, aaia_brand_code, lo)
        )
        return _fetch_price_bucket(aaia_brand_code, lo, 0.0, None)

    if depth >= _MAX_SPLIT_DEPTH:
        logger.error(
            "{} {}: inventory range [{}, {}] still capped at max split depth {} -- "
            "giving up further splitting, results for this slice may be incomplete.".format(
                _LOG_PREFIX, aaia_brand_code, lo, hi, _MAX_SPLIT_DEPTH
            )
        )
        return _fetch_single_filter(aaia_brand_code, _inventory_filter(lo, hi)) or []

    ceiling = hi if hi is not None else _INVENTORY_CEILING
    mid = lo + (ceiling - lo) // 2
    if mid <= lo:
        mid = lo  # force the lo==hi degenerate branch above on the next recursive call

    left = _fetch_bucket(aaia_brand_code, lo, mid, depth + 1)
    right = _fetch_bucket(aaia_brand_code, mid + 1, hi, depth + 1)
    return left + right


def _fetch_all_parts_for_brand(aaia_brand_code: str, brand_name: str) -> typing.List[typing.Dict]:
    hits = _fetch_bucket(aaia_brand_code, 0, None)
    # De-dupe defensively -- bucket boundaries are inclusive/exclusive-correct by construction,
    # but a part can theoretically flip which bucket it lands in between two separate HTTP calls
    # if Tri-State's stock count changes mid-import.
    by_object_id = {h.get("objectID"): h for h in hits if h.get("objectID")}
    deduped = list(by_object_id.values())
    if len(deduped) != len(hits):
        logger.warning(
            "{} {} ({}): {} raw hits deduped to {} unique objectIDs (stock likely changed "
            "mid-fetch).".format(_LOG_PREFIX, brand_name, aaia_brand_code, len(hits), len(deduped))
        )
    return deduped


def _hit_to_instance(hit: typing.Dict) -> typing.Optional[src_models.TriStatePart]:
    object_id = hit.get("objectID")
    part = hit.get("part")
    aaia_brand_code = hit.get("aaia_brand_code")
    if not object_id or not part or not aaia_brand_code:
        return None

    pace = hit.get("pace") or {}
    aw = hit.get("aw") or {}
    tse = hit.get("TSE") or {}
    fitment = {k: hit[k] for k in _FITMENT_ATTRS if k in hit and hit[k]}

    return src_models.TriStatePart(
        object_id=object_id,
        aaia_brand_code=aaia_brand_code,
        part_number=part,
        name=hit.get("name"),
        description=hit.get("ext"),
        price=hit.get("price"),
        part_search=hit.get("part_search") or [],
        pace_brand=pace.get("brand"),
        pace_name=pace.get("name"),
        pace_image=pace.get("image"),
        pace_slug=pace.get("slug"),
        aw_brand=aw.get("brand"),
        aw_name=aw.get("name"),
        aw_image=aw.get("image"),
        aw_slug=aw.get("slug"),
        tse_inventory=tse.get("inventory"),
        tse_category=tse.get("category") if isinstance(tse.get("category"), str) else None,
        tse_subcategory=tse.get("subcategory") if isinstance(tse.get("subcategory"), str) else None,
        tse_series=tse.get("series"),
        tse_product_id=tse.get("product_id"),
        tse_slug=tse.get("slug"),
        fitment=fitment,
    )


def _upsert_instances(instances: typing.List[src_models.TriStatePart]) -> int:
    written = 0
    for start in range(0, len(instances), _UPSERT_BATCH):
        batch = instances[start:start + _UPSERT_BATCH]
        pgbulk.upsert(
            src_models.TriStatePart,
            batch,
            unique_fields=["object_id"],
            update_fields=[
                "aaia_brand_code", "part_number", "name", "description", "price", "part_search",
                "pace_brand", "pace_name", "pace_image", "pace_slug",
                "aw_brand", "aw_name", "aw_image", "aw_slug",
                "tse_inventory", "tse_category", "tse_subcategory", "tse_series",
                "tse_product_id", "tse_slug", "fitment", "updated_at",
            ],
            returning=False,
        )
        written += len(batch)
    return written


class Command(BaseCommand):
    help = (
        "One-off local import of Tri-State Enterprises' own catalog (brand/part/price/fitment/"
        "inventory) from AAM's shared Algolia index into tristate_parts. Not scheduled -- run "
        "by hand from a developer machine."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brands", type=str, default=None,
            help="Comma-separated AAIA brand codes to import (default: all 146 bundled Tri-State brands).",
        )
        parser.add_argument(
            "--workers", type=int, default=6,
            help="Parallel brand fetches (I/O-bound HTTP calls to Algolia). Default: 6.",
        )
        parser.add_argument("--dry-run", action="store_true", help="Fetch and parse but don't write to the database.")

    def handle(self, *args, **options):
        brand_codes = _load_brand_codes()
        if options.get("brands"):
            wanted = {c.strip().upper() for c in options["brands"].split(",") if c.strip()}
            brand_codes = [b for b in brand_codes if b["code"].upper() in wanted]
            if not brand_codes:
                self.stdout.write(self.style.ERROR("No matching brand codes found for --brands filter."))
                return

        workers = max(1, int(options["workers"]))
        dry_run = bool(options["dry_run"])

        self.stdout.write(
            "Importing {} Tri-State brand(s) from Algolia (workers={}, dry_run={})...".format(
                len(brand_codes), workers, dry_run
            )
        )

        total_parts = 0
        total_written = 0
        failed_brands = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_brand = {
                executor.submit(_fetch_all_parts_for_brand, b["code"], b["name"]): b
                for b in brand_codes
            }
            for i, future in enumerate(as_completed(future_to_brand), start=1):
                brand = future_to_brand[future]
                try:
                    hits = future.result()
                except Exception as e:
                    failed_brands.append((brand["code"], brand["name"], str(e)))
                    self.stdout.write(self.style.ERROR(
                        "[{}/{}] {} ({}): FAILED - {}".format(i, len(brand_codes), brand["name"], brand["code"], e)
                    ))
                    continue

                instances = [inst for inst in (_hit_to_instance(h) for h in hits) if inst is not None]
                total_parts += len(instances)

                if instances and not dry_run:
                    written = _upsert_instances(instances)
                    total_written += written

                self.stdout.write(
                    "[{}/{}] {} ({}): {} parts".format(i, len(brand_codes), brand["name"], brand["code"], len(instances))
                )

        self.stdout.write(self.style.SUCCESS(
            "Done. {} parts fetched across {} brand(s){}.".format(
                total_parts, len(brand_codes),
                "" if dry_run else ", {} upserted into tristate_parts".format(total_written),
            )
        ))
        if failed_brands:
            self.stdout.write(self.style.WARNING(
                "{} brand(s) failed: {}".format(
                    len(failed_brands), ", ".join("{} ({})".format(n, c) for c, n, _ in failed_brands)
                )
            ))
