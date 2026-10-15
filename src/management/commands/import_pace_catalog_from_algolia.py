"""
One-off local import of AAM/PACE's shared catalog data (brand, part number, description, price,
category, fitment) from their public Algolia "parts" search index -- the same index and public
search-only key every visitor's browser already uses for the live search box on any PACE-powered
distributor site (e.g. etristate.com). Confirmed live 2026-10-15 against Tri-State's own site.

Deliberately NOT a scheduled/cron job: this hits an undocumented, non-contracted data source (not
the official PACE Customer API), so it's meant to be run by hand, when needed, from a developer's
own machine -- not wired into any recurring pipeline. See the project chat history for the full
reasoning: the index also carries every AAM member distributor's own live inventory count per
part (keyed by their member code, e.g. "TSE" for Tri-State) when ``attributesToRetrieve`` isn't
constrained -- this command explicitly allowlists only the shared catalog fields below, so that
per-distributor data is never even requested, let alone received or stored.

Run with no App-Id/Api-Key for any distributor's actual PACE account -- this only ever uses
Algolia's separate public search credentials (appId + search-only apiKey), the same ones embedded
in the site's own page source, safe to use from anywhere.

    python manage.py import_pace_catalog_from_algolia
    python manage.py import_pace_catalog_from_algolia --brands AIR,BBCG --dry-run
    python manage.py import_pace_catalog_from_algolia --workers 10
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

_LOG_PREFIX = "[IMPORT-PACE-CATALOG]"

# Public Algolia search credentials embedded client-side on etristate.com (and presumably every
# other PACE-powered site) -- a search-only key, not a distributor API credential. Safe to call
# from anywhere, same as any visitor's browser already does for the site's own search box.
_ALGOLIA_APP_ID = "KQYP22CAB6"
_ALGOLIA_SEARCH_API_KEY = "3c45e1ee9dac3ee161e58118412b600c"
_ALGOLIA_INDEX = "parts"
_ALGOLIA_QUERY_URL = "https://{app_id}-dsn.algolia.net/1/indexes/{index}/query".format(
    app_id=_ALGOLIA_APP_ID, index=_ALGOLIA_INDEX
)

_HITS_PER_PAGE = 1000  # Algolia's own search endpoint caps total addressable results at 1000
                        # for this key (confirmed live: page 1 beyond that returns empty, and
                        # /browse is blocked -- "missing ACL browse"). Rare for a single brand to
                        # exceed this; logged as a warning per-brand if it looks capped.

# Only these fields are ever requested from Algolia -- the shared catalog data, never any of the
# ~175 per-distributor sub-objects (inventory, category placement, product_id) the index also
# carries for every AAM member. Keeping this list explicit is the actual safeguard, not a filter
# applied after the fact.
_ATTRIBUTES_TO_RETRIEVE = [
    "objectID",
    "aaia_brand_code",
    "part",
    "name",
    "ext",
    "price",
    "category",
    "subcategory",
    "series",
    "part_search",
    "pace",
    "aw",
    "BaseVehicleID",
    "SubModelID",
    "EngineBaseID",
    "EngineMfrID",
    "EngineVINID",
    "EngineDesignationID",
    "EngineVersionID",
    "EngineSizeID",
    "EngineFamilyID",
    "FuelTypeID",
    "FuelDeliveryTypeID",
    "FuelDeliverySubTypeID",
    "FuelSystemControlTypeID",
    "FuelSystemDesignID",
    "DriveTypeID",
    "BodyTypeID",
    "BodyNumDoorsID",
    "BedTypeID",
    "BedLengthID",
    "WheelBaseID",
    "VehicleTypeID",
    "RegionID",
    "TransmissionTypeID",
    "TransmissionMfrID",
    "TransmissionMfrCodeID",
    "TransmissionControlTypeID",
    "TransmissionNumSpeedsID",
    "TransElecControlledID",
    "SteeringTypeID",
    "SteeringSystemID",
    "FrontBrakeTypeID",
    "RearBrakeTypeID",
    "BrakeSystemID",
    "BrakeABSID",
    "FrontSpringTypeID",
    "RearSpringTypeID",
    "CylinderHeadTypeID",
    "AspirationID",
    "IgnitionSystemTypeID",
    "PowerOutputID",
    "ValvesPerEngineID",
    "MfrBodyCodeID",
]

# Any of the above *ID/BaseVehicleID/SubModelID attributes actually present on a hit (Algolia
# omits attributes a record doesn't have, rather than sending 0 for every one of them).
# "objectID" is deliberately excluded even though it also ends in "ID" -- that's the Algolia
# record ID, already stored as its own column, not a fitment attribute.
_FITMENT_ATTRS = {
    a for a in _ATTRIBUTES_TO_RETRIEVE
    if a != "objectID" and (a.endswith("ID") or a in ("BaseVehicleID", "SubModelID"))
}

_UPSERT_BATCH = 2000
_REQUEST_TIMEOUT_SECONDS = 20
_MAX_RETRIES = 3


def _brand_codes_path() -> str:
    return os.path.join(os.path.dirname(__file__), "data", "pace_aaia_brand_codes.json")


def _load_brand_codes() -> typing.List[typing.Dict[str, str]]:
    with open(_brand_codes_path()) as f:
        return json.load(f)


def _query_algolia(aaia_brand_code: str, page: int) -> typing.Dict:
    body = {
        "query": "",
        "hitsPerPage": _HITS_PER_PAGE,
        "page": page,
        "facetFilters": ["aaia_brand_code:{}".format(aaia_brand_code)],
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


def _fetch_all_pages_for_brand(aaia_brand_code: str, brand_name: str) -> typing.List[typing.Dict]:
    """Paginate a single brand's results until a short page signals the end."""
    hits: typing.List[typing.Dict] = []
    page = 0
    while True:
        data = _query_algolia(aaia_brand_code, page)
        page_hits = data.get("hits", [])
        hits.extend(page_hits)
        if len(page_hits) < _HITS_PER_PAGE:
            break
        page += 1

    if len(hits) >= _HITS_PER_PAGE and page == 0:
        logger.warning(
            "{} {} ({}): hit the {}-result page cap -- this brand may have more parts than "
            "this key's search endpoint can return (no /browse ACL to confirm).".format(
                _LOG_PREFIX, brand_name, aaia_brand_code, _HITS_PER_PAGE
            )
        )
    return hits


def _hit_to_instance(hit: typing.Dict) -> typing.Optional[src_models.PaceCatalogPart]:
    object_id = hit.get("objectID")
    part = hit.get("part")
    aaia_brand_code = hit.get("aaia_brand_code")
    if not object_id or not part or not aaia_brand_code:
        return None

    pace = hit.get("pace") or {}
    aw = hit.get("aw") or {}
    fitment = {k: hit[k] for k in _FITMENT_ATTRS if k in hit and hit[k]}

    return src_models.PaceCatalogPart(
        object_id=object_id,
        aaia_brand_code=aaia_brand_code,
        part_number=part,
        name=hit.get("name"),
        description=hit.get("ext"),
        price=hit.get("price"),
        category=hit.get("category"),
        subcategory=hit.get("subcategory"),
        series=hit.get("series"),
        part_search=hit.get("part_search") or [],
        pace_brand=pace.get("brand"),
        pace_name=pace.get("name"),
        pace_image=pace.get("image"),
        pace_slug=pace.get("slug"),
        aw_brand=aw.get("brand"),
        aw_name=aw.get("name"),
        aw_image=aw.get("image"),
        aw_slug=aw.get("slug"),
        fitment=fitment,
    )


def _upsert_instances(instances: typing.List[src_models.PaceCatalogPart]) -> int:
    written = 0
    for start in range(0, len(instances), _UPSERT_BATCH):
        batch = instances[start:start + _UPSERT_BATCH]
        pgbulk.upsert(
            src_models.PaceCatalogPart,
            batch,
            unique_fields=["object_id"],
            update_fields=[
                "aaia_brand_code", "part_number", "name", "description", "price",
                "category", "subcategory", "series", "part_search",
                "pace_brand", "pace_name", "pace_image", "pace_slug",
                "aw_brand", "aw_name", "aw_image", "aw_slug",
                "fitment", "updated_at",
            ],
            returning=False,
        )
        written += len(batch)
    return written


class Command(BaseCommand):
    help = (
        "One-off local import of AAM/PACE's shared Algolia catalog (brand/part/price/fitment) "
        "into pace_catalog. Not scheduled -- run by hand from a developer machine."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--brands",
            type=str,
            default=None,
            help="Comma-separated AAIA brand codes to import (default: all bundled brand codes).",
        )
        parser.add_argument(
            "--workers",
            type=int,
            default=6,
            help="Parallel brand fetches (I/O-bound HTTP calls to Algolia). Default: 6.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Fetch and parse but don't write to the database.",
        )

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
            "Importing {} brand(s) from Algolia (workers={}, dry_run={})...".format(
                len(brand_codes), workers, dry_run
            )
        )

        total_parts = 0
        total_written = 0
        failed_brands = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_brand = {
                executor.submit(_fetch_all_pages_for_brand, b["code"], b["name"]): b
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
                total_parts,
                len(brand_codes),
                "" if dry_run else ", {} upserted into pace_catalog".format(total_written),
            )
        ))
        if failed_brands:
            self.stdout.write(self.style.WARNING(
                "{} brand(s) failed: {}".format(
                    len(failed_brands), ", ".join("{} ({})".format(n, c) for c, n, _ in failed_brands)
                )
            ))
