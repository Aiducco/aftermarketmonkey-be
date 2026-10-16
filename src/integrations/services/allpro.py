"""
AllPro Distributing integration services.

Unlike Tri-State, AllPro is a genuine bulk-feed distributor (FTP, plain username/password per
dealer) -- see src.integrations.clients.allpro.client's module docstring for the three real
files and their confirmed-live shapes/sizes. Four jobs, run in this order:

  1. ``fetch_and_save_allpro_brands`` -- upsert AllProBrand from APDLineCodes.txt. Prerequisite
     for catalog ingestion (AllProParts.brand is a FK to AllProBrand, not a raw AAIA string).

  2. ``sync_unmapped_allpro_brands_to_brands`` -- map AllProBrand -> Brands by exact AAIA code
     only, same reasoning as Tri-State's own brand-mapping function (AAIA-code matching is
     reliable; no fuzzy name matching, no auto-creating new Brands rows).

  3. ``fetch_and_save_allpro_catalog`` -- stream APDInventory.txt into AllProParts (shared
     catalog+inventory, not per-company -- this is AllPro's real warehouse stock, the same
     regardless of which dealer's login fetched it). Rows with no real AAIA code (blank, or the
     literal placeholder "NO") are skipped -- there's no reliable brand identity to key them on.

  4. ``fetch_and_save_allpro_company_pricing`` -- stream MASTER_PRICEFILE.CSV into
     AllProCompanyPricing for one company, joined against AllProParts.full_part_number with the
     file's own ":" separator stripped (see the client's docstring for why this only resolves a
     real subset of rows, logged rather than hidden).

A fifth, independent job enriches the catalog from the shared Algolia index (description,
category, image -- NOT in AllPro's own feed at all):

  5. ``enrich_allpro_parts_from_algolia`` -- same recursive inventory/price-bucketing technique
     proven on Tri-State (APD also hits Algolia's 1000-hit cap per brand, confirmed live against
     WeatherTech/BHTJ), matched by (brand.aaia_code, part_number) -- Algolia's own "part" field
     for APD entries is the vendor part number, not AllPro's concatenated full_part_number.
     Writes AllProParts.description/image_url/category/subcategory/series only -- NEVER read by
     master_parts.py's AllPro ingest for image_url, per explicit instruction: AllPro images must
     never reach MasterPart/ProviderPart.
"""
import logging
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation

import pgbulk
import requests
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.allpro import client as allpro_client
from src.integrations.clients.allpro import exceptions as allpro_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ALLPRO-SERVICES]"

_BRAND_UPSERT_BATCH = 500
_CATALOG_UPSERT_BATCH = 5000
_PRICING_UPSERT_BATCH = 5000


def primary_allpro_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """Prefer primary=True; otherwise first active AllPro CompanyProviders (catalog + brand
    seeding source for ingest_all_providers' Phase 1 -- same pattern as Meyer's own
    _primary_meyer_company_provider)."""
    base = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.ALLPRO_DISTRIBUTING.value, active=True,
    )
    return base.filter(primary=True).first() or base.first()


def _allpro_client_for_company_provider(company_provider_id: int) -> typing.Optional[allpro_client.AllProFTPClient]:
    cp = src_models.CompanyProviders.objects.filter(id=company_provider_id).select_related("company").first()
    if not cp:
        logger.warning("{} No CompanyProviders row id={}.".format(_LOG_PREFIX, company_provider_id))
        return None
    credentials = credentials_helper.get_feed_credentials(cp)
    try:
        return allpro_client.AllProFTPClient(credentials=credentials)
    except ValueError as e:
        logger.warning(
            "{} company_provider_id={}: invalid/missing credentials ({}). Skipping.".format(
                _LOG_PREFIX, company_provider_id, e
            )
        )
        return None


def fetch_and_save_allpro_brands(company_provider_id: int) -> int:
    """Upsert AllProBrand from APDLineCodes.txt (keyed by aaia_code -- see its own docstring).
    Returns the number of distinct AAIA codes upserted."""
    client = _allpro_client_for_company_provider(company_provider_id)
    if not client:
        return 0

    by_aaia: typing.Dict[str, typing.Dict] = {}
    try:
        for row in client.iter_line_codes():
            aaia = row["aaia_code"]
            if not aaia:
                continue
            by_aaia[aaia] = row  # last row for a given AAIA wins, same as every other dedupe here
    except allpro_exceptions.AllProException as e:
        logger.error("{} Failed to fetch AllPro line codes: {}".format(_LOG_PREFIX, e))
        return 0

    instances = [
        src_models.AllProBrand(aaia_code=aaia, line_code=row["line_code"], name=row["name"])
        for aaia, row in by_aaia.items()
    ]
    for start in range(0, len(instances), _BRAND_UPSERT_BATCH):
        pgbulk.upsert(
            src_models.AllProBrand,
            instances[start:start + _BRAND_UPSERT_BATCH],
            unique_fields=["aaia_code"],
            update_fields=["line_code", "name", "updated_at"],
            returning=False,
        )
    logger.info("{} Upserted {} AllProBrand rows from APDLineCodes.txt.".format(_LOG_PREFIX, len(instances)))
    return len(instances)


def sync_unmapped_allpro_brands_to_brands() -> int:
    """Map AllProBrand -> Brands by exact AAIA code only. Returns the number newly mapped."""
    logger.info("{} Syncing unmapped AllPro brands to Brands (AAIA code only).".format(_LOG_PREFIX))

    mapped_ids = set(
        src_models.BrandAllProBrandMapping.objects.values_list("allpro_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.AllProBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped AllPro brands. Nothing to sync.".format(_LOG_PREFIX))
        return 0

    aaia_codes = {ab.aaia_code.upper() for ab in unmapped}
    brand_by_aaia: typing.Dict[str, src_models.Brands] = {}
    for b in src_models.Brands.objects.filter(aaia_code__in=aaia_codes).order_by("id"):
        key = (b.aaia_code or "").strip().upper()
        if key and key not in brand_by_aaia:
            brand_by_aaia[key] = b

    new_mappings = []
    unresolved = 0
    for ab in unmapped:
        brand = brand_by_aaia.get(ab.aaia_code.upper())
        if brand:
            new_mappings.append(src_models.BrandAllProBrandMapping(brand=brand, allpro_brand=ab))
        else:
            unresolved += 1

    if new_mappings:
        pgbulk.upsert(
            src_models.BrandAllProBrandMapping,
            new_mappings,
            unique_fields=["brand", "allpro_brand"],
            update_fields=[],
        )
    logger.info(
        "{} Mapped {}/{} unmapped AllPro brands; {} unresolved (no matching AAIA code in Brands).".format(
            _LOG_PREFIX, len(new_mappings), len(unmapped), unresolved
        )
    )
    return len(new_mappings)


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    s = (value or "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    d = _safe_decimal(value)
    if d is None:
        return None
    try:
        return int(d)
    except (ValueError, OverflowError):
        return None


def fetch_and_save_allpro_catalog(company_provider_id: int) -> int:
    """
    Stream APDInventory.txt into AllProParts (shared catalog+inventory). Rows with a blank or
    "NO" AAIA, or whose AAIA has no AllProBrand row yet (run fetch_and_save_allpro_brands
    first), are skipped and counted rather than failing the run. Returns rows upserted.
    """
    client = _allpro_client_for_company_provider(company_provider_id)
    if not client:
        return 0

    brand_id_by_aaia = {
        ab.aaia_code.upper(): ab.id for ab in src_models.AllProBrand.objects.all()
    }
    if not brand_id_by_aaia:
        logger.warning(
            "{} No AllProBrand rows found -- run fetch_and_save_allpro_brands first.".format(_LOG_PREFIX)
        )
        return 0

    total_written = 0
    total_skipped_no_brand = 0
    batch: typing.List[src_models.AllProParts] = []

    def _flush(batch_list: typing.List[src_models.AllProParts]) -> int:
        if not batch_list:
            return 0
        pgbulk.upsert(
            src_models.AllProParts,
            batch_list,
            unique_fields=["full_part_number"],
            update_fields=[
                "brand", "part_number", "line_code", "length", "width", "height", "weight", "upc",
                "warehouse_va_qty", "warehouse_fl_qty", "warehouse_al_qty", "total_qty", "updated_at",
            ],
            returning=False,
        )
        return len(batch_list)

    try:
        for row in client.iter_inventory_records():
            aaia = (row.get("aaia_code") or "").strip().upper()
            if not aaia or aaia == "NO":
                total_skipped_no_brand += 1
                continue
            brand_id = brand_id_by_aaia.get(aaia)
            if brand_id is None:
                total_skipped_no_brand += 1
                continue
            full_part_number = row["full_part_number"].strip()
            part_number = row["part_number"].strip()
            if not full_part_number or not part_number:
                continue
            batch.append(
                src_models.AllProParts(
                    brand_id=brand_id,
                    full_part_number=full_part_number,
                    part_number=part_number,
                    line_code=row.get("line_code", "").strip(),
                    length=_safe_decimal(row.get("length")),
                    width=_safe_decimal(row.get("width")),
                    height=_safe_decimal(row.get("height")),
                    weight=_safe_decimal(row.get("weight")),
                    upc=row.get("upc", "").strip() or None,
                    warehouse_va_qty=_safe_int(row.get("warehouse_va_qty")),
                    warehouse_fl_qty=_safe_int(row.get("warehouse_fl_qty")),
                    warehouse_al_qty=_safe_int(row.get("warehouse_al_qty")),
                    total_qty=_safe_int(row.get("total_qty")),
                )
            )
            if len(batch) >= _CATALOG_UPSERT_BATCH:
                total_written += _flush(batch)
                batch = []
                logger.info(
                    "{} company_provider_id={}: {} AllProParts written so far ({} skipped, no brand).".format(
                        _LOG_PREFIX, company_provider_id, total_written, total_skipped_no_brand
                    )
                )
        total_written += _flush(batch)
    except allpro_exceptions.AllProException as e:
        logger.error("{} Failed mid-catalog-fetch: {}".format(_LOG_PREFIX, e))

    logger.info(
        "{} company_provider_id={}: catalog sync done -- {} written, {} skipped (no resolvable brand).".format(
            _LOG_PREFIX, company_provider_id, total_written, total_skipped_no_brand
        )
    )
    return total_written


def fetch_and_save_allpro_company_pricing(company_provider_id: int) -> int:
    """
    Stream MASTER_PRICEFILE.CSV into AllProCompanyPricing for this company. Joined against
    AllProParts.full_part_number with the file's own ":" stripped -- see this module's and the
    client's docstrings for why only a real subset resolves. Returns rows upserted.
    """
    cp = src_models.CompanyProviders.objects.filter(id=company_provider_id).first()
    if not cp:
        logger.warning("{} No CompanyProviders row id={}.".format(_LOG_PREFIX, company_provider_id))
        return 0
    client = _allpro_client_for_company_provider(company_provider_id)
    if not client:
        return 0

    part_id_by_full_number = dict(
        src_models.AllProParts.objects.values_list("full_part_number", "id").iterator(chunk_size=20000)
    )
    if not part_id_by_full_number:
        logger.warning(
            "{} No AllProParts rows found -- run fetch_and_save_allpro_catalog first.".format(_LOG_PREFIX)
        )
        return 0

    now = timezone.now()
    total_written = 0
    total_unmatched = 0
    total_rows = 0
    batch: typing.List[src_models.AllProCompanyPricing] = []

    def _flush(batch_list: typing.List[src_models.AllProCompanyPricing]) -> int:
        if not batch_list:
            return 0
        # MASTER_PRICEFILE.CSV has overlapping rows that resolve to the same full_part_number
        # after the ":" strip (confirmed live -- Postgres' ON CONFLICT rejects a statement that
        # would touch the same (part, company) row twice) -- dedupe within the batch, last row
        # wins, same convention as every other provider's pricing upsert in master_parts.py.
        deduped = {(r.part_id, r.company_id): r for r in batch_list}
        pgbulk.upsert(
            src_models.AllProCompanyPricing,
            list(deduped.values()),
            unique_fields=["part", "company"],
            update_fields=["cost", "updated_at"],
            returning=False,
        )
        return len(deduped)

    try:
        for row in client.iter_pricing_records():
            total_rows += 1
            key = row["raw_part_number"].upper().replace(":", "")
            part_id = part_id_by_full_number.get(key)
            if part_id is None:
                total_unmatched += 1
                continue
            cost = _safe_decimal(row.get("cost"))
            batch.append(
                src_models.AllProCompanyPricing(
                    part_id=part_id, company_id=cp.company_id, cost=cost, updated_at=now,
                )
            )
            if len(batch) >= _PRICING_UPSERT_BATCH:
                total_written += _flush(batch)
                batch = []
        total_written += _flush(batch)
    except allpro_exceptions.AllProException as e:
        logger.error("{} Failed mid-pricing-fetch: {}".format(_LOG_PREFIX, e))

    logger.info(
        "{} company_provider_id={}: pricing sync done -- {} rows read, {} written, {} unmatched "
        "(no corresponding AllProParts row).".format(
            _LOG_PREFIX, company_provider_id, total_rows, total_written, total_unmatched
        )
    )
    return total_written


def sync_allpro_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """Entry point wired into integration_pricing_sync_jobs._fetch_raw_pricing."""
    fetch_and_save_allpro_company_pricing(company_provider_id)


# ---------------------------------------------------------------------------
# Algolia enrichment (description/category/image -- NOT in AllPro's own feed)
# ---------------------------------------------------------------------------

_ALGOLIA_APP_ID = "KQYP22CAB6"
_ALGOLIA_SEARCH_API_KEY = "3c45e1ee9dac3ee161e58118412b600c"
_ALGOLIA_QUERY_URL = "https://{}-dsn.algolia.net/1/indexes/parts/query".format(_ALGOLIA_APP_ID)
_ALGOLIA_ATTRS = ["objectID", "part", "aaia_brand_code", "name", "ext", "pace", "aw", "APD"]
_ALGOLIA_MEMBER = "APD"
_ALGOLIA_MAX_SPLIT_DEPTH = 30
_ALGOLIA_REQUEST_TIMEOUT = 20
_ENRICH_UPDATE_BATCH = 2000
ENRICH_WORKERS = 6


def _algolia_query(filters: str, page: int) -> typing.Dict:
    body = {
        "query": "", "hitsPerPage": 1000, "page": page, "filters": filters,
        "attributesToRetrieve": _ALGOLIA_ATTRS,
    }
    headers = {
        "X-Algolia-API-Key": _ALGOLIA_SEARCH_API_KEY,
        "X-Algolia-Application-Id": _ALGOLIA_APP_ID,
        "Content-Type": "application/json",
    }
    r = requests.post(_ALGOLIA_QUERY_URL, headers=headers, json=body, timeout=_ALGOLIA_REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()


def _algolia_fetch_all(filters: str) -> typing.Optional[typing.List[typing.Dict]]:
    """Returns None (capped, caller must split) or the full hit list for this filter."""
    hits: typing.List[typing.Dict] = []
    page = 0
    while True:
        data = _algolia_query(filters, page)
        if "message" in data and "1000 hits" in data.get("message", ""):
            return None
        page_hits = data.get("hits", [])
        hits.extend(page_hits)
        if len(page_hits) < 1000:
            return hits
        page += 1


def _inv_filter(aaia_code: str, lo: int, hi: typing.Optional[int]) -> str:
    base = "aaia_brand_code:{}".format(aaia_code)
    if hi is None:
        return "{} AND {}.inventory >= {}".format(base, _ALGOLIA_MEMBER, lo)
    return "{} AND {}.inventory >= {} AND {}.inventory <= {}".format(base, _ALGOLIA_MEMBER, lo, _ALGOLIA_MEMBER, hi)


def _price_filter(aaia_code: str, exact_inv: int, plo: float, phi: typing.Optional[float]) -> str:
    base = "aaia_brand_code:{} AND {}.inventory = {}".format(aaia_code, _ALGOLIA_MEMBER, exact_inv)
    if phi is None:
        return "{} AND price >= {}".format(base, plo)
    return "{} AND price >= {} AND price <= {}".format(base, plo, phi)


def _fetch_price_bucket(aaia_code: str, exact_inv: int, plo: float, phi: typing.Optional[float], depth: int = 0) -> typing.List[typing.Dict]:
    result = _algolia_fetch_all(_price_filter(aaia_code, exact_inv, plo, phi))
    if result is not None:
        return result
    if depth >= _ALGOLIA_MAX_SPLIT_DEPTH or (phi is not None and phi - plo < 0.01):
        logger.warning(
            "{} Degenerate price bucket for {} inventory={} [{}, {}] -- results incomplete.".format(
                _LOG_PREFIX, aaia_code, exact_inv, plo, phi
            )
        )
        return []
    mid = plo + (phi - plo) / 2 if phi is not None else plo + 1_000_000
    return _fetch_price_bucket(aaia_code, exact_inv, plo, mid, depth + 1) + \
        _fetch_price_bucket(aaia_code, exact_inv, mid + 0.01, phi, depth + 1)


def _fetch_bucket(aaia_code: str, lo: int, hi: typing.Optional[int], depth: int = 0) -> typing.List[typing.Dict]:
    result = _algolia_fetch_all(_inv_filter(aaia_code, lo, hi))
    if result is not None:
        return result
    if lo == hi:
        return _fetch_price_bucket(aaia_code, lo, 0.0, None)
    if depth >= _ALGOLIA_MAX_SPLIT_DEPTH:
        logger.warning("{} Degenerate inventory bucket for {} [{}, {}] -- results incomplete.".format(_LOG_PREFIX, aaia_code, lo, hi))
        return []
    mid = lo + (hi - lo) // 2 if hi is not None else lo + 1_000_000
    if mid <= lo:
        mid = lo
    left = _fetch_bucket(aaia_code, lo, mid, depth + 1)
    right = _fetch_bucket(aaia_code, min(mid + 1, hi) if hi is not None else mid + 1, hi, depth + 1)
    return left + right


def enrich_allpro_parts_from_algolia(aaia_codes: typing.Optional[typing.List[str]] = None) -> int:
    """
    Backfills AllProParts.description/image_url/category/subcategory/series from the shared
    Algolia catalog, matched by (brand.aaia_code, part_number). ``aaia_codes`` restricts to a
    subset (default: every mapped AllProBrand -- unmapped brands aren't worth enriching since
    they'll never reach MasterPart anyway). Returns rows updated.

    Per explicit instruction: image_url written here is for AllProParts/display only --
    master_parts.py's AllPro ingest must never read it onto MasterPart/ProviderPart.
    """
    qs = src_models.AllProBrand.objects.filter(brand_mappings__isnull=False).distinct()
    if aaia_codes:
        qs = qs.filter(aaia_code__in=[c.upper() for c in aaia_codes])
    brands = list(qs)
    if not brands:
        logger.info("{} No mapped AllPro brands to enrich.".format(_LOG_PREFIX))
        return 0

    def _enrich_one_brand(brand: src_models.AllProBrand) -> int:
        hits = _fetch_bucket(brand.aaia_code, 0, None)
        if not hits:
            return 0
        by_part_number = {}
        for h in hits:
            pn = (h.get("part") or "").strip()
            if pn:
                by_part_number[pn.upper()] = h

        rows = list(
            src_models.AllProParts.objects.filter(brand=brand).values_list("id", "part_number")
        )
        now = timezone.now()
        updates = []
        for part_id, part_number in rows:
            hit = by_part_number.get(part_number.upper())
            if not hit:
                continue
            apd = hit.get("APD") or {}
            pace = hit.get("pace") or {}
            aw = hit.get("aw") or {}
            description = hit.get("ext") or pace.get("name") or aw.get("name") or hit.get("name")
            image_url = pace.get("image") or aw.get("image")
            updates.append(
                src_models.AllProParts(
                    id=part_id,
                    description=description,
                    image_url=image_url,
                    category=apd.get("category"),
                    subcategory=apd.get("subcategory"),
                    series=apd.get("series"),
                    enriched_at=now,
                )
            )
        for start in range(0, len(updates), _ENRICH_UPDATE_BATCH):
            src_models.AllProParts.objects.bulk_update(
                updates[start:start + _ENRICH_UPDATE_BATCH],
                ["description", "image_url", "category", "subcategory", "series", "enriched_at"],
                batch_size=2000,
            )
        return len(updates)

    total = 0
    with ThreadPoolExecutor(max_workers=ENRICH_WORKERS) as executor:
        futures = {executor.submit(_enrich_one_brand, b): b for b in brands}
        for future in as_completed(futures):
            brand = futures[future]
            try:
                n = future.result()
                total += n
                logger.info("{} Enriched {} AllProParts for brand {} ({}).".format(_LOG_PREFIX, n, brand.name, brand.aaia_code))
            except Exception as e:  # noqa: BLE001
                logger.error("{} Enrichment failed for brand {} ({}): {}".format(_LOG_PREFIX, brand.name, brand.aaia_code, e))

    logger.info("{} Enrichment complete: {} AllProParts rows updated across {} brands.".format(_LOG_PREFIX, total, len(brands)))
    return total
