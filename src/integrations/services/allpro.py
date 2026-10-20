"""
AllPro Distributing integration services.

Unlike Tri-State, AllPro is a genuine bulk-feed distributor (FTP, plain username/password per
dealer) -- see src.integrations.clients.allpro.client's module docstring for the three real
files and their confirmed-live shapes/sizes. Four jobs, run in this order:

  1. ``fetch_and_save_allpro_brands`` -- upsert AllProBrand from APDLineCodes.txt. Prerequisite
     for catalog ingestion (AllProParts.brand is a FK to AllProBrand, not a raw AAIA string).

  2. ``sync_unmapped_allpro_brands_to_brands`` -- map AllProBrand -> Brands: exact AAIA code,
     then compact-key, then fuzzy word-prefix match, otherwise create a new Brands row -- same
     technique as Keystone/DLG/Rough Country (see src.integrations.utils.brand_matching).
     Confirmed live 2026-10-20 that exact-AAIA-only (the original, more conservative approach)
     left 80% of AllPro's own catalog unmapped despite most of those brands already existing in
     Brands under a different code or name variant.

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
     Writes AllProParts.description/image_url/category/subcategory/series. image_url here is
     still never read by master_parts.py's regular AllPro ingest (_ingest_allpro_parts_for_mapped_brands
     passes image_url=None on purpose) -- see job 6 for the one place it is deliberately read.

  6. ``backfill_allpro_images_onto_master_parts`` -- fill-only backfill of the image_url job 5
     wrote onto MasterPart.image_url/MasterPartData.images, for parts that don't already have a
     real image from another source. Per explicit instruction 2026-10-20, reverses this module's
     earlier rule that AllPro images must never reach MasterPart/ProviderPart -- run by hand,
     after job 5, not wired into the regular ingest.
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
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

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
    """
    Map AllProBrand -> Brands: exact AAIA code, then compact-key (punctuation/spacing-only
    differences, e.g. 'BIG COUNTRY TRUCK ACCESSORIES' feed name vs catalog row 'BIG COUNTRY'
    caught by the earlier fuzzy phase instead -- compact-key specifically catches things like
    'AUTOMETER' vs 'AUTO METER'), then fuzzy word-prefix match with truncation on either side
    (e.g. 'Big Country' vs 'BIG COUNTRY TRUCK ACCESSORIES'). Otherwise creates a new Brands row.
    Same technique as Keystone/DLG/Rough Country -- see src.integrations.utils.brand_matching.

    Confirmed live 2026-10-20: 30 of AllPro's 589 previously-"unmapped" brands (80% of its
    catalog) were never actually missing from Brands at all -- exact-AAIA-only matching just
    never found them (e.g. AllPro's "BDS Suspension" vs catalog's bare "BDS", "RDS Manufacturing"
    vs "RDS MFG INC" -- an abbreviation difference no algorithm catches, added by hand once).

    Backfills Brands.aaia_code when a compact/fuzzy match's own code is still null -- AllPro's
    code is as good a source as any other distributor's for a brand nobody has coded yet, same
    self-healing precedent as TSEBrand.warehouse_code. Never overwrites an existing code: several
    of AllPro's own codes disagree with an already-coded Brands row (its own feed isn't always in
    sync with AAIA's canonical list), and that existing code is presumably already relied on by
    other distributors' own AAIA-code matching.

    Also upserts BrandProviders(brand, AllPro) -- generic "this provider carries this brand"
    bookkeeping, same role as Keystone/DLG's own BrandProviders upsert, independent of which
    companies are actually connected (AllPro already has real company connections, unlike
    Keystone/DLG's TICK_PERFORMANCE catalog-only placeholder, so there's no CompanyBrands step
    here).

    Returns the number of AllProBrand rows newly mapped (matched or created).
    """
    logger.info(
        "{} Syncing unmapped AllPro brands to Brands (code, compact, fuzzy, else create).".format(_LOG_PREFIX)
    )

    allpro_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ALLPRO_DISTRIBUTING.value,
    ).first()
    if not allpro_provider:
        logger.warning("{} AllPro provider not found. Skipping sync.".format(_LOG_PREFIX))
        return 0

    mapped_ids = set(
        src_models.BrandAllProBrandMapping.objects.values_list("allpro_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.AllProBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped AllPro brands. Nothing to sync.".format(_LOG_PREFIX))
        return 0

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}
    backfill_aaia: typing.Dict[int, str] = {}  # brand_id -> aaia_code, only when brand's own is null

    # Phase 1 -- exact AAIA code.
    aaia_codes = {ab.aaia_code.strip().upper() for ab in unmapped if (ab.aaia_code or "").strip()}
    brand_by_aaia: typing.Dict[str, src_models.Brands] = {}
    if aaia_codes:
        for b in src_models.Brands.objects.filter(aaia_code__in=aaia_codes).order_by("id"):
            key = (b.aaia_code or "").strip().upper()
            if key and key not in brand_by_aaia:
                brand_by_aaia[key] = b
    for ab in unmapped:
        code = (ab.aaia_code or "").strip().upper()
        if code:
            brand = brand_by_aaia.get(code)
            if brand:
                resolved_by_id[ab.id] = brand

    # Phase 2 -- compact key (punctuation/spacing-only differences).
    still_unresolved = [ab for ab in unmapped if ab.id not in resolved_by_id]
    compact_matches = 0
    if still_unresolved:
        compact_index = brands_by_compact_key()
        for ab in still_unresolved:
            key = normalize_compact_key(ab.name or "")
            if not key:
                continue
            brand = compact_index.get(key)
            if brand:
                resolved_by_id[ab.id] = brand
                compact_matches += 1
                if not brand.aaia_code and (ab.aaia_code or "").strip():
                    backfill_aaia[brand.id] = ab.aaia_code.strip().upper()

    # Phase 3 -- fuzzy word-prefix match.
    still_unresolved = [ab for ab in unmapped if ab.id not in resolved_by_id]
    fuzzy_matches = 0
    if still_unresolved:
        brands_first_index = brands_by_first_token_upper()
        all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
        for ab in still_unresolved:
            parts = normalize_upper_words(ab.name or "").split()
            candidates: typing.List[src_models.Brands] = (
                list(brands_first_index.get(parts[0], ())) if parts else []
            )
            if not candidates:
                if all_brands_fallback is None:
                    all_brands_fallback = list(
                        src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                    )
                candidates = all_brands_fallback
            brand = best_fuzzy_brand_match(ab.name or "", candidates)
            if brand:
                resolved_by_id[ab.id] = brand
                fuzzy_matches += 1
                if not brand.aaia_code and (ab.aaia_code or "").strip():
                    backfill_aaia[brand.id] = ab.aaia_code.strip().upper()
                logger.debug(
                    "{} Fuzzy-matched AllPro {!r} to Brand id={} name={!r}.".format(
                        _LOG_PREFIX, ab.name, brand.id, brand.name
                    )
                )

    # Phase 4 -- create a new Brands row for anything still unresolved.
    still_unresolved = [ab for ab in unmapped if ab.id not in resolved_by_id]
    created_brands = 0
    if still_unresolved:
        new_name_to_aaia: typing.Dict[str, typing.Optional[str]] = {}
        for ab in sorted(still_unresolved, key=lambda x: x.id):
            name_upper = (ab.name or "").strip().upper()
            if not name_upper or name_upper in new_name_to_aaia:
                continue
            new_name_to_aaia[name_upper] = (ab.aaia_code or "").strip().upper() or None

        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_name_to_aaia.keys())).values_list(
                "name", flat=True
            )
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=aaia_val,
            )
            for name, aaia_val in new_name_to_aaia.items()
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)

        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_name_to_aaia.keys()))
        }
        for ab in still_unresolved:
            name_upper = (ab.name or "").strip().upper()
            brand = by_name.get(name_upper)
            if brand:
                resolved_by_id[ab.id] = brand

    if backfill_aaia:
        src_models.Brands.objects.bulk_update(
            [src_models.Brands(id=bid, aaia_code=code) for bid, code in backfill_aaia.items()],
            ["aaia_code"],
        )

    new_mappings = [
        src_models.BrandAllProBrandMapping(brand=resolved_by_id[ab.id], allpro_brand=ab)
        for ab in unmapped
        if ab.id in resolved_by_id
    ]
    if new_mappings:
        pgbulk.upsert(
            src_models.BrandAllProBrandMapping,
            new_mappings,
            unique_fields=["brand", "allpro_brand"],
            update_fields=[],
        )

    brand_ids = {resolved_by_id[ab.id].id for ab in unmapped if ab.id in resolved_by_id}
    existing_bp_ids = set(
        src_models.BrandProviders.objects.filter(
            provider=allpro_provider, brand_id__in=brand_ids,
        ).values_list("brand_id", flat=True)
    )
    bp_to_create = [
        src_models.BrandProviders(brand_id=bid, provider_id=allpro_provider.id)
        for bid in brand_ids
        if bid not in existing_bp_ids
    ]
    if bp_to_create:
        src_models.BrandProviders.objects.bulk_create(bp_to_create, ignore_conflicts=True)

    unresolved = len(unmapped) - len(resolved_by_id)
    logger.info(
        "{} Sync complete. {}/{} unmapped AllPro brands resolved (aaia: {}, compact: {}, fuzzy: {}, "
        "created: {}); {} unresolved; {} Brands.aaia_code backfilled; {} BrandProviders created.".format(
            _LOG_PREFIX, len(resolved_by_id), len(unmapped),
            len(resolved_by_id) - compact_matches - fuzzy_matches - created_brands,
            compact_matches, fuzzy_matches, created_brands, unresolved,
            len(backfill_aaia), len(bp_to_create),
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


_IMAGE_BACKFILL_BATCH = 2000
_IMAGE_PLACEHOLDER_VALUES = frozenset({"na", "n/a", "none", "null", "unknown", "-", "--", "tbd"})


def _is_blank_image(value: typing.Optional[str]) -> bool:
    """Same placeholder rule as master_part_enrichment._PLACEHOLDER_VALUES -- 'NA' etc. count as
    no image, same as a blank/null one."""
    if not value:
        return True
    text = value.strip()
    return not text or text.casefold() in _IMAGE_PLACEHOLDER_VALUES


def backfill_allpro_images_onto_master_parts() -> typing.Tuple[int, int, int]:
    """
    Fill-only backfill of AllProParts.image_url (itself backfilled from Algolia -- see
    enrich_allpro_parts_from_algolia) onto MasterPart.image_url and MasterPartData.images, for
    AllPro-mapped parts that don't already carry a real image from another source.

    Per explicit instruction 2026-10-20, this reverses this module's own earlier rule that AllPro
    images must never reach MasterPart/ProviderPart. Run by hand after enrichment -- not wired
    into fetch_and_save_allpro_catalog or master_parts.py's regular ingest, which still never
    passes image_url automatically.

    Fill-only, same rule as backfill_tdg_images: never overwrites an existing non-placeholder
    image on either MasterPart or MasterPartData -- AllPro's Algolia-sourced images aren't a
    better source than whatever is already there, just a fallback for parts with nothing. Unlike
    TDG's backfill, a MasterPartData row is created (not skipped) when none exists -- AllPro has
    a real Providers row to attribute provenance to, unlike TDG.

    Returns (candidates with a usable AllPro image, MasterPart.image_url rows filled,
    MasterPartData rows filled-or-created).
    """
    allpro_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ALLPRO_DISTRIBUTING.value,
    ).first()
    if not allpro_provider:
        logger.warning("{} AllPro provider not found. Skipping image backfill.".format(_LOG_PREFIX))
        return 0, 0, 0

    allpro_brand_to_brand_id = {
        m.allpro_brand_id: m.brand_id for m in src_models.BrandAllProBrandMapping.objects.all()
    }
    if not allpro_brand_to_brand_id:
        logger.info("{} No BrandAllProBrandMapping rows. Nothing to backfill.".format(_LOG_PREFIX))
        return 0, 0, 0

    candidates = 0
    image_filled = 0
    data_filled_or_created = 0
    part_updates: typing.List[src_models.MasterPart] = []
    data_updates: typing.List[src_models.MasterPartData] = []
    data_creates: typing.List[src_models.MasterPartData] = []
    now = timezone.now()

    def _flush() -> None:
        nonlocal part_updates, data_updates, data_creates
        if part_updates:
            src_models.MasterPart.objects.bulk_update(part_updates, ["image_url", "updated_at"], batch_size=500)
            part_updates = []
        if data_updates:
            src_models.MasterPartData.objects.bulk_update(data_updates, ["images", "updated_at"], batch_size=500)
            data_updates = []
        if data_creates:
            pgbulk.upsert(
                src_models.MasterPartData, data_creates, unique_fields=["master_part"], update_fields=[],
            )
            data_creates = []

    batch_num = 0
    last_id = 0
    while True:
        batch_num += 1
        batch = list(
            src_models.AllProParts.objects.exclude(image_url__isnull=True)
            .exclude(image_url="")
            .filter(id__gt=last_id)
            .order_by("id")
            .values("id", "brand_id", "part_number", "image_url", "full_part_number")[:_IMAGE_BACKFILL_BATCH]
        )
        if not batch:
            break
        last_id = batch[-1]["id"]

        by_key: typing.Dict[typing.Tuple[int, str], typing.Dict] = {}
        for row in batch:
            brand_id = allpro_brand_to_brand_id.get(row["brand_id"])
            if not brand_id or _is_blank_image(row["image_url"]):
                continue
            pn = (row["part_number"] or "").strip()
            if pn:
                by_key[(brand_id, pn)] = row
        if not by_key:
            continue
        candidates += len(by_key)

        brand_ids = {k[0] for k in by_key}
        part_numbers = {k[1] for k in by_key}
        master_parts = list(
            src_models.MasterPart.objects.filter(
                brand_id__in=brand_ids, part_number__in=part_numbers,
            ).values("id", "brand_id", "part_number", "image_url")
        )
        mp_by_key = {
            (mp["brand_id"], mp["part_number"]): mp
            for mp in master_parts
            if (mp["brand_id"], mp["part_number"]) in by_key
        }
        mp_ids = [mp["id"] for mp in mp_by_key.values()]
        existing_data = {
            d.master_part_id: d
            for d in src_models.MasterPartData.objects.filter(master_part_id__in=mp_ids)
        }

        for key, row in by_key.items():
            mp = mp_by_key.get(key)
            if not mp:
                continue
            url = row["image_url"].strip()

            if _is_blank_image(mp["image_url"]):
                part_updates.append(src_models.MasterPart(id=mp["id"], image_url=url, updated_at=now))
                image_filled += 1

            data = existing_data.get(mp["id"])
            if data is None:
                data_creates.append(
                    src_models.MasterPartData(
                        master_part_id=mp["id"],
                        images=[url],
                        source_provider=allpro_provider,
                        source_external_id=row["full_part_number"],
                    )
                )
                data_filled_or_created += 1
            elif not data.images:
                data.images = [url]
                data.updated_at = now
                data_updates.append(data)
                data_filled_or_created += 1

        _flush()
        logger.info(
            "{} Image backfill batch {}: {} candidates, {} MasterPart.image_url filled, "
            "{} MasterPartData filled/created so far.".format(
                _LOG_PREFIX, batch_num, candidates, image_filled, data_filled_or_created
            )
        )

    _flush()
    logger.info(
        "{} Image backfill complete: {} candidates, {} MasterPart.image_url filled, "
        "{} MasterPartData filled/created.".format(
            _LOG_PREFIX, candidates, image_filled, data_filled_or_created
        )
    )
    return candidates, image_filled, data_filled_or_created
