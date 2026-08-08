"""
Helmet House raw-feed ingest.

One flat file (masterv.csv, ~41k rows, rewritten daily) carries the whole catalog, so the ingest
is a single download and two upserts -- no pagination, no per-part hydrate, no incremental cursor.
Structurally this follows Quadratec (flat file, per-warehouse qty stored on the part, prices split
out per company) rather than WPS or Motor State.

Flow:
  * ``fetch_and_save_helmet_house_catalog``  -- download + upsert HelmetHouseBrand and
        HelmetHousePart (catalog + Helmet House-wide West/East stock). Reads the primary
        connection's credentials, since the catalog is distributor-wide.
  * ``sync_unmapped_helmet_house_brands_to_brands`` -- HelmetHouseBrand -> Brands
  * ``sync_helmet_house_company_pricing_for_company_provider`` -- prices for one company

A note on "per company" here: Helmet House publishes one shared FTP login rather than issuing an
account per dealer, so every connection reads the same file and lands on the same dealer cost.
Pricing is still fetched and stored per company so the master pricing layer keys the same way as
every other provider, and so nothing downstream changes if per-dealer logins ever appear. Each
pricing run is one ~10 MB download, and the client reuses a local copy younger than its max age,
so a night of connections does not mean a download each.
"""
import logging
import typing
from decimal import Decimal, InvalidOperation

import pgbulk
from django.db.models.functions import Upper
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.helmet_house import client as helmet_house_client
from src.integrations.clients.helmet_house import exceptions as helmet_house_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[HELMET-HOUSE-SERVICES]"

HELMET_HOUSE_UPSERT_BATCH = 5000

_PART_UPDATE_FIELDS = [
    "alt_part_number", "vendor_part_number", "description", "long_description", "upc",
    "status", "category", "product_class", "size", "color", "model", "country_of_origin",
    "weight", "length", "width", "depth", "photo_filename", "alt_photo_filenames",
    "west_qty", "east_qty", "total_qty", "has_map_policy", "source_filename", "raw_data",
    "updated_at",
]
_COMPANY_PRICING_UPDATE_FIELDS = [
    "dealer_price", "retail_price", "map_price", "has_map_policy", "updated_at",
]

# Feed statuses that mean the part can still be bought. "Out of Stock" is Helmet House's own
# wording for a temporary outage and "Discontinued" for never again (README.txt); neither is
# filtered out of the catalog -- both are recorded so the status shows on the part -- but only
# these two count as sellable.
SELLABLE_STATUSES = frozenset({"OK", "ON SALE"})


# --------------------------------------------------------------------------------------
# Value coercion
# --------------------------------------------------------------------------------------
def _safe_str(value: typing.Any, max_len: typing.Optional[int] = None) -> typing.Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None:
        return None
    s = str(value).strip().replace("$", "").replace(",", "")
    if not s:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _safe_float(value: typing.Any) -> typing.Optional[float]:
    s = _safe_str(value)
    if s is None:
        return None
    try:
        return float(s.replace(",", ""))
    except (TypeError, ValueError):
        return None


def _safe_int(value: typing.Any) -> int:
    """Quantity columns; anything unparseable is 0 rather than None so stock maths stays simple."""
    s = _safe_str(value)
    if s is None:
        return 0
    try:
        return int(float(s.replace(",", "")))
    except (TypeError, ValueError):
        return 0


def _is_yes(value: typing.Any) -> bool:
    return (_safe_str(value) or "").upper().startswith("Y")


def _split_alt_photos(value: typing.Any) -> typing.Optional[typing.List[str]]:
    """The feed packs alternate photo filenames into one space-separated cell."""
    s = _safe_str(value)
    if not s:
        return None
    names = [part for part in s.split() if part]
    return names or None


def normalize_brand_name(raw: typing.Optional[str]) -> str:
    """
    Feed ``Brand`` value -> the uppercase name we resolve against Brands, applying
    HELMET_HOUSE_BRAND_ALIASES (T/M -> TOURMASTER, MISC/BAGS -> the house brand). A row with no
    brand at all also lands on the house brand rather than being dropped.
    """
    name = (raw or "").strip().upper()
    if not name:
        return src_constants.HELMET_HOUSE_HOUSE_BRAND_NAME
    return src_constants.HELMET_HOUSE_BRAND_ALIASES.get(name, name)


def _brand_name_upper_for_sync(hh_brand: src_models.HelmetHouseBrand) -> str:
    name_upper = (hh_brand.name or "").strip().upper()
    return name_upper or "BRAND_{}".format(hh_brand.external_id)


# --------------------------------------------------------------------------------------
# Connection / client resolution
# --------------------------------------------------------------------------------------
def _active_helmet_house_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.HELMHOUSE.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _catalog_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """Primary Helmet House connection for the shared catalog; else first active by id."""
    base = _active_helmet_house_company_providers_queryset()
    primary = base.filter(primary=True).first()
    if primary:
        return primary
    fallback = base.order_by("id").first()
    if fallback:
        logger.info(
            "{} No primary Helmet House connection; using company_id={} for the catalog.".format(
                _LOG_PREFIX, fallback.company_id
            )
        )
    return fallback


def _build_client(
    cp: src_models.CompanyProviders,
    local_file_path: typing.Optional[str] = None,
) -> helmet_house_client.HelmetHouseFTPClient:
    return helmet_house_client.HelmetHouseFTPClient(
        credentials=credentials_helper.get_feed_credentials(cp),
        local_file_path=local_file_path,
    )


# --------------------------------------------------------------------------------------
# Catalog ingest (HelmetHouseBrand + HelmetHousePart)
# --------------------------------------------------------------------------------------
def _ensure_helmet_house_brands(
    rows: typing.Sequence[typing.Dict[str, str]],
) -> typing.Dict[str, src_models.HelmetHouseBrand]:
    """
    Ensure a HelmetHouseBrand exists for every distinct normalised brand in the feed, keyed by
    normalised name. ``source_name`` records one of the raw spellings that mapped to it.
    """
    source_by_normalized: typing.Dict[str, str] = {}
    for row in rows:
        raw = _safe_str(row.get("Brand")) or ""
        source_by_normalized.setdefault(normalize_brand_name(raw), raw or None)
    # Always present so a row with an unusable brand still has somewhere to go.
    source_by_normalized.setdefault(src_constants.HELMET_HOUSE_HOUSE_BRAND_NAME, None)

    existing = set(
        src_models.HelmetHouseBrand.objects.filter(
            external_id__in=list(source_by_normalized.keys())
        ).values_list("external_id", flat=True)
    )
    missing = [name for name in source_by_normalized if name not in existing]
    if missing:
        src_models.HelmetHouseBrand.objects.bulk_create(
            [
                src_models.HelmetHouseBrand(
                    external_id=name,
                    name=name,
                    source_name=_safe_str(source_by_normalized.get(name), 255),
                )
                for name in missing
            ],
            ignore_conflicts=True,
        )
        logger.info("{} Created {} new HelmetHouseBrand rows.".format(_LOG_PREFIX, len(missing)))

    out = {
        b.external_id: b
        for b in src_models.HelmetHouseBrand.objects.filter(
            external_id__in=list(source_by_normalized.keys())
        )
    }
    logger.info("{} Using {} Helmet House brands.".format(_LOG_PREFIX, len(out)))
    return out


def _build_part(
    row: typing.Dict[str, str],
    brand: src_models.HelmetHouseBrand,
    source_filename: typing.Optional[str],
) -> src_models.HelmetHousePart:
    """Build a HelmetHousePart from one feed row. Prices are deliberately not read here."""
    return src_models.HelmetHousePart(
        brand=brand,
        sku=(_safe_str(row.get("Part Number")) or "")[:255],
        alt_part_number=_safe_str(row.get("Alt Part#"), 255),
        vendor_part_number=_safe_str(row.get("Vendor P/N"), 255),
        description=_safe_str(row.get("Description"), 512),
        long_description=_safe_str(row.get("Long Description")),
        upc=_safe_str(row.get("UPC"), 64),
        status=_safe_str(row.get("Status"), 32),
        category=_safe_str(row.get("Category"), 255),
        product_class=_safe_str(row.get("Class"), 255),
        size=_safe_str(row.get("Size"), 64),
        color=_safe_str(row.get("Color"), 128),
        model=_safe_str(row.get("Model"), 255),
        country_of_origin=_safe_str(row.get("Origin"), 16),
        weight=_safe_float(row.get("Weight")),
        length=_safe_float(row.get("Length")),
        width=_safe_float(row.get("Width")),
        depth=_safe_float(row.get("Depth")),
        photo_filename=_safe_str(row.get("Photo"), 255),
        alt_photo_filenames=_split_alt_photos(row.get("Alt Photos")),
        west_qty=_safe_int(row.get("West")),
        east_qty=_safe_int(row.get("East")),
        total_qty=_safe_int(row.get("TTL Qty")),
        has_map_policy=_is_yes(row.get("MAPP Y/N")),
        source_filename=source_filename,
        raw_data=row,
    )


def fetch_and_save_helmet_house_catalog(
    force_download: bool = True,
    local_file_path: typing.Optional[str] = None,
) -> None:
    """
    Download the Helmet House catalog and upsert HelmetHouseBrand + HelmetHousePart (catalog +
    Helmet House-wide West/East stock). Uses the primary connection's credentials (or the first
    active one) since the catalog is distributor-wide. Per-company pricing
    (HelmetHouseCompanyPricing) is handled separately by IntegrationPricingSyncJob.
    """
    logger.info("{} Starting Helmet House catalog sync.".format(_LOG_PREFIX))

    cp = _catalog_company_provider()
    if not cp:
        logger.info("{} No active Helmet House connection found. Nothing to sync.".format(_LOG_PREFIX))
        return

    logger.info(
        "{} Catalog feed using company_id={} (primary={}).".format(
            _LOG_PREFIX, cp.company_id, cp.primary
        )
    )
    try:
        client = _build_client(cp, local_file_path=local_file_path)
        rows = client.get_catalog_records(force_download=force_download)
    except (helmet_house_exceptions.HelmetHouseException, ValueError) as e:
        logger.error("{} Feed error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    rows = [row for row in rows if _safe_str(row.get("Part Number"))]
    if not rows:
        logger.warning("{} No Helmet House rows parsed from the feed.".format(_LOG_PREFIX))
        return

    brands_by_name = _ensure_helmet_house_brands(rows)
    house_brand = brands_by_name[src_constants.HELMET_HOUSE_HOUSE_BRAND_NAME]
    source_filename = client.downloaded_filename

    # The feed's Part Number column is unique across the whole file, but dedupe on
    # (brand, sku) anyway: pgbulk raises on a batch containing the same conflict target twice,
    # so one repeated row in a bad drop would fail the entire sync. Last row wins.
    parts_by_key: typing.Dict[typing.Tuple[int, str], src_models.HelmetHousePart] = {}
    for row in rows:
        brand = brands_by_name.get(normalize_brand_name(row.get("Brand"))) or house_brand
        part = _build_part(row, brand, source_filename)
        if not part.sku:
            continue
        parts_by_key[(brand.id, part.sku)] = part

    part_instances = list(parts_by_key.values())
    duplicates = len(rows) - len(part_instances)
    if duplicates > 0:
        logger.warning(
            "{} Feed carried {} duplicate (brand, part number) row(s); kept the last of each.".format(
                _LOG_PREFIX, duplicates
            )
        )

    now = timezone.now()
    total = 0
    for start in range(0, len(part_instances), HELMET_HOUSE_UPSERT_BATCH):
        batch = part_instances[start:start + HELMET_HOUSE_UPSERT_BATCH]
        for part in batch:
            part.updated_at = now
        pgbulk.upsert(
            src_models.HelmetHousePart,
            batch,
            unique_fields=["brand", "sku"],
            update_fields=_PART_UPDATE_FIELDS,
            returning=False,
        )
        total += len(batch)
        logger.info(
            "{} Upserted {}/{} Helmet House parts.".format(_LOG_PREFIX, total, len(part_instances))
        )

    logger.info(
        "{} Helmet House catalog sync complete ({} parts from {}).".format(
            _LOG_PREFIX, total, source_filename or "the feed"
        )
    )


# --------------------------------------------------------------------------------------
# Brand mapping (HelmetHouseBrand -> Brands)
# --------------------------------------------------------------------------------------
def sync_unmapped_helmet_house_brands_to_brands() -> typing.List[src_models.HelmetHouseBrand]:
    """
    For each HelmetHouseBrand without a BrandHelmetHouseBrandMapping: resolve Brand by exact name
    (uppercase), then compact-key, then fuzzy word-prefix; otherwise create it. Upserts
    BrandHelmetHouseBrandMapping, BrandProviders and CompanyBrands for TICK_PERFORMANCE. The feed
    carries no AAIA code, so matching is name-based only (same as Quadratec, Motor State and WPS).
    """
    logger.info("{} Syncing unmapped Helmet House brands to Brands.".format(_LOG_PREFIX))

    provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.HELMHOUSE.value,
    ).first()
    if not provider:
        logger.warning("{} Helmet House provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandHelmetHouseBrandMapping.objects.values_list(
            "helmet_house_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(
        src_models.HelmetHouseBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped Helmet House brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped Helmet House brands.".format(_LOG_PREFIX, len(unmapped)))
    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 1: exact uppercase-name match.
    name_keys = {(hb.name or "").strip().upper() for hb in unmapped if (hb.name or "").strip()}
    by_upper: typing.Dict[str, src_models.Brands] = {}
    if name_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_keys)
            .order_by("id")
        ):
            by_upper.setdefault((b.name or "").strip().upper(), b)
    for hb in unmapped:
        nm = (hb.name or "").strip().upper()
        if nm and nm in by_upper:
            resolved_by_id[hb.id] = by_upper[nm]

    # Phase 2: compact-key (punctuation/spacing-insensitive).
    compact_matches = 0
    still = [hb for hb in unmapped if hb.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for hb in still:
            key = normalize_compact_key(hb.name or "")
            if key and key in compact_index:
                resolved_by_id[hb.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix. Brands in HELMET_HOUSE_EXACT_MATCH_ONLY_BRANDS opt out and fall
    # through to phase 4, because the shared matcher resolves them to an unrelated brand (see the
    # constant for the measured evidence).
    unresolved = [
        hb
        for hb in unmapped
        if hb.id not in resolved_by_id
        and (hb.name or "").strip().upper() not in src_constants.HELMET_HOUSE_EXACT_MATCH_ONLY_BRANDS
    ]
    skipped_fuzzy = [
        hb
        for hb in unmapped
        if hb.id not in resolved_by_id
        and (hb.name or "").strip().upper() in src_constants.HELMET_HOUSE_EXACT_MATCH_ONLY_BRANDS
    ]
    if skipped_fuzzy:
        logger.info(
            "{} Skipping fuzzy matching for {}; these will be created rather than risk a wrong "
            "match.".format(_LOG_PREFIX, ", ".join(sorted(hb.name for hb in skipped_fuzzy)))
        )
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for hb in unresolved:
        parts = normalize_upper_words(hb.name or "").split()
        candidates: typing.List[src_models.Brands] = (
            list(first_index.get(parts[0], ())) if parts else []
        )
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(hb.name or "", candidates)
        if brand:
            resolved_by_id[hb.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved.
    new_specs: typing.Dict[str, None] = {}
    for hb in unmapped:
        if hb.id not in resolved_by_id:
            new_specs.setdefault(_brand_name_upper_for_sync(hb), None)

    created_brands = 0
    if new_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_specs.keys())).values_list(
                "name", flat=True
            )
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for name in new_specs
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            b.name: b for b in src_models.Brands.objects.filter(name__in=list(new_specs.keys()))
        }
        for hb in unmapped:
            if hb.id not in resolved_by_id:
                resolved_by_id[hb.id] = by_name[_brand_name_upper_for_sync(hb)]

    mapping_models = [
        src_models.BrandHelmetHouseBrandMapping(
            brand_id=resolved_by_id[hb.id].id, helmet_house_brand_id=hb.id
        )
        for hb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandHelmetHouseBrandMapping,
        mapping_models,
        unique_fields=["brand", "helmet_house_brand"],
        update_fields=[],
        returning=False,
    )

    created_bp = created_cb = 0
    for hb in unmapped:
        brand = resolved_by_id[hb.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand, provider=provider
        )
        created_bp += int(bp_created)
        _, cb_created = src_models.CompanyBrands.objects.get_or_create(
            company=tick_company,
            brand=brand,
            defaults={
                "status": src_enums.CompanyBrandStatus.ACTIVE.value,
                "status_name": src_enums.CompanyBrandStatus.ACTIVE.name,
            },
        )
        created_cb += int(cb_created)

    logger.info(
        "{} Sync complete. Brands created: {}, compact: {}, fuzzy: {}, mappings: {}, "
        "BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX, created_brands, compact_matches, fuzzy_matches,
            len(mapping_models), created_bp, created_cb,
        )
    )
    return unmapped


# --------------------------------------------------------------------------------------
# Per-company pricing (HelmetHouseCompanyPricing)
# --------------------------------------------------------------------------------------
def sync_helmet_house_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Read the feed with this connection's own credentials and upsert HelmetHouseCompanyPricing.
    Keyed to existing HelmetHousePart rows by (brand, sku); rows missing from the catalog table
    are skipped, so ``fetch_and_save_helmet_house_catalog`` has to have run first.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.HELMHOUSE.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active Helmet House CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    if not src_models.HelmetHousePart.objects.exists():
        logger.warning(
            "{} No shared Helmet House catalog yet; run fetch_and_save_helmet_house_catalog "
            "before pricing company_provider_id={}.".format(_LOG_PREFIX, cp.id)
        )
        return

    try:
        client = _build_client(cp)
        # force_download=False: the catalog pass will normally have just pulled the same file, and
        # every connection reads that one shared file anyway, so re-downloading ~10 MB per company
        # buys nothing. The client's own max-age check still refreshes a stale copy.
        rows = client.get_catalog_records(force_download=False)
    except (helmet_house_exceptions.HelmetHouseException, ValueError) as e:
        logger.error(
            "{} Feed error for company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e))
        )
        raise

    # (brand external_id, sku) -> HelmetHousePart.id, so only catalog rows get priced.
    part_id_by_key: typing.Dict[typing.Tuple[str, str], int] = {}
    for row in src_models.HelmetHousePart.objects.values(
        "id", "sku", "brand__external_id"
    ).iterator(chunk_size=5000):
        part_id_by_key[(row["brand__external_id"], row["sku"])] = row["id"]

    now = timezone.now()
    pricing_by_part_id: typing.Dict[int, src_models.HelmetHouseCompanyPricing] = {}
    missing = 0
    for row in rows:
        sku = _safe_str(row.get("Part Number"))
        if not sku:
            continue
        brand_name = normalize_brand_name(row.get("Brand"))
        # Same fallback the catalog ingest applies when a brand row could not be resolved, so a
        # part filed under the house brand there is still found here rather than going unpriced.
        part_id = part_id_by_key.get((brand_name, sku)) or part_id_by_key.get(
            (src_constants.HELMET_HOUSE_HOUSE_BRAND_NAME, sku)
        )
        if not part_id:
            missing += 1
            continue
        has_map_policy = _is_yes(row.get("MAPP Y/N"))
        map_price = _safe_decimal(row.get("MAPP Price")) if has_map_policy else None
        pricing_by_part_id[part_id] = src_models.HelmetHouseCompanyPricing(
            part_id=part_id,
            company=cp.company,
            dealer_price=_safe_decimal(row.get("Dealer")),
            retail_price=_safe_decimal(row.get("Retail")),
            map_price=map_price,
            has_map_policy=has_map_policy,
            updated_at=now,
        )

    pricing_rows = list(pricing_by_part_id.values())
    total = 0
    for start in range(0, len(pricing_rows), HELMET_HOUSE_UPSERT_BATCH):
        batch = pricing_rows[start:start + HELMET_HOUSE_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.HelmetHouseCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=_COMPANY_PRICING_UPDATE_FIELDS,
            returning=False,
        )
        total += len(batch)

    logger.info(
        "{} Pricing done for company_provider_id={} (company_id={}): {} rows, {} feed row(s) "
        "not in the catalog table.".format(_LOG_PREFIX, cp.id, cp.company_id, total, missing)
    )


def fetch_and_save_helmet_house_full_sync(force_download: bool = True) -> None:
    """Catalog -> brand mapping. Per-company pricing stays with the pricing-job queue."""
    fetch_and_save_helmet_house_catalog(force_download=force_download)
    sync_unmapped_helmet_house_brands_to_brands()
