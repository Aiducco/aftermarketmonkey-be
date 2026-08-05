"""
Quadratec feed integration: two flat files joined on the Quadratec part number.

  * catalog feed  (pricingSheet_quad.xlsx): Quadratec PN, MPN, Description, UPC, Brand,
    Retail Price, Wholesale Price, Shipping Surcharge.
  * pricing/inventory feed (quadratec_wholesale.csv): Quadratec Part No, Part No, Description,
    Brand, Inventory PA1/PA2/NV1/Total, Cost, Surcharge, UPC, MAP.

Populates QuadratecBrand, QuadratecPart (catalog + distributor-wide per-warehouse inventory), and
QuadratecCompanyPricing (per company). There is no vehicle fitment in the feeds. Structurally this
follows A-Tech (flat file + per-warehouse qty stored on the part) and Vossen (no fitment), not
Rough Country. Per-company feed URLs live in CompanyProviders.credentials['feed'] as
catalog_feed_url / pricing_feed_url; the client currently reads two bundled static files.
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
from src.integrations.clients.quadratec import client as quadratec_client
from src.integrations.clients.quadratec import exceptions as quadratec_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[QUADRATEC-SERVICES]"

DEFAULT_QUADRATEC_BRAND_NAME = "QUADRATEC"

QUADRATEC_PARTS_UPSERT_BATCH = 5000

QUADRATEC_PART_UPDATE_FIELDS = [
    "brand",
    "mpn",
    "title",
    "description",
    "upc",
    "inv_pa1",
    "inv_pa2",
    "inv_nv1",
    "inv_total",
    "shipping_surcharge",
    "raw_data",
    "updated_at",
]


# --------------------------------------------------------------------------------------
# Value coercion helpers
# --------------------------------------------------------------------------------------
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


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def _safe_str(value: typing.Any, max_len: typing.Optional[int] = None) -> typing.Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s[:max_len] if max_len else s


def _normalize_quadratec_brand_external_id(raw: typing.Optional[str]) -> str:
    """Normalize a feed brand to QuadratecBrand.external_id / name (uppercase)."""
    s = (raw or "").strip().upper()
    return s if s else DEFAULT_QUADRATEC_BRAND_NAME


def _brand_name_upper_for_sync(q_brand: src_models.QuadratecBrand) -> str:
    name_upper = (q_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(q_brand.external_id)
    return name_upper


# --------------------------------------------------------------------------------------
# Client / connection helpers
# --------------------------------------------------------------------------------------
def _quadratec_feed_client_for_credentials(
    credentials: typing.Optional[typing.Dict],
    catalog_url_override: typing.Optional[str] = None,
    pricing_url_override: typing.Optional[str] = None,
) -> quadratec_client.QuadratecFeedClient:
    """Build a client from CompanyProviders.credentials (two feed URLs) plus optional overrides."""
    creds = credentials or {}
    catalog_url = catalog_url_override or creds.get(
        src_constants.QUADRATEC_CREDENTIALS_CATALOG_FEED_URL
    )
    pricing_url = pricing_url_override or creds.get(
        src_constants.QUADRATEC_CREDENTIALS_PRICING_FEED_URL
    )
    return quadratec_client.QuadratecFeedClient(
        catalog_feed_url=catalog_url,
        pricing_feed_url=pricing_url,
    )


def _active_quadratec_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.QUADRATEC.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _catalog_company_provider(
    quadratec_provider: typing.Optional[src_models.Providers],
) -> typing.Optional[src_models.CompanyProviders]:
    """Primary Quadratec connection for shared catalog; else first active by id."""
    if not quadratec_provider:
        return None
    base = _active_quadratec_company_providers_queryset().filter(provider=quadratec_provider)
    primary = base.filter(primary=True).first()
    if primary:
        return primary
    fallback = base.order_by("id").first()
    if fallback:
        logger.info(
            "{} No primary Quadratec company provider; using company_id={} for catalog.".format(
                _LOG_PREFIX, fallback.company_id,
            )
        )
    return fallback


# --------------------------------------------------------------------------------------
# Feed row joining
# --------------------------------------------------------------------------------------
def _combined_rows_by_pn(
    data: typing.Dict[str, typing.List[typing.Dict]],
) -> typing.Dict[str, typing.Dict[str, typing.Dict]]:
    """
    Merge the catalog and pricing/inventory rows into ``{quadratec_pn: {"catalog": {...},
    "inv": {...}}}``. Rows present in only one feed still ingest.
    """
    by_pn: typing.Dict[str, typing.Dict[str, typing.Dict]] = {}
    for row in data.get("catalog") or []:
        pn = (row.get("quadratec_pn") or "").strip()
        if pn:
            by_pn.setdefault(pn, {})["catalog"] = row
    for row in data.get("pricing_inventory") or []:
        pn = (row.get("quadratec_pn") or "").strip()
        if pn:
            by_pn.setdefault(pn, {})["inv"] = row
    return by_pn


def _row_brand_name(pair: typing.Dict[str, typing.Dict]) -> str:
    cat = pair.get("catalog") or {}
    inv = pair.get("inv") or {}
    return _normalize_quadratec_brand_external_id(cat.get("brand") or inv.get("brand"))


def _ensure_quadratec_brands(
    combined: typing.Dict[str, typing.Dict[str, typing.Dict]],
) -> typing.Dict[str, src_models.QuadratecBrand]:
    """
    Ensure a QuadratecBrand exists for every distinct feed brand (external_id/name uppercase).
    Bulk-creates missing rows in one pass rather than a round-trip per brand -- the Quadratec
    catalog spans ~1k+ brands.
    """
    brand_names = {DEFAULT_QUADRATEC_BRAND_NAME}
    for pair in combined.values():
        brand_names.add(_row_brand_name(pair))

    existing = {
        b.external_id: b
        for b in src_models.QuadratecBrand.objects.filter(external_id__in=list(brand_names))
    }
    missing = [name for name in brand_names if name not in existing]
    if missing:
        src_models.QuadratecBrand.objects.bulk_create(
            [src_models.QuadratecBrand(external_id=name, name=name, aaia_code=None) for name in missing],
            ignore_conflicts=True,
        )
        logger.info("{} Created {} new QuadratecBrand rows.".format(_LOG_PREFIX, len(missing)))

    out = {
        b.external_id: b
        for b in src_models.QuadratecBrand.objects.filter(external_id__in=list(brand_names))
    }
    logger.info("{} Using {} Quadratec brands.".format(_LOG_PREFIX, len(out)))
    return out


def _build_part(
    pn: str,
    pair: typing.Dict[str, typing.Dict],
    brand: src_models.QuadratecBrand,
) -> src_models.QuadratecPart:
    """Build a QuadratecPart (catalog + distributor-wide inventory; prices are per-company)."""
    cat = pair.get("catalog") or {}
    inv = pair.get("inv") or {}
    description = _safe_str(cat.get("description") or inv.get("description"))
    title = _safe_str(cat.get("description") or inv.get("description"), 512)
    return src_models.QuadratecPart(
        brand=brand,
        sku=pn[:255],
        mpn=_safe_str(cat.get("mpn") or inv.get("mpn"), 255),
        title=title,
        description=description,
        upc=_safe_str(cat.get("upc") or inv.get("upc"), 255),
        inv_pa1=_safe_int(inv.get("inv_pa1")),
        inv_pa2=_safe_int(inv.get("inv_pa2")),
        inv_nv1=_safe_int(inv.get("inv_nv1")),
        inv_total=_safe_int(inv.get("inv_total")),
        shipping_surcharge=_safe_decimal(cat.get("shipping_surcharge") or inv.get("surcharge")),
        raw_data={"catalog": cat, "inv": inv},
    )


# --------------------------------------------------------------------------------------
# Catalog ingest (QuadratecBrand + QuadratecPart)
# --------------------------------------------------------------------------------------
def fetch_and_save_quadratec(
    catalog_feed_url: typing.Optional[str] = None,
    pricing_feed_url: typing.Optional[str] = None,
) -> None:
    """
    Read both feeds and upsert QuadratecBrand + QuadratecPart (catalog + distributor-wide
    inventory). Uses the primary Quadratec CompanyProvider's credentials (or first active) for the
    shared catalog. Per-company pricing (QuadratecCompanyPricing) is handled separately by
    IntegrationPricingSyncJob (Phase 3) for each CompanyProvider.
    """
    logger.info("{} Starting Quadratec feed sync.".format(_LOG_PREFIX))

    quadratec_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.QUADRATEC.value,
    ).first()

    catalog_cp = _catalog_company_provider(quadratec_provider)
    catalog_creds = credentials_helper.get_feed_credentials(catalog_cp) if catalog_cp else {}
    if catalog_cp:
        logger.info(
            "{} Catalog feed using company_id={} (primary={}).".format(
                _LOG_PREFIX, catalog_cp.company_id, catalog_cp.primary,
            )
        )
    client = _quadratec_feed_client_for_credentials(
        catalog_creds, catalog_feed_url, pricing_feed_url
    )
    try:
        data = client.get_feed_data()
    except quadratec_exceptions.QuadratecException as e:
        logger.error("{} Feed error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    combined = _combined_rows_by_pn(data)
    if not combined:
        logger.warning("{} No Quadratec rows parsed from feeds.".format(_LOG_PREFIX))
        return

    brands_by_name = _ensure_quadratec_brands(combined)

    part_instances: typing.List[src_models.QuadratecPart] = []
    for pn, pair in combined.items():
        brand = brands_by_name.get(_row_brand_name(pair)) or brands_by_name[DEFAULT_QUADRATEC_BRAND_NAME]
        part_instances.append(_build_part(pn, pair, brand))

    total = 0
    now = timezone.now()
    for start in range(0, len(part_instances), QUADRATEC_PARTS_UPSERT_BATCH):
        batch = part_instances[start:start + QUADRATEC_PARTS_UPSERT_BATCH]
        for p in batch:
            p.updated_at = now
        pgbulk.upsert(
            src_models.QuadratecPart,
            batch,
            unique_fields=["brand", "sku"],
            update_fields=QUADRATEC_PART_UPDATE_FIELDS,
            returning=False,
        )
        total += len(batch)
        logger.info("{} Upserted {}/{} Quadratec parts.".format(_LOG_PREFIX, total, len(part_instances)))

    logger.info("{} Quadratec feed sync complete ({} parts).".format(_LOG_PREFIX, total))


# --------------------------------------------------------------------------------------
# Brand mapping (QuadratecBrand -> Brands)
# --------------------------------------------------------------------------------------
def sync_unmapped_quadratec_brands_to_brands() -> typing.List[src_models.QuadratecBrand]:
    """
    For each QuadratecBrand without a BrandQuadratecBrandMapping: resolve Brand by exact name
    (uppercase), then compact-key, then fuzzy word-prefix match (same rules as other providers);
    otherwise create it. Upserts BrandQuadratecBrandMapping, BrandProviders, and CompanyBrands for
    TICK_PERFORMANCE. Quadratec feeds carry no AAIA codes, so matching is name-based only.
    """
    logger.info("{} Syncing unmapped Quadratec brands to Brands.".format(_LOG_PREFIX))

    quadratec_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.QUADRATEC.value,
    ).first()
    if not quadratec_provider:
        logger.warning("{} Quadratec provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    _, cp_created = src_models.CompanyProviders.objects.get_or_create(
        company=tick_company,
        provider=quadratec_provider,
        defaults={"credentials": {}, "primary": False},
    )
    if cp_created:
        logger.info("{} Created CompanyProviders for TICK_PERFORMANCE + Quadratec.".format(_LOG_PREFIX))

    mapped_ids = set(
        src_models.BrandQuadratecBrandMapping.objects.values_list(
            "quadratec_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(
        src_models.QuadratecBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped Quadratec brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped Quadratec brands.".format(_LOG_PREFIX, len(unmapped)))

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 1: exact uppercase-name match.
    name_upper_keys = {(qb.name or "").strip().upper() for qb in unmapped if (qb.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            key = (b.name or "").strip().upper()
            if key not in brands_by_upper_name:
                brands_by_upper_name[key] = b
    for qb in unmapped:
        nm = (qb.name or "").strip().upper()
        if nm and nm in brands_by_upper_name:
            resolved_by_id[qb.id] = brands_by_upper_name[nm]

    # Phase 2: compact-key (punctuation/spacing-insensitive).
    compact_matches = 0
    still = [qb for qb in unmapped if qb.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for qb in still:
            key = normalize_compact_key(qb.name or "")
            if key and key in compact_index:
                resolved_by_id[qb.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix.
    unresolved = [qb for qb in unmapped if qb.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for qb in unresolved:
        parts = normalize_upper_words(qb.name or "").split()
        candidates: typing.List[src_models.Brands] = list(first_index.get(parts[0], ())) if parts else []
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(qb.name or "", candidates)
        if brand:
            resolved_by_id[qb.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved.
    new_brand_specs: typing.Dict[str, None] = {}
    for qb in unmapped:
        if qb.id in resolved_by_id:
            continue
        new_brand_specs.setdefault(_brand_name_upper_for_sync(qb), None)

    created_brands = 0
    if new_brand_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_specs.keys())).values_list(
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
            for name in new_brand_specs
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_specs.keys()))
        }
        for qb in unmapped:
            if qb.id not in resolved_by_id:
                resolved_by_id[qb.id] = by_name[_brand_name_upper_for_sync(qb)]

    mapping_models = [
        src_models.BrandQuadratecBrandMapping(
            brand_id=resolved_by_id[qb.id].id,
            quadratec_brand_id=qb.id,
        )
        for qb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandQuadratecBrandMapping,
        mapping_models,
        unique_fields=["brand", "quadratec_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    created_company_brands = 0
    for qb in unmapped:
        brand = resolved_by_id[qb.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=quadratec_provider,
        )
        if bp_created:
            created_brand_providers += 1
        _, cb_created = src_models.CompanyBrands.objects.get_or_create(
            company=tick_company,
            brand=brand,
            defaults={
                "status": src_enums.CompanyBrandStatus.ACTIVE.value,
                "status_name": src_enums.CompanyBrandStatus.ACTIVE.name,
            },
        )
        if cb_created:
            created_company_brands += 1

    logger.info(
        "{} Sync complete. Brands created: {}, compact: {}, fuzzy: {}, mappings: {}, "
        "BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX, created_brands, compact_matches, fuzzy_matches,
            len(mapping_models), created_brand_providers, created_company_brands,
        )
    )
    return unmapped


# --------------------------------------------------------------------------------------
# Per-company pricing (QuadratecCompanyPricing)
# --------------------------------------------------------------------------------------
def sync_quadratec_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Read both feeds for one CompanyProviders row and upsert QuadratecCompanyPricing:
    ``retail_price``/``wholesale_price`` from the catalog feed, ``cost``/``map`` from the
    wholesale feed. Keyed to existing QuadratecPart rows by (brand, sku); parts missing from the
    catalog table are skipped (run fetch_and_save_quadratec first).
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.QUADRATEC.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active Quadratec CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    creds = credentials_helper.get_feed_credentials(cp)
    client = _quadratec_feed_client_for_credentials(creds)
    try:
        data = client.get_feed_data()
    except quadratec_exceptions.QuadratecException as e:
        logger.error(
            "{} Feed error for company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e))
        )
        raise

    combined = _combined_rows_by_pn(data)
    if not combined:
        logger.warning(
            "{} No Quadratec rows for company_id={}. Nothing to price.".format(_LOG_PREFIX, cp.company_id)
        )
        return

    # Map (brand_external_id, sku) -> QuadratecPart.id so we only price existing catalog rows.
    sku_to_part_id: typing.Dict[typing.Tuple[str, str], int] = {}
    for row in src_models.QuadratecPart.objects.values(
        "id", "sku", "brand__external_id"
    ).iterator(chunk_size=5000):
        sku_to_part_id[(row["brand__external_id"], row["sku"])] = row["id"]

    now = timezone.now()
    pricing_rows: typing.List[src_models.QuadratecCompanyPricing] = []
    missing = 0
    for pn, pair in combined.items():
        brand_ext = _row_brand_name(pair)
        part_id = sku_to_part_id.get((brand_ext, pn)) or sku_to_part_id.get(
            (DEFAULT_QUADRATEC_BRAND_NAME, pn)
        )
        if not part_id:
            missing += 1
            continue
        cat = pair.get("catalog") or {}
        inv = pair.get("inv") or {}
        pricing_rows.append(
            src_models.QuadratecCompanyPricing(
                part_id=part_id,
                company_id=cp.company_id,
                retail_price=_safe_decimal(cat.get("retail_price")),
                wholesale_price=_safe_decimal(cat.get("wholesale_price")),
                cost=_safe_decimal(inv.get("cost")),
                map=_safe_decimal(inv.get("map")),
                updated_at=now,
            )
        )

    total = 0
    for start in range(0, len(pricing_rows), QUADRATEC_PARTS_UPSERT_BATCH):
        batch = pricing_rows[start:start + QUADRATEC_PARTS_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.QuadratecCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["retail_price", "wholesale_price", "cost", "map", "updated_at"],
            returning=False,
        )
        total += len(batch)

    logger.info(
        "{} company_id={}: upserted {} QuadratecCompanyPricing rows ({} feed rows had no catalog part).".format(
            _LOG_PREFIX, cp.company_id, total, missing
        )
    )
