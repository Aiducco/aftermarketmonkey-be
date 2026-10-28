"""
Motor State Distributing API — brand mapping, and the client the ordering path uses.

Catalog, stock and per-company pricing all come from the FTP feed; see
``src.integrations.services.motorstate_feed``. One CSV per dealer account carries the whole
catalog plus fields the API has none of (UPC, dimensions, weight, MAP, MSRP, manufacturer part
number, shipping flags, and on enriched accounts an image URL, a three-level category taxonomy
and a full-length description).

What lives here:
  * ``fetch_and_save_motorstate_brands`` — GET /api/Brands -> MotorStateBrand.
  * ``sync_unmapped_motorstate_brands_to_brands`` — maps MotorStateBrand to Brands; called by
    the nightly ingest after the feed pass.
  * ``sync_motorstate_company_pricing_for_company_provider`` — the name the pricing-job queue
    dispatches on, now a thin delegate to the feed.

The API catalog path that used to live below this line -- an epoch-dated
ProductAvailabilityChange sweep building a part-number spine, an incremental poll off its
high-water mark, and a parallel /api/Product hydrate over it -- has been removed along with
the MotorStateAvailability table it wrote. The feed replaced all three: brand comes from the
part number's own three-character prefix rather than the spine, and the dealer file carries
detail and pricing in one pass. It was last run manually on 2026-08-08, was never scheduled,
and MotorStateAvailability had been frozen since 2026-09-02 while still being read as a
"shared availability spine" that no longer reflected anything.
"""
import decimal
import logging
import typing

import pgbulk
from django.db.models.functions import Upper

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.motorstate import client as motorstate_client
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MOTOR-STATE-SERVICES]"

# Epoch date that dumps the whole catalog through the availability endpoint.




# ---------------------------------------------------------------------------
# Connection / client resolution
# ---------------------------------------------------------------------------
def _active_motorstate_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        active=True,
    ).select_related("company", "provider")


def _resolve_company_provider(
    company_provider_id: typing.Optional[int] = None,
) -> typing.Optional[src_models.CompanyProviders]:
    """Explicit id when given, else primary active connection, else first active."""
    base = _active_motorstate_company_providers_queryset()
    if company_provider_id is not None:
        return base.filter(id=company_provider_id).first()
    return base.filter(primary=True).first() or base.first()


def _build_client(
    company_provider: src_models.CompanyProviders,
) -> motorstate_client.MotorStateApiClient:
    credentials = credentials_helper.get_feed_credentials(company_provider)
    return motorstate_client.MotorStateApiClient(credentials=credentials)


def _json_safe(value: typing.Any) -> typing.Any:
    """Recursively convert Decimals (from the client's parse_float=Decimal) to floats so
    the raw ``data`` blob is JSON-serializable. Exact prices are kept in the typed
    DecimalFields; this copy is a lossy-but-faithful raw mirror."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value




# ---------------------------------------------------------------------------
# Brands
# ---------------------------------------------------------------------------
def fetch_and_save_motorstate_brands(company_provider_id: typing.Optional[int] = None) -> None:
    logger.info("{} Started fetching Motor State brands.".format(_LOG_PREFIX))
    company_provider = _resolve_company_provider(company_provider_id)
    if not company_provider:
        logger.info("{} No active Motor State provider found.".format(_LOG_PREFIX))
        return

    client = _build_client(company_provider)
    brands_data = client.get_brands()
    if not brands_data:
        logger.warning("{} No brands returned from Motor State.".format(_LOG_PREFIX))
        return

    instances = _transform_brands(brands_data)
    pgbulk.upsert(
        src_models.MotorStateBrand,
        instances,
        unique_fields=["code"],
        update_fields=["name", "offered", "is_inventory_available", "data", "updated_at"],
        returning=False,
    )
    logger.info("{} Upserted {} Motor State brands.".format(_LOG_PREFIX, len(instances)))


def _transform_brands(brands_data: typing.List[typing.Dict]) -> typing.List[src_models.MotorStateBrand]:
    instances: typing.List[src_models.MotorStateBrand] = []
    for brand in brands_data:
        code = (brand.get("Code") or "").strip()
        if not code:
            continue
        instances.append(
            src_models.MotorStateBrand(
                code=code,
                name=brand.get("Name"),
                offered=bool(brand.get("Offered", False)),
                is_inventory_available=bool(brand.get("IsInventoryAvailable", False)),
                data=_json_safe(brand),
            )
        )
    return instances


# ---------------------------------------------------------------------------
# Brand mapping (MotorStateBrand -> Brands)
# ---------------------------------------------------------------------------
def _brand_name_upper_for_sync(ms_brand: src_models.MotorStateBrand) -> str:
    name_upper = (ms_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(ms_brand.code)
    return name_upper


def sync_unmapped_motorstate_brands_to_brands() -> typing.List[src_models.MotorStateBrand]:
    """
    For each MotorStateBrand without a BrandMotorStateBrandMapping: resolve Brand by exact name
    (uppercase), then compact-key, then fuzzy word-prefix match (same rules as other providers);
    otherwise create it. Upserts BrandMotorStateBrandMapping, BrandProviders, and CompanyBrands
    for TICK_PERFORMANCE. Motor State's /api/Brands carries no AAIA code, so matching is
    name-based only (same as Quadratec).
    """
    logger.info("{} Syncing unmapped Motor State brands to Brands.".format(_LOG_PREFIX))

    motorstate_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
    ).first()
    if not motorstate_provider:
        logger.warning("{} Motor State provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandMotorStateBrandMapping.objects.values_list(
            "motorstate_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(
        src_models.MotorStateBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped Motor State brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped Motor State brands.".format(_LOG_PREFIX, len(unmapped)))

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 1: exact uppercase-name match.
    name_upper_keys = {(mb.name or "").strip().upper() for mb in unmapped if (mb.name or "").strip()}
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
    for mb in unmapped:
        nm = (mb.name or "").strip().upper()
        if nm and nm in brands_by_upper_name:
            resolved_by_id[mb.id] = brands_by_upper_name[nm]

    # Phase 2: compact-key (punctuation/spacing-insensitive).
    compact_matches = 0
    still = [mb for mb in unmapped if mb.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for mb in still:
            key = normalize_compact_key(mb.name or "")
            if key and key in compact_index:
                resolved_by_id[mb.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix.
    unresolved = [mb for mb in unmapped if mb.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for mb in unresolved:
        parts = normalize_upper_words(mb.name or "").split()
        candidates: typing.List[src_models.Brands] = list(first_index.get(parts[0], ())) if parts else []
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(mb.name or "", candidates)
        if brand:
            resolved_by_id[mb.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved.
    new_brand_specs: typing.Dict[str, None] = {}
    for mb in unmapped:
        if mb.id in resolved_by_id:
            continue
        new_brand_specs.setdefault(_brand_name_upper_for_sync(mb), None)

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
        for mb in unmapped:
            if mb.id not in resolved_by_id:
                resolved_by_id[mb.id] = by_name[_brand_name_upper_for_sync(mb)]

    mapping_models = [
        src_models.BrandMotorStateBrandMapping(
            brand_id=resolved_by_id[mb.id].id,
            motorstate_brand_id=mb.id,
        )
        for mb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandMotorStateBrandMapping,
        mapping_models,
        unique_fields=["brand", "motorstate_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    created_company_brands = 0
    for mb in unmapped:
        brand = resolved_by_id[mb.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=motorstate_provider,
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


# ---------------------------------------------------------------------------
# Per-company pricing raw fetch (IntegrationPricingSyncJob entrypoint)
# ---------------------------------------------------------------------------
def sync_motorstate_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Refresh this connection's own MotorStateCompanyPricing rows from its own FTP feed file.

    Kept as the entrypoint name the IntegrationPricingSyncJob queue dispatches on; the work
    itself lives in ``motorstate_feed``. Each dealer's file carries the whole catalog priced
    for that account, so this is one download per connection -- not the ~10.4k /api/Product
    calls the API path needed.
    """
    from src.integrations.services import motorstate_feed

    motorstate_feed.sync_motorstate_company_pricing_from_feed(company_provider_id)
