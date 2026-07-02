import logging
import time
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import pandas as pd
import pgbulk

from django.conf import settings
from django.db import close_old_connections, connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.premier import client as premier_client
from src.integrations.clients.premier import exceptions as premier_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PREMIER-SERVICES]"

PREMIER_PGBULK_BATCH_SIZE = 5000
PREMIER_PGBULK_BATCH_DELAY_SECONDS = 0.5


def _premier_setting_int(name: str, default: int) -> int:
    try:
        return int(getattr(settings, name, default))
    except (TypeError, ValueError):
        return default


PREMIER_COMPANY_PRICING_SYNC_MAX_WORKERS = _premier_setting_int("PREMIER_COMPANY_PRICING_SYNC_MAX_WORKERS", 4)

PREMIER_PARTS_UPDATE_FIELDS = [
    "mfg_part_number", "long_description", "external_long_description",
    "length", "width", "height", "weight", "upc_code", "usa_item_availability",
    "core_charge", "jobber_price", "map_price", "retail_price", "inventory_status",
    "nv_qty", "ky_qty", "mfg_qty", "wa_qty", "image_url",
    "ships_ltl", "item_with_cores", "prop65_carcinogen", "prop65_reproductive_harm",
    "approved_line", "california_legal", "line_code", "pies_ems_code", "drop_ship_fee",
    "canada_map", "canada_msrp", "canada_jobber", "part_category", "part_subcategory",
    "part_terminology", "freight_cost", "minimum_order_qty", "drop_shippable_from_mfg",
    "vendor_enhanced_emissions_code", "is_kit", "kit_component_list", "raw_data", "updated_at",
]


def _clean(value: typing.Any) -> typing.Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    return s if s else None


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    s = _clean(value)
    if s is None:
        return None
    try:
        return Decimal(s)
    except Exception:
        return None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    s = _clean(value)
    if s is None:
        return None
    try:
        return int(float(s))
    except Exception:
        return None


def _safe_bool(value: typing.Any) -> bool:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().upper() in ("YES", "TRUE", "1", "T", "Y")


def fetch_and_save_premier_brands() -> None:
    """
    Download the Premier CSV and upsert brands from unique Brand + Line Code values.
    Uses the primary CompanyProvider for FTP credentials.
    """
    logger.info("{} Fetching Premier brands.".format(_LOG_PREFIX))

    primary_provider = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        primary=True,
    ).first()

    if not primary_provider:
        logger.info("{} No active primary Premier provider found.".format(_LOG_PREFIX))
        return

    try:
        ftp_client = premier_client.PremierFTPClient(credentials=primary_provider.credentials)
    except ValueError as e:
        logger.error("{} Invalid credentials: {}.".format(_LOG_PREFIX, str(e)))
        raise

    try:
        records = ftp_client.get_inventory_records()
    except premier_exceptions.PremierException as e:
        logger.error("{} FTP error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    if not records:
        logger.warning("{} No inventory records returned.".format(_LOG_PREFIX))
        return

    brand_data: typing.Dict[str, typing.Optional[str]] = {}
    for row in records:
        name = _clean(row.get("Brand"))
        if name and name not in brand_data:
            brand_data[name] = _clean(row.get("Line Code"))

    brand_instances = [
        src_models.PremierBrand(
            external_id=name,
            name=name,
            line_code=line_code,
        )
        for name, line_code in sorted(brand_data.items())
    ]

    if not brand_instances:
        logger.warning("{} No brand instances created.".format(_LOG_PREFIX))
        return

    pgbulk.upsert(
        src_models.PremierBrand,
        brand_instances,
        unique_fields=["external_id"],
        update_fields=["name", "line_code", "updated_at"],
        returning=True,
    )
    logger.info("{} Upserted {} Premier brands.".format(_LOG_PREFIX, len(brand_instances)))


def sync_unmapped_premier_brands_to_brands() -> typing.List[src_models.PremierBrand]:
    """
    For each PremierBrand without a BrandPremierBrandMapping: resolve Brand by exact name
    (case-insensitive), then compact-key match, then fuzzy match; otherwise create.
    Bulk-upsert BrandPremierBrandMapping and BrandProviders.
    """
    logger.info("{} Syncing unmapped Premier brands to Brands.".format(_LOG_PREFIX))

    premier_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    if not premier_provider:
        logger.warning("{} Premier provider not found.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandPremierBrandMapping.objects.values_list("premier_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.PremierBrand.objects.exclude(id__in=mapped_ids).order_by("id"))

    if not unmapped:
        logger.info("{} No unmapped Premier brands.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped Premier brands.".format(_LOG_PREFIX, len(unmapped)))

    name_upper_keys = {(b.name or "").strip().upper() for b in unmapped if (b.name or "").strip()}
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

    resolved: typing.Dict[int, src_models.Brands] = {}
    for pb in unmapped:
        nm = (pb.name or "").strip().upper()
        if nm and nm in brands_by_upper_name:
            resolved[pb.id] = brands_by_upper_name[nm]

    still_unresolved = [pb for pb in unmapped if pb.id not in resolved]
    if still_unresolved:
        compact_index = brands_by_compact_key()
        for pb in still_unresolved:
            key = normalize_compact_key(pb.name or "")
            if key and key in compact_index:
                resolved[pb.id] = compact_index[key]

    unresolved_after_compact = [pb for pb in unmapped if pb.id not in resolved]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_compact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0

    for pb in unresolved_after_compact:
        parts = normalize_upper_words(pb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(pb.name or "", candidates)
        if brand:
            resolved[pb.id] = brand
            fuzzy_matches += 1

    # Create brands for anything still unresolved
    new_brand_names: typing.Set[str] = set()
    for pb in unmapped:
        if pb.id not in resolved:
            name_upper = (pb.name or "").strip().upper()
            if name_upper:
                new_brand_names.add(name_upper)

    created_brands = 0
    if new_brand_names:
        existing = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_names)).values_list("name", flat=True)
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
            )
            for name in new_brand_names
            if name not in existing
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_names))
        }
        for pb in unmapped:
            if pb.id not in resolved:
                nu = (pb.name or "").strip().upper()
                if nu in by_name:
                    resolved[pb.id] = by_name[nu]

    mapping_models = [
        src_models.BrandPremierBrandMapping(
            brand_id=resolved[pb.id].id,
            premier_brand_id=pb.id,
        )
        for pb in unmapped
        if pb.id in resolved
    ]
    if mapping_models:
        pgbulk.upsert(
            src_models.BrandPremierBrandMapping,
            mapping_models,
            unique_fields=["brand", "premier_brand"],
            update_fields=["updated_at"],
        )

    brand_ids = {resolved[pb.id].id for pb in unmapped if pb.id in resolved}
    existing_bp = set(
        src_models.BrandProviders.objects.filter(
            provider=premier_provider, brand_id__in=brand_ids,
        ).values_list("brand_id", flat=True)
    )
    bp_to_create = [
        src_models.BrandProviders(brand_id=bid, provider_id=premier_provider.id)
        for bid in brand_ids
        if bid not in existing_bp
    ]
    if bp_to_create:
        src_models.BrandProviders.objects.bulk_create(bp_to_create, ignore_conflicts=True)

    logger.info(
        "{} Sync complete: brands_created={} fuzzy_matches={} mappings={} brand_providers={}.".format(
            _LOG_PREFIX, created_brands, fuzzy_matches, len(mapping_models), len(bp_to_create),
        )
    )
    return unmapped


def _pgbulk_upsert_premier_parts_batches(
    part_instances: typing.List[src_models.PremierParts],
    batch_size: int,
    batch_delay_seconds: float,
) -> int:
    if not part_instances:
        return 0
    num_batches = (len(part_instances) + batch_size - 1) // batch_size
    total = 0
    for i in range(0, len(part_instances), batch_size):
        batch = part_instances[i: i + batch_size]
        batch_num = (i // batch_size) + 1
        now = timezone.now()
        for p in batch:
            p.updated_at = now
        pgbulk.upsert(
            src_models.PremierParts,
            batch,
            unique_fields=["premier_part_number", "brand"],
            update_fields=PREMIER_PARTS_UPDATE_FIELDS,
            returning=False,
        )
        total += len(batch)
        logger.info("{} Upserted PremierParts batch {}/{} ({} rows).".format(
            _LOG_PREFIX, batch_num, num_batches, len(batch),
        ))
        connection.close()
        if batch_num < num_batches:
            time.sleep(batch_delay_seconds)
    return total


def _pgbulk_upsert_premier_company_pricing_batches(
    pricing_instances: typing.List[src_models.PremierCompanyPricing],
    batch_size: int,
    batch_delay_seconds: float,
) -> int:
    if not pricing_instances:
        return 0
    num_batches = (len(pricing_instances) + batch_size - 1) // batch_size
    total = 0
    for i in range(0, len(pricing_instances), batch_size):
        batch = pricing_instances[i: i + batch_size]
        batch_num = (i // batch_size) + 1
        pgbulk.upsert(
            src_models.PremierCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["customer_price", "jobber_price", "map_price", "core_charge", "customer_cad_price", "updated_at"],
            returning=False,
        )
        total += len(batch)
        logger.info("{} Upserted PremierCompanyPricing batch {}/{} ({} rows).".format(
            _LOG_PREFIX, batch_num, num_batches, len(batch),
        ))
        connection.close()
        if batch_num < num_batches:
            time.sleep(batch_delay_seconds)
    return total


def _transform_parts_data(
    records: typing.List[typing.Dict],
    brand_name_to_premier_brand: typing.Dict[str, src_models.PremierBrand],
    omit_pricing: bool = False,
) -> typing.List[src_models.PremierParts]:
    part_instances = []
    for row in records:
        try:
            brand_name = _clean(row.get("Brand"))
            if not brand_name:
                continue
            premier_brand = brand_name_to_premier_brand.get(brand_name)
            if not premier_brand:
                continue

            part_number = _clean(row.get("Premier Part Number"))
            if not part_number:
                logger.warning("{} Skipping row with missing Premier Part Number.".format(_LOG_PREFIX))
                continue

            part_instances.append(
                src_models.PremierParts(
                    premier_part_number=part_number,
                    brand=premier_brand,
                    mfg_part_number=_clean(row.get("Mfg Part Number")),
                    long_description=_clean(row.get("Long Description")),
                    external_long_description=_clean(row.get("External Long Description")),
                    length=_safe_decimal(row.get("Length")),
                    width=_safe_decimal(row.get("Width")),
                    height=_safe_decimal(row.get("Height")),
                    weight=_safe_decimal(row.get("Weight")),
                    upc_code=_clean(row.get("Upc")),
                    usa_item_availability=_safe_int(row.get("USA Item Availability")),
                    core_charge=None if omit_pricing else _safe_decimal(row.get("Core Charge")),
                    jobber_price=None if omit_pricing else _safe_decimal(row.get("Jobber")),
                    map_price=None if omit_pricing else _safe_decimal(row.get("MAP")),
                    retail_price=_safe_decimal(row.get("Retail")),
                    inventory_status=_clean(row.get("Inventory Status")),
                    nv_qty=_safe_int(row.get("NV whse")),
                    ky_qty=_safe_int(row.get("KY whse")),
                    mfg_qty=_safe_int(row.get("MFG Invt")),
                    wa_qty=_safe_int(row.get("WA whse")),
                    image_url=_clean(row.get("ImageURL")),
                    ships_ltl=_safe_bool(row.get("ShipsLTL")),
                    item_with_cores=_safe_bool(row.get("ItemWithCores")),
                    prop65_carcinogen=_safe_bool(row.get("Proposition 65 Carcinogen")),
                    prop65_reproductive_harm=_safe_bool(row.get("Proposition 65 Reproductive Harm")),
                    approved_line=_safe_bool(row.get("Approved Line")),
                    california_legal=_safe_bool(row.get("California Legal")),
                    line_code=_clean(row.get("Line Code")),
                    pies_ems_code=_clean(row.get("PIES EMS Code")),
                    drop_ship_fee=_safe_decimal(row.get("Drop Ship Fee")),
                    canada_map=_safe_decimal(row.get("Canada MAP")),
                    canada_msrp=_safe_decimal(row.get("Canada MSRP")),
                    canada_jobber=_safe_decimal(row.get("Canada Jobber")),
                    part_category=_clean(row.get("Part Category")),
                    part_subcategory=_clean(row.get("Part Subcategory")),
                    part_terminology=_clean(row.get("Part Terminology")),
                    freight_cost=_safe_decimal(row.get("Freight Cost")),
                    minimum_order_qty=_safe_int(row.get("Minimum Order Qty")),
                    drop_shippable_from_mfg=_safe_bool(row.get("Drop Shippable From MFG")),
                    vendor_enhanced_emissions_code=_clean(row.get("Vendor Enhanced Emissions Code")),
                    is_kit=_safe_bool(row.get("Kit")),
                    kit_component_list=_clean(row.get("Kit Component List")),
                    raw_data={
                        k: (None if (v is None or (isinstance(v, float) and pd.isna(v))) else v)
                        for k, v in row.items()
                    },
                )
            )
        except Exception as e:
            logger.warning("{} Error transforming row: {}. Skipping.".format(_LOG_PREFIX, str(e)))
            continue
    return part_instances


def _part_number_brand_id_lookup(
    records: typing.List[typing.Dict],
    brand_name_to_premier_brand: typing.Dict[str, src_models.PremierBrand],
) -> typing.Dict[typing.Tuple[str, int], int]:
    """Map (premier_part_number, premier_brand.id) -> PremierParts.id."""
    part_numbers: typing.Set[str] = set()
    brand_ids: typing.Set[int] = set()
    for row in records:
        brand_name = _clean(row.get("Brand"))
        pb = brand_name_to_premier_brand.get(brand_name or "")
        if not pb:
            continue
        pn = _clean(row.get("Premier Part Number"))
        if not pn:
            continue
        part_numbers.add(pn)
        brand_ids.add(pb.id)
    if not part_numbers or not brand_ids:
        return {}
    lookup: typing.Dict[typing.Tuple[str, int], int] = {}
    for p in (
        src_models.PremierParts.objects.filter(
            premier_part_number__in=part_numbers, brand_id__in=brand_ids
        )
        .only("id", "premier_part_number", "brand_id")
        .iterator(chunk_size=5000)
    ):
        lookup[(p.premier_part_number, p.brand_id)] = p.id
    return lookup


def _build_company_pricing_instances(
    records: typing.List[typing.Dict],
    brand_name_to_premier_brand: typing.Dict[str, src_models.PremierBrand],
    company: src_models.Company,
    part_lookup: typing.Dict[typing.Tuple[str, int], int],
) -> typing.List[src_models.PremierCompanyPricing]:
    instances = []
    for row in records:
        brand_name = _clean(row.get("Brand"))
        pb = brand_name_to_premier_brand.get(brand_name or "")
        if not pb:
            continue
        pn = _clean(row.get("Premier Part Number"))
        if not pn:
            continue
        part_id = part_lookup.get((pn, pb.id))
        if not part_id:
            continue
        instances.append(
            src_models.PremierCompanyPricing(
                part_id=part_id,
                company=company,
                customer_price=_safe_decimal(row.get("Customer Price")),
                jobber_price=_safe_decimal(row.get("Jobber")),
                map_price=_safe_decimal(row.get("MAP")),
                core_charge=_safe_decimal(row.get("Core Charge")),
                customer_cad_price=_safe_decimal(row.get("Customer CAD Price")),
            )
        )
    return instances


def fetch_and_save_all_premier_brand_parts() -> None:
    """
    Download the Premier CSV per active CompanyProvider and upsert PremierParts + PremierCompanyPricing.
    The primary CompanyProvider drives the shared catalog upsert (PremierParts);
    every active company gets PremierCompanyPricing from its own feed.
    """
    logger.info("{} Fetching all Premier brand parts.".format(_LOG_PREFIX))

    premier_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    if not premier_provider:
        logger.info("{} No Premier provider found.".format(_LOG_PREFIX))
        return

    company_providers = list(
        src_models.CompanyProviders.objects.filter(
            provider=premier_provider,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).select_related("company")
    )
    if not company_providers:
        logger.warning("{} No active Premier company providers found.".format(_LOG_PREFIX))
        return

    has_primary = any(cp.primary for cp in company_providers)
    if not has_primary:
        logger.warning(
            "{} No primary Premier CompanyProvider; upserting PremierParts for every company.".format(_LOG_PREFIX)
        )

    company_providers_ordered = sorted(
        company_providers,
        key=lambda cp: (not cp.primary, (cp.company.name or "")),
    )

    total_parts = 0
    total_pricing = 0

    # Separate primary providers (run sequentially first for catalog) from non-primaries.
    primary_providers = [cp for cp in company_providers_ordered if cp.primary or not has_primary]
    non_primary_providers = [cp for cp in company_providers_ordered if not cp.primary and has_primary]

    def _sync_premier_pricing_for_cp(company_provider: src_models.CompanyProviders) -> int:
        """Fetch records for one non-primary company and upsert PremierCompanyPricing only."""
        close_old_connections()
        try:
            company = company_provider.company
            if not company_provider.active:
                logger.info("{} Skipping PremierCompanyPricing for company={} (inactive).".format(
                    _LOG_PREFIX, company.name
                ))
                return 0
            try:
                ftp_client = premier_client.PremierFTPClient(credentials=company_provider.credentials)
            except ValueError as e:
                logger.error("{} Invalid credentials company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
                return 0
            try:
                records = ftp_client.get_inventory_records()
            except premier_exceptions.PremierException as e:
                logger.error("{} FTP error company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
                return 0
            if not records:
                logger.warning("{} No records company={}.".format(_LOG_PREFIX, company.name))
                return 0
            brand_data: typing.Dict[str, typing.Optional[str]] = {}
            for row in records:
                name = _clean(row.get("Brand"))
                if name and name not in brand_data:
                    brand_data[name] = _clean(row.get("Line Code"))
            premier_brands = {
                b.name: b
                for b in src_models.PremierBrand.objects.filter(external_id__in=brand_data.keys())
            }
            part_lookup = _part_number_brand_id_lookup(records, premier_brands)
            pricing_instances = _build_company_pricing_instances(records, premier_brands, company, part_lookup)
            try:
                n = _pgbulk_upsert_premier_company_pricing_batches(
                    pricing_instances, PREMIER_PGBULK_BATCH_SIZE, PREMIER_PGBULK_BATCH_DELAY_SECONDS,
                )
                return n
            except Exception as e:
                logger.error("{} Pricing upsert failed company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
                return 0
        finally:
            connection.close()

    # Run primary providers synchronously (they do catalog upserts).
    for company_provider in primary_providers:
        company = company_provider.company
        should_upsert_parts = company_provider.primary or not has_primary

        try:
            ftp_client = premier_client.PremierFTPClient(credentials=company_provider.credentials)
        except ValueError as e:
            logger.error("{} Invalid credentials company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
            continue

        try:
            records = ftp_client.get_inventory_records()
        except premier_exceptions.PremierException as e:
            logger.error("{} FTP error company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
            continue

        if not records:
            logger.warning("{} No records company={}.".format(_LOG_PREFIX, company.name))
            continue

        brand_data: typing.Dict[str, typing.Optional[str]] = {}
        for row in records:
            name = _clean(row.get("Brand"))
            if name and name not in brand_data:
                brand_data[name] = _clean(row.get("Line Code"))

        if should_upsert_parts and brand_data:
            brand_instances = [
                src_models.PremierBrand(external_id=name, name=name, line_code=lc)
                for name, lc in sorted(brand_data.items())
            ]
            pgbulk.upsert(
                src_models.PremierBrand,
                brand_instances,
                unique_fields=["external_id"],
                update_fields=["name", "line_code", "updated_at"],
                returning=False,
            )

        premier_brands = {
            b.name: b
            for b in src_models.PremierBrand.objects.filter(external_id__in=brand_data.keys())
        }

        if should_upsert_parts:
            part_instances = _transform_parts_data(records, premier_brands, omit_pricing=True)
            if part_instances:
                try:
                    total_parts += _pgbulk_upsert_premier_parts_batches(
                        part_instances, PREMIER_PGBULK_BATCH_SIZE, PREMIER_PGBULK_BATCH_DELAY_SECONDS,
                    )
                except Exception as e:
                    logger.error("{} Parts upsert failed company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
                    raise
        else:
            logger.info("{} Skipping PremierParts upsert for company={} (non-primary).".format(
                _LOG_PREFIX, company.name
            ))

        if not company_provider.active:
            logger.info("{} Skipping PremierCompanyPricing for company={} (inactive).".format(
                _LOG_PREFIX, company.name
            ))
            continue

        part_lookup = _part_number_brand_id_lookup(records, premier_brands)
        pricing_instances = _build_company_pricing_instances(records, premier_brands, company, part_lookup)
        try:
            total_pricing += _pgbulk_upsert_premier_company_pricing_batches(
                pricing_instances, PREMIER_PGBULK_BATCH_SIZE, PREMIER_PGBULK_BATCH_DELAY_SECONDS,
            )
        except Exception as e:
            logger.error("{} Pricing upsert failed company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
            raise

    # Run non-primary providers in parallel (pricing only).
    if non_primary_providers:
        n_np = len(non_primary_providers)
        np_workers = max(1, min(PREMIER_COMPANY_PRICING_SYNC_MAX_WORKERS, n_np))
        logger.info("{} Syncing {} non-primary company pricing records with {} workers.".format(
            _LOG_PREFIX, n_np, np_workers,
        ))
        if np_workers == 1:
            for cp in non_primary_providers:
                total_pricing += _sync_premier_pricing_for_cp(cp)
        else:
            with ThreadPoolExecutor(max_workers=np_workers) as ex:
                fut_to_cp = {ex.submit(_sync_premier_pricing_for_cp, cp): cp for cp in non_primary_providers}
                for fut in as_completed(fut_to_cp):
                    cp = fut_to_cp[fut]
                    try:
                        total_pricing += fut.result()
                    except Exception as e:
                        logger.error("{} Pricing sync failed company={}: {}.".format(
                            _LOG_PREFIX, cp.company.name, str(e)
                        ))

    logger.info("{} Finished: parts_upserted={} pricing_upserted={}.".format(
        _LOG_PREFIX, total_parts, total_pricing,
    ))
