import logging
import re
import time
import typing
from decimal import Decimal

import pandas as pd
import pgbulk

from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from django.db import connection

from src.integrations.clients.keystone import client as keystone_client
from src.integrations.clients.keystone import exceptions as keystone_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_first_token_upper,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[KEYSTONE-SERVICES]"

# Regex to strip Excel formula-style values: ="10406" -> 10406, =10406 -> 10406
_EXCEL_FORMULA_PATTERN = re.compile(r'^="?([^"]*)"?$|^=(\d+(?:\.\d+)?)$')


def _clean_csv_value(value: typing.Any) -> typing.Optional[str]:
    """
    Clean CSV values that may contain Excel formula format like ="10406" or ="073905104067".
    Returns the inner value as string, or None if empty/invalid.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    match = _EXCEL_FORMULA_PATTERN.match(s)
    if match:
        return (match.group(1) or match.group(2) or "").strip() or None
    return s


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    """Convert value to Decimal, return None if invalid."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        s = _clean_csv_value(value)
        if s is None or s == "":
            return None
        return Decimal(str(s))
    except Exception:
        return None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    """Convert value to int, return None if invalid."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    try:
        s = _clean_csv_value(value)
        if s is None or s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _safe_bool(value: typing.Any) -> bool:
    """Convert value to bool."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    s = str(value).strip().upper()
    return s in ("TRUE", "1", "YES", "T")


def fetch_and_save_keystone_brands() -> None:
    """
    Fetch the Keystone inventory CSV and upsert brands from unique VendorName values.
    """
    logger.info("{} Started fetching and saving Keystone brands.".format(_LOG_PREFIX))

    primary_provider = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.KEYSTONE.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        primary=True,
    ).first()

    if not primary_provider:
        logger.info("{} No Keystone active primary provider found.".format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    try:
        ftp_client = keystone_client.KeystoneFTPClient(credentials=credentials)
    except ValueError as e:
        logger.error("{} Invalid credentials or configuration: {}.".format(_LOG_PREFIX, str(e)))
        raise

    try:
        records = ftp_client.get_inventory_records()
    except keystone_exceptions.KeystoneException as e:
        logger.error("{} Keystone error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    if not records:
        logger.warning("{} No inventory records returned.".format(_LOG_PREFIX))
        return

    # Extract unique brands by VendorName
    brand_names = set()
    for row in records:
        name = _clean_csv_value(row.get("VendorName"))
        if name:
            brand_names.add(name)

    brand_instances = []
    for name in sorted(brand_names):
        external_id = name  # Use VendorName as external_id for Keystone
        aaia_code = None
        for row in records:
            if _clean_csv_value(row.get("VendorName")) == name:
                aaia_code = _clean_csv_value(row.get("AAIACode"))
                break
        brand_instances.append(
            src_models.KeystoneBrand(
                external_id=external_id,
                name=name,
                aaia_code=aaia_code or "",
            )
        )

    if not brand_instances:
        logger.warning("{} No valid brand instances created.".format(_LOG_PREFIX))
        return

    try:
        upserted = pgbulk.upsert(
            src_models.KeystoneBrand,
            brand_instances,
            unique_fields=["external_id"],
            update_fields=["name", "aaia_code", "updated_at"],
            returning=True,
        )
        logger.info("{} Successfully upserted {} Keystone brands.".format(
            _LOG_PREFIX, len(upserted) if upserted else 0
        ))
    except Exception as e:
        logger.error("{} Error during bulk upsert: {}.".format(_LOG_PREFIX, str(e)))
        raise


def _normalize_aaia_codes(aaia_code: typing.Optional[str]) -> typing.List[str]:
    """Split aaia_code by comma and return non-empty stripped parts."""
    if not aaia_code or not str(aaia_code).strip():
        return []
    return [p.strip() for p in str(aaia_code).split(",") if p and p.strip()]


def _keystone_brand_name_upper_for_sync(keystone_brand: src_models.KeystoneBrand) -> str:
    name_upper = (keystone_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(keystone_brand.external_id)
    return name_upper


def sync_unmapped_keystone_brands_to_brands() -> typing.List[src_models.KeystoneBrand]:
    """
    For each KeystoneBrand that does not yet have a BrandKeystoneBrandMapping:
    resolve Brand by aaia_code (comma-separated), then exact name (case-insensitive), then fuzzy
    word-prefix match with truncation on either side (e.g. BAK IND ↔ BAK INDUSTRIES,
    DIRTY LIFE ↔ DIRTY LIFE WHEELS); otherwise create.
    Bulk-upsert BrandKeystoneBrandMapping, bulk BrandProviders and CompanyBrands for TICK_PERFORMANCE.
    Returns the list of KeystoneBrand instances that were synced.
    """
    logger.info("{} Syncing unmapped Keystone brands to Brands.".format(_LOG_PREFIX))

    keystone_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.KEYSTONE.value,
    ).first()
    if not keystone_provider:
        logger.warning("{} Keystone provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_keystone_ids = set(
        src_models.BrandKeystoneBrandMapping.objects.values_list(
            "keystone_brand_id", flat=True
        ).distinct()
    )
    unmapped_keystone_brands = list(
        src_models.KeystoneBrand.objects.exclude(id__in=mapped_keystone_ids).order_by("id")
    )

    if not unmapped_keystone_brands:
        logger.info("{} No unmapped Keystone brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info(
        "{} Found {} unmapped Keystone brands.".format(
            _LOG_PREFIX, len(unmapped_keystone_brands)
        )
    )

    all_aaia: typing.Set[str] = set()
    for kb in unmapped_keystone_brands:
        for code in _normalize_aaia_codes(kb.aaia_code):
            all_aaia.add(code)

    aaia_to_brand: typing.Dict[str, src_models.Brands] = {}
    if all_aaia:
        for b in src_models.Brands.objects.filter(aaia_code__in=all_aaia).order_by("id"):
            if b.aaia_code and b.aaia_code not in aaia_to_brand:
                aaia_to_brand[b.aaia_code] = b

    name_upper_keys: typing.Set[str] = set()
    for kb in unmapped_keystone_brands:
        hit = False
        for code in _normalize_aaia_codes(kb.aaia_code):
            if code in aaia_to_brand:
                hit = True
                break
        if not hit and (kb.name or "").strip():
            name_upper_keys.add((kb.name or "").strip().upper())

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

    resolved_by_keystone_id: typing.Dict[int, src_models.Brands] = {}
    for kb in sorted(unmapped_keystone_brands, key=lambda x: x.id):
        brand = None
        for code in _normalize_aaia_codes(kb.aaia_code):
            brand = aaia_to_brand.get(code)
            if brand:
                break
        if not brand:
            nm = (kb.name or "").strip().upper()
            if nm:
                brand = brands_by_upper_name.get(nm)
        if brand:
            resolved_by_keystone_id[kb.id] = brand

    unresolved_after_exact = [kb for kb in unmapped_keystone_brands if kb.id not in resolved_by_keystone_id]
    brands_first_index = (
        brands_by_first_token_upper() if unresolved_after_exact else {}
    )
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for kb in unresolved_after_exact:
        parts = normalize_upper_words(kb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(kb.name or "", candidates)
        if brand:
            resolved_by_keystone_id[kb.id] = brand
            fuzzy_matches += 1
            logger.debug(
                "{} Fuzzy-matched Keystone {!r} to Brand id={} name={!r}.".format(
                    _LOG_PREFIX, kb.name, brand.id, brand.name
                )
            )

    new_brand_specs: typing.Dict[str, typing.Optional[str]] = {}
    for kb in sorted(unmapped_keystone_brands, key=lambda x: x.id):
        if kb.id in resolved_by_keystone_id:
            continue
        nu = _keystone_brand_name_upper_for_sync(kb)
        if nu not in new_brand_specs:
            parts = _normalize_aaia_codes(kb.aaia_code)
            new_brand_specs[nu] = parts[0] if parts else None

    created_brands = 0
    if new_brand_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_specs.keys())).values_list(
                "name", flat=True
            )
        )
        new_brand_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=aaia_val,
            )
            for name, aaia_val in new_brand_specs.items()
            if name not in existing_names
        ]
        if new_brand_rows:
            src_models.Brands.objects.bulk_create(new_brand_rows, ignore_conflicts=True)
            created_brands = len(new_brand_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_specs.keys()))
        }
        for kb in unmapped_keystone_brands:
            if kb.id not in resolved_by_keystone_id:
                nu = _keystone_brand_name_upper_for_sync(kb)
                resolved_by_keystone_id[kb.id] = by_name[nu]

    mapping_models = [
        src_models.BrandKeystoneBrandMapping(
            brand_id=resolved_by_keystone_id[kb.id].id,
            keystone_brand_id=kb.id,
        )
        for kb in unmapped_keystone_brands
    ]
    try:
        pgbulk.upsert(
            src_models.BrandKeystoneBrandMapping,
            mapping_models,
            unique_fields=["brand", "keystone_brand"],
            update_fields=["updated_at"],
        )
    except Exception as e:
        logger.error("{} Error upserting BrandKeystoneBrandMapping: {}.".format(_LOG_PREFIX, str(e)))
        raise

    brand_ids = {resolved_by_keystone_id[kb.id].id for kb in unmapped_keystone_brands}
    existing_bp_ids = set(
        src_models.BrandProviders.objects.filter(
            provider=keystone_provider,
            brand_id__in=brand_ids,
        ).values_list("brand_id", flat=True)
    )
    bp_to_create = [
        src_models.BrandProviders(brand_id=bid, provider_id=keystone_provider.id)
        for bid in brand_ids
        if bid not in existing_bp_ids
    ]
    if bp_to_create:
        src_models.BrandProviders.objects.bulk_create(bp_to_create, ignore_conflicts=True)
    created_brand_providers = len(bp_to_create)

    existing_cb_ids = set(
        src_models.CompanyBrands.objects.filter(
            company=tick_company,
            brand_id__in=brand_ids,
        ).values_list("brand_id", flat=True)
    )
    active_val = src_enums.CompanyBrandStatus.ACTIVE.value
    active_name = src_enums.CompanyBrandStatus.ACTIVE.name
    cb_to_create = [
        src_models.CompanyBrands(
            company_id=tick_company.id,
            brand_id=bid,
            status=active_val,
            status_name=active_name,
        )
        for bid in brand_ids
        if bid not in existing_cb_ids
    ]
    if cb_to_create:
        src_models.CompanyBrands.objects.bulk_create(cb_to_create, ignore_conflicts=True)
    created_company_brands = len(cb_to_create)

    logger.info(
        "{} Sync complete. Brands created: {}, fuzzy name matches: {}, "
        "BrandKeystoneBrandMapping upserted: {}, BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX,
            created_brands,
            fuzzy_matches,
            len(mapping_models),
            created_brand_providers,
            created_company_brands,
        )
    )
    return unmapped_keystone_brands


# Keystone provider id (use when provider record has id=4)
KEYSTONE_PROVIDER_ID = 4

# pgbulk upsert chunk size (larger = fewer round-trips; tune for DB load / memory).
KEYSTONE_PGBULK_BATCH_SIZE = 5000
KEYSTONE_PGBULK_BATCH_DELAY_SECONDS = 0.5

KEYSTONE_PARTS_UPDATE_FIELDS = [
    "vendor_code", "part_number", "manufacturer_part_no", "long_description",
    "upsable", "case_qty",
    "is_non_returnable", "prop65_toxicity", "upc_code", "weight",
    "height", "length", "width", "aaia_code", "is_hazmat", "is_chemical",
    "ups_ground_assessorial", "us_ltl", "east_qty", "midwest_qty",
    "california_qty", "southeast_qty", "pacific_nw_qty", "texas_qty",
    "great_lakes_qty", "florida_qty", "total_qty", "kit_components",
    "is_kit", "raw_data", "updated_at",
]


def _pgbulk_upsert_keystone_parts_batches(
    part_instances: typing.List[src_models.KeystoneParts],
    batch_size: int,
    batch_delay_seconds: float,
) -> int:
    if not part_instances:
        return 0
    num_batches = (len(part_instances) + batch_size - 1) // batch_size
    total = 0
    for i in range(0, len(part_instances), batch_size):
        batch = part_instances[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        _now = timezone.now()
        for _p in batch:
            _p.updated_at = _now
        pgbulk.upsert(
            src_models.KeystoneParts,
            batch,
            unique_fields=["vcpn", "brand"],
            update_fields=KEYSTONE_PARTS_UPDATE_FIELDS,
            returning=False,
        )
        total += len(batch)
        logger.info("{} Upserted Keystone parts batch {}/{} ({} rows).".format(
            _LOG_PREFIX, batch_num, num_batches, len(batch),
        ))
        connection.close()
        if batch_num < num_batches:
            time.sleep(batch_delay_seconds)
    return total


def _pgbulk_upsert_keystone_company_pricing_batches(
    pricing_instances: typing.List[src_models.KeystoneCompanyPricing],
    batch_size: int,
    batch_delay_seconds: float,
) -> int:
    if not pricing_instances:
        return 0
    num_batches = (len(pricing_instances) + batch_size - 1) // batch_size
    total = 0
    for i in range(0, len(pricing_instances), batch_size):
        batch = pricing_instances[i : i + batch_size]
        batch_num = (i // batch_size) + 1
        pgbulk.upsert(
            src_models.KeystoneCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["jobber_price", "cost", "core_charge", "updated_at"],
            returning=False,
        )
        total += len(batch)
        logger.info("{} Upserted KeystoneCompanyPricing batch {}/{} ({} rows).".format(
            _LOG_PREFIX, batch_num, num_batches, len(batch),
        ))
        connection.close()
        if batch_num < num_batches:
            time.sleep(batch_delay_seconds)
    return total


def fetch_and_save_all_keystone_brand_parts() -> None:
    """
    Fetch the Keystone inventory CSV and upsert parts for each brand that has
    BrandKeystoneBrandMapping (similar to Turn 14 flow).
    Uses BrandKeystoneBrandMapping as source of truth; BrandProviders is optional.

    When a Keystone CompanyProviders row is marked ``primary=True``, shared catalog rows
    (:class:`~src.models.KeystoneParts`) are upserted only for that company’s FTP pull.
    Every active company still upserts :class:`~src.models.KeystoneCompanyPricing` for
    its own credentials. If no primary is set, catalog upserts run for every company
    (legacy behaviour).
    """
    logger.info("{} Fetching all Keystone brand parts.".format(_LOG_PREFIX))

    # Prefer provider by id=4, fallback to kind=KEYSTONE
    keystone_provider = src_models.Providers.objects.filter(id=KEYSTONE_PROVIDER_ID).first()
    if not keystone_provider:
        keystone_provider = src_models.Providers.objects.filter(
            kind=src_enums.BrandProviderKind.KEYSTONE.value,
        ).first()

    if not keystone_provider:
        logger.info("{} No Keystone provider found.".format(_LOG_PREFIX))
        return

    # Build mappings from BrandKeystoneBrandMapping (source of truth)
    # Optionally filter to brands that also have BrandProviders for Keystone
    all_mappings = src_models.BrandKeystoneBrandMapping.objects.select_related(
        "brand", "keystone_brand"
    )
    brand_provider_brand_ids = set(
        src_models.BrandProviders.objects.filter(provider=keystone_provider).values_list(
            "brand_id", flat=True
        )
    )

    brand_mappings = {}
    for mapping in all_mappings:
        brand = mapping.brand
        if brand.status_name != src_enums.BrandProviderStatus.ACTIVE.name:
            logger.info("{} Brand {} status is not active.".format(_LOG_PREFIX, brand.name))
            continue
        # If BrandProviders has entries, only include brands that are linked; else include all
        if brand_provider_brand_ids and brand.id not in brand_provider_brand_ids:
            logger.debug("{} Brand {} not in BrandProviders for Keystone. Skipping.".format(
                _LOG_PREFIX, brand.name
            ))
            continue
        brand_mappings[mapping.keystone_brand.name] = mapping.keystone_brand

    if not brand_mappings:
        logger.warning("{} No brand mappings found. Skipping parts fetch.".format(_LOG_PREFIX))
        return

    company_providers = list(
        src_models.CompanyProviders.objects.filter(
            provider=keystone_provider,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).select_related("company")
    )
    if not company_providers:
        logger.warning("{} No active Keystone company providers found. Skipping.".format(_LOG_PREFIX))
        return

    has_keystone_primary = any(cp.primary for cp in company_providers)
    primary_count = sum(1 for cp in company_providers if cp.primary)
    if primary_count > 1:
        logger.warning(
            "{} Multiple Keystone CompanyProviders have primary=True (count={}); "
            "KeystoneParts upserts will run for each primary.".format(_LOG_PREFIX, primary_count)
        )
    if has_keystone_primary:
        logger.info(
            "{} KeystoneParts (shared catalog) upserts: only primary CompanyProvider; "
            "KeystoneCompanyPricing: every active company.".format(_LOG_PREFIX)
        )
    else:
        logger.warning(
            "{} No Keystone CompanyProviders with primary=True; upserting KeystoneParts "
            "for every company (set one primary to ingest the catalog once).".format(_LOG_PREFIX)
        )

    company_providers_ordered = sorted(
        company_providers,
        key=lambda cp: (not cp.primary, (cp.company.name or "")),
    )

    total_parts = 0
    total_pricing = 0

    for company_provider in company_providers_ordered:
        company = company_provider.company
        should_upsert_keystone_parts = company_provider.primary or not has_keystone_primary
        try:
            ftp_client = keystone_client.KeystoneFTPClient(credentials=company_provider.credentials)
        except ValueError as e:
            logger.error("{} Invalid Keystone credentials company={}: {}.".format(
                _LOG_PREFIX, company.name, str(e),
            ))
            continue

        try:
            records = ftp_client.get_inventory_records()
        except keystone_exceptions.KeystoneException as e:
            logger.error("{} Keystone error company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
            continue

        if not records:
            logger.warning("{} No inventory records company={}.".format(_LOG_PREFIX, company.name))
            continue

        logger.info("{} company={}: {} CSV rows, {} mapped brands.".format(
            _LOG_PREFIX, company.name, len(records), len(brand_mappings),
        ))

        if should_upsert_keystone_parts:
            part_instances = _transform_parts_data(records, brand_mappings, omit_pricing_for_parts=True)
            if not part_instances:
                logger.warning("{} No part instances company={}.".format(_LOG_PREFIX, company.name))
                continue

            try:
                total_parts += _pgbulk_upsert_keystone_parts_batches(
                    part_instances,
                    KEYSTONE_PGBULK_BATCH_SIZE,
                    KEYSTONE_PGBULK_BATCH_DELAY_SECONDS,
                )
            except Exception as e:
                logger.error("{} Keystone parts upsert failed company={}: {}.".format(
                    _LOG_PREFIX, company.name, str(e),
                ))
                raise
        else:
            logger.info(
                "{} Skipping KeystoneParts upsert for company={} (non-primary; catalog from primary only).".format(
                    _LOG_PREFIX, company.name,
                )
            )

        part_lookup = _vcpn_brand_id_lookup_for_records(records, brand_mappings)
        pricing_instances = _build_keystone_company_pricing_instances(
            records, brand_mappings, company, part_lookup,
        )
        try:
            total_pricing += _pgbulk_upsert_keystone_company_pricing_batches(
                pricing_instances,
                KEYSTONE_PGBULK_BATCH_SIZE,
                KEYSTONE_PGBULK_BATCH_DELAY_SECONDS,
            )
        except Exception as e:
            logger.error("{} KeystoneCompanyPricing upsert failed company={}: {}.".format(
                _LOG_PREFIX, company.name, str(e),
            ))
            raise

    logger.info(
        "{} Finished Keystone brand parts sync: {} part row upserts (batched), {} company-pricing upserts (batched).".format(
            _LOG_PREFIX, total_parts, total_pricing,
        )
    )


def fetch_and_save_all_keystone_brands_and_parts() -> None:
    """
    Fetch the Keystone inventory CSV per company (credentials) and upsert all brands and parts.
    Catalog fields on KeystoneParts; prices on KeystoneCompanyPricing per company.

    Shared KeystoneParts rows are upserted only from the ``primary=True`` CompanyProvider when
    one exists; each company still gets KeystoneCompanyPricing from its own feed. With no primary,
    catalog upserts run for every company (legacy).
    """
    logger.info("{} Started full Keystone brands and parts sync (per company).".format(_LOG_PREFIX))

    company_providers = list(
        src_models.CompanyProviders.objects.filter(
            provider__kind=src_enums.BrandProviderKind.KEYSTONE.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).select_related("company")
    )
    if not company_providers:
        logger.info("{} No active Keystone company providers found.".format(_LOG_PREFIX))
        return

    has_keystone_primary = any(cp.primary for cp in company_providers)
    primary_count = sum(1 for cp in company_providers if cp.primary)
    if primary_count > 1:
        logger.warning(
            "{} Multiple Keystone CompanyProviders have primary=True (count={}); "
            "KeystoneParts upserts will run for each primary.".format(_LOG_PREFIX, primary_count)
        )
    if has_keystone_primary:
        logger.info(
            "{} KeystoneParts upserts: primary CompanyProvider only; "
            "KeystoneCompanyPricing: every company.".format(_LOG_PREFIX)
        )
    else:
        logger.warning(
            "{} No Keystone CompanyProviders with primary=True; upserting KeystoneParts "
            "for every company (set one primary to ingest the catalog once).".format(_LOG_PREFIX)
        )

    company_providers_ordered = sorted(
        company_providers,
        key=lambda cp: (not cp.primary, (cp.company.name or "")),
    )

    brands_synced_once = False
    total_parts = 0
    total_pricing = 0

    for company_provider in company_providers_ordered:
        company = company_provider.company
        try:
            ftp_client = keystone_client.KeystoneFTPClient(credentials=company_provider.credentials)
        except ValueError as e:
            logger.error("{} Invalid Keystone credentials company={}: {}.".format(
                _LOG_PREFIX, company.name, str(e),
            ))
            continue

        try:
            records = ftp_client.get_inventory_records()
        except keystone_exceptions.KeystoneException as e:
            logger.error("{} Keystone error company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
            continue

        if not records:
            logger.warning("{} No inventory records company={}.".format(_LOG_PREFIX, company.name))
            continue

        brand_data = {}
        for row in records:
            name = _clean_csv_value(row.get("VendorName"))
            if name and name not in brand_data:
                brand_data[name] = _clean_csv_value(row.get("AAIACode"))

        if not brands_synced_once and brand_data:
            brand_instances = [
                src_models.KeystoneBrand(
                    external_id=name,
                    name=name,
                    aaia_code=brand_data.get(name) or "",
                )
                for name in sorted(brand_data.keys())
            ]
            try:
                pgbulk.upsert(
                    src_models.KeystoneBrand,
                    brand_instances,
                    unique_fields=["external_id"],
                    update_fields=["name", "aaia_code", "updated_at"],
                    returning=False,
                )
                logger.info("{} Upserted {} Keystone brands (from first successful company feed).".format(
                    _LOG_PREFIX, len(brand_instances),
                ))
            except Exception as e:
                logger.error("{} Error upserting Keystone brands: {}.".format(_LOG_PREFIX, str(e)))
                raise
            brands_synced_once = True

        keystone_brands = {
            b.name: b
            for b in src_models.KeystoneBrand.objects.filter(external_id__in=brand_data.keys())
        }

        should_upsert_keystone_parts = company_provider.primary or not has_keystone_primary

        if should_upsert_keystone_parts:
            part_instances = _transform_parts_data(records, keystone_brands, omit_pricing_for_parts=True)
            if not part_instances:
                continue

            try:
                total_parts += _pgbulk_upsert_keystone_parts_batches(
                    part_instances,
                    KEYSTONE_PGBULK_BATCH_SIZE,
                    KEYSTONE_PGBULK_BATCH_DELAY_SECONDS,
                )
            except Exception as e:
                logger.error("{} Keystone parts upsert failed company={}: {}.".format(
                    _LOG_PREFIX, company.name, str(e),
                ))
                raise
        else:
            logger.info(
                "{} Skipping KeystoneParts upsert for company={} (non-primary; catalog from primary only).".format(
                    _LOG_PREFIX, company.name,
                )
            )

        part_lookup = _vcpn_brand_id_lookup_for_records(records, keystone_brands)
        pricing_instances = _build_keystone_company_pricing_instances(
            records, keystone_brands, company, part_lookup,
        )
        try:
            total_pricing += _pgbulk_upsert_keystone_company_pricing_batches(
                pricing_instances,
                KEYSTONE_PGBULK_BATCH_SIZE,
                KEYSTONE_PGBULK_BATCH_DELAY_SECONDS,
            )
        except Exception as e:
            logger.error("{} KeystoneCompanyPricing upsert failed company={}: {}.".format(
                _LOG_PREFIX, company.name, str(e),
            ))
            raise

    logger.info(
        "{} Completed full Keystone sync: parts upserts={}, company pricing upserts={}.".format(
            _LOG_PREFIX, total_parts, total_pricing,
        )
    )


def _transform_parts_data(
    records: typing.List[typing.Dict],
    brand_name_to_keystone_brand: typing.Dict[str, src_models.KeystoneBrand],
    omit_pricing_for_parts: bool = False,
) -> typing.List[src_models.KeystoneParts]:
    part_instances = []

    for row in records:
        try:
            vendor_name = _clean_csv_value(row.get("VendorName"))
            if not vendor_name:
                continue

            keystone_brand = brand_name_to_keystone_brand.get(vendor_name)
            if not keystone_brand:
                logger.info("{} Skipping row with missing brand - part number: {}".format(_LOG_PREFIX, row.get('PartNumber')))
                continue

            vcpn = _clean_csv_value(row.get("VCPN"))
            if not vcpn:
                logger.warning("{} Skipping row with missing VCPN: {}.".format(_LOG_PREFIX, row))
                continue

            if omit_pricing_for_parts:
                jobber_price = None
                cost = None
                core_charge = None
            else:
                jobber_price = _safe_decimal(row.get("JobberPrice"))
                cost = _safe_decimal(row.get("Cost"))
                core_charge = _safe_decimal(row.get("CoreCharge"))

            part_instance = src_models.KeystoneParts(
                vcpn=vcpn,
                brand=keystone_brand,
                vendor_code=_clean_csv_value(row.get("VendorCode")),
                part_number=_clean_csv_value(row.get("PartNumber")),
                manufacturer_part_no=_clean_csv_value(row.get("ManufacturerPartNo")),
                long_description=_clean_csv_value(row.get("LongDescription")),
                jobber_price=jobber_price,
                cost=cost,
                upsable=_safe_bool(row.get("UPSable")),
                core_charge=core_charge,
                case_qty=_safe_int(row.get("CaseQty")),
                is_non_returnable=_safe_bool(row.get("IsNonReturnable")),
                prop65_toxicity=_clean_csv_value(row.get("Prop65Toxicity")),
                upc_code=_clean_csv_value(row.get("UPCCode")),
                weight=_safe_decimal(row.get("Weight")),
                height=_safe_decimal(row.get("Height")),
                length=_safe_decimal(row.get("Length")),
                width=_safe_decimal(row.get("Width")),
                aaia_code=_clean_csv_value(row.get("AAIACode")),
                is_hazmat=_safe_bool(row.get("IsHazmat")),
                is_chemical=_safe_bool(row.get("IsChemical")),
                ups_ground_assessorial=_safe_decimal(row.get("UPS_Ground_Assessorial")),
                us_ltl=_safe_decimal(row.get("US_LTL")),
                east_qty=_safe_int(row.get("EastQty")),
                midwest_qty=_safe_int(row.get("MidwestQty")),
                california_qty=_safe_int(row.get("CaliforniaQty")),
                southeast_qty=_safe_int(row.get("SoutheastQty")),
                pacific_nw_qty=_safe_int(row.get("PacificNWQty")),
                texas_qty=_safe_int(row.get("TexasQty")),
                great_lakes_qty=_safe_int(row.get("GreatLakesQty")),
                florida_qty=_safe_int(row.get("FloridaQty")),
                total_qty=_safe_int(row.get("TotalQty")),
                kit_components=_clean_csv_value(row.get("KitComponents")),
                is_kit=_safe_bool(row.get("IsKit")),
                raw_data={
                    k: (None if (v is None or (isinstance(v, float) and pd.isna(v))) else v)
                    for k, v in row.items()
                },
            )
            part_instances.append(part_instance)

        except Exception as e:
            logger.warning("{} Error transforming row {}: {}. Skipping.".format(
                _LOG_PREFIX, row, str(e)
            ))
            continue

    return part_instances


def _vcpn_brand_id_lookup_for_records(
    records: typing.List[typing.Dict],
    brand_name_to_keystone_brand: typing.Dict[str, src_models.KeystoneBrand],
) -> typing.Dict[typing.Tuple[str, int], int]:
    """Map (vcpn, keystone_brand.id) -> KeystoneParts.id for rows that resolve to a known brand."""
    vcpns: typing.Set[str] = set()
    brand_ids: typing.Set[int] = set()
    for row in records:
        vendor_name = _clean_csv_value(row.get("VendorName"))
        kb = brand_name_to_keystone_brand.get(vendor_name or "")
        if not kb:
            continue
        vcpn = _clean_csv_value(row.get("VCPN"))
        if not vcpn:
            continue
        vcpns.add(vcpn)
        brand_ids.add(kb.id)
    if not vcpns or not brand_ids:
        return {}
    lookup: typing.Dict[typing.Tuple[str, int], int] = {}
    for p in (
        src_models.KeystoneParts.objects.filter(vcpn__in=vcpns, brand_id__in=brand_ids)
        .only("id", "vcpn", "brand_id")
        .iterator(chunk_size=5000)
    ):
        lookup[(p.vcpn, p.brand_id)] = p.id
    return lookup


def _build_keystone_company_pricing_instances(
    records: typing.List[typing.Dict],
    brand_name_to_keystone_brand: typing.Dict[str, src_models.KeystoneBrand],
    company: src_models.Company,
    part_id_by_vcpn_brand: typing.Dict[typing.Tuple[str, int], int],
) -> typing.List[src_models.KeystoneCompanyPricing]:
    instances = []
    for row in records:
        vendor_name = _clean_csv_value(row.get("VendorName"))
        kb = brand_name_to_keystone_brand.get(vendor_name or "")
        if not kb:
            continue
        vcpn = _clean_csv_value(row.get("VCPN"))
        if not vcpn:
            continue
        part_id = part_id_by_vcpn_brand.get((vcpn, kb.id))
        if not part_id:
            continue
        instances.append(
            src_models.KeystoneCompanyPricing(
                part_id=part_id,
                company=company,
                jobber_price=_safe_decimal(row.get("JobberPrice")),
                cost=_safe_decimal(row.get("Cost")),
                core_charge=_safe_decimal(row.get("CoreCharge")),
            )
        )
    return instances


def sync_keystone_catalog_and_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    One company's Keystone CSV ingest: brands, parts, and KeystoneCompanyPricing (FTP credentials row).
    """
    company_provider = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.KEYSTONE.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company")
        .first()
    )
    if not company_provider:
        logger.warning(
            "{} No active Keystone CompanyProviders id={}. Skipping.".format(_LOG_PREFIX, company_provider_id)
        )
        return

    company = company_provider.company

    try:
        ftp_client = keystone_client.KeystoneFTPClient(credentials=company_provider.credentials)
    except ValueError as e:
        logger.error("{} Invalid Keystone credentials company={}: {}.".format(
            _LOG_PREFIX, company.name, str(e),
        ))
        raise

    try:
        records = ftp_client.get_inventory_records()
    except keystone_exceptions.KeystoneException as e:
        logger.error("{} Keystone error company={}: {}.".format(_LOG_PREFIX, company.name, str(e)))
        raise

    if not records:
        logger.warning("{} No inventory records company={}.".format(_LOG_PREFIX, company.name))
        return

    brand_data = {}
    for row in records:
        name = _clean_csv_value(row.get("VendorName"))
        if name and name not in brand_data:
            brand_data[name] = _clean_csv_value(row.get("AAIACode"))

    if brand_data:
        brand_instances = [
            src_models.KeystoneBrand(
                external_id=name,
                name=name,
                aaia_code=brand_data.get(name) or "",
            )
            for name in sorted(brand_data.keys())
        ]
        pgbulk.upsert(
            src_models.KeystoneBrand,
            brand_instances,
            unique_fields=["external_id"],
            update_fields=["name", "aaia_code", "updated_at"],
            returning=False,
        )
        logger.info("{} Upserted {} Keystone brands (company={}).".format(
            _LOG_PREFIX, len(brand_instances), company.name,
        ))

    keystone_brands = {
        b.name: b
        for b in src_models.KeystoneBrand.objects.filter(external_id__in=brand_data.keys())
    }

    part_instances = _transform_parts_data(records, keystone_brands, omit_pricing_for_parts=True)
    if not part_instances:
        logger.warning("{} No part instances company={}.".format(_LOG_PREFIX, company.name))
        return

    total_parts = _pgbulk_upsert_keystone_parts_batches(
        part_instances,
        KEYSTONE_PGBULK_BATCH_SIZE,
        KEYSTONE_PGBULK_BATCH_DELAY_SECONDS,
    )
    part_lookup = _vcpn_brand_id_lookup_for_records(records, keystone_brands)
    pricing_instances = _build_keystone_company_pricing_instances(
        records, keystone_brands, company, part_lookup,
    )
    total_pricing = _pgbulk_upsert_keystone_company_pricing_batches(
        pricing_instances,
        KEYSTONE_PGBULK_BATCH_SIZE,
        KEYSTONE_PGBULK_BATCH_DELAY_SECONDS,
    )
    logger.info(
        "{} Keystone sync for company_provider id={}: parts upserts={}, company pricing upserts={}.".format(
            _LOG_PREFIX, company_provider_id, total_parts, total_pricing,
        )
    )
