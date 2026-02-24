import logging
import re
import time
import typing
from decimal import Decimal

import pandas as pd
import pgbulk

from src import enums as src_enums
from src import models as src_models
from django.db import connection

from src.integrations.clients.keystone import client as keystone_client
from src.integrations.clients.keystone import exceptions as keystone_exceptions

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
            update_fields=["name", "aaia_code"],
            returning=True,
        )
        logger.info("{} Successfully upserted {} Keystone brands.".format(
            _LOG_PREFIX, len(upserted) if upserted else 0
        ))
    except Exception as e:
        logger.error("{} Error during bulk upsert: {}.".format(_LOG_PREFIX, str(e)))
        raise


# Keystone provider id (use when provider record has id=4)
KEYSTONE_PROVIDER_ID = 4


def fetch_and_save_all_keystone_brand_parts() -> None:
    """
    Fetch the Keystone inventory CSV and upsert parts for each brand that has
    BrandKeystoneBrandMapping (similar to Turn 14 flow).
    Uses BrandKeystoneBrandMapping as source of truth; BrandProviders is optional.
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

    primary_provider = src_models.CompanyProviders.objects.filter(
        provider=keystone_provider,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        primary=True,
    ).first()

    if not primary_provider:
        logger.warning("{} No Keystone active primary provider found. Skipping.".format(_LOG_PREFIX))
        return

    credentials = primary_provider.credentials
    try:
        ftp_client = keystone_client.KeystoneFTPClient(credentials=credentials)
    except ValueError as e:
        logger.error("{} Invalid credentials: {}.".format(_LOG_PREFIX, str(e)))
        raise

    try:
        records = ftp_client.get_inventory_records()
    except keystone_exceptions.KeystoneException as e:
        logger.error("{} Keystone error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    if not records:
        logger.warning("{} No inventory records returned.".format(_LOG_PREFIX))
        return

    # Filter records to only brands we have mappings for
    filtered_records = [
        r for r in records
        if _clean_csv_value(r.get("VendorName")) in brand_mappings
    ]

    logger.info("{} Filtered to {} records for {} brands.".format(
        _LOG_PREFIX, len(filtered_records), len(brand_mappings)
    ))

    part_instances = _transform_parts_data(filtered_records, brand_mappings)

    if not part_instances:
        logger.warning("{} No valid part instances created.".format(_LOG_PREFIX))
        return

    BATCH_SIZE = 1000
    BATCH_DELAY_SECONDS = 0.5
    total_upserted = 0
    update_fields = [
        "vendor_code", "part_number", "manufacturer_part_no", "long_description",
        "jobber_price", "cost", "upsable", "core_charge", "case_qty",
        "is_non_returnable", "prop65_toxicity", "upc_code", "weight",
        "height", "length", "width", "aaia_code", "is_hazmat", "is_chemical",
        "ups_ground_assessorial", "us_ltl", "east_qty", "midwest_qty",
        "california_qty", "southeast_qty", "pacific_nw_qty", "texas_qty",
        "great_lakes_qty", "florida_qty", "total_qty", "kit_components",
        "is_kit", "raw_data",
    ]

    num_batches = (len(part_instances) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(part_instances), BATCH_SIZE):
        batch = part_instances[i : i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        try:
            pgbulk.upsert(
                src_models.KeystoneParts,
                batch,
                unique_fields=["vcpn", "brand"],
                update_fields=update_fields,
                returning=False,
            )
            total_upserted += len(batch)
            logger.info("{} Upserted batch {}/{} ({} parts).".format(
                _LOG_PREFIX, batch_num, num_batches, len(batch)
            ))
        except Exception as e:
            logger.error("{} Error during bulk upsert batch at offset {}: {}.".format(
                _LOG_PREFIX, i, str(e)
            ))
            raise
        connection.close()
        if batch_num < num_batches:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Successfully upserted {} Keystone parts total.".format(_LOG_PREFIX, total_upserted))


def fetch_and_save_all_keystone_brands_and_parts() -> None:
    """
    Fetch the Keystone inventory CSV and upsert all brands and all parts.
    Use this when you want to sync the full inventory regardless of brand mappings.
    """
    logger.info("{} Started full Keystone brands and parts sync.".format(_LOG_PREFIX))

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
        logger.error("{} Invalid credentials: {}.".format(_LOG_PREFIX, str(e)))
        raise

    try:
        records = ftp_client.get_inventory_records()
    except keystone_exceptions.KeystoneException as e:
        logger.error("{} Keystone error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    if not records:
        logger.warning("{} No inventory records returned.".format(_LOG_PREFIX))
        return

    # Extract unique brands
    brand_data = {}
    for row in records:
        name = _clean_csv_value(row.get("VendorName"))
        if name and name not in brand_data:
            brand_data[name] = _clean_csv_value(row.get("AAIACode"))

    brand_instances = [
        src_models.KeystoneBrand(
            external_id=name,
            name=name,
            aaia_code=brand_data.get(name) or "",
        )
        for name in sorted(brand_data.keys())
    ]

    if brand_instances:
        try:
            pgbulk.upsert(
                src_models.KeystoneBrand,
                brand_instances,
                unique_fields=["external_id"],
                update_fields=["name", "aaia_code"],
            )
            logger.info("{} Upserted {} Keystone brands.".format(_LOG_PREFIX, len(brand_instances)))
        except Exception as e:
            logger.error("{} Error during bulk upsert brands: {}.".format(_LOG_PREFIX, str(e)))
            raise

    # Build VendorName -> KeystoneBrand (from DB)
    keystone_brands = {
        b.name: b
        for b in src_models.KeystoneBrand.objects.filter(external_id__in=brand_data.keys())
    }

    part_instances = _transform_parts_data(records, keystone_brands)

    if part_instances:
        BATCH_SIZE = 1000
        BATCH_DELAY_SECONDS = 0.5
        total_upserted = 0
        update_fields = [
            "vendor_code", "part_number", "manufacturer_part_no", "long_description",
            "jobber_price", "cost", "upsable", "core_charge", "case_qty",
            "is_non_returnable", "prop65_toxicity", "upc_code", "weight",
            "height", "length", "width", "aaia_code", "is_hazmat", "is_chemical",
            "ups_ground_assessorial", "us_ltl", "east_qty", "midwest_qty",
            "california_qty", "southeast_qty", "pacific_nw_qty", "texas_qty",
            "great_lakes_qty", "florida_qty", "total_qty", "kit_components",
            "is_kit", "raw_data",
        ]
        num_batches = (len(part_instances) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(part_instances), BATCH_SIZE):
            batch = part_instances[i : i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            try:
                pgbulk.upsert(
                    src_models.KeystoneParts,
                    batch,
                    unique_fields=["vcpn", "brand"],
                    update_fields=update_fields,
                    returning=False,
                )
                total_upserted += len(batch)
                logger.info("{} Upserted batch {}/{} ({} parts).".format(
                    _LOG_PREFIX, batch_num, num_batches, len(batch)
                ))
            except Exception as e:
                logger.error("{} Error during bulk upsert parts batch at offset {}: {}.".format(
                    _LOG_PREFIX, i, str(e)
                ))
                raise
            connection.close()
            if batch_num < num_batches:
                time.sleep(BATCH_DELAY_SECONDS)
        logger.info("{} Upserted {} Keystone parts total.".format(_LOG_PREFIX, total_upserted))

    logger.info("{} Completed full Keystone sync.".format(_LOG_PREFIX))


def _transform_parts_data(
    records: typing.List[typing.Dict],
    brand_name_to_keystone_brand: typing.Dict[str, src_models.KeystoneBrand],
) -> typing.List[src_models.KeystoneParts]:
    part_instances = []

    for row in records:
        try:
            vendor_name = _clean_csv_value(row.get("VendorName"))
            if not vendor_name:
                continue

            keystone_brand = brand_name_to_keystone_brand.get(vendor_name)
            if not keystone_brand:
                continue

            vcpn = _clean_csv_value(row.get("VCPN"))
            if not vcpn:
                logger.warning("{} Skipping row with missing VCPN: {}.".format(_LOG_PREFIX, row))
                continue

            part_instance = src_models.KeystoneParts(
                vcpn=vcpn,
                brand=keystone_brand,
                vendor_code=_clean_csv_value(row.get("VendorCode")),
                part_number=_clean_csv_value(row.get("PartNumber")),
                manufacturer_part_no=_clean_csv_value(row.get("ManufacturerPartNo")),
                long_description=_clean_csv_value(row.get("LongDescription")),
                jobber_price=_safe_decimal(row.get("JobberPrice")),
                cost=_safe_decimal(row.get("Cost")),
                upsable=_safe_bool(row.get("UPSable")),
                core_charge=_safe_decimal(row.get("CoreCharge")),
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
