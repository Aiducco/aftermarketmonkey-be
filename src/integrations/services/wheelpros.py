"""
WheelPros feed integration: SFTP CSV feed with brand, part, pricing, and warehouse inventory.
Syncs WheelProsBrand, WheelProsPart, and BrandWheelProsBrandMapping.
"""
import logging
import math
import typing
from datetime import datetime
from decimal import Decimal

import pgbulk

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.wheelpros import client as wheelpros_client
from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-SERVICES]"


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        s = str(value).strip()
        if not s or s.lower() in ("nan", ""):
            return None
        return Decimal(s)
    except Exception:
        return None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        s = str(value).strip()
        if not s or s.lower() in ("nan", ""):
            return None
        return int(float(s))
    except Exception:
        return None


def _safe_str(value: typing.Any, max_len: typing.Optional[int] = None) -> typing.Optional[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    s = str(value).strip()
    if not s:
        return None
    if max_len and len(s) > max_len:
        return s[:max_len]
    return s


def _safe_datetime(value: typing.Any):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime()
        if isinstance(value, str):
            for fmt in ("%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(value.strip(), fmt)
                except ValueError:
                    continue
        return None
    except Exception:
        return None


def _row_get(row: typing.Dict, *keys: str) -> typing.Any:
    keys_lower = [k.lower() for k in keys]
    for col, val in row.items():
        if col and str(col).strip().lower() in keys_lower:
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                return val
    return None


def _row_key(row: typing.Dict, *keys: str) -> typing.Optional[str]:
    value = _row_get(row, *keys)
    return _safe_str(value) if value is not None else None


def _find_brand_for_wheelpros_brand(wheelpros_brand: src_models.WheelProsBrand) -> typing.Optional[src_models.Brands]:
    name = (wheelpros_brand.name or "").strip()
    if name:
        return src_models.Brands.objects.filter(name__iexact=name).first()
    return None


def sync_unmapped_wheelpros_brands_to_brands() -> typing.List[src_models.WheelProsBrand]:
    """
    For each WheelProsBrand that does not yet have a BrandWheelProsBrandMapping:
    find or create a Brand by name; then add BrandWheelProsBrandMapping,
    BrandProviders (WheelPros), and CompanyBrands (TICK_PERFORMANCE).
    """
    logger.info("{} Syncing unmapped WheelPros brands to Brands.".format(_LOG_PREFIX))

    wheelpros_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    if not wheelpros_provider:
        logger.warning("{} WheelPros provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    _, cp_created = src_models.CompanyProviders.objects.get_or_create(
        company=tick_company,
        provider=wheelpros_provider,
        defaults={"credentials": {}, "primary": False},
    )
    if cp_created:
        logger.info("{} Created CompanyProviders for TICK_PERFORMANCE + WheelPros.".format(_LOG_PREFIX))

    mapped_ids = set(
        src_models.BrandWheelProsBrandMapping.objects.values_list("wheelpros_brand_id", flat=True).distinct()
    )
    unmapped_wheelpros_brands = list(
        src_models.WheelProsBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped_wheelpros_brands:
        logger.info("{} No unmapped WheelPros brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    created_mappings = 0
    created_brand_providers = 0
    created_company_brands = 0
    created_brands = 0

    for wheelpros_brand in unmapped_wheelpros_brands:
        brand = _find_brand_for_wheelpros_brand(wheelpros_brand)
        if not brand:
            name_upper = (wheelpros_brand.name or "").strip().upper()
            if not name_upper:
                name_upper = "BRAND_{}".format(wheelpros_brand.external_id)
            brand = src_models.Brands.objects.create(
                name=name_upper,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            created_brands += 1
            logger.info(
                "{} Created new Brand: name={!r} for WheelProsBrand id={}.".format(
                    _LOG_PREFIX, name_upper, wheelpros_brand.id
                )
            )

        mapping, mapping_created = src_models.BrandWheelProsBrandMapping.objects.get_or_create(
            brand=brand,
            wheelpros_brand=wheelpros_brand,
        )
        if mapping_created:
            created_mappings += 1

        bp, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=wheelpros_provider,
        )
        if bp_created:
            created_brand_providers += 1

        cb, cb_created = src_models.CompanyBrands.objects.get_or_create(
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
        "{} Sync complete. Brands created: {}, BrandWheelProsBrandMapping: {}, BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX,
            created_brands,
            created_mappings,
            created_brand_providers,
            created_company_brands,
        )
    )
    return unmapped_wheelpros_brands


_KNOWN_FEED_COLUMNS = frozenset(
    c.lower()
    for c in (
        "PartNumber",
        "PartDescription",
        "Brand",
        "DisplayStyleNo",
        "Finish",
        "Size",
        "BoltPattern",
        "Offset",
        "CenterBore",
        "LoadRating",
        "ShippingWeight",
        "ImageURL",
        "InvOrderType",
        "Style",
        "TotalQOH",
        "MSRP_USD",
        "MAP_USD",
        "RunDate",
    )
)


def _warehouse_inventory_from_row(row: typing.Dict) -> typing.Optional[typing.Dict[str, typing.Optional[int]]]:
    """
    Build warehouse availability from WheelPros warehouse columns (e.g. 1001, 1002, 1003).
    Keeps warehouse codes as strings. Known feed columns are excluded (case-insensitive).
    """
    out = {}
    for col, val in row.items():
        col_name = str(col).strip()
        if not col_name or col_name.lower() in _KNOWN_FEED_COLUMNS:
            continue
        if not col_name.isdigit():
            continue
        qty = _safe_int(val)
        if qty is None:
            continue
        out[col_name] = qty
    return out or None


def _row_to_wheelpros_part(row: typing.Dict, brand: src_models.WheelProsBrand) -> src_models.WheelProsPart:
    part_number = _row_key(row, "PartNumber") or ""
    part_description = _safe_str(_row_get(row, "PartDescription"))
    display_style_no = _safe_str(_row_get(row, "DisplayStyleNo"), 255)
    finish = _safe_str(_row_get(row, "Finish"), 255)
    size = _safe_str(_row_get(row, "Size"), 255)
    bolt_pattern = _safe_str(_row_get(row, "BoltPattern"), 255)
    offset = _safe_str(_row_get(row, "Offset"), 255)
    center_bore = _safe_str(_row_get(row, "CenterBore"), 255)
    load_rating = _safe_str(_row_get(row, "LoadRating"), 255)
    shipping_weight = _safe_decimal(_row_get(row, "ShippingWeight"))
    image_url = _safe_str(_row_get(row, "ImageURL"))
    inv_order_type = _safe_str(_row_get(row, "InvOrderType"), 255)
    style = _safe_str(_row_get(row, "Style"), 255)
    total_qoh = _safe_int(_row_get(row, "TotalQOH"))
    msrp_usd = _safe_decimal(_row_get(row, "MSRP_USD"))
    map_usd = _safe_decimal(_row_get(row, "MAP_USD"))
    run_date = _safe_datetime(_row_get(row, "RunDate"))
    warehouse_availability = _warehouse_inventory_from_row(row)
    raw_data = {
        k: (None if (v is None or (isinstance(v, float) and math.isnan(v))) else v)
        for k, v in row.items()
    }

    return src_models.WheelProsPart(
        brand=brand,
        part_number=part_number,
        part_description=part_description,
        display_style_no=display_style_no,
        finish=finish,
        size=size,
        bolt_pattern=bolt_pattern,
        offset=offset,
        center_bore=center_bore,
        load_rating=load_rating,
        shipping_weight=shipping_weight,
        image_url=image_url,
        inv_order_type=inv_order_type,
        style=style,
        total_qoh=total_qoh,
        msrp_usd=msrp_usd,
        map_usd=map_usd,
        run_date=run_date,
        warehouse_availability=warehouse_availability,
        raw_data=raw_data,
    )


def _get_wheelpros_credentials() -> typing.Optional[typing.Dict]:
    """Get SFTP credentials from primary CompanyProviders (TICK_PERFORMANCE + WheelPros), or None for settings fallback."""
    tick = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick:
        return None
    provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    if not provider:
        return None
    cp = src_models.CompanyProviders.objects.filter(
        company=tick,
        provider=provider,
        primary=True,
    ).first()
    if not cp:
        cp = src_models.CompanyProviders.objects.filter(
            company=tick,
            provider=provider,
        ).first()
    if not cp or not cp.credentials:
        return None
    return cp.credentials


def fetch_and_save_wheelpros(
    local_file_path: typing.Optional[str] = None,
    download: bool = True,
    local_only: bool = False,
) -> None:
    """
    Fetch the WheelPros CSV and upsert WheelProsBrand and WheelProsPart rows.
    Uses CompanyProviders credentials when available; otherwise falls back to settings.
    """
    logger.info("{} Starting WheelPros feed sync.".format(_LOG_PREFIX))

    credentials = _get_wheelpros_credentials() if not local_only else None
    client = wheelpros_client.WheelProsSFTPClient(
        credentials=credentials,
        local_file_path=local_file_path,
        require_credentials=not local_only,
    )
    try:
        records = client.get_feed_records(force_download=download, local_only=local_only)
    except wheelpros_exceptions.WheelProsException as e:
        logger.error("{} WheelPros feed error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    if not records:
        logger.warning("{} No rows returned from WheelPros feed.".format(_LOG_PREFIX))
        return

    brand_names = set()
    for row in records:
        name = _row_key(row, "Brand")
        if name:
            brand_names.add(name)

    brand_instances = []
    for name in sorted(brand_names):
        brand_instances.append(
            src_models.WheelProsBrand(
                external_id=name,
                name=name,
            )
        )
    if brand_instances:
        try:
            pgbulk.upsert(
                src_models.WheelProsBrand,
                brand_instances,
                unique_fields=["external_id"],
                update_fields=["name"],
                returning=False,
            )
            logger.info("{} Upserted {} WheelPros brands.".format(_LOG_PREFIX, len(brand_instances)))
        except Exception as e:
            logger.error("{} Error upserting WheelPros brands: {}.".format(_LOG_PREFIX, str(e)))
            raise

    brand_to_model = {
        b.name: b
        for b in src_models.WheelProsBrand.objects.filter(external_id__in=brand_names)
    }

    seen_by_key = {}
    for row in records:
        part_number = _row_key(row, "PartNumber")
        brand_name = _row_key(row, "Brand")
        if not part_number or not brand_name:
            continue
        brand = brand_to_model.get(brand_name)
        if not brand:
            continue
        seen_by_key[(brand.id, part_number)] = _row_to_wheelpros_part(row, brand)

    part_instances = list(seen_by_key.values())
    if not part_instances:
        logger.warning("{} No WheelPros part instances created.".format(_LOG_PREFIX))
        return

    try:
        pgbulk.upsert(
            src_models.WheelProsPart,
            part_instances,
            unique_fields=["brand", "part_number"],
            update_fields=[
                "part_description",
                "display_style_no",
                "finish",
                "size",
                "bolt_pattern",
                "offset",
                "center_bore",
                "load_rating",
                "shipping_weight",
                "image_url",
                "inv_order_type",
                "style",
                "total_qoh",
                "msrp_usd",
                "map_usd",
                "run_date",
                "warehouse_availability",
                "raw_data",
            ],
            returning=False,
        )
        logger.info("{} Upserted {} WheelPros parts.".format(_LOG_PREFIX, len(part_instances)))
    except Exception as e:
        logger.error("{} Error upserting WheelPros parts: {}.".format(_LOG_PREFIX, str(e)))
        raise

    logger.info("{} WheelPros feed sync complete.".format(_LOG_PREFIX))
