"""
Rough Country feed integration: Excel jobber file (General, Vehicle Fitment, Discontinued).
Syncs RoughCountryBrand, RoughCountryPart, RoughCountryFitment; applies discontinued status.
"""
import logging
import math
import typing
from datetime import datetime
from decimal import Decimal

import pgbulk

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.rough_country import client as rough_country_client
from src.integrations.clients.rough_country import exceptions as rough_country_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-SERVICES]"

# Default brand name from manufacturer column
DEFAULT_RC_BRAND_NAME = "Rough Country"


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    try:
        if isinstance(value, Decimal):
            return value
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
        if isinstance(value, int):
            return value
        return int(float(value))
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


def _safe_date(value: typing.Any):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if hasattr(value, "date"):
        return value.date() if hasattr(value, "date") else value
    try:
        if isinstance(value, str):
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(value.strip()[:19], fmt).date()
                except ValueError:
                    continue
        return None
    except Exception:
        return None


def _row_key(row: typing.Dict, *keys: str) -> typing.Optional[str]:
    """Get first non-empty string value from row for given keys (case-insensitive match)."""
    v = _row_get(row, *keys)
    return _safe_str(v) if v is not None else None


def _row_get(row: typing.Dict, *keys: str) -> typing.Any:
    """Get value from row by key (case-insensitive column match). Returns first match."""
    keys_lower = [k.lower() for k in keys]
    for col, val in row.items():
        if col and str(col).strip().lower() in keys_lower:
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                return val
    return None


def _normalize_aaia_codes(aaia_code: typing.Optional[str]) -> typing.List[str]:
    """Split aaia_code by comma and return non-empty stripped parts."""
    if not aaia_code or not str(aaia_code).strip():
        return []
    return [p.strip() for p in str(aaia_code).split(",") if p and p.strip()]


def _find_brand_for_rough_country_brand(
    rc_brand: src_models.RoughCountryBrand,
) -> typing.Optional[src_models.Brands]:
    """
    Find existing Brand for a RoughCountryBrand: first by aaia_code (comma-separated, use first match),
    then by name (case-insensitive).
    """
    aaia_parts = _normalize_aaia_codes(rc_brand.aaia_code)
    for code in aaia_parts:
        brand = src_models.Brands.objects.filter(aaia_code=code).first()
        if brand:
            return brand
    name = (rc_brand.name or "").strip()
    if name:
        return src_models.Brands.objects.filter(name__iexact=name).first()
    return None


def sync_unmapped_rough_country_brands_to_brands() -> typing.List[src_models.RoughCountryBrand]:
    """
    For each RoughCountryBrand that does not yet have a BrandRoughCountryBrandMapping:
    find or create a Brand (match by aaia_code then name; create with uppercase name if new),
    then add BrandRoughCountryBrandMapping, BrandProviders (Rough Country), and CompanyBrands (TICK_PERFORMANCE).
    Returns the list of RoughCountryBrand instances that were synced.
    """
    logger.info("{} Syncing unmapped Rough Country brands to Brands.".format(_LOG_PREFIX))

    rc_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    ).first()
    if not rc_provider:
        logger.warning("{} Rough Country provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_rc_ids = set(
        src_models.BrandRoughCountryBrandMapping.objects.values_list(
            "rough_country_brand_id", flat=True
        ).distinct()
    )
    unmapped_rc_brands = list(
        src_models.RoughCountryBrand.objects.exclude(id__in=mapped_rc_ids).order_by("id")
    )

    if not unmapped_rc_brands:
        logger.info("{} No unmapped Rough Country brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info(
        "{} Found {} unmapped Rough Country brands.".format(_LOG_PREFIX, len(unmapped_rc_brands))
    )

    created_mappings = 0
    created_brand_providers = 0
    created_company_brands = 0
    created_brands = 0

    for rc_brand in unmapped_rc_brands:
        brand = _find_brand_for_rough_country_brand(rc_brand)
        if not brand:
            name_upper = (rc_brand.name or "").strip().upper()
            if not name_upper:
                name_upper = "BRAND_{}".format(rc_brand.external_id)
            aaia_primary = _normalize_aaia_codes(rc_brand.aaia_code)
            aaia_code = aaia_primary[0] if aaia_primary else None
            brand = src_models.Brands.objects.create(
                name=name_upper,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=aaia_code,
            )
            created_brands += 1
            logger.info(
                "{} Created new Brand: name={!r} aaia_code={!r} for RoughCountryBrand id={}.".format(
                    _LOG_PREFIX, name_upper, aaia_code, rc_brand.id
                )
            )

        mapping, mapping_created = src_models.BrandRoughCountryBrandMapping.objects.get_or_create(
            brand=brand,
            rough_country_brand=rc_brand,
        )
        if mapping_created:
            created_mappings += 1

        bp, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=rc_provider,
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
        "{} Sync complete. Brands created: {}, BrandRoughCountryBrandMapping: {}, "
        "BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX,
            created_brands,
            created_mappings,
            created_brand_providers,
            created_company_brands,
        )
    )
    return unmapped_rc_brands


def fetch_and_save_rough_country_brands() -> src_models.RoughCountryBrand:
    """
    Ensure the default Rough Country brand exists. Returns the brand.
    """
    logger.info("{} Ensuring Rough Country brand.".format(_LOG_PREFIX))
    brand, _ = src_models.RoughCountryBrand.objects.get_or_create(
        external_id=DEFAULT_RC_BRAND_NAME,
        defaults={
            "name": DEFAULT_RC_BRAND_NAME,
            "aaia_code": None,
        },
    )
    logger.info("{} Rough Country brand id={}.".format(_LOG_PREFIX, brand.id))
    return brand


def _ensure_rough_country_brands_from_manufacturers(
    general: typing.List[typing.Dict],
) -> typing.Dict[str, src_models.RoughCountryBrand]:
    """
    Collect unique manufacturer values from General rows and get_or_create
    a RoughCountryBrand for each. Returns mapping manufacturer -> RoughCountryBrand.
    Ensures DEFAULT_RC_BRAND_NAME exists for rows with missing manufacturer.
    """
    manufacturers = set()
    for row in general:
        m = _row_key(row, "manufacturer")
        if m:
            manufacturers.add(m)
    if not manufacturers:
        manufacturers.add(DEFAULT_RC_BRAND_NAME)
    # Ensure default exists so we can fall back for empty manufacturer
    manufacturers.add(DEFAULT_RC_BRAND_NAME)

    out = {}
    for m in sorted(manufacturers):
        brand, created = src_models.RoughCountryBrand.objects.get_or_create(
            external_id=m,
            defaults={"name": m, "aaia_code": None},
        )
        out[m] = brand
        if created:
            logger.info("{} Created RoughCountryBrand: external_id={!r}.".format(_LOG_PREFIX, m))
    logger.info("{} Using {} Rough Country brands from manufacturers.".format(_LOG_PREFIX, len(out)))
    return out


def fetch_and_save_rough_country(
    file_url: typing.Optional[str] = None,
    local_file_name: typing.Optional[str] = None,
    local_file_path: typing.Optional[str] = None,
    download: bool = True,
) -> None:
    """
    Fetch Excel feed, upsert brand, parts (General), apply Discontinued, upsert Fitment.
    """
    logger.info("{} Starting Rough Country feed sync.".format(_LOG_PREFIX))

    client = rough_country_client.RoughCountryFeedClient(
        file_url=file_url,
        local_file_name=local_file_name,
        local_file_path=local_file_path,
    )
    try:
        data = client.get_feed_data(download_if_missing=download)
    except rough_country_exceptions.RoughCountryException as e:
        logger.error("{} Feed error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    general = data.get("general") or []
    discontinued = data.get("discontinued") or []
    fitment = data.get("fitment") or []

    manufacturer_to_brand = _ensure_rough_country_brands_from_manufacturers(general)

    # Build discontinued lookup: sku -> {discontinued_date, replacement_sku}
    discontinued_by_sku = {}
    for row in discontinued:
        sku = _row_key(row, "sku")
        if not sku:
            continue
        val = _row_get(row, "discontinued_date")
        if hasattr(val, "date"):
            disc_date = val.date()
        else:
            disc_date = _safe_date(val)
        repl = _row_key(row, "replacement")
        discontinued_by_sku[sku] = {"discontinued_date": disc_date, "replacement_sku": repl}

    # Transform General -> RoughCountryPart (dedupe by sku; feed can have duplicate skus)
    seen_by_sku = {}
    for row in general:
        sku = _row_key(row, "sku")
        if not sku:
            continue
        manufacturer = _row_key(row, "manufacturer") or DEFAULT_RC_BRAND_NAME
        brand = manufacturer_to_brand.get(manufacturer) or manufacturer_to_brand.get(
            DEFAULT_RC_BRAND_NAME
        )
        disc = discontinued_by_sku.get(sku) or {}
        seen_by_sku[sku] = _row_to_rough_country_part(row, brand, disc)
    part_instances = list(seen_by_sku.values())
    if len(general) > len(part_instances):
        logger.info(
            "{} Deduped General rows from {} to {} by sku.".format(
                _LOG_PREFIX, len(general), len(part_instances)
            )
        )

    if not part_instances:
        logger.warning("{} No part instances from General tab.".format(_LOG_PREFIX))
    else:
        try:
            pgbulk.upsert(
                src_models.RoughCountryPart,
                part_instances,
                unique_fields=["brand", "sku"],
                update_fields=[
                    "title", "description", "price", "sale_price", "cost",
                    "availability", "nv_stock", "tn_stock", "link",
                    "image_1", "image_2", "image_3", "image_4", "image_5", "image_6",
                    "video", "features", "notes", "category", "manufacturer", "upc",
                    "weight", "height", "width", "length", "added_date",
                    "is_discontinued", "discontinued_date", "replacement_sku",
                    "raw_data",
                ],
                returning=False,
            )
            logger.info("{} Upserted {} Rough Country parts.".format(_LOG_PREFIX, len(part_instances)))
        except Exception as e:
            logger.error("{} Error upserting parts: {}.".format(_LOG_PREFIX, str(e)))
            raise

    # Apply discontinued for skus that are in Discontinued but not in General (edge case: set existing parts)
    if discontinued_by_sku:
        skus_in_general = {p.sku for p in part_instances}
        skus_to_mark = set(discontinued_by_sku.keys()) - skus_in_general
        if skus_to_mark:
            parts_to_update = list(
                src_models.RoughCountryPart.objects.filter(sku__in=skus_to_mark)
            )
            for part in parts_to_update:
                disc = discontinued_by_sku.get(part.sku)
                if disc:
                    part.is_discontinued = True
                    part.discontinued_date = disc.get("discontinued_date")
                    part.replacement_sku = disc.get("replacement_sku") or None
            if parts_to_update:
                src_models.RoughCountryPart.objects.bulk_update(
                    parts_to_update,
                    ["is_discontinued", "discontinued_date", "replacement_sku"],
                )

    # Fitment: need part_id by sku; load parts we have (after upsert, all RC brands)
    rc_brands = list(manufacturer_to_brand.values())
    sku_to_part = {
        p.sku: p
        for p in src_models.RoughCountryPart.objects.filter(
            brand__in=rc_brands
        ).only("id", "sku")
    }
    fitment_instances = []
    for row in fitment:
        sku = _row_key(row, "Sku", "sku")
        if not sku:
            continue
        part = sku_to_part.get(sku)
        if not part:
            continue
        start_year = _safe_int(_row_get(row, "StartYear"))
        end_year = _safe_int(_row_get(row, "EndYear"))
        make = _safe_str(_row_get(row, "Make"), 128)
        model = _safe_str(_row_get(row, "Model"), 128)
        submodel = _safe_str(_row_get(row, "Submodel"), 255)
        drive = _safe_str(_row_get(row, "Drive"), 64)
        fitment_instances.append(
            src_models.RoughCountryFitment(
                part=part,
                start_year=start_year,
                end_year=end_year,
                make=make,
                model=model,
                submodel=submodel,
                drive=drive,
            )
        )

    if fitment_instances:
        try:
            pgbulk.upsert(
                src_models.RoughCountryFitment,
                fitment_instances,
                unique_fields=["part", "start_year", "end_year", "make", "model", "submodel", "drive"],
                update_fields=[],
                returning=False,
            )
            logger.info("{} Upserted {} Rough Country fitments.".format(_LOG_PREFIX, len(fitment_instances)))
        except Exception as e:
            logger.error("{} Error upserting fitments: {}.".format(_LOG_PREFIX, str(e)))
            raise

    logger.info("{} Rough Country feed sync complete.".format(_LOG_PREFIX))


def _row_to_rough_country_part(
    row: typing.Dict,
    brand: src_models.RoughCountryBrand,
    discontinued: typing.Dict,
) -> src_models.RoughCountryPart:
    """Build RoughCountryPart from General row and discontinued info."""
    sku = _row_key(row, "sku") or ""
    title = _safe_str(_row_get(row, "title"), 512)
    description = _safe_str(_row_get(row, "description"))
    price = _safe_decimal(_row_get(row, "price"))
    sale_price = _safe_decimal(_row_get(row, "sale_price"))
    cost = _safe_decimal(_row_get(row, "cost"))
    availability = _safe_str(_row_get(row, "availability"), 255)
    nv_stock = _safe_int(_row_get(row, "NV_Stock"))
    tn_stock = _safe_int(_row_get(row, "TN_Stock"))
    link = _safe_str(_row_get(row, "link"))
    image_1 = _safe_str(_row_get(row, "image_1"))
    image_2 = _safe_str(_row_get(row, "image_2"))
    image_3 = _safe_str(_row_get(row, "image_3"))
    image_4 = _safe_str(_row_get(row, "image_4"))
    image_5 = _safe_str(_row_get(row, "image_5"))
    image_6 = _safe_str(_row_get(row, "image_6"))
    video = _safe_str(_row_get(row, "video"))
    features = _safe_str(_row_get(row, "features"))
    notes = _safe_str(_row_get(row, "notes"))
    category = _safe_str(_row_get(row, "category"), 255)
    manufacturer = _safe_str(_row_get(row, "manufacturer"), 255)
    upc = _safe_str(_row_get(row, "upc"), 255)
    weight = _safe_str(_row_get(row, "weight"), 64)
    height = _safe_decimal(_row_get(row, "height"))
    width = _safe_decimal(_row_get(row, "width"))
    length = _safe_decimal(_row_get(row, "length"))
    added_date = _safe_date(_row_get(row, "added_date"))
    is_disc = bool(discontinued)
    disc_date = discontinued.get("discontinued_date") if discontinued else None
    repl_sku = (discontinued.get("replacement_sku") or None) if discontinued else None

    return src_models.RoughCountryPart(
        brand=brand,
        sku=sku,
        title=title,
        description=description,
        price=price,
        sale_price=sale_price,
        cost=cost,
        availability=availability,
        nv_stock=nv_stock,
        tn_stock=tn_stock,
        link=link,
        image_1=image_1,
        image_2=image_2,
        image_3=image_3,
        image_4=image_4,
        image_5=image_5,
        image_6=image_6,
        video=video,
        features=features,
        notes=notes,
        category=category,
        manufacturer=manufacturer,
        upc=upc,
        weight=weight,
        height=height,
        width=width,
        length=length,
        added_date=added_date,
        is_discontinued=is_disc,
        discontinued_date=disc_date,
        replacement_sku=repl_sku,
        raw_data=None,
    )
