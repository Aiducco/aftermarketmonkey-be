"""
Rough Country feed integration: Excel jobber file (General, Vehicle Fitment, Discontinued).
Syncs RoughCountryBrand, RoughCountryPart, RoughCountryCompanyPricing, RoughCountryFitment;
applies discontinued status.
Catalog and fitment use the primary CompanyProvider (or first active); pricing loads per company.
"""
import logging
import math
import typing
from datetime import datetime
from decimal import Decimal

import pgbulk
from django.db import connection
from django.db.models.functions import Upper

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.rough_country import client as rough_country_client
from src.integrations.clients.rough_country import exceptions as rough_country_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_first_token_upper,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ROUGH-COUNTRY-SERVICES]"

# Default manufacturer / RoughCountryBrand name + external_id (stored uppercase)
DEFAULT_RC_BRAND_NAME = "ROUGH COUNTRY"

RC_PRICING_UPSERT_BATCH = 2000


def _normalize_rc_manufacturer_external_id(raw: typing.Optional[str]) -> str:
    """Normalize feed manufacturer to RoughCountryBrand.external_id / name (uppercase)."""
    s = (raw or "").strip().upper()
    return s if s else DEFAULT_RC_BRAND_NAME


def _rc_brand_name_upper_for_sync(rc_brand: src_models.RoughCountryBrand) -> str:
    name_upper = (rc_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(rc_brand.external_id)
    return name_upper


def _rough_country_feed_client_for_credentials(
    credentials: typing.Optional[typing.Dict],
    file_url_override: typing.Optional[str],
    local_file_path_override: typing.Optional[str],
    local_file_name_override: typing.Optional[str],
) -> rough_country_client.RoughCountryFeedClient:
    """
    Build client from CompanyProviders.credentials (feed_url) plus optional CLI overrides.
    CLI file_url and local_file_path are for management-command / dev use only.
    """
    creds = credentials or {}
    url = file_url_override
    if url is None:
        raw_url = creds.get(src_constants.ROUGH_COUNTRY_CREDENTIALS_FEED_URL)
        url = raw_url.strip() if isinstance(raw_url, str) and raw_url.strip() else None
    else:
        url = url.strip() if url.strip() else None
    path = local_file_path_override
    if path is not None:
        path = path.strip() if path.strip() else None
    return rough_country_client.RoughCountryFeedClient(
        file_url=url,
        local_file_name=local_file_name_override,
        local_file_path=path,
    )


def _active_rough_country_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _catalog_company_provider(
    rc_provider: typing.Optional[src_models.Providers],
) -> typing.Optional[src_models.CompanyProviders]:
    """Primary Rough Country connection for shared catalog; else first active by id."""
    if not rc_provider:
        return None
    base = _active_rough_country_company_providers_queryset().filter(provider=rc_provider)
    primary = base.filter(primary=True).first()
    if primary:
        return primary
    fallback = base.order_by("id").first()
    if fallback:
        logger.info(
            "{} No primary Rough Country company provider; using company_id={} for catalog/fitment.".format(
                _LOG_PREFIX,
                fallback.company_id,
            )
        )
    return fallback


def _deduped_pricing_by_brand_sku_from_general(
    general: typing.List[typing.Dict],
    manufacturer_to_brand: typing.Dict[str, src_models.RoughCountryBrand],
) -> typing.Dict[typing.Tuple[int, str], typing.Dict[str, typing.Any]]:
    """Match catalog sku dedupe: last General row per sku wins."""
    pricing_by_brand_sku = {}
    seen_by_sku = {}
    for row in general:
        sku = _row_key(row, "sku")
        if not sku:
            continue
        m_key = _normalize_rc_manufacturer_external_id(_row_key(row, "manufacturer"))
        brand = manufacturer_to_brand.get(m_key) or manufacturer_to_brand.get(DEFAULT_RC_BRAND_NAME)
        seen_by_sku[sku] = (brand, _row_pricing_from_general_row(row))
    for sku, (brand, pdata) in seen_by_sku.items():
        pricing_by_brand_sku[(brand.id, sku)] = pdata
    return pricing_by_brand_sku


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


def sync_unmapped_rough_country_brands_to_brands() -> typing.List[src_models.RoughCountryBrand]:
    """
    For each RoughCountryBrand that does not yet have a BrandRoughCountryBrandMapping:
    resolve Brand by aaia_code (comma-separated), then exact name (uppercase match), then fuzzy
    word-prefix match (same rules as Keystone); otherwise create (uppercase name).
    Upserts BrandRoughCountryBrandMapping, BrandProviders, and CompanyBrands for TICK_PERFORMANCE.
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

    # Ensure TICK_PERFORMANCE has CompanyProviders for Rough Country (needed for pricing sync)
    _, cp_created = src_models.CompanyProviders.objects.get_or_create(
        company=tick_company,
        provider=rc_provider,
        defaults={"credentials": {}, "primary": False},
    )
    if cp_created:
        logger.info("{} Created CompanyProviders for TICK_PERFORMANCE + Rough Country.".format(_LOG_PREFIX))

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

    all_aaia: typing.Set[str] = set()
    for rb in unmapped_rc_brands:
        for code in _normalize_aaia_codes(rb.aaia_code):
            all_aaia.add(code)

    aaia_to_brand: typing.Dict[str, src_models.Brands] = {}
    if all_aaia:
        for b in src_models.Brands.objects.filter(aaia_code__in=all_aaia).order_by("id"):
            if b.aaia_code and b.aaia_code not in aaia_to_brand:
                aaia_to_brand[b.aaia_code] = b

    name_upper_keys: typing.Set[str] = set()
    for rb in unmapped_rc_brands:
        hit = any(c in aaia_to_brand for c in _normalize_aaia_codes(rb.aaia_code))
        if not hit and (rb.name or "").strip():
            name_upper_keys.add((rb.name or "").strip().upper())

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

    resolved_by_rc_id: typing.Dict[int, src_models.Brands] = {}
    for rb in sorted(unmapped_rc_brands, key=lambda x: x.id):
        brand = None
        for code in _normalize_aaia_codes(rb.aaia_code):
            brand = aaia_to_brand.get(code)
            if brand:
                break
        if not brand:
            nm = (rb.name or "").strip().upper()
            if nm:
                brand = brands_by_upper_name.get(nm)
        if brand:
            resolved_by_rc_id[rb.id] = brand

    unresolved_after_exact = [rb for rb in unmapped_rc_brands if rb.id not in resolved_by_rc_id]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_exact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for rb in unresolved_after_exact:
        parts = normalize_upper_words(rb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(rb.name or "", candidates)
        if brand:
            resolved_by_rc_id[rb.id] = brand
            fuzzy_matches += 1
            logger.debug(
                "{} Fuzzy-matched Rough Country brand {!r} to Brand id={} name={!r}.".format(
                    _LOG_PREFIX,
                    rb.name,
                    brand.id,
                    brand.name,
                )
            )

    new_brand_specs: typing.Dict[str, typing.Optional[str]] = {}
    for rb in sorted(unmapped_rc_brands, key=lambda x: x.id):
        if rb.id in resolved_by_rc_id:
            continue
        nu = _rc_brand_name_upper_for_sync(rb)
        if nu not in new_brand_specs:
            aaia_parts = _normalize_aaia_codes(rb.aaia_code)
            new_brand_specs[nu] = aaia_parts[0] if aaia_parts else None

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
        for rb in unmapped_rc_brands:
            if rb.id not in resolved_by_rc_id:
                nu = _rc_brand_name_upper_for_sync(rb)
                resolved_by_rc_id[rb.id] = by_name[nu]

    mapping_models = [
        src_models.BrandRoughCountryBrandMapping(
            brand_id=resolved_by_rc_id[rb.id].id,
            rough_country_brand_id=rb.id,
        )
        for rb in unmapped_rc_brands
    ]
    try:
        pgbulk.upsert(
            src_models.BrandRoughCountryBrandMapping,
            mapping_models,
            unique_fields=["brand", "rough_country_brand"],
            update_fields=[],
            returning=False,
        )
    except Exception as e:
        logger.error("{} Error upserting BrandRoughCountryBrandMapping: {}.".format(_LOG_PREFIX, str(e)))
        raise

    created_brand_providers = 0
    created_company_brands = 0
    for rb in unmapped_rc_brands:
        brand = resolved_by_rc_id[rb.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=rc_provider,
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
        "{} Sync complete. Brands created: {}, fuzzy name matches: {}, "
        "BrandRoughCountryBrandMapping upserted: {}, BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX,
            created_brands,
            fuzzy_matches,
            len(mapping_models),
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
    a RoughCountryBrand for each. external_id and name are stored uppercase.
    Returns mapping normalized manufacturer key -> RoughCountryBrand.
    """
    manufacturers = set()
    for row in general:
        m = _row_key(row, "manufacturer")
        if m:
            manufacturers.add(_normalize_rc_manufacturer_external_id(m))
    if not manufacturers:
        manufacturers.add(DEFAULT_RC_BRAND_NAME)
    manufacturers.add(DEFAULT_RC_BRAND_NAME)

    out = {}
    for m in sorted(manufacturers):
        brand, created = src_models.RoughCountryBrand.objects.get_or_create(
            external_id=m,
            defaults={"name": m, "aaia_code": None},
        )
        if not created and (brand.name or "").strip().upper() != m:
            brand.name = m
            brand.save(update_fields=["name", "updated_at"])
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
    Catalog and fitment use the primary Rough Country CompanyProvider's credentials (or first active).
    Pricing runs once per active CompanyProvider using that row's feed_url.
    """
    logger.info("{} Starting Rough Country feed sync.".format(_LOG_PREFIX))

    rc_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    ).first()
    pricing_cps = []
    if rc_provider:
        pricing_cps = list(_active_rough_country_company_providers_queryset().filter(provider=rc_provider))
    if rc_provider and not pricing_cps:
        logger.warning(
            "{} No active Rough Country company providers; catalog will sync but not company pricing.".format(
                _LOG_PREFIX
            )
        )

    catalog_cp = _catalog_company_provider(rc_provider)
    catalog_creds = (catalog_cp.credentials if catalog_cp else {}) or {}
    if catalog_cp:
        logger.info(
            "{} Catalog feed using company_id={} (primary={}).".format(
                _LOG_PREFIX, catalog_cp.company_id, catalog_cp.primary,
            )
        )
    catalog_client = _rough_country_feed_client_for_credentials(
        catalog_creds,
        file_url,
        local_file_path,
        local_file_name,
    )
    try:
        data = catalog_client.get_feed_data(download_if_missing=download)
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
        m_key = _normalize_rc_manufacturer_external_id(_row_key(row, "manufacturer"))
        brand = manufacturer_to_brand.get(m_key) or manufacturer_to_brand.get(DEFAULT_RC_BRAND_NAME)
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
                    "title",
                    "description",
                    "availability",
                    "nv_stock",
                    "tn_stock",
                    "link",
                    "image_1",
                    "image_2",
                    "image_3",
                    "image_4",
                    "image_5",
                    "image_6",
                    "video",
                    "features",
                    "notes",
                    "category",
                    "manufacturer",
                    "upc",
                    "weight",
                    "height",
                    "width",
                    "length",
                    "added_date",
                    "is_discontinued",
                    "discontinued_date",
                    "replacement_sku",
                    "raw_data",
                ],
                returning=False,
            )
            logger.info("{} Upserted {} Rough Country parts.".format(_LOG_PREFIX, len(part_instances)))
        except Exception as e:
            logger.error("{} Error upserting parts: {}.".format(_LOG_PREFIX, str(e)))
            raise

        if pricing_cps:
            pairs = list({(p.brand_id, p.sku) for p in part_instances})
            id_by_brand_sku = {}
            chunk_size = 3000
            for i in range(0, len(pairs), chunk_size):
                chunk = pairs[i : i + chunk_size]
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, brand_id, sku FROM rough_country_parts WHERE (brand_id, sku) IN %s",
                        (tuple(chunk),),
                    )
                    for pid, bid, psku in cur.fetchall():
                        id_by_brand_sku[(bid, psku)] = pid

            total_pricing_all = 0
            for cp in pricing_cps:
                logger.info(
                    "{} Pricing feed for company_id={} (primary={}).".format(
                        _LOG_PREFIX,
                        cp.company_id,
                        cp.primary,
                    )
                )
                price_client = _rough_country_feed_client_for_credentials(
                    cp.credentials or {},
                    file_url,
                    local_file_path,
                    local_file_name,
                )
                try:
                    price_data = price_client.get_feed_data(download_if_missing=download)
                except rough_country_exceptions.RoughCountryException as e:
                    logger.error(
                        "{} Pricing feed error company_id={}: {}.".format(
                            _LOG_PREFIX,
                            cp.company_id,
                            str(e),
                        )
                    )
                    raise
                g = price_data.get("general") or []
                m2b = _ensure_rough_country_brands_from_manufacturers(g)
                pmap = _deduped_pricing_by_brand_sku_from_general(g, m2b)
                pricing_to_upsert = []
                for (bid, sku), pdata in pmap.items():
                    part_id = id_by_brand_sku.get((bid, sku))
                    if not part_id:
                        continue
                    pricing_to_upsert.append(
                        src_models.RoughCountryCompanyPricing(
                            part_id=part_id,
                            company=cp.company,
                            price=pdata.get("price"),
                            sale_price=pdata.get("sale_price"),
                            cost=pdata.get("cost"),
                            cnd_map=pdata.get("cnd_map"),
                            cnd_price=pdata.get("cnd_price"),
                        )
                    )
                total_cp = 0
                for j in range(0, len(pricing_to_upsert), RC_PRICING_UPSERT_BATCH):
                    batch = pricing_to_upsert[j : j + RC_PRICING_UPSERT_BATCH]
                    pgbulk.upsert(
                        src_models.RoughCountryCompanyPricing,
                        batch,
                        unique_fields=["part", "company"],
                        update_fields=["price", "sale_price", "cost", "cnd_map", "cnd_price", "updated_at"],
                        returning=False,
                    )
                    total_cp += len(batch)
                    connection.close()
                total_pricing_all += total_cp
                logger.info(
                    "{} Upserted {} Rough Country company pricing rows for company_id={}.".format(
                        _LOG_PREFIX,
                        total_cp,
                        cp.company_id,
                    )
                )
            logger.info(
                "{} Rough Country pricing finished: {} rows across {} companies.".format(
                    _LOG_PREFIX,
                    total_pricing_all,
                    len(pricing_cps),
                )
            )

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


def _row_pricing_from_general_row(row: typing.Dict) -> typing.Dict[str, typing.Any]:
    """Feed price fields for RoughCountryCompanyPricing."""
    return {
        "price": _safe_decimal(_row_get(row, "price")),
        "sale_price": _safe_decimal(_row_get(row, "sale_price")),
        "cost": _safe_decimal(_row_get(row, "cost")),
        "cnd_map": _safe_decimal(_row_get(row, "cnd_map")),
        "cnd_price": _safe_decimal(_row_get(row, "cnd_price")),
    }


def _row_to_rough_country_part(
    row: typing.Dict,
    brand: src_models.RoughCountryBrand,
    discontinued: typing.Dict,
) -> src_models.RoughCountryPart:
    """Build RoughCountryPart from General row and discontinued info (catalog only; pricing is per-company)."""
    sku = _row_key(row, "sku") or ""
    title = _safe_str(_row_get(row, "title"), 512)
    description = _safe_str(_row_get(row, "description"))
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
