"""
ATD (American Tire Distributors) integration services.

Unlike every other distributor here, ATD's own API ties BOTH cost and availability to the
dealer's locationnumber (see src.integrations.clients.atd.client's module docstring) -- there is
no shared/global catalog sweep the way Tri-State's or AllPro's shared warehouse stock is. Every
connected company's own credentials must run their own full `productcatalog` sweep, and that one
sweep produces catalog attributes, cost AND stock all at once.

Two jobs, run per CompanyProviders connection (there is no "primary account" seed step the way
AllPro/Tri-State have -- a second company's ATD account can legitimately see a different catalog,
so brand discovery happens incrementally as each company's own sweep runs):

  1. ``fetch_and_save_atd_catalog_for_company_provider`` -- stream this company's `productcatalog`
     pages into ATDBrand (discovered incrementally, keyed by brandid) + ATDPart (shared catalog)
     + ATDCompanyPricing (this company's own cost + stock).

  2. ``sync_unmapped_atd_brands_to_brands`` -- map ATDBrand -> Brands by name (ATD gives no AAIA
     code anywhere), same fuzzy cascade as EliteWheel/TheWheelGroup via
     src.integrations.utils.brand_matching.
"""
import logging
import typing
from decimal import Decimal, InvalidOperation

import pgbulk
from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.atd import client as atd_client
from src.integrations.clients.atd import exceptions as atd_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ATD-SERVICES]"

_CATALOG_UPSERT_BATCH = 2000
_PRICING_UPSERT_BATCH = 2000

# ATDPart columns, one per productspecs key we promote -- see src.models.ATDPart's docstring for
# why the rest stays in raw_specs rather than getting its own column.
_WHEEL_SPEC_FIELD_MAP = {
    "wheeldiameter": "wheel_diameter",
    "wheelwidth": "wheel_width",
    "boltpattern1": "bolt_pattern_1",
    "boltpattern2": "bolt_pattern_2",
    "boltpattern3": "bolt_pattern_3",
    "boltpattern4": "bolt_pattern_4",
    "hubbore": "hub_bore",
    "offset": "offset",
    "backspacing": "backspacing",
    "atdfinish": "atd_finish",
    "mfgfinish": "mfg_finish",
    "loadrating": "load_rating",
    "material": "material",
    "numberoflugholes": "number_of_lug_holes",
}
_TIRE_SPEC_FIELD_MAP = {
    "tirewidth": "tire_width",
    "aspectratio": "aspect_ratio",
    "rimdiameter": "rim_diameter",
    "loadindex": "load_index",
    "speedrating": "speed_rating",
    "loadrange": "load_range",
    "sidewalldescription": "sidewall_description",
    "winterdesignation": "winter_designation",
    "seasonaldesignation": "seasonal_designation",
    "runflat": "run_flat",
    "treadwear": "treadwear",
}
# Explicit overrides for ATD brand names the fuzzy cascade is known to get wrong -- same role
# elite_wheel._ELITE_WHEEL_BRAND_NAME_OVERRIDE plays. Confirmed live 2026-10-16: ATD's bare
# "Continental" fuzzy-matched to "CONTINENTAL BELTS AND HOSES" rather than "CONTINENTAL TIRE",
# because best_fuzzy_brand_match deliberately prefers the LONGEST matching candidate name and
# "BELTS AND HOSES" is longer than "TIRE" -- exactly the kind of collision that heuristic cannot
# tell apart on its own. Keyed by the ATD brand name as ATD sends it (case-sensitive).
_ATD_BRAND_NAME_OVERRIDE: typing.Dict[str, str] = {
    "Continental": "CONTINENTAL TIRE",
    # "MASTERCRAFT SAFETY, INC." (24 chars) beat "MASTERCRAFT TIRES" (17 chars) under the same
    # longest-match heuristic -- Mastercraft is a Cooper-owned tire line, not a harness maker.
    "Mastercraft": "MASTERCRAFT TIRES",
    # Prefer the active "WEST COAST ACCESSORIES" over "WEST COAST ACCESS. -INACTIVE" -- would be
    # caught by the INACTIVE-marker filter below too, but this is cheaper and more explicit.
    "West Coast": "WEST COAST ACCESSORIES",
}

# A Brands row name ending "- INACTIVE" (or similar) must never be chosen by the fuzzy cascade
# below -- same marker/role as elite_wheel._INACTIVE_BRAND_NAME_MARKER. Confirmed live 2026-10-16:
# without this filter, ATD's "Cascade" fuzzy-matched onto the only candidate that existed,
# "CASCADE DESIGNS - INACTIVE".
_INACTIVE_BRAND_NAME_MARKER = "INACTIVE"

_SHARED_SPEC_FIELD_MAP = {
    "size": "size",
    "upc": "upc",
    "shippingweight": "shipping_weight",
    "countryoforigin": "country_of_origin",
}


def _atd_client_for_company_provider(company_provider_id: int) -> typing.Optional[atd_client.ATDApiClient]:
    cp = src_models.CompanyProviders.objects.filter(id=company_provider_id).select_related("company").first()
    if not cp:
        logger.warning("{} No CompanyProviders row id={}.".format(_LOG_PREFIX, company_provider_id))
        return None
    credentials = credentials_helper.get_feed_credentials(cp)
    try:
        return atd_client.ATDApiClient(credentials=credentials)
    except ValueError as e:
        logger.warning(
            "{} company_provider_id={}: invalid/missing credentials ({}). Skipping.".format(
                _LOG_PREFIX, company_provider_id, e
            )
        )
        return None


def _first_image_url(images: typing.Optional[typing.Dict]) -> typing.Optional[str]:
    """First 'large' image url, falling back to small/thumbnail -- see atd-connect-api.md section
    6 for the images.<size>.image[] shape (singular 'image' key, despite the PDF's own table)."""
    if not isinstance(images, dict):
        return None
    for size in ("large", "small", "thumbnail"):
        bucket = images.get(size)
        if isinstance(bucket, dict):
            items = bucket.get("image")
            if isinstance(items, list) and items:
                url = items[0].get("url")
                if url:
                    return url
    return None


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        return None
    return d if d != 0 else None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _str_or_none(value: typing.Any, max_len: typing.Optional[int] = None) -> typing.Optional[str]:
    s = str(value).strip() if value is not None else ""
    if not s:
        return None
    return s[:max_len] if max_len else s


def fetch_and_save_atd_catalog_for_company_provider(
    company_provider_id: int, max_pages: typing.Optional[int] = None
) -> typing.Tuple[int, int, int]:
    """
    Full `productcatalog` sweep for one company's ATD connection. Upserts ATDBrand (discovered
    incrementally) + ATDPart (shared catalog) + ATDCompanyPricing (this company's own cost+stock).

    Returns (brands_upserted, parts_upserted, pricing_rows_upserted).
    """
    cp = src_models.CompanyProviders.objects.filter(id=company_provider_id).select_related("company").first()
    if not cp:
        logger.warning("{} No CompanyProviders row id={}.".format(_LOG_PREFIX, company_provider_id))
        return 0, 0, 0
    client = _atd_client_for_company_provider(company_provider_id)
    if not client:
        return 0, 0, 0

    brands_seen: typing.Dict[str, str] = {}  # external_id -> name
    part_batch: typing.List[typing.Dict] = []
    total_brands = 0
    total_parts = 0
    total_pricing = 0
    now = timezone.now()

    def _flush_parts(batch: typing.List[typing.Dict]) -> typing.Tuple[int, int]:
        if not batch:
            return 0, 0

        # Resolve brand rows for this batch (brands discovered earlier in this same sweep may not
        # be in the DB yet -- upsert any new ones first, batch-local).
        local_brand_ids = {row["brand_external_id"]: row["brand_name"] for row in batch}
        existing_brands = {
            b.external_id: b
            for b in src_models.ATDBrand.objects.filter(external_id__in=local_brand_ids.keys())
        }
        new_brands = [
            src_models.ATDBrand(external_id=eid, name=name)
            for eid, name in local_brand_ids.items()
            if eid not in existing_brands
        ]
        if new_brands:
            pgbulk.upsert(
                src_models.ATDBrand, new_brands, unique_fields=["external_id"],
                update_fields=["name", "updated_at"], returning=False,
            )
            existing_brands.update({
                b.external_id: b
                for b in src_models.ATDBrand.objects.filter(external_id__in=[b.external_id for b in new_brands])
            })

        part_instances = []
        for row in batch:
            brand = existing_brands.get(row["brand_external_id"])
            if not brand:
                continue
            part_instances.append(src_models.ATDPart(brand=brand, **row["fields"]))

        if part_instances:
            pgbulk.upsert(
                src_models.ATDPart,
                part_instances,
                unique_fields=["atdproductnumber"],
                update_fields=[f.name for f in src_models.ATDPart._meta.fields if f.name not in (
                    "id", "atdproductnumber", "created_at",
                )],
            )

        atd_part_ids = {
            p.atdproductnumber: p.id
            for p in src_models.ATDPart.objects.filter(
                atdproductnumber__in=[row["fields"]["atdproductnumber"] for row in batch]
            ).only("id", "atdproductnumber")
        }
        pricing_instances = []
        for row in batch:
            part_id = atd_part_ids.get(row["fields"]["atdproductnumber"])
            if not part_id:
                continue
            pricing_instances.append(
                src_models.ATDCompanyPricing(part_id=part_id, company=cp.company, **row["pricing"], last_synced_at=now)
            )
        if pricing_instances:
            pgbulk.upsert(
                src_models.ATDCompanyPricing,
                pricing_instances,
                unique_fields=["part", "company"],
                update_fields=[
                    "cost", "fet", "map_price", "msrp", "retail_price",
                    "local_qty", "localplus_qty", "nationwide_qty", "onhand_qty", "sthinventory_qty",
                    "last_synced_at", "updated_at",
                ],
            )

        return len(part_instances), len(pricing_instances)

    try:
        for product in client.iter_product_catalog(max_pages=max_pages):
            atdproductnumber = _str_or_none(product.get("atdproductnumber"), 64)
            if not atdproductnumber:
                continue
            brand_name = _str_or_none(product.get("brand"), 255)
            brand_external_id = _str_or_none(product.get("brandid"), 64)
            if not brand_name or not brand_external_id:
                continue
            if brand_external_id not in brands_seen:
                brands_seen[brand_external_id] = brand_name
                total_brands += 1

            specs = product.get("productspecs") or {}
            fields = {
                "atdproductnumber": atdproductnumber,
                "mfgproductnumber": _str_or_none(product.get("mfgproductnumber"), 64),
                "style": _str_or_none(product.get("style"), 255),
                "styleid": _str_or_none(product.get("styleid"), 64),
                "productgroup": _str_or_none(product.get("productgroup"), 64),
                "description": _str_or_none(product.get("description")),
                "discontinued": bool(product.get("discontinued")),
                "replaced": bool(product.get("replaced")),
                "image_url": _first_image_url(product.get("images")),
                "raw_images": product.get("images") or None,
                "raw_specs": specs or None,
            }
            for spec_map in (_WHEEL_SPEC_FIELD_MAP, _TIRE_SPEC_FIELD_MAP, _SHARED_SPEC_FIELD_MAP):
                for spec_key, field_name in spec_map.items():
                    fields[field_name] = _str_or_none(specs.get(spec_key), 255)

            price = product.get("price") or {}
            availability = product.get("availability") or {}
            pricing = {
                "cost": _safe_decimal(price.get("cost")),
                "fet": _safe_decimal(price.get("fet")),
                "map_price": _safe_decimal(price.get("map")),
                "msrp": _safe_decimal(price.get("msrp")),
                "retail_price": _safe_decimal(price.get("retail")),
                "local_qty": _safe_int(availability.get("local")),
                "localplus_qty": _safe_int(availability.get("localplus")),
                "nationwide_qty": _safe_int(availability.get("nationwide")),
                "onhand_qty": _safe_int(availability.get("onhand")),
                "sthinventory_qty": _safe_int(availability.get("sthinventory")),
            }

            part_batch.append({"brand_external_id": brand_external_id, "brand_name": brand_name, "fields": fields, "pricing": pricing})
            if len(part_batch) >= _CATALOG_UPSERT_BATCH:
                p, pr = _flush_parts(part_batch)
                total_parts += p
                total_pricing += pr
                logger.info(
                    "{} company_provider_id={}: batch flushed ({} parts, {} pricing rows so far).".format(
                        _LOG_PREFIX, company_provider_id, total_parts, total_pricing
                    )
                )
                part_batch = []

        if part_batch:
            p, pr = _flush_parts(part_batch)
            total_parts += p
            total_pricing += pr
    except atd_exceptions.ATDAPIException as e:
        logger.error(
            "{} company_provider_id={}: catalog sweep failed: {}".format(_LOG_PREFIX, company_provider_id, e)
        )
        raise

    logger.info(
        "{} company_provider_id={}: synced {} brands, {} parts, {} pricing rows.".format(
            _LOG_PREFIX, company_provider_id, total_brands, total_parts, total_pricing
        )
    )
    return total_brands, total_parts, total_pricing


def sync_unmapped_atd_brands_to_brands() -> int:
    """
    Map ATDBrand -> Brands by name (ATD gives no AAIA code at all, confirmed live -- see
    src.models.ATDBrand's docstring). Exact uppercase-name match, then compact-key, then fuzzy
    word-prefix, then create a new Brands row -- same cascade as
    elite_wheel.sync_unmapped_elite_wheel_brands_to_brands. Returns the number newly mapped.
    """
    logger.info("{} Syncing unmapped ATD brands to Brands.".format(_LOG_PREFIX))

    atd_provider = src_models.Providers.objects.filter(kind=src_enums.BrandProviderKind.ATD.value).first()
    if not atd_provider:
        logger.warning("{} ATD provider not found. Skipping sync.".format(_LOG_PREFIX))
        return 0

    mapped_ids = set(
        src_models.BrandATDBrandMapping.objects.values_list("atd_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.ATDBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped ATD brands. Nothing to sync.".format(_LOG_PREFIX))
        return 0

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 0: explicit overrides (see _ATD_BRAND_NAME_OVERRIDE).
    override_matches = 0
    override_names = {
        _ATD_BRAND_NAME_OVERRIDE[ab.name] for ab in unmapped if ab.name in _ATD_BRAND_NAME_OVERRIDE
    }
    if override_names:
        override_brands = {b.name: b for b in src_models.Brands.objects.filter(name__in=list(override_names))}
        for ab in unmapped:
            target = _ATD_BRAND_NAME_OVERRIDE.get(ab.name)
            if target and target in override_brands:
                resolved_by_id[ab.id] = override_brands[target]
                override_matches += 1

    # Phase 1: exact uppercase-name match.
    name_upper_keys = {(ab.name or "").strip().upper() for ab in unmapped if (ab.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            brands_by_upper_name.setdefault((b.name or "").strip().upper(), b)
    for ab in unmapped:
        if ab.id in resolved_by_id:
            continue
        name_upper = (ab.name or "").strip().upper()
        if name_upper and name_upper in brands_by_upper_name:
            resolved_by_id[ab.id] = brands_by_upper_name[name_upper]

    # Phase 2: compact-key (punctuation/spacing-insensitive).
    compact_matches = 0
    still = [ab for ab in unmapped if ab.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for ab in still:
            key = normalize_compact_key(ab.name or "")
            if key and key in compact_index:
                resolved_by_id[ab.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix.
    unresolved = [ab for ab in unmapped if ab.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for ab in unresolved:
        tokens = normalize_upper_words(ab.name or "").split()
        candidates: typing.List[src_models.Brands] = list(first_index.get(tokens[0], ())) if tokens else []
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        candidates = [b for b in candidates if _INACTIVE_BRAND_NAME_MARKER not in (b.name or "").upper()]
        brand = best_fuzzy_brand_match(ab.name or "", candidates) if candidates else None
        if brand:
            resolved_by_id[ab.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved.
    created_brands = 0
    still_unresolved = [ab for ab in unmapped if ab.id not in resolved_by_id]
    if still_unresolved:
        new_names = {(ab.name or "").strip().upper() for ab in still_unresolved if (ab.name or "").strip()}
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_names)).values_list("name", flat=True)
        )
        new_rows = [
            src_models.Brands(
                name=name, status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name, aaia_code=None,
            )
            for name in new_names if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {b.name: b for b in src_models.Brands.objects.filter(name__in=list(new_names))}
        for ab in still_unresolved:
            key = (ab.name or "").strip().upper()
            if key in by_name:
                resolved_by_id[ab.id] = by_name[key]

    mapping_models = [
        src_models.BrandATDBrandMapping(brand_id=resolved_by_id[ab.id].id, atd_brand_id=ab.id)
        for ab in unmapped if ab.id in resolved_by_id
    ]
    if mapping_models:
        pgbulk.upsert(
            src_models.BrandATDBrandMapping, mapping_models,
            unique_fields=["brand", "atd_brand"], update_fields=[], returning=False,
        )

    created_brand_providers = 0
    for ab in unmapped:
        brand = resolved_by_id.get(ab.id)
        if not brand:
            continue
        _, bp_created = src_models.BrandProviders.objects.get_or_create(brand=brand, provider=atd_provider)
        if bp_created:
            created_brand_providers += 1

    logger.info(
        "{} Sync complete. Brands created: {}, override: {}, compact: {}, fuzzy: {}, mappings: {}, "
        "BrandProviders: {}.".format(
            _LOG_PREFIX, created_brands, override_matches, compact_matches, fuzzy_matches,
            len(mapping_models), created_brand_providers,
        )
    )
    return len(mapping_models)
