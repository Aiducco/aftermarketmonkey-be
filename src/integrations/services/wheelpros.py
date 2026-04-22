"""
WheelPros feed integration: SFTP CSV feed with brand, part, pricing, and warehouse inventory.
Syncs WheelProsBrand, WheelProsPart, WheelProsCompanyPricing, and BrandWheelProsBrandMapping.
Catalog uses the primary CompanyProvider (or first active); pricing loads per company SFTP.
"""
import logging
import math
import typing
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

import pgbulk

from django.conf import settings
from django.db import connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.wheelpros import client as wheelpros_client
from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_first_token_upper,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-SERVICES]"

WP_PRICING_UPSERT_BATCH = 2000


def _dedupe_wheelpros_part_instances(
    parts: typing.List[src_models.WheelProsPart],
) -> typing.List[src_models.WheelProsPart]:
    """
    One instance per (brand_id, part_number), last wins.
    Avoids PostgreSQL: ON CONFLICT DO UPDATE cannot affect row a second time.
    """
    by_key: typing.Dict[typing.Tuple[int, str], src_models.WheelProsPart] = {}
    for p in parts:
        bid = p.brand_id
        if bid is None and p.brand is not None:
            bid = p.brand.pk
        pn = (p.part_number or "").strip()
        if not bid or not pn:
            continue
        by_key[(int(bid), pn)] = p
    return list(by_key.values())


def _normalize_wheelpros_brand_key(raw: typing.Optional[str]) -> str:
    return (raw or "").strip().upper()


def _active_wheelpros_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.WHEELPROS.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _catalog_wheelpros_company_provider(
    wp_provider: typing.Optional[src_models.Providers],
) -> typing.Optional[src_models.CompanyProviders]:
    if not wp_provider:
        return None
    base = _active_wheelpros_company_providers_queryset().filter(provider=wp_provider)
    primary = base.filter(primary=True).first()
    if primary:
        return primary
    fallback = base.order_by("id").first()
    if fallback:
        logger.info(
            "{} No primary WheelPros company provider; using company_id={} for catalog.".format(
                _LOG_PREFIX,
                fallback.company_id,
            )
        )
    return fallback


def _wp_brand_name_upper_for_sync(wp_brand: src_models.WheelProsBrand) -> str:
    name_upper = (wp_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(wp_brand.external_id)
    return name_upper


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


# --- Dealer cost from MSRP + company credentials (wheel_markup / tire_markup / accessories_markup %) ---

# When a credential key is missing or empty, treat as this discount off MSRP (20% → cost = 0.8 × MSRP).
WHEELPROS_DEFAULT_DISCOUNT_PERCENT = Decimal(20)

_WHEELPROS_FEED_TO_MARKUP_KEY = {
    "wheel": "wheel_markup",
    "tire": "tire_markup",
    "accessories": "accessories_markup",
}


def _parse_wp_discount_percent(raw: typing.Any) -> typing.Optional[Decimal]:
    if raw is None or raw is "":
        return None
    try:
        d = Decimal(str(raw).strip())
    except Exception:
        return None
    if d < 0:
        d = Decimal(0)
    if d > 100:
        d = Decimal(100)
    return d


def discount_percent_for_feed(
    credentials: typing.Optional[typing.Dict],
    feed_type: typing.Optional[str],
) -> Decimal:
    """
    Discount percent *off* MSRP (0–100) for a feed, from ``CompanyProviders.credentials``.
    Unknown feed, missing key, or unparseable value → :data:`WHEELPROS_DEFAULT_DISCOUNT_PERCENT` (20%).
    """
    creds = credentials or {}
    ft = (feed_type or "wheel").strip().lower() if feed_type else "wheel"
    if ft not in _WHEELPROS_FEED_TO_MARKUP_KEY:
        ft = "wheel"
    key = _WHEELPROS_FEED_TO_MARKUP_KEY[ft]
    pct = _parse_wp_discount_percent(creds.get(key))
    if pct is None:
        return WHEELPROS_DEFAULT_DISCOUNT_PERCENT
    return pct


def dealer_cost_from_msrp(
    msrp: typing.Any,
    feed_type: typing.Optional[str],
    credentials: typing.Optional[typing.Dict],
) -> typing.Optional[Decimal]:
    """
    ``cost = MSRP * (1 - discount_percent / 100)`` using the markup key for ``feed_type``.
    Returns None if ``msrp`` is missing or invalid.
    """
    if msrp is None:
        return None
    try:
        m = msrp if isinstance(msrp, Decimal) else Decimal(str(msrp).strip())
    except Exception:
        return None
    if m < 0:
        m = Decimal(0)
    pct = discount_percent_for_feed(credentials, feed_type)
    q = m * (Decimal(1) - pct / Decimal(100))
    return q.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def sync_unmapped_wheelpros_brands_to_brands(dry_run: bool = False) -> typing.List[src_models.WheelProsBrand]:
    """
    For each WheelProsBrand that does not yet have a BrandWheelProsBrandMapping:
    resolve Brand by exact name (uppercase), then fuzzy word-prefix match (shared util with Keystone/Rough Country);
    otherwise create. Upserts mapping, BrandProviders, CompanyBrands for TICK_PERFORMANCE.

    If dry_run is True, no database writes; logs only WheelPros brands that matched an existing Brand (exact or fuzzy).
    """
    logger.info(
        "{} Syncing unmapped WheelPros brands to Brands{}.".format(
            _LOG_PREFIX,
            " (dry run)" if dry_run else "",
        )
    )

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

    if not dry_run:
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

    name_upper_keys: typing.Set[str] = set()
    for wb in unmapped_wheelpros_brands:
        if (wb.name or "").strip():
            name_upper_keys.add((wb.name or "").strip().upper())

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

    resolved_by_wp_id: typing.Dict[int, src_models.Brands] = {}
    exact_matched_wp_ids: typing.Set[int] = set()
    for wb in sorted(unmapped_wheelpros_brands, key=lambda x: x.id):
        nm = (wb.name or "").strip().upper()
        if nm:
            brand = brands_by_upper_name.get(nm)
            if brand:
                resolved_by_wp_id[wb.id] = brand
                exact_matched_wp_ids.add(wb.id)

    unresolved_after_exact = [
        wb for wb in unmapped_wheelpros_brands if wb.id not in resolved_by_wp_id
    ]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_exact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    fuzzy_matched_wp_ids: typing.Set[int] = set()
    for wb in unresolved_after_exact:
        parts = normalize_upper_words(wb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(wb.name or "", candidates)
        if brand:
            resolved_by_wp_id[wb.id] = brand
            fuzzy_matched_wp_ids.add(wb.id)
            fuzzy_matches += 1
            if not dry_run:
                logger.debug(
                    "{} Fuzzy-matched WheelPros brand {!r} to Brand id={} name={!r}.".format(
                        _LOG_PREFIX,
                        wb.name,
                        brand.id,
                        brand.name,
                    )
                )

    if dry_run:
        for wb in sorted(unmapped_wheelpros_brands, key=lambda x: x.id):
            if wb.id not in resolved_by_wp_id:
                continue
            brand = resolved_by_wp_id[wb.id]
            how = "exact" if wb.id in exact_matched_wp_ids else "fuzzy"
            logger.info(
                "{} [dry-run] match ({}) WheelProsBrand id={} external_id={!r} name={!r} "
                "-> Brand id={} name={!r}".format(
                    _LOG_PREFIX,
                    how,
                    wb.id,
                    wb.external_id,
                    wb.name,
                    brand.id,
                    brand.name,
                )
            )
        would_create = [wb for wb in unmapped_wheelpros_brands if wb.id not in resolved_by_wp_id]
        logger.info(
            "{} [dry-run] Summary: {} exact matches, {} fuzzy matches, {} unmatched (would create Brand). "
            "No writes performed.".format(
                _LOG_PREFIX,
                len(exact_matched_wp_ids),
                len(fuzzy_matched_wp_ids),
                len(would_create),
            )
        )
        return unmapped_wheelpros_brands

    new_brand_specs: typing.Set[str] = set()
    for wb in sorted(unmapped_wheelpros_brands, key=lambda x: x.id):
        if wb.id in resolved_by_wp_id:
            continue
        new_brand_specs.add(_wp_brand_name_upper_for_sync(wb))

    created_brands = 0
    if new_brand_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_specs)).values_list("name", flat=True)
        )
        new_brand_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for name in new_brand_specs
            if name not in existing_names
        ]
        if new_brand_rows:
            src_models.Brands.objects.bulk_create(new_brand_rows, ignore_conflicts=True)
            created_brands = len(new_brand_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_specs))
        }
        for wb in unmapped_wheelpros_brands:
            if wb.id not in resolved_by_wp_id:
                nu = _wp_brand_name_upper_for_sync(wb)
                resolved_by_wp_id[wb.id] = by_name[nu]

    mapping_models = [
        src_models.BrandWheelProsBrandMapping(
            brand_id=resolved_by_wp_id[wb.id].id,
            wheelpros_brand_id=wb.id,
        )
        for wb in unmapped_wheelpros_brands
    ]
    try:
        pgbulk.upsert(
            src_models.BrandWheelProsBrandMapping,
            mapping_models,
            unique_fields=["brand", "wheelpros_brand"],
            update_fields=[],
            returning=False,
        )
    except Exception as e:
        logger.error("{} Error upserting BrandWheelProsBrandMapping: {}.".format(_LOG_PREFIX, str(e)))
        raise

    created_brand_providers = 0
    created_company_brands = 0
    for wb in unmapped_wheelpros_brands:
        brand = resolved_by_wp_id[wb.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=wheelpros_provider,
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
        "BrandWheelProsBrandMapping upserted: {}, BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX,
            created_brands,
            fuzzy_matches,
            len(mapping_models),
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
        # Tire feed
        "CapHardwareDescription",
        "CapScrewQuantity",
        "CapWrench",
        "CapStyleDescription",
        # Accessories feed
        "ManufacturerPartNumber",
        "Division",
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


def _row_to_wheelpros_part(
    row: typing.Dict,
    brand: src_models.WheelProsBrand,
    include_pricing: bool = True,
    feed_type: typing.Optional[str] = None,
) -> src_models.WheelProsPart:
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
    if include_pricing:
        msrp_usd = _safe_decimal(_row_get(row, "MSRP_USD"))
        map_usd = _safe_decimal(_row_get(row, "MAP_USD"))
    else:
        msrp_usd = None
        map_usd = None
    run_date = _safe_datetime(_row_get(row, "RunDate"))
    warehouse_availability = _warehouse_inventory_from_row(row)
    raw_data = {
        k: (None if (v is None or (isinstance(v, float) and math.isnan(v))) else v)
        for k, v in row.items()
    }

    return src_models.WheelProsPart(
        brand=brand,
        feed_type=feed_type,
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


def _wheelpros_credentials_for_catalog(
    wp_provider: typing.Optional[src_models.Providers],
) -> typing.Dict:
    """Prefer primary / first active WheelPros CompanyProvider; else TICK_PERFORMANCE legacy lookup."""
    cp = _catalog_wheelpros_company_provider(wp_provider)
    if cp and cp.credentials:
        return dict(cp.credentials)
    return dict(_get_wheelpros_credentials() or {})


def _wheelpros_pricing_map_from_records(
    records: typing.List[typing.Dict],
    brand_by_external_id: typing.Dict[str, src_models.WheelProsBrand],
) -> typing.Dict[typing.Tuple[int, str], typing.Dict[str, typing.Any]]:
    """Last row per (brand_id, part_number) wins."""
    out: typing.Dict[typing.Tuple[int, str], typing.Dict[str, typing.Any]] = {}
    for row in records:
        pn = (_row_key(row, "PartNumber") or "").strip()
        bkey = _normalize_wheelpros_brand_key(_row_key(row, "Brand"))
        if not pn or not bkey:
            continue
        brand = brand_by_external_id.get(bkey)
        if not brand:
            continue
        out[(brand.id, pn)] = {
            "msrp_usd": _safe_decimal(_row_get(row, "MSRP_USD")),
            "map_usd": _safe_decimal(_row_get(row, "MAP_USD")),
        }
    return out


def _get_local_path_for_feed(feed_type: str) -> str:
    """Get local file path for feed type."""
    if feed_type == "wheel":
        return getattr(settings, "WHEELPROS_INVENTORY_LOCAL_PATH", "/tmp/wheelpros_wheel_inventory.csv")
    if feed_type == "tire":
        return getattr(settings, "WHEELPROS_TIRE_LOCAL_PATH", "/tmp/wheelpros_tire_inventory.csv")
    if feed_type == "accessories":
        return getattr(settings, "WHEELPROS_ACCESSORIES_LOCAL_PATH", "/tmp/wheelpros_accessories_inventory.csv")
    return "/tmp/wheelpros_{}_inventory.csv".format(feed_type)


def fetch_and_save_wheelpros(
    local_file_path: typing.Optional[str] = None,
    download: bool = True,
    local_only: bool = False,
    feed_type: str = "wheel",
) -> None:
    """
    Fetch the WheelPros CSV and upsert WheelProsBrand and WheelProsPart (catalog).
    Company pricing is loaded per active CompanyProvider using each row's SFTP credentials.
    feed_type: "wheel" | "tire" | "accessories"
    """
    sftp_path = src_constants.WHEELPROS_FEED_PATHS.get(feed_type)
    if not sftp_path:
        raise ValueError("Unknown feed_type: {}. Use wheel, tire, or accessories.".format(feed_type))

    local_path = local_file_path or _get_local_path_for_feed(feed_type)
    logger.info("{} Starting WheelPros {} feed sync (path={}).".format(_LOG_PREFIX, feed_type, sftp_path))

    wp_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    pricing_cps: typing.List[src_models.CompanyProviders] = []
    if wp_provider:
        pricing_cps = list(_active_wheelpros_company_providers_queryset().filter(provider=wp_provider))
    if wp_provider and not pricing_cps:
        logger.warning(
            "{} No active WheelPros company providers; catalog will sync but not company pricing.".format(
                _LOG_PREFIX
            )
        )

    catalog_creds = _wheelpros_credentials_for_catalog(wp_provider) if not local_only else {}
    catalog_creds = dict(catalog_creds)
    catalog_creds["sftp_path"] = sftp_path

    catalog_cp = _catalog_wheelpros_company_provider(wp_provider)
    if catalog_cp:
        logger.info(
            "{} Catalog feed using company_id={} (primary={}).".format(
                _LOG_PREFIX,
                catalog_cp.company_id,
                catalog_cp.primary,
            )
        )

    client = wheelpros_client.WheelProsSFTPClient(
        credentials=catalog_creds,
        local_file_path=local_path,
        require_credentials=not local_only,
    )
    try:
        records = client.get_feed_records(
            force_download=download,
            local_only=local_only,
            sftp_path=sftp_path,
            local_file_path=local_path,
        )
    except wheelpros_exceptions.WheelProsException as e:
        logger.error("{} WheelPros {} feed error: {}.".format(_LOG_PREFIX, feed_type, str(e)))
        raise

    if not records:
        logger.warning("{} No rows returned from WheelPros {} feed.".format(_LOG_PREFIX, feed_type))
        return

    brand_names = set()
    for row in records:
        name = _row_key(row, "Brand")
        if name:
            brand_names.add(_normalize_wheelpros_brand_key(name))

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
        b.external_id: b
        for b in src_models.WheelProsBrand.objects.filter(external_id__in=brand_names)
    }

    seen_by_key = {}
    for row in records:
        part_number = (_row_key(row, "PartNumber") or "").strip()
        brand_name = _normalize_wheelpros_brand_key(_row_key(row, "Brand"))
        if not part_number or not brand_name:
            continue
        brand = brand_to_model.get(brand_name)
        if not brand:
            continue
        seen_by_key[(brand.id, part_number)] = _row_to_wheelpros_part(
            row, brand, include_pricing=False, feed_type=feed_type
        )

    part_instances = _dedupe_wheelpros_part_instances(list(seen_by_key.values()))
    if not part_instances:
        logger.warning("{} No WheelPros part instances created.".format(_LOG_PREFIX))
        return

    try:
        _now = timezone.now()
        for _p in part_instances:
            _p.updated_at = _now
        pgbulk.upsert(
            src_models.WheelProsPart,
            part_instances,
            unique_fields=["brand", "part_number"],
            update_fields=[
                "feed_type",
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
                "run_date",
                "warehouse_availability",
                "raw_data",
                "updated_at",
            ],
            returning=False,
        )
        logger.info("{} Upserted {} WheelPros parts.".format(_LOG_PREFIX, len(part_instances)))
    except Exception as e:
        logger.error("{} Error upserting WheelPros parts: {}.".format(_LOG_PREFIX, str(e)))
        raise

    if pricing_cps:
        pairs = list({(p.brand_id, (p.part_number or "").strip()) for p in part_instances})
        id_by_brand_pn: typing.Dict[typing.Tuple[int, str], int] = {}
        chunk_size = 3000
        for i in range(0, len(pairs), chunk_size):
            chunk = pairs[i : i + chunk_size]
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM wheelpros_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(chunk),),
                )
                for pid, bid, pn in cur.fetchall():
                    id_by_brand_pn[(bid, (pn or "").strip())] = pid

        brand_by_external = {
            b.external_id: b
            for b in src_models.WheelProsBrand.objects.filter(external_id__in=brand_names)
        }
        total_pricing = 0
        for cp in pricing_cps:
            logger.info(
                "{} Pricing feed for company_id={} (primary={}).".format(
                    _LOG_PREFIX,
                    cp.company_id,
                    cp.primary,
                )
            )
            creds = dict(cp.credentials or {})
            creds["sftp_path"] = sftp_path
            pc = wheelpros_client.WheelProsSFTPClient(
                credentials=creds,
                local_file_path=local_path,
                require_credentials=not local_only,
            )
            try:
                price_records = pc.get_feed_records(
                    force_download=download,
                    local_only=local_only,
                    sftp_path=sftp_path,
                    local_file_path=local_path,
                )
            except wheelpros_exceptions.WheelProsException as e:
                logger.error(
                    "{} WheelPros pricing feed error company_id={}: {}.".format(
                        _LOG_PREFIX,
                        cp.company_id,
                        str(e),
                    )
                )
                raise
            pmap = _wheelpros_pricing_map_from_records(price_records, brand_by_external)
            # Collapse by catalog part_id so one INSERT row per (part, company); last pmap row wins.
            pricing_by_part_id: typing.Dict[int, typing.Dict[str, typing.Any]] = {}
            for (bid, pn), pdata in pmap.items():
                part_id = id_by_brand_pn.get((bid, pn))
                if not part_id:
                    continue
                pricing_by_part_id[int(part_id)] = pdata
            creds_for_cost = dict(cp.credentials or {})
            pricing_to_upsert = [
                src_models.WheelProsCompanyPricing(
                    part_id=pid,
                    company=cp.company,
                    msrp_usd=pdata.get("msrp_usd"),
                    map_usd=pdata.get("map_usd"),
                    cost_usd=dealer_cost_from_msrp(pdata.get("msrp_usd"), feed_type, creds_for_cost),
                )
                for pid, pdata in pricing_by_part_id.items()
            ]
            batch_total = 0
            for j in range(0, len(pricing_to_upsert), WP_PRICING_UPSERT_BATCH):
                batch = pricing_to_upsert[j : j + WP_PRICING_UPSERT_BATCH]
                pgbulk.upsert(
                    src_models.WheelProsCompanyPricing,
                    batch,
                    unique_fields=["part", "company"],
                    update_fields=["msrp_usd", "map_usd", "cost_usd", "updated_at"],
                    returning=False,
                )
                batch_total += len(batch)
                connection.close()
            total_pricing += batch_total
            logger.info(
                "{} Upserted {} WheelPros company pricing rows for company_id={}.".format(
                    _LOG_PREFIX,
                    batch_total,
                    cp.company_id,
                )
            )
        logger.info(
            "{} WheelPros pricing finished: {} rows across {} companies.".format(
                _LOG_PREFIX,
                total_pricing,
                len(pricing_cps),
            )
        )

    logger.info("{} WheelPros {} feed sync complete.".format(_LOG_PREFIX, feed_type))


def sync_wheelpros_company_pricing_for_company_provider(
    company_provider_id: int,
    download: bool = True,
    local_only: bool = False,
) -> None:
    """
    For one CompanyProviders row, download each WheelPros SFTP pricing feed (wheel, tire, accessories)
    and upsert WheelProsCompanyPricing. Parts must already exist for (brand, part_number).
    """
    wp_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    if not wp_provider:
        logger.warning("{} No WheelPros provider. Skipping.".format(_LOG_PREFIX))
        return

    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider_id=wp_provider.id,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active WheelPros CompanyProviders id={}. Skipping.".format(_LOG_PREFIX, company_provider_id)
        )
        return

    total_all = 0
    for feed_type in ("wheel", "tire", "accessories"):
        sftp_path = src_constants.WHEELPROS_FEED_PATHS.get(feed_type)
        if not sftp_path:
            continue
        local_path = "/tmp/wheelpros_{}_cp_{}.csv".format(feed_type, company_provider_id)

        creds = dict(cp.credentials or {})
        creds["sftp_path"] = sftp_path
        pc = wheelpros_client.WheelProsSFTPClient(
            credentials=creds,
            local_file_path=local_path,
            require_credentials=not local_only,
        )
        try:
            price_records = pc.get_feed_records(
                force_download=download,
                local_only=local_only,
                sftp_path=sftp_path,
                local_file_path=local_path,
            )
        except wheelpros_exceptions.WheelProsException as e:
            logger.error(
                "{} WheelPros {} pricing feed error company_provider id={}: {}.".format(
                    _LOG_PREFIX, feed_type, company_provider_id, str(e),
                )
            )
            raise

        if not price_records:
            logger.warning(
                "{} No rows for WheelPros {} pricing company_provider id={}.".format(
                    _LOG_PREFIX, feed_type, company_provider_id,
                )
            )
            continue

        brand_names = set()
        for row in price_records:
            name = _row_key(row, "Brand")
            if name:
                brand_names.add(_normalize_wheelpros_brand_key(name))
        brand_by_external = {
            b.external_id: b
            for b in src_models.WheelProsBrand.objects.filter(external_id__in=brand_names)
        }
        pmap = _wheelpros_pricing_map_from_records(price_records, brand_by_external)
        if not pmap:
            continue

        pairs = list(pmap.keys())
        id_by_brand_pn: typing.Dict[typing.Tuple[int, str], int] = {}
        chunk_size = 3000
        for i in range(0, len(pairs), chunk_size):
            chunk = pairs[i : i + chunk_size]
            if not chunk:
                continue
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM wheelpros_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(chunk),),
                )
                for pid, bid, pn in cur.fetchall():
                    id_by_brand_pn[(bid, (pn or "").strip())] = pid

        pricing_by_part_id: typing.Dict[int, typing.Dict[str, typing.Any]] = {}
        for (bid, pn), pdata in pmap.items():
            part_id = id_by_brand_pn.get((bid, pn))
            if not part_id:
                continue
            pricing_by_part_id[int(part_id)] = pdata
        creds_for_cost = dict(cp.credentials or {})
        pricing_to_upsert = [
            src_models.WheelProsCompanyPricing(
                part_id=pid,
                company=cp.company,
                msrp_usd=pdata.get("msrp_usd"),
                map_usd=pdata.get("map_usd"),
                cost_usd=dealer_cost_from_msrp(pdata.get("msrp_usd"), feed_type, creds_for_cost),
            )
            for pid, pdata in pricing_by_part_id.items()
        ]
        batch_total = 0
        for j in range(0, len(pricing_to_upsert), WP_PRICING_UPSERT_BATCH):
            batch = pricing_to_upsert[j : j + WP_PRICING_UPSERT_BATCH]
            pgbulk.upsert(
                src_models.WheelProsCompanyPricing,
                batch,
                unique_fields=["part", "company"],
                update_fields=["msrp_usd", "map_usd", "cost_usd", "updated_at"],
                returning=False,
            )
            batch_total += len(batch)
            connection.close()
        total_all += batch_total
        logger.info(
            "{} WheelPros {} company pricing: {} rows for company_provider id={}.".format(
                _LOG_PREFIX, feed_type, batch_total, company_provider_id,
            )
        )

    logger.info(
        "{} WheelPros company pricing total {} rows for company_provider id={}.".format(
            _LOG_PREFIX, total_all, company_provider_id,
        )
    )
