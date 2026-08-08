"""
The Wheel Group feed integration: one ``US Wheel Data Mastersheet.xlsx`` workbook holding TWG's
whole US catalog on its ``US Data Mastersheet`` worksheet -- one row per SKU across their house
brands (Touren, Mayhem, ION Alloy, Cali Off-Road, Ridler, Dirty Life, Kraze, American Truxx,
Mazzi, TuffStuff, ION Trailer), with wheel specs, images, marketing copy, MSRP and MAP.

This populates:

  * TheWheelGroupBrand -- one row per manufacturer name in the sheet;
  * TheWheelGroupPart -- catalog + distributor-wide list prices (MSRP / MAP);
  * TheWheelGroupCompanyPricing -- per company.

Structurally this follows Vossen and Elite Wheel (flat wheel catalog, wheel-spec attributes, no
vehicle fitment), not Rough Country.

Two things about this feed shape are worth knowing before reading further:

  * **No stock.** The mastersheet is a catalog and list-price sheet with no quantity column at
    all, so TWG parts get no ProviderPartInventory row (see
    ``master_parts.sync_provider_details_from_the_wheel_group``). Availability arrives only when
    TWG starts delivering a real feed to our relay.
  * **No dealer cost.** MSRP and MAP are distributor-wide, so they live on the part. Per-company
    rows are still written -- carrying MAP/MSRP with a null cost -- so a connected company sees
    TWG list pricing immediately, and ``cost`` fills in on its own the moment a relay feed with a
    jobber/dealer column lands (see ``_row_prices`` and the client's cost column aliases).
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
from src.integrations.clients.the_wheel_group import client as the_wheel_group_client
from src.integrations.clients.the_wheel_group import exceptions as the_wheel_group_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[THE-WHEEL-GROUP-SERVICES]"

THE_WHEEL_GROUP_UPSERT_BATCH = 5000

# TheWheelGroupBrand.external_id -> the exact Brands.name to use, bypassing the match cascade
# below (the row is created if it doesn't exist). Same mechanism as Elite's override map; add an
# entry whenever a TWG brand lands on the wrong Brands row.
#
#   * "TUFFSTUFF" is written as one word in the sheet's BRAND column, but the brand is Tuff Stuff
#     Overland -- the sheet's own sales copy spells it out ("TUFF STUFF OVERLAND ASCENT WHEEL").
#     The word-prefix matcher can't bridge a token-boundary difference, so without this it would
#     create a stray "TUFFSTUFF" brand alongside the real one.
_THE_WHEEL_GROUP_BRAND_NAME_OVERRIDE: typing.Dict[str, str] = {
    "TUFFSTUFF": "TUFF STUFF OVERLAND",
}

# Retired brands are kept in the catalog with an "- INACTIVE" suffix. They are valid exact-match
# targets, but never acceptable *fuzzy* targets -- see the same guard in the Elite Wheel service.
_INACTIVE_BRAND_NAME_MARKER = "INACTIVE"

_PART_UPDATE_FIELDS = [
    "aaia_code",
    "name",
    "style_number",
    "description",
    "short_description",
    "diameter",
    "wheel_width",
    "hub_bore",
    "bolt_pattern_1",
    "bolt_pattern_2",
    "offset",
    "offset_class",
    "backspace",
    "wheel_lip_size",
    "load_rating",
    "color",
    "finish",
    "upc",
    "country_of_origin",
    "division",
    "group_code",
    "wheel_cap",
    "screw",
    "dually_wheel",
    "winter_approved",
    "tpms_compatible",
    "lugnut_open_closed",
    "lugnut_type_1",
    "lugnut_type_2",
    "lugseat_type",
    "structure_warranty",
    "finish_warranty",
    "beadlock_instructions_url",
    "box_width",
    "box_height",
    "box_depth",
    "product_weight",
    "ship_weight",
    "image_1",
    "image_2",
    "image_3",
    "image_4",
    "note",
    "comment",
    "bullet_points",
    "sales_description",
    "msrp",
    "map_price",
    "map_enforced",
    "source_filename",
    "raw_data",
    "updated_at",
]

_TRUE_TOKENS = frozenset({"YES", "Y", "TRUE", "1"})
_FALSE_TOKENS = frozenset({"NO", "N", "FALSE", "0"})

# TWG writes "N/A" in beadlock_instructions_url for every wheel that isn't a beadlock; only a real
# link is worth keeping.
_URL_PREFIXES = ("http://", "https://")


# --------------------------------------------------------------------------------------
# Value coercion helpers
# --------------------------------------------------------------------------------------
def _safe_str(value: typing.Any, max_len: typing.Optional[int] = None) -> typing.Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:max_len] if max_len else text


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None:
        return None
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _safe_price(value: typing.Any) -> typing.Optional[Decimal]:
    """A price, or None for anything non-positive -- TWG writes 0 for "no price", not zero cost."""
    amount = _safe_decimal(value)
    if amount is None or amount <= 0:
        return None
    return amount


def _safe_bool(value: typing.Any) -> typing.Optional[bool]:
    text = (str(value).strip().upper() if value is not None else "")
    if text in _TRUE_TOKENS:
        return True
    if text in _FALSE_TOKENS:
        return False
    return None


def _safe_url(value: typing.Any, max_len: int) -> typing.Optional[str]:
    text = _safe_str(value, max_len)
    if not text or not text.lower().startswith(_URL_PREFIXES):
        return None
    return text


def brand_external_id(name: typing.Optional[str]) -> str:
    """``TheWheelGroupBrand.external_id`` for a feed brand name -- the uppercased name."""
    return (name or "").strip().upper()


def _brand_name_upper_for_sync(brand: src_models.TheWheelGroupBrand) -> str:
    name_upper = (brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(brand.id)
    return name_upper


# --------------------------------------------------------------------------------------
# Client / connection helpers
# --------------------------------------------------------------------------------------
def _the_wheel_group_client_for_credentials(
    credentials: typing.Optional[typing.Dict],
    local_file_path: typing.Optional[str] = None,
    force_public_share: typing.Optional[bool] = None,
) -> the_wheel_group_client.TheWheelGroupFeedClient:
    """Build a client from CompanyProviders.credentials (relay account and/or share override)."""
    return the_wheel_group_client.TheWheelGroupFeedClient(
        credentials=credentials or {},
        local_file_path=local_file_path,
        force_public_share=force_public_share,
    )


def _active_the_wheel_group_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.THE_WHEEL_GROUP.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _catalog_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """
    Primary TWG connection for the shared catalog; else the first active one. Like Elite Wheel and
    unlike every API-backed provider this may legitimately be None -- the public share needs no
    credentials, so the catalog ingest runs before any dealer has connected.
    """
    twg_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.THE_WHEEL_GROUP.value,
    ).first()
    if not twg_provider:
        return None
    base = _active_the_wheel_group_company_providers_queryset().filter(provider=twg_provider)
    primary = base.filter(primary=True).first()
    if primary:
        return primary
    fallback = base.order_by("id").first()
    if fallback:
        logger.info(
            "{} No primary TWG company provider; using company_id={} for catalog.".format(
                _LOG_PREFIX, fallback.company_id,
            )
        )
    return fallback


# --------------------------------------------------------------------------------------
# Brand + part building
# --------------------------------------------------------------------------------------
def _dominant_aaia_code_by_brand(
    rows: typing.List[typing.Dict],
) -> typing.Dict[str, typing.Optional[str]]:
    """
    external_id -> the AAIA code most of that brand's rows carry. TWG's codes are per-SKU and a
    couple of brands span two (Ridler is DXSV plus a handful of GVSK), so the brand-level code is
    the majority one; the per-row code is what actually reaches MasterPart.aaia_code.
    """
    counts: typing.Dict[str, typing.Dict[str, int]] = {}
    for row in rows:
        name = _safe_str(row.get("brand"), 255)
        code = _safe_str(row.get("aaia_code"), 255)
        if not name or not code:
            continue
        counts.setdefault(brand_external_id(name), {})
        counts[brand_external_id(name)][code] = counts[brand_external_id(name)].get(code, 0) + 1
    return {
        external_id: max(by_code.items(), key=lambda item: item[1])[0]
        for external_id, by_code in counts.items()
    }


def _ensure_the_wheel_group_brands(
    rows: typing.List[typing.Dict],
) -> typing.Dict[str, src_models.TheWheelGroupBrand]:
    """
    Ensure a TheWheelGroupBrand row exists for every brand in the sheet. Bulk-creates missing rows
    in one pass keyed by external_id, and keeps each brand's AAIA code current.
    """
    wanted: typing.Dict[str, str] = {}
    for row in rows:
        name = _safe_str(row.get("brand"), 255)
        if not name:
            continue
        wanted[brand_external_id(name)] = name

    if not wanted:
        return {}

    aaia_by_external_id = _dominant_aaia_code_by_brand(rows)
    existing = {
        b.external_id: b
        for b in src_models.TheWheelGroupBrand.objects.filter(external_id__in=list(wanted.keys()))
    }
    missing = [ext_id for ext_id in wanted if ext_id not in existing]
    if missing:
        src_models.TheWheelGroupBrand.objects.bulk_create(
            [
                src_models.TheWheelGroupBrand(
                    external_id=ext_id,
                    name=wanted[ext_id],
                    aaia_code=aaia_by_external_id.get(ext_id),
                )
                for ext_id in missing
            ],
            ignore_conflicts=True,
        )
        logger.info(
            "{} Created {} new TheWheelGroupBrand rows.".format(_LOG_PREFIX, len(missing))
        )

    brands = {
        b.external_id: b
        for b in src_models.TheWheelGroupBrand.objects.filter(external_id__in=list(wanted.keys()))
    }

    stale_aaia = [
        b
        for b in brands.values()
        if aaia_by_external_id.get(b.external_id)
        and b.aaia_code != aaia_by_external_id[b.external_id]
    ]
    if stale_aaia:
        for b in stale_aaia:
            b.aaia_code = aaia_by_external_id[b.external_id]
        src_models.TheWheelGroupBrand.objects.bulk_update(stale_aaia, ["aaia_code"])

    logger.info("{} Using {} TWG brands.".format(_LOG_PREFIX, len(brands)))
    return brands


def _build_part(
    row: typing.Dict,
    brand: src_models.TheWheelGroupBrand,
    source_filename: typing.Optional[str],
) -> src_models.TheWheelGroupPart:
    map_enforced = _safe_bool(row.get("map_enforced"))
    map_price = _safe_price(row.get("map_price"))
    return src_models.TheWheelGroupPart(
        brand=brand,
        sku=_safe_str(row.get("sku"), 255) or "",
        aaia_code=_safe_str(row.get("aaia_code"), 255),
        name=_safe_str(row.get("name"), 255),
        style_number=_safe_str(row.get("style_number"), 64),
        description=_safe_str(row.get("description")),
        short_description=_safe_str(row.get("short_description")),
        diameter=_safe_str(row.get("diameter"), 32),
        wheel_width=_safe_str(row.get("wheel_width"), 32),
        hub_bore=_safe_str(row.get("hub_bore"), 32),
        bolt_pattern_1=_safe_str(row.get("bolt_pattern_1"), 64),
        bolt_pattern_2=_safe_str(row.get("bolt_pattern_2"), 64),
        offset=_safe_str(row.get("offset"), 32),
        offset_class=_safe_str(row.get("offset_class"), 32),
        backspace=_safe_str(row.get("backspace"), 32),
        wheel_lip_size=_safe_str(row.get("wheel_lip_size"), 32),
        load_rating=_safe_str(row.get("load_rating"), 32),
        color=_safe_str(row.get("color"), 64),
        finish=_safe_str(row.get("finish"), 128),
        upc=_safe_str(row.get("upc"), 64),
        country_of_origin=_safe_str(row.get("country_of_origin"), 64),
        division=_safe_str(row.get("division"), 64),
        group_code=_safe_str(row.get("group_code"), 64),
        wheel_cap=_safe_str(row.get("wheel_cap"), 64),
        screw=_safe_str(row.get("screw"), 64),
        dually_wheel=_safe_bool(row.get("dually_wheel")),
        winter_approved=_safe_bool(row.get("winter_approved")),
        tpms_compatible=_safe_bool(row.get("tpms_compatible")),
        lugnut_open_closed=_safe_str(row.get("lugnut_open_closed"), 32),
        lugnut_type_1=_safe_str(row.get("lugnut_type_1"), 32),
        lugnut_type_2=_safe_str(row.get("lugnut_type_2"), 32),
        lugseat_type=_safe_str(row.get("lugseat_type"), 32),
        structure_warranty=_safe_str(row.get("structure_warranty"), 128),
        finish_warranty=_safe_str(row.get("finish_warranty"), 128),
        beadlock_instructions_url=_safe_url(row.get("beadlock_instructions_url"), 512),
        box_width=_safe_str(row.get("box_width"), 32),
        box_height=_safe_str(row.get("box_height"), 32),
        box_depth=_safe_str(row.get("box_depth"), 32),
        product_weight=_safe_str(row.get("product_weight"), 32),
        ship_weight=_safe_str(row.get("ship_weight"), 32),
        image_1=_safe_url(row.get("image_1"), 1024),
        image_2=_safe_url(row.get("image_2"), 1024),
        image_3=_safe_url(row.get("image_3"), 1024),
        image_4=_safe_url(row.get("image_4"), 1024),
        note=_safe_str(row.get("note")),
        comment=_safe_str(row.get("comment")),
        bullet_points=_safe_str(row.get("bullet_points")),
        sales_description=_safe_str(row.get("sales_description")),
        msrp=_safe_price(row.get("msrp")),
        # MAP only counts when TWG flags the SKU as MAP-enforced. The two always agree today
        # (every non-enforced row carries 0), but a 0 with the flag set would still be no price.
        map_price=map_price if map_enforced is not False else None,
        map_enforced=map_enforced,
        source_filename=_safe_str(source_filename, 255),
        raw_data=row,
    )


def _upsert_parts(instances: typing.List[src_models.TheWheelGroupPart]) -> int:
    if not instances:
        logger.warning("{} No TWG part rows to upsert.".format(_LOG_PREFIX))
        return 0
    now = timezone.now()
    total = 0
    for start in range(0, len(instances), THE_WHEEL_GROUP_UPSERT_BATCH):
        batch = instances[start:start + THE_WHEEL_GROUP_UPSERT_BATCH]
        for part in batch:
            part.updated_at = now
        pgbulk.upsert(
            src_models.TheWheelGroupPart,
            batch,
            unique_fields=["brand", "sku"],
            update_fields=_PART_UPDATE_FIELDS,
            returning=False,
        )
        total += len(batch)
        logger.info("{} Upserted {}/{} TWG parts.".format(_LOG_PREFIX, total, len(instances)))
    return total


# --------------------------------------------------------------------------------------
# Catalog ingest (TheWheelGroupBrand + TheWheelGroupPart)
# --------------------------------------------------------------------------------------
def fetch_and_save_the_wheel_group(
    local_file_path: typing.Optional[str] = None,
    force_public_share: typing.Optional[bool] = None,
) -> None:
    """
    Read the newest TWG mastersheet and upsert TheWheelGroupBrand plus TheWheelGroupPart (catalog +
    distributor-wide MSRP/MAP). Uses the primary TWG CompanyProvider's credentials when one exists,
    and TWG's public share otherwise -- the share needs no credentials, so the catalog stays
    current even with no dealer connected. Per-company pricing (TheWheelGroupCompanyPricing) is
    handled separately by IntegrationPricingSyncJob for each CompanyProvider.
    """
    logger.info("{} Starting The Wheel Group feed sync.".format(_LOG_PREFIX))

    catalog_cp = _catalog_company_provider()
    catalog_creds = credentials_helper.get_feed_credentials(catalog_cp) if catalog_cp else {}
    if catalog_cp:
        logger.info(
            "{} Catalog feed using company_id={} (primary={}).".format(
                _LOG_PREFIX, catalog_cp.company_id, catalog_cp.primary,
            )
        )
    else:
        logger.info(
            "{} No TWG CompanyProviders yet; reading the public share.".format(_LOG_PREFIX)
        )

    client = _the_wheel_group_client_for_credentials(
        catalog_creds, local_file_path, force_public_share
    )
    try:
        data = client.get_feed_data()
    except the_wheel_group_exceptions.TheWheelGroupException as e:
        logger.error("{} Feed error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    rows = data.get("parts") or []
    if not rows:
        logger.warning("{} No TWG rows parsed from the workbook.".format(_LOG_PREFIX))
        return

    source_filename = data.get("source_filename")
    brands_by_external_id = _ensure_the_wheel_group_brands(rows)

    # Dedupe on (brand, sku) -- the upsert conflict target -- so a SKU repeated in the sheet does
    # not fail the batch.
    parts_by_key: typing.Dict[typing.Tuple[int, str], src_models.TheWheelGroupPart] = {}
    for row in rows:
        brand = brands_by_external_id.get(brand_external_id(row.get("brand")))
        sku = _safe_str(row.get("sku"), 255)
        if not brand or not sku:
            continue
        parts_by_key[(brand.id, sku)] = _build_part(row, brand, source_filename)

    if len(parts_by_key) < len(rows):
        logger.info(
            "{} Deduped feed rows: {} -> {}.".format(_LOG_PREFIX, len(rows), len(parts_by_key))
        )

    written = _upsert_parts(list(parts_by_key.values()))

    logger.info(
        "{} The Wheel Group feed sync complete ({} parts from {}).".format(
            _LOG_PREFIX, written, source_filename,
        )
    )


# --------------------------------------------------------------------------------------
# Brand mapping (TheWheelGroupBrand -> Brands)
# --------------------------------------------------------------------------------------
def sync_unmapped_the_wheel_group_brands_to_brands() -> typing.List[src_models.TheWheelGroupBrand]:
    """
    For each TheWheelGroupBrand without a BrandTheWheelGroupBrandMapping: resolve Brands by exact
    name (uppercase), then compact-key, then fuzzy word-prefix match (same cascade as every other
    provider); otherwise create the Brand. Upserts BrandTheWheelGroupBrandMapping and
    BrandProviders.

    Like Elite Wheel and TireRack -- and unlike Quadratec -- this creates no CompanyBrands rows:
    TWG is a platform-wide catalog any connected company can browse, not one company's private
    brand set.
    """
    logger.info("{} Syncing unmapped TWG brands to Brands.".format(_LOG_PREFIX))

    twg_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.THE_WHEEL_GROUP.value,
    ).first()
    if not twg_provider:
        logger.warning("{} The Wheel Group provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandTheWheelGroupBrandMapping.objects.values_list(
            "the_wheel_group_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(
        src_models.TheWheelGroupBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped TWG brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped TWG brands.".format(_LOG_PREFIX, len(unmapped)))

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 0: explicit overrides for brands the cascade is known to get wrong (see
    # _THE_WHEEL_GROUP_BRAND_NAME_OVERRIDE). The target Brands row is created if missing, so an
    # override can name a brand the catalog does not carry yet.
    override_matches = 0
    override_names = {
        _THE_WHEEL_GROUP_BRAND_NAME_OVERRIDE[b.external_id]
        for b in unmapped
        if b.external_id in _THE_WHEEL_GROUP_BRAND_NAME_OVERRIDE
    }
    if override_names:
        existing_override_brands = {
            b.name: b for b in src_models.Brands.objects.filter(name__in=list(override_names))
        }
        to_create = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for name in override_names
            if name not in existing_override_brands
        ]
        if to_create:
            src_models.Brands.objects.bulk_create(to_create, ignore_conflicts=True)
            logger.info(
                "{} Created {} Brands row(s) for TWG brand overrides.".format(
                    _LOG_PREFIX, len(to_create)
                )
            )
            existing_override_brands = {
                b.name: b for b in src_models.Brands.objects.filter(name__in=list(override_names))
            }
        for b in unmapped:
            target = _THE_WHEEL_GROUP_BRAND_NAME_OVERRIDE.get(b.external_id)
            if target and target in existing_override_brands:
                resolved_by_id[b.id] = existing_override_brands[target]
                override_matches += 1

    # Phase 1: exact uppercase-name match.
    name_upper_keys = {
        (b.name or "").strip().upper() for b in unmapped if (b.name or "").strip()
    }
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            brands_by_upper_name.setdefault((b.name or "").strip().upper(), b)
    for twg_brand in unmapped:
        if twg_brand.id in resolved_by_id:
            continue
        name_upper = (twg_brand.name or "").strip().upper()
        if name_upper and name_upper in brands_by_upper_name:
            resolved_by_id[twg_brand.id] = brands_by_upper_name[name_upper]

    # Phase 2: compact-key (punctuation/spacing-insensitive) -- "CALI OFF-ROAD" vs "Cali Offroad".
    compact_matches = 0
    still = [b for b in unmapped if b.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for twg_brand in still:
            key = normalize_compact_key(twg_brand.name or "")
            if key and key in compact_index:
                resolved_by_id[twg_brand.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix -- "ION ALLOY" / "ION TRAILER" both resolve to "ION",
    # "DIRTY LIFE" to "DIRTY LIFE WHEELS".
    unresolved = [b for b in unmapped if b.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for twg_brand in unresolved:
        tokens = normalize_upper_words(twg_brand.name or "").split()
        candidates: typing.List[src_models.Brands] = (
            list(first_index.get(tokens[0], ())) if tokens else []
        )
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        candidates = [
            b for b in candidates if _INACTIVE_BRAND_NAME_MARKER not in (b.name or "").upper()
        ]
        brand = best_fuzzy_brand_match(twg_brand.name or "", candidates) if candidates else None
        if brand:
            resolved_by_id[twg_brand.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved, carrying TWG's own AAIA code.
    new_brands: typing.Dict[str, typing.Optional[str]] = {}
    for twg_brand in unmapped:
        if twg_brand.id not in resolved_by_id:
            new_brands.setdefault(_brand_name_upper_for_sync(twg_brand), twg_brand.aaia_code)

    created_brands = 0
    if new_brands:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brands.keys())).values_list(
                "name", flat=True
            )
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=aaia_code,
            )
            for name, aaia_code in new_brands.items()
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brands.keys()))
        }
        for twg_brand in unmapped:
            if twg_brand.id not in resolved_by_id:
                resolved_by_id[twg_brand.id] = by_name[_brand_name_upper_for_sync(twg_brand)]

    mapping_models = [
        src_models.BrandTheWheelGroupBrandMapping(
            brand_id=resolved_by_id[b.id].id,
            the_wheel_group_brand_id=b.id,
        )
        for b in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandTheWheelGroupBrandMapping,
        mapping_models,
        unique_fields=["brand", "the_wheel_group_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    for twg_brand in unmapped:
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=resolved_by_id[twg_brand.id],
            provider=twg_provider,
        )
        if bp_created:
            created_brand_providers += 1

    logger.info(
        "{} Sync complete. Brands created: {}, override: {}, compact: {}, fuzzy: {}, mappings: {}, "
        "BrandProviders: {}.".format(
            _LOG_PREFIX, created_brands, override_matches, compact_matches, fuzzy_matches,
            len(mapping_models), created_brand_providers,
        )
    )
    return unmapped


# --------------------------------------------------------------------------------------
# Per-company pricing (TheWheelGroupCompanyPricing)
# --------------------------------------------------------------------------------------
def _row_prices(row: typing.Dict) -> typing.Dict[str, typing.Optional[Decimal]]:
    """
    cost / map / retail for one feed row. The public mastersheet has no cost column, so ``cost``
    is None until a relay feed carries one (the client maps every plausible spelling of it onto
    ``cost`` -- see its header alias table).
    """
    map_enforced = _safe_bool(row.get("map_enforced"))
    map_price = _safe_price(row.get("map_price"))
    return {
        "cost": _safe_price(row.get("cost")),
        "map": map_price if map_enforced is not False else None,
        "retail_price": _safe_price(row.get("msrp")),
    }


def _part_id_by_brand_and_sku() -> typing.Dict[typing.Tuple[str, str], int]:
    return {
        (row["brand__external_id"], row["sku"]): row["id"]
        for row in src_models.TheWheelGroupPart.objects.values(
            "id", "sku", "brand__external_id"
        ).iterator(chunk_size=5000)
    }


def sync_the_wheel_group_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Read this company's own TWG feed and upsert TheWheelGroupCompanyPricing. Keyed to existing part
    rows by (brand external_id, sku); parts missing from the catalog table are skipped (run
    fetch_and_save_the_wheel_group first).

    A connection still on the public share has no dealer cost to read, so its rows carry MAP and
    MSRP with a null cost -- that is the expected state until TWG delivers to our relay, and it
    still gives the company real list pricing to work from. Rows with no price at all are never
    written, so an empty feed cannot blank out prices a real dealer feed previously set.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.THE_WHEEL_GROUP.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active TWG CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    creds = credentials_helper.get_feed_credentials(cp)
    client = _the_wheel_group_client_for_credentials(creds)
    source_mode = client.source_mode()
    try:
        data = client.get_feed_data()
    except the_wheel_group_exceptions.TheWheelGroupException as e:
        logger.error(
            "{} Feed error for company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e))
        )
        raise

    rows = data.get("parts") or []
    if not rows:
        logger.warning(
            "{} No TWG rows for company_id={}. Nothing to price.".format(
                _LOG_PREFIX, cp.company_id
            )
        )
        return

    part_id_by_key = _part_id_by_brand_and_sku()
    now = timezone.now()
    pricing_rows = []
    for row in rows:
        prices = _row_prices(row)
        if all(value is None for value in prices.values()):
            continue
        sku = _safe_str(row.get("sku"), 255)
        if not sku:
            continue
        part_id = part_id_by_key.get((brand_external_id(row.get("brand")), sku))
        if not part_id:
            continue
        pricing_rows.append(
            src_models.TheWheelGroupCompanyPricing(
                part_id=part_id,
                company_id=cp.company_id,
                cost=prices["cost"],
                map=prices["map"],
                retail_price=prices["retail_price"],
                updated_at=now,
            )
        )

    written = 0
    for start in range(0, len(pricing_rows), THE_WHEEL_GROUP_UPSERT_BATCH):
        batch = pricing_rows[start:start + THE_WHEEL_GROUP_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.TheWheelGroupCompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["cost", "map", "retail_price", "updated_at"],
            returning=False,
        )
        written += len(batch)

    priced_with_cost = sum(1 for row in pricing_rows if row.cost is not None)
    if not priced_with_cost:
        logger.info(
            "{} company_id={}: TWG feed ({}) carries no dealer cost column; wrote {} MAP/MSRP-only "
            "rows. This is expected until TWG delivers to our relay.".format(
                _LOG_PREFIX, cp.company_id, source_mode, written,
            )
        )
        return

    logger.info(
        "{} company_id={}: upserted {} TWG pricing rows ({} with cost, source={}).".format(
            _LOG_PREFIX, cp.company_id, written, priced_with_cost, source_mode,
        )
    )
