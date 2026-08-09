"""
Elite Wheel & Tire feed integration: one ``TriWeeklyUpdate<MM-DD-YYYY>.xlsx`` workbook per drop
holding the whole warehouse snapshot -- a worksheet per wheel manufacturer plus a final ``Tires``
sheet -- with on-hand counts per Elite location (Tampa / Atlanta / Miami / Decatur).

Elite's catalog is split across two part tables rather than one, because the two halves of the
workbook describe genuinely different products: wheels carry size / bolt pattern / offset /
centerbore / finish, tires carry model / packed size code / rim diameter, and neither set of
columns is meaningful for the other. So this populates:

  * EliteWheelBrand -- one row per manufacturer *per half* (external_id ``"WHEEL:AZARA"`` /
    ``"TIRE:LEXANI"``), since a manufacturer may appear on both sides;
  * EliteWheelPartWheel / EliteWheelPartTire -- catalog + distributor-wide per-location stock;
  * EliteWheelWheelCompanyPricing / EliteWheelTireCompanyPricing -- per company.

Structurally this follows A-Tech (flat file, per-warehouse qty stored on the part) and Vossen
(wheel-spec attributes, no vehicle fitment), not Rough Country.

Pricing note: Elite's public inventory share carries no price columns at all -- it is an
availability report. The shared catalog therefore ingests fine with no connections at all, and
``sync_elite_wheel_company_pricing_for_company_provider`` only writes rows once a dealer's own
SFTP feed (which does carry prices) is connected; until then it logs and no-ops rather than
writing nulls over anything.
"""
import logging
import typing
from decimal import Decimal, InvalidOperation

import pgbulk
from django.db.models.functions import Upper
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.elite_wheel import client as elite_wheel_client
from src.integrations.clients.elite_wheel import exceptions as elite_wheel_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ELITE-WHEEL-SERVICES]"

ELITE_WHEEL_UPSERT_BATCH = 5000

PRODUCT_TYPE_WHEEL = src_models.EliteWheelBrand.PRODUCT_TYPE_WHEEL
PRODUCT_TYPE_TIRE = src_models.EliteWheelBrand.PRODUCT_TYPE_TIRE

_QTY_FIELDS = list(src_constants.ELITE_WHEEL_QTY_FIELD_TO_LOCATION_LABEL.keys())

# EliteWheelBrand.external_id -> the exact Brands.name to use, bypassing the match cascade below
# (the row is created if it doesn't exist). Keyed on external_id rather than the bare name so the
# wheel and tire halves of one manufacturer name can resolve differently.
#
# Both entries below are real fuzzy mismatches caught on the first production run:
#   * Elite's "Spec-1" is a wheel line; the catalog's existing "SPEC" is Spec Clutch (hose lines,
#     pressure plates), and the word-prefix matcher happily collapsed the two.
#   * Elite's "Atlas" is Atlas Tire; the only Atlas in the catalog was "ATLAS SUSPENSION -
#     INACTIVE", a retired leaf-spring brand.
# Add an entry here whenever a new Elite manufacturer lands on the wrong Brands row.
_ELITE_WHEEL_BRAND_NAME_OVERRIDE: typing.Dict[str, str] = {
    "WHEEL:SPEC-1": "SPEC-1",
    "TIRE:ATLAS": "ATLAS TIRE",
}

# Retired brands are kept in the catalog with an "- INACTIVE" suffix on the name. They are valid
# exact-match targets (a feed genuinely naming one should still resolve), but never acceptable
# *fuzzy* targets: "ATLAS" scoring against "ATLAS SUSPENSION - INACTIVE" is how Elite's tire brand
# ended up on a dead leaf-spring row.
_INACTIVE_BRAND_NAME_MARKER = "INACTIVE"

_WHEEL_PART_UPDATE_FIELDS = [
    "group_label",
    "size",
    "bolt_pattern_1",
    "bolt_pattern_2",
    "offset",
    "center_bore",
    "finish",
    "total_available",
    "location_availability",
    "as_of_date",
    "source_filename",
    "raw_data",
    "updated_at",
] + _QTY_FIELDS

_TIRE_PART_UPDATE_FIELDS = [
    "model",
    "raw_size",
    "tire_diameter",
    "total_available",
    "location_availability",
    "as_of_date",
    "source_filename",
    "raw_data",
    "updated_at",
] + _QTY_FIELDS

# Price column names a dealer feed might use, in priority order. Elite's public share has none of
# these; a connected dealer's own workbook is expected to add them, and the exact spelling is not
# confirmed yet, so each target field accepts the plausible variants rather than one fixed label.
_PRICE_KEYS = {
    "cost": ("cost", "dealer_cost", "dealer_price", "your_price", "price"),
    "map": ("map", "map_price", "min_advertised_price"),
    "retail_price": ("retail", "retail_price", "msrp", "list_price"),
}


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


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


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


def brand_external_id(product_type: str, name: typing.Optional[str]) -> str:
    """``EliteWheelBrand.external_id`` for a feed manufacturer -- ``"WHEEL:AZARA"`` / ``"TIRE:LEXANI"``."""
    return "{}:{}".format(product_type, (name or "").strip().upper())


def _brand_name_upper_for_sync(brand: src_models.EliteWheelBrand) -> str:
    name_upper = (brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(brand.id)
    return name_upper


def _split_availability(
    availability: typing.Optional[typing.Dict[str, typing.Any]],
) -> typing.Tuple[typing.Dict[str, typing.Optional[int]], typing.Optional[int]]:
    """
    Map the workbook's ``{location: qty}`` onto the fixed ``qty_*`` columns and a total. Locations
    Elite adds later have no column of their own yet, but still land in the part's
    ``location_availability`` and are counted in the total, so stock is never silently dropped.
    """
    per_field: typing.Dict[str, typing.Optional[int]] = {field: None for field in _QTY_FIELDS}
    if not availability:
        return per_field, None
    total = 0
    for location, raw_qty in availability.items():
        qty = _safe_int(raw_qty) or 0
        total += qty
        field = src_constants.ELITE_WHEEL_LOCATION_TO_QTY_FIELD.get((location or "").strip())
        if field:
            per_field[field] = qty
    return per_field, total


# --------------------------------------------------------------------------------------
# Client / connection helpers
# --------------------------------------------------------------------------------------
def _elite_wheel_client_for_credentials(
    credentials: typing.Optional[typing.Dict],
    local_file_path: typing.Optional[str] = None,
    force_public_share: typing.Optional[bool] = None,
) -> elite_wheel_client.EliteWheelFeedClient:
    """Build a client from CompanyProviders.credentials (SFTP and/or public-share keys)."""
    return elite_wheel_client.EliteWheelFeedClient(
        credentials=credentials or {},
        local_file_path=local_file_path,
        force_public_share=force_public_share,
    )


def _active_elite_wheel_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.ELITE_WHEEL.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _catalog_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """
    Primary Elite connection, else the first active one. Only a *fallback* source for the shared
    catalog -- see :func:`fetch_and_save_elite_wheel` for why the public share is preferred.
    """
    elite_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ELITE_WHEEL.value,
    ).first()
    if not elite_provider:
        return None
    base = _active_elite_wheel_company_providers_queryset().filter(provider=elite_provider)
    primary = base.filter(primary=True).first()
    if primary:
        return primary
    fallback = base.order_by("id").first()
    if fallback:
        logger.info(
            "{} No primary Elite company provider; using company_id={} for catalog.".format(
                _LOG_PREFIX, fallback.company_id,
            )
        )
    return fallback


# --------------------------------------------------------------------------------------
# Brand + part building
# --------------------------------------------------------------------------------------
def _ensure_elite_wheel_brands(
    data: typing.Dict[str, typing.Any],
) -> typing.Dict[str, src_models.EliteWheelBrand]:
    """
    Ensure an EliteWheelBrand row exists for every manufacturer in each half of the workbook.
    Bulk-creates missing rows in one pass keyed by external_id.
    """
    wanted: typing.Dict[str, typing.Tuple[str, str]] = {}
    for product_type, rows in ((PRODUCT_TYPE_WHEEL, data.get("wheels")), (PRODUCT_TYPE_TIRE, data.get("tires"))):
        for row in rows or []:
            name = _safe_str(row.get("manufacturer"), 255)
            if not name:
                continue
            wanted[brand_external_id(product_type, name)] = (name, product_type)

    if not wanted:
        return {}

    existing = {
        b.external_id: b
        for b in src_models.EliteWheelBrand.objects.filter(external_id__in=list(wanted.keys()))
    }
    missing = [ext_id for ext_id in wanted if ext_id not in existing]
    if missing:
        src_models.EliteWheelBrand.objects.bulk_create(
            [
                src_models.EliteWheelBrand(
                    external_id=ext_id,
                    name=wanted[ext_id][0],
                    product_type=wanted[ext_id][1],
                )
                for ext_id in missing
            ],
            ignore_conflicts=True,
        )
        logger.info("{} Created {} new EliteWheelBrand rows.".format(_LOG_PREFIX, len(missing)))

    brands = {
        b.external_id: b
        for b in src_models.EliteWheelBrand.objects.filter(external_id__in=list(wanted.keys()))
    }
    logger.info("{} Using {} Elite brands.".format(_LOG_PREFIX, len(brands)))
    return brands


def _build_wheel_part(
    row: typing.Dict,
    brand: src_models.EliteWheelBrand,
    as_of_date,
    source_filename: typing.Optional[str],
) -> src_models.EliteWheelPartWheel:
    qty_by_field, total = _split_availability(row.get("availability"))
    return src_models.EliteWheelPartWheel(
        brand=brand,
        part_number=_safe_str(row.get("part_number"), 255) or "",
        group_label=_safe_str(row.get("group_label"), 512),
        size=_safe_str(row.get("size"), 128),
        bolt_pattern_1=_safe_str(row.get("bolt_pattern_1"), 128),
        bolt_pattern_2=_safe_str(row.get("bolt_pattern_2"), 128),
        offset=_safe_str(row.get("offset"), 64),
        center_bore=_safe_str(row.get("center_bore"), 64),
        finish=_safe_str(row.get("finish"), 255),
        total_available=total,
        location_availability=row.get("availability") or None,
        as_of_date=as_of_date,
        source_filename=_safe_str(source_filename, 255),
        raw_data=row,
        **qty_by_field,
    )


def _build_tire_part(
    row: typing.Dict,
    brand: src_models.EliteWheelBrand,
    as_of_date,
    source_filename: typing.Optional[str],
) -> src_models.EliteWheelPartTire:
    qty_by_field, total = _split_availability(row.get("availability"))
    return src_models.EliteWheelPartTire(
        brand=brand,
        part_number=_safe_str(row.get("part_number"), 255) or "",
        model=_safe_str(row.get("model"), 255),
        raw_size=_safe_str(row.get("raw_size"), 64),
        tire_diameter=_safe_str(row.get("tire_diameter"), 16),
        total_available=total,
        location_availability=row.get("availability") or None,
        as_of_date=as_of_date,
        source_filename=_safe_str(source_filename, 255),
        raw_data=row,
        **qty_by_field,
    )


def _zero_out_parts_missing_from_feed(model, as_of_date, label: str) -> int:
    """
    Zero the stock on parts that were in a previous workbook but not this one.

    Elite's workbook states its own scope in the banner -- "Including items with more than 0
    on-hand" -- so a part vanishing from the feed does not mean "unchanged", it means the last one
    sold. Without this the row keeps advertising whatever it had the day it dropped off, which is
    worse than showing nothing: it sends a dealer to order a tire Elite no longer has.

    Only rows strictly older than this run's ``as_of_date`` are touched, so a workbook with no
    readable banner (``as_of_date`` null) never triggers a mass zeroing.
    """
    if not as_of_date:
        logger.warning(
            "{} No as_of date on this workbook; skipping sold-out sweep for {}.".format(_LOG_PREFIX, label)
        )
        return 0
    stale = model.objects.filter(as_of_date__lt=as_of_date).exclude(total_available=0)
    updates = {field: 0 for field in _QTY_FIELDS}
    updates.update(total_available=0, location_availability=None, updated_at=timezone.now())
    count = stale.update(**updates)
    if count:
        logger.info(
            "{} Zeroed {} {} no longer listed in the {} workbook (sold out).".format(
                _LOG_PREFIX, count, label, as_of_date,
            )
        )
    return count


def _upsert_parts(
    model,
    instances: typing.List,
    update_fields: typing.List[str],
    label: str,
) -> int:
    if not instances:
        logger.warning("{} No Elite {} rows to upsert.".format(_LOG_PREFIX, label))
        return 0
    now = timezone.now()
    total = 0
    for start in range(0, len(instances), ELITE_WHEEL_UPSERT_BATCH):
        batch = instances[start:start + ELITE_WHEEL_UPSERT_BATCH]
        for part in batch:
            part.updated_at = now
        pgbulk.upsert(
            model,
            batch,
            unique_fields=["brand", "part_number"],
            update_fields=update_fields,
            returning=False,
        )
        total += len(batch)
        logger.info(
            "{} Upserted {}/{} Elite {}.".format(_LOG_PREFIX, total, len(instances), label)
        )
    return total


# --------------------------------------------------------------------------------------
# Catalog ingest (EliteWheelBrand + EliteWheelPartWheel + EliteWheelPartTire)
# --------------------------------------------------------------------------------------
def fetch_and_save_elite_wheel(
    local_file_path: typing.Optional[str] = None,
    force_public_share: typing.Optional[bool] = None,
) -> None:
    """
    Read the newest Elite workbook and upsert EliteWheelBrand plus both part tables (catalog +
    distributor-wide per-location stock). Per-company pricing (EliteWheel*CompanyPricing) is
    handled separately by IntegrationPricingSyncJob for each CompanyProvider.

    Source order is public share first, dealer connection only as a fallback. That is deliberate,
    and the opposite of what most providers do: Elite's public share needs no credentials, covers
    every warehouse, and is the same workbook a dealer account serves minus the price columns --
    which the catalog does not read anyway. Preferring a dealer connection here made the shared
    catalog for *every* company hostage to one dealer's credentials, and that is exactly how it
    broke: a connection saved with a placeholder host ("32") took the nightly Elite catalog down
    for three consecutive runs while the public share sat there working.
    """
    logger.info("{} Starting Elite Wheel feed sync.".format(_LOG_PREFIX))

    if local_file_path:
        sources: typing.List[typing.Tuple[str, typing.Dict]] = [("local file", {})]
    else:
        sources = [("public inventory share", {})]
        catalog_cp = _catalog_company_provider()
        if catalog_cp:
            sources.append(
                (
                    "company_id={} dealer feed".format(catalog_cp.company_id),
                    credentials_helper.get_feed_credentials(catalog_cp),
                )
            )

    data = None
    last_error: typing.Optional[Exception] = None
    for label, creds in sources:
        client = _elite_wheel_client_for_credentials(creds, local_file_path, force_public_share)
        try:
            data = client.get_feed_data()
            logger.info("{} Catalog feed read from {}.".format(_LOG_PREFIX, label))
            break
        except elite_wheel_exceptions.EliteWheelException as e:
            last_error = e
            logger.warning(
                "{} Catalog feed source {!r} failed: {}.".format(_LOG_PREFIX, label, str(e) or repr(e))
            )
    if data is None:
        logger.error(
            "{} Every Elite catalog feed source failed ({} tried).".format(_LOG_PREFIX, len(sources))
        )
        raise last_error if last_error else elite_wheel_exceptions.EliteWheelDownloadError(
            "No Elite catalog feed source available."
        )

    wheel_rows = data.get("wheels") or []
    tire_rows = data.get("tires") or []
    if not wheel_rows and not tire_rows:
        logger.warning("{} No Elite rows parsed from the workbook.".format(_LOG_PREFIX))
        return

    as_of_date = data.get("as_of")
    source_filename = data.get("source_filename")
    brands_by_external_id = _ensure_elite_wheel_brands(data)

    # Dedupe on (brand, part_number) -- the upsert conflict target -- so a part number repeated
    # across two blocks of the same sheet does not fail the batch.
    wheels_by_key: typing.Dict[typing.Tuple[int, str], src_models.EliteWheelPartWheel] = {}
    for row in wheel_rows:
        brand = brands_by_external_id.get(
            brand_external_id(PRODUCT_TYPE_WHEEL, row.get("manufacturer"))
        )
        part_number = _safe_str(row.get("part_number"), 255)
        if not brand or not part_number:
            continue
        wheels_by_key[(brand.id, part_number)] = _build_wheel_part(
            row, brand, as_of_date, source_filename
        )

    tires_by_key: typing.Dict[typing.Tuple[int, str], src_models.EliteWheelPartTire] = {}
    for row in tire_rows:
        brand = brands_by_external_id.get(
            brand_external_id(PRODUCT_TYPE_TIRE, row.get("manufacturer"))
        )
        part_number = _safe_str(row.get("part_number"), 255)
        if not brand or not part_number:
            continue
        tires_by_key[(brand.id, part_number)] = _build_tire_part(
            row, brand, as_of_date, source_filename
        )

    if len(wheels_by_key) < len(wheel_rows) or len(tires_by_key) < len(tire_rows):
        logger.info(
            "{} Deduped feed rows: wheels {} -> {}, tires {} -> {}.".format(
                _LOG_PREFIX, len(wheel_rows), len(wheels_by_key), len(tire_rows), len(tires_by_key),
            )
        )

    wheels_written = _upsert_parts(
        src_models.EliteWheelPartWheel, list(wheels_by_key.values()), _WHEEL_PART_UPDATE_FIELDS, "wheels"
    )
    tires_written = _upsert_parts(
        src_models.EliteWheelPartTire, list(tires_by_key.values()), _TIRE_PART_UPDATE_FIELDS, "tires"
    )

    # Sold-out sweep, but only for a warehouse-wide source. A dealer's own feed may legitimately
    # cover a subset of Elite's catalog, and treating everything outside that subset as sold out
    # would blank the catalog for every other company.
    source_mode = data.get("source_mode")
    if source_mode in (elite_wheel_client.SOURCE_MODE_PUBLIC_SHARE, "local_file"):
        _zero_out_parts_missing_from_feed(src_models.EliteWheelPartWheel, as_of_date, "wheels")
        _zero_out_parts_missing_from_feed(src_models.EliteWheelPartTire, as_of_date, "tires")
    else:
        logger.info(
            "{} Source is {!r}, not the warehouse-wide share; skipping the sold-out sweep.".format(
                _LOG_PREFIX, source_mode,
            )
        )

    logger.info(
        "{} Elite Wheel feed sync complete ({} wheels, {} tires from {}, as_of={}).".format(
            _LOG_PREFIX, wheels_written, tires_written, source_filename, as_of_date,
        )
    )


# --------------------------------------------------------------------------------------
# Brand mapping (EliteWheelBrand -> Brands)
# --------------------------------------------------------------------------------------
def sync_unmapped_elite_wheel_brands_to_brands() -> typing.List[src_models.EliteWheelBrand]:
    """
    For each EliteWheelBrand without a BrandEliteWheelBrandMapping: resolve Brands by exact name
    (uppercase), then compact-key, then fuzzy word-prefix match (same cascade as every other
    provider); otherwise create the Brand. Upserts BrandEliteWheelBrandMapping and BrandProviders.

    Like TireRack -- and unlike Quadratec -- this creates no CompanyBrands rows: Elite is a
    platform-wide catalog any connected company can browse, not one company's private brand set.
    A manufacturer selling both wheels and tires has one EliteWheelBrand row per half, and both
    map to the same Brands row.
    """
    logger.info("{} Syncing unmapped Elite brands to Brands.".format(_LOG_PREFIX))

    elite_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ELITE_WHEEL.value,
    ).first()
    if not elite_provider:
        logger.warning("{} Elite Wheel provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandEliteWheelBrandMapping.objects.values_list(
            "elite_wheel_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(
        src_models.EliteWheelBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped Elite brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped Elite brands.".format(_LOG_PREFIX, len(unmapped)))

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 0: explicit overrides for manufacturers the cascade is known to get wrong
    # (see _ELITE_WHEEL_BRAND_NAME_OVERRIDE). The target Brands row is created if missing, so an
    # override can name a brand the catalog does not carry yet.
    override_matches = 0
    override_names = {
        _ELITE_WHEEL_BRAND_NAME_OVERRIDE[eb.external_id]
        for eb in unmapped
        if eb.external_id in _ELITE_WHEEL_BRAND_NAME_OVERRIDE
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
                "{} Created {} Brands row(s) for Elite brand overrides.".format(_LOG_PREFIX, len(to_create))
            )
            existing_override_brands = {
                b.name: b for b in src_models.Brands.objects.filter(name__in=list(override_names))
            }
        for eb in unmapped:
            target = _ELITE_WHEEL_BRAND_NAME_OVERRIDE.get(eb.external_id)
            if target and target in existing_override_brands:
                resolved_by_id[eb.id] = existing_override_brands[target]
                override_matches += 1

    # Phase 1: exact uppercase-name match.
    name_upper_keys = {(eb.name or "").strip().upper() for eb in unmapped if (eb.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            brands_by_upper_name.setdefault((b.name or "").strip().upper(), b)
    for eb in unmapped:
        if eb.id in resolved_by_id:
            continue
        name_upper = (eb.name or "").strip().upper()
        if name_upper and name_upper in brands_by_upper_name:
            resolved_by_id[eb.id] = brands_by_upper_name[name_upper]

    # Phase 2: compact-key (punctuation/spacing-insensitive) -- "O.E. Revolution" vs "OE Revolution".
    compact_matches = 0
    still = [eb for eb in unmapped if eb.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for eb in still:
            key = normalize_compact_key(eb.name or "")
            if key and key in compact_index:
                resolved_by_id[eb.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix.
    unresolved = [eb for eb in unmapped if eb.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for eb in unresolved:
        tokens = normalize_upper_words(eb.name or "").split()
        candidates: typing.List[src_models.Brands] = list(first_index.get(tokens[0], ())) if tokens else []
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        candidates = [
            b for b in candidates if _INACTIVE_BRAND_NAME_MARKER not in (b.name or "").upper()
        ]
        brand = best_fuzzy_brand_match(eb.name or "", candidates) if candidates else None
        if brand:
            resolved_by_id[eb.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved. A manufacturer that sells both wheels
    # and tires has two EliteWheelBrand rows resolving to the same name, so this is keyed by name
    # and both mappings end up pointing at the one Brands row.
    new_brand_names: typing.Dict[str, None] = {}
    for eb in unmapped:
        if eb.id not in resolved_by_id:
            new_brand_names.setdefault(_brand_name_upper_for_sync(eb), None)

    created_brands = 0
    if new_brand_names:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_names.keys())).values_list(
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
            for name in new_brand_names
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_names.keys()))
        }
        for eb in unmapped:
            if eb.id not in resolved_by_id:
                resolved_by_id[eb.id] = by_name[_brand_name_upper_for_sync(eb)]

    mapping_models = [
        src_models.BrandEliteWheelBrandMapping(
            brand_id=resolved_by_id[eb.id].id,
            elite_wheel_brand_id=eb.id,
        )
        for eb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandEliteWheelBrandMapping,
        mapping_models,
        unique_fields=["brand", "elite_wheel_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    for eb in unmapped:
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=resolved_by_id[eb.id],
            provider=elite_provider,
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
# Per-company pricing (EliteWheelWheelCompanyPricing / EliteWheelTireCompanyPricing)
# --------------------------------------------------------------------------------------
def _row_prices(row: typing.Dict) -> typing.Dict[str, typing.Optional[Decimal]]:
    """
    Pull cost / map / retail out of one feed row, tolerating the column spellings a dealer feed
    might use (see ``_PRICE_KEYS``). The public inventory share has none of them, so this returns
    all-None for every public-share row and the caller skips the write entirely.
    """
    lowered = {
        str(key).strip().lower().replace(" ", "_"): value
        for key, value in row.items()
        if not isinstance(value, (dict, list))
    }
    prices: typing.Dict[str, typing.Optional[Decimal]] = {}
    for field, candidates in _PRICE_KEYS.items():
        value = None
        for candidate in candidates:
            if candidate in lowered:
                value = _safe_decimal(lowered[candidate])
                if value is not None:
                    break
        prices[field] = value
    return prices


def _upsert_company_pricing(
    model,
    rows: typing.List[typing.Dict],
    product_type: str,
    part_id_by_key: typing.Dict[typing.Tuple[str, str], int],
    company_id: int,
) -> typing.Tuple[int, int]:
    """
    Upsert one half's pricing rows. Returns ``(written, priced_rows)`` -- rows whose feed carried
    no price at all are never written, so an inventory-only feed cannot blank out prices a dealer
    feed previously set.
    """
    now = timezone.now()
    pricing_rows = []
    for row in rows:
        prices = _row_prices(row)
        if all(value is None for value in prices.values()):
            continue
        external_id = brand_external_id(product_type, row.get("manufacturer"))
        part_number = _safe_str(row.get("part_number"), 255)
        if not part_number:
            continue
        part_id = part_id_by_key.get((external_id, part_number))
        if not part_id:
            continue
        pricing_rows.append(
            model(
                part_id=part_id,
                company_id=company_id,
                cost=prices["cost"],
                map=prices["map"],
                retail_price=prices["retail_price"],
                updated_at=now,
            )
        )

    written = 0
    for start in range(0, len(pricing_rows), ELITE_WHEEL_UPSERT_BATCH):
        batch = pricing_rows[start:start + ELITE_WHEEL_UPSERT_BATCH]
        pgbulk.upsert(
            model,
            batch,
            unique_fields=["part", "company"],
            update_fields=["cost", "map", "retail_price", "updated_at"],
            returning=False,
        )
        written += len(batch)
    return written, len(pricing_rows)


def _part_id_by_brand_and_part_number(model) -> typing.Dict[typing.Tuple[str, str], int]:
    return {
        (row["brand__external_id"], row["part_number"]): row["id"]
        for row in model.objects.values("id", "part_number", "brand__external_id").iterator(
            chunk_size=5000
        )
    }


def sync_elite_wheel_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Read this company's own Elite feed and upsert EliteWheelWheelCompanyPricing /
    EliteWheelTireCompanyPricing. Keyed to existing part rows by (brand external_id, part_number);
    parts missing from the catalog tables are skipped (run fetch_and_save_elite_wheel first).

    A connection still on the public inventory share has no price columns to read. That is the
    expected state until Elite issues the dealer their SFTP account, so it is logged as a normal
    outcome and nothing is written -- not treated as a failure, and never written as null prices.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.ELITE_WHEEL.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active Elite CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    creds = credentials_helper.get_feed_credentials(cp)
    client = _elite_wheel_client_for_credentials(creds)
    source_mode = client.source_mode()
    try:
        data = client.get_feed_data()
    except elite_wheel_exceptions.EliteWheelException as e:
        logger.error(
            "{} Feed error for company_id={}: {}.".format(_LOG_PREFIX, cp.company_id, str(e))
        )
        raise

    wheel_rows = data.get("wheels") or []
    tire_rows = data.get("tires") or []
    if not wheel_rows and not tire_rows:
        logger.warning(
            "{} No Elite rows for company_id={}. Nothing to price.".format(_LOG_PREFIX, cp.company_id)
        )
        return

    wheels_written, wheels_priced = _upsert_company_pricing(
        src_models.EliteWheelWheelCompanyPricing,
        wheel_rows,
        PRODUCT_TYPE_WHEEL,
        _part_id_by_brand_and_part_number(src_models.EliteWheelPartWheel),
        cp.company_id,
    )
    tires_written, tires_priced = _upsert_company_pricing(
        src_models.EliteWheelTireCompanyPricing,
        tire_rows,
        PRODUCT_TYPE_TIRE,
        _part_id_by_brand_and_part_number(src_models.EliteWheelPartTire),
        cp.company_id,
    )

    if not wheels_priced and not tires_priced:
        logger.info(
            "{} company_id={}: Elite feed ({}) carries no price columns; no pricing rows written. "
            "This is expected until the dealer's own SFTP account is connected.".format(
                _LOG_PREFIX, cp.company_id, source_mode,
            )
        )
        return

    logger.info(
        "{} company_id={}: upserted {} wheel and {} tire Elite pricing rows (source={}).".format(
            _LOG_PREFIX, cp.company_id, wheels_written, tires_written, source_mode,
        )
    )
