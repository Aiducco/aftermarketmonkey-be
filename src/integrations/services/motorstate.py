"""
Motor State Distributing raw-feed ingest.

Populates three raw mirror tables (MotorStateBrand, MotorStateAvailability,
MotorStateProduct) from the Motor State API. Nothing here touches the
master-parts layer — this is the raw first stage only.

Flow:
  * ``fetch_and_save_motorstate_brands`` — GET /api/Brands -> MotorStateBrand.
  * ``fetch_and_save_motorstate_availability_full`` — one epoch-dated
    ProductAvailabilityChange pass per brand builds the full part-number spine
    (the only way to enumerate every part number, since there is no product
    export). Brands whose part count exceeds the 5000-row cap are recovered by
    walking a fromDateTime ladder and unioning.
  * ``fetch_and_save_motorstate_availability_updates`` — periodic incremental
    poll from the stored high-water mark (max source_updated_on); the Turn14
    inventory-updates analog.
  * ``fetch_and_save_motorstate_products`` — parallel /api/Product lookups
    (<=15 part numbers each) over the spine, for detail + account pricing.
"""
import decimal
import logging
import time
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

import pgbulk
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from django.db.models.functions import Upper

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.motorstate import client as motorstate_client
from src.integrations.clients.motorstate import exceptions as motorstate_exceptions
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MOTOR-STATE-SERVICES]"

# Epoch date that dumps the whole catalog through the availability endpoint.
_EPOCH_FROM_DATE = "1900-01-01"

# fromDateTime ladder for brands that exceed the 5000-row availability cap. Only
# brands large enough to truncate ever trigger this (a single call otherwise),
# so the extra requests are spent only where they are actually needed.
_AVAILABILITY_DATE_LADDER = [
    "1900-01-01", "2015-01-01", "2018-01-01", "2019-06-01",
    "2020-01-01", "2020-07-01", "2021-01-01", "2021-07-01",
    "2022-01-01", "2022-07-01", "2023-01-01", "2023-07-01",
    "2024-01-01", "2024-07-01", "2025-01-01", "2025-04-01",
    "2025-07-01", "2025-10-01", "2026-01-01", "2026-03-01",
    "2026-05-01", "2026-06-01", "2026-07-01", "2026-08-01",
]

# Re-poll a small window before the last-seen change to tolerate clock skew and
# rows sharing a timestamp boundary; upserts make the overlap harmless.
_INCREMENTAL_OVERLAP = timedelta(minutes=30)

_DEFAULT_PRODUCT_WORKERS = 5

# Rows buffered on the main thread before a single bulk upsert (~9 chunks' worth).
_PRODUCT_FLUSH_SIZE = 2000

_AVAILABILITY_UPDATE_FIELDS_SPINE = [
    "brand_code", "status_type", "quantity_available", "source_updated_on", "updated_at",
]
# Incremental polls carry no brand filter, so brand_code is left untouched to
# avoid clobbering the brand recorded during the spine pass.
_AVAILABILITY_UPDATE_FIELDS_INCREMENTAL = [
    "status_type", "quantity_available", "source_updated_on", "updated_at",
]

# Catalog columns (global MotorStateProduct row).
_PRODUCT_UPDATE_FIELDS = [
    "brand", "brand_code", "found", "vendor_part_number", "supersede_part_number",
    "short_description", "status", "is_stocking", "quantity",
    "can_special_order", "can_drop_ship", "can_regular_back_order",
    "data", "updated_at",
]

# Price columns (per-company MotorStateCompanyPricing row).
_COMPANY_PRICING_UPDATE_FIELDS = [
    "customer_price", "customer_price_non_promotional", "base_price", "list_price",
    "map_price", "is_map_restricted", "special_order_charge", "drop_ship_charge",
    "updated_at",
]


# ---------------------------------------------------------------------------
# Connection / client resolution
# ---------------------------------------------------------------------------
def _active_motorstate_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        active=True,
    ).select_related("company", "provider")


def _resolve_company_provider(
    company_provider_id: typing.Optional[int] = None,
) -> typing.Optional[src_models.CompanyProviders]:
    """Explicit id when given, else primary active connection, else first active."""
    base = _active_motorstate_company_providers_queryset()
    if company_provider_id is not None:
        return base.filter(id=company_provider_id).first()
    return base.filter(primary=True).first() or base.first()


def _build_client(
    company_provider: src_models.CompanyProviders,
) -> motorstate_client.MotorStateApiClient:
    credentials = credentials_helper.get_feed_credentials(company_provider)
    return motorstate_client.MotorStateApiClient(credentials=credentials)


def _json_safe(value: typing.Any) -> typing.Any:
    """Recursively convert Decimals (from the client's parse_float=Decimal) to floats so
    the raw ``data`` blob is JSON-serializable. Exact prices are kept in the typed
    DecimalFields; this copy is a lossy-but-faithful raw mirror."""
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _parse_source_updated_on(value: typing.Optional[str]):
    """Motor State 'UpdatedOn' (naive ISO, treated as UTC) -> aware datetime, or None."""
    if not value:
        return None
    parsed = parse_datetime(str(value))
    if parsed is None:
        return None
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.utc)
    return parsed


# ---------------------------------------------------------------------------
# Brands
# ---------------------------------------------------------------------------
def fetch_and_save_motorstate_brands(company_provider_id: typing.Optional[int] = None) -> None:
    logger.info("{} Started fetching Motor State brands.".format(_LOG_PREFIX))
    company_provider = _resolve_company_provider(company_provider_id)
    if not company_provider:
        logger.info("{} No active Motor State provider found.".format(_LOG_PREFIX))
        return

    client = _build_client(company_provider)
    brands_data = client.get_brands()
    if not brands_data:
        logger.warning("{} No brands returned from Motor State.".format(_LOG_PREFIX))
        return

    instances = _transform_brands(brands_data)
    pgbulk.upsert(
        src_models.MotorStateBrand,
        instances,
        unique_fields=["code"],
        update_fields=["name", "offered", "is_inventory_available", "data", "updated_at"],
        returning=False,
    )
    logger.info("{} Upserted {} Motor State brands.".format(_LOG_PREFIX, len(instances)))


def _transform_brands(brands_data: typing.List[typing.Dict]) -> typing.List[src_models.MotorStateBrand]:
    instances: typing.List[src_models.MotorStateBrand] = []
    for brand in brands_data:
        code = (brand.get("Code") or "").strip()
        if not code:
            continue
        instances.append(
            src_models.MotorStateBrand(
                code=code,
                name=brand.get("Name"),
                offered=bool(brand.get("Offered", False)),
                is_inventory_available=bool(brand.get("IsInventoryAvailable", False)),
                data=_json_safe(brand),
            )
        )
    return instances


# ---------------------------------------------------------------------------
# Availability (spine + incremental) — GET /api/ProductAvailabilityChange
# ---------------------------------------------------------------------------
def fetch_and_save_motorstate_availability_full(
    company_provider_id: typing.Optional[int] = None,
) -> None:
    """Rebuild the full part-number spine, one brand at a time. Requires brands to
    have been fetched first (reads MotorStateBrand for the brand code list)."""
    logger.info("{} Started full Motor State availability (spine) sync.".format(_LOG_PREFIX))
    company_provider = _resolve_company_provider(company_provider_id)
    if not company_provider:
        logger.info("{} No active Motor State provider found.".format(_LOG_PREFIX))
        return

    company = company_provider.company
    client = _build_client(company_provider)

    brand_codes = list(
        src_models.MotorStateBrand.objects.order_by("code").values_list("code", flat=True)
    )
    if not brand_codes:
        logger.warning(
            "{} No Motor State brands stored — run fetch_and_save_motorstate_brands first.".format(
                _LOG_PREFIX
            )
        )
        return

    started = time.monotonic()
    total_rows = 0
    for idx, brand_code in enumerate(brand_codes, start=1):
        rows = _collect_brand_availability(client, brand_code)
        instances = _transform_availability_rows(rows, company=company, brand_code=brand_code)
        if instances:
            _upsert_availability(instances, update_fields=_AVAILABILITY_UPDATE_FIELDS_SPINE)
            total_rows += len(instances)
        if idx % 25 == 0 or idx == len(brand_codes):
            logger.info(
                "{} Availability spine: {}/{} brands, {} rows in {:.1f}s.".format(
                    _LOG_PREFIX, idx, len(brand_codes), total_rows, time.monotonic() - started
                )
            )
    logger.info("{} Full availability sync done: {} rows.".format(_LOG_PREFIX, total_rows))


def _collect_brand_availability(
    client: motorstate_client.MotorStateApiClient,
    brand_code: str,
) -> typing.List[typing.Dict]:
    """All availability rows for a brand. One epoch call unless it hits the 5000-row
    cap, in which case walk the fromDateTime ladder and union by part number."""
    rows = client.get_product_availability_changes(from_date_time=_EPOCH_FROM_DATE, brand=brand_code)
    if len(rows) < motorstate_client.MAX_AVAILABILITY_RECORDS:
        return rows

    logger.info(
        "{} Brand {} hit the {}-row cap; recovering via date ladder.".format(
            _LOG_PREFIX, brand_code, motorstate_client.MAX_AVAILABILITY_RECORDS
        )
    )
    by_part: typing.Dict[str, typing.Dict] = {}
    for from_date in _AVAILABILITY_DATE_LADDER:
        ladder_rows = client.get_product_availability_changes(from_date_time=from_date, brand=brand_code)
        for row in ladder_rows:
            part_number = (row.get("PartNumber") or "").strip()
            if part_number:
                by_part[part_number] = row
    return list(by_part.values())


def fetch_and_save_motorstate_availability_updates(
    company_provider_id: typing.Optional[int] = None,
) -> None:
    """Incremental poll from the stored high-water mark (max source_updated_on for
    this company). Unfiltered by brand — a single paged-by-date sweep of recent
    changes. Falls back to the epoch date when nothing has been ingested yet."""
    logger.info("{} Started Motor State availability updates.".format(_LOG_PREFIX))
    company_provider = _resolve_company_provider(company_provider_id)
    if not company_provider:
        logger.info("{} No active Motor State provider found.".format(_LOG_PREFIX))
        return

    company = company_provider.company
    client = _build_client(company_provider)

    from_date_time = _incremental_from_date_time(company)
    logger.info("{} Availability updates fromDateTime={}.".format(_LOG_PREFIX, from_date_time))

    rows = client.get_product_availability_changes(from_date_time=from_date_time)
    if len(rows) >= motorstate_client.MAX_AVAILABILITY_RECORDS:
        # More than one page of changes in the window; walk forward from the newest
        # row's timestamp until a call comes back under the cap.
        rows = _drain_incremental(client, seed_rows=rows)

    instances = _transform_availability_rows(rows, company=company, brand_code=None)
    if instances:
        _upsert_availability(instances, update_fields=_AVAILABILITY_UPDATE_FIELDS_INCREMENTAL)
    logger.info("{} Availability updates done: {} rows.".format(_LOG_PREFIX, len(instances)))


def _incremental_from_date_time(company: src_models.Company) -> str:
    latest = (
        src_models.MotorStateAvailability.objects.filter(company=company)
        .exclude(source_updated_on__isnull=True)
        .order_by("-source_updated_on")
        .values_list("source_updated_on", flat=True)
        .first()
    )
    if not latest:
        return _EPOCH_FROM_DATE
    return (latest - _INCREMENTAL_OVERLAP).strftime("%Y-%m-%dT%H:%M:%S")


def _drain_incremental(
    client: motorstate_client.MotorStateApiClient,
    seed_rows: typing.List[typing.Dict],
) -> typing.List[typing.Dict]:
    """Advance fromDateTime past the newest seen change until a page is under the
    cap, unioning by part number. Bounded so a stuck timestamp cannot loop forever."""
    by_part: typing.Dict[str, typing.Dict] = {}

    def _absorb(rows: typing.List[typing.Dict]) -> typing.Optional[str]:
        newest: typing.Optional[str] = None
        for row in rows:
            part_number = (row.get("PartNumber") or "").strip()
            if part_number:
                by_part[part_number] = row
            updated_on = row.get("UpdatedOn")
            if updated_on and (newest is None or str(updated_on) > newest):
                newest = str(updated_on)
        return newest

    newest_seen = _absorb(seed_rows)
    for _ in range(len(_AVAILABILITY_DATE_LADDER)):
        if not newest_seen:
            break
        next_rows = client.get_product_availability_changes(from_date_time=newest_seen)
        prev_newest = newest_seen
        newest_seen = _absorb(next_rows)
        under_cap = len(next_rows) < motorstate_client.MAX_AVAILABILITY_RECORDS
        if under_cap or newest_seen == prev_newest:
            break
    return list(by_part.values())


def _transform_availability_rows(
    rows: typing.List[typing.Dict],
    company: src_models.Company,
    brand_code: typing.Optional[str],
) -> typing.List[src_models.MotorStateAvailability]:
    # Dedupe within the batch — pgbulk rejects duplicate unique keys in one call.
    by_part: typing.Dict[str, src_models.MotorStateAvailability] = {}
    for row in rows:
        part_number = (row.get("PartNumber") or "").strip()
        if not part_number:
            continue
        by_part[part_number] = src_models.MotorStateAvailability(
            company=company,
            part_number=part_number,
            brand_code=brand_code,
            status_type=row.get("StatusType"),
            quantity_available=row.get("QuantityAvailable"),
            source_updated_on=_parse_source_updated_on(row.get("UpdatedOn")),
        )
    return list(by_part.values())


def _upsert_availability(
    instances: typing.List[src_models.MotorStateAvailability],
    update_fields: typing.List[str],
) -> None:
    pgbulk.upsert(
        src_models.MotorStateAvailability,
        instances,
        unique_fields=["company", "part_number"],
        update_fields=update_fields,
        returning=False,
    )


# ---------------------------------------------------------------------------
# Products (detail + pricing) — GET /api/Product, parallel 15-part batches
# ---------------------------------------------------------------------------
def fetch_and_save_motorstate_products(
    company_provider_id: typing.Optional[int] = None,
    part_numbers: typing.Optional[typing.List[str]] = None,
    workers: int = _DEFAULT_PRODUCT_WORKERS,
    include_discontinued: bool = False,
    flush_size: int = _PRODUCT_FLUSH_SIZE,
) -> None:
    """Hydrate detail + pricing for ``part_numbers`` (default: this company's spine,
    minus discontinued parts — Motor State returns Found=false with no data for
    StatusType X, so hydrating them just burns API calls; pass include_discontinued
    to force them).

    Worker threads do HTTP only (shared keep-alive session); the main thread buffers
    the parsed rows and flushes them with a single bulk upsert every ``flush_size``
    rows. Keeping all DB writes on one thread avoids per-chunk connection churn against
    the remote database (the earlier stall) while still landing data progressively."""
    logger.info("{} Started Motor State product hydrate.".format(_LOG_PREFIX))
    company_provider = _resolve_company_provider(company_provider_id)
    if not company_provider:
        logger.info("{} No active Motor State provider found.".format(_LOG_PREFIX))
        return

    company = company_provider.company
    client = _build_client(company_provider)

    # Motor State's /api/Product carries no brand, so resolve each part's brand from
    # the availability spine up front: part_number -> (MotorStateBrand id, brand code).
    brand_by_part = _build_part_brand_map(company)

    if part_numbers is None:
        availability = src_models.MotorStateAvailability.objects.filter(company=company)
        if not include_discontinued:
            # StatusType X/x = discontinued; /api/Product has no detail for these.
            availability = availability.exclude(status_type__iexact="X")
        part_numbers = list(availability.order_by("part_number").values_list("part_number", flat=True))
    part_numbers = [str(pn).strip() for pn in part_numbers if str(pn).strip()]
    if not part_numbers:
        logger.warning("{} No part numbers to hydrate.".format(_LOG_PREFIX))
        return

    chunks = [
        part_numbers[i : i + motorstate_client.MAX_PRODUCT_BATCH]
        for i in range(0, len(part_numbers), motorstate_client.MAX_PRODUCT_BATCH)
    ]
    logger.info(
        "{} Hydrating {} part numbers in {} chunks, {} workers (discontinued={}).".format(
            _LOG_PREFIX, len(part_numbers), len(chunks), workers, include_discontinued
        )
    )

    started = time.monotonic()
    processed = 0
    buffer: typing.List[typing.Tuple[src_models.MotorStateProduct, typing.Dict]] = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_fetch_product_chunk_rows, client, chunk) for chunk in chunks]
        for done, future in enumerate(as_completed(futures), start=1):
            for row in future.result():
                pair = _transform_product_row(row, brand_by_part=brand_by_part)
                if pair is not None:
                    buffer.append(pair)
            if len(buffer) >= flush_size:
                processed += _flush_product_buffer(buffer, company)
                buffer = []
            if done % 200 == 0 or done == len(chunks):
                logger.info(
                    "{} Products: {}/{} chunks, {} rows in {:.1f}s.".format(
                        _LOG_PREFIX, done, len(chunks), processed, time.monotonic() - started
                    )
                )
    processed += _flush_product_buffer(buffer, company)
    logger.info("{} Product hydrate done: {} rows.".format(_LOG_PREFIX, processed))


def _build_part_brand_map(
    company: src_models.Company,
) -> typing.Dict[str, typing.Tuple[typing.Optional[int], typing.Optional[str]]]:
    """part_number -> (MotorStateBrand id, brand code) from the availability spine.
    Brand id is None for a code with no matching MotorStateBrand row."""
    brand_id_by_code = dict(src_models.MotorStateBrand.objects.values_list("code", "id"))
    mapping: typing.Dict[str, typing.Tuple[typing.Optional[int], typing.Optional[str]]] = {}
    for part_number, brand_code in (
        src_models.MotorStateAvailability.objects.filter(company=company)
        .values_list("part_number", "brand_code")
        .iterator()
    ):
        mapping[part_number] = (brand_id_by_code.get(brand_code) if brand_code else None, brand_code)
    return mapping


def _fetch_product_chunk_rows(
    client: motorstate_client.MotorStateApiClient,
    chunk: typing.List[str],
) -> typing.List[typing.Dict]:
    """Network-only worker: fetch one <=15-part batch, no DB access. A failed batch
    is logged and dropped so it can't sink the whole run."""
    try:
        return client.get_products(chunk)
    except motorstate_exceptions.MotorStateAPIException as e:
        logger.error("{} Product chunk failed ({}): {}".format(_LOG_PREFIX, chunk, str(e)))
        return []


def _flush_product_buffer(
    buffer: typing.List[typing.Tuple[src_models.MotorStateProduct, typing.Dict]],
    company: src_models.Company,
) -> int:
    """
    Bulk-upsert one buffer into both tables (main thread only): the global catalog rows first,
    then this company's price rows keyed to them. Returns catalog rows written.
    """
    if not buffer:
        return 0

    # Same part number can repeat across chunks; last one wins (pgbulk rejects dupes in a batch).
    by_part: typing.Dict[str, typing.Tuple[src_models.MotorStateProduct, typing.Dict]] = {}
    for product, price_kwargs in buffer:
        by_part[product.part_number] = (product, price_kwargs)

    products = [p for p, _ in by_part.values()]
    pgbulk.upsert(
        src_models.MotorStateProduct,
        products,
        unique_fields=["part_number"],
        update_fields=_PRODUCT_UPDATE_FIELDS,
        returning=False,
    )

    # Re-read ids: pgbulk doesn't populate pks on the instances it upserts.
    product_ids = dict(
        src_models.MotorStateProduct.objects.filter(
            part_number__in=list(by_part.keys())
        ).values_list("part_number", "id")
    )
    pricing_rows = [
        src_models.MotorStateCompanyPricing(
            product_id=product_ids[part_number], company=company, **price_kwargs
        )
        for part_number, (_, price_kwargs) in by_part.items()
        if part_number in product_ids
    ]
    if pricing_rows:
        pgbulk.upsert(
            src_models.MotorStateCompanyPricing,
            pricing_rows,
            unique_fields=["product", "company"],
            update_fields=_COMPANY_PRICING_UPDATE_FIELDS,
            returning=False,
        )

    return len(products)


def _transform_product_row(
    row: typing.Dict,
    brand_by_part: typing.Dict[str, typing.Tuple[typing.Optional[int], typing.Optional[str]]],
) -> typing.Optional[typing.Tuple[src_models.MotorStateProduct, typing.Dict]]:
    """
    Split one /api/Product row into its global catalog row and the account-specific price
    values. Returns ``(product, price_kwargs)``; the caller attaches ``price_kwargs`` to a
    MotorStateCompanyPricing row once the product's id is known.
    """
    part_number = (row.get("PartNumber") or "").strip()
    if not part_number:
        return None
    found = bool(row.get("Found", False))
    product = row.get("Product") or {}
    brand_id, brand_code = brand_by_part.get(part_number, (None, None))

    catalog = src_models.MotorStateProduct(
        part_number=part_number,
        brand_id=brand_id,
        brand_code=brand_code,
        found=found,
        vendor_part_number=product.get("VendorPartNumber"),
        supersede_part_number=product.get("SupersedePartNumber"),
        short_description=product.get("ShortDescription"),
        status=product.get("Status"),
        is_stocking=bool(product.get("IsStocking", False)),
        quantity=product.get("Quantity"),
        can_special_order=bool(product.get("CanSpecialOrder", False)),
        can_drop_ship=bool(product.get("CanDropShip", False)),
        can_regular_back_order=bool(product.get("CanRegularBackOrder", False)),
        data=_json_safe(product) or None,
    )
    price_kwargs = {
        "customer_price": product.get("CustomerPrice"),
        "customer_price_non_promotional": product.get("CustomerPriceNonPromotional"),
        "base_price": product.get("BasePrice"),
        "list_price": product.get("ListPrice"),
        "map_price": product.get("MapPrice"),
        "is_map_restricted": bool(product.get("IsMapRestricted", False)),
        "special_order_charge": product.get("SpecialOrderCharge"),
        "drop_ship_charge": product.get("DropShipCharge"),
    }
    return catalog, price_kwargs


# ---------------------------------------------------------------------------
# Brand mapping (MotorStateBrand -> Brands)
# ---------------------------------------------------------------------------
def _brand_name_upper_for_sync(ms_brand: src_models.MotorStateBrand) -> str:
    name_upper = (ms_brand.name or "").strip().upper()
    if not name_upper:
        name_upper = "BRAND_{}".format(ms_brand.code)
    return name_upper


def sync_unmapped_motorstate_brands_to_brands() -> typing.List[src_models.MotorStateBrand]:
    """
    For each MotorStateBrand without a BrandMotorStateBrandMapping: resolve Brand by exact name
    (uppercase), then compact-key, then fuzzy word-prefix match (same rules as other providers);
    otherwise create it. Upserts BrandMotorStateBrandMapping, BrandProviders, and CompanyBrands
    for TICK_PERFORMANCE. Motor State's /api/Brands carries no AAIA code, so matching is
    name-based only (same as Quadratec).
    """
    logger.info("{} Syncing unmapped Motor State brands to Brands.".format(_LOG_PREFIX))

    motorstate_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
    ).first()
    if not motorstate_provider:
        logger.warning("{} Motor State provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandMotorStateBrandMapping.objects.values_list(
            "motorstate_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(
        src_models.MotorStateBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped Motor State brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped Motor State brands.".format(_LOG_PREFIX, len(unmapped)))

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 1: exact uppercase-name match.
    name_upper_keys = {(mb.name or "").strip().upper() for mb in unmapped if (mb.name or "").strip()}
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
    for mb in unmapped:
        nm = (mb.name or "").strip().upper()
        if nm and nm in brands_by_upper_name:
            resolved_by_id[mb.id] = brands_by_upper_name[nm]

    # Phase 2: compact-key (punctuation/spacing-insensitive).
    compact_matches = 0
    still = [mb for mb in unmapped if mb.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for mb in still:
            key = normalize_compact_key(mb.name or "")
            if key and key in compact_index:
                resolved_by_id[mb.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix.
    unresolved = [mb for mb in unmapped if mb.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for mb in unresolved:
        parts = normalize_upper_words(mb.name or "").split()
        candidates: typing.List[src_models.Brands] = list(first_index.get(parts[0], ())) if parts else []
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(mb.name or "", candidates)
        if brand:
            resolved_by_id[mb.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved.
    new_brand_specs: typing.Dict[str, None] = {}
    for mb in unmapped:
        if mb.id in resolved_by_id:
            continue
        new_brand_specs.setdefault(_brand_name_upper_for_sync(mb), None)

    created_brands = 0
    if new_brand_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_specs.keys())).values_list(
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
            for name in new_brand_specs
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_specs.keys()))
        }
        for mb in unmapped:
            if mb.id not in resolved_by_id:
                resolved_by_id[mb.id] = by_name[_brand_name_upper_for_sync(mb)]

    mapping_models = [
        src_models.BrandMotorStateBrandMapping(
            brand_id=resolved_by_id[mb.id].id,
            motorstate_brand_id=mb.id,
        )
        for mb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandMotorStateBrandMapping,
        mapping_models,
        unique_fields=["brand", "motorstate_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    created_company_brands = 0
    for mb in unmapped:
        brand = resolved_by_id[mb.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=motorstate_provider,
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
        "{} Sync complete. Brands created: {}, compact: {}, fuzzy: {}, mappings: {}, "
        "BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX, created_brands, compact_matches, fuzzy_matches,
            len(mapping_models), created_brand_providers, created_company_brands,
        )
    )
    return unmapped


# ---------------------------------------------------------------------------
# Per-company pricing raw fetch (IntegrationPricingSyncJob entrypoint)
# ---------------------------------------------------------------------------
def sync_motorstate_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Refresh this connection's own MotorStateProduct rows (detail + account pricing) using its
    API key. Motor State returns catalog and price in the same /api/Product payload, so this is
    the product hydrate scoped to one company — there is no separate price-only endpoint.

    Called by the IntegrationPricingSyncJob queue: on connect/reconnect (full first sync) and
    on the recurring cadence (see integration_pricing_sync_jobs, throttled to 7 days for Motor
    State because a refresh is ~10.4k API calls).
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active Motor State CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    # A connection with no spine yet (freshly connected company) has nothing to price against,
    # so build it first — otherwise the hydrate would have zero part numbers to ask about.
    if not src_models.MotorStateAvailability.objects.filter(company=cp.company).exists():
        logger.info(
            "{} No availability spine for company={}; building it before pricing.".format(
                _LOG_PREFIX, cp.company_id
            )
        )
        fetch_and_save_motorstate_brands(company_provider_id=cp.id)
        fetch_and_save_motorstate_availability_full(company_provider_id=cp.id)

    fetch_and_save_motorstate_products(company_provider_id=cp.id)
