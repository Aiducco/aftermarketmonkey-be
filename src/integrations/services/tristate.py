"""
Tri-State Enterprises (TSE) distributor integration services.

Unlike every other provider in this package, Tri-State has no bulk catalog/brand feed to pull
from -- its real, documented API (src.integrations.clients.tristate.client, PACE's Customer API)
only exposes per-item ``get_inventory_and_pricing`` lookups, and ``get_brands`` is currently
ACL-restricted for our App-Id (pending a wider scope grant from Tri-State's rep). So this module
is built around two separate, narrower jobs instead of one feed-ingest job:

  1. ``sync_unmapped_tse_brands_to_brands`` -- map TSEBrand rows (seeded one-time from
     src/management/commands/data/tristate_brand_codes.json, see seed_tse_brands) to Brands by
     exact AAIA code match only. No fuzzy name matching and no brand auto-creation: AAIA-code
     matching was confirmed far more reliable than name matching for this distributor's brand
     list during exploration, and creating new global Brands rows from a 146-brand curated list
     is a bigger product decision than this integration should make unilaterally -- an unmatched
     TSEBrand just stays unmapped (logged), and is skipped everywhere downstream.

  2. ``fetch_tristate_inventory_and_pricing_for_company`` -- the actual live sync: for a given
     company's connection, batch-call get_inventory_and_pricing (always with ``aliasing=True``)
     for every TSEParts row, sending the brand's ``warehouse_code`` when known or its
     ``aaia_code`` otherwise -- aliasing resolves either against PACE's real brand_code and
     echoes back the match (confirmed live, see TSEBrand's own docstring), so this also
     self-heals ``TSEBrand.warehouse_code`` over time rather than needing it pre-populated.
     Quantity/availability (shared, account-agnostic -- confirmed live) is written to TSEParts;
     cost/list_price (genuinely account-specific) is upserted into TSECompanyPricing for this
     company. This is the function wired into integration_pricing_sync_jobs._fetch_raw_pricing.

TSEParts itself is NOT populated here -- see
src.management.commands.sync_tse_parts_catalog, which copies from the already-imported
``tristate_parts`` (TriStatePart, Algolia-sourced) table rather than re-fetching anything.
"""
import logging
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed

import pgbulk
from django.utils import timezone

_CATALOG_SYNC_UPSERT_BATCH = 5000

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.tristate import client as tristate_client
from src.integrations.clients.tristate import exceptions as tristate_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TRISTATE-SERVICES]"

PRICING_UPSERT_BATCH = 2000
PRICING_FETCH_WORKERS = 4


def sync_unmapped_tse_brands_to_brands() -> int:
    """Map TSEBrand -> Brands by exact AAIA code only. Returns the number newly mapped."""
    logger.info("{} Syncing unmapped TSE brands to Brands (AAIA code only).".format(_LOG_PREFIX))

    mapped_tse_ids = set(
        src_models.BrandTSEBrandMapping.objects.values_list("tse_brand_id", flat=True).distinct()
    )
    unmapped = list(
        src_models.TSEBrand.objects.exclude(id__in=mapped_tse_ids)
        .exclude(aaia_code__isnull=True)
        .exclude(aaia_code="")
        .order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped TSE brands with an AAIA code. Nothing to sync.".format(_LOG_PREFIX))
        return 0

    aaia_codes = {tb.aaia_code.strip().upper() for tb in unmapped if tb.aaia_code}
    brand_by_aaia: typing.Dict[str, src_models.Brands] = {}
    for b in src_models.Brands.objects.filter(aaia_code__in=aaia_codes).order_by("id"):
        key = (b.aaia_code or "").strip().upper()
        if key and key not in brand_by_aaia:
            brand_by_aaia[key] = b

    new_mappings = []
    unresolved = []
    for tb in unmapped:
        brand = brand_by_aaia.get((tb.aaia_code or "").strip().upper())
        if brand:
            new_mappings.append(src_models.BrandTSEBrandMapping(brand=brand, tse_brand=tb))
        else:
            unresolved.append("{} ({})".format(tb.name, tb.aaia_code))

    if new_mappings:
        pgbulk.upsert(
            src_models.BrandTSEBrandMapping,
            new_mappings,
            unique_fields=["brand", "tse_brand"],
            update_fields=[],
        )
    logger.info(
        "{} Mapped {}/{} unmapped TSE brands; {} unresolved (no matching AAIA code in Brands): {}".format(
            _LOG_PREFIX, len(new_mappings), len(unmapped), len(unresolved), ", ".join(unresolved[:20])
        )
    )
    return len(new_mappings)


def sync_tse_parts_from_tristate_part_seed() -> int:
    """
    Copy ``tristate_parts`` (TriStatePart -- Algolia-sourced catalog discovery) into TSEParts for
    every known TSEBrand. A DB-to-DB copy, not a network fetch, so unlike the Algolia import it
    depends on, this is safe to run on the regular nightly cycle (see
    master_parts.sync_derived_from_tristate / ingest_all_providers._run_tristate) -- the
    management command src.management.commands.sync_tse_parts_catalog is a thin wrapper around
    this same function for manual/one-off runs.
    """
    brand_by_aaia = {tb.aaia_code.upper(): tb for tb in src_models.TSEBrand.objects.all() if tb.aaia_code}
    if not brand_by_aaia:
        logger.info("{} No TSEBrand rows found -- run seed_tse_brands first.".format(_LOG_PREFIX))
        return 0

    total = 0
    last_id = 0
    while True:
        batch = list(
            src_models.TriStatePart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values(
                "id", "aaia_brand_code", "part_number", "name", "description",
                "pace_image", "aw_image", "tse_category", "tse_subcategory",
            )[:_CATALOG_SYNC_UPSERT_BATCH]
        )
        if not batch:
            break
        last_id = batch[-1]["id"]

        instances = []
        seen = set()
        for row in batch:
            tse_brand = brand_by_aaia.get((row.get("aaia_brand_code") or "").upper())
            if not tse_brand:
                continue
            part_number = (row.get("part_number") or "").strip()
            if not part_number:
                continue
            key = (tse_brand.id, part_number)
            if key in seen:
                continue
            seen.add(key)
            instances.append(
                src_models.TSEParts(
                    brand=tse_brand,
                    part_number=part_number,
                    description=row.get("name") or row.get("description"),
                    image_url=row.get("pace_image") or row.get("aw_image"),
                    category=row.get("tse_category"),
                    subcategory=row.get("tse_subcategory"),
                )
            )

        if instances:
            pgbulk.upsert(
                src_models.TSEParts,
                instances,
                unique_fields=["brand", "part_number"],
                update_fields=["description", "image_url", "category", "subcategory", "updated_at"],
                returning=False,
            )
            total += len(instances)

    logger.info("{} Upserted {} TSEParts rows from the tristate_parts seed.".format(_LOG_PREFIX, total))
    return total


def _active_tse_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.TRI_STATE.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        active=True,
    ).select_related("company", "provider")


def _warehouse_availability(locations: typing.Any) -> typing.Optional[typing.Dict[str, int]]:
    """``inventory_locations`` ([{location_name, location_inventory}, ...]) -> {name: qty}."""
    if not isinstance(locations, list):
        return None
    out: typing.Dict[str, int] = {}
    for loc in locations:
        name = (loc or {}).get("location_name")
        qty = (loc or {}).get("location_inventory")
        if name is not None and qty is not None:
            out[name] = qty
    return out or None


def fetch_tristate_inventory_and_pricing_for_company(company_provider_id: int) -> int:
    """
    For one company's Tri-State connection: call get_inventory_and_pricing (aliasing=True) for
    every TSEParts row, sending each brand's warehouse_code when known or its aaia_code
    otherwise. Writes quantity/orderable/per-warehouse breakdown to TSEParts (shared -- see its
    docstring), backfills TSEBrand.warehouse_code from whatever aliasing resolves, and upserts
    cost/list_price into TSECompanyPricing for this company. Returns the number of
    TSECompanyPricing rows upserted.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(id=company_provider_id)
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning("{} No CompanyProviders row id={}.".format(_LOG_PREFIX, company_provider_id))
        return 0

    credentials = credentials_helper.get_feed_credentials(cp)
    try:
        client = tristate_client.TriStateApiClient(credentials=credentials)
    except ValueError as e:
        logger.warning(
            "{} company_provider_id={}: invalid/missing credentials ({}). Skipping.".format(
                _LOG_PREFIX, company_provider_id, e
            )
        )
        return 0

    parts = list(
        src_models.TSEParts.objects.select_related("brand").values(
            "id", "part_number", "brand_id", "brand__warehouse_code", "brand__aaia_code"
        )
    )
    if not parts:
        logger.info("{} company_provider_id={}: no TSEParts yet.".format(_LOG_PREFIX, company_provider_id))
        return 0

    # The token actually sent per item -- warehouse_code when known, else aaia_code (aliasing
    # resolves either, see this module's and the client's docstrings).
    for row in parts:
        row["_sent_token"] = (row.get("brand__warehouse_code") or row["brand__aaia_code"]).upper()

    by_sent_token = {(row["_sent_token"], row["part_number"].upper()): row for row in parts}
    by_aaia = {(row["brand__aaia_code"].upper(), row["part_number"].upper()): row for row in parts}

    batch_size = tristate_client.DEFAULT_ITEMS_BATCH_SIZE
    chunks = [parts[i : i + batch_size] for i in range(0, len(parts), batch_size)]

    def _fetch_chunk(chunk: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
        items = [(row["_sent_token"], row["part_number"]) for row in chunk]
        try:
            return client.get_inventory_and_pricing(items, aliasing=True)
        except tristate_exceptions.TriStateException as e:
            logger.error(
                "{} company_provider_id={}: get_inventory_and_pricing failed for a batch of {}: {}".format(
                    _LOG_PREFIX, company_provider_id, len(chunk), e
                )
            )
            return []

    results: typing.List[typing.Dict] = []
    with ThreadPoolExecutor(max_workers=PRICING_FETCH_WORKERS) as executor:
        futures = [executor.submit(_fetch_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            results.extend(future.result())

    if not results:
        logger.info("{} company_provider_id={}: no results from the API.".format(_LOG_PREFIX, company_provider_id))
        return 0

    now = timezone.now()
    pricing_upserts = []
    part_updates = []
    brand_warehouse_code_updates: typing.Dict[int, str] = {}

    for item in results:
        part_number_u = (item.get("part_number") or "").upper()
        brand_code_u = (item.get("brand_code") or "").upper()
        aaia_brand_code = item.get("aaia_brand_code")

        row = by_sent_token.get((brand_code_u, part_number_u))
        if row is None and aaia_brand_code:
            row = by_aaia.get((aaia_brand_code.upper(), part_number_u))
        if row is None:
            continue

        part_updates.append(
            src_models.TSEParts(
                id=row["id"],
                quantity_available=item.get("inventory"),
                orderable=bool(item.get("orderable", 1)),
                warehouse_availability=_warehouse_availability(item.get("inventory_locations")),
                inventory_synced_at=now,
            )
        )
        pricing_upserts.append(
            src_models.TSECompanyPricing(
                part_id=row["id"],
                company_id=cp.company_id,
                cost=item.get("account_price"),
                list_price=item.get("list_price"),
                updated_at=now,
            )
        )
        # Resolved via aliasing against a real warehouse brand_code -- backfill so future runs
        # for this brand skip straight to the cheaper non-aliased path. aaia_brand_code is only
        # present when aliasing actually matched the item (see the client's docstring).
        if aaia_brand_code and item.get("brand_code"):
            brand_warehouse_code_updates[row["brand_id"]] = item["brand_code"]

    if part_updates:
        src_models.TSEParts.objects.bulk_update(
            part_updates,
            ["quantity_available", "orderable", "warehouse_availability", "inventory_synced_at"],
            batch_size=2000,
        )

    written = 0
    for start in range(0, len(pricing_upserts), PRICING_UPSERT_BATCH):
        batch = pricing_upserts[start : start + PRICING_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.TSECompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["cost", "list_price", "updated_at"],
            returning=False,
        )
        written += len(batch)

    if brand_warehouse_code_updates:
        brands_to_update = []
        existing = {
            b.id: b for b in src_models.TSEBrand.objects.filter(id__in=brand_warehouse_code_updates.keys())
        }
        for brand_id, resolved_code in brand_warehouse_code_updates.items():
            brand = existing.get(brand_id)
            if brand and brand.warehouse_code != resolved_code:
                brand.warehouse_code = resolved_code
                brands_to_update.append(brand)
        if brands_to_update:
            src_models.TSEBrand.objects.bulk_update(brands_to_update, ["warehouse_code"], batch_size=500)
            logger.info(
                "{} Backfilled warehouse_code for {} TSE brand(s) via aliasing.".format(
                    _LOG_PREFIX, len(brands_to_update)
                )
            )

    logger.info(
        "{} company_provider_id={}: fetched {} items, updated {} TSEParts, upserted {} "
        "TSECompanyPricing rows.".format(
            _LOG_PREFIX, company_provider_id, len(results), len(part_updates), written
        )
    )
    return written


def sync_tristate_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """Entry point wired into integration_pricing_sync_jobs._fetch_raw_pricing."""
    fetch_tristate_inventory_and_pricing_for_company(company_provider_id)
