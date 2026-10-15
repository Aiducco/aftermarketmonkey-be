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
     company's connection, batch-call get_inventory_and_pricing for every TSEParts row under a
     TSEBrand that has a confirmed ``warehouse_code`` (see TSEBrand's own docstring for why most
     rows don't have one yet), and upsert the results into TSECompanyPricing. This is the
     function wired into integration_pricing_sync_jobs._fetch_raw_pricing.

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


def fetch_tristate_inventory_and_pricing_for_company(company_provider_id: int) -> int:
    """
    For one company's Tri-State connection: call get_inventory_and_pricing for every TSEParts
    row whose brand has a known warehouse_code, batched DEFAULT_ITEMS_BATCH_SIZE items at a
    time, and upsert the results into TSECompanyPricing. Returns the number of rows upserted.

    Brands with no warehouse_code yet (most of them -- see TSEBrand's docstring) are skipped
    entirely rather than failing the whole sync; this simply widens in coverage as more
    warehouse codes get confirmed, no code change required.
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
        src_models.TSEParts.objects.filter(brand__warehouse_code__isnull=False)
        .exclude(brand__warehouse_code="")
        .select_related("brand")
        .values("id", "part_number", "brand__warehouse_code")
    )
    if not parts:
        logger.info(
            "{} company_provider_id={}: no TSEParts with a known warehouse_code yet. "
            "Nothing to price.".format(_LOG_PREFIX, company_provider_id)
        )
        return 0

    batch_size = tristate_client.DEFAULT_ITEMS_BATCH_SIZE
    chunks = [parts[i : i + batch_size] for i in range(0, len(parts), batch_size)]

    def _fetch_chunk(chunk: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
        items = [(row["brand__warehouse_code"], row["part_number"]) for row in chunk]
        try:
            return client.get_inventory_and_pricing(items)
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

    part_by_key = {
        (row["brand__warehouse_code"].upper(), row["part_number"].upper()): row["id"] for row in parts
    }
    now = timezone.now()
    to_upsert = []
    for item in results:
        key = ((item.get("brand_code") or "").upper(), (item.get("part_number") or "").upper())
        part_id = part_by_key.get(key)
        if not part_id:
            continue
        to_upsert.append(
            src_models.TSECompanyPricing(
                part_id=part_id,
                company_id=cp.company_id,
                cost=item.get("account_price"),
                list_price=item.get("list_price"),
                quantity_available=item.get("inventory"),
                orderable=bool(item.get("orderable", 1)),
                updated_at=now,
            )
        )

    written = 0
    for start in range(0, len(to_upsert), PRICING_UPSERT_BATCH):
        batch = to_upsert[start : start + PRICING_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.TSECompanyPricing,
            batch,
            unique_fields=["part", "company"],
            update_fields=["cost", "list_price", "quantity_available", "orderable", "updated_at"],
            returning=False,
        )
        written += len(batch)

    logger.info(
        "{} company_provider_id={}: fetched {} items, upserted {} TSECompanyPricing rows.".format(
            _LOG_PREFIX, company_provider_id, len(results), written
        )
    )
    return written


def sync_tristate_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """Entry point wired into integration_pricing_sync_jobs._fetch_raw_pricing."""
    fetch_tristate_inventory_and_pricing_for_company(company_provider_id)
