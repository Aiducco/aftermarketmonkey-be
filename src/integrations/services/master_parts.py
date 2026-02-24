"""
Sync MasterPart, ProviderPart, ProviderPartInventory, and ProviderPartCompanyPricing
from Turn14 and Keystone provider data.
"""
import logging
import time
import typing

from django.db import connection
from django.db.models import Q
from django.utils import timezone

import pgbulk

from src import enums as src_enums
from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MASTER-PARTS]"


def _get_brand_for_turn14_brand(turn14_brand: src_models.Turn14Brand) -> typing.Optional[src_models.Brands]:
    mapping = src_models.BrandTurn14BrandMapping.objects.filter(turn14_brand=turn14_brand).first()
    return mapping.brand if mapping else None


def _get_brand_for_keystone_brand(keystone_brand: src_models.KeystoneBrand) -> typing.Optional[src_models.Brands]:
    mapping = src_models.BrandKeystoneBrandMapping.objects.filter(keystone_brand=keystone_brand).first()
    return mapping.brand if mapping else None


BATCH_SIZE_MASTER_PARTS = 5000
BATCH_DELAY_SECONDS = 0.3


def sync_master_parts_from_turn14() -> None:
    """
    Create/update MasterPart and ProviderPart from Turn14Items.
    Only processes items whose Turn14Brand has a BrandTurn14BrandMapping.
    Uses cursor-based pagination (id__gt) and preloaded brand mapping to avoid N+1 and slow OFFSET.
    """
    logger.info("{} Syncing master parts from Turn14 (batched, cursor-based).".format(_LOG_PREFIX))

    turn14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn14_provider:
        logger.info("{} No Turn14 provider found.".format(_LOG_PREFIX))
        return

    # Preload turn14_brand_id -> (Brand, aaia_code) in one query
    mappings = list(
        src_models.BrandTurn14BrandMapping.objects.select_related("brand", "turn14_brand")
    )
    t14_brand_to_brand = {m.turn14_brand_id: m.brand for m in mappings}
    t14_brand_to_aaia = {
        m.turn14_brand_id: (m.turn14_brand.aaia_code if m.turn14_brand else None)
        for m in mappings
    }
    if not t14_brand_to_brand:
        logger.info("{} No BrandTurn14BrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    mapped_t14_brand_ids = set(t14_brand_to_brand.keys())

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        # Cursor-based: id > last_id, no OFFSET (fast on large tables)
        batch = list(
            src_models.Turn14Items.objects.filter(
                brand_id__in=mapped_t14_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values("id", "external_id", "mfr_part_number", "part_number", "part_description", "thumbnail", "brand_id")[
                :BATCH_SIZE_MASTER_PARTS
            ]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        item_to_brand_part = {}

        for row in batch:
            brand = t14_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = (
                (row.get("mfr_part_number") or row.get("part_number") or row.get("external_id") or "")
            )
            if isinstance(part_number, str):
                part_number = part_number.strip()
            else:
                part_number = str(part_number or "").strip()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=row["external_id"],
                        description=row.get("part_description"),
                        aaia_code=t14_brand_to_aaia.get(row["brand_id"]),
                        image_url=row.get("thumbnail"),
                    )
                )
            item_to_brand_part[row["external_id"]] = (brand.id, part_number)

        if not master_parts:
            connection.close()
            if len(batch) == BATCH_SIZE_MASTER_PARTS:
                time.sleep(BATCH_DELAY_SECONDS)
            continue

        pgbulk.upsert(
            src_models.MasterPart,
            master_parts,
            unique_fields=["brand", "part_number"],
            update_fields=["sku", "description", "aaia_code", "image_url"],
        )
        total_master += len(master_parts)

        conditions = Q()
        for b, p in seen:
            conditions |= Q(brand_id=b, part_number=p)
        brand_part_to_master = {
            (mp.brand_id, mp.part_number): mp
            for mp in src_models.MasterPart.objects.filter(conditions)
        }

        # Dedupe by (master_part, provider) - multiple items can map to same MasterPart
        provider_parts_seen = {}
        for row in batch:
            key = item_to_brand_part.get(row["external_id"])
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, turn14_provider.id)
            if pp_key not in provider_parts_seen:
                provider_parts_seen[pp_key] = src_models.ProviderPart(
                    master_part=master_part,
                    provider=turn14_provider,
                    provider_external_id=row["external_id"],
                )
        provider_parts = list(provider_parts_seen.values())

        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=["provider_external_id"],
            )
            total_provider += len(provider_parts)

        logger.info("{} Batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} master parts and {} provider parts from Turn14 total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def sync_master_parts_from_keystone() -> None:
    """
    Create/update MasterPart and ProviderPart from KeystoneParts.
    Only processes parts whose KeystoneBrand has a BrandKeystoneBrandMapping.
    Uses cursor-based pagination and preloaded brand mapping (same pattern as Turn14).
    """
    logger.info("{} Syncing master parts from Keystone (batched, cursor-based).".format(_LOG_PREFIX))

    keystone_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.KEYSTONE.value,
    ).first()
    if not keystone_provider:
        logger.info("{} No Keystone provider found.".format(_LOG_PREFIX))
        return

    # Preload keystone_brand_id -> (Brand, aaia_code) in one query
    mappings = list(
        src_models.BrandKeystoneBrandMapping.objects.select_related("brand", "keystone_brand")
    )
    ks_brand_to_brand = {m.keystone_brand_id: m.brand for m in mappings}
    ks_brand_to_aaia = {
        m.keystone_brand_id: (m.keystone_brand.aaia_code if m.keystone_brand else None)
        for m in mappings
    }
    if not ks_brand_to_brand:
        logger.info("{} No BrandKeystoneBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    mapped_ks_brand_ids = set(ks_brand_to_brand.keys())

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.KeystoneParts.objects.filter(
                brand_id__in=mapped_ks_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values("id", "vcpn", "brand_id", "manufacturer_part_no", "part_number", "long_description", "aaia_code")[
                :BATCH_SIZE_MASTER_PARTS
            ]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        vcpn_to_brand_part = {}

        for row in batch:
            brand = ks_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = (
                row.get("manufacturer_part_no") or row.get("part_number") or row.get("vcpn") or ""
            )
            if isinstance(part_number, str):
                part_number = part_number.strip()
            else:
                part_number = str(part_number or "").strip()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                aaia = row.get("aaia_code") or ks_brand_to_aaia.get(row["brand_id"])
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=row["vcpn"],
                        description=row.get("long_description"),
                        aaia_code=aaia,
                        image_url=None,
                    )
                )
            vcpn_to_brand_part[row["vcpn"]] = (brand.id, part_number)

        if not master_parts:
            connection.close()
            if len(batch) == BATCH_SIZE_MASTER_PARTS:
                time.sleep(BATCH_DELAY_SECONDS)
            continue

        pgbulk.upsert(
            src_models.MasterPart,
            master_parts,
            unique_fields=["brand", "part_number"],
            update_fields=["sku", "description", "aaia_code", "image_url"],
        )
        total_master += len(master_parts)

        conditions = Q()
        for b, p in seen:
            conditions |= Q(brand_id=b, part_number=p)
        brand_part_to_master = {
            (mp.brand_id, mp.part_number): mp
            for mp in src_models.MasterPart.objects.filter(conditions)
        }

        # Dedupe by (master_part, provider) - multiple parts can map to same MasterPart
        provider_parts_seen = {}
        for row in batch:
            key = vcpn_to_brand_part.get(row["vcpn"])
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, keystone_provider.id)
            if pp_key not in provider_parts_seen:
                provider_parts_seen[pp_key] = src_models.ProviderPart(
                    master_part=master_part,
                    provider=keystone_provider,
                    provider_external_id=row["vcpn"],
                )
        provider_parts = list(provider_parts_seen.values())

        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=["provider_external_id"],
            )
            total_provider += len(provider_parts)

        logger.info("{} Batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} master parts and {} provider parts from Keystone total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def sync_provider_inventory_from_turn14() -> None:
    """
    Sync ProviderPartInventory from Turn14BrandInventory.
    """
    logger.info("{} Syncing provider inventory from Turn14.".format(_LOG_PREFIX))

    turn14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn14_provider:
        logger.info("{} No Turn14 provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=turn14_provider).select_related("master_part")
    }

    inventories = src_models.Turn14BrandInventory.objects.all()
    to_update = []

    for inv in inventories:
        provider_part = provider_parts.get(inv.external_id)
        if not provider_part:
            continue

        manufacturer_qty = None
        if isinstance(inv.manufacturer, dict) and "stock" in inv.manufacturer:
            try:
                manufacturer_qty = int(inv.manufacturer["stock"])
            except (TypeError, ValueError):
                pass

        inv_obj, created = src_models.ProviderPartInventory.objects.get_or_create(
            provider_part=provider_part,
            defaults={
                "total_qty": inv.total_inventory or 0,
                "manufacturer_inventory": manufacturer_qty,
                "warehouse_availability": inv.inventory,
                "last_synced_at": timezone.now(),
            },
        )
        if not created:
            inv_obj.total_qty = inv.total_inventory or 0
            inv_obj.manufacturer_inventory = manufacturer_qty
            inv_obj.warehouse_availability = inv.inventory
            inv_obj.last_synced_at = timezone.now()
            to_update.append(inv_obj)

    if to_update:
        src_models.ProviderPartInventory.objects.bulk_update(
            to_update,
            ["total_qty", "manufacturer_inventory", "warehouse_availability", "last_synced_at", "updated_at"],
        )

    logger.info("{} Synced {} Turn14 inventory records.".format(_LOG_PREFIX, len(inventories)))


def sync_provider_inventory_from_keystone() -> None:
    """
    Sync ProviderPartInventory from KeystoneParts.
    """
    logger.info("{} Syncing provider inventory from Keystone.".format(_LOG_PREFIX))

    keystone_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.KEYSTONE.value,
    ).first()
    if not keystone_provider:
        logger.info("{} No Keystone provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=keystone_provider).select_related("master_part")
    }

    to_update = []
    for kp in src_models.KeystoneParts.objects.all():
        provider_part = provider_parts.get(kp.vcpn)
        if not provider_part:
            continue

        inv_obj, created = src_models.ProviderPartInventory.objects.get_or_create(
            provider_part=provider_part,
            defaults={
                "total_qty": kp.total_qty or 0,
                "manufacturer_inventory": None,
                "warehouse_availability": {
                    "east": kp.east_qty,
                    "midwest": kp.midwest_qty,
                    "california": kp.california_qty,
                    "southeast": kp.southeast_qty,
                    "pacific_nw": kp.pacific_nw_qty,
                    "texas": kp.texas_qty,
                    "great_lakes": kp.great_lakes_qty,
                    "florida": kp.florida_qty,
                } if any([kp.east_qty, kp.midwest_qty, kp.california_qty, kp.southeast_qty, kp.pacific_nw_qty, kp.texas_qty, kp.great_lakes_qty, kp.florida_qty]) else None,
                "last_synced_at": timezone.now(),
            },
        )
        if not created:
            inv_obj.total_qty = kp.total_qty or 0
            inv_obj.warehouse_availability = {
                "east": kp.east_qty,
                "midwest": kp.midwest_qty,
                "california": kp.california_qty,
                "southeast": kp.southeast_qty,
                "pacific_nw": kp.pacific_nw_qty,
                "texas": kp.texas_qty,
                "great_lakes": kp.great_lakes_qty,
                "florida": kp.florida_qty,
            } if any([kp.east_qty, kp.midwest_qty, kp.california_qty, kp.southeast_qty, kp.pacific_nw_qty, kp.texas_qty, kp.great_lakes_qty, kp.florida_qty]) else None
            inv_obj.last_synced_at = timezone.now()
            to_update.append(inv_obj)

    if to_update:
        src_models.ProviderPartInventory.objects.bulk_update(
            to_update,
            ["total_qty", "warehouse_availability", "last_synced_at", "updated_at"],
        )

    logger.info("{} Synced Keystone inventory.".format(_LOG_PREFIX))


def sync_provider_pricing_from_turn14() -> None:
    """
    Sync ProviderPartCompanyPricing from Turn14BrandPricing.
    Uses purchase_cost as cost. Creates pricing for each company that has Turn14 CompanyProviders.
    """
    logger.info("{} Syncing provider pricing from Turn14.".format(_LOG_PREFIX))

    turn14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn14_provider:
        logger.info("{} No Turn14 provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=turn14_provider)
    }

    companies = list(
        src_models.CompanyProviders.objects.filter(
            provider=turn14_provider,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).values_list("company_id", flat=True).distinct()
    )

    pricing_records = src_models.Turn14BrandPricing.objects.all()
    to_update = []

    for pr in pricing_records:
        provider_part = provider_parts.get(pr.external_id)
        if not provider_part:
            continue

        cost = pr.purchase_cost
        jobber_price = None
        map_price = None
        msrp = None
        if isinstance(pr.pricelists, list) and pr.pricelists:
            for pl in pr.pricelists:
                if isinstance(pl, dict):
                    jobber_price = jobber_price or pl.get("jobber_price") or pl.get("jobber")
                    map_price = map_price or pl.get("map_price") or pl.get("map")
                    msrp = msrp or pl.get("msrp") or pl.get("retail_price") or pl.get("retail")

        for company_id in companies:
            company = src_models.Company.objects.get(id=company_id)
            obj, created = src_models.ProviderPartCompanyPricing.objects.get_or_create(
                provider_part=provider_part,
                company=company,
                defaults={
                    "cost": cost,
                    "jobber_price": jobber_price,
                    "map_price": map_price,
                    "msrp": msrp,
                    "last_synced_at": timezone.now(),
                },
            )
            if not created:
                obj.cost = cost
                obj.jobber_price = jobber_price
                obj.map_price = map_price
                obj.msrp = msrp
                obj.last_synced_at = timezone.now()
                to_update.append(obj)

    if to_update:
        src_models.ProviderPartCompanyPricing.objects.bulk_update(
            to_update,
            ["cost", "jobber_price", "map_price", "msrp", "last_synced_at", "updated_at"],
        )

    logger.info("{} Synced Turn14 pricing.".format(_LOG_PREFIX))


def sync_provider_pricing_from_keystone() -> None:
    """
    Sync ProviderPartCompanyPricing from KeystoneParts.
    Creates pricing for each company that has Keystone CompanyProviders.
    """
    logger.info("{} Syncing provider pricing from Keystone.".format(_LOG_PREFIX))

    keystone_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.KEYSTONE.value,
    ).first()
    if not keystone_provider:
        logger.info("{} No Keystone provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=keystone_provider)
    }

    companies = list(
        src_models.CompanyProviders.objects.filter(
            provider=keystone_provider,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).values_list("company_id", flat=True).distinct()
    )

    to_update = []
    for kp in src_models.KeystoneParts.objects.all():
        provider_part = provider_parts.get(kp.vcpn)
        if not provider_part:
            continue

        for company_id in companies:
            company = src_models.Company.objects.get(id=company_id)
            obj, created = src_models.ProviderPartCompanyPricing.objects.get_or_create(
                provider_part=provider_part,
                company=company,
                defaults={
                    "cost": kp.cost,
                    "jobber_price": kp.jobber_price,
                    "map_price": None,
                    "msrp": None,
                    "last_synced_at": timezone.now(),
                },
            )
            if not created:
                obj.cost = kp.cost
                obj.jobber_price = kp.jobber_price
                obj.last_synced_at = timezone.now()
                to_update.append(obj)

    if to_update:
        src_models.ProviderPartCompanyPricing.objects.bulk_update(
            to_update,
            ["cost", "jobber_price", "last_synced_at", "updated_at"],
        )

    logger.info("{} Synced Keystone pricing.".format(_LOG_PREFIX))


def sync_all_master_parts() -> None:
    """
    Run all master parts syncs in sequence:
    1. Master parts + provider parts from Turn14
    2. Master parts + provider parts from Keystone
    3. Provider inventory from Turn14
    4. Provider inventory from Keystone
    5. Provider pricing from Turn14
    6. Provider pricing from Keystone
    """
    logger.info("{} Starting full master parts sync.".format(_LOG_PREFIX))

    sync_master_parts_from_turn14()
    connection.close()

    sync_master_parts_from_keystone()
    connection.close()

    sync_provider_inventory_from_turn14()
    connection.close()

    sync_provider_inventory_from_keystone()
    connection.close()

    sync_provider_pricing_from_turn14()
    connection.close()

    sync_provider_pricing_from_keystone()

    logger.info("{} Completed full master parts sync.".format(_LOG_PREFIX))
