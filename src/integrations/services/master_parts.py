"""
Sync MasterPart, ProviderPart, ProviderPartInventory, and ProviderPartCompanyPricing
from Turn14 and Keystone provider data.
"""
import logging
import time
import typing
from datetime import date

from django.db import connection
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
BATCH_DELAY_SECONDS = 0.1  # Reduced from 0.3 - was adding ~30s per 100 batches


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

        # Fast lookup: (brand_id, part_number) IN (...) via raw SQL (avoids huge OR query)
        pairs = list(seen)
        brand_part_to_master = {}
        if pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(pairs),),
                )
                for row in cur.fetchall():
                    mp_id, b_id, p_num = row
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts = []
        for row in batch:
            key = item_to_brand_part.get(row["external_id"])
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            provider_parts.append(
                src_models.ProviderPart(
                    master_part=master_part,
                    provider=turn14_provider,
                    provider_external_id=row["external_id"],
                )
            )

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

        # Fast lookup: (brand_id, part_number) IN (...) via raw SQL (avoids huge OR query)
        pairs = list(seen)
        brand_part_to_master = {}
        if pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(pairs),),
                )
                for row in cur.fetchall():
                    mp_id, b_id, p_num = row
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts = []
        for row in batch:
            key = vcpn_to_brand_part.get(row["vcpn"])
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            provider_parts.append(
                src_models.ProviderPart(
                    master_part=master_part,
                    provider=keystone_provider,
                    provider_external_id=row["vcpn"],
                )
            )

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
    Uses bulk upsert instead of get_or_create loop.
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
        for pp in src_models.ProviderPart.objects.filter(provider=turn14_provider)
    }

    inventories = list(src_models.Turn14BrandInventory.objects.values("external_id", "manufacturer", "inventory"))
    now = timezone.now()
    to_upsert = []

    for inv in inventories:
        provider_part = provider_parts.get(inv["external_id"])
        if not provider_part:
            continue

        manufacturer_qty = None
        manufacturer_esd = None
        manufacturer = inv.get("manufacturer")
        if isinstance(manufacturer, dict):
            if "stock" in manufacturer:
                try:
                    manufacturer_qty = int(manufacturer["stock"])
                except (TypeError, ValueError):
                    pass
            if "esd" in manufacturer:
                try:
                    esd_val = manufacturer["esd"]
                    if isinstance(esd_val, str):
                        manufacturer_esd = date.fromisoformat(esd_val)
                    elif hasattr(esd_val, "date"):
                        manufacturer_esd = esd_val.date() if esd_val else None
                except (TypeError, ValueError):
                    pass

        warehouse_total = 0
        inventory = inv.get("inventory")
        if isinstance(inventory, dict):
            for qty in inventory.values():
                if isinstance(qty, (int, float)):
                    warehouse_total += int(qty)

        to_upsert.append(
            src_models.ProviderPartInventory(
                provider_part=provider_part,
                warehouse_total_qty=warehouse_total,
                manufacturer_inventory=manufacturer_qty,
                manufacturer_esd=manufacturer_esd,
                warehouse_availability=inv.get("inventory"),
                last_synced_at=now,
            )
        )

    if to_upsert:
        pgbulk.upsert(
            src_models.ProviderPartInventory,
            to_upsert,
            unique_fields=["provider_part"],
            update_fields=["warehouse_total_qty", "manufacturer_inventory", "manufacturer_esd", "warehouse_availability", "last_synced_at"],
        )

    logger.info("{} Synced {} Turn14 inventory records.".format(_LOG_PREFIX, len(to_upsert)))


KEYSTONE_WAREHOUSE_QTY_FIELDS = (
    "east_qty", "midwest_qty", "california_qty", "southeast_qty",
    "pacific_nw_qty", "texas_qty", "great_lakes_qty", "florida_qty",
)


def _keystone_warehouse_availability(row: typing.Dict) -> typing.Optional[typing.Dict]:
    if any(row.get(f) for f in KEYSTONE_WAREHOUSE_QTY_FIELDS):
        return {
            "east": row.get("east_qty"),
            "midwest": row.get("midwest_qty"),
            "california": row.get("california_qty"),
            "southeast": row.get("southeast_qty"),
            "pacific_nw": row.get("pacific_nw_qty"),
            "texas": row.get("texas_qty"),
            "great_lakes": row.get("great_lakes_qty"),
            "florida": row.get("florida_qty"),
        }
    return None


def sync_provider_inventory_from_keystone() -> None:
    """
    Sync ProviderPartInventory from KeystoneParts.
    Uses bulk upsert instead of get_or_create loop.
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
        for pp in src_models.ProviderPart.objects.filter(provider=keystone_provider)
    }

    keystone_parts = list(
        src_models.KeystoneParts.objects.values(
            "vcpn", "total_qty",
            "east_qty", "midwest_qty", "california_qty", "southeast_qty",
            "pacific_nw_qty", "texas_qty", "great_lakes_qty", "florida_qty",
        )
    )

    now = timezone.now()
    to_upsert = []

    for kp in keystone_parts:
        provider_part = provider_parts.get(kp["vcpn"])
        if not provider_part:
            continue

        to_upsert.append(
            src_models.ProviderPartInventory(
                provider_part=provider_part,
                warehouse_total_qty=kp.get("total_qty") or 0,
                manufacturer_inventory=None,
                manufacturer_esd=None,
                warehouse_availability=_keystone_warehouse_availability(kp),
                last_synced_at=now,
            )
        )

    if to_upsert:
        pgbulk.upsert(
            src_models.ProviderPartInventory,
            to_upsert,
            unique_fields=["provider_part"],
            update_fields=["warehouse_total_qty", "manufacturer_inventory", "manufacturer_esd", "warehouse_availability", "last_synced_at"],
        )

    logger.info("{} Synced {} Keystone inventory records.".format(_LOG_PREFIX, len(to_upsert)))


def _extract_turn14_prices(pricelists: typing.Any) -> typing.Tuple[typing.Any, typing.Any, typing.Any]:
    """Extract jobber_price, map_price, msrp from Turn14 pricelists."""
    jobber_price = None
    map_price = None
    msrp = None
    if isinstance(pricelists, list) and pricelists:
        for pl in pricelists:
            if isinstance(pl, dict):
                jobber_price = jobber_price or pl.get("jobber_price") or pl.get("jobber")
                map_price = map_price or pl.get("map_price") or pl.get("map")
                msrp = msrp or pl.get("msrp") or pl.get("retail_price") or pl.get("retail")
    return jobber_price, map_price, msrp


def sync_provider_pricing_from_turn14() -> None:
    """
    Sync ProviderPartCompanyPricing from Turn14BrandPricing.
    Uses purchase_cost as cost. Creates pricing for each company that has Turn14 CompanyProviders.
    Uses bulk upsert instead of get_or_create loop.
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

    company_ids = list(
        src_models.CompanyProviders.objects.filter(
            provider=turn14_provider,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).values_list("company_id", flat=True).distinct()
    )
    companies = {c.id: c for c in src_models.Company.objects.filter(id__in=company_ids)} if company_ids else {}

    pricing_records = list(
        src_models.Turn14BrandPricing.objects.values("external_id", "purchase_cost", "pricelists")
    )
    now = timezone.now()
    to_upsert = []

    for pr in pricing_records:
        provider_part = provider_parts.get(pr["external_id"])
        if not provider_part:
            continue

        cost = pr.get("purchase_cost")
        jobber_price, map_price, msrp = _extract_turn14_prices(pr.get("pricelists"))

        for company_id in company_ids:
            company = companies.get(company_id)
            if not company:
                continue

            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=cost,
                    jobber_price=jobber_price,
                    map_price=map_price,
                    msrp=msrp,
                    last_synced_at=now,
                )
            )

    if to_upsert:
        pgbulk.upsert(
            src_models.ProviderPartCompanyPricing,
            to_upsert,
            unique_fields=["provider_part", "company"],
            update_fields=["cost", "jobber_price", "map_price", "msrp", "last_synced_at"],
        )

    logger.info("{} Synced {} Turn14 pricing records.".format(_LOG_PREFIX, len(to_upsert)))


def sync_provider_pricing_from_keystone() -> None:
    """
    Sync ProviderPartCompanyPricing from KeystoneParts.
    Creates pricing for each company that has Keystone CompanyProviders.
    Uses bulk upsert instead of get_or_create loop.
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

    company_ids = list(
        src_models.CompanyProviders.objects.filter(
            provider=keystone_provider,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).values_list("company_id", flat=True).distinct()
    )
    companies = {c.id: c for c in src_models.Company.objects.filter(id__in=company_ids)} if company_ids else {}

    keystone_parts = list(src_models.KeystoneParts.objects.values("vcpn", "cost", "jobber_price"))
    now = timezone.now()
    to_upsert = []

    for kp in keystone_parts:
        provider_part = provider_parts.get(kp["vcpn"])
        if not provider_part:
            continue

        for company_id in company_ids:
            company = companies.get(company_id)
            if not company:
                continue

            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=kp.get("cost"),
                    jobber_price=kp.get("jobber_price"),
                    map_price=None,
                    msrp=None,
                    last_synced_at=now,
                )
            )

    if to_upsert:
        pgbulk.upsert(
            src_models.ProviderPartCompanyPricing,
            to_upsert,
            unique_fields=["provider_part", "company"],
            update_fields=["cost", "jobber_price", "map_price", "msrp", "last_synced_at"],
        )

    logger.info("{} Synced {} Keystone pricing records.".format(_LOG_PREFIX, len(to_upsert)))


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
