"""
Sync MasterPart, ProviderPart, ProviderPartInventory, and ProviderPartCompanyPricing
from Turn14, Keystone, and Rough Country provider data.
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


def _get_brand_for_rough_country_brand(
    rc_brand: src_models.RoughCountryBrand,
) -> typing.Optional[src_models.Brands]:
    mapping = src_models.BrandRoughCountryBrandMapping.objects.filter(
        rough_country_brand=rc_brand,
    ).first()
    return mapping.brand if mapping else None


BATCH_SIZE_MASTER_PARTS = 5000
BATCH_SIZE_INVENTORY = 20000
BATCH_SIZE_PRICING = 20000
BATCH_DELAY_SECONDS = 0.1  # Reduced from 0.3 - was adding ~30s per 100 batches

# Master part field priority: Turn14 is primary for description, image_url.
# Other providers (Keystone, etc.) only update sku, aaia_code on existing parts.
# We use a two-phase approach for non-primary providers: INSERT new, UPDATE existing (sku/aaia only).
MASTER_PART_FULL_UPDATE_FIELDS = ["sku", "description", "aaia_code", "image_url"]
MASTER_PART_PARTIAL_UPDATE_FIELDS = ["sku", "aaia_code"]  # Non-primary providers


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
        # if batch_num < 82:
        #     continue
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
            update_fields=MASTER_PART_FULL_UPDATE_FIELDS,  # Turn14: primary source for all fields
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

        # Deduplicate by (master_part, provider): multiple external_ids can map to same (brand, part_number)
        provider_parts_by_key = {}
        for row in batch:
            key = item_to_brand_part.get(row["external_id"])
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, turn14_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=turn14_provider,
                provider_external_id=row["external_id"],
            )

        provider_parts = list(provider_parts_by_key.values())
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
                # New parts: include description, image_url. Existing parts: Phase 2 only updates sku, aaia_code.
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

        pairs = list(seen)
        # Two-phase: query existing first so we never overwrite description/image_url on existing parts
        existing_by_key = {}
        if pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(pairs),),
                )
                for row in cur.fetchall():
                    mp_id, b_id, p_num = row
                    existing_by_key[(b_id, p_num)] = mp_id

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]
        existing_keys = [k for k in pairs if k in existing_by_key]

        # Phase 1: INSERT new parts only (full data). DO NOTHING on conflict.
        if new_parts:
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        # Phase 2: UPDATE existing parts with ONLY sku, aaia_code via raw SQL (never description/image_url)
        if existing_keys:
            key_to_mp = {(mp.brand_id, mp.part_number): mp for mp in master_parts}
            values = [
                (existing_by_key[k], key_to_mp[k].sku, key_to_mp[k].aaia_code)
                for k in existing_keys
            ]
            placeholders = ", ".join(["(%s::bigint, %s, %s)"] * len(values))
            params = [x for row in values for x in row]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET sku = v.sku, aaia_code = v.aaia_code
                    FROM (VALUES {}) AS v(id, sku, aaia_code)
                    WHERE mp.id = v.id
                    """.format(placeholders),
                    params,
                )

        # Build brand_part_to_master for provider_parts (need master_part refs)
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

        # Deduplicate by (master_part, provider): multiple vcpns can map to same (brand, part_number)
        provider_parts_by_key = {}
        for row in batch:
            key = vcpn_to_brand_part.get(row["vcpn"])
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, keystone_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=keystone_provider,
                provider_external_id=row["vcpn"],
            )

        provider_parts = list(provider_parts_by_key.values())
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


def _rough_country_provider_external_id(rc_brand_id: int, sku: str) -> str:
    """Unique per Rough Country provider: rc_brand_id + sku (same sku can exist under different RC brands)."""
    return "{}_{}".format(rc_brand_id, sku)


def sync_master_parts_from_rough_country() -> None:
    """
    Create/update MasterPart and ProviderPart from RoughCountryPart.
    Only processes parts whose RoughCountryBrand has a BrandRoughCountryBrandMapping.
    Uses cursor-based pagination and two-phase upsert (non-primary: INSERT new, UPDATE existing sku/aaia only).
    """
    logger.info("{} Syncing master parts from Rough Country (batched, cursor-based).".format(_LOG_PREFIX))

    rc_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    ).first()
    if not rc_provider:
        logger.info("{} No Rough Country provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandRoughCountryBrandMapping.objects.select_related("brand", "rough_country_brand")
    )
    rc_brand_to_brand = {m.rough_country_brand_id: m.brand for m in mappings}
    rc_brand_to_aaia = {
        m.rough_country_brand_id: (m.rough_country_brand.aaia_code if m.rough_country_brand else None)
        for m in mappings
    }
    if not rc_brand_to_brand:
        logger.info("{} No BrandRoughCountryBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    mapped_rc_brand_ids = set(rc_brand_to_brand.keys())

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.RoughCountryPart.objects.filter(
                brand_id__in=mapped_rc_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "brand_id",
                "sku",
                "title",
                "description",
                "image_1",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        rc_external_id_to_brand_part = {}

        for row in batch:
            brand = rc_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = (row.get("sku") or "").strip()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                aaia = rc_brand_to_aaia.get(row["brand_id"])
                desc = row.get("title") or row.get("description")
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=desc,
                        aaia_code=aaia,
                        image_url=row.get("image_1"),
                    )
                )
            rc_external_id_to_brand_part[_rough_country_provider_external_id(row["brand_id"], row["sku"])] = (
                brand.id,
                part_number,
            )

        if not master_parts:
            connection.close()
            if len(batch) == BATCH_SIZE_MASTER_PARTS:
                time.sleep(BATCH_DELAY_SECONDS)
            continue

        pairs = list(seen)
        existing_by_key = {}
        if pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(pairs),),
                )
                for row in cur.fetchall():
                    mp_id, b_id, p_num = row
                    existing_by_key[(b_id, p_num)] = mp_id

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]
        existing_keys = [k for k in pairs if k in existing_by_key]

        if new_parts:
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        if existing_keys:
            key_to_mp = {(mp.brand_id, mp.part_number): mp for mp in master_parts}
            values = [
                (existing_by_key[k], key_to_mp[k].sku, key_to_mp[k].aaia_code)
                for k in existing_keys
            ]
            placeholders = ", ".join(["(%s::bigint, %s, %s)"] * len(values))
            params = [x for row in values for x in row]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET sku = v.sku, aaia_code = v.aaia_code
                    FROM (VALUES {}) AS v(id, sku, aaia_code)
                    WHERE mp.id = v.id
                    """.format(placeholders),
                    params,
                )

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

        provider_parts_by_key = {}
        for row in batch:
            ext_id = _rough_country_provider_external_id(row["brand_id"], row["sku"])
            key = rc_external_id_to_brand_part.get(ext_id)
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, rc_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=rc_provider,
                provider_external_id=ext_id,
            )

        provider_parts = list(provider_parts_by_key.values())
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

    logger.info("{} Synced {} master parts and {} provider parts from Rough Country total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _rough_country_warehouse_availability(
    nv_stock: typing.Optional[int], tn_stock: typing.Optional[int]
) -> typing.Optional[typing.Dict[str, int]]:
    """Build warehouse_availability from RoughCountryPart NV/TN stock with friendly names."""
    if nv_stock is None and tn_stock is None:
        return None
    out = {}
    if nv_stock is not None:
        out["Nevada"] = nv_stock
    if tn_stock is not None:
        out["Tennessee"] = tn_stock
    return out if out else None


def sync_provider_inventory_from_rough_country() -> None:
    """
    Sync ProviderPartInventory from RoughCountryPart (nv_stock, tn_stock).
    Uses bulk upsert with cursor-based batching.
    """
    logger.info("{} Syncing provider inventory from Rough Country.".format(_LOG_PREFIX))

    rc_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    ).first()
    if not rc_provider:
        logger.info("{} No Rough Country provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=rc_provider)
    }

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.RoughCountryPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "brand_id", "sku", "nv_stock", "tn_stock")[:BATCH_SIZE_INVENTORY]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        to_upsert = []
        for row in batch:
            ext_id = _rough_country_provider_external_id(row["brand_id"], row["sku"])
            provider_part = provider_parts.get(ext_id)
            if not provider_part:
                continue
            nv = row.get("nv_stock")
            tn = row.get("tn_stock")
            total = (nv or 0) + (tn or 0)
            to_upsert.append(
                src_models.ProviderPartInventory(
                    provider_part=provider_part,
                    warehouse_total_qty=total,
                    manufacturer_inventory=None,
                    manufacturer_esd=None,
                    warehouse_availability=_rough_country_warehouse_availability(nv, tn),
                    last_synced_at=now,
                    updated_at=now,
                )
            )

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartInventory,
                to_upsert,
                unique_fields=["provider_part"],
                update_fields=[
                    "warehouse_total_qty",
                    "manufacturer_inventory",
                    "manufacturer_esd",
                    "warehouse_availability",
                    "last_synced_at",
                    "updated_at",
                ],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Rough Country inventory batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_INVENTORY:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Rough Country inventory records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_rough_country() -> None:
    """
    Sync ProviderPartCompanyPricing from RoughCountryPart.
    Mapping: cost=price, jobber/map=cnd_map, msrp/retail=cnd_price.
    Creates pricing for each company that has Rough Country CompanyProviders.
    """
    logger.info("{} Syncing provider pricing from Rough Country.".format(_LOG_PREFIX))

    rc_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    ).first()
    if not rc_provider:
        logger.info("{} No Rough Country provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=rc_provider)
    }

    company_ids = list(
        src_models.CompanyProviders.objects.filter(
            provider=rc_provider,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        ).values_list("company_id", flat=True).distinct()
    )
    companies = {c.id: c for c in src_models.Company.objects.filter(id__in=company_ids)} if company_ids else {}

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.RoughCountryPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "brand_id", "sku", "price", "cnd_map", "cnd_price")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        companies_list = list(companies.values())
        to_upsert = []
        for row in batch:
            ext_id = _rough_country_provider_external_id(row["brand_id"], row["sku"])
            provider_part = provider_parts.get(ext_id)
            if not provider_part:
                continue
            price = row.get("price")
            cnd_map = row.get("cnd_map")
            cnd_price = row.get("cnd_price")
            for company in companies_list:
                to_upsert.append(
                    src_models.ProviderPartCompanyPricing(
                        provider_part=provider_part,
                        company=company,
                        cost=price,
                        jobber_price=cnd_map,
                        map_price=cnd_map,
                        msrp=cnd_price,
                        retail_price=cnd_price,
                        last_synced_at=now,
                    )
                )

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Rough Country pricing batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Rough Country pricing records total.".format(_LOG_PREFIX, total_upserted))


def _parse_turn14_inventory(inv: typing.Dict) -> typing.Tuple[typing.Optional[int], typing.Optional[date], int]:
    """Extract manufacturer_qty, manufacturer_esd, warehouse_total from Turn14 inventory row."""
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
    return manufacturer_qty, manufacturer_esd, warehouse_total


def _get_turn14_location_map() -> typing.Dict[str, str]:
    """Load all Turn14Location and return external_id -> name. Supports "01" and "1" via zero-pad."""
    rows = src_models.Turn14Location.objects.all().values("external_id", "name")
    result = {}
    for row in rows:
        eid = (row.get("external_id") or "").strip()
        name = (row.get("name") or eid).strip()
        if not eid:
            continue
        result[eid] = name
        # Allow lookup by zero-padded form (e.g. "1" -> same as "01")
        if eid.isdigit():
            result[eid.zfill(2)] = name
            result[str(int(eid))] = name
    return result


def _map_turn14_inventory_to_location_names(
    inventory: typing.Optional[typing.Dict],
    location_map: typing.Dict[str, str],
) -> typing.Optional[typing.Dict[str, typing.Union[int, float]]]:
    """
    Map Turn14 inventory keys (external_id e.g. "01") to location names from turn14_locations.
    Returns dict with location name as key, qty as value.
    """
    if not inventory or not isinstance(inventory, dict) or not location_map:
        return inventory if isinstance(inventory, dict) else None
    result = {}
    for key, qty in inventory.items():
        if not isinstance(qty, (int, float)):
            continue
        key_str = str(key).strip()
        # Direct lookup; support "1" via zero-pad to match "01" in DB
        display_name = (
            location_map.get(key_str)
            or (location_map.get(key_str.zfill(2)) if key_str.isdigit() else None)
            or key
        )
        result[display_name] = int(qty) if isinstance(qty, float) and qty == int(qty) else qty
    return result if result else None


def sync_provider_inventory_from_turn14() -> None:
    """
    Sync ProviderPartInventory from Turn14BrandInventory.
    Uses bulk upsert with cursor-based batching.
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

    # Load Turn14 locations once: map external_id (e.g. "01") -> name (e.g. "Hatfield")
    location_map = _get_turn14_location_map()

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.Turn14BrandInventory.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "external_id", "manufacturer", "inventory")[:BATCH_SIZE_INVENTORY]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        to_upsert = []
        for inv in batch:
            provider_part = provider_parts.get(inv["external_id"])
            if not provider_part:
                continue
            mfr_qty, mfr_esd, wh_total = _parse_turn14_inventory(inv)
            warehouse_availability = _map_turn14_inventory_to_location_names(
                inv.get("inventory"), location_map
            )
            to_upsert.append(
                src_models.ProviderPartInventory(
                    provider_part=provider_part,
                    warehouse_total_qty=wh_total,
                    manufacturer_inventory=mfr_qty,
                    manufacturer_esd=mfr_esd,
                    warehouse_availability=warehouse_availability,
                    last_synced_at=now,
                    updated_at=now,
                )
            )

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartInventory,
                to_upsert,
                unique_fields=["provider_part"],
                update_fields=["warehouse_total_qty", "manufacturer_inventory", "manufacturer_esd", "warehouse_availability", "last_synced_at", "updated_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Turn14 inventory batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_INVENTORY:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Turn14 inventory records total.".format(_LOG_PREFIX, total_upserted))


KEYSTONE_WAREHOUSE_QTY_FIELDS = (
    "east_qty", "midwest_qty", "california_qty", "southeast_qty",
    "pacific_nw_qty", "texas_qty", "great_lakes_qty", "florida_qty",
)

# Display names for Keystone warehouse_availability (nice capitalization)
KEYSTONE_WAREHOUSE_DISPLAY_NAMES = {
    "east_qty": "East",
    "midwest_qty": "Midwest",
    "california_qty": "California",
    "southeast_qty": "Southeast",
    "pacific_nw_qty": "Pacific NW",
    "texas_qty": "Texas",
    "great_lakes_qty": "Great Lakes",
    "florida_qty": "Florida",
}


def _keystone_warehouse_availability(row: typing.Dict) -> typing.Optional[typing.Dict]:
    if any(row.get(f) for f in KEYSTONE_WAREHOUSE_QTY_FIELDS):
        return {
            KEYSTONE_WAREHOUSE_DISPLAY_NAMES[field]: row.get(field)
            for field in KEYSTONE_WAREHOUSE_QTY_FIELDS
        }
    return None


def sync_provider_inventory_from_keystone() -> None:
    """
    Sync ProviderPartInventory from KeystoneParts.
    Uses bulk upsert with cursor-based batching.
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

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.KeystoneParts.objects.filter(id__gt=last_id)
            .order_by("id")
            .values(
                "id", "vcpn", "total_qty",
                "east_qty", "midwest_qty", "california_qty", "southeast_qty",
                "pacific_nw_qty", "texas_qty", "great_lakes_qty", "florida_qty",
            )[:BATCH_SIZE_INVENTORY]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        to_upsert = [
            src_models.ProviderPartInventory(
                provider_part=provider_parts[kp["vcpn"]],
                warehouse_total_qty=kp.get("total_qty") or 0,
                manufacturer_inventory=None,
                manufacturer_esd=None,
                warehouse_availability=_keystone_warehouse_availability(kp),
                last_synced_at=now,
                updated_at=now,
            )
            for kp in batch
            if kp["vcpn"] in provider_parts
        ]

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartInventory,
                to_upsert,
                unique_fields=["provider_part"],
                update_fields=["warehouse_total_qty", "manufacturer_inventory", "manufacturer_esd", "warehouse_availability", "last_synced_at", "updated_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Keystone inventory batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_INVENTORY:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Keystone inventory records total.".format(_LOG_PREFIX, total_upserted))


def _extract_turn14_prices(pricelists: typing.Any) -> typing.Tuple[typing.Any, typing.Any, typing.Any, typing.Any]:
    """Extract jobber_price, map_price, msrp, retail_price from Turn14 pricelists.
    Supports both key-based items (jobber_price, map_price, ...) and name+price items
    ([{"name": "MAP", "price": 1344}, {"name": "Jobber", "price": 1291.14}]).
    """
    jobber_price = None
    map_price = None
    msrp = None
    retail_price = None
    if not isinstance(pricelists, list) or not pricelists:
        return jobber_price, map_price, msrp, retail_price
    for pl in pricelists:
        if not isinstance(pl, dict):
            continue
        # Name + price format (e.g. {"name": "MAP", "price": 1344}, {"name": "Jobber", "price": 1291.14})
        name = (pl.get("name") or "").strip()
        price = pl.get("price")
        if name and price is not None:
            try:
                price_val = float(price) if not isinstance(price, (int, float)) else price
            except (TypeError, ValueError):
                price_val = None
            if price_val is not None:
                name_lower = name.lower()
                if name_lower == "map" and map_price is None:
                    map_price = price_val
                elif name_lower == "jobber" and jobber_price is None:
                    jobber_price = price_val
                elif name_lower == "msrp" and msrp is None:
                    msrp = price_val
                elif name_lower == "retail" and retail_price is None:
                    retail_price = price_val
        else:
            # Key-based format (legacy)
            if jobber_price is None:
                jobber_price = pl.get("jobber_price") or pl.get("jobber")
            if map_price is None:
                map_price = pl.get("map_price") or pl.get("map")
            if msrp is None:
                msrp = pl.get("msrp")
            if retail_price is None:
                retail_price = pl.get("retail_price") or pl.get("retail")
    return jobber_price, map_price, msrp, retail_price


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

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.Turn14BrandPricing.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "external_id", "purchase_cost", "pricelists")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        # Build (provider_part, company) records via itertools - no nested manual loops
        to_upsert = []
        for pr in batch:
            provider_part = provider_parts.get(pr["external_id"])
            if not provider_part:
                continue
            cost = pr.get("purchase_cost")
            jobber_price, map_price, msrp, retail_price = _extract_turn14_prices(pr.get("pricelists"))
            for company in companies.values():
                to_upsert.append(
                    src_models.ProviderPartCompanyPricing(
                        provider_part=provider_part,
                        company=company,
                        cost=cost,
                        jobber_price=jobber_price,
                        map_price=map_price,
                        msrp=msrp,
                        retail_price=retail_price,
                        last_synced_at=now,
                    )
                )

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Turn14 pricing batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Turn14 pricing records total.".format(_LOG_PREFIX, total_upserted))


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

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.KeystoneParts.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "vcpn", "cost", "jobber_price")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        companies_list = list(companies.values())
        to_upsert = [
            src_models.ProviderPartCompanyPricing(
                provider_part=provider_parts[kp["vcpn"]],
                company=company,
                cost=kp.get("cost"),
                jobber_price=kp.get("jobber_price"),
                map_price=None,
                msrp=None,
                retail_price=None,
                last_synced_at=now,
            )
            for kp in batch
            if kp["vcpn"] in provider_parts
            for company in companies_list
        ]

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Keystone pricing batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Keystone pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_all_master_parts() -> None:
    """
    Run all master parts syncs in sequence:
    1. Master parts + provider parts from Turn14
    2. Master parts + provider parts from Keystone
    3. Master parts + provider parts from Rough Country
    4. Provider inventory from Turn14
    5. Provider inventory from Keystone
    6. Provider inventory from Rough Country
    7. Provider pricing from Turn14
    8. Provider pricing from Keystone
    9. Provider pricing from Rough Country
    """
    logger.info("{} Starting full master parts sync.".format(_LOG_PREFIX))

    sync_master_parts_from_turn14()
    connection.close()

    sync_master_parts_from_keystone()
    connection.close()

    sync_master_parts_from_rough_country()
    connection.close()

    sync_provider_inventory_from_turn14()
    connection.close()

    sync_provider_inventory_from_keystone()
    connection.close()

    sync_provider_inventory_from_rough_country()
    connection.close()

    sync_provider_pricing_from_turn14()
    connection.close()

    sync_provider_pricing_from_keystone()
    connection.close()

    sync_provider_pricing_from_rough_country()

    logger.info("{} Completed full master parts sync.".format(_LOG_PREFIX))
