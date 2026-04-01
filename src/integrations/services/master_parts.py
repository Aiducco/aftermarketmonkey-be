"""
Sync MasterPart, ProviderPart, ProviderPartInventory, and ProviderPartCompanyPricing
from Turn14, Keystone, Meyer, Rough Country, and WheelPros provider data.
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


def _get_brand_for_wheelpros_brand(
    wp_brand: src_models.WheelProsBrand,
) -> typing.Optional[src_models.Brands]:
    mapping = src_models.BrandWheelProsBrandMapping.objects.filter(
        wheelpros_brand=wp_brand,
    ).first()
    return mapping.brand if mapping else None


BATCH_SIZE_MASTER_PARTS = 5000
BATCH_SIZE_MASTER_PARTS_WHEELPROS = 10000  # Larger batches = fewer round-trips
# Max tuples per IN clause to avoid PostgreSQL stack depth limit (StatementTooComplex)
WHEELPROS_LOOKUP_CHUNK = 200
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
        duplicates_seen = []
        master_parts = []
        item_to_brand_part = {}

        for row in batch:
            brand = t14_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            # part_number always from mfr_part_number; sku always from part_number
            part_number = row.get("mfr_part_number") or ""
            if isinstance(part_number, str):
                part_number = part_number.strip()
            else:
                part_number = str(part_number or "").strip()
            sku = row.get("part_number") or ""
            if isinstance(sku, str):
                sku = sku.strip().upper()
            else:
                sku = str(sku or "").strip().upper()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=sku,
                        description=row.get("part_description"),
                        aaia_code=t14_brand_to_aaia.get(row["brand_id"]),
                        image_url=row.get("thumbnail"),
                    )
                )
            else:
                duplicates_seen.append({
                    "brand": brand.name,
                    "part_number": part_number,
                    "external_id": row.get("external_id"),
                    "mfr_part_number": row.get("mfr_part_number"),
                    "part_number_t14": row.get("part_number"),
                })
            item_to_brand_part[row["external_id"]] = (brand.id, part_number)

        if not master_parts:
            if duplicates_seen:
                logger.info("{} Batch {}: {} duplicate keys (brand, part_number) skipped: {}".format(
                    _LOG_PREFIX, batch_num, len(duplicates_seen), duplicates_seen
                ))
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
        if duplicates_seen:
            logger.info("{} Batch {}: {} duplicate keys (brand, part_number) skipped: {}".format(
                _LOG_PREFIX, batch_num, len(duplicates_seen), duplicates_seen
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

            # part_number always from manufacturer_part_no; sku from vcpn
            part_number = row.get("manufacturer_part_no") or ""
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
                    UPDATE master_parts mp SET aaia_code = v.aaia_code
                    FROM (VALUES {}) AS v(id, aaia_code)
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


def sync_master_parts_from_meyer() -> None:
    """
    Create/update MasterPart and ProviderPart from MeyerParts (Meyer Pricing + Inventory feeds).
    Only processes rows whose MeyerBrand has a BrandMeyerBrandMapping.

    Existing master part resolution (same idea as WheelPros): prefer (brand_id, sku) where sku
    equals Meyer ``meyer_part``, else (brand_id, part_number) where part_number is ``mfg_item_number``.
    New rows: INSERT with part_number=mfg_item_number, sku=meyer_part; existing: UPDATE sku/aaia only.
    """
    logger.info("{} Syncing master parts from Meyer (batched, cursor-based).".format(_LOG_PREFIX))

    meyer_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    if not meyer_provider:
        logger.info("{} No Meyer provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandMeyerBrandMapping.objects.select_related("brand", "meyer_brand")
    )
    my_brand_to_brand = {m.meyer_brand_id: m.brand for m in mappings}
    my_brand_to_aaia = {
        m.meyer_brand_id: (m.brand.aaia_code if m.brand else None)
        for m in mappings
    }
    if not my_brand_to_brand:
        logger.info("{} No BrandMeyerBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    mapped_ids = set(my_brand_to_brand.keys())

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.MeyerParts.objects.filter(
                brand_id__in=mapped_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "meyer_part",
                "brand_id",
                "mfg_item_number",
                "description",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts_list = []
        meyer_part_to_brand_part = {}
        part_to_sku = {}

        for row in batch:
            brand = my_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = row.get("mfg_item_number") or ""
            if isinstance(part_number, str):
                part_number = part_number.strip()
            else:
                part_number = str(part_number or "").strip()
            if not part_number:
                continue

            meyer_part = row.get("meyer_part") or ""
            if isinstance(meyer_part, str):
                meyer_part = meyer_part.strip()
            else:
                meyer_part = str(meyer_part or "").strip()
            if not meyer_part:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                aaia = my_brand_to_aaia.get(row["brand_id"])
                master_parts_list.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=meyer_part,
                        description=row.get("description"),
                        aaia_code=aaia,
                        image_url=None,
                    )
                )
            part_to_sku[key] = meyer_part
            meyer_part_to_brand_part[meyer_part] = (brand.id, part_number)

        if not master_parts_list:
            connection.close()
            if len(batch) == BATCH_SIZE_MASTER_PARTS:
                time.sleep(BATCH_DELAY_SECONDS)
            continue

        pairs = list(seen)
        existing_by_key = {}
        id_to_mp = {}
        existing_by_sku = {}
        existing_by_part = {}
        if pairs:
            for i in range(0, len(pairs), WHEELPROS_LOOKUP_CHUNK):
                chunk_pairs = pairs[i : i + WHEELPROS_LOOKUP_CHUNK]
                chunk_sku_pairs = list(
                    {(b_id, part_to_sku[(b_id, pn)]) for (b_id, pn) in chunk_pairs}
                )
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, brand_id, part_number, sku FROM master_parts WHERE (brand_id, sku) IN %s",
                        (tuple(chunk_sku_pairs),),
                    )
                    for row in cur.fetchall():
                        mp_id, b_id, p_num, sku_val = row
                        if mp_id not in id_to_mp:
                            mp = src_models.MasterPart()
                            mp.id = mp_id
                            mp.brand_id = b_id
                            mp.part_number = p_num
                            id_to_mp[mp_id] = mp
                        if sku_val is not None:
                            sk_key = (b_id, (sku_val or "").strip())
                            if sk_key not in existing_by_sku:
                                existing_by_sku[sk_key] = []
                            existing_by_sku[sk_key].append((mp_id, p_num))
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                        (tuple(chunk_pairs),),
                    )
                    for row in cur.fetchall():
                        mp_id, b_id, p_num = row
                        if mp_id not in id_to_mp:
                            mp = src_models.MasterPart()
                            mp.id = mp_id
                            mp.brand_id = b_id
                            mp.part_number = p_num
                            id_to_mp[mp_id] = mp
                        if (b_id, p_num) not in existing_by_part:
                            existing_by_part[(b_id, p_num)] = mp_id
            for (b_id, p_num) in pairs:
                feed_sku = part_to_sku[(b_id, p_num)]
                sku_candidates = existing_by_sku.get((b_id, feed_sku)) or []
                sku_match = next((mp_id for mp_id, pn in sku_candidates if pn == p_num), None)
                if sku_match is None and sku_candidates:
                    sku_match = sku_candidates[0][0]
                existing_by_key[(b_id, p_num)] = sku_match or existing_by_part.get((b_id, p_num))

        new_parts = [
            mp for mp in master_parts_list if existing_by_key.get((mp.brand_id, mp.part_number)) is None
        ]
        existing_keys = [k for k in pairs if existing_by_key.get(k) is not None]

        if new_parts:
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        if existing_keys:
            key_to_mp = {(mp.brand_id, mp.part_number): mp for mp in master_parts_list}
            for i in range(0, len(existing_keys), WHEELPROS_LOOKUP_CHUNK):
                chunk_keys = existing_keys[i : i + WHEELPROS_LOOKUP_CHUNK]
                values = [
                    (existing_by_key[k], key_to_mp[k].sku, key_to_mp[k].aaia_code)
                    for k in chunk_keys
                ]
                placeholders = ", ".join(["(%s::bigint, %s, %s)"] * len(values))
                params = [x for row_vals in values for x in row_vals]
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
        for (b_id, p_num), mp_id in existing_by_key.items():
            if mp_id is not None and mp_id in id_to_mp:
                brand_part_to_master[(b_id, p_num)] = id_to_mp[mp_id]
        new_pairs = [(b_id, p_num) for (b_id, p_num) in pairs if (b_id, p_num) not in brand_part_to_master]
        if new_pairs:
            for i in range(0, len(new_pairs), WHEELPROS_LOOKUP_CHUNK):
                chunk = new_pairs[i : i + WHEELPROS_LOOKUP_CHUNK]
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                        (tuple(chunk),),
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
            mp_ext = row.get("meyer_part")
            if isinstance(mp_ext, str):
                mp_ext = mp_ext.strip()
            else:
                mp_ext = str(mp_ext or "").strip()
            key_bp = meyer_part_to_brand_part.get(mp_ext)
            if not key_bp:
                continue
            master_part = brand_part_to_master.get(key_bp)
            if not master_part:
                continue
            pp_key = (master_part.id, meyer_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=meyer_provider,
                provider_external_id=mp_ext,
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

        logger.info("{} Meyer batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts_list), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} master parts and {} provider parts from Meyer total.".format(
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
    Sync ProviderPartCompanyPricing from RoughCountryCompanyPricing (per-company rows).
    Mapping: cost=cost or price or sale_price (first available), jobber/map=cnd_map, msrp/retail=cnd_price.
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

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.RoughCountryCompanyPricing.objects.filter(id__gt=last_id)
            .order_by("id")
            .values(
                "id",
                "company_id",
                "cost",
                "price",
                "sale_price",
                "cnd_map",
                "cnd_price",
                "part__brand_id",
                "part__sku",
            )[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for row in batch:
            ext_id = _rough_country_provider_external_id(row["part__brand_id"], row["part__sku"])
            provider_part = provider_parts.get(ext_id)
            if not provider_part:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            cost = row.get("cost") or row.get("price") or row.get("sale_price")
            cnd_map = row.get("cnd_map")
            cnd_price = row.get("cnd_price")
            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=cost,
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


def _wheelpros_provider_external_id(wp_brand_id: int, part_number: str) -> str:
    """Unique per WheelPros provider: wp_brand_id + part_number."""
    return "{}_{}".format(wp_brand_id, part_number)


def _wheelpros_provider_part_lookup(wp_provider):
    """
    Build two lookups for WheelPros ProviderPart: by provider_external_id, and by (brand_id, sku).
    Use ext_id first, then (brand_id, sku) so inventory/pricing resolve like master parts (sku then part_number).
    """
    provider_parts_list = list(
        src_models.ProviderPart.objects.filter(provider=wp_provider).select_related("master_part")
    )
    by_ext_id = {pp.provider_external_id: pp for pp in provider_parts_list}
    by_brand_sku = {}
    for pp in provider_parts_list:
        if pp.master_part and pp.master_part.sku:
            key = (pp.master_part.brand_id, (pp.master_part.sku or "").strip())
            if key not in by_brand_sku:
                by_brand_sku[key] = pp
    return by_ext_id, by_brand_sku


def sync_master_parts_from_wheelpros() -> None:
    """
    Create/update MasterPart and ProviderPart from WheelProsPart (wheels, tires, accessories).
    Only processes parts whose WheelProsBrand has a BrandWheelProsBrandMapping.
    """
    logger.info("{} Syncing master parts from WheelPros (batched, cursor-based).".format(_LOG_PREFIX))

    wp_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    if not wp_provider:
        logger.info("{} No WheelPros provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandWheelProsBrandMapping.objects.select_related("brand", "wheelpros_brand")
    )
    wp_brand_to_brand = {m.wheelpros_brand_id: m.brand for m in mappings}
    wp_brand_to_aaia = {
        m.wheelpros_brand_id: (m.brand.aaia_code if m.brand else None)
        for m in mappings
    }
    if not wp_brand_to_brand:
        logger.info("{} No BrandWheelProsBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    mapped_wp_brand_ids = set(wp_brand_to_brand.keys())

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.WheelProsPart.objects.filter(
                brand_id__in=mapped_wp_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "brand_id",
                "part_number",
                "part_description",
                "image_url",
            )[:BATCH_SIZE_MASTER_PARTS_WHEELPROS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        wp_external_id_to_brand_part = {}

        for row in batch:
            brand = wp_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = (row.get("part_number") or "").strip()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                aaia = wp_brand_to_aaia.get(row["brand_id"])
                # New parts: full data from feed. Existing parts: Phase 2 only updates sku, aaia_code (never description/image_url).
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=row.get("part_description"),
                        aaia_code=aaia,
                        image_url=row.get("image_url"),
                    )
                )
            wp_external_id_to_brand_part[_wheelpros_provider_external_id(row["brand_id"], part_number)] = (
                brand.id,
                part_number,
            )

        if not master_parts:
            if len(batch) == BATCH_SIZE_MASTER_PARTS_WHEELPROS:
                time.sleep(BATCH_DELAY_SECONDS)
            continue

        pairs = list(seen)
        # Find existing by (brand_id, sku) then (brand_id, part_number). Two separate queries per chunk
        # (no OR) to avoid PostgreSQL stack depth limit; prefer sku match.
        # When multiple master_parts share same sku, prefer the one whose part_number matches (avoids wrong link).
        existing_by_key = {}
        id_to_mp = {}
        existing_by_sku = {}  # (b_id, sku) -> [(mp_id, part_number), ...] for disambiguation
        existing_by_part = {}
        if pairs:
            for i in range(0, len(pairs), WHEELPROS_LOOKUP_CHUNK):
                chunk = pairs[i : i + WHEELPROS_LOOKUP_CHUNK]
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, brand_id, part_number, sku FROM master_parts WHERE (brand_id, sku) IN %s",
                        (tuple(chunk),),
                    )
                    for row in cur.fetchall():
                        mp_id, b_id, p_num, sku_val = row
                        if mp_id not in id_to_mp:
                            mp = src_models.MasterPart()
                            mp.id = mp_id
                            mp.brand_id = b_id
                            mp.part_number = p_num
                            id_to_mp[mp_id] = mp
                        if sku_val is not None:
                            key = (b_id, (sku_val or "").strip())
                            if key not in existing_by_sku:
                                existing_by_sku[key] = []
                            existing_by_sku[key].append((mp_id, p_num))
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                        (tuple(chunk),),
                    )
                    for row in cur.fetchall():
                        mp_id, b_id, p_num = row
                        if mp_id not in id_to_mp:
                            mp = src_models.MasterPart()
                            mp.id = mp_id
                            mp.brand_id = b_id
                            mp.part_number = p_num
                            id_to_mp[mp_id] = mp
                        if (b_id, p_num) not in existing_by_part:
                            existing_by_part[(b_id, p_num)] = mp_id
            for (b_id, p_num) in pairs:
                sku_candidates = existing_by_sku.get((b_id, p_num)) or []
                # Prefer master part whose part_number matches p_num (e.g. AMP74604-01A vs 74604-01A)
                sku_match = next((mp_id for mp_id, pn in sku_candidates if pn == p_num), None)
                if sku_match is None and sku_candidates:
                    sku_match = sku_candidates[0][0]
                existing_by_key[(b_id, p_num)] = sku_match or existing_by_part.get((b_id, p_num))

        new_parts = [mp for mp in master_parts if existing_by_key.get((mp.brand_id, mp.part_number)) is None]
        existing_keys = [k for k in pairs if existing_by_key.get(k) is not None]

        # Phase 1: INSERT new parts only (full data). DO NOTHING on conflict. Same as Keystone.
        if new_parts:
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        # Phase 2: UPDATE existing parts with ONLY sku, aaia_code via raw SQL (never description/image_url).
        # Keeps existing description and image_url from primary source (e.g. Turn14, catalog). Same as Keystone.
        if existing_keys:
            key_to_mp = {(mp.brand_id, mp.part_number): mp for mp in master_parts}
            for i in range(0, len(existing_keys), WHEELPROS_LOOKUP_CHUNK):
                chunk_keys = existing_keys[i : i + WHEELPROS_LOOKUP_CHUNK]
                values = [
                    (existing_by_key[k], key_to_mp[k].sku, key_to_mp[k].aaia_code)
                    for k in chunk_keys
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

        # (brand_id, part_number) -> MasterPart; id_to_mp already filled from lookup above
        brand_part_to_master = {}
        for (b_id, p_num), mp_id in existing_by_key.items():
            if mp_id is not None and mp_id in id_to_mp:
                brand_part_to_master[(b_id, p_num)] = id_to_mp[mp_id]
        new_pairs = [(b_id, p_num) for (b_id, p_num) in pairs if (b_id, p_num) not in brand_part_to_master]
        if new_pairs:
            for i in range(0, len(new_pairs), WHEELPROS_LOOKUP_CHUNK):
                chunk = new_pairs[i : i + WHEELPROS_LOOKUP_CHUNK]
                with connection.cursor() as cur:
                    cur.execute(
                        "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                        (tuple(chunk),),
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
            part_number = (row.get("part_number") or "").strip()
            if not part_number:
                continue
            ext_id = _wheelpros_provider_external_id(row["brand_id"], part_number)
            key = wp_external_id_to_brand_part.get(ext_id)
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, wp_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=wp_provider,
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

        logger.info("{} WheelPros batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        if len(batch) == BATCH_SIZE_MASTER_PARTS_WHEELPROS:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} master parts and {} provider parts from WheelPros total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def sync_provider_inventory_from_wheelpros() -> None:
    """
    Sync ProviderPartInventory from WheelProsPart (total_qoh, warehouse_availability).
    Resolves ProviderPart by provider_external_id first, then by (brand_id, sku) to align with master parts lookup.
    """
    logger.info("{} Syncing provider inventory from WheelPros.".format(_LOG_PREFIX))

    wp_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    if not wp_provider:
        logger.info("{} No WheelPros provider found.".format(_LOG_PREFIX))
        return

    provider_parts_by_ext_id, provider_parts_by_brand_sku = _wheelpros_provider_part_lookup(wp_provider)

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.WheelProsPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "brand_id", "part_number", "total_qoh", "warehouse_availability")[:BATCH_SIZE_INVENTORY]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        to_upsert = []
        for row in batch:
            part_number = (row.get("part_number") or "").strip()
            if not part_number:
                continue
            ext_id = _wheelpros_provider_external_id(row["brand_id"], part_number)
            provider_part = provider_parts_by_ext_id.get(ext_id) or provider_parts_by_brand_sku.get(
                (row["brand_id"], part_number)
            )
            if not provider_part:
                continue
            total_qoh = row.get("total_qoh") or 0
            wh_avail = row.get("warehouse_availability")
            if isinstance(wh_avail, dict):
                try:
                    wh_avail = {str(k): int(float(v)) if v is not None else 0 for k, v in wh_avail.items()}
                except (TypeError, ValueError):
                    wh_avail = None
            else:
                wh_avail = None
            to_upsert.append(
                src_models.ProviderPartInventory(
                    provider_part=provider_part,
                    warehouse_total_qty=total_qoh,
                    manufacturer_inventory=None,
                    manufacturer_esd=None,
                    warehouse_availability=wh_avail if wh_avail else None,
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

        logger.info("{} WheelPros inventory batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_INVENTORY:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} WheelPros inventory records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_wheelpros() -> None:
    """
    Sync ProviderPartCompanyPricing from WheelProsCompanyPricing (per-company SFTP pricing).
    Resolves ProviderPart by provider_external_id first, then by (brand_id, sku) to align with master parts lookup.
    """
    logger.info("{} Syncing provider pricing from WheelPros.".format(_LOG_PREFIX))

    wp_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    if not wp_provider:
        logger.info("{} No WheelPros provider found.".format(_LOG_PREFIX))
        return

    provider_parts_by_ext_id, provider_parts_by_brand_sku = _wheelpros_provider_part_lookup(wp_provider)

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.WheelProsCompanyPricing.objects.filter(id__gt=last_id)
            .order_by("id")
            .values(
                "id",
                "company_id",
                "msrp_usd",
                "map_usd",
                "part__brand_id",
                "part__part_number",
            )[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for row in batch:
            part_number = (row.get("part__part_number") or "").strip()
            if not part_number:
                continue
            ext_id = _wheelpros_provider_external_id(row["part__brand_id"], part_number)
            provider_part = provider_parts_by_ext_id.get(ext_id) or provider_parts_by_brand_sku.get(
                (row["part__brand_id"], part_number)
            )
            if not provider_part:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            msrp = row.get("msrp_usd")
            map_price = row.get("map_usd")
            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=None,
                    jobber_price=map_price,
                    map_price=map_price,
                    msrp=msrp,
                    retail_price=msrp,
                    last_synced_at=now,
                )
            )

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} WheelPros pricing batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} WheelPros pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_turn14_for_company(company_id: int) -> None:
    """Like sync_provider_pricing_from_turn14 but only rows for one company_id."""
    logger.info("{} Syncing Turn14 provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

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

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.Turn14BrandPricing.objects.filter(id__gt=last_id, company_id=company_id)
            .order_by("id")
            .values("id", "external_id", "purchase_cost", "pricelists", "company_id")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {pr["company_id"] for pr in batch if pr.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for pr in batch:
            provider_part = provider_parts.get(pr["external_id"])
            if not provider_part:
                continue
            comp_id = pr.get("company_id")
            company = companies_by_id.get(comp_id) if comp_id else None
            if not company:
                continue
            cost = pr.get("purchase_cost")
            jobber_price, map_price, msrp, retail_price = _extract_turn14_prices(pr.get("pricelists"))
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

        logger.info("{} Turn14 pricing company={} batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Turn14 pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))


def sync_provider_pricing_from_keystone_for_company(company_id: int) -> None:
    logger.info("{} Syncing Keystone provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

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
            src_models.KeystoneCompanyPricing.objects.filter(id__gt=last_id, company_id=company_id)
            .order_by("id")
            .values("id", "company_id", "cost", "jobber_price", "part__vcpn")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for row in batch:
            vcpn = row.get("part__vcpn")
            provider_part = provider_parts.get(vcpn) if vcpn else None
            if not provider_part:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=row.get("cost"),
                    jobber_price=row.get("jobber_price"),
                    map_price=None,
                    msrp=None,
                    retail_price=None,
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

        logger.info("{} Keystone pricing company={} batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Keystone pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))


def sync_provider_pricing_from_meyer_for_company(company_id: int) -> None:
    logger.info("{} Syncing Meyer provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    meyer_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    if not meyer_provider:
        logger.info("{} No Meyer provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=meyer_provider)
    }

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.MeyerCompanyPricing.objects.filter(id__gt=last_id, company_id=company_id)
            .order_by("id")
            .values(
                "id",
                "company_id",
                "cost",
                "jobber_price",
                "map_price",
                "part__meyer_part",
            )[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
        for row in batch:
            ext = row.get("part__meyer_part")
            if isinstance(ext, str):
                ext = ext.strip()
            else:
                ext = str(ext or "").strip()
            pp = provider_parts.get(ext)
            if not pp:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            pricing_by_pp_company[(pp.id, company.id)] = src_models.ProviderPartCompanyPricing(
                provider_part=pp,
                company=company,
                cost=row.get("cost"),
                jobber_price=row.get("jobber_price"),
                map_price=row.get("map_price"),
                msrp=None,
                retail_price=None,
                last_synced_at=now,
            )

        to_upsert = list(pricing_by_pp_company.values())
        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Meyer pricing company={} batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Meyer pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))


def sync_provider_pricing_from_rough_country_for_company(company_id: int) -> None:
    logger.info("{} Syncing Rough Country provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

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
            src_models.RoughCountryCompanyPricing.objects.filter(id__gt=last_id, company_id=company_id)
            .order_by("id")
            .values(
                "id",
                "company_id",
                "cost",
                "price",
                "sale_price",
                "cnd_map",
                "cnd_price",
                "part__brand_id",
                "part__sku",
            )[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for row in batch:
            ext_id = _rough_country_provider_external_id(row["part__brand_id"], row["part__sku"])
            provider_part = provider_parts.get(ext_id)
            if not provider_part:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            cost = row.get("cost") or row.get("price") or row.get("sale_price")
            cnd_map = row.get("cnd_map")
            cnd_price = row.get("cnd_price")
            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=cost,
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

        logger.info("{} Rough Country pricing company={} batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Rough Country pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))


def sync_provider_pricing_from_wheelpros_for_company(company_id: int) -> None:
    logger.info("{} Syncing WheelPros provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    wp_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WHEELPROS.value,
    ).first()
    if not wp_provider:
        logger.info("{} No WheelPros provider found.".format(_LOG_PREFIX))
        return

    provider_parts_by_ext_id, provider_parts_by_brand_sku = _wheelpros_provider_part_lookup(wp_provider)

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.WheelProsCompanyPricing.objects.filter(id__gt=last_id, company_id=company_id)
            .order_by("id")
            .values(
                "id",
                "company_id",
                "msrp_usd",
                "map_usd",
                "part__brand_id",
                "part__part_number",
            )[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for row in batch:
            part_number = (row.get("part__part_number") or "").strip()
            if not part_number:
                continue
            ext_id = _wheelpros_provider_external_id(row["part__brand_id"], part_number)
            provider_part = provider_parts_by_ext_id.get(ext_id) or provider_parts_by_brand_sku.get(
                (row["part__brand_id"], part_number)
            )
            if not provider_part:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            msrp = row.get("msrp_usd")
            map_price = row.get("map_usd")
            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=None,
                    jobber_price=map_price,
                    map_price=map_price,
                    msrp=msrp,
                    retail_price=msrp,
                    last_synced_at=now,
                )
            )

        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} WheelPros pricing company={} batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} WheelPros pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))


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


def _meyer_warehouse_availability(row: typing.Dict) -> typing.Optional[typing.Dict]:
    out = {}
    if row.get("is_stocking") is not None:
        out["stocking"] = bool(row.get("is_stocking"))
    if row.get("is_special_order") is not None:
        out["special_order"] = bool(row.get("is_special_order"))
    if row.get("inventory_ltl") is not None:
        out["ltl"] = row.get("inventory_ltl")
    return out if out else None


def sync_provider_inventory_from_meyer() -> None:
    """Sync ProviderPartInventory from MeyerParts (Available + manufacturer qty)."""
    logger.info("{} Syncing provider inventory from Meyer.".format(_LOG_PREFIX))

    meyer_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    if not meyer_provider:
        logger.info("{} No Meyer provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=meyer_provider)
    }

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.MeyerParts.objects.filter(id__gt=last_id)
            .order_by("id")
            .values(
                "id",
                "meyer_part",
                "available_qty",
                "mfg_qty_available",
                "is_stocking",
                "is_special_order",
                "inventory_ltl",
            )[:BATCH_SIZE_INVENTORY]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        # Last row per ProviderPart wins: same meyer_part SKU can exist on multiple MeyerParts (different brands)
        # but maps to one ProviderPart — PostgreSQL rejects duplicate constrained keys in one INSERT.
        inv_by_provider_part_id: typing.Dict[int, src_models.ProviderPartInventory] = {}
        for mp in batch:
            ext = mp.get("meyer_part")
            if isinstance(ext, str):
                ext = ext.strip()
            else:
                ext = str(ext or "").strip()
            pp = provider_parts.get(ext)
            if not pp:
                continue
            avail = mp.get("available_qty")
            wh_total = 0
            if avail is not None:
                try:
                    wh_total = int(float(avail))
                except (TypeError, ValueError):
                    wh_total = 0
            inv_by_provider_part_id[pp.id] = src_models.ProviderPartInventory(
                provider_part=pp,
                warehouse_total_qty=wh_total,
                manufacturer_inventory=mp.get("mfg_qty_available"),
                manufacturer_esd=None,
                warehouse_availability=_meyer_warehouse_availability(mp),
                last_synced_at=now,
                updated_at=now,
            )

        to_upsert = list(inv_by_provider_part_id.values())
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

        logger.info("{} Meyer inventory batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_INVENTORY:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Meyer inventory records total.".format(_LOG_PREFIX, total_upserted))


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
    Each Turn14BrandPricing row is scoped to a company; one ProviderPartCompanyPricing row per
    (provider_part, company) using that row's costs for that company only.
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

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.Turn14BrandPricing.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "external_id", "purchase_cost", "pricelists", "company_id")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {pr["company_id"] for pr in batch if pr.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for pr in batch:
            provider_part = provider_parts.get(pr["external_id"])
            if not provider_part:
                continue
            company_id = pr.get("company_id")
            company = companies_by_id.get(company_id) if company_id else None
            if not company:
                continue
            cost = pr.get("purchase_cost")
            jobber_price, map_price, msrp, retail_price = _extract_turn14_prices(pr.get("pricelists"))
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
    Sync ProviderPartCompanyPricing from KeystoneCompanyPricing (per-company FTP pricing).
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

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.KeystoneCompanyPricing.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "company_id", "cost", "jobber_price", "part__vcpn")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        to_upsert = []
        for row in batch:
            vcpn = row.get("part__vcpn")
            provider_part = provider_parts.get(vcpn) if vcpn else None
            if not provider_part:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            to_upsert.append(
                src_models.ProviderPartCompanyPricing(
                    provider_part=provider_part,
                    company=company,
                    cost=row.get("cost"),
                    jobber_price=row.get("jobber_price"),
                    map_price=None,
                    msrp=None,
                    retail_price=None,
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

        logger.info("{} Keystone pricing batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Keystone pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_meyer() -> None:
    """
    Sync ProviderPartCompanyPricing from MeyerCompanyPricing (per-company SFTP pricing).
    """
    logger.info("{} Syncing provider pricing from Meyer.".format(_LOG_PREFIX))

    meyer_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    if not meyer_provider:
        logger.info("{} No Meyer provider found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=meyer_provider)
    }

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.MeyerCompanyPricing.objects.filter(id__gt=last_id)
            .order_by("id")
            .values(
                "id",
                "company_id",
                "cost",
                "jobber_price",
                "map_price",
                "part__meyer_part",
            )[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
        companies_by_id = {
            c.id: c
            for c in src_models.Company.objects.filter(id__in=batch_company_ids)
        }
        # Last row per (ProviderPart, Company): duplicate meyer_part across MeyerParts brands collapses to one pp.
        pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
        for row in batch:
            ext = row.get("part__meyer_part")
            if isinstance(ext, str):
                ext = ext.strip()
            else:
                ext = str(ext or "").strip()
            pp = provider_parts.get(ext)
            if not pp:
                continue
            company = companies_by_id.get(row.get("company_id"))
            if not company:
                continue
            pricing_by_pp_company[(pp.id, company.id)] = src_models.ProviderPartCompanyPricing(
                provider_part=pp,
                company=company,
                cost=row.get("cost"),
                jobber_price=row.get("jobber_price"),
                map_price=row.get("map_price"),
                msrp=None,
                retail_price=None,
                last_synced_at=now,
            )

        to_upsert = list(pricing_by_pp_company.values())
        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} Meyer pricing batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} Meyer pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_all_master_parts() -> None:
    """
    Run all master parts syncs in sequence:
    1. Master parts + provider parts from Turn14
    2. Master parts + provider parts from Keystone
    3. Master parts + provider parts from Meyer
    4. Master parts + provider parts from Rough Country
    5. Master parts + provider parts from WheelPros (wheels, tires, accessories)
    6. Provider inventory from Turn14
    7. Provider inventory from Keystone
    8. Provider inventory from Meyer
    9. Provider inventory from Rough Country
    10. Provider inventory from WheelPros
    11. Provider pricing from Turn14
    12. Provider pricing from Keystone
    13. Provider pricing from Meyer
    14. Provider pricing from Rough Country
    15. Provider pricing from WheelPros
    """
    logger.info("{} Starting full master parts sync.".format(_LOG_PREFIX))

    sync_master_parts_from_turn14()
    connection.close()

    sync_master_parts_from_keystone()
    connection.close()

    sync_master_parts_from_meyer()
    connection.close()

    sync_master_parts_from_rough_country()
    connection.close()

    sync_master_parts_from_wheelpros()
    connection.close()

    sync_provider_inventory_from_turn14()
    connection.close()

    sync_provider_inventory_from_keystone()
    connection.close()

    sync_provider_inventory_from_meyer()
    connection.close()

    sync_provider_inventory_from_rough_country()
    connection.close()

    sync_provider_inventory_from_wheelpros()
    connection.close()

    sync_provider_pricing_from_turn14()
    connection.close()

    sync_provider_pricing_from_keystone()
    connection.close()

    sync_provider_pricing_from_meyer()
    connection.close()

    sync_provider_pricing_from_rough_country()
    connection.close()

    sync_provider_pricing_from_wheelpros()

    logger.info("{} Completed full master parts sync.".format(_LOG_PREFIX))
