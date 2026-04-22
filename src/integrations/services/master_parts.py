"""
Sync MasterPart, ProviderPart, ProviderPartInventory, and ProviderPartCompanyPricing
from Turn14, Keystone, Meyer, A-Tech, Rough Country, WheelPros, and DLG provider data (including DLG company pricing).
"""
import logging
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from django.db import close_old_connections, connection
from django.db.models import Q
from django.utils import timezone

import pgbulk

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations.services.wheelpros import dealer_cost_from_msrp

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MASTER-PARTS]"

# ProviderPart upsert from sync_master_parts_from_* (distributor_refreshed_at from source row updated_at)
_PROVIDER_PART_SYNC_UPDATE_FIELDS = ["provider_external_id", "distributor_refreshed_at"]
# Turn14 / Meyer / Rough Country: map ``CategoryMapping`` -> ``ProviderPart.category`` /
# ``ProviderPart.overview_category`` (Meyer: first ``category`` segment split on ``;``).
# If source is empty or not in ``category_mappings``, those two fields are null on upsert.
TURN14_PROVIDER_PART_SYNC_UPDATE_FIELDS = [
    "provider_external_id",
    "distributor_refreshed_at",
    "category",
    "overview_category",
]


def _meyer_first_category_token(raw: typing.Any) -> typing.Optional[str]:
    """
    Meyer ``category`` can be multiple values separated by ';'. Use the first non-empty
    segment (stripped) for CategoryMapping, same as Turn14 after that point.
    """
    if raw is None:
        return None
    s = raw.strip() if isinstance(raw, str) else str(raw).strip()
    if not s:
        return None
    first = s.split(";", 1)[0].strip()
    return first or None


def _load_category_mapping_by_source() -> typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]]:
    """
    source_category (stripped) -> (category, overview_category) from category_mappings.
    Later rows with the same source_category win (order by id).
    """
    out: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]] = {}
    for row in src_models.CategoryMapping.objects.order_by("id").values(
        "source_category",
        "category",
        "overview_category",
    ):
        key = (row["source_category"] or "").strip()
        if not key:
            continue
        out[key] = (row["category"], row["overview_category"])
    return out


def _lookup_categories_from_mapping(
    raw_source: typing.Any,
    mapping_by_source: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> typing.Tuple[typing.Optional[str], typing.Optional[str]]:
    """
    ``CategoryMapping`` -> ``(category, overview_category)`` for ``ProviderPart`` on Turn14,
    Meyer, and Rough Country.

    If the source is empty or there is no ``source_category`` row, returns ``(None, None)`` so
    the upsert clears any previously stored category fields. Does not set ``subcategory``.
    """
    if raw_source is None:
        return None, None
    key = raw_source.strip() if isinstance(raw_source, str) else str(raw_source).strip()
    if not key:
        return None, None
    hit = mapping_by_source.get(key)
    if not hit:
        return None, None
    return hit


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

# Partition ``sync_master_parts_from_*`` by internal ``Brands.id`` so workers never compete on the same
# ``(brand_id, part_number)`` upsert. Tune down if PostgreSQL connection pool is tight.
MASTER_PARTS_SYNC_MAX_WORKERS = 8


def _partition_mapped_brands_for_parallel_ingest(
    mapping_rows: typing.Sequence[typing.Any],
    max_workers: int,
) -> typing.List[typing.List[int]]:
    internal_ids = sorted({int(m.brand_id) for m in mapping_rows})
    if not internal_ids:
        return []
    n = min(max_workers, len(internal_ids))
    if n <= 1:
        return [internal_ids]
    chunks = [internal_ids[i::n] for i in range(n)]
    return [c for c in chunks if c]


def _catalog_brand_pk_set_for_internal_brand_partition(
    mapping_rows: typing.Sequence[typing.Any],
    internal_brand_ids: typing.Collection[int],
    catalog_brand_id_attr: str,
) -> typing.Set[int]:
    ib = frozenset(int(x) for x in internal_brand_ids)
    out: typing.Set[int] = set()
    for m in mapping_rows:
        if int(m.brand_id) not in ib:
            continue
        out.add(int(getattr(m, "{}_id".format(catalog_brand_id_attr))))
    return out


def _run_parallel_master_parts_ingest(
    internal_brand_chunks: typing.List[typing.List[int]],
    mapping_rows: typing.Sequence[typing.Any],
    catalog_brand_id_attr: str,
    worker: typing.Callable[[typing.Set[int]], typing.Tuple[int, int]],
) -> typing.Tuple[int, int]:
    """
    ``catalog_brand_id_attr``: base name of the catalog FK on mapping rows, e.g. ``turn14_brand``
    (uses ``turn14_brand_id``).
    """
    if not internal_brand_chunks:
        return 0, 0
    if len(internal_brand_chunks) <= 1:
        chunk = internal_brand_chunks[0]
        catalog_ids = _catalog_brand_pk_set_for_internal_brand_partition(
            mapping_rows, chunk, catalog_brand_id_attr
        )
        return worker(catalog_ids)

    def run_partition_worker(internal_chunk: typing.List[int]) -> typing.Tuple[int, int]:
        close_old_connections()
        try:
            catalog_ids = _catalog_brand_pk_set_for_internal_brand_partition(
                mapping_rows, internal_chunk, catalog_brand_id_attr
            )
            if not catalog_ids:
                return 0, 0
            return worker(catalog_ids)
        finally:
            connection.close()

    total_m, total_p = 0, 0
    with ThreadPoolExecutor(max_workers=len(internal_brand_chunks)) as ex:
        futs = [ex.submit(run_partition_worker, ch) for ch in internal_brand_chunks]
        for fut in futs:
            m, p = fut.result()
            total_m += m
            total_p += p
    return total_m, total_p


def _run_parallel_mapped_brand_int_worker(
    mapping_rows: typing.Sequence[typing.Any],
    catalog_brand_id_attr: str,
    worker: typing.Callable[[typing.Set[int]], int],
    max_workers: int = MASTER_PARTS_SYNC_MAX_WORKERS,
) -> int:
    """
    Run ``worker`` once per disjoint catalog-brand partition (via internal ``Brands`` round-robin).
    Workers return ints (e.g. rows upserted); results are summed.
    """
    internal_brand_chunks = _partition_mapped_brands_for_parallel_ingest(mapping_rows, max_workers)
    if not internal_brand_chunks:
        return 0
    if len(internal_brand_chunks) <= 1:
        catalog_ids = _catalog_brand_pk_set_for_internal_brand_partition(
            mapping_rows, internal_brand_chunks[0], catalog_brand_id_attr
        )
        return worker(catalog_ids)

    def run_partition_worker(internal_chunk: typing.List[int]) -> int:
        close_old_connections()
        try:
            catalog_ids = _catalog_brand_pk_set_for_internal_brand_partition(
                mapping_rows, internal_chunk, catalog_brand_id_attr
            )
            if not catalog_ids:
                return 0
            return worker(catalog_ids)
        finally:
            connection.close()

    total = 0
    with ThreadPoolExecutor(max_workers=len(internal_brand_chunks)) as ex:
        futs = [ex.submit(run_partition_worker, ch) for ch in internal_brand_chunks]
        for fut in futs:
            total += fut.result()
    return total


# Wheel Pros feed uses numeric warehouse columns; map to "CITY, ST" for ProviderPartInventory JSON.
# When two codes share the same city/state, labels include " (code)" so quantities are not merged.
_WHEELPROS_WAREHOUSE_RAW: typing.List[typing.Tuple[str, str, str]] = [
    ("1009", "SAN ANTONIO", "TX"),
    ("1086", "GRAND PRAIRIE", "TX"),
    ("1088", "ATLANTA", "GA"),
    ("1002", "DALLAS", "TX"),
    ("1003", "HOUSTON", "TX"),
    ("1005", "NEW ORLEANS", "LA"),
    ("1007", "OKLAHOMA CITY", "OK"),
    ("1085", "BUENA PARK", "CA"),
    ("1001", "DENVER", "CO"),
    ("1004", "KANSAS CITY", "MO"),
    ("1006", "PHOENIX", "AZ"),
    ("1008", "SACRAMENTO", "CA"),
    ("1011", "LOS ANGELES", "CA"),
    ("1013", "SEATTLE", "WA"),
    ("1014", "ATLANTA", "GA"),
    ("1015", "CHICAGO", "IL"),
    ("1016", "ORLANDO", "FL"),
    ("1018", "MIAMI", "FL"),
    ("1019", "CLEVELAND", "OH"),
    ("1020", "CINCINNATI", "OH"),
    ("1021", "CHARLOTTE", "NC"),
    ("1022", "CRANBURY", "NJ"),
    ("1024", "NASHVILLE", "TN"),
    ("1025", "SALT LAKE", "UT"),
    ("1026", "HARTFORD", "CT"),
    ("1027", "BUENA PARK", "CA"),
    ("1028", "MINNEAPOLIS", "MN"),
    ("1030", "RICHMOND", "VA"),
    ("1031", "RIVERSIDE", "CA"),
    ("1032", "PORTLAND", "OR"),
    ("1034", "BALTIMORE", "MD"),
    ("1036", "DETROIT", "MI"),
    ("1041", "HONOLULU", "HI"),
    ("1042", "NEW YORK", "NY"),
    ("1053", "CORONA", "CA"),
    ("1054", "YORK", "SC"),
    ("1055", "OGDEN", "UT"),
    ("1057", "SALT LAKE CITY", "UT"),
    ("1060", "ARLINGTON", "TX"),
    ("1072", "OGDEN", "UT"),
    ("1421", "GREENSBORO", "NC"),
]


def _wheelpros_warehouse_code_to_label() -> typing.Dict[str, str]:
    label_codes: typing.Dict[str, typing.List[str]] = {}
    for code, city, st in _WHEELPROS_WAREHOUSE_RAW:
        lbl = "{}, {}".format(city, st)
        label_codes.setdefault(lbl, []).append(code)
    out: typing.Dict[str, str] = {}
    for code, city, st in _WHEELPROS_WAREHOUSE_RAW:
        lbl = "{}, {}".format(city, st)
        if len(label_codes[lbl]) > 1:
            out[code] = "{} ({})".format(lbl, code)
        else:
            out[code] = lbl
    return out


_WHEELPROS_WAREHOUSE_CODE_TO_LABEL = _wheelpros_warehouse_code_to_label()


def _map_wheelpros_warehouse_availability(
    wh_avail: typing.Optional[typing.Dict[str, int]],
) -> typing.Optional[typing.Dict[str, int]]:
    """
    Replace numeric warehouse keys with location labels for API/display.
    Unknown codes keep a readable fallback so new feed columns still surface.
    """
    if not wh_avail:
        return None
    out: typing.Dict[str, int] = {}
    for k, v in wh_avail.items():
        code = str(k).strip()
        label = _WHEELPROS_WAREHOUSE_CODE_TO_LABEL.get(code, "WH {}".format(code))
        out[label] = int(v)
    return out if out else None


def _dedupe_provider_part_inventory_for_upsert(
    rows: typing.List[src_models.ProviderPartInventory],
    context: str = "",
) -> typing.List[src_models.ProviderPartInventory]:
    """
    One row per provider_part_id; last wins.
    PostgreSQL rejects INSERT .. ON CONFLICT when the same conflict key appears twice in one batch.
    """
    by_pp: typing.Dict[int, src_models.ProviderPartInventory] = {}
    skipped_no_pp = 0
    merged_dup_rows = 0
    dup_pp_ids_order: typing.List[int] = []
    for r in rows:
        pp_id = r.provider_part_id
        if pp_id is None and r.provider_part is not None:
            pp_id = r.provider_part_id
        if pp_id is None:
            skipped_no_pp += 1
            continue
        k = int(pp_id)
        if k in by_pp:
            merged_dup_rows += 1
            dup_pp_ids_order.append(k)
        by_pp[k] = r
    out = list(by_pp.values())
    if merged_dup_rows or skipped_no_pp:
        sample_pp: typing.List[int] = []
        seen_pp: typing.Set[int] = set()
        for k in dup_pp_ids_order:
            if k not in seen_pp:
                seen_pp.add(k)
                sample_pp.append(k)
            if len(sample_pp) >= 10:
                break
        ctx = " {}".format(context) if context else ""
        logger.info(
            "{} Upsert dedupe ProviderPartInventory{}: input_rows={} unique_keys={} "
            "merged_duplicate_rows={} skipped_missing_provider_part_id={}; "
            "sample provider_part_id with duplicate rows: {}".format(
                _LOG_PREFIX,
                ctx,
                len(rows),
                len(out),
                merged_dup_rows,
                skipped_no_pp,
                sample_pp,
            )
        )
    return out


def _dedupe_provider_part_company_pricing_for_upsert(
    rows: typing.List[src_models.ProviderPartCompanyPricing],
    context: str = "",
) -> typing.List[src_models.ProviderPartCompanyPricing]:
    """One row per (provider_part_id, company_id); last wins."""
    out: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
    skipped = 0
    merged = 0
    dup_keys_order: typing.List[typing.Tuple[int, int]] = []
    for r in rows:
        pp_id = r.provider_part_id
        if pp_id is None and r.provider_part is not None:
            pp_id = r.provider_part_id
        co_id = r.company_id
        if co_id is None and r.company is not None:
            co_id = r.company_id
        if pp_id is None or co_id is None:
            skipped += 1
            continue
        k = (int(pp_id), int(co_id))
        if k in out:
            merged += 1
            dup_keys_order.append(k)
        out[k] = r
    result = list(out.values())
    if merged or skipped:
        sample: typing.List[typing.Tuple[int, int]] = []
        seen_k: typing.Set[typing.Tuple[int, int]] = set()
        for k in dup_keys_order:
            if k not in seen_k:
                seen_k.add(k)
                sample.append(k)
            if len(sample) >= 10:
                break
        ctx = " {}".format(context) if context else ""
        logger.info(
            "{} Upsert dedupe ProviderPartCompanyPricing{}: input_rows={} unique_keys={} "
            "merged_duplicate_rows={} skipped_missing_fk={}; "
            "sample (provider_part_id, company_id): {}".format(
                _LOG_PREFIX,
                ctx,
                len(rows),
                len(result),
                merged,
                skipped,
                sample,
            )
        )
    return result


def _dedupe_master_parts_for_upsert(
    parts: typing.List[src_models.MasterPart],
    context: str = "",
) -> typing.List[src_models.MasterPart]:
    """One row per (brand_id, part_number); last wins."""
    by_key: typing.Dict[typing.Tuple[int, str], src_models.MasterPart] = {}
    skipped = 0
    merged = 0
    dup_keys_order: typing.List[typing.Tuple[int, str]] = []
    for mp in parts:
        bid = mp.brand_id
        if bid is None and mp.brand is not None:
            bid = mp.brand_id
        pn = (mp.part_number or "").strip()
        if bid is None or not pn:
            skipped += 1
            continue
        k = (int(bid), pn)
        if k in by_key:
            merged += 1
            dup_keys_order.append(k)
        by_key[k] = mp
    result = list(by_key.values())
    if merged or skipped:
        sample: typing.List[typing.Tuple[int, str]] = []
        seen_k: typing.Set[typing.Tuple[int, str]] = set()
        for k in dup_keys_order:
            if k not in seen_k:
                seen_k.add(k)
                sample.append(k)
            if len(sample) >= 8:
                break
        ctx = " {}".format(context) if context else ""
        logger.info(
            "{} Upsert dedupe MasterPart{}: input_rows={} unique_keys={} "
            "merged_duplicate_rows={} skipped_missing_brand_or_part_number={}; "
            "sample (brand_id, part_number): {}".format(
                _LOG_PREFIX,
                ctx,
                len(parts),
                len(result),
                merged,
                skipped,
                sample,
            )
        )
    return result


# Master part field priority: Turn14 is primary for description, image_url.
# Other providers (Keystone, etc.) only update sku, aaia_code on existing parts.
# We use a two-phase approach for non-primary providers: INSERT new, UPDATE existing (sku/aaia only).
MASTER_PART_FULL_UPDATE_FIELDS = ["sku", "description", "aaia_code", "image_url", "updated_at"]
MASTER_PART_PARTIAL_UPDATE_FIELDS = ["sku", "aaia_code"]  # Non-primary providers


def _ingest_turn14_items_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    turn14_provider: src_models.Providers,
    t14_brand_to_brand: typing.Dict[int, src_models.Brands],
    t14_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
    category_by_source: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> typing.Tuple[int, int]:
    """Batch-ingest Turn14Items into MasterPart / ProviderPart for one disjoint set of Turn14 brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0
    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0
    while True:
        batch_num += 1
        batch = list(
            src_models.Turn14Items.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "external_id",
                "mfr_part_number",
                "part_number",
                "part_description",
                "thumbnail",
                "brand_id",
                "updated_at",
                "category",
            )[:BATCH_SIZE_MASTER_PARTS]
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

        master_parts = _dedupe_master_parts_for_upsert(
            master_parts, context="Turn14 master_parts batch={}".format(batch_num)
        )
        _now_mp = timezone.now()
        for _mp in master_parts:
            _mp.updated_at = _now_mp
        pgbulk.upsert(
            src_models.MasterPart,
            master_parts,
            unique_fields=["brand", "part_number"],
            update_fields=MASTER_PART_FULL_UPDATE_FIELDS,
        )
        total_master += len(master_parts)

        pairs = list(seen)
        brand_part_to_master = {}
        if pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(pairs),),
                )
                for sql_row in cur.fetchall():
                    mp_id, b_id, p_num = sql_row
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts_by_key = {}
        for row in batch:
            key = item_to_brand_part.get(row["external_id"])
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, turn14_provider.id)
            cat_v, over_v = _lookup_categories_from_mapping(
                row.get("category"), category_by_source
            )
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=turn14_provider,
                provider_external_id=row["external_id"],
                distributor_refreshed_at=row.get("updated_at"),
                category=cat_v,
                overview_category=over_v,
            )

        provider_parts = list(provider_parts_by_key.values())
        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=TURN14_PROVIDER_PART_SYNC_UPDATE_FIELDS,
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

    return total_master, total_provider


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

    category_by_source = _load_category_mapping_by_source()
    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "turn14_brand",
        lambda cids: _ingest_turn14_items_for_mapped_brands(
            cids, turn14_provider, t14_brand_to_brand, t14_brand_to_aaia, category_by_source
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Turn14 total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _ingest_keystone_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    keystone_provider: src_models.Providers,
    ks_brand_to_brand: typing.Dict[int, src_models.Brands],
    ks_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
) -> typing.Tuple[int, int]:
    """Batch-ingest KeystoneParts into MasterPart / ProviderPart for one disjoint set of Keystone brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0
    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0
    while True:
        batch_num += 1
        batch = list(
            src_models.KeystoneParts.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "vcpn",
                "brand_id",
                "manufacturer_part_no",
                "part_number",
                "long_description",
                "aaia_code",
                "updated_at",
            )[:BATCH_SIZE_MASTER_PARTS]
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
        existing_by_key = {}
        if pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(pairs),),
                )
                for sql_row in cur.fetchall():
                    mp_id, b_id, p_num = sql_row
                    existing_by_key[(b_id, p_num)] = mp_id

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]
        existing_keys = [k for k in pairs if k in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="Keystone new_parts batch={}".format(batch_num)
            )
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        if existing_keys:
            key_to_mp = {(mp.brand_id, mp.part_number): mp for mp in master_parts}
            values = [(existing_by_key[k], key_to_mp[k].aaia_code) for k in existing_keys]
            placeholders = ", ".join(["(%s::bigint, %s)"] * len(values))
            params = [x for t in values for x in t]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET aaia_code = v.aaia_code
                    FROM (VALUES {}) AS v(id, aaia_code)
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
                for sql_row in cur.fetchall():
                    mp_id, b_id, p_num = sql_row
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

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
                distributor_refreshed_at=row.get("updated_at"),
            )

        provider_parts = list(provider_parts_by_key.values())
        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=_PROVIDER_PART_SYNC_UPDATE_FIELDS,
            )
            total_provider += len(provider_parts)

        logger.info("{} Batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


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

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "keystone_brand",
        lambda cids: _ingest_keystone_parts_for_mapped_brands(
            cids, keystone_provider, ks_brand_to_brand, ks_brand_to_aaia
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Keystone total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _ingest_meyer_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    meyer_provider: src_models.Providers,
    my_brand_to_brand: typing.Dict[int, src_models.Brands],
    my_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
    category_by_source: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> typing.Tuple[int, int]:
    """Batch-ingest MeyerParts for one disjoint set of Meyer catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.MeyerParts.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "meyer_part",
                "brand_id",
                "mfg_item_number",
                "description",
                "updated_at",
                "category",
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
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="Meyer new_parts batch={}".format(batch_num)
            )
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
            first_for_map = _meyer_first_category_token(row.get("category"))
            cat_v, over_v = _lookup_categories_from_mapping(
                first_for_map, category_by_source
            )
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=meyer_provider,
                provider_external_id=mp_ext,
                distributor_refreshed_at=row.get("updated_at"),
                category=cat_v,
                overview_category=over_v,
            )

        provider_parts = list(provider_parts_by_key.values())
        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=TURN14_PROVIDER_PART_SYNC_UPDATE_FIELDS,
            )
            total_provider += len(provider_parts)

        logger.info("{} Meyer batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts_list), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


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

    category_by_source = _load_category_mapping_by_source()
    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "meyer_brand",
        lambda cids: _ingest_meyer_parts_for_mapped_brands(
            cids,
            meyer_provider,
            my_brand_to_brand,
            my_brand_to_aaia,
            category_by_source,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Meyer total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _atech_provider_external_id(atech_brand_id: int, part_number: str) -> str:
    """Stable A-Tech ProviderPart key: ``AtechBrand`` id + part number (same PN can exist under multiple feed brands)."""
    pn = (part_number or "").strip()
    return "{}_{}".format(int(atech_brand_id), pn)


def _ingest_atech_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    atech_provider: src_models.Providers,
    atech_brand_to_brand: typing.Dict[int, src_models.Brands],
    atech_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
) -> typing.Tuple[int, int]:
    """Batch-ingest AtechParts for one disjoint set of A-Tech catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.AtechParts.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "brand_id",
                "part_number",
                "description",
                "image_url",
                "updated_at",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts_list = []
        atech_feed_brand_pn_to_brand_part: typing.Dict[typing.Tuple[int, str], typing.Tuple[int, str]] = {}

        for row in batch:
            brand = atech_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            pn = row.get("part_number") or ""
            if isinstance(pn, str):
                pn = pn.strip()
            else:
                pn = str(pn or "").strip()
            if not pn:
                continue

            key = (brand.id, pn)
            if key not in seen:
                seen.add(key)
                aaia = atech_brand_to_aaia.get(row["brand_id"])
                master_parts_list.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=pn,
                        sku=pn,
                        description=row.get("description"),
                        aaia_code=aaia,
                        image_url=row.get("image_url"),
                    )
                )
            atech_feed_brand_pn_to_brand_part[(int(row["brand_id"]), pn)] = (brand.id, pn)

        if not master_parts_list:
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

        new_parts = [mp for mp in master_parts_list if (mp.brand_id, mp.part_number) not in existing_by_key]
        existing_keys = [k for k in pairs if k in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="A-Tech new_parts batch={}".format(batch_num)
            )
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        if existing_keys:
            key_to_mp = {(mp.brand_id, mp.part_number): mp for mp in master_parts_list}
            values = [
                (existing_by_key[k], key_to_mp[k].sku, key_to_mp[k].aaia_code)
                for k in existing_keys
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
            if row["brand_id"] not in atech_brand_to_brand:
                continue
            pn = row.get("part_number") or ""
            if isinstance(pn, str):
                pn = pn.strip()
            else:
                pn = str(pn or "").strip()
            if not pn:
                continue
            key_bp = atech_feed_brand_pn_to_brand_part.get((int(row["brand_id"]), pn))
            if not key_bp:
                continue
            master_part = brand_part_to_master.get(key_bp)
            if not master_part:
                continue
            pp_key = (master_part.id, atech_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=atech_provider,
                provider_external_id=_atech_provider_external_id(int(row["brand_id"]), pn),
                distributor_refreshed_at=row.get("updated_at"),
            )

        provider_parts = list(provider_parts_by_key.values())
        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=_PROVIDER_PART_SYNC_UPDATE_FIELDS,
            )
            total_provider += len(provider_parts)

        logger.info("{} A-Tech batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts_list), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_atech() -> None:
    """
    Create/update MasterPart and ProviderPart from AtechParts.
    Only rows whose ``AtechParts.brand`` has a ``BrandAtechBrandMapping`` are included.

    ``MasterPart.part_number`` and ``MasterPart.sku`` are both ``AtechParts.part_number`` (suffix
    after prefix, e.g. ``35370`` for ``ACC-35370``). Pricing resolves ``ProviderPart`` by
    ``(internal brand, MasterPart.part_number)`` via ``_provider_parts_by_master_brand_and_part_number``
    (same logical key as ingest's ``unique_together`` on ``MasterPart``).
    ``ProviderPart.provider_external_id`` is ``_atech_provider_external_id(brand_id, part_number)``
    (matches ``sync_provider_inventory_from_atech``).
    """
    logger.info("{} Syncing master parts from A-Tech (batched, cursor-based).".format(_LOG_PREFIX))

    atech_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ATECH.value,
    ).first()
    if not atech_provider:
        logger.info("{} No A-Tech provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandAtechBrandMapping.objects.select_related("brand", "atech_brand")
    )
    atech_brand_to_brand = {m.atech_brand_id: m.brand for m in mappings}
    atech_brand_to_aaia = {
        m.atech_brand_id: (m.atech_brand.aaia_code if m.atech_brand else None)
        for m in mappings
    }
    if not atech_brand_to_brand:
        logger.info("{} No BrandAtechBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "atech_brand",
        lambda cids: _ingest_atech_parts_for_mapped_brands(
            cids,
            atech_provider,
            atech_brand_to_brand,
            atech_brand_to_aaia,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from A-Tech total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _rough_country_provider_external_id(rc_brand_id: int, sku: str) -> str:
    """Unique per Rough Country provider: rc_brand_id + sku (same sku can exist under different RC brands)."""
    return "{}_{}".format(rc_brand_id, sku)


def _ingest_rough_country_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    rc_provider: src_models.Providers,
    rc_brand_to_brand: typing.Dict[int, src_models.Brands],
    rc_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
    category_by_source: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> typing.Tuple[int, int]:
    """Batch-ingest RoughCountryPart rows for one disjoint set of RC catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.RoughCountryPart.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
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
                "updated_at",
                "category",
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
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="Rough Country new_parts batch={}".format(batch_num)
            )
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
            cat_v, over_v = _lookup_categories_from_mapping(
                row.get("category"), category_by_source
            )
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=rc_provider,
                provider_external_id=ext_id,
                distributor_refreshed_at=row.get("updated_at"),
                category=cat_v,
                overview_category=over_v,
            )

        provider_parts = list(provider_parts_by_key.values())
        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=TURN14_PROVIDER_PART_SYNC_UPDATE_FIELDS,
            )
            total_provider += len(provider_parts)

        logger.info("{} Batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


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

    category_by_source = _load_category_mapping_by_source()
    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "rough_country_brand",
        lambda cids: _ingest_rough_country_parts_for_mapped_brands(
            cids,
            rc_provider,
            rc_brand_to_brand,
            rc_brand_to_aaia,
            category_by_source,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Rough Country total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _dlg_provider_external_id(dlg_brand_id: int, part_number: str) -> str:
    """Stable DLG ProviderPart key: dlg brand row id + part number (same PN can exist under multiple feed brands)."""
    pn = (part_number or "").strip()
    return "{}_{}".format(int(dlg_brand_id), pn)


def _ingest_dlg_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    dlg_provider: src_models.Providers,
    dlg_brand_to_brand: typing.Dict[int, src_models.Brands],
    dlg_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
) -> typing.Tuple[int, int]:
    """Batch-ingest DlgParts for one disjoint set of DLG catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.DlgParts.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "brand_id",
                "part_number",
                "display_name",
                "updated_at",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        dlg_external_to_brand_part = {}

        for row in batch:
            brand = dlg_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = row.get("part_number") or ""
            if isinstance(part_number, str):
                part_number = part_number.strip()
            else:
                part_number = str(part_number or "").strip()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                aaia = dlg_brand_to_aaia.get(row["brand_id"])
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=row.get("display_name"),
                        aaia_code=aaia,
                        image_url=None,
                    )
                )
            dlg_external_to_brand_part[
                _dlg_provider_external_id(row["brand_id"], part_number)
            ] = (brand.id, part_number)

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
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="DLG new_parts batch={}".format(batch_num)
            )
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
            pn = row.get("part_number") or ""
            if isinstance(pn, str):
                pn = pn.strip()
            else:
                pn = str(pn or "").strip()
            if not pn:
                continue
            ext_id = _dlg_provider_external_id(row["brand_id"], pn)
            key_bp = dlg_external_to_brand_part.get(ext_id)
            if not key_bp:
                continue
            master_part = brand_part_to_master.get(key_bp)
            if not master_part:
                continue
            pp_key = (master_part.id, dlg_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=dlg_provider,
                provider_external_id=ext_id,
                distributor_refreshed_at=row.get("updated_at"),
            )

        provider_parts = list(provider_parts_by_key.values())
        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=_PROVIDER_PART_SYNC_UPDATE_FIELDS,
            )
            total_provider += len(provider_parts)

        logger.info("{} DLG batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_dlg() -> None:
    """
    Create/update MasterPart and ProviderPart from DlgParts.
    Only rows whose DlgBrand has a BrandDlgBrandMapping are included.

    Resolution uses **(catalog brand_id, part_number) only** — no Meyer-style ``(brand_id, sku)`` lookup.
    ``MasterPart.sku`` is set to ``part_number`` for consistency with other single-key feeds.
    ``ProviderPart.provider_external_id`` is ``f\"{dlg_brand_id}_{part_number}\"`` so inventory joins stay unique.
    """
    logger.info("{} Syncing master parts from DLG (batched, part_number only).".format(_LOG_PREFIX))

    dlg_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.DLG.value,
    ).first()
    if not dlg_provider:
        logger.info("{} No DLG provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandDlgBrandMapping.objects.select_related("brand", "dlg_brand")
    )
    dlg_brand_to_brand = {m.dlg_brand_id: m.brand for m in mappings}
    dlg_brand_to_aaia = {
        m.dlg_brand_id: (m.brand.aaia_code if m.brand else None)
        for m in mappings
    }
    if not dlg_brand_to_brand:
        logger.info("{} No BrandDlgBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "dlg_brand",
        lambda cids: _ingest_dlg_parts_for_mapped_brands(
            cids,
            dlg_provider,
            dlg_brand_to_brand,
            dlg_brand_to_aaia,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from DLG total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _rough_country_inventory_warehouse_availability(
    nv_stock: typing.Optional[int],
    tn_stock: typing.Optional[int],
) -> typing.Optional[typing.Dict[str, typing.Any]]:
    """NV/TN stock by friendly DC names."""
    out: typing.Dict[str, typing.Any] = {}
    if nv_stock is not None:
        out["Nevada"] = nv_stock
    if tn_stock is not None:
        out["Tennessee"] = tn_stock
    if not out:
        return None
    return out


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

    mappings = list(
        src_models.BrandRoughCountryBrandMapping.objects.select_related("brand", "rough_country_brand")
    )
    if not mappings:
        logger.info("{} No BrandRoughCountryBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=rc_provider)
    }

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.RoughCountryPart.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
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
                        warehouse_availability=_rough_country_inventory_warehouse_availability(nv, tn),
                        last_synced_at=now,
                        updated_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="Rough Country inventory batch={}".format(batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "rough_country_brand", _worker)
    logger.info("{} Synced {} Rough Country inventory records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_inventory_from_dlg() -> None:
    """Sync ProviderPartInventory from DlgParts (mapped brands): totals + warehouse_availability.available_on_hand."""
    logger.info("{} Syncing provider inventory from DLG.".format(_LOG_PREFIX))

    dlg_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.DLG.value,
    ).first()
    if not dlg_provider:
        logger.info("{} No DLG provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandDlgBrandMapping.objects.select_related("brand", "dlg_brand")
    )
    if not mappings:
        logger.info("{} No BrandDlgBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=dlg_provider)
    }

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.DlgParts.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
                .order_by("id")
                .values("id", "brand_id", "part_number", "available_on_hand")[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            to_upsert = []
            for row in batch:
                pn = row.get("part_number") or ""
                if isinstance(pn, str):
                    pn = pn.strip()
                else:
                    pn = str(pn or "").strip()
                if not pn:
                    continue
                ext_id = _dlg_provider_external_id(row["brand_id"], pn)
                provider_part = provider_parts.get(ext_id)
                if not provider_part:
                    continue
                qty = row.get("available_on_hand")
                wh_avail: typing.Dict[str, typing.Any]
                try:
                    if qty is None:
                        total_qty = 0
                        wh_avail = {"available_on_hand": None}
                    else:
                        total_qty = int(qty)
                        wh_avail = {"available_on_hand": total_qty}
                except (TypeError, ValueError):
                    total_qty = 0
                    wh_avail = {"available_on_hand": None}
                to_upsert.append(
                    src_models.ProviderPartInventory(
                        provider_part=provider_part,
                        warehouse_total_qty=total_qty,
                        manufacturer_inventory=None,
                        manufacturer_esd=None,
                        warehouse_availability=wh_avail,
                        last_synced_at=now,
                        updated_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="DLG inventory batch={}".format(batch_num)
                )
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

            logger.info("{} DLG inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "dlg_brand", _worker)
    logger.info("{} Synced {} DLG inventory records total.".format(_LOG_PREFIX, total_upserted))


def _atech_dc_inventory_sum(row: typing.Dict) -> int:
    total = 0
    for k in ("qty_tallmadge", "qty_sparks", "qty_mcdonough", "qty_arlington"):
        v = row.get(k)
        if v is not None:
            try:
                total += int(v)
            except (TypeError, ValueError):
                pass
    return total


def _atech_inventory_warehouse_availability(row: typing.Dict) -> typing.Dict[str, typing.Any]:
    """DC qty fields from AtechParts; keys are user-facing city, state labels (see ``constants.ATECH_DC_QTY_FIELD_TO_LOCATION_LABEL``)."""
    return {
        label: row.get(field)
        for field, label in src_constants.ATECH_DC_QTY_FIELD_TO_LOCATION_LABEL.items()
    }


def sync_provider_inventory_from_atech() -> None:
    """
    Sync ProviderPartInventory from AtechParts DC quantities (mapped brands only).
    ``ProviderPart`` rows must exist; ``provider_external_id`` is
    ``_atech_provider_external_id(brand_id, part_number)``.
    """
    logger.info("{} Syncing provider inventory from A-Tech.".format(_LOG_PREFIX))

    atech_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ATECH.value,
    ).first()
    if not atech_provider:
        logger.info("{} No A-Tech provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandAtechBrandMapping.objects.select_related("brand", "atech_brand")
    )
    if not mappings:
        logger.info("{} No BrandAtechBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=atech_provider)
    }

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.AtechParts.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
                .order_by("id")
                .values(
                    "id",
                    "brand_id",
                    "part_number",
                    "qty_tallmadge",
                    "qty_sparks",
                    "qty_mcdonough",
                    "qty_arlington",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            inv_by_provider_part_id: typing.Dict[int, src_models.ProviderPartInventory] = {}
            for ap in batch:
                ext = ap.get("part_number")
                if isinstance(ext, str):
                    ext = ext.strip()
                else:
                    ext = str(ext or "").strip()
                if not ext:
                    continue
                bid = ap.get("brand_id")
                if bid is None:
                    continue
                ext_id = _atech_provider_external_id(int(bid), ext)
                pp = provider_parts.get(ext_id)
                if not pp:
                    continue
                wh_total = _atech_dc_inventory_sum(ap)
                inv_by_provider_part_id[pp.id] = src_models.ProviderPartInventory(
                    provider_part=pp,
                    warehouse_total_qty=wh_total,
                    manufacturer_inventory=None,
                    manufacturer_esd=None,
                    warehouse_availability=_atech_inventory_warehouse_availability(ap),
                    last_synced_at=now,
                    updated_at=now,
                )

            to_upsert = list(inv_by_provider_part_id.values())
            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="A-Tech inventory batch={}".format(batch_num)
                )
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

            logger.info("{} A-Tech inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "atech_brand", _worker)
    logger.info("{} Synced {} A-Tech inventory records total.".format(_LOG_PREFIX, total_upserted))


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

    mappings = list(
        src_models.BrandRoughCountryBrandMapping.objects.select_related("brand", "rough_country_brand")
    )
    rc_brand_to_brand = {m.rough_country_brand_id: m.brand for m in mappings}
    if not rc_brand_to_brand:
        logger.info("{} No BrandRoughCountryBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.RoughCountryCompanyPricing.objects.filter(
                    id__gt=last_id, part__brand_id__in=catalog_ids
                )
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
            master_keys = []
            for row in batch:
                brand = rc_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = row.get("part__sku")
                if isinstance(sku, str):
                    sku = sku.strip()
                else:
                    sku = str(sku or "").strip()
                if not sku:
                    continue
                master_keys.append((brand.id, sku))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(rc_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = rc_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = row.get("part__sku")
                if isinstance(sku, str):
                    sku = sku.strip()
                else:
                    sku = str(sku or "").strip()
                if not sku:
                    continue
                provider_part = pp_by_key.get((brand.id, sku))
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Rough Country pricing batch={}".format(batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "rough_country_brand", _worker)
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


def _provider_parts_by_master_brand_and_part_number(
    provider: src_models.Providers,
    keys: typing.Collection[typing.Tuple[int, str]],
) -> typing.Dict[typing.Tuple[int, str], src_models.ProviderPart]:
    """
    Map (internal Brands.id, MasterPart.part_number) -> ProviderPart for this distributor.
    This is the join path sync_master_parts_from_* uses (unique_together master_part + provider).
    """
    out: typing.Dict[typing.Tuple[int, str], src_models.ProviderPart] = {}
    normalized: typing.List[typing.Tuple[int, str]] = []
    for b_id, pn in keys:
        pn_s = (pn or "").strip() if pn is not None else ""
        if not pn_s:
            continue
        normalized.append((int(b_id), pn_s))
    if not normalized:
        return out
    uniq: typing.List[typing.Tuple[int, str]] = list(dict.fromkeys(normalized))
    for i in range(0, len(uniq), WHEELPROS_LOOKUP_CHUNK):
        chunk_list = uniq[i : i + WHEELPROS_LOOKUP_CHUNK]
        chunk_set = set(chunk_list)
        brand_ids = {b for b, _ in chunk_list}
        part_numbers = {pn for _, pn in chunk_list}
        for pp in (
            src_models.ProviderPart.objects.filter(
                provider=provider,
                master_part__brand_id__in=brand_ids,
                master_part__part_number__in=part_numbers,
            ).select_related("master_part")
        ):
            mp = pp.master_part
            if not mp:
                continue
            k = (mp.brand_id, (mp.part_number or "").strip())
            if k in chunk_set and k not in out:
                out[k] = pp
    return out


def _meyer_provider_parts_by_brand_sku_first(
    provider: src_models.Providers,
    keys: typing.Collection[typing.Tuple[int, str]],
) -> typing.Dict[typing.Tuple[int, str], src_models.ProviderPart]:
    """
    (brand_id, MasterPart.sku) -> ProviderPart: lowest MasterPart.id per (brand, sku) that has a
    Meyer ProviderPart. Uses PostgreSQL DISTINCT ON + join per chunk (one tight round-trip, then
    one ProviderPart IN per chunk) so duplicate skus do not inflate rows read from master_parts.
    """
    out: typing.Dict[typing.Tuple[int, str], src_models.ProviderPart] = {}
    sku_by_brand: typing.Dict[int, typing.Set[str]] = {}
    for b_id, sku in keys:
        sku_s = (sku or "").strip() if sku is not None else ""
        if not sku_s:
            continue
        bid = int(b_id)
        if bid not in sku_by_brand:
            sku_by_brand[bid] = set()
        sku_by_brand[bid].add(sku_s)

    provider_id = provider.id
    sql = """
        SELECT DISTINCT ON (mp.brand_id, mp.sku)
            pp.id,
            mp.brand_id,
            mp.sku
        FROM master_parts mp
        INNER JOIN provider_parts pp
            ON pp.master_part_id = mp.id AND pp.provider_id = %s
        WHERE mp.brand_id = %s AND mp.sku IN %s
        ORDER BY mp.brand_id, mp.sku, mp.id
        """

    key_by_pp_id: typing.Dict[int, typing.Tuple[int, str]] = {}
    for b_id, skus in sku_by_brand.items():
        sku_list = list(skus)
        for i in range(0, len(sku_list), WHEELPROS_LOOKUP_CHUNK):
            chunk = tuple(sku_list[i : i + WHEELPROS_LOOKUP_CHUNK])
            with connection.cursor() as cur:
                cur.execute(sql, (provider_id, b_id, chunk))
                for pp_id, mp_brand_id, mp_sku in cur.fetchall():
                    k = (int(mp_brand_id), (mp_sku or "").strip())
                    key_by_pp_id[int(pp_id)] = k

    if not key_by_pp_id:
        return out

    for pp in (
        src_models.ProviderPart.objects.filter(id__in=key_by_pp_id.keys())
        .select_related("master_part")
    ):
        k = key_by_pp_id.get(pp.id)
        if k:
            out[k] = pp
    return out


def _turn14_pricing_batch_external_id_to_master_keys(
    batch: typing.List[typing.Dict],
    t14_brand_to_brand: typing.Dict[int, src_models.Brands],
    mapped_t14_brand_ids: typing.Set[int],
) -> typing.Dict[str, typing.Tuple[int, str]]:
    """
    Turn14BrandPricing.external_id -> (internal brand id, MasterPart.part_number) via Turn14Items,
    matching sync_master_parts_from_turn14 (mfr_part_number + BrandTurn14BrandMapping).
    """
    ext_ids_raw = []
    for pr in batch:
        eid = pr.get("external_id")
        if eid is None or eid == "":
            continue
        ext_ids_raw.append(str(eid).strip())
    if not ext_ids_raw:
        return {}
    ext_ids = list(dict.fromkeys(ext_ids_raw))
    result: typing.Dict[str, typing.Tuple[int, str]] = {}
    scan_chunk = 500
    for i in range(0, len(ext_ids), scan_chunk):
        part = ext_ids[i : i + scan_chunk]
        for row in (
            src_models.Turn14Items.objects.filter(
                external_id__in=part,
                brand_id__in=mapped_t14_brand_ids,
            ).values("external_id", "brand_id", "mfr_part_number")
        ):
            brand = t14_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue
            pn = row.get("mfr_part_number") or ""
            if isinstance(pn, str):
                pn = pn.strip()
            else:
                pn = str(pn or "").strip()
            if not pn:
                continue
            eid_key = str(row["external_id"]).strip()
            result[eid_key] = (brand.id, pn)
    return result


def _meyer_company_pricing_batch_row_id_to_provider_part(
    batch: typing.List[typing.Dict],
    meyer_provider: src_models.Providers,
    my_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.Dict[int, src_models.ProviderPart]:
    """
    MeyerCompanyPricing row id -> ProviderPart: (brand, meyer_part) -> first MasterPart.sku match,
    then (brand, mfg_item_number) -> MasterPart.part_number. All lookups batched per batch.
    """
    row_id_to_pp: typing.Dict[int, src_models.ProviderPart] = {}
    row_meta: typing.Dict[int, typing.Tuple[src_models.Brands, str, str]] = {}
    sku_lookup_keys: typing.List[typing.Tuple[int, str]] = []

    for row in batch:
        rid = row["id"]
        brand = my_brand_to_brand.get(row.get("part__brand_id"))
        if not brand:
            continue
        mfg = row.get("part__mfg_item_number")
        if isinstance(mfg, str):
            mfg = mfg.strip()
        else:
            mfg = str(mfg or "").strip()
        mp_ext = row.get("part__meyer_part")
        if isinstance(mp_ext, str):
            mp_ext = mp_ext.strip()
        else:
            mp_ext = str(mp_ext or "").strip()
        if not mfg and not mp_ext:
            continue
        row_meta[rid] = (brand, mfg, mp_ext)
        if mp_ext:
            sku_lookup_keys.append((brand.id, mp_ext))

    pp_by_sku = _meyer_provider_parts_by_brand_sku_first(
        meyer_provider,
        list(dict.fromkeys(sku_lookup_keys)),
    )
    for rid, (brand, _, mp_ext) in row_meta.items():
        if not mp_ext:
            continue
        pp = pp_by_sku.get((brand.id, mp_ext))
        if pp:
            row_id_to_pp[rid] = pp

    part_fallback_keys: typing.List[typing.Tuple[int, str]] = []
    for rid, (brand, mfg, _mp_ext) in row_meta.items():
        if rid in row_id_to_pp or not mfg:
            continue
        part_fallback_keys.append((brand.id, mfg))

    if part_fallback_keys:
        pp_by_pn = _provider_parts_by_master_brand_and_part_number(
            meyer_provider,
            list(dict.fromkeys(part_fallback_keys)),
        )
        for rid, (brand, mfg, _mp_ext) in row_meta.items():
            if rid in row_id_to_pp:
                continue
            if not mfg:
                continue
            pp = pp_by_pn.get((brand.id, mfg))
            if pp:
                row_id_to_pp[rid] = pp

    return row_id_to_pp


def _dlg_company_pricing_batch_row_id_to_provider_part(
    batch: typing.List[typing.Dict],
    dlg_provider: src_models.Providers,
    dlg_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.Dict[int, src_models.ProviderPart]:
    """
    DlgCompanyPricing row id -> ProviderPart via (catalog brand_id, MasterPart.part_number) only
    (no sku-based resolution; matches sync_master_parts_from_dlg).
    """
    row_id_to_pp: typing.Dict[int, src_models.ProviderPart] = {}
    lookup_keys: typing.List[typing.Tuple[int, str]] = []
    row_meta: typing.Dict[int, typing.Tuple[src_models.Brands, str]] = {}

    for row in batch:
        rid = row["id"]
        brand = dlg_brand_to_brand.get(row.get("part__brand_id"))
        if not brand:
            continue
        pn = row.get("part__part_number")
        if isinstance(pn, str):
            pn = pn.strip()
        else:
            pn = str(pn or "").strip()
        if not pn:
            continue
        row_meta[rid] = (brand, pn)
        lookup_keys.append((brand.id, pn))

    pp_by_pn = _provider_parts_by_master_brand_and_part_number(
        dlg_provider,
        list(dict.fromkeys(lookup_keys)),
    )
    for rid, (brand, pn) in row_meta.items():
        pp = pp_by_pn.get((brand.id, pn))
        if pp:
            row_id_to_pp[rid] = pp
    return row_id_to_pp


def _dlg_decimal_to_provider_money(value: typing.Any) -> typing.Optional[typing.Any]:
    """Quantize to 2 decimals for ProviderPartCompanyPricing columns."""
    if value is None:
        return None
    try:
        from decimal import ROUND_HALF_UP, Decimal

        d = value if isinstance(value, Decimal) else Decimal(str(value))
        return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except Exception:
        return None


def _ingest_wheelpros_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    wp_provider: src_models.Providers,
    wp_brand_to_brand: typing.Dict[int, src_models.Brands],
    wp_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
) -> typing.Tuple[int, int]:
    """Batch-ingest WheelProsPart rows for one disjoint set of WheelPros catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.WheelProsPart.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "brand_id",
                "part_number",
                "part_description",
                "image_url",
                "updated_at",
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
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="WheelPros new_parts batch={}".format(batch_num)
            )
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
                distributor_refreshed_at=row.get("updated_at"),
            )

        provider_parts = list(provider_parts_by_key.values())
        if provider_parts:
            pgbulk.upsert(
                src_models.ProviderPart,
                provider_parts,
                unique_fields=["master_part", "provider"],
                update_fields=_PROVIDER_PART_SYNC_UPDATE_FIELDS,
            )
            total_provider += len(provider_parts)

        logger.info("{} WheelPros batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        if len(batch) == BATCH_SIZE_MASTER_PARTS_WHEELPROS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


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

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "wheelpros_brand",
        lambda cids: _ingest_wheelpros_parts_for_mapped_brands(
            cids,
            wp_provider,
            wp_brand_to_brand,
            wp_brand_to_aaia,
        ),
    )
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

    mappings = list(
        src_models.BrandWheelProsBrandMapping.objects.select_related("brand", "wheelpros_brand")
    )
    if not mappings:
        logger.info("{} No BrandWheelProsBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts_by_ext_id, provider_parts_by_brand_sku = _wheelpros_provider_part_lookup(wp_provider)

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.WheelProsPart.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
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
                if wh_avail:
                    wh_avail = _map_wheelpros_warehouse_availability(wh_avail)
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
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="WheelPros inventory batch={}".format(batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "wheelpros_brand", _worker)
    logger.info("{} Synced {} WheelPros inventory records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_wheelpros() -> None:
    """
    Sync ProviderPartCompanyPricing from WheelProsCompanyPricing (per-company SFTP pricing).
    Resolves ProviderPart via (internal brand id, MasterPart.part_number) like sync_master_parts_from_wheelpros.
    Sets ``cost`` from MSRP and company ``wheel_markup`` / ``tire_markup`` / ``accessories_markup`` (see
    :func:`src.integrations.services.wheelpros.dealer_cost_from_msrp`) using ``WheelProsPart.feed_type``.
    """
    logger.info("{} Syncing provider pricing from WheelPros.".format(_LOG_PREFIX))

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
    if not wp_brand_to_brand:
        logger.info("{} No BrandWheelProsBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.WheelProsCompanyPricing.objects.filter(
                    id__gt=last_id, part__brand_id__in=catalog_ids
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "msrp_usd",
                    "map_usd",
                    "part__brand_id",
                    "part__part_number",
                    "part__feed_type",
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
            creds_by_company: typing.Dict[int, typing.Dict] = {}
            if batch_company_ids:
                for cpr in src_models.CompanyProviders.objects.filter(
                    company_id__in=batch_company_ids,
                    provider_id=wp_provider.id,
                ):
                    creds_by_company[cpr.company_id] = cpr.credentials or {}
            master_keys = []
            for row in batch:
                wb = wp_brand_to_brand.get(row.get("part__brand_id"))
                if not wb:
                    continue
                part_number = (row.get("part__part_number") or "").strip()
                if not part_number:
                    continue
                master_keys.append((wb.id, part_number))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(wp_provider, master_keys)

            to_upsert = []
            for row in batch:
                wb = wp_brand_to_brand.get(row.get("part__brand_id"))
                if not wb:
                    continue
                part_number = (row.get("part__part_number") or "").strip()
                if not part_number:
                    continue
                provider_part = pp_by_key.get((wb.id, part_number))
                if not provider_part:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                msrp = row.get("msrp_usd")
                map_price = row.get("map_usd")
                cid = row.get("company_id")
                creds = creds_by_company.get(cid) if cid is not None else {}
                cost = dealer_cost_from_msrp(msrp, row.get("part__feed_type"), creds)
                to_upsert.append(
                    src_models.ProviderPartCompanyPricing(
                        provider_part=provider_part,
                        company=company,
                        cost=cost,
                        jobber_price=map_price,
                        map_price=map_price,
                        msrp=msrp,
                        retail_price=msrp,
                        last_synced_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="WheelPros pricing batch={}".format(batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} WheelPros pricing batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "wheelpros_brand", _worker)
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

    mappings = list(
        src_models.BrandTurn14BrandMapping.objects.select_related("brand", "turn14_brand")
    )
    t14_brand_to_brand = {m.turn14_brand_id: m.brand for m in mappings}
    mapped_t14_brand_ids = set(t14_brand_to_brand.keys())
    if not t14_brand_to_brand:
        logger.info("{} No BrandTurn14BrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.Turn14BrandPricing.objects.filter(
                    id__gt=last_id,
                    company_id=company_id,
                    brand_id__in=catalog_ids,
                )
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
            ext_to_key = _turn14_pricing_batch_external_id_to_master_keys(
                batch, t14_brand_to_brand, mapped_t14_brand_ids
            )
            pp_by_key = _provider_parts_by_master_brand_and_part_number(turn14_provider, ext_to_key.values())

            to_upsert = []
            for pr in batch:
                eid = str(pr.get("external_id") or "").strip()
                key = ext_to_key.get(eid)
                if not key:
                    continue
                provider_part = pp_by_key.get(key)
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Turn14 pricing company={} batch={}".format(company_id, batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "turn14_brand", _worker)
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

    mappings = list(
        src_models.BrandKeystoneBrandMapping.objects.select_related("brand", "keystone_brand")
    )
    ks_brand_to_brand = {m.keystone_brand_id: m.brand for m in mappings}
    if not ks_brand_to_brand:
        logger.info("{} No BrandKeystoneBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.KeystoneCompanyPricing.objects.filter(
                    id__gt=last_id,
                    company_id=company_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "cost",
                    "jobber_price",
                    "part__brand_id",
                    "part__manufacturer_part_no",
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
            master_keys = []
            for row in batch:
                brand = ks_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__manufacturer_part_no") or ""
                if isinstance(pn, str):
                    pn = pn.strip()
                else:
                    pn = str(pn or "").strip()
                if not pn:
                    continue
                master_keys.append((brand.id, pn))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(keystone_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = ks_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__manufacturer_part_no") or ""
                if isinstance(pn, str):
                    pn = pn.strip()
                else:
                    pn = str(pn or "").strip()
                if not pn:
                    continue
                provider_part = pp_by_key.get((brand.id, pn))
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Keystone pricing company={} batch={}".format(company_id, batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "keystone_brand", _worker)
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

    mappings = list(
        src_models.BrandMeyerBrandMapping.objects.select_related("brand", "meyer_brand")
    )
    my_brand_to_brand = {m.meyer_brand_id: m.brand for m in mappings}
    if not my_brand_to_brand:
        logger.info("{} No BrandMeyerBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.MeyerCompanyPricing.objects.filter(
                    id__gt=last_id,
                    company_id=company_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "cost",
                    "jobber_price",
                    "map_price",
                    "part__brand_id",
                    "part__meyer_part",
                    "part__mfg_item_number",
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
            row_id_to_pp = _meyer_company_pricing_batch_row_id_to_provider_part(
                batch, meyer_provider, my_brand_to_brand
            )
            pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
            for row in batch:
                pp = row_id_to_pp.get(row["id"])
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Meyer pricing company={} batch={}".format(company_id, batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "meyer_brand", _worker)
    logger.info("{} Synced {} Meyer pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))


def sync_provider_pricing_from_dlg_for_company(company_id: int) -> None:
    logger.info("{} Syncing DLG provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    dlg_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.DLG.value,
    ).first()
    if not dlg_provider:
        logger.info("{} No DLG provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandDlgBrandMapping.objects.select_related("brand", "dlg_brand")
    )
    dlg_brand_to_brand = {m.dlg_brand_id: m.brand for m in mappings}
    if not dlg_brand_to_brand:
        logger.info("{} No BrandDlgBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.DlgCompanyPricing.objects.filter(
                    id__gt=last_id,
                    company_id=company_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "base_price",
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
            row_id_to_pp = _dlg_company_pricing_batch_row_id_to_provider_part(
                batch, dlg_provider, dlg_brand_to_brand
            )
            pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
            for row in batch:
                pp = row_id_to_pp.get(row["id"])
                if not pp:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                bp = _dlg_decimal_to_provider_money(row.get("base_price"))
                pricing_by_pp_company[(pp.id, company.id)] = src_models.ProviderPartCompanyPricing(
                    provider_part=pp,
                    company=company,
                    cost=bp,
                    jobber_price=bp,
                    map_price=None,
                    msrp=None,
                    retail_price=bp,
                    last_synced_at=now,
                )

            to_upsert = list(pricing_by_pp_company.values())
            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="DLG pricing company={} batch={}".format(company_id, batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} DLG pricing company={} batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "dlg_brand", _worker)
    logger.info("{} Synced {} DLG pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))


def _atech_company_pricing_batch_row_id_to_provider_part(
    batch: typing.List[typing.Dict],
    atech_provider: src_models.Providers,
    atech_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.Dict[int, src_models.ProviderPart]:
    """
    AtechCompanyPricing row id -> ProviderPart via ``(internal Brands.id, MasterPart.part_number)``
    from the linked ``AtechParts`` row (``part__part_number``), matching
    ``_ingest_atech_parts_for_mapped_brands`` / ``_provider_parts_by_master_brand_and_part_number``.

    Inventory instead resolves by ``ProviderPart.provider_external_id`` =
    ``_atech_provider_external_id(catalog_brand_id, part_number)``; both paths target the same
    ``ProviderPart`` when ingest used that master row (one ``ProviderPart`` per ``master_part`` for A-Tech).
    """
    row_id_to_pp: typing.Dict[int, src_models.ProviderPart] = {}
    part_lookup_keys: typing.List[typing.Tuple[int, str]] = []
    row_meta: typing.Dict[int, typing.Tuple[src_models.Brands, str]] = {}

    for row in batch:
        rid = row["id"]
        brand = atech_brand_to_brand.get(row.get("part__brand_id"))
        if not brand:
            continue
        pn = row.get("part__part_number")
        if isinstance(pn, str):
            pn = pn.strip()
        else:
            pn = str(pn or "").strip()
        if not pn:
            continue
        row_meta[rid] = (brand, pn)
        part_lookup_keys.append((brand.id, pn))

    pp_by_brand_part_number = _provider_parts_by_master_brand_and_part_number(
        atech_provider,
        list(dict.fromkeys(part_lookup_keys)),
    )
    for rid, (brand, pn) in row_meta.items():
        pp = pp_by_brand_part_number.get((brand.id, pn))
        if pp:
            row_id_to_pp[rid] = pp
    return row_id_to_pp


def _run_atech_provider_pricing_sync(company_id: typing.Optional[int]) -> int:
    """
    Fan out AtechCompanyPricing -> ProviderPartCompanyPricing.
    If company_id is None, process all companies (single pass by id, like Keystone / Rough Country).
    """
    atech_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ATECH.value,
    ).first()
    if not atech_provider:
        logger.info("{} No A-Tech provider found.".format(_LOG_PREFIX))
        return 0

    mappings = list(
        src_models.BrandAtechBrandMapping.objects.select_related("brand", "atech_brand")
    )
    atech_brand_to_brand = {m.atech_brand_id: m.brand for m in mappings}
    if not atech_brand_to_brand:
        logger.info("{} No BrandAtechBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return 0

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.AtechCompanyPricing.objects.filter(
                id__gt=last_id,
                part__brand_id__in=catalog_ids,
            )
            if company_id is not None:
                qs = qs.filter(company_id=company_id)
            batch = list(
                qs.order_by("id")
                .values(
                    "id",
                    "company_id",
                    "cost",
                    "retail_price",
                    "jobber_price",
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
            row_id_to_pp = _atech_company_pricing_batch_row_id_to_provider_part(
                batch, atech_provider, atech_brand_to_brand
            )
            pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
            for row in batch:
                pp = row_id_to_pp.get(row["id"])
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
                    map_price=None,
                    msrp=None,
                    retail_price=row.get("retail_price"),
                    last_synced_at=now,
                )

            to_upsert = list(pricing_by_pp_company.values())
            if company_id is not None:
                dedupe_ctx = "A-Tech pricing company={} batch={}".format(company_id, batch_num)
            else:
                dedupe_ctx = "A-Tech pricing batch={}".format(batch_num)
            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(to_upsert, context=dedupe_ctx)
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            if company_id is not None:
                logger.info("{} A-Tech pricing company={} batch {}: {} records (last_id={})".format(
                    _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
                ))
            else:
                logger.info("{} A-Tech pricing batch {}: {} records (last_id={})".format(
                    _LOG_PREFIX, batch_num, len(to_upsert), last_id
                ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    return _run_parallel_mapped_brand_int_worker(mappings, "atech_brand", _worker)


def sync_provider_pricing_from_atech() -> None:
    """
    Sync ProviderPartCompanyPricing from all AtechCompanyPricing rows (all companies).
    Same mapping as :func:`sync_provider_pricing_from_atech_for_company`; used by full master parts / cron.
    """
    logger.info("{} Syncing A-Tech provider pricing (all companies).".format(_LOG_PREFIX))
    total = _run_atech_provider_pricing_sync(None)
    logger.info("{} Synced {} A-Tech pricing records total.".format(_LOG_PREFIX, total))


def sync_provider_pricing_from_atech_for_company(company_id: int) -> None:
    logger.info("{} Syncing A-Tech provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))
    total = _run_atech_provider_pricing_sync(company_id)
    logger.info("{} Synced {} A-Tech pricing records for company_id={}.".format(
        _LOG_PREFIX, total, company_id,
    ))


def sync_provider_pricing_from_rough_country_for_company(company_id: int) -> None:
    logger.info("{} Syncing Rough Country provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

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
    if not rc_brand_to_brand:
        logger.info("{} No BrandRoughCountryBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.RoughCountryCompanyPricing.objects.filter(
                    id__gt=last_id,
                    company_id=company_id,
                    part__brand_id__in=catalog_ids,
                )
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
            master_keys = []
            for row in batch:
                brand = rc_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = row.get("part__sku")
                if isinstance(sku, str):
                    sku = sku.strip()
                else:
                    sku = str(sku or "").strip()
                if not sku:
                    continue
                master_keys.append((brand.id, sku))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(rc_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = rc_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = row.get("part__sku")
                if isinstance(sku, str):
                    sku = sku.strip()
                else:
                    sku = str(sku or "").strip()
                if not sku:
                    continue
                provider_part = pp_by_key.get((brand.id, sku))
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Rough Country pricing company={} batch={}".format(company_id, batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "rough_country_brand", _worker)
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

    mappings = list(
        src_models.BrandWheelProsBrandMapping.objects.select_related("brand", "wheelpros_brand")
    )
    wp_brand_to_brand = {m.wheelpros_brand_id: m.brand for m in mappings}
    if not wp_brand_to_brand:
        logger.info("{} No BrandWheelProsBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()
    company_cp = src_models.CompanyProviders.objects.filter(
        company_id=company_id,
        provider_id=wp_provider.id,
    ).first()
    company_creds = (company_cp.credentials or {}) if company_cp else {}

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.WheelProsCompanyPricing.objects.filter(
                    id__gt=last_id,
                    company_id=company_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "msrp_usd",
                    "map_usd",
                    "part__brand_id",
                    "part__part_number",
                    "part__feed_type",
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
            master_keys = []
            for row in batch:
                wb = wp_brand_to_brand.get(row.get("part__brand_id"))
                if not wb:
                    continue
                part_number = (row.get("part__part_number") or "").strip()
                if not part_number:
                    continue
                master_keys.append((wb.id, part_number))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(wp_provider, master_keys)

            to_upsert = []
            for row in batch:
                wb = wp_brand_to_brand.get(row.get("part__brand_id"))
                if not wb:
                    continue
                part_number = (row.get("part__part_number") or "").strip()
                if not part_number:
                    continue
                provider_part = pp_by_key.get((wb.id, part_number))
                if not provider_part:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                msrp = row.get("msrp_usd")
                map_price = row.get("map_usd")
                cost = dealer_cost_from_msrp(msrp, row.get("part__feed_type"), company_creds)
                to_upsert.append(
                    src_models.ProviderPartCompanyPricing(
                        provider_part=provider_part,
                        company=company,
                        cost=cost,
                        jobber_price=map_price,
                        map_price=map_price,
                        msrp=msrp,
                        retail_price=msrp,
                        last_synced_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="WheelPros pricing company={} batch={}".format(company_id, batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} WheelPros pricing company={} batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "wheelpros_brand", _worker)
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


def _sync_turn14_provider_inventory_batches(
    *,
    provider_parts: typing.Dict[str, src_models.ProviderPart],
    location_map: typing.Dict[str, str],
    now: typing.Any,
    catalog_brand_ids: typing.Optional[typing.Set[int]],
    all_mapped_catalog_brand_ids: typing.Optional[typing.Set[int]],
    log_context: str,
) -> int:
    """
    Cursor-batched Turn14BrandInventory -> ProviderPartInventory upsert for one brand scope.

    If ``catalog_brand_ids`` is set, restrict to those Turn14 brand ids (parallel partition).
    Else if ``all_mapped_catalog_brand_ids`` is set, process rows with no brand or a catalog brand
    not in that set (legacy / unmapped inventory still keyed by ``external_id``).
    """
    total_upserted = 0
    batch_num = 0
    last_id = 0
    while True:
        batch_num += 1
        qs = src_models.Turn14BrandInventory.objects.filter(id__gt=last_id)
        if catalog_brand_ids is not None:
            if not catalog_brand_ids:
                break
            qs = qs.filter(brand_id__in=catalog_brand_ids)
        elif all_mapped_catalog_brand_ids is not None:
            qs = qs.filter(
                Q(brand__isnull=True) | ~Q(brand_id__in=all_mapped_catalog_brand_ids)
            )
        else:
            break
        batch = list(
            qs.order_by("id")
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
            ctx = "Turn14 inventory {} batch={}".format(log_context, batch_num)
            to_upsert = _dedupe_provider_part_inventory_for_upsert(to_upsert, context=ctx)
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

        logger.info("{} Turn14 inventory {} batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, log_context, batch_num, len(to_upsert), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_INVENTORY:
            time.sleep(BATCH_DELAY_SECONDS)
    return total_upserted


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

    mappings = list(
        src_models.BrandTurn14BrandMapping.objects.select_related("brand", "turn14_brand")
    )
    if not mappings:
        logger.info("{} No BrandTurn14BrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=turn14_provider)
    }

    # Load Turn14 locations once: map external_id (e.g. "01") -> name (e.g. "Hatfield")
    location_map = _get_turn14_location_map()

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        return _sync_turn14_provider_inventory_batches(
            provider_parts=provider_parts,
            location_map=location_map,
            now=now,
            catalog_brand_ids=catalog_ids,
            all_mapped_catalog_brand_ids=None,
            log_context="mapped brand",
        )

    mapped_catalog_brand_ids = {m.turn14_brand_id for m in mappings}
    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "turn14_brand", _worker)
    total_upserted += _sync_turn14_provider_inventory_batches(
        provider_parts=provider_parts,
        location_map=location_map,
        now=now,
        catalog_brand_ids=None,
        all_mapped_catalog_brand_ids=mapped_catalog_brand_ids,
        log_context="unmapped or null brand",
    )
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

    mappings = list(
        src_models.BrandKeystoneBrandMapping.objects.select_related("brand", "keystone_brand")
    )
    if not mappings:
        logger.info("{} No BrandKeystoneBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=keystone_provider)
    }

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.KeystoneParts.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
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
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="Keystone inventory batch={}".format(batch_num)
                )
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

            logger.info("{} Keystone inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "keystone_brand", _worker)
    logger.info("{} Synced {} Keystone inventory records total.".format(_LOG_PREFIX, total_upserted))


def _meyer_warehouse_availability(row: typing.Dict) -> typing.Dict:
    """Meyer inventory flags + quantities for ProviderPartInventory.warehouse_availability JSON."""
    inv = 0
    avail = row.get("available_qty")
    if avail is not None:
        try:
            inv = int(float(avail))
        except (TypeError, ValueError):
            inv = 0
    mfg_out = 0
    mfg_inv = row.get("mfg_qty_available")
    if mfg_inv is not None:
        try:
            mfg_out = int(mfg_inv)
        except (TypeError, ValueError):
            mfg_out = 0
    ltl_out = 0
    inv_ltl = row.get("inventory_ltl")
    if inv_ltl is not None:
        try:
            ltl_out = int(inv_ltl)
        except (TypeError, ValueError):
            ltl_out = 0
    return {
        "inventory": inv,
        "manufacturer_inventory": mfg_out,
        "ltl_inventory": ltl_out,
        "stocking": bool(row.get("is_stocking")),
        "special_order": bool(row.get("is_special_order")),
    }


def sync_provider_inventory_from_meyer() -> None:
    """Sync ProviderPartInventory from MeyerParts (Available + manufacturer qty)."""
    logger.info("{} Syncing provider inventory from Meyer.".format(_LOG_PREFIX))

    meyer_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    if not meyer_provider:
        logger.info("{} No Meyer provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandMeyerBrandMapping.objects.select_related("brand", "meyer_brand")
    )
    if not mappings:
        logger.info("{} No BrandMeyerBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        pp.provider_external_id: pp
        for pp in src_models.ProviderPart.objects.filter(provider=meyer_provider)
    }

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.MeyerParts.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
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
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="Meyer inventory batch={}".format(batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "meyer_brand", _worker)
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

    mappings = list(
        src_models.BrandTurn14BrandMapping.objects.select_related("brand", "turn14_brand")
    )
    t14_brand_to_brand = {m.turn14_brand_id: m.brand for m in mappings}
    mapped_t14_brand_ids = set(t14_brand_to_brand.keys())
    if not t14_brand_to_brand:
        logger.info("{} No BrandTurn14BrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.Turn14BrandPricing.objects.filter(
                    id__gt=last_id,
                    brand_id__in=catalog_ids,
                )
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
            ext_to_key = _turn14_pricing_batch_external_id_to_master_keys(
                batch, t14_brand_to_brand, mapped_t14_brand_ids
            )
            pp_by_key = _provider_parts_by_master_brand_and_part_number(turn14_provider, ext_to_key.values())

            to_upsert = []
            for pr in batch:
                eid = str(pr.get("external_id") or "").strip()
                key = ext_to_key.get(eid)
                if not key:
                    continue
                provider_part = pp_by_key.get(key)
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Turn14 pricing batch={}".format(batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "turn14_brand", _worker)
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

    mappings = list(
        src_models.BrandKeystoneBrandMapping.objects.select_related("brand", "keystone_brand")
    )
    ks_brand_to_brand = {m.keystone_brand_id: m.brand for m in mappings}
    if not ks_brand_to_brand:
        logger.info("{} No BrandKeystoneBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.KeystoneCompanyPricing.objects.filter(
                    id__gt=last_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "cost",
                    "jobber_price",
                    "part__brand_id",
                    "part__manufacturer_part_no",
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
            master_keys = []
            for row in batch:
                brand = ks_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__manufacturer_part_no") or ""
                if isinstance(pn, str):
                    pn = pn.strip()
                else:
                    pn = str(pn or "").strip()
                if not pn:
                    continue
                master_keys.append((brand.id, pn))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(keystone_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = ks_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__manufacturer_part_no") or ""
                if isinstance(pn, str):
                    pn = pn.strip()
                else:
                    pn = str(pn or "").strip()
                if not pn:
                    continue
                provider_part = pp_by_key.get((brand.id, pn))
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Keystone pricing batch={}".format(batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "keystone_brand", _worker)
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

    mappings = list(
        src_models.BrandMeyerBrandMapping.objects.select_related("brand", "meyer_brand")
    )
    my_brand_to_brand = {m.meyer_brand_id: m.brand for m in mappings}
    if not my_brand_to_brand:
        logger.info("{} No BrandMeyerBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.MeyerCompanyPricing.objects.filter(
                    id__gt=last_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "cost",
                    "jobber_price",
                    "map_price",
                    "part__brand_id",
                    "part__meyer_part",
                    "part__mfg_item_number",
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
            row_id_to_pp = _meyer_company_pricing_batch_row_id_to_provider_part(
                batch, meyer_provider, my_brand_to_brand
            )
            pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
            for row in batch:
                pp = row_id_to_pp.get(row["id"])
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
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Meyer pricing batch={}".format(batch_num)
                )
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
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "meyer_brand", _worker)
    logger.info("{} Synced {} Meyer pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_dlg() -> None:
    """Sync ProviderPartCompanyPricing from DlgCompanyPricing (per-company inventory SFTP pulls)."""
    logger.info("{} Syncing provider pricing from DLG.".format(_LOG_PREFIX))

    dlg_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.DLG.value,
    ).first()
    if not dlg_provider:
        logger.info("{} No DLG provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandDlgBrandMapping.objects.select_related("brand", "dlg_brand")
    )
    dlg_brand_to_brand = {m.dlg_brand_id: m.brand for m in mappings}
    if not dlg_brand_to_brand:
        logger.info("{} No BrandDlgBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.DlgCompanyPricing.objects.filter(
                    id__gt=last_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "base_price",
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
            row_id_to_pp = _dlg_company_pricing_batch_row_id_to_provider_part(
                batch, dlg_provider, dlg_brand_to_brand
            )
            pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
            for row in batch:
                pp = row_id_to_pp.get(row["id"])
                if not pp:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                bp = _dlg_decimal_to_provider_money(row.get("base_price"))
                pricing_by_pp_company[(pp.id, company.id)] = src_models.ProviderPartCompanyPricing(
                    provider_part=pp,
                    company=company,
                    cost=bp,
                    jobber_price=bp,
                    map_price=None,
                    msrp=None,
                    retail_price=bp,
                    last_synced_at=now,
                )

            to_upsert = list(pricing_by_pp_company.values())
            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="DLG pricing batch={}".format(batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} DLG pricing batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "dlg_brand", _worker)
    logger.info("{} Synced {} DLG pricing records total.".format(_LOG_PREFIX, total_upserted))


def _maybe_reindex_meilisearch_after_master_parts(
    *,
    reindex_meilisearch: bool,
    provider_label: str,
    continuation: typing.Callable[[], None],
) -> None:
    """
    After ``sync_master_parts_from_*``, optionally run a full Meilisearch reindex (delete + index)
    overlapped with inventory/pricing DB work on this thread. No-op when Meilisearch is not configured.
    """
    if not reindex_meilisearch:
        continuation()
        return

    from src.search import meilisearch_client as meilisearch_client

    if not meilisearch_client.is_configured():
        continuation()
        return

    def _reindex() -> typing.Tuple[int, int]:
        return meilisearch_client.reindex_all_master_parts()

    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(_reindex)
        try:
            continuation()
        finally:
            ok, fail = fut.result()

    logger.info(
        "{} Meilisearch reindex after {} master parts: indexed={} failed={}".format(
            _LOG_PREFIX, provider_label, ok, fail
        )
    )


def sync_derived_from_turn14(*, reindex_meilisearch: bool = False) -> None:
    """
    Propagate Turn14 source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after Turn14 item/catalog fetches so the unified
    layer stays aligned without waiting for the global ``sync_all_master_parts`` job.
    """
    logger.info("{} Starting Turn14-only derived sync (parts, inventory, pricing).".format(_LOG_PREFIX))
    sync_master_parts_from_turn14()
    connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_turn14()
        connection.close()
        sync_provider_pricing_from_turn14()
        connection.close()

    _maybe_reindex_meilisearch_after_master_parts(
        reindex_meilisearch=reindex_meilisearch,
        provider_label="Turn14",
        continuation=_cont,
    )
    logger.info("{} Completed Turn14-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_keystone(*, reindex_meilisearch: bool = False) -> None:
    """
    Propagate Keystone source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after Keystone inventory/CSV fetches.

    Set ``reindex_meilisearch=True`` from ingest commands to refresh the parts search index
    after master parts sync (skipped when Meilisearch is not configured).
    """
    logger.info("{} Starting Keystone-only derived sync (parts, inventory, pricing).".format(_LOG_PREFIX))
    sync_master_parts_from_keystone()
    connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_keystone()
        connection.close()
        sync_provider_pricing_from_keystone()
        connection.close()

    _maybe_reindex_meilisearch_after_master_parts(
        reindex_meilisearch=reindex_meilisearch,
        provider_label="Keystone",
        continuation=_cont,
    )
    logger.info("{} Completed Keystone-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_rough_country(*, reindex_meilisearch: bool = False) -> None:
    """
    Propagate Rough Country source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after Rough Country feed ingest.
    """
    logger.info("{} Starting Rough Country-only derived sync (parts, inventory, pricing).".format(_LOG_PREFIX))
    sync_master_parts_from_rough_country()
    connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_rough_country()
        connection.close()
        sync_provider_pricing_from_rough_country()
        connection.close()

    _maybe_reindex_meilisearch_after_master_parts(
        reindex_meilisearch=reindex_meilisearch,
        provider_label="Rough Country",
        continuation=_cont,
    )
    logger.info("{} Completed Rough Country-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_wheelpros(*, reindex_meilisearch: bool = False) -> None:
    """
    Propagate WheelPros source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after WheelPros CSV ingest.
    """
    logger.info("{} Starting WheelPros-only derived sync (parts, inventory, pricing).".format(_LOG_PREFIX))
    sync_master_parts_from_wheelpros()
    connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_wheelpros()
        connection.close()
        sync_provider_pricing_from_wheelpros()
        connection.close()

    _maybe_reindex_meilisearch_after_master_parts(
        reindex_meilisearch=reindex_meilisearch,
        provider_label="WheelPros",
        continuation=_cont,
    )
    logger.info("{} Completed WheelPros-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_meyer(*, reindex_meilisearch: bool = False) -> None:
    """
    Propagate Meyer source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after Meyer catalog / inventory ingest.
    """
    logger.info("{} Starting Meyer-only derived sync (parts, inventory, pricing).".format(_LOG_PREFIX))
    sync_master_parts_from_meyer()
    connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_meyer()
        connection.close()
        sync_provider_pricing_from_meyer()
        connection.close()

    _maybe_reindex_meilisearch_after_master_parts(
        reindex_meilisearch=reindex_meilisearch,
        provider_label="Meyer",
        continuation=_cont,
    )
    logger.info("{} Completed Meyer-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_atech(*, reindex_meilisearch: bool = False) -> None:
    """
    Propagate A-Tech source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after A-Tech feed ingest.
    """
    logger.info("{} Starting A-Tech-only derived sync (parts, inventory, pricing).".format(_LOG_PREFIX))
    sync_master_parts_from_atech()
    connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_atech()
        connection.close()
        sync_provider_pricing_from_atech()
        connection.close()

    _maybe_reindex_meilisearch_after_master_parts(
        reindex_meilisearch=reindex_meilisearch,
        provider_label="A-Tech",
        continuation=_cont,
    )
    logger.info("{} Completed A-Tech-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_dlg(*, reindex_meilisearch: bool = False) -> None:
    """
    Propagate DLG source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after DLG catalog ingest.
    """
    logger.info("{} Starting DLG-only derived sync (parts, inventory, pricing).".format(_LOG_PREFIX))
    sync_master_parts_from_dlg()
    connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_dlg()
        connection.close()
        sync_provider_pricing_from_dlg()
        connection.close()

    _maybe_reindex_meilisearch_after_master_parts(
        reindex_meilisearch=reindex_meilisearch,
        provider_label="DLG",
        continuation=_cont,
    )
    logger.info("{} Completed DLG-only derived sync.".format(_LOG_PREFIX))


def sync_all_master_parts() -> None:
    """
    Run each distributor's derived sync in order (master + provider parts, then inventory, then pricing
    for that distributor before moving to the next). Order matches the previous implementation so
    dependencies (e.g. Meyer before A-Tech) are preserved.

    Does not reindex Meilisearch per provider; use ``sync_master_parts --reindex-meilisearch`` or
    individual ingest commands that pass ``reindex_meilisearch=True``.
    """
    logger.info("{} Starting full master parts sync.".format(_LOG_PREFIX))

    sync_derived_from_turn14(reindex_meilisearch=False)
    sync_derived_from_keystone(reindex_meilisearch=False)
    sync_derived_from_meyer(reindex_meilisearch=False)
    sync_derived_from_atech(reindex_meilisearch=False)
    sync_derived_from_rough_country(reindex_meilisearch=False)
    sync_derived_from_dlg(reindex_meilisearch=False)
    sync_derived_from_wheelpros(reindex_meilisearch=False)

    logger.info("{} Completed full master parts sync.".format(_LOG_PREFIX))
