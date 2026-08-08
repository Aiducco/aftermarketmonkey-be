"""
Sync MasterPart, ProviderPart, ProviderPartInventory, and ProviderPartCompanyPricing
from Turn14, Keystone, Meyer, A-Tech, Rough Country, WheelPros, DLG, Premier (APG Wholesale),
and Vossen provider data.
"""
import ctypes
import gc
import logging
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from datetime import date

from django.db import close_old_connections, connection
from django.db.models import Max, Min, Q
from django.utils import timezone

import pgbulk

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.services import wps as wps_services
from src.integrations.utils import master_part_matching as mp_matching
from src.integrations.services.vossen import dealer_cost_from_price as vossen_dealer_cost_from_price
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


def _get_brand_for_vossen_brand(
    vossen_brand: src_models.VossenBrand,
) -> typing.Optional[src_models.Brands]:
    mapping = src_models.BrandVossenBrandMapping.objects.filter(
        vossen_brand=vossen_brand,
    ).first()
    return mapping.brand if mapping else None


BATCH_SIZE_MASTER_PARTS = 5000
BATCH_SIZE_MASTER_PARTS_WHEELPROS = 3000
# Max tuples per IN clause to avoid PostgreSQL stack depth limit (StatementTooComplex)
WHEELPROS_LOOKUP_CHUNK = 200
BATCH_SIZE_INVENTORY = 10000
BATCH_SIZE_PRICING = 10000
BATCH_DELAY_SECONDS = 0.1  # Reduced from 0.3 - was adding ~30s per 100 batches
# Turn14 VCdb fitment sync batches by item (not by raw fitment row) so every vehicle_id for a
# given item is seen together before years are collapsed into ranges.
FITMENT_ITEM_CHUNK_SIZE = 2000

# Partition ``sync_master_parts_from_*`` by internal ``Brands.id`` so workers never compete on the same
# ``(brand_id, part_number)`` upsert. Tune down if PostgreSQL connection pool is tight.
MASTER_PARTS_SYNC_MAX_WORKERS = 16


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
    ("1009", "San Antonio", "TX"),
    ("1086", "Grand Prairie", "TX"),
    ("1088", "Atlanta", "GA"),
    ("1002", "Dallas", "TX"),
    ("1003", "Houston", "TX"),
    ("1005", "New Orleans", "LA"),
    ("1007", "Oklahoma City", "OK"),
    ("1085", "Buena Park", "CA"),
    ("1001", "Denver", "CO"),
    ("1004", "Kansas City", "MO"),
    ("1006", "Phoenix", "AZ"),
    ("1008", "Sacramento", "CA"),
    ("1011", "Los Angeles", "CA"),
    ("1013", "Seattle", "WA"),
    ("1014", "Atlanta", "GA"),
    ("1015", "Chicago", "IL"),
    ("1016", "Orlando", "FL"),
    ("1018", "Miami", "FL"),
    ("1019", "Cleveland", "OH"),
    ("1020", "Cincinnati", "OH"),
    ("1021", "Charlotte", "NC"),
    ("1022", "Cranbury", "NJ"),
    ("1024", "Nashville", "TN"),
    ("1025", "Salt Lake", "UT"),
    ("1026", "Hartford", "CT"),
    ("1027", "Buena Park", "CA"),
    ("1028", "Minneapolis", "MN"),
    ("1030", "Richmond", "VA"),
    ("1031", "Riverside", "CA"),
    ("1032", "Portland", "OR"),
    ("1034", "Baltimore", "MD"),
    ("1036", "Detroit", "MI"),
    ("1041", "Honolulu", "HI"),
    ("1042", "New York", "NY"),
    ("1053", "Corona", "CA"),
    ("1054", "York", "SC"),
    ("1055", "Ogden", "UT"),
    ("1057", "Salt Lake City", "UT"),
    ("1060", "Arlington", "TX"),
    ("1072", "Ogden", "UT"),
    ("1421", "Greensboro", "NC"),
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


def _collapse_years_to_ranges(years: typing.Iterable[int]) -> typing.List[typing.Tuple[int, int]]:
    """
    Collapse a set of individual years into the minimal list of contiguous
    ``(year_start, year_end)`` ranges, e.g. {1998, 1999, 2000, 2005} -> [(1998, 2000), (2005, 2005)].
    """
    sorted_years = sorted(set(years))
    if not sorted_years:
        return []
    ranges = []
    range_start = prev = sorted_years[0]
    for y in sorted_years[1:]:
        if y == prev + 1:
            prev = y
            continue
        ranges.append((range_start, prev))
        range_start = prev = y
    ranges.append((range_start, prev))
    return ranges


def _dedupe_master_part_fitments_for_upsert(
    rows: typing.List[src_models.MasterPartFitment],
    context: str = "",
) -> typing.List[src_models.MasterPartFitment]:
    """
    One row per (master_part_id, year_start, year_end, make, model, submodel, engine, drive_type);
    last wins. PostgreSQL rejects INSERT .. ON CONFLICT when the same conflict key appears twice
    in one batch.
    """
    by_key: typing.Dict[typing.Tuple, src_models.MasterPartFitment] = {}
    merged_dup_rows = 0
    for r in rows:
        k = (r.master_part_id, r.year_start, r.year_end, r.make, r.model, r.submodel, r.engine, r.drive_type)
        if k in by_key:
            merged_dup_rows += 1
        by_key[k] = r
    out = list(by_key.values())
    if merged_dup_rows:
        ctx = " {}".format(context) if context else ""
        logger.info(
            "{} Upsert dedupe MasterPartFitment{}: input_rows={} unique_keys={} merged_duplicate_rows={}".format(
                _LOG_PREFIX, ctx, len(rows), len(out), merged_dup_rows
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
MASTER_PART_FULL_UPDATE_FIELDS = ["sku", "description", "aaia_code", "image_url", "gtin", "updated_at"]
MASTER_PART_PARTIAL_UPDATE_FIELDS = ["sku", "aaia_code"]  # Non-primary providers


def _collapse_doubled_sku_prefix(sku, part_number):
    """
    Turn14's own internal SKU field sometimes has the brand/vendor prefix baked in twice
    (e.g. sku="FPEFPE-HSC-4-S" for mfr_part_number="FPE-HSC-4-S") -- a data defect on
    Turn14's side, not something we generate. Left as-is, that garbled sku can never match
    another provider's correctly single-prefixed sku/part_number for the same real part,
    so ingest creates a duplicate MasterPart instead of finding the existing Turn14 row.
    If sku is exactly "<prefix><part_number>" where part_number itself already starts with
    that same prefix, collapse sku down to part_number.
    """
    if not sku or not part_number or sku == part_number:
        return sku
    if not sku.upper().endswith(part_number.upper()):
        return sku
    prefix = sku[: len(sku) - len(part_number)]
    if prefix and part_number.upper().startswith(prefix.upper()):
        return part_number
    return sku


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
                "barcode",
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
            sku = _collapse_doubled_sku_prefix(sku, part_number)
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
                        gtin=row.get("barcode") or None,
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
                "upc_code",
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
                        gtin=row.get("upc_code") or None,
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

        # Keystone spells manufacturer part numbers differently from the other feeds
        # ('MS 96587' vs 'MS96587'), so anything the exact match missed gets a second,
        # heavily-guarded pass on the punctuation/case-insensitive form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=keystone_provider.id,
            provider_kind=src_enums.BrandProviderKind.KEYSTONE.value,
            external_id_by_key={v: k for k, v in vcpn_to_brand_part.items()},
        )

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
            values = [(existing_by_key[k], key_to_mp[k].aaia_code, key_to_mp[k].gtin) for k in existing_keys]
            placeholders = ", ".join(["(%s::bigint, %s, %s)"] * len(values))
            params = [x for t in values for x in t]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET
                        aaia_code = v.aaia_code,
                        gtin = COALESCE(mp.gtin, v.gtin)
                    FROM (VALUES {}) AS v(id, aaia_code, gtin)
                    WHERE mp.id = v.id::bigint
                    """.format(placeholders),
                    params,
                )

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart under
        # the feed's own spelling, so re-querying by exact part_number below would not find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
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


def _parse_keystone_kit_components(kit_components_raw: typing.Optional[str]) -> typing.List[typing.Tuple[str, int]]:
    """
    Decodes Keystone's pipe-delimited KitComponents field (e.g.
    "B94GNRC823-1|B94GNRM1123-1|") into [(component_vcpn, quantity), ...]. Splitting on the
    LAST "-" in each token is safe and unambiguous: Keystone's own VCPN format is validated
    server-side against the regex "^[A-Z][A-Z0-9]{3,12}$" (confirmed in the SDK docs' error
    section for full part numbers), which never allows a hyphen, so the only hyphen in
    "VCPN-QTY" is the one separating them.
    """
    if not kit_components_raw:
        return []
    pairs: typing.List[typing.Tuple[str, int]] = []
    for token in kit_components_raw.split("|"):
        token = token.strip()
        if not token:
            continue
        vcpn, sep, qty_str = token.rpartition("-")
        if not sep or not vcpn:
            continue
        try:
            quantity = int(qty_str)
        except ValueError:
            continue
        if quantity <= 0:
            continue
        pairs.append((vcpn, quantity))
    return pairs


def sync_keystone_kit_components() -> None:
    """
    Decodes KeystoneParts.kit_components (see _parse_keystone_kit_components) into real
    ProviderPartKitComponent rows against the same provider's own catalog, and sets
    ProviderPart.is_kit for each kit's own row. Run after sync_master_parts_from_keystone --
    needs every Keystone ProviderPart (kit rows AND component rows) to already exist. Can't be
    scoped to one brand's ingestion batch: a kit's components aren't guaranteed to belong to the
    same KAO vendor code/brand as the kit itself (confirmed live: kit "B94GNRK1123" and its
    components "B94GNRC823"/"B94GNRM1123" happen to share vendor code B94 here, but nothing in
    the feed guarantees that in general).

    See ProviderPartKitComponent's own docstring for why this exists: expanding a kit into its
    components ourselves, at add-to-cart time (see purchase_orders_services.add_cart_item), is
    what lets purchase order quote/submit avoid ever sending a kit's own VCPN to Keystone's
    order API -- confirmed live that neither call handles one cleanly (see
    src.integrations.orders.keystone's module docstring).
    """
    logger.info("{} Syncing Keystone kit components.".format(_LOG_PREFIX))

    keystone_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.KEYSTONE.value,
    ).first()
    if not keystone_provider:
        logger.info("{} No Keystone provider found.".format(_LOG_PREFIX))
        return

    vcpn_to_provider_part_id = dict(
        src_models.ProviderPart.objects.filter(provider=keystone_provider).values_list(
            "provider_external_id", "id"
        )
    )

    kit_provider_part_ids: typing.Set[int] = set()
    components: typing.List[src_models.ProviderPartKitComponent] = []
    kits_seen = 0
    kits_missing_own_part = 0
    components_unresolved = 0

    kit_rows = (
        src_models.KeystoneParts.objects.filter(is_kit=True)
        .exclude(kit_components__isnull=True)
        .exclude(kit_components="")
        .values("vcpn", "kit_components")
        .iterator(chunk_size=2000)
    )
    for row in kit_rows:
        kits_seen += 1
        kit_part_id = vcpn_to_provider_part_id.get(row["vcpn"])
        if kit_part_id is None:
            kits_missing_own_part += 1
            continue
        kit_provider_part_ids.add(kit_part_id)
        for component_vcpn, quantity in _parse_keystone_kit_components(row["kit_components"]):
            component_part_id = vcpn_to_provider_part_id.get(component_vcpn)
            if component_part_id is None:
                components_unresolved += 1
                continue
            components.append(
                src_models.ProviderPartKitComponent(
                    kit_part_id=kit_part_id, component_part_id=component_part_id, quantity=quantity
                )
            )

    if components:
        pgbulk.upsert(
            src_models.ProviderPartKitComponent,
            components,
            unique_fields=["kit_part", "component_part"],
            update_fields=["quantity"],
        )
    if kit_provider_part_ids:
        # Marked True regardless of whether any component resolved (see ProviderPartKitComponent's
        # docstring) -- a kit whose components haven't synced yet is still a kit.
        src_models.ProviderPart.objects.filter(id__in=kit_provider_part_ids).update(is_kit=True)

    logger.info(
        "{} Kit components: {} kit rows seen, {} linked components, {} kits with no matching "
        "ProviderPart, {} component VCPNs unresolved.".format(
            _LOG_PREFIX, kits_seen, len(components), kits_missing_own_part, components_unresolved
        )
    )


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
                "upc",
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
                        gtin=row.get("upc") or None,
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

        # Neither the sku bridge nor the exact part number placed these, so try the
        # punctuation/case-insensitive form (Meyer's '211172-1X' is A-Tech's '2111721X').
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts_list},
            provider_id=meyer_provider.id,
            provider_kind=src_enums.BrandProviderKind.MEYER.value,
            external_id_by_key={v: k for k, v in meyer_part_to_brand_part.items()},
        )

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
                    (existing_by_key[k], key_to_mp[k].aaia_code, key_to_mp[k].gtin)
                    for k in chunk_keys
                ]
                placeholders = ", ".join(["(%s::bigint, %s, %s)"] * len(values))
                params = [x for row_vals in values for x in row_vals]
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE master_parts mp SET
                            aaia_code = v.aaia_code,
                            gtin = COALESCE(mp.gtin, v.gtin)
                        FROM (VALUES {}) AS v(id, aaia_code, gtin)
                        WHERE mp.id = v.id::bigint
                        """.format(placeholders),
                        params,
                    )

        brand_part_to_master = {}
        for (b_id, p_num), mp_id in existing_by_key.items():
            if mp_id is None:
                continue
            if mp_id in id_to_mp:
                brand_part_to_master[(b_id, p_num)] = id_to_mp[mp_id]
            else:
                # Resolved on the normalized part number, so neither the sku nor the exact
                # lookup above preloaded a stub for this id.
                mp = src_models.MasterPart()
                mp.id = mp_id
                mp.brand_id = b_id
                brand_part_to_master[(b_id, p_num)] = mp
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
                "gtin",
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
                        gtin=row.get("gtin") or None,
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

        # A-Tech often strips punctuation the manufacturer publishes (its '2111721X' is Meyer's
        # '211172-1X'), so anything the exact match missed gets a second, heavily-guarded pass
        # on the normalized form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts_list},
            provider_id=atech_provider.id,
            provider_kind=src_enums.BrandProviderKind.ATECH.value,
            external_id_by_key={
                bp: _atech_provider_external_id(feed_brand_id, feed_pn)
                for (feed_brand_id, feed_pn), bp in atech_feed_brand_pn_to_brand_part.items()
            },
        )

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
                (existing_by_key[k], key_to_mp[k].aaia_code, key_to_mp[k].gtin)
                for k in existing_keys
            ]
            placeholders = ", ".join(["(%s::bigint, %s, %s)"] * len(values))
            params = [x for row_vals in values for x in row_vals]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET
                        aaia_code = v.aaia_code,
                        gtin = COALESCE(mp.gtin, v.gtin)
                    FROM (VALUES {}) AS v(id, aaia_code, gtin)
                    WHERE mp.id = v.id::bigint
                    """.format(placeholders),
                    params,
                )

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart
        # under the feed's own spelling, so the exact-part_number query below cannot find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
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
                "upc",
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
                        gtin=row.get("upc") or None,
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

        # Rough Country spells manufacturer part numbers in its own house style, so anything the exact
        # match missed gets a second, heavily-guarded pass on the normalized form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=rc_provider.id,
            provider_kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
            external_id_by_key={v: k for k, v in rc_external_id_to_brand_part.items()},
        )

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
                (existing_by_key[k], key_to_mp[k].aaia_code, key_to_mp[k].gtin)
                for k in existing_keys
            ]
            placeholders = ", ".join(["(%s::bigint, %s, %s)"] * len(values))
            params = [x for row in values for x in row]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET
                        aaia_code = v.aaia_code,
                        gtin = COALESCE(mp.gtin, v.gtin)
                    FROM (VALUES {}) AS v(id, aaia_code, gtin)
                    WHERE mp.id = v.id::bigint
                    """.format(placeholders),
                    params,
                )

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart
        # under the feed's own spelling, so the exact-part_number query below cannot find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
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

        # DLG spells manufacturer part numbers in its own house style, so anything the exact
        # match missed gets a second, heavily-guarded pass on the normalized form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=dlg_provider.id,
            provider_kind=src_enums.BrandProviderKind.DLG.value,
            external_id_by_key={v: k for k, v in dlg_external_to_brand_part.items()},
        )

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
                (existing_by_key[k], key_to_mp[k].aaia_code)
                for k in existing_keys
            ]
            placeholders = ", ".join(["(%s::bigint, %s)"] * len(values))
            params = [x for row_vals in values for x in row_vals]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET aaia_code = v.aaia_code
                    FROM (VALUES {}) AS v(id, aaia_code)
                    WHERE mp.id = v.id
                    """.format(placeholders),
                    params,
                )

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart
        # under the feed's own spelling, so the exact-part_number query below cannot find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
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


def _rough_country_product_details(row: typing.Dict) -> typing.List[typing.Dict]:
    """
    Build the product_details list for a RoughCountryPart row.
    - Discontinued / Discontinued Date: None unless SKU is on the Discontinued tab.
    - Replacement Part #: None if blank.
    """
    is_disc = bool(row.get("is_discontinued"))
    disc_date = row.get("discontinued_date")
    disc_date_str = disc_date.isoformat() if disc_date and hasattr(disc_date, "isoformat") else (str(disc_date) if disc_date else None)
    repl = (row.get("replacement_sku") or "").strip() or None

    return [
        {"key": "sku",               "label": "SKU",                 "value": row.get("sku") or None},
        {"key": "brand",             "label": "Brand",               "value": row.get("manufacturer") or None},
        {"key": "description",       "label": "Description",         "value": row.get("title") or None},
        {"key": "link",              "label": "Product Page URL",    "value": row.get("link") or None},
        {"key": "is_discontinued",   "label": "Discontinued",        "value": "Yes" if is_disc else None},
        {"key": "discontinued_date", "label": "Discontinued Date",   "value": disc_date_str if is_disc else None},
        {"key": "replacement_sku",   "label": "Replacement Part #",  "value": repl},
    ]


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
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=rc_provider).values("id", "provider_external_id")
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
                .values(
                    "id", "brand_id", "sku", "nv_stock", "tn_stock",
                    "link", "is_discontinued", "discontinued_date", "replacement_sku",
                    "manufacturer", "title",
                )[:BATCH_SIZE_INVENTORY]
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

            # Update product_details and is_discontinued on ProviderPart
            pp_details_to_update = []
            for row in batch:
                ext_id = _rough_country_provider_external_id(row["brand_id"], row["sku"])
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = _rough_country_product_details(row)
                    pp.is_discontinued = bool(row.get("is_discontinued"))
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details", "is_discontinued"], batch_size=500
                )

            logger.info("{} Rough Country inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "rough_country_brand", _worker)
    logger.info("{} Synced {} Rough Country inventory records total.".format(_LOG_PREFIX, total_upserted))


def sync_master_part_fitments_from_rough_country() -> None:
    """
    Sync MasterPartFitment from RoughCountryFitment (Vehicle Fitment tab feed), joined via
    ProviderPart.provider_external_id - same rc_brand_id+sku composite key used by inventory and
    pricing above. Same cursor-batched, parallel-by-brand-partition pattern as
    sync_provider_inventory_from_rough_country.

    RoughCountryFitment has no engine field, so MasterPartFitment.engine is always left at its
    default ("") for rows synced from this source.
    """
    logger.info("{} Syncing master part fitments from Rough Country.".format(_LOG_PREFIX))

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

    master_part_id_by_ext_id = {
        row["provider_external_id"]: row["master_part_id"]
        for row in src_models.ProviderPart.objects.filter(provider=rc_provider).values(
            "provider_external_id", "master_part_id"
        )
    }

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            batch = list(
                src_models.RoughCountryFitment.objects.filter(id__gt=last_id, part__brand_id__in=catalog_ids)
                .order_by("id")
                .values(
                    "id", "part__brand_id", "part__sku",
                    "start_year", "end_year", "make", "model", "submodel", "drive",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            to_upsert = []
            for row in batch:
                ext_id = _rough_country_provider_external_id(row["part__brand_id"], row["part__sku"])
                master_part_id = master_part_id_by_ext_id.get(ext_id)
                if not master_part_id:
                    continue
                if row["start_year"] is None or row["end_year"] is None:
                    continue
                make = (row["make"] or "").strip()
                model = (row["model"] or "").strip()
                if not make or not model:
                    continue
                to_upsert.append(
                    src_models.MasterPartFitment(
                        master_part_id=master_part_id,
                        year_start=row["start_year"],
                        year_end=row["end_year"],
                        make=make,
                        model=model,
                        submodel=(row["submodel"] or "").strip(),
                        drive_type=(row["drive"] or "").strip(),
                        source_provider=rc_provider,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_master_part_fitments_for_upsert(
                    to_upsert, context="Rough Country fitment batch={}".format(batch_num)
                )
                pgbulk.upsert(
                    src_models.MasterPartFitment,
                    to_upsert,
                    unique_fields=[
                        "master_part", "year_start", "year_end", "make", "model",
                        "submodel", "engine", "drive_type",
                    ],
                    update_fields=["source_provider"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} Rough Country fitment batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "rough_country_brand", _worker)
    logger.info("{} Synced {} Rough Country master part fitment records total.".format(_LOG_PREFIX, total_upserted))


def sync_master_part_fitments_from_turn14_vcdb() -> None:
    """
    Sync MasterPartFitment from Turn14ItemFitment, decoding each row's Turn14 vehicle_id via
    VcdbVehicle (year/make/model/submodel/engine/drive_type) and joining to MasterPart directly
    via (catalog Brand, part_number) — part_number on MasterPart is Turn14Items.mfr_part_number
    (see _ingest_turn14_items_for_mapped_brands), not routed through ProviderPart.

    On-demand only: not wired into any scheduled pipeline (never called from
    sync_master_parts_from_turn14, sync_master_parts, or ingest_all_providers). Requires
    sync_master_parts_from_turn14 (for MasterPart + BrandTurn14BrandMapping) and
    import_vcdb_vehicles (for the VcdbVehicle lookup) to have already run.

    Parallelized across disjoint Turn14-brand partitions using the same
    _partition_mapped_brands_for_parallel_ingest / _run_parallel_mapped_brand_int_worker split
    sync_master_parts_from_turn14 itself uses, so concurrent workers never race on the same
    MasterPartFitment (master_part, year_start, year_end, make, model, submodel, engine,
    drive_type) upsert key.

    Turn14 gives one discrete VCdb vehicle per row, not a year range. Batches are built per item
    (every vehicle_id for an item is loaded together) so all years for a given (master_part, make,
    model, submodel, engine, drive_type) combination are seen before being collapsed into the
    minimal set of contiguous (year_start, year_end) ranges via _collapse_years_to_ranges -
    batching by raw fitment row instead would risk splitting one item's years across batches and
    under-collapsing.
    """
    logger.info("{} Syncing master part fitments from Turn14 (via VCdb).".format(_LOG_PREFIX))

    turn14_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TURN_14.value,
    ).first()
    if not turn14_provider:
        logger.info("{} No Turn14 provider found.".format(_LOG_PREFIX))
        return

    mappings = list(src_models.BrandTurn14BrandMapping.objects.select_related("brand", "turn14_brand"))
    if not mappings:
        logger.info("{} No BrandTurn14BrandMapping found.".format(_LOG_PREFIX))
        return
    t14_brand_id_to_catalog_brand_id = {m.turn14_brand_id: m.brand_id for m in mappings}

    logger.info("{} Loading MasterPart (brand, part_number) lookup...".format(_LOG_PREFIX))
    master_part_id_by_brand_partnum = {
        (row["brand_id"], row["part_number"]): row["id"]
        for row in src_models.MasterPart.objects.values("id", "brand_id", "part_number")
    }
    logger.info("{} Loaded {} MasterPart rows.".format(_LOG_PREFIX, len(master_part_id_by_brand_partnum)))

    logger.info("{} Resolving Turn14Items -> MasterPart lookup...".format(_LOG_PREFIX))
    item_ext_id_to_master_part_id: typing.Dict[str, int] = {}
    for row in src_models.Turn14Items.objects.values("external_id", "mfr_part_number", "brand_id"):
        pn = (row["mfr_part_number"] or "").strip()
        if not pn:
            continue
        catalog_brand_id = t14_brand_id_to_catalog_brand_id.get(row["brand_id"])
        if not catalog_brand_id:
            continue
        master_part_id = master_part_id_by_brand_partnum.get((catalog_brand_id, pn))
        if not master_part_id:
            continue
        item_ext_id_to_master_part_id[row["external_id"]] = master_part_id
    logger.info(
        "{} Resolved {} Turn14Items to a MasterPart.".format(_LOG_PREFIX, len(item_ext_id_to_master_part_id))
    )

    vcdb_by_vehicle_id = {
        row["vehicle_id"]: row
        for row in src_models.VcdbVehicle.objects.all().values(
            "vehicle_id", "year", "make", "model", "submodel", "engine", "drive_type"
        )
    }
    if not vcdb_by_vehicle_id:
        logger.info("{} VcdbVehicle table is empty. Run import_vcdb_vehicles first.".format(_LOG_PREFIX))
        return
    logger.info("{} Loaded {} VcdbVehicle rows.".format(_LOG_PREFIX, len(vcdb_by_vehicle_id)))

    total_fitment_rows = src_models.Turn14ItemFitment.objects.count()
    num_partitions = min(MASTER_PARTS_SYNC_MAX_WORKERS, len({m.brand_id for m in mappings}))
    logger.info(
        "{} Starting parallel sync over {} Turn14ItemFitment rows across up to {} worker partition(s).".format(
            _LOG_PREFIX, total_fitment_rows, num_partitions
        )
    )

    def _worker(turn14_brand_ids: typing.Set[int]) -> int:
        if not turn14_brand_ids:
            return 0
        worker_label = "brands={}".format(sorted(turn14_brand_ids)[:5])
        total_upserted = 0
        skipped_no_master_part = 0
        skipped_no_vcdb = 0

        item_external_ids = list(
            src_models.Turn14Items.objects.filter(brand_id__in=turn14_brand_ids)
            .values_list("external_id", flat=True)
        )
        logger.info(
            "{} [{}] {} Turn14Items to process.".format(_LOG_PREFIX, worker_label, len(item_external_ids))
        )

        batch_num = 0
        for chunk_start in range(0, len(item_external_ids), FITMENT_ITEM_CHUNK_SIZE):
            batch_num += 1
            item_chunk = item_external_ids[chunk_start : chunk_start + FITMENT_ITEM_CHUNK_SIZE]
            fitment_rows = list(
                src_models.Turn14ItemFitment.objects.filter(item_external_id__in=item_chunk).values(
                    "item_external_id", "vehicle_id"
                )
            )

            years_by_key: typing.Dict[typing.Tuple, typing.Set[int]] = {}
            for row in fitment_rows:
                master_part_id = item_ext_id_to_master_part_id.get(row["item_external_id"])
                if not master_part_id:
                    skipped_no_master_part += 1
                    continue
                vcdb = vcdb_by_vehicle_id.get(row["vehicle_id"])
                if not vcdb:
                    skipped_no_vcdb += 1
                    continue
                key = (
                    master_part_id,
                    vcdb["make"],
                    vcdb["model"],
                    vcdb["submodel"] or "",
                    vcdb["engine"] or "",
                    vcdb["drive_type"] or "",
                )
                years_by_key.setdefault(key, set()).add(vcdb["year"])

            to_upsert = []
            for (master_part_id, make, model, submodel, engine, drive_type), years in years_by_key.items():
                for year_start, year_end in _collapse_years_to_ranges(years):
                    to_upsert.append(
                        src_models.MasterPartFitment(
                            master_part_id=master_part_id,
                            year_start=year_start,
                            year_end=year_end,
                            make=make,
                            model=model,
                            submodel=submodel,
                            engine=engine,
                            drive_type=drive_type,
                            source_provider=turn14_provider,
                        )
                    )

            if to_upsert:
                to_upsert = _dedupe_master_part_fitments_for_upsert(
                    to_upsert, context="Turn14 VCdb fitment [{}] batch={}".format(worker_label, batch_num)
                )
                pgbulk.upsert(
                    src_models.MasterPartFitment,
                    to_upsert,
                    unique_fields=[
                        "master_part", "year_start", "year_end", "make", "model",
                        "submodel", "engine", "drive_type",
                    ],
                    update_fields=["source_provider"],
                )
                total_upserted += len(to_upsert)

            logger.info(
                "{} [{}] batch {}: {} items, {} fitment rows -> {} ranges upserted, running_total={} "
                "(skipped: no_master_part={} no_vcdb={})".format(
                    _LOG_PREFIX, worker_label, batch_num, len(item_chunk), len(fitment_rows), len(to_upsert),
                    total_upserted, skipped_no_master_part, skipped_no_vcdb,
                )
            )
            connection.close()
            if len(item_chunk) == FITMENT_ITEM_CHUNK_SIZE:
                time.sleep(BATCH_DELAY_SECONDS)

        logger.info(
            "{} [{}] Done: {} upserted total (skipped: no_master_part={} no_vcdb={}).".format(
                _LOG_PREFIX, worker_label, total_upserted, skipped_no_master_part, skipped_no_vcdb,
            )
        )
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "turn14_brand", _worker)
    logger.info("{} Synced {} Turn14 master part fitment records via VCdb total.".format(_LOG_PREFIX, total_upserted))


FITMENT_DELETE_BATCH_SIZE = 20000


def _delete_fitments_for_provider_in_id_range(
    provider: src_models.Providers,
    id_range: typing.Optional[typing.Tuple[int, int]],
    batch_size: int,
    label: str,
) -> int:
    """Batched delete loop scoped to one ``id`` slice (or the whole table when ``id_range`` is None)."""
    base_qs = src_models.MasterPartFitment.objects.filter(source_provider=provider)
    if id_range is not None:
        base_qs = base_qs.filter(id__gte=id_range[0], id__lte=id_range[1])

    total_deleted = 0
    while True:
        # Each pass re-queries the lowest remaining ids in this slice - rows already deleted
        # (by this worker or, at the boundary, none other since slices are disjoint) simply
        # drop out, so no id cursor bookkeeping is needed between batches.
        ids = list(base_qs.order_by("id").values_list("id", flat=True)[:batch_size])
        if not ids:
            break
        src_models.MasterPartFitment.objects.filter(id__in=ids).delete()
        total_deleted += len(ids)
        logger.info(
            "{} [{}] Deleted {} MasterPartFitment rows (worker_total={}).".format(
                _LOG_PREFIX, label, len(ids), total_deleted
            )
        )
        connection.close()
        time.sleep(BATCH_DELAY_SECONDS)
    return total_deleted


def delete_master_part_fitments_by_provider_kind(
    kind: str,
    *,
    batch_size: int = FITMENT_DELETE_BATCH_SIZE,
    dry_run: bool = True,
    workers: int = 1,
) -> int:
    """
    Batched delete of every MasterPartFitment row whose source_provider matches the given
    provider kind (e.g. "TURN_14"), deleting ``batch_size`` rows per statement rather than one
    huge DELETE, to avoid a long lock hold / WAL spike on a table this size.

    ``workers`` > 1 splits the matching rows' id span into that many disjoint, equal-width id
    ranges and runs one delete loop per range concurrently (ThreadPoolExecutor) - safe because
    ranges never overlap, so two workers never contend for the same row. Each worker still
    deletes in the same ``batch_size`` commits as the single-worker path; only the number of
    concurrent delete statements against Postgres changes. Higher worker counts trade more
    concurrent I/O/WAL/lock pressure on a table that's still serving live app traffic for a
    shorter total wall-clock time - keep this modest (a handful) rather than maxing it out.

    Written to clear the ~17.9M single-year-per-row backlog that
    sync_master_part_fitments_from_turn14_vcdb wrote before it started collapsing years into
    ranges (see that function's docstring). Intended usage: run this once for kind="TURN_14",
    then re-run sync_master_part_fitments_from_turn14_vcdb to repopulate with proper year
    ranges. NOT wired into any scheduled pipeline - run manually via the
    delete_master_part_fitments_by_provider management command. Defaults to dry_run=True; the
    command's --apply flag is required to actually delete.
    """
    provider = src_models.Providers.objects.filter(kind_name=kind).first()
    if not provider:
        logger.info("{} No provider found for kind_name={}.".format(_LOG_PREFIX, kind))
        return 0

    base_qs = src_models.MasterPartFitment.objects.filter(source_provider=provider)
    total_matching = base_qs.count()
    logger.info(
        "{} {} MasterPartFitment rows match source_provider kind={} ({}, workers={}).".format(
            _LOG_PREFIX, total_matching, kind, "dry-run, nothing will be deleted" if dry_run else "deleting", workers
        )
    )
    if dry_run or not total_matching:
        return total_matching

    workers = max(1, int(workers))
    if workers == 1:
        total_deleted = _delete_fitments_for_provider_in_id_range(provider, None, batch_size, "all")
        logger.info(
            "{} Done: deleted {} MasterPartFitment rows for kind={}.".format(_LOG_PREFIX, total_deleted, kind)
        )
        return total_deleted

    id_bounds = base_qs.aggregate(min_id=Min("id"), max_id=Max("id"))
    lo, hi = id_bounds["min_id"], id_bounds["max_id"]
    span = hi - lo + 1
    workers = min(workers, span)
    step = -(-span // workers)  # ceil division, so ranges cover [lo, hi] fully
    id_ranges = []
    start = lo
    while start <= hi:
        end = min(start + step - 1, hi)
        id_ranges.append((start, end))
        start = end + 1

    def run_range(id_range: typing.Tuple[int, int]) -> int:
        close_old_connections()
        try:
            return _delete_fitments_for_provider_in_id_range(
                provider, id_range, batch_size, "id {}-{}".format(id_range[0], id_range[1])
            )
        finally:
            connection.close()

    total_deleted = 0
    with ThreadPoolExecutor(max_workers=len(id_ranges)) as ex:
        futs = [ex.submit(run_range, r) for r in id_ranges]
        for fut in futs:
            total_deleted += fut.result()

    logger.info(
        "{} Done: deleted {} MasterPartFitment rows for kind={} using {} workers.".format(
            _LOG_PREFIX, total_deleted, kind, len(id_ranges)
        )
    )
    return total_deleted


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

    dlg_brand_id_to_name = {m.dlg_brand_id: m.dlg_brand.name for m in mappings}

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=dlg_provider).values("id", "provider_external_id")
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
                .values("id", "brand_id", "part_number", "available_on_hand", "units", "display_name")[:BATCH_SIZE_INVENTORY]
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

            # Update product_details on ProviderPart (product metadata, not inventory)
            pp_details_to_update = []
            for row in batch:
                pn = row.get("part_number") or ""
                pn = pn.strip() if isinstance(pn, str) else str(pn or "").strip()
                if not pn:
                    continue
                ext_id = _dlg_provider_external_id(row["brand_id"], pn)
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = [
                        {"key": "sku",         "label": "SKU",         "value": pn or None},
                        {"key": "brand",       "label": "Brand",       "value": dlg_brand_id_to_name.get(row.get("brand_id")) or None},
                        {"key": "description", "label": "Description", "value": row.get("display_name") or None},
                        {"key": "units",       "label": "Unit",        "value": row.get("units") or None},
                    ]
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

            logger.info("{} DLG inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "dlg_brand", _worker)
    logger.info("{} Synced {} DLG inventory records total.".format(_LOG_PREFIX, total_upserted))


def _atech_product_details(row: typing.Dict, brand_name: typing.Optional[str] = None) -> typing.List[typing.Dict]:
    """
    Build the product_details list for an AtechParts row.
    Fee fields: None when value is 0 or null (no charge).
    """
    def _fee(val):
        if val is None:
            return None
        try:
            f = float(val)
            return f if f != 0 else None
        except (TypeError, ValueError):
            return None

    return [
        {"key": "sku",                 "label": "SKU",                 "value": row.get("part_number") or None},
        {"key": "brand",               "label": "Brand",               "value": brand_name or None},
        {"key": "description",         "label": "Description",         "value": row.get("description") or None},
        {"key": "fee_hazmat",          "label": "HAZMAT Fee",          "value": _fee(row.get("fee_hazmat"))},
        {"key": "fee_ground_shipping",  "label": "Ground Shipping Fee", "value": _fee(row.get("fee_truck_us"))},
        {"key": "fee_air_shipping",     "label": "Air Shipping Fee",    "value": _fee(row.get("fee_handling_air"))},
        {"key": "fee_freight",          "label": "Freight Fee",         "value": _fee(row.get("fee_truck_us"))},
    ]


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

    atech_brand_id_to_name = {m.atech_brand_id: m.atech_brand.name for m in mappings}

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=atech_provider).values("id", "provider_external_id")
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
                    "description",
                    "qty_tallmadge",
                    "qty_sparks",
                    "qty_mcdonough",
                    "qty_arlington",
                    "fee_hazmat",
                    "fee_truck_us",
                    "fee_handling_air",
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

            # Update product_details on ProviderPart (product metadata, not inventory)
            pp_details_to_update = []
            for ap in batch:
                ext = ap.get("part_number")
                if isinstance(ext, str):
                    ext = ext.strip()
                else:
                    ext = str(ext or "").strip()
                bid = ap.get("brand_id")
                if not ext or bid is None:
                    continue
                ext_id = _atech_provider_external_id(int(bid), ext)
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = _atech_product_details(ap, brand_name=atech_brand_id_to_name.get(ap.get("brand_id")))
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

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


def _vossen_provider_external_id(vossen_brand_id: int, sku: str) -> str:
    """Unique per Vossen provider: vossen_brand_id + sku (Vossen only ever has one brand, but
    kept the same compound shape as every other provider's external id for consistency)."""
    return "{}_{}".format(vossen_brand_id, sku)


def _vossen_product_details(row: typing.Dict) -> typing.List[typing.Dict]:
    """Wheel-spec attributes for a VossenPart row, shown as ProviderPart.product_details."""
    diameter = row.get("diameter")
    width = row.get("width")
    offset = row.get("offset")
    bolt_pattern = row.get("bolt_pattern")
    center_bore = row.get("center_bore")
    return [
        {"key": "sku", "label": "SKU", "value": row.get("sku") or None},
        {"key": "diameter", "label": "Diameter", "value": diameter if diameter and diameter != "0" else None},
        {"key": "width", "label": "Width", "value": width if width and width != "0.00" else None},
        {"key": "offset", "label": "Offset", "value": offset if offset and offset != "0" else None},
        {"key": "bolt_pattern", "label": "Bolt Pattern", "value": bolt_pattern or None},
        {"key": "center_bore", "label": "Centerbore", "value": center_bore if center_bore and center_bore != "0.00" else None},
    ]


def _ingest_vossen_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    vossen_provider: src_models.Providers,
    vossen_brand_to_brand: typing.Dict[int, src_models.Brands],
    category_by_source: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> typing.Tuple[int, int]:
    """Batch-ingest VossenPart rows for one disjoint set of Vossen catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.VossenPart.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values("id", "brand_id", "sku", "description", "updated_at")[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        vossen_external_id_to_brand_part = {}

        for row in batch:
            brand = vossen_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = (row.get("sku") or "").strip()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=row.get("description"),
                    )
                )
            vossen_external_id_to_brand_part[
                _vossen_provider_external_id(row["brand_id"], row["sku"])
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

        # Vossen spells manufacturer part numbers in its own house style, so anything the exact
        # match missed gets a second, heavily-guarded pass on the normalized form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=vossen_provider.id,
            provider_kind=src_enums.BrandProviderKind.VOSSEN.value,
            external_id_by_key={v: k for k, v in vossen_external_id_to_brand_part.items()},
        )

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="Vossen new_parts batch={}".format(batch_num)
            )
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart
        # under the feed's own spelling, so the exact-part_number query below cannot find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
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
            ext_id = _vossen_provider_external_id(row["brand_id"], row["sku"])
            key = vossen_external_id_to_brand_part.get(ext_id)
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, vossen_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=vossen_provider,
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

        logger.info("{} Batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_vossen() -> None:
    """
    Create/update MasterPart and ProviderPart from VossenPart.
    Only processes parts whose VossenBrand has a BrandVossenBrandMapping (in practice always
    the single "VOSSEN" brand). Same cursor-based, two-phase upsert pattern as every other
    provider.
    """
    logger.info("{} Syncing master parts from Vossen (batched, cursor-based).".format(_LOG_PREFIX))

    vossen_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.VOSSEN.value,
    ).first()
    if not vossen_provider:
        logger.info("{} No Vossen provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandVossenBrandMapping.objects.select_related("brand", "vossen_brand")
    )
    vossen_brand_to_brand = {m.vossen_brand_id: m.brand for m in mappings}
    if not vossen_brand_to_brand:
        logger.info("{} No BrandVossenBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    category_by_source = _load_category_mapping_by_source()
    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "vossen_brand",
        lambda cids: _ingest_vossen_parts_for_mapped_brands(
            cids,
            vossen_provider,
            vossen_brand_to_brand,
            category_by_source,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Vossen total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def sync_provider_inventory_from_vossen() -> None:
    """
    Sync ProviderPartInventory from VossenPart.available (manufacturer-wide stock, not
    per-warehouse -- so warehouse_availability stays null, only warehouse_total_qty is set).
    Also refreshes ProviderPart.product_details with wheel-spec attributes.
    """
    logger.info("{} Syncing provider inventory from Vossen.".format(_LOG_PREFIX))

    vossen_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.VOSSEN.value,
    ).first()
    if not vossen_provider:
        logger.info("{} No Vossen provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandVossenBrandMapping.objects.select_related("brand", "vossen_brand")
    )
    if not mappings:
        logger.info("{} No BrandVossenBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=vossen_provider).values(
            "id", "provider_external_id"
        )
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
                src_models.VossenPart.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
                .order_by("id")
                .values(
                    "id", "brand_id", "sku", "available",
                    "diameter", "width", "offset", "bolt_pattern", "center_bore",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            to_upsert = []
            for row in batch:
                ext_id = _vossen_provider_external_id(row["brand_id"], row["sku"])
                provider_part = provider_parts.get(ext_id)
                if not provider_part:
                    continue
                to_upsert.append(
                    src_models.ProviderPartInventory(
                        provider_part=provider_part,
                        warehouse_total_qty=row.get("available") or 0,
                        manufacturer_inventory=None,
                        manufacturer_esd=None,
                        warehouse_availability=None,
                        last_synced_at=now,
                        updated_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="Vossen inventory batch={}".format(batch_num)
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

            pp_details_to_update = []
            for row in batch:
                ext_id = _vossen_provider_external_id(row["brand_id"], row["sku"])
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = _vossen_product_details(row)
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

            logger.info("{} Vossen inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "vossen_brand", _worker)
    logger.info("{} Synced {} Vossen inventory records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_vossen() -> None:
    """
    Sync ProviderPartCompanyPricing from VossenCompanyPricing (per-company raw feed price).
    Sets ``cost`` from that price and company ``discount_percent`` (see
    :func:`src.integrations.services.vossen.dealer_cost_from_price`); ``msrp``/``retail_price``
    are the raw feed price as-is (no separate MSRP/MAP columns in Vossen's feed).
    """
    logger.info("{} Syncing provider pricing from Vossen.".format(_LOG_PREFIX))

    vossen_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.VOSSEN.value,
    ).first()
    if not vossen_provider:
        logger.info("{} No Vossen provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandVossenBrandMapping.objects.select_related("brand", "vossen_brand")
    )
    vossen_brand_to_brand = {m.vossen_brand_id: m.brand for m in mappings}
    if not vossen_brand_to_brand:
        logger.info("{} No BrandVossenBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
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
                src_models.VossenCompanyPricing.objects.filter(
                    id__gt=last_id, part__brand_id__in=catalog_ids
                )
                .order_by("id")
                .values("id", "company_id", "price", "part__brand_id", "part__sku")[:BATCH_SIZE_PRICING]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
            companies_by_id = {
                c.id: c for c in src_models.Company.objects.filter(id__in=batch_company_ids)
            }
            creds_by_company: typing.Dict[int, typing.Dict] = {}
            if batch_company_ids:
                for cpr in src_models.CompanyProviders.objects.filter(
                    company_id__in=batch_company_ids,
                    provider_id=vossen_provider.id,
                ):
                    creds_by_company[cpr.company_id] = credentials_helper.get_feed_credentials(cpr)
            master_keys = []
            for row in batch:
                brand = vossen_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = (row.get("part__sku") or "").strip()
                if not sku:
                    continue
                master_keys.append((brand.id, sku))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(vossen_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = vossen_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = (row.get("part__sku") or "").strip()
                if not sku:
                    continue
                provider_part = pp_by_key.get((brand.id, sku))
                if not provider_part:
                    continue
                cid = row.get("company_id")
                company = companies_by_id.get(cid)
                if not company:
                    continue
                price = row.get("price")
                creds = creds_by_company.get(cid) if cid is not None else {}
                cost = vossen_dealer_cost_from_price(price, creds)
                to_upsert.append(
                    src_models.ProviderPartCompanyPricing(
                        provider_part=provider_part,
                        company=company,
                        cost=cost,
                        jobber_price=None,
                        map_price=None,
                        msrp=price,
                        retail_price=price,
                        last_synced_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Vossen pricing batch={}".format(batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} Vossen pricing batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "vossen_brand", _worker)
    logger.info("{} Synced {} Vossen pricing records total.".format(_LOG_PREFIX, total_upserted))


# ============================================================
# QUADRATEC
# ============================================================

def _quadratec_provider_external_id(quadratec_brand_id: int, quadratec_pn: str) -> str:
    """
    Stable Quadratec ProviderPart key: quadratec_brand row id + the Quadratec part number
    (Quadratec's own PN, which is unique within the feed), same compound shape as every other
    provider's external id.
    """
    pn = (quadratec_pn or "").strip()
    return "{}_{}".format(int(quadratec_brand_id), pn)


def _quadratec_master_part_number(mpn: typing.Optional[str], quadratec_pn: str) -> str:
    """
    MasterPart part number for a Quadratec row: the manufacturer part number (``mpn``) when the
    feed carries one, else the Quadratec PN. Keying on the MPN lets the same physical part from
    Quadratec dedupe against the same part from other distributors; Quadratec house-brand items
    (Quadratec / QuadraTop / TACTIK ...) have no separate MPN, so they fall back to the Quadratec PN.
    """
    m = (mpn or "").strip()
    return m if m else (quadratec_pn or "").strip()


def _quadratec_product_details(row: typing.Dict) -> typing.List[typing.Dict]:
    """product_details for a QuadratecPart row (shown on ProviderPart)."""
    return [
        {"key": "quadratec_pn", "label": "Quadratec Part No", "value": row.get("sku") or None},
        {"key": "mpn", "label": "MPN", "value": row.get("mpn") or None},
        {"key": "upc", "label": "UPC", "value": row.get("upc") or None},
        {"key": "description", "label": "Description", "value": row.get("description") or None},
    ]


def _quadratec_inventory_warehouse_availability(row: typing.Dict) -> typing.Dict[str, typing.Any]:
    """Per-warehouse stock from a QuadratecPart row (PA1/PA2/NV1)."""
    return {
        "PA1": row.get("inv_pa1"),
        "PA2": row.get("inv_pa2"),
        "NV1": row.get("inv_nv1"),
    }


def _ingest_quadratec_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    quadratec_provider: src_models.Providers,
    quadratec_brand_to_brand: typing.Dict[int, src_models.Brands],
    category_by_source: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> typing.Tuple[int, int]:
    """Batch-ingest QuadratecPart rows for one disjoint set of Quadratec catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.QuadratecPart.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values("id", "brand_id", "sku", "mpn", "description", "updated_at", "upc")[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        external_id_to_brand_part = {}

        for row in batch:
            brand = quadratec_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = _quadratec_master_part_number(row.get("mpn"), row.get("sku"))
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=row.get("description"),
                        # Without this every Quadratec master part is created with a null gtin,
                        # so the gtin map handed to extend_with_normalized_matches below is all
                        # None and every hyphen/dot-level match is refused for want of
                        # corroboration -- Quadratec then creates its own row spelled the way
                        # another distributor spells it, and that row steals their exact match
                        # on the next sync. 58,946 of its 96,649 feed rows carry a upc.
                        gtin=row.get("upc") or None,
                    )
                )
            external_id_to_brand_part[
                _quadratec_provider_external_id(row["brand_id"], row["sku"])
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
                for r in cur.fetchall():
                    mp_id, b_id, p_num = r
                    existing_by_key[(b_id, p_num)] = mp_id

        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=quadratec_provider.id,
            provider_kind=src_enums.BrandProviderKind.QUADRATEC.value,
            external_id_by_key={v: k for k, v in external_id_to_brand_part.items()},
        )

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="Quadratec new_parts batch={}".format(batch_num)
            )
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
                )
                for r in cur.fetchall():
                    mp_id, b_id, p_num = r
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts_by_key = {}
        for row in batch:
            ext_id = _quadratec_provider_external_id(row["brand_id"], row["sku"])
            key = external_id_to_brand_part.get(ext_id)
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, quadratec_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=quadratec_provider,
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

        logger.info("{} Quadratec batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_quadratec() -> None:
    """
    Create/update MasterPart and ProviderPart from QuadratecPart. Only processes parts whose
    QuadratecBrand has a BrandQuadratecBrandMapping. Same cursor-based, two-phase upsert pattern
    as every other provider.
    """
    logger.info("{} Syncing master parts from Quadratec (batched, cursor-based).".format(_LOG_PREFIX))

    quadratec_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.QUADRATEC.value,
    ).first()
    if not quadratec_provider:
        logger.info("{} No Quadratec provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandQuadratecBrandMapping.objects.select_related("brand", "quadratec_brand")
    )
    quadratec_brand_to_brand = {m.quadratec_brand_id: m.brand for m in mappings}
    if not quadratec_brand_to_brand:
        logger.info("{} No BrandQuadratecBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    category_by_source = _load_category_mapping_by_source()
    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "quadratec_brand",
        lambda cids: _ingest_quadratec_parts_for_mapped_brands(
            cids,
            quadratec_provider,
            quadratec_brand_to_brand,
            category_by_source,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Quadratec total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def sync_provider_inventory_from_quadratec() -> None:
    """
    Sync ProviderPartInventory from QuadratecPart per-warehouse stock (PA1/PA2/NV1 + total).
    Also refreshes ProviderPart.product_details.
    """
    logger.info("{} Syncing provider inventory from Quadratec.".format(_LOG_PREFIX))

    quadratec_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.QUADRATEC.value,
    ).first()
    if not quadratec_provider:
        logger.info("{} No Quadratec provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandQuadratecBrandMapping.objects.select_related("brand", "quadratec_brand")
    )
    if not mappings:
        logger.info("{} No BrandQuadratecBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=quadratec_provider).values(
            "id", "provider_external_id"
        )
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
                src_models.QuadratecPart.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
                .order_by("id")
                .values(
                    "id", "brand_id", "sku", "mpn", "upc", "description",
                    "inv_pa1", "inv_pa2", "inv_nv1", "inv_total",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            to_upsert = []
            for row in batch:
                ext_id = _quadratec_provider_external_id(row["brand_id"], row["sku"])
                provider_part = provider_parts.get(ext_id)
                if not provider_part:
                    continue
                to_upsert.append(
                    src_models.ProviderPartInventory(
                        provider_part=provider_part,
                        warehouse_total_qty=row.get("inv_total") or 0,
                        manufacturer_inventory=None,
                        manufacturer_esd=None,
                        warehouse_availability=_quadratec_inventory_warehouse_availability(row),
                        last_synced_at=now,
                        updated_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="Quadratec inventory batch={}".format(batch_num)
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

            pp_details_to_update = []
            for row in batch:
                ext_id = _quadratec_provider_external_id(row["brand_id"], row["sku"])
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = _quadratec_product_details(row)
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

            logger.info("{} Quadratec inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "quadratec_brand", _worker)
    logger.info("{} Synced {} Quadratec inventory records total.".format(_LOG_PREFIX, total_upserted))


def _quadratec_provider_part_company_pricing_from_row(
    row: typing.Dict,
    provider_part: src_models.ProviderPart,
    company: src_models.Company,
    now,
) -> src_models.ProviderPartCompanyPricing:
    """Map a QuadratecCompanyPricing row to a ProviderPartCompanyPricing instance."""
    return src_models.ProviderPartCompanyPricing(
        provider_part=provider_part,
        company=company,
        cost=row.get("cost"),
        jobber_price=row.get("wholesale_price"),
        map_price=row.get("map"),
        msrp=row.get("retail_price"),
        retail_price=row.get("retail_price"),
        last_synced_at=now,
    )


def _quadratec_pricing_master_keys(
    batch: typing.List[typing.Dict],
    quadratec_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.List[typing.Tuple[int, str]]:
    keys = []
    for row in batch:
        brand = quadratec_brand_to_brand.get(row.get("part__brand_id"))
        if not brand:
            continue
        part_number = _quadratec_master_part_number(row.get("part__mpn"), row.get("part__sku"))
        if not part_number:
            continue
        keys.append((brand.id, part_number))
    return keys


_QUADRATEC_PRICING_VALUES = (
    "id", "company_id", "cost", "wholesale_price", "map", "retail_price",
    "part__brand_id", "part__mpn", "part__sku",
)
_QUADRATEC_PRICING_UPSERT_FIELDS = ["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"]


def sync_provider_pricing_from_quadratec() -> None:
    """Sync ProviderPartCompanyPricing from QuadratecCompanyPricing (all companies)."""
    logger.info("{} Syncing provider pricing from Quadratec.".format(_LOG_PREFIX))

    quadratec_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.QUADRATEC.value,
    ).first()
    if not quadratec_provider:
        logger.info("{} No Quadratec provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandQuadratecBrandMapping.objects.select_related("brand", "quadratec_brand")
    )
    quadratec_brand_to_brand = {m.quadratec_brand_id: m.brand for m in mappings}
    if not quadratec_brand_to_brand:
        logger.info("{} No BrandQuadratecBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
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
                src_models.QuadratecCompanyPricing.objects.filter(
                    id__gt=last_id, part__brand_id__in=catalog_ids
                )
                .order_by("id")
                .values(*_QUADRATEC_PRICING_VALUES)[:BATCH_SIZE_PRICING]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
            companies_by_id = {
                c.id: c for c in src_models.Company.objects.filter(id__in=batch_company_ids)
            }
            pp_by_key = _provider_parts_by_master_brand_and_part_number(
                quadratec_provider, _quadratec_pricing_master_keys(batch, quadratec_brand_to_brand)
            )

            to_upsert = []
            for row in batch:
                brand = quadratec_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                part_number = _quadratec_master_part_number(row.get("part__mpn"), row.get("part__sku"))
                if not part_number:
                    continue
                provider_part = pp_by_key.get((brand.id, part_number))
                if not provider_part:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                to_upsert.append(
                    _quadratec_provider_part_company_pricing_from_row(row, provider_part, company, now)
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Quadratec pricing batch={}".format(batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=_QUADRATEC_PRICING_UPSERT_FIELDS,
                )
                total_upserted += len(to_upsert)

            logger.info("{} Quadratec pricing batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "quadratec_brand", _worker)
    logger.info("{} Synced {} Quadratec pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_quadratec_for_company(company_id: int) -> None:
    logger.info("{} Syncing Quadratec provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    quadratec_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.QUADRATEC.value,
    ).first()
    if not quadratec_provider:
        logger.info("{} No Quadratec provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandQuadratecBrandMapping.objects.select_related("brand", "quadratec_brand")
    )
    quadratec_brand_to_brand = {m.quadratec_brand_id: m.brand for m in mappings}
    if not quadratec_brand_to_brand:
        logger.info("{} No BrandQuadratecBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider_id=quadratec_provider.id
    ).first()

    now = timezone.now()

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} Quadratec pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.QuadratecCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs.order_by("id").values(*_QUADRATEC_PRICING_VALUES)[:BATCH_SIZE_PRICING]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
            companies_by_id = {
                c.id: c for c in src_models.Company.objects.filter(id__in=batch_company_ids)
            }
            pp_by_key = _provider_parts_by_master_brand_and_part_number(
                quadratec_provider, _quadratec_pricing_master_keys(batch, quadratec_brand_to_brand)
            )

            to_upsert = []
            for row in batch:
                brand = quadratec_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                part_number = _quadratec_master_part_number(row.get("part__mpn"), row.get("part__sku"))
                if not part_number:
                    continue
                provider_part = pp_by_key.get((brand.id, part_number))
                if not provider_part:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                to_upsert.append(
                    _quadratec_provider_part_company_pricing_from_row(row, provider_part, company, now)
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Quadratec pricing company={} batch={}".format(company_id, batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=_QUADRATEC_PRICING_UPSERT_FIELDS,
                )
                total_upserted += len(to_upsert)

            logger.info("{} Quadratec pricing company={} batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "quadratec_brand", _worker)
    logger.info("{} Synced {} Quadratec pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


def sync_derived_from_quadratec(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Quadratec source data into MasterPart, ProviderPart, ProviderPartInventory, and
    ProviderPartCompanyPricing. Call after Quadratec feed ingest. No fitment sync -- the Quadratec
    feeds carry no vehicle fitment.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting Quadratec-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_quadratec()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_quadratec()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_quadratec()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Quadratec",
            continuation=_cont,
        )
    logger.info("{} Completed Quadratec-only derived sync.".format(_LOG_PREFIX))


# ==========================================================================================
# Motor State Distributing
# ==========================================================================================
def _motorstate_provider_external_id(motorstate_brand_id: int, part_number: str) -> str:
    """
    Stable Motor State ProviderPart key: motorstate_brand row id + Motor State's own part
    number (unique across their catalog), same compound shape as every other provider.
    """
    pn = (part_number or "").strip()
    return "{}_{}".format(int(motorstate_brand_id), pn)


def _motorstate_master_part_number(
    vendor_part_number: typing.Optional[str], part_number: str
) -> str:
    """
    MasterPart part number for a Motor State row: the manufacturer part number
    (``VendorPartNumber``) when present, else Motor State's own part number.

    Motor State's own PartNumber is brand-code-prefixed (``AAA01816`` for MPN ``A1P01816``), so
    keying on it would never dedupe against the same physical part from another distributor.
    ~100% of found rows carry a VendorPartNumber, so the fallback is a rare safety net.
    """
    v = (vendor_part_number or "").strip()
    return v if v else (part_number or "").strip()


def _motorstate_product_details(row: typing.Dict) -> typing.List[typing.Dict]:
    """product_details for a MotorStateProduct row (shown on ProviderPart)."""
    return [
        {"key": "motorstate_pn", "label": "Motor State Part No", "value": row.get("part_number") or None},
        {"key": "mpn", "label": "MPN", "value": row.get("vendor_part_number") or None},
        {"key": "description", "label": "Description", "value": row.get("short_description") or None},
        {"key": "supersede", "label": "Superseded By", "value": row.get("supersede_part_number") or None},
    ]


def _ingest_motorstate_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    motorstate_provider: src_models.Providers,
    motorstate_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.Tuple[int, int]:
    """Batch-ingest MotorStateProduct rows for one disjoint set of Motor State brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.MotorStateProduct.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                found=True,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id", "brand_id", "part_number", "vendor_part_number", "short_description",
                "supersede_part_number", "updated_at",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        external_id_to_brand_part = {}

        for row in batch:
            brand = motorstate_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = _motorstate_master_part_number(
                row.get("vendor_part_number"), row.get("part_number")
            )
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=row.get("short_description"),
                        # Motor State's /api/Product carries no UPC/GTIN (confirmed against the
                        # live payload), so normalized cross-distributor matching has no
                        # corroborating identifier here -- left null rather than faked.
                        gtin=None,
                    )
                )
            external_id_to_brand_part[
                _motorstate_provider_external_id(row["brand_id"], row["part_number"])
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
                for r in cur.fetchall():
                    mp_id, b_id, p_num = r
                    existing_by_key[(b_id, p_num)] = mp_id

        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=motorstate_provider.id,
            provider_kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
            external_id_by_key={v: k for k, v in external_id_to_brand_part.items()},
        )

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="MotorState new_parts batch={}".format(batch_num)
            )
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
                )
                for r in cur.fetchall():
                    mp_id, b_id, p_num = r
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts_by_key = {}
        for row in batch:
            ext_id = _motorstate_provider_external_id(row["brand_id"], row["part_number"])
            key = external_id_to_brand_part.get(ext_id)
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            pp_key = (master_part.id, motorstate_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=motorstate_provider,
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

        logger.info("{} MotorState batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_motorstate() -> None:
    """
    Create/update MasterPart and ProviderPart from MotorStateProduct (a global catalog table --
    prices live per company on MotorStateCompanyPricing). Only processes parts whose
    MotorStateBrand has a BrandMotorStateBrandMapping. Same cursor-based, two-phase upsert
    pattern as every other provider.
    """
    logger.info("{} Syncing master parts from Motor State (batched, cursor-based).".format(_LOG_PREFIX))

    motorstate_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
    ).first()
    if not motorstate_provider:
        logger.info("{} No Motor State provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandMotorStateBrandMapping.objects.select_related("brand", "motorstate_brand")
    )
    motorstate_brand_to_brand = {m.motorstate_brand_id: m.brand for m in mappings}
    if not motorstate_brand_to_brand:
        logger.info("{} No BrandMotorStateBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "motorstate_brand",
        lambda cids: _ingest_motorstate_parts_for_mapped_brands(
            cids,
            motorstate_provider,
            motorstate_brand_to_brand,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Motor State total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def sync_provider_inventory_from_motorstate() -> None:
    """
    Sync ProviderPartInventory from MotorStateAvailability (quantity + StatusType), which is
    distributor-wide. Motor State exposes no per-warehouse breakdown -- QuantityAvailable is a
    single distributor-wide number -- so warehouse_availability carries the status instead.
    Also refreshes ProviderPart.product_details from the matching MotorStateProduct row.
    """
    logger.info("{} Syncing provider inventory from Motor State.".format(_LOG_PREFIX))

    motorstate_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
    ).first()
    if not motorstate_provider:
        logger.info("{} No Motor State provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandMotorStateBrandMapping.objects.select_related("brand", "motorstate_brand")
    )
    if not mappings:
        logger.info("{} No BrandMotorStateBrandMapping found.".format(_LOG_PREFIX))
        return

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=motorstate_provider).values(
            "id", "provider_external_id"
        )
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
                src_models.MotorStateProduct.objects.filter(
                    id__gt=last_id,
                    brand_id__in=catalog_ids,
                    found=True,
                )
                .order_by("id")
                .values(
                    "id", "brand_id", "part_number", "vendor_part_number", "short_description",
                    "supersede_part_number", "quantity", "is_stocking",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]

            # Availability is the fresher stock signal (polled incrementally between full
            # product hydrates), so prefer its quantity over the product row's snapshot.
            part_numbers = [r["part_number"] for r in batch]
            availability = {
                a["part_number"]: a
                for a in src_models.MotorStateAvailability.objects.filter(
                    part_number__in=part_numbers
                ).values("part_number", "quantity_available", "status_type")
            }

            to_upsert = []
            for row in batch:
                ext_id = _motorstate_provider_external_id(row["brand_id"], row["part_number"])
                provider_part = provider_parts.get(ext_id)
                if not provider_part:
                    continue
                avail = availability.get(row["part_number"]) or {}
                qty = avail.get("quantity_available")
                if qty is None:
                    qty = row.get("quantity")
                status_type = (avail.get("status_type") or "").upper() or None
                to_upsert.append(
                    src_models.ProviderPartInventory(
                        provider_part=provider_part,
                        warehouse_total_qty=qty or 0,
                        manufacturer_inventory=None,
                        manufacturer_esd=None,
                        warehouse_availability={"status_type": status_type, "total": qty or 0},
                        last_synced_at=now,
                        updated_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="MotorState inventory batch={}".format(batch_num)
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

            pp_details_to_update = []
            for row in batch:
                ext_id = _motorstate_provider_external_id(row["brand_id"], row["part_number"])
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = _motorstate_product_details(row)
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

            logger.info("{} MotorState inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "motorstate_brand", _worker)
    logger.info("{} Synced {} Motor State inventory records total.".format(_LOG_PREFIX, total_upserted))


def _motorstate_provider_part_company_pricing_from_row(
    row: typing.Dict,
    provider_part: src_models.ProviderPart,
    company: src_models.Company,
    now,
) -> src_models.ProviderPartCompanyPricing:
    """
    Map a MotorStateCompanyPricing row to a ProviderPartCompanyPricing instance.

    ``customer_price`` is this account's actual buy price (what they pay); ``base_price`` is
    Motor State's undiscounted wholesale, mapped to jobber. ``list_price`` is MSRP.
    """
    return src_models.ProviderPartCompanyPricing(
        provider_part=provider_part,
        company=company,
        cost=row.get("customer_price"),
        jobber_price=row.get("base_price"),
        map_price=row.get("map_price"),
        msrp=row.get("list_price"),
        retail_price=row.get("list_price"),
        last_synced_at=now,
    )


def _motorstate_pricing_master_keys(
    batch: typing.List[typing.Dict],
    motorstate_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.List[typing.Tuple[int, str]]:
    keys = []
    for row in batch:
        brand = motorstate_brand_to_brand.get(row.get("product__brand_id"))
        if not brand:
            continue
        part_number = _motorstate_master_part_number(
            row.get("product__vendor_part_number"), row.get("product__part_number")
        )
        if not part_number:
            continue
        keys.append((brand.id, part_number))
    return keys


_MOTORSTATE_PRICING_VALUES = (
    "id", "company_id",
    "product__brand_id", "product__part_number", "product__vendor_part_number",
    "customer_price", "base_price", "map_price", "list_price",
)
_MOTORSTATE_PRICING_UPSERT_FIELDS = [
    "cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at",
]


def _sync_motorstate_pricing(company_id: typing.Optional[int] = None) -> None:
    """
    Shared body for the all-companies and single-company Motor State pricing syncs.
    ``company_id=None`` processes every company's rows.
    """
    scope = "company_id={}".format(company_id) if company_id else "all companies"
    logger.info("{} Syncing provider pricing from Motor State ({}).".format(_LOG_PREFIX, scope))

    motorstate_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
    ).first()
    if not motorstate_provider:
        logger.info("{} No Motor State provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandMotorStateBrandMapping.objects.select_related("brand", "motorstate_brand")
    )
    motorstate_brand_to_brand = {m.motorstate_brand_id: m.brand for m in mappings}
    if not motorstate_brand_to_brand:
        logger.info("{} No BrandMotorStateBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    company_provider = None
    watermark = None
    sync_started_at = timezone.now()
    if company_id:
        company_provider = src_models.CompanyProviders.objects.filter(
            company_id=company_id, provider_id=motorstate_provider.id
        ).first()
        # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
        watermark = company_provider.pricing_propagation_watermark if company_provider else None
        logger.info(
            "{} Motor State pricing company={}: propagation watermark={}.".format(
                _LOG_PREFIX, company_id, watermark or "none (processing everything)",
            )
        )

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.MotorStateCompanyPricing.objects.filter(
                id__gt=last_id, product__brand_id__in=catalog_ids, product__found=True
            )
            if company_id:
                qs = qs.filter(company_id=company_id)
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs.order_by("id").values(*_MOTORSTATE_PRICING_VALUES)[:BATCH_SIZE_PRICING]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
            companies_by_id = {
                c.id: c for c in src_models.Company.objects.filter(id__in=batch_company_ids)
            }
            pp_by_key = _provider_parts_by_master_brand_and_part_number(
                motorstate_provider, _motorstate_pricing_master_keys(batch, motorstate_brand_to_brand)
            )

            to_upsert = []
            for row in batch:
                brand = motorstate_brand_to_brand.get(row.get("product__brand_id"))
                if not brand:
                    continue
                part_number = _motorstate_master_part_number(
                    row.get("product__vendor_part_number"), row.get("product__part_number")
                )
                if not part_number:
                    continue
                provider_part = pp_by_key.get((brand.id, part_number))
                if not provider_part:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                to_upsert.append(
                    _motorstate_provider_part_company_pricing_from_row(row, provider_part, company, now)
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="MotorState pricing {} batch={}".format(scope, batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=_MOTORSTATE_PRICING_UPSERT_FIELDS,
                )
                total_upserted += len(to_upsert)

            logger.info("{} MotorState pricing {} batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, scope, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "motorstate_brand", _worker)
    logger.info("{} Synced {} Motor State pricing records ({}).".format(
        _LOG_PREFIX, total_upserted, scope,
    ))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


def sync_provider_pricing_from_motorstate() -> None:
    """Sync ProviderPartCompanyPricing from MotorStateProduct (all companies)."""
    _sync_motorstate_pricing(company_id=None)


def sync_provider_pricing_from_motorstate_for_company(company_id: int) -> None:
    """Sync ProviderPartCompanyPricing from MotorStateProduct for one company."""
    _sync_motorstate_pricing(company_id=company_id)


def sync_derived_from_motorstate(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Motor State source data into MasterPart, ProviderPart, ProviderPartInventory, and
    ProviderPartCompanyPricing. Call after the Motor State raw ingest. No fitment sync -- the
    Motor State API exposes no vehicle fitment.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting Motor State-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_motorstate()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_motorstate()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_motorstate()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Motor State",
            continuation=_cont,
        )
    logger.info("{} Completed Motor State-only derived sync.".format(_LOG_PREFIX))


# ==========================================================================================
# Western Power Sports
# ==========================================================================================
# WPS's supplier_product_id is free text, not a validated MPN column: most rows carry a clean
# manufacturer number, some repeat the WPS sku verbatim, and a few are notes ("OLD 0923003A-4").
# Anything with whitespace inside is treated as a note rather than a part number.
_WPS_BAD_MPN_PREFIXES = ("OLD ", "OLD-", "USE ", "NLA ", "SEE ")


def _wps_provider_external_id(wps_brand_id: typing.Optional[int], sku: str) -> str:
    """Stable WPS ProviderPart key: wps_brand row id + WPS's own sku (unique in their catalog)."""
    return "{}_{}".format(int(wps_brand_id or 0), (sku or "").strip())


def _wps_master_part_number(
    supplier_product_id: typing.Optional[str], sku: typing.Optional[str]
) -> str:
    """
    MasterPart part number for a WPS row: the manufacturer part number
    (``supplier_product_id``) when it looks like one, else WPS's own ``sku``.

    Measured against live data before choosing: across 12 brands we already carry, WPS ``sku``
    matched an existing MasterPart for 0% of rows on 11 of them (it is a WPS-internal number),
    while supplier_product_id matched 15.8-100%. The one exception was FLY RACING, WPS's own
    house brand, where sku and supplier_product_id are the same string anyway.

    A *lookup* fallback on sku was measured too and rejected: it added 0.5% more matches while
    risking attaching WPS to another distributor's part that happens to share a WPS-style
    number. This is only a value-level fallback -- it picks which string to use, and never
    matches on a WPS-internal number.
    """
    mpn = (supplier_product_id or "").strip()
    upper = mpn.upper()
    if mpn and " " not in mpn and not upper.startswith(_WPS_BAD_MPN_PREFIXES):
        return mpn
    return (sku or "").strip()


def _wps_product_details(row: typing.Dict) -> typing.List[typing.Dict]:
    """product_details for a WpsItem row (shown on ProviderPart)."""
    return [
        {"key": "wps_sku", "label": "WPS Part No", "value": row.get("sku") or None},
        {"key": "mpn", "label": "MPN", "value": row.get("supplier_product_id") or None},
        {"key": "upc", "label": "UPC", "value": row.get("upc") or None},
        {"key": "description", "label": "Description", "value": row.get("name") or None},
        {"key": "status", "label": "Status", "value": row.get("status") or None},
        {"key": "superseded_sku", "label": "Superseded By", "value": row.get("superseded_sku") or None},
    ]


def _wps_warehouse_availability(
    inv: typing.Dict, warehouse_names: typing.Dict[str, str]
) -> typing.Dict[str, typing.Any]:
    """
    Per-warehouse stock keyed by real place name (Boise, Fresno, ...) rather than the raw
    ``pa2_warehouse`` column, using the GET /warehouses decode table.

    Every figure is capped at 25 by WPS on purpose, so ``total`` is a floor, not a true count --
    recorded here so downstream consumers do not read it as exact depth.
    """
    out: typing.Dict[str, typing.Any] = {}
    for db2_key, column in wps_services.WAREHOUSE_COLUMN_BY_KEY.items():
        label = warehouse_names.get(db2_key) or db2_key
        out[label] = inv.get(column) or 0
    out["total"] = inv.get("total") or 0
    out["capped_at_25_per_warehouse"] = True
    return out


def _ingest_wps_items_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    wps_provider: src_models.Providers,
    wps_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.Tuple[int, int]:
    """Batch-ingest WpsItem rows for one disjoint set of WPS brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.WpsItem.objects.filter(
                brand_id__in=mapped_catalog_brand_ids, id__gt=last_id
            )
            .order_by("id")
            .values(
                "id", "brand_id", "sku", "name", "supplier_product_id", "upc",
                "image_url", "source_updated_at", "product__description",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        external_id_to_brand_part = {}

        for row in batch:
            brand = wps_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue
            part_number = _wps_master_part_number(row.get("supplier_product_id"), row.get("sku"))
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        # The item's own name is a short label ("MULTIRATE FORK SPRINGS 35MM");
                        # the long copy lives on the parent product.
                        description=row.get("product__description") or row.get("name"),
                        image_url=row.get("image_url"),
                        # 47% of WPS items carry a upc -- the corroboration
                        # extend_with_normalized_matches needs to accept a formatting-only match.
                        gtin=(row.get("upc") or None),
                    )
                )
            external_id_to_brand_part[
                _wps_provider_external_id(row["brand_id"], row.get("sku"))
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
                for r in cur.fetchall():
                    mp_id, b_id, p_num = r
                    existing_by_key[(b_id, p_num)] = mp_id

        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=wps_provider.id,
            provider_kind=src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
            external_id_by_key={v: k for k, v in external_id_to_brand_part.items()},
        )

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]
        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="WPS new_parts batch={}".format(batch_num)
            )
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
                )
                for r in cur.fetchall():
                    mp_id, b_id, p_num = r
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts_by_key = {}
        for row in batch:
            ext_id = _wps_provider_external_id(row["brand_id"], row.get("sku"))
            key = external_id_to_brand_part.get(ext_id)
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            provider_parts_by_key[(master_part.id, wps_provider.id)] = src_models.ProviderPart(
                master_part=master_part,
                provider=wps_provider,
                provider_external_id=ext_id,
                distributor_refreshed_at=row.get("source_updated_at"),
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

        logger.info("{} WPS batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_wps() -> None:
    """
    Create/update MasterPart and ProviderPart from WpsItem (a distributor-wide catalog; prices
    live per company on WpsCompanyPricing). Only processes items whose WpsBrand has a
    BrandWpsBrandMapping.
    """
    logger.info("{} Syncing master parts from WPS (batched, cursor-based).".format(_LOG_PREFIX))

    wps_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
    ).first()
    if not wps_provider:
        logger.info("{} No WPS provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandWpsBrandMapping.objects.select_related("brand", "wps_brand")
    )
    wps_brand_to_brand = {m.wps_brand_id: m.brand for m in mappings}
    if not wps_brand_to_brand:
        logger.info("{} No BrandWpsBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "wps_brand",
        lambda cids: _ingest_wps_items_for_mapped_brands(cids, wps_provider, wps_brand_to_brand),
    )
    logger.info("{} Synced {} master parts and {} provider parts from WPS total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def sync_provider_inventory_from_wps() -> None:
    """
    Sync ProviderPartInventory from WpsInventory. Warehouse columns are decoded to place names
    through WpsWarehouse (GET /warehouses), so warehouse_availability reads
    {"Boise": 25, "Fresno": 0, ...} rather than raw db2 keys.
    """
    logger.info("{} Syncing provider inventory from WPS.".format(_LOG_PREFIX))

    wps_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
    ).first()
    if not wps_provider:
        logger.info("{} No WPS provider found.".format(_LOG_PREFIX))
        return

    mappings = list(src_models.BrandWpsBrandMapping.objects.select_related("brand", "wps_brand"))
    if not mappings:
        logger.info("{} No BrandWpsBrandMapping found.".format(_LOG_PREFIX))
        return

    warehouse_names = {
        (w.db2_key or "").upper(): (w.name or "")
        for w in src_models.WpsWarehouse.objects.all()
    }
    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=wps_provider).values(
            "id", "provider_external_id"
        )
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
                src_models.WpsItem.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
                .order_by("id")
                .values(
                    "id", "brand_id", "sku", "name", "supplier_product_id", "upc", "status",
                    "superseded_sku",
                    "inventory__ca_warehouse", "inventory__ga_warehouse", "inventory__id_warehouse",
                    "inventory__in_warehouse", "inventory__pa_warehouse", "inventory__pa2_warehouse",
                    "inventory__tx_warehouse", "inventory__total",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            to_upsert = []
            for row in batch:
                ext_id = _wps_provider_external_id(row["brand_id"], row.get("sku"))
                provider_part = provider_parts.get(ext_id)
                if not provider_part:
                    continue
                inv = {
                    col: row.get("inventory__{}".format(col))
                    for col in (
                        "ca_warehouse", "ga_warehouse", "id_warehouse", "in_warehouse",
                        "pa_warehouse", "pa2_warehouse", "tx_warehouse", "total",
                    )
                }
                to_upsert.append(
                    src_models.ProviderPartInventory(
                        provider_part=provider_part,
                        warehouse_total_qty=inv.get("total") or 0,
                        manufacturer_inventory=None,
                        manufacturer_esd=None,
                        warehouse_availability=_wps_warehouse_availability(inv, warehouse_names),
                        last_synced_at=now,
                        updated_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="WPS inventory batch={}".format(batch_num)
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

            pp_details_to_update = []
            for row in batch:
                ext_id = _wps_provider_external_id(row["brand_id"], row.get("sku"))
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = _wps_product_details(row)
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

            logger.info("{} WPS inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "wps_brand", _worker)
    logger.info("{} Synced {} WPS inventory records total.".format(_LOG_PREFIX, total_upserted))


_WPS_PRICING_VALUES = (
    "id", "company_id", "list_price", "dealer_price", "map_price",
    "item__brand_id", "item__sku", "item__supplier_product_id",
)
_WPS_PRICING_UPSERT_FIELDS = ["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"]


def _wps_pricing_master_keys(
    batch: typing.List[typing.Dict],
    wps_brand_to_brand: typing.Dict[int, src_models.Brands],
) -> typing.List[typing.Tuple[int, str]]:
    keys = []
    for row in batch:
        brand = wps_brand_to_brand.get(row.get("item__brand_id"))
        if not brand:
            continue
        part_number = _wps_master_part_number(
            row.get("item__supplier_product_id"), row.get("item__sku")
        )
        if not part_number:
            continue
        keys.append((brand.id, part_number))
    return keys


def _sync_wps_pricing(company_id: typing.Optional[int] = None) -> None:
    """Shared body for the all-companies and single-company WPS pricing syncs."""
    scope = "company_id={}".format(company_id) if company_id else "all companies"
    logger.info("{} Syncing provider pricing from WPS ({}).".format(_LOG_PREFIX, scope))

    wps_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
    ).first()
    if not wps_provider:
        logger.info("{} No WPS provider found.".format(_LOG_PREFIX))
        return

    mappings = list(src_models.BrandWpsBrandMapping.objects.select_related("brand", "wps_brand"))
    wps_brand_to_brand = {m.wps_brand_id: m.brand for m in mappings}
    if not wps_brand_to_brand:
        logger.info("{} No BrandWpsBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    company_provider = None
    watermark = None
    sync_started_at = timezone.now()
    if company_id:
        company_provider = src_models.CompanyProviders.objects.filter(
            company_id=company_id, provider_id=wps_provider.id
        ).first()
        watermark = company_provider.pricing_propagation_watermark if company_provider else None

    now = timezone.now()

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.WpsCompanyPricing.objects.filter(
                id__gt=last_id, item__brand_id__in=catalog_ids
            )
            if company_id:
                qs = qs.filter(company_id=company_id)
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(qs.order_by("id").values(*_WPS_PRICING_VALUES)[:BATCH_SIZE_PRICING])
            if not batch:
                break

            last_id = batch[-1]["id"]
            companies_by_id = {
                c.id: c
                for c in src_models.Company.objects.filter(
                    id__in={r["company_id"] for r in batch if r.get("company_id")}
                )
            }
            pp_by_key = _provider_parts_by_master_brand_and_part_number(
                wps_provider, _wps_pricing_master_keys(batch, wps_brand_to_brand)
            )

            to_upsert = []
            for row in batch:
                brand = wps_brand_to_brand.get(row.get("item__brand_id"))
                if not brand:
                    continue
                part_number = _wps_master_part_number(
                    row.get("item__supplier_product_id"), row.get("item__sku")
                )
                if not part_number:
                    continue
                provider_part = pp_by_key.get((brand.id, part_number))
                if not provider_part:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                # WPS returns mapp_price "0.00" on rows that carry no MAP policy; storing that
                # as a real 0.00 MAP would floor the price downstream.
                map_price = row.get("map_price")
                if not map_price:
                    map_price = None
                to_upsert.append(
                    src_models.ProviderPartCompanyPricing(
                        provider_part=provider_part,
                        company=company,
                        cost=row.get("dealer_price"),
                        jobber_price=row.get("dealer_price"),
                        map_price=map_price,
                        msrp=row.get("list_price"),
                        retail_price=row.get("list_price"),
                        last_synced_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="WPS pricing {} batch={}".format(scope, batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=_WPS_PRICING_UPSERT_FIELDS,
                )
                total_upserted += len(to_upsert)

            logger.info("{} WPS pricing {} batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, scope, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "wps_brand", _worker)
    logger.info("{} Synced {} WPS pricing records ({}).".format(_LOG_PREFIX, total_upserted, scope))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


def sync_provider_pricing_from_wps() -> None:
    """Sync ProviderPartCompanyPricing from WpsCompanyPricing (all companies)."""
    _sync_wps_pricing(company_id=None)


def sync_provider_pricing_from_wps_for_company(company_id: int) -> None:
    """Sync ProviderPartCompanyPricing from WpsCompanyPricing for one company."""
    _sync_wps_pricing(company_id=company_id)


def sync_derived_from_wps(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate WPS source data into MasterPart, ProviderPart, ProviderPartInventory and
    ProviderPartCompanyPricing. Call after the WPS raw ingest. No fitment sync yet -- WPS does
    expose /vehicles per item, but that needs its own vehicle-id mapping pass.
    """
    logger.info("{} Starting WPS-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_wps()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_wps()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_wps()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="WPS",
            continuation=_cont,
        )
    logger.info("{} Completed WPS-only derived sync.".format(_LOG_PREFIX))


def _wheelpros_provider_external_id(wp_brand_id: int, part_number: str) -> str:
    """Unique per WheelPros provider: wp_brand_id + part_number."""
    return "{}_{}".format(wp_brand_id, part_number)


def _wheelpros_provider_part_lookup(wp_provider):
    """
    Build two lookups for WheelPros ProviderPart: by provider_external_id, and by (brand_id, sku).
    Use ext_id first, then (brand_id, sku) so inventory/pricing resolve like master parts (sku then part_number).
    Uses .values() to avoid loading full ORM objects + select_related master_part into memory.
    """
    by_ext_id: typing.Dict[str, src_models.ProviderPart] = {}
    by_brand_sku: typing.Dict[typing.Tuple[int, str], src_models.ProviderPart] = {}
    qs = (
        src_models.ProviderPart.objects
        .filter(provider=wp_provider)
        .values("id", "provider_external_id", "master_part__brand_id", "master_part__sku")
    )
    for row in qs:
        pp_stub = src_models.ProviderPart(id=row["id"])
        ext_id = row["provider_external_id"]
        if ext_id:
            by_ext_id[ext_id] = pp_stub
        brand_id = row["master_part__brand_id"]
        sku = (row["master_part__sku"] or "").strip() if row["master_part__sku"] else ""
        if brand_id and sku:
            key = (brand_id, sku)
            if key not in by_brand_sku:
                by_brand_sku[key] = pp_stub
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

        # Neither the sku bridge nor the exact part number placed these. WheelPros ships no
        # barcode, so only case/whitespace differences can clear the guards here -- punctuation
        # differences need a matching GTIN and will fall through to creating a new MasterPart.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=wp_provider.id,
            provider_kind=src_enums.BrandProviderKind.WHEELPROS.value,
            external_id_by_key={v: k for k, v in wp_external_id_to_brand_part.items()},
        )

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

        # Phase 2: UPDATE existing parts with ONLY aaia_code via raw SQL (never sku/description/image_url).
        # sku is set once at creation and never overwritten by later syncs, since it's shared across
        # providers with different formats (e.g. brand-prefixed vs bare part numbers) and is used as a
        # matching key — overwriting it here causes cross-provider matching to flip-flop and spawn
        # duplicate MasterPart rows. Keeps existing description and image_url from primary source
        # (e.g. Turn14, catalog). Same as Keystone.
        if existing_keys:
            key_to_mp = {(mp.brand_id, mp.part_number): mp for mp in master_parts}
            for i in range(0, len(existing_keys), WHEELPROS_LOOKUP_CHUNK):
                chunk_keys = existing_keys[i : i + WHEELPROS_LOOKUP_CHUNK]
                values = [
                    (existing_by_key[k], key_to_mp[k].aaia_code)
                    for k in chunk_keys
                ]
                placeholders = ", ".join(["(%s::bigint, %s)"] * len(values))
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

        # (brand_id, part_number) -> MasterPart; id_to_mp already filled from lookup above
        brand_part_to_master = {}
        for (b_id, p_num), mp_id in existing_by_key.items():
            if mp_id is None:
                continue
            if mp_id in id_to_mp:
                brand_part_to_master[(b_id, p_num)] = id_to_mp[mp_id]
            else:
                # Resolved on the normalized part number, so neither the sku nor the exact
                # lookup above preloaded a stub for this id.
                mp = src_models.MasterPart()
                mp.id = mp_id
                mp.brand_id = b_id
                brand_part_to_master[(b_id, p_num)] = mp
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


_WHEELPROS_INV_ORDER_TYPE_LABELS: typing.Dict[str, str] = {
    "BL": "Blanks",
    "BW": "Blowout",
    "CS": "Custom Shop",
    "DB": "Drill from Blank",
    "IT": "International Only",
    "N2": "Subject to Inventory (Discontinued)",
    "NW": "New",
    "RG": "Regional",
    "RW": "Race Wheels",
    "SO": "Special Order",
    "ST": "Stocking (Standard)",
}


def _wheelpros_product_details(row: typing.Dict, brand_name: typing.Optional[str] = None) -> typing.List[typing.Dict]:
    """
    Build the product_details list for a WheelProsPart row.
    inv_order_type is mapped to a human-readable label.
    """
    def _decimal(val):
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    raw_inv_type = (row.get("inv_order_type") or "").strip()
    inv_type_label = _WHEELPROS_INV_ORDER_TYPE_LABELS.get(raw_inv_type, raw_inv_type) if raw_inv_type else None

    return [
        {"key": "sku",             "label": "SKU",              "value": row.get("part_number") or None},
        {"key": "brand",           "label": "Brand",            "value": brand_name or None},
        {"key": "description",     "label": "Description",      "value": row.get("part_description") or None},
        {"key": "image_url",       "label": "Image",            "value": row.get("image_url") or None},
        {"key": "inv_order_type",  "label": "Inventory Type",   "value": inv_type_label},
        {"key": "shipping_weight", "label": "Shipping Weight",  "value": _decimal(row.get("shipping_weight"))},
        {"key": "finish",          "label": "Finish",           "value": row.get("finish") or None},
        {"key": "size",            "label": "Size",             "value": row.get("size") or None},
        {"key": "bolt_pattern",    "label": "Bolt Pattern",     "value": row.get("bolt_pattern") or None},
        {"key": "offset",          "label": "Offset",           "value": row.get("offset") or None},
        {"key": "center_bore",     "label": "Center Bore",      "value": row.get("center_bore") or None},
        {"key": "load_rating",     "label": "Load Rating",      "value": row.get("load_rating") or None},
    ]


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

    wheelpros_brand_id_to_name = {m.wheelpros_brand_id: m.wheelpros_brand.name for m in mappings}

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
                .values(
                    "id", "brand_id", "part_number", "total_qoh", "warehouse_availability",
                    "part_description", "image_url", "inv_order_type", "shipping_weight",
                    "finish", "size", "bolt_pattern", "offset", "center_bore", "load_rating",
                )[:BATCH_SIZE_INVENTORY]
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

            # Update product_details and is_discontinued on ProviderPart
            pp_details_to_update = []
            for row in batch:
                part_number = (row.get("part_number") or "").strip()
                if not part_number:
                    continue
                ext_id = _wheelpros_provider_external_id(row["brand_id"], part_number)
                provider_part = provider_parts_by_ext_id.get(ext_id) or provider_parts_by_brand_sku.get(
                    (row["brand_id"], part_number)
                )
                if provider_part:
                    provider_part.product_details = _wheelpros_product_details(row, brand_name=wheelpros_brand_id_to_name.get(row.get("brand_id")))
                    provider_part.is_discontinued = (row.get("inv_order_type") or "").strip() == "N2"
                    pp_details_to_update.append(provider_part)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details", "is_discontinued"], batch_size=500
                )

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
                    creds_by_company[cpr.company_id] = credentials_helper.get_feed_credentials(cpr)
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

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider__kind=src_enums.BrandProviderKind.KEYSTONE.value,
    ).first()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} Keystone pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.KeystoneCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs
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

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


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

    # Watermark: meyer.py's raw-pricing upsert only bumps MeyerCompanyPricing.updated_at when a
    # row's price actually changed (see _flush_buf), so filtering on it here means we only walk
    # what changed since the last successful propagation instead of the whole per-company raw
    # table every cycle -- this is what was blowing up memory (16 parallel workers each paging
    # through the full, mostly-unchanged, dataset). Captured at the START of this run, before
    # querying, so it's a safe (if slightly conservative) lower bound for next time regardless
    # of how long this run takes. Null (never propagated / row missing) means "process everything",
    # matching prior behavior.
    sync_started_at = timezone.now()
    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider__kind=src_enums.BrandProviderKind.MEYER.value,
    ).first()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} Meyer pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.MeyerCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs
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

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


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

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider__kind=src_enums.BrandProviderKind.DLG.value,
    ).first()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} DLG pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.DlgCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs
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

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


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


def _run_atech_provider_pricing_sync(
    company_id: typing.Optional[int], watermark: typing.Optional["timezone.datetime"] = None,
) -> int:
    """
    Fan out AtechCompanyPricing -> ProviderPartCompanyPricing.
    If company_id is None, process all companies (single pass by id, like Keystone / Rough Country).
    ``watermark``, when given (only meaningful with a specific company_id -- see
    sync_provider_pricing_from_atech_for_company), filters to rows changed since then instead of
    the whole per-company raw table every cycle -- see sync_provider_pricing_from_meyer_for_company
    for the full rationale.
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
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
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

    sync_started_at = timezone.now()
    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider__kind=src_enums.BrandProviderKind.ATECH.value,
    ).first()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} A-Tech pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    total = _run_atech_provider_pricing_sync(company_id, watermark=watermark)
    logger.info("{} Synced {} A-Tech pricing records for company_id={}.".format(
        _LOG_PREFIX, total, company_id,
    ))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


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

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider__kind=src_enums.BrandProviderKind.ROUGH_COUNTRY.value,
    ).first()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} Rough Country pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.RoughCountryCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs
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

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


def sync_provider_pricing_from_vossen_for_company(company_id: int) -> None:
    logger.info("{} Syncing Vossen provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    vossen_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.VOSSEN.value,
    ).first()
    if not vossen_provider:
        logger.info("{} No Vossen provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandVossenBrandMapping.objects.select_related("brand", "vossen_brand")
    )
    vossen_brand_to_brand = {m.vossen_brand_id: m.brand for m in mappings}
    if not vossen_brand_to_brand:
        logger.info("{} No BrandVossenBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider_id=vossen_provider.id
    ).first()
    creds = credentials_helper.get_feed_credentials(company_provider) if company_provider else {}

    now = timezone.now()

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} Vossen pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.VossenCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs
                .order_by("id")
                .values("id", "company_id", "price", "part__brand_id", "part__sku")[:BATCH_SIZE_PRICING]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            batch_company_ids = {r["company_id"] for r in batch if r.get("company_id")}
            companies_by_id = {
                c.id: c for c in src_models.Company.objects.filter(id__in=batch_company_ids)
            }
            master_keys = []
            for row in batch:
                brand = vossen_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = (row.get("part__sku") or "").strip()
                if not sku:
                    continue
                master_keys.append((brand.id, sku))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(vossen_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = vossen_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                sku = (row.get("part__sku") or "").strip()
                if not sku:
                    continue
                provider_part = pp_by_key.get((brand.id, sku))
                if not provider_part:
                    continue
                company = companies_by_id.get(row.get("company_id"))
                if not company:
                    continue
                price = row.get("price")
                cost = vossen_dealer_cost_from_price(price, creds)
                to_upsert.append(
                    src_models.ProviderPartCompanyPricing(
                        provider_part=provider_part,
                        company=company,
                        cost=cost,
                        jobber_price=None,
                        map_price=None,
                        msrp=price,
                        retail_price=price,
                        last_synced_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Vossen pricing company={} batch={}".format(company_id, batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} Vossen pricing company={} batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "vossen_brand", _worker)
    logger.info("{} Synced {} Vossen pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


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
    company_creds = credentials_helper.get_feed_credentials(company_cp) if company_cp else {}

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    watermark = company_cp.pricing_propagation_watermark if company_cp else None
    logger.info(
        "{} WheelPros pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.WheelProsCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs
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

    if company_cp:
        company_cp.pricing_propagation_watermark = sync_started_at
        company_cp.save(update_fields=["pricing_propagation_watermark"])


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


def _turn14_product_details(item_row: typing.Dict) -> typing.List[typing.Dict]:
    """
    Build the product_details list for a Turn14Items row.
    """
    return [
        {"key": "sku",                   "label": "SKU",             "value": item_row.get("part_number") or None},
        {"key": "brand",                 "label": "Brand",           "value": item_row.get("brand_name") or None},
        {"key": "description",           "label": "Description",     "value": item_row.get("part_description") or None},
        {"key": "ltl_freight_required",  "label": "LTL (Freight)",   "value": bool(item_row.get("ltl_freight_required"))},
        {"key": "clearance_item",        "label": "Final Sale",      "value": bool(item_row.get("clearance_item"))},
    ]


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

        # Update product_details on ProviderPart from Turn14Items fields
        batch_ext_ids = [inv["external_id"] for inv in batch if inv.get("external_id")]
        if batch_ext_ids:
            items_by_ext_id = {
                row["external_id"]: row
                for row in src_models.Turn14Items.objects.filter(
                    external_id__in=batch_ext_ids
                ).values("external_id", "part_number", "brand_name", "part_description", "ltl_freight_required", "clearance_item")
            }
            pp_details_to_update = []
            for inv in batch:
                ext_id = inv.get("external_id")
                if not ext_id:
                    continue
                pp = provider_parts.get(ext_id)
                item_row = items_by_ext_id.get(ext_id)
                if pp and item_row:
                    pp.product_details = _turn14_product_details(item_row)
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

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
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=turn14_provider).values("id", "provider_external_id")
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


def _keystone_product_details(row: typing.Dict, brand_name: typing.Optional[str] = None) -> typing.List[typing.Dict]:
    """
    Build the product_details list for a KeystoneParts row.
    Each entry: {key, label, value} — ready for FE to iterate and render.
    """
    upsable = bool(row.get("upsable"))

    def _decimal(val):
        return float(val) if val is not None else None

    kit_raw = row.get("kit_components") or ""
    kit_html = kit_raw.replace("|", "<br>") if kit_raw else None

    details = [
        {"key": "sku",              "label": "SKU",                       "value": row.get("vcpn") or None},
        {"key": "brand",            "label": "Brand",                     "value": brand_name or None},
        {"key": "description",      "label": "Description",               "value": row.get("long_description") or None},
        {"key": "parcel_eligible",  "label": "Parcel Eligible",           "value": upsable},
    ]

    # Shipping fee: only one applies depending on UPSable
    if upsable:
        details.append({
            "key": "ups_assessorial_fee",
            "label": "UPS Assessorial Fee (Drop Ship Orders)",
            "value": _decimal(row.get("ups_ground_assessorial")),
        })
    else:
        details.append({
            "key": "freight_fee",
            "label": "Freight Fee (Drop Ship Orders)",
            "value": _decimal(row.get("us_ltl")),
        })

    details += [
        {"key": "non_returnable",   "label": "Non-Returnable",            "value": bool(row.get("is_non_returnable"))},
        {"key": "hazmat",           "label": "HAZMAT",                    "value": bool(row.get("is_hazmat"))},
        {"key": "length_in",        "label": "Length (in)",               "value": _decimal(row.get("length"))},
        {"key": "width_in",         "label": "Width (in)",                "value": _decimal(row.get("width"))},
        {"key": "height_in",        "label": "Height (in)",               "value": _decimal(row.get("height"))},
        {"key": "weight_lbs",       "label": "Weight (lbs)",              "value": _decimal(row.get("weight"))},
        {"key": "is_kit",           "label": "Kit",                       "value": bool(row.get("is_kit"))},
        {"key": "kit_components",   "label": "Kit Components",            "value": kit_html},
    ]

    return details


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

    keystone_brand_id_to_name = {m.keystone_brand_id: m.keystone_brand.name for m in mappings}

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=keystone_provider).values("id", "provider_external_id")
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
                    "id", "brand_id", "vcpn", "long_description", "total_qty",
                    "east_qty", "midwest_qty", "california_qty", "southeast_qty",
                    "pacific_nw_qty", "texas_qty", "great_lakes_qty", "florida_qty",
                    # product_details fields
                    "upsable", "ups_ground_assessorial", "us_ltl",
                    "is_non_returnable", "is_hazmat",
                    "length", "width", "height", "weight",
                    "is_kit", "kit_components",
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

            # Update product_details on ProviderPart (product metadata, not inventory)
            pp_details_to_update = []
            for kp in batch:
                if kp["vcpn"] in provider_parts:
                    pp = provider_parts[kp["vcpn"]]
                    pp.product_details = _keystone_product_details(kp, brand_name=keystone_brand_id_to_name.get(kp.get("brand_id")))
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
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


def _meyer_product_details(row: typing.Dict, brand_name: typing.Optional[str] = None) -> typing.List[typing.Dict]:
    """
    Build the product_details list for a MeyerParts row.
    Boolean flags are stored as True/False; dimensions as float or None.
    """
    def _decimal(val):
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    return [
        {"key": "sku",                    "label": "SKU",                         "value": row.get("meyer_part") or None},
        {"key": "brand",                  "label": "Brand",                       "value": brand_name or None},
        {"key": "description",            "label": "Description",                 "value": row.get("description") or None},
        {"key": "is_discontinued",        "label": "Discontinued",                "value": bool(row.get("is_discontinued"))},
        {"key": "addtl_handling_charge",  "label": "Additional Handling Charge",  "value": bool(row.get("addtl_handling_charge"))},
        {"key": "is_oversize",            "label": "Oversize Parcel",             "value": bool(row.get("is_oversize"))},
        {"key": "is_ltl",                 "label": "LTL (Freight)",               "value": bool(row.get("is_ltl"))},
        {"key": "length_in",              "label": "Length (in)",                 "value": _decimal(row.get("length"))},
        {"key": "width_in",               "label": "Width (in)",                  "value": _decimal(row.get("width"))},
        {"key": "height_in",              "label": "Height (in)",                 "value": _decimal(row.get("height"))},
        {"key": "weight_lbs",             "label": "Weight (lbs)",                "value": _decimal(row.get("weight"))},
    ]


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

    meyer_brand_id_to_name = {m.meyer_brand_id: m.meyer_brand.name for m in mappings}

    # Load only id + provider_external_id to avoid materialising ~950k full ORM objects (~1-2 GB).
    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=meyer_provider).values("id", "provider_external_id")
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
                    "brand_id",
                    "meyer_part",
                    "description",
                    "available_qty",
                    "mfg_qty_available",
                    "is_stocking",
                    "is_special_order",
                    "inventory_ltl",
                    "is_discontinued",
                    "addtl_handling_charge",
                    "is_oversize",
                    "is_ltl",
                    "length",
                    "width",
                    "height",
                    "weight",
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

            # Update product_details and is_discontinued on ProviderPart.
            # batch_size=500 breaks each call into small SQL statements (~100 KB each)
            # instead of one massive CASE...WHEN statement for all 10k rows, which was
            # OOM-killing Postgres when 16 threads fired simultaneously.
            pp_details_to_update = []
            for mp in batch:
                ext = mp.get("meyer_part")
                if isinstance(ext, str):
                    ext = ext.strip()
                else:
                    ext = str(ext or "").strip()
                pp = provider_parts.get(ext)
                if pp:
                    pp.product_details = _meyer_product_details(mp, brand_name=meyer_brand_id_to_name.get(mp.get("brand_id")))
                    pp.is_discontinued = bool(mp.get("is_discontinued"))
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update,
                    ["product_details", "is_discontinued"],
                    batch_size=500,
                )

            logger.info("{} Meyer inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    # max_workers=4 instead of the global 16: Meyer's bulk_update(product_details) generates
    # large SQL statements (JSON per row). 16 concurrent threads were OOM-killing Postgres at
    # 627 MiB RSS. 4 workers × batch_size=500 keeps peak SQL in-flight at ~400 KB total.
    total_upserted = _run_parallel_mapped_brand_int_worker(
        mappings, "meyer_brand", _worker, max_workers=4
    )
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
    After ``sync_master_parts_from_*``, run provider inventory + pricing, then optionally a full
    Meilisearch reindex (used rarely; the scheduled job uses a single reindex at the end of
    ``ingest_all_providers``). No-op reindex when Meilisearch is not configured.
    """
    from src.search import meilisearch_client as meilisearch_client

    if not reindex_meilisearch:
        continuation()
        return

    continuation()
    if not meilisearch_client.is_configured():
        return
    ok, fail = meilisearch_client.reindex_all_master_parts()
    logger.info(
        "{} Meilisearch reindex after {} master parts: indexed={} failed={}".format(
            _LOG_PREFIX, provider_label, ok, fail
        )
    )


def sync_derived_from_turn14(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Turn14 source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after Turn14 item/catalog fetches so the unified
    layer stays aligned without waiting for the global ``sync_all_master_parts`` job.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path;
    pricing handled separately via IntegrationPricingSyncJob queue).
    """
    logger.info("{} Starting Turn14-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_turn14()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_turn14()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_turn14()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Turn14",
            continuation=_cont,
        )
    logger.info("{} Completed Turn14-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_keystone(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Keystone source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after Keystone inventory/CSV fetches.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting Keystone-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_keystone()
        connection.close()
        # Needs every Keystone ProviderPart to exist first (see sync_keystone_kit_components'
        # own docstring for why it can't run per-brand) -- only worth re-running when master
        # parts themselves were just resynced, not on the fast inventory/pricing-only path.
        sync_keystone_kit_components()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_keystone()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_keystone()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Keystone",
            continuation=_cont,
        )
    logger.info("{} Completed Keystone-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_rough_country(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Rough Country source data into MasterPart, ProviderPart, ProviderPartInventory,
    MasterPartFitment, and ProviderPartCompanyPricing. Call after Rough Country feed ingest.

    Pass ``skip_master_parts=True`` to run only inventory + fitment + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory + fitment (global catalog sync path).
    """
    logger.info("{} Starting Rough Country-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_rough_country()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_rough_country()
        connection.close()
        sync_master_part_fitments_from_rough_country()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_rough_country()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Rough Country",
            continuation=_cont,
        )
    logger.info("{} Completed Rough Country-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_vossen(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Vossen source data into MasterPart, ProviderPart, ProviderPartInventory, and
    ProviderPartCompanyPricing. Call after Vossen feed ingest. No fitment sync -- the Vossen
    feed carries wheel-spec attributes (diameter/width/offset/bolt_pattern/center_bore), not
    vehicle fitment.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting Vossen-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_vossen()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_vossen()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_vossen()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Vossen",
            continuation=_cont,
        )
    logger.info("{} Completed Vossen-only derived sync.".format(_LOG_PREFIX))


# ============================================================
# TIRERACK
# ============================================================

def _tirerack_provider_external_id(tirerack_brand_id: int, part_number: str) -> str:
    """Stable TireRack ProviderPart key: tirerack brand row id + part number (same PN can exist under multiple feed brands)."""
    pn = (part_number or "").strip()
    return "{}_{}".format(int(tirerack_brand_id), pn)


def _ingest_tirerack_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    tirerack_provider: src_models.Providers,
    tirerack_brand_to_brand: typing.Dict[int, src_models.Brands],
    tirerack_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
) -> typing.Tuple[int, int]:
    """Batch-ingest TireRackParts for one disjoint set of TireRack catalog brand PKs. Mirrors _ingest_dlg_parts_for_mapped_brands."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.TireRackParts.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "brand_id",
                "part_number",
                "description",
                "updated_at",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        tirerack_external_to_brand_part = {}

        for row in batch:
            brand = tirerack_brand_to_brand.get(row["brand_id"])
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
                aaia = tirerack_brand_to_aaia.get(row["brand_id"])
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=row.get("description"),
                        aaia_code=aaia,
                        image_url=None,
                    )
                )
            tirerack_external_to_brand_part[
                _tirerack_provider_external_id(row["brand_id"], part_number)
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

        # TireRack keeps hyphens/'+' that other feeds drop ('964SB-29051-00' vs '964SB2905100'), so anything the exact
        # match missed gets a second, heavily-guarded pass on the normalized form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=tirerack_provider.id,
            provider_kind=src_enums.BrandProviderKind.TIRERACK.value,
            external_id_by_key={v: k for k, v in tirerack_external_to_brand_part.items()},
        )

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]
        existing_keys = [k for k in pairs if k in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="TireRack new_parts batch={}".format(batch_num)
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
                (existing_by_key[k], key_to_mp[k].aaia_code)
                for k in existing_keys
            ]
            placeholders = ", ".join(["(%s::bigint, %s)"] * len(values))
            params = [x for row_vals in values for x in row_vals]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET aaia_code = v.aaia_code
                    FROM (VALUES {}) AS v(id, aaia_code)
                    WHERE mp.id = v.id
                    """.format(placeholders),
                    params,
                )

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart
        # under the feed's own spelling, so the exact-part_number query below cannot find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
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
            ext_id = _tirerack_provider_external_id(row["brand_id"], pn)
            key_bp = tirerack_external_to_brand_part.get(ext_id)
            if not key_bp:
                continue
            master_part = brand_part_to_master.get(key_bp)
            if not master_part:
                continue
            pp_key = (master_part.id, tirerack_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=tirerack_provider,
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

        logger.info("{} TireRack batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_tirerack() -> None:
    """
    Create/update MasterPart and ProviderPart from TireRackParts.
    Only rows whose TireRackBrand has a BrandTireRackBrandMapping are included.

    Resolution uses (catalog brand_id, part_number) only, same as DLG.
    ``MasterPart.sku`` is set to ``part_number`` for consistency with other single-key feeds.
    ``ProviderPart.provider_external_id`` is ``f"{tirerack_brand_id}_{part_number}"`` so inventory joins stay unique.
    """
    logger.info("{} Syncing master parts from TireRack (batched, part_number only).".format(_LOG_PREFIX))

    tirerack_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TIRERACK.value,
    ).first()
    if not tirerack_provider:
        logger.info("{} No TireRack provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandTireRackBrandMapping.objects.select_related("brand", "tirerack_brand")
    )
    tirerack_brand_to_brand = {m.tirerack_brand_id: m.brand for m in mappings}
    tirerack_brand_to_aaia = {
        m.tirerack_brand_id: (m.brand.aaia_code if m.brand else None)
        for m in mappings
    }
    if not tirerack_brand_to_brand:
        logger.info("{} No BrandTireRackBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "tirerack_brand",
        lambda cids: _ingest_tirerack_parts_for_mapped_brands(
            cids,
            tirerack_provider,
            tirerack_brand_to_brand,
            tirerack_brand_to_aaia,
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from TireRack total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _tirerack_product_details(row: typing.Dict, brand_name: typing.Optional[str] = None) -> typing.List[typing.Dict]:
    return [
        {"key": "sku",                  "label": "SKU",                  "value": row.get("part_number") or None},
        {"key": "brand",                "label": "Brand",                "value": brand_name},
        {"key": "description",          "label": "Description",          "value": row.get("description") or None},
        {"key": "country_of_origin",    "label": "Country of Origin",    "value": row.get("country_of_origin") or None},
        {"key": "fet",                  "label": "FET",                  "value": str(row["fet"]) if row.get("fet") is not None else None},
        {"key": "base_price",           "label": "Base Price (excl. FET)", "value": str(row["base_price"]) if row.get("base_price") is not None else None},
        {"key": "road_hazard_warranty", "label": "Road Hazard Warranty", "value": row.get("road_hazard_warranty") or None},
        {"key": "treadlife_warranty_1", "label": "Treadlife Warranty 1", "value": row.get("treadlife_warranty_1") or None},
        {"key": "treadlife_warranty_2", "label": "Treadlife Warranty 2", "value": row.get("treadlife_warranty_2") or None},
        {"key": "treadlife_warranty_3", "label": "Treadlife Warranty 3", "value": row.get("treadlife_warranty_3") or None},
    ]


def sync_provider_inventory_from_tirerack() -> None:
    """Sync ProviderPartInventory from TireRackParts (mapped brands): totals + product_details (warranty/FET/country of origin)."""
    logger.info("{} Syncing provider inventory from TireRack.".format(_LOG_PREFIX))

    tirerack_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TIRERACK.value,
    ).first()
    if not tirerack_provider:
        logger.info("{} No TireRack provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandTireRackBrandMapping.objects.select_related("brand", "tirerack_brand")
    )
    if not mappings:
        logger.info("{} No BrandTireRackBrandMapping found.".format(_LOG_PREFIX))
        return

    tirerack_brand_id_to_name = {m.tirerack_brand_id: m.tirerack_brand.name for m in mappings}

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=tirerack_provider).values("id", "provider_external_id")
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
                src_models.TireRackParts.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
                .order_by("id")
                .values(
                    "id", "brand_id", "part_number", "quantity", "description",
                    "country_of_origin", "fet", "base_price",
                    "road_hazard_warranty", "treadlife_warranty_1", "treadlife_warranty_2", "treadlife_warranty_3",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            to_upsert = []
            for row in batch:
                pn = row.get("part_number") or ""
                pn = pn.strip() if isinstance(pn, str) else str(pn or "").strip()
                if not pn:
                    continue
                ext_id = _tirerack_provider_external_id(row["brand_id"], pn)
                provider_part = provider_parts.get(ext_id)
                if not provider_part:
                    continue
                qty = row.get("quantity")
                try:
                    total_qty = int(qty) if qty is not None else 0
                    wh_avail = {"available_on_hand": total_qty if qty is not None else None}
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
                    to_upsert, context="TireRack inventory batch={}".format(batch_num)
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

            # Update product_details on ProviderPart (product metadata, not inventory)
            pp_details_to_update = []
            for row in batch:
                pn = row.get("part_number") or ""
                pn = pn.strip() if isinstance(pn, str) else str(pn or "").strip()
                if not pn:
                    continue
                ext_id = _tirerack_provider_external_id(row["brand_id"], pn)
                pp = provider_parts.get(ext_id)
                if pp:
                    pp.product_details = _tirerack_product_details(
                        row, brand_name=tirerack_brand_id_to_name.get(row.get("brand_id"))
                    )
                    pp_details_to_update.append(pp)
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )

            logger.info("{} TireRack inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "tirerack_brand", _worker)
    logger.info("{} Synced {} TireRack inventory records total.".format(_LOG_PREFIX, total_upserted))


def _tirerack_company_pricing_batch_row_id_to_provider_part(
    batch: typing.List[typing.Dict],
    tirerack_provider: src_models.Providers,
) -> typing.Dict[int, src_models.ProviderPart]:
    ext_ids = {
        _tirerack_provider_external_id(row["part__brand_id"], row["part__part_number"])
        for row in batch
    }
    pp_by_ext_id = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(
            provider=tirerack_provider, provider_external_id__in=ext_ids
        ).values("id", "provider_external_id")
    }
    out: typing.Dict[int, src_models.ProviderPart] = {}
    for row in batch:
        ext_id = _tirerack_provider_external_id(row["part__brand_id"], row["part__part_number"])
        pp = pp_by_ext_id.get(ext_id)
        if pp:
            out[row["id"]] = pp
    return out


def sync_provider_pricing_from_tirerack_for_company(company_id: int) -> None:
    """
    Write ProviderPartCompanyPricing for one company from that company's own
    TireRackCompanyPricing rows (cost = Total Price; all other pricing fields left null --
    GTIN/MAP/Retail/Jobber/Core aren't in TireRack's feed). Each company has its own TireRack
    dealer account/feed (same pattern as DLG) -- see
    tirerack.sync_tirerack_company_pricing_for_company_provider, which populates
    TireRackCompanyPricing from that company's own SFTP pull.
    """
    logger.info("{} Syncing TireRack provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    tirerack_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TIRERACK.value,
    ).first()
    if not tirerack_provider:
        logger.info("{} No TireRack provider found.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider__kind=src_enums.BrandProviderKind.TIRERACK.value,
    ).first()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} TireRack pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    total_upserted = 0
    batch_num = 0
    last_id = 0
    while True:
        batch_num += 1
        qs = src_models.TireRackCompanyPricing.objects.filter(id__gt=last_id, company_id=company_id)
        if watermark:
            qs = qs.filter(updated_at__gte=watermark)
        batch = list(
            qs
            .order_by("id")
            .values("id", "total_price", "part__brand_id", "part__part_number")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break
        last_id = batch[-1]["id"]

        row_id_to_pp = _tirerack_company_pricing_batch_row_id_to_provider_part(batch, tirerack_provider)
        pricing_by_pp: typing.Dict[int, src_models.ProviderPartCompanyPricing] = {}
        for row in batch:
            pp = row_id_to_pp.get(row["id"])
            if not pp:
                continue
            pricing_by_pp[pp.id] = src_models.ProviderPartCompanyPricing(
                provider_part=pp,
                company_id=company_id,
                cost=row.get("total_price"),
                jobber_price=None,
                map_price=None,
                msrp=None,
                retail_price=None,
                last_synced_at=now,
            )

        to_upsert = list(pricing_by_pp.values())
        if to_upsert:
            pgbulk.upsert(
                src_models.ProviderPartCompanyPricing,
                to_upsert,
                unique_fields=["provider_part", "company"],
                update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
            )
            total_upserted += len(to_upsert)

        logger.info("{} TireRack pricing company={} batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id,
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} TireRack pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


def sync_provider_pricing_from_tirerack() -> None:
    """Sync ProviderPartCompanyPricing from TireRackCompanyPricing (each company's own SFTP pull) for every connected company."""
    logger.info("{} Syncing provider pricing from TireRack (all companies).".format(_LOG_PREFIX))

    tirerack_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TIRERACK.value,
    ).first()
    if not tirerack_provider:
        logger.info("{} No TireRack provider found.".format(_LOG_PREFIX))
        return

    now = timezone.now()
    total_upserted = 0
    batch_num = 0
    last_id = 0
    while True:
        batch_num += 1
        batch = list(
            src_models.TireRackCompanyPricing.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "company_id", "total_price", "part__brand_id", "part__part_number")[:BATCH_SIZE_PRICING]
        )
        if not batch:
            break
        last_id = batch[-1]["id"]

        row_id_to_pp = _tirerack_company_pricing_batch_row_id_to_provider_part(batch, tirerack_provider)
        pricing_by_pp_company: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartCompanyPricing] = {}
        for row in batch:
            pp = row_id_to_pp.get(row["id"])
            if not pp:
                continue
            pricing_by_pp_company[(pp.id, row["company_id"])] = src_models.ProviderPartCompanyPricing(
                provider_part=pp,
                company_id=row["company_id"],
                cost=row.get("total_price"),
                jobber_price=None,
                map_price=None,
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

        logger.info("{} TireRack pricing batch {}: {} records (last_id={})".format(
            _LOG_PREFIX, batch_num, len(to_upsert), last_id,
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_PRICING:
            time.sleep(BATCH_DELAY_SECONDS)

    logger.info("{} Synced {} TireRack pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_derived_from_tirerack(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate TireRack source data into MasterPart, ProviderPart, ProviderPartInventory, and
    ProviderPartCompanyPricing. Call after fetch_and_save_tirerack_catalog.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting TireRack-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_tirerack()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_tirerack()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_tirerack()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="TireRack",
            continuation=_cont,
        )
    logger.info("{} Completed TireRack-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_wheelpros(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate WheelPros source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after WheelPros CSV ingest.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting WheelPros-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_wheelpros()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_wheelpros()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_wheelpros()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="WheelPros",
            continuation=_cont,
        )
    logger.info("{} Completed WheelPros-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_meyer(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Meyer source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after Meyer catalog / inventory ingest.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting Meyer-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_meyer()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_meyer()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_meyer()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Meyer",
            continuation=_cont,
        )
    logger.info("{} Completed Meyer-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_atech(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate A-Tech source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after A-Tech feed ingest.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting A-Tech-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_atech()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_atech()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_atech()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="A-Tech",
            continuation=_cont,
        )
    logger.info("{} Completed A-Tech-only derived sync.".format(_LOG_PREFIX))


def sync_derived_from_dlg(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate DLG source data into MasterPart, ProviderPart, ProviderPartInventory,
    and ProviderPartCompanyPricing. Call after DLG catalog ingest.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting DLG-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_dlg()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_dlg()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_dlg()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="DLG",
            continuation=_cont,
        )
    logger.info("{} Completed DLG-only derived sync.".format(_LOG_PREFIX))


# ============================================================
# PREMIER PERFORMANCE (APG Wholesale)
# ============================================================

def _premier_provider_external_id(premier_brand_id: int, premier_part_number: str) -> str:
    """Composite ProviderPart key: premier_brand row id + premier_part_number."""
    pn = (premier_part_number or "").strip()
    return "{}_{}".format(int(premier_brand_id), pn)


def _premier_warehouse_availability(row: typing.Dict) -> typing.Optional[typing.Dict]:
    """Build warehouse_availability dict using user-facing labels from ``constants.PREMIER_WAREHOUSE_QTY_FIELD_TO_LOCATION_LABEL``."""
    avail: typing.Dict[str, int] = {}
    for field, label in src_constants.PREMIER_WAREHOUSE_QTY_FIELD_TO_LOCATION_LABEL.items():
        v = row.get(field)
        if v is not None:
            try:
                avail[label] = int(v)
            except (TypeError, ValueError):
                avail[label] = 0
    return avail if avail else None


def _premier_product_details(row: typing.Dict, brand_name: typing.Optional[str] = None) -> typing.List[typing.Dict]:
    def _decimal(val):
        return float(val) if val is not None else None

    def _bool(val):
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            return val.strip().lower() in ("yes", "true", "1")
        return bool(val) if val is not None else False

    kit_raw = row.get("kit_component_list") or ""
    kit_html = kit_raw.replace("|", "<br>") if isinstance(kit_raw, str) and kit_raw else None

    return [
        {"key": "sku",                 "label": "SKU",                         "value": row.get("premier_part_number") or None},
        {"key": "brand",               "label": "Brand",                       "value": brand_name or None},
        {"key": "description",         "label": "Description",                 "value": row.get("long_description") or None},
        {"key": "ships_ltl",           "label": "Ships LTL",                   "value": _bool(row.get("ships_ltl"))},
        {"key": "drop_ship_fee",        "label": "Drop Ship Fee",               "value": _decimal(row.get("drop_ship_fee"))},
        {"key": "length_in",            "label": "Length (in)",                 "value": _decimal(row.get("length"))},
        {"key": "width_in",             "label": "Width (in)",                  "value": _decimal(row.get("width"))},
        {"key": "height_in",            "label": "Height (in)",                 "value": _decimal(row.get("height"))},
        {"key": "weight_lbs",           "label": "Weight (lbs)",                "value": _decimal(row.get("weight"))},
        {"key": "item_with_cores",      "label": "Item With Cores",             "value": _bool(row.get("item_with_cores"))},
        {"key": "is_kit",               "label": "Kit",                         "value": _bool(row.get("is_kit"))},
        {"key": "kit_components",       "label": "Kit Components",              "value": kit_html},
        {"key": "prop65_carcinogen",    "label": "Prop 65 Warning (Cancer)",            "value": _bool(row.get("prop65_carcinogen"))},
        {"key": "prop65_reproductive",  "label": "Prop 65 Warning (Reproductive Harm)", "value": _bool(row.get("prop65_reproductive_harm"))},
        {"key": "approved_line",        "label": "Approved Line",               "value": _bool(row.get("approved_line"))},
        {"key": "california_legal",     "label": "California Legal",            "value": _bool(row.get("california_legal"))},
        {"key": "inventory_status",     "label": "Inventory Status",            "value": row.get("inventory_status")},
        {"key": "pies_ems_code",        "label": "PIES EMS Code",               "value": row.get("pies_ems_code")},
        {"key": "minimum_order_qty",    "label": "Minimum Order Qty",           "value": row.get("minimum_order_qty")},
        {"key": "drop_shippable_mfg",              "label": "Drop Shippable from MFG",       "value": _bool(row.get("drop_shippable_from_mfg"))},
        {"key": "freight_cost",                    "label": "Freight Cost",                  "value": _decimal(row.get("freight_cost"))},
        {"key": "line_code",                       "label": "Line Code",                     "value": row.get("line_code") or None},
        {"key": "part_category",                   "label": "Part Category",                 "value": row.get("part_category") or None},
        {"key": "part_subcategory",                "label": "Part Subcategory",              "value": row.get("part_subcategory") or None},
        {"key": "part_terminology",                "label": "Part Terminology",              "value": row.get("part_terminology") or None},
        {"key": "vendor_enhanced_emissions_code",  "label": "Vendor Enhanced Emissions Code","value": row.get("vendor_enhanced_emissions_code") or None},
    ]


def _ingest_premier_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    premier_provider: src_models.Providers,
    pr_brand_to_brand: typing.Dict[int, src_models.Brands],
    pr_brand_to_aaia: typing.Dict[int, typing.Optional[str]],
) -> typing.Tuple[int, int]:
    """Batch-ingest PremierParts into MasterPart / ProviderPart for one disjoint set of Premier brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            src_models.PremierParts.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(
                "id",
                "premier_part_number",
                "brand_id",
                "brand_override_id",
                "mfg_part_number",
                "long_description",
                "image_url",
                "updated_at",
                "upc_code",
            )[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen: typing.Set[typing.Tuple[int, str]] = set()
        master_parts_list: typing.List[src_models.MasterPart] = []
        premier_ext_to_brand_part: typing.Dict[str, typing.Tuple[int, str]] = {}

        # brand_override (see PremierParts docstring / premier.resolve_wheelpros_bucket_brands)
        # takes precedence over the feed-brand-level BrandPremierBrandMapping for whichever rows
        # have it set -- fetched once per batch, not per row.
        override_ids = {row["brand_override_id"] for row in batch if row.get("brand_override_id")}
        override_brands = (
            {b.id: b for b in src_models.Brands.objects.filter(id__in=override_ids)}
            if override_ids else {}
        )

        for row in batch:
            override_brand = override_brands.get(row.get("brand_override_id"))
            if override_brand:
                brand_id = override_brand.id
                aaia = override_brand.aaia_code
            else:
                brand = pr_brand_to_brand.get(row["brand_id"])
                if not brand:
                    continue
                brand_id = brand.id
                aaia = pr_brand_to_aaia.get(row["brand_id"])

            part_number = row.get("mfg_part_number") or ""
            if isinstance(part_number, str):
                part_number = part_number.strip()
            else:
                part_number = str(part_number or "").strip()
            if not part_number:
                continue

            premier_pn = row.get("premier_part_number") or ""
            if isinstance(premier_pn, str):
                premier_pn = premier_pn.strip()
            else:
                premier_pn = str(premier_pn or "").strip()
            if not premier_pn:
                continue

            key = (brand_id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts_list.append(
                    src_models.MasterPart(
                        brand_id=brand_id,
                        part_number=part_number,
                        sku=premier_pn,
                        description=row.get("long_description"),
                        aaia_code=aaia,
                        image_url=row.get("image_url"),
                        gtin=row.get("upc_code") or None,
                    )
                )
            premier_ext_to_brand_part[
                _premier_provider_external_id(row["brand_id"], premier_pn)
            ] = (brand_id, part_number)

        if not master_parts_list:
            connection.close()
            if len(batch) == BATCH_SIZE_MASTER_PARTS:
                time.sleep(BATCH_DELAY_SECONDS)
            continue

        pairs = list(seen)
        existing_by_key: typing.Dict[typing.Tuple[int, str], int] = {}
        if pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(pairs),),
                )
                for sql_row in cur.fetchall():
                    mp_id, b_id, p_num = sql_row
                    existing_by_key[(b_id, p_num)] = mp_id

        # Premier spells manufacturer part numbers in its own house style, so anything the exact
        # match missed gets a second, heavily-guarded pass on the normalized form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts_list},
            provider_id=premier_provider.id,
            provider_kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
            external_id_by_key={v: k for k, v in premier_ext_to_brand_part.items()},
        )

        new_parts = [mp for mp in master_parts_list if (mp.brand_id, mp.part_number) not in existing_by_key]
        existing_keys = [k for k in pairs if k in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="Premier new_parts batch={}".format(batch_num)
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
                (existing_by_key[k], key_to_mp[k].aaia_code, key_to_mp[k].image_url, key_to_mp[k].gtin)
                for k in existing_keys
            ]
            placeholders = ", ".join(["(%s::bigint, %s, %s, %s)"] * len(values))
            params = [x for t in values for x in t]
            with connection.cursor() as cur:
                cur.execute(
                    """
                    UPDATE master_parts mp SET
                        aaia_code = v.aaia_code,
                        image_url = CASE
                            WHEN v.image_url IS NOT NULL AND v.image_url != ''
                            THEN v.image_url
                            ELSE mp.image_url
                        END,
                        gtin = COALESCE(mp.gtin, v.gtin)
                    FROM (VALUES {}) AS v(id, aaia_code, image_url, gtin)
                    WHERE mp.id = v.id::bigint
                    """.format(placeholders),
                    params,
                )

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart
        # under the feed's own spelling, so the exact-part_number query below cannot find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
                )
                for sql_row in cur.fetchall():
                    mp_id, b_id, p_num = sql_row
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts_by_key: typing.Dict[typing.Tuple[int, int], src_models.ProviderPart] = {}
        for row in batch:
            premier_pn = row.get("premier_part_number") or ""
            if isinstance(premier_pn, str):
                premier_pn = premier_pn.strip()
            else:
                premier_pn = str(premier_pn or "").strip()
            if not premier_pn:
                continue
            ext_id = _premier_provider_external_id(row["brand_id"], premier_pn)
            key_bp = premier_ext_to_brand_part.get(ext_id)
            if not key_bp:
                continue
            master_part = brand_part_to_master.get(key_bp)
            if not master_part:
                continue
            pp_key = (master_part.id, premier_provider.id)
            provider_parts_by_key[pp_key] = src_models.ProviderPart(
                master_part=master_part,
                provider=premier_provider,
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

        logger.info("{} Premier batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, batch_num, len(batch), len(master_parts_list), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def sync_master_parts_from_premier() -> None:
    """
    Create/update MasterPart and ProviderPart from PremierParts.
    Only processes parts whose PremierBrand has a BrandPremierBrandMapping.
    Non-primary provider: INSERTs new parts (mfg_part_number as part_number, premier_part_number as
    sku), updates aaia_code on existing. Uses cursor-based pagination and parallel brand partitioning.
    """
    logger.info("{} Syncing master parts from Premier (batched, cursor-based).".format(_LOG_PREFIX))

    premier_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    if not premier_provider:
        logger.info("{} No Premier provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandPremierBrandMapping.objects.select_related("brand", "premier_brand")
    )
    pr_brand_to_brand = {m.premier_brand_id: m.brand for m in mappings}
    pr_brand_to_aaia = {
        m.premier_brand_id: (m.brand.aaia_code if m.brand else None)
        for m in mappings
    }
    if not pr_brand_to_brand:
        logger.info("{} No BrandPremierBrandMapping found. Nothing to sync.".format(_LOG_PREFIX))
        return

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    total_master, total_provider = _run_parallel_master_parts_ingest(
        chunks,
        mappings,
        "premier_brand",
        lambda cids: _ingest_premier_parts_for_mapped_brands(
            cids, premier_provider, pr_brand_to_brand, pr_brand_to_aaia
        ),
    )
    logger.info("{} Synced {} master parts and {} provider parts from Premier total.".format(
        _LOG_PREFIX, total_master, total_provider
    ))


def _parse_premier_kit_components(kit_component_list_raw: typing.Optional[str]) -> typing.List[typing.Tuple[str, int]]:
    """
    Decodes Premier's "Kit Component List" feed field into [(component_part_number, quantity),
    ...]. Confirmed live against real PremierParts rows: alternating pipe-delimited tokens --
    "PARTNUM|QTY|PARTNUM|QTY|..." -- NOT hyphen-joined pairs like Keystone's KitComponents.
    Premier part numbers routinely contain hyphens themselves (e.g. "SYN8863-10"), which is
    exactly why a hyphen-split (Keystone's approach) would be ambiguous here; alternating
    pipe-delimited tokens sidesteps that entirely. Malformed rows (odd token count, non-integer
    quantity) return as many valid leading pairs as parsed and drop the rest, rather than
    raising -- confirmed against the live catalog that ~0.15% of kit rows are malformed this
    way, and a partial link is better than losing the whole kit's decode over one bad token.
    """
    if not kit_component_list_raw:
        return []
    tokens = kit_component_list_raw.split("|")
    pairs: typing.List[typing.Tuple[str, int]] = []
    for i in range(0, len(tokens) - 1, 2):
        part_number = tokens[i].strip()
        qty_str = tokens[i + 1].strip()
        if not part_number:
            continue
        try:
            quantity = int(qty_str)
        except ValueError:
            continue
        if quantity <= 0:
            continue
        pairs.append((part_number, quantity))
    return pairs


def sync_premier_kit_components() -> None:
    """
    Decodes PremierParts.kit_component_list (see _parse_premier_kit_components) into real
    ProviderPartKitComponent rows against the same provider's own catalog, and sets
    ProviderPart.is_kit for each kit's own row. Run after sync_master_parts_from_premier --
    needs every Premier ProviderPart (kit AND component rows) to already exist.

    Component resolution prefers the SAME PremierBrand as the kit itself first -- confirmed
    live against real examples (e.g. kit "SYN8820-2000" under brand_id=547, whose components
    "SYN8863-10"/"SYN8855-02"/etc. all resolve under that same brand_id) -- since
    ProviderPart.provider_external_id for Premier is a composite
    "{premier_brand_id}_{premier_part_number}" key (see _premier_provider_external_id), not the
    bare part number Keystone's VCPN is. Falls back to a global (any-brand) lookup only when
    that part number is unambiguous across the whole Premier catalog (exactly one match);
    otherwise it's left unresolved rather than guessing which brand's part was meant --
    PremierParts.unique_together = ["premier_part_number", "brand"] means the same part number
    CAN legitimately recur under different brands elsewhere in the catalog even though the
    confirmed examples above didn't show that.

    See ProviderPartKitComponent's own docstring for why this exists at all: expanding a kit
    into its components ourselves, at add-to-cart time (see
    purchase_orders_services.add_cart_item, which is already provider-agnostic and needs no
    Premier-specific changes), avoids ever sending a kit's own item number to a distributor's
    order API. Unlike Keystone (confirmed-live rejection), Premier's own order API's kit
    behavior is untested/undocumented -- treated as unverified and risky by default, matching
    submit_order's existing no-dry-run caution, rather than assumed safe just because no
    failure has been observed yet.
    """
    logger.info("{} Syncing Premier kit components.".format(_LOG_PREFIX))

    premier_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    if not premier_provider:
        logger.info("{} No Premier provider found.".format(_LOG_PREFIX))
        return

    ext_id_to_pp_id: typing.Dict[str, int] = dict(
        src_models.ProviderPart.objects.filter(provider=premier_provider).values_list(
            "provider_external_id", "id"
        )
    )
    # Reverse index for the any-brand fallback -- part_number -> every ext_id it appears under,
    # so ambiguity (recurs under >1 brand) is detectable instead of silently picking one.
    part_number_to_ext_ids: typing.Dict[str, typing.List[str]] = {}
    for ext_id in ext_id_to_pp_id:
        _, _, part_number = ext_id.partition("_")
        part_number_to_ext_ids.setdefault(part_number, []).append(ext_id)

    kit_provider_part_ids: typing.Set[int] = set()
    # Keyed on (kit_part_id, component_part_id) rather than a plain list -- confirmed live that
    # Premier's own kit_component_list string can repeat the exact same component block 2-3x
    # within one kit row (e.g. "WCF100418-BRZ" lists component "WCF100129" three times over),
    # which pgbulk.upsert's single ON CONFLICT DO UPDATE statement cannot apply twice to the same
    # target row (psycopg2.errors.CardinalityViolation). Collapsing duplicates here, keeping the
    # max quantity seen for a pair, avoids both the crash and inflating quantity by summing what
    # is almost certainly a redundant feed listing rather than a genuine multi-line quantity.
    components_by_pair: typing.Dict[typing.Tuple[int, int], src_models.ProviderPartKitComponent] = {}
    kits_seen = 0
    kits_missing_own_part = 0
    components_unresolved = 0
    components_ambiguous = 0
    components_deduped = 0

    kit_rows = (
        src_models.PremierParts.objects.filter(is_kit=True)
        .exclude(kit_component_list__isnull=True)
        .exclude(kit_component_list="")
        .values("premier_part_number", "brand_id", "kit_component_list")
        .iterator(chunk_size=2000)
    )
    for row in kit_rows:
        kits_seen += 1
        kit_ext_id = _premier_provider_external_id(row["brand_id"], row["premier_part_number"])
        kit_part_id = ext_id_to_pp_id.get(kit_ext_id)
        if kit_part_id is None:
            kits_missing_own_part += 1
            continue
        kit_provider_part_ids.add(kit_part_id)

        for component_part_number, quantity in _parse_premier_kit_components(row["kit_component_list"]):
            same_brand_ext_id = _premier_provider_external_id(row["brand_id"], component_part_number)
            component_part_id = ext_id_to_pp_id.get(same_brand_ext_id)
            if component_part_id is None:
                candidates = part_number_to_ext_ids.get(component_part_number) or []
                if len(candidates) == 1:
                    component_part_id = ext_id_to_pp_id[candidates[0]]
                elif len(candidates) > 1:
                    components_ambiguous += 1
                    continue
                else:
                    components_unresolved += 1
                    continue
            pair_key = (kit_part_id, component_part_id)
            existing = components_by_pair.get(pair_key)
            if existing is not None:
                components_deduped += 1
                if quantity > existing.quantity:
                    existing.quantity = quantity
                continue
            components_by_pair[pair_key] = src_models.ProviderPartKitComponent(
                kit_part_id=kit_part_id, component_part_id=component_part_id, quantity=quantity
            )

    components = list(components_by_pair.values())
    if components:
        pgbulk.upsert(
            src_models.ProviderPartKitComponent,
            components,
            unique_fields=["kit_part", "component_part"],
            update_fields=["quantity"],
        )
    if kit_provider_part_ids:
        src_models.ProviderPart.objects.filter(id__in=kit_provider_part_ids).update(is_kit=True)

    logger.info(
        "{} Kit components: {} kit rows seen, {} linked components ({} duplicate pairs "
        "collapsed), {} kits with no matching ProviderPart, {} component part numbers "
        "unresolved, {} ambiguous across brands.".format(
            _LOG_PREFIX, kits_seen, len(components), components_deduped, kits_missing_own_part,
            components_unresolved, components_ambiguous,
        )
    )


def sync_provider_inventory_from_premier() -> None:
    """
    Sync ProviderPartInventory and ProviderPart.product_details from PremierParts.
    Warehouse quantities: nv_qty, ky_qty, mfg_qty, wa_qty.
    """
    logger.info("{} Syncing provider inventory from Premier.".format(_LOG_PREFIX))

    premier_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    if not premier_provider:
        logger.info("{} No Premier provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandPremierBrandMapping.objects.select_related("brand", "premier_brand")
    )
    if not mappings:
        logger.info("{} No BrandPremierBrandMapping found.".format(_LOG_PREFIX))
        return

    premier_brand_id_to_name = {m.premier_brand_id: m.premier_brand.name for m in mappings}

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=premier_provider).values("id", "provider_external_id")
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
                src_models.PremierParts.objects.filter(
                    id__gt=last_id,
                    brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "brand_id",
                    "premier_part_number",
                    "long_description",
                    "nv_qty",
                    "ky_qty",
                    "wa_qty",
                    "mfg_qty",
                    "usa_item_availability",
                    "inventory_status",
                    "ships_ltl",
                    "drop_ship_fee",
                    "length",
                    "width",
                    "height",
                    "weight",
                    "item_with_cores",
                    "is_kit",
                    "kit_component_list",
                    "prop65_carcinogen",
                    "prop65_reproductive_harm",
                    "approved_line",
                    "california_legal",
                    "pies_ems_code",
                    "minimum_order_qty",
                    "drop_shippable_from_mfg",
                    "freight_cost",
                    "line_code",
                    "part_category",
                    "part_subcategory",
                    "part_terminology",
                    "vendor_enhanced_emissions_code",
                )[:BATCH_SIZE_INVENTORY]
            )
            if not batch:
                break

            last_id = batch[-1]["id"]
            to_upsert: typing.List[src_models.ProviderPartInventory] = []
            pp_details_to_update: typing.List[src_models.ProviderPart] = []

            for kp in batch:
                premier_pn = (kp.get("premier_part_number") or "").strip()
                if not premier_pn:
                    continue
                ext_id = _premier_provider_external_id(kp["brand_id"], premier_pn)
                pp = provider_parts.get(ext_id)
                if not pp:
                    continue

                total_qty = kp.get("usa_item_availability") or 0

                to_upsert.append(
                    src_models.ProviderPartInventory(
                        provider_part=pp,
                        warehouse_total_qty=total_qty,
                        manufacturer_inventory=kp.get("mfg_qty"),
                        manufacturer_esd=None,
                        warehouse_availability=_premier_warehouse_availability(kp),
                        last_synced_at=now,
                        updated_at=now,
                    )
                )
                pp.product_details = _premier_product_details(kp, brand_name=premier_brand_id_to_name.get(kp.get("brand_id")))
                pp_details_to_update.append(pp)

            if to_upsert:
                to_upsert = _dedupe_provider_part_inventory_for_upsert(
                    to_upsert, context="Premier inventory batch={}".format(batch_num)
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
            if pp_details_to_update:
                src_models.ProviderPart.objects.bulk_update(
                    pp_details_to_update, ["product_details"], batch_size=500
                )
            total_upserted += len(to_upsert)

            logger.info("{} Premier inventory batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_INVENTORY:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "premier_brand", _worker)
    logger.info("{} Synced {} Premier inventory records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_premier() -> None:
    """
    Sync ProviderPartCompanyPricing from PremierCompanyPricing (all companies).
    customer_price -> cost, jobber_price -> jobber_price, map_price -> map_price.
    """
    logger.info("{} Syncing provider pricing from Premier.".format(_LOG_PREFIX))

    premier_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    if not premier_provider:
        logger.info("{} No Premier provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandPremierBrandMapping.objects.select_related("brand", "premier_brand")
    )
    pr_brand_to_brand = {m.premier_brand_id: m.brand for m in mappings}
    if not pr_brand_to_brand:
        logger.info("{} No BrandPremierBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
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
                src_models.PremierCompanyPricing.objects.filter(
                    id__gt=last_id,
                    part__brand_id__in=catalog_ids,
                )
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "customer_price",
                    "jobber_price",
                    "map_price",
                    "part__brand_id",
                    "part__mfg_part_number",
                    "part__retail_price",
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
                brand = pr_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__mfg_part_number") or ""
                if isinstance(pn, str):
                    pn = pn.strip()
                else:
                    pn = str(pn or "").strip()
                if not pn:
                    continue
                master_keys.append((brand.id, pn))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(premier_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = pr_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__mfg_part_number") or ""
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
                        cost=row.get("customer_price"),
                        jobber_price=row.get("jobber_price"),
                        map_price=row.get("map_price"),
                        msrp=row.get("part__retail_price"),
                        retail_price=None,
                        last_synced_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Premier pricing batch={}".format(batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} Premier pricing batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "premier_brand", _worker)
    logger.info("{} Synced {} Premier pricing records total.".format(_LOG_PREFIX, total_upserted))


def sync_provider_pricing_from_premier_for_company(company_id: int) -> None:
    logger.info("{} Syncing Premier provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    premier_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    if not premier_provider:
        logger.info("{} No Premier provider found.".format(_LOG_PREFIX))
        return

    mappings = list(
        src_models.BrandPremierBrandMapping.objects.select_related("brand", "premier_brand")
    )
    pr_brand_to_brand = {m.premier_brand_id: m.brand for m in mappings}
    if not pr_brand_to_brand:
        logger.info("{} No BrandPremierBrandMapping found. Nothing to price.".format(_LOG_PREFIX))
        return

    now = timezone.now()

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    # premier.py's raw-pricing upsert only bumps PremierCompanyPricing.updated_at when a row's
    # price actually changed, so filtering on it here means only walking what changed since the
    # last successful propagation instead of the whole per-company raw table every cycle.
    sync_started_at = timezone.now()
    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider__kind=src_enums.BrandProviderKind.PREMIER_PERFORMANCE.value,
    ).first()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} Premier pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    def _worker(catalog_ids: typing.Set[int]) -> int:
        if not catalog_ids:
            return 0
        total_upserted = 0
        batch_num = 0
        last_id = 0
        while True:
            batch_num += 1
            qs = src_models.PremierCompanyPricing.objects.filter(
                id__gt=last_id,
                company_id=company_id,
                part__brand_id__in=catalog_ids,
            )
            if watermark:
                qs = qs.filter(updated_at__gte=watermark)
            batch = list(
                qs
                .order_by("id")
                .values(
                    "id",
                    "company_id",
                    "customer_price",
                    "jobber_price",
                    "map_price",
                    "part__brand_id",
                    "part__mfg_part_number",
                    "part__retail_price",
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
                brand = pr_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__mfg_part_number") or ""
                if isinstance(pn, str):
                    pn = pn.strip()
                else:
                    pn = str(pn or "").strip()
                if not pn:
                    continue
                master_keys.append((brand.id, pn))
            pp_by_key = _provider_parts_by_master_brand_and_part_number(premier_provider, master_keys)

            to_upsert = []
            for row in batch:
                brand = pr_brand_to_brand.get(row.get("part__brand_id"))
                if not brand:
                    continue
                pn = row.get("part__mfg_part_number") or ""
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
                        cost=row.get("customer_price"),
                        jobber_price=row.get("jobber_price"),
                        map_price=row.get("map_price"),
                        msrp=row.get("part__retail_price"),
                        retail_price=None,
                        last_synced_at=now,
                    )
                )

            if to_upsert:
                to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                    to_upsert, context="Premier pricing company={} batch={}".format(company_id, batch_num)
                )
                pgbulk.upsert(
                    src_models.ProviderPartCompanyPricing,
                    to_upsert,
                    unique_fields=["provider_part", "company"],
                    update_fields=["cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"],
                )
                total_upserted += len(to_upsert)

            logger.info("{} Premier pricing company={} batch {}: {} records (last_id={})".format(
                _LOG_PREFIX, company_id, batch_num, len(to_upsert), last_id
            ))
            connection.close()
            if len(batch) == BATCH_SIZE_PRICING:
                time.sleep(BATCH_DELAY_SECONDS)
        return total_upserted

    total_upserted = _run_parallel_mapped_brand_int_worker(mappings, "premier_brand", _worker)
    logger.info("{} Synced {} Premier pricing records for company_id={}.".format(
        _LOG_PREFIX, total_upserted, company_id,
    ))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


def sync_derived_from_premier(*, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False) -> None:
    """
    Propagate Premier (APG Wholesale) source data into MasterPart, ProviderPart,
    ProviderPartInventory, and ProviderPartCompanyPricing. Call after Premier FTP ingest.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting Premier-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_premier()
        connection.close()
        # Needs every Premier ProviderPart to exist first (see sync_premier_kit_components' own
        # docstring) -- only worth re-running when master parts themselves were just resynced,
        # not on the fast inventory/pricing-only path. Same rule as Keystone's own kit sync.
        sync_premier_kit_components()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_premier()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_premier()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Premier",
            continuation=_cont,
        )
    logger.info("{} Completed Premier-only derived sync.".format(_LOG_PREFIX))


def _reclaim_memory() -> None:
    """
    Force Python to return freed heap to the OS between major sync phases.

    Python's allocator (pymalloc + glibc malloc) holds freed pages for reuse rather than
    returning them to the OS. After a large provider sync (e.g. Turn14 with 776k parts +
    inventory + pricing in parallel workers), RSS can be several GB even though objects are
    freed. By calling gc.collect() and malloc_trim(0) we get that memory back before the
    next provider starts, preventing cumulative OOM across the full sync_all_master_parts run.
    """
    collected = gc.collect()
    try:
        libc = ctypes.CDLL("libc.so.6")
        libc.malloc_trim(0)
        logger.info("{} _reclaim_memory: gc collected={} malloc_trim done".format(_LOG_PREFIX, collected))
    except Exception as e:
        logger.info("{} _reclaim_memory: gc collected={} malloc_trim unavailable ({})".format(
            _LOG_PREFIX, collected, e
        ))


# ============================================================
# ELITE WHEEL & TIRE
# ============================================================
# Elite is the only provider whose catalog lives in two source tables (EliteWheelPartWheel /
# EliteWheelPartTire) behind one provider and one brand mapping, because its workbook describes
# wheels and tires with disjoint column sets. Both halves are handled by the same parametrized
# ingest below rather than two near-identical copies; each half is identified by an
# ``_EliteWheelHalf`` spec.


def _elite_wheel_provider_external_id(elite_brand_id: int, part_number: str) -> str:
    """
    Stable Elite ProviderPart key: elite brand row id + part number, the same compound shape every
    other provider uses. No wheel/tire marker is needed -- EliteWheelBrand rows are per half
    (``WHEEL:AZARA`` vs ``TIRE:LEXANI``), so a brand id belongs to exactly one of the two tables.
    """
    return "{}_{}".format(int(elite_brand_id), (part_number or "").strip())


def _elite_wheel_inventory_warehouse_availability(row: typing.Dict) -> typing.Dict[str, typing.Any]:
    """
    Per-location stock from an Elite part row; keys are user-facing labels (see
    ``constants.ELITE_WHEEL_QTY_FIELD_TO_LOCATION_LABEL``). A location Elite adds before it gets a
    ``qty_*`` column still shows up here, carried through from the part's raw
    ``location_availability``, so new warehouses surface without a schema change.
    """
    availability = {
        label: row.get(field)
        for field, label in src_constants.ELITE_WHEEL_QTY_FIELD_TO_LOCATION_LABEL.items()
    }
    known_locations = set(src_constants.ELITE_WHEEL_LOCATION_TO_QTY_FIELD.keys())
    for location, qty in (row.get("location_availability") or {}).items():
        if str(location).strip() not in known_locations:
            availability[str(location).strip()] = qty
    return availability


def _elite_wheel_wheel_product_details(row: typing.Dict) -> typing.List[typing.Dict]:
    """Wheel-spec attributes for an EliteWheelPartWheel row, shown as ProviderPart.product_details."""
    bolt_patterns = [row.get("bolt_pattern_1"), row.get("bolt_pattern_2")]
    bolt_pattern = " / ".join([bp for bp in bolt_patterns if bp])
    return [
        {"key": "part_number", "label": "Part Number", "value": row.get("part_number") or None},
        {"key": "size", "label": "Size", "value": row.get("size") or None},
        {"key": "bolt_pattern", "label": "Bolt Pattern", "value": bolt_pattern or None},
        {"key": "offset", "label": "Offset", "value": row.get("offset") or None},
        {"key": "center_bore", "label": "Centerbore", "value": row.get("center_bore") or None},
        {"key": "finish", "label": "Finish", "value": row.get("finish") or None},
    ]


def _elite_wheel_tire_product_details(row: typing.Dict) -> typing.List[typing.Dict]:
    """Tire-spec attributes for an EliteWheelPartTire row, shown as ProviderPart.product_details."""
    return [
        {"key": "part_number", "label": "Part Number", "value": row.get("part_number") or None},
        {"key": "model", "label": "Model", "value": row.get("model") or None},
        {"key": "raw_size", "label": "Size", "value": row.get("raw_size") or None},
        {"key": "tire_diameter", "label": "Rim Diameter", "value": row.get("tire_diameter") or None},
    ]


class _EliteWheelHalf(typing.NamedTuple):
    """One side of Elite's split catalog, so both run through the same ingest code."""
    label: str                      # "wheel" / "tire", for log lines
    part_model: typing.Any          # EliteWheelPartWheel / EliteWheelPartTire
    pricing_model: typing.Any       # EliteWheelWheelCompanyPricing / EliteWheelTireCompanyPricing
    catalog_values: typing.Tuple[str, ...]
    inventory_values: typing.Tuple[str, ...]
    description_fields: typing.Tuple[str, ...]
    product_details: typing.Callable[[typing.Dict], typing.List[typing.Dict]]


def _elite_wheel_halves() -> typing.List[_EliteWheelHalf]:
    return [
        _EliteWheelHalf(
            label="wheel",
            part_model=src_models.EliteWheelPartWheel,
            pricing_model=src_models.EliteWheelWheelCompanyPricing,
            catalog_values=("id", "brand_id", "part_number", "group_label", "size", "finish", "updated_at"),
            inventory_values=(
                "id", "brand_id", "part_number", "total_available", "location_availability",
                "qty_tampa", "qty_atlanta", "qty_miami", "qty_decatur",
                "size", "bolt_pattern_1", "bolt_pattern_2", "offset", "center_bore", "finish",
            ),
            description_fields=("group_label", "size", "finish"),
            product_details=_elite_wheel_wheel_product_details,
        ),
        _EliteWheelHalf(
            label="tire",
            part_model=src_models.EliteWheelPartTire,
            pricing_model=src_models.EliteWheelTireCompanyPricing,
            catalog_values=("id", "brand_id", "part_number", "model", "raw_size", "tire_diameter", "updated_at"),
            inventory_values=(
                "id", "brand_id", "part_number", "total_available", "location_availability",
                "qty_tampa", "qty_atlanta", "qty_miami", "qty_decatur",
                "model", "raw_size", "tire_diameter",
            ),
            description_fields=("model", "raw_size"),
            product_details=_elite_wheel_tire_product_details,
        ),
    ]


def _elite_wheel_description(row: typing.Dict, half: _EliteWheelHalf) -> typing.Optional[str]:
    """
    MasterPart description for an Elite row. The workbook has no description column, so it is
    composed from the columns that actually identify the part to a human: for a wheel the
    model/finish block title plus size, for a tire the model plus its size code.
    """
    parts = [str(row.get(field)).strip() for field in half.description_fields if row.get(field)]
    # De-dupe while preserving order: a wheel's group_label ("XF-211 Gloss Black & Milled")
    # already ends with the finish, so appending it again would read as a stutter.
    seen: typing.List[str] = []
    for part in parts:
        if part and part not in " ".join(seen):
            seen.append(part)
    return " ".join(seen) or None


def _ingest_elite_wheel_parts_for_mapped_brands(
    mapped_catalog_brand_ids: typing.Set[int],
    elite_provider: src_models.Providers,
    elite_brand_to_brand: typing.Dict[int, src_models.Brands],
    half: _EliteWheelHalf,
) -> typing.Tuple[int, int]:
    """Batch-ingest one half's Elite part rows for one disjoint set of Elite catalog brand PKs."""
    if not mapped_catalog_brand_ids:
        return 0, 0

    total_master = 0
    total_provider = 0
    batch_num = 0
    last_id = 0

    while True:
        batch_num += 1
        batch = list(
            half.part_model.objects.filter(
                brand_id__in=mapped_catalog_brand_ids,
                id__gt=last_id,
            )
            .order_by("id")
            .values(*half.catalog_values)[:BATCH_SIZE_MASTER_PARTS]
        )
        if not batch:
            break

        last_id = batch[-1]["id"]
        seen = set()
        master_parts = []
        elite_external_id_to_brand_part = {}

        for row in batch:
            brand = elite_brand_to_brand.get(row["brand_id"])
            if not brand:
                continue

            part_number = (row.get("part_number") or "").strip()
            if not part_number:
                continue

            key = (brand.id, part_number)
            if key not in seen:
                seen.add(key)
                master_parts.append(
                    src_models.MasterPart(
                        brand=brand,
                        part_number=part_number,
                        sku=part_number,
                        description=_elite_wheel_description(row, half),
                    )
                )
            elite_external_id_to_brand_part[
                _elite_wheel_provider_external_id(row["brand_id"], part_number)
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
                for mp_id, b_id, p_num in cur.fetchall():
                    existing_by_key[(b_id, p_num)] = mp_id

        # Elite spells manufacturer part numbers in its own house style (offset suffixes like
        # "+12GBM" that other feeds punctuate differently), so anything the exact match missed
        # gets a second, heavily-guarded pass on the normalized form.
        mp_matching.extend_with_normalized_matches(
            existing_by_key,
            pairs,
            {(mp.brand_id, mp.part_number): mp.gtin for mp in master_parts},
            provider_id=elite_provider.id,
            provider_kind=src_enums.BrandProviderKind.ELITE_WHEEL.value,
            external_id_by_key={v: k for k, v in elite_external_id_to_brand_part.items()},
        )

        new_parts = [mp for mp in master_parts if (mp.brand_id, mp.part_number) not in existing_by_key]

        if new_parts:
            new_parts = _dedupe_master_parts_for_upsert(
                new_parts, context="Elite {} new_parts batch={}".format(half.label, batch_num)
            )
            pgbulk.upsert(
                src_models.MasterPart,
                new_parts,
                unique_fields=["brand", "part_number"],
                update_fields=[],
            )
            total_master += len(new_parts)

        # Seed from the resolved ids first: a row matched on formatting has no MasterPart under
        # the feed's own spelling, so the exact-part_number query below cannot find it.
        brand_part_to_master = mp_matching.master_part_stubs_for(existing_by_key)
        unresolved_pairs = [k for k in pairs if k not in brand_part_to_master]
        if unresolved_pairs:
            with connection.cursor() as cur:
                cur.execute(
                    "SELECT id, brand_id, part_number FROM master_parts WHERE (brand_id, part_number) IN %s",
                    (tuple(unresolved_pairs),),
                )
                for mp_id, b_id, p_num in cur.fetchall():
                    mp = src_models.MasterPart()
                    mp.id = mp_id
                    mp.brand_id = b_id
                    mp.part_number = p_num
                    brand_part_to_master[(b_id, p_num)] = mp

        provider_parts_by_key = {}
        for row in batch:
            ext_id = _elite_wheel_provider_external_id(
                row["brand_id"], (row.get("part_number") or "").strip()
            )
            key = elite_external_id_to_brand_part.get(ext_id)
            if not key:
                continue
            master_part = brand_part_to_master.get(key)
            if not master_part:
                continue
            provider_parts_by_key[(master_part.id, elite_provider.id)] = src_models.ProviderPart(
                master_part=master_part,
                provider=elite_provider,
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

        logger.info("{} Elite {} batch {}: {} items -> {} master, {} provider (last_id={})".format(
            _LOG_PREFIX, half.label, batch_num, len(batch), len(master_parts), len(provider_parts), last_id
        ))
        connection.close()
        if len(batch) == BATCH_SIZE_MASTER_PARTS:
            time.sleep(BATCH_DELAY_SECONDS)

    return total_master, total_provider


def _elite_wheel_provider_and_mappings(
    action: str,
) -> typing.Tuple[
    typing.Optional[src_models.Providers],
    typing.List[src_models.BrandEliteWheelBrandMapping],
    typing.Dict[int, src_models.Brands],
]:
    """Shared preamble for every Elite derived sync: provider row + brand mappings, or empties."""
    elite_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.ELITE_WHEEL.value,
    ).first()
    if not elite_provider:
        logger.info("{} No Elite Wheel provider found.".format(_LOG_PREFIX))
        return None, [], {}

    mappings = list(
        src_models.BrandEliteWheelBrandMapping.objects.select_related("brand", "elite_wheel_brand")
    )
    elite_brand_to_brand = {m.elite_wheel_brand_id: m.brand for m in mappings}
    if not elite_brand_to_brand:
        logger.info(
            "{} No BrandEliteWheelBrandMapping found. Nothing to {}.".format(_LOG_PREFIX, action)
        )
        return elite_provider, [], {}
    return elite_provider, mappings, elite_brand_to_brand


def sync_master_parts_from_elite_wheel() -> None:
    """
    Create/update MasterPart and ProviderPart from EliteWheelPartWheel and EliteWheelPartTire.
    Only processes parts whose EliteWheelBrand has a BrandEliteWheelBrandMapping. Same
    cursor-based, two-phase upsert pattern as every other provider, run once per half.
    """
    logger.info("{} Syncing master parts from Elite Wheel (batched, cursor-based).".format(_LOG_PREFIX))

    elite_provider, mappings, elite_brand_to_brand = _elite_wheel_provider_and_mappings("sync")
    if not elite_provider or not elite_brand_to_brand:
        return

    chunks = _partition_mapped_brands_for_parallel_ingest(mappings, MASTER_PARTS_SYNC_MAX_WORKERS)
    grand_master, grand_provider = 0, 0
    for half in _elite_wheel_halves():
        total_master, total_provider = _run_parallel_master_parts_ingest(
            chunks,
            mappings,
            "elite_wheel_brand",
            lambda cids, half=half: _ingest_elite_wheel_parts_for_mapped_brands(
                cids,
                elite_provider,
                elite_brand_to_brand,
                half,
            ),
        )
        grand_master += total_master
        grand_provider += total_provider
        logger.info("{} Elite {}: {} master parts, {} provider parts.".format(
            _LOG_PREFIX, half.label, total_master, total_provider
        ))

    logger.info("{} Synced {} master parts and {} provider parts from Elite Wheel total.".format(
        _LOG_PREFIX, grand_master, grand_provider
    ))


def sync_provider_inventory_from_elite_wheel() -> None:
    """
    Sync ProviderPartInventory from both Elite part tables: ``total_available`` as the total plus
    per-location stock in ``warehouse_availability``. Also refreshes ProviderPart.product_details
    with the wheel- or tire-spec attributes for that half.
    """
    logger.info("{} Syncing provider inventory from Elite Wheel.".format(_LOG_PREFIX))

    elite_provider, mappings, elite_brand_to_brand = _elite_wheel_provider_and_mappings("sync")
    if not elite_provider or not elite_brand_to_brand:
        return

    provider_parts = {
        row["provider_external_id"]: src_models.ProviderPart(id=row["id"])
        for row in src_models.ProviderPart.objects.filter(provider=elite_provider).values(
            "id", "provider_external_id"
        )
    }

    now = timezone.now()
    grand_total = 0

    for half in _elite_wheel_halves():

        def _worker(catalog_ids: typing.Set[int], half: _EliteWheelHalf = half) -> int:
            if not catalog_ids:
                return 0
            total_upserted = 0
            batch_num = 0
            last_id = 0
            while True:
                batch_num += 1
                batch = list(
                    half.part_model.objects.filter(id__gt=last_id, brand_id__in=catalog_ids)
                    .order_by("id")
                    .values(*half.inventory_values)[:BATCH_SIZE_INVENTORY]
                )
                if not batch:
                    break

                last_id = batch[-1]["id"]
                to_upsert = []
                for row in batch:
                    ext_id = _elite_wheel_provider_external_id(
                        row["brand_id"], (row.get("part_number") or "").strip()
                    )
                    provider_part = provider_parts.get(ext_id)
                    if not provider_part:
                        continue
                    to_upsert.append(
                        src_models.ProviderPartInventory(
                            provider_part=provider_part,
                            warehouse_total_qty=row.get("total_available") or 0,
                            manufacturer_inventory=None,
                            manufacturer_esd=None,
                            warehouse_availability=_elite_wheel_inventory_warehouse_availability(row),
                            last_synced_at=now,
                            updated_at=now,
                        )
                    )

                if to_upsert:
                    to_upsert = _dedupe_provider_part_inventory_for_upsert(
                        to_upsert,
                        context="Elite {} inventory batch={}".format(half.label, batch_num),
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

                pp_details_to_update = []
                for row in batch:
                    ext_id = _elite_wheel_provider_external_id(
                        row["brand_id"], (row.get("part_number") or "").strip()
                    )
                    pp = provider_parts.get(ext_id)
                    if pp:
                        pp.product_details = half.product_details(row)
                        pp_details_to_update.append(pp)
                if pp_details_to_update:
                    src_models.ProviderPart.objects.bulk_update(
                        pp_details_to_update, ["product_details"], batch_size=500
                    )

                logger.info("{} Elite {} inventory batch {}: {} records (last_id={})".format(
                    _LOG_PREFIX, half.label, batch_num, len(to_upsert), last_id
                ))
                connection.close()
                if len(batch) == BATCH_SIZE_INVENTORY:
                    time.sleep(BATCH_DELAY_SECONDS)
            return total_upserted

        grand_total += _run_parallel_mapped_brand_int_worker(mappings, "elite_wheel_brand", _worker)

    logger.info("{} Synced {} Elite Wheel inventory records total.".format(_LOG_PREFIX, grand_total))


_ELITE_WHEEL_PRICING_UPSERT_FIELDS = [
    "cost", "jobber_price", "map_price", "msrp", "retail_price", "last_synced_at"
]


def _elite_wheel_pricing_rows_to_upsert(
    batch: typing.List[typing.Dict],
    elite_provider: src_models.Providers,
    elite_brand_to_brand: typing.Dict[int, src_models.Brands],
    now,
) -> typing.List[src_models.ProviderPartCompanyPricing]:
    """Map one batch of Elite*CompanyPricing rows onto ProviderPartCompanyPricing instances."""
    master_keys = []
    for row in batch:
        brand = elite_brand_to_brand.get(row.get("part__brand_id"))
        part_number = (row.get("part__part_number") or "").strip()
        if brand and part_number:
            master_keys.append((brand.id, part_number))
    pp_by_key = _provider_parts_by_master_brand_and_part_number(elite_provider, master_keys)

    company_ids = {r["company_id"] for r in batch if r.get("company_id")}
    companies_by_id = {c.id: c for c in src_models.Company.objects.filter(id__in=company_ids)}

    to_upsert = []
    for row in batch:
        brand = elite_brand_to_brand.get(row.get("part__brand_id"))
        part_number = (row.get("part__part_number") or "").strip()
        if not brand or not part_number:
            continue
        provider_part = pp_by_key.get((brand.id, part_number))
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
                jobber_price=None,
                map_price=row.get("map"),
                msrp=row.get("retail_price"),
                retail_price=row.get("retail_price"),
                last_synced_at=now,
            )
        )
    return to_upsert


_ELITE_WHEEL_PRICING_VALUES = (
    "id", "company_id", "cost", "map", "retail_price", "part__brand_id", "part__part_number",
)


def sync_provider_pricing_from_elite_wheel() -> None:
    """
    Sync ProviderPartCompanyPricing from both Elite company-pricing tables (all companies).
    Elite's feed has no jobber column, and its retail price doubles as MSRP.
    """
    logger.info("{} Syncing provider pricing from Elite Wheel.".format(_LOG_PREFIX))

    elite_provider, mappings, elite_brand_to_brand = _elite_wheel_provider_and_mappings("price")
    if not elite_provider or not elite_brand_to_brand:
        return

    now = timezone.now()
    grand_total = 0

    for half in _elite_wheel_halves():

        def _worker(catalog_ids: typing.Set[int], half: _EliteWheelHalf = half) -> int:
            if not catalog_ids:
                return 0
            total_upserted = 0
            batch_num = 0
            last_id = 0
            while True:
                batch_num += 1
                batch = list(
                    half.pricing_model.objects.filter(
                        id__gt=last_id, part__brand_id__in=catalog_ids
                    )
                    .order_by("id")
                    .values(*_ELITE_WHEEL_PRICING_VALUES)[:BATCH_SIZE_PRICING]
                )
                if not batch:
                    break

                last_id = batch[-1]["id"]
                to_upsert = _elite_wheel_pricing_rows_to_upsert(
                    batch, elite_provider, elite_brand_to_brand, now
                )

                if to_upsert:
                    to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                        to_upsert, context="Elite {} pricing batch={}".format(half.label, batch_num)
                    )
                    pgbulk.upsert(
                        src_models.ProviderPartCompanyPricing,
                        to_upsert,
                        unique_fields=["provider_part", "company"],
                        update_fields=_ELITE_WHEEL_PRICING_UPSERT_FIELDS,
                    )
                    total_upserted += len(to_upsert)

                logger.info("{} Elite {} pricing batch {}: {} records (last_id={})".format(
                    _LOG_PREFIX, half.label, batch_num, len(to_upsert), last_id
                ))
                connection.close()
                if len(batch) == BATCH_SIZE_PRICING:
                    time.sleep(BATCH_DELAY_SECONDS)
            return total_upserted

        grand_total += _run_parallel_mapped_brand_int_worker(mappings, "elite_wheel_brand", _worker)

    logger.info("{} Synced {} Elite Wheel pricing records total.".format(_LOG_PREFIX, grand_total))


def sync_provider_pricing_from_elite_wheel_for_company(company_id: int) -> None:
    """One company's Elite pricing — the per-company path used by IntegrationPricingSyncJob."""
    logger.info("{} Syncing Elite Wheel provider pricing for company_id={}.".format(_LOG_PREFIX, company_id))

    elite_provider, mappings, elite_brand_to_brand = _elite_wheel_provider_and_mappings("price")
    if not elite_provider or not elite_brand_to_brand:
        return

    company_provider = src_models.CompanyProviders.objects.filter(
        company_id=company_id, provider_id=elite_provider.id
    ).first()

    now = timezone.now()

    # Watermark -- see sync_provider_pricing_from_meyer_for_company for the full rationale.
    sync_started_at = timezone.now()
    watermark = company_provider.pricing_propagation_watermark if company_provider else None
    logger.info(
        "{} Elite Wheel pricing company={}: propagation watermark={}.".format(
            _LOG_PREFIX, company_id, watermark or "none (processing everything)",
        )
    )

    grand_total = 0
    for half in _elite_wheel_halves():

        def _worker(catalog_ids: typing.Set[int], half: _EliteWheelHalf = half) -> int:
            if not catalog_ids:
                return 0
            total_upserted = 0
            batch_num = 0
            last_id = 0
            while True:
                batch_num += 1
                qs = half.pricing_model.objects.filter(
                    id__gt=last_id,
                    company_id=company_id,
                    part__brand_id__in=catalog_ids,
                )
                if watermark:
                    qs = qs.filter(updated_at__gte=watermark)
                batch = list(
                    qs.order_by("id").values(*_ELITE_WHEEL_PRICING_VALUES)[:BATCH_SIZE_PRICING]
                )
                if not batch:
                    break

                last_id = batch[-1]["id"]
                to_upsert = _elite_wheel_pricing_rows_to_upsert(
                    batch, elite_provider, elite_brand_to_brand, now
                )

                if to_upsert:
                    to_upsert = _dedupe_provider_part_company_pricing_for_upsert(
                        to_upsert,
                        context="Elite {} pricing company={} batch={}".format(
                            half.label, company_id, batch_num
                        ),
                    )
                    pgbulk.upsert(
                        src_models.ProviderPartCompanyPricing,
                        to_upsert,
                        unique_fields=["provider_part", "company"],
                        update_fields=_ELITE_WHEEL_PRICING_UPSERT_FIELDS,
                    )
                    total_upserted += len(to_upsert)

                logger.info("{} Elite {} pricing company={} batch {}: {} records (last_id={})".format(
                    _LOG_PREFIX, half.label, company_id, batch_num, len(to_upsert), last_id
                ))
                connection.close()
                if len(batch) == BATCH_SIZE_PRICING:
                    time.sleep(BATCH_DELAY_SECONDS)
            return total_upserted

        grand_total += _run_parallel_mapped_brand_int_worker(mappings, "elite_wheel_brand", _worker)

    logger.info("{} Synced {} Elite Wheel pricing records for company_id={}.".format(
        _LOG_PREFIX, grand_total, company_id,
    ))

    if company_provider:
        company_provider.pricing_propagation_watermark = sync_started_at
        company_provider.save(update_fields=["pricing_propagation_watermark"])


def sync_derived_from_elite_wheel(
    *, reindex_meilisearch: bool = False, skip_master_parts: bool = False, skip_pricing: bool = False
) -> None:
    """
    Propagate Elite Wheel source data into MasterPart, ProviderPart, ProviderPartInventory, and
    ProviderPartCompanyPricing. Call after the Elite feed ingest. No fitment sync -- the workbook
    carries wheel/tire spec attributes, not vehicle fitment.

    Pass ``skip_master_parts=True`` to run only inventory + pricing (fast incremental path).
    Pass ``skip_pricing=True`` to run only master parts + inventory (global catalog sync path).
    """
    logger.info("{} Starting Elite Wheel-only derived sync ({}).".format(
        _LOG_PREFIX,
        "inventory + pricing only" if skip_master_parts else "parts, inventory" + ("" if skip_pricing else ", pricing"),
    ))
    if not skip_master_parts:
        sync_master_parts_from_elite_wheel()
        connection.close()

    def _cont() -> None:
        sync_provider_inventory_from_elite_wheel()
        connection.close()
        if not skip_pricing:
            sync_provider_pricing_from_elite_wheel()
            connection.close()

    if skip_master_parts:
        _cont()
    else:
        _maybe_reindex_meilisearch_after_master_parts(
            reindex_meilisearch=reindex_meilisearch,
            provider_label="Elite Wheel",
            continuation=_cont,
        )
    logger.info("{} Completed Elite Wheel-only derived sync.".format(_LOG_PREFIX))


def sync_all_master_parts() -> None:
    """
    Run each distributor's derived sync in order (master + provider parts, then inventory, then pricing
    for that distributor before moving to the next). Order matches the previous implementation so
    dependencies (e.g. Meyer before A-Tech) are preserved.

    Does not reindex Meilisearch. After scheduled ``ingest_all_providers`` or a manual run, use
    ``index_parts_meilisearch`` or ``sync_master_parts --reindex-meilisearch`` to refresh search.
    """
    logger.info("{} Starting full master parts sync.".format(_LOG_PREFIX))

    sync_derived_from_turn14(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_keystone(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_meyer(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_atech(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_rough_country(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_dlg(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_wheelpros(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_premier(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_vossen(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_tirerack(reindex_meilisearch=False, skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_elite_wheel(reindex_meilisearch=False, skip_pricing=True)

    logger.info("{} Completed full master parts sync.".format(_LOG_PREFIX))


def sync_all_master_parts_global() -> None:
    """
    Global catalog + inventory sync — **no per-company pricing**.

    Designed for the nightly ``ingest_all_providers`` pipeline:
    - Runs MasterPart / ProviderPart upsert + ProviderPartInventory for every provider.
    - Skips ProviderPartCompanyPricing (``skip_pricing=True``) to keep memory low and
      separate the long-running catalog phase from per-company pricing concerns.
    - After this call, the caller should enqueue ``IntegrationPricingSyncJob`` rows for all
      active company-providers so pricing syncs run asynchronously.

    Provider ordering:
    - Turn14 always runs **first** (other providers may shadow Turn14 data; Turn14 must settle first).
    - All remaining providers run sequentially with ``_reclaim_memory()`` between each to prevent
      cumulative RSS growth across the multi-hour sync window.
    """
    logger.info("{} Starting global master parts sync (catalog + inventory, no pricing).".format(_LOG_PREFIX))

    sync_derived_from_turn14(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_keystone(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_meyer(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_atech(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_rough_country(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_dlg(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_wheelpros(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_premier(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_vossen(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_tirerack(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_quadratec(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_elite_wheel(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_motorstate(skip_pricing=True)
    _reclaim_memory()
    sync_derived_from_wps(skip_pricing=True)

    logger.info("{} Completed global master parts sync (catalog + inventory, no pricing).".format(_LOG_PREFIX))


def sync_all_master_parts_incremental() -> None:
    """
    Fast sync: ProviderPartInventory + ProviderPartCompanyPricing only for all providers.
    Skips the MasterPart / ProviderPart upsert phase — no new parts, descriptions, or SKU
    changes are picked up. Only updates inventory quantities and company pricing for parts
    that already exist in the master layer.

    Run this frequently (e.g. every few hours) to keep inventory and pricing fresh.
    Run ``sync_all_master_parts`` once per day (off-peak) to pick up new/changed parts
    and trigger a Meilisearch reindex.

    Provider order is preserved so any cross-provider dependencies remain consistent.
    """
    logger.info("{} Starting incremental master parts sync (inventory + pricing only).".format(_LOG_PREFIX))

    sync_derived_from_turn14(skip_master_parts=True)
    sync_derived_from_keystone(skip_master_parts=True)
    sync_derived_from_meyer(skip_master_parts=True)
    sync_derived_from_atech(skip_master_parts=True)
    sync_derived_from_rough_country(skip_master_parts=True)
    sync_derived_from_dlg(skip_master_parts=True)
    sync_derived_from_wheelpros(skip_master_parts=True)
    sync_derived_from_premier(skip_master_parts=True)
    sync_derived_from_vossen(skip_master_parts=True)
    sync_derived_from_tirerack(skip_master_parts=True)
    sync_derived_from_quadratec(skip_master_parts=True)
    sync_derived_from_elite_wheel(skip_master_parts=True)
    sync_derived_from_motorstate(skip_master_parts=True)
    sync_derived_from_wps(skip_master_parts=True)

    logger.info("{} Completed incremental master parts sync.".format(_LOG_PREFIX))
