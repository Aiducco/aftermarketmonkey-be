"""
Catalog-wide Turn 14 sweeps -- the "daily full sweep" and "weekly fitment" tiers of Turn 14's
proposed integration model.

Why flat rather than per brand
------------------------------
The existing services in ``turn_14.py`` page each of the 464 mapped brands separately
(``items/brand/{id}``, ``inventory/brand/{id}``, ``pricing/brand/{id}``). Turn 14 serves the
brand-scoped endpoints 200 rows to a page but the unscoped collections 1 000 (items/data 450,
fitment 200) -- measured, not assumed. Sweeping the whole catalog flat is therefore ~776
requests where brand-by-brand is ~4 200, for identical data. Against a 5 000/hour allowance
that is the difference between a 9-minute sync and one that cannot finish inside its budget.

Ordering constraint
-------------------
Only ``/v1/items`` carries ``attributes.brand_id``. The data, inventory, pricing and fitment
collections identify rows by item id alone, so their brand is resolved through Turn14Items --
which means :func:`sweep_items` must run before the others on a first-ever sync. On steady-state
runs the table is already populated and order stops mattering.

Deactivation
------------
A flat sweep is also the only way to notice an item Turn 14 has *withdrawn*. The per-brand path
only ever upserts, so a SKU that disappears from the feed stays ``active=True`` forever. See
:func:`deactivate_items_missing_from_sweep`.
"""
import json
import logging
import time
import typing
from decimal import Decimal, InvalidOperation

import pgbulk
from django.conf import settings
from django.db import connection
from django.utils import timezone

from src import models as src_models
from src.integrations import rate_limit as rate_limit_base
from src.integrations.services import master_parts
from src.integrations.services import turn_14 as turn_14_services
from src.integrations.services import turn_14_global

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TURN-14-SWEEPS]"

# Rows buffered before an upsert. Large enough that the round trips disappear next to the
# ~845ms each API page costs, small enough that a page's worth of JSON plus a batch of model
# instances stays well clear of the memory ceilings the nightly ingest already works around.
_BATCH_SIZE = 2000


def _to_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _dedupe_by_external_id(
    instances: typing.List[typing.Any], key_attr: str = "external_id"
) -> typing.List[typing.Any]:
    """
    Last-wins dedupe by the model's unique key right before a bulk upsert.

    A flush batch spans several pages (_BATCH_SIZE=2000 against 1000-row pages), and Turn 14's
    catalog keeps changing while a sweep pages through it -- a real update can shift an item's
    sort position between two page fetches, landing the same external_id on both pages and
    inside the same flush batch. Postgres rejects that outright: "ON CONFLICT DO UPDATE command
    cannot affect row a second time" (confirmed live 2026-08-26, sync_turn14_global_sweep).
    Keeping the last occurrence is correct either way -- it reflects whichever page fetch
    happened later, i.e. the freshest read of that item during this sweep.
    """
    by_key = {}
    for instance in instances:
        by_key[getattr(instance, key_attr)] = instance
    return list(by_key.values())


def _brand_by_external_id() -> typing.Dict[str, src_models.Turn14Brand]:
    """Turn 14's numeric brand id -> our Turn14Brand. 464 rows; safe to hold for a sweep."""
    return {
        str(b.external_id): b
        for b in src_models.Turn14Brand.objects.all()
    }


def _brand_ids_for_items(item_external_ids: typing.Sequence[str]) -> typing.Dict[str, int]:
    """
    item id -> Turn14Brand pk, for the collections that do not carry brand_id themselves.

    Resolved per batch rather than as one 793k-entry dict: the sweeps run inside the same
    memory envelope as the nightly ingest, and a batch-scoped query is a few milliseconds
    against a unique index.
    """
    if not item_external_ids:
        return {}
    return dict(
        src_models.Turn14Items.objects.filter(external_id__in=list(item_external_ids))
        .values_list("external_id", "brand_id")
    )


def _sweep(
    label: str,
    fetch_page: typing.Callable[[int], typing.Tuple[typing.List[typing.Dict], typing.Optional[int]]],
    flush: typing.Callable[[typing.List[typing.Dict]], int],
    max_pages: typing.Optional[int] = None,
    start_page: int = 1,
    pace_seconds: typing.Optional[float] = None,
) -> typing.Tuple[int, int]:
    """
    Page an endpoint to exhaustion, handing ``flush`` batches of raw rows.

    Returns (rows_seen, rows_written). Deliberately does not swallow RateBudgetExhausted: a
    spent budget must abort the sweep so the caller can defer it, not silently truncate the
    catalog and report success -- a half-swept catalog that looks complete is worse than a
    failed run, because the deactivation pass would then mark every unseen item inactive.
    It is, however, caught just long enough to attach the page it happened on as the
    exception's ``checkpoint`` before re-raising -- see ``start_page`` below for why.

    ``max_pages`` stops after that many pages regardless of what the API still has left -- a
    real, permanent feature (not a test-only hack) for bounded smoke tests of a sweep before
    trusting it with a full, hours-long catalog run.

    ``start_page`` resumes a walk that previously failed partway through, instead of paging
    from 1 again. Real incident (2026-08-27): a ~1,724-page sweep hit a transient upstream 429
    five times over 49 minutes; every retry restarted from page 1 because nothing carried the
    failed page forward, burning ~4,600 requests without the sweep ever actually finishing. A
    caller that catches ``RateBudgetExhausted`` and passes its ``.checkpoint`` back in as
    ``start_page`` on the next attempt resumes instead of repeating already-fetched pages.

    ``pace_seconds`` sleeps this long after every page, on top of whatever the shared rate
    buckets already allow. The bucket-based pacing only intervenes once a soft window (5/s,
    100/min) is already full, so left alone a sweep bursts up to that ceiling, pauses, and
    bursts again -- a shape, not just a rate. Real incident (2026-08-27): items/data (Turn 14's
    own smaller, 450-row page for this endpoint, so ~1,724 pages against items' ~778 for the
    same catalog) hit a real upstream 429 in every one of two independent full-catalog runs,
    always well under the documented 5,000/hour ceiling by our own accounting -- consistent
    with Turn 14 reacting to the burst shape on this specific endpoint, not a simple count.
    Smoothing to an even pace is a direct, cheap way to test that without guessing further from
    logs alone.
    """
    page: typing.Optional[int] = start_page
    buffer: typing.List[typing.Dict] = []
    seen = written = 0
    pages_fetched = 0

    while page is not None:
        try:
            rows, next_page = fetch_page(page)
        except rate_limit_base.RateBudgetExhausted as e:
            e.checkpoint = page
            raise
        pages_fetched += 1
        seen += len(rows)
        buffer.extend(rows)

        if len(buffer) >= _BATCH_SIZE:
            written += flush(buffer)
            buffer = []
            logger.info("{} {}: {} seen / {} written (page {}).".format(
                _LOG_PREFIX, label, seen, written, page
            ))

        page = next_page
        if max_pages is not None and pages_fetched >= max_pages:
            if page is not None:
                logger.info("{} {}: stopping after max_pages={} (more pages remain).".format(
                    _LOG_PREFIX, label, max_pages
                ))
            break

        if pace_seconds and page is not None:
            time.sleep(pace_seconds)

    if buffer:
        written += flush(buffer)

    logger.info("{} {} complete: {} seen / {} written.".format(_LOG_PREFIX, label, seen, written))
    return seen, written


# ---------------------------------------------------------------------------------------
# Global (shared) sweeps
# ---------------------------------------------------------------------------------------

def sweep_items(
    client=None, max_pages: typing.Optional[int] = None, start_page: int = 1,
    pace_seconds: typing.Optional[float] = None,
) -> typing.Tuple[int, int]:
    """GET /v1/items over the whole catalog into Turn14Items."""
    client = client or turn_14_global.get_global_client()
    brands = _brand_by_external_id()
    unknown_brands: typing.Set[str] = set()

    def flush(rows: typing.List[typing.Dict]) -> int:
        instances = []
        for row in rows:
            brand_id = str((row.get("attributes") or {}).get("brand_id") or "")
            brand = brands.get(brand_id)
            if brand is None:
                unknown_brands.add(brand_id)
                continue
            instances.extend(turn_14_services._transform_items_data([row], brand))
        if not instances:
            return 0
        instances = _dedupe_by_external_id(instances)
        pgbulk.upsert(
            src_models.Turn14Items,
            instances,
            unique_fields=["external_id"],
            update_fields=[
                "brand", "product_name", "part_number", "mfr_part_number", "part_description",
                "category", "subcategory", "external_brand_id", "brand_name", "price_group_id",
                "price_group", "active", "born_on_date", "regular_stock",
                "powersports_indicator", "dropship_controller_id", "air_freight_prohibited",
                "not_carb_approved", "carb_acknowledgement_required", "ltl_freight_required",
                "prop_65", "epa", "units_per_sku", "clearance_item", "thumbnail",
                "barcode", "dimensions", "warehouse_availability", "updated_at",
            ],
        )
        return len(instances)

    result = _sweep(
        "items", client.get_items, flush, max_pages=max_pages, start_page=start_page, pace_seconds=pace_seconds,
    )
    if unknown_brands:
        # Turn 14 published items for a brand we have not mapped yet. Not fatal -- the next
        # brands sync picks it up -- but silence here would hide a growing blind spot.
        logger.warning(
            "{} items sweep skipped rows for {} unmapped brand id(s): {}.".format(
                _LOG_PREFIX, len(unknown_brands), sorted(unknown_brands)[:20]
            )
        )
    return result


def sweep_items_data(
    client=None, max_pages: typing.Optional[int] = None, start_page: int = 1,
    pace_seconds: typing.Optional[float] = None,
) -> typing.Tuple[int, int]:
    """GET /v1/items/data over the whole catalog into Turn14BrandData (media + descriptions)."""
    client = client or turn_14_global.get_global_client()

    def flush(rows: typing.List[typing.Dict]) -> int:
        brand_ids = _brand_ids_for_items([str(r.get("id", "")) for r in rows])
        instances = []
        for row in rows:
            external_id = str(row.get("id", ""))
            brand_id = brand_ids.get(external_id)
            if not external_id or brand_id is None:
                continue
            instances.append(src_models.Turn14BrandData(
                external_id=external_id,
                brand_id=brand_id,
                type=row.get("type"),
                files=row.get("files"),
                descriptions=row.get("descriptions"),
                relationships=row.get("relationships"),
                updated_at=timezone.now(),
            ))
        if not instances:
            return 0
        instances = _dedupe_by_external_id(instances)
        pgbulk.upsert(
            src_models.Turn14BrandData,
            instances,
            unique_fields=["external_id"],
            update_fields=["brand", "type", "files", "descriptions", "relationships", "updated_at"],
        )
        return len(instances)

    return _sweep(
        "items/data", client.get_items_data, flush,
        max_pages=max_pages, start_page=start_page, pace_seconds=pace_seconds,
    )


def sweep_inventory(
    client=None, max_pages: typing.Optional[int] = None, start_page: int = 1,
    pace_seconds: typing.Optional[float] = None,
) -> typing.Tuple[int, int]:
    """GET /v1/inventory over the whole catalog into Turn14BrandInventory."""
    client = client or turn_14_global.get_global_client()

    def flush(rows: typing.List[typing.Dict]) -> int:
        brand_ids = _brand_ids_for_items([str(r.get("id", "")) for r in rows])
        instances = []
        for row in rows:
            external_id = str(row.get("id", ""))
            if not external_id:
                continue
            attributes = row.get("attributes") or {}
            inventory = attributes.get("inventory") or {}
            # Warehouse counts are keyed by location code; anything non-numeric is a stray key
            # rather than a warehouse, so it must not be summed into the total.
            total = 0
            for value in inventory.values():
                try:
                    total += int(value)
                except (TypeError, ValueError):
                    continue
            instances.append(src_models.Turn14BrandInventory(
                external_id=external_id,
                brand_id=brand_ids.get(external_id),
                type=row.get("type"),
                inventory=inventory,
                manufacturer=attributes.get("manufacturer"),
                eta=attributes.get("eta"),
                relationships=row.get("relationships"),
                total_inventory=total,
                updated_at=timezone.now(),
            ))
        if not instances:
            return 0
        instances = _dedupe_by_external_id(instances)
        pgbulk.upsert(
            src_models.Turn14BrandInventory,
            instances,
            unique_fields=["external_id"],
            update_fields=[
                "brand", "type", "inventory", "manufacturer", "eta", "relationships",
                "total_inventory", "updated_at",
            ],
        )
        return len(instances)

    return _sweep(
        "inventory", client.get_inventory, flush,
        max_pages=max_pages, start_page=start_page, pace_seconds=pace_seconds,
    )


def sweep_fitment(
    client=None, max_pages: typing.Optional[int] = None, start_page: int = 1,
    pace_seconds: typing.Optional[float] = None,
) -> typing.Tuple[int, int]:
    """
    GET /v1/items/fitment over the whole catalog, decoded straight into MasterPartFitment.

    Deliberately does NOT persist into Turn14ItemFitment: a 2-page smoke test of the old
    explode-into-a-row-per-vehicle design wrote 30,591 rows from 400 items (~76 vehicles/item),
    extrapolating to ~59M rows catalog-wide against a (item_external_id, vehicle_id) unique index
    -- an unresolved capacity question. Instead every vehicle_id on a row is decoded via VcdbVehicle
    and collapsed into year ranges immediately, per item, exactly like
    master_parts.sync_master_part_fitments_from_turn14_vcdb already does for a persisted
    Turn14ItemFitment table -- just applied inline to the raw API response instead of a DB read.
    One Turn14 fitment row already carries every vehicle id for that item in a single response,
    so this collapsing is correct per-row without needing a cross-page accumulator.

    The output table (MasterPartFitment) is sized by real distinct fitment combinations, the same
    way Rough Country's ~273k and ASAP's ~905k rows already are -- not one row per raw vehicle id
    -- so the ~59M-row risk that applied to Turn14ItemFitment does not apply here.
    """
    client = client or turn_14_global.get_global_client()
    ctx = master_parts.build_turn14_fitment_join_context()
    if ctx is None:
        logger.warning(
            "{} fitment sweep: join context unavailable (no Turn14 provider, no "
            "BrandTurn14BrandMapping, or VcdbVehicle is empty -- run import_vcdb_vehicles "
            "first). Skipping.".format(_LOG_PREFIX)
        )
        return 0, 0

    skipped_unknown_item = [0]
    skipped_unknown_vehicle = [0]

    def flush(rows: typing.List[typing.Dict]) -> int:
        to_upsert = []
        for row in rows:
            external_id = str(row.get("id", ""))
            master_part_id = ctx.item_ext_id_to_master_part_id.get(external_id)
            if not external_id or not master_part_id:
                # An item with no resolvable MasterPart (not yet in Turn14Items, or its brand
                # isn't mapped) has nowhere to attach fitment to. Counted rather than logged per
                # row -- on a first run before the items sweep this would be every single row.
                skipped_unknown_item[0] += 1
                continue
            attributes = row.get("attributes") or {}
            years_by_key: typing.Dict[typing.Tuple, typing.Set[int]] = {}
            for vehicle_id in (attributes.get("vehicle_ids") or []):
                try:
                    vehicle_id = int(vehicle_id)
                except (TypeError, ValueError):
                    continue
                vcdb = ctx.vcdb_by_vehicle_id.get(vehicle_id)
                if not vcdb:
                    skipped_unknown_vehicle[0] += 1
                    continue
                key = (
                    vcdb["make"], vcdb["model"], vcdb["submodel"] or "",
                    vcdb["engine"] or "", vcdb["drive_type"] or "",
                )
                years_by_key.setdefault(key, set()).add(vcdb["year"])

            for (make, model, submodel, engine, drive_type), years in years_by_key.items():
                for year_start, year_end in master_parts._collapse_years_to_ranges(years):
                    to_upsert.append(src_models.MasterPartFitment(
                        master_part_id=master_part_id,
                        year_start=year_start,
                        year_end=year_end,
                        make=make,
                        model=model,
                        submodel=submodel,
                        engine=engine,
                        drive_type=drive_type,
                        source_provider=ctx.turn14_provider,
                    ))

        if not to_upsert:
            return 0
        to_upsert = master_parts._dedupe_master_part_fitments_for_upsert(
            to_upsert, context="Turn14 fitment sweep"
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
        return len(to_upsert)

    result = _sweep(
        "items/fitment", client.get_items_fitment, flush,
        max_pages=max_pages, start_page=start_page, pace_seconds=pace_seconds,
    )
    if skipped_unknown_item[0] or skipped_unknown_vehicle[0]:
        logger.warning(
            "{} fitment sweep: skipped {} row(s) for items with no MasterPart, {} vehicle_id(s) "
            "not in VcdbVehicle.".format(_LOG_PREFIX, skipped_unknown_item[0], skipped_unknown_vehicle[0])
        )
    return result


def sweep_dropship_controllers(
    client=None, start_index: int = 0, pace_seconds: typing.Optional[float] = None
) -> int:
    """
    Resolve every distinct Turn14Items.dropship_controller_id through GET /v1/dropship/{id}.

    There is no bulk endpoint for these (an open question for Turn 14), but the set is small --
    a few hundred at most across the catalog -- so one request each is affordable. Id 0 is
    Turn 14's sentinel for "no controller" and 404s, so it is filtered out rather than fetched.

    ``start_index`` resumes a run that previously failed partway through (see
    ``rate_limit.resumable_sweep`` -- ``RateBudgetExhausted.checkpoint`` is set to the index of
    the controller that failed). Real incident (2026-08-27): this used to accumulate every
    result in memory and upsert only at the very end, so a 429 anywhere in a 298-controller run
    -- immediately after a heavy inventory sweep with no gap, the same burst-after-burst shape
    that broke items_data -- discarded every controller successfully resolved so far, on top of
    restarting from the first one on retry. Now flushes what it has before re-raising, and
    ``pace_seconds`` (see ``_sweep``) smooths the same burst shape.
    """
    client = client or turn_14_global.get_global_client()

    controller_ids = sorted(
        set(
            src_models.Turn14Items.objects
            .filter(dropship_controller_id__isnull=False)
            .exclude(dropship_controller_id=0)
            .values_list("dropship_controller_id", flat=True)
            .distinct()
        )
    )
    logger.info("{} Resolving {} dropship controller(s).".format(_LOG_PREFIX, len(controller_ids)))

    def _flush(instances: typing.List[src_models.Turn14DropshipController]) -> int:
        if not instances:
            return 0
        pgbulk.upsert(
            src_models.Turn14DropshipController,
            instances,
            unique_fields=["external_id"],
            update_fields=["charges", "updated_at"],
        )
        return len(instances)

    buffer: typing.List[src_models.Turn14DropshipController] = []
    written = 0
    last_index = len(controller_ids) - 1
    for i in range(start_index, len(controller_ids)):
        controller_id = controller_ids[i]
        try:
            data = client.get_dropship_controller(int(controller_id))
        except rate_limit_base.RateBudgetExhausted as e:
            written += _flush(buffer)
            e.checkpoint = i
            raise
        if data:
            buffer.append(src_models.Turn14DropshipController(
                external_id=str(controller_id),
                charges=(data.get("attributes") or {}).get("charges"),
                updated_at=timezone.now(),
            ))
        if pace_seconds and i < last_index:
            time.sleep(pace_seconds)

    written += _flush(buffer)
    logger.info("{} Upserted {} dropship controller(s).".format(_LOG_PREFIX, written))
    return written


def sweep_shipping_options(client=None) -> int:
    """
    GET /v1/shipping -- the account-wide shipping service levels (e.g. "UPS Ground"), into
    Turn14ShippingOption. Small and static (50 rows measured live) and not paginated -- one
    request. Part of Dan Ziegler's proposed Daily Full Sweep list; previously only fetched live,
    ad hoc, from orders/turn_14.py at quote time, never cached.
    """
    client = client or turn_14_global.get_global_client()

    data = client.get_shipping_options()
    instances = [
        src_models.Turn14ShippingOption(
            external_id=str(row.get("id", "")),
            transportation_name=(row.get("attributes") or {}).get("transportation_name") or "",
            carrier_name=(row.get("attributes") or {}).get("carrier_name") or "",
            updated_at=timezone.now(),
        )
        for row in data
        if row.get("id") is not None
    ]
    if instances:
        pgbulk.upsert(
            src_models.Turn14ShippingOption,
            instances,
            unique_fields=["external_id"],
            update_fields=["transportation_name", "carrier_name", "updated_at"],
        )
    logger.info("{} Upserted {} shipping option(s).".format(_LOG_PREFIX, len(instances)))
    return len(instances)


def sweep_shipping_estimates(
    client=None, max_pages: typing.Optional[int] = None, start_page: int = 1,
    pace_seconds: typing.Optional[float] = None,
) -> typing.Tuple[int, int]:
    """
    GET /v1/shipping/item_estimation over the whole catalog into Turn14ItemShippingEstimate.

    Flat, not brand-scoped -- confirmed live 2026-08-25 both variants return 1000 rows/page
    (contradicting the earlier assumption the per-brand one was 200/page, which was never
    actually measured). Summed over every brand's own ceil(items/1000), the per-brand walk costs
    1081 requests against this endpoint's 795 for the same catalog (measured live: 457 brands,
    794581 items) -- 26.5% fewer requests, same efficiency gain items/items-data/inventory
    already get from going flat. Brand attribution costs a Turn14Items lookup per batch here
    (the flat response carries no brand_id), same trade the other flat sweeps already make.
    """
    client = client or turn_14_global.get_global_client()

    def flush(rows: typing.List[typing.Dict]) -> int:
        brand_ids = _brand_ids_for_items([str(r.get("id", "")) for r in rows])
        instances = []
        for row in rows:
            external_id = str(row.get("id", ""))
            if not external_id:
                continue
            attributes = row.get("attributes") or {}
            rate = attributes.get("ground_continental_us_base_rate") or {}
            instances.append(src_models.Turn14ItemShippingEstimate(
                item_external_id=external_id,
                brand_id=brand_ids.get(external_id),
                can_ship=bool(rate.get("can_ship", False)),
                min_rate=_to_decimal(rate.get("min")),
                average_rate=_to_decimal(rate.get("average")),
                max_rate=_to_decimal(rate.get("max")),
                fees=attributes.get("fees"),
                updated_at=timezone.now(),
            ))
        if not instances:
            return 0
        instances = _dedupe_by_external_id(instances, key_attr="item_external_id")
        pgbulk.upsert(
            src_models.Turn14ItemShippingEstimate,
            instances,
            unique_fields=["item_external_id"],
            update_fields=[
                "brand", "can_ship", "min_rate", "average_rate", "max_rate", "fees",
                "updated_at",
            ],
        )
        return len(instances)

    return _sweep(
        "shipping/item_estimation", client.get_item_shipping_estimates, flush,
        max_pages=max_pages, start_page=start_page, pace_seconds=pace_seconds,
    )


def deactivate_items_missing_from_sweep(sweep_started_at) -> int:
    """
    Mark items untouched by a *completed* full sweep as inactive.

    Turn 14 withdraws SKUs, but every existing path only upserts, so a withdrawn part stays
    active forever and keeps surfacing in search. A completed flat sweep touches every item
    Turn 14 still carries, so anything whose updated_at predates the sweep is gone.

    Only ever call this after a sweep that ran to completion. A sweep aborted by a spent rate
    budget has seen an arbitrary prefix of the catalog, and deactivating "everything unseen"
    would take out most of it.
    """
    stale = src_models.Turn14Items.objects.filter(active=True, updated_at__lt=sweep_started_at)
    count = stale.count()
    if count:
        stale.update(active=False, updated_at=timezone.now())
        logger.warning(
            "{} Deactivated {} item(s) absent from the completed sweep.".format(_LOG_PREFIX, count)
        )
    return count


# ---------------------------------------------------------------------------------------
# Customer-specific sweep
# ---------------------------------------------------------------------------------------

def _upsert_turn14_brand_pricing_skip_unchanged(
    instances: typing.List[src_models.Turn14BrandPricing],
) -> int:
    """
    Hand-written upsert (not pgbulk) so a row whose price genuinely did not change is a true
    no-op -- no row touched, no index maintenance, ``updated_at`` left alone -- instead of
    bumping ``updated_at`` on every one of ~780k rows every cycle regardless of whether Turn 14's
    price actually moved. Mirrors ``meyer.py``'s ``_flush_buf``, built from the same kind of
    investigation there (company_provider_id=19): comparing on the real columns via
    ``IS DISTINCT FROM`` and excluding ``updated_at`` from that comparison (it always differs)
    means it only actually advances when something real changed, so it becomes a genuine "this
    row's price last changed at" signal -- which is what
    ``sync_provider_pricing_from_turn14_for_company`` (master_parts.py) filters on downstream to
    skip walking and re-propagating rows that did not change at all.
    """
    if not instances:
        return 0
    now = timezone.now()
    rows = [
        (
            inst.external_id,
            inst.brand_id,
            inst.company_id,
            inst.type,
            inst.purchase_cost,
            inst.has_map,
            inst.can_purchase,
            json.dumps(inst.pricelists) if inst.pricelists is not None else None,
            now,
            now,
        )
        for inst in instances
    ]
    placeholders = ", ".join(["(%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)"] * len(rows))
    params = [v for row in rows for v in row]
    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO turn14_brand_pricing
                (external_id, brand_id, company_id, type, purchase_cost, has_map, can_purchase,
                 pricelists, created_at, updated_at)
            VALUES {}
            ON CONFLICT (company_id, external_id) DO UPDATE SET
                brand_id = EXCLUDED.brand_id,
                type = EXCLUDED.type,
                purchase_cost = EXCLUDED.purchase_cost,
                has_map = EXCLUDED.has_map,
                can_purchase = EXCLUDED.can_purchase,
                pricelists = EXCLUDED.pricelists,
                updated_at = EXCLUDED.updated_at
            WHERE (turn14_brand_pricing.brand_id, turn14_brand_pricing.type,
                   turn14_brand_pricing.purchase_cost, turn14_brand_pricing.has_map,
                   turn14_brand_pricing.can_purchase, turn14_brand_pricing.pricelists)
                IS DISTINCT FROM
                  (EXCLUDED.brand_id, EXCLUDED.type, EXCLUDED.purchase_cost, EXCLUDED.has_map,
                   EXCLUDED.can_purchase, EXCLUDED.pricelists)
            """.format(placeholders),
            params,
        )
    return len(instances)


def sweep_pricing_for_company_provider(
    company_provider: src_models.CompanyProviders,
    pace_seconds: typing.Optional[float] = None,
) -> typing.Tuple[int, int]:
    """
    GET /v1/pricing over the whole catalog for one customer, into Turn14BrandPricing.

    The flat replacement for ``_fetch_and_save_turn_14_brand_pricing_for_company_provider``,
    which walks 464 brands at 200 rows a page (~4 200 requests) to fetch what this does in 776.
    Against a 5 000/hour allowance that is the difference between finishing inside the budget
    and not: 776 requests is a ~9 minute floor, 4 200 is ~50 minutes and cannot be repeated for
    eleven customers inside a day.

    Uses the *customer's* credentials, never the global ones -- pricing is the half of Turn 14's
    model that is genuinely per-account, and one customer's costs must never be written from
    another's connection.

    Same shape as items_data (~776 pages, one continuous burst) and confirmed live 2026-08-31 to
    hit the same real upstream 429 items_data did -- see turn_14_sweeps._sweep's pace_seconds
    docstring. No ``start_page`` here yet, unlike the daily global sweep: the caller
    (run_integration_pricing_sync_job) defers a 429 as a whole-job retry rather than resuming
    in-process, and that retry can happen long after this process exits, so a checkpoint would
    need to live on the job row itself, not an in-memory dict -- a real follow-up, not done here.
    """
    from src.integrations import credentials as credentials_helper
    from src.integrations.clients.turn_14 import client as turn_14_client

    company = company_provider.company
    client = turn_14_client.Turn14ApiClient(
        credentials=credentials_helper.get_feed_credentials(company_provider)
    )
    # Instance-level override, same mechanism as turn_14_global.get_global_client() -- lets
    # TURN14_GLOBAL_BASE_URL redirect pricing to Turn 14's sandbox during the temporary
    # integrator-credential validation window without touching the Turn14ApiClient class
    # default (order placement builds its own client and is never affected by this).
    client.API_BASE_URL = settings.TURN14_GLOBAL_BASE_URL

    def flush(rows: typing.List[typing.Dict]) -> int:
        brand_ids = _brand_ids_for_items([str(r.get("id", "")) for r in rows])
        instances = []
        for row in rows:
            external_id = str(row.get("id", ""))
            brand_id = brand_ids.get(external_id)
            if not external_id or brand_id is None:
                # brand is NOT NULL here. An item we have never catalogued cannot be priced
                # against a brand, so skip it; the next items sweep will pick it up.
                continue
            attributes = row.get("attributes") or {}
            # updated_at is deliberately not set here -- _upsert_turn14_brand_pricing_skip_unchanged
            # computes and applies it itself, only when a row actually changes.
            instances.append(src_models.Turn14BrandPricing(
                external_id=external_id,
                brand_id=brand_id,
                company=company,
                type=row.get("type"),
                purchase_cost=_to_decimal(attributes.get("purchase_cost")),
                has_map=bool(attributes.get("has_map", False)),
                can_purchase=bool(attributes.get("can_purchase", False)),
                pricelists=attributes.get("pricelists"),
            ))
        if not instances:
            return 0
        instances = _dedupe_by_external_id(instances)
        return _upsert_turn14_brand_pricing_skip_unchanged(instances)

    return _sweep("pricing company={}".format(company.name), client.get_pricing, flush, pace_seconds=pace_seconds)
