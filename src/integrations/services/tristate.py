"""
Tri-State Enterprises (TSE) distributor integration services.

Unlike every other provider in this package, Tri-State has no bulk catalog feed to pull from --
its real, documented API (src.integrations.clients.tristate.client, PACE's Customer API) only
exposes per-item ``get_inventory_and_pricing`` lookups for the catalog itself. ``get_brands`` WAS
ACL-restricted for our App-Id, but access was granted after following up with Tri-State's rep
(confirmed live 2026-10-15) -- see ``refresh_tse_brands_from_api`` below. So this module is built
around a few narrower jobs instead of one feed-ingest job:

  1. ``refresh_tse_brands_from_api`` -- the authoritative brand source now that get_brands works:
     calls it for one company's connection and backfills TSEBrand.name/warehouse_code by exact
     AAIA code match (TSEBrand rows themselves are still seeded one-time from the bundled
     src/management/commands/data/tristate_brand_codes.json via seed_tse_brands -- this only
     refreshes fields on existing rows, it doesn't create new TSEBrand rows for brands outside
     that curated 146-brand set, even though get_brands itself returns Tri-State's full ~717
     brand catalog).

  2. ``sync_unmapped_tse_brands_to_brands`` -- map TSEBrand rows to Brands: exact AAIA code, then
     compact-key, then fuzzy word-prefix match, otherwise create a new Brands row -- same
     technique as AllPro/Keystone/DLG (see src.integrations.utils.brand_matching). Confirmed live
     2026-10-20 that AAIA-only left 46% of TSE's 146 curated brands unmapped despite most of them
     already existing in Brands under a different code or name variant, the same class of gap
     found and fixed for AllPro the same day.

  3. ``fetch_tristate_inventory_and_pricing_for_company`` -- the actual live sync: for a given
     company's connection, batch-call get_inventory_and_pricing (always with ``aliasing=True``)
     for every TSEParts row, sending the brand's ``warehouse_code`` when known or its
     ``aaia_code`` otherwise -- aliasing resolves either against PACE's real brand_code and
     echoes back the match, so this also self-heals ``TSEBrand.warehouse_code`` for any brand
     ``refresh_tse_brands_from_api`` hasn't covered. Quantity/availability (shared,
     account-agnostic -- confirmed live) is written to TSEParts; cost/list_price (genuinely
     account-specific) is upserted into TSECompanyPricing for this company. This is the function
     wired into integration_pricing_sync_jobs._fetch_raw_pricing.

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
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

# Hand-verified collision, confirmed live 2026-10-20: the fuzzy matcher's otherwise-correct
# word-prefix logic resolves this TSEBrand to Brand id=3978 ("RAM", aaia=BCKL) -- the same
# canonical row AllPro's unrelated "RAM Trailer Parts" also resolves to. "Ram Mounts" (phone/
# tablet mounting hardware) and "RAM Trailer Parts" (towing equipment) are almost certainly two
# different real-world brands sharing one ambiguous single-word canonical entry -- auto-linking
# either would risk misattributing parts to the wrong brand, so this one name is excluded from
# fuzzy resolution rather than trusted. Left for manual review instead.
_FUZZY_MATCH_EXCLUSIONS = {"RAM MOUNTS"}

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TRISTATE-SERVICES]"

PRICING_UPSERT_BATCH = 2000
# Each call measured at ~6.8s round trip regardless of batch size (backend-processing-bound,
# not payload-bound -- see the client's DEFAULT_ITEMS_BATCH_SIZE comment), and a full-catalog
# run is thousands of calls -- concurrency is what keeps that from taking hours. No documented
# or observed rate limit for Tri-State specifically; 8 is a moderate step up from the original 4,
# not an aggressive one.
PRICING_FETCH_WORKERS = 8


def refresh_tse_brands_from_api(company_provider_id: int) -> int:
    """
    Authoritatively backfill TSEBrand.warehouse_code (and refresh .name) from the real
    get_brands endpoint, matched by exact AAIA code -- now that access is granted, this is
    strictly better than waiting for fetch_tristate_inventory_and_pricing_for_company's
    per-item aliasing to discover each brand's code one part at a time. Only touches existing
    TSEBrand rows (the curated 146-brand set from seed_tse_brands); does not create new ones
    for brands in Tri-State's full ~717-brand response that aren't in our curated list. Returns
    the number of TSEBrand rows updated.
    """
    cp = src_models.CompanyProviders.objects.filter(id=company_provider_id).first()
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

    try:
        api_brands = client.get_brands()
    except tristate_exceptions.TriStateException as e:
        logger.error("{} get_brands failed: {}".format(_LOG_PREFIX, e))
        return 0

    by_aaia = {
        b["aaia_brand_code"].upper(): b
        for b in api_brands
        if b.get("aaia_brand_code") and b.get("brand_code")
    }
    if not by_aaia:
        logger.info("{} get_brands returned no usable rows.".format(_LOG_PREFIX))
        return 0

    to_update = []
    for tb in src_models.TSEBrand.objects.all():
        match = by_aaia.get((tb.aaia_code or "").upper())
        if not match:
            continue
        changed = False
        if tb.warehouse_code != match["brand_code"]:
            tb.warehouse_code = match["brand_code"]
            changed = True
        if match.get("name") and tb.name != match["name"]:
            tb.name = match["name"]
            changed = True
        if changed:
            to_update.append(tb)

    if to_update:
        src_models.TSEBrand.objects.bulk_update(to_update, ["warehouse_code", "name"], batch_size=200)

    logger.info(
        "{} Refreshed {} TSEBrand row(s) from get_brands ({} brands returned by the API).".format(
            _LOG_PREFIX, len(to_update), len(api_brands)
        )
    )
    return len(to_update)


def sync_unmapped_tse_brands_to_brands() -> int:
    """
    Map TSEBrand -> Brands: exact AAIA code, then compact-key, then fuzzy word-prefix match --
    same technique as AllPro/Keystone/DLG, see src.integrations.utils.brand_matching. Confirmed
    live 2026-10-20 that AAIA-only left 67 of TSE's 146 curated brands unmapped (46%), yet 54 of
    those 67 already existed in Brands under a different code or name variant -- several of them
    (GEN-Y Hitch, Big Country Truck Accessories, Iconic Accessories, Boomerang Rubber Company)
    are the exact same AllPro-confirmed cases fixed the same day. Supersedes this function's
    earlier AAIA-only approach and its docstring's claim that fuzzy matching was "confirmed less
    reliable" -- that exploration evidently predates this technique's word-prefix alignment.

    For whatever still remains unresolved, creates a new Brands row -- same as AllPro's phase 4,
    per explicit instruction 2026-10-20. Confirmed live only 13 of TSE's 146 curated brands reach
    this phase (AAMP of America, Milwaukee Tool, Polk Audio, Powerbass, etc.) after the three
    phases above, so this is a small, deliberate top-up rather than blind mass-creation. See
    _FUZZY_MATCH_EXCLUSIONS for one hand-verified exception excluded from auto-resolution even
    though the matcher finds it (left unmapped rather than wrongly linked or wrongly created).

    Also upserts BrandProviders(brand, TSE) for every resolved brand, same bookkeeping AllPro's
    equivalent added. Returns the number of TSEBrand rows newly mapped.
    """
    logger.info(
        "{} Syncing unmapped TSE brands to Brands (code, compact, fuzzy, else leave unmapped).".format(_LOG_PREFIX)
    )

    tse_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.TRI_STATE.value,
    ).first()

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

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 1 -- exact AAIA code.
    aaia_codes = {tb.aaia_code.strip().upper() for tb in unmapped if tb.aaia_code}
    brand_by_aaia: typing.Dict[str, src_models.Brands] = {}
    for b in src_models.Brands.objects.filter(aaia_code__in=aaia_codes).order_by("id"):
        key = (b.aaia_code or "").strip().upper()
        if key and key not in brand_by_aaia:
            brand_by_aaia[key] = b
    for tb in unmapped:
        code = (tb.aaia_code or "").strip().upper()
        if code:
            brand = brand_by_aaia.get(code)
            if brand:
                resolved_by_id[tb.id] = brand

    # Phase 2 -- compact key.
    still_unresolved = [tb for tb in unmapped if tb.id not in resolved_by_id]
    compact_matches = 0
    if still_unresolved:
        compact_index = brands_by_compact_key()
        for tb in still_unresolved:
            if (tb.name or "").strip().upper() in _FUZZY_MATCH_EXCLUSIONS:
                continue
            key = normalize_compact_key(tb.name or "")
            if not key:
                continue
            brand = compact_index.get(key)
            if brand:
                resolved_by_id[tb.id] = brand
                compact_matches += 1

    # Phase 3 -- fuzzy word-prefix match.
    still_unresolved = [tb for tb in unmapped if tb.id not in resolved_by_id]
    fuzzy_matches = 0
    if still_unresolved:
        brands_first_index = brands_by_first_token_upper()
        all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
        for tb in still_unresolved:
            if (tb.name or "").strip().upper() in _FUZZY_MATCH_EXCLUSIONS:
                continue
            parts = normalize_upper_words(tb.name or "").split()
            candidates: typing.List[src_models.Brands] = (
                list(brands_first_index.get(parts[0], ())) if parts else []
            )
            if not candidates:
                if all_brands_fallback is None:
                    all_brands_fallback = list(
                        src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                    )
                candidates = all_brands_fallback
            brand = best_fuzzy_brand_match(tb.name or "", candidates)
            if brand:
                resolved_by_id[tb.id] = brand
                fuzzy_matches += 1
                logger.debug(
                    "{} Fuzzy-matched TSE {!r} to Brand id={} name={!r}.".format(
                        _LOG_PREFIX, tb.name, brand.id, brand.name
                    )
                )

    # Phase 4 -- create a new Brands row for anything still unresolved (excluding the hand-
    # verified collision in _FUZZY_MATCH_EXCLUSIONS, which stays unmapped on purpose).
    still_unresolved = [
        tb for tb in unmapped
        if tb.id not in resolved_by_id and (tb.name or "").strip().upper() not in _FUZZY_MATCH_EXCLUSIONS
    ]
    created_brands = 0
    if still_unresolved:
        new_name_to_aaia: typing.Dict[str, typing.Optional[str]] = {}
        for tb in sorted(still_unresolved, key=lambda x: x.id):
            name_upper = (tb.name or "").strip().upper()
            if not name_upper or name_upper in new_name_to_aaia:
                continue
            new_name_to_aaia[name_upper] = (tb.aaia_code or "").strip().upper() or None

        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_name_to_aaia.keys())).values_list(
                "name", flat=True
            )
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=aaia_val,
            )
            for name, aaia_val in new_name_to_aaia.items()
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)

        by_name = {
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_name_to_aaia.keys()))
        }
        for tb in still_unresolved:
            name_upper = (tb.name or "").strip().upper()
            brand = by_name.get(name_upper)
            if brand:
                resolved_by_id[tb.id] = brand

    new_mappings = [
        src_models.BrandTSEBrandMapping(brand=resolved_by_id[tb.id], tse_brand=tb)
        for tb in unmapped
        if tb.id in resolved_by_id
    ]
    if new_mappings:
        pgbulk.upsert(
            src_models.BrandTSEBrandMapping,
            new_mappings,
            unique_fields=["brand", "tse_brand"],
            update_fields=[],
        )

    bp_to_create = []
    if tse_provider and resolved_by_id:
        brand_ids = {b.id for b in resolved_by_id.values()}
        existing_bp_ids = set(
            src_models.BrandProviders.objects.filter(
                provider=tse_provider, brand_id__in=brand_ids,
            ).values_list("brand_id", flat=True)
        )
        bp_to_create = [
            src_models.BrandProviders(brand_id=bid, provider_id=tse_provider.id)
            for bid in brand_ids
            if bid not in existing_bp_ids
        ]
        if bp_to_create:
            src_models.BrandProviders.objects.bulk_create(bp_to_create, ignore_conflicts=True)

    unresolved_names = [
        "{} ({})".format(tb.name, tb.aaia_code) for tb in unmapped if tb.id not in resolved_by_id
    ]
    logger.info(
        "{} Mapped {}/{} unmapped TSE brands (aaia: {}, compact: {}, fuzzy: {}, created: {}); "
        "{} unresolved; {} BrandProviders created. Unresolved: {}".format(
            _LOG_PREFIX, len(resolved_by_id), len(unmapped),
            len(resolved_by_id) - compact_matches - fuzzy_matches - created_brands,
            compact_matches, fuzzy_matches, created_brands,
            len(unresolved_names), len(bp_to_create), ", ".join(unresolved_names[:20]),
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


def _report_sync_progress(cp: src_models.CompanyProviders, message: str) -> None:
    """
    Writes live progress into CompanyProviders.status_reason -- the same field the integrations
    page already surfaces for every other provider's "Ingesting..."/"Waiting..." state (see
    src.api.services.integrations), so anyone can check where a long-running Tri-State sync
    stands at any time through the normal connection status, no SSH/log-tailing required.
    """
    cp.status_reason = message
    cp.status_checked_at = timezone.now()
    cp.save(update_fields=["status_reason", "status_checked_at"])


# A full-catalog run takes well over an hour, so a run can get killed partway through by
# anything that restarts the app process (a deploy, a server restart) -- defaulting to "skip
# whatever already has fresh pricing" means a relaunch picks up roughly where the last one left
# off instead of redoing completed work from zero. Pass 0 to force a full re-fetch of everything.
DEFAULT_RESUME_SKIP_HOURS = 6


def fetch_tristate_inventory_and_pricing_for_company(
    company_provider_id: int, resume_skip_hours: float = DEFAULT_RESUME_SKIP_HOURS
) -> int:
    """
    For one company's Tri-State connection: call get_inventory_and_pricing (aliasing=True) for
    every TSEParts row, sending each brand's warehouse_code when known or its aaia_code
    otherwise. Writes quantity/orderable/per-warehouse breakdown to TSEParts (shared -- see its
    docstring), backfills TSEBrand.warehouse_code from whatever aliasing resolves, and upserts
    cost/list_price into TSECompanyPricing for this company. Returns the number of
    TSECompanyPricing rows upserted (this run only -- rows skipped as already-fresh don't count).

    Resumable: parts with a TSECompanyPricing row for this company updated within the last
    ``resume_skip_hours`` are skipped entirely (see DEFAULT_RESUME_SKIP_HOURS above) -- a
    relaunch after an interruption continues roughly where the previous run stopped rather than
    re-fetching everything. Pass ``resume_skip_hours=0`` to force a full re-fetch.

    Progress (batches done/total, items fetched, rows written) is written to
    CompanyProviders.status_reason as it runs -- see _report_sync_progress.
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

    parts_qs = src_models.TSEParts.objects.select_related("brand")
    already_fresh_count = 0
    if resume_skip_hours:
        cutoff = timezone.now() - timezone.timedelta(hours=resume_skip_hours)
        already_fresh_ids = set(
            src_models.TSECompanyPricing.objects.filter(
                company_id=cp.company_id, updated_at__gte=cutoff
            ).values_list("part_id", flat=True)
        )
        if already_fresh_ids:
            already_fresh_count = len(already_fresh_ids)
            parts_qs = parts_qs.exclude(id__in=already_fresh_ids)

    parts = list(
        parts_qs.values("id", "part_number", "brand_id", "brand__warehouse_code", "brand__aaia_code")
    )
    if already_fresh_count:
        logger.info(
            "{} company_provider_id={}: skipping {} part(s) already synced in the last {}h "
            "(resuming from an earlier run).".format(
                _LOG_PREFIX, company_provider_id, already_fresh_count, resume_skip_hours
            )
        )
    if not parts:
        logger.info(
            "{} company_provider_id={}: nothing left to sync ({} already fresh).".format(
                _LOG_PREFIX, company_provider_id, already_fresh_count
            )
        )
        return 0

    # The token actually sent per item -- warehouse_code when known, else aaia_code (aliasing
    # resolves either, see this module's and the client's docstrings).
    for row in parts:
        row["_sent_token"] = (row.get("brand__warehouse_code") or row["brand__aaia_code"]).upper()

    by_sent_token = {(row["_sent_token"], row["part_number"].upper()): row for row in parts}
    by_aaia = {(row["brand__aaia_code"].upper(), row["part_number"].upper()): row for row in parts}

    batch_size = tristate_client.DEFAULT_ITEMS_BATCH_SIZE
    chunks = [parts[i : i + batch_size] for i in range(0, len(parts), batch_size)]
    total_chunks = len(chunks)

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

    # Written incrementally as results come back (flushed every FLUSH_EVERY_ITEMS, not held in
    # memory until every one of the (potentially thousands of) API calls finishes) -- a run over
    # the full catalog takes long enough in wall-clock time that an all-at-the-end write loses
    # everything on a crash/timeout and shows zero progress for its whole duration.
    part_updates: typing.List[src_models.TSEParts] = []
    pricing_upserts: typing.List[src_models.TSECompanyPricing] = []
    brand_warehouse_code_updates: typing.Dict[int, str] = {}
    total_fetched = 0
    total_parts_written = 0
    total_pricing_written = 0
    chunks_done = 0

    def _flush() -> None:
        nonlocal part_updates, pricing_upserts, total_parts_written, total_pricing_written
        if part_updates:
            src_models.TSEParts.objects.bulk_update(
                part_updates,
                ["quantity_available", "orderable", "warehouse_availability", "inventory_synced_at"],
                batch_size=2000,
            )
            total_parts_written += len(part_updates)
            part_updates = []
        if pricing_upserts:
            pgbulk.upsert(
                src_models.TSECompanyPricing,
                pricing_upserts,
                unique_fields=["part", "company"],
                update_fields=["cost", "list_price", "updated_at"],
                returning=False,
            )
            total_pricing_written += len(pricing_upserts)
            pricing_upserts = []

    skip_note = (
        " ({} part(s) already fresh, skipped)".format(already_fresh_count) if already_fresh_count else ""
    )
    _report_sync_progress(
        cp, "Tri-State sync starting: 0/{} batches ({} parts){}.".format(total_chunks, len(parts), skip_note)
    )

    with ThreadPoolExecutor(max_workers=PRICING_FETCH_WORKERS) as executor:
        futures = [executor.submit(_fetch_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            chunks_done += 1
            now = timezone.now()
            for item in future.result():
                total_fetched += 1
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
                # Resolved via aliasing against a real warehouse brand_code -- backfill so future
                # runs for this brand skip straight to the cheaper non-aliased path.
                # aaia_brand_code is only present when aliasing actually matched the item.
                if aaia_brand_code and item.get("brand_code"):
                    brand_warehouse_code_updates[row["brand_id"]] = item["brand_code"]

            if len(pricing_upserts) >= PRICING_UPSERT_BATCH or len(part_updates) >= PRICING_UPSERT_BATCH:
                _flush()
                pct = round(100 * chunks_done / total_chunks)
                progress_message = (
                    "Tri-State sync in progress: {}/{} batches ({}%), {} items fetched, "
                    "{} TSEParts + {} TSECompanyPricing written so far.".format(
                        chunks_done, total_chunks, pct,
                        total_fetched, total_parts_written, total_pricing_written,
                    )
                )
                logger.info("{} company_provider_id={}: {}".format(_LOG_PREFIX, company_provider_id, progress_message))
                _report_sync_progress(cp, progress_message)

    _flush()

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

    final_message = (
        "Tri-State sync completed at {}: {} items fetched, {} TSEParts updated, "
        "{} TSECompanyPricing rows upserted.".format(
            timezone.now().strftime("%Y-%m-%d %H:%M:%S"), total_fetched, total_parts_written, total_pricing_written
        )
    )
    logger.info("{} company_provider_id={}: {}".format(_LOG_PREFIX, company_provider_id, final_message))
    _report_sync_progress(cp, final_message)
    return total_pricing_written


def sync_tristate_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """Entry point wired into integration_pricing_sync_jobs._fetch_raw_pricing."""
    fetch_tristate_inventory_and_pricing_for_company(company_provider_id)
