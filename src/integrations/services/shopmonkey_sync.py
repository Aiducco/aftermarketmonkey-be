"""
ShopMonkey inventory sync -- see the module docstring in src/models.py just above
ShopMonkeySyncVendor for the overall picture. Pushes a company's own already-ingested distributor
data (MasterPart / ProviderPart / ProviderPartInventory / ProviderPartCompanyPricing -- all
already multi-tenant) into that company's ShopMonkey account as inventory parts.

Ported from a standalone single-tenant tool built for Trident Motorsports
(~/Documents/code/shopmonkey, still live there: ~88K parts synced). Same vendor -> brand -> part
hierarchy and the same create-once / diff-and-update / mark-missing-as-obsolete behavior as that
tool's src/services/inventory.py -- preserved deliberately, including its quirks (see
src.integrations.clients.shopmonkey.client's own docstring for the vendorId: null one, and
_part_name_for_status below for the "[OBSOLETE] " / "[ERROR UPDATING PART] " name-prefix
convention). The one real difference: we read parts from data already ingested per company
instead of re-fetching from each distributor directly, which is what makes this safe to offer
per-company instead of bespoke per-client tooling.

Multi-vendor: a brand syncs from one *or many* of the company's connected distributors, not
exactly one. One ShopMonkey inventory part always represents one MasterPart (never one
ProviderPart) -- availability is summed across every chosen vendor's own
ProviderPartInventory.warehouse_total_qty, and pricing/binLocation come from the primary vendor,
falling down a priority list of the additional vendors for any master part the primary doesn't
itself carry. sync_brand_parts() is the only function that talks to ShopMonkey, whether invoked
directly by the cron (sync_all_enabled_brands) or via the async queue
(enqueue_sync_brand_run / claim_next_queued_execution / process_shopmonkey_sync_jobs) -- there is
deliberately no second sync path for the "run now" button or for anything else.

Status model, mapped from signals we already have instead of the old tool's provider-specific
"discontinued SKUs" feed:
  - a master part whose contributing ProviderParts are ALL is_discontinued -> INACTIVE /
    "[OBSOLETE] " (a clean, known signal, uniform across every provider -- better than the old
    tool had). A part carried by two vendors where only one has discontinued it stays ACTIVE --
    the other vendor can still supply it.
  - a part previously synced that no chosen vendor carries any more (reassigned provider,
    genuinely vanished from every one of them) -> ERROR / "[ERROR UPDATING PART] ", matching the
    old tool's own use of that status for exactly this "disappeared unexpectedly" case (see its
    _mark_missing_parts_as_error). Counted separately from a push-attempt failure in the
    execution metadata (see sync_brand_parts), even though both set the same per-part status.
"""
import decimal
import logging
import typing

from django.core.cache import cache
from django.db import transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.api.services import feature_flags as feature_flags_services
from src.integrations.clients.shopmonkey import client as shopmonkey_client
from src.integrations.clients.shopmonkey import exceptions as shopmonkey_exceptions

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SHOPMONKEY-SYNC]"

# available_brands_for_company's brand/provider/part_count aggregation is a GROUP BY over every
# ProviderPart row for the company's connected providers -- for Trident (connected to most
# providers) that's 4.6M+ rows, confirmed live to take 30s+ and 502 at the web server's timeout.
# That data only changes when a distributor's catalog re-syncs (hours, not seconds), so it's
# cached here rather than recomputed per request. Warmed proactively by the
# warm_shopmonkey_available_brands_cache management command (same cadence as
# sync_shopmonkey_inventory) so a live request should always hit a warm cache; the TTL below is
# just a safety net in case warming ever falls behind.
_AVAILABLE_BRANDS_CACHE_KEY_FMT = "shopmonkey:available_brands_base:{company_id}"
_AVAILABLE_BRANDS_CACHE_TTL_SECONDS = 6 * 60 * 60

_OBSOLETE_PREFIX = "[OBSOLETE] "
_ERROR_PREFIX = "[ERROR UPDATING PART] "

# Bounds how many per-run failures the execution metadata keeps inline -- unbounded failure lists
# in JSONB at 88K-parts scale is how this column gets unusable (see src.models.ShopMonkeySyncExecution).
_MAX_INLINE_FAILURES = 100

# BrandProviderKind.value -> binLocation code, verified against the live ShopMonkey account.
# Deliberately NOT derived from kind_name[:3] -- Turn 14 and Keystone's historical codes don't
# match that convention (kind_name[:3] would give "TUR"/"KEY", not "T14"/"KST"), confirmed from
# seed_providers.py setting kind_name = enum.name. SDC is excluded on purpose: it's a catalog in
# the old tool, never one of our distributors.
_BIN_NAME_OVERRIDES = {
    src_enums.BrandProviderKind.TURN_14.value: "T14",
    src_enums.BrandProviderKind.KEYSTONE.value: "KST",
    src_enums.BrandProviderKind.WHEELPROS.value: "WHE",
    src_enums.BrandProviderKind.MEYER.value: "MEY",
    src_enums.BrandProviderKind.ROUGH_COUNTRY.value: "ROU",
}


def _bin_name_for_provider(provider: src_models.Providers) -> str:
    return _BIN_NAME_OVERRIDES.get(provider.kind, (provider.kind_name or "")[:3].upper())


# --------------------------------------------------------------------------------------
# Company <-> connected providers <-> available brands
# --------------------------------------------------------------------------------------
def company_connected_provider_ids(company_id: int) -> typing.Set[int]:
    """
    Provider ids this company has an active connection to, with real feed credentials -- the
    same "connected" definition used by the distributor integrations catalog (see
    src.api.services.integrations.get_providers_catalog's own "connected" comment).
    """
    return {
        cp.provider_id
        for cp in src_models.CompanyProviders.objects.filter(company_id=company_id, active=True)
        if (cp.credentials or {}).get("feed")
    }


def _compute_available_brands_base(company_id: int) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    The expensive(-ish) part of available_brands_for_company: brand/provider/part_count, with
    none of this company's own sync-configuration state overlaid (that part is cheap and always
    recomputed fresh -- see available_brands_for_company). Never call this directly from a
    request path; go through the cached wrapper below.

    Reads BrandProviders (one row per (provider, brand) pair that has ever existed, 7,567 rows
    platform-wide at the time this was written) rather than GROUP BY-ing ProviderPart itself (a
    live aggregation over 4.6M+ rows for a company connected to most providers -- confirmed live
    to take 30s+ and 502 at the web server's timeout on its own, i.e. the cache above isn't
    enough by itself if what it's caching is still this expensive to *compute* on every warm
    cycle). part_count on BrandProviders is precomputed by refresh_brand_provider_part_counts,
    not live here -- see that field's own docstring in src/models.py.
    """
    connected_provider_ids = company_connected_provider_ids(company_id)
    if not connected_provider_ids:
        return []

    rows = src_models.BrandProviders.objects.filter(provider_id__in=connected_provider_ids).select_related(
        "brand", "provider"
    )

    by_brand: typing.Dict[int, typing.Dict[str, typing.Any]] = {}
    for row in rows:
        if not row.brand_id:
            continue
        entry = by_brand.setdefault(row.brand_id, {"brand_name": row.brand.name, "providers": []})
        entry["providers"].append(
            {
                "provider_id": row.provider_id,
                "provider_kind_name": row.provider.kind_name if row.provider_id else None,
                # None until refresh_brand_provider_part_counts has run at least once for this pair --
                # treated as 0 for sort purposes only, not shown as a confirmed zero to the FE.
                "part_count": row.part_count,
            }
        )

    result = []
    for brand_id, entry in by_brand.items():
        entry["providers"].sort(key=lambda p: -(p["part_count"] or 0))
        result.append({"brand_id": brand_id, "brand_name": entry["brand_name"], "providers": entry["providers"]})
    return sorted(result, key=lambda r: r["brand_name"] or "")


def _cache_get(cache_key: str) -> typing.Optional[typing.Any]:
    """
    The cache is an optimization here, never a dependency: every brand list it holds can be
    recomputed from the database. So a cache that is down, misconfigured or evicting must degrade
    to the slow path, never surface as a 500 on the brand-picker endpoint. Mirrors the guarded
    pattern in src.integrations.services.purchase_order_jobs._shipping_method_names.
    """
    try:
        return cache.get(cache_key)
    except Exception:
        logger.warning(
            "{} Cache unavailable reading '{}'; falling back to a live (uncached) "
            "recomputation.".format(_LOG_PREFIX, cache_key)
        )
        return None


def _cache_set(cache_key: str, value: typing.Any, ttl_seconds: int) -> None:
    try:
        cache.set(cache_key, value, ttl_seconds)
    except Exception:
        logger.warning(
            "{} Cache unavailable writing '{}'; the next read will recompute live instead of "
            "using the cache.".format(_LOG_PREFIX, cache_key)
        )


def refresh_available_brands_cache(company_id: int) -> int:
    """Forces recomputation of the slow base list and overwrites the cache. Returns the brand count. Called by warm_shopmonkey_available_brands_cache; also safe to call ad hoc after a distributor catalog re-sync."""
    base = _compute_available_brands_base(company_id)
    _cache_set(_AVAILABLE_BRANDS_CACHE_KEY_FMT.format(company_id=company_id), base, _AVAILABLE_BRANDS_CACHE_TTL_SECONDS)
    return len(base)


def available_brands_for_company(company_id: int) -> typing.List[typing.Dict[str, typing.Any]]:
    """
    Brands this company could sync to ShopMonkey, and which of its own connected providers carry
    each one -- the choice set the brand-picker UI needs. A fixed allowlist the way the old tool
    hardcoded one enum per distributor wouldn't work here -- a company only ever sees brands its
    own connected distributors actually have, which differs per company.

    The brand/provider/part_count portion comes from cache (see refresh_available_brands_cache) --
    confirmed live that computing it per-request times out at Trident's real data volume (4.6M+
    ProviderPart rows). This company's own sync-configuration (sync_brand_id/enabled/
    current_provider_ids/primary_provider_id) is NOT part of that cache -- it's a small, fast,
    per-company query, always run fresh here so toggling a brand on/off (or changing its vendor
    set) is reflected immediately rather than waiting on the cache TTL.
    """
    cache_key = _AVAILABLE_BRANDS_CACHE_KEY_FMT.format(company_id=company_id)
    base = _cache_get(cache_key)
    if base is None:
        # Cache miss (not yet warmed, TTL lapsed with warming behind, or the cache itself is
        # unreachable) -- falls back to the slow live path rather than returning nothing or
        # failing the request, and populates the cache for next time.
        base = _compute_available_brands_base(company_id)
        _cache_set(cache_key, base, _AVAILABLE_BRANDS_CACHE_TTL_SECONDS)
    if not base:
        return []

    brand_ids = [row["brand_id"] for row in base]
    existing_sync_brands = {
        sb.brand_id: sb
        for sb in src_models.ShopMonkeySyncBrand.objects.filter(company_id=company_id, brand_id__in=brand_ids)
        .select_related("vendor")
        .prefetch_related("additional_vendors__vendor")
    }

    result = []
    for row in base:
        sync_brand = existing_sync_brands.get(row["brand_id"])
        if sync_brand:
            current_provider_ids = [sync_brand.vendor.provider_id] + [
                link.vendor.provider_id for link in sync_brand.additional_vendors.all()
            ]
            primary_provider_id = sync_brand.vendor.provider_id
        else:
            current_provider_ids, primary_provider_id = [], None
        result.append(
            {
                **row,
                "sync_brand_id": sync_brand.id if sync_brand else None,
                "enabled": sync_brand.enabled if sync_brand else False,
                "current_provider_ids": current_provider_ids,
                "primary_provider_id": primary_provider_id,
            }
        )
    return result


# --------------------------------------------------------------------------------------
# Configuration: enabling/disabling a brand and choosing its vendor set
# --------------------------------------------------------------------------------------
def _get_or_create_vendor(company_id: int, provider_id: int) -> src_models.ShopMonkeySyncVendor:
    provider = src_models.Providers.objects.get(id=provider_id)
    vendor, _created = src_models.ShopMonkeySyncVendor.objects.get_or_create(
        company_id=company_id,
        provider_id=provider_id,
        defaults={
            "status": src_enums.ShopMonkeySyncStatus.INACTIVE.value,
            "status_name": src_enums.ShopMonkeySyncStatus.INACTIVE.name,
            "bin_name": _bin_name_for_provider(provider),
        },
    )
    return vendor


def configure_sync_brand(
    company_id: int,
    brand_id: int,
    provider_ids: typing.List[int],
    primary_provider_id: int,
    enabled: bool = True,
) -> src_models.ShopMonkeySyncBrand:
    """
    Enable (or reconfigure) a brand for sync against a set of providers. Idempotent and fully
    replaces the additional-vendor set on every call -- safe to call again with a different
    provider_ids/primary_provider_id after the brand is already enabled (editable after first
    enable). shopmonkey_external_id on the brand and on every already-created vendor is untouched
    either way. This is the ONLY function that writes a ShopMonkeySyncBrand's vendor set --
    both POST and PATCH on the API side resolve down to this one call, never a separate
    toggle-only reconciliation path.
    """
    if primary_provider_id not in provider_ids:
        raise ValueError("primary_provider_id must be one of provider_ids.")

    primary_vendor = _get_or_create_vendor(company_id, primary_provider_id)
    sync_brand, created = src_models.ShopMonkeySyncBrand.objects.get_or_create(
        company_id=company_id,
        brand_id=brand_id,
        defaults={
            "vendor": primary_vendor,
            "enabled": enabled,
            "status": src_enums.ShopMonkeySyncStatus.INACTIVE.value,
            "status_name": src_enums.ShopMonkeySyncStatus.INACTIVE.name,
        },
    )
    if not created:
        sync_brand.vendor = primary_vendor
        sync_brand.enabled = enabled
        sync_brand.save(update_fields=["vendor", "enabled", "updated_at"])

    additional_provider_ids = [pid for pid in provider_ids if pid != primary_provider_id]
    existing_links = {
        link.vendor.provider_id: link
        for link in src_models.ShopMonkeySyncBrandVendor.objects.filter(sync_brand=sync_brand).select_related("vendor")
    }
    # Drop anything no longer selected.
    stale_provider_ids = set(existing_links) - set(additional_provider_ids)
    if stale_provider_ids:
        src_models.ShopMonkeySyncBrandVendor.objects.filter(
            sync_brand=sync_brand, vendor__provider_id__in=stale_provider_ids
        ).delete()
    # Upsert the rest, priority = position in the given list.
    for priority, provider_id in enumerate(additional_provider_ids, start=1):
        vendor = _get_or_create_vendor(company_id, provider_id)
        link = existing_links.get(provider_id)
        if link:
            if link.priority != priority:
                link.priority = priority
                link.save(update_fields=["priority", "updated_at"])
        else:
            src_models.ShopMonkeySyncBrandVendor.objects.create(sync_brand=sync_brand, vendor=vendor, priority=priority)
    return sync_brand


# --------------------------------------------------------------------------------------
# ShopMonkey client per company
# --------------------------------------------------------------------------------------
def _client_for_company(company_id: int) -> shopmonkey_client.ShopMonkeyApiClient:
    connection = src_models.CompanyShopManagementProviders.objects.filter(
        company_id=company_id,
        provider__kind=src_enums.ShopManagementProviderKind.SHOPMONKEY.value,
        active=True,
    ).first()
    if not connection or not (connection.credentials or {}).get("api_key"):
        raise ValueError("No active ShopMonkey connection with credentials for this company.")
    return shopmonkey_client.ShopMonkeyApiClient(credentials=connection.credentials)


# --------------------------------------------------------------------------------------
# Vendor / brand creation on ShopMonkey (create-once, same as the old tool)
# --------------------------------------------------------------------------------------
def _ensure_vendor_created_on_shopmonkey(
    vendor: src_models.ShopMonkeySyncVendor, client: shopmonkey_client.ShopMonkeyApiClient
) -> None:
    if vendor.shopmonkey_external_id:
        return
    provider = src_models.Providers.objects.get(id=vendor.provider_id)
    shopmonkey_vendor = client.create_vendor(vendor_name=provider.kind_name)
    vendor.shopmonkey_external_id = shopmonkey_vendor["id"]
    vendor.status = src_enums.ShopMonkeySyncStatus.ACTIVE.value
    vendor.status_name = src_enums.ShopMonkeySyncStatus.ACTIVE.name
    vendor.save(update_fields=["shopmonkey_external_id", "status", "status_name", "updated_at"])


def _ensure_brand_created_on_shopmonkey(
    sync_brand: src_models.ShopMonkeySyncBrand, client: shopmonkey_client.ShopMonkeyApiClient
) -> None:
    if sync_brand.shopmonkey_external_id:
        return
    brand = src_models.Brands.objects.get(id=sync_brand.brand_id)
    shopmonkey_brand = client.create_brand(entity=sync_brand.entity, brand_name=brand.name)
    sync_brand.shopmonkey_external_id = shopmonkey_brand["id"]
    sync_brand.status = src_enums.ShopMonkeySyncStatus.ACTIVE.value
    sync_brand.status_name = src_enums.ShopMonkeySyncStatus.ACTIVE.name
    sync_brand.save(update_fields=["shopmonkey_external_id", "status", "status_name", "updated_at"])


# --------------------------------------------------------------------------------------
# Part-level sync
# --------------------------------------------------------------------------------------
def _clean_part_name(name: str) -> str:
    """
    Strip a status prefix the way the old tool does: match the bare tag, then trim whitespace,
    applied to each tag in turn. Matching the bare tag rather than the tag plus a trailing space
    matters because we inherit ~94K names already in ShopMonkey -- one hand-edited to
    "[OBSOLETE]Foo", or with doubled spaces, would otherwise keep its prefix and get a second one
    stacked on top of it on the next status flip.
    """
    clean = name
    for tag in (_OBSOLETE_PREFIX.strip(), _ERROR_PREFIX.strip()):
        if clean.startswith(tag):
            clean = clean[len(tag) :].strip()
    return clean


def _part_name_for_status(name: str, status: int) -> str:
    clean = _clean_part_name(name or "")
    if status == src_enums.ShopMonkeySyncPartStatus.INACTIVE.value:
        return "{}{}".format(_OBSOLETE_PREFIX, clean)
    if status == src_enums.ShopMonkeySyncPartStatus.ERROR.value:
        return "{}{}".format(_ERROR_PREFIX, clean)
    return clean


def _cents(value: typing.Optional[typing.Any]) -> int:
    if value is None:
        return 0
    return int(round(float(value) * 100))


# The old tool never had a single pricing rule -- each distributor's own client built retail and
# wholesale from that feed's columns, and three of the six then applied the same 20% floor by
# hand (Turn 14, Meyer, Wheel Pros -- between them ~71K of its ~88K parts). We don't need the
# per-distributor half of that: ingestion has already normalised every feed into
# ProviderPartCompanyPricing, so what's left to reproduce is the shared part, which is the
# priority order and the floor:
#
#   retail = first non-null of (MAP, jobber, retail, msrp), else cost * 1.2
#   retail = max(retail, cost * 1.2)
#   wholesale = cost
#
# Applying the floor to every distributor rather than three also fixes the two odd ones out:
# Keystone hardcoded retail to 0 ("As specified"), and Rough Country passed the feed's price
# straight through unfloored. Both now land on cost * 1.2 or better instead of shipping at or
# below cost.
_RETAIL_MINIMUM_MARKUP = decimal.Decimal("1.2")


def _retail_and_wholesale_cents(
    pricing: typing.Optional["src_models.ProviderPartCompanyPricing"],
) -> typing.Tuple[int, int]:
    """Returns (retail_cents, wholesale_cents). See the rule above."""
    if pricing is None:
        return 0, 0

    cost = decimal.Decimal(str(pricing.cost)) if pricing.cost is not None else decimal.Decimal("0")
    floor = cost * _RETAIL_MINIMUM_MARKUP

    retail = None
    for candidate in (pricing.map_price, pricing.jobber_price, pricing.retail_price, pricing.msrp):
        if candidate is not None:
            retail = decimal.Decimal(str(candidate))
            break
    if retail is None:
        retail = floor
    if retail < floor:
        retail = floor

    return _cents(retail), _cents(cost)


def _master_parts_for_sync_brand(
    brand_id: int,
    provider_ids: typing.List[int],
    batch_size: int = 500,
) -> typing.Iterator[typing.Tuple[src_models.MasterPart, typing.Dict[int, src_models.ProviderPart]]]:
    """
    Yields (MasterPart, {provider_id: ProviderPart}) for every master part in this brand carried
    by at least one of provider_ids. Two-step and batched by master_part id on purpose: a single
    query joining provider_parts (4.6M+ rows at Trident's scale) against an IN-list of ~39
    provider_ids has filled the prod temp disk before (see the repo's own standing gotcha). Step
    1 hits only master_parts, filtered on its own indexed brand_id column (implicit FK index, no
    join) -- for any one brand this is at most a few thousand rows. Step 2 then looks up
    ProviderPart in bounded batches, filtered on (master_part_id IN <=500 ids) AND (provider_id IN
    <=~40 ids) -- both sides of that filter hit an indexed column, so Postgres does an index scan
    per batch instead of a hash join over the whole table.
    """
    master_part_ids = list(
        src_models.MasterPart.objects.filter(brand_id=brand_id).order_by("id").values_list("id", flat=True)
    )
    for i in range(0, len(master_part_ids), batch_size):
        batch_ids = master_part_ids[i : i + batch_size]
        provider_parts = src_models.ProviderPart.objects.filter(
            master_part_id__in=batch_ids, provider_id__in=provider_ids
        ).select_related("inventory")
        by_master_part: typing.Dict[int, typing.Dict[int, src_models.ProviderPart]] = {}
        for pp in provider_parts:
            by_master_part.setdefault(pp.master_part_id, {})[pp.provider_id] = pp
        if not by_master_part:
            continue
        master_parts = src_models.MasterPart.objects.filter(id__in=list(by_master_part.keys()))
        for mp in master_parts:
            yield mp, by_master_part[mp.id]


def _sync_one_part(
    sync_brand: src_models.ShopMonkeySyncBrand,
    master_part: src_models.MasterPart,
    provider_parts_by_provider: typing.Dict[int, src_models.ProviderPart],
    ordered_vendors: typing.List[src_models.ShopMonkeySyncVendor],
    pricing_by_provider_part_id: typing.Dict[int, src_models.ProviderPartCompanyPricing],
    client: shopmonkey_client.ShopMonkeyApiClient,
    existing_sync_part: typing.Optional[src_models.ShopMonkeySyncPart],
) -> str:
    """Returns one of "created"/"updated"/"unchanged"/"obsoleted" for the caller's metadata
    counts. Raises shopmonkey_exceptions.ShopMonkeyException on a failed API call -- the caller
    persists last_error, this function never swallows it."""
    # Summed availability across every contributing ProviderPart's own inventory row.
    #
    # This is recorded and shown in our own UI; it is deliberately NOT sent to ShopMonkey. Their
    # `quantity` field means "units this shop has on hand", and distributor availability is not
    # that -- writing 73 there because two distributors between them stock 73 tells ShopMonkey
    # the shop owns parts it does not, lets staff add them to a repair order as in-stock, and
    # makes the account's own inventory valuation (totalCost/totalValue) fiction. The standalone
    # tool this was ported from never sent quantity either, across ~88K parts and several years;
    # we match it on the wire. See the payload in src.integrations.clients.shopmonkey.client.
    #
    # ProviderPartInventory is a reverse OneToOne (related_name="inventory") -- absent for a part
    # never inventory-synced, and accessing pp.inventory directly raises DoesNotExist in that
    # case (the FK lives on ProviderPartInventory pointing at ProviderPart, not the other way),
    # so every read goes through getattr(pp, "inventory", None) first.
    quantity = sum(
        getattr(pp, "inventory", None).warehouse_total_qty
        for pp in provider_parts_by_provider.values()
        if getattr(pp, "inventory", None) is not None
    )

    # Pricing + binLocation: walk primary-first. The first vendor that actually CARRIES this
    # master part (has a ProviderPart row here, regardless of whether a pricing row exists for
    # it) wins. sourced_vendor is never None here: provider_parts_by_provider is only ever
    # non-empty for a master part carried by >=1 of ordered_vendors' provider_ids (see
    # _master_parts_for_sync_brand).
    sourced_vendor = None
    sourced_provider_part = None
    for vendor in ordered_vendors:
        pp = provider_parts_by_provider.get(vendor.provider_id)
        if pp is not None:
            sourced_vendor, sourced_provider_part = vendor, pp
            break

    pricing = pricing_by_provider_part_id.get(sourced_provider_part.id)
    retail_cost_cents, wholesale_cost_cents = _retail_and_wholesale_cents(pricing)

    # INACTIVE only when EVERY contributing ProviderPart is discontinued -- a part carried by two
    # vendors where only one has discontinued it stays ACTIVE, since the other can still supply it.
    all_discontinued = all(pp.is_discontinued for pp in provider_parts_by_provider.values())
    status = (
        src_enums.ShopMonkeySyncPartStatus.INACTIVE.value
        if all_discontinued
        else src_enums.ShopMonkeySyncPartStatus.ACTIVE.value
    )
    # SKU: keep what was pushed before (so a part ported from the old tool keeps its historical
    # SKU rather than being silently renumbered) -- else derive fresh from the master part.
    sku = (
        existing_sync_part.last_pushed_sku
        if existing_sync_part and existing_sync_part.last_pushed_sku
        else (master_part.sku or master_part.part_number)
    )
    name = _part_name_for_status(master_part.description or sku, status)
    bin_location = sourced_vendor.bin_name
    number = master_part.part_number

    # Not just "no row exists yet": a part that failed on its very first push also has a row
    # (see _record_sync_part_failure) but no shopmonkey_external_id, since it never actually
    # reached ShopMonkey. Without this second condition, a retried first-time failure would call
    # update_inventory_part with external_id=None instead of retrying the create -- a correctness
    # bug, not a cosmetic one, since it would never succeed.
    needs_create = not existing_sync_part or not existing_sync_part.shopmonkey_external_id
    if needs_create:
        shopmonkey_part = client.create_inventory_part(
            name=name,
            vendor_id=sourced_vendor.shopmonkey_external_id,
            brand_id=sync_brand.shopmonkey_external_id,
            number=number,
            sku=sku,
            retail_cost=retail_cost_cents,
            wholesale_cost=wholesale_cost_cents,
            vendor_bin_location=bin_location,
        )
        if existing_sync_part:
            existing_sync_part.shopmonkey_external_id = shopmonkey_part["id"]
            existing_sync_part.last_pushed_name = name
            existing_sync_part.last_pushed_sku = sku
            existing_sync_part.last_pushed_retail_cost_cents = retail_cost_cents
            existing_sync_part.last_pushed_wholesale_cost_cents = wholesale_cost_cents
            existing_sync_part.last_pushed_quantity = quantity
            existing_sync_part.last_pushed_bin_location = bin_location
            existing_sync_part.sourced_from_vendor = sourced_vendor
            existing_sync_part.last_error = None
            existing_sync_part.status = status
            existing_sync_part.status_name = src_enums.ShopMonkeySyncPartStatus(status).name
            existing_sync_part.save(
                update_fields=[
                    "shopmonkey_external_id",
                    "last_pushed_name",
                    "last_pushed_sku",
                    "last_pushed_retail_cost_cents",
                    "last_pushed_wholesale_cost_cents",
                    "last_pushed_quantity",
                    "last_pushed_bin_location",
                    "sourced_from_vendor",
                    "last_error",
                    "status",
                    "status_name",
                    "updated_at",
                ]
            )
        else:
            src_models.ShopMonkeySyncPart.objects.create(
                sync_brand=sync_brand,
                master_part=master_part,
                shopmonkey_external_id=shopmonkey_part["id"],
                last_pushed_name=name,
                last_pushed_sku=sku,
                last_pushed_retail_cost_cents=retail_cost_cents,
                last_pushed_wholesale_cost_cents=wholesale_cost_cents,
                last_pushed_quantity=quantity,
                last_pushed_bin_location=bin_location,
                sourced_from_vendor=sourced_vendor,
                status=status,
                status_name=src_enums.ShopMonkeySyncPartStatus(status).name,
            )
        return "obsoleted" if status == src_enums.ShopMonkeySyncPartStatus.INACTIVE.value else "created"

    changed = (
        existing_sync_part.last_pushed_name != name
        or existing_sync_part.last_pushed_sku != sku
        or existing_sync_part.last_pushed_retail_cost_cents != retail_cost_cents
        or existing_sync_part.last_pushed_wholesale_cost_cents != wholesale_cost_cents
        or existing_sync_part.last_pushed_bin_location != bin_location
        or existing_sync_part.status != status
    )
    was_inactive = status == src_enums.ShopMonkeySyncPartStatus.INACTIVE.value
    if not changed:
        return "obsoleted" if was_inactive else "unchanged"

    shopmonkey_part = client.update_inventory_part(
        external_id=existing_sync_part.shopmonkey_external_id,
        name=name,
        vendor_id=sourced_vendor.shopmonkey_external_id,
        brand_id=sync_brand.shopmonkey_external_id,
        number=number,
        sku=sku,
        retail_cost=retail_cost_cents,
        wholesale_cost=wholesale_cost_cents,
        vendor_bin_location=bin_location,
    )
    existing_sync_part.shopmonkey_external_id = shopmonkey_part.get("id", existing_sync_part.shopmonkey_external_id)
    existing_sync_part.last_pushed_name = name
    existing_sync_part.last_pushed_sku = sku
    existing_sync_part.last_pushed_retail_cost_cents = retail_cost_cents
    existing_sync_part.last_pushed_wholesale_cost_cents = wholesale_cost_cents
    existing_sync_part.last_pushed_quantity = quantity
    existing_sync_part.last_pushed_bin_location = bin_location
    existing_sync_part.sourced_from_vendor = sourced_vendor
    existing_sync_part.last_error = None  # clear a stale error on a successful push
    existing_sync_part.status = status
    existing_sync_part.status_name = src_enums.ShopMonkeySyncPartStatus(status).name
    existing_sync_part.save(
        update_fields=[
            "shopmonkey_external_id",
            "last_pushed_name",
            "last_pushed_sku",
            "last_pushed_retail_cost_cents",
            "last_pushed_wholesale_cost_cents",
            "last_pushed_quantity",
            "last_pushed_bin_location",
            "sourced_from_vendor",
            "last_error",
            "status",
            "status_name",
            "updated_at",
        ]
    )
    return "obsoleted" if was_inactive else "updated"


def _record_sync_part_failure(
    sync_brand: src_models.ShopMonkeySyncBrand,
    master_part: src_models.MasterPart,
    existing_sync_part: typing.Optional[src_models.ShopMonkeySyncPart],
    error: Exception,
) -> None:
    """Persists a push-attempt failure onto the per-part row, creating one if this master part
    has never synced before. Today a part that fails on its very first push would otherwise leave
    no row at all (the create() call lives on the success path only) -- that silently dropped the
    failure from per-part visibility, leaving only the execution-level failures list (bounded,
    see sync_brand_parts) as the record of it."""
    message = str(error)[:4000]
    if existing_sync_part:
        existing_sync_part.status = src_enums.ShopMonkeySyncPartStatus.ERROR.value
        existing_sync_part.status_name = src_enums.ShopMonkeySyncPartStatus.ERROR.name
        existing_sync_part.last_error = message
        existing_sync_part.save(update_fields=["status", "status_name", "last_error", "updated_at"])
    else:
        src_models.ShopMonkeySyncPart.objects.create(
            sync_brand=sync_brand,
            master_part=master_part,
            status=src_enums.ShopMonkeySyncPartStatus.ERROR.value,
            status_name=src_enums.ShopMonkeySyncPartStatus.ERROR.name,
            last_error=message,
        )


def _mark_sync_part_unmanaged(
    sync_part: src_models.ShopMonkeySyncPart, client: shopmonkey_client.ShopMonkeyApiClient
) -> None:
    """A previously-synced part whose master part is no longer carried by any of this brand's
    chosen vendors -- mirrors the old tool's _mark_missing_parts_as_error / _mark_part_as_error.
    last_error distinguishes this ("no chosen vendor carries it any more") from a push-attempt
    failure (_record_sync_part_failure) even though both land on the same ERROR status."""
    if sync_part.status == src_enums.ShopMonkeySyncPartStatus.ERROR.value:
        return
    name = _part_name_for_status(sync_part.last_pushed_name or "", src_enums.ShopMonkeySyncPartStatus.ERROR.value)
    master_part = sync_part.master_part
    client.update_inventory_part(
        external_id=sync_part.shopmonkey_external_id,
        name=name,
        vendor_id=sync_part.sourced_from_vendor.shopmonkey_external_id if sync_part.sourced_from_vendor_id else None,
        brand_id=sync_part.sync_brand.shopmonkey_external_id,
        number=master_part.part_number,
        sku=sync_part.last_pushed_sku or (master_part.sku or master_part.part_number),
        retail_cost=int(sync_part.last_pushed_retail_cost_cents or 0),
        wholesale_cost=int(sync_part.last_pushed_wholesale_cost_cents or 0),
        vendor_bin_location=sync_part.last_pushed_bin_location,
    )
    sync_part.last_pushed_name = name
    sync_part.last_error = "No chosen vendor carries this master part any more."
    sync_part.status = src_enums.ShopMonkeySyncPartStatus.ERROR.value
    sync_part.status_name = src_enums.ShopMonkeySyncPartStatus.ERROR.name
    sync_part.save(update_fields=["last_pushed_name", "last_error", "status", "status_name", "updated_at"])


# --------------------------------------------------------------------------------------
# One brand's full sync run
# --------------------------------------------------------------------------------------
def _fail_execution(execution: src_models.ShopMonkeySyncExecution, message: str) -> src_models.ShopMonkeySyncExecution:
    logger.error("{} {}".format(_LOG_PREFIX, message))
    execution.status = src_enums.ShopMonkeySyncExecutionStatus.FAILURE.value
    execution.message = message
    execution.save(update_fields=["status", "message", "updated_at"])
    return execution


def sync_brand_parts(
    sync_brand_id: int,
    execution: typing.Optional[src_models.ShopMonkeySyncExecution] = None,
) -> src_models.ShopMonkeySyncExecution:
    """Full sync for one ShopMonkeySyncBrand: ensure every chosen vendor/the brand exist on
    ShopMonkey, then create/update every part from the union of those vendors' own ProviderPart
    data (one ShopMonkey part per MasterPart, quantity summed, pricing/binLocation from whichever
    vendor sourced it), diffed against what was last pushed, and mark anything no longer carried
    by any chosen vendor as an error -- mirrors the old tool's
    fetch_and_sync_inventory_brand_parts end to end, widened from one vendor to a priority-ordered
    set. ``execution`` is passed in by the async queue path (process_shopmonkey_sync_jobs), which
    already created the row as QUEUED and claimed it to RUNNING; the cron path (sync_all_enabled_brands)
    leaves it None and this function creates+runs synchronously instead."""
    sync_brand = src_models.ShopMonkeySyncBrand.objects.select_related(
        "vendor", "vendor__provider", "brand", "company"
    ).get(id=sync_brand_id)

    if execution is None:
        execution = src_models.ShopMonkeySyncExecution.objects.create(
            company_id=sync_brand.company_id,
            sync_brand=sync_brand,
            status=src_enums.ShopMonkeySyncExecutionStatus.RUNNING.value,
            message="Starting sync for brand {!r}.".format(sync_brand.brand.name),
            metadata={},
        )
    else:
        execution.status = src_enums.ShopMonkeySyncExecutionStatus.RUNNING.value
        execution.message = "Starting sync for brand {!r}.".format(sync_brand.brand.name)
        execution.save(update_fields=["status", "message", "updated_at"])

    started_at = timezone.now()
    try:
        client = _client_for_company(sync_brand.company_id)
    except ValueError as e:
        return _fail_execution(execution, str(e))

    try:
        _ensure_vendor_created_on_shopmonkey(sync_brand.vendor, client)
        _ensure_brand_created_on_shopmonkey(sync_brand, client)
    except shopmonkey_exceptions.ShopMonkeyException as e:
        return _fail_execution(execution, "Could not ensure vendor/brand on ShopMonkey: {}".format(e))

    additional = list(
        src_models.ShopMonkeySyncBrandVendor.objects.filter(sync_brand=sync_brand)
        .select_related("vendor", "vendor__provider")
        .order_by("priority")
    )
    for link in additional:
        try:
            _ensure_vendor_created_on_shopmonkey(link.vendor, client)
        except shopmonkey_exceptions.ShopMonkeyException as e:
            return _fail_execution(execution, "Could not ensure vendor on ShopMonkey: {}".format(e))

    ordered_vendors = [sync_brand.vendor] + [link.vendor for link in additional]
    provider_ids = [v.provider_id for v in ordered_vendors]

    existing_sync_parts = {
        sp.master_part_id: sp
        for sp in src_models.ShopMonkeySyncPart.objects.filter(sync_brand=sync_brand).select_related("master_part")
    }
    seen_master_part_ids = set()
    counts = {"created": 0, "updated": 0, "unchanged": 0, "obsoleted": 0, "failed": 0, "unmanaged": 0}
    vendor_contrib: typing.Dict[int, typing.Dict[str, typing.Any]] = {
        v.provider_id: {
            "provider_id": v.provider_id,
            "provider_kind_name": v.provider.kind_name,
            "parts_contributed": 0,
            "quantity_contributed": 0,
        }
        for v in ordered_vendors
    }
    failures: typing.List[typing.Dict[str, str]] = []
    quantity_total = 0

    for master_part, pp_by_provider in _master_parts_for_sync_brand(sync_brand.brand_id, provider_ids):
        seen_master_part_ids.add(master_part.id)
        pp_ids = [pp.id for pp in pp_by_provider.values()]
        pricing_by_pp_id = {
            p.provider_part_id: p
            for p in src_models.ProviderPartCompanyPricing.objects.filter(
                provider_part_id__in=pp_ids, company_id=sync_brand.company_id
            )
        }
        part_quantity = 0
        for provider_id, pp in pp_by_provider.items():
            inventory = getattr(pp, "inventory", None)
            qty = inventory.warehouse_total_qty if inventory else 0
            vendor_contrib[provider_id]["parts_contributed"] += 1
            vendor_contrib[provider_id]["quantity_contributed"] += qty
            part_quantity += qty

        try:
            bucket = _sync_one_part(
                sync_brand,
                master_part,
                pp_by_provider,
                ordered_vendors,
                pricing_by_pp_id,
                client,
                existing_sync_parts.get(master_part.id),
            )
            counts[bucket] += 1
            if bucket != "obsoleted":
                quantity_total += part_quantity
        except shopmonkey_exceptions.ShopMonkeyException as e:
            counts["failed"] += 1
            sku = master_part.sku or master_part.part_number
            if len(failures) < _MAX_INLINE_FAILURES:
                failures.append({"sku": sku, "reason": str(e)})
            _record_sync_part_failure(sync_brand, master_part, existing_sync_parts.get(master_part.id), e)
            logger.exception("{} Failed syncing part (master_part_id={}): {}".format(_LOG_PREFIX, master_part.id, e))

    missing_sync_parts = [sp for mp_id, sp in existing_sync_parts.items() if mp_id not in seen_master_part_ids]
    for sp in missing_sync_parts:
        try:
            _mark_sync_part_unmanaged(sp, client)
            counts["unmanaged"] += 1
        except shopmonkey_exceptions.ShopMonkeyException as e:
            counts["failed"] += 1
            logger.exception("{} Failed marking part unmanaged (id={}): {}".format(_LOG_PREFIX, sp.id, e))

    metadata = {
        "counts": counts,
        "duration_seconds": (timezone.now() - started_at).total_seconds(),
        "vendors": list(vendor_contrib.values()),
        "failures": failures,
        "failures_total": counts["failed"],
        "quantity_total": quantity_total,
    }
    execution.metadata = metadata
    total = sum(counts.values())
    execution.status = (
        src_enums.ShopMonkeySyncExecutionStatus.FINISHED.value
        if counts["failed"] == 0
        else src_enums.ShopMonkeySyncExecutionStatus.FAILURE.value
    )
    execution.message = "Synced {}/{} master part(s) for brand {!r}.".format(
        total - counts["failed"], total, sync_brand.brand.name
    )
    logger.info(
        "{} Finished sync for sync_brand_id={} (company_id={}): {}".format(
            _LOG_PREFIX, sync_brand_id, sync_brand.company_id, metadata
        )
    )
    execution.save(update_fields=["status", "message", "metadata", "updated_at"])
    return execution


def sync_all_enabled_brands() -> None:
    """Entry point for the scheduled (every 4 hours) sync -- every enabled ShopMonkeySyncBrand
    across every company that has FEATURE_SHOPMONKEY_INVENTORY_SYNC on (Trident Motorsports only
    today). Calls sync_brand_parts() directly and synchronously, same as always -- only the
    user-triggered "run now" HTTP action goes through the QUEUED queue (see
    enqueue_sync_brand_run / process_shopmonkey_sync_jobs); keeping these two call paths distinct
    is what avoids needing a second sync function."""
    sync_brand_ids = list(
        src_models.ShopMonkeySyncBrand.objects.filter(
            enabled=True,
            company__feature_flags__feature=feature_flags_services.FEATURE_SHOPMONKEY_INVENTORY_SYNC,
            company__feature_flags__enabled=True,
        ).values_list("id", flat=True)
    )
    logger.info("{} Starting sync for {} enabled brand(s).".format(_LOG_PREFIX, len(sync_brand_ids)))
    for sync_brand_id in sync_brand_ids:
        try:
            sync_brand_parts(sync_brand_id)
        except Exception as e:  # noqa: BLE001 -- one brand's failure must not stop the rest
            logger.exception("{} Unhandled error syncing sync_brand_id={}: {}".format(_LOG_PREFIX, sync_brand_id, e))
    logger.info("{} Finished sync run.".format(_LOG_PREFIX))


# --------------------------------------------------------------------------------------
# Async "run now": QUEUED execution + queue drain (Phase 3)
# --------------------------------------------------------------------------------------
def enqueue_sync_brand_run(company_id: int, sync_brand_id: int) -> src_models.ShopMonkeySyncExecution:
    """Creates a QUEUED execution and returns immediately -- the HTTP "run now" endpoint never
    blocks on a ShopMonkey API round trip. process_shopmonkey_sync_jobs claims and runs it."""
    sync_brand = src_models.ShopMonkeySyncBrand.objects.get(id=sync_brand_id, company_id=company_id)
    return src_models.ShopMonkeySyncExecution.objects.create(
        company_id=company_id,
        sync_brand=sync_brand,
        status=src_enums.ShopMonkeySyncExecutionStatus.QUEUED.value,
        message="Queued.",
        metadata={},
    )


def claim_next_queued_execution() -> typing.Optional[src_models.ShopMonkeySyncExecution]:
    """Atomically mark one QUEUED execution as RUNNING. Returns None if none available. Mirrors
    claim_next_open_job (src.integrations.services.purchase_order_jobs) -- same
    transaction.atomic() + select_for_update(skip_locked=True) + order_by("id").first() + status
    flip shape. One difference: ShopMonkeySyncExecutionStatus is a string enum (QUEUED/RUNNING/...),
    not an int like PurchaseOrderJobStatus -- filter on the string value."""
    with transaction.atomic():
        execution = (
            src_models.ShopMonkeySyncExecution.objects.select_for_update(skip_locked=True)
            .filter(status=src_enums.ShopMonkeySyncExecutionStatus.QUEUED.value)
            .order_by("id")
            .first()
        )
        if not execution:
            return None
        execution.status = src_enums.ShopMonkeySyncExecutionStatus.RUNNING.value
        execution.message = "Claimed by process_shopmonkey_sync_jobs."
        execution.save(update_fields=["status", "message", "updated_at"])
        return execution
