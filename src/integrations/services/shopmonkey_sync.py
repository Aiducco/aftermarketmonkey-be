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

Status model, mapped from signals we already have instead of the old tool's provider-specific
"discontinued SKUs" feed:
  - a part whose ProviderPart.is_discontinued is True -> INACTIVE / "[OBSOLETE] " (a clean,
    known signal, uniform across every provider -- better than the old tool had).
  - a part previously synced whose ProviderPart no longer matches this (company, provider,
    brand) at all (reassigned provider, genuinely vanished) -> ERROR / "[ERROR UPDATING PART] ",
    matching the old tool's own use of that status for exactly this "disappeared unexpectedly"
    case (see its _mark_missing_parts_as_error).
"""
import logging
import typing

from django.core.cache import cache
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

    rows = (
        src_models.BrandProviders.objects.filter(provider_id__in=connected_provider_ids)
        .select_related("brand", "provider")
    )

    by_brand: typing.Dict[int, typing.Dict[str, typing.Any]] = {}
    for row in rows:
        if not row.brand_id:
            continue
        entry = by_brand.setdefault(row.brand_id, {"brand_name": row.brand.name, "providers": []})
        entry["providers"].append({
            "provider_id": row.provider_id,
            "provider_kind_name": row.provider.kind_name if row.provider_id else None,
            # None until refresh_brand_provider_part_counts has run at least once for this pair --
            # treated as 0 for sort purposes only, not shown as a confirmed zero to the FE.
            "part_count": row.part_count,
        })

    result = []
    for brand_id, entry in by_brand.items():
        entry["providers"].sort(key=lambda p: -(p["part_count"] or 0))
        result.append({"brand_id": brand_id, "brand_name": entry["brand_name"], "providers": entry["providers"]})
    return sorted(result, key=lambda r: r["brand_name"] or "")


def refresh_available_brands_cache(company_id: int) -> int:
    """Forces recomputation of the slow base list and overwrites the cache. Returns the brand count. Called by warm_shopmonkey_available_brands_cache; also safe to call ad hoc after a distributor catalog re-sync."""
    base = _compute_available_brands_base(company_id)
    cache.set(_AVAILABLE_BRANDS_CACHE_KEY_FMT.format(company_id=company_id), base, _AVAILABLE_BRANDS_CACHE_TTL_SECONDS)
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
    current_provider_id) is NOT part of that cache -- it's a small, fast, per-company query,
    always run fresh here so toggling a brand on/off is reflected immediately rather than waiting
    on the cache TTL.
    """
    cache_key = _AVAILABLE_BRANDS_CACHE_KEY_FMT.format(company_id=company_id)
    base = cache.get(cache_key)
    if base is None:
        # Cache miss (not yet warmed, or TTL lapsed with warming behind) -- falls back to the slow
        # live path rather than returning nothing, and populates the cache for next time.
        base = _compute_available_brands_base(company_id)
        cache.set(cache_key, base, _AVAILABLE_BRANDS_CACHE_TTL_SECONDS)
    if not base:
        return []

    brand_ids = [row["brand_id"] for row in base]
    existing_sync_brands = {
        sb.brand_id: sb
        for sb in src_models.ShopMonkeySyncBrand.objects.filter(
            company_id=company_id, brand_id__in=brand_ids
        ).select_related("vendor")
    }

    result = []
    for row in base:
        sync_brand = existing_sync_brands.get(row["brand_id"])
        result.append({
            **row,
            "sync_brand_id": sync_brand.id if sync_brand else None,
            "enabled": sync_brand.enabled if sync_brand else False,
            "current_provider_id": sync_brand.vendor.provider_id if sync_brand else None,
        })
    return result


# --------------------------------------------------------------------------------------
# Configuration: enabling/disabling a brand for sync
# --------------------------------------------------------------------------------------
def _get_or_create_vendor(company_id: int, provider_id: int) -> src_models.ShopMonkeySyncVendor:
    vendor, _created = src_models.ShopMonkeySyncVendor.objects.get_or_create(
        company_id=company_id,
        provider_id=provider_id,
        defaults={
            "status": src_enums.ShopMonkeySyncStatus.INACTIVE.value,
            "status_name": src_enums.ShopMonkeySyncStatus.INACTIVE.name,
        },
    )
    return vendor


def add_sync_brand(company_id: int, brand_id: int, provider_id: int) -> src_models.ShopMonkeySyncBrand:
    """
    Enable a brand for sync, sourced from the given provider. Idempotent: calling again just
    re-enables it / can change which provider feeds it -- the brand's own ShopMonkey identity
    (shopmonkey_external_id) is untouched either way, since switching providers only changes
    which of our ProviderPart rows feed its parts going forward, not the brand on ShopMonkey
    itself.
    """
    vendor = _get_or_create_vendor(company_id, provider_id)
    sync_brand, created = src_models.ShopMonkeySyncBrand.objects.get_or_create(
        company_id=company_id,
        brand_id=brand_id,
        defaults={
            "vendor": vendor,
            "enabled": True,
            "status": src_enums.ShopMonkeySyncStatus.INACTIVE.value,
            "status_name": src_enums.ShopMonkeySyncStatus.INACTIVE.name,
        },
    )
    if not created:
        sync_brand.vendor = vendor
        sync_brand.enabled = True
        sync_brand.save(update_fields=["vendor", "enabled", "updated_at"])
    return sync_brand


def set_sync_brand_enabled(sync_brand_id: int, company_id: int, enabled: bool) -> None:
    src_models.ShopMonkeySyncBrand.objects.filter(id=sync_brand_id, company_id=company_id).update(
        enabled=enabled, updated_at=timezone.now()
    )


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
    for prefix in (_OBSOLETE_PREFIX, _ERROR_PREFIX):
        if name.startswith(prefix):
            return name[len(prefix):]
    return name


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


def _provider_parts_for_sync_brand(sync_brand: src_models.ShopMonkeySyncBrand):
    return (
        src_models.ProviderPart.objects.filter(
            provider_id=sync_brand.vendor.provider_id, master_part__brand_id=sync_brand.brand_id
        )
        .select_related("master_part")
    )


def _sync_one_part(
    sync_brand: src_models.ShopMonkeySyncBrand,
    provider_part: src_models.ProviderPart,
    pricing: typing.Optional[src_models.ProviderPartCompanyPricing],
    client: shopmonkey_client.ShopMonkeyApiClient,
    existing_sync_part: typing.Optional[src_models.ShopMonkeySyncPart],
) -> None:
    master_part = provider_part.master_part
    sku = master_part.sku or master_part.part_number
    number = master_part.part_number

    retail_cost = None
    wholesale_cost = None
    if pricing:
        retail_cost = pricing.retail_price if pricing.retail_price is not None else pricing.msrp
        wholesale_cost = pricing.cost
    retail_cost_cents = _cents(retail_cost)
    wholesale_cost_cents = _cents(wholesale_cost)

    status = (
        src_enums.ShopMonkeySyncPartStatus.INACTIVE.value
        if provider_part.is_discontinued
        else src_enums.ShopMonkeySyncPartStatus.ACTIVE.value
    )
    name = _part_name_for_status(master_part.description or sku, status)

    if not existing_sync_part:
        shopmonkey_part = client.create_inventory_part(
            name=name,
            vendor_id=sync_brand.vendor.shopmonkey_external_id,
            brand_id=sync_brand.shopmonkey_external_id,
            number=number,
            sku=sku,
            retail_cost=retail_cost_cents,
            wholesale_cost=wholesale_cost_cents,
        )
        src_models.ShopMonkeySyncPart.objects.create(
            sync_brand=sync_brand,
            provider_part=provider_part,
            shopmonkey_external_id=shopmonkey_part["id"],
            last_pushed_name=name,
            last_pushed_retail_cost_cents=retail_cost_cents,
            last_pushed_wholesale_cost_cents=wholesale_cost_cents,
            status=status,
            status_name=src_enums.ShopMonkeySyncPartStatus(status).name,
        )
        return

    changed = (
        existing_sync_part.last_pushed_name != name
        or existing_sync_part.last_pushed_retail_cost_cents != retail_cost_cents
        or existing_sync_part.last_pushed_wholesale_cost_cents != wholesale_cost_cents
        or existing_sync_part.status != status
    )
    if not changed:
        return

    shopmonkey_part = client.update_inventory_part(
        external_id=existing_sync_part.shopmonkey_external_id,
        name=name,
        vendor_id=sync_brand.vendor.shopmonkey_external_id,
        brand_id=sync_brand.shopmonkey_external_id,
        number=number,
        sku=sku,
        retail_cost=retail_cost_cents,
        wholesale_cost=wholesale_cost_cents,
    )
    existing_sync_part.shopmonkey_external_id = shopmonkey_part.get("id", existing_sync_part.shopmonkey_external_id)
    existing_sync_part.last_pushed_name = name
    existing_sync_part.last_pushed_retail_cost_cents = retail_cost_cents
    existing_sync_part.last_pushed_wholesale_cost_cents = wholesale_cost_cents
    existing_sync_part.status = status
    existing_sync_part.status_name = src_enums.ShopMonkeySyncPartStatus(status).name
    existing_sync_part.save(update_fields=[
        "shopmonkey_external_id", "last_pushed_name", "last_pushed_retail_cost_cents",
        "last_pushed_wholesale_cost_cents", "status", "status_name", "updated_at",
    ])


def _mark_sync_part_error(sync_part: src_models.ShopMonkeySyncPart, client: shopmonkey_client.ShopMonkeyApiClient) -> None:
    """A previously-synced part whose ProviderPart no longer matches this (company, provider,
    brand) at all -- mirrors the old tool's _mark_missing_parts_as_error / _mark_part_as_error."""
    if sync_part.status == src_enums.ShopMonkeySyncPartStatus.ERROR.value:
        return
    name = _part_name_for_status(sync_part.last_pushed_name or "", src_enums.ShopMonkeySyncPartStatus.ERROR.value)
    master_part = sync_part.provider_part.master_part
    client.update_inventory_part(
        external_id=sync_part.shopmonkey_external_id,
        name=name,
        vendor_id=sync_part.sync_brand.vendor.shopmonkey_external_id,
        brand_id=sync_part.sync_brand.shopmonkey_external_id,
        number=master_part.part_number,
        sku=master_part.sku or master_part.part_number,
        retail_cost=int(sync_part.last_pushed_retail_cost_cents or 0),
        wholesale_cost=int(sync_part.last_pushed_wholesale_cost_cents or 0),
    )
    sync_part.last_pushed_name = name
    sync_part.status = src_enums.ShopMonkeySyncPartStatus.ERROR.value
    sync_part.status_name = src_enums.ShopMonkeySyncPartStatus.ERROR.name
    sync_part.save(update_fields=["last_pushed_name", "status", "status_name", "updated_at"])


# --------------------------------------------------------------------------------------
# One brand's full sync run
# --------------------------------------------------------------------------------------
def _fail_execution(execution: src_models.ShopMonkeySyncExecution, message: str) -> src_models.ShopMonkeySyncExecution:
    logger.error("{} {}".format(_LOG_PREFIX, message))
    execution.status = src_enums.ShopMonkeySyncExecutionStatus.FAILURE.value
    execution.message = message
    execution.save(update_fields=["status", "message", "updated_at"])
    return execution


def sync_brand_parts(sync_brand_id: int) -> src_models.ShopMonkeySyncExecution:
    """Full sync for one ShopMonkeySyncBrand: ensure vendor/brand exist on ShopMonkey, then
    create/update every part from this (company, provider, brand)'s own ProviderPart data,
    diffed against what was last pushed, and mark anything that disappeared as an error --
    mirrors the old tool's fetch_and_sync_inventory_brand_parts end to end."""
    sync_brand = src_models.ShopMonkeySyncBrand.objects.select_related("vendor", "brand", "company").get(
        id=sync_brand_id
    )
    execution = src_models.ShopMonkeySyncExecution.objects.create(
        company_id=sync_brand.company_id,
        sync_brand=sync_brand,
        status=src_enums.ShopMonkeySyncExecutionStatus.RUNNING.value,
        message="Starting sync for brand {!r}.".format(sync_brand.brand.name),
        metadata={},
    )

    try:
        client = _client_for_company(sync_brand.company_id)
    except ValueError as e:
        return _fail_execution(execution, str(e))

    try:
        _ensure_vendor_created_on_shopmonkey(sync_brand.vendor, client)
        _ensure_brand_created_on_shopmonkey(sync_brand, client)
    except shopmonkey_exceptions.ShopMonkeyException as e:
        return _fail_execution(execution, "Could not ensure vendor/brand on ShopMonkey: {}".format(e))

    provider_parts = list(_provider_parts_for_sync_brand(sync_brand))
    provider_part_ids = [pp.id for pp in provider_parts]
    total = len(provider_parts)

    pricing_by_pp_id = {
        pp.provider_part_id: pp
        for pp in src_models.ProviderPartCompanyPricing.objects.filter(
            provider_part_id__in=provider_part_ids, company_id=sync_brand.company_id
        )
    }
    existing_sync_parts = {
        sp.provider_part_id: sp
        for sp in src_models.ShopMonkeySyncPart.objects.filter(sync_brand=sync_brand).select_related(
            "provider_part__master_part"
        )
    }

    success = 0
    failures: typing.List[str] = []
    seen_provider_part_ids = set()
    for pp in provider_parts:
        seen_provider_part_ids.add(pp.id)
        try:
            _sync_one_part(
                sync_brand, pp, pricing_by_pp_id.get(pp.id), client, existing_sync_parts.get(pp.id)
            )
            success += 1
        except shopmonkey_exceptions.ShopMonkeyException as e:
            failures.append(pp.master_part.sku or pp.master_part.part_number)
            logger.exception(
                "{} Failed syncing part (provider_part_id={}): {}".format(_LOG_PREFIX, pp.id, e)
            )

    missing_sync_parts = [
        sp for pp_id, sp in existing_sync_parts.items() if pp_id not in seen_provider_part_ids
    ]
    for sp in missing_sync_parts:
        try:
            _mark_sync_part_error(sp, client)
        except shopmonkey_exceptions.ShopMonkeyException as e:
            logger.exception(
                "{} Failed marking part as error (id={}): {}".format(_LOG_PREFIX, sp.id, e)
            )

    metadata = {"total_products": total, "success": success, "failure": failures}
    execution.metadata = metadata
    execution.status = (
        src_enums.ShopMonkeySyncExecutionStatus.FINISHED.value
        if not failures
        else src_enums.ShopMonkeySyncExecutionStatus.FAILURE.value
    )
    execution.message = "Synced {}/{} parts for brand {!r}.".format(success, total, sync_brand.brand.name)
    execution.save(update_fields=["status", "message", "metadata", "updated_at"])
    return execution


def sync_all_enabled_brands() -> None:
    """Entry point for the scheduled (every 4 hours) sync -- every enabled ShopMonkeySyncBrand
    across every company that has FEATURE_SHOPMONKEY_INVENTORY_SYNC on (Trident Motorsports only
    today)."""
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
            logger.exception(
                "{} Unhandled error syncing sync_brand_id={}: {}".format(_LOG_PREFIX, sync_brand_id, e)
            )
    logger.info("{} Finished sync run.".format(_LOG_PREFIX))
