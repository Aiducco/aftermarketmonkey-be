"""
API-facing layer for the ShopMonkey inventory-sync module (brand picker + sync monitor). Thin
wrapper around src.integrations.services.shopmonkey_sync -- this module owns request shaping,
company scoping, and the feature-flag gate; the integration-services module owns the actual sync
logic and ShopMonkey calls. Gated behind FEATURE_SHOPMONKEY_INVENTORY_SYNC the same way
src.api.services.shop_management gates the whole Shop Management category.
"""
import logging
import typing

from src import enums as src_enums
from src import constants as src_constants
from src import models as src_models
from src.api.services import inventory_access as inventory_access_services
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SHOPMONKEY-INVENTORY-API]"

ERROR_FEATURE_NOT_AVAILABLE = "feature_not_available"
ERROR_NOT_FOUND = "not_found"
ERROR_INVALID_INPUT = "invalid_input"
ERROR_CONFLICT = "conflict"

# Paging ceilings -- same shape as list_part_detail_audit_history (src/api/services/parts.py).
_PARTS_MAX_LIMIT = 500
_PARTS_DEFAULT_LIMIT = 100
_EXECUTIONS_MAX_LIMIT = 200
_EXECUTIONS_DEFAULT_LIMIT = 50


def _feature_enabled(company_id: int) -> bool:
    return inventory_access_services.company_has_inventory_access(company_id)


# Shop-management systems the inventory sync can actually push to. The catalog may list more (they can be connected for
# other features); only these get a working page.
_INVENTORY_SUPPORTED_KINDS = {src_enums.ShopManagementProviderKind.SHOPMONKEY.value}

STATE_NOT_CONNECTED = "NOT_CONNECTED"
STATE_CONNECTION_FAILING = "CONNECTION_FAILING"
STATE_NO_DISTRIBUTORS = "NO_DISTRIBUTORS"
STATE_NOT_SUPPORTED = "NOT_SUPPORTED"
STATE_READY = "READY"


def get_inventory_state(company_id: int) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    """GET .../inventory/state/ -- what the Inventory Management page should show before it loads anything.
    One row per shop-management system in the catalog, each with the state the page needs: not connected (send the
    user to connect it), connection failing (reconnect), no distributors connected (nothing to sync from), not
    supported (listed, but no inventory sync for it yet) or ready. The page's selector is built from ``systems``;
    ``default_kind`` is the system to preselect (the first ready one, else the first connected one)."""
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    connections = {
        c.provider_id: c
        for c in src_models.CompanyShopManagementProviders.objects.filter(company_id=company_id, active=True)
    }
    providers_by_kind = {p.kind: p for p in src_models.ShopManagementProviders.objects.all()}
    has_distributors = bool(shopmonkey_sync_services.company_connected_provider_ids(company_id))

    systems = []
    for entry in src_constants.SHOP_MANAGEMENT_PROVIDER_CATALOG:
        kind = entry["kind"].value
        provider = providers_by_kind.get(kind)
        if not provider:
            continue
        connection = connections.get(provider.id)
        if kind not in _INVENTORY_SUPPORTED_KINDS:
            state = STATE_NOT_SUPPORTED
        elif not connection:
            state = STATE_NOT_CONNECTED
        elif connection.status == src_enums.ShopManagementConnectionStatus.FAILING.value:
            state = STATE_CONNECTION_FAILING
        elif not has_distributors:
            state = STATE_NO_DISTRIBUTORS
        else:
            state = STATE_READY
        systems.append(
            {
                "kind": kind,
                "kind_name": provider.kind_name or "",
                "name": provider.name,
                "icon_url": entry.get("icon_url") or None,
                "coming_soon": bool(provider.coming_soon),
                "inventory_supported": kind in _INVENTORY_SUPPORTED_KINDS,
                "connected": connection is not None,
                "state": state,
                "status_reason": connection.status_reason if connection else None,
            }
        )

    default = next((s for s in systems if s["state"] == STATE_READY), None) or next(
        (s for s in systems if s["connected"] and s["inventory_supported"]), None
    )
    if default is None and systems:
        default = next((s for s in systems if s["inventory_supported"]), systems[0])
    return {"systems": systems, "default_kind": default["kind"] if default else None}, None


def get_available_brands(
    company_id: int,
) -> typing.Tuple[typing.Optional[typing.List[typing.Dict]], typing.Optional[str]]:
    """Returns (data, error_code)."""
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    return shopmonkey_sync_services.available_brands_for_company(company_id), None


def _sync_brand_row(
    sync_brand: src_models.ShopMonkeySyncBrand, deleting: typing.Optional[bool] = None
) -> typing.Dict[str, typing.Any]:
    """Per contract.md Decision 1: ``providers`` is a list of {provider_id, provider_kind_name}
    objects, never bare ids -- the single-brand GET's whole purpose is to let the FE label its
    vendor chips without a second fetch of available-brands. Primary vendor is always first;
    ``primary_provider_id`` is carried explicitly too so nothing depends on list ordering."""
    providers = []
    primary_provider_id = None
    if sync_brand.vendor_id:
        primary_provider_id = sync_brand.vendor.provider_id
        providers.append(
            {
                "provider_id": sync_brand.vendor.provider_id,
                "provider_kind_name": sync_brand.vendor.provider.kind_name if sync_brand.vendor.provider_id else None,
            }
        )
    for link in sync_brand.additional_vendors.all():
        providers.append(
            {
                "provider_id": link.vendor.provider_id,
                "provider_kind_name": link.vendor.provider.kind_name if link.vendor.provider_id else None,
            }
        )
    return {
        "id": sync_brand.id,
        "company_id": sync_brand.company_id,
        "brand_id": sync_brand.brand_id,
        "brand_name": sync_brand.brand.name if sync_brand.brand_id else None,
        "providers": providers,
        "primary_provider_id": primary_provider_id,
        "enabled": sync_brand.enabled,
        # True while a delete of this brand is queued or running: the page locks the row, and the API refuses
        # to re-enable or sync it.
        "deleting": (
            shopmonkey_sync_services.active_delete_executions([sync_brand.id]).exists()
            if deleting is None
            else deleting
        ),
        "shopmonkey_external_id": sync_brand.shopmonkey_external_id,
        "entity": sync_brand.entity,
        "status": sync_brand.status,
        "status_name": sync_brand.status_name,
        # Parts actually pushed -- a SKIPPED part is one we deliberately did not push.
        "part_count": src_models.ShopMonkeySyncPart.objects.filter(sync_brand=sync_brand)
        .exclude(status=src_enums.ShopMonkeySyncPartStatus.SKIPPED.value)
        .count(),
        "created_at": sync_brand.created_at.isoformat() if sync_brand.created_at else None,
        "updated_at": sync_brand.updated_at.isoformat() if sync_brand.updated_at else None,
    }


def _sync_brand_queryset():
    return src_models.ShopMonkeySyncBrand.objects.select_related(
        "brand", "vendor", "vendor__provider"
    ).prefetch_related("additional_vendors__vendor__provider")


def list_sync_brands(company_id: int) -> typing.Tuple[typing.Optional[typing.List[typing.Dict]], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    rows = list(_sync_brand_queryset().filter(company_id=company_id))
    deleting_ids = set(
        shopmonkey_sync_services.active_delete_executions([r.id for r in rows]).values_list("sync_brand_id", flat=True)
    )
    return [_sync_brand_row(r, deleting=r.id in deleting_ids) for r in rows], None


def get_sync_brand(
    company_id: int, sync_brand_id: int
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    """GET .../sync-brands/<id>/ -- the new single-brand lookup (Phase 3), so the FE detail page
    stops fetching every brand just to find one. A brand belonging to another company is 404,
    never 403 -- same not-found-vs-forbidden handling as every other per-id lookup here."""
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = _sync_brand_queryset().filter(id=sync_brand_id, company_id=company_id).first()
    if not sync_brand:
        return None, ERROR_NOT_FOUND
    return _sync_brand_row(sync_brand), None


def _validate_provider_ids(
    company_id: int, provider_ids: typing.List[int], primary_provider_id: int
) -> typing.Optional[str]:
    if not provider_ids:
        return ERROR_INVALID_INPUT
    if primary_provider_id not in provider_ids:
        return ERROR_INVALID_INPUT
    connected = shopmonkey_sync_services.company_connected_provider_ids(company_id)
    if any(pid not in connected for pid in provider_ids):
        return ERROR_INVALID_INPUT
    return None


def create_sync_brand(
    company_id: int, brand_id: int, provider_ids: typing.List[int], primary_provider_id: int
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    if not src_models.Brands.objects.filter(id=brand_id).exists():
        return None, ERROR_NOT_FOUND
    error_code = _validate_provider_ids(company_id, provider_ids, primary_provider_id)
    if error_code:
        return None, error_code
    sync_brand = shopmonkey_sync_services.configure_sync_brand(
        company_id=company_id,
        brand_id=brand_id,
        provider_ids=provider_ids,
        primary_provider_id=primary_provider_id,
        enabled=True,
    )
    return get_sync_brand(company_id, sync_brand.id)


def update_sync_brand(
    company_id: int,
    sync_brand_id: int,
    enabled: typing.Optional[bool],
    provider_ids: typing.Optional[typing.List[int]],
    primary_provider_id: typing.Optional[int],
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    """PATCH resolves any omitted field from the current row, then calls the same
    configure_sync_brand() that POST calls -- there is no separate toggle-only write path. A body
    of {"enabled": false} alone must leave the existing provider set untouched (contract.md)."""
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = _sync_brand_queryset().filter(id=sync_brand_id, company_id=company_id).first()
    if not sync_brand:
        return None, ERROR_NOT_FOUND
    if shopmonkey_sync_services.active_delete_executions([sync_brand.id]).exists():
        return None, ERROR_CONFLICT

    if enabled is None:
        enabled = sync_brand.enabled
    if provider_ids is None:
        provider_ids = [sync_brand.vendor.provider_id] + [
            link.vendor.provider_id for link in sync_brand.additional_vendors.all()
        ]
    if primary_provider_id is None:
        primary_provider_id = sync_brand.vendor.provider_id

    error_code = _validate_provider_ids(company_id, provider_ids, primary_provider_id)
    if error_code:
        return None, error_code

    shopmonkey_sync_services.configure_sync_brand(
        company_id=company_id,
        brand_id=sync_brand.brand_id,
        provider_ids=provider_ids,
        primary_provider_id=primary_provider_id,
        enabled=enabled,
    )
    return get_sync_brand(company_id, sync_brand_id)


def run_sync_brand_now(
    company_id: int, sync_brand_id: int
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    """Enqueues a QUEUED execution and returns immediately (Phase 3) -- process_shopmonkey_sync_jobs
    is what actually calls sync_brand_parts(). Never run the sync inline on the request thread."""
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = src_models.ShopMonkeySyncBrand.objects.filter(id=sync_brand_id, company_id=company_id).first()
    if not sync_brand:
        return None, ERROR_NOT_FOUND
    if not sync_brand.enabled:
        return None, ERROR_INVALID_INPUT
    execution = shopmonkey_sync_services.enqueue_sync_brand_run(company_id, sync_brand_id)
    return _execution_row(execution), None


def delete_sync_brand(
    company_id: int, sync_brand_id: int, confirm_brand_name: typing.Optional[str]
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    """Queues the deletion of a configured brand: its parts are soft-deleted on ShopMonkey (by process_shopmonkey_sync_jobs,
    in slices) and then the brand's sync config is removed. Destructive and not undoable on ShopMonkey's side, so the
    caller must send the brand's own name back as confirmation -- a stray DELETE with no body does nothing. Returns the
    QUEUED execution. A brand with a sync or delete already queued/running is a conflict (409)."""
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = (
        src_models.ShopMonkeySyncBrand.objects.select_related("brand")
        .filter(id=sync_brand_id, company_id=company_id)
        .first()
    )
    if not sync_brand:
        return None, ERROR_NOT_FOUND
    if (confirm_brand_name or "").strip().casefold() != sync_brand.brand.name.strip().casefold():
        return None, ERROR_INVALID_INPUT
    execution = shopmonkey_sync_services.enqueue_brand_delete(company_id, sync_brand_id)
    if execution is None:
        return None, ERROR_CONFLICT
    return _execution_row(execution), None


def _execution_row(execution: src_models.ShopMonkeySyncExecution) -> typing.Dict[str, typing.Any]:
    return {
        "id": execution.id,
        "company_id": execution.company_id,
        "sync_brand_id": execution.sync_brand_id,
        "status": execution.status,
        "message": execution.message,
        "metadata": execution.metadata,
        "created_at": execution.created_at.isoformat() if execution.created_at else None,
        "updated_at": execution.updated_at.isoformat() if execution.updated_at else None,
    }


def list_executions(
    company_id: int,
    sync_brand_id: typing.Optional[int] = None,
    status: typing.Optional[str] = None,
    limit: int = _EXECUTIONS_DEFAULT_LIMIT,
    offset: int = 0,
) -> typing.Tuple[typing.Optional[typing.Dict[str, typing.Any]], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    qs = src_models.ShopMonkeySyncExecution.objects.filter(company_id=company_id)
    if sync_brand_id:
        qs = qs.filter(sync_brand_id=sync_brand_id)
    if status:
        qs = qs.filter(status=status)
    qs = qs.order_by("-created_at")
    total = qs.count()
    limit = max(1, min(limit, _EXECUTIONS_MAX_LIMIT))
    offset = max(0, offset)
    page = qs[offset : offset + limit]
    return {
        "data": [_execution_row(e) for e in page],
        "total": total,
        "limit": limit,
        "offset": offset,
    }, None


def list_sync_brand_parts(
    company_id: int,
    sync_brand_id: int,
    limit: int = _PARTS_DEFAULT_LIMIT,
    offset: int = 0,
    status: typing.Optional[int] = None,
) -> typing.Tuple[typing.Optional[typing.Dict[str, typing.Any]], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = src_models.ShopMonkeySyncBrand.objects.filter(id=sync_brand_id, company_id=company_id).first()
    if not sync_brand:
        return None, ERROR_NOT_FOUND

    qs = src_models.ShopMonkeySyncPart.objects.filter(sync_brand=sync_brand).select_related(
        "master_part", "sourced_from_vendor__provider"
    )
    if status is not None:
        qs = qs.filter(status=status)
    qs = qs.order_by("-updated_at")
    total = qs.count()
    limit = max(1, min(limit, _PARTS_MAX_LIMIT))
    offset = max(0, offset)
    page = qs[offset : offset + limit]

    data = []
    for p in page:
        master_part = p.master_part if p.master_part_id else None
        vendor = p.sourced_from_vendor
        data.append(
            {
                "id": p.id,
                "master_part_id": p.master_part_id,
                # MasterPart.sku is nullable and is null for a meaningful share of real
                # distributor data, but the contract promises a non-null string here and the
                # frontend's type is non-optional. Resolve it the same way the push path does:
                # prefer what we actually sent upstream (last_pushed_sku), then the master part's
                # own sku, then its part number -- never null for a part that has been synced.
                "sku": (
                    p.last_pushed_sku
                    or (master_part.sku if master_part else None)
                    or (master_part.part_number if master_part else "")
                ),
                "part_number": (master_part.part_number if master_part else ""),
                "shopmonkey_external_id": p.shopmonkey_external_id,
                # From sourced_from_vendor -- which vendor's price/binLocation this part's last push
                # actually used, not a live recomputation.
                "provider_kind_name": vendor.provider.kind_name if vendor and vendor.provider_id else None,
                "quantity": p.last_pushed_quantity,
                "bin_location": p.last_pushed_bin_location,
                "last_pushed_name": p.last_pushed_name,
                # Integer cents per contract -- the column is a DecimalField (shared with the
                # pricing tables' own precision), so cast explicitly rather than let the FE receive
                # a decimal-with-trailing-zeros value.
                "last_pushed_retail_cost_cents": (
                    int(p.last_pushed_retail_cost_cents) if p.last_pushed_retail_cost_cents is not None else None
                ),
                "last_pushed_wholesale_cost_cents": (
                    int(p.last_pushed_wholesale_cost_cents) if p.last_pushed_wholesale_cost_cents is not None else None
                ),
                "status": p.status,
                "status_name": p.status_name,
                "last_error": p.last_error,
                "updated_at": p.updated_at.isoformat() if p.updated_at else None,
            }
        )
    return {
        "data": data,
        "total": total,
        "limit": limit,
        "offset": offset,
    }, None
