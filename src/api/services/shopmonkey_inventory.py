"""
API-facing layer for the ShopMonkey inventory-sync module (brand picker + sync monitor). Thin
wrapper around src.integrations.services.shopmonkey_sync -- this module owns request shaping,
company scoping, and the feature-flag gate; the integration-services module owns the actual sync
logic and ShopMonkey calls. Gated behind FEATURE_SHOPMONKEY_INVENTORY_SYNC the same way
src.api.services.shop_management gates the whole Shop Management category.
"""
import logging
import typing

from src import models as src_models
from src.api.services import feature_flags as feature_flags_services
from src.integrations.services import shopmonkey_sync as shopmonkey_sync_services

logger = logging.getLogger(__name__)
_LOG_PREFIX = "[SHOPMONKEY-INVENTORY-API]"

ERROR_FEATURE_NOT_AVAILABLE = "feature_not_available"
ERROR_NOT_FOUND = "not_found"
ERROR_INVALID_INPUT = "invalid_input"


def _feature_enabled(company_id: int) -> bool:
    return feature_flags_services.company_has_feature(
        company_id, feature_flags_services.FEATURE_SHOPMONKEY_INVENTORY_SYNC
    )


def get_available_brands(company_id: int) -> typing.Tuple[typing.Optional[typing.List[typing.Dict]], typing.Optional[str]]:
    """Returns (data, error_code)."""
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    return shopmonkey_sync_services.available_brands_for_company(company_id), None


def _sync_brand_row(sync_brand: src_models.ShopMonkeySyncBrand) -> typing.Dict[str, typing.Any]:
    return {
        "id": sync_brand.id,
        "company_id": sync_brand.company_id,
        "brand_id": sync_brand.brand_id,
        "brand_name": sync_brand.brand.name if sync_brand.brand_id else None,
        "provider_id": sync_brand.vendor.provider_id if sync_brand.vendor_id else None,
        "provider_kind_name": (
            sync_brand.vendor.provider.kind_name if sync_brand.vendor_id and sync_brand.vendor.provider_id else None
        ),
        "enabled": sync_brand.enabled,
        "shopmonkey_external_id": sync_brand.shopmonkey_external_id,
        "status": sync_brand.status,
        "status_name": sync_brand.status_name,
        "part_count": src_models.ShopMonkeySyncPart.objects.filter(sync_brand=sync_brand).count(),
        "created_at": sync_brand.created_at.isoformat() if sync_brand.created_at else None,
        "updated_at": sync_brand.updated_at.isoformat() if sync_brand.updated_at else None,
    }


def list_sync_brands(company_id: int) -> typing.Tuple[typing.Optional[typing.List[typing.Dict]], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    rows = src_models.ShopMonkeySyncBrand.objects.filter(company_id=company_id).select_related(
        "brand", "vendor", "vendor__provider"
    )
    return [_sync_brand_row(r) for r in rows], None


def create_sync_brand(
    company_id: int, brand_id: int, provider_id: int
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    if not src_models.Brands.objects.filter(id=brand_id).exists():
        return None, ERROR_NOT_FOUND
    if provider_id not in shopmonkey_sync_services.company_connected_provider_ids(company_id):
        return None, ERROR_INVALID_INPUT
    sync_brand = shopmonkey_sync_services.add_sync_brand(company_id, brand_id, provider_id)
    sync_brand.refresh_from_db()
    return _sync_brand_row(
        src_models.ShopMonkeySyncBrand.objects.select_related("brand", "vendor", "vendor__provider").get(
            id=sync_brand.id
        )
    ), None


def update_sync_brand(
    company_id: int, sync_brand_id: int, enabled: typing.Optional[bool]
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = src_models.ShopMonkeySyncBrand.objects.filter(id=sync_brand_id, company_id=company_id).first()
    if not sync_brand:
        return None, ERROR_NOT_FOUND
    if enabled is not None:
        shopmonkey_sync_services.set_sync_brand_enabled(sync_brand_id, company_id, enabled)
    sync_brand = src_models.ShopMonkeySyncBrand.objects.select_related(
        "brand", "vendor", "vendor__provider"
    ).get(id=sync_brand_id)
    return _sync_brand_row(sync_brand), None


def run_sync_brand_now(
    company_id: int, sync_brand_id: int
) -> typing.Tuple[typing.Optional[typing.Dict], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = src_models.ShopMonkeySyncBrand.objects.filter(id=sync_brand_id, company_id=company_id).first()
    if not sync_brand:
        return None, ERROR_NOT_FOUND
    execution = shopmonkey_sync_services.sync_brand_parts(sync_brand_id)
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
    limit: int = 50,
) -> typing.Tuple[typing.Optional[typing.List[typing.Dict]], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    qs = src_models.ShopMonkeySyncExecution.objects.filter(company_id=company_id)
    if sync_brand_id:
        qs = qs.filter(sync_brand_id=sync_brand_id)
    if status:
        qs = qs.filter(status=status)
    qs = qs.order_by("-created_at")[: max(1, min(limit, 200))]
    return [_execution_row(e) for e in qs], None


def list_sync_brand_parts(
    company_id: int, sync_brand_id: int, limit: int = 100
) -> typing.Tuple[typing.Optional[typing.List[typing.Dict]], typing.Optional[str]]:
    if not _feature_enabled(company_id):
        return None, ERROR_FEATURE_NOT_AVAILABLE
    sync_brand = src_models.ShopMonkeySyncBrand.objects.filter(id=sync_brand_id, company_id=company_id).first()
    if not sync_brand:
        return None, ERROR_NOT_FOUND
    parts = (
        src_models.ShopMonkeySyncPart.objects.filter(sync_brand=sync_brand)
        .select_related("provider_part__master_part")
        .order_by("-updated_at")[: max(1, min(limit, 500))]
    )
    data = []
    for p in parts:
        master_part = p.provider_part.master_part if p.provider_part_id else None
        data.append({
            "id": p.id,
            "sku": master_part.sku if master_part else None,
            "part_number": master_part.part_number if master_part else None,
            "shopmonkey_external_id": p.shopmonkey_external_id,
            "last_pushed_name": p.last_pushed_name,
            "last_pushed_retail_cost_cents": p.last_pushed_retail_cost_cents,
            "last_pushed_wholesale_cost_cents": p.last_pushed_wholesale_cost_cents,
            "status": p.status,
            "status_name": p.status_name,
            "updated_at": p.updated_at.isoformat() if p.updated_at else None,
        })
    return data, None
