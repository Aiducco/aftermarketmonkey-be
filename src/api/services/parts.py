"""
API services for parts search and detail.
"""
import logging
import typing

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[PARTS-SERVICES]"


def get_parts_search(sku: str, limit: int = 50) -> typing.List[typing.Dict]:
    """
    Search MasterPart by part_number (case-insensitive contains).
    Returns MasterPart fields + brand_id.
    """
    if not sku or not str(sku).strip():
        return []

    q = str(sku).strip()
    parts = (
        src_models.MasterPart.objects.filter(part_number__icontains=q)
        .select_related("brand")
        .order_by("brand__name", "part_number")[:limit]
    )

    return [
        {
            "id": p.id,
            "brand_id": p.brand_id,
            "part_number": p.part_number,
            "sku": p.sku,
            "description": p.description,
            "aaia_code": p.aaia_code,
            "image_url": p.image_url,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "updated_at": p.updated_at.isoformat() if p.updated_at else None,
        }
        for p in parts
    ]


def get_part_detail(master_part_id: int, company_id: typing.Optional[int] = None) -> typing.Optional[typing.Dict]:
    """
    Get detailed info for one MasterPart.
    Returns MasterPart + per provider: inventory, pricing (per company if company_id given).
    """
    try:
        part = src_models.MasterPart.objects.select_related("brand").get(id=master_part_id)
    except src_models.MasterPart.DoesNotExist:
        return None

    base = {
        "id": part.id,
        "brand_id": part.brand_id,
        "brand_name": part.brand.name if part.brand else None,
        "part_number": part.part_number,
        "sku": part.sku,
        "description": part.description,
        "aaia_code": part.aaia_code,
        "image_url": part.image_url,
        "created_at": part.created_at.isoformat() if part.created_at else None,
        "updated_at": part.updated_at.isoformat() if part.updated_at else None,
    }

    provider_parts = (
        src_models.ProviderPart.objects.filter(master_part=part)
        .select_related("provider")
        .prefetch_related("inventory", "company_pricing")
    )

    providers_data = []
    for pp in provider_parts:
        try:
            inv_obj = pp.inventory
        except src_models.ProviderPartInventory.DoesNotExist:
            inv_obj = None

        provider_info = {
            "provider_id": pp.provider_id,
            "provider_name": pp.provider.name if pp.provider else None,
            "provider_kind_name": pp.provider.kind_name if pp.provider else None,
            "provider_external_id": pp.provider_external_id,
            "inventory": None,
            "pricing": None,
        }

        if inv_obj:
            provider_info["inventory"] = {
                "total_qty": inv_obj.total_qty,
                "manufacturer_inventory": inv_obj.manufacturer_inventory,
                "warehouse_availability": inv_obj.warehouse_availability,
                "last_synced_at": inv_obj.last_synced_at.isoformat() if inv_obj.last_synced_at else None,
            }

        if company_id:
            pricings = [p for p in pp.company_pricing.all() if p.company_id == company_id]
            pricing = pricings[0] if pricings else None
            if pricing:
                provider_info["pricing"] = {
                    "cost": float(pricing.cost) if pricing.cost else None,
                    "jobber_price": float(pricing.jobber_price) if pricing.jobber_price else None,
                    "map_price": float(pricing.map_price) if pricing.map_price else None,
                    "msrp": float(pricing.msrp) if pricing.msrp else None,
                    "last_synced_at": pricing.last_synced_at.isoformat() if pricing.last_synced_at else None,
                }

        providers_data.append(provider_info)

    base["providers"] = providers_data
    return base
