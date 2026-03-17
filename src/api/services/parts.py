"""
API services for parts search and detail.
"""
import logging
import re
import typing

from src import models as src_models

logger = logging.getLogger(__name__)

# Turn14 warehouse keys may be "01", "02" or "wh 01", "wh 02" - normalize to external_id
_TURN14_WH_KEY_RE = re.compile(r"^(?:wh\s+)?(\d+)$", re.IGNORECASE)


def _map_turn14_warehouse_availability(
    warehouse_availability: typing.Optional[typing.Dict],
) -> typing.Optional[typing.Dict[str, typing.Union[int, float]]]:
    """
    Map Turn14 warehouse codes (e.g. "01", "wh 01") to location names from Turn14Location.
    Returns dict with location name as key, qty as value. Unknown keys keep original.
    """
    if not warehouse_availability or not isinstance(warehouse_availability, dict):
        return warehouse_availability

    locations = {
        loc["external_id"]: loc["name"]
        for loc in src_models.Turn14Location.objects.all().values("external_id", "name")
    }

    result = {}
    for key, qty in warehouse_availability.items():
        if not isinstance(qty, (int, float)):
            continue
        match = _TURN14_WH_KEY_RE.match(str(key).strip())
        external_id = match.group(1) if match else str(key).strip()
        display_name = locations.get(external_id) or key
        result[display_name] = int(qty) if isinstance(qty, float) and qty == int(qty) else qty

    return result if result else None

_LOG_PREFIX = "[PARTS-SERVICES]"

# Provider kind_name -> display name for API
PROVIDER_DISPLAY_NAMES = {
    "TURN_14": "Turn 14",
    "KEYSTONE": "Keystone",
    "ROUGH_COUNTRY": "Rough Country",
    "SDC": "SDC",
}

# Provider kind_name -> image URL (edit here to add logos)
PROVIDER_IMAGE_URLS = {
    "TURN_14": "https://api.aftermarketmonkey.com/uploads/t14_logo.png",
    "KEYSTONE": "https://api.aftermarketmonkey.com/uploads/keystone.png",
    "ROUGH_COUNTRY": "https://api.aftermarketmonkey.com/uploads/rough_country.png",
    "SDC": "",
}


def _get_provider_image_url(kind_name: typing.Optional[str]) -> typing.Optional[str]:
    """Get provider image URL. Returns None if not configured."""
    if not kind_name:
        return None
    url = PROVIDER_IMAGE_URLS.get(kind_name) or PROVIDER_IMAGE_URLS.get(kind_name.upper())
    return url if url else None


def get_parts_search(sku: str, limit: int = 50) -> typing.Dict:
    """
    Search MasterPart by part_number (case-insensitive contains).
    Returns MasterPart fields + brand_id, and provider_image_urls map for frontend.
    """
    if not sku or not str(sku).strip():
        return {"data": [], "provider_image_urls": _get_all_provider_image_urls()}

    q = str(sku).strip()
    parts = (
        src_models.MasterPart.objects.filter(part_number__icontains=q)
        .select_related("brand")
        .order_by("brand__name", "part_number")[:limit]
    )

    data = [
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
    return {"data": data, "provider_image_urls": _get_all_provider_image_urls()}


def _get_all_provider_image_urls() -> typing.Dict[str, typing.Optional[str]]:
    """Return provider kind_name -> image URL map for all configured providers."""
    return {k: (v if v else None) for k, v in PROVIDER_IMAGE_URLS.items()}


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

        kind_name = pp.provider.kind_name if pp.provider else None
        provider_info = {
            "provider_id": pp.provider_id,
            "provider_name": pp.provider.name if pp.provider else None,
            "provider_kind_name": kind_name,
            "provider_display_name": PROVIDER_DISPLAY_NAMES.get(kind_name, kind_name) if kind_name else None,
            "provider_image_url": _get_provider_image_url(kind_name),
            "provider_external_id": pp.provider_external_id,
            "inventory": None,
            "pricing": None,
        }

        if inv_obj:
            wh_avail = inv_obj.warehouse_availability
            if kind_name == "TURN_14":
                wh_avail = _map_turn14_warehouse_availability(wh_avail)
            provider_info["inventory"] = {
                "warehouse_total_qty": inv_obj.warehouse_total_qty,
                "manufacturer_inventory": inv_obj.manufacturer_inventory,
                "manufacturer_esd": inv_obj.manufacturer_esd.isoformat() if inv_obj.manufacturer_esd else None,
                "warehouse_availability": wh_avail,
                "last_synced_at": inv_obj.last_synced_at.isoformat() if inv_obj.last_synced_at else None,
            }

        # Prefer pricing for request company; if no company_id or no row for that company, use first available
        all_pricing = list(pp.company_pricing.all())
        pricing = None
        if company_id:
            for p in all_pricing:
                if p.company_id == company_id:
                    pricing = p
                    break
        if pricing is None and all_pricing:
            pricing = all_pricing[0]
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
