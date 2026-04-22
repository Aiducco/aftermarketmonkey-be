"""
API services for parts search and detail.
"""
import logging
import re
import typing
from urllib.parse import quote

from django.db.models import Prefetch

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.search import meilisearch_client as meilisearch_client

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

# Provider kind_name -> image URL (edit here to add logos)
PROVIDER_IMAGE_URLS = {
    "TURN_14": "https://api.aftermarketmonkey.com/uploads/t14_logo.png",
    "KEYSTONE": "https://api.aftermarketmonkey.com/uploads/keystone.png",
    "MEYER": "https://api.aftermarketmonkey.com/uploads/meyer_logo.png",
    "ATECH": "https://api.aftermarketmonkey.com/uploads/atech_logo.png",
    "DLG": "https://api.aftermarketmonkey.com/uploads/dlg_logo.png",
    "ROUGH_COUNTRY": "https://api.aftermarketmonkey.com/uploads/rough_country.png",
    "WHEELPROS": "https://api.aftermarketmonkey.com/uploads/wheel_pros_logo.png",
    "SDC": "",
}


def _get_provider_image_url(kind_name: typing.Optional[str]) -> typing.Optional[str]:
    """Get provider image URL. Returns None if not configured."""
    if not kind_name:
        return None
    url = src_constants.PROVIDER_IMAGE_URLS.get(kind_name) or src_constants.PROVIDER_IMAGE_URLS.get(kind_name.upper())
    return url if url else None


def get_parts_search(sku: str, limit: int = 50) -> typing.Dict:
    """
    Search MasterPart by part_number (case-insensitive contains).
    Returns the same part field shape as the Meilisearch index (plus provider_image_urls).
    """
    if not sku or not str(sku).strip():
        return {"data": [], "provider_image_urls": _get_all_provider_image_urls()}

    q = str(sku).strip()
    parts = (
        src_models.MasterPart.objects.filter(part_number__icontains=q)
        .select_related("brand")
        .prefetch_related(
            Prefetch("provider_parts", queryset=src_models.ProviderPart.objects.order_by("id"))
        )
        .order_by("brand__name", "part_number")[:limit]
    )

    data = [meilisearch_client.master_part_to_index_shape(p) for p in parts]
    return {"data": data, "provider_image_urls": _get_all_provider_image_urls()}


def get_master_part_brand_filter_options(
    q: str = "",
    limit: int = 100,
) -> typing.List[typing.Dict[str, typing.Union[int, str]]]:
    """
    Distinct brands that have at least one MasterPart, ordered by name.
    ``name`` matches Meilisearch ``brand_name`` / :func:`master_part_to_index_shape` for filters.

    Optional ``q`` applies case-insensitive substring match (for typeahead in a combobox).
    """
    cap = min(max(limit, 1), 2000)
    brand_ids = src_models.MasterPart.objects.values_list("brand_id", flat=True).distinct()
    brands = src_models.Brands.objects.filter(id__in=brand_ids).order_by("name")
    sq = (q or "").strip()
    if sq:
        brands = brands.filter(name__icontains=sq)
    return [{"id": row["id"], "name": row["name"]} for row in brands.values("id", "name")[:cap]]


def get_master_part_category_filter_options(
    q: str = "",
    limit: int = 200,
) -> typing.Dict[str, typing.List[str]]:
    """
    Distinct non-empty ``category`` and ``overview_category`` from ``CategoryMapping`` (the
    same normalized labels written to ``ProviderPart`` / Meilisearch during sync). Used for
    filter comboboxes so options match the controlled mapping vocabulary, not a scan of
    every distributor row.

    Optional ``q`` applies case-insensitive substring match to each list separately.
    """
    cap = min(max(limit, 1), 2000)
    sq = (q or "").strip()
    base_cat = src_models.CategoryMapping.objects.exclude(category__isnull=True).exclude(
        category=""
    )
    base_over = src_models.CategoryMapping.objects.exclude(overview_category__isnull=True).exclude(
        overview_category=""
    )
    if sq:
        base_cat = base_cat.filter(category__icontains=sq)
        base_over = base_over.filter(overview_category__icontains=sq)
    categories = list(
        base_cat.order_by("category")
        .values_list("category", flat=True)
        .distinct()[:cap]
    )
    overview_categories = list(
        base_over.order_by("overview_category")
        .values_list("overview_category", flat=True)
        .distinct()[:cap]
    )
    return {
        "categories": categories,
        "overview_categories": overview_categories,
    }


def _get_all_provider_image_urls() -> typing.Dict[str, typing.Optional[str]]:
    """Return provider kind_name -> image URL map for all configured providers."""
    return {k: (v if v else None) for k, v in src_constants.PROVIDER_IMAGE_URLS.items()}


def _provider_go_to_link(
    kind_name: typing.Optional[str],
    master_part: src_models.MasterPart,
    provider_external_id: str,
    turn14_vmm_part: typing.Optional[str] = None,
) -> typing.Optional[str]:
    """
    Public web URL for this row's part on the distributor site, when we can derive it.
    Uses distributor-specific identifiers (not only MasterPart.sku, which may reflect another source).
    """
    ext = (provider_external_id or "").strip()
    if kind_name == "KEYSTONE":
        if not ext:
            return None
        return "https://wwwsc.ekeystone.com/Search/Detail?pid={}".format(quote(ext, safe=""))
    if kind_name == "TURN_14":
        slug_src = turn14_vmm_part or master_part.sku or master_part.part_number or ""
        slug = str(slug_src).strip()
        if not slug:
            return None
        return "https://www.turn14.com/search/index.php?vmmPart={}".format(quote(slug.lower(), safe=""))
    if kind_name == "MEYER":
        if not ext:
            return None
        return "https://online.meyerdistributing.com/parts/details/{}".format(quote(ext, safe=""))
    if kind_name == "DLG":
        brand_name = master_part.brand.name if master_part.brand else None
        pn = (master_part.part_number or "").strip()
        kw = src_constants.dlg_b2b_search_keywords(brand_name, pn)
        if not kw:
            return None
        return src_constants.DLG_B2B_INVENTORY_SEARCH_URL_TEMPLATE.format(keywords=quote(kw, safe=""))
    if kind_name == "ATECH":
        # ``provider_external_id`` is ``{atech_brand_id}_{part_number}``; public PDP slug is the part number only.
        slug_src = master_part.sku or master_part.part_number or ext
        slug = str(slug_src).strip().lower()
        if not slug:
            return None
        return src_constants.ATECH_INVENTORY_PART_URL_TEMPLATE.format(part_slug=slug)
    if kind_name == "ROUGH_COUNTRY":
        sku_src = master_part.sku or master_part.part_number or ""
        sku_clean = str(sku_src).strip()
        if not sku_clean:
            return None
        return src_constants.ROUGH_COUNTRY_INVENTORY_SEARCH_URL_TEMPLATE.format(sku=quote(sku_clean, safe=""))
    if kind_name == "WHEELPROS":
        return None
    return None


def get_part_detail(master_part_id: int, company_id: typing.Optional[int] = None) -> typing.Optional[typing.Dict]:
    """
    Get detailed info for one MasterPart.

    Inventory and pricing are returned only when the request ``company_id`` has an active
    ``CompanyProviders`` row for that distributor. Otherwise those fields are null and
    ``company_integration.connected`` is false.
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

    connected_provider_ids: typing.Set[int] = set()
    if company_id is not None:
        connected_provider_ids = set(
            src_models.CompanyProviders.objects.filter(
                company_id=company_id,
                provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
            ).values_list("provider_id", flat=True)
        )

    provider_parts = list(
        src_models.ProviderPart.objects.filter(master_part=part)
        .select_related("provider")
        .prefetch_related("inventory", "company_pricing")
    )

    t14_eid_to_part_number: typing.Dict[str, str] = {}
    t14_external_ids = [
        pp.provider_external_id
        for pp in provider_parts
        if pp.provider
        and pp.provider.kind_name == "TURN_14"
        and (pp.provider_external_id or "").strip()
    ]
    if t14_external_ids:
        for row in src_models.Turn14Items.objects.filter(external_id__in=t14_external_ids).values(
            "external_id", "part_number"
        ):
            pn = row.get("part_number")
            if isinstance(pn, str):
                pn = pn.strip()
            else:
                pn = str(pn or "").strip()
            if pn:
                t14_eid_to_part_number[row["external_id"]] = pn

    providers_data = []
    for pp in provider_parts:
        try:
            inv_obj = pp.inventory
        except src_models.ProviderPartInventory.DoesNotExist:
            inv_obj = None

        kind_name = pp.provider.kind_name if pp.provider else None
        integrated = (
            company_id is not None
            and pp.provider_id is not None
            and pp.provider_id in connected_provider_ids
        )
        turn14_vmm = (
            t14_eid_to_part_number.get(pp.provider_external_id)
            if kind_name == "TURN_14" and pp.provider_external_id
            else None
        )
        distributor_logo_image_url = _get_provider_image_url(kind_name)
        provider_info = {
            "provider_id": pp.provider_id,
            "provider_name": pp.provider.name if pp.provider else None,
            "provider_kind_name": kind_name,
            "provider_display_name": src_constants.PROVIDER_DISPLAY_NAMES.get(kind_name, kind_name) if kind_name else None,
            "provider_image_url": distributor_logo_image_url,
            "distributor_logo_image_url": distributor_logo_image_url,
            "distributor_refreshed_at": (
                pp.distributor_refreshed_at.isoformat() if pp.distributor_refreshed_at else None
            ),
            "provider_external_id": pp.provider_external_id,
            "provider_go_to_link": _provider_go_to_link(
                kind_name,
                part,
                pp.provider_external_id or "",
                turn14_vmm,
            ),
            "company_integration": {"connected": integrated},
            "inventory": None,
            "pricing": None,
        }

        if integrated:
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

            pricing_row = None
            if company_id is not None:
                for p in pp.company_pricing.all():
                    if p.company_id == company_id:
                        pricing_row = p
                        break
            if pricing_row:
                provider_info["pricing"] = {
                    "cost": float(pricing_row.cost) if pricing_row.cost else None,
                    "jobber_price": float(pricing_row.jobber_price) if pricing_row.jobber_price else None,
                    "map_price": float(pricing_row.map_price) if pricing_row.map_price else None,
                    "msrp": float(pricing_row.msrp) if pricing_row.msrp else None,
                    "retail_price": (
                        float(pricing_row.retail_price) if pricing_row.retail_price else None
                    ),
                    "last_synced_at": (
                        pricing_row.last_synced_at.isoformat() if pricing_row.last_synced_at else None
                    ),
                }

        providers_data.append(provider_info)

    base["provider_image_urls"] = _get_all_provider_image_urls()
    base["providers"] = providers_data
    return base


def list_part_detail_audit_history(
    *,
    company_id: int,
    user_id: typing.Optional[int] = None,
    limit: int = 50,
    offset: int = 0,
) -> typing.Dict:
    """
    Rows from ``PartRequestAudit`` for part detail requests (action=detail, ``master_part_id`` set).
    When ``user_id`` is set, restrict to that user; otherwise include all users in the company.

    Each item includes ``part`` using :func:`src.search.meilisearch_client.master_part_to_index_shape`
    when the master part still exists; otherwise ``part`` is null and ``master_part_id`` is still set.
    """
    qs = (
        src_models.PartRequestAudit.objects.filter(
            company_id=company_id,
            action="detail",
        )
        .exclude(master_part_id__isnull=True)
        .order_by("-created_at")
    )
    if user_id is not None:
        qs = qs.filter(user_id=user_id)

    total = qs.count()
    page = qs[offset : offset + limit]
    rows = list(
        page.values(
            "id",
            "action",
            "created_at",
            "master_part_id",
            "user_id",
            "user__id",
            "user__email",
        )
    )
    part_ids = list({r["master_part_id"] for r in rows if r.get("master_part_id")})
    parts_by_id: typing.Dict[int, src_models.MasterPart] = {}
    if part_ids:
        for mp in src_models.MasterPart.objects.filter(id__in=part_ids).select_related("brand").prefetch_related(
            Prefetch("provider_parts", queryset=src_models.ProviderPart.objects.order_by("id"))
        ):
            parts_by_id[mp.id] = mp

    data: typing.List[typing.Dict] = []
    for r in rows:
        mpid = r.get("master_part_id")
        mp = parts_by_id.get(mpid) if mpid else None
        part_payload = meilisearch_client.master_part_to_index_shape(mp) if mp else None
        uid = r.get("user_id")
        u_id = r.get("user__id")
        u_email = r.get("user__email")
        data.append(
            {
                "id": r["id"],
                "action": r["action"],
                "requested_at": r["created_at"].isoformat() if r["created_at"] else None,
                "master_part_id": mpid,
                "part": part_payload,
                "user": (
                    {
                        "id": u_id,
                        "email": u_email or None,
                    }
                    if uid is not None
                    else None
                ),
            }
        )

    return {
        "data": data,
        "provider_image_urls": _get_all_provider_image_urls(),
        "total": total,
        "limit": limit,
        "offset": offset,
    }
