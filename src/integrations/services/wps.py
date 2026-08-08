"""
Western Power Sports raw-feed ingest.

WPS exposes a cursor-paginated JSON:API, so the whole catalog comes down in a couple of dozen
calls -- no per-part hydrate. Brands, warehouses, products, items and inventory are all
distributor-wide and maintained once from the primary connection; only price is per company
(WpsCompanyPricing), since each connection reads /items with its own bearer token.

Flow:
  * ``fetch_and_save_wps_brands``      -- GET /brands      -> WpsBrand
  * ``fetch_and_save_wps_warehouses``  -- GET /warehouses  -> WpsWarehouse (decodes the
        per-warehouse inventory columns into real place names)
  * ``fetch_and_save_wps_products``    -- GET /products    -> WpsProduct (long descriptions)
  * ``fetch_and_save_wps_items``       -- GET /items?include=inventory,images,taxonomyterms
        -> WpsItem + WpsInventory. Images and taxonomy terms have no item foreign key on their
        own collection endpoints, so the include is the only way to attach them.
  * ``fetch_and_save_wps_items_updates`` -- the same, scoped to filter[updated_at][gt]=watermark
  * ``fetch_and_save_wps_inventory``   -- GET /inventory   -> WpsInventory only (cheap refresh)
  * ``sync_wps_company_pricing_for_company_provider`` -- prices for one company
"""
import decimal
import logging
import time
import typing

import pgbulk
from django.db.models.functions import Upper
from django.utils import timezone
from django.utils.dateparse import parse_datetime

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.wps import client as wps_client
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WPS-SERVICES]"

_ITEM_INCLUDES = "inventory,images,taxonomyterms"

# Re-poll a window before the last-seen change; upserts make the overlap harmless.
_INCREMENTAL_OVERLAP_MINUTES = 60

_BRAND_UPDATE_FIELDS = ["name", "updated_at"]
_WAREHOUSE_UPDATE_FIELDS = ["db2_key", "name", "updated_at"]
_PRODUCT_UPDATE_FIELDS = ["name", "description", "data", "updated_at"]
_ITEM_UPDATE_FIELDS = [
    "brand", "product", "sku", "name", "supplier_product_id", "upc", "superseded_sku",
    "status", "product_type", "length", "width", "height", "weight",
    "has_map_policy", "drop_ship_eligible", "drop_ship_fee",
    "image_url", "images", "taxonomy", "source_updated_at", "data", "updated_at",
]
_INVENTORY_UPDATE_FIELDS = [
    "external_id", "sku", "ca_warehouse", "ga_warehouse", "id_warehouse", "in_warehouse",
    "pa_warehouse", "pa2_warehouse", "tx_warehouse", "total", "source_updated_at", "updated_at",
]
_COMPANY_PRICING_UPDATE_FIELDS = [
    "list_price", "dealer_price", "map_price", "has_map_policy", "updated_at",
]

# WpsInventory column <-> WpsWarehouse.db2_key. Kept here (not derived) so a warehouse row that
# has not been fetched yet cannot silently drop stock from warehouse_availability.
WAREHOUSE_COLUMN_BY_KEY = {
    "ID": "id_warehouse",
    "CA": "ca_warehouse",
    "PA": "pa_warehouse",
    "IN": "in_warehouse",
    "TX": "tx_warehouse",
    "GA": "ga_warehouse",
    "PA2": "pa2_warehouse",
}


# ---------------------------------------------------------------------------
# Connection / client resolution
# ---------------------------------------------------------------------------
def _active_wps_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        active=True,
    ).select_related("company", "provider")


def _resolve_company_provider(
    company_provider_id: typing.Optional[int] = None,
) -> typing.Optional[src_models.CompanyProviders]:
    """Explicit id when given, else primary active connection, else first active."""
    base = _active_wps_company_providers_queryset()
    if company_provider_id is not None:
        return base.filter(id=company_provider_id).first()
    return base.filter(primary=True).first() or base.first()


def _build_client(cp: src_models.CompanyProviders) -> wps_client.WpsApiClient:
    return wps_client.WpsApiClient(credentials=credentials_helper.get_feed_credentials(cp))


def _json_safe(value: typing.Any) -> typing.Any:
    """
    Recursively convert Decimals (the client parses floats as Decimal so prices keep their
    exact scale) to floats, so the raw ``data`` blob is JSON-serializable. Typed price columns
    keep the exact values; this copy is a lossy-but-faithful raw mirror.
    """
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _parse_dt(value: typing.Optional[str]):
    """WPS timestamps are naive 'YYYY-MM-DD HH:MM:SS', treated as UTC."""
    if not value:
        return None
    parsed = parse_datetime(str(value))
    if parsed is None:
        return None
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, timezone.utc)
    return parsed


def _decimal_or_none(value):
    if value in (None, ""):
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _image_url(image: typing.Dict) -> typing.Optional[str]:
    """WPS returns image parts separately; the usable URL is domain + path + filename."""
    domain = (image.get("domain") or "").strip()
    path = (image.get("path") or "").strip()
    filename = (image.get("filename") or "").strip()
    if not (domain and filename):
        return None
    return "https://{}{}{}".format(domain, path, filename)


# ---------------------------------------------------------------------------
# Brands / warehouses / products
# ---------------------------------------------------------------------------
def fetch_and_save_wps_brands(company_provider_id: typing.Optional[int] = None) -> None:
    cp = _resolve_company_provider(company_provider_id)
    if not cp:
        logger.info("{} No active WPS provider found.".format(_LOG_PREFIX))
        return
    rows = _build_client(cp).get_brands()
    instances = [
        src_models.WpsBrand(external_id=str(r["id"]), name=r.get("name"))
        for r in rows
        if r.get("id") is not None
    ]
    if instances:
        pgbulk.upsert(
            src_models.WpsBrand, instances,
            unique_fields=["external_id"], update_fields=_BRAND_UPDATE_FIELDS, returning=False,
        )
    logger.info("{} Upserted {} WPS brands.".format(_LOG_PREFIX, len(instances)))


def fetch_and_save_wps_warehouses(company_provider_id: typing.Optional[int] = None) -> None:
    cp = _resolve_company_provider(company_provider_id)
    if not cp:
        logger.info("{} No active WPS provider found.".format(_LOG_PREFIX))
        return
    rows = _build_client(cp).get_warehouses()
    instances = [
        src_models.WpsWarehouse(
            external_id=str(r["id"]), db2_key=r.get("db2_key"), name=r.get("name")
        )
        for r in rows
        if r.get("id") is not None
    ]
    if instances:
        pgbulk.upsert(
            src_models.WpsWarehouse, instances,
            unique_fields=["external_id"], update_fields=_WAREHOUSE_UPDATE_FIELDS, returning=False,
        )
    logger.info("{} Upserted {} WPS warehouses.".format(_LOG_PREFIX, len(instances)))


def fetch_and_save_wps_products(company_provider_id: typing.Optional[int] = None) -> None:
    cp = _resolve_company_provider(company_provider_id)
    if not cp:
        logger.info("{} No active WPS provider found.".format(_LOG_PREFIX))
        return
    client = _build_client(cp)
    total = 0
    for page in client.iter_products():
        instances = [
            src_models.WpsProduct(
                external_id=str(r["id"]),
                name=r.get("name"),
                description=r.get("description"),
                data=_json_safe(r),
            )
            for r in page
            if r.get("id") is not None
        ]
        if instances:
            pgbulk.upsert(
                src_models.WpsProduct, instances,
                unique_fields=["external_id"], update_fields=_PRODUCT_UPDATE_FIELDS, returning=False,
            )
            total += len(instances)
    logger.info("{} Upserted {} WPS products.".format(_LOG_PREFIX, total))


# ---------------------------------------------------------------------------
# Items (+ inventory / images / taxonomy via include)
# ---------------------------------------------------------------------------
def fetch_and_save_wps_items(
    company_provider_id: typing.Optional[int] = None,
    updated_after: typing.Optional[str] = None,
) -> None:
    """
    Full catalog pass (or incremental when ``updated_after`` is given). Pulls inventory, images
    and taxonomy in the same request via ?include=, since those have no item foreign key on
    their own collection endpoints.
    """
    cp = _resolve_company_provider(company_provider_id)
    if not cp:
        logger.info("{} No active WPS provider found.".format(_LOG_PREFIX))
        return
    client = _build_client(cp)

    brand_ids = dict(src_models.WpsBrand.objects.values_list("external_id", "id"))
    product_ids = dict(src_models.WpsProduct.objects.values_list("external_id", "id"))
    if not brand_ids:
        logger.warning(
            "{} No WPS brands stored — run fetch_and_save_wps_brands first.".format(_LOG_PREFIX)
        )
        return

    started = time.monotonic()
    total_items = 0
    page_num = 0
    for page in client.iter_items(include=_ITEM_INCLUDES, updated_after=updated_after):
        page_num += 1
        items, inventories = _transform_items_page(page, brand_ids, product_ids)
        if items:
            pgbulk.upsert(
                src_models.WpsItem, items,
                unique_fields=["external_id"], update_fields=_ITEM_UPDATE_FIELDS, returning=False,
            )
            total_items += len(items)
        _upsert_inventory_for_items(inventories)
        if page_num % 20 == 0:
            logger.info(
                "{} Items: page {}, {} rows in {:.1f}s.".format(
                    _LOG_PREFIX, page_num, total_items, time.monotonic() - started
                )
            )
    logger.info(
        "{} Items done: {} rows in {} pages, {:.1f}s (updated_after={}).".format(
            _LOG_PREFIX, total_items, page_num, time.monotonic() - started, updated_after or "none"
        )
    )


def fetch_and_save_wps_items_updates(company_provider_id: typing.Optional[int] = None) -> None:
    """Incremental item/inventory poll from the stored high-water mark."""
    latest = (
        src_models.WpsItem.objects.exclude(source_updated_at__isnull=True)
        .order_by("-source_updated_at")
        .values_list("source_updated_at", flat=True)
        .first()
    )
    if not latest:
        logger.info("{} No WPS items yet; running a full item pass.".format(_LOG_PREFIX))
        return fetch_and_save_wps_items(company_provider_id=company_provider_id)
    since = (latest - timezone.timedelta(minutes=_INCREMENTAL_OVERLAP_MINUTES)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    logger.info("{} Item updates since {}.".format(_LOG_PREFIX, since))
    fetch_and_save_wps_items(company_provider_id=company_provider_id, updated_after=since)


def _transform_items_page(
    page: typing.List[typing.Dict],
    brand_ids: typing.Dict[str, int],
    product_ids: typing.Dict[str, int],
) -> typing.Tuple[typing.List[src_models.WpsItem], typing.List[typing.Tuple[str, typing.Dict]]]:
    """One /items page -> (WpsItem rows, [(item_external_id, raw inventory dict)])."""
    items: typing.Dict[str, src_models.WpsItem] = {}
    inventories: typing.List[typing.Tuple[str, typing.Dict]] = []

    for row in page:
        external_id = str(row.get("id") or "").strip()
        if not external_id:
            continue

        images_raw = ((row.get("images") or {}).get("data")) or []
        image_urls = [u for u in (_image_url(i) for i in images_raw) if u]
        taxonomy_raw = ((row.get("taxonomyterms") or {}).get("data")) or []
        taxonomy = [t.get("name") for t in taxonomy_raw if t.get("name")]

        # Keep the raw payload minus the included relations; those are columned above and
        # would otherwise duplicate a large blob per row.
        data = _json_safe(
            {k: v for k, v in row.items() if k not in ("inventory", "images", "taxonomyterms")}
        )

        items[external_id] = src_models.WpsItem(
            external_id=external_id,
            brand_id=brand_ids.get(str(row.get("brand_id"))),
            product_id=product_ids.get(str(row.get("product_id"))),
            sku=row.get("sku"),
            name=row.get("name"),
            supplier_product_id=row.get("supplier_product_id"),
            upc=row.get("upc"),
            superseded_sku=row.get("superseded_sku"),
            status=row.get("status") or row.get("status_id"),
            product_type=row.get("product_type"),
            length=row.get("length"),
            width=row.get("width"),
            height=row.get("height"),
            weight=row.get("weight"),
            has_map_policy=bool(row.get("has_map_policy", False)),
            drop_ship_eligible=bool(row.get("drop_ship_eligible", False)),
            drop_ship_fee=row.get("drop_ship_fee"),
            image_url=image_urls[0] if image_urls else None,
            images=image_urls or None,
            taxonomy=taxonomy or None,
            source_updated_at=_parse_dt(row.get("updated_at")),
            data=data,
        )

        inv = (row.get("inventory") or {}).get("data")
        if isinstance(inv, dict) and inv:
            inventories.append((external_id, inv))

    return list(items.values()), inventories


def _upsert_inventory_for_items(
    inventories: typing.List[typing.Tuple[str, typing.Dict]]
) -> int:
    """Upsert WpsInventory rows given (item external_id, raw inventory dict) pairs."""
    if not inventories:
        return 0
    item_ids = dict(
        src_models.WpsItem.objects.filter(
            external_id__in=[e for e, _ in inventories]
        ).values_list("external_id", "id")
    )
    rows: typing.Dict[int, src_models.WpsInventory] = {}
    for external_id, inv in inventories:
        item_id = item_ids.get(external_id)
        if not item_id:
            continue
        rows[item_id] = src_models.WpsInventory(
            item_id=item_id,
            external_id=str(inv.get("id") or "") or None,
            sku=inv.get("sku"),
            ca_warehouse=inv.get("ca_warehouse") or 0,
            ga_warehouse=inv.get("ga_warehouse") or 0,
            id_warehouse=inv.get("id_warehouse") or 0,
            in_warehouse=inv.get("in_warehouse") or 0,
            pa_warehouse=inv.get("pa_warehouse") or 0,
            pa2_warehouse=inv.get("pa2_warehouse") or 0,
            tx_warehouse=inv.get("tx_warehouse") or 0,
            total=inv.get("total") or 0,
            source_updated_at=_parse_dt(inv.get("updated_at")),
        )
    if rows:
        pgbulk.upsert(
            src_models.WpsInventory, list(rows.values()),
            unique_fields=["item"], update_fields=_INVENTORY_UPDATE_FIELDS, returning=False,
        )
    return len(rows)


def fetch_and_save_wps_inventory(company_provider_id: typing.Optional[int] = None) -> None:
    """
    Stock-only refresh straight off /inventory (no item payload). Cheaper than a full item pass
    when only quantities need to move; item rows must already exist.
    """
    cp = _resolve_company_provider(company_provider_id)
    if not cp:
        logger.info("{} No active WPS provider found.".format(_LOG_PREFIX))
        return
    client = _build_client(cp)

    started = time.monotonic()
    total = 0
    page_num = 0
    for page in client.iter_inventory():
        page_num += 1
        pairs = [(str(r.get("item_id")), r) for r in page if r.get("item_id") is not None]
        total += _upsert_inventory_for_items(pairs)
        if page_num % 10 == 0:
            logger.info(
                "{} Inventory: page {}, {} rows in {:.1f}s.".format(
                    _LOG_PREFIX, page_num, total, time.monotonic() - started
                )
            )
    logger.info(
        "{} Inventory done: {} rows in {} pages, {:.1f}s.".format(
            _LOG_PREFIX, total, page_num, time.monotonic() - started
        )
    )


# ---------------------------------------------------------------------------
# Brand mapping (WpsBrand -> Brands)
# ---------------------------------------------------------------------------
def _brand_name_upper_for_sync(wps_brand: src_models.WpsBrand) -> str:
    name_upper = (wps_brand.name or "").strip().upper()
    return name_upper or "BRAND_{}".format(wps_brand.external_id)


def sync_unmapped_wps_brands_to_brands() -> typing.List[src_models.WpsBrand]:
    """
    For each WpsBrand without a BrandWpsBrandMapping: resolve Brand by exact name (uppercase),
    then compact-key, then fuzzy word-prefix; otherwise create it. Upserts BrandWpsBrandMapping,
    BrandProviders and CompanyBrands for TICK_PERFORMANCE. WPS carries no AAIA code, so matching
    is name-based only (same as Quadratec and Motor State).
    """
    logger.info("{} Syncing unmapped WPS brands to Brands.".format(_LOG_PREFIX))

    provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
    ).first()
    if not provider:
        logger.warning("{} WPS provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    tick_company = src_models.Company.objects.filter(name="TICK_PERFORMANCE").first()
    if not tick_company:
        logger.warning("{} Company TICK_PERFORMANCE not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandWpsBrandMapping.objects.values_list("wps_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.WpsBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped WPS brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped WPS brands.".format(_LOG_PREFIX, len(unmapped)))
    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 1: exact uppercase-name match.
    name_keys = {(b.name or "").strip().upper() for b in unmapped if (b.name or "").strip()}
    by_upper: typing.Dict[str, src_models.Brands] = {}
    if name_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_keys)
            .order_by("id")
        ):
            by_upper.setdefault((b.name or "").strip().upper(), b)
    for wb in unmapped:
        nm = (wb.name or "").strip().upper()
        if nm and nm in by_upper:
            resolved_by_id[wb.id] = by_upper[nm]

    # Phase 2: compact-key (punctuation/spacing-insensitive).
    compact_matches = 0
    still = [wb for wb in unmapped if wb.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for wb in still:
            key = normalize_compact_key(wb.name or "")
            if key and key in compact_index:
                resolved_by_id[wb.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix.
    unresolved = [wb for wb in unmapped if wb.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for wb in unresolved:
        parts = normalize_upper_words(wb.name or "").split()
        candidates = list(first_index.get(parts[0], ())) if parts else []
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(wb.name or "", candidates)
        if brand:
            resolved_by_id[wb.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved.
    new_specs: typing.Dict[str, None] = {}
    for wb in unmapped:
        if wb.id not in resolved_by_id:
            new_specs.setdefault(_brand_name_upper_for_sync(wb), None)

    created_brands = 0
    if new_specs:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_specs.keys())).values_list(
                "name", flat=True
            )
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for name in new_specs
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            b.name: b for b in src_models.Brands.objects.filter(name__in=list(new_specs.keys()))
        }
        for wb in unmapped:
            if wb.id not in resolved_by_id:
                resolved_by_id[wb.id] = by_name[_brand_name_upper_for_sync(wb)]

    mapping_models = [
        src_models.BrandWpsBrandMapping(brand_id=resolved_by_id[wb.id].id, wps_brand_id=wb.id)
        for wb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandWpsBrandMapping, mapping_models,
        unique_fields=["brand", "wps_brand"], update_fields=[], returning=False,
    )

    created_bp = created_cb = 0
    for wb in unmapped:
        brand = resolved_by_id[wb.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(brand=brand, provider=provider)
        created_bp += int(bp_created)
        _, cb_created = src_models.CompanyBrands.objects.get_or_create(
            company=tick_company,
            brand=brand,
            defaults={
                "status": src_enums.CompanyBrandStatus.ACTIVE.value,
                "status_name": src_enums.CompanyBrandStatus.ACTIVE.name,
            },
        )
        created_cb += int(cb_created)

    logger.info(
        "{} Sync complete. Brands created: {}, compact: {}, fuzzy: {}, mappings: {}, "
        "BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX, created_brands, compact_matches, fuzzy_matches,
            len(mapping_models), created_bp, created_cb,
        )
    )
    return unmapped


# ---------------------------------------------------------------------------
# Per-company pricing (IntegrationPricingSyncJob entrypoint)
# ---------------------------------------------------------------------------
def sync_wps_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Refresh this connection's WpsCompanyPricing rows using its own bearer token.

    WPS has no pricing endpoint -- prices ride on /items -- so this re-reads /items with sparse
    fieldsets (id + the three price columns) and writes only prices. Catalog, inventory and
    brands are distributor-wide and stay with the primary connection. The sparse read is ~13
    calls, so unlike Motor State this needs no throttling.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.WESTERN_POWER_SPORTS.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active WPS CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    if not src_models.WpsItem.objects.exists():
        logger.warning(
            "{} No shared WPS catalog yet; run fetch_wps_items before pricing "
            "company_provider_id={}.".format(_LOG_PREFIX, cp.id)
        )
        return

    client = _build_client(cp)
    item_ids = dict(src_models.WpsItem.objects.values_list("external_id", "id"))

    started = time.monotonic()
    total = 0
    for page in client.iter_items(
        fields="id,list_price,standard_dealer_price,mapp_price,has_map_policy"
    ):
        rows: typing.Dict[int, src_models.WpsCompanyPricing] = {}
        for r in page:
            item_id = item_ids.get(str(r.get("id")))
            if not item_id:
                continue  # not in the shared catalog yet; next catalog sync will add it
            rows[item_id] = src_models.WpsCompanyPricing(
                item_id=item_id,
                company=cp.company,
                list_price=_decimal_or_none(r.get("list_price")),
                dealer_price=_decimal_or_none(r.get("standard_dealer_price")),
                map_price=_decimal_or_none(r.get("mapp_price")),
                has_map_policy=bool(r.get("has_map_policy", False)),
            )
        if rows:
            pgbulk.upsert(
                src_models.WpsCompanyPricing, list(rows.values()),
                unique_fields=["item", "company"],
                update_fields=_COMPANY_PRICING_UPDATE_FIELDS, returning=False,
            )
            total += len(rows)
    logger.info(
        "{} Pricing done for company_provider_id={}: {} rows in {:.1f}s.".format(
            _LOG_PREFIX, cp.id, total, time.monotonic() - started
        )
    )


def fetch_and_save_wps_full_sync(company_provider_id: typing.Optional[int] = None) -> None:
    """Brands -> warehouses -> products -> items(+inventory) -> brand mapping."""
    fetch_and_save_wps_brands(company_provider_id=company_provider_id)
    fetch_and_save_wps_warehouses(company_provider_id=company_provider_id)
    fetch_and_save_wps_products(company_provider_id=company_provider_id)
    fetch_and_save_wps_items(company_provider_id=company_provider_id)
    sync_unmapped_wps_brands_to_brands()
