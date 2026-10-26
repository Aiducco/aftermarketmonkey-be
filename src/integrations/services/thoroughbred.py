"""
Thoroughbred Diesel integration: a fully public, unauthenticated HawkSearch catalog (no login,
no cookies, no API key beyond a public ClientGuid) plus a Cloudflare-fronted main-site ajax
endpoint used only to decode vehicle ids into year/make/model. See
docs/THOROUGHBRED_DIESEL_INGEST_SPEC.md for the full live-measured spec this module implements.

Syncs ThoroughbredBrand, ThoroughbredParts, ThoroughbredVehicle, BrandThoroughbredBrandMapping,
and (per-company) ThoroughbredCompanyPricing. There is no distributor-side login of any kind --
the only thing a connecting company supplies is their own markup percentage off Thoroughbred's
public retail price, since Thoroughbred exposes no wholesale/dealer cost anywhere public.
"""
import logging
import typing
from decimal import Decimal, ROUND_HALF_UP

import pgbulk

from django.db.models.functions import Upper
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.thoroughbred import client as thoroughbred_client
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[THOROUGHBRED-SERVICES]"

CATALOG_UPSERT_BATCH = 2000
VEHICLE_UPSERT_BATCH = 2000

# CompanyProviders.credentials key a connecting company supplies at connect time -- see
# PROVIDER_CATALOG's "connection_required_fields" entry for THOROUGHBRED_DIESEL in constants.py
# and _validate_thoroughbred_markup_field in api/services/integrations.py.
MARKUP_CREDENTIAL_KEY = "markup"


def _thoroughbred_provider() -> typing.Optional[src_models.Providers]:
    return src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.THOROUGHBRED_DIESEL.value,
    ).first()


def _parse_thoroughbred_price(raw: typing.Optional[str]) -> typing.Optional[Decimal]:
    """HawkSearch ships price as a zero-padded Solr sortable string, e.g.
    "+0000000000002400.0000" -> Decimal("2400.00")."""
    if raw is None:
        return None
    try:
        value = Decimal(str(raw).lstrip("+")).quantize(Decimal("0.01"))
    except Exception:
        return None
    if value <= 0 or value >= 30000:
        logger.warning(
            "{} Parsed price {} outside expected (0, 30000) bound, raw={!r}.".format(
                _LOG_PREFIX, value, raw
            )
        )
    return value


def _clamp_thoroughbred_inventory(raw: typing.Optional[str]) -> typing.Optional[int]:
    """One doc in the index uses 99999999 as an "unlimited / drop-ship" sentinel -- never a real
    quantity. Clamped to None (unknown) rather than persisted raw, or a single row injects 100M
    phantom units into any sum."""
    if raw is None:
        return None
    try:
        value = int(str(raw).strip())
    except Exception:
        return None
    if value == thoroughbred_client.INVENTORY_SENTINEL:
        return None
    return value


def _parse_vehicle_ids(doc: dict) -> typing.Optional[typing.List[int]]:
    raw = doc.get("vehicleid")
    if not raw:
        return None
    if not isinstance(raw, list):
        raw = [raw]
    out = []
    for v in raw:
        try:
            out.append(int(v))
        except (TypeError, ValueError):
            continue
    return out or None


def _is_placeholder_image(url: typing.Optional[str]) -> bool:
    return not url or "no-image" in url.lower()


def fetch_and_save_thoroughbred_catalog() -> int:
    """
    Pulls the entire public HawkSearch catalog (no Query -> full 51,245-doc index, 712 pages at
    MaxPerPage=72) and upserts ThoroughbredParts keyed on external_id (HawkSearch's own doc id).
    ThoroughbredBrand rows are discovered inline from distinct `b` values seen -- HawkSearch has
    no separate brand-list endpoint.
    """
    logger.info("{} Fetching Thoroughbred Diesel HawkSearch catalog.".format(_LOG_PREFIX))

    client = thoroughbred_client.HawkSearchClient()
    brand_by_name: typing.Dict[str, src_models.ThoroughbredBrand] = {
        b.name: b for b in src_models.ThoroughbredBrand.objects.all()
    }

    total_upserted = 0
    batch: typing.List[dict] = []
    batch_num = 0

    def flush(docs: typing.List[dict]) -> int:
        nonlocal batch_num
        batch_num += 1

        new_brand_names = {
            (thoroughbred_client.g(doc, "b") or "").strip()
            for doc in docs
            if (thoroughbred_client.g(doc, "b") or "").strip() and (thoroughbred_client.g(doc, "b") or "").strip() not in brand_by_name
        }
        if new_brand_names:
            src_models.ThoroughbredBrand.objects.bulk_create(
                [src_models.ThoroughbredBrand(name=name) for name in new_brand_names],
                ignore_conflicts=True,
            )
            for b in src_models.ThoroughbredBrand.objects.filter(name__in=new_brand_names):
                brand_by_name[b.name] = b

        instances = []
        for doc in docs:
            external_id = thoroughbred_client.g(doc, "id")
            sku = thoroughbred_client.g(doc, "sku")
            if not external_id or not sku:
                continue
            brand_name = (thoroughbred_client.g(doc, "b") or "").strip()
            image_url = thoroughbred_client.g(doc, "image")
            instances.append(
                src_models.ThoroughbredParts(
                    brand=brand_by_name.get(brand_name),
                    external_id=str(external_id),
                    sku=str(sku),
                    url=thoroughbred_client.g(doc, "url"),
                    title=thoroughbred_client.g(doc, "title"),
                    image_url=None if _is_placeholder_image(image_url) else image_url,
                    price=_parse_thoroughbred_price(thoroughbred_client.g(doc, "price")),
                    inventory=_clamp_thoroughbred_inventory(thoroughbred_client.g(doc, "inventory")),
                    item_type=thoroughbred_client.g(doc, "it"),
                    kit_short_description=thoroughbred_client.g(doc, "kitshortdescription"),
                    vehicleid=_parse_vehicle_ids(doc),
                    raw_data=doc,
                )
            )
        if not instances:
            return 0

        by_external_id = {p.external_id: p for p in instances}
        pgbulk.upsert(
            src_models.ThoroughbredParts,
            list(by_external_id.values()),
            unique_fields=["external_id"],
            update_fields=[
                "brand", "sku", "url", "title", "image_url", "price", "inventory",
                "item_type", "kit_short_description", "vehicleid", "raw_data", "updated_at",
            ],
        )
        logger.info(
            "{} Catalog batch {}: {} docs -> {} upserted.".format(
                _LOG_PREFIX, batch_num, len(docs), len(by_external_id)
            )
        )
        return len(by_external_id)

    for doc in client.iter_all_docs():
        batch.append(doc)
        if len(batch) >= CATALOG_UPSERT_BATCH:
            total_upserted += flush(batch)
            batch = []
    if batch:
        total_upserted += flush(batch)

    logger.info(
        "{} Synced {} Thoroughbred Diesel catalog rows total.".format(_LOG_PREFIX, total_upserted)
    )
    return total_upserted


def sync_unmapped_thoroughbred_brands_to_brands(
    dry_run: bool = False,
) -> typing.List[src_models.ThoroughbredBrand]:
    """
    For each ThoroughbredBrand without a BrandThoroughbredBrandMapping: resolve Brand by exact
    name (uppercase), then compact-key (punctuation/spacing-only differences), then fuzzy
    word-prefix match (shared util with WheelPros/Keystone/Rough Country); otherwise create.

    No AAIA-code phase: HawkSearch's `b` field is a bare brand name with no AAIA code anywhere in
    its schema -- same situation WheelPros is already in, which is why its own brand sync has no
    AAIA phase either. If dry_run is True, no database writes; only logs matches/would-creates.
    """
    logger.info(
        "{} Syncing unmapped Thoroughbred brands to Brands{}.".format(
            _LOG_PREFIX, " (dry run)" if dry_run else ""
        )
    )

    thoroughbred_provider = _thoroughbred_provider()
    if not thoroughbred_provider:
        logger.warning("{} Thoroughbred Diesel provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandThoroughbredBrandMapping.objects.values_list(
            "thoroughbred_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(
        src_models.ThoroughbredBrand.objects.exclude(id__in=mapped_ids).order_by("id")
    )
    if not unmapped:
        logger.info("{} No unmapped Thoroughbred brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    name_upper_keys = {(tb.name or "").strip().upper() for tb in unmapped if (tb.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            key = (b.name or "").strip().upper()
            brands_by_upper_name.setdefault(key, b)

    resolved_by_tb_id: typing.Dict[int, src_models.Brands] = {}
    exact_matched: typing.Set[int] = set()
    for tb in sorted(unmapped, key=lambda x: x.id):
        nm = (tb.name or "").strip().upper()
        if nm and nm in brands_by_upper_name:
            resolved_by_tb_id[tb.id] = brands_by_upper_name[nm]
            exact_matched.add(tb.id)

    compact_matched: typing.Set[int] = set()
    still_unresolved = [tb for tb in unmapped if tb.id not in resolved_by_tb_id]
    if still_unresolved:
        compact_index = brands_by_compact_key()
        for tb in sorted(still_unresolved, key=lambda x: x.id):
            key = normalize_compact_key(tb.name or "")
            if not key:
                continue
            brand = compact_index.get(key)
            if brand:
                resolved_by_tb_id[tb.id] = brand
                compact_matched.add(tb.id)

    unresolved_after_exact = [tb for tb in unmapped if tb.id not in resolved_by_tb_id]
    brands_first_index = brands_by_first_token_upper() if unresolved_after_exact else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matched: typing.Set[int] = set()
    for tb in unresolved_after_exact:
        parts = normalize_upper_words(tb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(tb.name or "", candidates)
        if brand:
            resolved_by_tb_id[tb.id] = brand
            fuzzy_matched.add(tb.id)

    if dry_run:
        for tb in sorted(unmapped, key=lambda x: x.id):
            brand = resolved_by_tb_id.get(tb.id)
            if not brand:
                continue
            how = "exact" if tb.id in exact_matched else "compact" if tb.id in compact_matched else "fuzzy"
            logger.info(
                "{} [dry-run] match ({}) ThoroughbredBrand id={} name={!r} -> Brand id={} name={!r}".format(
                    _LOG_PREFIX, how, tb.id, tb.name, brand.id, brand.name
                )
            )
        would_create = [tb for tb in unmapped if tb.id not in resolved_by_tb_id]
        logger.info(
            "{} [dry-run] Summary: {} exact, {} compact, {} fuzzy, {} unmatched (would create "
            "Brand). No writes performed.".format(
                _LOG_PREFIX, len(exact_matched), len(compact_matched), len(fuzzy_matched), len(would_create)
            )
        )
        return unmapped

    new_brand_names = {
        (tb.name or "").strip().upper() or "BRAND_{}".format(tb.id)
        for tb in unmapped
        if tb.id not in resolved_by_tb_id
    }
    created_brands = 0
    if new_brand_names:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_names)).values_list("name", flat=True)
        )
        new_rows = [
            src_models.Brands(
                name=name,
                status=src_enums.BrandProviderStatus.ACTIVE.value,
                status_name=src_enums.BrandProviderStatus.ACTIVE.name,
                aaia_code=None,
            )
            for name in new_brand_names
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {b.name: b for b in src_models.Brands.objects.filter(name__in=list(new_brand_names))}
        for tb in unmapped:
            if tb.id not in resolved_by_tb_id:
                nm = (tb.name or "").strip().upper() or "BRAND_{}".format(tb.id)
                resolved_by_tb_id[tb.id] = by_name[nm]

    mapping_models = [
        src_models.BrandThoroughbredBrandMapping(
            brand_id=resolved_by_tb_id[tb.id].id,
            thoroughbred_brand_id=tb.id,
        )
        for tb in unmapped
    ]
    pgbulk.upsert(
        src_models.BrandThoroughbredBrandMapping,
        mapping_models,
        unique_fields=["brand", "thoroughbred_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    for tb in unmapped:
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=resolved_by_tb_id[tb.id],
            provider=thoroughbred_provider,
        )
        if bp_created:
            created_brand_providers += 1

    logger.info(
        "{} Sync complete. Brands created: {}, compact matches: {}, fuzzy matches: {}, "
        "mappings upserted: {}, BrandProviders created: {}.".format(
            _LOG_PREFIX, created_brands, len(compact_matched), len(fuzzy_matched),
            len(mapping_models), created_brand_providers,
        )
    )
    return unmapped


def fetch_and_save_thoroughbred_vehicles() -> int:
    """
    Walks the main site's ajax vehicle picker (GetYears/GetModels/GetVehicle over the 8 hardcoded
    makes, ~1,545 requests) and upserts ThoroughbredVehicle keyed on vehicle_id. Cheap enough to
    re-run on every ingest -- makes/years/models change rarely, but there is no cost reason not to.
    """
    logger.info("{} Fetching Thoroughbred Diesel vehicle decode table.".format(_LOG_PREFIX))

    client = thoroughbred_client.MainSiteAjaxClient()
    total_upserted = 0
    batch: typing.List[src_models.ThoroughbredVehicle] = []

    def flush(rows: typing.List[src_models.ThoroughbredVehicle]) -> int:
        by_vehicle_id = {r.vehicle_id: r for r in rows}
        pgbulk.upsert(
            src_models.ThoroughbredVehicle,
            list(by_vehicle_id.values()),
            unique_fields=["vehicle_id"],
            update_fields=["year", "make", "model", "updated_at"],
        )
        return len(by_vehicle_id)

    for vehicle_id, year, make_name, model_name in client.walk_all_vehicles():
        batch.append(
            src_models.ThoroughbredVehicle(
                vehicle_id=vehicle_id, year=year, make=make_name, model=model_name,
            )
        )
        if len(batch) >= VEHICLE_UPSERT_BATCH:
            total_upserted += flush(batch)
            batch = []
    if batch:
        total_upserted += flush(batch)

    logger.info(
        "{} Synced {} Thoroughbred Diesel vehicles total.".format(_LOG_PREFIX, total_upserted)
    )
    return total_upserted


def _parse_markup_percent(raw: typing.Any) -> typing.Optional[Decimal]:
    if raw is None or raw == "":
        return None
    try:
        d = Decimal(str(raw).strip())
    except Exception:
        return None
    if d < 0:
        d = Decimal(0)
    if d > 100:
        d = Decimal(100)
    return d


def markup_percent_for_company(credentials: typing.Optional[typing.Dict]) -> Decimal:
    """
    Markup percent (0-100) off Thoroughbred's public retail price, from
    CompanyProviders.credentials["markup"]. Unlike WheelPros there is no invented default here --
    the connect-flow validator (_validate_thoroughbred_markup_field) already guarantees this is
    present and numeric before a connection is saved, since it is the company's *only* input.
    A missing/corrupt value at sync time is treated defensively as 0% markup (cost == price) with
    a logged warning, rather than assuming any particular discount.
    """
    creds = credentials or {}
    pct = _parse_markup_percent(creds.get(MARKUP_CREDENTIAL_KEY))
    if pct is None:
        logger.warning(
            "{} Missing/unparseable markup credential, defaulting to 0% (cost == price). "
            "credentials keys={}".format(_LOG_PREFIX, list(creds.keys()))
        )
        return Decimal(0)
    return pct


def dealer_cost_from_retail(
    price: typing.Optional[Decimal],
    credentials: typing.Optional[typing.Dict],
) -> typing.Optional[Decimal]:
    """``cost = price * (1 - markup / 100)``. Returns None if price is missing or invalid."""
    if price is None:
        return None
    try:
        p = price if isinstance(price, Decimal) else Decimal(str(price).strip())
    except Exception:
        return None
    if p < 0:
        p = Decimal(0)
    pct = markup_percent_for_company(credentials)
    cost = p * (Decimal(1) - pct / Decimal(100))
    return cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def sync_thoroughbred_company_pricing_for_company_provider(company_provider_id: int) -> int:
    """
    Recomputes dealer_cost_from_retail for every ThoroughbredParts row against this company's
    own markup credential, and upserts ThoroughbredCompanyPricing. Propagation into
    ProviderPartCompanyPricing is a separate step -- see
    master_parts.sync_provider_pricing_from_thoroughbred_for_company.
    """
    cp = src_models.CompanyProviders.objects.filter(id=company_provider_id).select_related(
        "company", "provider"
    ).first()
    if not cp:
        logger.warning(
            "{} CompanyProviders id={} not found. Skipping pricing sync.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return 0

    logger.info(
        "{} Syncing Thoroughbred company pricing for company_id={}.".format(
            _LOG_PREFIX, cp.company_id
        )
    )

    markup_pct = markup_percent_for_company(cp.credentials)
    total_upserted = 0
    batch: typing.List[src_models.ThoroughbredCompanyPricing] = []
    last_id = 0

    while True:
        rows = list(
            src_models.ThoroughbredParts.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "price")[:CATALOG_UPSERT_BATCH]
        )
        if not rows:
            break
        last_id = rows[-1]["id"]

        for row in rows:
            cost = dealer_cost_from_retail(row.get("price"), cp.credentials)
            if cost is None:
                continue
            batch.append(
                src_models.ThoroughbredCompanyPricing(
                    part_id=row["id"],
                    company_id=cp.company_id,
                    cost=cost,
                    markup_percent_applied=markup_pct,
                )
            )

        if batch:
            by_key = {(r.part_id, r.company_id): r for r in batch}
            pgbulk.upsert(
                src_models.ThoroughbredCompanyPricing,
                list(by_key.values()),
                unique_fields=["part", "company"],
                update_fields=["cost", "markup_percent_applied", "updated_at"],
            )
            total_upserted += len(by_key)
            batch = []

    logger.info(
        "{} Synced {} Thoroughbred company pricing rows for company_id={}.".format(
            _LOG_PREFIX, total_upserted, cp.company_id
        )
    )
    return total_upserted
