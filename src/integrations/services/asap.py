"""
Sync ASAP Network (paid third-party catalog) brand, fitment, and enrichment data into MasterPart,
MasterPartData, and MasterPartFitment.

ASAP is not a per-company distributor - see CompanyProviders docstring; it never creates
ProviderPart rows, only enriches parts already ingested from another distributor. Because the API
is paid and billed per call, product sync is meant to run largely once per brand: a brand with
``last_synced_at`` set is skipped on future runs unless ``force=True``.
"""
import logging
import typing
from concurrent.futures import ThreadPoolExecutor, as_completed

from django.conf import settings
from django.db.models.functions import Upper
from django.utils import timezone

import pgbulk

from src import enums as src_enums
from src import models as src_models
from src.integrations.clients.asap.client import AsapApiClient
from src.integrations.services.master_parts import (
    _load_category_mapping_by_source,
    _lookup_categories_from_mapping,
)
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[ASAP-NETWORK]"

_MASTER_PART_DATA_FIELDS = (
    "images",
    "description",
    "color",
    "material",
    "series",
    "warranty",
    "vehicle_type",
    "field_specs",
    "youtube_video",
    "installation_instructions",
)


def _get_asap_provider() -> src_models.Providers:
    return src_models.Providers.objects.get(kind=src_enums.BrandProviderKind.ASAP_NETWORK.value)


def sync_asap_brands() -> typing.Dict[str, int]:
    """Fetch GET /brands, upsert AsapBrand, then resolve brand FK for any unmatched rows."""
    client = AsapApiClient()
    raw_brands = client.get_brands()  # {brand_id: {term_name, brand_id, name}}

    rows = [
        src_models.AsapBrand(
            external_id=str(b.get("brand_id") or key),
            term_name=b.get("term_name") or b.get("name") or "",
            name=b.get("name") or "",
        )
        for key, b in raw_brands.items()
    ]
    if rows:
        pgbulk.upsert(
            src_models.AsapBrand,
            rows,
            unique_fields=["external_id"],
            update_fields=["term_name", "name"],
        )

    matched = _match_unmapped_asap_brands()
    total = src_models.AsapBrand.objects.count()
    logger.info(
        "{} Synced {} ASAP brand(s) (total={}); matched {} to a canonical Brand this run.".format(
            _LOG_PREFIX, len(rows), total, matched
        )
    )
    return {"brands_synced": len(rows), "brands_matched": matched, "brands_total": total}


def _match_unmapped_asap_brands() -> int:
    """
    Resolve AsapBrand.brand for rows where it's still null: exact upper-name match -> compact-key
    match -> fuzzy word-prefix match (same cascade used by Keystone/A-Tech, via
    src.integrations.utils.brand_matching). Never auto-creates new canonical Brands - ASAP is
    enrichment-only, so an unmatched brand just stays unmatched and is skipped by product sync.
    """
    unmapped = list(src_models.AsapBrand.objects.filter(brand__isnull=True).order_by("id"))
    if not unmapped:
        return 0

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Exact upper-name match
    name_upper_keys = {normalize_upper_words(ab.name) for ab in unmapped if ab.name}
    brands_by_upper: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            ku = normalize_upper_words(b.name or "")
            if ku not in brands_by_upper:
                brands_by_upper[ku] = b
    for ab in unmapped:
        nm = normalize_upper_words(ab.name)
        if nm and nm in brands_by_upper:
            resolved_by_id[ab.id] = brands_by_upper[nm]

    # Compact-key match (punctuation/spacing-only differences)
    still_unresolved = [ab for ab in unmapped if ab.id not in resolved_by_id]
    if still_unresolved:
        compact_index = brands_by_compact_key()
        for ab in still_unresolved:
            key = normalize_compact_key(ab.name or "")
            brand = compact_index.get(key) if key else None
            if brand:
                resolved_by_id[ab.id] = brand

    # Fuzzy word-prefix match
    still_unresolved = [ab for ab in unmapped if ab.id not in resolved_by_id]
    if still_unresolved:
        first_token_index = brands_by_first_token_upper()
        all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
        for ab in still_unresolved:
            parts = normalize_upper_words(ab.name).split()
            candidates = first_token_index.get(parts[0], []) if parts else []
            if not candidates:
                if all_brands_fallback is None:
                    all_brands_fallback = list(src_models.Brands.objects.only("id", "name").order_by("id"))
                candidates = all_brands_fallback
            brand = best_fuzzy_brand_match(ab.name or "", candidates)
            if brand:
                resolved_by_id[ab.id] = brand

    to_update = []
    for ab in unmapped:
        brand = resolved_by_id.get(ab.id)
        if brand:
            ab.brand = brand
            to_update.append(ab)
    if to_update:
        src_models.AsapBrand.objects.bulk_update(to_update, ["brand"])

    unresolved_count = len(unmapped) - len(to_update)
    if unresolved_count:
        logger.info(
            "{} {} ASAP brand(s) still unmatched to a canonical Brand (left as-is; skipped by product sync).".format(
                _LOG_PREFIX, unresolved_count
            )
        )
    return len(to_update)


def sync_asap_products_for_brand(asap_brand: src_models.AsapBrand, force: bool = False) -> typing.Dict[str, typing.Any]:
    """
    Fetch every product for ``asap_brand`` and enrich matching MasterPart rows. Skips (no API
    calls) if the brand has no matched canonical Brand, or was already synced and ``force`` is
    False. Sets ``last_synced_at`` once the whole product list has been processed - individual
    fetch/processing failures are logged and skipped, not fatal.
    """
    if asap_brand.brand_id is None:
        logger.info(
            "{} Skipping ASAP brand {!r} (external_id={}): no matched canonical Brand.".format(
                _LOG_PREFIX, asap_brand.name, asap_brand.external_id
            )
        )
        return {"skipped": True, "processed": 0, "matched": 0, "unmatched": 0, "fetch_failed": 0, "fitments": 0}

    if asap_brand.last_synced_at is not None and not force:
        logger.info(
            "{} Skipping ASAP brand {!r}: already synced at {} (use --force to re-sync).".format(
                _LOG_PREFIX, asap_brand.name, asap_brand.last_synced_at
            )
        )
        return {"skipped": True, "processed": 0, "matched": 0, "unmatched": 0, "fetch_failed": 0, "fitments": 0}

    client = AsapApiClient()
    raw_products = client.get_products(asap_brand.external_id)  # {sku: {sku, title, changed}}
    skus = list(raw_products.keys())
    total = len(skus)

    provider = _get_asap_provider()
    category_mapping = _load_category_mapping_by_source()
    max_workers = max(1, int(getattr(settings, "ASAP_NETWORK_MAX_CONCURRENT_REQUESTS", 4) or 4))

    logger.info(
        "{} Brand {!r}: {} product(s) found via ASAP; starting fetch (max_workers={}).".format(
            _LOG_PREFIX, asap_brand.name, total, max_workers
        )
    )

    stats = {"processed": 0, "matched": 0, "unmatched": 0, "fetch_failed": 0, "fitments": 0}
    completed = 0
    progress_every = 25

    # Fetch product details concurrently (pure network I/O, independent per SKU); DB writes stay
    # on the main thread as each fetch completes, avoiding any concurrent-write races on
    # MasterPartData's OneToOneField without needing per-thread connection handling.
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(client.get_product_detail, sku): sku for sku in skus}
        for fut in as_completed(futures):
            sku = futures[fut]
            completed += 1
            try:
                product = fut.result()
            except Exception:
                logger.exception("{} Failed to fetch ASAP product sku={}.".format(_LOG_PREFIX, sku))
                stats["fetch_failed"] += 1
                if completed % progress_every == 0 or completed == total:
                    logger.info(
                        "{} Brand {!r}: {}/{} fetched (matched={} unmatched={} fetch_failed={}).".format(
                            _LOG_PREFIX, asap_brand.name, completed, total,
                            stats["matched"], stats["unmatched"], stats["fetch_failed"],
                        )
                    )
                continue

            stats["processed"] += 1
            try:
                matched = _process_asap_product(asap_brand, provider, product, category_mapping)
            except Exception:
                logger.exception("{} Failed to process ASAP product sku={}.".format(_LOG_PREFIX, sku))
                matched = None
            if matched:
                stats["matched"] += 1
                stats["fitments"] += len(product.get("fitment") or [])
            elif matched is not None:
                stats["unmatched"] += 1

            if completed % progress_every == 0 or completed == total:
                logger.info(
                    "{} Brand {!r}: {}/{} fetched (matched={} unmatched={} fetch_failed={}).".format(
                        _LOG_PREFIX, asap_brand.name, completed, total,
                        stats["matched"], stats["unmatched"], stats["fetch_failed"],
                    )
                )

    asap_brand.last_synced_at = timezone.now()
    asap_brand.save(update_fields=["last_synced_at", "updated_at"])

    logger.info(
        "{} Brand {!r} done: processed={} matched={} unmatched={} fetch_failed={} fitments={}.".format(
            _LOG_PREFIX,
            asap_brand.name,
            stats["processed"],
            stats["matched"],
            stats["unmatched"],
            stats["fetch_failed"],
            stats["fitments"],
        )
    )
    return stats


def _process_asap_product(
    asap_brand: src_models.AsapBrand,
    provider: src_models.Providers,
    product: typing.Dict,
    category_mapping: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> bool:
    """
    Resolve the MasterPart this ASAP product enriches and write to it. Returns False (no-op)
    when there's no ``mfg_original_sku`` or no matching MasterPart - ASAP only enriches parts
    that already exist from another distributor; it never creates new MasterPart rows.
    """
    mfg_sku = (product.get("mfg_original_sku") or "").strip()
    if not mfg_sku:
        logger.debug(
            "{} ASAP sku={} has no mfg_original_sku; skipping.".format(_LOG_PREFIX, product.get("sku"))
        )
        return False

    master_part = src_models.MasterPart.objects.filter(
        brand_id=asap_brand.brand_id, part_number=mfg_sku
    ).first()
    if master_part is None:
        logger.debug(
            "{} No MasterPart for brand_id={} part_number={!r} (ASAP sku={}); skipping.".format(
                _LOG_PREFIX, asap_brand.brand_id, mfg_sku, product.get("sku")
            )
        )
        return False

    _patch_master_part_core_fields(master_part, product, category_mapping)
    _upsert_master_part_data(master_part, provider, product)
    _upsert_master_part_fitments(master_part, provider, product)
    return True


def _patch_master_part_core_fields(
    master_part: src_models.MasterPart,
    product: typing.Dict,
    category_mapping: typing.Dict[str, typing.Tuple[typing.Optional[str], typing.Optional[str]]],
) -> None:
    """Fill-if-blank only - never overwrite a value another distributor already supplied."""
    update_fields = []

    if not master_part.description:
        desc = _first_or_value(product.get("product_description"))
        if desc:
            master_part.description = desc
            update_fields.append("description")

    if not master_part.image_url:
        images = product.get("image")
        if isinstance(images, list) and images:
            master_part.image_url = images[0]
            update_fields.append("image_url")

    if not master_part.gtin:
        upc = _extract_field_spec(product.get("field_specs"), "UPC")
        if upc:
            master_part.gtin = upc
            update_fields.append("gtin")

    if not master_part.category and not master_part.overview_category:
        raw_category = _first_or_value(product.get("category"))
        category, overview_category = _lookup_categories_from_mapping(raw_category, category_mapping)
        if category:
            master_part.category = category
            update_fields.append("category")
        if overview_category:
            master_part.overview_category = overview_category
            update_fields.append("overview_category")

    if update_fields:
        master_part.save(update_fields=update_fields + ["updated_at"])


def _upsert_master_part_data(
    master_part: src_models.MasterPart,
    provider: src_models.Providers,
    product: typing.Dict,
) -> None:
    """Per-field fill-if-blank so other sources can enrich other brands without clobbering."""
    data, _created = src_models.MasterPartData.objects.get_or_create(master_part=master_part)

    new_values = {
        "images": product.get("image"),
        "description": _first_or_value(product.get("product_description")),
        "color": product.get("color"),
        "material": product.get("material"),
        "series": product.get("series"),
        "warranty": product.get("warranty"),
        "vehicle_type": product.get("vehicle_type"),
        "field_specs": product.get("field_specs"),
        "youtube_video": product.get("youtube_video"),
        "installation_instructions": product.get("installation_instructions"),
    }

    update_fields = []
    for field in _MASTER_PART_DATA_FIELDS:
        new_value = new_values.get(field)
        if _is_blank(new_value) or not _is_blank(getattr(data, field)):
            continue
        setattr(data, field, new_value)
        update_fields.append(field)

    if not data.source_provider_id:
        data.source_provider = provider
        data.source_external_id = product.get("sku")
        update_fields.extend(["source_provider", "source_external_id"])

    if update_fields:
        data.save(update_fields=update_fields + ["updated_at"])


def _upsert_master_part_fitments(
    master_part: src_models.MasterPart,
    provider: src_models.Providers,
    product: typing.Dict,
) -> None:
    fitments = product.get("fitment") or []
    if not fitments:
        return

    rows = []
    for f in fitments:
        try:
            year_start = int(f.get("from_year"))
            year_end = int(f.get("to_year"))
        except (TypeError, ValueError):
            logger.warning(
                "{} Skipping fitment row with non-numeric year(s) for sku={}: {!r}".format(
                    _LOG_PREFIX, product.get("sku"), f
                )
            )
            continue
        make = (f.get("make") or "").strip()
        model = (f.get("model") or "").strip()
        if not make or not model:
            continue
        rows.append(
            src_models.MasterPartFitment(
                master_part=master_part,
                year_start=year_start,
                year_end=year_end,
                make=make,
                model=model,
                submodel=(f.get("sub") or "").strip(),
                source_provider=provider,
            )
        )

    if rows:
        pgbulk.upsert(
            src_models.MasterPartFitment,
            rows,
            unique_fields=["master_part", "year_start", "year_end", "make", "model", "submodel", "engine", "drive_type"],
            update_fields=["source_provider"],
        )


def _first_or_value(value: typing.Any) -> typing.Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def _extract_field_spec(field_specs: typing.Any, spec_name: str) -> typing.Optional[str]:
    if not isinstance(field_specs, list):
        return None
    for spec in field_specs:
        if isinstance(spec, dict) and spec.get("spec_name") == spec_name:
            return spec.get("spec_value")
    return None


def _is_blank(value: typing.Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False
