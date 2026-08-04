"""
Vossen feed integration: single AfterMarket.aspx CSV feed (SKU, Description, Available, Price,
Diameter, Width, Offset, BoltPattern, Centerbore). Vossen is a single-brand distributor -- every
row belongs to the "VOSSEN" brand -- so unlike Rough Country there is no fuzzy brand matching:
just one VossenBrand row, mapped once to the existing global "VOSSEN" Brands row.

Catalog + stock (VossenPart) sync from the primary/catalog CompanyProvider; per-company pricing
(VossenCompanyPricing) loads from each company's own feed_url, same split as Rough Country.
"""
import logging
import typing
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

import pgbulk
from django.db import connection
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.vossen import client as vossen_client
from src.integrations.clients.vossen import exceptions as vossen_exceptions

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[VOSSEN-SERVICES]"

# VossenBrand.external_id / name -- always this single value (Vossen is brand == distributor).
VOSSEN_BRAND_EXTERNAL_ID = "VOSSEN"

# When discount_percent is missing/invalid, treat as 0% off (cost = full feed price) -- unlike
# WheelPros' 20% default, there's no established business assumption for Vossen's typical dealer
# discount, so the safe default is "no discount applied" rather than guessing one.
VOSSEN_DEFAULT_DISCOUNT_PERCENT = Decimal(0)


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return int(float(s))
    except (TypeError, ValueError):
        return None


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    s = str(value or "").strip()
    if not s:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def _safe_str(value: typing.Any) -> typing.Optional[str]:
    s = str(value or "").strip()
    return s or None


def _parse_discount_percent(raw: typing.Any) -> typing.Optional[Decimal]:
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


def discount_percent_from_credentials(credentials: typing.Optional[typing.Dict]) -> Decimal:
    """
    Discount percent *off* the feed's Price column (0-100), from CompanyProviders.credentials
    (see src.constants.VOSSEN_CREDENTIALS_DISCOUNT_PERCENT). Missing or unparseable value ->
    VOSSEN_DEFAULT_DISCOUNT_PERCENT (0%, i.e. no discount) -- connect/update validates this field
    as required (see _validate_vossen_discount_percent in src.api.services.integrations), so this
    fallback is only a defensive default for callers that bypass that validation.
    """
    creds = credentials or {}
    pct = _parse_discount_percent(creds.get(src_constants.VOSSEN_CREDENTIALS_DISCOUNT_PERCENT))
    if pct is None:
        return VOSSEN_DEFAULT_DISCOUNT_PERCENT
    return pct


def dealer_cost_from_price(
    price: typing.Any,
    credentials: typing.Optional[typing.Dict],
) -> typing.Optional[Decimal]:
    """
    ``cost = price * (1 - discount_percent / 100)`` using this company's discount_percent
    credential. Returns None if ``price`` is missing or invalid. Mirrors
    src.integrations.services.wheelpros.dealer_cost_from_msrp's shape.
    """
    if price is None:
        return None
    try:
        p = price if isinstance(price, Decimal) else Decimal(str(price).strip())
    except Exception:
        return None
    if p < 0:
        p = Decimal(0)
    pct = discount_percent_from_credentials(credentials)
    q = p * (Decimal(1) - pct / Decimal(100))
    return q.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _vossen_feed_client_for_credentials(
    credentials: typing.Optional[typing.Dict],
    file_url_override: typing.Optional[str],
    local_file_path_override: typing.Optional[str],
) -> vossen_client.VossenFeedClient:
    """Build client from CompanyProviders.credentials (feed_url) plus optional CLI overrides."""
    creds = credentials or {}
    url = file_url_override
    if url is None:
        raw_url = creds.get("feed_url")
        url = raw_url.strip() if isinstance(raw_url, str) and raw_url.strip() else None
    else:
        url = url.strip() if url.strip() else None
    return vossen_client.VossenFeedClient(file_url=url, local_file_path=local_file_path_override)


def _active_vossen_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.VOSSEN.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def _catalog_company_provider(
    vossen_provider: typing.Optional[src_models.Providers],
) -> typing.Optional[src_models.CompanyProviders]:
    """Primary Vossen connection for the shared catalog; else first active by id."""
    if not vossen_provider:
        return None
    base = _active_vossen_company_providers_queryset().filter(provider=vossen_provider)
    primary = base.filter(primary=True).first()
    if primary:
        return primary
    fallback = base.order_by("id").first()
    if fallback:
        logger.info(
            "{} No primary Vossen company provider; using company_id={} for catalog.".format(
                _LOG_PREFIX, fallback.company_id,
            )
        )
    return fallback


def _row_to_vossen_part(row: typing.Dict, brand: src_models.VossenBrand) -> src_models.VossenPart:
    return src_models.VossenPart(
        brand=brand,
        sku=_safe_str(row.get("SKU")) or "",
        description=_safe_str(row.get("Description")),
        available=_safe_int(row.get("Available")),
        diameter=_safe_str(row.get("Diameter")),
        width=_safe_str(row.get("Width")),
        offset=_safe_str(row.get("Offset")),
        bolt_pattern=_safe_str(row.get("BoltPattern")),
        center_bore=_safe_str(row.get("Centerbore")),
        raw_data=row,
    )


def ensure_vossen_brand_and_mapping() -> typing.Optional[src_models.VossenBrand]:
    """
    Ensure the single VossenBrand row exists, mapped to the existing global "VOSSEN" Brands row
    (already present -- Turn 14 carries this brand too) via BrandVossenBrandMapping, and that
    BrandProviders links it to the Vossen provider. No fuzzy matching needed: there is exactly
    one brand this feed will ever produce.
    """
    vossen_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.VOSSEN.value,
    ).first()
    if not vossen_provider:
        logger.warning("{} Vossen provider not found. Skipping brand sync.".format(_LOG_PREFIX))
        return None

    vossen_brand, _ = src_models.VossenBrand.objects.get_or_create(
        external_id=VOSSEN_BRAND_EXTERNAL_ID,
        defaults={"name": VOSSEN_BRAND_EXTERNAL_ID},
    )
    brand, brand_created = src_models.Brands.objects.get_or_create(
        name=VOSSEN_BRAND_EXTERNAL_ID,
        defaults={
            "status": src_enums.BrandProviderStatus.ACTIVE.value,
            "status_name": src_enums.BrandProviderStatus.ACTIVE.name,
        },
    )
    if brand_created:
        logger.info("{} Created new Brands row for VOSSEN (none existed).".format(_LOG_PREFIX))

    src_models.BrandVossenBrandMapping.objects.get_or_create(brand=brand, vossen_brand=vossen_brand)
    _, bp_created = src_models.BrandProviders.objects.get_or_create(brand=brand, provider=vossen_provider)
    if bp_created:
        logger.info(
            "{} Linked Brands id={} to Vossen provider via BrandProviders.".format(_LOG_PREFIX, brand.id)
        )
    return vossen_brand


def fetch_and_save_vossen(
    file_url: typing.Optional[str] = None,
    local_file_path: typing.Optional[str] = None,
    download: bool = True,
) -> None:
    """
    Fetch the catalog feed (primary Vossen CompanyProvider's credentials, or first active),
    upsert the single VossenBrand and all VossenPart rows (catalog + manufacturer-wide stock).
    Per-company pricing (VossenCompanyPricing) is handled separately by IntegrationPricingSyncJob.
    """
    logger.info("{} Starting Vossen feed sync.".format(_LOG_PREFIX))

    vossen_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.VOSSEN.value,
    ).first()

    catalog_cp = _catalog_company_provider(vossen_provider)
    catalog_creds = credentials_helper.get_feed_credentials(catalog_cp) if catalog_cp else {}
    if catalog_cp:
        logger.info(
            "{} Catalog feed using company_id={} (primary={}).".format(
                _LOG_PREFIX, catalog_cp.company_id, catalog_cp.primary,
            )
        )
    client = _vossen_feed_client_for_credentials(catalog_creds, file_url, local_file_path)
    try:
        rows = client.get_feed_data(download_if_missing=download)
    except vossen_exceptions.VossenException as e:
        logger.error("{} Feed error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    if not rows:
        logger.warning("{} No rows in Vossen feed.".format(_LOG_PREFIX))
        return

    brand, _ = src_models.VossenBrand.objects.get_or_create(
        external_id=VOSSEN_BRAND_EXTERNAL_ID,
        defaults={"name": VOSSEN_BRAND_EXTERNAL_ID},
    )

    # Dedupe by sku (last row wins) in case the feed ever repeats a sku.
    seen_by_sku: typing.Dict[str, src_models.VossenPart] = {}
    for row in rows:
        sku = _safe_str(row.get("SKU"))
        if not sku:
            continue
        seen_by_sku[sku] = _row_to_vossen_part(row, brand)
    part_instances = list(seen_by_sku.values())
    if len(rows) > len(part_instances):
        logger.info(
            "{} Deduped feed rows from {} to {} by sku.".format(_LOG_PREFIX, len(rows), len(part_instances))
        )

    if not part_instances:
        logger.warning("{} No part instances parsed from feed.".format(_LOG_PREFIX))
        return

    try:
        now = timezone.now()
        for p in part_instances:
            p.updated_at = now
        pgbulk.upsert(
            src_models.VossenPart,
            part_instances,
            unique_fields=["brand", "sku"],
            update_fields=[
                "description",
                "available",
                "diameter",
                "width",
                "offset",
                "bolt_pattern",
                "center_bore",
                "raw_data",
                "updated_at",
            ],
            returning=False,
        )
        logger.info("{} Upserted {} Vossen parts.".format(_LOG_PREFIX, len(part_instances)))
    except Exception as e:
        logger.error("{} Error upserting parts: {}.".format(_LOG_PREFIX, str(e)))
        raise

    logger.info("{} Vossen feed sync complete.".format(_LOG_PREFIX))


def sync_vossen_company_pricing_for_company_provider(
    company_provider_id: int,
    file_url: typing.Optional[str] = None,
    local_file_path: typing.Optional[str] = None,
    download: bool = True,
) -> None:
    """
    Fetch this company's Vossen feed and upsert VossenCompanyPricing only.
    VossenPart rows must already exist (sku) for pricing rows to apply.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.VOSSEN.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company")
        .first()
    )
    if not cp:
        logger.warning(
            "{} No active Vossen CompanyProviders id={}. Skipping.".format(_LOG_PREFIX, company_provider_id)
        )
        return

    vossen_brand = src_models.VossenBrand.objects.filter(external_id=VOSSEN_BRAND_EXTERNAL_ID).first()
    if not vossen_brand:
        logger.warning(
            "{} No VossenBrand yet (catalog sync hasn't run?) company_provider id={}.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    client = _vossen_feed_client_for_credentials(
        credentials_helper.get_feed_credentials(cp), file_url, local_file_path
    )
    try:
        rows = client.get_feed_data(download_if_missing=download)
    except vossen_exceptions.VossenException as e:
        logger.error(
            "{} Pricing feed error company_provider id={}: {}.".format(_LOG_PREFIX, company_provider_id, str(e))
        )
        raise

    if not rows:
        logger.warning("{} No rows for company_provider id={}.".format(_LOG_PREFIX, company_provider_id))
        return

    price_by_sku: typing.Dict[str, typing.Optional[Decimal]] = {}
    for row in rows:
        sku = _safe_str(row.get("SKU"))
        if not sku:
            continue
        price_by_sku[sku] = _safe_decimal(row.get("Price"))

    part_id_by_sku = {
        p["sku"]: p["id"]
        for p in src_models.VossenPart.objects.filter(
            brand=vossen_brand, sku__in=list(price_by_sku.keys())
        ).values("id", "sku")
    }

    pricing_to_upsert = [
        src_models.VossenCompanyPricing(part_id=part_id_by_sku[sku], company=cp.company, price=price)
        for sku, price in price_by_sku.items()
        if sku in part_id_by_sku
    ]
    if not pricing_to_upsert:
        logger.warning(
            "{} No matching VossenPart rows for company_provider id={}.".format(_LOG_PREFIX, company_provider_id)
        )
        return

    # Hand-written upsert (not pgbulk) so unchanged rows are skipped in the same statement via
    # IS DISTINCT FROM -- see Meyer's _flush_buf for the full rationale.
    now = timezone.now()
    total_written = 0
    batch_size = 5000
    for i in range(0, len(pricing_to_upsert), batch_size):
        batch = pricing_to_upsert[i : i + batch_size]
        rows = [(p.part_id, p.company_id, p.price, now, now) for p in batch]
        placeholders = ", ".join(["(%s, %s, %s, %s, %s)"] * len(rows))
        params = [v for row in rows for v in row]
        with connection.cursor() as cur:
            cur.execute(
                """
                INSERT INTO vossen_company_pricing (part_id, company_id, price, created_at, updated_at)
                VALUES {}
                ON CONFLICT (part_id, company_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    updated_at = EXCLUDED.updated_at
                WHERE vossen_company_pricing.price IS DISTINCT FROM EXCLUDED.price
                """.format(placeholders),
                params,
            )
            total_written += cur.rowcount
        connection.close()
    logger.info(
        "{} VossenCompanyPricing considered {} rows, actually wrote {} (unchanged skipped) "
        "for company_provider id={}.".format(
            _LOG_PREFIX, len(pricing_to_upsert), total_written, company_provider_id,
        )
    )
