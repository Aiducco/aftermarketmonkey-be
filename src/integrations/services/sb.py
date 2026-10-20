"""
S&B Filters feed integration: two shared dealer workbooks in, SbBrand / SbPart / SbFitment /
SbCompanyPricing out.

What makes S&B different from every other distributor here is that **there is no per-dealer feed**.
S&B maintain two Google Sheets for their whole dealer network (see ``src.constants``' S&B block and
``src.integrations.clients.sb``), so one ingest serves every connected company and the only
per-company input is which dealer price level that company is on. Consequences worth knowing
before editing:

  * ``fetch_and_save_sb`` needs no CompanyProviders row at all -- unlike Rough Country or
    Quadratec, which read the primary connection's own feed URL. A company connecting to S&B gets
    a complete catalog the moment the nightly ingest has run once, with nothing to wait for.
  * ``sync_sb_company_pricing_for_company_provider`` re-reads the same shared workbook per
    company, but that is cheap: the client caches both files on disk for six hours, so a run of
    pricing jobs in one window hits Google once.
  * Stock is the one thing S&B do not publish. ``fetch_and_save_sb_inventory`` reads a file the
    dealer emailed for and forwarded to support, which support drops on that company's relay
    folder -- see ``src.integrations.clients.sb.relay``. Its format is unconfirmed, so the parser
    resolves columns by header name and says so loudly when it cannot.

The catalog tab is authoritative for **which SKUs exist**. The Shopify export carries ~1,400 SKUs
against the catalog's ~830 -- storefront products with no dealer price -- and those extras are
deliberately not ingested: a part a dealer cannot buy at a known cost has no business in the
catalog. Shopify only ever enriches a SKU the pricing master already lists.
"""
import csv
import json
import logging
import os
import re
import typing
from decimal import Decimal, InvalidOperation

import openpyxl
import pgbulk
from django.db import connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.sb import client as sb_client
from src.integrations.clients.sb import exceptions as sb_exceptions
from src.integrations.clients.sb import feed_spec as sb_feed_spec
from src.integrations.clients.sb import relay as sb_relay
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[SB-SERVICES]"

#: Fallback SbBrand when a catalog row has no brand at all. S&B's own name, since an unlabelled
#: row in S&B's sheet is an S&B part.
DEFAULT_SB_BRAND_NAME = "S&B"

SB_PARTS_UPSERT_BATCH = 2000
SB_FITMENT_UPSERT_BATCH = 5000
SB_PRICING_UPSERT_BATCH = 2000

#: ``SbFitment.source`` values, in precedence order: the Fitment tab is S&B's curated list,
#: Shopify's metafield is what their own storefront shows, and the miva dump is a legacy export
#: kept for its reach rather than its freshness. A vehicle present in more than one is stored once
#: and attributed to the first of these that had it.
FITMENT_SOURCE_FITMENT_TAB = "fitment_tab"
FITMENT_SOURCE_SHOPIFY = "shopify"
FITMENT_SOURCE_MIVA = "miva"
FITMENT_SOURCE_PRECEDENCE = (
    FITMENT_SOURCE_FITMENT_TAB,
    FITMENT_SOURCE_SHOPIFY,
    FITMENT_SOURCE_MIVA,
)

#: S&B write "-" (and occasionally "N/A") in a cell that doesn't apply to a product. Treated as
#: empty so a filter_type of "-" never reads as a real filter media.
_EMPTY_SENTINELS = frozenset({"", "-", "--", "n/a", "na", "none", "null", "tbd"})

SB_PART_UPDATE_FIELDS = [
    "upc",
    "title",
    "description",
    "category",
    "filter_type",
    "vehicle_type",
    "shopify_product_type",
    "shopify_product_id",
    "us_retail_price",
    "canadian_retail_price",
    "level_1_price",
    "level_2_price",
    "weight",
    "shipping_width",
    "shipping_height",
    "shipping_length",
    "prop_65_warning",
    "warning_label_letter",
    "emissions_code",
    "carb_exempt_id",
    "fifty_state_street_legal",
    "tune_required",
    "iso5011_test_results",
    "install_video_url",
    "installation_instructions_url",
    "quantity_of_oil",
    "quantity_of_oil_power_stack",
    "image_1",
    "image_2",
    "image_3",
    "image_4",
    "image_5",
    "image_6",
    "images",
    "video",
    "raw_data",
    "updated_at",
]


# ======================================================================================
# Value coercion
# ======================================================================================
def _safe_str(value: typing.Any, max_len: typing.Optional[int] = None) -> typing.Optional[str]:
    """Stripped string, or None for blanks and S&B's not-applicable sentinels."""
    if value is None:
        return None
    text = str(value).strip()
    if text.lower() in _EMPTY_SENTINELS:
        return None
    return text[:max_len] if max_len else text


def _safe_decimal(value: typing.Any) -> typing.Optional[Decimal]:
    text = _safe_str(value)
    if text is None:
        return None
    text = text.replace("$", "").replace(",", "").strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _safe_int(value: typing.Any) -> typing.Optional[int]:
    text = _safe_str(value)
    if text is None:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _safe_code(value: typing.Any, max_len: typing.Optional[int] = None) -> typing.Optional[str]:
    """
    An identifier-ish cell, minus the damage Sheets does to numeric-looking ones.

    Anything entirely numeric comes back from the workbook as a float, so S&B's filter-wrap SKU
    ``101010`` arrives as ``101010.0`` and emissions code ``1`` as ``1.0``. Only the exact
    ``<digits>.0`` shape is trimmed -- a value whose real last characters are ".0" would be
    indistinguishable, and none exist in either workbook.
    """
    text = _safe_str(value, max_len)
    if text is None:
        return None
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _safe_sku(value: typing.Any) -> typing.Optional[str]:
    """SKU as S&B wrote it; see ``_safe_code`` for why the float trim is needed."""
    return _safe_code(value, 255)


def _safe_upc(value: typing.Any) -> typing.Optional[str]:
    """
    UPC as digits. Same float problem as SKUs but worse: ``841372040375.0`` would be stored
    verbatim and never match a GTIN. Non-numeric UPCs are kept as-is -- some rows carry text.
    """
    text = _safe_code(value, 255)
    if text is None:
        return None
    if "e+" in text.lower():
        # Sheets occasionally exports a long UPC in scientific notation.
        try:
            text = "{:.0f}".format(float(text))
        except (TypeError, ValueError):
            pass
    return text[:255]


def _normalize_brand_key(value: typing.Any) -> str:
    """Feed brand/vendor label -> SbBrand.external_id / name (uppercase)."""
    text = _safe_str(value)
    return text.upper() if text else DEFAULT_SB_BRAND_NAME


def _brand_name_upper_for_sync(sb_brand: src_models.SbBrand) -> str:
    name_upper = (sb_brand.name or "").strip().upper()
    return name_upper or "BRAND_{}".format(sb_brand.external_id)


# ======================================================================================
# Fitment parsing (pure functions -- see src/integrations/tests/test_sb_feed.py)
# ======================================================================================
_YEAR_RANGE_RE = re.compile(r"^(\d{4})\s*(?:-|–|—|to)\s*(\d{4})$")
_YEAR_SINGLE_RE = re.compile(r"^(\d{4})$")

#: Sanity bounds on a parsed year. A four-digit number outside these is a typo or a stray value in
#: the wrong column, and inventing fitment for model year 1203 is worse than dropping the row.
_MIN_MODEL_YEAR = 1900
_MAX_MODEL_YEAR = 2100


class SbFitmentSpec(typing.NamedTuple):
    """One parsed vehicle: the exact tuple SbFitment and MasterPartFitment are keyed on."""

    start_year: int
    end_year: int
    make: str
    model: str
    engine: str


def parse_year_ranges(value: typing.Any) -> typing.List[typing.Tuple[int, int]]:
    """
    S&B's Year column into ``[(start, end), ...]``.

    Every shape that actually appears in the workbooks:
    ``2015`` (as the float ``2015.0``), ``2011 - 2016``, ``2015-2026`` (Shopify's spelling),
    ``2004, 2006`` and ``1990 - 1991, 1993 - 1996``.

    Comma-separated segments stay separate rather than collapsing to one span, because the gaps are
    real -- ``1990 - 1991, 1993 - 1996`` means 1992 is not covered, and flattening it to 1990-1996
    would invent fitment for a vehicle S&B never claimed. Returns ``[]`` for anything
    unparseable; the caller drops the row rather than guessing.
    """
    text = _safe_str(value)
    if text is None:
        return []
    if text.endswith(".0") and text[:-2].isdigit():
        text = text[:-2]

    out: typing.List[typing.Tuple[int, int]] = []
    for segment in text.split(","):
        segment = segment.strip()
        if not segment:
            continue
        match = _YEAR_RANGE_RE.match(segment)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
        else:
            match = _YEAR_SINGLE_RE.match(segment)
            if not match:
                return []
            start = end = int(match.group(1))
        if start > end:
            start, end = end, start
        if not (_MIN_MODEL_YEAR <= start <= _MAX_MODEL_YEAR and _MIN_MODEL_YEAR <= end <= _MAX_MODEL_YEAR):
            return []
        out.append((start, end))
    return out


def _fitment_specs(
    year_value: typing.Any,
    make: typing.Any,
    model: typing.Any,
    engine: typing.Any,
) -> typing.List[SbFitmentSpec]:
    """
    Build specs for one vehicle line, one per year range. Make and model are mandatory -- a row
    without them describes no vehicle, and both workbooks carry a few hundred such filler rows.
    """
    make_text = _safe_str(make, 128)
    model_text = _safe_str(model, 128)
    if not make_text or not model_text:
        return []
    engine_text = _safe_str(engine, 255) or ""
    return [
        SbFitmentSpec(
            start_year=start,
            end_year=end,
            make=make_text,
            model=model_text,
            engine=engine_text,
        )
        for start, end in parse_year_ranges(year_value)
    ]


def parse_fitment_tab_row(row: typing.Mapping[str, typing.Any]) -> typing.List[SbFitmentSpec]:
    """``Fitment`` tab row -> specs. Columns are already named by the client's feed_spec."""
    return _fitment_specs(row.get("year"), row.get("make"), row.get("model"), row.get("engine"))


def parse_pipe_fitment(value: typing.Any) -> typing.List[SbFitmentSpec]:
    """
    The Shopify ``convermax.fitment`` metafield -> specs.

    Two spellings, both live: the current tab stores newline-separated
    ``2024-2026|Ford|Mustang|5.0L, Gas`` and the older tab a JSON array of the same strings. Both
    are handled here rather than in two places, since which tab a SKU came from is not something
    the caller should have to care about.
    """
    text = _safe_str(value)
    if text is None:
        return []

    entries: typing.List[str]
    if text.startswith("["):
        try:
            decoded = json.loads(text)
        except ValueError:
            decoded = None
        if isinstance(decoded, list):
            entries = [str(item) for item in decoded if item is not None]
        else:
            entries = re.split(r"[\r\n]+", text)
    else:
        entries = re.split(r"[\r\n]+", text)

    out: typing.List[SbFitmentSpec] = []
    for entry in entries:
        entry = entry.strip().strip('"')
        if not entry:
            continue
        fields = [part.strip() for part in entry.split("|")]
        if len(fields) < 3:
            continue
        year, make, model = fields[0], fields[1], fields[2]
        engine = fields[3] if len(fields) > 3 else None
        out.extend(_fitment_specs(year, make, model, engine))
    return out


def parse_miva_fitment(value: typing.Any) -> typing.List[SbFitmentSpec]:
    """
    The legacy ``miva export`` tab's ``YEAR_MAKE_MODEL_ENGINE`` column -> specs.

    One cell holds every vehicle for the SKU, pipe-joined, each ``>``-delimited and single-year:
    ``2022>Toyota>Tundra>3.5L, Gas|2021>Ford>Bronco>2.3L Ecoboost, Gas``. Consecutive years are
    left as separate rows rather than collapsed into a range -- MasterPartFitment stores ranges,
    but collapsing here would have to assume the gaps are complete, and this is the one source
    whose freshness we don't trust.
    """
    text = _safe_str(value)
    if text is None:
        return []

    out: typing.List[SbFitmentSpec] = []
    for entry in text.split("|"):
        entry = entry.strip()
        if not entry:
            continue
        fields = [part.strip() for part in entry.split(">")]
        if len(fields) < 3:
            continue
        year, make, model = fields[0], fields[1], fields[2]
        engine = fields[3] if len(fields) > 3 else None
        out.extend(_fitment_specs(year, make, model, engine))
    return out


# ======================================================================================
# Shopify export shaping (pure)
# ======================================================================================
class SbShopifyProduct(typing.NamedTuple):
    """Product-level fields from the Shopify export, ready to enrich every SKU it covers."""

    product_id: typing.Optional[str]
    title: typing.Optional[str]
    body_html: typing.Optional[str]
    product_type: typing.Optional[str]
    vendor: typing.Optional[str]
    images: typing.List[str]
    video: typing.Optional[str]
    fitment: typing.List[SbFitmentSpec]


def group_shopify_products(
    rows: typing.Sequence[typing.Mapping[str, typing.Any]],
) -> typing.Dict[str, SbShopifyProduct]:
    """
    Shopify's positional export -> ``{sku: SbShopifyProduct}``.

    The format repeats a product across consecutive rows: its first row carries the product-level
    fields and the rest carry only extra image URLs. A product can also have several variant rows,
    each with its own SKU, and the product-level description, imagery and fitment apply to all of
    them -- which is why this maps every SKU of a product to the same record rather than keying on
    the product.

    Rows arrive in tab-priority order (see ``feed_spec.SHOPIFY_TAB_PRIORITY``) and the first record
    seen for a SKU wins, so the current export beats the older leftover tab.
    """
    by_product: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    product_order: typing.List[str] = []

    for row in rows:
        product_id = _safe_str(row.get("product_id"))
        if not product_id:
            continue
        # Keyed by tab too: the same Shopify product id appears in both tabs, and merging the two
        # exports' image lists would mix a current product with its own older revision.
        key = "{}::{}".format(_safe_str(row.get("tab")) or "", product_id)
        entry = by_product.get(key)
        if entry is None:
            entry = {
                "product_id": product_id,
                "skus": [],
                "images": [],
                "video": None,
                "title": None,
                "body_html": None,
                "product_type": None,
                "vendor": None,
                "fitment_raw": None,
            }
            by_product[key] = entry
            product_order.append(key)

        sku = _safe_sku(row.get("sku"))
        if sku and sku not in entry["skus"]:
            entry["skus"].append(sku)

        for field in ("title", "body_html", "product_type", "vendor"):
            if entry[field] is None:
                entry[field] = _safe_str(row.get(field))
        if entry["fitment_raw"] is None:
            entry["fitment_raw"] = _safe_str(row.get("fitment"))

        image_src = _safe_str(row.get("image_src"))
        if image_src:
            image_type = (_safe_str(row.get("image_type")) or "").lower()
            if image_type == sb_feed_spec.SHOPIFY_IMAGE_TYPE_VIDEO:
                if entry["video"] is None:
                    entry["video"] = image_src
            elif image_src not in entry["images"]:
                entry["images"].append(image_src)

    out: typing.Dict[str, SbShopifyProduct] = {}
    for key in product_order:
        entry = by_product[key]
        if not entry["skus"]:
            continue
        product = SbShopifyProduct(
            product_id=entry["product_id"],
            title=_safe_str(entry["title"], 512),
            body_html=entry["body_html"],
            product_type=_safe_str(entry["product_type"], 255),
            vendor=entry["vendor"],
            images=list(entry["images"]),
            video=entry["video"],
            fitment=parse_pipe_fitment(entry["fitment_raw"]),
        )
        for sku in entry["skus"]:
            out.setdefault(sku, product)
    return out


# ======================================================================================
# Client / connection helpers
# ======================================================================================
def _sb_sheet_client(
    pricing_master_url: typing.Optional[str] = None,
    shopify_export_url: typing.Optional[str] = None,
    pricing_master_path: typing.Optional[str] = None,
    shopify_export_path: typing.Optional[str] = None,
) -> sb_client.SbSheetClient:
    """
    Build a sheet client. Takes no credentials on purpose: S&B's workbooks are the same files for
    every dealer, so there is nothing per-connection to read (see the module docstring). The
    overrides are for the management command and tests.
    """
    return sb_client.SbSheetClient(
        pricing_master_url=pricing_master_url,
        shopify_export_url=shopify_export_url,
        pricing_master_path=pricing_master_path,
        shopify_export_path=shopify_export_path,
    )


def _active_sb_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.SB.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
    ).select_related("company", "provider")


def sb_master_company() -> typing.Optional[src_models.Company]:
    """
    The company S&B's brand graph (BrandProviders / CompanyBrands) hangs off.

    Unlike every other distributor here, this is **not** the internal ``TICK_PERFORMANCE`` catalog
    company -- S&B maps onto the live Trident Motorsports tenant instead. Looked up by slug rather
    than name on purpose; ``src.constants.SB_MASTER_COMPANY_SLUG`` explains why that distinction is
    load-bearing.
    """
    return src_models.Company.objects.filter(
        slug=src_constants.SB_MASTER_COMPANY_SLUG
    ).first()


def sb_pricing_level_for_credentials(credentials: typing.Optional[typing.Dict]) -> int:
    """
    Dealer price level from a connection's feed credentials, defaulting to Level 1.

    Level 1 is the deliberate default for anything missing or unrecognized: it is the *higher*
    cost of the two, so a misconfigured connection understates margin rather than overstating it.
    """
    raw = (credentials or {}).get(src_constants.SB_CREDENTIALS_PRICING_LEVEL)
    text = str(raw or "").strip()
    if not text:
        return src_constants.SB_DEFAULT_PRICING_LEVEL
    # Accept "2", 2, "Level 2" and "level2" -- all of which a support user might type.
    digits = re.sub(r"\D", "", text)
    try:
        level = int(digits)
    except ValueError:
        return src_constants.SB_DEFAULT_PRICING_LEVEL
    if level not in src_constants.SB_PRICING_LEVELS:
        logger.warning(
            "{} Unrecognized {}={!r}; using level {}.".format(
                _LOG_PREFIX,
                src_constants.SB_CREDENTIALS_PRICING_LEVEL,
                raw,
                src_constants.SB_DEFAULT_PRICING_LEVEL,
            )
        )
        return src_constants.SB_DEFAULT_PRICING_LEVEL
    return level


def sb_price_region_for_credentials(credentials: typing.Optional[typing.Dict]) -> str:
    """Retail-price region ("US"/"CA") from feed credentials, defaulting to US."""
    raw = (credentials or {}).get(src_constants.SB_CREDENTIALS_PRICE_REGION)
    text = str(raw or "").strip().upper()
    if text in src_constants.SB_PRICE_REGIONS:
        return text
    if text and text not in ("", "USA", "UNITED STATES"):
        if text in ("CAN", "CANADA"):
            return src_constants.SB_PRICE_REGION_CA
        logger.warning(
            "{} Unrecognized {}={!r}; using {}.".format(
                _LOG_PREFIX,
                src_constants.SB_CREDENTIALS_PRICE_REGION,
                raw,
                src_constants.SB_DEFAULT_PRICE_REGION,
            )
        )
    return src_constants.SB_DEFAULT_PRICE_REGION


# ======================================================================================
# Brands
# ======================================================================================
def _ensure_sb_brands(
    brand_keys: typing.Iterable[str],
) -> typing.Dict[str, src_models.SbBrand]:
    """
    Ensure an SbBrand exists per distinct feed brand label (external_id == name, uppercase).
    S&B only ship three (S&B, JLT, J&L), so this is a get_or_create loop rather than a bulk pass.
    """
    keys = {key for key in brand_keys if key}
    keys.add(DEFAULT_SB_BRAND_NAME)

    out: typing.Dict[str, src_models.SbBrand] = {}
    for key in sorted(keys):
        brand, created = src_models.SbBrand.objects.get_or_create(
            external_id=key,
            defaults={"name": key, "aaia_code": None},
        )
        if not created and (brand.name or "").strip().upper() != key:
            brand.name = key
            brand.save(update_fields=["name", "updated_at"])
        if created:
            logger.info("{} Created SbBrand external_id={!r}.".format(_LOG_PREFIX, key))
        out[key] = brand
    logger.info("{} Using {} S&B brands.".format(_LOG_PREFIX, len(out)))
    return out


def sync_unmapped_sb_brands_to_brands() -> typing.List[src_models.SbBrand]:
    """
    For each SbBrand without a BrandSbBrandMapping: resolve a Brand by exact name (uppercase),
    then compact-key, then fuzzy word-prefix match -- the same four-phase matcher every other
    provider uses -- otherwise create it. Upserts BrandSbBrandMapping, BrandProviders, and
    CompanyBrands for the S&B master company (see ``sb_master_company``).

    S&B's workbooks carry no AAIA codes, so matching is name-based only (as for Quadratec).
    """
    logger.info("{} Syncing unmapped S&B brands to Brands.".format(_LOG_PREFIX))

    sb_provider = src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.SB.value,
    ).first()
    if not sb_provider:
        logger.warning("{} S&B provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    master_company = sb_master_company()
    if not master_company:
        logger.warning(
            "{} Master company slug={!r} not found. Skipping brand sync.".format(
                _LOG_PREFIX, src_constants.SB_MASTER_COMPANY_SLUG
            )
        )
        return []

    _, cp_created = src_models.CompanyProviders.objects.get_or_create(
        company=master_company,
        provider=sb_provider,
        defaults={"credentials": {}, "primary": False},
    )
    if cp_created:
        logger.info(
            "{} Created CompanyProviders for {!r} (id={}) + S&B.".format(
                _LOG_PREFIX, master_company.name, master_company.id
            )
        )

    mapped_ids = set(
        src_models.BrandSbBrandMapping.objects.values_list("sb_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.SbBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped S&B brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    logger.info("{} Found {} unmapped S&B brands.".format(_LOG_PREFIX, len(unmapped)))

    resolved_by_id: typing.Dict[int, src_models.Brands] = {}

    # Phase 1: exact uppercase-name match.
    name_upper_keys = {(b.name or "").strip().upper() for b in unmapped if (b.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for brand in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            key = (brand.name or "").strip().upper()
            brands_by_upper_name.setdefault(key, brand)
    for sb_brand in unmapped:
        key = (sb_brand.name or "").strip().upper()
        if key and key in brands_by_upper_name:
            resolved_by_id[sb_brand.id] = brands_by_upper_name[key]

    # Phase 2: compact-key (punctuation/spacing-insensitive) -- the phase that matters most here,
    # since "S&B" and "S&B Filters" differ only by punctuation and a word.
    compact_matches = 0
    still = [b for b in unmapped if b.id not in resolved_by_id]
    if still:
        compact_index = brands_by_compact_key()
        for sb_brand in still:
            key = normalize_compact_key(sb_brand.name or "")
            if key and key in compact_index:
                resolved_by_id[sb_brand.id] = compact_index[key]
                compact_matches += 1

    # Phase 3: fuzzy word-prefix.
    unresolved = [b for b in unmapped if b.id not in resolved_by_id]
    first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matches = 0
    for sb_brand in unresolved:
        tokens = normalize_upper_words(sb_brand.name or "").split()
        candidates: typing.List[src_models.Brands] = (
            list(first_index.get(tokens[0], ())) if tokens else []
        )
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(sb_brand.name or "", candidates)
        if brand:
            resolved_by_id[sb_brand.id] = brand
            fuzzy_matches += 1

    # Phase 4: create Brands for anything still unresolved.
    new_brand_names: typing.Dict[str, None] = {}
    for sb_brand in unmapped:
        if sb_brand.id not in resolved_by_id:
            new_brand_names.setdefault(_brand_name_upper_for_sync(sb_brand), None)

    created_brands = 0
    if new_brand_names:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_names.keys())).values_list(
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
            for name in new_brand_names
            if name not in existing_names
        ]
        if new_rows:
            src_models.Brands.objects.bulk_create(new_rows, ignore_conflicts=True)
            created_brands = len(new_rows)
        by_name = {
            brand.name: brand
            for brand in src_models.Brands.objects.filter(name__in=list(new_brand_names.keys()))
        }
        for sb_brand in unmapped:
            if sb_brand.id not in resolved_by_id:
                resolved_by_id[sb_brand.id] = by_name[_brand_name_upper_for_sync(sb_brand)]

    pgbulk.upsert(
        src_models.BrandSbBrandMapping,
        [
            src_models.BrandSbBrandMapping(
                brand_id=resolved_by_id[sb_brand.id].id,
                sb_brand_id=sb_brand.id,
            )
            for sb_brand in unmapped
        ],
        unique_fields=["brand", "sb_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    created_company_brands = 0
    for sb_brand in unmapped:
        brand = resolved_by_id[sb_brand.id]
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand,
            provider=sb_provider,
        )
        if bp_created:
            created_brand_providers += 1
        _, cb_created = src_models.CompanyBrands.objects.get_or_create(
            company=master_company,
            brand=brand,
            defaults={
                "status": src_enums.CompanyBrandStatus.ACTIVE.value,
                "status_name": src_enums.CompanyBrandStatus.ACTIVE.name,
            },
        )
        if cb_created:
            created_company_brands += 1

    logger.info(
        "{} Sync complete. Brands created: {}, compact: {}, fuzzy: {}, mappings: {}, "
        "BrandProviders: {}, CompanyBrands: {}.".format(
            _LOG_PREFIX,
            created_brands,
            compact_matches,
            fuzzy_matches,
            len(unmapped),
            created_brand_providers,
            created_company_brands,
        )
    )
    return unmapped


# ======================================================================================
# Catalog ingest
# ======================================================================================
def _catalog_rows_by_sku(
    catalog: typing.Sequence[typing.Mapping[str, typing.Any]],
) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
    """
    Catalog rows keyed by SKU, last row winning.

    Last-wins rather than first-wins deliberately: when S&B revise a price they have been seen to
    append the corrected row below the original rather than editing in place, so the lower row is
    the newer one. Matches how Rough Country's General tab is deduped.
    """
    out: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    for row in catalog:
        sku = _safe_sku(row.get("sku"))
        if not sku:
            continue
        out[sku] = dict(row)
    return out


def _build_part(
    sku: str,
    row: typing.Mapping[str, typing.Any],
    brand: src_models.SbBrand,
    shopify: typing.Optional[SbShopifyProduct],
) -> src_models.SbPart:
    """
    One SbPart from its catalog row plus (optionally) its Shopify product.

    The catalog wins every field it carries; Shopify only fills the two things it alone has
    (HTML description, imagery) plus a title fallback. Stock is not here at all: it lives in
    ``SbInventory``, written by a separate sweep on its own cadence, so a catalog run can never
    clobber it.
    """
    images = list(shopify.images) if shopify else []
    title = _safe_str(row.get("title"), 512) or (shopify.title if shopify else None)

    return src_models.SbPart(
        brand=brand,
        sku=sku,
        upc=_safe_upc(row.get("upc")),
        title=title,
        description=(shopify.body_html if shopify else None),
        category=_safe_str(row.get("category"), 255),
        filter_type=_safe_str(row.get("filter_type"), 64),
        vehicle_type=_safe_str(row.get("vehicle_type"), 64),
        shopify_product_type=(shopify.product_type if shopify else None),
        shopify_product_id=(_safe_str(shopify.product_id, 64) if shopify else None),
        us_retail_price=_safe_decimal(row.get("us_retail_price")),
        canadian_retail_price=_safe_decimal(row.get("canadian_retail_price")),
        level_1_price=_safe_decimal(row.get("level_1_price")),
        level_2_price=_safe_decimal(row.get("level_2_price")),
        weight=_safe_decimal(row.get("weight")),
        shipping_width=_safe_decimal(row.get("shipping_width")),
        shipping_height=_safe_decimal(row.get("shipping_height")),
        shipping_length=_safe_decimal(row.get("shipping_length")),
        prop_65_warning=_safe_str(row.get("prop_65_warning")),
        warning_label_letter=_safe_code(row.get("warning_label_letter"), 8),
        emissions_code=_safe_code(row.get("emissions_code"), 16),
        carb_exempt_id=_safe_code(row.get("carb_exempt_id"), 128),
        fifty_state_street_legal=_safe_str(row.get("fifty_state_street_legal"), 16),
        tune_required=_safe_str(row.get("tune_required"), 16),
        iso5011_test_results=_safe_str(row.get("iso5011_test_results")),
        install_video_url=_safe_str(row.get("install_video_url")),
        installation_instructions_url=_safe_str(row.get("installation_instructions_url")),
        quantity_of_oil=_safe_code(row.get("quantity_of_oil"), 64),
        quantity_of_oil_power_stack=_safe_code(row.get("quantity_of_oil_power_stack"), 64),
        image_1=images[0] if len(images) > 0 else None,
        image_2=images[1] if len(images) > 1 else None,
        image_3=images[2] if len(images) > 2 else None,
        image_4=images[3] if len(images) > 3 else None,
        image_5=images[4] if len(images) > 4 else None,
        image_6=images[5] if len(images) > 5 else None,
        images=images or None,
        video=(shopify.video if shopify else None) or _safe_str(row.get("install_video_url")),
        raw_data={"catalog": _jsonable(row)},
    )


def _jsonable(row: typing.Mapping[str, typing.Any]) -> typing.Dict[str, typing.Any]:
    """Row as JSON-safe values for ``raw_data`` (Decimals and datetimes become strings)."""
    out: typing.Dict[str, typing.Any] = {}
    for key, value in row.items():
        if value is None or isinstance(value, (str, int, float, bool)):
            out[key] = value
        else:
            out[key] = str(value)
    return out


def collapse_years_to_ranges(years: typing.Iterable[int]) -> typing.List[typing.Tuple[int, int]]:
    """
    Individual years -> the minimal list of contiguous ``(start, end)`` ranges:
    ``{2011, 2012, 2013, 2016}`` becomes ``[(2011, 2013), (2016, 2016)]``.

    Same helper as ``master_parts._collapse_years_to_ranges`` and the same reason for existing:
    MasterPartFitment stores ranges, and the miva dump enumerates one row per model year.
    """
    sorted_years = sorted(set(years))
    if not sorted_years:
        return []
    ranges: typing.List[typing.Tuple[int, int]] = []
    range_start = previous = sorted_years[0]
    for year in sorted_years[1:]:
        if year == previous + 1:
            previous = year
            continue
        ranges.append((range_start, previous))
        range_start = previous = year
    ranges.append((range_start, previous))
    return ranges


def _collapse_specs(specs: typing.Iterable[SbFitmentSpec]) -> typing.List[SbFitmentSpec]:
    """
    Merge specs for the same vehicle into the fewest year ranges that cover them.

    This is what turns the miva dump's ``2011, 2012, 2013, 2014, 2015, 2016`` rows for one
    Ford F-250 into a single ``2011-2016`` row. Collapsing only joins years that are genuinely
    adjacent, so a gap in the enumeration stays a gap -- the one thing that must not be invented.
    """
    years_by_vehicle: typing.Dict[typing.Tuple[str, str, str], typing.Set[int]] = {}
    for spec in specs:
        key = (spec.make, spec.model, spec.engine)
        years = years_by_vehicle.setdefault(key, set())
        years.update(range(spec.start_year, spec.end_year + 1))

    out: typing.List[SbFitmentSpec] = []
    for (make, model, engine), years in years_by_vehicle.items():
        for start, end in collapse_years_to_ranges(years):
            out.append(
                SbFitmentSpec(
                    start_year=start, end_year=end, make=make, model=model, engine=engine
                )
            )
    return out


def _fitment_specs_by_sku(
    data: typing.Mapping[str, typing.Any],
    shopify_by_sku: typing.Mapping[str, SbShopifyProduct],
) -> typing.Dict[str, typing.Dict[SbFitmentSpec, str]]:
    """
    Merge all three fitment sources into ``{sku: {spec: source}}``.

    **Precedence is per vehicle, not per year range.** Once a better source has described a
    (make, model, engine) for a SKU, a weaker source is ignored for that vehicle entirely. Without
    that rule the legacy miva dump -- which enumerates one row per model year -- would add six
    redundant single-year rows beside the Fitment tab's one ``2011-2016`` row for the same truck,
    and measured against the real workbooks it more than doubled the fitment table with rows that
    said nothing new (5,203 miva rows against 2,160 curated ones). A vehicle the better source
    never mentions is still genuinely new information and is kept.

    Within a source, years for one vehicle are collapsed into the fewest contiguous ranges (see
    ``_collapse_specs``), which is both what MasterPartFitment wants and what makes the miva rows
    comparable to the curated ones in the first place.

    Deduping here rather than at upsert time is what keeps the pgbulk call free of conflicting rows
    for the same unique key.
    """
    out: typing.Dict[str, typing.Dict[SbFitmentSpec, str]] = {}
    # SKU -> the (make, model, engine) tuples a higher-precedence source already covered.
    claimed_by_sku: typing.Dict[str, typing.Set[typing.Tuple[str, str, str]]] = {}

    def _add(
        specs_by_sku: typing.Mapping[str, typing.List[SbFitmentSpec]], source: str
    ) -> None:
        for sku, specs in specs_by_sku.items():
            if not sku or not specs:
                continue
            claimed = claimed_by_sku.setdefault(sku, set())
            fresh = [
                spec
                for spec in specs
                if (spec.make, spec.model, spec.engine) not in claimed
            ]
            if not fresh:
                continue
            bucket = out.setdefault(sku, {})
            for spec in _collapse_specs(fresh):
                bucket.setdefault(spec, source)
            claimed.update((spec.make, spec.model, spec.engine) for spec in fresh)

    fitment_tab: typing.Dict[str, typing.List[SbFitmentSpec]] = {}
    for row in data.get("fitment") or []:
        sku = _safe_sku(row.get("sku"))
        if sku:
            fitment_tab.setdefault(sku, []).extend(parse_fitment_tab_row(row))
    _add(fitment_tab, FITMENT_SOURCE_FITMENT_TAB)

    _add(
        {sku: list(product.fitment) for sku, product in shopify_by_sku.items()},
        FITMENT_SOURCE_SHOPIFY,
    )

    miva: typing.Dict[str, typing.List[SbFitmentSpec]] = {}
    for row in data.get("miva") or []:
        # PRODUCT_SKU is blank on some rows; PRODUCT_CODE carries the same value there.
        sku = _safe_sku(row.get("sku")) or _safe_sku(row.get("product_code"))
        if sku:
            miva.setdefault(sku, []).extend(parse_miva_fitment(row.get("fitment")))
    _add(miva, FITMENT_SOURCE_MIVA)

    return out


def fetch_and_save_sb(
    pricing_master_url: typing.Optional[str] = None,
    shopify_export_url: typing.Optional[str] = None,
    pricing_master_path: typing.Optional[str] = None,
    shopify_export_path: typing.Optional[str] = None,
    force_download: bool = False,
) -> None:
    """
    Read both S&B workbooks and upsert SbBrand, SbPart and SbFitment.

    Needs no CompanyProviders row: the workbooks are shared, so this is a single catalog ingest for
    the whole network (see the module docstring). Per-company pricing (SbCompanyPricing) is handled
    separately by IntegrationPricingSyncJob, and stock by ``fetch_and_save_sb_inventory``.
    """
    logger.info("{} Starting S&B feed sync.".format(_LOG_PREFIX))

    client = _sb_sheet_client(
        pricing_master_url=pricing_master_url,
        shopify_export_url=shopify_export_url,
        pricing_master_path=pricing_master_path,
        shopify_export_path=shopify_export_path,
    )
    try:
        data = client.get_feed_data(force_download=force_download)
    except sb_exceptions.SbException as e:
        logger.error("{} Feed error: {}.".format(_LOG_PREFIX, str(e)))
        raise

    catalog_by_sku = _catalog_rows_by_sku(data.get("catalog") or [])
    if not catalog_by_sku:
        logger.warning("{} No catalog rows parsed; leaving existing parts untouched.".format(_LOG_PREFIX))
        return

    shopify_by_sku = group_shopify_products(data.get("shopify") or [])
    logger.info(
        "{} Catalog SKUs: {}. Shopify SKUs: {} ({} of them in the catalog).".format(
            _LOG_PREFIX,
            len(catalog_by_sku),
            len(shopify_by_sku),
            len(set(shopify_by_sku) & set(catalog_by_sku)),
        )
    )

    brands_by_key = _ensure_sb_brands(
        _normalize_brand_key(row.get("brand")) for row in catalog_by_sku.values()
    )

    part_instances: typing.List[src_models.SbPart] = []
    for sku, row in catalog_by_sku.items():
        brand_key = _normalize_brand_key(row.get("brand"))
        brand = brands_by_key.get(brand_key) or brands_by_key[DEFAULT_SB_BRAND_NAME]
        part_instances.append(_build_part(sku, row, brand, shopify_by_sku.get(sku)))

    now = timezone.now()
    for part in part_instances:
        part.updated_at = now

    upserted = 0
    for start in range(0, len(part_instances), SB_PARTS_UPSERT_BATCH):
        batch = part_instances[start:start + SB_PARTS_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.SbPart,
            batch,
            unique_fields=["brand", "sku"],
            update_fields=SB_PART_UPDATE_FIELDS,
            returning=False,
        )
        upserted += len(batch)
    logger.info("{} Upserted {} S&B parts.".format(_LOG_PREFIX, upserted))
    connection.close()

    _upsert_sb_fitments(data, shopify_by_sku)

    logger.info("{} S&B feed sync complete.".format(_LOG_PREFIX))


def _upsert_sb_fitments(
    data: typing.Mapping[str, typing.Any],
    shopify_by_sku: typing.Mapping[str, SbShopifyProduct],
) -> None:
    """
    Upsert SbFitment from all three sources for SKUs that exist in the catalog.

    Fitment for a SKU with no catalog row is dropped, not stored: the miva dump alone names 21.5k
    SKUs against the catalog's ~830, almost all of them retired, and fitment with nothing to attach
    to is noise. The count of dropped SKUs is logged so a sudden jump is visible.
    """
    specs_by_sku = _fitment_specs_by_sku(data, shopify_by_sku)
    if not specs_by_sku:
        logger.warning("{} No fitment parsed from any source.".format(_LOG_PREFIX))
        return

    # SKU -> SbPart.id. SKUs are unique across S&B's brands in the catalog (confirmed: 828 rows,
    # 828 distinct SKUs), so a flat map is safe; a collision would mean S&B started reusing a SKU
    # across their lines, which is worth a warning rather than a silent pick.
    part_id_by_sku: typing.Dict[str, int] = {}
    collisions = 0
    for row in src_models.SbPart.objects.values("id", "sku").iterator(chunk_size=5000):
        sku = (row["sku"] or "").strip()
        if not sku:
            continue
        if sku in part_id_by_sku:
            collisions += 1
            continue
        part_id_by_sku[sku] = row["id"]
    if collisions:
        logger.warning(
            "{} {} SKU(s) exist under more than one S&B brand; fitment attached to the first "
            "only.".format(_LOG_PREFIX, collisions)
        )

    instances: typing.List[src_models.SbFitment] = []
    missing_skus = 0
    for sku, specs in specs_by_sku.items():
        part_id = part_id_by_sku.get(sku)
        if not part_id:
            missing_skus += 1
            continue
        for spec, source in specs.items():
            instances.append(
                src_models.SbFitment(
                    part_id=part_id,
                    start_year=spec.start_year,
                    end_year=spec.end_year,
                    make=spec.make,
                    model=spec.model,
                    submodel="",
                    engine=spec.engine,
                    source=source,
                )
            )

    by_source: typing.Dict[str, int] = {}
    for instance in instances:
        by_source[instance.source] = by_source.get(instance.source, 0) + 1
    logger.info(
        "{} Fitment rows to upsert: {} ({}); {} SKU(s) had fitment but no catalog part.".format(
            _LOG_PREFIX,
            len(instances),
            ", ".join(
                "{}={}".format(source, by_source.get(source, 0))
                for source in FITMENT_SOURCE_PRECEDENCE
            ),
            missing_skus,
        )
    )
    if not instances:
        return

    upserted = 0
    for start in range(0, len(instances), SB_FITMENT_UPSERT_BATCH):
        batch = instances[start:start + SB_FITMENT_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.SbFitment,
            batch,
            unique_fields=["part", "start_year", "end_year", "make", "model", "submodel", "engine"],
            update_fields=["source", "updated_at"],
            returning=False,
        )
        upserted += len(batch)
        connection.close()
    logger.info("{} Upserted {} S&B fitments.".format(_LOG_PREFIX, upserted))


# ======================================================================================
# Live inventory (emailed, forwarded, dropped on the relay)
# ======================================================================================
def _inventory_rows_from_file(path: str) -> typing.List[typing.Tuple[str, int]]:
    """
    ``[(sku, qty), ...]`` from a forwarded S&B live-inventory file.

    **The format is unconfirmed** -- S&B have never delivered one -- so columns are resolved by
    header name against ``feed_spec.INVENTORY_SKU_COLUMNS`` / ``INVENTORY_QTY_COLUMNS`` rather than
    by position, and a file whose headers match neither raises instead of silently importing
    nothing. Both CSV and xlsx are accepted since an emailed export could be either.
    """
    extension = os.path.splitext(path)[1].lower()
    if extension in (".xlsx", ".xlsm"):
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        try:
            worksheet = workbook.worksheets[0]
            rows = [
                row
                for row in worksheet.iter_rows(values_only=True)
                if row and any(cell is not None for cell in row)
            ]
        finally:
            workbook.close()
    else:
        with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as handle:
            sample = handle.read(8192)
            handle.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = csv.excel
            rows = [tuple(row) for row in csv.reader(handle, dialect) if any(cell for cell in row)]

    if not rows:
        raise sb_exceptions.SbParseError(
            "S&B inventory file {} is empty.".format(path)
        )

    header_index = None
    sku_index = None
    qty_index = None
    limit = min(len(rows), sb_feed_spec.INVENTORY_MAX_HEADER_SCAN_ROWS)
    for index in range(limit):
        candidate_sku = sb_feed_spec.find_column(rows[index], sb_feed_spec.INVENTORY_SKU_COLUMNS)
        candidate_qty = sb_feed_spec.find_column(rows[index], sb_feed_spec.INVENTORY_QTY_COLUMNS)
        if candidate_sku is not None and candidate_qty is not None:
            header_index, sku_index, qty_index = index, candidate_sku, candidate_qty
            break

    if header_index is None:
        raise sb_exceptions.SbParseError(
            "Could not find a SKU column and a quantity column in S&B inventory file {} (first "
            "row was {!r}). S&B's live-inventory format is not yet confirmed — add the real "
            "header spellings to feed_spec.INVENTORY_SKU_COLUMNS / INVENTORY_QTY_COLUMNS.".format(
                path, rows[0][:20],
            )
        )

    logger.info(
        "{} Inventory file {}: header row {}, sku column {}, qty column {}.".format(
            _LOG_PREFIX, path, header_index, sku_index, qty_index
        )
    )

    out: typing.List[typing.Tuple[str, int]] = []
    for row in rows[header_index + 1:]:
        sku = _safe_sku(row[sku_index] if sku_index < len(row) else None)
        if not sku:
            continue
        qty = _safe_int(row[qty_index] if qty_index < len(row) else None)
        if qty is None:
            continue
        out.append((sku, max(qty, 0)))
    return out


def _sb_inventory_company_providers() -> typing.List[src_models.CompanyProviders]:
    """
    Connections whose relay folder is worth checking for an inventory file, newest first by id.

    Every active S&B connection is checked rather than just the primary: unlike the catalog, stock
    is per-dealer (S&B send each dealer their own file), and the first one that yields a usable
    file supplies the shared ``SbInventory`` figures -- see ``fetch_and_save_sb_inventory`` for
    that trade-off. Primary connections are tried first so the choice is deterministic.
    """
    return list(
        _active_sb_company_providers_queryset().order_by("-primary", "id")
    )


def fetch_and_save_sb_inventory(force_download: bool = False) -> None:
    """
    Pull a forwarded S&B live-inventory file off the relay and upsert ``SbInventory`` rows.

    **One figure per part, not per company.** S&B send each dealer their own file, but everything
    else about SbPart is shared across the network, and ProviderPartInventory is keyed per
    provider-part rather than per company -- so a per-company quantity could not propagate
    anywhere. The first connection with a usable file supplies the figures, and
    ``SbInventory.source_company`` / ``source_filename`` record which file they came from, so the
    sharing is auditable rather than invisible. Revisit the shape if real files from two dealers
    ever turn out to disagree.

    **Only SKUs named in the file are touched.** A SKU that was in last week's file and is absent
    from this one keeps the quantity it had, because we do not know whether S&B's file is a full
    snapshot or only what moved -- and that is unanswerable until a real second file arrives.
    ``last_synced_at`` is written per row on every pass, so the staleness stays visible per SKU
    rather than hidden: a part whose timestamp has stopped advancing is one the file stopped
    mentioning. Revisit this (and zero the missing ones) once the file's semantics are confirmed.

    A connection with nothing delivered yet is normal, not an error -- it is logged and skipped.
    """
    logger.info("{} Starting S&B live inventory ingest.".format(_LOG_PREFIX))

    company_providers = _sb_inventory_company_providers()
    if not company_providers:
        logger.info("{} No active S&B connections. Skipping inventory.".format(_LOG_PREFIX))
        return

    rows: typing.List[typing.Tuple[str, int]] = []
    source_company_id = None
    source_filename = None
    for company_provider in company_providers:
        creds = credentials_helper.get_feed_credentials(company_provider)
        try:
            relay = sb_relay.SbRelayClient(credentials=creds)
        except ValueError as e:
            logger.warning(
                "{} company_id={}: relay not configured ({}).".format(
                    _LOG_PREFIX, company_provider.company_id, str(e)
                )
            )
            continue

        try:
            path = relay.download_inventory_file(force_download=force_download)
        except sb_exceptions.SbFileNotFoundError:
            logger.info(
                "{} company_id={}: no inventory file on the relay yet.".format(
                    _LOG_PREFIX, company_provider.company_id
                )
            )
            continue
        except sb_exceptions.SbException as e:
            logger.warning(
                "{} company_id={}: relay error ({}).".format(
                    _LOG_PREFIX, company_provider.company_id, str(e)
                )
            )
            continue

        try:
            rows = _inventory_rows_from_file(path)
        except sb_exceptions.SbException as e:
            logger.error(
                "{} company_id={}: could not parse {} ({}).".format(
                    _LOG_PREFIX, company_provider.company_id, path, str(e)
                )
            )
            continue

        if rows:
            source_company_id = company_provider.company_id
            source_filename = os.path.basename(path)
            break
        logger.warning(
            "{} company_id={}: {} parsed to zero usable rows.".format(
                _LOG_PREFIX, company_provider.company_id, path
            )
        )

    if not rows:
        logger.info("{} No S&B inventory rows available; stock left as-is.".format(_LOG_PREFIX))
        return

    qty_by_sku: typing.Dict[str, int] = {}
    for sku, qty in rows:
        # Last wins: a file that lists a SKU per warehouse would need summing instead, which is
        # exactly the kind of thing to settle against a real file rather than guess at now.
        qty_by_sku[sku] = qty

    part_ids = list(
        src_models.SbPart.objects.filter(sku__in=list(qty_by_sku.keys())).values("id", "sku")
    )
    now = timezone.now()
    instances = [
        src_models.SbInventory(
            part_id=row["id"],
            available_qty=qty_by_sku.get((row["sku"] or "").strip()),
            source_company_id=source_company_id,
            source_filename=source_filename,
            last_synced_at=now,
            updated_at=now,
        )
        for row in part_ids
    ]

    updated = 0
    for start in range(0, len(instances), SB_PARTS_UPSERT_BATCH):
        batch = instances[start:start + SB_PARTS_UPSERT_BATCH]
        pgbulk.upsert(
            src_models.SbInventory,
            batch,
            unique_fields=["part"],
            update_fields=[
                "available_qty",
                "source_company",
                "source_filename",
                "last_synced_at",
                "updated_at",
            ],
            returning=False,
        )
        updated += len(batch)
        connection.close()

    logger.info(
        "{} S&B inventory: {} rows from company_id={} file={!r} applied to {} parts ({} SKU(s) in "
        "the file had no catalog part).".format(
            _LOG_PREFIX,
            len(qty_by_sku),
            source_company_id,
            source_filename,
            updated,
            len(qty_by_sku) - len(part_ids),
        )
    )


# ======================================================================================
# Per-company pricing (SbCompanyPricing)
# ======================================================================================
def sync_sb_company_pricing_for_company_provider(company_provider_id: int) -> None:
    """
    Resolve S&B's published prices for one connection into SbCompanyPricing.

    Reads straight from SbPart rather than re-parsing the workbook: the prices are identical for
    every dealer, so the catalog ingest has already stored them and all that is left is applying
    this company's ``pricing_level`` and ``price_region``. That makes this the cheapest pricing job
    of any provider -- no network at all -- and it means a company that changes level sees new
    costs on the next job run without waiting for a catalog sync.
    """
    company_provider = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.SB.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not company_provider:
        logger.warning(
            "{} No active S&B CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return

    creds = credentials_helper.get_feed_credentials(company_provider)
    level = sb_pricing_level_for_credentials(creds)
    region = sb_price_region_for_credentials(creds)
    cost_field = (
        "level_2_price" if level == src_constants.SB_PRICING_LEVEL_2 else "level_1_price"
    )
    retail_field = (
        "canadian_retail_price"
        if region == src_constants.SB_PRICE_REGION_CA
        else "us_retail_price"
    )
    logger.info(
        "{} company_id={}: pricing level {} ({}), retail from {}.".format(
            _LOG_PREFIX, company_provider.company_id, level, cost_field, retail_field
        )
    )

    total = 0
    last_id = 0
    while True:
        batch = list(
            src_models.SbPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "level_1_price", "level_2_price", "us_retail_price", "canadian_retail_price")[
                :SB_PRICING_UPSERT_BATCH
            ]
        )
        if not batch:
            break
        last_id = batch[-1]["id"]

        now = timezone.now()
        to_upsert = [
            src_models.SbCompanyPricing(
                part_id=row["id"],
                company_id=company_provider.company_id,
                pricing_level=level,
                # Level 2 is blank on a handful of rows; falling back to Level 1 is right -- S&B
                # bill the standard dealer price when no discounted one is published.
                cost=row.get(cost_field) or row.get("level_1_price"),
                retail_price=row.get(retail_field),
                updated_at=now,
            )
            for row in batch
        ]
        pgbulk.upsert(
            src_models.SbCompanyPricing,
            to_upsert,
            unique_fields=["part", "company"],
            update_fields=["pricing_level", "cost", "retail_price", "updated_at"],
            returning=False,
        )
        total += len(to_upsert)
        connection.close()

    logger.info(
        "{} company_id={}: upserted {} SbCompanyPricing rows.".format(
            _LOG_PREFIX, company_provider.company_id, total
        )
    )
