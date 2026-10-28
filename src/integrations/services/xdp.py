"""
XDP (Xtreme Diesel Performance) integration.

XDP is a retailer, and cost reaches us two different ways depending on whose brand it is:

- **XDP's own brand** has a real per-dealer CSV feed (``credentials["feed_url"]``) carrying actual
  ``COST``, ``MAP_PRICE`` and a numeric ``IN-STOCK``. 449 parts in the measured feed.
- **Every other brand XDP resells** (241 of them) has no wholesale figure exposed anywhere
  reachable, so cost is derived from the company's own markup off XDP's public retail price --
  the same arrangement Thoroughbred Diesel uses, for the same reason.

``XdpCompanyPricing.cost_source`` records which of the two produced each row, so the two never
get confused downstream.

The catalog itself is public (no login anywhere in this module). It is swept from SearchSpring
rather than the Miva API because SearchSpring is already flat -- see fetch_and_save_xdp_catalog
for why that distinction decides catalog completeness.
"""
import html
import json
import logging
import typing
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pgbulk

from django.db.models.functions import Upper

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.xdp import client as xdp_client
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[XDP-SERVICES]"

CATALOG_UPSERT_BATCH = 2000
PRICING_UPSERT_BATCH = 5000

# CompanyProviders.credentials keys a connecting company supplies at connect time -- see
# PROVIDER_CATALOG's XDP entry in constants.py and _validate_xdp_connection in
# api/services/integrations.py.
MARKUP_CREDENTIAL_KEY = "markup"
FEED_URL_CREDENTIAL_KEY = "feed_url"

# XdpBrand.name of XDP's own house brand -- the only brand the dealer CSV covers.
XDP_HOUSE_BRAND_NAME = "XDP"

COST_SOURCE_FEED = "feed"
COST_SOURCE_MARKUP = "markup"


def _xdp_provider() -> typing.Optional[src_models.Providers]:
    return src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.XDP.value,
    ).first()


def _active_xdp_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.XDP.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        active=True,
    ).select_related("company", "provider")


def _primary_xdp_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    """
    The connection whose dealer feed fills in the shared, XDP-wide columns on XdpPart
    (``map_price``, ``quantity``, ``application``, dimensions). Prefer ``primary=True``, else the
    lowest-id active connection -- same resolution Meyer and Tri-State use.

    Only the house-brand numbers come from here, and only the ones that are account-agnostic in
    practice: XDP's warehouse stock is the same number whoever asks. Each company's own cost and
    quantity still come from their own feed, onto XdpCompanyPricing -- the same split
    NtwParts/NtwCompanyPricing uses, and for the same reason.
    """
    base = _active_xdp_company_providers_queryset()
    cp = base.filter(primary=True).first()
    if cp:
        return cp
    return base.first()


def _feed_url_for_company_provider(cp: src_models.CompanyProviders) -> typing.Optional[str]:
    creds = credentials_helper.get_feed_credentials(cp)
    url = (creds or {}).get(FEED_URL_CREDENTIAL_KEY)
    return (str(url).strip() or None) if url else None


# ---------------------------------------------------------------------------
# parsing helpers
# ---------------------------------------------------------------------------
def _dec(raw: typing.Any) -> typing.Optional[Decimal]:
    if raw is None or raw == "":
        return None
    try:
        return Decimal(str(raw).strip()).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        return None


def _clean_text(raw: typing.Any) -> typing.Optional[str]:
    """SearchSpring ships display strings HTML-escaped (``Size: 18x9&quot;``). Unescape once so
    stored copy is not double-encoded when it reaches a template or the search index."""
    if raw is None:
        return None
    text = html.unescape(str(raw)).strip()
    return text or None


def _absolute_image_url(row: typing.Dict) -> typing.Optional[str]:
    """``thumbnailImageUrl`` comes back protocol-relative (``//www.xtremediesel.com/...``);
    ``imageUrl`` is a bare storefront-relative path. Normalise both to absolute."""
    thumb = (row.get("thumbnailImageUrl") or "").strip()
    if thumb.startswith("//"):
        return "https:" + thumb
    if thumb.startswith("http"):
        return thumb
    rel = (row.get("imageUrl") or row.get("product_image") or "").strip()
    if not rel:
        return None
    if rel.startswith("http"):
        return rel
    return "https://www.xtremediesel.com/mm5/" + rel.lstrip("/")


def _yes(raw: typing.Any) -> typing.Optional[bool]:
    if raw is None or raw == "":
        return None
    return str(raw).strip().lower() == "yes"


def _parse_inline_variants(raw: typing.Any) -> typing.List[typing.Dict]:
    """
    A variant parent carries its children inline in SearchSpring's ``variants`` field as
    HTML-escaped JSON: ``[{"code":…, "mpn":…, "name":…, "price":…, "sku":…, "url":…}]``.

    This is not a nicety -- 5,226 of 13,496 inline children (39%) have NO separately indexed row
    of their own, concentrated in brands like Covercraft (1,033) and WCFab (770). Ignoring this
    field drops 5,226 sellable SKUs, each with a real price.
    """
    if not raw:
        return []
    try:
        parsed = json.loads(html.unescape(str(raw)))
    except (ValueError, TypeError) as e:
        logger.warning("{} could not parse inline variants: {}".format(_LOG_PREFIX, e))
        return []
    return parsed if isinstance(parsed, list) else []


def _part_from_index_row(
    row: typing.Dict, brand: typing.Optional[src_models.XdpBrand]
) -> typing.Optional[src_models.XdpPart]:
    code = (row.get("uid") or "").strip()
    if not code:
        return None
    sku = (row.get("sku") or "").strip() or None
    # On a variant parent, `mpn` is the list of all its CHILDREN's part numbers -- taking the
    # first would stamp one child's MPN onto the grouping row and read like the parent's own.
    # Parents have no part number of their own, so leave it null.
    mpn = None
    if sku is not None:
        mpn = str(xdp_client._first(row.get("mpn")) or "").strip() or None
    return src_models.XdpPart(
        brand=brand,
        code=code,
        product_id=(row.get("product_id") or "").strip() or None,
        sku=sku,
        mpn=mpn,
        name=_clean_text(row.get("name")),
        short_desc=_clean_text(row.get("short_desc")),
        url=(row.get("url") or row.get("product_url") or "").strip() or None,
        image_url=_absolute_image_url(row),
        price=_dec(row.get("price")),
        in_stock=_yes(row.get("quickships")),
        limited_stock=_yes(row.get("limited_stock")),
        product_type=_clean_text(row.get("product_type_unigram")),
        is_variant_parent=sku is None,
        raw_data=row,
    )


def _part_from_inline_variant(
    variant: typing.Dict, parent_row: typing.Dict, brand: typing.Optional[src_models.XdpBrand]
) -> typing.Optional[src_models.XdpPart]:
    """A child that SearchSpring does not index separately. It carries its own sku/mpn/price, but
    no stock flag and no image of its own -- the image is inherited from the parent and
    ``in_stock`` is left NULL rather than guessed from the parent's."""
    code = str(variant.get("code") or "").strip()
    sku = (variant.get("sku") or "").strip() or None
    if not code or not sku:
        return None
    return src_models.XdpPart(
        brand=brand,
        code=code,
        product_id=None,
        sku=sku,
        mpn=(str(variant.get("mpn") or "").strip() or None),
        name=_clean_text(variant.get("name")) or _clean_text(parent_row.get("name")),
        short_desc=_clean_text(parent_row.get("short_desc")),
        url=(variant.get("url") or "").strip() or None,
        image_url=_absolute_image_url(parent_row),
        price=_dec(variant.get("price")),
        in_stock=None,
        limited_stock=None,
        product_type=_clean_text(parent_row.get("product_type_unigram")),
        is_variant_parent=False,
        raw_data={"variant": variant, "parent_uid": parent_row.get("uid")},
    )


_PART_UPDATE_FIELDS = [
    "brand", "product_id", "sku", "mpn", "name", "short_desc", "url", "image_url",
    "price", "in_stock", "limited_stock", "product_type", "is_variant_parent",
    "raw_data", "updated_at",
]


def fetch_and_save_xdp_catalog() -> int:
    """
    Sweeps XDP's SearchSpring index brand by brand and upserts XdpBrand + XdpPart.

    Brand is the partition key for a hard reason: the index enforces a 10,000-result window
    (resultsPerPage caps at 100, page caps at 100) and the catalog is 60,539 rows, so an
    unpartitioned sweep silently stops at 10,000. No single brand comes close to the cap --
    largest is Dorman at ~4,500 -- and the per-brand sweep was measured returning exactly the
    facet-claimed 60,539 rows with no brand truncated.

    SearchSpring, not the Miva catalog API, is the source of record here. Miva's
    Runtime_ProductList_Load_Query is one call for everything and looks more convenient, but it
    carries the non-sellable variant grouping rows AND is missing some children outright
    (variant code 15315 is confirmed absent from it), while SearchSpring indexes variant children
    as their own rows with their own sku/mpn/price. Miva is used only to fill in weight --
    see enrich_xdp_catalog_from_json_mvc.
    """
    logger.info("{} Fetching XDP SearchSpring catalog.".format(_LOG_PREFIX))

    client = xdp_client.XdpSearchSpringClient()
    brands = client.brands()
    if not brands:
        logger.warning("{} SearchSpring returned no brands. Aborting catalog sweep.".format(_LOG_PREFIX))
        return 0

    # Discover/create XdpBrand rows up front -- the brand facet is one request and complete.
    brand_names = [name for name, _count in brands]
    existing = {b.name: b for b in src_models.XdpBrand.objects.filter(name__in=brand_names)}
    new_names = [n for n in brand_names if n not in existing]
    if new_names:
        src_models.XdpBrand.objects.bulk_create(
            [src_models.XdpBrand(name=n) for n in new_names], ignore_conflicts=True
        )
    brand_by_name = {b.name: b for b in src_models.XdpBrand.objects.filter(name__in=brand_names)}
    logger.info(
        "{} {} brand(s) in facet, {} newly created.".format(
            _LOG_PREFIX, len(brand_names), len(new_names)
        )
    )

    total_upserted = 0
    total_rows = 0
    total_parents = 0
    total_inline_recovered = 0

    def flush(parts: typing.List[src_models.XdpPart]) -> int:
        if not parts:
            return 0
        by_code = {p.code: p for p in parts}
        pgbulk.upsert(
            src_models.XdpPart,
            list(by_code.values()),
            unique_fields=["code"],
            update_fields=_PART_UPDATE_FIELDS,
        )
        return len(by_code)

    for brand_index, (brand_name, claimed) in enumerate(brands, start=1):
        brand = brand_by_name.get(brand_name)
        rows = list(client.iter_brand_products(brand_name))
        total_rows += len(rows)
        # Per-brand progress: the sweep is ~850 requests over 242 brands and takes minutes, and
        # without this the whole run looks hung between the opening log line and the closing one.
        logger.info(
            "{} brand {}/{} {!r}: {} row(s) swept ({} total so far).".format(
                _LOG_PREFIX, brand_index, len(brands), brand_name, len(rows), total_rows
            )
        )
        if claimed and len(rows) != claimed:
            logger.warning(
                "{} brand {!r}: facet claimed {} rows, swept {}.".format(
                    _LOG_PREFIX, brand_name, claimed, len(rows)
                )
            )

        indexed_skus = {(r.get("sku") or "").strip() for r in rows if (r.get("sku") or "").strip()}
        batch: typing.List[src_models.XdpPart] = []
        for row in rows:
            part = _part_from_index_row(row, brand)
            if part is None:
                continue
            batch.append(part)
            if part.is_variant_parent:
                total_parents += 1
                # Recover only children with no indexed row of their own; the rest are already
                # in `rows` with richer data (stock flag, image).
                for variant in _parse_inline_variants(row.get("variants")):
                    sku = (variant.get("sku") or "").strip()
                    if not sku or sku in indexed_skus:
                        continue
                    child = _part_from_inline_variant(variant, row, brand)
                    if child is None:
                        continue
                    batch.append(child)
                    indexed_skus.add(sku)
                    total_inline_recovered += 1
            if len(batch) >= CATALOG_UPSERT_BATCH:
                total_upserted += flush(batch)
                batch = []
        total_upserted += flush(batch)

    logger.info(
        "{} Catalog sweep complete: {} index rows ({} variant parents), {} inline children "
        "recovered, {} XdpPart rows upserted.".format(
            _LOG_PREFIX, total_rows, total_parents, total_inline_recovered, total_upserted
        )
    )
    return total_upserted


def enrich_xdp_catalog_from_json_mvc() -> int:
    """
    Fills in ``weight`` from Miva's own catalog dump, joined on ``code``. ~13 requests for all
    60,582 rows, so it is cheap enough to run on every ingest.

    Weight only: everything else Miva returns is either already on the row from SearchSpring or
    worse there (its ``sku`` is blank on grouping rows, its price is 0 on them). Miva exposes no
    brand field at all, which is why it cannot be the catalog spine.
    """
    logger.info("{} Enriching XDP catalog with weights from Miva json.mvc.".format(_LOG_PREFIX))
    client = xdp_client.XdpJsonMvcClient()

    weight_by_code: typing.Dict[str, typing.Optional[Decimal]] = {}
    for row in client.iter_catalog():
        code = str(row.get("code") or "").strip()
        if not code:
            continue
        weight = _dec(row.get("weight"))
        if weight is not None:
            weight_by_code[code] = weight
    if not weight_by_code:
        logger.warning("{} Miva catalog produced no weights. Nothing to enrich.".format(_LOG_PREFIX))
        return 0

    total = 0
    last_id = 0
    while True:
        rows = list(
            src_models.XdpPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "code", "weight")[:CATALOG_UPSERT_BATCH]
        )
        if not rows:
            break
        last_id = rows[-1]["id"]
        to_update = []
        for row in rows:
            weight = weight_by_code.get(row["code"])
            if weight is None or row["weight"] == weight:
                continue
            to_update.append(src_models.XdpPart(id=row["id"], weight=weight))
        if to_update:
            src_models.XdpPart.objects.bulk_update(to_update, ["weight"], batch_size=1000)
            total += len(to_update)

    logger.info("{} Enriched weight on {} XdpPart row(s).".format(_LOG_PREFIX, total))
    return total


def fetch_and_save_xdp_dealer_feed() -> int:
    """
    Reads the *primary* connection's dealer CSV and writes the XDP-wide house-brand columns onto
    XdpPart: ``map_price``, ``quantity``/``quantity_raw``, ``application`` and the shipping
    dimensions. Per-company cost is NOT written here -- that is
    sync_xdp_company_pricing_for_company_provider's job, off each company's own feed.

    Matches on the house brand only, by ``sku`` then ``mpn``. Every one of the 449 measured feed
    part numbers matched SearchSpring on both, so a miss here means the catalog sweep has not run
    or XDP added a part number the index does not carry yet -- counted and logged, never raised.
    """
    cp = _primary_xdp_company_provider()
    if not cp:
        logger.info(
            "{} No active XDP connection, so no dealer feed to read. House-brand cost and "
            "quantity stay unset; the public catalog is unaffected.".format(_LOG_PREFIX)
        )
        return 0

    feed_url = _feed_url_for_company_provider(cp)
    if not feed_url:
        logger.info(
            "{} Primary XDP connection (company_id={}) has no {} credential. Skipping dealer "
            "feed.".format(_LOG_PREFIX, cp.company_id, FEED_URL_CREDENTIAL_KEY)
        )
        return 0

    logger.info(
        "{} Reading dealer feed for primary connection company_id={}.".format(
            _LOG_PREFIX, cp.company_id
        )
    )
    rows = xdp_client.XdpDealerFeedClient(feed_url=feed_url).fetch_rows()
    by_part_number = {r["part_number"]: r for r in rows}

    house_brand = src_models.XdpBrand.objects.filter(name=XDP_HOUSE_BRAND_NAME).first()
    if not house_brand:
        logger.warning(
            "{} No XdpBrand named {!r} yet -- run the catalog sweep first. Skipping dealer "
            "feed.".format(_LOG_PREFIX, XDP_HOUSE_BRAND_NAME)
        )
        return 0

    parts = list(
        src_models.XdpPart.objects.filter(brand=house_brand).values("id", "sku", "mpn")
    )
    matched = 0
    to_update = []
    for part in parts:
        row = by_part_number.get((part.get("sku") or "").strip()) or by_part_number.get(
            (part.get("mpn") or "").strip()
        )
        if not row:
            continue
        matched += 1
        to_update.append(
            src_models.XdpPart(
                id=part["id"],
                map_price=_dec(row.get("map_price")),
                quantity=row.get("quantity"),
                quantity_raw=row.get("quantity_raw"),
                application=row.get("application"),
                weight=_dec(row.get("weight")),
                width=_dec(row.get("width")),
                length=_dec(row.get("length")),
                height=_dec(row.get("height")),
            )
        )
    if to_update:
        src_models.XdpPart.objects.bulk_update(
            to_update,
            ["map_price", "quantity", "quantity_raw", "application", "weight", "width", "length", "height"],
            batch_size=1000,
        )

    unmatched = len(rows) - matched
    if unmatched > 0:
        logger.warning(
            "{} dealer feed: {} of {} feed row(s) matched no XdpPart under brand {!r}.".format(
                _LOG_PREFIX, unmatched, len(rows), XDP_HOUSE_BRAND_NAME
            )
        )
    logger.info(
        "{} dealer feed: updated {} house-brand XdpPart row(s) from {} feed row(s).".format(
            _LOG_PREFIX, matched, len(rows)
        )
    )
    return matched


# ---------------------------------------------------------------------------
# pricing
# ---------------------------------------------------------------------------
def _parse_markup_percent(raw: typing.Any) -> typing.Optional[Decimal]:
    if raw is None or raw == "":
        return None
    try:
        d = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if d < 0:
        d = Decimal(0)
    if d > 100:
        d = Decimal(100)
    return d


def markup_percent_for_company(credentials: typing.Optional[typing.Dict]) -> Decimal:
    """
    Markup percent (0-100) off XDP's public retail price, from
    ``CompanyProviders.credentials["feed"]["markup"]``. The connect-flow validator
    (_validate_xdp_connection) guarantees it is present and numeric before a connection saves.

    A missing/corrupt value at sync time is treated defensively as 0% (cost == retail) with a
    warning, rather than assuming a discount that would understate cost -- the same choice
    Thoroughbred makes, and deliberately not WheelPros' invented default.
    """
    creds = credentials or {}
    pct = _parse_markup_percent(creds.get(MARKUP_CREDENTIAL_KEY))
    if pct is None:
        logger.warning(
            "{} Missing/unparseable markup credential, defaulting to 0% (cost == retail). "
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
    except (InvalidOperation, ValueError, TypeError):
        return None
    if p < 0:
        p = Decimal(0)
    pct = markup_percent_for_company(credentials)
    cost = p * (Decimal(1) - pct / Decimal(100))
    return cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def sync_xdp_company_pricing_for_company_provider(company_provider_id: int) -> int:
    """
    Upserts XdpCompanyPricing for one company, from both cost sources at once:

    - house-brand parts present in that company's own dealer CSV get the feed's real ``COST``,
      ``MAP_PRICE`` and quantity, with ``cost_source="feed"``;
    - everything else gets ``retail * (1 - markup/100)`` with ``cost_source="markup"``.

    A company with no ``feed_url`` gets markup rows for the whole catalog, house brand included --
    that is the correct answer for a dealer who has no XDP wholesale account, not a degraded one.

    Propagation into ProviderPartCompanyPricing is a separate step; see
    master_parts.sync_provider_pricing_from_xdp_for_company.
    """
    cp = (
        src_models.CompanyProviders.objects.filter(id=company_provider_id)
        .select_related("company", "provider")
        .first()
    )
    if not cp:
        logger.warning(
            "{} CompanyProviders id={} not found. Skipping pricing sync.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return 0

    feed_credentials = credentials_helper.get_feed_credentials(cp)
    markup_pct = markup_percent_for_company(feed_credentials)
    feed_url = _feed_url_for_company_provider(cp)

    feed_by_part_number: typing.Dict[str, typing.Dict] = {}
    if feed_url:
        try:
            rows = xdp_client.XdpDealerFeedClient(feed_url=feed_url).fetch_rows()
            feed_by_part_number = {r["part_number"]: r for r in rows}
        except (xdp_client.XdpApiError, ValueError) as e:
            # A broken feed URL must not wipe out this company's pricing: fall through to
            # markup-derived cost for everything and say so loudly.
            logger.warning(
                "{} company_id={} dealer feed unusable ({}); falling back to markup-derived "
                "cost for the whole catalog.".format(_LOG_PREFIX, cp.company_id, e)
            )

    house_brand_id = (
        src_models.XdpBrand.objects.filter(name=XDP_HOUSE_BRAND_NAME)
        .values_list("id", flat=True)
        .first()
    )

    logger.info(
        "{} Syncing XDP company pricing for company_id={} (markup={}%, feed_rows={}).".format(
            _LOG_PREFIX, cp.company_id, markup_pct, len(feed_by_part_number)
        )
    )

    total_upserted = 0
    from_feed = 0
    last_id = 0
    while True:
        rows = list(
            src_models.XdpPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "brand_id", "sku", "mpn", "price", "map_price", "is_variant_parent")[
                :PRICING_UPSERT_BATCH
            ]
        )
        if not rows:
            break
        last_id = rows[-1]["id"]

        batch: typing.List[src_models.XdpCompanyPricing] = []
        for row in rows:
            # Grouping rows are not sellable and their price is only the minimum variant price;
            # pricing them would publish a cost for something nobody can buy.
            if row.get("is_variant_parent"):
                continue
            retail = row.get("price")
            feed_row = None
            if feed_by_part_number and row.get("brand_id") == house_brand_id:
                feed_row = feed_by_part_number.get(
                    (row.get("sku") or "").strip()
                ) or feed_by_part_number.get((row.get("mpn") or "").strip())

            if feed_row:
                cost = _dec(feed_row.get("cost"))
                map_price = _dec(feed_row.get("map_price"))
                batch.append(
                    src_models.XdpCompanyPricing(
                        part_id=row["id"],
                        company_id=cp.company_id,
                        cost=cost,
                        map_price=map_price,
                        retail_price=retail or map_price,
                        quantity=feed_row.get("quantity"),
                        markup_percent_applied=None,
                        cost_source=COST_SOURCE_FEED,
                    )
                )
                from_feed += 1
                continue

            cost = dealer_cost_from_retail(retail, feed_credentials)
            if cost is None:
                continue
            batch.append(
                src_models.XdpCompanyPricing(
                    part_id=row["id"],
                    company_id=cp.company_id,
                    cost=cost,
                    map_price=row.get("map_price"),
                    retail_price=retail,
                    quantity=None,
                    markup_percent_applied=markup_pct,
                    cost_source=COST_SOURCE_MARKUP,
                )
            )

        if batch:
            by_key = {(r.part_id, r.company_id): r for r in batch}
            pgbulk.upsert(
                src_models.XdpCompanyPricing,
                list(by_key.values()),
                unique_fields=["part", "company"],
                update_fields=[
                    "cost", "map_price", "retail_price", "quantity",
                    "markup_percent_applied", "cost_source", "updated_at",
                ],
            )
            total_upserted += len(by_key)

    logger.info(
        "{} Synced {} XDP company pricing row(s) for company_id={} ({} from the dealer feed, "
        "{} markup-derived).".format(
            _LOG_PREFIX, total_upserted, cp.company_id, from_feed, total_upserted - from_feed
        )
    )
    return total_upserted


# ---------------------------------------------------------------------------
# fitment
# ---------------------------------------------------------------------------
def fetch_and_save_xdp_fitment_paths() -> int:
    """
    Walks SearchSpring's ``parts_finder_data`` hierarchy and stores each product's raw
    ``year>make>model>engine`` paths on XdpPart.fitment_paths. Decoding into MasterPartFitment is
    a separate step -- see master_parts.sync_master_part_fitments_from_xdp.

    This is the expensive pipeline: ~3,300 node-discovery requests plus a page per 100
    product-vehicle rows, against ~338,043 product-year pairs. Standalone on purpose, like
    Thoroughbred's vehicle walk -- do not put it in the hourly cycle.

    Wildcard ``All`` nodes are already dropped by the client. They report their parent's own
    count, so walking them would attribute the entire catalog to every level of the tree.
    """
    logger.info("{} Starting XDP fitment path walk.".format(_LOG_PREFIX))
    client = xdp_client.XdpSearchSpringClient()

    paths_by_code: typing.Dict[str, typing.Set[str]] = {}
    leaves = 0
    for leaf in client.iter_engine_leaves():
        leaves += 1
        for row in client.iter_fitment_node_products(leaf):
            code = (row.get("uid") or "").strip()
            if not code:
                continue
            paths_by_code.setdefault(code, set()).add(leaf)
        if leaves % 250 == 0:
            logger.info(
                "{} fitment walk: {} leaf node(s) done, {} product(s) with fitment so far.".format(
                    _LOG_PREFIX, leaves, len(paths_by_code)
                )
            )

    if not paths_by_code:
        logger.warning("{} fitment walk produced nothing. Leaving XdpPart.fitment_paths alone.".format(_LOG_PREFIX))
        return 0

    logger.info(
        "{} fitment walk complete: {} leaf node(s), {} product(s) with fitment. Writing.".format(
            _LOG_PREFIX, leaves, len(paths_by_code)
        )
    )

    total = 0
    last_id = 0
    while True:
        rows = list(
            src_models.XdpPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "code")[:CATALOG_UPSERT_BATCH]
        )
        if not rows:
            break
        last_id = rows[-1]["id"]
        to_update = []
        for row in rows:
            paths = paths_by_code.get(row["code"])
            if not paths:
                continue
            to_update.append(
                src_models.XdpPart(id=row["id"], fitment_paths=sorted(paths))
            )
        if to_update:
            src_models.XdpPart.objects.bulk_update(to_update, ["fitment_paths"], batch_size=500)
            total += len(to_update)

    logger.info("{} Wrote fitment_paths on {} XdpPart row(s).".format(_LOG_PREFIX, total))
    return total


# ---------------------------------------------------------------------------
# brand mapping
# ---------------------------------------------------------------------------
def sync_unmapped_xdp_brands_to_brands(
    dry_run: bool = False,
) -> typing.List[src_models.XdpBrand]:
    """
    For each XdpBrand without a BrandXdpBrandMapping: resolve Brands by exact name (uppercase),
    then compact key (punctuation/spacing-only differences), then fuzzy word-prefix match;
    otherwise create a Brands row.

    No AAIA-code phase: SearchSpring's ``manufacturer_name`` is a bare brand name with no AAIA
    code anywhere in the schema -- same position WheelPros and Thoroughbred are in.
    """
    logger.info(
        "{} Syncing unmapped XDP brands to Brands{}.".format(
            _LOG_PREFIX, " (dry run)" if dry_run else ""
        )
    )

    xdp_provider = _xdp_provider()
    if not xdp_provider:
        logger.warning("{} XDP provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandXdpBrandMapping.objects.values_list("xdp_brand_id", flat=True).distinct()
    )
    unmapped = list(src_models.XdpBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped XDP brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    name_upper_keys = {(b.name or "").strip().upper() for b in unmapped if (b.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for b in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            brands_by_upper_name.setdefault((b.name or "").strip().upper(), b)

    resolved: typing.Dict[int, src_models.Brands] = {}
    exact_matched: typing.Set[int] = set()
    for xb in unmapped:
        nm = (xb.name or "").strip().upper()
        if nm and nm in brands_by_upper_name:
            resolved[xb.id] = brands_by_upper_name[nm]
            exact_matched.add(xb.id)

    compact_matched: typing.Set[int] = set()
    still = [xb for xb in unmapped if xb.id not in resolved]
    if still:
        compact_index = brands_by_compact_key()
        for xb in still:
            key = normalize_compact_key(xb.name or "")
            if not key:
                continue
            brand = compact_index.get(key)
            if brand:
                resolved[xb.id] = brand
                compact_matched.add(xb.id)

    unresolved = [xb for xb in unmapped if xb.id not in resolved]
    brands_first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matched: typing.Set[int] = set()
    for xb in unresolved:
        parts = normalize_upper_words(xb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if parts:
            candidates = list(brands_first_index.get(parts[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(xb.name or "", candidates)
        if brand:
            resolved[xb.id] = brand
            fuzzy_matched.add(xb.id)

    if dry_run:
        for xb in unmapped:
            brand = resolved.get(xb.id)
            if not brand:
                continue
            how = (
                "exact" if xb.id in exact_matched
                else "compact" if xb.id in compact_matched
                else "fuzzy"
            )
            logger.info(
                "{} [dry-run] match ({}) XdpBrand id={} name={!r} -> Brand id={} name={!r}".format(
                    _LOG_PREFIX, how, xb.id, xb.name, brand.id, brand.name
                )
            )
        would_create = [xb for xb in unmapped if xb.id not in resolved]
        logger.info(
            "{} [dry-run] Summary: {} exact, {} compact, {} fuzzy, {} unmatched (would create "
            "Brand). No writes performed.".format(
                _LOG_PREFIX, len(exact_matched), len(compact_matched), len(fuzzy_matched),
                len(would_create),
            )
        )
        return unmapped

    new_brand_names = {
        (xb.name or "").strip().upper() or "BRAND_{}".format(xb.id)
        for xb in unmapped
        if xb.id not in resolved
    }
    created_brands = 0
    if new_brand_names:
        existing_names = set(
            src_models.Brands.objects.filter(name__in=list(new_brand_names)).values_list(
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
            b.name: b
            for b in src_models.Brands.objects.filter(name__in=list(new_brand_names))
        }
        for xb in unmapped:
            if xb.id not in resolved:
                nm = (xb.name or "").strip().upper() or "BRAND_{}".format(xb.id)
                resolved[xb.id] = by_name[nm]

    mapping_models = [
        src_models.BrandXdpBrandMapping(brand_id=resolved[xb.id].id, xdp_brand_id=xb.id)
        for xb in unmapped
        if xb.id in resolved
    ]
    pgbulk.upsert(
        src_models.BrandXdpBrandMapping,
        mapping_models,
        unique_fields=["brand", "xdp_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    for xb in unmapped:
        brand = resolved.get(xb.id)
        if not brand:
            continue
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand, provider=xdp_provider
        )
        if bp_created:
            created_brand_providers += 1

    logger.info(
        "{} Sync complete. Brands created: {}, exact: {}, compact: {}, fuzzy: {}, mappings "
        "upserted: {}, BrandProviders created: {}.".format(
            _LOG_PREFIX, created_brands, len(exact_matched), len(compact_matched),
            len(fuzzy_matched), len(mapping_models), created_brand_providers,
        )
    )
    return unmapped
