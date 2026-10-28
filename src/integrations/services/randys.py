"""
RANDYS Worldwide integration.

RANDYS are a distributor whose own brands (Yukon Gear & Axle, Zumbrota Drivetrain, USA Standard
Gear, ICON Vehicle Dynamics, Carli Suspension, Van Compass, Dynatrac, BulletProof Hitches, Adams
Driveshaft and a few more) make up the whole 17,314-part catalogue. The integration is split down
an unusual line, and the split is the thing to understand before changing anything here:

* **Catalogue, images and fitment are public and need no credentials.** They come from the
  Convermax index in front of their Shopify storefront, enriched from ``/products.json``. This is
  not scraping around a gap -- the index's item count matches Shopify's catalogue exactly, and it
  additionally carries category, RANDYS' differential taxonomy and real Year/Make/Model fitment
  rows that the storefront does not expose.
* **Stock arrives as an SFTP push** of ``Inventory_RWW_<YYMMDDHHMM>.csv``, two columns,
  ``Item,Stock``. ``Item`` joins to ``RandysPart.sku``. Quantity is RANDYS-wide rather than
  per-company -- their warehouse stock is the same number whichever dealer asks -- so it is read
  from the primary connection and written globally, exactly as Meyer's is.
* **Dealer cost does not exist here yet.** The push carries quantity only, and no wholesale
  figure is reachable publicly (their Storefront API token is scoped without inventory or cost).
  ``RandysPart.price`` is RANDYS' published *retail*, never a cost. When per-company cost arrives
  it belongs in its own ``RandysCompanyPricing`` table and in ``_PRICING_SYNC_KINDS``; nothing in
  this module should start inferring one in the meantime.

Two traps are worth reading before touching the parsers:

1. ``Year`` in a fitment row is a range-and-list string -- "1992 - 1994, 1996 - 1997, 1999,
   2001 - 2003" is a real measured value. ``expand_randys_year_field`` is the only thing that
   should read it; ``int()`` on it raises, and taking the first four characters silently discards
   most of a part's vehicle coverage.
2. ``sku`` is not unique: 71 skus are shared by two or more products. The inventory join is
   therefore one-to-many by design, and the catalogue is keyed on the Shopify product id.
"""
import csv
import logging
import re
import time
import typing
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pgbulk

from django.conf import settings
from django.db import connection
from django.db.models.functions import Upper
from django.utils import timezone

from src import constants as src_constants
from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.randys import client as randys_client
from src.integrations.clients.randys import exceptions as randys_exceptions
from src.integrations.clients.randys import sftp_client as randys_sftp_client
from src.integrations.utils.brand_matching import (
    best_fuzzy_brand_match,
    brands_by_compact_key,
    brands_by_first_token_upper,
    normalize_compact_key,
    normalize_upper_words,
)

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[RANDYS-SERVICES]"

CATALOG_UPSERT_BATCH = 2000
FITMENT_UPSERT_BATCH = 500
INVENTORY_UPSERT_BATCH = 2000
PRICING_UPSERT_BATCH = 2000
BATCH_DELAY_SECONDS = 0.3

# Inventory rows can be long once quoted; match the other feed parsers and lift the ceiling.
for _limit in (2 ** 31 - 1, 2 ** 27, 2 ** 20):
    try:
        csv.field_size_limit(_limit)
        break
    except OverflowError:
        continue

_YEAR_RANGE_RE = re.compile(r"^(\d{4})\s*(?:-|–|—|to)\s*(\d{4})$")
_YEAR_SINGLE_RE = re.compile(r"^(\d{4})$")
_MIN_MODEL_YEAR = 1900
_MAX_MODEL_YEAR = 2100


def _randys_provider() -> typing.Optional[src_models.Providers]:
    return src_models.Providers.objects.filter(
        kind=src_enums.BrandProviderKind.RANDYS_WORLDWIDE.value
    ).first()


def _active_randys_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.RANDYS_WORLDWIDE.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        active=True,
    ).select_related("company", "provider")


def _primary_randys_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    qs = _active_randys_company_providers_queryset()
    return qs.filter(primary=True).order_by("id").first() or qs.order_by("id").first()


# --------------------------------------------------------------------------- coercion helpers


def _clean_text(raw: typing.Any) -> typing.Optional[str]:
    if raw is None:
        return None
    text = str(raw).replace("\x00", "").strip()
    return text or None


def _dec(raw: typing.Any) -> typing.Optional[Decimal]:
    text = _clean_text(raw)
    if text is None:
        return None
    text = text.replace("$", "").replace(",", "")
    try:
        return Decimal(text)
    except (InvalidOperation, ValueError):
        return None


def _int(raw: typing.Any) -> typing.Optional[int]:
    text = _clean_text(raw)
    if text is None:
        return None
    try:
        return int(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def _first_str(raw: typing.Any) -> typing.Optional[str]:
    """Convermax returns several fields as single-element lists; take the first usable value."""
    if isinstance(raw, (list, tuple)):
        for entry in raw:
            text = _clean_text(entry)
            if text:
                return text
        return None
    return _clean_text(raw)


def _str_list(raw: typing.Any) -> typing.Optional[typing.List[str]]:
    if raw is None:
        return None
    values = raw if isinstance(raw, (list, tuple)) else [raw]
    out = [t for t in (_clean_text(v) for v in values) if t]
    return out or None


def expand_randys_year_field(raw: typing.Any) -> typing.List[int]:
    """
    Expand a RANDYS fitment ``Year`` value into the individual model years it covers.

    The field is a comma-separated list whose members are each either a single year or an
    inclusive range. Every shape below was measured on the live endpoint:

        "1992"                                    -> [1992]
        "1994 - 1997"                             -> [1994, 1995, 1996, 1997]
        "2000, 2002"                              -> [2000, 2002]
        "1975 - 1983, 1986"                       -> [1975..1983, 1986]
        "1992 - 1994, 1996 - 1997, 1999, 2001 - 2003"

    Anything that does not parse cleanly contributes nothing rather than guessing: a fabricated
    year is far more expensive than a missing one, since it tells a customer a part fits a vehicle
    it does not. Reversed and out-of-range bounds are rejected on the same principle.
    """
    text = _clean_text(raw)
    if not text:
        return []
    years: typing.Set[int] = set()
    for chunk in text.split(","):
        part = chunk.strip()
        if not part:
            continue
        single = _YEAR_SINGLE_RE.match(part)
        if single:
            year = int(single.group(1))
            if _MIN_MODEL_YEAR <= year <= _MAX_MODEL_YEAR:
                years.add(year)
            continue
        ranged = _YEAR_RANGE_RE.match(part)
        if ranged:
            start, end = int(ranged.group(1)), int(ranged.group(2))
            if start > end:
                continue
            if not (_MIN_MODEL_YEAR <= start <= _MAX_MODEL_YEAR):
                continue
            if not (_MIN_MODEL_YEAR <= end <= _MAX_MODEL_YEAR):
                continue
            years.update(range(start, end + 1))
            continue
        logger.debug("{} Unparseable fitment Year chunk {!r}.".format(_LOG_PREFIX, part))
    return sorted(years)


def _in_stock_from_index(item: typing.Dict) -> typing.Optional[bool]:
    """
    The index exposes stock only as a coarse facet: a ``stock`` list that contains "In-stock Only"
    for stocked parts, plus an ``out_of_stock`` marker on a handful. There is no public quantity
    anywhere, so this boolean is all that exists until the SFTP push lands.
    """
    if item.get("out_of_stock"):
        return False
    stock = item.get("stock")
    if isinstance(stock, (list, tuple)):
        labels = {str(s).strip().lower() for s in stock}
        if "in-stock only" in labels:
            return True
        if labels:
            return False
    return None


def _part_from_index_item(
    item: typing.Dict, brand_by_name: typing.Dict[str, src_models.RandysBrand]
) -> typing.Optional[src_models.RandysPart]:
    code = _clean_text(item.get("id"))
    if not code:
        return None
    vendor = _clean_text(item.get("vendor"))
    brand = brand_by_name.get(vendor) if vendor else None
    url = _clean_text(item.get("url"))
    return src_models.RandysPart(
        brand=brand,
        code=code,
        sku=_clean_text(item.get("sku")),
        title=_clean_text(item.get("title")),
        handle=_clean_text(item.get("handle")),
        url="{}{}".format(src_constants.RANDYS_SHOPIFY_BASE_URL, url) if url and url.startswith("/") else url,
        image_url=_clean_text(item.get("image")),
        price=_dec(item.get("price")),
        compare_at_price=_dec(item.get("compare_at_price")),
        in_stock=_in_stock_from_index(item),
        category=_first_str(item.get("category")),
        sub_category=_first_str(item.get("custom_sub_category")),
        fitment_type=_first_str(item.get("FitmentType")) or _first_str(item.get("Fitment")),
        differential_codes=_str_list(item.get("differential_codes")),
        differential_brands=_str_list(item.get("differential_brands")),
        wheel_diameter=_first_str(item.get("wheel_diameter")),
        wheel_width=_first_str(item.get("wheel_width")),
        wheel_offset=_first_str(item.get("wheel_offset")),
        wheel_bolt_pattern=_first_str(item.get("wheel_bolt_pattern")),
        raw_data=item,
    )


CATALOG_UPDATE_FIELDS = [
    "brand",
    "sku",
    "title",
    "handle",
    "url",
    "image_url",
    "price",
    "compare_at_price",
    "in_stock",
    "category",
    "sub_category",
    "fitment_type",
    "differential_codes",
    "differential_brands",
    "wheel_diameter",
    "wheel_width",
    "wheel_offset",
    "wheel_bolt_pattern",
    "raw_data",
    "updated_at",
]

FITMENT_UPDATE_FIELDS = [
    "fitment_rows",
    "fitment_row_count",
    "fitment_truncated",
    "fitment_synced_at",
    "updated_at",
]


def _flush(parts: typing.List[src_models.RandysPart], update_fields: typing.List[str]) -> int:
    if not parts:
        return 0
    now = timezone.now()
    for part in parts:
        part.updated_at = now
    pgbulk.upsert(
        src_models.RandysPart,
        parts,
        unique_fields=["code"],
        update_fields=update_fields,
        returning=False,
    )
    written = len(parts)
    parts.clear()
    connection.close()
    time.sleep(BATCH_DELAY_SECONDS)
    return written


# --------------------------------------------------------------------------- catalogue


def fetch_and_save_randys_catalog() -> int:
    """
    Sweep the public Convermax index into RandysBrand/RandysPart.

    No credentials are involved, so this runs whether or not any company has connected RANDYS --
    the catalogue is the same for everyone and only stock and (later) cost are per-connection.
    """
    logger.info("{} Fetching RANDYS catalogue from Convermax.".format(_LOG_PREFIX))
    client = randys_client.RandysClient()
    items = client.fetch_catalog()
    if not items:
        logger.warning("{} Catalogue sweep returned nothing. Skipping.".format(_LOG_PREFIX))
        return 0

    vendor_names = sorted({v for v in (_clean_text(i.get("vendor")) for i in items) if v})
    if vendor_names:
        pgbulk.upsert(
            src_models.RandysBrand,
            [src_models.RandysBrand(name=name) for name in vendor_names],
            unique_fields=["name"],
            update_fields=[],
            returning=False,
        )
    brand_by_name = {
        b.name: b for b in src_models.RandysBrand.objects.filter(name__in=vendor_names)
    }
    logger.info("{} {} vendor brands present.".format(_LOG_PREFIX, len(brand_by_name)))

    buffer: typing.List[src_models.RandysPart] = []
    seen_codes: typing.Set[str] = set()
    total = 0
    skipped = 0
    for item in items:
        part = _part_from_index_item(item, brand_by_name)
        if part is None:
            skipped += 1
            continue
        # Postgres rejects two rows with the same conflict target in one INSERT, so dedupe
        # within the batch; the sweep already deduped by id, this guards a repeated page.
        if part.code in seen_codes:
            continue
        seen_codes.add(part.code)
        buffer.append(part)
        if len(buffer) >= CATALOG_UPSERT_BATCH:
            total += _flush(buffer, CATALOG_UPDATE_FIELDS)
            logger.info("{} Upserted {} parts so far.".format(_LOG_PREFIX, total))

    total += _flush(buffer, CATALOG_UPDATE_FIELDS)
    logger.info(
        "{} Catalogue ingest complete. Parts upserted: {}, skipped (no id): {}.".format(
            _LOG_PREFIX, total, skipped
        )
    )
    return total


def enrich_randys_catalog_from_shopify() -> int:
    """
    Fill in what the index does not carry: description (``body_html``), the full image list, tags
    and per-variant grams. Joined on the Shopify product id, which is ``RandysPart.code``.

    Update-only by design -- ``bulk_update`` keyed on the primary key rather than an upsert, so a
    Shopify product we have never seen in the index cannot insert a half-populated catalogue row
    (same reason XDP's Miva enrichment does it this way). It also never touches the catalogue
    columns, so a Shopify hiccup cannot damage an otherwise good sweep.
    """
    logger.info("{} Enriching RANDYS catalogue from Shopify.".format(_LOG_PREFIX))
    client = randys_client.RandysClient()

    enrichment_by_code: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
    for products in client.iter_shopify_products():
        for product in products:
            code = _clean_text(product.get("id"))
            if not code:
                continue
            images = [
                u for u in (_clean_text(img.get("src")) for img in (product.get("images") or []))
                if u
            ]
            variants = product.get("variants") or []
            enrichment_by_code[code] = {
                "description": _clean_text(product.get("body_html")),
                "image_urls": images or None,
                "tags": _str_list(product.get("tags")),
                "weight_grams": _int((variants[0] or {}).get("grams")) if variants else None,
            }
    if not enrichment_by_code:
        logger.warning("{} Shopify returned no products. Nothing to enrich.".format(_LOG_PREFIX))
        return 0

    fields = ["description", "image_urls", "tags", "weight_grams"]
    total = 0
    last_id = 0
    while True:
        rows = list(
            src_models.RandysPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "code", *fields)[:CATALOG_UPSERT_BATCH]
        )
        if not rows:
            break
        last_id = rows[-1]["id"]
        to_update = []
        for row in rows:
            enrichment = enrichment_by_code.get(row["code"])
            if not enrichment:
                continue
            if all(row.get(f) == enrichment.get(f) for f in fields):
                continue
            to_update.append(src_models.RandysPart(id=row["id"], **enrichment))
        if to_update:
            src_models.RandysPart.objects.bulk_update(to_update, fields, batch_size=1000)
            total += len(to_update)
        connection.close()

    logger.info(
        "{} Shopify enrichment complete. Parts enriched: {} (of {} Shopify products).".format(
            _LOG_PREFIX, total, len(enrichment_by_code)
        )
    )
    return total


# --------------------------------------------------------------------------- fitment


def fetch_and_save_randys_fitment(
    only_missing: bool = False,
    max_workers: typing.Optional[int] = None,
) -> int:
    """
    Pull Year/Make/Model/Drive/Differential rows for every part that has any, storing them
    verbatim on ``RandysPart.fitment_rows``.

    One request per part -- there is no batch endpoint. Convermax rate-limits this endpoint on
    sustained volume even though short bursts measure clean, so the worker count is deliberately
    modest (4 by default, ~11 req/s, roughly 25 minutes for the catalogue) and the client backs
    off on 429 rather than retrying immediately. ``only_missing`` skips parts that already have
    rows, which is how an interrupted or rate-limited run is resumed.

    Parts whose ``fitment_type`` says they have no vehicle data are not requested at all.
    """
    workers = int(
        max_workers
        if max_workers is not None
        else getattr(settings, "RANDYS_FITMENT_MAX_WORKERS", 4) or 4
    )
    queryset = src_models.RandysPart.objects.exclude(fitment_type="No Fitment Data")
    if only_missing:
        queryset = queryset.filter(fitment_rows__isnull=True)
    targets = list(queryset.values_list("code", flat=True).order_by("code"))
    if not targets:
        logger.info("{} No parts need fitment. Nothing to do.".format(_LOG_PREFIX))
        return 0

    logger.info(
        "{} Fetching fitment for {} parts at {} workers.".format(_LOG_PREFIX, len(targets), workers)
    )
    client = randys_client.RandysClient()

    def fetch(code: str):
        try:
            rows, truncated = client.fetch_fitments(code)
            return code, rows, truncated, None
        except Exception as e:
            return code, None, False, e

    buffer: typing.List[src_models.RandysPart] = []
    total = 0
    failures = 0
    truncated_count = 0
    now = timezone.now()
    for index in range(0, len(targets), FITMENT_UPSERT_BATCH):
        chunk = targets[index:index + FITMENT_UPSERT_BATCH]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for code, rows, truncated, error in pool.map(fetch, chunk):
                if error is not None:
                    failures += 1
                    logger.warning(
                        "{} Fitment fetch failed for code={}: {}.".format(_LOG_PREFIX, code, error)
                    )
                    continue
                if truncated:
                    truncated_count += 1
                    logger.warning(
                        "{} Fitment for code={} hit the {}-row cap; rows beyond it are "
                        "unreachable (the endpoint supports no paging).".format(
                            _LOG_PREFIX, code, src_constants.RANDYS_CONVERMAX_FITMENT_ROW_CAP
                        )
                    )
                buffer.append(
                    src_models.RandysPart(
                        code=code,
                        fitment_rows=rows or None,
                        fitment_row_count=len(rows or []),
                        fitment_truncated=bool(truncated),
                        fitment_synced_at=now,
                    )
                )
        total += _flush(buffer, FITMENT_UPDATE_FIELDS)
        logger.info("{} Fitment stored for {} parts so far.".format(_LOG_PREFIX, total))

    logger.info(
        "{} Fitment ingest complete. Parts updated: {}, failures: {}, truncated: {}.".format(
            _LOG_PREFIX, total, failures, truncated_count
        )
    )
    return total


# --------------------------------------------------------------------------- inventory


def _iter_inventory_rows(path: str) -> typing.Iterator[typing.Tuple[str, typing.Optional[int]]]:
    """
    Yield ``(item, stock)`` from the pushed CSV.

    The measured file is ``Item,Stock`` with a header and quoted item numbers that may contain
    spaces ("AK R-J01"). Header presence is detected rather than assumed, so a headerless
    delivery still parses instead of silently dropping its first part -- NTW's feed arrives
    headerless and this costs nothing to tolerate.
    """
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        for row in reader:
            if not row:
                continue
            item = _clean_text(row[0])
            if not item:
                continue
            if item.lower() == "item":
                continue
            stock = _int(row[1]) if len(row) > 1 else None
            yield item, stock


def apply_randys_inventory_file(
    force_download: bool = False,
    local_path: typing.Optional[str] = None,
    zero_missing: bool = False,
) -> int:
    """
    Read the newest pushed inventory file and write quantities onto RandysPart.

    Quantity is RANDYS-wide, not per-company, so this reads the *primary* connection's folder and
    writes global quantities -- the same arrangement Meyer uses, and the reason
    ``ProviderPartInventory`` has no company column downstream.

    The ``Item`` -> ``sku`` join is one-to-many by design: 71 skus are shared by two or more
    products, and a quantity for a sku is true for every product carrying it. Walking the parts
    table and looking each sku up in the file (rather than the reverse) makes that fan-out fall
    out naturally, and keeps the pass update-only -- a stock line for an item we have no
    catalogue row for can never insert one.

    ``zero_missing`` is off by default and should stay off until a full file has been inspected.
    Several distributors ship in-stock-only files, and treating absence as zero would wipe the
    catalogue's quantities the first time one arrived. Rows present in the file are updated and
    ``inventory_synced_at`` stamped either way, so staleness stays visible.
    """
    path = local_path
    if not path:
        company_provider = _primary_randys_company_provider()
        if not company_provider:
            logger.warning(
                "{} No active RANDYS connection; cannot locate an inventory push. "
                "Catalogue is unaffected.".format(_LOG_PREFIX)
            )
            return 0
        feed_credentials = credentials_helper.get_feed_credentials(company_provider)
        client = randys_sftp_client.RandysSFTPClient(credentials=feed_credentials)
        try:
            path = client.download_inventory_file(force_download=force_download)
        except randys_exceptions.RandysFileNotFoundError:
            logger.warning(
                "{} No inventory file has landed for company_id={} yet.".format(
                    _LOG_PREFIX, company_provider.company_id
                )
            )
            return 0

    logger.info("{} Applying RANDYS inventory from {}.".format(_LOG_PREFIX, path))

    stock_by_item: typing.Dict[str, typing.Optional[int]] = {}
    for item, stock in _iter_inventory_rows(path):
        stock_by_item[item] = stock
    if not stock_by_item:
        logger.warning("{} Inventory file {} held no usable rows.".format(_LOG_PREFIX, path))
        return 0
    # Case-insensitive fallback only; RANDYS' skus are mixed case ("YGK068stg2") and an
    # upper-cased primary index could collide two genuinely distinct skus.
    stock_by_item_upper = {k.upper(): v for k, v in stock_by_item.items()}

    now = timezone.now()
    total = 0
    matched_file_keys: typing.Set[str] = set()
    missing_codes: typing.List[int] = []
    last_id = 0
    while True:
        rows = list(
            src_models.RandysPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "sku", "quantity")[:INVENTORY_UPSERT_BATCH]
        )
        if not rows:
            break
        last_id = rows[-1]["id"]
        to_update = []
        for row in rows:
            sku = row.get("sku")
            if not sku:
                continue
            if sku in stock_by_item:
                stock = stock_by_item[sku]
                matched_file_keys.add(sku.upper())
            elif sku.upper() in stock_by_item_upper:
                stock = stock_by_item_upper[sku.upper()]
                matched_file_keys.add(sku.upper())
            else:
                missing_codes.append(row["id"])
                continue
            to_update.append(
                src_models.RandysPart(
                    id=row["id"], quantity=stock, inventory_synced_at=now
                )
            )
        if to_update:
            src_models.RandysPart.objects.bulk_update(
                to_update, ["quantity", "inventory_synced_at"], batch_size=1000
            )
            total += len(to_update)
        connection.close()

    if zero_missing and missing_codes:
        zeroed = 0
        for index in range(0, len(missing_codes), INVENTORY_UPSERT_BATCH):
            chunk = missing_codes[index:index + INVENTORY_UPSERT_BATCH]
            zeroed += (
                src_models.RandysPart.objects.filter(id__in=chunk)
                .exclude(quantity=0)
                .update(quantity=0, inventory_synced_at=now)
            )
        logger.info("{} zero_missing: zeroed {} absent parts.".format(_LOG_PREFIX, zeroed))

    unmatched_file_items = len({k.upper() for k in stock_by_item}) - len(matched_file_keys)
    logger.info(
        "{} Inventory applied. Parts updated: {}, file rows: {}, file items matching no part: "
        "{}, parts with no stock line: {}.".format(
            _LOG_PREFIX, total, len(stock_by_item), unmatched_file_items, len(missing_codes)
        )
    )
    return total


# --------------------------------------------------------------------------- per-company pricing


def _normalize_header(raw: typing.Any) -> str:
    """Lower-case and strip spaces/underscores/hyphens so "Your Price" == "yourprice"."""
    return re.sub(r"[\s_\-]+", "", str(raw or "")).strip().lower()


def _map_pricing_columns(
    fieldnames: typing.Sequence[str],
) -> typing.Dict[str, typing.Optional[str]]:
    """
    Map a delivered pricing file's header onto the fields we store.

    Header-driven rather than positional because RANDYS have not delivered a pricing file yet and
    the column names are therefore unknown -- see src.constants. Whatever they call the cost
    column, as long as it is one of the documented candidates the file is read; anything
    unrecognised is reported by name so the candidate list can be extended in one place.
    """
    found: typing.Dict[str, typing.Optional[str]] = {
        "item": None, "cost": None, "jobber_price": None, "map_price": None, "retail_price": None,
    }
    candidates = (
        ("item", src_constants.RANDYS_PRICING_ITEM_COLUMNS),
        ("cost", src_constants.RANDYS_PRICING_COST_COLUMNS),
        ("jobber_price", src_constants.RANDYS_PRICING_JOBBER_COLUMNS),
        ("map_price", src_constants.RANDYS_PRICING_MAP_COLUMNS),
        ("retail_price", src_constants.RANDYS_PRICING_RETAIL_COLUMNS),
    )
    for name in (fieldnames or []):
        normalized = _normalize_header(name)
        for field, accepted in candidates:
            if found[field] is None and normalized in accepted:
                found[field] = name
                break
    return found


def _iter_pricing_rows(
    path: str,
) -> typing.Iterator[typing.Tuple[str, typing.Dict[str, typing.Optional[Decimal]]]]:
    """
    Yield ``(item, {cost, jobber_price, map_price, retail_price})`` from a delivered pricing file.

    Raises RandysDataValidationError if the header carries no recognisable item or cost column:
    a pricing file we cannot read must fail loudly rather than quietly writing nulls over a
    company's real costs.
    """
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = _map_pricing_columns(reader.fieldnames or [])
        if not columns["item"] or not columns["cost"]:
            raise randys_exceptions.RandysDataValidationError(
                "RANDYS pricing file {} has no recognisable {} column. Header was: {}. Extend "
                "the candidate lists in src.constants (RANDYS_PRICING_*_COLUMNS).".format(
                    path,
                    "item" if not columns["item"] else "cost",
                    list(reader.fieldnames or []),
                )
            )
        for row in reader:
            item = _clean_text(row.get(columns["item"]))
            if not item:
                continue
            yield item, {
                field: (_dec(row.get(columns[field])) if columns[field] else None)
                for field in ("cost", "jobber_price", "map_price", "retail_price")
            }


def _parse_markup_percent(raw: typing.Any) -> typing.Optional[Decimal]:
    if raw is None or raw == "":
        return None
    try:
        value = Decimal(str(raw).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if value < 0:
        value = Decimal(0)
    if value > 100:
        value = Decimal(100)
    return value


def markup_percent_for_company(
    credentials: typing.Optional[typing.Dict],
) -> typing.Optional[Decimal]:
    """
    Markup percent (0-100) off RANDYS' retail, from
    ``CompanyProviders.credentials["feed"]["markup"]``, or None when the company has not set one.

    Returns None rather than XDP's defensive 0% precisely because this field is optional here:
    XDP requires it at connect time, so a missing value there is corruption and 0% (cost ==
    retail) is the safe reading. For RANDYS a missing value is the normal state of a company
    waiting on a real pricing file, and silently pricing their cost at RANDYS' retail would be a
    worse answer than declining to quote a cost at all.
    """
    return _parse_markup_percent((credentials or {}).get(src_constants.RANDYS_CREDENTIALS_MARKUP))


def dealer_cost_from_retail(
    price: typing.Optional[typing.Any],
    markup_percent: typing.Optional[Decimal],
) -> typing.Optional[Decimal]:
    """``cost = price * (1 - markup / 100)``, or None if either input is missing/invalid."""
    if price is None or markup_percent is None:
        return None
    try:
        retail = price if isinstance(price, Decimal) else Decimal(str(price).strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    if retail < 0:
        retail = Decimal(0)
    cost = retail * (Decimal(1) - (markup_percent / Decimal(100)))
    return cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def sync_randys_company_pricing_for_company_provider(
    company_provider_id: int,
    force_download: bool = True,
) -> int:
    """
    Populate RandysCompanyPricing for one connection -- the Phase A half of the standard
    IntegrationPricingSyncJob, the same position Meyer's and Keystone's pricing sync occupy.

    Cost comes from one of two places, and ``cost_source`` always records which:

    1. ``"feed"`` -- a per-company pricing file RANDYS delivered. Always wins when present.
    2. ``"markup"`` -- the company's own markup off RANDYS' published retail, the stand-in XDP
       and Thoroughbred use. RANDYS' push carries ``Item,Stock`` only today, so until a pricing
       file arrives this is what gives a connected company a real cost basis.

    A company with neither gets **no rows at all**, and that is the point of the design rather
    than an oversight: writing retail with a null cost would put RANDYS' own retail price in
    front of the dealer as though it were their cost, which is worse than showing no price.
    ``retail_price``/``msrp`` are still carried as the published reference, but only alongside a
    cost we can actually stand behind.
    """
    company_provider = (
        src_models.CompanyProviders.objects.filter(
            id=company_provider_id,
            provider__kind=src_enums.BrandProviderKind.RANDYS_WORLDWIDE.value,
            provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        )
        .select_related("company", "provider")
        .first()
    )
    if not company_provider:
        logger.warning(
            "{} No active RANDYS CompanyProviders id={}. Skipping.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return 0

    company_id = company_provider.company_id
    feed_credentials = dict(credentials_helper.get_feed_credentials(company_provider))
    markup_percent = markup_percent_for_company(feed_credentials)

    # ---- the pricing file, if there is one ----
    pricing_by_item: typing.Dict[str, typing.Dict[str, typing.Optional[Decimal]]] = {}
    try:
        client = randys_sftp_client.RandysSFTPClient(credentials=feed_credentials)
    except ValueError as e:
        logger.warning("{} company_id={}: {}".format(_LOG_PREFIX, company_id, str(e)))
        client = None

    if client is not None:
        try:
            pricing_path = client.download_pricing_file(force_download=force_download)
            for item, values in _iter_pricing_rows(pricing_path):
                pricing_by_item[item] = values
            logger.info(
                "{} Read {} pricing rows for company_id={} from {}.".format(
                    _LOG_PREFIX, len(pricing_by_item), company_id, pricing_path
                )
            )
        except randys_exceptions.RandysFileNotFoundError:
            logger.info(
                "{} No RANDYS pricing file for company_id={} yet.".format(_LOG_PREFIX, company_id)
            )
        except randys_exceptions.RandysSFTPConnectionError as e:
            # Falling back to markup for a transient relay outage would quietly replace real
            # delivered costs with derived ones, so bail instead and keep the last good rows.
            logger.error(
                "{} RANDYS pricing download failed for company_id={}: {}. Leaving existing "
                "pricing untouched.".format(_LOG_PREFIX, company_id, str(e))
            )
            return 0

    if not pricing_by_item and markup_percent is None:
        logger.warning(
            "{} company_id={} has no RANDYS pricing file and no '{}' credential, so there is no "
            "cost basis. Writing no pricing rows -- the company sees RANDYS parts with no price "
            "rather than RANDYS' retail presented as their cost. Set a markup on the connection "
            "to price them in the meantime.".format(
                _LOG_PREFIX, company_id, src_constants.RANDYS_CREDENTIALS_MARKUP
            )
        )
        return 0

    pricing_by_item_upper = {k.upper(): v for k, v in pricing_by_item.items()}

    now = timezone.now()
    total = 0
    from_feed = 0
    from_markup = 0
    skipped_no_cost = 0
    matched_file_keys: typing.Set[str] = set()
    last_id = 0
    while True:
        rows = list(
            src_models.RandysPart.objects.filter(id__gt=last_id)
            .order_by("id")
            .values("id", "sku", "price", "compare_at_price")[:PRICING_UPSERT_BATCH]
        )
        if not rows:
            break
        last_id = rows[-1]["id"]

        to_upsert = []
        for row in rows:
            sku = row.get("sku")
            values: typing.Dict[str, typing.Optional[Decimal]] = {}
            if sku:
                if sku in pricing_by_item:
                    values = pricing_by_item[sku]
                    matched_file_keys.add(sku.upper())
                elif sku.upper() in pricing_by_item_upper:
                    values = pricing_by_item_upper[sku.upper()]
                    matched_file_keys.add(sku.upper())

            retail = values.get("retail_price") or row.get("price")
            cost = values.get("cost")
            cost_source = "feed" if cost is not None else None
            markup_applied = None
            if cost is None:
                cost = dealer_cost_from_retail(retail, markup_percent)
                if cost is not None:
                    cost_source = "markup"
                    markup_applied = markup_percent

            if cost is None:
                # No delivered cost and no usable retail to mark up -- skip rather than write a
                # priceless row carrying RANDYS' retail.
                skipped_no_cost += 1
                continue

            if cost_source == "feed":
                from_feed += 1
            else:
                from_markup += 1

            to_upsert.append(
                src_models.RandysCompanyPricing(
                    part_id=row["id"],
                    company_id=company_id,
                    cost=cost,
                    jobber_price=values.get("jobber_price"),
                    map_price=values.get("map_price"),
                    retail_price=retail,
                    markup_percent_applied=markup_applied,
                    cost_source=cost_source,
                    updated_at=now,
                )
            )
        if to_upsert:
            pgbulk.upsert(
                src_models.RandysCompanyPricing,
                to_upsert,
                unique_fields=["part", "company"],
                update_fields=[
                    "cost", "jobber_price", "map_price", "retail_price",
                    "markup_percent_applied", "cost_source", "updated_at",
                ],
                returning=False,
            )
            total += len(to_upsert)
        connection.close()
        time.sleep(BATCH_DELAY_SECONDS)

    unmatched = len({k.upper() for k in pricing_by_item}) - len(matched_file_keys)
    logger.info(
        "{} RANDYS company pricing complete for company_id={}. Rows upserted: {} "
        "(feed cost: {}, markup-derived: {}), skipped for no cost basis: {}, pricing-file items "
        "matching no part: {}.".format(
            _LOG_PREFIX, company_id, total, from_feed, from_markup, skipped_no_cost, unmatched
        )
    )
    return total


# --------------------------------------------------------------------------- brand mapping


def _ampersand_compact_keys(name: str) -> typing.Set[str]:
    """
    Compact keys for ``name`` with "&" and "and" treated as the same word.

    RANDYS are the reason this exists: their index carries "Yukon Gear and Axle" (4,887 parts) and
    "Yukon Gear & Axle" (11) as two separate vendor strings for one brand, and our Brands table
    already held it as "YUKON GEAR & AXLE". The shared compact key strips "&" to nothing, so
    "YUKONGEARAXLE" and "YUKONGEARANDAXLE" never meet and the big vendor lands on a brand-new
    duplicate brand -- measured: 4,866 master parts created under a second Yukon row rather than
    matching the 10,968 already on the real one.

    Deliberately local rather than a change to brand_matching.normalize_compact_key: that key is
    the identity rung for every provider's brand resolution, and widening it would silently
    re-point brands well outside RANDYS.
    """
    upper = (name or "").upper()
    variants = {
        upper,
        upper.replace("&", " AND "),
        re.sub(r"\bAND\b", " & ", upper),
    }
    return {k for k in (normalize_compact_key(v) for v in variants) if k}


def sync_unmapped_randys_brands_to_brands(
    dry_run: bool = False,
) -> typing.List[src_models.RandysBrand]:
    """
    For each RandysBrand with no BrandRandysBrandMapping: resolve Brands by exact name
    (uppercase), then compact key (punctuation/spacing-only differences), then fuzzy word-prefix
    match; otherwise create a Brands row.

    No AAIA phase -- the index carries a bare vendor string with no AAIA code anywhere, the same
    position WheelPros, Thoroughbred and XDP are in. The compact-key rung is what collapses
    RANDYS' own near-duplicate vendors ("Yukon Gear and Axle" vs "Yukon Gear & Axle") onto one
    Brands row.
    """
    logger.info(
        "{} Syncing unmapped RANDYS brands to Brands{}.".format(
            _LOG_PREFIX, " (dry run)" if dry_run else ""
        )
    )
    randys_provider = _randys_provider()
    if not randys_provider:
        logger.warning("{} RANDYS provider not found. Skipping sync.".format(_LOG_PREFIX))
        return []

    mapped_ids = set(
        src_models.BrandRandysBrandMapping.objects.values_list(
            "randys_brand_id", flat=True
        ).distinct()
    )
    unmapped = list(src_models.RandysBrand.objects.exclude(id__in=mapped_ids).order_by("id"))
    if not unmapped:
        logger.info("{} No unmapped RANDYS brands. Nothing to sync.".format(_LOG_PREFIX))
        return []

    name_upper_keys = {(b.name or "").strip().upper() for b in unmapped if (b.name or "").strip()}
    brands_by_upper_name: typing.Dict[str, src_models.Brands] = {}
    if name_upper_keys:
        for brand in (
            src_models.Brands.objects.annotate(_name_u=Upper("name"))
            .filter(_name_u__in=name_upper_keys)
            .order_by("id")
        ):
            brands_by_upper_name.setdefault((brand.name or "").strip().upper(), brand)

    resolved: typing.Dict[int, src_models.Brands] = {}
    exact_matched: typing.Set[int] = set()
    for rb in unmapped:
        name = (rb.name or "").strip().upper()
        if name and name in brands_by_upper_name:
            resolved[rb.id] = brands_by_upper_name[name]
            exact_matched.add(rb.id)

    compact_matched: typing.Set[int] = set()
    still = [rb for rb in unmapped if rb.id not in resolved]
    if still:
        compact_index = brands_by_compact_key()
        for rb in still:
            key = normalize_compact_key(rb.name or "")
            if not key:
                continue
            brand = compact_index.get(key)
            if brand:
                resolved[rb.id] = brand
                compact_matched.add(rb.id)

    # "&" / "and" equivalence, before fuzzy -- see _ampersand_compact_keys for why this rung
    # exists at all and why it is not in the shared normalizer.
    ampersand_matched: typing.Set[int] = set()
    still = [rb for rb in unmapped if rb.id not in resolved]
    if still:
        compact_index = brands_by_compact_key()
        for rb in still:
            for key in _ampersand_compact_keys(rb.name or ""):
                brand = compact_index.get(key)
                if brand:
                    resolved[rb.id] = brand
                    ampersand_matched.add(rb.id)
                    break

    unresolved = [rb for rb in unmapped if rb.id not in resolved]
    brands_first_index = brands_by_first_token_upper() if unresolved else {}
    all_brands_fallback: typing.Optional[typing.List[src_models.Brands]] = None
    fuzzy_matched: typing.Set[int] = set()
    for rb in unresolved:
        words = normalize_upper_words(rb.name or "").split()
        candidates: typing.List[src_models.Brands] = []
        if words:
            candidates = list(brands_first_index.get(words[0], ()))
        if not candidates:
            if all_brands_fallback is None:
                all_brands_fallback = list(
                    src_models.Brands.objects.only("id", "name", "aaia_code").order_by("id")
                )
            candidates = all_brands_fallback
        brand = best_fuzzy_brand_match(rb.name or "", candidates)
        if brand:
            resolved[rb.id] = brand
            fuzzy_matched.add(rb.id)

    if dry_run:
        for rb in unmapped:
            brand = resolved.get(rb.id)
            if not brand:
                continue
            how = (
                "exact" if rb.id in exact_matched
                else "compact" if rb.id in compact_matched
                else "ampersand" if rb.id in ampersand_matched
                else "fuzzy"
            )
            logger.info(
                "{} [dry-run] match ({}) RandysBrand id={} name={!r} -> Brand id={} "
                "name={!r}".format(_LOG_PREFIX, how, rb.id, rb.name, brand.id, brand.name)
            )
        would_create = [rb for rb in unmapped if rb.id not in resolved]
        logger.info(
            "{} [dry-run] Summary: {} exact, {} compact, {} ampersand, {} fuzzy, {} unmatched "
            "(would create Brand). No writes performed.".format(
                _LOG_PREFIX, len(exact_matched), len(compact_matched), len(ampersand_matched),
                len(fuzzy_matched), len(would_create),
            )
        )
        return unmapped

    new_brand_names = {
        (rb.name or "").strip().upper() or "BRAND_{}".format(rb.id)
        for rb in unmapped
        if rb.id not in resolved
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
            b.name: b for b in src_models.Brands.objects.filter(name__in=list(new_brand_names))
        }
        for rb in unmapped:
            if rb.id not in resolved:
                name = (rb.name or "").strip().upper() or "BRAND_{}".format(rb.id)
                if name in by_name:
                    resolved[rb.id] = by_name[name]

    mapping_models = [
        src_models.BrandRandysBrandMapping(brand_id=resolved[rb.id].id, randys_brand_id=rb.id)
        for rb in unmapped
        if rb.id in resolved
    ]
    pgbulk.upsert(
        src_models.BrandRandysBrandMapping,
        mapping_models,
        unique_fields=["brand", "randys_brand"],
        update_fields=[],
        returning=False,
    )

    created_brand_providers = 0
    for rb in unmapped:
        brand = resolved.get(rb.id)
        if not brand:
            continue
        _, bp_created = src_models.BrandProviders.objects.get_or_create(
            brand=brand, provider=randys_provider
        )
        if bp_created:
            created_brand_providers += 1

    logger.info(
        "{} Sync complete. Brands created: {}, exact: {}, compact: {}, ampersand: {}, fuzzy: "
        "{}, mappings upserted: {}, BrandProviders created: {}.".format(
            _LOG_PREFIX, created_brands, len(exact_matched), len(compact_matched),
            len(ampersand_matched), len(fuzzy_matched), len(mapping_models),
            created_brand_providers,
        )
    )
    return unmapped
