"""
Motor State FTP feed ingest — the catalog and pricing source of record.

Replaces the API ingest that used to build the catalog (an epoch-dated
``ProductAvailabilityChange`` pass per brand to enumerate part numbers, then ~10.4k
``/api/Product`` calls to hydrate them). One ~20-40MB CSV per account carries the whole
catalog, and carries fields the API has none of at all: UPC, dimensions, weight, MAP, MSRP,
manufacturer part number, shipping/restriction flags, and — on enriched accounts — image URL,
a three-level category taxonomy, and a full-length description. ``src.integrations.services.motorstate``
keeps the API client for ordering.

Two modes, both reading the same file shape:

  * :func:`fetch_and_save_motorstate_catalog_from_feed` — primary connection only. Writes the
    distributor-wide catalog (MotorStateProduct), including stock and status. Enriched columns
    are written only when that account's file actually has them.
  * :func:`sync_motorstate_company_pricing_from_feed` — every active connection, primary
    included. Writes only that company's MotorStateCompanyPricing rows from its own file's
    ``Cost`` / ``Jobber`` / ``SuggestedRetail`` / ``MapPrice`` / ``VendorMSRP``.

Brand resolution needs no feed-specific mapping table: the first three characters of Motor
State's part number are its brand code (``AAA00004`` -> ``AAA`` -> "A-1 PRODUCTS"), which
resolves ~98.5% of rows against MotorStateBrand. Rows whose prefix is not a known brand are
ingested with the prefix recorded and the FK left null; :func:`sync_motorstate_brands_from_feed`
creates the missing MotorStateBrand rows so the next run attaches them.
"""
import csv
import datetime
import decimal
import logging
import sys
import time
import typing

import pgbulk
from django.conf import settings
from django.db import connection
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.motorstate import exceptions as motorstate_exceptions
from src.integrations.clients.motorstate import feed_spec
from src.integrations.clients.motorstate import ftp_client as motorstate_ftp_client

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[MOTOR-STATE-FEED]"

# Long descriptions and note fields can exceed the csv module's default 131072-byte field
# limit; raise it as high as the platform allows (same guard as the A-Tech feed reader).
_maxint = sys.maxsize
while True:
    try:
        csv.field_size_limit(_maxint)
        break
    except OverflowError:
        _maxint = int(_maxint / 10)

# The file is UTF-8 with a BOM in both live samples; the rest are fallbacks for a vendor-side
# encoding change, tried in order until one decodes the whole file.
_FEED_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")

FEED_STREAMING_BATCH_SIZE = 5000
FEED_UPSERT_DELAY = 0.05
FEED_PARSE_PROGRESS_EVERY = 25000

# Motor State's brand code is the first three characters of its part number.
_BRAND_CODE_LENGTH = 3

_YES_VALUES = {"YES", "Y", "TRUE", "T", "1"}
_NO_VALUES = {"NO", "N", "FALSE", "F", "0"}

_CATALOG_UPDATE_FIELDS = [
    "brand", "brand_code", "found", "vendor_part_number", "short_description",
    "status_type", "is_stocking", "quantity", "upc", "aaia_code",
    "length", "width", "height", "weight",
    "air_restricted", "state_restricted", "truck_freight_only", "ship_alone",
    "canada_restricted", "emissions_warning", "oversized", "notes", "acquired_date",
    "feed_updated_at", "updated_at",
]

# Enriched columns are appended to the update list only when the file actually carries them,
# so a plain-account run can never null out content an enriched run already wrote.
_ENRICHED_UPDATE_FIELDS = {
    "long_description": "long_description",
    "image_url": "image_url",
    "category_level_1": "category_level_1",
    "category_level_2": "category_level_2",
    "category_level_3": "category_level_3",
}

_COMPANY_PRICING_UPDATE_FIELDS = [
    "customer_price", "base_price", "list_price", "map_price", "vendor_msrp",
    "is_map_restricted", "feed_updated_at", "updated_at",
]


# ---------------------------------------------------------------------------
# Value coercion
# ---------------------------------------------------------------------------
def _clean(value: typing.Any) -> typing.Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _to_decimal(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    s = _clean(value)
    if s is None:
        return None
    s = s.replace("$", "").replace(",", "")
    try:
        d = decimal.Decimal(s)
    except (decimal.InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


# Dimension columns are numeric(12,5) -- seven integer digits. Motor State occasionally emits
# a junk figure far beyond that (one live row lists a 788,120,000,000 lb weight), which would
# abort the whole batch on overflow. Such a value is not a big part, it is a broken record, so
# it is dropped rather than clamped: a clamped 9,999,999 lb weight would quietly feed a wrong
# number into freight quoting, where a null is visibly missing data.
_DIMENSION_MAX = decimal.Decimal("9999999.99999")


def _to_dimension(value: typing.Any, part_number: typing.Any = None, column: str = "") -> typing.Optional[decimal.Decimal]:
    d = _to_decimal(value)
    if d is None:
        return None
    if abs(d) > _DIMENSION_MAX:
        logger.warning(
            "{} Discarding out-of-range {} for part {}: {}.".format(
                _LOG_PREFIX, column or "dimension", _clean(part_number) or "?", d
            )
        )
        return None
    return d


def _to_int(value: typing.Any) -> typing.Optional[int]:
    s = _clean(value)
    if s is None:
        return None
    try:
        return int(decimal.Decimal(s.replace(",", "")))
    except (decimal.InvalidOperation, ValueError):
        return None


def _to_bool(value: typing.Any) -> typing.Optional[bool]:
    """YES/NO flags. Returns None for blank or unrecognized values so "not told" stays
    distinguishable from "told no" — the flags drive shipping decisions."""
    s = _clean(value)
    if s is None:
        return None
    u = s.upper()
    if u in _YES_VALUES:
        return True
    if u in _NO_VALUES:
        return False
    return None


_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%Y/%m/%d")


def _to_date(value: typing.Any) -> typing.Optional[datetime.date]:
    s = _clean(value)
    if s is None:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _normalized_upc(value: typing.Any) -> typing.Optional[str]:
    """Digits only. Motor State leaves the column blank for ~33% of rows and pads some of the
    rest, and a UPC is only useful here as a cross-distributor join key."""
    s = _clean(value)
    if s is None:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits or None


def brand_code_from_part_number(part_number: typing.Any) -> typing.Optional[str]:
    s = _clean(part_number)
    if not s or len(s) < _BRAND_CODE_LENGTH:
        return None
    return s[:_BRAND_CODE_LENGTH].upper()


# ---------------------------------------------------------------------------
# Feed reading
# ---------------------------------------------------------------------------
def _select_feed_encoding(path: str) -> str:
    for encoding in _FEED_ENCODINGS:
        try:
            with open(path, "r", encoding=encoding, newline="") as fh:
                for _ in fh:
                    pass
            return encoding
        except UnicodeDecodeError:
            continue
    # latin-1 maps every byte, so this is only reached if the file could not be opened at all.
    return _FEED_ENCODINGS[-1]


def read_feed_header(path: str) -> typing.Dict[int, str]:
    """Column index -> canonical name for one feed file. Empty when the file has no header."""
    encoding = _select_feed_encoding(path)
    with open(path, "r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return {}
    return feed_spec.map_header_row(header)


def iter_feed_rows(path: str) -> typing.Iterator[typing.Dict[str, typing.Any]]:
    """
    Stream the feed as canonical-name dicts. Columns the account does not receive are simply
    absent from each dict — callers must use ``.get()``, never assume a key exists.
    """
    encoding = _select_feed_encoding(path)
    with open(path, "r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh)
        try:
            header = next(reader)
        except StopIteration:
            return
        index_to_name = feed_spec.map_header_row(header)
        if not index_to_name:
            logger.warning("{} No recognized columns in feed header: {}.".format(_LOG_PREFIX, path))
            return
        missing_required = [c for c in feed_spec.REQUIRED_COLUMNS if c not in index_to_name.values()]
        if missing_required:
            logger.error(
                "{} Feed {} is missing required column(s): {}.".format(
                    _LOG_PREFIX, path, ", ".join(missing_required)
                )
            )
            return
        for raw in reader:
            if not raw:
                continue
            row: typing.Dict[str, typing.Any] = {}
            for i, name in index_to_name.items():
                if i < len(raw):
                    row[name] = raw[i]
            yield row


def feed_has_catalog_columns(index_to_name: typing.Mapping[int, str]) -> bool:
    """True when this account's file carries the enriched (image/category/long-description)
    columns. Used only for logging — catalog ingest writes whatever the file has."""
    present = set(index_to_name.values())
    return any(c in present for c in feed_spec.ENRICHED_COLUMNS)


# ---------------------------------------------------------------------------
# Connection resolution
# ---------------------------------------------------------------------------
def _active_company_providers_queryset():
    return src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.MOTOR_STATE_DISTRIBUTING.value,
        provider__status=src_enums.BrandProviderStatus.ACTIVE.value,
        active=True,
    ).select_related("company", "provider")


def _primary_company_provider() -> typing.Optional[src_models.CompanyProviders]:
    base = _active_company_providers_queryset()
    return base.filter(primary=True).first() or base.first()


def _client_for(
    company_provider: src_models.CompanyProviders,
    local_feed_path: typing.Optional[str] = None,
) -> motorstate_ftp_client.MotorStateFTPClient:
    creds = dict(credentials_helper.get_feed_credentials(company_provider))
    if local_feed_path:
        creds["local_feed_path"] = local_feed_path
    elif not str(creds.get("local_feed_path") or "").strip():
        default_local = getattr(
            settings, "MOTORSTATE_FEED_LOCAL_PATH", motorstate_ftp_client.DEFAULT_LOCAL_FEED
        )
        root, _, ext = default_local.rpartition(".")
        creds["local_feed_path"] = "{}_company_{}.{}".format(
            root or default_local, company_provider.company_id, ext or "csv"
        )
    return motorstate_ftp_client.MotorStateFTPClient(credentials=creds)


def _content_client_for(
    company_provider: src_models.CompanyProviders,
) -> typing.Optional[motorstate_ftp_client.MotorStateFTPClient]:
    """
    Client for the optional *content* feed -- a second Motor State account whose file carries
    the enriched columns.

    Motor State provisions image / category / long-description on some accounts and a fresh
    daily refresh on others, and not necessarily the same one. When they differ, the connection
    carries ``content_ftp_user`` / ``content_ftp_password`` alongside its main credentials: the
    main account drives catalog, stock and price, and this one supplies content only.

    Returns None when the connection has no content credentials -- the normal case, where the
    main feed already carries the enriched columns.
    """
    creds = dict(credentials_helper.get_feed_credentials(company_provider))
    user = str(creds.get("content_ftp_user") or "").strip()
    password = str(creds.get("content_ftp_password") or "").strip()
    if not user or not password:
        return None

    content_creds = {
        k: v for k, v in creds.items()
        if k in ("ftp_host", "ftp_server", "server_url", "ftp_port", "ftp_directory")
    }
    content_creds["ftp_user"] = user
    content_creds["ftp_password"] = password
    remote_file = str(creds.get("content_ftp_remote_file") or "").strip()
    if remote_file:
        content_creds["ftp_remote_file"] = remote_file
    default_local = getattr(
        settings, "MOTORSTATE_FEED_LOCAL_PATH", motorstate_ftp_client.DEFAULT_LOCAL_FEED
    )
    root, _, ext = default_local.rpartition(".")
    content_creds["local_feed_path"] = "{}_content_{}.{}".format(
        root or default_local, company_provider.company_id, ext or "csv"
    )
    return motorstate_ftp_client.MotorStateFTPClient(credentials=content_creds)


def download_feed_for_company_provider(
    company_provider: src_models.CompanyProviders,
    force_download: bool = True,
    local_feed_path: typing.Optional[str] = None,
) -> str:
    client = _client_for(company_provider, local_feed_path=local_feed_path)
    logger.info(
        "{} Downloading feed: company_id={} company_provider_id={} file={} force={}.".format(
            _LOG_PREFIX,
            company_provider.company_id,
            company_provider.id,
            client.ftp_remote_file,
            force_download,
        )
    )
    return client.download_feed_file(force_download=force_download)


# ---------------------------------------------------------------------------
# Brands
# ---------------------------------------------------------------------------
def _brand_id_by_code() -> typing.Dict[str, int]:
    return {
        (code or "").strip().upper(): pk
        for pk, code in src_models.MotorStateBrand.objects.values_list("id", "code")
        if (code or "").strip()
    }


def sync_motorstate_brands_from_feed(feed_path: str) -> int:
    """
    Create MotorStateBrand rows for brand codes the feed uses but the brand table lacks.

    The API's ``/api/Brands`` list runs behind the feed — REESE, CENTRIC BRAKE PARTS, CHASSIS
    ENGINEERING, STOPTECH and ~60 more appear in the file with no brand row — so the feed is
    also the brand source. Existing rows are left alone: ``offered`` / ``is_inventory_available``
    / ``data`` are API-only fields the feed cannot speak to.

    Returns the number of brands created.
    """
    existing = _brand_id_by_code()
    seen: typing.Dict[str, str] = {}
    for row in iter_feed_rows(feed_path):
        code = brand_code_from_part_number(row.get("part_number"))
        if not code or code in existing or code in seen:
            continue
        name = _clean(row.get("brand"))
        if name:
            seen[code] = name

    if not seen:
        logger.info("{} No new brand codes in feed.".format(_LOG_PREFIX))
        return 0

    # ignore_conflicts makes bulk_create's return value useless as a count (Postgres returns
    # no rows for skipped inserts), so count the table instead.
    before = src_models.MotorStateBrand.objects.count()
    src_models.MotorStateBrand.objects.bulk_create(
        [src_models.MotorStateBrand(code=code, name=name) for code, name in sorted(seen.items())],
        ignore_conflicts=True,
    )
    created = src_models.MotorStateBrand.objects.count() - before
    logger.info(
        "{} Created {} MotorStateBrand rows from feed (e.g. {}).".format(
            _LOG_PREFIX,
            created,
            ", ".join("{}={}".format(c, n) for c, n in list(sorted(seen.items()))[:5]),
        )
    )
    return created


# ---------------------------------------------------------------------------
# Catalog ingest (primary connection)
# ---------------------------------------------------------------------------
def _product_from_feed_row(
    row: typing.Dict[str, typing.Any],
    brand_id_by_code: typing.Dict[str, int],
    has_enriched: bool,
    now,
) -> typing.Optional[src_models.MotorStateProduct]:
    part_number = _clean(row.get("part_number"))
    if not part_number:
        return None

    brand_code = brand_code_from_part_number(part_number)
    status_type = (_clean(row.get("status")) or "").upper() or None
    quantity = _to_int(row.get("qty_avail"))

    product = src_models.MotorStateProduct(
        part_number=part_number,
        brand_id=brand_id_by_code.get(brand_code) if brand_code else None,
        brand_code=brand_code,
        # A row in the account's own feed file is by definition a part Motor State sells us.
        found=True,
        vendor_part_number=_clean(row.get("manufacturer_part")),
        short_description=_clean(row.get("description")),
        status_type=status_type,
        is_stocking=status_type == feed_spec.STATUS_STOCKING,
        quantity=quantity,
        upc=_normalized_upc(row.get("upc")),
        aaia_code=_clean(row.get("aaia_code")),
        length=_to_dimension(row.get("length"), part_number, "length"),
        width=_to_dimension(row.get("width"), part_number, "width"),
        height=_to_dimension(row.get("height"), part_number, "height"),
        weight=_to_dimension(row.get("weight"), part_number, "weight"),
        air_restricted=_to_bool(row.get("air_restricted")),
        state_restricted=_clean(row.get("state_restricted")),
        truck_freight_only=_to_bool(row.get("truck_freight_only")),
        ship_alone=_to_bool(row.get("ship_alone")),
        canada_restricted=_to_bool(row.get("canada_restricted")),
        emissions_warning=_to_bool(row.get("emissions_warning")),
        oversized=_to_bool(row.get("oversized")),
        notes=_clean(row.get("notes")),
        acquired_date=_to_date(row.get("acquired_date")),
        feed_updated_at=now,
        updated_at=now,
    )
    if has_enriched:
        product.long_description = _clean(row.get("long_description"))
        product.image_url = _clean(row.get("image_url"))
        product.category_level_1 = _clean(row.get("category_level_1"))
        product.category_level_2 = _clean(row.get("category_level_2"))
        product.category_level_3 = _clean(row.get("category_level_3"))
    return product


# A truncated download must never be able to delist the catalog, so the sweep below refuses to
# run unless the pass ingested at least this many rows. The live feed carries ~102k.
MIN_ROWS_FOR_DELIST_SWEEP = 10000


def delist_products_missing_from_feed(run_timestamp) -> int:
    """
    Mark catalog rows the primary feed no longer lists as not found, with no stock.

    Necessary because stock is now feed-only. 59.5k rows predate the feed (they came from the
    retired /api/Product hydrate) and Motor State's current file does not list them -- 48k of
    those are absent from every account's feed, i.e. genuinely delisted. Left alone they would
    keep flowing into master parts as live, sellable parts carrying a stock number that nothing
    refreshes any more.

    ``found=False`` is what actually takes them out of circulation: both
    ``sync_master_parts_from_motorstate`` and ``sync_provider_inventory_from_motorstate``
    filter on it. Rows are kept rather than deleted -- a part can come back in a later feed,
    and the row still carries the identifiers that make it matchable.

    Returns the number of rows delisted.
    """
    delisted = (
        src_models.MotorStateProduct.objects.filter(found=True)
        .exclude(feed_updated_at=run_timestamp)
        .update(found=False, quantity=0, is_stocking=False, updated_at=timezone.now())
    )
    if delisted:
        logger.info(
            "{} Delisted {} catalog rows absent from this feed pass (found=False, quantity=0).".format(
                _LOG_PREFIX, delisted
            )
        )
    return delisted


def fetch_and_save_motorstate_catalog_from_feed(
    force_download: bool = True,
    company_provider_id: typing.Optional[int] = None,
    sync_brands: bool = True,
    delist_missing: bool = True,
) -> int:
    """
    Download the primary connection's feed and upsert MotorStateProduct.

    Catalog, stock and status are distributor-wide — the same whichever dealer's file is read —
    so this runs once, from the primary connection, like every other provider's shared catalog.
    Per-company prices are :func:`sync_motorstate_company_pricing_from_feed`'s job and are never
    touched here.

    Returns the number of rows upserted.
    """
    cp = (
        _active_company_providers_queryset().filter(id=company_provider_id).first()
        if company_provider_id is not None
        else _primary_company_provider()
    )
    if not cp:
        logger.info("{} No active Motor State CompanyProviders. Skipping catalog.".format(_LOG_PREFIX))
        return 0

    feed_path = download_feed_for_company_provider(cp, force_download=force_download)

    header = read_feed_header(feed_path)
    if not header:
        logger.error("{} Feed {} has no readable header. Aborting catalog.".format(_LOG_PREFIX, feed_path))
        return 0
    has_enriched = feed_has_catalog_columns(header)
    logger.info(
        "{} Catalog ingest from company_provider_id={} ({} columns recognized, enriched={}).".format(
            _LOG_PREFIX, cp.id, len(header), has_enriched
        )
    )
    if not has_enriched:
        logger.warning(
            "{} Primary account's feed carries no image/category/long-description columns; "
            "those fields will be left as-is on existing rows and null on new ones. Ask Motor "
            "State to enable the enriched columns for account on company_provider_id={}.".format(
                _LOG_PREFIX, cp.id
            )
        )

    if sync_brands:
        sync_motorstate_brands_from_feed(feed_path)

    brand_id_by_code = _brand_id_by_code()
    update_fields = list(_CATALOG_UPDATE_FIELDS)
    if has_enriched:
        update_fields.extend(_ENRICHED_UPDATE_FIELDS.values())

    now = timezone.now()
    started = time.monotonic()
    buf: typing.Dict[str, src_models.MotorStateProduct] = {}
    feed_rows = 0
    unknown_brand_rows = 0
    total_upserted = 0
    batch_num = 0

    def _flush() -> None:
        nonlocal batch_num, total_upserted
        if not buf:
            return
        batch = list(buf.values())
        pgbulk.upsert(
            src_models.MotorStateProduct,
            batch,
            unique_fields=["part_number"],
            update_fields=update_fields,
            returning=False,
        )
        batch_num += 1
        total_upserted += len(batch)
        logger.info(
            "{} MotorStateProduct batch {} upserted ({} rows, {} total, {:.1f}s).".format(
                _LOG_PREFIX, batch_num, len(batch), total_upserted, time.monotonic() - started
            )
        )
        connection.close()
        time.sleep(FEED_UPSERT_DELAY)
        buf.clear()

    for row in iter_feed_rows(feed_path):
        feed_rows += 1
        product = _product_from_feed_row(row, brand_id_by_code, has_enriched, now)
        if not product:
            continue
        if product.brand_id is None:
            unknown_brand_rows += 1
        # Last row per part number wins, matching upsert semantics.
        buf[product.part_number] = product
        if len(buf) >= FEED_STREAMING_BATCH_SIZE:
            _flush()
        if (feed_rows % FEED_PARSE_PROGRESS_EVERY) == 0:
            logger.info(
                "{} Catalog progress: {} rows scanned, {} upserted, {:.1f}s.".format(
                    _LOG_PREFIX, feed_rows, total_upserted, time.monotonic() - started
                )
            )

    _flush()

    if feed_rows == 0:
        logger.warning("{} Feed file empty or unreadable: {}.".format(_LOG_PREFIX, feed_path))
        return 0
    if unknown_brand_rows:
        logger.info(
            "{} {} rows had a brand code with no MotorStateBrand row (brand left null; "
            "re-run after sync_motorstate_brands_from_feed to attach).".format(
                _LOG_PREFIX, unknown_brand_rows
            )
        )
    if delist_missing:
        if total_upserted >= MIN_ROWS_FOR_DELIST_SWEEP:
            delist_products_missing_from_feed(now)
        else:
            logger.warning(
                "{} Skipping the delist sweep: only {} rows ingested (need {}). A short feed "
                "is more likely a truncated download than a shrunken catalog.".format(
                    _LOG_PREFIX, total_upserted, MIN_ROWS_FOR_DELIST_SWEEP
                )
            )

    logger.info(
        "{} Catalog done: {} MotorStateProduct rows upserted from {} feed rows in {:.1f}s.".format(
            _LOG_PREFIX, total_upserted, feed_rows, time.monotonic() - started
        )
    )
    return total_upserted


# ---------------------------------------------------------------------------
# Per-company pricing ingest (every connection, primary included)
# ---------------------------------------------------------------------------
# ``part_number__in`` lookups when resolving feed keys to MotorStateProduct ids.
FEED_KEY_LOOKUP_CHUNK = 15000


def _product_id_by_part_number(part_numbers: typing.List[str]) -> typing.Dict[str, int]:
    out: typing.Dict[str, int] = {}
    for i in range(0, len(part_numbers), FEED_KEY_LOOKUP_CHUNK):
        chunk = part_numbers[i:i + FEED_KEY_LOOKUP_CHUNK]
        for pk, pn in src_models.MotorStateProduct.objects.filter(
            part_number__in=chunk
        ).values_list("id", "part_number"):
            out[pn] = pk
        connection.close()
    return out


def _pricing_from_feed_row(
    row: typing.Dict[str, typing.Any],
) -> typing.Optional[typing.Dict[str, typing.Any]]:
    """Price fields for one feed row, or None when the row prices nothing.

    A row with a part number but every price column blank is skipped rather than written as
    all-nulls — that would blank out a previously good price on re-sync.
    """
    values = {
        "customer_price": _to_decimal(row.get("cost")),
        "base_price": _to_decimal(row.get("jobber")),
        "list_price": _to_decimal(row.get("suggested_retail")),
        "map_price": _to_decimal(row.get("map_price")),
        "vendor_msrp": _to_decimal(row.get("vendor_msrp")),
    }
    if all(v is None for v in values.values()):
        return None
    values["is_map_restricted"] = values["map_price"] is not None
    return values


def sync_motorstate_company_pricing_from_feed(
    company_provider_id: int,
    force_download: bool = True,
) -> int:
    """
    Download one connection's own feed file and upsert its MotorStateCompanyPricing rows.

    Every account's file carries the same catalog with that account's own ``Cost`` and
    ``Jobber``, which is why this is per company rather than a single shared pull. Rows whose
    part number is not in MotorStateProduct yet are skipped — the catalog pass owns creating
    them, and a price row cannot exist without one.

    Called by the IntegrationPricingSyncJob queue (see integration_pricing_sync_jobs).
    Returns the number of pricing rows upserted.
    """
    cp = _active_company_providers_queryset().filter(id=company_provider_id).first()
    if not cp:
        logger.warning(
            "{} No active Motor State CompanyProviders id={}. Skipping pricing.".format(
                _LOG_PREFIX, company_provider_id
            )
        )
        return 0

    if not src_models.MotorStateProduct.objects.exists():
        logger.warning(
            "{} No Motor State catalog yet; run fetch_motorstate_feed before pricing "
            "company_provider_id={}.".format(_LOG_PREFIX, cp.id)
        )
        return 0

    feed_path = download_feed_for_company_provider(cp, force_download=force_download)

    # Pass 1 — collect part numbers only, then resolve them all in one bulk lookup. Holding
    # ~100k short strings is cheap; holding the parsed price rows for the whole file is not.
    logger.info(
        "{} Pricing pass 1 — collecting part numbers company_id={} company_provider_id={}.".format(
            _LOG_PREFIX, cp.company_id, cp.id
        )
    )
    seen: typing.Set[str] = set()
    keys: typing.List[str] = []
    for row in iter_feed_rows(feed_path):
        pn = _clean(row.get("part_number"))
        if pn and pn not in seen:
            seen.add(pn)
            keys.append(pn)
    del seen

    if not keys:
        logger.warning(
            "{} No pricing rows parsed for company_provider_id={}.".format(_LOG_PREFIX, cp.id)
        )
        return 0

    product_id_by_pn = _product_id_by_part_number(keys)
    logger.info(
        "{} Pricing lookup: {} feed part numbers -> {} catalog rows (company_provider_id={}).".format(
            _LOG_PREFIX, len(keys), len(product_id_by_pn), cp.id
        )
    )
    del keys

    # Pass 2 — stream again and upsert in batches; part-id lookups are O(1) against the dict.
    now = timezone.now()
    started = time.monotonic()
    buf: typing.Dict[int, src_models.MotorStateCompanyPricing] = {}
    total_upserted = 0
    unmatched = 0
    unpriced = 0
    batch_num = 0

    def _flush() -> None:
        nonlocal batch_num, total_upserted
        if not buf:
            return
        batch = list(buf.values())
        pgbulk.upsert(
            src_models.MotorStateCompanyPricing,
            batch,
            unique_fields=["product", "company"],
            update_fields=_COMPANY_PRICING_UPDATE_FIELDS,
            returning=False,
        )
        batch_num += 1
        total_upserted += len(batch)
        logger.info(
            "{} Pricing batch {} upserted ({} rows, {} total, company_id={}, {:.1f}s).".format(
                _LOG_PREFIX, batch_num, len(batch), total_upserted, cp.company_id,
                time.monotonic() - started,
            )
        )
        connection.close()
        time.sleep(FEED_UPSERT_DELAY)
        buf.clear()

    for row in iter_feed_rows(feed_path):
        pn = _clean(row.get("part_number"))
        if not pn:
            continue
        product_id = product_id_by_pn.get(pn)
        if not product_id:
            unmatched += 1
            continue
        values = _pricing_from_feed_row(row)
        if values is None:
            unpriced += 1
            continue
        buf[product_id] = src_models.MotorStateCompanyPricing(
            product_id=product_id,
            company_id=cp.company_id,
            feed_updated_at=now,
            updated_at=now,
            **values,
        )
        if len(buf) >= FEED_STREAMING_BATCH_SIZE:
            _flush()

    _flush()

    logger.info(
        "{} Pricing done for company_id={} company_provider_id={}: {} rows upserted, "
        "{} feed rows had no catalog row, {} carried no prices ({:.1f}s).".format(
            _LOG_PREFIX, cp.company_id, cp.id, total_upserted, unmatched, unpriced,
            time.monotonic() - started,
        )
    )
    return total_upserted


def sync_motorstate_company_pricing_from_feed_for_all_companies(
    force_download: bool = True,
) -> typing.Dict[int, int]:
    """Run the pricing pull for every active connection. Sequential on purpose — each pull
    downloads a ~20-40MB file and streams it twice, so the work is IO- and DB-bound rather
    than something extra concurrency helps."""
    results: typing.Dict[int, int] = {}
    for cp in _active_company_providers_queryset().order_by("id"):
        try:
            results[cp.id] = sync_motorstate_company_pricing_from_feed(
                cp.id, force_download=force_download
            )
        except (motorstate_exceptions.MotorStateFTPException, ValueError) as e:
            # One dealer's broken FTP account -- or one still carrying only the API-era
            # credentials, which raises ValueError out of the client constructor -- must not
            # stop the connections that are configured correctly.
            logger.error(
                "{} Pricing failed for company_provider_id={} (company_id={}): {}.".format(
                    _LOG_PREFIX, cp.id, cp.company_id, str(e)
                )
            )
            results[cp.id] = 0
    return results


# ---------------------------------------------------------------------------
# Content enrichment (optional second feed)
# ---------------------------------------------------------------------------
CONTENT_UPDATE_BATCH_SIZE = 2000

_CONTENT_UPDATE_SQL = """
UPDATE motorstate_products p SET
    long_description = CASE
        WHEN v.long_description IS NOT NULL AND v.long_description != ''
        THEN v.long_description ELSE p.long_description END,
    image_url = CASE
        WHEN v.image_url IS NOT NULL AND v.image_url != ''
        THEN v.image_url ELSE p.image_url END,
    category_level_1 = CASE
        WHEN v.category_level_1 IS NOT NULL AND v.category_level_1 != ''
        THEN v.category_level_1 ELSE p.category_level_1 END,
    category_level_2 = CASE
        WHEN v.category_level_2 IS NOT NULL AND v.category_level_2 != ''
        THEN v.category_level_2 ELSE p.category_level_2 END,
    category_level_3 = CASE
        WHEN v.category_level_3 IS NOT NULL AND v.category_level_3 != ''
        THEN v.category_level_3 ELSE p.category_level_3 END,
    updated_at = now()
FROM (VALUES {}) AS v(
    part_number, long_description, image_url,
    category_level_1, category_level_2, category_level_3
)
WHERE p.part_number = v.part_number
"""


def sync_motorstate_content_from_feed(
    company_provider_id: typing.Optional[int] = None,
    force_download: bool = True,
) -> int:
    """
    Overlay image, categories and long description from the content feed onto the catalog.

    Only runs when the connection carries ``content_ftp_user`` / ``content_ftp_password``
    (see :func:`_content_client_for`). Deliberately narrow:

      * It updates existing rows only -- it never inserts. A part the content feed knows about
        but the catalog feed no longer lists is not a part we can sell, so creating a row for
        it would resurrect something the delist sweep just retired.
      * It writes five columns and nothing else. In particular it does not touch
        ``feed_updated_at``, which is the catalog pass's delist marker -- writing it here would
        silently un-delist every part the content feed still remembers.
      * A blank value leaves the existing one alone, so a partially populated content file
        cannot erase content an earlier run wrote.

    Returns the number of catalog rows updated.
    """
    cp = (
        _active_company_providers_queryset().filter(id=company_provider_id).first()
        if company_provider_id is not None
        else _primary_company_provider()
    )
    if not cp:
        logger.info("{} No active Motor State CompanyProviders. Skipping content.".format(_LOG_PREFIX))
        return 0

    client = _content_client_for(cp)
    if client is None:
        logger.info(
            "{} company_provider_id={} has no content-feed credentials; skipping content "
            "overlay (the main feed's own columns are used).".format(_LOG_PREFIX, cp.id)
        )
        return 0

    logger.info(
        "{} Downloading content feed: company_provider_id={} file={} force={}.".format(
            _LOG_PREFIX, cp.id, client.ftp_remote_file, force_download
        )
    )
    feed_path = client.download_feed_file(force_download=force_download)

    header = read_feed_header(feed_path)
    if not feed_has_catalog_columns(header):
        logger.warning(
            "{} Content feed {} carries none of the enriched columns ({}); nothing to "
            "overlay.".format(_LOG_PREFIX, client.ftp_remote_file, ", ".join(feed_spec.ENRICHED_COLUMNS))
        )
        return 0

    started = time.monotonic()
    buf: typing.Dict[str, typing.Tuple] = {}
    feed_rows = 0
    total_updated = 0
    batch_num = 0

    def _flush() -> None:
        nonlocal batch_num, total_updated
        if not buf:
            return
        rows = list(buf.values())
        placeholders = ", ".join(["(%s, %s::text, %s::text, %s::varchar, %s::varchar, %s::varchar)"] * len(rows))
        params = [x for r in rows for x in r]
        with connection.cursor() as cur:
            cur.execute(_CONTENT_UPDATE_SQL.format(placeholders), params)
            batch_num += 1
            total_updated += cur.rowcount or 0
        logger.info(
            "{} Content batch {} applied ({} rows offered, {} catalog rows updated, {:.1f}s).".format(
                _LOG_PREFIX, batch_num, len(rows), total_updated, time.monotonic() - started
            )
        )
        connection.close()
        time.sleep(FEED_UPSERT_DELAY)
        buf.clear()

    for row in iter_feed_rows(feed_path):
        feed_rows += 1
        part_number = _clean(row.get("part_number"))
        if not part_number:
            continue
        values = (
            part_number,
            _clean(row.get("long_description")),
            _clean(row.get("image_url")),
            _clean(row.get("category_level_1")),
            _clean(row.get("category_level_2")),
            _clean(row.get("category_level_3")),
        )
        if all(v is None for v in values[1:]):
            continue
        buf[part_number] = values
        if len(buf) >= CONTENT_UPDATE_BATCH_SIZE:
            _flush()

    _flush()

    logger.info(
        "{} Content overlay done: {} catalog rows updated from {} content-feed rows "
        "in {:.1f}s.".format(_LOG_PREFIX, total_updated, feed_rows, time.monotonic() - started)
    )
    return total_updated
