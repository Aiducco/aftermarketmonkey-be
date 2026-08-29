"""
Pull TDG Access's whole catalog into :class:`src.models.TdgProduct` -- every product type.

Transport: :class:`src.integrations.clients.tdg.client.TdgApiClient`.
Entry point: the ``fetch_tdg_catalog`` management command.

Why this is one request and not a crawl
---------------------------------------
``POST /api/product/all`` returns the entire catalogue in a single response: ~45,000 products,
~32 MB, in under ten seconds. There is no pagination and no cursor, so there is nothing to
resume, no rate limit to respect and no checkpoint file -- which is what makes this module so
much shorter than ``simpletire.py`` or ``wheelpros_vehicles.py`` next door. Do not add
concurrency here; there is only ever one request to make.

The response is streamed to a temp file first (see the client) and parsed from there, so a
mapping bug costs a re-parse rather than a re-download. ``--from-file`` exposes that: fix the
mapper, re-run against the file you already have.

Six product types, one table
----------------------------
Tires, wheels, lug kits, hub rings, generic products and services arrive in one array. Their
``specifications`` objects share almost no keys, so :func:`map_product` dispatches on ``type``
and fills only that type's block of columns -- see :class:`src.models.TdgProduct` for why they
live in one table rather than six. An unrecognised ``type`` is still written: identity, brand and
the raw blob are type-independent, and a new TDG category should not silently vanish from a pull
that claims to be complete.

Every value is a string, including the numbers
----------------------------------------------
TDG sends ``"101"``, ``"8.9"``, ``"No"`` -- never an int, float or bool. The ``_as_*`` helpers
below are the entire type system for this table, and each returns ``None`` rather than a default
when the string does not parse: NULL means "TDG did not publish it", and a 0 tread depth or a
False 3PMSF flag would both be lies. Their strings are display strings and will drift, which is
why the untouched product object survives in ``raw`` -- a parser fix must never require a
re-pull.

Idempotence
-----------
The write is an upsert keyed on ``tdg_id``, so re-running is safe and a partial run costs
nothing. Rows that vanish from TDG's catalog are **not** deleted: they were real once, and
``is_inactive`` (which TDG sets on ~20% of tires and over half of wheels) is a better signal than
absence. Prune deliberately, with :func:`stale_rows`, not as a side effect of a fetch.
"""
import dataclasses
import datetime
import decimal
import logging
import os
import re
import typing

from django.db import transaction
from django.db import utils as db_utils
from django.utils import timezone

from src.integrations.clients.tdg import client as tdg_client
from src.models import TdgProduct

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TDG-CATALOG]"

DEFAULT_BATCH_SIZE = 1000

# Every mapped column except the conflict target and created_at. Kept explicit rather than
# derived from the model so that adding a column without mapping it is a visible omission here.
UPDATE_FIELDS = [
    "item_number",
    "part_number",
    "gtin",
    "product_type",
    "brand_id",
    "brand_name",
    "product_line_id",
    "product_line_name",
    "is_inactive",
    "is_availability_restricted",
    "product_image_url",
    "tire_size_raw",
    "tire_size_code",
    "tire_size_display",
    "tire_size_type",
    "tire_type",
    "season",
    "service_type",
    "service_description",
    "load_index",
    "speed_rating",
    "load_range",
    "rim_diameter_in",
    "outside_diameter_in",
    "tread_depth_32nds",
    "sidewall",
    "rim_width_min_in",
    "rim_width_max_in",
    "utqg",
    "utqg_treadwear",
    "utqg_traction",
    "utqg_temperature",
    "warranty_mileage_miles",
    "is_run_flat",
    "is_ev_optimized",
    "is_3pmsf",
    "winter_studding",
    "stud_size",
    "oe_marking",
    "additional_model_information",
    "wheel_diameter_in",
    "wheel_width_in",
    "bolt_pattern",
    "offset_mm",
    "centerbore_mm",
    "max_load_lb",
    "wheel_type",
    "construction",
    "lug_seat",
    "is_winter_approved",
    "thread",
    "seat",
    "style",
    "end_type",
    "inside_diameter_mm",
    "overall_diameter_mm",
    "finish",
    "material",
    "superseded_by_item",
    "raw",
    "fetched_at",
]


@dataclasses.dataclass
class FetchStats:
    """Counters for one run. ``by_type`` is the headline: it says what the pull actually contained."""

    products_seen: int = 0
    products_written: int = 0
    products_skipped: int = 0
    products_rejected: int = 0
    bytes_downloaded: int = 0
    by_type: typing.Dict[str, int] = dataclasses.field(default_factory=dict)


# -- scalar parsing --------------------------------------------------------------------------
# TDG sends every specification as a string. None means "not published"; see the module docstring.


def _as_text(value, *, max_length: typing.Optional[int] = None) -> typing.Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if max_length is not None and len(text) > max_length:
        # Truncating loses data, so say which row did it -- a repeated warning is a column that
        # needs widening, not noise.
        logger.warning("%s truncating %r to %d chars", _LOG_PREFIX, text, max_length)
        text = text[:max_length]
    return text


def _as_int(value) -> typing.Optional[int]:
    text = _as_text(value)
    if text is None:
        return None
    match = re.search(r"-?\d+", text.replace(",", ""))
    return int(match.group(0)) if match else None


def _as_decimal(value) -> typing.Optional[decimal.Decimal]:
    text = _as_text(value)
    if text is None:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text.replace(",", ""))
    if not match:
        return None
    try:
        return decimal.Decimal(match.group(0))
    except decimal.InvalidOperation:
        return None


def _as_measurement(value) -> typing.Optional[decimal.Decimal]:
    """
    A physical measurement, where zero means "not published" rather than a reading.

    TDG sends ``'0'`` for a tread depth or rim width it does not hold -- 36 tires claim a 0/32"
    tread and 8 a 0" minimum rim, neither of which is a tire. Distinct from :func:`_as_decimal`,
    which keeps zeros because some fields (a wheel's offset) are legitimately 0.
    """
    parsed = _as_decimal(value)
    return parsed if parsed is not None and parsed > 0 else None


def _as_bool(value) -> typing.Optional[bool]:
    """
    ``'Yes'``/``'No'`` -- and nothing else, on purpose.

    Anything unrecognised returns None rather than falling back to False, because every boolean
    on this table is a claim (run-flat, EV-optimised, winter-approved) where "unknown" and "no"
    have to stay distinguishable.
    """
    text = _as_text(value)
    if text is None:
        return None
    lowered = text.lower()
    if lowered in ("yes", "true", "y", "1"):
        return True
    if lowered in ("no", "false", "n", "0"):
        return False
    return None


def _split_size(value) -> typing.Tuple[typing.Optional[str], typing.Optional[str], typing.Optional[str]]:
    """
    ``'2255517, P225/55R17 XL'`` -> ``(raw, '2255517', 'P225/55R17 XL')``.

    Both halves are optional in practice: some rows carry only the printed size, a few only the
    numeric code. Anything that is not a bare run of digits is treated as the printed half, since
    that is the one downstream size parsing needs and guessing wrong there is worse than a NULL
    code column.
    """
    raw = _as_text(value, max_length=64)
    if raw is None:
        return None, None, None
    code = display = None
    for part in (piece.strip() for piece in raw.split(",")):
        if not part:
            continue
        if re.fullmatch(r"\d+", part) and code is None:
            code = part
        elif display is None:
            display = part
    return raw, code, display


def _split_utqg(value) -> typing.Tuple[typing.Optional[str], typing.Optional[int], typing.Optional[str], typing.Optional[str]]:
    """
    ``'400 A A'`` -> ``('400 A A', 400, 'A', 'A')``.

    TDG uses three separators for the same grade -- spaced (``'400 A A'``), hyphenated
    (``'340-AA-A'``) and run together (``'460AA'``) -- so they are normalised before matching
    rather than given a pattern each.

    A treadwear of 0 is not a grade: ``'0'`` and ``'0 0 0'`` are how TDG spells "ungraded", and
    on ~800 tires. Those keep the raw string and null every component, as does anything else
    unrecognised.
    """
    raw = _as_text(value, max_length=16)
    if raw is None:
        return None, None, None, None
    normalised = re.sub(r"[-/]", " ", raw.upper())
    match = re.fullmatch(r"\s*(\d{1,3})\s*([A-Z]{1,2})?\s*([A-Z])?\s*", normalised)
    if not match:
        return raw, None, None, None
    treadwear = int(match.group(1))
    if treadwear <= 0:
        return raw, None, None, None
    return raw, treadwear, match.group(2), match.group(3)


def _mileage_to_miles(value) -> typing.Optional[int]:
    """``'70000'`` -> 70000, ``'65k'`` -> 65000. Their storefront has used both."""
    text = _as_text(value)
    if text is None:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)\s*([kK])?", text.replace(",", ""))
    if not match:
        return None
    miles = float(match.group(1)) * (1000 if match.group(2) else 1)
    return int(miles) if miles > 0 else None


# -- mapping ---------------------------------------------------------------------------------


def map_product(product: dict, *, fetched_at: typing.Optional[datetime.datetime] = None) -> typing.Optional[TdgProduct]:
    """
    One TDG product object -> one unsaved :class:`TdgProduct`.

    Returns None for a row with no ``id``, which cannot be upserted and is not worth guessing a
    key for. Every other shape is mapped as best it can be: an unknown ``type`` still gets its
    identity, brand and raw blob.
    """
    tdg_id = _as_int(product.get("id"))
    if tdg_id is None:
        logger.warning("%s skipping a product with no id: %r", _LOG_PREFIX, product.get("itemNumber"))
        return None

    specs = product.get("specifications") or {}
    if not isinstance(specs, dict):
        specs = {}

    row = TdgProduct(
        tdg_id=tdg_id,
        item_number=_as_text(product.get("itemNumber"), max_length=64) or str(tdg_id),
        part_number=_as_text(product.get("partNumber"), max_length=64),
        gtin=_as_text(product.get("gtin"), max_length=32),
        product_type=_as_text(product.get("type"), max_length=32) or "",
        brand_id=_as_int(product.get("brandId")),
        brand_name=_as_text(product.get("brandName"), max_length=128),
        product_line_id=_as_int(product.get("productId")),
        product_line_name=_as_text(product.get("productName"), max_length=255),
        # bool() and not _as_bool(): these two arrive as real JSON booleans, and a missing flag
        # here means "not flagged" rather than unknown -- they are TDG's assertion about their
        # own catalog, which they always hold an opinion on.
        is_inactive=bool(product.get("isInactive")),
        is_availability_restricted=bool(product.get("isAvailabilityRestricted")),
        product_image_url=_as_text(product.get("productImageUrl")),
        superseded_by_item=_as_text(specs.get("supersededByItem"), max_length=32),
        raw=product,
        fetched_at=fetched_at or timezone.now(),
    )

    mapper = _SPEC_MAPPERS.get(row.product_type)
    if mapper is not None:
        mapper(row, specs)
    elif specs:
        logger.warning("%s unmapped product type %r (id=%s); raw kept", _LOG_PREFIX, row.product_type, tdg_id)
    return row


def _map_tire_specs(row: TdgProduct, specs: dict) -> None:
    row.tire_size_raw, row.tire_size_code, row.tire_size_display = _split_size(specs.get("size"))
    row.tire_size_type = _as_text(specs.get("tireSizeType"), max_length=16)
    row.tire_type = _as_text(specs.get("tireType"), max_length=64)
    row.season = _as_text(specs.get("season"), max_length=32)
    row.service_type = _as_text(specs.get("serviceType"), max_length=32)
    row.service_description = _as_text(specs.get("serviceDescription"), max_length=16)
    row.load_index = _as_int(specs.get("loadIndex"))
    row.speed_rating = _as_text(specs.get("speedRating"), max_length=8)
    row.load_range = _as_text(specs.get("loadRange"), max_length=8)
    row.rim_diameter_in = _as_measurement(specs.get("wheelDiameter"))
    row.outside_diameter_in = _as_measurement(specs.get("outsideDiameter"))
    row.tread_depth_32nds = _as_measurement(specs.get("treadDepth32nds"))
    row.sidewall = _as_text(specs.get("sidewall"), max_length=32)
    row.rim_width_min_in = _as_measurement(specs.get("wheelWidthMin"))
    row.rim_width_max_in = _as_measurement(specs.get("wheelWidthMax"))
    row.utqg, row.utqg_treadwear, row.utqg_traction, row.utqg_temperature = _split_utqg(specs.get("uTQG"))
    row.warranty_mileage_miles = _mileage_to_miles(specs.get("warrantyMileage"))
    row.is_run_flat = _as_bool(specs.get("runFlat"))
    row.is_ev_optimized = _as_bool(specs.get("eVOptimized"))
    # 'otherAttributeS' [sic] is a free-text attribute list whose only observed value is '3PMS'.
    # Absent means TDG asserted nothing -- NULL, never False. See the field's help_text.
    other = _as_text(specs.get("otherAttributeS"))
    row.is_3pmsf = True if other and "3PMS" in other.upper() else None
    row.winter_studding = _as_text(specs.get("winterStudding"), max_length=16)
    row.stud_size = _as_text(specs.get("studSize"), max_length=16)
    row.oe_marking = _as_text(specs.get("oEMarking"), max_length=64)
    row.additional_model_information = _as_text(specs.get("additionalModelInformation"), max_length=128)


def _map_wheel_specs(row: TdgProduct, specs: dict) -> None:
    row.wheel_diameter_in = _as_measurement(specs.get("diameter"))
    row.wheel_width_in = _as_measurement(specs.get("width"))
    row.bolt_pattern = _as_text(specs.get("boltPattern"), max_length=32)
    row.offset_mm = _as_int(specs.get("offset"))
    row.centerbore_mm = _as_measurement(specs.get("centerbore"))
    row.max_load_lb = _as_int(specs.get("maxLoad"))
    # Their wheel-only 'type' key (Alloy/Steel/Specialty) -- not the top-level product type.
    row.wheel_type = _as_text(specs.get("type"), max_length=32)
    row.construction = _as_text(specs.get("construction"), max_length=32)
    row.lug_seat = _as_text(specs.get("lugSeat"), max_length=16)
    row.is_winter_approved = _as_bool(specs.get("winterApproved"))
    row.finish = _as_text(specs.get("finish"), max_length=64)
    row.material = _as_text(specs.get("material"), max_length=32)


def _map_lug_kit_specs(row: TdgProduct, specs: dict) -> None:
    row.thread = _as_text(specs.get("thread"), max_length=32)
    row.seat = _as_text(specs.get("seat"), max_length=16)
    row.style = _as_text(specs.get("style"), max_length=16)
    row.end_type = _as_text(specs.get("endType"), max_length=16)
    # A lug kit's 'type' is Nut/Bolt. It shares a key name with the wheel type but not a meaning,
    # so it lands in the same column with a different vocabulary -- read it alongside product_type.
    row.wheel_type = _as_text(specs.get("type"), max_length=32)
    row.finish = _as_text(specs.get("finish"), max_length=64)
    row.material = _as_text(specs.get("material"), max_length=32)


def _map_hub_ring_specs(row: TdgProduct, specs: dict) -> None:
    row.inside_diameter_mm = _as_measurement(specs.get("insideDiameter"))
    row.overall_diameter_mm = _as_measurement(specs.get("overallDiameter"))
    row.finish = _as_text(specs.get("finish"), max_length=64)
    row.material = _as_text(specs.get("material"), max_length=32)


def _map_no_specs(row: TdgProduct, specs: dict) -> None:
    """Generic products and services carry an empty specifications object. Nothing to do."""


_SPEC_MAPPERS: typing.Dict[str, typing.Callable[[TdgProduct, dict], None]] = {
    TdgProduct.TYPE_TIRE: _map_tire_specs,
    TdgProduct.TYPE_WHEEL: _map_wheel_specs,
    TdgProduct.TYPE_LUG_KIT: _map_lug_kit_specs,
    TdgProduct.TYPE_HUB_RING: _map_hub_ring_specs,
    TdgProduct.TYPE_GENERIC: _map_no_specs,
    TdgProduct.TYPE_SERVICE: _map_no_specs,
}


# -- the run ---------------------------------------------------------------------------------


def run_fetch(
    *,
    api_key: str = "",
    environment: str = "",
    from_file: str = "",
    keep_file: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
    product_types: typing.Optional[typing.Iterable[str]] = None,
    dry_run: bool = False,
    progress: typing.Optional[typing.Callable[[str], None]] = None,
) -> FetchStats:
    """
    Fetch (or re-read) the catalog and upsert it.

    ``from_file`` re-parses a response saved by an earlier run instead of calling TDG -- the
    fast loop for a mapping change. ``keep_file`` leaves the downloaded response on disk and
    prints its path, which is how you get a file to pass back in.
    """
    say = progress or (lambda message: None)
    stats = FetchStats()
    fetched_at = timezone.now()
    wanted = {str(item) for item in product_types} if product_types else None

    path = from_file
    downloaded = False
    if not path:
        client = tdg_client.TdgApiClient(api_key=api_key, environment=environment)
        say(f"Requesting the full catalog from {client.api_base_url}/api/product/all ...")
        path = client.stream_all_products_to_file()
        downloaded = True

    try:
        stats.bytes_downloaded = os.path.getsize(path)
        say(f"  {stats.bytes_downloaded / (1 << 20):.1f} MB at {path}")
        products = tdg_client.load_products_from_file(path)
        stats.products_seen = len(products)
        say(f"  {stats.products_seen} products in the response")

        batch: typing.List[TdgProduct] = []
        for product in products:
            product_type = str(product.get("type") or "")
            stats.by_type[product_type] = stats.by_type.get(product_type, 0) + 1
            if wanted is not None and product_type not in wanted:
                stats.products_skipped += 1
                continue
            row = map_product(product, fetched_at=fetched_at)
            if row is None:
                stats.products_skipped += 1
                continue
            batch.append(row)
            if len(batch) >= batch_size:
                _flush(batch, stats, dry_run=dry_run, say=say)
                batch = []
        _flush(batch, stats, dry_run=dry_run, say=say)
    finally:
        if downloaded and not keep_file:
            try:
                os.unlink(path)
            except OSError:
                logger.warning("%s could not remove %s", _LOG_PREFIX, path)
        elif downloaded:
            say(f"  response kept at {path} (re-run with --from-file to re-parse it)")

    return stats


def _flush(batch: typing.List[TdgProduct], stats: FetchStats, *, dry_run: bool, say) -> None:
    if not batch:
        return
    if dry_run:
        stats.products_written += len(batch)
        return
    try:
        with transaction.atomic():
            _upsert(batch)
        stats.products_written += len(batch)
    except db_utils.DatabaseError as exc:
        # One bad row must not cost the batch. Retry individually so the rejects are named and
        # everything else still lands -- the same failure mode simpletire.py handles, and for the
        # same reason: a rejected row is a column we sized wrong, not bad luck.
        logger.warning("%s batch of %d rejected (%s); retrying individually", _LOG_PREFIX, len(batch), exc)
        stats.products_written += _upsert_individually(batch, stats)
    say(f"  {stats.products_written} products written")


def _upsert(rows: typing.List[TdgProduct]) -> None:
    TdgProduct.objects.bulk_create(
        rows,
        update_conflicts=True,
        unique_fields=["tdg_id"],
        update_fields=UPDATE_FIELDS,
    )


def _upsert_individually(rows: typing.List[TdgProduct], stats: FetchStats) -> int:
    written = 0
    for row in rows:
        try:
            with transaction.atomic():
                _upsert([row])
            written += 1
        except db_utils.DatabaseError as exc:
            stats.products_rejected += 1
            logger.error(
                "%s REJECTED tdg_id=%s (%s %s %s) -- %s",
                _LOG_PREFIX,
                row.tdg_id,
                row.item_number,
                row.brand_name,
                row.product_line_name,
                str(exc).strip().replace("\n", " "),
            )
    return written


def stale_rows(before: datetime.datetime):
    """
    Rows TDG did not return on the run that finished at ``before``.

    A pull is complete by construction -- one request, no pagination -- so anything left with an
    older ``fetched_at`` is genuinely gone from their catalog. Returned as a queryset rather than
    deleted: dropping the row also drops the only record that the SKU ever existed, and
    ``is_inactive`` already covers the discontinued-but-listed case.
    """
    return TdgProduct.objects.filter(fetched_at__lt=before)
