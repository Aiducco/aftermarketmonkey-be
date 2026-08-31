"""
Pull the Wheel Pros Product API catalogue into ``wheelpros_parts``.

Transport: :class:`src.integrations.clients.wheelpros.product_client.WheelProsProductApiClient`.
Entry point: the ``fetch_wheelpros_products`` management command.
Spec: https://developer.wheelpros.com/assets/specs/product-api/openapi/api.html

Why this exists alongside the SFTP sync
---------------------------------------
``wheelpros.fetch_and_save_wheelpros`` already builds ``wheelpros_parts`` from the nightly SFTP
CSV. This does not replace it. The CSV is the source of per-warehouse availability and the
per-company MSRP/MAP that ``WheelProsCompanyPricing`` is derived from; the API is the source of
everything the CSV never carried -- the full image set (four size variants per aspect), UPC,
structured wheel properties, and real ``nip`` cost. So the API run *enriches* CSV rows rather
than overwriting them, and writes its payload to ``api_data``, never to ``raw_data``.

The API catalogue is also substantially larger: ~115.9k SKUs against the CSV's ~69.1k. SKUs the
CSV never carried are inserted, with API inventory and **no purchase pricing** -- see
:func:`build_part` and the command's ``--no-insert``.

The 10,000-row window, and why every slice is checked
------------------------------------------------------
The search endpoints page with ``page``/``pageSize``, and refuse to return anything past an
offset of 10,000 (``page * pageSize > 10000`` is a 500). Worse, ``totalCount`` *saturates* at
exactly ``10000`` rather than reporting the true size, so a naive crawl of ``search/wheel`` reads
"10000 results", fetches all 10,000, and looks like it finished -- having silently missed 75,863
of the 85,863 wheels that are actually there.

Facet counts are not subject to that window. So this module works exclusively from facets:

1. ask for the facets of a search and read the true count;
2. partition the search along a facet axis so that no slice exceeds the window;
3. recurse onto the next axis for any slice that is still too big;
4. after fetching a slice, **assert the row count matches the facet count** -- a slice that comes
   up short is a hard error, not a silent truncation.

``wheel_diameter`` is the right first axis for wheels: exactly one value per SKU, so slices
partition cleanly with no double counting. ``brand`` deliberately is **not** -- it is a *prefix*
match on the API side (``brand=American Force`` also returns "American Force Cast" and
"American Force Powersports"), so brand slices overlap and summing them over-counts.

Matching to existing rows
-------------------------
``WheelProsPart`` is keyed ``(brand, part_number)``. The API's ``sku`` matches ``part_number``
directly for most rows but not all -- observed match rates are ~99% for tires, ~82% for
accessories and ~28% for wheels (the wheel gap is mostly API-only SKUs, not a formatting
difference). Brands resolve by upper-cased name, which is exactly how ``WheelProsBrand.
external_id`` is already populated.
"""
import dataclasses
import decimal
import json
import logging
import pathlib
import typing

from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.integrations import credentials as credentials_helper
from src.integrations.clients.wheelpros import exceptions as wheelpros_exceptions
from src.integrations.clients.wheelpros.product_client import WheelProsProductApiClient

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-PRODUCTS]"

DEFAULT_PAGE_SIZE = 1000
DEFAULT_BATCH_SIZE = 500
DEFAULT_RATE_PER_SECOND = 4.0

# Hard ceiling the API enforces on page*pageSize. Slices are partitioned to stay under it, with
# margin so a catalogue that grows between the facet call and the fetch does not tip over.
RESULT_WINDOW = 10000
SLICE_TARGET = 9000

# skuType as the API spells it -> feed_type as wheelpros_parts already spells it. The CSV sync
# writes "accessories"; the API says "ACC". Matching the existing vocabulary matters because
# feed_type is what downstream code filters on.
_SKU_TYPE_TO_FEED_TYPE = {"WHEEL": "wheel", "TIRE": "tire", "ACC": "accessories"}

# Search kind -> the facet axes to partition by, most selective first.
_PARTITION_AXES = {
    "wheel": [("diameter", "wheel_diameter"), ("width", "width"), ("finish", "abbreviated_finish_desc")],
    "tire": [("diameter", "diameter"), ("width", "width")],
    "accessory": [("brand", "brandDescription")],
}


class WheelProsProductError(Exception):
    """The crawl cannot proceed, or a slice came back incomplete."""


@dataclasses.dataclass
class ProductStats:
    slices_planned: int = 0
    slices_done: int = 0
    slices_skipped: int = 0
    rows_fetched: int = 0
    parts_updated: int = 0
    parts_inserted: int = 0
    brands_created: int = 0
    feed_types_backfilled: int = 0
    unmatched: int = 0
    requests_made: int = 0


@dataclasses.dataclass(frozen=True)
class Slice:
    """One partitioned search: a kind plus the filters that narrow it under the window."""

    kind: str
    filters: tuple[tuple[str, str], ...]
    expected: int

    @property
    def params(self) -> dict:
        return dict(self.filters)

    @property
    def key(self) -> str:
        return "{}|{}".format(self.kind, json.dumps(sorted(self.filters)))

    def __str__(self) -> str:
        if not self.filters:
            return "{} (all)".format(self.kind)
        return "{} {}".format(self.kind, " ".join("{}={}".format(k, v) for k, v in self.filters))


# -- planning -------------------------------------------------------------------------------------


def _filterable(value: str, *, param: str, kind: str) -> str:
    """
    A facet value trimmed to something the search endpoint will actually accept as a filter.

    Wheel Pros' own catalogue contains brand names with an ampersand ("Teraflex Axles & Shafts"),
    and passing one back as a filter returns a hard 500 ("Error while searching ACC") no matter
    how it is encoded -- their bug, not an encoding mistake on our side. Since ``brand`` matches
    as a *prefix*, truncating at the ampersand addresses the same rows: ``brand=Teraflex Axles``
    returns exactly the 168 SKUs that ``Teraflex Axles & Shafts`` refuses to.

    The prefix may match sibling brands too. That is harmless -- slices are allowed to overlap,
    rows are keyed by sku, and the upsert is idempotent -- and the slice's expected count is
    re-derived from the truncated filter, so the completeness check still holds.
    """
    if not isinstance(value, str) or "&" not in value:
        return value
    trimmed = value.split("&")[0].strip()
    logger.info(
        "%s %s: %s=%r 500s on their side; using the prefix %r instead",
        _LOG_PREFIX, kind, param, value, trimmed,
    )
    return trimmed


def plan_slices(
    client,
    kind: str,
    *,
    axes: typing.Optional[list] = None,
    stats: typing.Optional[ProductStats] = None,
    progress: typing.Optional[typing.Callable[[str], None]] = None,
) -> list[Slice]:
    """
    Partition ``kind`` into slices that each fit inside the result window.

    Recursive: a slice still over ``SLICE_TARGET`` after the first axis is split again on the
    next. A slice that cannot be reduced far enough by any available axis is returned anyway,
    with a warning -- fetching it will retrieve the first 10,000 and :func:`fetch_slice` will
    then fail the completeness check rather than pretend it is whole.
    """
    stats = stats or ProductStats()
    emit = progress or (lambda message: None)
    axes = axes if axes is not None else _PARTITION_AXES.get(kind, [])

    def split(filters: tuple, remaining: list, expected: int) -> list[Slice]:
        if expected <= SLICE_TARGET or not remaining:
            if expected > RESULT_WINDOW:
                logger.warning(
                    "%s slice %s has %s rows and no axis left to split on; it will fail its "
                    "completeness check", _LOG_PREFIX, filters, expected,
                )
            return [Slice(kind=kind, filters=filters, expected=expected)]

        (param, facet_name), rest = remaining[0], remaining[1:]
        buckets = client.facet_buckets(kind, facet_name, **dict(filters))
        if not buckets:
            return split(filters, rest, expected)

        # The buckets must account for every row, or the rows they miss are in no slice at all
        # and would never be fetched -- the exact silent truncation this partitioning exists to
        # avoid. Fall back to the unsliced query rather than crawl a knowingly-partial plan.
        covered = sum(count for _, count in buckets)
        if covered < expected:
            logger.warning(
                "%s %s%s: the %s facet accounts for %s of %s rows; %s would be unreachable, so "
                "this level is left unsliced", _LOG_PREFIX, kind,
                " {}".format(dict(filters)) if filters else "", facet_name, covered, expected,
                expected - covered,
            )
            return split(filters, rest, expected)

        out: list[Slice] = []
        for value, count in buckets:
            if not count:
                continue
            # An empty facet value cannot be expressed as a filter, so it can never be fetched
            # as its own slice. Surface it rather than silently dropping those rows.
            original_value = value
            value = _filterable(value, param=param, kind=kind)
            if value == "":
                logger.warning(
                    "%s %s: %s rows have an empty %s and cannot be sliced on it",
                    _LOG_PREFIX, kind, count, param,
                )
                out.append(Slice(kind=kind, filters=filters, expected=count))
                continue
            child_filters = filters + ((param, value),)
            if value != original_value:
                # The prefix may span more rows than this one bucket; ask for the real figure so
                # fetch_slice checks against what the filter will actually return.
                count = client.true_count(kind, **dict(child_filters))
            out.extend(split(child_filters, rest, count))
        return out

    total = client.true_count(kind)
    emit("  {}: {} SKUs (API totalCount reports {})".format(kind, total, min(total, RESULT_WINDOW)))
    slices = split((), list(axes), total)
    stats.slices_planned += len(slices)
    emit("  {}: {} slices, largest {}".format(
        kind, len(slices), max((s.expected for s in slices), default=0)))
    return slices


# -- fetching -------------------------------------------------------------------------------------


def fetch_slice(client, slice_: Slice, *, page_size: int = DEFAULT_PAGE_SIZE) -> list[dict]:
    """
    Page through one slice and return its rows, keyed unique by sku.

    Raises :class:`WheelProsProductError` if fewer rows come back than the facet promised -- the
    whole point of the partitioning is that a short read means truncation, and truncation must
    never be mistaken for a complete catalogue.
    """
    rows: dict[str, dict] = {}
    page = 1
    while True:
        if page * page_size > RESULT_WINDOW:
            break
        batch = client.search(slice_.kind, page=page, page_size=page_size, **slice_.params)
        if not batch:
            break
        for row in batch:
            sku = (row.get("sku") or "").strip()
            if sku:
                rows[sku] = row
        page += 1

    if len(rows) < slice_.expected:
        raise WheelProsProductError(
            "slice {} returned {} rows but its facet promised {} -- the result window truncated "
            "it. Split it further before trusting this run.".format(slice_, len(rows), slice_.expected)
        )
    return list(rows.values())


# -- payload -> row -------------------------------------------------------------------------------


def _decimal(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    if value in (None, "", "null"):
        return None
    try:
        parsed = decimal.Decimal(str(value).replace(",", "").strip())
    except (decimal.InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _price(row: dict, kind: str) -> typing.Optional[decimal.Decimal]:
    entries = (row.get("prices") or {}).get(kind) or []
    if not entries:
        return None
    return _decimal(entries[0].get("currencyAmount"))


def primary_image_url(row: dict) -> typing.Optional[str]:
    """The one URL that fits ``image_url``. Prefers the "Standard" aspect -- the product shot the
    CSV feed also used -- falling back to whatever image is first. The full set, every aspect at
    every size, stays available on ``api_data``."""
    images = row.get("images") or []
    if not images:
        return None
    chosen = next((i for i in images if (i.get("aspect") or "").lower() == "standard"), images[0])
    return chosen.get("imageUrlLarge") or chosen.get("imageUrlOriginal") or None


def total_qoh(row: dict) -> typing.Optional[int]:
    """Stock as the API reports it. ``globalStock`` is the network-wide figure, which is what
    ``total_qoh`` has always meant here; local is kept on ``api_data`` alongside it."""
    inventory = row.get("inventory") or {}
    value = inventory.get("globalStock")
    if value is None:
        value = inventory.get("localStock")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def feed_type_for(row: dict) -> typing.Optional[str]:
    return _SKU_TYPE_TO_FEED_TYPE.get((row.get("skuType") or "").strip().upper())


def build_part(row: dict, *, brand: src_models.WheelProsBrand, synced_at) -> src_models.WheelProsPart:
    """
    A brand-new ``WheelProsPart`` for a SKU the CSV feed never carried.

    ``raw_data`` stays NULL -- that field means "the CSV row", and there is no CSV row. That is
    also the discriminator for finding API-only parts later: ``raw_data__isnull=True,
    api_synced_at__isnull=False``.

    Inventory comes from the API. **Purchase pricing does not**: ``WheelProsCompanyPricing`` is
    derived from a company's own CSV feed and its negotiated discount, and inventing a cost for a
    SKU that feed never mentioned would put a fabricated number in front of a buyer. ``msrp_usd``
    and ``map_usd`` are list prices and safe to carry; cost is left to the pricing job.
    """
    properties = row.get("properties") or {}
    return src_models.WheelProsPart(
        brand=brand,
        feed_type=feed_type_for(row),
        part_number=(row.get("sku") or "").strip()[:255],
        part_description=row.get("title") or None,
        display_style_no=(properties.get("model") or None),
        finish=(properties.get("finish") or None),
        size=None,
        bolt_pattern=(properties.get("boltPattern") or None),
        offset=(properties.get("offset") or None),
        center_bore=(properties.get("centerbore") or None),
        load_rating=None,
        shipping_weight=None,
        image_url=primary_image_url(row),
        inv_order_type=((row.get("inventory") or {}).get("type") or None),
        style=None,
        total_qoh=total_qoh(row),
        msrp_usd=_price(row, "msrp"),
        map_usd=_price(row, "map"),
        run_date=None,
        warehouse_availability=None,
        raw_data=None,
        api_data=row,
        api_synced_at=synced_at,
    )


def apply_enrichment(part: src_models.WheelProsPart, row: dict, *, synced_at) -> list[str]:
    """
    Update an existing CSV-sourced row from its API twin, in place.

    Returns the names of the fields that changed, so the caller can upsert only what moved.
    Conservative on purpose: it refreshes what the API is authoritative for (the payload,
    inventory, images) and fills gaps (``feed_type``), but never blanks a CSV value that the API
    happens not to carry -- the CSV remains the source of size, load rating, shipping weight and
    per-warehouse availability.
    """
    changed = ["api_data", "api_synced_at"]
    part.api_data = row
    part.api_synced_at = synced_at

    # The user-visible gap this run exists to close: rows the CSV never labelled.
    if not part.feed_type:
        derived = feed_type_for(row)
        if derived:
            part.feed_type = derived
            changed.append("feed_type")

    stock = total_qoh(row)
    if stock is not None and stock != part.total_qoh:
        part.total_qoh = stock
        changed.append("total_qoh")

    image = primary_image_url(row)
    if image and not (part.image_url or "").strip():
        part.image_url = image
        changed.append("image_url")

    if not (part.part_description or "").strip() and row.get("title"):
        part.part_description = row["title"]
        changed.append("part_description")

    return changed


# -- brands ---------------------------------------------------------------------------------------


def resolve_brands(rows: typing.Iterable[dict], *, create_missing: bool, stats: ProductStats) -> dict:
    """
    Map upper-cased API brand description -> ``WheelProsBrand``.

    ``WheelProsBrand.external_id`` is already the upper-cased name for every existing row, so
    that is the key both sides agree on. Only three API brands were missing at time of writing
    (ASANTI, LEVEL 8 POWERSPORTS, MOTEGI FORGED).
    """
    wanted = set()
    for row in rows:
        name = ((row.get("brand") or {}).get("description") or "").strip()
        if name:
            wanted.add(name)

    by_key = {
        (b.external_id or "").strip().upper(): b
        for b in src_models.WheelProsBrand.objects.filter(
            external_id__in=[n.upper() for n in wanted]
        )
    }
    missing = sorted(n for n in wanted if n.upper() not in by_key)
    if missing and create_missing:
        created = src_models.WheelProsBrand.objects.bulk_create(
            [src_models.WheelProsBrand(external_id=n.upper(), name=n.upper()) for n in missing],
            ignore_conflicts=True,
        )
        stats.brands_created += len(missing)
        logger.info("%s created %s WheelPros brands: %s", _LOG_PREFIX, len(missing), missing)
        by_key.update({
            (b.external_id or "").strip().upper(): b
            for b in src_models.WheelProsBrand.objects.filter(external_id__in=[n.upper() for n in missing])
        })
    elif missing:
        logger.warning("%s %s API brands have no WheelProsBrand row: %s", _LOG_PREFIX, len(missing), missing)
    return by_key


# -- persistence ----------------------------------------------------------------------------------


def persist_rows(
    rows: list[dict],
    *,
    insert_new: bool,
    dry_run: bool,
    stats: ProductStats,
    unmatched_sink: typing.Optional[typing.Callable[[dict], None]] = None,
) -> None:
    """
    Enrich matching parts and (optionally) insert the rest.

    Matching is by ``part_number`` alone rather than the full ``(brand, part_number)`` key: the
    API's brand description and the CSV's brand name disagree often enough (parent vs sub-brand)
    that keying on both would miss real matches, while ``part_number`` collisions across Wheel
    Pros brands are not a thing in this catalogue.
    """
    if not rows:
        return
    synced_at = timezone.now()
    by_sku = {(r.get("sku") or "").strip(): r for r in rows if (r.get("sku") or "").strip()}

    existing = list(src_models.WheelProsPart.objects.filter(part_number__in=list(by_sku)))
    to_update: list[src_models.WheelProsPart] = []
    update_fields: set[str] = set()
    for part in existing:
        row = by_sku.get(part.part_number)
        if row is None:
            continue
        changed = apply_enrichment(part, row, synced_at=synced_at)
        if "feed_type" in changed:
            stats.feed_types_backfilled += 1
        update_fields.update(changed)
        to_update.append(part)

    matched = {p.part_number for p in existing}
    new_rows = [row for sku, row in by_sku.items() if sku not in matched]
    stats.unmatched += len(new_rows)
    if unmatched_sink:
        for row in new_rows:
            unmatched_sink(row)

    if dry_run:
        stats.parts_updated += len(to_update)
        if insert_new:
            stats.parts_inserted += len(new_rows)
        return

    if to_update:
        src_models.WheelProsPart.objects.bulk_update(
            to_update, sorted(update_fields | {"updated_at"}), batch_size=DEFAULT_BATCH_SIZE
        )
        stats.parts_updated += len(to_update)

    if insert_new and new_rows:
        brands = resolve_brands(new_rows, create_missing=True, stats=stats)
        instances = []
        for row in new_rows:
            name = ((row.get("brand") or {}).get("description") or "").strip().upper()
            brand = brands.get(name)
            if brand is None:
                logger.warning("%s no brand for sku %s (%r); skipping", _LOG_PREFIX, row.get("sku"), name)
                continue
            instances.append(build_part(row, brand=brand, synced_at=synced_at))
        if instances:
            # Django's native upsert rather than pgbulk, which the rest of this codebase uses:
            # pgbulk picks its SQL-quoting path from whichever psycopg it can import, so on a
            # machine with both psycopg2 and psycopg3 installed it detects 3 while Django runs
            # on 2 and dies in _quote. bulk_create works on either driver, and ON CONFLICT DO
            # UPDATE is the same statement underneath.
            src_models.WheelProsPart.objects.bulk_create(
                instances,
                update_conflicts=True,
                unique_fields=["brand", "part_number"],
                update_fields=["api_data", "api_synced_at", "total_qoh", "image_url", "feed_type"],
                batch_size=DEFAULT_BATCH_SIZE,
            )
            stats.parts_inserted += len(instances)


# -- credentials ----------------------------------------------------------------------------------


def get_product_api_client(
    company_provider_id: typing.Optional[int] = None,
) -> WheelProsProductApiClient:
    """
    Build a client from a Wheel Pros connection's API credentials.

    Unlike the vehicle/warehouse endpoints, product *pricing* is account-scoped -- ``nip`` is the
    calling customer's negotiated cost -- so which connection is used matters. Defaults to the
    primary active connection that has credentials; pass ``company_provider_id`` to pin one (16's
    connection is id 17).
    """
    queryset = src_models.CompanyProviders.objects.filter(
        provider__kind=src_enums.BrandProviderKind.WHEELPROS.value, active=True
    )
    if company_provider_id is not None:
        queryset = queryset.filter(id=company_provider_id)

    for company_provider in queryset.order_by("-primary", "id"):
        creds = credentials_helper.get_order_credentials(company_provider)
        if creds.get("username") and creds.get("password"):
            logger.info(
                "%s Using CompanyProviders id=%s (company_id=%s) for Product API auth.",
                _LOG_PREFIX, company_provider.id, company_provider.company_id,
            )
            return WheelProsProductApiClient(credentials=creds)

    raise WheelProsProductError(
        "No active Wheel Pros connection with API username/password found{}.".format(
            " for company_provider_id={}".format(company_provider_id) if company_provider_id else ""
        )
    )


# -- the run --------------------------------------------------------------------------------------


def run(
    client,
    *,
    kinds: typing.Sequence[str] = ("wheel", "tire", "accessory"),
    insert_new: bool = True,
    dry_run: bool = False,
    page_size: int = DEFAULT_PAGE_SIZE,
    unmatched_path: typing.Optional[pathlib.Path] = None,
    progress: typing.Optional[typing.Callable[[str], None]] = None,
    stats: typing.Optional[ProductStats] = None,
) -> ProductStats:
    """Plan every kind, then fetch and persist slice by slice."""
    stats = stats or ProductStats()
    emit = progress or (lambda message: None)

    sink = None
    handle = None
    if unmatched_path is not None:
        unmatched_path.parent.mkdir(parents=True, exist_ok=True)
        handle = unmatched_path.open("w", encoding="utf-8")
        def sink(row):  # noqa: E306 - tied to the handle's lifetime
            handle.write(json.dumps({
                "sku": row.get("sku"), "skuType": row.get("skuType"),
                "brand": (row.get("brand") or {}).get("description"), "title": row.get("title"),
            }) + "\n")

    try:
        for kind in kinds:
            emit("Planning {} ...".format(kind))
            slices = plan_slices(client, kind, stats=stats, progress=emit)
            for index, slice_ in enumerate(slices, start=1):
                try:
                    rows = fetch_slice(client, slice_, page_size=page_size)
                except (WheelProsProductError, wheelpros_exceptions.WheelProsException) as exc:
                    stats.slices_skipped += 1
                    logger.warning("%s %s", _LOG_PREFIX, exc)
                    emit("  FAILED {}: {}".format(slice_, exc))
                    continue
                stats.rows_fetched += len(rows)
                persist_rows(
                    rows, insert_new=insert_new, dry_run=dry_run, stats=stats, unmatched_sink=sink
                )
                stats.slices_done += 1
                emit("  [{}/{}] {} -> {} rows (updated {}, inserted {})".format(
                    index, len(slices), slice_, len(rows), stats.parts_updated, stats.parts_inserted))
    finally:
        if handle is not None:
            handle.close()

    stats.requests_made = client.requests_made
    return stats
