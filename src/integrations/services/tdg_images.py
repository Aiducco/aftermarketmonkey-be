"""
Backfill product images from ``tdg_products`` into the catalog.

One-time job, driven by the ``backfill_tdg_images`` command. It is the first thing that joins the
TDG landing table to ``master_parts`` -- everything else about that table is deliberately
unjoined -- so the matching rules live here rather than being inlined at the call site.

What it writes, and what it will not touch
------------------------------------------
Only empty fields, on parts we can identify:

* ``MasterPart.image_url`` when it is NULL or blank;
* ``MasterPartData.images`` when the row exists and its list is empty.

A part that already has an image keeps it. TDG is one distributor's art, not a better source than
whatever is already there, and this job has no way to judge which of two images is nicer -- so
"empty" is the whole of its remit and re-running it is a no-op.

**Parts with no ``MasterPartData`` row are skipped, not created.** Every one of the 2.2M existing
rows carries a ``source_provider``, and there is no TDG provider; inventing rows with a NULL
provider would break that invariant to store one URL. The count is reported so the gap stays
visible, but creating a provider is a decision for a human.

Matching
--------
GTIN first, then brand + manufacturer part number. Both need normalising because neither side is
clean:

* our GTINs carry leading zeros inconsistently, so both sides are reduced to digits with leading
  zeros stripped -- ``'086699174888'`` and ``'86699174888'`` are the same barcode;
* our brand names are suffixed where TDG's are bare (``'YOKOHAMA TIRE'`` vs ``'Yokohama'``), so a
  literal join finds a fraction of what is there;
* part numbers differ in punctuation only (``'AH-4126'`` vs ``'AH4126'``).

GTIN is tried first because it identifies a package outright, where brand+MPN can collide across
two brands that normalise to the same key.

One image, many SKUs
--------------------
TDG's ``productImageUrl`` is per **product line**, so every size of a model repeats one URL and a
backfill legitimately writes the same image to dozens of parts. That is not a bug: the shared
URLs were checked against their product lines and they are variant spellings of one model
(``'Geolandar X CV'`` / ``'GL X-CV G057F'``, ``'Open Country HT'`` / ``'OPHTD'``), not generic
brand art. No URL in the catalog is shared across two different brands.
"""
import collections
import dataclasses
import logging
import re
import typing

from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from src.models import Brands, MasterPart, MasterPartData, TdgProduct

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TDG-IMAGES]"

DEFAULT_BATCH_SIZE = 2000

# Words that appear in our brand names and not in TDG's. Stripped from both sides so
# 'COOPER TIRES' and 'Cooper' collapse to the same key.
# 'and' is in here for 'CARLISLE TIRE AND WHEEL COMPANY', which otherwise normalises to
# 'carlisleand' and matches nothing -- the connector survives once the words around it are gone.
_BRAND_NOISE = re.compile(
    r"\b(tire|tires|tyre|tyres|wheel|wheels|corporation|corp|inc|usa|company|co|na|brand|brands|llc|group|and|the|of)\b"
)
# Deliberate, hand-checked equivalences. Kept short and explicit rather than fuzzy-matched: a
# wrong brand pairing here silently puts one manufacturer's photo on another's product.
_BRAND_ALIASES = {
    "goodrich": "bfgoodrich",
    "pirellib": "pirelli",
    "carlstar": "carlisle",
    "toyomotorsports": "toyo",
}


@dataclasses.dataclass
class BackfillStats:
    tdg_images: int = 0
    candidates_scanned: int = 0
    matched: int = 0
    image_url_filled: int = 0
    data_images_filled: int = 0
    skipped_no_data_row: int = 0
    skipped_already_set: int = 0
    by_match: typing.Dict[str, int] = dataclasses.field(default_factory=dict)


def normalize_gtin(value: typing.Optional[str]) -> typing.Optional[str]:
    """Digits only, leading zeros stripped -- '086699174888' and '86699174888' are one barcode."""
    digits = re.sub(r"\D", "", value or "")
    return digits.lstrip("0") or None


def normalize_brand(value: typing.Optional[str]) -> str:
    """'YOKOHAMA TIRE' -> 'yokohama'. Empty string when there is nothing left to match on."""
    text = re.sub(r"[^a-z0-9 ]", " ", (value or "").lower())
    text = _BRAND_NOISE.sub("", text)
    text = re.sub(r"[^a-z0-9]", "", text)
    return _BRAND_ALIASES.get(text, text)


def normalize_part_number(value: typing.Optional[str]) -> str:
    """'AH-4126' -> 'AH4126'. Punctuation is the only difference between the two catalogs."""
    return re.sub(r"[^A-Z0-9]", "", (value or "").upper())


def build_image_index() -> typing.Tuple[typing.Dict[str, str], typing.Dict[typing.Tuple[str, str], str]]:
    """
    Two lookups over every TDG row that has an image: by GTIN, and by (brand, part number).

    First writer wins on a collision. Sizes of one model share an image, so a second row claiming
    the same key almost always carries the identical URL -- and where it does not, neither is more
    authoritative than the other.
    """
    by_gtin: typing.Dict[str, str] = {}
    by_brand_part: typing.Dict[typing.Tuple[str, str], str] = {}
    rows = TdgProduct.objects.exclude(product_image_url__isnull=True).exclude(product_image_url="")
    for row in rows.values("gtin", "brand_name", "part_number", "product_image_url").iterator(chunk_size=10000):
        url = row["product_image_url"]
        gtin = normalize_gtin(row["gtin"])
        if gtin:
            by_gtin.setdefault(gtin, url)
        brand = normalize_brand(row["brand_name"])
        part = normalize_part_number(row["part_number"])
        if brand and part:
            by_brand_part.setdefault((brand, part), url)
    return by_gtin, by_brand_part


def _candidate_queryset(by_gtin, by_brand_part):
    """
    Narrow 3M master parts to the ones that could possibly match.

    Driving from the TDG side keeps this to a brand filter plus a GTIN set rather than a full
    scan: a part matches only if its brand is one TDG stocks, or its barcode is one TDG published.
    """
    tdg_brand_keys = {brand for brand, _ in by_brand_part}
    brand_ids = [
        brand["id"] for brand in Brands.objects.values("id", "name") if normalize_brand(brand["name"]) in tdg_brand_keys
    ]
    logger.info("%s %d of our brands map to a TDG brand", _LOG_PREFIX, len(brand_ids))
    return (
        MasterPart.objects.filter(Q(brand_id__in=brand_ids) | Q(gtin__in=list(by_gtin)))
        .values("id", "brand__name", "part_number", "gtin", "image_url", "data__id", "data__images")
        .order_by("id")
    )


def run_backfill(
    *,
    batch_size: int = DEFAULT_BATCH_SIZE,
    limit: typing.Optional[int] = None,
    dry_run: bool = False,
    progress: typing.Optional[typing.Callable[[str], None]] = None,
) -> BackfillStats:
    say = progress or (lambda message: None)
    stats = BackfillStats()

    by_gtin, by_brand_part = build_image_index()
    stats.tdg_images = len(by_gtin) + len(by_brand_part)
    say(f"Indexed TDG images: {len(by_gtin)} by GTIN, {len(by_brand_part)} by brand+part number")

    part_updates: typing.List[MasterPart] = []
    data_updates: typing.List[MasterPartData] = []
    now = timezone.now()

    for row in _candidate_queryset(by_gtin, by_brand_part).iterator(chunk_size=10000):
        stats.candidates_scanned += 1
        if limit is not None and stats.matched >= limit:
            break

        url, how = _lookup(row, by_gtin, by_brand_part)
        if url is None:
            continue
        stats.matched += 1
        stats.by_match[how] = stats.by_match.get(how, 0) + 1

        if not row["image_url"]:
            part_updates.append(MasterPart(id=row["id"], image_url=url, updated_at=now))
            stats.image_url_filled += 1

        if row["data__id"] is None:
            stats.skipped_no_data_row += 1
        elif not row["data__images"]:
            data_updates.append(MasterPartData(id=row["data__id"], images=[url], updated_at=now))
            stats.data_images_filled += 1
        else:
            stats.skipped_already_set += 1

        if len(part_updates) >= batch_size or len(data_updates) >= batch_size:
            _flush(part_updates, data_updates, dry_run=dry_run)
            part_updates, data_updates = [], []
            say(
                f"  scanned {stats.candidates_scanned}, matched {stats.matched}, "
                f"filled {stats.image_url_filled} image_url / {stats.data_images_filled} data.images"
            )

    _flush(part_updates, data_updates, dry_run=dry_run)
    return stats


def _lookup(row, by_gtin, by_brand_part) -> typing.Tuple[typing.Optional[str], str]:
    gtin = normalize_gtin(row["gtin"])
    if gtin:
        url = by_gtin.get(gtin)
        if url:
            return url, "gtin"
    key = (normalize_brand(row["brand__name"]), normalize_part_number(row["part_number"]))
    if all(key):
        url = by_brand_part.get(key)
        if url:
            return url, "brand+part_number"
    return None, ""


def _flush(part_updates, data_updates, *, dry_run: bool) -> None:
    if dry_run or not (part_updates or data_updates):
        return
    # updated_at is listed explicitly: bulk_update bypasses auto_now, and a row silently keeping
    # its old timestamp would make this backfill invisible to anything watching for changes.
    with transaction.atomic():
        if part_updates:
            MasterPart.objects.bulk_update(part_updates, ["image_url", "updated_at"])
        if data_updates:
            MasterPartData.objects.bulk_update(data_updates, ["images", "updated_at"])
