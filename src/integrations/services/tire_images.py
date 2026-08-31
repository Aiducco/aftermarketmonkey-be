"""
Fill ``MasterPart.image_url`` for tires, from whichever catalog we matched them to.

Two thirds of our tires have no picture -- 31,960 of 47,655 -- and both scraped catalogs carry one.
The match has already been made and recorded on the spec (``simpletire_sku`` / ``tdg_product``), so
this pass is only a copy: no matching, no heuristics, no new trust placed in anything.

**This is the one part of the tire pipeline that writes to ``master_parts``**, which is the table
the whole product catalog is built on. Everything about it is therefore deliberately narrow:

* only rows that have a ``tire_specs`` row, so nothing outside tires can be touched
* only rows whose ``image_url`` is empty -- an existing image is never replaced
* only the one column, named explicitly in ``bulk_update``
* nothing is written unless the caller passes ``apply_changes``

A tire photograph is a property of the *model*, not the size: SimpleTire publishes 9,023 distinct
images across 58,124 SKUs and TDG 1,288 across 30,540, roughly 24 sizes to a picture. That is
correct and expected, not a sign of bad data -- every 275/60R20 of a Ridge Grappler looks the same.

What is *not* correct is a picture that stands in for a tire nobody photographed. SimpleTire serves
one: ``_generic/sidewall/sample-tire.png``, used by 4,792 SKUs across 339 unrelated brands. It is
filtered out by ``is_placeholder``, and the test for that filter is the reason to keep the check
rather than trust the URL. TDG has no equivalent -- zero of its images are shared across brands.

Preference is SimpleTire first, TDG second, matching the spec merge: not because its pictures are
better, but so a tire described by both catalogs takes both its specs and its image from the same
place and cannot end up showing one manufacturer's photo beside another's measurements.
"""
import dataclasses
import logging
import typing

from django.db import transaction

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRE-IMAGES]"

BATCH_SIZE = 1000

# A URL that is not a picture of the product. Substring rather than exact match: the same generic
# asset is served at several sizes.
_PLACEHOLDER_MARKERS = ("/_generic/", "sample-tire", "no-image", "noimage", "placeholder")

# Above this many distinct brands, an image cannot be a product photograph. Used by the audit
# helper rather than the copy itself, so a new placeholder shows up as a number to look at.
MAX_BRANDS_PER_IMAGE = 20


def is_placeholder(url: typing.Optional[str]) -> bool:
    if not url:
        return True
    lowered = url.lower()
    return any(marker in lowered for marker in _PLACEHOLDER_MARKERS)


@dataclasses.dataclass
class ImageStats:
    scanned: int = 0
    already_had_one: int = 0
    no_catalog_match: int = 0
    catalog_has_none: int = 0
    placeholder_skipped: int = 0
    filled: int = 0
    written: int = 0
    by_source: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    samples: typing.List[str] = dataclasses.field(default_factory=list)

    def bump(self, source: str) -> None:
        self.by_source[source] = self.by_source.get(source, 0) + 1


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool = False,
    limit: typing.Optional[int] = None,
) -> ImageStats:
    stats = ImageStats()

    specs = src_models.TireSpec.objects.select_related("master_part", "simpletire_sku", "tdg_product").order_by(
        "master_part_id"
    )
    if brand_ids:
        specs = specs.filter(master_part__brand_id__in=list(brand_ids))

    pending: typing.List[typing.Any] = []
    for spec in specs.iterator(chunk_size=BATCH_SIZE):
        if limit is not None and stats.scanned >= limit:
            break
        stats.scanned += 1
        part = spec.master_part
        if part.image_url:
            stats.already_had_one += 1
            continue
        if spec.simpletire_sku_id is None and spec.tdg_product_id is None:
            stats.no_catalog_match += 1
            continue

        # SimpleTire first, for the same reason it wins the spec merge: one tire, one source.
        candidates = [
            ("simpletire", spec.simpletire_sku.product_line_image_url if spec.simpletire_sku_id else None),
            ("tdg", spec.tdg_product.product_image_url if spec.tdg_product_id else None),
        ]
        offered = [(source, url) for source, url in candidates if url]
        usable = [(source, url) for source, url in offered if not is_placeholder(url)]
        if not offered:
            stats.catalog_has_none += 1
            continue
        if not usable:
            stats.placeholder_skipped += 1
            continue

        source, url = usable[0]
        part.image_url = url
        stats.filled += 1
        stats.bump(source)
        if len(stats.samples) < 8:
            stats.samples.append("{} {} <- {}".format(source, part.part_number, url[:76]))
        pending.append(part)

        if apply_changes and len(pending) >= BATCH_SIZE:
            stats.written += _write(pending)
            pending = []

    if apply_changes and pending:
        stats.written += _write(pending)
    return stats


@transaction.atomic
def _write(parts: typing.Sequence[typing.Any]) -> int:
    """
    One column, named explicitly.

    ``master_parts`` carries the entire product catalog, so the write is as narrow as it can be
    made: these instances were loaded from the database, only ``image_url`` was touched, and
    ``bulk_update`` is told that is the only column it may send.
    """
    src_models.MasterPart.objects.bulk_update(list(parts), ["image_url"], batch_size=500)
    logger.info("%s filled %d image urls", _LOG_PREFIX, len(parts))
    return len(parts)
