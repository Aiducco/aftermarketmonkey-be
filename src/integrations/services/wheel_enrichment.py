"""
Populate ``wheel_specs`` from the structured wheel feeds.

The wheel counterpart to ``tire_enrichment``, and deliberately not built the same way. Tires had
no structured attributes anywhere in the catalog: tread depth, load range and UTQG existed only
inside distributor prose, so every one of the 47,655 tire specs cost an LLM call to identify and a
parser pass to size. Wheels do not work like that. Wheel Pros publishes ``size``, ``bolt_pattern``,
``offset``, ``center_bore``, ``load_rating``, ``finish`` and ``style`` as their own columns, and
its ``feed_type`` states outright that the row is a wheel -- identification we had to buy for every
single tire.

So **this module calls no LLM at all**. Every field is either read from a column or derived by a
rule that can be read and tested:

    diameter / width        the feed's ``size``, parsed by src.domain.wheel_size
    bolt pattern(s)         the feed's ``bolt_pattern``, converted to millimetres
    offset / bore / load    the feed's own columns
    model_name              the feed's ``style``
    style_number            the feed's ``display_style_no``
    finish_family           src.integrations.services.wheel_finish, 99.2% coverage
    beadlock / dually / UTV / forged    explicit keywords only, see _KEYWORDS

Nothing is guessed. ``" BL "`` looks like a beadlock marker and appears on 1,257 rows, but
``19X8.5BASTILLE 5X130 BL 6.64`` is not a beadlock wheel; ``" DL "`` appears on 6,672 and sits on
``FF071 DL 20X9 5X135``, a pattern no dually uses. Both are excluded. A field we cannot decide
stays NULL, which is the same rule the tire pipeline follows and the reason its numbers can be
trusted.

The feed is still cross-checked against the title. Where the two disagree on a dimension the row
is written with the feed's value and ``size_disputed`` set, because the feed is the better source
but a disagreement is worth seeing rather than swallowing.

Read-only unless the caller passes ``apply_changes``.
"""
import dataclasses
import decimal
import logging
import re
import typing

from django.db import connection, transaction
from django.utils import timezone

from src import enums as src_enums
from src import models as src_models
from src.domain import wheel_size
from src.integrations.services.wheel_finish import finish_family

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEEL-ENRICH]"

PAGE_SIZE = 2000
WRITE_BATCH = 500

FEED_WHEELPROS = "wheelpros"

# What a match on the description or the style column means. Only unambiguous markers: see the
# module docstring for the two that look useful and are not.
_KEYWORDS = {
    "is_beadlock": (re.compile(r"\bBEADLOCK\b", re.IGNORECASE), True),
    "is_dually": (re.compile(r"\bDUALLY\b", re.IGNORECASE), True),
}
_UTV_RE = re.compile(r"\bUTV\b", re.IGNORECASE)
_FORGED_RE = re.compile(r"\bFORGED\b", re.IGNORECASE)


@dataclasses.dataclass
class EnrichStats:
    scanned: int = 0
    written: int = 0
    no_size: int = 0
    no_bolt_pattern: int = 0
    blank_drilled: int = 0
    size_disputed: int = 0
    with_offset: int = 0
    with_finish_family: int = 0
    with_model_name: int = 0
    product_type_set: int = 0
    samples: typing.List[str] = dataclasses.field(default_factory=list)
    skipped: typing.Dict[str, int] = dataclasses.field(default_factory=dict)

    def skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1


# ==============================================================================================
# Reading the feed
# ==============================================================================================
# ``DISTINCT ON``, freshest row first, because a master part can join the feed twice.
#
# ``wheelpros_parts`` is unique on ``(brand_id, part_number)``, not on the part number alone, and
# this join reaches it by part number. So when Wheel Pros moves a part to a different brand the
# sync inserts a second row instead of updating the first, and the original is orphaned:
# ``FO2138106145L`` exists under brand 20, last refreshed 2026-04-28, and under brand 21, refreshed
# today. 438 part numbers are in that state across the feed; five of them are wheels.
#
# That makes the ordering a correctness question rather than a tie-break. One row is being
# maintained and the other stopped months ago, so ``run_date`` decides. Without any de-duplication
# a write batch holds the same master_part_id twice and Postgres rejects the whole batch:
# "ON CONFLICT DO UPDATE command cannot affect row a second time".
_WHEELPROS_SQL = """
    SELECT DISTINCT ON (mp.id)
           mp.id            AS master_part_id,
           mp.description   AS master_description,
           wp.part_number,
           wp.part_description,
           wp.size,
           wp.bolt_pattern,
           wp."offset"      AS offset_raw,
           wp.center_bore,
           wp.load_rating,
           wp.finish,
           wp.style,
           wp.display_style_no,
           wp.image_url
    FROM master_parts mp
    JOIN provider_parts pp ON pp.master_part_id = mp.id AND pp.provider_id = %s
    JOIN wheelpros_parts wp ON wp.part_number = split_part(pp.provider_external_id, '_', 2)
    WHERE wp.feed_type = 'wheel'
      AND mp.id > %s
      {brand_filter}
    ORDER BY mp.id, wp.run_date DESC NULLS LAST, wp.id DESC
    LIMIT %s
"""

WHEELPROS_PROVIDER_ID = 6


def iter_wheelpros(*, brand_ids: typing.Optional[typing.Sequence[int]] = None) -> typing.Iterator[typing.List[dict]]:
    """Pages of Wheel Pros wheel rows joined to the master part they belong to."""
    brand_filter = "AND mp.brand_id = ANY(%s)" if brand_ids else ""
    sql = _WHEELPROS_SQL.format(brand_filter=brand_filter)

    last_id = 0
    while True:
        params: typing.List[typing.Any] = [WHEELPROS_PROVIDER_ID, last_id]
        if brand_ids:
            params.append(list(brand_ids))
        params.append(PAGE_SIZE)
        with connection.cursor() as cursor:
            cursor.execute(sql, params)
            names = [c[0] for c in cursor.description]
            page = [dict(zip(names, row)) for row in cursor.fetchall()]
        if not page:
            return
        last_id = page[-1]["master_part_id"]
        yield page


# ==============================================================================================
# Building one spec
# ==============================================================================================
def build_spec(row: dict, *, stats: typing.Optional[EnrichStats] = None):
    """
    One ``WheelSpec`` from one feed row, or ``None`` when the row does not describe a wheel.

    The feed wins on every dimension it publishes. The title is read anyway, for two reasons: it
    supplies the bolt pattern on the rows where the attribute column is empty, and where both
    speak it is a free cross-check.
    """
    text = " ".join(filter(None, (row.get("part_description"), row.get("style"), row.get("master_description"))))

    size = wheel_size.parse_size(row.get("size"))
    from_title = wheel_size.parse(row.get("part_description") or "")
    if size is None and from_title is not None:
        size = (from_title.diameter_in, from_title.width_in)
    if size is None:
        if stats:
            stats.no_size += 1
            stats.skip("no parseable size")
        return None
    diameter, width = size

    disputed = bool(from_title and (from_title.diameter_in, from_title.width_in) != (diameter, width))

    raw_pattern = row.get("bolt_pattern")
    blank = wheel_size.is_blank(raw_pattern)
    patterns = [] if blank else wheel_size.parse_bolt_patterns(raw_pattern)
    if not patterns and not blank and from_title is not None:
        patterns = [p for p in (from_title.bolt_pattern, from_title.bolt_pattern_2) if p]
    if patterns and from_title is not None and from_title.bolt_pattern is not None:
        theirs = (from_title.bolt_pattern.lug_count, from_title.bolt_pattern.circle_mm)
        if theirs != (patterns[0].lug_count, patterns[0].circle_mm):
            disputed = True

    offset = wheel_size.parse_offset_mm(row.get("offset_raw"))
    if offset is None and from_title is not None:
        offset = from_title.offset_mm

    spec = src_models.WheelSpec(
        master_part_id=row["master_part_id"],
        diameter_in=diameter,
        width_in=width,
        size_display="{}x{}".format(wheel_size._trim(diameter), wheel_size._trim(width)),
        bolt_lug_count=patterns[0].lug_count if patterns else None,
        bolt_circle_mm=patterns[0].circle_mm if patterns else None,
        bolt_pattern_display=patterns[0].display if patterns else None,
        bolt_lug_count_2=patterns[1].lug_count if len(patterns) > 1 else None,
        bolt_circle_mm_2=patterns[1].circle_mm if len(patterns) > 1 else None,
        bolt_pattern_2_display=patterns[1].display if len(patterns) > 1 else None,
        is_blank_drilled=blank,
        offset_mm=offset,
        center_bore_mm=wheel_size.parse_center_bore_mm(row.get("center_bore")),
        load_rating_lb=_positive_int(row.get("load_rating")),
        model_name=_clean(row.get("style")),
        style_number=_clean(row.get("display_style_no")),
        finish=_clean(row.get("finish"), limit=128),
        finish_family=finish_family(row.get("finish")),
        construction=src_models.WheelSpec.CONSTRUCTION_FORGED if _FORGED_RE.search(text) else None,
        vehicle_class="atv_utv" if _UTV_RE.search(text) else None,
        is_beadlock=True if _KEYWORDS["is_beadlock"][0].search(text) else None,
        is_dually=True if _KEYWORDS["is_dually"][0].search(text) else None,
        spec_source=src_models.WheelSpec.SPEC_SOURCE_FEED,
        source_feed=FEED_WHEELPROS,
        source_external_id=row.get("part_number"),
        size_disputed=disputed,
        search_aliases=[],
        enriched_at=timezone.now(),
    )

    if stats:
        if blank:
            stats.blank_drilled += 1
        elif not patterns:
            stats.no_bolt_pattern += 1
        if disputed:
            stats.size_disputed += 1
        if spec.offset_mm is not None:
            stats.with_offset += 1
        if spec.finish_family:
            stats.with_finish_family += 1
        if spec.model_name:
            stats.with_model_name += 1
    return spec


def _clean(value: typing.Optional[str], *, limit: int = 255) -> typing.Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text[:limit] or None


def _positive_int(value: typing.Any) -> typing.Optional[int]:
    try:
        number = int(decimal.Decimal(str(value)))
    except (TypeError, ValueError, decimal.InvalidOperation):
        return None
    return number if number > 0 else None


# ==============================================================================================
# The run
# ==============================================================================================
WRITE_FIELDS = (
    "diameter_in",
    "width_in",
    "size_display",
    "bolt_lug_count",
    "bolt_circle_mm",
    "bolt_pattern_display",
    "bolt_lug_count_2",
    "bolt_circle_mm_2",
    "bolt_pattern_2_display",
    "is_blank_drilled",
    "offset_mm",
    "center_bore_mm",
    "load_rating_lb",
    "model_name",
    "style_number",
    "finish",
    "finish_family",
    "construction",
    "vehicle_class",
    "is_beadlock",
    "is_dually",
    "spec_source",
    "source_feed",
    "source_external_id",
    "size_disputed",
    "enriched_at",
)


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    limit: typing.Optional[int] = None,
    apply_changes: bool = False,
) -> EnrichStats:
    stats = EnrichStats()
    pending: typing.List[src_models.WheelSpec] = []

    for page in iter_wheelpros(brand_ids=brand_ids):
        for row in page:
            if limit is not None and stats.scanned >= limit:
                break
            stats.scanned += 1
            spec = build_spec(row, stats=stats)
            if spec is None:
                continue
            if len(stats.samples) < 12:
                stats.samples.append(
                    "{:<9} {:<10} off={:<5} {:<22} {}".format(
                        spec.size_display,
                        spec.bolt_pattern_display or ("blank" if spec.is_blank_drilled else "-"),
                        spec.offset_mm if spec.offset_mm is not None else "-",
                        (spec.model_name or "")[:22],
                        (spec.finish or "")[:26],
                    )
                )
            pending.append(spec)
            if apply_changes and len(pending) >= WRITE_BATCH:
                stats.written += _write(pending, stats)
                pending = []
        if limit is not None and stats.scanned >= limit:
            break

    if apply_changes and pending:
        stats.written += _write(pending, stats)
    return stats


@transaction.atomic
def _write(specs: typing.Sequence[src_models.WheelSpec], stats: EnrichStats) -> int:
    """
    Upsert the specs and stamp the master part.

    ``update_conflicts`` on ``master_part_id`` rather than delete-and-insert, so re-running the
    command is idempotent and never leaves a window where a wheel has no spec row.
    """
    src_models.WheelSpec.objects.bulk_create(
        list(specs),
        update_conflicts=True,
        update_fields=list(WRITE_FIELDS),
        unique_fields=["master_part_id"],
        batch_size=WRITE_BATCH,
    )
    # The classification pass already stamps these from the feed's own type signal; this is the
    # backstop for a part that reached us before that ran.
    stamped = (
        src_models.MasterPart.objects.filter(id__in=[s.master_part_id for s in specs])
        .exclude(product_type=src_enums.ProductType.WHEEL.value)
        .update(
            product_type=src_enums.ProductType.WHEEL.value,
            product_type_source="wheelpros:feed_type",
            updated_at=timezone.now(),
        )
    )
    stats.product_type_set += stamped
    return len(specs)
