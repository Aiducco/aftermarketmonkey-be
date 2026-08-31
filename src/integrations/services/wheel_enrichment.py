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

# Bolt patterns that belong to exactly one kind of vehicle, so the class follows from the geometry
# with nothing guessed. Only these two: no passenger car is drilled 8x200 or 10x225, and nothing
# but an ATV or a side-by-side is drilled 4x137.
#
# Deliberately absent are the patterns that look tempting and are not decisive. 5x114.3 and 6x139.7
# span cars, crossovers and trucks; mapping them would have labelled 11,236 wheels on a guess.
# Those stay NULL until a feed says otherwise.
VEHICLE_CLASS_BY_BOLT_PATTERN = {
    (8, "200"): "commercial",
    (10, "225"): "commercial",
    (8, "210"): "commercial",
    (8, "275"): "commercial",
    (4, "137"): "atv_utv",
    (4, "156"): "atv_utv",
    (4, "110"): "atv_utv",
    (4, "115"): "atv_utv",
    (4, "144"): "atv_utv",
}


def _vehicle_class(text: str, patterns) -> typing.Optional[str]:
    """UTV in the name, or a bolt pattern only one kind of vehicle uses. Otherwise nothing."""
    if _UTV_RE.search(text):
        return "atv_utv"
    for pattern in patterns:
        # wheel_size._trim, not a bare rstrip: stripping trailing zeros off "200" yields "2", which
        # silently matched nothing for 8x200 and 8x210 while 10x225 worked, because 225 happens to
        # end in a non-zero digit.
        key = (pattern.lug_count, wheel_size._trim(pattern.circle_mm))
        found = VEHICLE_CLASS_BY_BOLT_PATTERN.get(key)
        if found:
            return found
    return None


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
    built: int = 0
    collisions: int = 0
    per_feed: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    collision_detail: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    samples: typing.List[str] = dataclasses.field(default_factory=list)
    skipped: typing.Dict[str, int] = dataclasses.field(default_factory=dict)

    def skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1

    def bump_collision(self, key: str) -> None:
        self.collision_detail[key] = self.collision_detail.get(key, 0) + 1


# ==============================================================================================
# The feeds
# ==============================================================================================
# Each feed states outright that a row is a wheel, publishes the dimensions as columns, and needs
# no LLM. What differs is only *which* columns and what they are called, so each adapter is a
# query that renames its own into one shared vocabulary; everything downstream is common.
#
# The SELECT list is identical across adapters, NULL-padded where a feed does not carry a field.
# That is what keeps ``build_spec`` from growing a branch per source.
#
# ``DISTINCT ON (mp.id)`` with the freshest row first, on every adapter: a master part can join a
# feed more than once (Wheel Pros orphans a row when a part changes brand -- see 0199), and a write
# batch holding one master_part_id twice makes Postgres reject the whole batch.

_COMMON_SELECT = """
    SELECT DISTINCT ON (mp.id)
           mp.id                AS master_part_id,
           mp.description       AS master_description,
           {part_number}        AS part_number,
           {title}              AS title,
           {size}               AS size_raw,
           {diameter}           AS diameter_raw,
           {width}              AS width_raw,
           {bolt_1}             AS bolt_pattern_1,
           {bolt_2}             AS bolt_pattern_2,
           {offset}             AS offset_raw,
           {center_bore}        AS center_bore_raw,
           {load_rating}        AS load_rating_raw,
           {backspace}          AS backspace_raw,
           {weight}             AS weight_raw,
           {finish}             AS finish_raw,
           {model}              AS model_raw,
           {style_number}       AS style_number_raw,
           {lug_seat}           AS lug_seat_raw,
           {lug_thread}         AS lug_thread_raw,
           {structural_warranty} AS structural_warranty_raw,
           {finish_warranty}    AS finish_warranty_raw,
           {tpms}               AS tpms_raw,
           {dually}             AS dually_raw,
           {image_url}          AS image_url
    FROM master_parts mp
    JOIN provider_parts pp ON pp.master_part_id = mp.id AND pp.provider_id = {provider_id}
    JOIN {table} f ON f.{key} = split_part(pp.provider_external_id, '_', 2)
    WHERE mp.id > %s
      {feed_where}
      {{brand_filter}}
    ORDER BY mp.id, {freshest}
    LIMIT %s
"""

_NULL = "NULL"


@dataclasses.dataclass(frozen=True)
class Feed:
    name: str
    provider_id: int
    sql: str


def _feed(name, provider_id, table, key, freshest, feed_where="", **columns) -> Feed:
    fields = {
        "part_number": _NULL,
        "title": _NULL,
        "size": _NULL,
        "diameter": _NULL,
        "width": _NULL,
        "bolt_1": _NULL,
        "bolt_2": _NULL,
        "offset": _NULL,
        "center_bore": _NULL,
        "load_rating": _NULL,
        "backspace": _NULL,
        "weight": _NULL,
        "finish": _NULL,
        "model": _NULL,
        "style_number": _NULL,
        "lug_seat": _NULL,
        "lug_thread": _NULL,
        "structural_warranty": _NULL,
        "finish_warranty": _NULL,
        "tpms": _NULL,
        "dually": _NULL,
        "image_url": _NULL,
    }
    fields.update(columns)
    return Feed(
        name=name,
        provider_id=provider_id,
        sql=_COMMON_SELECT.format(
            provider_id=provider_id, table=table, key=key, freshest=freshest, feed_where=feed_where, **fields
        ),
    )


FEED_WHEELPROS = "wheelpros"
FEED_THEWHEELGROUP = "thewheelgroup"
FEED_VOSSEN = "vossen"
FEED_ELITEWHEELS = "elitewheels"

# Precedence, best first. Consulted only when two feeds describe the same master part, which does
# not happen today -- measured across all four, zero master parts are reached by more than one, and
# zero (brand, part number) pairs appear in two feeds. The order exists because catalogs grow and
# the alternative is last-writer-wins, which would make the winner depend on dict iteration order.
#
# The Wheel Group leads on completeness: it is the only feed publishing lug seat, both warranties,
# TPMS and a real product weight, on 2,072 of 2,072 rows. Wheel Pros is next on volume. Vossen and
# Elite carry dimensions only.
FEED_ORDER = (FEED_THEWHEELGROUP, FEED_WHEELPROS, FEED_VOSSEN, FEED_ELITEWHEELS)

FEEDS = {
    FEED_WHEELPROS: _feed(
        FEED_WHEELPROS,
        6,
        "wheelpros_parts",
        "part_number",
        freshest="f.run_date DESC NULLS LAST, f.id DESC",
        feed_where="AND f.feed_type = 'wheel'",
        part_number="f.part_number",
        title="f.part_description",
        size="f.size",
        bolt_1="f.bolt_pattern",
        offset='f."offset"',
        center_bore="f.center_bore",
        load_rating="f.load_rating",
        finish="f.finish",
        model="f.style",
        style_number="f.display_style_no",
        image_url="f.image_url",
    ),
    # The richest of the four: everything the detail card asks for, for every row it has.
    FEED_THEWHEELGROUP: _feed(
        FEED_THEWHEELGROUP,
        31,
        "thewheelgroup_parts",
        "sku",
        freshest="f.updated_at DESC NULLS LAST, f.id DESC",
        part_number="f.sku",
        title="f.description",
        diameter="f.diameter",
        width="f.wheel_width",
        bolt_1="f.bolt_pattern_1",
        bolt_2="f.bolt_pattern_2",
        offset='f."offset"',
        center_bore="f.hub_bore",
        load_rating="f.load_rating",
        backspace="f.backspace",
        weight="f.product_weight",
        finish="f.finish",
        model="f.name",
        style_number="f.style_number",
        lug_seat="f.lugseat_type",
        lug_thread="f.screw",
        structural_warranty="f.structure_warranty",
        finish_warranty="f.finish_warranty",
        tpms="f.tpms_compatible",
        dually="f.dually_wheel",
        image_url="f.image_1",
    ),
    # Vossen ships centre caps and hardware in the same table as wheels, with diameter 0 -- 119 of
    # 3,263 rows. The size gate in build_spec rejects them; feed_type alone would not have.
    FEED_VOSSEN: _feed(
        FEED_VOSSEN,
        37,
        "vossen_parts",
        "sku",
        freshest="f.updated_at DESC NULLS LAST, f.id DESC",
        part_number="f.sku",
        title="f.description",
        diameter="f.diameter",
        width="f.width",
        bolt_1="f.bolt_pattern",
        offset='f."offset"',
        center_bore="f.center_bore",
    ),
    FEED_ELITEWHEELS: _feed(
        FEED_ELITEWHEELS,
        17,
        "elitewheels_part_wheels",
        "part_number",
        freshest="f.updated_at DESC NULLS LAST, f.id DESC",
        part_number="f.part_number",
        title="f.group_label",
        size="f.size",
        bolt_1="f.bolt_pattern_1",
        bolt_2="f.bolt_pattern_2",
        offset='f."offset"',
        center_bore="f.center_bore",
        finish="f.finish",
        model="f.group_label",
    ),
}


def iter_feed(
    feed: Feed, *, brand_ids: typing.Optional[typing.Sequence[int]] = None
) -> typing.Iterator[typing.List[dict]]:
    """Pages of one feed's wheel rows, joined to the master part they belong to."""
    brand_filter = "AND mp.brand_id = ANY(%s)" if brand_ids else ""
    sql = feed.sql.format(brand_filter=brand_filter)

    last_id = 0
    while True:
        params: typing.List[typing.Any] = [last_id]
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
_TITLE_CASE_WARRANTY = True


def _warranty(value: typing.Optional[str]) -> typing.Optional[str]:
    """The Wheel Group ships the same warranty in two casings -- "LIMITED 1 YEAR WARRANTY" on 1,545
    rows and "Limited 1 Year Warranty" on 322. Title case makes them one value."""
    text = _clean(value, limit=64)
    return text.title() if text else None


def _boolean(value: typing.Any) -> typing.Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().upper()
    if text in ("YES", "TRUE", "1", "Y"):
        return True
    if text in ("NO", "FALSE", "0", "N"):
        return False
    return None


def build_spec(row: dict, *, feed: str, stats: typing.Optional[EnrichStats] = None):
    """
    One ``WheelSpec`` from one feed row, or ``None`` when the row does not describe a wheel.

    The feed wins on every dimension it publishes; the title is read anyway, because it supplies
    the bolt pattern where the attribute column is empty and is a free cross-check where both
    speak.

    **The size gate is the identification.** A feed saying "wheel" is not enough on its own: Vossen
    ships centre caps and hardware in the same table as its wheels, with diameter 0, and 119 of its
    3,263 rows are those. Requiring a plausible diameter and width rejects them.
    """
    text = " ".join(filter(None, (row.get("title"), row.get("model_raw"), row.get("master_description"))))

    size = None
    if row.get("diameter_raw") is not None and row.get("width_raw") is not None:
        size = wheel_size.parse_size("{}x{}".format(row["diameter_raw"], row["width_raw"]))
    if size is None:
        size = wheel_size.parse_size(row.get("size_raw"))
    from_title = wheel_size.parse(row.get("title") or "")
    if size is None and from_title is not None:
        size = (from_title.diameter_in, from_title.width_in)
    if size is None:
        if stats:
            stats.no_size += 1
            stats.skip("{}: no plausible wheel size".format(feed))
        return None
    diameter, width = size

    disputed = bool(from_title and (from_title.diameter_in, from_title.width_in) != (diameter, width))

    raw_pattern = row.get("bolt_pattern_1")
    blank = wheel_size.is_blank(raw_pattern)
    patterns = [] if blank else wheel_size.parse_bolt_patterns(raw_pattern, row.get("bolt_pattern_2"))
    if not patterns and not blank and from_title is not None:
        patterns = [p for p in (from_title.bolt_pattern, from_title.bolt_pattern_2) if p]
    if patterns and from_title is not None and from_title.bolt_pattern is not None:
        theirs = (from_title.bolt_pattern.lug_count, from_title.bolt_pattern.circle_mm)
        if theirs != (patterns[0].lug_count, patterns[0].circle_mm):
            disputed = True

    offset = wheel_size.parse_offset_mm(row.get("offset_raw"))
    if offset is None and from_title is not None:
        offset = from_title.offset_mm

    backspacing = None
    if row.get("backspace_raw") is not None:
        backspacing = wheel_size.parse_backspacing_in(str(row["backspace_raw"])) or _positive_decimal(
            row["backspace_raw"]
        )

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
        backspacing_in=backspacing,
        center_bore_mm=wheel_size.parse_center_bore_mm(row.get("center_bore_raw")),
        load_rating_lb=_positive_int(row.get("load_rating_raw")),
        weight_lb=_positive_decimal(row.get("weight_raw")),
        model_name=_clean(row.get("model_raw")),
        style_number=_clean(row.get("style_number_raw"), limit=64),
        finish=_clean(row.get("finish_raw"), limit=128),
        finish_family=finish_family(row.get("finish_raw")),
        construction=src_models.WheelSpec.CONSTRUCTION_FORGED if _FORGED_RE.search(text) else None,
        vehicle_class=_vehicle_class(text, patterns),
        is_beadlock=True if _KEYWORDS["is_beadlock"][0].search(text) else None,
        is_dually=_boolean(row.get("dually_raw")) or (True if _KEYWORDS["is_dually"][0].search(text) else None),
        tpms_compatible=_boolean(row.get("tpms_raw")),
        lug_seat=_clean(row.get("lug_seat_raw"), limit=24),
        lug_thread_size=_clean(row.get("lug_thread_raw"), limit=24),
        structural_warranty=_warranty(row.get("structural_warranty_raw")),
        finish_warranty=_warranty(row.get("finish_warranty_raw")),
        spec_source=src_models.WheelSpec.SPEC_SOURCE_FEED,
        source_feed=feed,
        source_external_id=_clean(row.get("part_number"), limit=128),
        size_disputed=disputed,
        search_aliases=[],
        style_tags=[],
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


# What distributors write in place of a null. The Wheel Group ships a literal "NONE" in its screw
# column on 820 rows, which reached the detail card as the text "Lug thread size: NONE" -- a row
# that should not have rendered at all.
_PLACEHOLDER_VALUES = frozenset(["", "NONE", "N/A", "NA", "NULL", "-", "--", "TBD", "UNKNOWN"])


def _clean(value: typing.Optional[str], *, limit: int = 255) -> typing.Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text.upper() in _PLACEHOLDER_VALUES:
        return None
    return text[:limit] or None


def _positive_int(value: typing.Any) -> typing.Optional[int]:
    try:
        number = int(decimal.Decimal(str(value)))
    except (TypeError, ValueError, decimal.InvalidOperation):
        return None
    return number if number > 0 else None


def _positive_decimal(value: typing.Any) -> typing.Optional[decimal.Decimal]:
    try:
        number = decimal.Decimal(str(value))
    except (TypeError, decimal.InvalidOperation):
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
    "backspacing_in",
    "center_bore_mm",
    "load_rating_lb",
    "weight_lb",
    "model_name",
    "style_number",
    "finish",
    "finish_family",
    "construction",
    "vehicle_class",
    "is_beadlock",
    "is_dually",
    "tpms_compatible",
    "lug_seat",
    "lug_thread_size",
    "structural_warranty",
    "finish_warranty",
    "spec_source",
    "source_feed",
    "source_external_id",
    "size_disputed",
    "enriched_at",
)


def run(
    *,
    feeds: typing.Optional[typing.Sequence[str]] = None,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    limit: typing.Optional[int] = None,
    apply_changes: bool = False,
) -> EnrichStats:
    """
    Build specs from every requested feed, resolving to **one spec per master part**.

    Two feeds describing the same wheel is the failure this guards against. It does not happen
    today -- across all four, no master part is reached by more than one feed and no
    (brand, part number) pair appears in two -- but catalogs grow, and without an explicit rule the
    winner would be whichever feed happened to run last. ``FEED_ORDER`` decides, and every
    collision is counted and logged rather than silently resolved.

    Specs are collected across all feeds before anything is written, because the precedence
    decision cannot be made one feed at a time.
    """
    stats = EnrichStats()
    chosen: typing.Dict[int, typing.Any] = {}
    chosen_feed: typing.Dict[int, str] = {}
    rank = {name: index for index, name in enumerate(FEED_ORDER)}

    for name in feeds or FEED_ORDER:
        if name not in FEEDS:
            raise ValueError("Unknown feed {!r}. Known: {}".format(name, ", ".join(sorted(FEEDS))))
        feed = FEEDS[name]
        seen_here = 0
        for page in iter_feed(feed, brand_ids=brand_ids):
            for row in page:
                if limit is not None and seen_here >= limit:
                    break
                stats.scanned += 1
                seen_here += 1
                spec = build_spec(row, feed=name, stats=stats)
                if spec is None:
                    continue

                master_part_id = spec.master_part_id
                incumbent = chosen_feed.get(master_part_id)
                if incumbent is None:
                    chosen[master_part_id] = spec
                    chosen_feed[master_part_id] = name
                else:
                    stats.collisions += 1
                    winner = incumbent if rank[incumbent] <= rank[name] else name
                    stats.bump_collision("{} vs {} -> {}".format(incumbent, name, winner))
                    logger.info(
                        "%s master part %s described by %s and %s; %s wins on precedence",
                        _LOG_PREFIX,
                        master_part_id,
                        incumbent,
                        name,
                        winner,
                    )
                    if winner == name:
                        chosen[master_part_id] = spec
                        chosen_feed[master_part_id] = name
                    continue

                if len(stats.samples) < 12:
                    stats.samples.append(
                        "{:<14} {:<9} {:<10} off={:<5} {:<20} {}".format(
                            name,
                            spec.size_display,
                            spec.bolt_pattern_display or ("blank" if spec.is_blank_drilled else "-"),
                            spec.offset_mm if spec.offset_mm is not None else "-",
                            (spec.model_name or "")[:20],
                            (spec.finish or "")[:22],
                        )
                    )
            if limit is not None and seen_here >= limit:
                break
        stats.per_feed[name] = sum(1 for f in chosen_feed.values() if f == name)

    pending = list(chosen.values())
    if apply_changes:
        for index in range(0, len(pending), WRITE_BATCH):
            stats.written += _write(pending[index : index + WRITE_BATCH], stats)
    else:
        stats.written = 0
    stats.built = len(pending)
    return stats


@transaction.atomic
def _write(specs: typing.Sequence[typing.Any], stats: EnrichStats) -> int:
    """
    Upsert the specs and stamp the master part.

    ``update_conflicts`` on ``master_part_id`` rather than delete-and-insert, so re-running is
    idempotent and never leaves a window where a wheel has no spec row. The unique constraint is
    also the last line of defence against two feeds writing the same part: the run resolves that
    by precedence first, and this would raise rather than silently keep one at random.
    """
    src_models.WheelSpec.objects.bulk_create(
        list(specs),
        update_conflicts=True,
        update_fields=list(WRITE_FIELDS),
        unique_fields=["master_part_id"],
        batch_size=WRITE_BATCH,
    )
    stamped = (
        src_models.MasterPart.objects.filter(id__in=[s.master_part_id for s in specs])
        .exclude(product_type=src_enums.ProductType.WHEEL.value)
        .update(
            product_type=src_enums.ProductType.WHEEL.value,
            product_type_source="wheel_specs:feed",
            updated_at=timezone.now(),
        )
    )
    stats.product_type_set += stamped
    return len(specs)
