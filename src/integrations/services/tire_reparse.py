"""
Re-derive the parser-owned half of ``tire_specs`` in place, without touching the LLM half.

Every field on a tire spec comes from one of two places, and they have wildly different costs to
regenerate:

  the parser   12 size fields, plus 3 resolved from the lookup tables.  free, ~20 seconds
  the LLM      18 identification fields.                                ~$190, ~4 hours

A parser fix therefore must not require re-running the LLM. Four real bugs -- a load range read
out of "GRAND SPORT A/S", a dual load taken as the primary on space-separated sizes, unparseable
decimal-less commercial rims, and fields discarded from non-winning titles -- affected roughly
1,290 rows, and re-enriching to correct them would have paid for identification work the audit
says is sound. This module recomputes the size block and leaves the rest alone.

Also re-resolves ``max_load_lb``, ``max_speed_mph`` and ``ply_rating``, which are denormalised
from the lookup tables and were derived from the *wrong* load index and load range on the
affected rows -- a corrected load index with a stale max_load_lb would be worse than either.

``size_disputed`` is recomputed too, since it depends on the parse.

Read-only unless the caller passes ``apply_changes``.
"""
import dataclasses
import decimal
import logging
import typing

from django.db import connection, transaction

from src import models as src_models
from src.integrations.services import tire_enrichment

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRE-REPARSE]"

BATCH_SIZE = 2000

# Everything the parser owns, plus what the lookup tables resolve from it. Deliberately explicit:
# an LLM field appearing in this list would be silently destroyed on every run.
PARSER_FIELDS = (
    "notation",
    "service_type",
    "section_width_mm",
    "aspect_ratio",
    "section_width_in",
    "overall_diameter_in",
    "construction",
    "rim_diameter_in",
    "load_index",
    "load_index_dual",
    "speed_rating",
    "load_range",
    "size_display",
)
RESOLVED_FIELDS = ("max_load_lb", "max_speed_mph", "ply_rating")
UPDATE_FIELDS = PARSER_FIELDS + RESOLVED_FIELDS + ("size_disputed", "updated_at")

# Fields this module must never write. Asserted at import so a later edit to PARSER_FIELDS cannot
# quietly start clobbering identification work.
_LLM_OWNED = frozenset(
    [
        "model_name",
        "sub_model",
        "tread_category",
        "vehicle_class",
        "search_aliases",
        "use_case_tags",
        "tier",
        "noise_level",
        "is_3pmsf",
        "is_ms",
        "is_run_flat",
        "is_studdable",
        "has_reinforced_sidewall",
        "llm_confidence",
        "llm_reason",
        "llm_model_used",
        "category_reconciled",
    ]
)
assert not (set(UPDATE_FIELDS) & _LLM_OWNED), "reparse would overwrite an LLM-owned field"

# Fields a matched SimpleTire row owns instead of the parser. On those rows the value was
# measured and published by the manufacturer's own catalog, so re-deriving it from the sidewall
# string is a downgrade -- and a silent one, since reparse runs on every parser fix.
#
# Note what is *not* here. ``overall_diameter_in`` stays the parser's: ours is the nominal
# diameter the size prints (35.0 for a 35X12.50R20), theirs is the measured one (33.07), and
# search for "35 inch" has to keep finding it. ``max_speed_mph`` stays ours too -- both sides
# derive it from the speed rating, but we floor 160 km/h to 99 and they ceil to 100, and
# ``speed_sort`` range filters would straddle the boundary if the column mixed conventions.
CATALOG_OWNED = (
    "load_range",
    "load_index",
    "load_index_dual",
    "speed_rating",
    "max_load_lb",
    "ply_rating",
)
assert set(CATALOG_OWNED) <= set(UPDATE_FIELDS), "CATALOG_OWNED names a field reparse never writes"

# What reparse may still recompute on a catalog-backed row: the size block, which is ours either
# way because a match is only accepted when both sides agree on the dimensions.
PARSER_FIELDS_CATALOG = tuple(f for f in PARSER_FIELDS if f not in CATALOG_OWNED)
RESOLVED_FIELDS_CATALOG = tuple(f for f in RESOLVED_FIELDS if f not in CATALOG_OWNED)


@dataclasses.dataclass
class ReparseStats:
    scanned: int = 0
    unchanged: int = 0
    changed: int = 0
    now_unparseable: int = 0
    field_changes: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    changed_master_part_ids: typing.List[int] = dataclasses.field(default_factory=list)
    unparseable_master_part_ids: typing.List[int] = dataclasses.field(default_factory=list)
    samples: typing.List[str] = dataclasses.field(default_factory=list)

    def bump(self, field: str) -> None:
        self.field_changes[field] = self.field_changes.get(field, 0) + 1


def _iter_specs(brand_ids: typing.Optional[typing.Sequence[int]]) -> typing.Iterator[typing.List[dict]]:
    """Pages of persisted specs joined to everything ``build_candidate`` needs."""
    where = ""
    params: typing.List[typing.Any] = []
    if brand_ids:
        where = "AND mp.brand_id = ANY(%s)"
        params.append(list(brand_ids))

    columns = ", ".join("ts.{}".format(f) for f in PARSER_FIELDS + RESOLVED_FIELDS)
    sql = """
        SELECT mp.id, mp.brand_id, b.name AS brand_name, mp.part_number, mp.sku, mp.description,
               mp.overview_category, mp.category, mp.product_type, mp.product_type_source,
               ts.size_disputed, ts.spec_source, {columns}
        FROM master_parts mp
        JOIN brands b ON b.id = mp.brand_id
        JOIN tire_specs ts ON ts.master_part_id = mp.id
        WHERE mp.id > %s {where}
        ORDER BY mp.id
        LIMIT %s
    """.format(
        columns=columns, where=where
    )

    last_id = 0
    while True:
        with connection.cursor() as cursor:
            cursor.execute(sql, [last_id] + params + [BATCH_SIZE])
            names = [c[0] for c in cursor.description]
            page = [dict(zip(names, row)) for row in cursor.fetchall()]
        if not page:
            return
        last_id = page[-1]["id"]
        yield page


def _differs(old: typing.Any, new: typing.Any) -> bool:
    """
    Compare the way the database would. ``Decimal("18")`` and ``Decimal("18.0")`` are the same
    rim -- comparing them as strings would report a change on every row and make the whole diff
    meaningless.
    """
    if old is None and new is None:
        return False
    if old is None or new is None:
        return True
    if isinstance(old, (decimal.Decimal, float, int)) or isinstance(new, (decimal.Decimal, float, int)):
        try:
            return decimal.Decimal(str(old)) != decimal.Decimal(str(new))
        except (decimal.InvalidOperation, ValueError):
            pass
    return str(old) != str(new)


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool = False,
    stats: typing.Optional[ReparseStats] = None,
) -> ReparseStats:
    """
    Recompute every persisted spec's size block from today's parser.

    A row whose titles no longer parse is **reported, never deleted**. That means a parser change
    that loses coverage shows up as a number rather than as inventory quietly disappearing, and
    the decision about what to do with those rows stays a human one.
    """
    stats = stats or ReparseStats()
    lookups = tire_enrichment.LookupTables()

    for page in _iter_specs(brand_ids):
        provider_rows = tire_enrichment._provider_rows_for([row["id"] for row in page])
        pending: typing.List[src_models.TireSpec] = []
        pending_catalog: typing.List[src_models.TireSpec] = []

        for row in page:
            stats.scanned += 1
            candidate = tire_enrichment.build_candidate(master_part=row, provider_rows=provider_rows.get(row["id"], []))
            if candidate is None:
                stats.now_unparseable += 1
                stats.unparseable_master_part_ids.append(row["id"])
                continue

            parsed = candidate.parsed
            resolved = lookups.resolve(parsed)
            disputed = len(candidate.size_variants) > 1

            catalog_backed = row["spec_source"] == src_models.TireSpec.SPEC_SOURCE_SIMPLETIRE
            parser_fields = PARSER_FIELDS_CATALOG if catalog_backed else PARSER_FIELDS
            resolved_fields = RESOLVED_FIELDS_CATALOG if catalog_backed else RESOLVED_FIELDS

            diff = [f for f in parser_fields if _differs(row[f], getattr(parsed, f))]
            diff += [f for f in resolved_fields if _differs(row[f], resolved[f])]
            if row["size_disputed"] != disputed:
                diff.append("size_disputed")
            if not diff:
                stats.unchanged += 1
                continue

            stats.changed += 1
            stats.changed_master_part_ids.append(row["id"])
            for field in diff:
                stats.bump(field)
            if len(stats.samples) < 12:
                stats.samples.append(
                    "{} {} -> {}  [{}]".format(
                        row["size_display"],
                        ", ".join("{}={}".format(f, row[f]) for f in diff[:3] if f in row),
                        ", ".join("{}".format(getattr(parsed, f, resolved.get(f))) for f in diff[:3]),
                        (row["description"] or "")[:44],
                    )
                )

            spec = src_models.TireSpec(master_part_id=row["id"])
            for field in parser_fields:
                setattr(spec, field, getattr(parsed, field))
            for field in resolved_fields:
                setattr(spec, field, resolved[field])
            spec.size_disputed = disputed
            (pending_catalog if catalog_backed else pending).append(spec)

        if apply_changes:
            if pending:
                _write(pending, PARSER_FIELDS + RESOLVED_FIELDS)
            if pending_catalog:
                _write(pending_catalog, PARSER_FIELDS_CATALOG + RESOLVED_FIELDS_CATALOG)

    return stats


@transaction.atomic
def _write(specs: typing.Sequence[src_models.TireSpec], fields: typing.Sequence[str]) -> None:
    """
    Update only the size block, and only the columns ``fields`` names.

    The real rows are loaded and mutated rather than constructed, so every field this module does
    not own arrives from the database untouched and goes back unchanged. ``bulk_update`` is then
    given an explicit column list, which is the second guard: even a mistake in the loop cannot
    write a column that is not named here. Catalog-backed rows are passed a shorter list, which
    is what keeps a parser fix from reverting measured values to derived ones.
    """
    assert not (set(fields) & _LLM_OWNED), "reparse would overwrite an LLM-owned field"
    by_master = {spec.master_part_id: spec for spec in specs}
    rows = list(src_models.TireSpec.objects.filter(master_part_id__in=list(by_master)))
    for row in rows:
        source = by_master[row.master_part_id]
        for field in fields:
            setattr(row, field, getattr(source, field))
        row.size_disputed = source.size_disputed
    src_models.TireSpec.objects.bulk_update(rows, list(tuple(fields) + ("size_disputed",)), batch_size=500)
