"""
Fill ``max_load_lb``, ``max_speed_mph`` and ``ply_rating`` from the values already on the row.

These three are not facts anyone publishes about a tire; they are lookups. A load index of 121 is
3,197 lb, a speed rating of T is 118 mph, load range E on an LT tire is 10 ply -- straight reads of
``tire_load_index``, ``tire_speed_rating`` and ``load_range_ply``.

Until now they were only ever resolved from what ``src.domain.tire_size`` could read out of a
distributor title, which leaves two problems that the catalog merges made visible:

**Gaps.** A catalog can supply a speed rating the title never carried. 17,364 rows hold a
``speed_rating`` with no ``max_speed_mph`` beside it, 26,218 a ``load_range`` with no
``ply_rating``, 3,243 a ``load_index`` with no ``max_load_lb`` -- all derivable, none derived,
because the parser had nothing to read.

**Contradictions.** Worse than a gap. When the rating on the row comes from a catalog and the
derived figure comes from the title, they can disagree: one row carried ``speed_rating`` T from
SimpleTire and wanted ``max_speed_mph`` 130, which is H. Same row, two different speed ratings.

So the rule here is simply: derive each from the input **on this row**, whatever wrote it.

Fill-only by default. Where a catalog published ``max_load_lb`` directly it is better than our
lookup -- it is the manufacturer's own figure for that exact SKU, including dual-load cases the
index alone does not capture -- so an existing value is left alone unless ``overwrite`` is asked
for explicitly.

Read-only unless the caller passes ``apply_changes``.
"""
import dataclasses
import logging
import typing

from django.db import connection, transaction

from src import models as src_models
from src.integrations.services import tire_enrichment

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[TIRE-DERIVED]"

BATCH_SIZE = 2000

# Derived field -> the column it is a function of. Both halves are needed: there is no point
# resolving a field whose input is missing, and the pairing is what keeps the two consistent.
DERIVED_FROM = {
    "max_load_lb": "load_index",
    "max_speed_mph": "speed_rating",
    "ply_rating": "load_range",
}
UPDATE_FIELDS = tuple(DERIVED_FROM)


class _Inputs(typing.NamedTuple):
    """The shape ``LookupTables.resolve`` expects, filled from the row rather than from a parse."""

    load_index: typing.Optional[int]
    speed_rating: typing.Optional[str]
    load_range: typing.Optional[str]


@dataclasses.dataclass
class DerivedStats:
    scanned: int = 0
    changed: int = 0
    written: int = 0
    filled: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    overwritten: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    unresolvable: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    samples: typing.List[str] = dataclasses.field(default_factory=list)

    def bump(self, bucket: str, key: str) -> None:
        target = getattr(self, bucket)
        target[key] = target.get(key, 0) + 1


def run(
    *,
    brand_ids: typing.Optional[typing.Sequence[int]] = None,
    apply_changes: bool = False,
    overwrite: bool = False,
) -> DerivedStats:
    stats = DerivedStats()
    lookups = tire_enrichment.LookupTables()

    qs = src_models.TireSpec.objects.only("id", "load_index", "speed_rating", "load_range", *UPDATE_FIELDS).order_by(
        "id"
    )
    if brand_ids:
        qs = qs.filter(master_part__brand_id__in=list(brand_ids))

    pending: typing.List[typing.Any] = []
    for spec in qs.iterator(chunk_size=BATCH_SIZE):
        stats.scanned += 1
        resolved = lookups.resolve(
            _Inputs(load_index=spec.load_index, speed_rating=spec.speed_rating, load_range=spec.load_range)
        )
        touched = False
        for field, source_field in DERIVED_FROM.items():
            value = resolved[field]
            current = getattr(spec, field)
            if value is None:
                # Two very different reasons a lookup comes back empty, and only one is a gap.
                # SL, XL and LL are passenger designations that *have* no ply rating -- the row
                # exists in load_range_ply with a NULL, and reporting those 25,697 rows as
                # unresolved would bury the handful that are genuinely missing. So only a code the
                # lookup table has never heard of is counted.
                source_value = getattr(spec, source_field)
                if source_value is not None and current is None and not lookups.knows(field, source_value):
                    stats.bump("unresolvable", "{}={}".format(source_field, source_value))
                continue
            if current == value:
                continue
            if current is not None and not overwrite:
                continue
            stats.bump("overwritten" if current is not None else "filled", field)
            if len(stats.samples) < 12:
                stats.samples.append(
                    "{}={} -> {} {} (was {})".format(source_field, getattr(spec, source_field), field, value, current)
                )
            setattr(spec, field, value)
            touched = True

        if touched:
            stats.changed += 1
            pending.append(spec)
        if apply_changes and len(pending) >= BATCH_SIZE:
            stats.written += _write(pending)
            pending = []

    if apply_changes and pending:
        stats.written += _write(pending)
    return stats


@transaction.atomic
def _write(specs: typing.Sequence[typing.Any]) -> int:
    """Only the three derived columns, named explicitly."""
    src_models.TireSpec.objects.bulk_update(list(specs), list(UPDATE_FIELDS), batch_size=500)
    return len(specs)


# ---------------------------------------------------------------------------------------------
# Season, propagated across the sizes of one model
# ---------------------------------------------------------------------------------------------
# A tire model has one season. Every size of a Blizzak is a winter tire; every size of a Defender
# LTX is all-season. So where one SKU of a model carries a season and its siblings do not, the
# season is known -- it just never reached those rows, because only the SKUs a catalog happened to
# match got one.
#
# Unanimity is required, not a majority. A model whose SKUs disagree is either a naming collision
# or a genuine dual-line, and in both cases guessing is worse than leaving the column empty; the
# disagreements are counted instead so they can be looked at.
_PROPAGATE_SQL = """
WITH model_season AS (
    SELECT mp.brand_id,
           ts.model_name,
           MIN(ts.season_category) AS season,
           COUNT(DISTINCT ts.season_category) AS distinct_seasons
    FROM tire_specs ts
    JOIN master_parts mp ON mp.id = ts.master_part_id
    WHERE ts.season_category IS NOT NULL AND ts.model_name IS NOT NULL
    GROUP BY mp.brand_id, ts.model_name
)
UPDATE tire_specs ts
SET season_category = ms.season
FROM master_parts mp, model_season ms
WHERE mp.id = ts.master_part_id
  AND ms.brand_id = mp.brand_id
  AND ms.model_name = ts.model_name
  AND ms.distinct_seasons = 1
  AND ts.season_category IS NULL
  {brand_clause}
"""

_COUNT_SQL = _PROPAGATE_SQL.replace(
    "UPDATE tire_specs ts\nSET season_category = ms.season\nFROM master_parts mp, model_season ms",
    "SELECT COUNT(*) FROM tire_specs ts, master_parts mp, model_season ms",
)

_AMBIGUOUS_SQL = """
SELECT COUNT(*) FROM (
    SELECT mp.brand_id, ts.model_name
    FROM tire_specs ts
    JOIN master_parts mp ON mp.id = ts.master_part_id
    WHERE ts.season_category IS NOT NULL AND ts.model_name IS NOT NULL
    GROUP BY mp.brand_id, ts.model_name
    HAVING COUNT(DISTINCT ts.season_category) > 1
) x
"""


def propagate_season_by_model(
    *, brand_ids: typing.Optional[typing.Sequence[int]] = None, apply_changes: bool = False
) -> typing.Tuple[int, int]:
    """Fill an empty ``season_category`` from the other sizes of the same model.

    Returns ``(rows_filled, models_left_alone_for_disagreeing)``."""
    params: typing.List[typing.Any] = []
    brand_clause = ""
    if brand_ids:
        brand_clause = "AND mp.brand_id = ANY(%s)"
        params.append(list(brand_ids))

    with connection.cursor() as cursor:
        cursor.execute(_AMBIGUOUS_SQL)
        ambiguous = cursor.fetchone()[0]
        if apply_changes:
            cursor.execute(_PROPAGATE_SQL.format(brand_clause=brand_clause), params)
            filled = cursor.rowcount
        else:
            cursor.execute(_COUNT_SQL.format(brand_clause=brand_clause), params)
            filled = cursor.fetchone()[0]

    logger.info("%s season propagated to %d rows (%d models disagree)", _LOG_PREFIX, filled, ambiguous)
    return filled, ambiguous
