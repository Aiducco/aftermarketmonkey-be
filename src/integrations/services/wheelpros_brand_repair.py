"""
Move Premier's "Wheel Pros" master parts onto the manufacturer that actually made them.

The second half of the repair. :func:`src.integrations.services.premier.resolve_wheelpros_bucket_brands`
sets ``PremierParts.brand_override`` so that *future* ingests build the part under the right brand;
this fixes the parts already built under the wrong one.

**Order matters and is not interchangeable.** The overrides must be written first. Repair the
master parts while the bucket rows are still override-less and the next Premier sync recreates
every one of them overnight -- verified against the live data: of the 5,800 parts this touches,
zero still have an override-less bucket row, so the next ingest resolves all of them to the real
brand and nothing comes back.

Two shapes of work, because ``master_parts`` is unique on ``(brand_id, part_number)``:

**Rename** (709 parts). No row exists at the target brand, so the brand id is simply updated. The
master part keeps its id and everything hanging off it is untouched.

**Merge** (5,091 parts). The correctly-branded part already exists, because Premier lists the same
product twice -- once under its own ``WPR`` SKU and once under a marque SKU -- and only the second
resolved. These are not mislabelled parts, they are duplicates, and a plain ``UPDATE`` would
violate the unique constraint. The Premier-side row is folded into the existing one and deleted.

Only three tables carry anything on the losing side; ``wheel_specs``, fitments, product groups and
the ML classification are all empty for these parts, confirmed by count before this was written.

    provider_parts     repointed, or dropped where the survivor already has that provider
    master_part_data   repointed where the survivor has none, else dropped
    tire_specs         the better-sourced row wins -- see _better_spec

Read-only unless the caller passes ``apply_changes``.
"""
import dataclasses
import logging
import typing

from django.db import connection, transaction

from src import models as src_models

logger = logging.getLogger(__name__)

_LOG_PREFIX = "[WHEELPROS-REPAIR]"

WHEELPROS_BRAND_NAME = "WHEEL PROS"
PREMIER_PROVIDER_ID = 29
BATCH = 250

# Which tire_specs row survives when both sides have one. Same precedence the catalog merge already
# uses, so tire_specs is governed by one rule everywhere: a row a manufacturer catalog vouched for
# beats one derived from a sidewall string, and SimpleTire beats TDG on coverage and precision.
SPEC_SOURCE_RANK = {"simpletire": 3, "tdg": 2, "parser": 1}


@dataclasses.dataclass
class RepairStats:
    resolvable: int = 0
    renamed: int = 0
    merged: int = 0
    provider_parts_moved: int = 0
    provider_parts_dropped: int = 0
    data_moved: int = 0
    data_dropped: int = 0
    specs_moved: int = 0
    specs_kept_survivor: int = 0
    specs_replaced_survivor: int = 0
    losers_deleted: int = 0
    skipped: typing.Dict[str, int] = dataclasses.field(default_factory=dict)
    samples: typing.List[str] = dataclasses.field(default_factory=list)

    def skip(self, reason: str) -> None:
        self.skipped[reason] = self.skipped.get(reason, 0) + 1


_PLAN_SQL = """
    SELECT DISTINCT ON (mp.id)
           mp.id            AS loser_id,
           mp.part_number   AS part_number,
           prp.brand_override_id AS target_brand_id,
           b.name           AS target_brand_name,
           survivor.id      AS survivor_id
    FROM master_parts mp
    JOIN provider_parts pp ON pp.master_part_id = mp.id AND pp.provider_id = %s
    JOIN premier_parts prp
      ON prp.premier_part_number = split_part(pp.provider_external_id, '_', 2)
    JOIN brands b ON b.id = prp.brand_override_id
    LEFT JOIN master_parts survivor
      ON survivor.brand_id = prp.brand_override_id
     AND survivor.part_number = mp.part_number
     AND survivor.id <> mp.id
    WHERE mp.brand_id = %s
      AND prp.brand_override_id IS NOT NULL
    ORDER BY mp.id, prp.brand_override_id
"""


def build_plan() -> typing.List[dict]:
    """Every WHEEL PROS master part an override can now place, with its survivor if one exists."""
    brand = src_models.Brands.objects.filter(name=WHEELPROS_BRAND_NAME).first()
    if brand is None:
        logger.info("%s No %r brand; nothing to repair.", _LOG_PREFIX, WHEELPROS_BRAND_NAME)
        return []
    with connection.cursor() as cursor:
        cursor.execute(_PLAN_SQL, [PREMIER_PROVIDER_ID, brand.id])
        names = [c[0] for c in cursor.description]
        return [dict(zip(names, row)) for row in cursor.fetchall()]


def _populated_field_count(spec) -> int:
    return sum(
        1
        for field in spec._meta.concrete_fields
        if field.attname not in ("id", "master_part_id", "created_at", "updated_at", "enriched_at")
        and getattr(spec, field.attname) not in (None, "", [])
    )


def _better_spec(a, b):
    """
    The tire spec to keep. Source precedence first, then how much of it is filled in.

    Two independently enriched rows for one physical tire is what a duplicate master part produces.
    Keeping the survivor's unconditionally would sometimes discard a catalog-matched row in favour
    of a parser-derived one, which is the wrong direction: the whole point of the catalog merge was
    that a manufacturer's figures beat a sidewall reading.
    """
    rank_a = SPEC_SOURCE_RANK.get(a.spec_source or "", 0)
    rank_b = SPEC_SOURCE_RANK.get(b.spec_source or "", 0)
    if rank_a != rank_b:
        return a if rank_a > rank_b else b
    return a if _populated_field_count(a) >= _populated_field_count(b) else b


@transaction.atomic
def _repair_batch(rows: typing.Sequence[dict], stats: RepairStats) -> None:
    """
    Repair a batch of master parts with a fixed number of queries, not a fixed number per part.

    Row-at-a-time cost about eight round trips each, which against a remote database measured at
    21 parts a minute -- four and a half hours for the full run. Everything here is therefore read
    in bulk, decided in Python, and written in bulk: ~12 statements per batch regardless of size.

    Ordering inside the transaction is load-bearing. ``tire_specs`` and ``master_part_data`` are
    OneToOne on ``master_part``, so a losing row must be deleted before the winning row is moved
    onto that master part, or the move violates the constraint.
    """
    renames = [r for r in rows if r["survivor_id"] is None]
    merges = [r for r in rows if r["survivor_id"] is not None]

    # ---- renames: one statement per target brand ---------------------------------------------
    by_target: typing.Dict[int, typing.List[int]] = {}
    for row in renames:
        by_target.setdefault(row["target_brand_id"], []).append(row["loser_id"])
    for brand_id, ids in by_target.items():
        src_models.MasterPart.objects.filter(id__in=ids).update(brand_id=brand_id)
        stats.renamed += len(ids)

    if not merges:
        return

    loser_ids = [r["loser_id"] for r in merges]
    survivor_of = {r["loser_id"]: r["survivor_id"] for r in merges}
    survivor_ids = list({r["survivor_id"] for r in merges})

    # ---- tire_specs --------------------------------------------------------------------------
    loser_specs = {s.master_part_id: s for s in src_models.TireSpec.objects.filter(master_part_id__in=loser_ids)}
    survivor_specs = {s.master_part_id: s for s in src_models.TireSpec.objects.filter(master_part_id__in=survivor_ids)}
    delete_spec_ids: typing.List[int] = []
    move_specs: typing.List[typing.Any] = []
    for loser_id, spec in loser_specs.items():
        survivor_id = survivor_of[loser_id]
        rival = survivor_specs.get(survivor_id)
        if rival is None:
            spec.master_part_id = survivor_id
            move_specs.append(spec)
            stats.specs_moved += 1
        elif _better_spec(rival, spec) is spec:
            delete_spec_ids.append(rival.pk)
            spec.master_part_id = survivor_id
            move_specs.append(spec)
            stats.specs_replaced_survivor += 1
        else:
            delete_spec_ids.append(spec.pk)
            stats.specs_kept_survivor += 1
    if delete_spec_ids:
        src_models.TireSpec.objects.filter(pk__in=delete_spec_ids).delete()
    if move_specs:
        src_models.TireSpec.objects.bulk_update(move_specs, ["master_part"], batch_size=500)

    # ---- master_part_data --------------------------------------------------------------------
    loser_data = {d.master_part_id: d for d in src_models.MasterPartData.objects.filter(master_part_id__in=loser_ids)}
    survivors_with_data = set(
        src_models.MasterPartData.objects.filter(master_part_id__in=survivor_ids).values_list(
            "master_part_id", flat=True
        )
    )
    drop_data, move_data = [], []
    for loser_id, data in loser_data.items():
        if survivor_of[loser_id] in survivors_with_data:
            drop_data.append(data.pk)
        else:
            data.master_part_id = survivor_of[loser_id]
            move_data.append(data)
            survivors_with_data.add(data.master_part_id)
    if drop_data:
        src_models.MasterPartData.objects.filter(pk__in=drop_data).delete()
        stats.data_dropped += len(drop_data)
    if move_data:
        src_models.MasterPartData.objects.bulk_update(move_data, ["master_part"], batch_size=500)
        stats.data_moved += len(move_data)

    # ---- provider_parts: unique per (master_part, provider) ----------------------------------
    taken = set(
        src_models.ProviderPart.objects.filter(master_part_id__in=survivor_ids).values_list(
            "master_part_id", "provider_id"
        )
    )
    drop_pp, move_pp = [], []
    for part in src_models.ProviderPart.objects.filter(master_part_id__in=loser_ids):
        survivor_id = survivor_of[part.master_part_id]
        key = (survivor_id, part.provider_id)
        if key in taken:
            drop_pp.append(part.pk)
        else:
            part.master_part_id = survivor_id
            move_pp.append(part)
            taken.add(key)
    if drop_pp:
        src_models.ProviderPart.objects.filter(pk__in=drop_pp).delete()
        stats.provider_parts_dropped += len(drop_pp)
    if move_pp:
        src_models.ProviderPart.objects.bulk_update(move_pp, ["master_part"], batch_size=500)
        stats.provider_parts_moved += len(move_pp)

    deleted, _ = src_models.MasterPart.objects.filter(id__in=loser_ids).delete()
    stats.losers_deleted += len(loser_ids)
    stats.merged += len(merges)


def run(*, apply_changes: bool = False, limit: typing.Optional[int] = None) -> RepairStats:
    stats = RepairStats()
    plan = build_plan()
    stats.resolvable = len(plan)
    logger.info("%s %d master parts to repair", _LOG_PREFIX, len(plan))

    for index, row in enumerate(plan if limit is None else plan[:limit]):
        if len(stats.samples) < 12:
            stats.samples.append(
                "{:<12} {:<22} -> {:<26} {}".format(
                    row["loser_id"],
                    (row["part_number"] or "")[:22],
                    (row["target_brand_name"] or "")[:26],
                    "merge into {}".format(row["survivor_id"]) if row["survivor_id"] else "rename",
                )
            )
        if not apply_changes:
            if row["survivor_id"]:
                stats.merged += 1
            else:
                stats.renamed += 1
    if not apply_changes:
        return stats

    work = plan if limit is None else plan[:limit]
    for start in range(0, len(work), BATCH):
        batch = work[start : start + BATCH]
        try:
            _repair_batch(batch, stats)
        except Exception as exc:  # one bad batch must not abandon the rest
            logger.exception("%s batch at %s failed: %s", _LOG_PREFIX, start, exc)
            stats.skip("batch error: {}".format(type(exc).__name__))
        logger.info("%s repaired %s/%s", _LOG_PREFIX, min(start + BATCH, len(work)), len(work))
    return stats
