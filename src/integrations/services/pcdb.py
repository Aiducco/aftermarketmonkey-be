"""
Computes PcdbTerminologyFlat from the raw Pcdb* mirror tables loaded by load_pcdb. See
src/models.py's PcdbTerminologyFlat docstring for what this table is for.

Everything here is read from the DB into plain dicts once (these are all small-to-medium
reference tables, largest ~84K rows) rather than queried per-terminology -- 40K+ individual
queries would be the wrong shape for a batch job like this.
"""
import collections
import logging

from django.db import transaction

from src import models as src_models

logger = logging.getLogger(__name__)

CHUNK_SIZE = 5_000


def _resolve_supersession_chains(raw_old_to_new):
    """
    raw_old_to_new: {old_id: new_id} for every direct supersession row. Walk each key
    transitively to its terminal (never-superseded) ID.

    Rows where old_id == new_id are excluded before walking -- these are same-ID rename
    records (e.g. PartsSupersessionID 5: 1424 "Trunk Lid Release Solenoid" -> 1424 "Deck Lid
    Release Solenoid"), not real replacements, and would trivially "cycle" on themselves.

    A genuine multi-node cycle among the remaining edges (verified against the real 2026-07-30
    export: 23500 <-> 52408, "Parking Brake Adjuster Cable" / "Parking Brake Cable Adjuster",
    bidirectional supersession) is excluded from resolution rather than failing the whole
    build -- both terminologies are left terminal/active and the cycle is reported in the
    returned stats for review. This is a deliberate deviation from "fail the run if a cycle
    exists" for a 2-row, well-isolated case; flip to raising ValueError here if a hard fail is
    preferred instead.
    """
    old_to_new = {k: v for k, v in raw_old_to_new.items() if k != v}
    terminal_of = {}
    cycles = []

    def _walk(start):
        if start in terminal_of:
            return terminal_of[start]
        path = [start]
        seen = {start}
        current = start
        while current in old_to_new:
            nxt = old_to_new[current]
            if nxt in seen:
                cycle = path[path.index(nxt):] + [nxt]
                cycles.append(cycle)
                for node in cycle[:-1]:
                    terminal_of[node] = None
                return terminal_of.get(start)
            path.append(nxt)
            seen.add(nxt)
            current = nxt
        for node in path:
            if node not in terminal_of:
                terminal_of[node] = current if current != node else None
        return terminal_of[start]

    for old_id in old_to_new:
        _walk(old_id)
    return terminal_of, cycles


@transaction.atomic
def build_terminology_flat():
    parts = list(src_models.PcdbParts.objects.values(
        "part_terminology_id", "part_terminology_name", "part_terminology_description",
    ))

    categories_by_id = dict(src_models.PcdbCategories.objects.values_list("category_id", "category_name"))
    subcategories_by_id = dict(
        src_models.PcdbSubCategories.objects.values_list("subcategory_id", "subcategory_name")
    )

    # One row per terminology today (verified against the real export), but resolve defensively
    # in case a future PCdb release reintroduces multiple historical rows per terminology: take
    # the currently-effective one (end_date_time IS NULL), falling back to the most recent by
    # effective_date_time if every row for a terminology happens to be closed out.
    category_rows = collections.defaultdict(list)
    for row in src_models.PcdbPartCategory.objects.values(
        "part_terminology_id", "category_id", "subcategory_id", "effective_date_time", "end_date_time",
    ):
        category_rows[row["part_terminology_id"]].append(row)

    def _current_category_row(tid):
        rows = category_rows.get(tid)
        if not rows:
            return None
        open_rows = [r for r in rows if r["end_date_time"] is None]
        candidates = open_rows or rows
        return max(candidates, key=lambda r: r["effective_date_time"] or "")

    use_by_id = dict(src_models.PcdbUse.objects.values_list("use_id", "use_description"))
    aces_use_ids = {uid for uid, desc in use_by_id.items() if desc and "ACES" in desc.upper()}
    pies_use_ids = {uid for uid, desc in use_by_id.items() if desc and "PIES" in desc.upper()}

    aces_terminology_ids = set()
    pies_terminology_ids = set()
    for tid, use_id in src_models.PcdbPartsToUse.objects.values_list("part_terminology_id", "use_id"):
        if use_id in aces_use_ids:
            aces_terminology_ids.add(tid)
        if use_id in pies_use_ids:
            pies_terminology_ids.add(tid)

    alias_name_by_id = dict(src_models.PcdbAlias.objects.values_list("alias_id", "alias_name"))
    aliases_by_terminology = collections.defaultdict(list)
    skipped_alias_refs = 0
    for tid, alias_id in src_models.PcdbPartsToAlias.objects.values_list("part_terminology_id", "alias_id"):
        name = alias_name_by_id.get(alias_id)
        if name is None:
            skipped_alias_refs += 1
            logger.warning("PcdbPartsToAlias references missing AliasID=%s (terminology=%s) -- skipping", alias_id, tid)
            continue
        aliases_by_terminology[tid].append(name)

    old_to_new = dict(
        src_models.PcdbPartsSupersession.objects.values_list(
            "old_part_terminology_id", "new_part_terminology_id",
        )
    )
    superseded_by_map, cycles = _resolve_supersession_chains(old_to_new)
    for cycle in cycles:
        logger.warning("Supersession cycle excluded from resolution (left terminal/active): %s", cycle)

    src_models.PcdbTerminologyFlat.objects.all().delete()

    objs = []
    for part in parts:
        tid = part["part_terminology_id"]
        cat_row = _current_category_row(tid)
        category_id = cat_row["category_id"] if cat_row else None
        subcategory_id = cat_row["subcategory_id"] if cat_row else None
        superseded_by = superseded_by_map.get(tid)

        objs.append(src_models.PcdbTerminologyFlat(
            part_terminology_id=tid,
            name=part["part_terminology_name"],
            category_id=category_id,
            category_name=categories_by_id.get(category_id),
            subcategory_id=subcategory_id,
            subcategory_name=subcategories_by_id.get(subcategory_id),
            description=part["part_terminology_description"],
            aliases=aliases_by_terminology.get(tid, []),
            aces_valid=tid in aces_terminology_ids,
            pies_valid=tid in pies_terminology_ids,
            superseded_by=superseded_by,
            is_active=superseded_by is None,
        ))

    src_models.PcdbTerminologyFlat.objects.bulk_create(objs, batch_size=CHUNK_SIZE)

    return {
        "terminology_count": len(objs),
        "with_category_count": sum(1 for o in objs if o.category_id is not None),
        "aces_invalid_count": sum(1 for o in objs if not o.aces_valid),
        "with_aliases_count": sum(1 for o in objs if o.aliases),
        "inactive_count": sum(1 for o in objs if not o.is_active),
        "skipped_alias_refs": skipped_alias_refs,
        "supersession_cycles": cycles,
    }
