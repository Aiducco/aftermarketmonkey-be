"""
Stage 1 orchestration: pulls MasterPart/VcdbVehicle data, runs the domain-layer masking/mining/
reconciliation functions, falls back to an LLM per-brand when coverage is too low, and persists
ProductGroup/ProductGroupMember. See src/domain/{title_mask,ngram_mining,prefix_trie,reconcile}.py
for the pure logic this wires together.
"""
import collections
import logging

from django.db import transaction

from src import models as src_models
from src.domain import ngram_mining, prefix_trie, reconcile, title_mask
from src.integrations.llm import azure_llm

logger = logging.getLogger(__name__)

MIN_BRAND_COVERAGE = 0.60
LLM_SAMPLE_SIZE = 50
LLM_METHOD = "llm"
LLM_CONFIDENCE = 0.70  # same tier as "one method fires" -- an LLM grouping is a single method, not a reconciled agreement

_LLM_SYSTEM_PROMPT = """You group auto-parts product titles into terminology-homogeneous product lines.
Given a numbered list of masked product titles (brand/year/make/model/color/part-number already
stripped) from ONE brand, identify recurring product-line groups: short, descriptive labels that
several titles share in common wording (e.g. "seatsaver custom seat cover", "control arm").
Rules:
- Prefer MORE, PURER groups over fewer, broader ones -- over-splitting is fine, merging unrelated
  titles into one group is not.
- Only propose a group if at least 3 of the listed titles clearly belong to it.
- A title that doesn't clearly fit any group should be left out of every group, not forced in.
- Group labels should be lowercase, generic descriptive phrases (not brand/marketing copy).
Return JSON: {"groups": [{"label": "...", "title_numbers": [1, 4, 7]}, ...]}. Titles may appear
in at most one group. Omit titles that don't fit anywhere."""


def _vcdb_lookup():
    rows = src_models.VcdbVehicle.objects.values_list("make", "model").distinct()
    return title_mask.VcdbLookup.from_rows(rows)


def _mask_brand_titles(master_parts, brand_name, vcdb):
    """master_parts: list of (id, part_number, description). Returns {id: residue}."""
    residues = {}
    for mp_id, _pn, description in master_parts:
        if not description:
            continue
        residues[mp_id] = title_mask.mask_title(description, brand_name, vcdb).residue
    return residues


def _llm_fallback(brand_name, unresolved_residues: dict, all_residues: dict) -> dict:
    """
    Stage 1e. unresolved_residues: {sku_id: residue} for SKUs Method A/B failed to group.
    Samples up to LLM_SAMPLE_SIZE of them, asks the LLM to name recurring groups, then maps
    each returned label back across ALL of unresolved_residues by substring match -- one LLM
    call per brand, not per SKU, per spec ("hundreds of calls total, not millions").
    """
    sample_items = list(unresolved_residues.items())[:LLM_SAMPLE_SIZE]
    sample_items = [(sku_id, residue) for sku_id, residue in sample_items if residue]
    if len(sample_items) < 3:
        return {}

    numbered = "\n".join(f"{i+1}. {residue}" for i, (_sku_id, residue) in enumerate(sample_items))
    cli = azure_llm.client()
    parsed, err = azure_llm.complete_json(
        cli, _LLM_SYSTEM_PROMPT, "Brand: {}\n\nTitles:\n{}".format(brand_name, numbered),
    )
    if err or not parsed:
        logger.warning("LLM fallback failed for brand=%s: %s", brand_name, err)
        return {}

    labels = []
    for group in parsed.get("groups", []):
        label = (group.get("label") or "").strip().lower()
        if label:
            labels.append(label)

    assignments = {}
    for sku_id, residue in unresolved_residues.items():
        for label in labels:
            if label and label in residue:
                assignments[sku_id] = label
                break

    counts = collections.Counter(assignments.values())
    return {
        sku_id: label for sku_id, label in assignments.items()
        if counts[label] >= ngram_mining.MIN_GROUP_SIZE
    }


def _display_name(group_key: str) -> str:
    return group_key.strip().title()


@transaction.atomic
def group_brand(brand_id: int, vcdb=None, master_parts=None) -> dict:
    """Runs Stage 1 end-to-end for one brand and persists the result. Returns a coverage report
    dict. Replaces any existing groups for this brand (idempotent re-run).

    master_parts: optional pre-fetched list of (id, part_number, description) rows, already
    filtered to this brand with a non-empty description -- pass this when calling group_brand
    for many brands in a row (see run_stage1) so each call doesn't issue its own DB round trip;
    with ~4,200 brands, per-brand queries measured as the dominant cost (83s alone to fetch one
    266K-SKU brand), far more than the masking/mining/reconcile work itself (~7s combined)."""
    brand = src_models.Brands.objects.get(id=brand_id)
    vcdb = vcdb or _vcdb_lookup()

    if master_parts is None:
        master_parts = list(
            src_models.MasterPart.objects.filter(brand_id=brand_id)
            .exclude(description=None).exclude(description="")
            .values_list("id", "part_number", "description")
        )
    total_skus = len(master_parts)
    if total_skus == 0:
        return {"brand_id": brand_id, "brand_name": brand.name, "total_skus": 0, "coverage": 0.0, "flagged": False}

    residues = _mask_brand_titles(master_parts, brand.name, vcdb)
    part_numbers = {mp_id: pn for mp_id, pn, _desc in master_parts}

    ngram_assignments = ngram_mining.mine_ngrams(residues)
    prefix_assignments = prefix_trie.mine_prefixes(part_numbers)
    reconciled = reconcile.reconcile(ngram_assignments, prefix_assignments)

    coverage = len(reconciled) / total_skus
    used_llm = False
    if coverage < MIN_BRAND_COVERAGE:
        unresolved = {mp_id: residues.get(mp_id, "") for mp_id, _pn, _desc in master_parts if mp_id not in reconciled}
        llm_assignments = _llm_fallback(brand.name, unresolved, residues)
        if llm_assignments:
            used_llm = True
            for sku_id, label in llm_assignments.items():
                reconciled[sku_id] = (label, LLM_METHOD, LLM_CONFIDENCE)
        coverage = len(reconciled) / total_skus

    # Persist: group by (group_key, method) since the same key text from different methods
    # after LLM fallback should not silently merge into a Method A/B group of the same name.
    # ProductGroupMember cascades on ProductGroup delete, so deleting groups is enough.
    src_models.ProductGroup.objects.filter(brand_id=brand_id).delete()

    # ProductGroup has a UNIQUE (brand, group_key) constraint -- dedup on group_key alone.
    # reconcile() already keeps every member of a given key on the same (method, confidence),
    # but an LLM-fallback label could in principle collide with an existing ngram/prefix key
    # text; if that ever happens, keep the highest-confidence verdict rather than crash.
    members_by_group = collections.defaultdict(list)
    group_meta = {}
    for sku_id, (group_key, method, confidence) in reconciled.items():
        members_by_group[group_key].append(sku_id)
        if group_key not in group_meta or confidence > group_meta[group_key][1]:
            group_meta[group_key] = (method, confidence)

    # Two bulk_create calls total (not one INSERT round-trip per group) -- with brands running
    # into thousands of distinct groups, per-group .create() calls were the actual bottleneck
    # measured here, well past the masking/mining/reconcile cost. Postgres returns pks from
    # bulk_create (no ignore_conflicts used), so the created objects can be used directly to
    # build member rows without a second query.
    created_groups = src_models.ProductGroup.objects.bulk_create([
        src_models.ProductGroup(
            brand_id=brand_id,
            group_key=group_key,
            display_name=_display_name(group_key),
            method=group_meta[group_key][0],
            grouping_confidence=group_meta[group_key][1],
            sku_count=len(sku_ids),
        )
        for group_key, sku_ids in members_by_group.items()
    ], batch_size=1000)

    member_objs = [
        src_models.ProductGroupMember(master_part_id=sku_id, group=group)
        for group in created_groups
        for sku_id in members_by_group[group.group_key]
    ]
    src_models.ProductGroupMember.objects.bulk_create(member_objs, batch_size=5000)

    return {
        "brand_id": brand_id,
        "brand_name": brand.name,
        "total_skus": total_skus,
        "grouped_skus": len(reconciled),
        "coverage": coverage,
        "used_llm": used_llm,
        "flagged": coverage < MIN_BRAND_COVERAGE,
        "high_confidence_skus": sum(1 for _k, _m, c in reconciled.values() if c >= 0.70),
    }


def run_stage1(brand_ids=None, progress_every=100) -> list:
    """brand_ids: optional iterable to restrict to; defaults to every brand with MasterParts.

    Fetches every relevant MasterPart row ONCE up front and groups them in Python, rather than
    one query per brand -- with ~4,200 brands, per-brand queries measured as the dominant cost
    by a wide margin (see group_brand's docstring)."""
    vcdb = _vcdb_lookup()

    qs = src_models.MasterPart.objects.exclude(description=None).exclude(description="")
    if brand_ids is not None:
        brand_ids = list(brand_ids)
        qs = qs.filter(brand_id__in=brand_ids)

    by_brand = collections.defaultdict(list)
    for mp_id, brand_id, pn, desc in qs.values_list("id", "brand_id", "part_number", "description").iterator(chunk_size=20_000):
        by_brand[brand_id].append((mp_id, pn, desc))

    if brand_ids is None:
        brand_ids = list(by_brand.keys())

    reports = []
    for i, brand_id in enumerate(brand_ids, start=1):
        try:
            report = group_brand(brand_id, vcdb=vcdb, master_parts=by_brand.get(brand_id, []))
        except Exception as e:
            logger.exception("Stage 1 grouping failed for brand_id=%s", brand_id)
            report = {"brand_id": brand_id, "error": str(e)}
        reports.append(report)
        if progress_every and i % progress_every == 0:
            logger.info("Stage 1: processed %d/%d brands", i, len(brand_ids))
    return reports
