"""
Stages B-F orchestration for the part-classification pipeline: pulls MasterPart/VcdbVehicle
data, masks titles (Stage A, see title_mask.py), extracts residues (B), derives head-noun
candidates (C, see head_nouns.py), calls the LLM for product-line extraction rules (D), applies
them deterministically (E), and validates coverage (F). Persists ProductGroup/ProductGroupMember.

Replaces the earlier n-gram-mining/prefix-trie/exact-partition-reconciliation design (see
src/domain/reconcile.py's removal in this same change) -- that design measured real but opaque
output (5.5% of SKUs above the confidence bar its own acceptance criteria required, because two
independently-derived partitions almost never match exactly even when both are individually
correct). This design instead has the LLM propose validated literal-substring match rules from a
sample of residues, applied deterministically across the whole brand -- no per-SKU LLM calls, no
exact-partition-agreement requirement. Verified against real WeatherTech data (32,543 SKUs) at
89.7%+ coverage with 37 correctly-distinguished product lines before the fixes below; this
module includes two additional fixes found in that verification (see _call_stage_d).
"""
import collections
import logging

from django.db import transaction

from src import models as src_models
from src.domain import head_nouns, title_mask
from src.domain.prefix_trie import mine_prefixes
from src.integrations.llm import azure_llm

logger = logging.getLogger(__name__)

TOP_RESIDUES_FOR_STAGE_D = 150
TOP_PREFIXES_FOR_STAGE_D = 30
MAX_STAGE_D_PASSES = 3  # bounded like the original n-gram spec's "up to 3 passes" -- each pass
# mines a shrinking remainder, diminishing returns past a few rounds.
STAGE_D_MAX_TOKENS = 24000  # a real run against WeatherTech truncated invalid JSON at 8000
# once the multi-variant-match-rule instruction was added (legitimately larger output -- more
# {name, match} entries per head now) -- an earlier, larger-prompt scratchpad test needed 32000
# to complete reliably; 24000 is a middle ground revisit if real runs still truncate.
CONSOLIDATION_MAX_TOKENS = 4000
COVERAGE_TARGET = 0.90  # Stage F gate
SANITY_MAX_GROUP_SHARE = 0.30  # Stage F gate: a group holding more than this share of the
# brand's SKUs gets flagged for a manual look, same as a group holding exactly 1 SKU.

GROUPING_METHOD = "llm"
GROUPING_CONFIDENCE = 0.85  # Flat baseline, not a cross-method-agreement score like the old
# design's -- Stage D's rules are individually validated (literal-substring check) rather than
# scored per-SKU. Real per-group confidence lives downstream in Stage H's classification step.

_STAGE_D_SYSTEM = """Extract product lines from an auto parts catalogue. A product line is the
manufacturer's product family -- the head noun phrase (e.g. "FloorLiner", "Cargo Liner").
Position (front/rear/2nd row), colour, fitment qualifiers (AWD only, quad cab, no spare tire,
behind 3rd row) and variant codes are ATTRIBUTES of a line, not separate lines. Prefer more
lines over fewer -- but never merge families a customer would consider different products (e.g.
FloorLiner, Cargo Liner, All-Weather Floor Mats, and Rubber Mats are four different products).
Flag any lines that are not automotive parts.

Rules are applied by SUBSTRING match, so ONE short match string already catches every residue
that contains it -- "floorliner" alone matches "front floorliner", "rear floorliner", "3500
front floorliner", "is front floorliner", etc. Do NOT add a separate {"name", "match"} entry for
every position/trim/qualifier combination around the same head noun -- that is redundant with
the one short match and wastes output. Propose ONE match string per line in the normal case.

The ONLY time to add a second {"name", "match"} entry sharing the same "name" is when the core
product wording itself changes such that no single short substring covers both -- e.g. "Cargo
Liner with Bumper Protector" vs "Cargo with Bumper Protector" (the word "Liner" is missing
entirely, so "cargo liner" as a match string would NOT catch the second residue) vs "Liner with
Bumper Protector" (missing "Cargo" instead). That is genuine wording drift; a "floorliner" vs
"3500 front floorliner" pair is NOT -- it's the same wording, just with more attribute words
around it, already handled by one match string.

Return strict JSON:
{
  "heads": [{"name": "...", "match": "...", "priority": N}, ...],
  "attribute_tokens": {"position": [...], "color": [...], "variant": [...], "qualifier": [...]},
  "non_automotive": [...]
}

"match" MUST be a literal, verbatim substring of at least one of the residues given below (case
-insensitive) -- do not paraphrase or invent a match string that doesn't literally appear.
"priority" is an integer; higher priority match strings are tested first when applying rules
(so a longer, more specific match like "seatback cargo liner" should have higher priority than
the more general "cargo liner" it's a substring of, so it wins when both would match)."""

_STAGE_D_RETRY_NOTE = """
Your previous response included these "match" values that are NOT literal substrings of any
residue given -- they were rejected. Do not repeat this mistake: every "match" string must be
copy-pasted verbatim from the residue list, not paraphrased or invented.
Rejected: {rejected}"""

_CONSOLIDATION_SYSTEM = """You are given a list of product-line group names for one brand, each
with how many SKUs currently belong to it. Some of these names may actually be the SAME real
product, just named inconsistently because the brand's own titles worded it differently in
different places -- e.g. "Liner", "Cargo Liner", "Cargo With Bumper Protector", and "Liner with
Bumper Protector" might all really be one "Cargo Liner with Bumper Protector" product, split
apart by wording differences rather than being genuinely different products.

Identify names that should be merged into one canonical name. Do NOT merge genuinely different
products (e.g. "FloorLiner" and "Cargo Liner" are different products, not the same thing worded
differently) -- when in doubt, don't merge.

Return strict JSON:
{"merges": [{"canonical_name": "...", "merge_names": ["name1", "name2", ...]}]}
merge_names MUST be copied verbatim from the names given -- do not paraphrase them. Only include
groups genuinely being merged (>=2 names sharing one canonical_name); omit everything else."""


def _consolidate_group_names(brand_name, group_sizes: dict) -> dict:
    """
    Final cleanup pass, run once after all Stage D passes finish for a brand: reconciles
    near-duplicate group NAMES that fragment one real product across passes/wording variants.
    Operates on the small list of already-created names (typically tens to low hundreds), not
    the raw residues, so this stays cheap regardless of brand size.

    group_sizes: {name: sku_count}. Returns {old_name: canonical_name} for names that should be
    renamed; names not present in the returned dict are left as-is (including the common case
    where the model finds nothing worth merging).
    """
    if len(group_sizes) < 2:
        return {}

    lines = "\n".join(f"{count}\t{name}" for name, count in sorted(group_sizes.items(), key=lambda kv: -kv[1]))
    user_text = f"Brand: {brand_name}\n\n{lines}"

    cli = azure_llm.client()
    cost = azure_llm.estimate_cost(_CONSOLIDATION_SYSTEM, user_text, CONSOLIDATION_MAX_TOKENS)
    logger.info("Consolidation call for brand=%s: ~%d input tokens, worst-case $%.4f",
                brand_name, cost["input_tokens_est"], cost["total_cost_usd_worst_case"])

    parsed, err = azure_llm.complete_json(cli, _CONSOLIDATION_SYSTEM, user_text, max_tokens=CONSOLIDATION_MAX_TOKENS)
    if err or not parsed:
        logger.warning("Consolidation failed for brand=%s: %s", brand_name, err)
        return {}

    valid_names = set(group_sizes.keys())
    rename_map = {}
    for merge in parsed.get("merges", []):
        canonical = (merge.get("canonical_name") or "").strip()
        names = [n for n in (merge.get("merge_names") or []) if n in valid_names]
        if not canonical or len(names) < 2:
            continue  # not a real merge, or references names that don't exist -- skip it
        for n in names:
            rename_map[n] = canonical
    if rename_map:
        logger.info("Consolidation for brand=%s merged %d names into %d canonical group(s)",
                     brand_name, len(rename_map), len(set(rename_map.values())))
    return rename_map


def _vcdb_lookup():
    return title_mask.VcdbLookup.from_rows(
        src_models.VcdbVehicle.objects.values_list("make", "model").distinct(),
        trims=src_models.VcdbVehicle.objects.exclude(submodel="").values_list("submodel", flat=True).distinct(),
    )


def extract_residues(master_parts, brand_name, vcdb):
    """
    Stage B. master_parts: list of (id, part_number, description).
    Returns (residue_counts: {residue: sku_count}, residue_samples: {residue: [example_titles]},
    sku_residue: {sku_id: residue}, sku_part_number: {sku_id: part_number}).
    """
    residue_counts = collections.Counter()
    residue_samples = collections.defaultdict(list)
    sku_residue = {}
    sku_part_number = {}

    for mp_id, pn, description in master_parts:
        sku_part_number[mp_id] = pn
        if not description:
            continue
        residue = title_mask.mask_title(description, brand_name, vcdb).residue
        sku_residue[mp_id] = residue
        residue_counts[residue] += 1
        if len(residue_samples[residue]) < 2:
            residue_samples[residue].append(description)

    return dict(residue_counts), dict(residue_samples), sku_residue, sku_part_number


def _validate_heads(heads, residue_counts):
    """Every 'match' must be a literal substring of some real residue. Returns (valid, invalid)."""
    haystack = " | ".join(residue_counts.keys()).lower()
    valid, invalid = [], []
    for h in heads:
        match = (h.get("match") or "").strip()
        if match and match.lower() in haystack:
            valid.append(h)
        else:
            invalid.append(h)
    return valid, invalid


def _build_stage_d_user_text(brand_name, residue_counts, residue_samples, sku_part_number, top_n):
    top = sorted(residue_counts.items(), key=lambda kv: -kv[1])[:top_n]
    if not top:
        return None

    candidates = head_nouns.derive_head_candidates(residue_counts)
    residue_lines = "\n".join(
        f"{count}\t{residue}\t| e.g. " + " || ".join(residue_samples.get(residue, []))
        for residue, count in top
    )
    candidate_lines = "\n".join(f"{weight}\t{candidate}" for candidate, weight in candidates[:40])

    prefix_block = ""
    if sku_part_number:
        prefix_assignments = mine_prefixes(sku_part_number)
        prefix_counts = collections.Counter(prefix_assignments.values())
        top_prefixes = prefix_counts.most_common(TOP_PREFIXES_FOR_STAGE_D)
        if top_prefixes:
            prefix_lines = "\n".join(f"{prefix}\t{count}" for prefix, count in top_prefixes)
            prefix_block = f"\n\nPart-number prefixes (prefix, sku_count) -- a supporting signal, not authoritative:\n{prefix_lines}"

    return (
        f"Brand: {brand_name}\n\n"
        f"Top residues (sku_count, residue, example raw titles):\n{residue_lines}\n\n"
        f"Head-noun candidates (weight, candidate) -- deterministically derived, a starting hint only:\n{candidate_lines}"
        f"{prefix_block}"
    )


def _call_stage_d(brand_name, residue_counts, residue_samples, sku_part_number=None):
    """
    Stage D: one LLM call proposing {name, match, priority} rules. Two independent retry paths,
    both observed for real on WeatherTech/Sherman Parts:
    - Response parses but has a hallucinated match (silently groups nothing if unchecked) ->
      retry once with the specific rejected match strings called out.
    - Response fails to parse at all, usually because it was truncated mid-JSON by an
      over-large output (Sherman Parts pass 3: model tried to generate too many rules for a
      150-residue input) -> retry once with HALF the residues, which reduces expected output
      size directly rather than just hoping a bigger max_tokens is enough.

    sku_part_number: optional {sku_id: part_number} restricted to the SKUs whose residues are
    in residue_counts (i.e. the current pass's pool, not necessarily the whole brand) -- part
    number prefixes are a supporting signal alongside residues/head candidates, not the primary
    input, so this degrades gracefully to no prefix info if not supplied.
    """
    user_text = _build_stage_d_user_text(brand_name, residue_counts, residue_samples, sku_part_number, TOP_RESIDUES_FOR_STAGE_D)
    if user_text is None:
        return []

    cli = azure_llm.client()
    cost = azure_llm.estimate_cost(_STAGE_D_SYSTEM, user_text, STAGE_D_MAX_TOKENS)
    logger.info(
        "Stage D call for brand=%s: ~%d input tokens, worst-case $%.4f",
        brand_name, cost["input_tokens_est"], cost["total_cost_usd_worst_case"],
    )

    parsed, err = azure_llm.complete_json(cli, _STAGE_D_SYSTEM, user_text, max_tokens=STAGE_D_MAX_TOKENS)
    if err or not parsed:
        logger.warning("Stage D failed to parse for brand=%s (%s), retrying with half the residues",
                        brand_name, err)
        smaller_user_text = _build_stage_d_user_text(
            brand_name, residue_counts, residue_samples, sku_part_number, TOP_RESIDUES_FOR_STAGE_D // 2,
        )
        if smaller_user_text is None:
            return []
        parsed, err = azure_llm.complete_json(cli, _STAGE_D_SYSTEM, smaller_user_text, max_tokens=STAGE_D_MAX_TOKENS)
        if err or not parsed:
            logger.warning("Stage D failed again for brand=%s even with fewer residues: %s", brand_name, err)
            return []

    heads = parsed.get("heads", [])
    valid, invalid = _validate_heads(heads, residue_counts)
    if not invalid:
        return valid

    logger.warning("Stage D for brand=%s produced %d hallucinated match(es), retrying once: %s",
                    brand_name, len(invalid), [h.get("match") for h in invalid])
    retry_user = user_text + _STAGE_D_RETRY_NOTE.format(rejected=[h.get("match") for h in invalid])
    parsed2, err2 = azure_llm.complete_json(cli, _STAGE_D_SYSTEM, retry_user, max_tokens=STAGE_D_MAX_TOKENS)
    if err2 or not parsed2:
        return valid  # keep whatever validated the first time, drop the rest

    heads2 = parsed2.get("heads", [])
    valid2, invalid2 = _validate_heads(heads2, residue_counts)
    if invalid2:
        logger.warning("Stage D retry for brand=%s still had %d hallucinated match(es), dropping them: %s",
                        brand_name, len(invalid2), [h.get("match") for h in invalid2])
    return valid2


def apply_rules(heads, sku_residue):
    """
    Stage E: priority-ordered substring match, longest-specific-first among ties. Pure Python
    over the in-memory {sku_id: residue} map (not literal SQL LIKE) -- brands range from a
    handful of SKUs to 266K+ and the brand-loop already has to stay in Python for the
    multi-pass Stage D orchestration, so there's no separate DB round trip to save here.
    Returns {sku_id: head_name} for every matched SKU (unmatched SKUs are simply absent).
    """
    ordered = sorted(heads, key=lambda h: (-h["priority"], -len(h["match"])))
    assignments = {}
    for sku_id, residue in sku_residue.items():
        low = residue.lower()
        for h in ordered:
            if h["match"].lower() in low:
                assignments[sku_id] = h["name"]
                break
    return assignments


@transaction.atomic
def group_brand(brand_id: int, vcdb=None, master_parts=None) -> dict:
    """
    Runs Stages B-F end-to-end for one brand and persists the result. Returns a coverage report
    dict. Replaces any existing groups for this brand (idempotent re-run).

    master_parts: optional pre-fetched list of (id, part_number, description) rows, already
    filtered to this brand with a non-empty description -- pass this when calling group_brand
    for many brands in a row (see run_grouping) so each call doesn't issue its own DB round
    trip; with ~4,200 brands, per-brand queries measured as the dominant cost (83s alone to
    fetch one 266K-SKU brand).
    """
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

    residue_counts, residue_samples, sku_residue, sku_part_number = extract_residues(master_parts, brand.name, vcdb)

    all_heads = []
    assignments = {}
    remaining_sku_residue = dict(sku_residue)

    for _pass_num in range(MAX_STAGE_D_PASSES):
        if not remaining_sku_residue:
            break
        remaining_residue_counts = collections.Counter()
        for residue in remaining_sku_residue.values():
            remaining_residue_counts[residue] += 1
        remaining_pn = {sku_id: sku_part_number[sku_id] for sku_id in remaining_sku_residue}

        pass_heads = _call_stage_d(brand.name, dict(remaining_residue_counts), residue_samples, remaining_pn)
        if not pass_heads:
            break
        all_heads.extend(pass_heads)

        pass_assignments = apply_rules(pass_heads, remaining_sku_residue)
        assignments.update(pass_assignments)
        remaining_sku_residue = {
            sku_id: r for sku_id, r in remaining_sku_residue.items() if sku_id not in pass_assignments
        }
        if not pass_assignments:
            break  # no progress this pass, stop rather than spin

    coverage = len(assignments) / total_skus

    # Cheap final cleanup: the multi-pass loop above can leave near-duplicate group NAMES for
    # what's really one product, fragmented by wording differences across passes (found for
    # real on WeatherTech: "Liner with Bumper Protector" and "Cargo With Bumper Protector"
    # ended up classified to two *different* PCdb terminologies downstream, when they're almost
    # certainly the same product). One LLM call over the small list of already-created names,
    # not the raw residues, so this is cheap regardless of brand size.
    group_sizes_pre = collections.Counter(assignments.values())
    rename_map = _consolidate_group_names(brand.name, dict(group_sizes_pre))
    if rename_map:
        assignments = {sku_id: rename_map.get(name, name) for sku_id, name in assignments.items()}

    # Stage F: coverage/sanity gates. No model -- pure reporting, nothing here blocks
    # persistence; a flagged brand just shows up in the report for review.
    group_sizes = collections.Counter(assignments.values())
    sanity_flags = [
        name for name, size in group_sizes.items()
        if size == 1 or size / total_skus > SANITY_MAX_GROUP_SHARE
    ]

    src_models.ProductGroup.objects.filter(brand_id=brand_id).delete()

    members_by_group = collections.defaultdict(list)
    for sku_id, name in assignments.items():
        members_by_group[name].append(sku_id)

    created_groups = src_models.ProductGroup.objects.bulk_create([
        src_models.ProductGroup(
            brand_id=brand_id,
            group_key=name.strip().lower(),
            display_name=name,
            method=GROUPING_METHOD,
            grouping_confidence=GROUPING_CONFIDENCE,
            sku_count=len(sku_ids),
        )
        for name, sku_ids in members_by_group.items()
    ], batch_size=1000)

    member_objs = [
        src_models.ProductGroupMember(master_part_id=sku_id, group=group)
        for group in created_groups
        for sku_id in members_by_group[group.display_name]
    ]
    src_models.ProductGroupMember.objects.bulk_create(member_objs, batch_size=5000)

    return {
        "brand_id": brand_id,
        "brand_name": brand.name,
        "total_skus": total_skus,
        "grouped_skus": len(assignments),
        "coverage": coverage,
        "groups": len(created_groups),
        "stage_d_passes": _pass_num + 1,
        "flagged_coverage": coverage < COVERAGE_TARGET,
        "flagged_sanity": sanity_flags,
    }


def run_grouping(brand_ids=None, progress_every=100) -> list:
    """brand_ids: optional iterable to restrict to; defaults to every brand with MasterParts.

    Fetches every relevant MasterPart row ONCE up front and groups them in Python, rather than
    one query per brand -- with ~4,200 brands, per-brand queries measured as the dominant DB
    cost by a wide margin (see group_brand's docstring)."""
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
            logger.exception("Grouping failed for brand_id=%s", brand_id)
            report = {"brand_id": brand_id, "error": str(e)}
        reports.append(report)
        if progress_every and i % progress_every == 0:
            logger.info("Grouping: processed %d/%d brands", i, len(brand_ids))
    return reports
