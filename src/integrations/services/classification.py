"""
Stages G-I: PCdb terminology retrieval + adjudication + write-back. Deliberately a separate
module from product_grouping.py (Stages A-F) -- a grouping fix should never force
reclassification and vice versa. See src/domain/terminology_index.py for the pure retrieval
logic this wires together.

Confidence-gate thresholds below are calibrated against real WeatherTech retrieval scores, not
the plan's illustrative numbers verbatim: querying with just a group's display name produced
near-total ties (ratio ~1.0) across almost every group -- richer query text (head name + a
handful of masked member residues) fixed most of that, but even then, real "clearly correct"
top hits landed at ratio 1.18-1.52, not the plan's suggested >=1.8 (which would have sent even
the unambiguous FloorLiner/All-Weather-Floor-Mats cases to the model unnecessarily).
"""
import collections
import json
import logging

from django.db import transaction

from src import models as src_models
from src.domain import title_mask
from src.domain.terminology_index import TerminologyIndex
from src.integrations.llm import azure_llm
from src.integrations.services import product_grouping

logger = logging.getLogger(__name__)

SCORE_RATIO_THRESHOLD = 1.15
MIN_ABSOLUTE_SCORE = 3.0
SHORTLIST_K = 8
SIBLING_COUNT = 5
QUERY_SAMPLE_MEMBERS = 5
STAGE_H_MAX_TOKENS = 800
KEYWORD_EXPANSION_MAX_TOKENS = 200

_KEYWORD_EXPANSION_SYSTEM = """A retrieval search over auto-parts terminology found ZERO matches
for a product line -- this usually means its name is a manufacturer coinage (e.g. "LampGard",
"FloorLiner") that doesn't share any vocabulary with a standard parts catalogue, not that the
product has no real-world equivalent.

Given the brand and product line name (and any real sample titles), name the GENERIC part type
in plain catalogue English -- what a parts catalogue would actually call this, not the brand's
marketing name for it. Give 2-4 short keyword phrases, most likely first.

Return strict JSON: {"keywords": ["...", "...", ...]}"""

_STAGE_H_SYSTEM = """You classify an auto-parts product line into the single best-matching PCdb
part terminology from a shortlist of candidates.

Pick the candidate whose "name"/"description" most precisely describes what this product line
actually IS -- not a loosely related category. PCdb descriptions are often nearly tautological
("Truck Bed Mat." is a real, complete description), so use "siblings" (other real terminologies
in the same PCdb subcategory) as contrast -- they tell you what the chosen candidate is NOT,
often the only way to disambiguate.

If the group's residues describe more than one genuinely different part type (not just cosmetic
variants like colour or position), return "splits" instead of a single terminology_id -- e.g. a
group that turns out to mix Front Floor Liner and Cargo Liner SKUs under one name needs two
different terminology_ids depending on which residue matched. Any group with splits gets routed
to human review regardless of confidence -- never auto-apply a split.

Return strict JSON:
{"terminology_id": <int or null>, "confidence": <0.0-1.0>, "reasoning": "<one sentence>",
 "splits": [{"terminology_id": <int>, "match_on": "residue substring"}] or null}

terminology_id MUST be one of the candidate "id" values given -- never invent an ID. If none of
the candidates are a good match, return terminology_id: null and explain why in reasoning."""


def build_index() -> TerminologyIndex:
    rows = list(src_models.PcdbTerminologyFlat.objects.values(
        "part_terminology_id", "name", "aliases", "is_active",
        "category_name", "subcategory_name", "subcategory_id", "description",
    ))
    return TerminologyIndex(rows)


def _query_text_for_group(group, index, vcdb, brand_name):
    """Rich query per Stage G's design: head name + masked residues of a few member titles --
    NOT raw titles (still carry brand/model/year noise that measurably pollutes retrieval, see
    module docstring) and not the bare display name alone (near-total ties on real data)."""
    member_ids = list(
        src_models.ProductGroupMember.objects.filter(group=group)
        .values_list("master_part_id", flat=True)[:QUERY_SAMPLE_MEMBERS]
    )
    titles = list(src_models.MasterPart.objects.filter(id__in=member_ids).values_list("description", flat=True))
    residues = [title_mask.mask_title(t, brand_name, vcdb).residue for t in titles if t]
    return group.display_name + " " + " ".join(residues)


def _siblings_for(tid, index, k=SIBLING_COUNT):
    meta = index.term_meta[tid]
    subcat = meta["subcategory_id"]
    siblings = []
    for other_id, other_meta in index.term_meta.items():
        if other_id == tid or other_meta["subcategory_id"] != subcat:
            continue
        siblings.append(other_meta["name"])
        if len(siblings) >= k:
            break
    return siblings


def _candidate_payload(shortlist, index):
    payload = []
    for tid, _score in shortlist:
        meta = index.term_meta[tid]
        payload.append({
            "id": tid,
            "name": meta["name"],
            "description": meta["description"] or meta["name"],
            "category": meta["category_name"],
            "subcategory": meta["subcategory_name"],
            "siblings": _siblings_for(tid, index),
        })
    return payload


def _expand_query_with_keywords(group, brand_name, cli):
    """
    Fallback for a manufacturer coinage name (e.g. "LampGard") whose masked residues carry NO
    generic vocabulary at all -- the brand's own titles never say what the product generically
    IS, only its trademarked name, so no amount of richer local query text helps (verified: this
    is not a query-construction bug, WeatherTech's LampGard residues are just "lampgard"/
    "lampgard transparent" and nothing else). Asks the model to name the generic part type from
    brand/world knowledge, then that becomes the query -- the final answer is still constrained
    to whatever real PCdb terminology retrieval finds for those keywords, never the model's
    guess directly, so this doesn't reopen the "unconstrained LLM classification" risk.
    """
    payload = {"brand": brand_name, "product_line": group.display_name, "sku_count": group.sku_count}
    parsed, err = azure_llm.complete_json(
        cli, _KEYWORD_EXPANSION_SYSTEM, json.dumps(payload), max_tokens=KEYWORD_EXPANSION_MAX_TOKENS,
    )
    if err or not parsed:
        return None
    keywords = [k for k in parsed.get("keywords", []) if k]
    return " ".join(keywords) if keywords else None


def classify_group(group, index, vcdb, brand_name, cli=None) -> dict:
    """Stage G (retrieval) + Stage H (gate-first adjudication) for one ProductGroup. Returns
    {terminology_id, confidence, source, shortlist, ...} -- terminology_id is None if nothing
    could be determined (no candidates at all, or the model legitimately found no good match).
    Never persists anything; see stage I functions below for that."""
    cli = cli or azure_llm.client()
    query_text = _query_text_for_group(group, index, vcdb, brand_name)
    shortlist = index.search(query_text, k=SHORTLIST_K)

    if not shortlist:
        keyword_query = _expand_query_with_keywords(group, brand_name, cli)
        if keyword_query:
            shortlist = index.search(keyword_query, k=SHORTLIST_K)
            if shortlist:
                query_text = keyword_query
                logger.info("Group=%s had no candidates on masked residues, recovered via keyword expansion: %r",
                            group.display_name, keyword_query)
        if not shortlist:
            return {"terminology_id": None, "confidence": 0.0, "source": "no_candidates", "shortlist": []}

    q_tokens = set(index.query_tokens(query_text))
    top_id, top_score = shortlist[0]
    second_score = shortlist[1][1] if len(shortlist) > 1 else 0.0
    ratio = (top_score / second_score) if second_score > 0 else float("inf")
    head_noun_ok = index.term_head_noun.get(top_id) in q_tokens

    if ratio >= SCORE_RATIO_THRESHOLD and head_noun_ok and top_score >= MIN_ABSOLUTE_SCORE:
        return {
            "terminology_id": top_id, "confidence": 0.95, "source": "gate",
            "shortlist": shortlist, "gate_ratio": ratio,
        }

    candidates = _candidate_payload(shortlist, index)
    payload = {"group": {"brand": brand_name, "name": group.display_name, "sku_count": group.sku_count},
               "candidates": candidates}

    cost = azure_llm.estimate_cost(_STAGE_H_SYSTEM, json.dumps(payload), STAGE_H_MAX_TOKENS)
    logger.info("Stage H call for group=%s: ~%d input tokens, worst-case $%.4f",
                group.display_name, cost["input_tokens_est"], cost["total_cost_usd_worst_case"])

    parsed, err = azure_llm.complete_json(cli, _STAGE_H_SYSTEM, json.dumps(payload), max_tokens=STAGE_H_MAX_TOKENS)
    if err or not parsed:
        logger.warning("Stage H failed for group=%s: %s", group.display_name, err)
        return {"terminology_id": None, "confidence": 0.0, "source": "model_error", "shortlist": shortlist, "error": err}

    tid = parsed.get("terminology_id")
    valid_ids = {c["id"] for c in candidates}
    if tid is not None and tid not in valid_ids:
        logger.warning("Stage H for group=%s returned terminology_id=%s not in candidate list %s -- rejecting",
                        group.display_name, tid, sorted(valid_ids))
        return {"terminology_id": None, "confidence": 0.0, "source": "invalid_response", "shortlist": shortlist}

    splits = parsed.get("splits")
    return {
        "terminology_id": tid,
        "confidence": parsed.get("confidence", 0.5),
        "source": "model_split" if splits else "model",
        "reasoning": parsed.get("reasoning"),
        "splits": splits,
        "shortlist": shortlist,
    }


def persist_classification(group, result: dict) -> src_models.ProductLineTerminologyMap:
    """Stage I: write the Stage H verdict for one group. Writes a row even for terminology_id=None
    outcomes (no_candidates/model_error/invalid_response) -- a brand-wide run's unresolved groups
    stay queryable for review rather than silently absent. Keyed on `group` (the map's PK), so
    re-running classification for a still-live group updates its row rather than duplicating it."""
    return src_models.ProductLineTerminologyMap.objects.update_or_create(
        group=group,
        defaults={
            "part_terminology_id": result.get("terminology_id"),
            "confidence": round(result.get("confidence") or 0.0, 2),
            "source": result["source"],
            "splits": result.get("splits"),
            "reasoning": result.get("reasoning"),
        },
    )[0]


def propagate_to_master_parts(group, result: dict) -> int:
    """Applies a clean (resolved, non-split) classification to every MasterPart in the group.
    Splits are never auto-applied: Stage H's own contract is that a split means the group's
    residues actually cover more than one real terminology, so a single group-level ID would be
    wrong for some fraction of its members -- those stay NULL until a human resolves the split
    (see _STAGE_H_SYSTEM). Returns the number of MasterPart rows updated."""
    tid = result.get("terminology_id")
    if tid is None or result.get("splits"):
        return 0
    member_ids = list(
        src_models.ProductGroupMember.objects.filter(group=group).values_list("master_part_id", flat=True)
    )
    return src_models.MasterPart.objects.filter(id__in=member_ids).update(part_terminology_id=tid)


@transaction.atomic
def classify_brand(brand_id: int, index=None, vcdb=None, cli=None) -> dict:
    """
    Stages G-I for every ProductGroup already persisted for one brand -- grouping
    (product_grouping.group_brand) must run first. Idempotent re-run against the same live group
    set: persist_classification updates in place rather than duplicating rows.

    index/vcdb/cli are optional pre-built instances to avoid rebuilding the terminology index
    and VCdb lookup on every call when classifying many brands in a row (see classify_brands).
    """
    brand = src_models.Brands.objects.get(id=brand_id)
    index = index or build_index()
    vcdb = vcdb or product_grouping._vcdb_lookup()
    cli = cli or azure_llm.client()

    groups = list(src_models.ProductGroup.objects.filter(brand_id=brand_id))
    if not groups:
        return {"brand_id": brand_id, "brand_name": brand.name, "groups": 0}

    resolved = 0
    unresolved = 0
    split_count = 0
    propagated_skus = 0
    by_source = collections.Counter()

    for group in groups:
        result = classify_group(group, index, vcdb, brand.name, cli=cli)
        persist_classification(group, result)
        by_source[result["source"]] += 1
        if result.get("splits"):
            split_count += 1
        elif result.get("terminology_id") is not None:
            resolved += 1
            propagated_skus += propagate_to_master_parts(group, result)
        else:
            unresolved += 1

    return {
        "brand_id": brand_id,
        "brand_name": brand.name,
        "groups": len(groups),
        "resolved": resolved,
        "unresolved": unresolved,
        "splits": split_count,
        "propagated_skus": propagated_skus,
        "by_source": dict(by_source),
    }


def classify_brands(brand_ids, progress_every=10) -> list:
    """Bulk entry point: builds the terminology index and VCdb lookup once, then runs
    classify_brand for each brand_id in turn. Mirrors product_grouping.run_grouping's shape."""
    index = build_index()
    vcdb = product_grouping._vcdb_lookup()
    cli = azure_llm.client()

    reports = []
    for i, brand_id in enumerate(brand_ids, start=1):
        reports.append(classify_brand(brand_id, index=index, vcdb=vcdb, cli=cli))
        if progress_every and i % progress_every == 0:
            logger.info("classify_brands: %d/%d brands done", i, len(brand_ids))
    return reports
