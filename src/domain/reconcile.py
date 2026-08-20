"""
Stage 1d: reconcile Method A (n-gram) and Method B (prefix) assignments per the spec's 2x2
table. Pure function of two {sku_id: group_key} dicts; no Django imports.

Operates per GROUP, not per SKU: product_group has a UNIQUE (brand, group_key) constraint (one
row per key, holding one method/confidence for the whole group), so every SKU sharing an
n-gram or prefix key must land on the same verdict -- resolving this per-SKU independently (as
an earlier version of this function did) let two SKUs with the identical group_key disagree on
confidence and crash the unique constraint on insert.
"""
import collections

AGREE_CONFIDENCE = 0.95
ONE_FIRES_CONFIDENCE = 0.70
DISAGREE_CONFIDENCE = 0.40


def reconcile(ngram_assignments: dict, prefix_assignments: dict) -> dict:
    """
    Returns {sku_id: (group_key, method, confidence)} for every SKU assigned by at least one
    method. A SKU in neither dict is "both fail" -- absent here, Stage 1e's job to pick up.

    "Agree" means the two methods landed on the SAME PARTITION: an n-gram group's full SKU
    membership exactly equals some prefix group's full SKU membership, not just that this one
    SKU happens to have an assignment from both. "One fires, other silent" means every member
    of this group has no assignment at all from the other method. Anything else (the other
    method fired for at least one member, but the partitions don't match) is "disagree".
    """
    ngram_members = collections.defaultdict(set)
    for sku_id, key in ngram_assignments.items():
        ngram_members[key].add(sku_id)
    prefix_members = collections.defaultdict(set)
    for sku_id, key in prefix_assignments.items():
        prefix_members[key].add(sku_id)

    # Exact-partition-match lookup keyed by frozenset(members) -> prefix_key, built once, so
    # each n-gram group below is an O(1) average dict lookup instead of comparing against every
    # prefix group in turn (O(ngram_groups x prefix_groups) -- measured multi-minute-plus stalls
    # on brands with thousands of groups on either side, e.g. Covercraft's 266K SKUs).
    prefix_by_memberset = {frozenset(members): key for key, members in prefix_members.items()}

    result = {}
    prefix_keys_consumed = set()

    for ngram_key, members in ngram_members.items():
        matching_prefix_key = prefix_by_memberset.get(frozenset(members))
        if matching_prefix_key is not None:
            method, confidence = "both", AGREE_CONFIDENCE
            prefix_keys_consumed.add(matching_prefix_key)
        elif any(sku_id in prefix_assignments for sku_id in members):
            method, confidence = "both", DISAGREE_CONFIDENCE
        else:
            method, confidence = "ngram", ONE_FIRES_CONFIDENCE
        for sku_id in members:
            result[sku_id] = (ngram_key, method, confidence)

    for prefix_key, members in prefix_members.items():
        if prefix_key in prefix_keys_consumed:
            continue
        remaining = members - set(result)
        if not remaining:
            continue
        if any(sku_id in ngram_assignments for sku_id in remaining):
            method, confidence = "both", DISAGREE_CONFIDENCE
        else:
            method, confidence = "prefix", ONE_FIRES_CONFIDENCE
        for sku_id in remaining:
            result[sku_id] = (prefix_key, method, confidence)

    return result
