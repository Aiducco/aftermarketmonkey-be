"""
Stage 1b: Method A -- group SKUs within a brand by contiguous n-grams mined from their masked
title residue (see title_mask.py). Pure function of {sku_id: residue_text}; no Django imports.
"""
import collections

MIN_GROUP_SIZE = 5
MAX_PASSES = 3
NGRAM_MIN = 2
NGRAM_MAX = 5


def _ngrams(tokens):
    grams = []
    for n in range(NGRAM_MIN, NGRAM_MAX + 1):
        if n > len(tokens):
            break
        for i in range(len(tokens) - n + 1):
            grams.append(" ".join(tokens[i:i + n]))
    return grams


def mine_ngrams(sku_residues: dict, min_group_size: int = MIN_GROUP_SIZE, max_passes: int = MAX_PASSES) -> dict:
    """
    sku_residues: {sku_id: masked_residue_text}.
    Returns {sku_id: group_key} for every SKU that landed in a group of >= min_group_size
    (unassigned SKUs are simply absent from the result -- that's the "ungrouped" remainder
    Stage 1's acceptance criteria caps at 5%).

    Each pass mines n-gram candidates from whatever's still unassigned, assigns every SKU to
    its longest matching candidate (longest wins -> finer split, per spec), then re-mines the
    leftover residue up to max_passes times so a SKU that didn't cross the threshold in one
    grouping might still join a coarser group found once its finer competitors are removed.
    """
    assignments = {}
    remaining = dict(sku_residues)

    for _ in range(max_passes):
        if not remaining:
            break

        ngram_to_skus = collections.defaultdict(set)
        sku_grams = {}
        for sku_id, residue in remaining.items():
            tokens = residue.split()
            grams = _ngrams(tokens)
            sku_grams[sku_id] = grams
            for g in grams:
                ngram_to_skus[g].add(sku_id)

        candidates = {g for g, skus in ngram_to_skus.items() if len(skus) >= min_group_size}
        if not candidates:
            break

        newly_assigned = []
        for sku_id, grams in sku_grams.items():
            matching = [g for g in grams if g in candidates]
            if not matching:
                continue
            best = max(matching, key=lambda g: (len(g.split()), g))
            assignments[sku_id] = best
            newly_assigned.append(sku_id)

        if not newly_assigned:
            break
        for sku_id in newly_assigned:
            del remaining[sku_id]

    return assignments
