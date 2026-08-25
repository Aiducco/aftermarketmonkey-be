"""
Stage C: derive head-noun candidates from masked residues. Pure, deterministic, no model --
this is the input Stage D's LLM call reasons over, not the final answer. See the pipeline
review notes: this pre-aggregation step is what makes Stage D's job tractable (a dense, ranked
~30-80 candidate list instead of thousands of raw residue strings).
"""
import re

MIN_CANDIDATE_LEN = 1
MAX_CANDIDATE_LEN = 4
DEFAULT_FLOOR_RATIO = 0.005  # 0.5% of brand SKUs

# Variant/trim-code style trailing tokens -- literal known suffixes, not colours (those are
# already handled by title_mask's own colour list, but a few slip through here too since this
# runs on residues that may still carry leftover noise).
_TRAILING_VARIANT_TOKENS = {
    "hp", "sk", "im", "matte", "vinyl", "rubber",
}
_TRAILING_DIGIT_RE = re.compile(r"^\d+$")


def _strip_trailing_attributes(tokens: list) -> list:
    """Repeatedly drop a trailing token if it's a known variant code or pure digits -- these
    describe a SKU variant, not the product itself, so the head noun lives before them."""
    tokens = list(tokens)
    while tokens:
        last = tokens[-1].lower()
        if last in _TRAILING_VARIANT_TOKENS or _TRAILING_DIGIT_RE.match(last):
            tokens.pop()
            continue
        break
    return tokens


def derive_head_candidates(residue_counts: dict, floor_ratio: float = DEFAULT_FLOOR_RATIO) -> list:
    """
    residue_counts: {residue: sku_count} for one brand (as produced by masking + counting).
    Returns [(candidate, weight), ...] sorted longest-candidate-first, weight descending within
    a length tier -- specificity beats generality when this feeds Stage D / Stage E matching.

    A candidate's weight is the SUM of sku_count across every residue where it appears as a
    trailing 1-4 token window (after stripping trailing variant codes/digits) -- so "floorliner"
    accumulates weight from "front floorliner", "rear floorliner", "front floorliner hp", etc.
    """
    total_skus = sum(residue_counts.values())
    if total_skus == 0:
        return []
    floor = total_skus * floor_ratio

    weights = {}
    for residue, count in residue_counts.items():
        tokens = _strip_trailing_attributes(residue.split())
        if not tokens:
            continue
        for n in range(MIN_CANDIDATE_LEN, min(MAX_CANDIDATE_LEN, len(tokens)) + 1):
            candidate = " ".join(tokens[-n:])
            weights[candidate] = weights.get(candidate, 0) + count

    kept = [(c, w) for c, w in weights.items() if w >= floor]
    kept.sort(key=lambda cw: (-len(cw[0].split()), -cw[1]))
    return kept
