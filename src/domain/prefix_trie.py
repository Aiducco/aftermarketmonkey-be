"""
Stage 1c: Method B -- group SKUs within a brand by part-number prefix, cutting where the trie's
branching factor spikes (the boundary between a shared line code and a per-fitment suffix, e.g.
"XLTBM" | "Y24DCS"). Pure function of {sku_id: part_number}; no Django imports.
"""
import collections
import re

MIN_GROUP_SIZE = 5
MIN_PREFIX_LEN = 3
# How many distinct next-characters counts as "branching" enough to mark the line-code/fitment-
# code boundary. Not spec-given as a number -- 3 is a reasonable starting point (2 is too easily
# triggered by ordinary part-number variety; spec gives no exact value, so this is a tunable
# constant like MIN_GROUP_SIZE, not a derived one).
BRANCH_SPIKE_THRESHOLD = 3

_NON_ALNUM_RE = re.compile(r"[^A-Z0-9]")


def normalize_part_number(part_number: str) -> str:
    return _NON_ALNUM_RE.sub("", (part_number or "").upper())


class _TrieNode:
    __slots__ = ("children",)

    def __init__(self):
        self.children = {}


def _build_trie(part_numbers):
    root = _TrieNode()
    for pn in part_numbers:
        node = root
        for ch in pn:
            node = node.children.setdefault(ch, _TrieNode())
    return root


def _cut_prefix(root, pn, min_prefix_len, branch_spike_threshold):
    node = root
    for depth, ch in enumerate(pn):
        child = node.children.get(ch)
        if child is None:
            return pn[:depth]
        branch = len(child.children)
        if depth + 1 >= min_prefix_len and branch >= branch_spike_threshold:
            return pn[:depth + 1]
        node = child
    return pn


def mine_prefixes(
    sku_part_numbers: dict,
    min_group_size: int = MIN_GROUP_SIZE,
    min_prefix_len: int = MIN_PREFIX_LEN,
    branch_spike_threshold: int = BRANCH_SPIKE_THRESHOLD,
) -> dict:
    """
    sku_part_numbers: {sku_id: raw_part_number}.
    Returns {sku_id: group_key} where group_key is the cut prefix, for every SKU whose prefix
    ends up shared by >= min_group_size SKUs (unassigned SKUs absent from the result).
    """
    normalized = {sku_id: normalize_part_number(pn) for sku_id, pn in sku_part_numbers.items()}
    normalized = {sku_id: pn for sku_id, pn in normalized.items() if pn}
    if not normalized:
        return {}

    root = _build_trie(normalized.values())

    sku_prefix = {}
    for sku_id, pn in normalized.items():
        prefix = _cut_prefix(root, pn, min_prefix_len, branch_spike_threshold)
        if len(prefix) >= min_prefix_len:
            sku_prefix[sku_id] = prefix

    prefix_counts = collections.Counter(sku_prefix.values())
    return {
        sku_id: prefix
        for sku_id, prefix in sku_prefix.items()
        if prefix_counts[prefix] >= min_group_size
    }
