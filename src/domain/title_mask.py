"""
Stage 1a of the part-classification pipeline: strip variable tokens (brand, year, make/model,
dimensions, colours, part numbers, packaging) from a raw product title, leaving the descriptive
residue that Stage 1b/1c mine for product-line groups.

Deliberately free of Django imports -- like azure_llm.py, this needs to stay importable from
standalone scripts and from a fast in-memory VcdbLookup that's built once from already-fetched
rows, not re-queried per title. See product_grouping.py for the Django-facing orchestration.
"""
import re
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# VCdb lookup -- "you have the table, do not regex this": match make/model by exact vocabulary,
# not pattern-guessing. Built once from VcdbVehicle rows, reused across every title.
# ---------------------------------------------------------------------------

_TOKEN_SPLIT_RE = re.compile(r"[\s\-]+")


def _phrase_tokens(phrase: str) -> tuple:
    """Splits on whitespace AND hyphens so "Mercedes-Benz"/"Mercedes Benz" and "2-Series"/
    "2 Series" match the same way regardless of which the title or VCdb happens to use."""
    return tuple(t for t in _TOKEN_SPLIT_RE.split(phrase.upper()) if t)


class VcdbLookup:
    """
    Set-based, not regex-based: with ~400 makes and ~17K models, compiling/searching a
    per-value regex for every title (as the naive "for name in vcdb.makes: regex search"
    approach would) does not finish at millions-of-titles scale. Phrases are pre-tokenized
    once here; _strip_make_model does an O(title length) sliding-window set lookup instead.
    """
    def __init__(self, makes_and_models):
        """makes_and_models: iterable of (make, model) string pairs, e.g. from
        VcdbVehicle.objects.values_list('make', 'model').distinct()."""
        make_tuples = set()
        model_tuples = set()
        for make, model in makes_and_models:
            if make:
                make_tuples.add(_phrase_tokens(make))
            if model:
                model_tuples.add(_phrase_tokens(model))
        make_tuples.discard(())
        model_tuples.discard(())

        self.make_phrases = make_tuples
        self.model_phrases = model_tuples
        self.max_make_len = max((len(t) for t in make_tuples), default=1)
        self.max_model_len = max((len(t) for t in model_tuples), default=1)

    @classmethod
    def from_rows(cls, rows):
        """rows: iterable of (make, model) tuples."""
        return cls(rows)


_COLOURS = {
    "black", "white", "gray", "grey", "charcoal", "tan", "beige", "brown", "red", "blue",
    "green", "yellow", "orange", "purple", "silver", "chrome", "gold", "bronze", "copper",
    "ivory", "cream", "navy", "maroon", "burgundy", "khaki", "olive", "graphite", "gunmetal",
    "titanium", "platinum", "pewter", "slate", "cinder", "gravel", "camo", "camouflage",
}

_PACKAGING_WORDS = {"each", "pair", "pairs", "pc", "pcs", "piece", "pieces", "kit", "kits"}
# "kit"/"kits" are listed for completeness but NOT stripped here -- Stage 3 explicitly keeps
# kit/set undropped because they're discriminative ("Suspension Lift Kit" vs "...Bracket Kit").
# mask_title only strips true packaging noise, not part-type words that happen to look similar.
_PACKAGING_STRIP_WORDS = _PACKAGING_WORDS - {"kit", "kits"}

_SET_OF_RE = re.compile(r"\bset of \d+\b", re.IGNORECASE)
_YEAR_4_RE = re.compile(r"\b(19|20)\d{2}\b")
_YEAR_RANGE_RE = re.compile(r"\b\d{2}-\d{2}\+?\b")
_YEAR_OPEN_RE = re.compile(r"\b\d{2}\+\b")
# Sherman-style concatenated 2-digit start+end with no separator ("0507" = 2005-2007, "9901" =
# 1999-2001 wrapping the century). Not in the original token-class table but real, common data
# from a 51K-SKU brand -- see title_mask review notes. Guarded to plausible model-year decades
# (00-29 or 60-99) on both halves so it doesn't eat ordinary 4-digit part-number fragments.
_YEAR_CONCAT_RE = re.compile(r"\b([0-2]\d|[6-9]\d)([0-2]\d|[6-9]\d)\b")
_DIMENSION_UNIT_RE = re.compile(r'\b\d+(\.\d+)?\s*(in|ft|mm|cm)\b\.?', re.IGNORECASE)
_DIMENSION_QUOTE_RE = re.compile(r'\b\d+(\.\d+)?"')
_BARE_DECIMAL_RE = re.compile(r"\b\d+\.\d+\b")
# "Tokens with >= 2 digits mixed with letters" -- a part-number-shaped token, not a bare number.
_PART_NUMBER_TOKEN_RE = re.compile(r"\b(?=[A-Za-z0-9/-]*[A-Za-z])(?=(?:[A-Za-z0-9/-]*\d){2,})[A-Za-z0-9/-]+\b")

_WHITESPACE_RE = re.compile(r"\s+")


@dataclass
class MaskedTitle:
    residue: str
    removed: dict = field(default_factory=dict)


def _strip_brand(text, brand_name, aliases=()):
    removed = []
    for name in [brand_name, *aliases]:
        if not name:
            continue
        pattern = re.compile(r"\b" + re.escape(name) + r"\b", re.IGNORECASE)
        text, n = pattern.subn(" ", text)
        if n:
            removed.append(name)
    return text, removed


def _strip_years_clean(text):
    """Strip year tokens; returns (text, removed_list). Order matters: ranges/open-ranges
    before bare 4-digit years so "2010-2015" doesn't get half-eaten by the 4-digit pattern
    leaving a dangling hyphen, and before the concatenated-pair heuristic so real hyphenated
    ranges are never mis-read as concatenated pairs."""
    removed = []
    for pattern in (_YEAR_RANGE_RE, _YEAR_OPEN_RE, _YEAR_4_RE, _YEAR_CONCAT_RE):
        text, n = pattern.subn(" ", text)
        if n:
            removed.append(pattern.pattern)
    return text, removed


def _strip_make_model(text, vcdb: VcdbLookup):
    """Tokenizes on whitespace/hyphens (so "Mercedes-Benz" and "Mercedes Benz" both match a
    VCdb phrase stored either way) and does a greedy longest-window set lookup at each
    position -- O(title length x max phrase length), not O(vcdb size), so this stays fast
    across millions of titles. Note: this necessarily also drops any hyphen used as a plain
    separator elsewhere in the title (e.g. inside a part number) since tokenization happens
    once for the whole string; that's fine here since later steps re-check each surviving
    fragment independently, not as a single hyphenated unit."""
    tokens = [t for t in _TOKEN_SPLIT_RE.split(text) if t]
    max_window = max(vcdb.max_make_len, vcdb.max_model_len, 1)

    removed = []
    kept = []
    i = 0
    n = len(tokens)
    while i < n:
        matched = False
        for w in range(min(max_window, n - i), 0, -1):
            window = tuple(tok.upper() for tok in tokens[i:i + w])
            if window in vcdb.make_phrases or window in vcdb.model_phrases:
                removed.append(" ".join(tokens[i:i + w]))
                i += w
                matched = True
                break
        if not matched:
            kept.append(tokens[i])
            i += 1

    return " ".join(kept), removed


def _strip_dimensions(text):
    removed = []
    for pattern in (_DIMENSION_UNIT_RE, _DIMENSION_QUOTE_RE, _BARE_DECIMAL_RE):
        text, n = pattern.subn(" ", text)
        if n:
            removed.append(pattern.pattern)
    return text, removed


def _strip_colours(text):
    removed = []
    tokens = text.split()
    kept = []
    for tok in tokens:
        bare = re.sub(r"[^A-Za-z]", "", tok).lower()
        if bare in _COLOURS:
            removed.append(tok)
        else:
            kept.append(tok)
    return " ".join(kept), removed


def _strip_part_numbers(text):
    found = _PART_NUMBER_TOKEN_RE.findall(text)
    text, n = _PART_NUMBER_TOKEN_RE.subn(" ", text)
    return text, (found or [])


def _strip_packaging(text):
    removed = []
    text, n = _SET_OF_RE.subn(" ", text)
    if n:
        removed.append("set of N")
    tokens = text.split()
    kept = []
    for tok in tokens:
        bare = re.sub(r"[^A-Za-z]", "", tok).lower()
        if bare in _PACKAGING_STRIP_WORDS:
            removed.append(tok)
        else:
            kept.append(tok)
    return " ".join(kept), removed


def _normalize(text):
    text = _WHITESPACE_RE.sub(" ", text).strip().lower()
    tokens = text.split()
    cleaned = []
    for tok in tokens:
        # Strip leading/trailing punctuation left over from masking (e.g. "w/" -> "w"); drop
        # tokens that are pure punctuation with nothing alphanumeric left (e.g. a lone "-").
        tok = tok.strip(".,/;:!?()[]{}\"'`~*&%$#@")
        if not tok or not any(c.isalnum() for c in tok):
            continue
        if len(tok) > 4 and tok.endswith("s") and not tok.endswith("ss"):
            tok = tok[:-1]
        cleaned.append(tok)
    return " ".join(cleaned)


def mask_title(title: str, brand_name: str, vcdb: VcdbLookup, brand_aliases=()) -> MaskedTitle:
    """Strip variable tokens, returning the descriptive residue. Order: brand, years, make/model
    (VCdb), dimensions, colours, part numbers, packaging -- matching Stage 1a's token-class
    table exactly, since later steps assume earlier noise (e.g. a colour word that's also a
    valid part-number-shaped token) is already gone."""
    if not title:
        return MaskedTitle(residue="", removed={})

    removed = {}
    text = title

    text, removed["brand"] = _strip_brand(text, brand_name, brand_aliases)
    text, removed["years"] = _strip_years_clean(text)
    text, removed["make_model"] = _strip_make_model(text, vcdb)
    text, removed["dimensions"] = _strip_dimensions(text)
    text, removed["colours"] = _strip_colours(text)
    text, removed["part_numbers"] = _strip_part_numbers(text)
    text, removed["packaging"] = _strip_packaging(text)

    residue = _normalize(text)
    return MaskedTitle(residue=residue, removed=removed)
