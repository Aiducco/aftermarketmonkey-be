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


# Single-word makes/models that are also ordinary English words used constantly in parts
# terminology -- see VcdbLookup.__init__ for how these get excluded. Found by a data-driven
# audit (not one-at-a-time guessing): counted how often every single-token VCdb make/model
# appears in a 150K-title sample across ALL brands, not just the ones manually reviewed --
# these are the ones with damaging hit counts (900-9800+ hits each) that are legitimate but
# obscure vehicle models (Lucid Air, Nissan Leaf, Siata Spring, Honda Fit, BYD Seal, Chevrolet/
# Dodge "Truck", Buick Super, Chevrolet Universal, Volkswagen Panel, American Motors/Chrysler
# Custom, Alfa Romeo/Polaris Sport, the make "Standard" itself) crowded out by the ordinary-
# English-word sense in real parts titles (e.g. "LEAF SPRING" masked to '' before this fix --
# both words independently ate by a vehicle-model match).
_COLLISION_STOPLIST = {
    ("AIR",), ("SEAT",), ("CUSTOM",), ("LEAF",), ("SPRING",), ("FIT",), ("TRUCK",),
    ("SPORT",), ("SUPER",), ("UNIVERSAL",), ("SEAL",), ("STANDARD",), ("PANEL",),
}


class VcdbLookup:
    """
    Set-based, not regex-based: with ~400 makes and ~17K models, compiling/searching a
    per-value regex for every title (as the naive "for name in vcdb.makes: regex search"
    approach would) does not finish at millions-of-titles scale. Phrases are pre-tokenized
    once here; _strip_make_model does an O(title length) sliding-window set lookup instead.
    """
    def __init__(self, makes_and_models, trims=()):
        """makes_and_models: iterable of (make, model) string pairs, e.g. from
        VcdbVehicle.objects.values_list('make', 'model').distinct().
        trims: iterable of submodel/trim strings, e.g. VcdbVehicle.values_list('submodel',
        flat=True).distinct() -- catches leaks like "Si", "Denali", "M Sport" that a bare
        make/model check misses (these are real, observed on WeatherTech titles)."""
        make_tuples = set()
        model_tuples = set()
        for make, model in makes_and_models:
            if make:
                make_tuples.add(_phrase_tokens(make))
            if model:
                model_tuples.add(_phrase_tokens(model))
        make_tuples.discard(())
        model_tuples.discard(())
        # A handful of single-word makes/models are also ordinary, high-frequency English words
        # in parts terminology -- stripping them does more damage than the rare title that
        # actually means the vehicle. Confirmed on real data: "AIR" is Lucid's model name
        # ("Lucid Air"), which silently ate the word "air" out of every "AIR SPRING"/"AIR
        # FILTER"/etc title for every OTHER brand too, since VcdbLookup is brand-agnostic.
        # "SEAT" (the Spanish make) and "CUSTOM" (a real model on some makes) are the same
        # class of collision, lower-damage but real -- found reviewing Covercraft data.
        make_tuples -= _COLLISION_STOPLIST
        model_tuples -= _COLLISION_STOPLIST

        # Short trims ("Si", "SE", "LX", "S") are too collision-prone against ordinary English
        # words to strip anywhere in a title -- _strip_make_model only ever tries a trim match
        # in the token(s) immediately after a matched make/model span, never standalone, so the
        # false-positive risk here is much lower than it would be for a global word match.
        trim_tuples = {_phrase_tokens(t) for t in trims if t}
        trim_tuples.discard(())

        self.make_phrases = make_tuples
        self.model_phrases = model_tuples
        self.trim_phrases = trim_tuples
        self.max_make_len = max((len(t) for t in make_tuples), default=1)
        self.max_model_len = max((len(t) for t in model_tuples), default=1)
        self.max_trim_len = max((len(t) for t in trim_tuples), default=1)

    @classmethod
    def from_rows(cls, rows, trims=()):
        """rows: iterable of (make, model) tuples. trims: optional iterable of submodel strings."""
        return cls(rows, trims=trims)


_COLOURS = {
    "black", "white", "gray", "grey", "charcoal", "tan", "beige", "brown", "red", "blue",
    "green", "yellow", "orange", "purple", "silver", "chrome", "gold", "bronze", "copper",
    "ivory", "cream", "navy", "maroon", "burgundy", "khaki", "olive", "graphite", "gunmetal",
    "titanium", "platinum", "pewter", "slate", "cinder", "gravel", "camo", "camouflage",
    # Finish/tint words -- these behave exactly like a colour (pure cosmetic variance) even
    # though they're not colours in the strict sense. Found reviewing WeatherTech data: "Cocoa",
    "cocoa", "mocha", "stone", "sand", "mahogany", "walnut", "espresso",
    "matte", "blk", "wht", "gry", "transparent", "clear", "smoke", "tint", "tinted", "dark",
}

_PACKAGING_WORDS = {
    "each", "ea", "pair", "pairs", "pc", "pcs", "piece", "pieces", "kit", "kits",
    # Pure title-template connector, no product-line meaning -- e.g. WeatherTech's bundle SKU
    # titles ("...Floor Mats Fits -20-21 Civic") leave a dangling "Fits" once the vehicle info
    # after it is stripped by make/model/year masking. Found reviewing real WeatherTech data;
    # was previously a major driver of head-noun candidate noise ("floorliner fits", etc).
    "fits", "fit", "for",
}
# "kit"/"kits" are listed for completeness but NOT stripped here -- Stage 3 explicitly keeps
# kit/set undropped because they're discriminative ("Suspension Lift Kit" vs "...Bracket Kit").
# mask_title only strips true packaging noise, not part-type words that happen to look similar.
_PACKAGING_STRIP_WORDS = _PACKAGING_WORDS - {"kit", "kits"}

_SET_OF_RE = re.compile(r"\bset of \d+\b", re.IGNORECASE)
_YEAR_4_RE = re.compile(r"\b(19|20)\d{2}\b")
_YEAR_RANGE_RE = re.compile(r"\b\d{2}-\d{2}\+?\b")
# No trailing \b: "+" and a following space/end-of-string are both non-word characters, so \b
# never matches between them -- this pattern previously never fired at all (real bug, found
# reviewing WeatherTech data: "11+ Front FloorLiner" left "11+" in the residue verbatim).
_YEAR_OPEN_RE = re.compile(r"\b\d{2}\+")
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
# "w/" or standalone "w" meaning "with" -- e.g. "Cargo Liner w/ Bumper Protector". Replacement
# is "with " (trailing space) unconditionally -- some titles write "w/Bumper" with no space at
# all, and substituting bare "with" there glues it straight onto the next word ("withBumper").
# Final whitespace normalization collapses the occasional resulting double space.
_CONNECTOR_RE = re.compile(r"\bw/|\bw\b(?=\s)", re.IGNORECASE)


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
    max_trim_window = max(vcdb.max_trim_len, 1)

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
                # Try to also consume a trim/submodel immediately following the make/model
                # match (e.g. "Ford Ranger Denali" -> Denali here) -- short trims like "Si" are
                # only ever attempted in this position, never as a standalone scan over the
                # whole title, to keep the false-positive rate against ordinary words low.
                for tw in range(min(max_trim_window, n - i), 0, -1):
                    trim_window = tuple(tok.upper() for tok in tokens[i:i + tw])
                    if trim_window in vcdb.trim_phrases:
                        removed.append(" ".join(tokens[i:i + tw]))
                        i += tw
                        break
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
    """Whitespace-split tokens only, but a token like "Silver/Black" (no space) is itself
    checked as a slash/hyphen-joined compound of colour words -- drop the whole token only if
    EVERY part is a recognized colour, so e.g. "W/Bumper" (a real word, not a colour) survives."""
    removed = []
    tokens = text.split()
    kept = []
    for tok in tokens:
        bare = re.sub(r"[^A-Za-z]", "", tok).lower()
        if bare in _COLOURS:
            removed.append(tok)
            continue
        parts = [p for p in re.split(r"[/\-]", tok) if p]
        if parts and all(re.sub(r"[^A-Za-z]", "", p).lower() in _COLOURS for p in parts):
            removed.append(tok)
            continue
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

    # Normalize connector-word variants BEFORE anything else, so "Cargo Liner w/ Bumper
    # Protector", "...w Bumper Protector", and "...with Bumper Protector" all collapse to the
    # same residue instead of three distinct ones (found reviewing WeatherTech data -- this was
    # a bigger source of residue fragmentation than any single token-class gap).
    text = _CONNECTOR_RE.sub("with ", text)

    text, removed["brand"] = _strip_brand(text, brand_name, brand_aliases)
    text, removed["years"] = _strip_years_clean(text)
    text, removed["make_model"] = _strip_make_model(text, vcdb)
    text, removed["dimensions"] = _strip_dimensions(text)
    text, removed["colours"] = _strip_colours(text)
    text, removed["part_numbers"] = _strip_part_numbers(text)
    text, removed["packaging"] = _strip_packaging(text)

    residue = _normalize(text)
    return MaskedTitle(residue=residue, removed=removed)
