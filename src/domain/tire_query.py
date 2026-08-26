"""
Turn free text a shopper typed into structured tire filters.

Pure: text in, a ``ParsedQuery`` out. Brand names are passed in rather than looked up, so this
module needs no database and can be table-tested.

**This runs only when the request carries no filters.** Re-parsing ``q`` on a refinement is the
classic bug in this kind of search: the user removes the "Mud Terrain" chip, the server re-reads
"mud terrain" out of the query text they never edited, and the chip comes back. Once the client
has sent filters, the filters are the truth and this module is not called -- see
``src.api.services.tire_search``.

What gets extracted, in order:

  1. the size, via ``tire_size.parse_query`` -- the only thing that reliably collapses a result
     set from thousands to dozens
  2. tread category, from the words people actually type ("mud terrain", "m/t", "at")
  3. load range, severe-snow intent, and other single-token signals
  4. brand, matched against the names supplied by the caller
  5. whatever text is left becomes ``residue`` and is passed to Meilisearch as ``q``

Anything not recognised stays in the residue rather than being dropped, so a query is never
silently narrowed to something the user did not ask for.
"""
import dataclasses
import re
import typing

from src.domain import tire_size

# Phrases a shopper types -> tread_category code. Longest first at match time, so "all terrain"
# is not consumed by "all season" style partial overlaps and "m/t" beats a bare "mt".
TREAD_CATEGORY_SYNONYMS = {
    "mud terrain": "MT",
    "mud-terrain": "MT",
    "muddy terrain": "MT",
    "m/t": "MT",
    "mud": "MT",
    "muddy": "MT",
    "mudder": "MT",
    "mudders": "MT",
    "all terrain": "AT",
    "all terrains": "AT",
    "all-terrain": "AT",
    "all-terrains": "AT",
    "a/t": "AT",
    "rugged terrain": "RT",
    "rugged terrains": "RT",
    "rugged": "RT",
    "r/t": "RT",
    "hybrid terrain": "RT",
    "extreme terrain": "XT",
    "highway terrain": "HT",
    "highway": "HT",
    "h/t": "HT",
    "all season": "ALL_SEASON",
    "all seasons": "ALL_SEASON",
    "all-season": "ALL_SEASON",
    "all-seasons": "ALL_SEASON",
    "all weather": "ALL_WEATHER",
    "all-weather": "ALL_WEATHER",
    "summer": "SUMMER",
    "winter": "WINTER",
    "snow tire": "WINTER",
    "snow tires": "WINTER",
    "touring": "TOURING",
    "performance": "PERFORMANCE",
    "ultra high performance": "UHP",
    "uhp": "UHP",
    "track": "TRACK",
    "drag": "TRACK",
    "racing": "TRACK",
    "competition": "TRACK",
    "trailer": "TRAILER",
    "commercial": "COMMERCIAL",
    "spare": "SPARE",
    "vintage": "VINTAGE",
    "paddle": "SAND",
    "sand": "SAND",
}

# Severe-snow intent. Only ever filters for ``true`` -- the index omits the field when unknown, so
# there is no "not certified" set worth filtering to.
_SEVERE_SNOW_PHRASES = ("3pmsf", "three peak", "three-peak", "severe snow", "snowflake")

_RUN_FLAT_PHRASES = ("run flat", "run-flat", "runflat")

# "load range e", "lre", "lr e". A bare "e" is deliberately not matched: it is a letter, and
# guessing that it means a load range would wreck ordinary text queries.
_LOAD_RANGE_RE = re.compile(r"\b(?:load\s*range\s*|lr\s*)([a-n]|sl|xl|rf)\b", re.IGNORECASE)

# "10 ply", "10-ply"
_PLY_RE = re.compile(r"\b(\d{1,2})\s*-?\s*ply\b", re.IGNORECASE)


@dataclasses.dataclass
class ParsedQuery:
    """
    The result of reading a query. ``filters`` goes to the filter builder, ``residue`` becomes
    Meilisearch's ``q``, and ``matched`` records what was understood so the API can tell the user
    how their query was read.
    """

    filters: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)
    residue: str = ""
    size: typing.Optional[tire_size.ParsedSize] = None
    matched: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)

    @property
    def parsed_anything(self) -> bool:
        return bool(self.filters)


def size_filters(size: tire_size.ParsedSize) -> typing.Dict[str, typing.Any]:
    """
    The filter set that identifies one size.

    Metric and flotation are filtered on different fields because they describe the tire
    differently: metric states a section width in millimetres and an aspect ratio, flotation states
    an overall diameter and a width in inches. ``overall_diameter_in`` is the value they share, and
    it is what a cross-notation search would join on -- but an exact query for a metric size should
    not silently pull in flotation sizes, so it is not used here.

    ``service_type`` is deliberately **not** filtered on. ``LT275/70R18`` and ``275/70R18`` are
    different tires, but a shopper typing the plain form usually wants to see both, and the
    distributor titles disagree about the prefix constantly (see ``tire_size.disagreements``).
    """
    filters: typing.Dict[str, typing.Any] = {}
    if size.notation == tire_size.NOTATION_METRIC:
        if size.section_width_mm is not None:
            filters["section_width_mm"] = size.section_width_mm
        # An assumed aspect ratio is a guess (a bare "275R18"); filtering on it would exclude
        # exactly the tires the shopper is most likely after.
        if size.aspect_ratio is not None and not size.aspect_assumed:
            filters["aspect_ratio"] = size.aspect_ratio
        filters["rim_diameter_in"] = float(size.rim_diameter_in)
    else:
        if size.section_width_in is not None:
            filters["section_width_in"] = float(size.section_width_in)
        filters["overall_diameter_in"] = float(size.overall_diameter_in)
        filters["rim_diameter_in"] = float(size.rim_diameter_in)
    return filters


def _take_phrase(text: str, phrase: str) -> typing.Tuple[bool, str]:
    """Remove ``phrase`` from ``text`` if present at a word boundary. Returns (found, remainder)."""
    pattern = re.compile(r"(?<!\w){}(?!\w)".format(re.escape(phrase)), re.IGNORECASE)
    if not pattern.search(text):
        return False, text
    return True, pattern.sub(" ", text, count=1)


def _find_size(text: str) -> typing.Optional[tire_size.ParsedSize]:
    """
    Find a size anywhere in the query.

    ``tire_size.parse_query`` anchors its loose forms to the whole string on purpose -- that is
    what stops "2757018" being read out of the middle of a part number in catalog text. A search
    box is different: "2757018 nitto" is two tokens and the first one is a size. So the whole
    string is tried first (which also catches the strict, embedded forms), then each token on its
    own, with the span rebased onto the original text so residue extraction still works.
    """
    whole = tire_size.parse_query(text)
    if whole is not None:
        return whole

    # Token windows, widest first. "275 70 18" is a three-token size and "35 12.50 20" is too,
    # so a single-token scan would miss both; widest-first stops "275 70 18" being read as the
    # two-token "275 70" plus a stray 18.
    tokens = text.split(" ")
    starts, offset = [], 0
    for token in tokens:
        starts.append(offset)
        offset += len(token) + 1

    for width in (3, 2, 1):
        for index in range(len(tokens) - width + 1):
            window = tokens[index : index + width]
            if not any(window):
                continue
            candidate = " ".join(window)
            parsed = tire_size.parse_query(candidate)
            if parsed is not None:
                start = starts[index]
                return dataclasses.replace(parsed, span=(start, start + len(candidate)))
    return None


# A user-typed size missing its rim diameter -- "275/55", "225-45", "205 60". This does not
# identify one exact wheel size the way a full "275/55R18" does, but section width + aspect
# ratio alone is still a real, useful filter (cross-shop every rim diameter available at this
# width/aspect) -- confirmed live: "275/55" previously fell through _find_size entirely and
# went to Meilisearch as plain text, where coincidental digit-substring matches in the much
# larger parts index outscored real tire hits. Exactly 3-digit width + 2-digit aspect, the same
# fixed-digit-count discipline every other loose pattern in this module uses to avoid reading a
# part number or a wheel bolt pattern as a size. Deliberately does not build a tire_size.ParsedSize
# (which requires rim_diameter_in and overall_diameter_in) -- this is a query-only, rim-less
# filter pair, never reused by catalog parsing.
_PARTIAL_SIZE_RE = re.compile(r"(?<!\w)(\d{3})\s*[/ -]\s*(\d{2})(?!\w)")


def _find_partial_size(
    text: str,
) -> typing.Optional[typing.Tuple[typing.Dict[str, typing.Any], str, typing.Tuple[int, int]]]:
    """Last resort, tried only after ``_find_size`` (a full size, with rim) has already failed.
    Returns (filters, display, span), or ``None``."""
    match = _PARTIAL_SIZE_RE.search(text)
    if match is None:
        return None
    width_mm = int(match.group(1))
    aspect = int(match.group(2))
    if not tire_size._MIN_SECTION_MM <= width_mm <= tire_size._MAX_SECTION_MM:
        return None
    if not tire_size._MIN_ASPECT <= aspect <= tire_size._MAX_ASPECT:
        return None
    return {"section_width_mm": width_mm, "aspect_ratio": aspect}, "{}/{}".format(width_mm, aspect), match.span()


def parse_query(
    text: typing.Optional[str],
    *,
    brand_names: typing.Optional[typing.AbstractSet[str]] = None,
    valid_categories: typing.Optional[typing.AbstractSet[str]] = None,
) -> ParsedQuery:
    """
    Read ``text`` into filters plus a residue.

    ``brand_names`` and ``valid_categories`` come from the caller (the API service loads them at
    startup) so this stays pure and testable.
    """
    result = ParsedQuery()
    if not text or not text.strip():
        return result

    remaining = text

    size = _find_size(remaining)
    if size is not None:
        result.size = size
        result.filters.update(size_filters(size))
        result.matched["size"] = size.size_display
        remaining = tire_size.residue(remaining, size)
        if size.load_range:
            result.filters["load_range"] = size.load_range
            result.matched["load_range"] = size.load_range
        if size.speed_rating:
            result.matched["speed_rating"] = size.speed_rating
    else:
        partial = _find_partial_size(remaining)
        if partial is not None:
            partial_filters, display, span = partial
            result.filters.update(partial_filters)
            result.matched["size"] = display
            start, end = span
            remaining = " ".join((remaining[:start] + " " + remaining[end:]).split())

    # Severe-snow intent runs before tread category on purpose: "severe snow tires" shares the
    # word "snow" with the WINTER synonym "snow tires", and the tread-category loop below would
    # otherwise consume it first, leaving "severe" stranded with no "snow" left to pair with --
    # confirmed by a real regression when "snow tires" was added as a WINTER synonym (see below).
    for phrase in _SEVERE_SNOW_PHRASES:
        found, remaining = _take_phrase(remaining, phrase)
        if found:
            result.filters["is_3pmsf"] = True
            result.matched["is_3pmsf"] = True
            break

    # Longest phrase first: "all terrain" must win over any shorter substring it contains.
    for phrase in sorted(TREAD_CATEGORY_SYNONYMS, key=len, reverse=True):
        code = TREAD_CATEGORY_SYNONYMS[phrase]
        if valid_categories is not None and code not in valid_categories:
            continue
        found, remaining = _take_phrase(remaining, phrase)
        if found:
            result.filters["tread_category"] = code
            result.matched["tread_category"] = code
            break

    for phrase in _RUN_FLAT_PHRASES:
        found, remaining = _take_phrase(remaining, phrase)
        if found:
            result.filters["is_run_flat"] = True
            result.matched["is_run_flat"] = True
            break

    match = _LOAD_RANGE_RE.search(remaining)
    if match:
        # Consume the text even if the size parser already supplied the value from the service
        # description -- otherwise "load range E" survives into the residue and gets text-searched
        # against model names, which matches nothing and looks like a broken query.
        result.filters.setdefault("load_range", match.group(1).upper())
        result.matched["load_range"] = result.filters["load_range"]
        remaining = _LOAD_RANGE_RE.sub(" ", remaining, count=1)

    match = _PLY_RE.search(remaining)
    if match:
        result.filters["ply_rating"] = int(match.group(1))
        result.matched["ply_rating"] = int(match.group(1))
        remaining = _PLY_RE.sub(" ", remaining, count=1)

    if brand_names:
        # Longest brand first so "Mickey Thompson" is not shadowed by a brand called "Mickey".
        for brand in sorted(brand_names, key=len, reverse=True):
            if len(brand) < 3:
                continue
            found, candidate = _take_phrase(remaining, brand)
            if found:
                result.filters["brand_name"] = brand
                result.matched["brand"] = brand
                remaining = candidate
                break

    result.residue = " ".join(remaining.split())
    return result


# Relaxation order when a parsed query returns nothing. Dropped left to right, one at a time.
#
# The dimensional fields are absent on purpose and must stay absent: if someone asked for a
# 275/70R18 they do not want a 265/65R17, and returning one is worse than returning nothing --
# it looks like a match and fits nothing.
RELAXATION_ORDER = (
    "use_case_tags",
    "ply_rating",
    "is_run_flat",
    "is_3pmsf",
    "brand_name",
    "tread_category",
    "load_range",
)

NEVER_RELAX = frozenset(
    ["section_width_mm", "aspect_ratio", "rim_diameter_in", "section_width_in", "overall_diameter_in", "service_type"]
)


def relax(filters: typing.Mapping[str, typing.Any]) -> typing.Optional[typing.Tuple[str, typing.Dict[str, typing.Any]]]:
    """
    Drop the least important filter still present. Returns ``(dropped_field, new_filters)``, or
    ``None`` when only un-relaxable filters remain.
    """
    for field in RELAXATION_ORDER:
        if field in filters and field not in NEVER_RELAX:
            reduced = dict(filters)
            reduced.pop(field)
            return field, reduced
    return None
