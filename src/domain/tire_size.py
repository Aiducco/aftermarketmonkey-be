"""
Deterministic parser for the tire size string stamped on a sidewall.

**This module is the source of truth for size.** The LLM enrichment pass in
``src.integrations.services.tire_enrichment`` is explicitly forbidden from returning any
dimension (see its system prompt, rule 5) -- section width, aspect ratio, rim diameter, load
index, speed rating and load range all come from here, because they are encoded in the string
and arithmetic beats recall. The division of labour is: this module owns anything the sidewall
already says, the model owns anything that requires knowing the tire market (what "TER GRAP G3"
actually is). They overlap on nothing except a cross-check.

Pure: plain strings in, a ``ParsedSize`` out. No Django, no DB, no network -- so it can be
unit-tested on its own (``manage.py test src.domain``) and reused by the classification tiers in
``src.integrations.utils.product_type``.

Three notations are recognised, told apart by shape rather than by any distributor's say-so:

  ``metric``     ``LT275/70R18``, ``205/55-16``, ``275/40ZR20`` -- mm section width / aspect %
  ``flotation``  ``33X12.50R15LT``, ``31x10.50-15``            -- inch overall dia X width - rim
  ``numeric``    ``7.50-16``, ``9.00R20``                       -- inch section width - rim

Everything else returns ``None``. **Nothing here guesses**, matching the convention in
``src.integrations.utils.product_type``: a string we cannot decode is unparsed, never
approximated, because a wrong overall diameter is a wrong fitment result and nobody reports it
as a bug. Wheel sizes are the specific hazard -- ``20X10 5X127`` and ``17X9 -12`` look like
flotation sizes to a careless regex, so they are rejected explicitly and covered by tests.

Overall diameter is computed, not read, for ``metric`` (rim + two section heights) and taken
directly from the string for ``flotation`` where it is the first number. For ``numeric`` there
is no aspect ratio in the string at all; the pre-1965 convention of section height == section
width is applied, which makes that one value *nominal* -- accurate to a few tenths on passenger
sizes and worse on truck sizes. Check ``notation == "numeric"`` before presenting it as exact.
"""
import dataclasses
import decimal
import re
import typing

MM_PER_INCH = decimal.Decimal("25.4")

NOTATION_METRIC = "metric"
NOTATION_FLOTATION = "flotation"
NOTATION_NUMERIC = "numeric"

CONSTRUCTION_RADIAL = "R"
CONSTRUCTION_ZR = "ZR"
CONSTRUCTION_BIAS = "D"  # a bare hyphen on the sidewall: diagonal/bias ply
CONSTRUCTION_BELTED_BIAS = "B"

# Service type prefix. ``T`` is the temporary spare, which is why it cannot be dropped as noise.
SERVICE_TYPES = ("LT", "ST", "P", "T", "C")

# Speed symbols, mirroring the rows seeded by migration 0178. Held as a literal here rather than
# read from ``TireSpeedRating`` on purpose -- this module stays importable without Django. The
# enrichment service resolves the code against the table for max_speed_mph, so a code that got
# past this set but is missing from the table surfaces as a NULL mph rather than a bad parse.
# ZR and Z are deliberately absent: they are size markers, not ratings (see the model docstring).
SPEED_RATINGS = frozenset(["A1", "A2", "A3", "A4", "A5", "A6", "A7", "A8"] + list("BCDEFGJKLMNPQRSTUHVWY") + ["(Y)"])

# Load range / load designation, mirroring migration 0179. Letters here collide with speed
# symbols (C, D, E, F, G, H, J, L, M, N are in both), which is why load range is only ever read
# from a *standalone* token after the service description has already claimed the letter glued
# to the load index. Position disambiguates; a set lookup alone cannot.
# Mirrors ``load_range_ply`` plus its aliases, MINUS "RD". RD is a genuine XL stamping and stays
# in the lookup table for resolution, but as a token in a distributor title it is "Road"
# abbreviated: all 68 catalog matches were Kumho Road Venture ("KU RD VENTURE RT"), none a load
# range. LL is kept -- its 39 matches are all competition tires from three different brands
# ("HO DOT DRAG RAD2 LL", "GY EG F1 GS2 EMT LL"), and that cross-brand consistency is what marks
# it as a standard designation rather than one vendor's code.
LOAD_RANGES = frozenset(["SL", "XL", "LL", "RF", "REINFORCED"] + list("ABCDEFGHJLMN"))

# Physically plausible bounds. These are not style checks -- they are what stops a wheel offset
# or a bolt pattern from being read as a rim diameter. Every one of them fires on real catalog
# text, so widening one means re-checking the wheel-rejection tests.
_MIN_RIM_IN = decimal.Decimal(8)
_MAX_RIM_IN = decimal.Decimal(30)
_MIN_SECTION_MM = 100
_MAX_SECTION_MM = 500
_MIN_ASPECT = 15
_MAX_ASPECT = 100
# Minimum aspect ratio for a BIAS metric size. Low-profile tires are always radial -- a bias
# 19-series does not exist -- so a low aspect with a bare hyphen is a model-year range, not a
# size: "Westin 15-25 Ford F-150/19-24 RAM 1500" parsed as 150/19-24.
_MIN_BIAS_ASPECT = 30
# A tall sidewall only occurs on a narrow tire. Motorcycle sizes reach 100-series but at
# 100-140mm wide (120/100-18); the widest car and truck sizes stop around 85-series
# (235/85R16). Nothing is both 250mm wide and 99-series -- "Vertex Pistons 22-24 XX 250/99-24
# YZ 250" is a motocross model list, and 250/99-24 is not a tire.
_MAX_ASPECT_FOR_WIDE = 90
_WIDE_SECTION_MM = 150
_MIN_OVERALL_IN = decimal.Decimal(15)
_MAX_OVERALL_IN = decimal.Decimal(60)
_MIN_SECTION_IN = decimal.Decimal(4)
# A rim measured in half inches is always a commercial/trailer size (14.5 through 24.5). Below
# that a ".5" is something else entirely -- found in production on
# "Fork Springs - Prog. 4.5-10.5 N/mm", where 4.5-10.5 is a spring rate and parsed as a
# 4.5-inch tire on a 10.5-inch rim.
_MIN_HALF_INCH_RIM = decimal.Decimal("14.5")
_MAX_SECTION_IN = decimal.Decimal(25)

# ``33×12.50R18`` -- Wheel Pros ships the unicode multiplication sign in a minority of rows, and
# a plain ``x`` in the rest. Normalised before matching rather than alternated in every pattern.
_UNICODE_X = str.maketrans({"×": "x", "✕": "x", "✖": "x", "⁄": "/"})

_SERVICE_TYPE_ALT = "|".join(sorted(SERVICE_TYPES, key=len, reverse=True))

# Metric. The separator between width and aspect is normally ``/`` but Premier writes a space
# ("Nitto NT90W 275 45R19 103T"), so both are accepted -- the trailing R/ZR/- and a 2-digit rim
# are what keep "275 45" from matching arbitrary number pairs. Rim allows ``.5`` for the 16.5 /
# 19.5 / 22.5 commercial sizes.
_METRIC_RE = re.compile(
    # Unlike the inch notations below, a leading hyphen is allowed: metric carries its own
    # construction character *after* the aspect, so a hyphen in front is a separator rather than
    # part of the size ("MOTIVO 365-275/40R17"), and 3-digit width plus R/ZR keeps it specific.
    r"(?<![\w./+])" r"(?P<service>{service})?" r"(?P<width>\d{{3}})"
    # Exactly one separator character, with no whitespace around it. A real size is written
    # "275/70R18" or, from Premier, "275 45R19" -- never "275 / 45". Allowing spaces on both
    # sides let the pattern span a sentence: "Ford F-150 / 23-24 F-250" parsed as a 150/23-24.
    r"(?:/|\ )" r"(?P<aspect>\d{{2,3}})"
    # Wheel Pros and a handful of Premier rows write a second slash before the construction
    # letter ("305/45/r22"); everyone else writes it flush.
    # ``(?<!\s)-`` on the bias alternative: a hyphen with a space in front of it is an
    # offset ("20X9 -12MM", "4.56 -24"), not a construction marker. Letters may be spaced.
    r"\s*/?\s*(?P<construction>ZR|Z-|Z|R|B|D|(?<!\s)-)\s*"
    # Commercial rims appear both ways: "245/70R19.5" and, on 43 catalog rows, "225/70R195" with
    # the decimal dropped. Only 14.5 through 24.5 are accepted in the decimal-less form, so a
    # stray three-digit number cannot be read as a rim.
    r"(?P<rim>1[4-9]5|2[0-4]5|\d{{2}}(?:\.5)?)"
    r"(?P<trailing_service>LT|ST|C)?"
    r"(?P<trailing_marker>XL|SL|RF|TL)?"
    r"(?![\w.])".format(service=_SERVICE_TYPE_ALT),
    re.IGNORECASE,
)

# Flotation (inch). The construction character is mandatory: it is the only thing separating
# ``33X12.50R15`` from the wheel size ``20X10 5X127``, where a space and a bolt pattern follow
# instead. A bare ``33X12.50 15`` is left unparsed rather than assumed. The service type is
# accepted on either end -- Nitto writes ``33x11.50R16LT`` and Mickey Thompson writes
# ``LT33X12.50-20`` for the same shape of tire.
_FLOTATION_RE = re.compile(
    r"(?<![\w./+-])"
    r"(?P<service>{service})?"
    r"(?P<overall>\d{{2}}(?:\.\d+)?)"
    r"\s*[xX]\s*"
    r"(?P<width>\d{{1,2}}(?:\.\d+)?)"
    r"\s*(?P<construction>R|B|D|(?<!\s)-)\s*"
    r"(?P<rim>\d{{1,2}}(?:\.5)?)"
    r"(?P<trailing_service>LT|ST|C)?"
    r"(?P<trailing_marker>XL|SL|RF|TL)?"
    r"(?![\w.])".format(service=_SERVICE_TYPE_ALT),
    re.IGNORECASE,
)

# Drag-racing flotation, where the two inch figures are slash-separated instead of X-separated:
# ``33.0/14.5R15``, ``27.5/4.0-17``. Only reachable when the section width carries a decimal
# point, which is what keeps ``275/45R19`` (3-digit mm width, no decimal) in the metric lane.
_FLOTATION_SLASH_RE = re.compile(
    r"(?<![\w./+-])"
    r"(?P<service>{service})?"
    r"(?P<overall>\d{{2}}(?:\.\d+)?)"
    r"\s*/\s*"
    r"(?P<width>\d{{1,2}}\.\d+)"
    r"\s*(?P<construction>R|B|D|(?<!\s)-)\s*"
    r"(?P<rim>\d{{2}}(?:\.5)?)"
    r"(?P<trailing_service>LT|ST|C)?"
    r"(?P<trailing_marker>XL|SL|RF|TL)?"
    r"(?![\w.])".format(service=_SERVICE_TYPE_ALT),
    re.IGNORECASE,
)

_MC_MIN_SECTION_MM = 60
_MC_MAX_SECTION_MM = 99
_MC_MIN_ASPECT = 60
_MOTORCYCLE_METRIC_RE = re.compile(
    r"(?<![\w./+-])"
    r"(?P<width>\d{2})"
    r"\s*/\s*"
    r"(?P<aspect>\d{2,3})"
    r"\s*(?P<construction>ZR|R|B|D|(?<!\s)-)\s*"
    r"(?P<rim>\d{2}(?:\.5)?)"
    r"(?P<trailing_marker>M/C|TL|TT)?"
    r"(?![\w.])",
    re.IGNORECASE,
)

# Numeric / conventional. Requires the decimal point (``7.50-16``): without it, ``750-16`` is
# indistinguishable from a part number, so it is not accepted.
_NUMERIC_RE = re.compile(
    r"(?<![\w./+-])" r"(?P<service>{service})?"
    # Exactly two decimals, and no leading zero. Numeric sizes are conventionally written to two
    # places ("7.50-16", "4.00-18", "4.60-17"), and requiring that is what keeps model-year ranges
    # out: production rows read "Belltech LOWERING KIT 16.5-17 Chevy Silverado" (MY2016.5-2017)
    # and "South Bend Clutch 05.5-13 Dodge" (MY2005.5-2013), both of which this notation would
    # otherwise read as a tire. Costs the one-decimal agricultural sizes ("8.3-24", 9 rows in the
    # catalog) -- worth it against year ranges appearing across 172 non-tire brands.
    r"(?P<width>[1-9]\d?\.\d{{2}})"
    r"\s*(?P<construction>R|B|D|(?<!\s)-)\s*"
    r"(?P<rim>\d{{2}}(?:\.5)?)"
    r"(?P<trailing_service>LT|ST|C)?"
    r"(?P<trailing_marker>XL|SL|RF|TL)?"
    r"(?![\w.])".format(service=_SERVICE_TYPE_ALT),
    re.IGNORECASE,
)

# A bolt pattern immediately after an inch pair is the wheel tell: ``20X10 5X127``,
# ``17X8.5 6X139.7``. Three-digit (mm) or ``4.5``-style (inch) PCDs both appear.
_BOLT_PATTERN_RE = re.compile(r"\b\d(?:\.\d)?\s*[xX]\s*(?:\d{2,3}(?:\.\d+)?|\d\.\d+)\b")

# Service description: load index (optionally a dual-load pair) glued or slash-joined to the
# speed symbol. Wheel Pros writes ``109/T`` where the slash is a separator, not a dual load --
# the dual form is ``121/118S``, told apart by the second element being numeric.
_SERVICE_DESCRIPTION_RE = re.compile(
    r"(?<![\w.])" r"(?P<load_index>\d{2,3})"
    # The dual load separator is a slash for most distributors and a SPACE for Premier, which
    # writes the whole size that way ("LT275 65R20 126 123S"). Accepting only the slash made the
    # regex skip 126 and match "123S" as the load index -- the dual, and the lower number -- on
    # 224 catalog rows, understating max_load_lb on every one of them.
    r"(?:[/ ](?P<load_index_dual>\d{2,3}))?" r"\s*/?\s*" r"(?P<speed>\(Y\)|[A-Z]\d?)" r"(?![\w.])",
    re.IGNORECASE,
)

# ``(?!/[A-Za-z])`` rejects the letter of an X/Y model designation. "GRAND SPORT A/S" and
# "OPEN COUNTRY M/T" were yielding Load Range A and M -- 978 catalog rows, 1,021 persisted specs,
# and a wrong ply_rating derived from each. A slash followed by a DIGIT is still fine, because
# that is the real "E/10" ply notation.
# Two guards, one for each side of an X/Y model designation:
#   ``(?!/[A-Za-z])``   rejects the A of "GRAND SPORT A/S" and the M of "OPEN COUNTRY M/T"
#   ``(?<![A-Za-z]/)``  rejects the A of "All Terrain T/A", where the letter follows the slash
# A slash preceded by a DIGIT is the real thing and must still pass: "LT255/75R17/C" writes its
# load range exactly that way. Without the second guard, every BFGoodrich T/A -- 173 rows --
# was about to be given Load Range A, overwriting the C or E the same title actually stated.
# "/C" or "/E" immediately after the rim -- BFGoodrich writes "LT255/75R17/C 111/108S". Anchored
# with ``\A`` so only the character right after the size can match.
# Of the markers that can be glued to a rim, only these are load designations.
_GLUED_LOAD_DESIGNATIONS = frozenset(["XL", "SL", "RF"])

_LEADING_LOAD_RANGE_RE = re.compile(
    # A digit may follow with no space -- "LT235/75R15/D110/107S" glues the load index
    # straight onto the designation. A letter may not, or "/DOT" would read as Load Range D.
    r"\A/(SL|XL|LL|RF|[A-N])(?![A-Za-z.])",
    re.IGNORECASE,
)

# Guards against every way a letter that is not a load range shows up standing alone after a
# size. All four were found in production:
#
#   (?<![A-Za-z]/)   "All Terrain T/A KO3"     -- the A follows the slash
#   (?!/[A-Za-z])    "GRAND SPORT A/S"         -- the A precedes it
#   (?<!-) (?!-\w)   "GY EAG RS-A", "M-108+"   -- a hyphenated model suffix or prefix
#   (?! [TS]\b)      "BAJA BOSS A T"           -- A/T and M/T written with a space
#   (?! ?&) (?<!& )  "M & H RADIAL DRAG"       -- M&H Racemaster, on both sides of the ampersand
#
# The last one is safe because a real designation is never followed by a bare T or S: a load
# range is the end of the service description, and what follows is a diameter or a part number.
_LOAD_RANGE_TOKEN_RE = re.compile(
    r"(?<![A-Za-z]/)(?<!-)(?<!& )(?<![\w.])"
    r"(?:LR)?(REINFORCED|SL|XL|LL|RF|[A-N])"
    r"(?!/[A-Za-z])(?!-\w)(?! [TS]\b)(?! ?&)(?![\w.])",
    re.IGNORECASE,
)


@dataclasses.dataclass(frozen=True)
class ParsedSize:
    """
    One decoded sidewall string. Field names match the ``tire_specs`` columns they populate.

    ``overall_diameter_in`` is exact for ``flotation`` (the string states it) and computed for
    ``metric``; for ``numeric`` it is nominal -- see the module docstring.
    """

    notation: str
    size_display: str
    construction: str
    rim_diameter_in: decimal.Decimal
    overall_diameter_in: decimal.Decimal
    service_type: typing.Optional[str] = None
    section_width_mm: typing.Optional[int] = None
    aspect_ratio: typing.Optional[int] = None
    section_width_in: typing.Optional[decimal.Decimal] = None
    load_index: typing.Optional[int] = None
    load_index_dual: typing.Optional[int] = None
    speed_rating: typing.Optional[str] = None
    load_range: typing.Optional[str] = None
    # The exact substring this came from, so a disagreement between two provider titles can be
    # reported with the text that caused it instead of just the decoded values.
    matched_text: str = ""
    # Offsets of ``matched_text`` in the string it was parsed from. Query parsing strips the
    # size out by span and hands the remainder to brand/model matching, so a substring search
    # would be wrong the moment the same digits appear twice ("275/70R18 vs 275/70R18").
    span: typing.Tuple[int, int] = (0, 0)
    # True when aspect_ratio was assumed rather than read (a bare "275R18" is conventionally
    # 82-series). Query building must not filter on an assumed aspect -- it would exclude the
    # 45- and 70-series tires the user is most likely looking for.
    aspect_assumed: bool = False

    def as_llm_payload(self) -> typing.Dict[str, typing.Any]:
        """
        The compact form handed to the enrichment prompt. Deliberately a subset: the model is
        given the size so it stops guessing at dimensions, not so it can reason about ply
        ratings, and every extra key is tokens on 100k calls.
        """
        payload = {
            "size_display": self.size_display,
            "notation": self.notation,
            "rim_diameter_in": float(self.rim_diameter_in),
        }
        if self.service_type:
            payload["service_type"] = self.service_type
        if self.section_width_mm is not None:
            payload["section_width_mm"] = self.section_width_mm
        if self.aspect_ratio is not None:
            payload["aspect_ratio"] = self.aspect_ratio
        if self.section_width_in is not None:
            payload["section_width_in"] = float(self.section_width_in)
        if self.load_index is not None:
            payload["load_index"] = self.load_index
        if self.speed_rating:
            payload["speed_rating"] = self.speed_rating
        if self.load_range:
            payload["load_range"] = self.load_range
        return payload


def _round(value: decimal.Decimal, places: str) -> decimal.Decimal:
    """Half-up, matching TireLoadIndex.max_load_lb -- Python's default half-to-even would round
    32.25 down to 32.2 and put us a tenth under every published chart."""
    return value.quantize(decimal.Decimal(places), rounding=decimal.ROUND_HALF_UP)


def _normalize(text: str) -> str:
    return text.translate(_UNICODE_X)


def _rim_value(raw: str) -> decimal.Decimal:
    """
    Rim text -> inches. A bare three-digit group is the decimal-less commercial form: "195" means
    19.5, not 195 inches. Two-digit and explicit ".5" forms pass through unchanged.
    """
    if "." not in raw and len(raw) == 3:
        return decimal.Decimal("{}.{}".format(raw[:2], raw[2]))
    return decimal.Decimal(raw)


def _plausible_rim(rim: decimal.Decimal) -> bool:
    """Bounds check plus the half-inch rule -- see ``_MIN_HALF_INCH_RIM``."""
    if not _MIN_RIM_IN <= rim <= _MAX_RIM_IN:
        return False
    return rim % 1 == 0 or rim >= _MIN_HALF_INCH_RIM


def _construction_from(raw: str) -> str:
    upper = raw.upper()
    if upper in ("ZR", "Z", "Z-"):
        # ``255/35Z-18`` and ``255/35ZR18`` are the same tire written two ways -- Z is the
        # high-speed size marker either way, never a speed rating (see TireSpeedRating).
        return CONSTRUCTION_ZR
    if upper == "R":
        return CONSTRUCTION_RADIAL
    if upper == "B":
        return CONSTRUCTION_BELTED_BIAS
    return CONSTRUCTION_BIAS


def _service_type_from(match: "re.Match") -> typing.Optional[str]:
    """
    Service type is written either as a prefix (``LT275/70R18``) or as a suffix
    (``33X12.50R15LT``) depending on notation and distributor. Both land in the same field.
    """
    groups = match.groupdict()
    prefix = groups.get("service")
    suffix = groups.get("trailing_service")
    chosen = prefix or suffix
    return chosen.upper() if chosen else None


def _parse_service_description(tail: str) -> typing.Dict[str, typing.Any]:
    """
    Read load index / dual load / speed symbol / load range out of the text that follows the
    size. ``tail`` is truncated by the caller: the further from the size string we look, the more
    likely a part number or a price is read as a load index.
    """
    result: typing.Dict[str, typing.Any] = {
        "load_index": None,
        "load_index_dual": None,
        "speed_rating": None,
        "load_range": None,
    }

    # A load range glued to the rim with a slash comes BEFORE the service description:
    # "LT255/75R17/C 111/108S" leaves a tail of "/C 111/108S". It has to be taken off the front
    # first, or the letter blocks the service description's own lookbehind and
    # "LT235/75R15/D110/107S" reads the dual load 107 as the primary instead of 110.
    leading = _LEADING_LOAD_RANGE_RE.match(tail)
    if leading is not None and leading.group(1).upper() in LOAD_RANGES:
        result["load_range"] = leading.group(1).upper()
        tail = tail[leading.end() :]

    consumed_to = 0
    for match in _SERVICE_DESCRIPTION_RE.finditer(tail):
        speed = match.group("speed").upper()
        if speed not in SPEED_RATINGS:
            continue
        load_index = int(match.group("load_index"))
        # Outside the seeded 60-150 range this is not a load index -- most often it is the
        # overall diameter Wheel Pros appends ("... 116S XL 32.9") or a part-number fragment.
        if not 60 <= load_index <= 150:
            continue
        result["load_index"] = load_index
        dual = match.group("load_index_dual")
        if dual is not None and 60 <= int(dual) <= 150:
            result["load_index_dual"] = int(dual)
        result["speed_rating"] = speed
        consumed_to = match.end()
        break

    if result["load_range"] is not None:
        return result

    # Otherwise it is a standalone token, and only after the service description has already taken
    # the letter glued to the load index -- otherwise the E of "115/Q E" and the Q both compete
    # for the same vocabulary.
    for match in _LOAD_RANGE_TOKEN_RE.finditer(tail, consumed_to):
        token = match.group(1).upper()
        if token in LOAD_RANGES:
            result["load_range"] = token
            break

    return result


# The letter designations A-N are the LT/ST vocabulary. A passenger tire expresses load through
# SL/XL/LL and never carries one, so a lone letter after a passenger size is something else --
# and on premium brands it is almost always an OE homologation marking: Pirelli stamps J for
# Jaguar, L for Lamborghini, N for Porsche, F for Ferrari. Those were landing as Load Range J,
# L, N and F on hundreds of rows. Accepting letters only where they can physically apply kills
# the whole class at once, rather than one brand's code at a time.
_LETTER_LOAD_RANGES = frozenset("ABCDEFGHJLMN")


def _letter_load_range_applies(
    *,
    service_type: typing.Optional[str],
    notation: str,
    rim: decimal.Decimal,
    load_index_dual: typing.Optional[int],
) -> bool:
    if service_type in ("LT", "ST", "C"):
        return True
    if notation == NOTATION_FLOTATION:
        return True
    # Commercial rims (17.5 and up in half-inch sizes) and a dual load rating both mean a
    # truck tire, whether or not the distributor wrote the service type.
    if rim >= decimal.Decimal("17.5") and rim % 1:
        return True
    return load_index_dual is not None


def _finish(
    *,
    notation: str,
    match: "re.Match",
    tail: str,
    size_display: str,
    construction: str,
    rim_diameter_in: decimal.Decimal,
    overall_diameter_in: decimal.Decimal,
    section_width_mm: typing.Optional[int] = None,
    aspect_ratio: typing.Optional[int] = None,
    section_width_in: typing.Optional[decimal.Decimal] = None,
) -> ParsedSize:
    service_description = _parse_service_description(tail)
    # An allowlist, not a denylist: the marker group also carries TL/TT (tube markers) and M/C
    # (the motorcycle marker), none of which are load designations. Denying only "TL" let M/C
    # through as a load range on real rows.
    glued_marker = (match.groupdict().get("trailing_marker") or "").upper()
    if glued_marker in _GLUED_LOAD_DESIGNATIONS:
        service_description["load_range"] = glued_marker

    if service_description["load_range"] in _LETTER_LOAD_RANGES and not _letter_load_range_applies(
        service_type=_service_type_from(match),
        notation=notation,
        rim=rim_diameter_in,
        load_index_dual=service_description["load_index_dual"],
    ):
        service_description["load_range"] = None
    return ParsedSize(
        notation=notation,
        size_display=size_display,
        construction=construction,
        rim_diameter_in=rim_diameter_in,
        overall_diameter_in=overall_diameter_in,
        service_type=_service_type_from(match),
        section_width_mm=section_width_mm,
        aspect_ratio=aspect_ratio,
        section_width_in=section_width_in,
        matched_text=match.group(0).strip(),
        span=match.span(),
        **service_description,
    )


# How far past the size string to look for the service description. Long enough for
# "115/Q E" plus the diameter Wheel Pros appends, short enough that the next field on the line
# (a part number, a price) is out of reach.
_TAIL_WINDOW = 24


def _iter_matches(text: str, *patterns: "re.Pattern") -> typing.Iterator["re.Match"]:
    """Matches from several patterns, each pattern exhausted in turn. Order is the caller's
    priority order, not position in the string."""
    for pattern in patterns:
        for match in pattern.finditer(text):
            yield match


def _parse_metric(text: str) -> typing.Optional[ParsedSize]:
    for match in _METRIC_RE.finditer(text):
        width_mm = int(match.group("width"))
        aspect = int(match.group("aspect"))
        rim = _rim_value(match.group("rim"))
        if not _MIN_SECTION_MM <= width_mm <= _MAX_SECTION_MM:
            continue
        if not _MIN_ASPECT <= aspect <= _MAX_ASPECT:
            continue
        if not _plausible_rim(rim):
            continue
        construction = _construction_from(match.group("construction"))
        if construction == CONSTRUCTION_BIAS and aspect < _MIN_BIAS_ASPECT:
            continue
        if width_mm > _WIDE_SECTION_MM and aspect > _MAX_ASPECT_FOR_WIDE:
            continue

        section_height_in = (decimal.Decimal(width_mm) * aspect / 100) / MM_PER_INCH
        overall = _round(rim + 2 * section_height_in, "0.1")
        service_type = _service_type_from(match)
        # Always rendered with the decimal, even when the source dropped it, so "225/70R195" and
        # "225/70R19.5" collapse to one size_display and therefore one search facet value.
        rim_text = str(rim.normalize()) if rim % 1 else str(int(rim))
        # Bias renders as the bare hyphen the source wrote, not as the letter D. D is the formal
        # metric marker for diagonal construction, but in practice a hyphen in a metric size is
        # either a motorcycle size ("120/100-18", where the hyphen IS the convention) or a
        # distributor writing R sloppily -- and "205/55D16" is a spelling neither of them uses.
        # ``construction`` still carries the semantic "D".
        # C is the European commercial marker and is written AFTER the rim ("225/75R16C"), unlike
        # LT/ST/P/T which prefix. Rendering it leading produced "C225/75R16", a size string that
        # appears nowhere in the industry and would not match a customer's search.
        leading = "" if service_type == "C" else (service_type or "")
        trailing = "C" if service_type == "C" else ""
        display = "{service}{width}/{aspect}{construction}{rim}{trailing}".format(
            service=leading,
            width=width_mm,
            aspect=aspect,
            construction="-" if construction == CONSTRUCTION_BIAS else construction,
            rim=rim_text,
            trailing=trailing,
        )
        return _finish(
            notation=NOTATION_METRIC,
            match=match,
            tail=text[match.end() : match.end() + _TAIL_WINDOW],
            size_display=display,
            construction=construction,
            rim_diameter_in=rim,
            overall_diameter_in=overall,
            section_width_mm=width_mm,
            aspect_ratio=aspect,
            section_width_in=_round(decimal.Decimal(width_mm) / MM_PER_INCH, "0.01"),
        )
    return None


def _parse_flotation(text: str) -> typing.Optional[ParsedSize]:
    for match in _iter_matches(text, _FLOTATION_RE, _FLOTATION_SLASH_RE):
        overall = decimal.Decimal(match.group("overall"))
        width_in = decimal.Decimal(match.group("width"))
        rim = decimal.Decimal(match.group("rim"))
        if not _MIN_OVERALL_IN <= overall <= _MAX_OVERALL_IN:
            continue
        if not _MIN_SECTION_IN <= width_in <= _MAX_SECTION_IN:
            continue
        if not _plausible_rim(rim):
            continue
        # A tire is always taller than the wheel it mounts on. This is what rejects a wheel
        # written as ``20X12-44`` (offset -44 read as a rim) and any transposed pair.
        if overall <= rim:
            continue
        # ``20X10 5X127`` never reaches here (no construction character), but ``17X8.5-5X114.3``
        # can start to look plausible, so the bolt-pattern tell is checked on the tail too.
        if _BOLT_PATTERN_RE.search(text[match.end() : match.end() + 12]):
            continue

        construction = _construction_from(match.group("construction"))
        service_type = _service_type_from(match)
        # Inch notations are conventionally written with a bare hyphen for bias construction
        # ("33X12.50-20LT"), where metric uses the letter D ("205/55D16"). ``construction`` keeps
        # the semantic "D" either way; only the display string differs.
        display = "{overall}X{width}{construction}{rim}{service}".format(
            overall=match.group("overall"),
            width=match.group("width"),
            construction="-" if construction == CONSTRUCTION_BIAS else construction,
            rim=match.group("rim"),
            service=service_type or "",
        )
        return _finish(
            notation=NOTATION_FLOTATION,
            match=match,
            tail=text[match.end() : match.end() + _TAIL_WINDOW],
            size_display=display,
            construction=construction,
            rim_diameter_in=rim,
            overall_diameter_in=_round(overall, "0.1"),
            section_width_in=_round(width_in, "0.01"),
        )
    return None


def _parse_motorcycle_metric(text: str) -> typing.Optional[ParsedSize]:
    """
    Two-digit-width metric, which is a motorcycle size: ``90/90-21``, ``80/100-21``.

    Three-digit motorcycle sizes (``180/55ZR17``) are indistinguishable from car sizes in the
    string and are handled by ``_parse_metric``; the vehicle type comes from the enrichment model,
    not from here. This function exists only for the widths the car pattern cannot express.
    """
    for match in _MOTORCYCLE_METRIC_RE.finditer(text):
        width_mm = int(match.group("width"))
        aspect = int(match.group("aspect"))
        rim = decimal.Decimal(match.group("rim"))
        if not _MC_MIN_SECTION_MM <= width_mm <= _MC_MAX_SECTION_MM:
            continue
        if not _MC_MIN_ASPECT <= aspect <= 130:
            continue
        if not _plausible_rim(rim):
            continue

        section_height_in = (decimal.Decimal(width_mm) * aspect / 100) / MM_PER_INCH
        construction = _construction_from(match.group("construction"))
        display = "{}/{}{}{}".format(
            width_mm, aspect, "-" if construction == CONSTRUCTION_BIAS else construction, match.group("rim")
        )
        return _finish(
            notation=NOTATION_METRIC,
            match=match,
            tail=text[match.end() : match.end() + _TAIL_WINDOW],
            size_display=display,
            construction=construction,
            rim_diameter_in=rim,
            overall_diameter_in=_round(rim + 2 * section_height_in, "0.1"),
            section_width_mm=width_mm,
            aspect_ratio=aspect,
            section_width_in=_round(decimal.Decimal(width_mm) / MM_PER_INCH, "0.01"),
        )
    return None


def _parse_numeric(text: str) -> typing.Optional[ParsedSize]:
    for match in _NUMERIC_RE.finditer(text):
        width_in = decimal.Decimal(match.group("width"))
        rim = decimal.Decimal(match.group("rim"))
        if not _MIN_SECTION_IN <= width_in <= _MAX_SECTION_IN:
            continue
        if not _plausible_rim(rim):
            continue

        construction = _construction_from(match.group("construction"))
        service_type = _service_type_from(match)
        # No aspect ratio exists in this notation. Section height == section width is the
        # pre-1965 convention; the resulting diameter is nominal, not measured. See module
        # docstring -- callers must key off notation == "numeric" before treating it as exact.
        overall = _round(rim + 2 * width_in, "0.1")
        display = "{service}{width}{construction}{rim}".format(
            service=service_type or "",
            width=match.group("width"),
            construction="-" if construction == CONSTRUCTION_BIAS else construction,
            rim=match.group("rim"),
        )
        return _finish(
            notation=NOTATION_NUMERIC,
            match=match,
            tail=text[match.end() : match.end() + _TAIL_WINDOW],
            size_display=display,
            construction=construction,
            rim_diameter_in=rim,
            overall_diameter_in=overall,
            section_width_in=_round(width_in, "0.01"),
        )
    return None


# How far a notation is trusted when two titles for the same SKU decode differently. Metric
# and flotation are equally explicit; numeric states neither an aspect ratio nor a real
# diameter and is the easiest to match out of surrounding noise.
_NOTATION_TRUST = {NOTATION_METRIC: 2, NOTATION_FLOTATION: 2, NOTATION_NUMERIC: 1}


def parse(text: typing.Optional[str]) -> typing.Optional[ParsedSize]:
    """
    Decode the first tire size in ``text``, or ``None`` if there isn't one.

    Notations are tried most-specific first. Metric leads because it is both the commonest and
    the least ambiguous; flotation before numeric because ``33X12.50R15`` contains a substring
    (``12.50R15``) that the numeric pattern would otherwise happily claim as a 12.5-inch tire on
    a 15-inch rim.
    """
    if not text:
        return None
    normalized = _normalize(text)
    # A bolt pattern anywhere in the string means this row is a wheel (or a wheel-and-tire
    # package, which is not a tire either -- see the enrichment prompt's rule 6). Checked before
    # anything else because a wheel line carries numbers that read as sizes on their own:
    # "20X10 TORSION SE 5X127 P 5.00-12" has a perfectly plausible 5.00-12 in it.
    if _BOLT_PATTERN_RE.search(normalized):
        return None
    for parser in (_parse_metric, _parse_flotation, _parse_motorcycle_metric, _parse_numeric):
        parsed = parser(normalized)
        if parsed is not None:
            return parsed
    return None


def parse_best(texts: typing.Iterable[typing.Optional[str]]) -> typing.Optional[ParsedSize]:
    """
    Parse every candidate title and return the richest result.

    Distributors truncate differently -- TireRack ships ``285/70R17~~ NI RIDGE GRAPPLER`` with no
    service description at all while Wheel Pros ships ``RIDGE GRAP 285/70R17 116Q SL 32.7`` --
    so the size that decodes into the most fields is the one to keep. Ties go to the first, which
    makes the result stable for a given ordering of titles.
    """
    best: typing.Optional[ParsedSize] = None
    best_score = (-1, -1)
    candidates: typing.List[ParsedSize] = []
    for text in texts:
        parsed = parse(text)
        if parsed is None:
            continue
        field_count = sum(
            1
            for value in (
                parsed.service_type,
                parsed.load_index,
                parsed.load_index_dual,
                parsed.speed_rating,
                parsed.load_range,
            )
            if value is not None
        )
        # Notation trust comes FIRST, field count only breaks ties within a notation. Learned
        # from a real row: a distributor typo'd "37X12.5R18LT" as "37+12.5R18LT", the numeric
        # pattern read "12.5R18LT" out of the middle of it as a 12.5-inch tire on an 18-inch rim
        # (43" tall), and because that fragment carried a service type it outscored the correct
        # flotation parse of the same SKU's other title. Numeric is the weakest notation -- it
        # states no aspect ratio and its diameter is nominal -- so it must never win over a
        # metric or flotation reading, no matter how many fields it happens to fill.
        score = (_NOTATION_TRUST[parsed.notation], field_count)
        if score > best_score:
            best, best_score = parsed, score
        candidates.append(parsed)

    if best is None:
        return None
    return _merge_service_description(best, candidates)


# Fields a *different* title may legitimately supply. Dimensions are deliberately absent: they
# define which tire this is, so taking them from another title would merge two products. These
# describe the same tire and are simply omitted by some distributors.
_MERGEABLE_FIELDS = ("service_type", "load_index", "load_index_dual", "speed_rating", "load_range")


def _merge_service_description(best: ParsedSize, candidates: typing.Sequence[ParsedSize]) -> ParsedSize:
    """
    Fill fields the winning title omitted from other titles describing the same tire.

    ``parse_best`` used to pick one title and take everything from it, which threw away whatever
    the other titles knew. Measured over 3,000 enriched tires, 2.6% had a field in a non-winning
    title that the winner lacked -- mostly load range (1.7%) and the ZR marker (0.6%). One real
    row: the winning title read "Toyo Extensa A/S II P255/50R20 109H" while a sibling read
    "255/50R20~ TO EXTENSA AS II XL", and the XL was simply lost.

    **Only titles agreeing on the dimensions contribute.** A title describing a different size is
    a different tire, and merging across those would be worse than the omission it fixes.
    """
    same_tire = [
        other
        for other in candidates
        if other is not best
        and other.notation == best.notation
        and other.rim_diameter_in == best.rim_diameter_in
        and other.section_width_mm == best.section_width_mm
        and other.aspect_ratio == best.aspect_ratio
        and other.section_width_in == best.section_width_in
    ]
    if not same_tire:
        return best

    updates = {}
    for field in _MERGEABLE_FIELDS:
        if getattr(best, field) is None:
            value = next((getattr(other, field) for other in same_tire if getattr(other, field) is not None), None)
            if value is not None:
                updates[field] = value

    # ZR is strictly more specific than R -- it says the size carries the high-speed marker. A
    # title stating it is more informative than one that does not, so it wins; every other
    # construction disagreement is left alone, since those are contradictions rather than detail.
    if best.construction == CONSTRUCTION_RADIAL and any(other.construction == CONSTRUCTION_ZR for other in same_tire):
        updates["construction"] = CONSTRUCTION_ZR
        updates["size_display"] = best.size_display.replace("R", "ZR", 1)

    return dataclasses.replace(best, **updates) if updates else best


def disagreements(texts: typing.Iterable[typing.Optional[str]]) -> typing.List[str]:
    """
    Distinct *dimensions* across the given titles, as display strings.

    More than one means the providers linked to this master part describe physically different
    tires, which is a merge bug upstream rather than a parse failure -- the enrichment service
    flags those rather than silently picking one.

    Compared on dimensions rather than on ``size_display``, because distributors disagree about
    the service type constantly: Rough Country writes ``35x12.50R17`` where Wheel Pros writes
    ``35x12.50R17LT`` for the same SKU. That is one tire described two ways, not two tires, and
    treating it as a conflict would flag most of the catalog for review.
    """
    seen: typing.Dict[typing.Tuple, str] = {}
    for text in texts:
        parsed = parse(text)
        if parsed is None:
            continue
        key = (
            parsed.notation,
            parsed.section_width_mm,
            parsed.aspect_ratio,
            parsed.section_width_in,
            parsed.overall_diameter_in,
            parsed.rim_diameter_in,
        )
        seen.setdefault(key, parsed.size_display)
    return list(seen.values())


# ==========================================================================================
# Query parsing -- looser than the catalog parser, and deliberately kept apart from it
# ==========================================================================================

# Separator-free and space-separated sizes a *person types*: "275 70 18", "275/70/18",
# "2757018", "35 12.50 20". These are NOT accepted by parse() and must never be, because in a
# catalog title the same digits are routinely a part number -- "N205-730", "4981910504924".
# The difference is intent: someone typing into a search box who writes three numbers means a
# size, whereas a distributor writing three numbers usually does not. Keeping the two parsers
# separate is what lets the search box be generous without the classifier becoming reckless.
_LOOSE_METRIC_RE = re.compile(
    r"^\s*(?P<service>{service})?\s*"
    r"(?P<width>\d{{3}})\s*[/ -]\s*(?P<aspect>\d{{2}})\s*[/ -]\s*(?P<rim>\d{{2}}(?:\.5)?)\s*$".format(
        service=_SERVICE_TYPE_ALT
    ),
    re.IGNORECASE,
)
# "2757018" -- 3 digits width, 2 aspect, 2 rim. Exactly 7 digits: at 6 or 8 the split is
# ambiguous and the answer is None, per spec.
_LOOSE_DIGITS_RE = re.compile(r"^\s*(?P<width>\d{3})(?P<aspect>\d{2})(?P<rim>\d{2})\s*$")
# A size with no aspect ratio at all. 82-series is the convention for these, but it is an
# assumption and is flagged as one.
_ASSUMED_ASPECT_RATIO = 82
_BARE_WIDTH_RE = re.compile(
    r"^\s*(?P<service>{service})?\s*(?P<width>\d{{3}})\s*R\s*(?P<rim>\d{{2}}(?:\.5)?)\s*$".format(
        service=_SERVICE_TYPE_ALT
    ),
    re.IGNORECASE,
)
_LOOSE_FLOTATION_RE = re.compile(
    r"^\s*(?P<overall>\d{2}(?:\.\d+)?)\s*[ /-]\s*" r"(?P<width>\d{1,2}\.\d+)\s*[ /-]\s*(?P<rim>\d{1,2}(?:\.5)?)\s*$"
)


def _from_metric_parts(
    *, service: typing.Optional[str], width_mm: int, aspect: int, rim: decimal.Decimal, text: str
) -> typing.Optional[ParsedSize]:
    if not _MIN_SECTION_MM <= width_mm <= _MAX_SECTION_MM:
        return None
    if not _MIN_ASPECT <= aspect <= _MAX_ASPECT:
        return None
    if not _plausible_rim(rim):
        return None
    section_height_in = (decimal.Decimal(width_mm) * aspect / 100) / MM_PER_INCH
    rim_text = str(rim.normalize()) if rim % 1 else str(int(rim))
    service = service.upper() if service else None
    return ParsedSize(
        notation=NOTATION_METRIC,
        size_display="{}{}/{}R{}".format(service or "", width_mm, aspect, rim_text),
        construction=CONSTRUCTION_RADIAL,
        rim_diameter_in=rim,
        overall_diameter_in=_round(rim + 2 * section_height_in, "0.1"),
        service_type=service,
        section_width_mm=width_mm,
        aspect_ratio=aspect,
        section_width_in=_round(decimal.Decimal(width_mm) / MM_PER_INCH, "0.01"),
        matched_text=text.strip(),
        span=(0, len(text)),
    )


def parse_query(text: typing.Optional[str]) -> typing.Optional[ParsedSize]:
    """
    Parse a size out of something a **user typed**. Strict ``parse()`` first, then the loose
    forms above.

    Use this for search input only. Use ``parse()`` for catalog text -- see the comment on
    ``_LOOSE_METRIC_RE`` for why the two must not be merged.
    """
    if not text:
        return None
    strict = parse(text)
    if strict is not None:
        return strict

    normalized = _normalize(text)

    bare = _BARE_WIDTH_RE.match(normalized)
    if bare is not None:
        parsed = _from_metric_parts(
            service=bare.group("service"),
            width_mm=int(bare.group("width")),
            aspect=_ASSUMED_ASPECT_RATIO,
            rim=decimal.Decimal(bare.group("rim")),
            text=text,
        )
        if parsed is not None:
            # Display without the assumed aspect: showing "275/82R18" would present a guess as
            # if it were stamped on the tire.
            return dataclasses.replace(
                parsed,
                aspect_assumed=True,
                size_display="{}{}R{}".format(
                    (bare.group("service") or "").upper(), bare.group("width"), bare.group("rim")
                ),
            )

    match = _LOOSE_METRIC_RE.match(normalized) or _LOOSE_DIGITS_RE.match(normalized)
    if match is not None:
        groups = match.groupdict()
        return _from_metric_parts(
            service=groups.get("service"),
            width_mm=int(groups["width"]),
            aspect=int(groups["aspect"]),
            rim=decimal.Decimal(groups["rim"]),
            text=text,
        )

    match = _LOOSE_FLOTATION_RE.match(normalized)
    if match is not None:
        overall = decimal.Decimal(match.group("overall"))
        width_in = decimal.Decimal(match.group("width"))
        rim = decimal.Decimal(match.group("rim"))
        if (
            _MIN_OVERALL_IN <= overall <= _MAX_OVERALL_IN
            and _MIN_SECTION_IN <= width_in <= _MAX_SECTION_IN
            and _plausible_rim(rim)
            and overall > rim
        ):
            rim_text = str(rim.normalize()) if rim % 1 else str(int(rim))
            return ParsedSize(
                notation=NOTATION_FLOTATION,
                size_display="{}X{}R{}".format(match.group("overall"), match.group("width"), rim_text),
                construction=CONSTRUCTION_RADIAL,
                rim_diameter_in=rim,
                overall_diameter_in=_round(overall, "0.1"),
                section_width_in=_round(width_in, "0.01"),
                matched_text=text.strip(),
                span=(0, len(text)),
            )
    return None


def residue(text: str, parsed: typing.Optional[ParsedSize]) -> str:
    """
    ``text`` with the matched size cut out, whitespace collapsed.

    What is left is the part of a query that has to be matched against brand and model names.
    Cut by span, not by string replacement -- see the note on ``ParsedSize.span``.
    """
    if parsed is None:
        return " ".join(text.split())
    start, end = parsed.span
    return " ".join((text[:start] + " " + text[end:]).split())
