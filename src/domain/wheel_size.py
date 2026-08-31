"""
Decode wheel sizing from the strings distributors actually publish.

The counterpart to ``src.domain.tire_size``, and the source of truth for a wheel's dimensions in
the same way. It is pure: no Django, no database, no network, so it can be tested against real
production strings cheaply and exhaustively.

A wheel is described by four numbers and they arrive in four different house styles:

    WEBB BL UTV 14X7 4X110 +36 80 M-BLK          Wheel Pros
    TOUREN TR60 3260 ... 17X7.5 5-112/5-120 42MM 72.62MM   The Wheel Group
    20x8.25  8x165.1  offset 110  bore 121.6     Elite Wheels
    ITP Delta Steel 12x7 / 4/137 12mm BP / 4+3 Offset Black Wheel   a distributor title

**The one thing that must not go wrong is inches versus millimetres.** Both appear in the same
column, unmarked: Wheel Pros writes ``6X5.5`` and ``6X135`` one row apart, and they are a 139.7 mm
circle and a 135 mm circle -- two genuinely different, non-interchangeable wheels. Storing 5.5 as
though it were millimetres would not merely be wrong, it would be a fitment error that reaches a
customer's car. Everything here therefore canonicalises to millimetres, and the rule that decides
which unit a number is in gets its own constant and its own tests.

``BLANK`` is a value, not a gap. 674 Wheel Pros rows are undrilled wheels sold to be drilled to
order; reading that as "bolt pattern unknown" would put them in fitment results for every vehicle.
"""
import dataclasses
import decimal
import re
import typing

MM_PER_INCH = decimal.Decimal("25.4")

# A bolt circle stated in inches is never as large as one stated in millimetres. The largest inch
# circle in the catalog is 8.75" and the smallest metric one is 98 mm (4x98, Fiat), so anything
# under this is inches. The gap is an order of magnitude -- this is not a close call, which is why
# a plain threshold is safe where a guess would not be.
_INCH_IF_BELOW = decimal.Decimal("20")

# Physical bounds, used to reject numbers that merely look like wheel dimensions. A 4" diameter is
# a caster; a 40" wheel does not exist outside heavy equipment we do not sell.
_MIN_DIAMETER_IN = decimal.Decimal("8")
_MAX_DIAMETER_IN = decimal.Decimal("32")
_MIN_WIDTH_IN = decimal.Decimal("3")
_MAX_WIDTH_IN = decimal.Decimal("20")
_MIN_LUGS = 3
_MAX_LUGS = 10
_MIN_CIRCLE_MM = decimal.Decimal("90")
_MAX_CIRCLE_MM = decimal.Decimal("260")
_MIN_OFFSET_MM = -200
_MAX_OFFSET_MM = 200
_MIN_BORE_MM = decimal.Decimal("40")
_MAX_BORE_MM = decimal.Decimal("180")

BLANK = "BLANK"

# The bolt circles that actually exist, in millimetres, taken from every wheel feed we ingest
# rather than from a reference table -- 32 distinct values across ~60,000 SKUs.
#
# They exist because the same physical pattern is published several ways. A 5.5" circle is
# ``6X5.5`` in the Wheel Pros attribute column and ``6X139`` in that row's own title; a 4.5" one is
# ``5X4.5`` and ``5X114``. Left alone, one wheel becomes two fitments and nothing cross-matches.
#
# 107.95 (4.25") is deliberately absent so it snaps to 108: the industry publishes that pattern
# both ways and means one thing by it. 114 and 139 are absent for the same reason -- they are
# truncations of 114.3 and 139.7, not circles in their own right.
CANONICAL_BOLT_CIRCLES_MM = tuple(
    decimal.Decimal(v)
    for v in (
        "98",
        "100",
        "101.6",
        "105",
        "108",
        "110",
        "112",
        "114.3",
        "115",
        "118",
        "120",
        "120.65",
        "125",
        "127",
        "128",
        "130",
        "132",
        "135",
        "137",
        "139.7",
        "140",
        "150",
        "156",
        "160",
        "165.1",
        "170",
        "180",
        "190.5",
        "200",
        "205",
        "210",
        "225",
    )
)

# How far a published value may sit from a standard before we stop trusting the snap. The tightest
# real neighbours are 139.7 and 140, 0.3 mm apart, so this only ever moves a value to the *nearest*
# standard and refuses when two are equally close. 0.8 covers the truncations that occur (139 is
# 0.7 from 139.7) without reaching across the 127/128 pair, which are 1.0 apart and both real.
_SNAP_MAX_MM = decimal.Decimal("0.8")


def canonical_circle_mm(value: decimal.Decimal) -> decimal.Decimal:
    """Snap a published bolt circle to the standard it is a spelling of, or leave it alone."""
    best: typing.Optional[decimal.Decimal] = None
    best_gap: typing.Optional[decimal.Decimal] = None
    tied = False
    for standard in CANONICAL_BOLT_CIRCLES_MM:
        gap = abs(standard - value)
        if best_gap is None or gap < best_gap:
            best, best_gap, tied = standard, gap, False
        elif gap == best_gap:
            tied = True
    if best is None or best_gap is None or best_gap > _SNAP_MAX_MM or tied:
        return value
    return best


@dataclasses.dataclass(frozen=True)
class BoltPattern:
    """
    A lug count and the circle they sit on, always in millimetres.

    ``stated_in_inches`` is kept because it is how the source wrote it, and a display string built
    from the metric value would not match what the customer sees on the product page ("6x5.5" is
    what a Jeep owner searches for, not "6x139.7").
    """

    lug_count: int
    circle_mm: decimal.Decimal
    stated_in_inches: bool
    display: str

    @property
    def circle_in(self) -> decimal.Decimal:
        return (self.circle_mm / MM_PER_INCH).quantize(decimal.Decimal("0.01"))

    def __str__(self) -> str:
        return self.display


@dataclasses.dataclass(frozen=True)
class ParsedWheel:
    diameter_in: decimal.Decimal
    width_in: decimal.Decimal
    bolt_pattern: typing.Optional[BoltPattern] = None
    bolt_pattern_2: typing.Optional[BoltPattern] = None
    offset_mm: typing.Optional[int] = None
    center_bore_mm: typing.Optional[decimal.Decimal] = None
    is_blank: bool = False

    @property
    def size_display(self) -> str:
        return "{}x{}".format(_trim(self.diameter_in), _trim(self.width_in))

    @property
    def is_dually(self) -> bool:
        """Two bolt patterns on one wheel is a multi-fit, not a dually -- but a wheel published
        with a second pattern is always at least multi-fit, and callers want to know."""
        return self.bolt_pattern_2 is not None


def _trim(value: decimal.Decimal) -> str:
    """9.0 -> '9', 8.50 -> '8.5'. Sizes are written the short way everywhere."""
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _tidy(value: decimal.Decimal) -> decimal.Decimal:
    """
    Drop trailing zeros without letting Decimal reach for an exponent.

    ``Decimal("110.00").normalize()`` is ``Decimal("1.1E+2")`` -- equal in value, unreadable in a
    log and in a display string. Round-tripping through the plain format avoids it.
    """
    return decimal.Decimal(_trim(value))


def _decimal(text: str) -> typing.Optional[decimal.Decimal]:
    try:
        return decimal.Decimal(text)
    except (decimal.InvalidOperation, TypeError):
        return None


# ---------------------------------------------------------------------------------------------
# Bolt pattern
# ---------------------------------------------------------------------------------------------
# Separator is x, X or a hyphen: '6X135', '5-114.3', '5x100'. The lug count is a single digit in
# every pattern that exists, but two are allowed so a malformed value fails a bound check rather
# than matching half of itself.
_BOLT_RE = re.compile(r"^\s*(?P<lugs>\d{1,2})\s*[xX\-/]\s*(?P<circle>\d{1,3}(?:\.\d+)?)\s*(?:MM|mm)?\s*$")


def parse_bolt_pattern(text: typing.Optional[str]) -> typing.Optional[BoltPattern]:
    """
    Read one bolt pattern, in whichever unit it was written.

    Returns ``None`` for anything that is not a pattern, including ``BLANK`` -- an undrilled wheel
    has no circle, and callers must handle that as its own state rather than as a missing value.
    Use :func:`is_blank` to tell the two apart.
    """
    if not text:
        return None
    raw = text.strip()
    match = _BOLT_RE.match(raw)
    if not match:
        return None
    lugs = int(match.group("lugs"))
    value = _decimal(match.group("circle"))
    if value is None or not _MIN_LUGS <= lugs <= _MAX_LUGS:
        return None

    stated_in_inches = value < _INCH_IF_BELOW
    circle_mm = (value * MM_PER_INCH) if stated_in_inches else value
    circle_mm = canonical_circle_mm(_tidy(circle_mm.quantize(decimal.Decimal("0.01"))))
    if not _MIN_CIRCLE_MM <= circle_mm <= _MAX_CIRCLE_MM:
        return None
    return BoltPattern(
        lug_count=lugs,
        circle_mm=circle_mm,
        stated_in_inches=stated_in_inches,
        display="{}x{}".format(lugs, _trim(value)),
    )


def is_blank(text: typing.Optional[str]) -> bool:
    """``BLANK`` / ``Blank 5x/6x``: an undrilled wheel, drilled to order. A real product state."""
    return bool(text) and text.strip().upper().startswith(BLANK)


def parse_bolt_patterns(*values: typing.Optional[str]) -> typing.List[BoltPattern]:
    """
    Every distinct pattern across the fields a source offers.

    Sources disagree on how to publish a multi-fit wheel: The Wheel Group uses two columns, Wheel
    Pros packs both into one as ``6X135/5.5``. Both mean the wheel is drilled twice.
    """
    found: typing.List[BoltPattern] = []
    for value in values:
        if not value:
            continue
        for part in _split_multi(value):
            pattern = parse_bolt_pattern(part)
            if pattern and all((pattern.lug_count, pattern.circle_mm) != (p.lug_count, p.circle_mm) for p in found):
                found.append(pattern)
    return found


def _split_multi(value: str) -> typing.List[str]:
    """
    ``6X135/5.5`` is two patterns sharing a lug count, not one pattern with a slash in it.

    The trailing fragment inherits the lug count from the leading one, which is the only reading
    that makes physical sense: a wheel cannot have six lugs on one circle and five on another.
    """
    text = value.strip()
    if "/" not in text:
        return [text]
    head, _, tail = text.partition("/")
    head = head.strip()
    tail = tail.strip()
    lead = _BOLT_RE.match(head)
    if lead and tail and not _BOLT_RE.match(tail):
        return [head, "{}x{}".format(lead.group("lugs"), tail)]
    return [head, tail]


# ---------------------------------------------------------------------------------------------
# Diameter x width
# ---------------------------------------------------------------------------------------------
# '20X9', '17X8.5', '20x8.25'. Both figures are inches; nobody publishes a metric wheel size.
_SIZE_RE = re.compile(r"(?<![\d.])(?P<diameter>\d{1,2}(?:\.\d+)?)\s*[xX]\s*(?P<width>\d{1,2}(?:\.\d+)?)(?![\d.])")


def parse_size(text: typing.Optional[str]) -> typing.Optional[typing.Tuple[decimal.Decimal, decimal.Decimal]]:
    """Diameter and width in inches, or ``None`` if the string is not a wheel size."""
    found = _find_size(text)
    return (found[0], found[1]) if found else None


def _find_size(
    text: typing.Optional[str],
) -> typing.Optional[typing.Tuple[decimal.Decimal, decimal.Decimal, typing.Tuple[int, int]]]:
    """As :func:`parse_size`, but also where in the string it was found.

    The span matters: the width of ``12x7 / 4/137`` sits immediately before a slash, and a
    bolt-pattern scan that does not know the size has already claimed those characters reads
    "7 / 4" as a 7-lug pattern on a 4-inch circle. That is a plausible-looking value for a wheel
    that does not exist, which is the worst kind."""
    if not text:
        return None
    for match in _SIZE_RE.finditer(text):
        diameter = _decimal(match.group("diameter"))
        width = _decimal(match.group("width"))
        if diameter is None or width is None:
            continue
        if not _MIN_DIAMETER_IN <= diameter <= _MAX_DIAMETER_IN:
            continue
        if not _MIN_WIDTH_IN <= width <= _MAX_WIDTH_IN:
            continue
        # A wheel is always wider across than it is deep. The reverse pair is a box dimension or a
        # bolt pattern that slipped through, both of which appear in these descriptions.
        if width > diameter:
            continue
        return _tidy(diameter), _tidy(width), match.span()
    return None


def parse_offset_mm(text: typing.Optional[str]) -> typing.Optional[int]:
    """
    Offset in millimetres, signed. ``+36``, ``-40``, ``0``, ``42MM``.

    Zero is a real offset and a common one (5,493 Wheel Pros rows), so it must survive every
    falsy check between here and the database.
    """
    if text is None:
        return None
    match = re.search(r"(?<![\d.])(?P<sign>[+-]?)\s*(?P<value>\d{1,3})\s*(?:MM|mm)?(?![\d.])", str(text).strip())
    if not match:
        return None
    value = int(match.group("value"))
    if match.group("sign") == "-":
        value = -value
    if not _MIN_OFFSET_MM <= value <= _MAX_OFFSET_MM:
        return None
    return value


def parse_center_bore_mm(text: typing.Optional[str]) -> typing.Optional[decimal.Decimal]:
    """Hub bore in millimetres: '72.62', '80.20', '106.1'."""
    if text is None:
        return None
    match = re.search(r"(?<![\d.])(?P<value>\d{2,3}(?:\.\d+)?)\s*(?:MM|mm)?(?![\d.])", str(text).strip())
    if not match:
        return None
    value = _decimal(match.group("value"))
    if value is None or not _MIN_BORE_MM <= value <= _MAX_BORE_MM:
        return None
    return _tidy(value)


# ---------------------------------------------------------------------------------------------
# Whole strings
# ---------------------------------------------------------------------------------------------
# Backspacing, the off-road way of stating offset: '4+3' is 4.3 inches of backspacing. Kept
# separate from offset because converting between them needs the width, and a wrong conversion is
# a fitment error -- see ``backspacing_to_offset_mm``.
_BACKSPACE_RE = re.compile(r"(?<![\d.])(?P<whole>\d)\s*\+\s*(?P<frac>\d)(?![\d.])")

# Bolt patterns inside a free-text title. The hyphen is allowed because The Wheel Group writes
# ``5-112/5-120``, but only with the lug count glued to it -- a bare hyphen between two numbers is
# a range far more often than it is a bolt circle.
#
# A single-digit circle is allowed because inch patterns are written that way: ``5X5`` is a 5-lug
# 127 mm Jeep pattern. The noise this lets through is caught by the millimetre bounds rather than
# by the pattern -- ``3-3.5`` from a shock-kit title is 88.9 mm, under the 90 mm floor, so it
# fails on physics instead of on punctuation.
_TEXT_BOLT_RE = re.compile(r"(?<![\d.])(\d)\s*[xX/-]\s*(\d{1,3}(?:\.\d+)?)(?![\d.])")


def parse_backspacing_in(text: typing.Optional[str]) -> typing.Optional[decimal.Decimal]:
    if not text:
        return None
    match = _BACKSPACE_RE.search(text)
    if not match:
        return None
    return _tidy(decimal.Decimal("{}.{}".format(match.group("whole"), match.group("frac"))))


def backspacing_to_offset_mm(backspacing_in: decimal.Decimal, width_in: decimal.Decimal) -> int:
    """
    Convert backspacing to offset. Both describe where the mounting face sits; neither can be
    derived without the wheel's width, which is why they are stored separately and converted only
    when the width is known.

        offset = backspacing - (width / 2 + 0.5")
    """
    centreline = width_in / 2 + decimal.Decimal("0.5")
    return int(((backspacing_in - centreline) * MM_PER_INCH).quantize(decimal.Decimal("1")))


def parse(text: typing.Optional[str]) -> typing.Optional[ParsedWheel]:
    """
    Decode a whole distributor title.

    Used for wheels that reach us without a structured feed. Requires a diameter and width at
    minimum: a bolt pattern alone describes a hub, an adapter or a spacer, and there are tens of
    thousands of those in the catalog.
    """
    if not text:
        return None
    found = _find_size(text)
    if found is None:
        return None
    diameter, width, size_span = found

    # Blank the size out of a working copy rather than skipping matches that overlap it. Skipping
    # is not enough: ``finditer`` still consumes the characters it matched, so rejecting "7 / 4"
    # in ``12x7 / 4/137`` also swallowed the 4 and the real pattern 4/137 became unreachable.
    # Overwriting with spaces keeps every offset intact while removing the digits from play.
    scannable = text[: size_span[0]] + " " * (size_span[1] - size_span[0]) + text[size_span[1] :]

    patterns: typing.List[BoltPattern] = []
    for match in _TEXT_BOLT_RE.finditer(scannable):
        candidate = parse_bolt_pattern("{}x{}".format(match.group(1), match.group(2)))
        if candidate is None:
            continue
        if all((candidate.lug_count, candidate.circle_mm) != (p.lug_count, p.circle_mm) for p in patterns):
            patterns.append(candidate)

    offset = None
    signed = re.search(r"(?<![\w.])([+-]\s*\d{1,3})\s*(?:MM|mm)?(?![\d.])", text)
    if signed:
        offset = parse_offset_mm(signed.group(1))
    if offset is None:
        explicit = re.search(r"(?<![\d.])(\d{1,3})\s*(?:MM|mm)(?![\w.])", text)
        if explicit:
            offset = parse_offset_mm(explicit.group(1))

    return ParsedWheel(
        diameter_in=diameter,
        width_in=width,
        bolt_pattern=patterns[0] if patterns else None,
        bolt_pattern_2=patterns[1] if len(patterns) > 1 else None,
        offset_mm=offset,
        center_bore_mm=None,
        is_blank=is_blank(text),
    )


# ---------------------------------------------------------------------------------------------
# Search queries
# ---------------------------------------------------------------------------------------------
@dataclasses.dataclass(frozen=True)
class ParsedWheelQuery:
    """
    A search box turned into filters. ``residue`` is whatever text was left over and becomes
    Meilisearch's ``q``.

    The split matters. "20x9 6x4.5" carries no words a text index can match -- the searchable
    attributes are brand, model and style number, and none of them contain a size -- so passing it
    as text returns nothing at all. It has to become numeric filters. Anything the parser did not
    claim ("fuel 20x9") stays as text so the brand still narrows the result.
    """

    filters: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)
    residue: str = ""
    matched: typing.Dict[str, typing.Any] = dataclasses.field(default_factory=dict)

    @property
    def parsed_anything(self) -> bool:
        return bool(self.filters)


def parse_query(text: typing.Optional[str]) -> ParsedWheelQuery:
    """
    Read a wheel search box.

    The bolt pattern becomes ``bolt_circle_mm`` plus ``bolt_lug_count``, never the display string.
    A customer typing "6x4.5" and a feed publishing "6x114.3" mean one circle, and the index stores
    whichever spelling its source used -- matching on the canonical millimetre value is the only
    way both find the same wheels.
    """
    if not text:
        return ParsedWheelQuery()
    parsed = parse(text)
    if parsed is None:
        return ParsedWheelQuery(residue=text.strip())

    filters: typing.Dict[str, typing.Any] = {
        "diameter_in": float(parsed.diameter_in),
        "width_in": float(parsed.width_in),
    }
    matched: typing.Dict[str, typing.Any] = {"size": parsed.size_display}

    if parsed.bolt_pattern is not None:
        filters["bolt_circle_mm"] = float(parsed.bolt_pattern.circle_mm)
        filters["bolt_lug_count"] = parsed.bolt_pattern.lug_count
        matched["bolt_pattern"] = parsed.bolt_pattern.display
    if parsed.offset_mm is not None:
        filters["offset_mm"] = parsed.offset_mm
        matched["offset_mm"] = parsed.offset_mm

    return ParsedWheelQuery(filters=filters, residue=_query_residue(text, parsed), matched=matched)


# Everything the filters already account for, removed so it cannot also be text-matched: the size,
# any bolt pattern, and a signed or mm-suffixed offset.
_RESIDUE_STRIP_RE = re.compile(
    r"(?<![\d.])\d{1,2}(?:\.\d+)?\s*[xX]\s*\d{1,2}(?:\.\d+)?(?![\d.])"  # 20x9
    r"|(?<![\d.])\d\s*[xX/-]\s*\d{1,3}(?:\.\d+)?(?![\d.])"  # 6x4.5, 5-114.3
    r"|(?<![\w.])[+-]\s*\d{1,3}\s*(?:MM|mm)?(?![\d.])"  # +18, -12
    r"|(?<![\d.])\d{1,3}\s*(?:MM|mm)(?![\w.])",  # 18mm
)


def _query_residue(text: str, parsed: ParsedWheel) -> str:
    """What is left of the query once the structured parts are removed."""
    remaining = _RESIDUE_STRIP_RE.sub(" ", text)
    return re.sub(r"\s{2,}", " ", remaining).strip(" ,-/")
