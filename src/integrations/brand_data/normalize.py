"""
Readers for the display strings a manufacturer publishes, one per shape of string.

Everything a brand sends is written for a human: ``'11.5/32"'``, ``'2,205 lbs (116)'``,
``'6.0" - 8.0"'``, ``'500 A A'``, ``'Yes'``, ``'—'``. Each function here turns one of those
shapes into the value a column can hold, and returns ``None`` -- never a zero, never a False --
when the string says nothing. That distinction is the whole contract of ``raw_tire_specs``: a
NULL there means the manufacturer did not publish the figure, and a parser that guesses instead
of returning None destroys the only thing the table is for.

Failures are reported rather than swallowed. Each function returns ``(value, warning)``: a
warning is set only when there *was* text and it could not be read, so a source whose column we
misidentified shows up as a pile of warnings on the run instead of a column of quiet NULLs.

None of this is specific to one brand. Brand-specific cleanup belongs in that brand's loader.
"""
import decimal
import re
import typing

Result = typing.Tuple[typing.Any, typing.Optional[str]]

# Strings that mean "nothing here". Manufacturers spell an empty cell a dozen ways and a
# spreadsheet export turns some of them into text; all of them are absence, not a value.
BLANKS = frozenset(
    [
        "",
        "-",
        "--",
        "---",
        "—",
        "–",
        "N/A",
        "NA",
        "N.A.",
        "NONE",
        "NULL",
        "TBD",
        "TBA",
        "#N/A",
        "NOT APPLICABLE",
        "NOT AVAILABLE",
    ]
)

_WS_RE = re.compile(r"\s+")
_NUMBER_RE = re.compile(r"-?\d+(?:,\d{3})*(?:\.\d+)?")

_TRUE_WORDS = frozenset(["Y", "YES", "TRUE", "T", "1", "X", "✓", "STD", "STANDARD", "AVAILABLE", "INCLUDED"])
_FALSE_WORDS = frozenset(["N", "NO", "FALSE", "F", "0", "NOT AVAILABLE", "NONE"])


def clean(value: typing.Any) -> typing.Optional[str]:
    """Whitespace-collapsed text, or None for anything in :data:`BLANKS`. The gate everything passes."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = _WS_RE.sub(" ", str(value)).strip()
    # A stray non-breaking space is invisible in the source file and makes an exact-match blank
    # check fail, so strip those too rather than letting '\xa0' through as a value.
    text = text.replace("\xa0", " ").strip()
    if text.upper() in BLANKS:
        return None
    return text


def text(value: typing.Any, *, max_length: typing.Optional[int] = None) -> Result:
    """
    Trimmed text. Over-long values are truncated rather than dropped: the column limits here are
    generous, and a 130-character OE marking is still worth having in a 128-character column --
    but the truncation is reported, because it is usually a mapping pointed at a description.
    """
    cleaned = clean(value)
    if cleaned is None:
        return None, None
    if max_length is not None and len(cleaned) > max_length:
        return cleaned[:max_length], f"truncated to {max_length} chars: {cleaned[:60]!r}"
    return cleaned, None


def integer(value: typing.Any) -> Result:
    """First whole number in the string. ``'2,205 lbs (116)'`` -> 2205, ``'65,000 miles'`` -> 65000."""
    cleaned = clean(value)
    if cleaned is None:
        return None, None
    match = _NUMBER_RE.search(cleaned)
    if match is None:
        return None, f"no number in {cleaned!r}"
    try:
        return int(round(float(match.group(0).replace(",", "")))), None
    except (ValueError, OverflowError):
        return None, f"no number in {cleaned!r}"


def decimal_value(value: typing.Any) -> Result:
    """First decimal number in the string, as ``Decimal``. Floats never enter this package."""
    cleaned = clean(value)
    if cleaned is None:
        return None, None
    match = _NUMBER_RE.search(cleaned)
    if match is None:
        return None, f"no number in {cleaned!r}"
    try:
        return decimal.Decimal(match.group(0).replace(",", "")), None
    except decimal.InvalidOperation:
        return None, f"no number in {cleaned!r}"


def boolean(value: typing.Any) -> Result:
    """
    Tri-state. Yes/No/blank, and the blank is a real answer: it means the brand made no claim.

    Anything that is neither a yes-word nor a no-word is *reported and dropped*, not coerced.
    A '3PMSF' column carrying 'Severe Snow' is a column we have misread, and the way to find that
    out is a warning rather than a table full of False.
    """
    cleaned = clean(value)
    if cleaned is None:
        return None, None
    upper = cleaned.upper()
    if upper in _TRUE_WORDS:
        return True, None
    if upper in _FALSE_WORDS:
        return False, None
    return None, f"not yes/no: {cleaned!r}"


def tread_depth(value: typing.Any) -> Result:
    """
    32nds of an inch, as a number. ``'11/32"'``, ``'11.5/32nds'``, ``'11.5'`` all read 11.5.

    The denominator is dropped rather than divided: the column is *in* 32nds, which is how the
    industry quotes tread and how ``tire_specs.tread_depth_32nds`` stores it. Dividing here would
    silently produce 0.34 inches and look plausible.
    """
    cleaned = clean(value)
    if cleaned is None:
        return None, None
    match = re.search(r"(\d+(?:\.\d+)?)\s*/\s*32", cleaned)
    if match:
        return decimal.Decimal(match.group(1)), None
    if re.search(r"/\s*(?!32)\d+", cleaned):
        # '11/16' is not a tread depth in 32nds and must not be read as 11.
        return None, f"tread depth in unexpected units: {cleaned!r}"
    return decimal_value(cleaned)


def miles(value: typing.Any) -> Result:
    """Warranty mileage. ``'65k'`` -> 65000, ``'65,000 miles'`` -> 65000, ``'N/A'`` -> None."""
    cleaned = clean(value)
    if cleaned is None:
        return None, None
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*[kK]", cleaned)
    if match:
        return int(float(match.group(1)) * 1000), None
    return integer(cleaned)


_UTQG_RE = re.compile(r"^(?P<wear>\d{2,4})\s*[- ]?\s*(?P<traction>AAA|AA|[ABC])\s*[- ]?\s*(?P<temp>[ABC])$", re.I)


def utqg(value: typing.Any) -> typing.Tuple[typing.Optional[str], typing.Dict[str, typing.Any], typing.Optional[str]]:
    """
    ``'500 A A'`` / ``'500AA'`` / ``'500-A-A'`` -> the string as printed, plus its three graded parts.

    Returned split as well as whole because the grades are separately searchable and the printed
    form is what a customer recognises. A grade the pattern does not cover is kept as text with a
    warning rather than forced into the three columns.
    """
    cleaned = clean(value)
    if cleaned is None:
        return None, {}, None
    match = _UTQG_RE.match(cleaned)
    if match is None:
        return cleaned, {}, f"UTQG not in 'wear traction temperature' form: {cleaned!r}"
    return (
        cleaned,
        {
            "utqg_treadwear": int(match.group("wear")),
            "utqg_traction": match.group("traction").upper(),
            "utqg_temperature": match.group("temp").upper(),
        },
        None,
    )


_SERVICE_DESC_RE = re.compile(
    r"^(?P<single>\d{2,3})(?:\s*/\s*(?P<dual>\d{2,3}))?\s*(?P<speed>[A-Z]\d?|\(\w\d?\))?$", re.I
)


def service_description(value: typing.Any) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Optional[str]]:
    """
    ``'116T'`` -> load index 116, speed T. ``'121/118Q'`` -> single 121, dual 118, speed Q.

    Brands print this as one cell more often than as three, and the dual index is not optional
    detail: on a commercial tire it is the figure that governs a dual-wheel axle's rating.
    """
    cleaned = clean(value)
    if cleaned is None:
        return {}, None
    match = _SERVICE_DESC_RE.match(cleaned.replace(" ", ""))
    if match is None:
        return {}, f"service description not in '116T' form: {cleaned!r}"
    out: typing.Dict[str, typing.Any] = {"load_index": int(match.group("single"))}
    if match.group("dual"):
        out["load_index_dual"] = int(match.group("dual"))
    if match.group("speed"):
        out["speed_rating"] = match.group("speed").strip("()").upper()
    return out, None


_LOAD_RANGE_RE = re.compile(
    r"^(?:LR\s*)?(?P<letter>SL|XL|LL|HL|[A-N])\b(?:\s*\(?\s*(?P<ply>\d+)\s*(?:PLY|PR)\)?)?$", re.I
)
_LOAD_RANGE_WORDS = {
    "STANDARD": "SL",
    "STANDARD LOAD": "SL",
    "EXTRA LOAD": "XL",
    "REINFORCED": "XL",
    "LIGHT LOAD": "LL",
    "HIGH LOAD": "HL",
}


def load_range(value: typing.Any) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Optional[str]]:
    """
    ``'E (10 Ply)'`` -> load range E and a ply rating of 10; ``'Standard Load'`` -> SL.

    One cell, two facts, and the ply half is the one that is hard to get elsewhere: it is only
    derivable from the letter for LT tires, and not at all for the SL/XL passenger codes.
    """
    cleaned = clean(value)
    if cleaned is None:
        return {}, None
    upper = cleaned.upper().replace("-", " ").strip()
    word = _LOAD_RANGE_WORDS.get(upper)
    if word:
        return {"load_range": word}, None
    match = _LOAD_RANGE_RE.match(upper)
    if match is None:
        return {}, f"load range unrecognised: {cleaned!r}"
    out: typing.Dict[str, typing.Any] = {"load_range": match.group("letter").upper()}
    if match.group("ply"):
        out["ply_rating"] = int(match.group("ply"))
    return out, None


def rim_width_range(value: typing.Any) -> typing.Tuple[typing.Dict[str, typing.Any], typing.Optional[str]]:
    """
    ``'6.0" - 8.0"'`` -> min 6.0, max 8.0. A single width ``'7.5"'`` sets both, as the range it is.
    """
    cleaned = clean(value)
    if cleaned is None:
        return {}, None
    numbers = _NUMBER_RE.findall(cleaned.replace('"', " "))
    if not numbers:
        return {}, f"no widths in {cleaned!r}"
    try:
        values = [decimal.Decimal(number.replace(",", "")) for number in numbers[:2]]
    except decimal.InvalidOperation:
        return {}, f"no widths in {cleaned!r}"
    low, high = (values[0], values[-1])
    if low > high:
        low, high = high, low
    return {"rim_width_min_in": low, "rim_width_max_in": high}, None


def ply_rating(value: typing.Any) -> Result:
    """``'10 Ply'`` / ``'10PR'`` / ``'10'`` -> 10."""
    return integer(value)
